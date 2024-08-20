from datetime import datetime
from typing import AsyncIterator, TextIO, Optional, TypeVar

from aiostream import pipe, stream
import asyncclick as click
from pydantic import SecretStr, ValidationError
from tqdm.asyncio import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from nasa_csda.buffer import Buffer
from nasa_csda.client import Client
from nasa_csda.config import Settings
from nasa_csda.models.bulk_download import BulkDownload
from nasa_csda.models.item_collection import CSDAItemCollection
from nasa_csda.models.search import CSDASearch
from nasa_csda.streaming import download, extract_links, search

T = TypeVar("T")


async def _run_search(
    client: Client,
    *queries: CSDASearch,
    mode: str,
    limit: Optional[int],
    destination: str,
    progress: bool,
) -> None:
    async def identity(x: T) -> T:
        return x

    async def split_queries(*qs: CSDASearch) -> AsyncIterator[CSDASearch]:
        for q in qs:
            for s in q.split():
                yield s

    async with client.session(), Buffer[CSDAItemCollection, [], CSDASearch](client.config.item_buffer_size) as buffered:
        p = (
            stream.iterate(queries)
            | pipe.flatmap(split_queries)
            | search.pipe(client, task_limit=client.config.concurrent_searches)
            | buffered.pipe()
        )
        if mode != "raw":
            p |= extract_links.pipe(client=client)  # type: ignore

        if mode == "download":
            p = p | download.pipe(client=client, prefix=destination, task_limit=10) | pipe.filter(identity)  # type: ignore

        if limit is not None:
            p = p | pipe.take(limit)

        with tqdm(unit=" files", disable=not progress) as pbar, logging_redirect_tqdm():
            async with p.stream() as streamer:
                async for item in streamer:
                    pbar.update(1)
                    if mode == "raw":
                        click.echo(item.model_dump_json())
                    elif mode == "list":
                        click.echo(item)


@click.group()
@click.option("--username", envvar="CSDA_USERNAME")
@click.option("--password", envvar="CSDA_PASSWORD")
@click.option("--settings-file", envvar="CSDA_SETTINGS_FILE", default=".env")
@click.pass_context
def cli(ctx, username, password, settings_file):
    kwargs = {}
    if username:
        kwargs["username"] = username
    if password:
        kwargs["password"] = password
    config = Settings(_env_file=settings_file, **kwargs)
    if not config.username or not config.password.get_secret_value():
        raise click.BadParameter(
            "username and password must be provided",
            ctx,
            None,
            "username, password",
        )
    if username is not None:
        config = config.model_copy(update={"username": username})
    if password is not None:
        config = config.model_copy(update={"password": SecretStr(password)})
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["client"] = Client(config)


@cli.command()
@click.pass_obj
async def token(obj):
    """Print an authentication token for STAC endpoints.

    This will log into the STAC catalog with the provided credentials.  The
    token returned will be valid for up to one hour.  In order to use this
    token, pass it as a Bearer token in the Authentication header.

    \b
    For example
    curl -H "Authentication:Bearer <token>" https://nasa-csda.wx.spire.com/stac/collections
    """
    client: Client = obj["client"]
    async with client.session():
        click.echo(await client.get_token())


@cli.command()
@click.option("--start-date", type=click.DateTime(), default=datetime(2019, 1, 1))
@click.option("--end-date", type=click.DateTime(), default=datetime.now())
@click.option("--min-latitude", type=click.FloatRange(min=-90, max=90), default=-90)
@click.option("--max-latitude", type=click.FloatRange(min=-90, max=90), default=90)
@click.option("--min-longitude", type=click.FloatRange(min=-180, max=180), default=-180)
@click.option("--max-longitude", type=click.FloatRange(min=-180, max=180), default=180)
@click.option("--products", default="", help="A comma-separated list of product names")
@click.option("--limit", type=click.IntRange(min=1), help="Stop after finding this many items")
@click.option(
    "--mode",
    type=click.Choice(["download", "list", "raw"]),
    default="download",
    help="Operational mode (see help)",
)
@click.option(
    "--progress/--no-progress",
    default=True,
    help="Enable progress reporting while downloading",
)
@click.option(
    "--destination",
    default="csda/{product}/{datetime:%Y}/{datetime:%m}/{datetime:%d}",
    help="Configure download location",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    help="Overwrite existing files when downloading",
)
@click.pass_context
async def query(
    ctx,
    start_date: datetime,
    end_date: datetime,
    min_latitude: float,
    max_latitude: float,
    min_longitude: float,
    max_longitude: float,
    products: str,
    limit: Optional[int],
    mode: str,
    progress: bool,
    destination: str,
    overwrite: bool,
):
    """
    Query data from Spire's CSDA catalog.

    This command can return data in three modes:

    \b
    * download: download files
    * list:     show urls
    * raw:      show raw geojson items

    When in download mode, you can configure the path where the file is placed
    with a format-style string as the `destination` parameter. This parameter
    allows you to provide any static path as well as sort the downloads into
    prefixes based on properties of the file.  These properties include:

    \b
    * collection
    * datetime
    * receiver
    * product

    In addition, it supports standard python format strings, so you can extract
    components of the datetime as path elements such as `{datetime:%Y-%m-%d}`
    to make a subdirectory for each day.  The path string is automatically
    converted to backslashes when running in Windows.

    The default destination is `csda/{product}/{datetime:%Y}/{datetime:%m}/{datetime:%d}`.
    """
    obj = ctx.obj
    if min_latitude > max_latitude:
        raise click.BadParameter(
            "min-latitude must be <= max-latitude",
            ctx,
            None,
            "min-latitude, max-latitude",
        )
    if min_longitude > max_longitude:
        raise click.BadParameter(
            "min-longitude must be <= max-longitude",
            ctx,
            None,
            "min-longitude, max-longitude",
        )
    if start_date > end_date:
        raise click.BadParameter(
            "start-date must be <= end-date",
            ctx,
            None,
            "start-date, end-date",
        )

    client: Client = obj["client"]
    query = CSDASearch.build_query(
        start_date,
        end_date,
        min_latitude,
        max_latitude,
        min_longitude,
        max_longitude,
        products,
    )

    await _run_search(client, query, mode=mode, limit=limit, destination=destination, progress=progress)


@cli.command()
@click.option("--limit", type=click.IntRange(min=1), help="Stop after downloading this many files")
@click.option(
    "--progress/--no-progress",
    default=True,
    help="Enable progress reporting while downloading",
)
@click.option(
    "--destination",
    default="csda/{product}/{datetime:%Y}/{datetime:%m}/{datetime:%d}",
    help="Configure download location",
)
@click.option(
    "--overwrite/--no-overwrite",
    default=True,
    help="Overwrite existing files when downloading",
)
@click.argument("config", type=click.File("r"), nargs=-1)
@click.pass_context
async def bulk_download(
    ctx,
    limit: Optional[int],
    progress: bool,
    destination: str,
    overwrite: bool,
    config: list[TextIO],
):
    """
    Bulk download files the Spire's CSDA catalog.

    You can configure the path where the file is placed with a format-style
    string as the `destination` parameter. This parameter allows you to provide
    any static path as well as sort the downloads into prefixes based on
    properties of the file.  These properties include:

    \b
    * collection
    * datetime
    * receiver
    * product

    In addition, it supports standard python format strings, so you can extract
    components of the datetime as path elements such as `{datetime:%Y-%m-%d}`
    to make a subdirectory for each day.  The path string is automatically
    converted to backslashes when running in Windows.

    The default destination is `csda/{product}/{datetime:%Y}/{datetime:%m}/{datetime:%d}`.
    """
    client: Client = ctx.obj["client"]
    for file in config:
        try:
            queries = BulkDownload.model_validate_json(file.read()).queries
        except ValidationError as err:
            click.secho(str(err), err=True, fg="red")
            raise click.BadParameter(
                "could not parse file",
                ctx,
                None,
                "config",
            )

    await _run_search(client, *queries, mode="download", limit=limit, destination=destination, progress=progress)
