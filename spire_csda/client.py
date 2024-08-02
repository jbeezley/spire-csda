from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import AsyncIterator, Optional, Union

from aiostream import stream
from httpx import AsyncClient, HTTPStatusError, Request
from tqdm.asyncio import tqdm

from spire_csda.buffer import buffered
from spire_csda.config import Settings
from spire_csda.models.link import DownloadLink
from spire_csda.models.item_collection import CSDAItemCollection
from spire_csda.models.search import CSDASearch
from spire_csda.transport import RetryableTransport

logger = logging.getLogger(__name__)
_session: ContextVar[AsyncClient] = ContextVar("session")


class CSDAClientError(Exception):
    pass


class Client(object):
    _cognito_client_id = "7agre1j1gooj2jng6mkddasp9o"

    def __init__(self) -> None:
        config = Settings.current()
        self.username = config.username
        self.password = config.password
        self.base_url = config.api
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expiration: Optional[datetime] = None

    @property
    def current_session(self) -> AsyncClient:
        try:
            return _session.get()
        except LookupError:
            raise CSDAClientError("Client method must be used inside a session context.")

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncClient]:
        config = Settings.current()
        transport = RetryableTransport(http2=config.use_http2)

        async def set_auth(request: Request):
            if str(self.base_url) in str(request.url):
                request.headers.setdefault("Authentication", f"Bearer {await self.get_token()}")

        async with AsyncClient(
            transport=transport,
            base_url=str(self.base_url),
            timeout=None,
            event_hooks={
                "request": [set_auth],
            },
        ) as session:
            if self._access_token is None:
                await self._login(session)
            token = _session.set(session)
            yield session
            _session.reset(token)

    async def _login(self, session: AsyncClient) -> None:
        data = {
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": self._cognito_client_id,
            "AuthParameters": {
                "PASSWORD": self.password.get_secret_value(),
                "USERNAME": self.username,
            },
        }
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }
        resp = await session.post("https://cognito-idp.us-east-1.amazonaws.com/", json=data, headers=headers)
        if resp.status_code != 200:
            raise ValueError("Authentication failure")
        auth = resp.json()["AuthenticationResult"]
        self._expiration = datetime.now() + timedelta(seconds=auth["ExpiresIn"]) - timedelta(minutes=5)
        self._access_token = auth["AccessToken"]
        self._refresh_token = auth["RefreshToken"]

    async def _refresh(self, session: AsyncClient) -> None:
        # If the refrest token has expired, we try logging in again.
        for _ in range(2):
            data = {
                "AuthFlow": "REFRESH_TOKEN_AUTH",
                "ClientId": self._cognito_client_id,
                "AuthParameters": {
                    "REFRESH_TOKEN": self._refresh_token,
                },
            }
            headers = {
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
            }
            resp = await session.post(
                "https://cognito-idp.us-east-1.amazonaws.com/",
                json=data,
                headers=headers,
            )
            if resp.status_code == 200:
                break
            await self._login(session)
        else:
            raise CSDAClientError("Authentication failure")

        assert self._access_token, "Login failed"
        auth = resp.json()["AuthenticationResult"]
        self._access_token = auth["AccessToken"]

    async def get_token(self) -> str:
        session = self.current_session
        assert self._access_token, "Login failed"
        if not self._expiration or self._expiration < datetime.now():
            await self._refresh(session)
        return self._access_token

    async def search(
        self,
        query: CSDASearch,
        *,
        limit: Optional[int] = None,
        parallel: int = 1,
        page_size: int = 100,
    ) -> AsyncIterator[CSDAItemCollection]:
        session = self.current_session
        token: Optional[str] = None
        item_count = 0
        while True:
            page_size = min(page_size, limit - item_count) if limit is not None else page_size
            query = query.copy(update={"token": token, "limit": page_size})

            resp = await session.post("stac/search", content=query.json(exclude_none=True, by_alias=True))
            if resp.status_code != 200:
                raise ValueError(resp.content)
            items = CSDAItemCollection.parse_raw(resp.content)
            yield items
            token = items.next_token
            item_count += len(items.features)
            if token is None or (limit is not None and item_count >= limit):
                break

    async def search_parallel(
        self,
        query: CSDASearch,
        *,
        limit: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[CSDAItemCollection]:
        async def run_search(query: CSDASearch):
            async for item in self.search(query, **kwargs):
                yield item

        def iterate_partitions(query: CSDASearch):
            for month_partition in query.split_by_datetime():
                for product_partition in month_partition.split_by_product():
                    yield product_partition

        streamer = stream.flatmap(
            stream.iterate(iterate_partitions(query)),
            run_search,
            task_limit=10,
        )
        item_count = 0
        async with streamer.stream() as iterator:
            async for item in iterator:
                if limit is not None and item_count >= limit:
                    break
                yield item
                item_count += 1

    async def download_links(
        self,
        query: CSDASearch,
        *,
        limit: Optional[int] = None,
        page_size: int = 100,
    ) -> AsyncIterator[DownloadLink]:
        query = query.copy(update={"fields": {"include": {"assets"}}})
        found: set[str] = set()

        async def get_urls(items: CSDAItemCollection) -> AsyncIterator[DownloadLink]:
            for item in items.features:
                for asset in item.assets.values():
                    try:
                        url = DownloadLink.parse_url(f"{self.base_url}{asset.href.lstrip('/')}")
                    except Exception:
                        logger.exception(f"Could not parse {asset.href}")
                    if url.file not in found:
                        found.add(url.file)
                        yield url

        streamer = stream.flatmap(
            self.search_parallel(query, page_size=page_size),
            get_urls,
            task_limit=1,
        )

        async with streamer.stream() as iterator:
            async for url in buffered(iterator, 1000, limit):
                yield url

    async def download_file(
        self,
        url: Union[str, DownloadLink],
        prefix: str,
        *,
        overwrite: bool = False,
        progress: bool = False,
    ) -> bool:
        session = self.current_session
        if not isinstance(url, DownloadLink):
            url = DownloadLink.parse_url(url)
        prefix_path = Path(prefix.format_map(url.model_dump()))
        destination = prefix_path / str(url).split("/")[-1]
        if destination.exists() and not overwrite:
            return True
        prefix_path.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open(mode="wb") as f:
                async with session.stream("GET", str(url), follow_redirects=True) as resp:
                    if resp.status_code == 404:
                        logger.warning(f"Missing file at {url}")
                        return False
                    try:
                        resp.raise_for_status()
                    except HTTPStatusError:
                        logger.error((await resp.aread()).decode())
                    total = None
                    if "content-length" in resp.headers:
                        total = int(resp.headers["content-length"])
                    prog = tqdm(
                        unit="b",
                        unit_scale=True,
                        desc=f"{destination.name:30}",
                        leave=False,
                        total=total,
                        delay=0.5,
                        disable=not progress,
                    )
                    with prog as bar:
                        async for chunk in resp.aiter_bytes():
                            bar.update(len(chunk))
                            f.write(chunk)
        except BaseException:
            destination.unlink(missing_ok=True)
            raise
        return True

    async def download_query(
        self,
        query: CSDASearch,
        *,
        overwrite: bool = False,
        prefix: str = "download/{product}/{datetime:%Y}/{datetime:%m}/{datetime:%d}",
        parallel: int = 16,
        limit: Optional[int] = None,
        page_size: int = 100,
        progress: bool = False,
    ) -> AsyncIterator[DownloadLink]:
        async def download_file(link: DownloadLink):
            try:
                return link, await self.download_file(link, prefix=prefix, overwrite=overwrite, progress=False)
            except HTTPStatusError:
                logger.exception(str(link))
            return link, False

        streamer = stream.map(
            self.download_links(query, limit=limit, page_size=page_size),
            download_file,
            task_limit=parallel,
        )
        async with streamer.stream() as iterator:
            async for link, success in iterator:
                if success:
                    yield link
