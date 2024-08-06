from pathlib import Path
from tempfile import TemporaryDirectory

from spire_csda.client import Client


async def test_authenticate(httpx_csda, config):
    client = Client(config)
    async with client.session():
        assert (await client.get_token()) is not None


async def test_search(httpx_csda_client, config, search):
    client = Client(config)
    async with client.session():
        items = [item async for item in client.search(search.build())]

    assert len(items) == 2


async def test_list(httpx_csda_client, config, search):
    client = Client(config)
    async with client.session():
        links = [link async for link in client.download_links(client.search(search.build()))]

    assert len(links) == 4
    for link in links:
        assert link.url.startswith(f"{config.api}download")


async def test_download(httpx_csda_client, config, search):
    with TemporaryDirectory() as dir:
        client = Client(config)
        async with client.session():
            file_count = 0
            async for link in client.download(
                client.download_links(client.search(search.build())),
                prefix=dir,
            ):
                file_count += 1
        assert file_count == 4
        files = list(Path(dir).iterdir())
        assert len(files) == 2
        for file in files:
            with file.open("rb") as f:
                assert f.read() == b"chunk" * 5
