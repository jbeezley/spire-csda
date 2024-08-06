from pathlib import Path
from tempfile import TemporaryDirectory

from aiostream.stream import iterate

from spire_csda.client import Client
from spire_csda.streaming import download, extract_links, search as search_


async def test_search(httpx_csda_client, config, search):
    client = Client(config)
    with TemporaryDirectory() as dir:
        async with client.stream_context():
            p = (
                iterate([search.build()])
                | search_.pipe(client=client)
                | extract_links.pipe(client=client)
                | download.pipe(client=client, prefix=dir)
            )

            count = 0
            async with p.stream() as streamer:
                async for _ in streamer:
                    count += 1

            assert count == 2
            files = list(Path(dir).iterdir())
            assert len(files) == 2
            for file in files:
                with file.open("rb") as f:
                    assert f.read() == b"chunk" * 5
