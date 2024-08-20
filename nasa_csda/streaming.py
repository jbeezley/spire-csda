from collections.abc import MutableMapping
import logging
from typing import AsyncIterable, AsyncIterator, Optional

from aiostream.core import pipable_operator, streamcontext
from aiostream.stream import advanced
from cachetools import LRUCache

from nasa_csda.client import Client
from nasa_csda.models.link import DownloadLink
from nasa_csda.models.item_collection import CSDAItemCollection
from nasa_csda.models.search import CSDASearch

logger = logging.getLogger(__name__)


@pipable_operator
async def search(
    query: AsyncIterable[CSDASearch],
    client: Client,
    task_limit: Optional[int] = None,
) -> AsyncIterator[CSDAItemCollection]:
    """Perform concurrent searches to the CSDA STAC instance."""
    async for item in advanced.flatmap.raw(query, client.search, task_limit=task_limit):
        yield item


@pipable_operator
async def extract_links(item_collection: AsyncIterable[CSDAItemCollection], client: Client) -> AsyncIterator[DownloadLink]:
    """Extract links from STAC features."""
    base_url = client.config.api

    found: MutableMapping[str, bool] = LRUCache(maxsize=client.config.max_deduplication_cache)
    async with streamcontext(item_collection) as streamer:
        async for feature in streamer:
            for item in feature.features:
                for asset in item.assets.values():
                    file = asset.href.split("/")[-1]
                    if file in found:
                        continue
                    found[file] = True
                    yield DownloadLink.parse_url(f"{base_url}{asset.href.lstrip('/')}")


@pipable_operator
async def download(
    links: AsyncIterable[DownloadLink],
    client: Client,
    prefix: str,
    task_limit: Optional[int] = None,
) -> AsyncIterator[Optional[str]]:
    """Download links concurrently."""

    async def _download(
        *links: DownloadLink,
    ) -> AsyncIterable[Optional[str]]:
        for link in links:
            yield await client.download_file(url=link, prefix=prefix)

    async for success in advanced.flatmap.raw(links, _download, task_limit=task_limit):
        yield success
