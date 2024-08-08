"""Download files matching a single satellite."""

from asyncio import run

from aiostream import pipe, stream

from nasa_csda.buffer import Buffer
from nasa_csda.client import Client
from nasa_csda.config import Settings
from nasa_csda.models.item_collection import CSDAItemCollection
from nasa_csda.models.search import CSDASearch
from nasa_csda.streaming import download, extract_links, search

_max_buffer_size = 10
_max_concurrent_downloads = 4
_download_limit = 10


async def search_pipeline(query: CSDASearch, satellite: str):
    # Initialize a client object using your credentials
    # config = Settings(username="user", password="pass")
    # or use environment variables
    config = Settings()
    client = Client(config)

    def filter_by_satellite(collection: CSDAItemCollection) -> CSDAItemCollection:
        """Filter the features in a response to only those matching the satellite id."""
        features = [f for f in collection.features if f.properties.dict().get("ro:leo_satellite/id") == satellite]
        return collection.copy(update={"features": features})

    # Create a streaming and buffering context.
    # You can use aiostream pipes to interact with and modify the data
    # in the stream
    async with client.stream_context(), Buffer(_max_buffer_size) as buffered:
        # create a streaming pipeline
        p = (
            # generate a stream containing just the single query
            stream.iterate([query])
            # pipe query into the search streaming method to query catalog
            | search.pipe(client)
            # perform a custom filter based on the satellite id
            | pipe.map(filter_by_satellite)
            # add a buffer so that queries can occur while files are downloading
            | buffered.pipe()
            # extract all links from the catalog response
            | extract_links.pipe(client)
            # download all links and limit the number of concurrent downloads
            | download.pipe(client, prefix="csda", task_limit=_max_concurrent_downloads)
            # stop after downloading 10 files
            | pipe.take(_download_limit)
        )

        # execute the pipeline
        async with p.stream() as streamer:
            async for file in streamer:
                print(f"downloaded {file}")


def main():
    run(search_pipeline(CSDASearch(), "FM166"))


if __name__ == "__main__":
    main()
