"""Download a single file using the SDK."""

from asyncio import run

from spire_csda.client import Client
from spire_csda.config import Settings
from spire_csda.models.search import CSDASearch


async def download_first_file(query: CSDASearch):
    """Use the CSDA client to download the first file from a query."""

    # Initialize a client object using your credentials
    # config = Settings(username="user", password="pass")
    # or use environment variables
    config = Settings()
    client = Client(config)

    # create a session and get the first result
    async with client.session():
        link = None
        async for item_collection in client.search(query):
            for item in item_collection.features:
                for asset in item.assets.values():
                    link = asset.href
                    break
                break
            break

        if link is None:
            raise ValueError("No files found")

        # get a link from the item
        path = await client.download_file(link, prefix="csda")
        print(f"downloaded {path}")


def main():
    query = CSDASearch()
    run(download_first_file(query))


if __name__ == "__main__":
    main()
