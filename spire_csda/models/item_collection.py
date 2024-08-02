from typing import Optional

from stac_pydantic.api.item_collection import ItemCollection
from stac_pydantic.api.links import PaginationLink, SearchLink


class CSDAItemCollection(ItemCollection):
    @property
    def next_token(self) -> Optional[str]:
        for link in self.links.link_iterator():
            rel = link.rel
            if not isinstance(rel, str):
                rel = rel.value
            if isinstance(link, PaginationLink) and rel == "next":
                return link.token
            elif isinstance(link, SearchLink) and rel == "next":
                return link.body.get("token")
        return None
