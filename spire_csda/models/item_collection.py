from typing import Optional

from stac_pydantic.api.item_collection import ItemCollection
from stac_pydantic.api.links import PaginationLink, SearchLink


class CSDAItemCollection(ItemCollection):
    @property
    def next_token(self) -> Optional[str]:
        if self.links is None:
            return None
        for link in self.links.link_iterator():
            if not isinstance(link, (PaginationLink, SearchLink)):
                continue
            rel = link.rel
            if rel == "next" and link.body is not None:
                return link.body.get("token")
        return None
