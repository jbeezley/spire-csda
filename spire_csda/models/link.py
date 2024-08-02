from datetime import datetime as datetime_, timezone
import re

from pydantic import BaseModel

_link_re = re.compile(
    r"^.*/download"
    r"/(?P<collection>[^/]+)"
    r"/(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})"
    r"_(?P<receiver>FM\d+)"
    r"[^/]*"
    r"_(?P<product>[^/_]+)"
    r".*$"
)


class DownloadLink(BaseModel):
    url: str
    collection: str
    datetime: datetime_
    receiver: str
    product: str

    @classmethod
    def parse_url(cls, url: str) -> "DownloadLink":
        m = _link_re.match(url)
        if m is None:
            raise ValueError(f"Could not parse {url}")
        data = m.groupdict()
        data["datetime"] = datetime_.strptime(data["datetime"], "%Y-%m-%dT%H-%M-%S").replace(tzinfo=timezone.utc)
        return cls(url=url, **data)

    def __str__(self) -> str:
        return self.url

    @property
    def file(self) -> str:
        return self.url.split("/")[-1]
