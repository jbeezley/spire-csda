from __future__ import annotations

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Iterator, Literal, Optional, Union

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field
from stac_pydantic.api.search import ExtendedSearch


class Operator(str, Enum):
    EQ = "="
    AND = "and"
    OR = "or"
    LIKE = "like"
    IN = "in"
    T_INTERSECTS = "t_intersects"  # temporal intersection
    S_INTERSECTS = "s_intersects"  # spatial intersection
    S_WITHIN = "s_within"  # geometry within
    S_CONTAINS = "s_contains"  # geometry contains
    A_CONTAINS = "a_contains"  # array operator contains
    A_OVERLAPS = "a_overlaps"


class GeometryType(str, Enum):
    POLYGON = "Polygon"
    POINT = "Point"


class PropertyQuery(BaseModel):
    property: str


class GeometryTypeQuery(BaseModel):
    type: GeometryType


class IntervalQuery(BaseModel):
    interval: list[str]


class DatetimeQuery(BaseModel):
    timestamp: datetime


class CoordinatesQuery(BaseModel):
    coordinates: Any


class FilterExpression(BaseModel):
    """
    https://github.com/stac-api-extensions/filter
    https://docs.up42.com/developers/api-assets/stac-cql
    """

    op: Operator
    args: list[
        Union[
            PropertyQuery,
            IntervalQuery,
            FilterExpression,
            DatetimeQuery,
            CoordinatesQuery,
            GeometryTypeQuery,
            int,
            float,
            str,
            list,
            datetime,
        ]
    ]


class CSDASearch(ExtendedSearch):
    token: Optional[str] = None
    filter: Optional[FilterExpression] = None
    filter_lang: Literal["cql2-json"] = Field("cql2-json", alias="filter-lang")

    @property
    def start_end(self) -> tuple[datetime, datetime]:
        start = datetime(2019, 10, 1) if self.start_date is None else self.start_date
        end = datetime.now() if self.end_date is None else self.end_date
        start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
        end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
        return start, end

    def split_by_datetime(self) -> Iterator[CSDASearch]:
        start, end = self.start_end
        month = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        while month <= end:
            month_start = max(start, month).isoformat()
            month_end = min(end, month + relativedelta(months=1)).isoformat()
            yield self.copy(update={"datetime": f"{month_start}/{month_end}"})
            month += relativedelta(months=1)

    def split_by_product(self) -> Iterator[CSDASearch]:
        if self.filter and self.filter.op == Operator.IN and self.filter.args[0] == PropertyQuery(property="item_type"):
            for product in self.filter.args[1]:  # type: ignore
                yield self.copy(update={"filter": FilterExpression(op=Operator.EQ, args=[self.filter.args[0], product])})
        else:
            yield self

    def split(self) -> Iterator[CSDASearch]:
        for q in self.split_by_datetime():
            for p in self.split_by_product():
                yield p

    @classmethod
    def build_query(
        cls,
        start_date: datetime,
        end_date: datetime,
        min_latitude: float,
        max_latitude: float,
        min_longitude: float,
        max_longitude: float,
        products: str,
    ) -> CSDASearch:
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc) - timedelta(days=30)

        field_filter = None
        if products:
            field_filter = {
                "op": "in",
                "args": [
                    {"property": "item_type"},
                    products.split(","),
                ],
            }

        datetime_ = f"{start_date.isoformat()}/{end_date.isoformat()}"
        return CSDASearch(
            bbox=[
                min_longitude,
                min_latitude,
                max_longitude,
                max_latitude,
            ],
            datetime=datetime_,
            filter=field_filter,
        )
