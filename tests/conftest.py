from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from typing import Any, Generic, Type, TypeVar

from httpx import Request, Response
from polyfactory.pytest_plugin import register_fixture
from polyfactory.factories.pydantic_factory import ModelFactory
from pydantic import BaseModel
import pytest
from pytest_httpx import HTTPXMock, IteratorStream
from stac_pydantic.shared import BBox
from stac_pydantic.api.search import FieldsExtension
from stac_pydantic.api.item import Item

from spire_csda.client import Client
from spire_csda.config import Settings
from spire_csda.models.item_collection import CSDAItemCollection
from spire_csda.models.search import CSDASearch

T = TypeVar("T", bound=BaseModel)
HERE = Path(__file__).parent


with open(HERE / "base_item_collection.json") as f:
    _item_collection = json.load(f)


class BaseFactory(Generic[T], ModelFactory[T]):
    __is_base_factory__ = True
    __random_seed__ = 1

    @classmethod
    def random_range(cls, min: float, max: float) -> float:
        return cls.__random__.random() * (max - min) - min

    @classmethod
    def get_provider_map(cls) -> dict[Type, Any]:
        providers_map = super().get_provider_map()
        return {
            Type[CSDAItemCollection]: generate_item_collection(cls),
            Type[Item]: generate_item(cls),
            **providers_map,
        }


def generate_item(factory: BaseFactory) -> Item:
    return generate_item_collection(factory).features[0]


def generate_item_collection(factory: BaseFactory) -> CSDAItemCollection:
    return CSDAItemCollection(**deepcopy(_item_collection))


class ItemFactory(BaseFactory[Item]):
    @classmethod
    def links(cls):
        return generate_item(cls).links

    @classmethod
    def properties(cls):
        return generate_item(cls).properties

    @classmethod
    def bbox(cls):
        return generate_item(cls).bbox

    @classmethod
    def geometry(cls):
        return generate_item(cls).geometry

    @classmethod
    def assets(cls):
        return generate_item(cls).assets


class CSDAItemCollectionFactory(BaseFactory[CSDAItemCollection]):
    @classmethod
    def features(cls) -> list[Item]:
        return [ItemFactory.build()]

    @classmethod
    def links(cls):
        return generate_item_collection(cls).links

    @classmethod
    def bbox(cls):
        return generate_item_collection(cls).bbox


class CSDASearchFactory(BaseFactory[CSDASearch]):
    @classmethod
    def datetime(cls) -> str:
        end = cls.__faker__.date_time(end_datetime=datetime.now(tz=timezone.utc) - timedelta(days=30), tzinfo=timezone.utc)
        start = cls.__faker__.date_time(end_datetime=end, tzinfo=timezone.utc)
        return f"{start}/{end}"

    @classmethod
    def bbox(cls) -> BBox:
        return [-180, -90, 180, 90]

    @classmethod
    def query(cls) -> None:
        return None

    @classmethod
    def sortby(cls) -> None:
        return None

    @classmethod
    def intersects(cls) -> None:
        return None

    @classmethod
    def filter(cls) -> FieldsExtension:
        return dict(
            op="in",
            args=[
                {"property": "item_type"},
                cls.__faker__.words(),
            ],
        )

    @classmethod
    def token(cls) -> str:
        return cls.__faker__.pystr()


csda_search_fixture = register_fixture(CSDASearchFactory, name="search")


@pytest.fixture
def config() -> Settings:
    with Settings(username="test", password="test").context() as c:
        yield c


@pytest.fixture
async def client(config) -> Client:
    client = Client(config)
    async with client.session():
        yield client


@pytest.fixture
def httpx_csda(httpx_mock: HTTPXMock, config: Settings) -> HTTPXMock:
    httpx_mock.add_response(
        url="https://cognito-idp.us-east-1.amazonaws.com/",
        method="POST",
        status_code=200,
        json={
            "AuthenticationResult": {
                "AccessToken": "access",
                "RefreshToken": "refresh",
                "ExpiresIn": 1e6,
            },
        },
    )
    yield httpx_mock
    httpx_mock.reset(False)


@pytest.fixture
def httpx_csda_client(httpx_csda: HTTPXMock, config: Settings) -> HTTPXMock:
    paged_response = CSDAItemCollectionFactory.build()
    end_response = CSDAItemCollectionFactory.build()
    end_response.links[0].body["token"] = None

    def search_callback(request: Request) -> Response:
        data = CSDASearch.model_validate_json(request.content)
        if data.token is None:
            body = paged_response
        else:
            body = end_response
        return Response(
            status_code=200,
            json=body.model_dump(mode="json"),
        )

    httpx_csda.add_callback(url=f"{config.api}stac/search", callback=search_callback)
    httpx_csda.add_response(
        status_code=307,
        url=re.compile(r".*/download/[^/]+/[^/]+"),
        headers={
            "Location": "https://spc-processing-prod.s3.amazonaws.com/download",
        },
    )
    httpx_csda.add_response(
        status_code=200,
        url="https://spc-processing-prod.s3.amazonaws.com/download",
        stream=IteratorStream([b"chunk"] * 5),
    )
