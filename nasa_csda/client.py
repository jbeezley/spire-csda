from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
import logging
from pathlib import Path, PurePosixPath
from typing import AsyncIterable, AsyncIterator, Optional, Union

from httpx import AsyncClient, HTTPStatusError, Request
from tqdm.asyncio import tqdm

from nasa_csda.config import Settings
from nasa_csda.models.link import DownloadLink
from nasa_csda.models.item_collection import CSDAItemCollection
from nasa_csda.models.search import CSDASearch
from nasa_csda.transport import RetryableTransport

logger = logging.getLogger(__name__)
_session: ContextVar[AsyncClient] = ContextVar("session")
_client: ContextVar[Client] = ContextVar("client")


class CSDAClientError(Exception):
    pass


class Client(object):
    def __init__(self, config: Settings) -> None:
        self.config = config
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._expiration: Optional[datetime] = None

    @asynccontextmanager
    async def stream_context(self) -> AsyncIterator[Client]:
        token = _client.set(self)
        async with self.session():
            yield self
        _client.reset(token)

    @property
    def current_session(self) -> AsyncClient:
        try:
            return _session.get()
        except LookupError:
            raise CSDAClientError("Client method must be used inside a session context.")

    @classmethod
    def current_client(self) -> Client:
        try:
            return _client.get()
        except LookupError:
            raise CSDAClientError("Client method must be used inside a streaming context.")

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncClient]:
        config = self.config
        transport = RetryableTransport(config)

        async def set_auth(request: Request):
            if str(self.config.api) in str(request.url):
                request.headers.setdefault("Authorization", f"Bearer {await self.get_token()}")

        async with AsyncClient(
            transport=transport,
            base_url=str(self.config.api),
            timeout=None,
            event_hooks={
                "request": [set_auth],
            },
        ) as session:
            if self._access_token is None:
                await self._login(session)
            token = _session.set(session)
            yield session
            _session.reset(token)

    async def get_token(self) -> str:
        session = self.current_session
        assert self._access_token, "Login failed"
        if not self._expiration or self._expiration < datetime.now():
            await self._refresh(session)
        return self._access_token

    async def search(self, *queries: CSDASearch) -> AsyncIterator[CSDAItemCollection]:
        page_size = self.config.search_page_size
        session = self.current_session
        token: Optional[str] = None
        item_count = 0
        for query in queries:
            while True:
                query_json = query.model_copy(update={"token": token, "limit": page_size}).model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
                resp = await session.post("stac/search", json=query_json)
                if resp.status_code != 200:
                    raise ValueError(resp.content)
                items = CSDAItemCollection.model_validate_json(resp.content)
                yield items
                token = items.next_token
                item_count += len(items.features)
                if token is None:
                    break

    async def download_links(
        self,
        iterator: AsyncIterable[CSDAItemCollection],
        /,
    ) -> AsyncIterator[DownloadLink]:
        async for item_collection in iterator:
            for item in item_collection.features:
                for asset in item.assets.values():
                    try:
                        url = DownloadLink.parse_url(f"{self.config.api}{asset.href.lstrip('/')}")
                    except Exception:
                        logger.exception(f"Could not parse {asset.href}")
                    yield url

    async def download(
        self,
        links: AsyncIterable[Union[str, DownloadLink]],
        prefix: str,
        overwrite: bool = False,
    ) -> AsyncIterator[Optional[str]]:

        async for link in links:
            yield await self.download_file(link, prefix=prefix, overwrite=overwrite, progress=self.config.download_progress)

    async def download_file(
        self,
        url: Union[str, DownloadLink],
        prefix: str,
        *,
        overwrite: bool = False,
        progress: bool = False,
    ) -> Optional[str]:
        session = self.current_session
        if not isinstance(url, DownloadLink):
            url = DownloadLink.parse_url(url)
        prefix_path = Path(PurePosixPath(prefix.format_map(url.model_dump())))
        destination = prefix_path / str(url).split("/")[-1]
        if destination.exists() and not overwrite:
            return str(destination)
        prefix_path.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open(mode="wb") as f:
                async with session.stream("GET", str(url), follow_redirects=True) as resp:
                    if resp.status_code == 404:
                        logger.warning(f"Missing file at {url}")
                        return None
                    try:
                        resp.raise_for_status()
                    except HTTPStatusError:
                        logger.error((await resp.aread()).decode())
                    total = None
                    if "content-length" in resp.headers:
                        total = int(resp.headers["content-length"])
                    prog = tqdm(
                        unit="b",
                        unit_scale=True,
                        desc=f"{destination.name:30}",
                        leave=False,
                        total=total,
                        delay=0.5,
                        disable=not progress,
                    )
                    with prog as bar:
                        async for chunk in resp.aiter_bytes():
                            bar.update(len(chunk))
                            f.write(chunk)
        except BaseException:
            # delete partial file on any exception
            destination.unlink(missing_ok=True)
            raise
        return str(destination)

    async def _login(self, session: AsyncClient) -> None:
        config = self.config
        data = {
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": config.cognito_client_id,
            "AuthParameters": {
                "PASSWORD": self.config.password.get_secret_value(),
                "USERNAME": self.config.username,
            },
        }
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }
        resp = await session.post(self.config.cognito_endpoint, json=data, headers=headers)
        if resp.status_code != 200:
            raise CSDAClientError("Authentication failure")
        auth = resp.json()["AuthenticationResult"]
        self._expiration = datetime.now() + timedelta(seconds=auth["ExpiresIn"]) - timedelta(minutes=5)
        self._access_token = auth["AccessToken"]
        self._refresh_token = auth["RefreshToken"]

    async def _refresh(self, session: AsyncClient) -> None:
        config = self.config

        # If the refresh token has expired, we try logging in again.
        data = {
            "AuthFlow": "REFRESH_TOKEN_AUTH",
            "ClientId": config.cognito_client_id,
            "AuthParameters": {
                "REFRESH_TOKEN": self._refresh_token,
            },
        }
        headers = {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": "AWSCognitoIdentityProviderService.InitiateAuth",
        }
        resp = await session.post(
            "https://cognito-idp.us-east-1.amazonaws.com/",
            json=data,
            headers=headers,
        )
        if resp.status_code != 200:
            await self._login(session)

        assert self._access_token, "Login failed"
        auth = resp.json()["AuthenticationResult"]
        self._access_token = auth["AccessToken"]
