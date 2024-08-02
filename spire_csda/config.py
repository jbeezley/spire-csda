from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import logging
from typing import Iterator

from pydantic import HttpUrl, SecretStr, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger()
_env_prefix = "csda_"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix=_env_prefix,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    username: str
    password: SecretStr
    api: HttpUrl = HttpUrl("https://nasa-csda.wx.spire.com/")
    cognito_client_id: str = "7agre1j1gooj2jng6mkddasp9o"

    parallel_downloads: int = 12
    item_buffer_size: int = 100
    retry_count: int = 10
    max_retry_wait_seconds: int = 30
    concurrent_searches: int = 4
    use_http2: bool = True

    @contextmanager
    def context(self) -> Iterator[Settings]:
        token = _config.set(self)
        yield self
        _config.reset(token)

    @classmethod
    def current(cls) -> Settings:
        try:
            try:
                config = _config.get()
            except LookupError:
                return cls()
        except ValidationError:
            logger.error("There are missing or invalid settings. " "See this projects README for settings configuration.")
            raise
        return config


_config: ContextVar[Settings] = ContextVar("config")
