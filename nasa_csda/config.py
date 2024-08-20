from __future__ import annotations

import logging

from pydantic import HttpUrl, SecretStr
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

    username: str = ""
    password: SecretStr = SecretStr("")
    api: HttpUrl = HttpUrl("https://nasa-csda.wx.spire.com/")
    cognito_client_id: str = "7agre1j1gooj2jng6mkddasp9o"

    search_page_size: int = 100
    concurrent_downloads: int = 12
    item_buffer_size: int = 10
    retry_count: int = 10
    max_retry_wait_seconds: int = 30
    concurrent_searches: int = 4
    use_http2: bool = True
    download_progress: bool = False
    max_deduplication_cache: int = 1000
