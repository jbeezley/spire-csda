import logging

from httpx import AsyncHTTPTransport, Request, Response
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

from spire_csda.config import Settings

logger = logging.getLogger(__name__)


class RetryableTransport(AsyncHTTPTransport):
    def __init__(self, config: Settings, *args, **kwargs) -> None:
        kwargs.setdefault("http2", config.use_http2)
        super().__init__(*args, **kwargs)
        self.__config = config

    async def handle_async_request(self, request: Request) -> Response:
        path = request.url.path
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self.__config.retry_count),
            wait=wait_exponential_jitter(max=self.__config.max_retry_wait_seconds),
            reraise=True,
        ):
            with attempt:
                resp = await super().handle_async_request(request)
                if resp.status_code >= 500:
                    msg = f"Failed request to /{path} ({resp.status_code}) attempt {attempt.retry_state.attempt_number}/10"
                    logger.debug(msg)
                    raise Exception(await resp.aread())
                return resp
        assert False
