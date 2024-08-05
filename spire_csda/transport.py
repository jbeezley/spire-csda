import logging

from httpx import AsyncHTTPTransport, Request, Response
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

logger = logging.getLogger(__name__)


class RetryableTransport(AsyncHTTPTransport):
    async def handle_async_request(self, request: Request) -> Response:
        path = request.url.path
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(1),
            wait=wait_exponential_jitter(max=60),
            reraise=True,
        ):
            with attempt:
                resp = await super().handle_async_request(request)
                if resp.status_code >= 500:
                    msg = f"Failed request to /{path} ({resp.status_code}) attempt {attempt.retry_state.attempt_number}/10"
                    logger.debug(msg)
                    raise Exception(await resp.aread())
                return resp
