from asyncio import CancelledError, create_task, Queue
from typing import Any, AsyncIterable, Optional, TypeVar

T = TypeVar("T")


async def _producer(it: AsyncIterable[Any], queue: Queue) -> None:
    try:
        async for item in it:
            await queue.put((False, item))
    except BaseException as err:
        await queue.put((True, err))
    else:
        await queue.put((None, None))


async def buffered(it: AsyncIterable[T], size: int, limit: Optional[int] = None) -> AsyncIterable[T]:
    queue: Queue = Queue(size)
    task = create_task(_producer(it, queue))
    count = 0
    try:
        while True:
            status, item = await queue.get()
            if status:
                raise item
            if status is None:
                break
            if limit and count >= limit:
                break
            yield item
            count += 1
    except CancelledError:
        task.cancel()
        try:
            await task
        except BaseException:
            pass
        raise
    task.cancel()
