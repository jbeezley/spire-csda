from asyncio import CancelledError, create_task, Queue, Task
from contextlib import AbstractAsyncContextManager
from typing import Any, AsyncIterable, AsyncIterator, Generic, Optional, TypeVar

from aiostream.core import pipable_operator, PipableOperator, streamcontext

A = TypeVar("A", contravariant=True)
T = TypeVar("T", covariant=True)


class Buffer(AbstractAsyncContextManager, Generic[A, T]):
    """Create a buffering context manager..

    >>> async with Buffer() as buffered:
            # the stream `p` will be iterated in the background
            p |= buffered.pipe()
            async with streamcontext(p) as streamer:
                async for item in streamer:
                    item
    """

    def __init__(self, size: int = 1000) -> None:
        self.queue: Queue = Queue(size)
        self.task: Optional[Task] = None

    @staticmethod
    async def _producer(it: AsyncIterable[Any], queue: Queue) -> None:
        async with streamcontext(it) as items:
            try:
                async for item in items:
                    await queue.put((False, item))
            except BaseException as err:
                await queue.put((True, err))
            else:
                await queue.put((None, None))

    async def __aenter__(self) -> PipableOperator[A, [], T]:

        @pipable_operator
        async def buffered(it: AsyncIterable[A]) -> AsyncIterator[T]:
            """Buffer values from an iterator in the background.

            This function creates an asynchronous task that pushes data into a
            bounded queue.
            """
            self.task = create_task(self._producer(it, self.queue))
            try:
                while True:
                    status, item = await self.queue.get()
                    if status:
                        raise item
                    if status is None:
                        break
                    yield item
            except CancelledError:
                pass

        return buffered

    async def __aexit__(self, *args):
        if self.task:
            self.task.cancel()
            await self.task
