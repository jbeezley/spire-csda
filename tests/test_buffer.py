from asyncio import sleep
from aiostream.stream import range as arange
from aiostream.pipe import take
import pytest

from nasa_csda.buffer import Buffer


async def test_buffer_end():
    count = 0
    buffer_obj = Buffer(5)
    async with buffer_obj as buffer:
        p = arange(10) | buffer.pipe()
        async with p.stream() as streamer:
            async for i in streamer:
                if i == 0:
                    await sleep(0.5)
                    assert buffer_obj.queue.qsize() > 0
                count += 1
    assert count == 10


async def test_buffer_limit():
    count = 0
    buffer_obj = Buffer(5)
    async with buffer_obj as buffer:
        p = arange(10) | buffer.pipe() | take(2)
        async with p.stream() as streamer:
            async for i in streamer:
                count += 1
    assert count == 2


async def test_buffer_exception():
    count = 0
    buffer_obj = Buffer(5)
    async with buffer_obj as buffer:
        p = arange(10) | buffer.pipe() | take(2)
        with pytest.raises(AssertionError):
            async with p.stream() as streamer:
                async for i in streamer:
                    assert False
    assert count == 0
