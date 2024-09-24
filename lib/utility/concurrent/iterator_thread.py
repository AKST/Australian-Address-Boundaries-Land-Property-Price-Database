import asyncio
from typing import Any, Iterator, Generator, Callable, AsyncIterator, AsyncGenerator, Tuple, List, TypeVar

T = TypeVar('T')

async def iterator_thread(get_iter: Callable[..., Iterator[T]], *args, **kwargs) -> AsyncGenerator[T, None]:
    def run_sync(queue: asyncio.Queue[T | None], get_iter: Callable[..., Iterator[T]], *args, **kwargs) -> None:
        for item in get_iter(*args, **kwargs):
            queue.put_nowait(item)
        queue.put_nowait(None)

    queue = asyncio.Queue[T | None]()
    thread = asyncio.to_thread(run_sync, queue, get_iter, *args, **kwargs)
    task = asyncio.create_task(thread)

    while True:
        result = await queue.get()
        if result is None:
            break
        yield result
    await task


