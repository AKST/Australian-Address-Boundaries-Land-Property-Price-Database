import asyncio
from asyncio import Task, TaskGroup, wait
from typing import Dict, List, Set, Any, AsyncIterator, Callable, Awaitable

async def merge_async_iters[T](iters: List[AsyncIterator[T]]) -> AsyncIterator[T]:
    def create_task(it: AsyncIterator[T]) -> Task[T]:
        return asyncio.create_task(it.__anext__()) # type: ignore
    """
    This takes multiple async iterators and combines them in
    to the same async iterator stream.
    """
    tasks = { create_task(it): it for it in iters }

    while tasks:
        done, _ = await wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            iterator = tasks.pop(task)
            try:
                yield task.result()
                tasks[create_task(iterator)] = iterator
            except StopAsyncIteration:
                continue
