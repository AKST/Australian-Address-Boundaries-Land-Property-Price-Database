import asyncio
from asyncio import Task, TaskGroup, wait
from dataclasses import dataclass, field
from typing import Dict, List, Set, Any, AsyncIterator, Callable, Awaitable

async def pipe[T, U](producer: Callable[[], AsyncIterator[T]],
                     consumer: Callable[[T], Awaitable[U]],
                     tg: TaskGroup | None = None) -> AsyncIterator[U]:
    """
    This function creates an async generator with a
    producer and a consumer (there is an optional
    task group as well for grouping the tasks.

    - The Producer is an async iterator that emits values
      which the consumer can directory take and use.
    - The Consumer (as the name suggests) consumes those
      values and produces another value which is then
      emitted from this generator.

    Why not just loop over the producer and call the
    cosumer? That doesn't seem hard? Why not write this?

    ```
    async def pipe(producer, consumer):
        async for item of producer():
            yield await consumer(item)
    ```

    Well here's a fairly not trivial difference. The above
    function will block the producer till the consumer has
    finished running, and you cannot start tasks a time and
    yield them as they complete.

    If you have blocking tasks in both that vary in duration
    then it makes sense to want to run as many simulanenously
    as possible.
    """
    tg_create_task = tg.create_task if tg is not None else asyncio.create_task

    pending_tasks: Set[Any] = set()
    producer_coroutine = producer()

    try:
        shard_task: Awaitable[T] = asyncio.create_task(producer_coroutine.__anext__()) # type: ignore
        while True:
            done, _ = await wait(
                [shard_task] + list(pending_tasks),
                return_when=asyncio.FIRST_COMPLETED
            )

            if shard_task in done:
                try:
                    task: Awaitable[U] = tg_create_task(consumer(await shard_task)) # type: ignore
                    pending_tasks.add(task)
                    shard_task = asyncio.create_task(producer_coroutine.__anext__()) # type: ignore
                except StopAsyncIteration:
                    break

            completed_tasks = done - {shard_task}
            for task in completed_tasks:
                if task not in pending_tasks:
                    continue

                pending_tasks.remove(task)
                result = await task
                if result is not None:
                    yield result

    except StopAsyncIteration:
        pass

    for task in asyncio.as_completed(pending_tasks):
        try:
            result = await task
        except StopAsyncIteration:
            continue

        if result is not None:
            yield result

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

class NullableSemaphore:
    _semaphore: asyncio.Semaphore | None
    _disabled: bool

    def __init__(self,
                 semaphore: asyncio.Semaphore | None,
                 disabled: bool = False) -> None:
        self._semaphore = semaphore
        self._disabled = disabled

    @property
    def disabled(self) -> bool:
        return self._disabled

    async def disable(self) -> None:
        if self._semaphore:
            async with self._semaphore:
                self._disabled = True

    async def enable(self) -> None:
        if self._semaphore:
            async with self._semaphore:
                self._disabled = False

    async def __aenter__(self) -> 'NullableSemaphore':
        if self._semaphore and not self._disabled:
            await self._semaphore.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        if self._semaphore and not self._disabled:
            await self._semaphore.__aexit__(exc_type, exc_value, traceback)
        return False
