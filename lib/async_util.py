import asyncio
from asyncio import TaskGroup, wait
from typing import Dict, List, Set, Any, AsyncIterator, Callable, Awaitable, TypeVar

T = TypeVar('T')
U = TypeVar('U')

async def pipe(producer: Callable[[], AsyncIterator[T]],
               consumer: Callable[[T], Awaitable[U]],
               tg: TaskGroup) -> AsyncIterator[U]:
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
    pending_tasks: Set[Any] = set()
    producer_coroutine = producer()

    try:
        shard_task: Awaitable[T] = tg.create_task(producer_coroutine.__anext__()) # type: ignore
        while True:
            done, _ = await wait(
                [shard_task] + list(pending_tasks),
                return_when=asyncio.FIRST_COMPLETED
            )

            if shard_task in done:
                try:
                    task: Awaitable[U] = tg.create_task(consumer(await shard_task)) # type: ignore
                    pending_tasks.add(task)
                    shard_task = tg.create_task(producer_coroutine.__anext__()) # type: ignore
                except StopAsyncIteration:
                    break
                except Exception as e:
                    raise e

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
        result = await task
        if result is not None:
            yield result

async def merge_async_iters(iters: List[AsyncIterator[Any]],
                            tg: TaskGroup):
    tasks: Dict[Any, Any] = {
        tg.create_task(it.__anext__()): it # type: ignore
        for it in iters
    }

    while tasks:
        done, _ = await wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            iterator = tasks.pop(task)
            try:
                yield task.result()
                tasks[tg.create_task(iterator.__anext__())] = iterator
            except StopAsyncIteration:
                continue

