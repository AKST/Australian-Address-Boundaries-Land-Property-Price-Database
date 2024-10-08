from typing import Awaitable, Callable, TypeVar

A = TypeVar('A')
B = TypeVar('B')

async def fmap(f: Callable[[A], B], t_a: Awaitable[A]) -> B:
    return f(await t_a)
