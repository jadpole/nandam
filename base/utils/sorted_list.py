import bisect

from collections.abc import Callable, Iterable
from typing import Literal


def bisect_find[T, K](xs: list[T], k: K, key: Callable[[T], K]) -> T | None:
    index = bisect.bisect_left(xs, k, key=key)  # type: ignore
    if index != len(xs) and key(xs[index]) == k:
        return xs[index]
    else:
        return None


def bisect_insert[T, K](
    xs: list[T],
    x: T,
    key: Callable[[T], K],
    on_conflict: Literal["keep", "replace"] | Callable[[T, T], T] = "replace",
) -> T | None:
    """
    Insert `x` into `xs` at the correct index, sorted by `key`.
    Use `on_conflict` to pick the resolution strategy when the key is equal.

    NOTE: When a conflict occurs:
    - If `on_conflict` is "keep", then return the new value (not inserted).
    - Otherwise, return the previous value.
    """
    # Since `bisect_insert` is often applied to already-sorted lists, especially
    # via `bisect_make`, first check whether it fits at the end.
    key_x = key(x)
    if not xs or key(xs[-1]) < key_x:  # type: ignore
        xs.append(x)
        return None

    index = bisect.bisect_left(xs, key_x, key=key)  # type: ignore
    if index == len(xs):
        xs.append(x)
        return None
    elif key(xs[index]) != key_x:
        xs.insert(index, x)
        return None
    elif on_conflict == "keep":
        return x
    else:
        prev_x = xs[index]
        xs[index] = x if on_conflict == "replace" else on_conflict(prev_x, x)
        return prev_x


def bisect_make[T, K](
    xs: Iterable[T],
    key: Callable[[T], K],
    on_conflict: Literal["keep", "replace"] | Callable[[T, T], T] = "replace",
) -> list[T]:
    """
    Return a copy of the list, sorted and deduplicated by `key`.
    """
    result: list[T] = []
    for x in xs:
        bisect_insert(result, x, key, on_conflict)
    return result


def bisect_union[T, K](
    xs: list[T],
    ys: list[T],
    key: Callable[[T], K],
    on_conflict: Literal["keep", "replace"] | Callable[[T, T], T] = "replace",
) -> list[T]:
    """
    Return a a copy of `xs` where `ys` were added with `bisect_insert`.
    NOTE: The input list must already be sorted and deduplicated by `key`.
    """
    result: list[T] = xs.copy()
    for x in ys:
        bisect_insert(result, x, key, on_conflict)
    return result
