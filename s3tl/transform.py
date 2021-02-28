from typing import Coroutine, Optional, Any, Union


async def noop(*args: Any, **kwargs: Any) -> Any:
    pass


class Operation:
    def __init__(
        self,
        fn: Coroutine,
        load: Optional[Coroutine] = noop,
    ):
        self._fn = fn
        self._load = load

    done = noop


class Map(Operation):
    async def __call__(self, line: Union[bytes, Any], bucket: str, key: str) -> Any:
        line = await self._fn(line, bucket, key)
        await self._load(line, bucket, key)
        return True, line


class Reduce(Operation):
    def __init__(
        self,
        accu: Any,
        fn: Coroutine,
        load: Optional[Coroutine] = noop,
    ):
        self._fn = fn
        self._load = load
        self._accu = accu()

    async def __call__(self, line: Union[bytes, Any], bucket: str, key: str) -> Any:
        self._accu = await self._fn(self._accu, line, bucket, key)
        return True, line

    async def done(self, bucket) -> None:
        await self._load(self._accu, bucket)


class Filter(Operation):
    async def __call__(self, line: Union[bytes, Any], bucket: str, key: str) -> Any:
        include = await self._fn(line, bucket, key)
        if include:
            await self._load(line, bucket, key)
        return include, line
