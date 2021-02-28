import asyncio

from typing import Callable, Optional, Coroutine, Any, Union, List

import aiofiles

from toolz import curry

from s3tl.transform import Operation, Map, Filter, Reduce, noop
from s3tl.extract import Extractor


class S3TL:
    def __init__(self):
        self._pipeline = []

    @curry
    def map(
        self,
        fn: Coroutine,
        load: Optional[Coroutine] = noop,
    ) -> Coroutine:
        self._pipeline.append((Map, (fn, load)))
        return fn

    @curry
    def filter(
        self,
        fn: Coroutine,
        load: Optional[Coroutine] = noop,
    ) -> Coroutine:
        self._pipeline.append((Filter, (fn, load)))
        return fn

    @curry
    def reduce(
        self, accu: Callable, fn: Coroutine, load: Optional[Coroutine] = None
    ) -> Coroutine:
        self._pipeline.append((Reduce, (accu, fn, load)))
        return fn

    async def _process_line(
        self, _pipeline: List[Operation], line: Union[bytes, Any], bucket: str, key: str
    ) -> None:
        for item in _pipeline:
            include, line = await item(line, bucket, key)
            if not include:
                return

    async def run(
        self,
        extractor: Extractor,
        concurrency: int = 20,
    ):
        tasks = set()
        _pipeline = [C(*args) for C, args in self._pipeline]
        async for line, bucket, key in extractor():
            tasks.add(
                asyncio.create_task(self._process_line(_pipeline, line, bucket, key))
            )
            if len(tasks) == concurrency:
                _, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
        if tasks:
            await asyncio.wait(tasks)
        for item in _pipeline:
            await item.done(bucket)
