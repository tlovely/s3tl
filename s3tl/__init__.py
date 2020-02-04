import aioboto3
import asyncio
import json
import fnmatch
import functools


class S3tl:
    def __init__(self, loop=None, **config):
        self._loop = loop or asyncio.get_event_loop()
        self._config = config
        self._handlers = {}
        self._s3 = None

    def _s3_client(self):
        return aioboto3.Session(**self._config).client('s3')

    async def _s3_get_object_body(self, bucket, key):
        r = await self._s3.get_object(Bucket=bucket, Key=key)
        return key, await r['Body'].read()

    async def _s3_get_object_body_as_json(self, bucket, key):
        return key, await self._s3_get_object_body(self, bucket, key)

    async def _s3_glob(self, bucket, pattern, ffn, handler, concurrency):
        marker = ''
        prefix = pattern.split('*', 1)[0] if pattern and '*' in pattern else pattern
        is_truncated = True
        tasks = set()
        while is_truncated:
            response = await self._s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
            contents = [i['Key'] for i in response.get('Contents', [])]
            is_truncated = response['IsTruncated']
            marker = contents[-1] if is_truncated else None
            keys = []
            for key in contents:
                if fnmatch.fnmatch(key, pattern) and ffn is None or ffn(key):
                    tasks.add(self._loop.create_task(handler(bucket, key)))
                if len(tasks) > concurrency:
                    done, tasks = await asyncio.wait(tasks)
                    for task in done:
                        yield await task
            done, _ = await asyncio.wait(tasks)
            for task in done:
                yield await task
            #yield is_truncated, marker, contents, (s3_get_object_body(s3, 'com.vivintsolar.fusion.etl', k) for k in contents)

    def on(self, bucket, pattern, ffn=None, handle_body=None, concurrency=1000):
        def _on(fn): 
            body_handler = handle_body or self._s3_get_object_body
            if bucket not in self._handlers:
                self._handlers[bucket] = {}
            if pattern not in self._handlers:
                self._handlers[bucket][pattern] = {
                    'globber': self._s3_glob(bucket, pattern, ffn, body_handler, concurrency),
                    'handlers': []
                }
            ctx = {}
            self._handlers[bucket][pattern]['handlers'].append((ctx, fn))
        return _on

    async def _run_handler(self, handler, ctx, key, data):
        ctx.update(await self._loop.run_in_executor(None, handler, ctx, key, data))

    async def _run(self, concurrency):
        tasks = set()
        async with self._s3_client() as s3:
            self._s3 = s3
            working = True
            while working:
                working = False
                for bucket in self._handlers:
                    for pattern, handlers in self._handlers[bucket].items():
                        try:
                            key, data = await handlers['globber'].__anext__()
                            working = True
                            for ctx, handler in handlers['handlers']:
                                tasks.add(self._loop.create_task(self._run_handler(handler, ctx, key, data)))
                                while len(tasks) > concurrency:
                                    _, tasks = await asyncio.wait(tasks)
                        except StopIteration:
                            pass
    
    def run(self, concurrency=1000):
        self._loop.run_until_complete(self._run(concurrency))

