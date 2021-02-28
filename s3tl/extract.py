from typing import AsyncGenerator
from os import walk

import aiofiles

from aiobotocore import get_session
from aiobotocore.client import AioBaseClient


class Extractor:
    pass


class S3Extractor(Extractor):
    def __init__(
        self,
        uri: str,
        access_key_id: str,
        secret_access_key: str,
        region: str = "us-west-2",
    ) -> None:
        assert uri.startswith("s3://")
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._region = region
        self._session = get_session()
        self._client = self._session.create_client(
            "s3",
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            region_name=self._region,
        )
        self._bucket, self._prefix = uri[5:].split("/", 1)

    async def __call__(self) -> AsyncGenerator[bytes, None]:
        async with self._client as s3:
            paginator = s3.get_paginator("list_objects")
            async for result in paginator.paginate(
                Bucket=self._bucket,
                **({} if self._prefix is None else {"Prefix": self._prefix}),
            ):
                for item in result.get("Contents", []):
                    response = await s3.get_object(Bucket=self._bucket, Key=item["Key"])
                    async with response["Body"] as stream:
                        while not stream.at_eof():
                            yield (await stream.readline(), self._bucket, item["Key"])


class FSExtractor(Extractor):
    def __init__(self, uri: str) -> None:
        self._path = uri

    async def __call__(self) -> AsyncGenerator[bytes, None]:
        _, _, filepaths = next(walk(self._path))
        for filepath in filepaths:
            async with aiofiles.open(f"{self._path}{filepath}", "rb") as f:
                async for line in f:
                    yield (line, self._path, filepath)
