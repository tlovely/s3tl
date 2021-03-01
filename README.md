# s3tl

Framework for building ETL scripts against S3.

```python
import json
import asyncio

from decimal import Decimal

from s3tl import S3TL, S3Extractor

loop = asyncio.get_event_loop()
loop_run = loop.run_until_complete
etl = S3TL()
# This is an async generator that will yield the lines (one at a time) of all files
# found in the bucket which match the prefix.
extractor = S3Extractor(
	"s3://<bucket>/[<prefix>]",
	access_key_id="<AWS_ACCESS_KEY_ID>",
	secret_access_key="<AWS_SECRET_ACCESS_KEY>",
	region="<AWS_REGION>"
)


# Assemble ETL pipeline. These will be called in order decorated, at least
# once per line each.
@etl.map
async def parse(line, bucket, key):
	# at beginning of pipeline, line is byte string
	return json.loads(line.decode("utf-8"))


@etl.map
async def amount_to_decimal(line, bucket, key):
	# line is now a python data structure
	return {
		**line,
		"amount": Decimal(line["amount"]),
	}


@etl.filter
async def amount_gte_10(line, bucket, key):
	return line["amount"] >= 10


async def handle_reduce_result(accu, bucket):
	# Make SQL insert, API request, create file locally, etc.
	pass


# "load" hook will be called when extractor is consumed. 
@etl.reduce(lambda: {}, load=handle_reduce_result)
async def sum_amount_by_user_id(accu, line, bucket, key)
	if line["user_id"] not in accu:
		accu[line["user_id"]] = Decimal("0")
	accu[line["user_id"]] += line["amount"]
	return accu


# Throttled to only handle 20 lines concurrently. To increase, provide
# concurrency=<n> parameter.
loop_run(etl.run(extractor))
```

## Installation

```bash
pip install s3tl
```

## Working Example using `FSExtractor`

Clone report, call `make setup`, then `./env/bin/python example.py`.

## TODO

- Address mypy errors.
- Add unit tests.
- Create example of using ProcessPoolExecutor for compute heavy tranformations.
- Add line batching.
- Add ability to process by file vs only by line.
- Add examples to demonstrate the behavior of using multiple reducers.
- Explain API.
- Add hook for filtering files from being processed by key.