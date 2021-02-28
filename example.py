import json

from asyncio import get_event_loop
from functools import partial
from concurrent.futures import ProcessPoolExecutor

from s3tl import S3TL, FSExtractor

loop = get_event_loop()
loop_run = loop.run_until_complete
etl = S3TL()


# Load step.
async def print_(*args, **kwargs):
    print(args, kwargs)


# Assemble pipeline. Runs in order decorated. With first step, line
# is bytes from source. With subsequent steps, line is whatever was
# returned by the previous.
@etl.map
async def to_dict(line, bucket, key):
    return json.loads(line.decode("utf-8"))


@etl.filter
async def filter_a_1_to_3(line, bucket, key):
    return (
        line["a"] > 3
    )  # if False, halts processing of line (reduce won't run for this one line)


@etl.reduce(lambda: {}, load=print_)  # Load defined here.
async def aggregate(accu, line, bucket, key):
    if line["a"] not in accu:
        accu[line["a"]] = 0
    accu[line["a"]] += line["b"]
    return accu


# Run ETL. Pipeline is fed lines as they stream from extraction source.
# Load hooks are run immediately after processing a single line for map and filter,
# and after all lines are processed for reduce.
loop_run(etl.run(FSExtractor("data/")))
