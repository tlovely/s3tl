import json

from asyncio import get_event_loop

from s3tl import S3TL, FSExtractor

loop = get_event_loop()
loop_run = loop.run_until_complete


def test_run():
    result = []
    etl = S3TL()

    async def print_(*args, **kwargs):
        result.append((args, kwargs))

    @etl.map
    async def to_dict(line, bucket, key):
        return json.loads(line.decode("utf-8"))

    @etl.filter
    async def filter_a_1_to_3(line, bucket, key):
        return line["a"] > 3

    @etl.reduce(lambda: {}, load=print_)
    async def aggregate(accu, line, bucket, key):
        if line["a"] not in accu:
            accu[line["a"]] = 0
        accu[line["a"]] += line["b"]
        return accu

    loop_run(etl.run(FSExtractor("data")))

    assert result == [
        (
            (
                {
                    10: 1122280,
                    4: 1058371,
                    9: 1081909,
                    8: 1138254,
                    7: 1108567,
                    5: 1105879,
                    6: 1113573,
                },
                "data/",
            ),
            {},
        )
    ]
