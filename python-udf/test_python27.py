from udf import decode
import json


def test_udf():
    with open("payload.json", "r") as f:
        for (i,) in json.load(f):
            assert decode(i)
