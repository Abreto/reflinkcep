import logging
import os
import unittest

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.operator import CEPOperator

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())


def EventE(name: str, price: float = 0) -> Event:
    if not hasattr(EventE, "id"):
        EventE.id = 0
    EventE.id += 1
    return Event("e", {"id": EventE.id, "name": name, "price": price})


def echo(*args):
    print(*args, sep="")


class TestBasicPatternSequence(unittest.TestCase):
    def test_hello(self):
        query = Query.from_sample("00-hello")
        input = EventStream(
            EventE(n, p) for n, p in [(1, 0), (1, 5), (2, 0), (1, 2), (1, 8)]
        )
        operator = CEPOperator.from_query(query)
        echo("query: ", query)
        echo("input: ", input)
        echo("output: ", operator << input)
