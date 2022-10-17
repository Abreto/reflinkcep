import logging
import os
import unittest

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.operator import CEPOperator

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "WARNING").upper())


def EventE(name: str, price: float = 0) -> Event:
    if not hasattr(EventE, "id"):
        EventE.id = 0
    EventE.id += 1
    return Event("e", {"id": EventE.id, "name": name, "price": price})


def ese_from_list(input: list[int, int]) -> EventStream:
    return EventStream(EventE(n, p) for n, p in input)


def echo(*args):
    print(*args, sep="")


def run_query(query: Query, input: EventStream) -> str:
    operator = CEPOperator.from_query(query)
    output = operator << input
    # echo("query: ", query)
    # echo("input: ", input)
    # echo("output: ", output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
    logger.info("output: %s", output)
    return str(output)


class TestBasicPatternSequence(unittest.TestCase):
    def test_hello(self):
        query = Query.from_sample("00-hello")
        input = ese_from_list([(1, 0), (1, 5), (2, 0), (1, 2), (1, 8)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'a1': [e{'id': 1, 'name': 1, 'price': 0}]}, {'a1': [e{'id': 4, 'name': 1, 'price': 2}]}]",
        )

    def test_lpat1(self):
        query = Query.from_sample("lpat1")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e{'id': 8, 'name': 1, 'price': 1}, e{'id': 9, 'name': 1, 'price': 2}]}, {'al': [e{'id': 9, 'name': 1, 'price': 2}, e{'id': 10, 'name': 1, 'price': 3}]}]",
        )