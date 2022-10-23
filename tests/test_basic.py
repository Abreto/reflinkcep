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
    EventE.id = 0
    return EventStream(EventE(n, p) for n, p in input)


def echo(*args):
    print(*args, sep="")


def run_query_raw(query: Query, input: EventStream) -> EventStream:
    operator = CEPOperator.from_query(query)
    output = operator << input
    return output


def run_query(query: Query, input: EventStream, with_raw=False) -> str:
    output = run_query_raw(query, input)
    # echo("query: ", query)
    # echo("input: ", input)
    # echo("output: ", output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
    logger.info("output: %s", output)
    if with_raw:
        return str(output), output
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

    def test_lpat_nn(self):
        query = Query.from_sample("lpat-n-n")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e{'id': 3, 'name': 1, 'price': 1}, e{'id': 4, 'name': 1, 'price': 2}]}, {'al': [e{'id': 4, 'name': 1, 'price': 2}, e{'id': 5, 'name': 1, 'price': 3}]}]",
        )

    def test_lpat_nm(self):
        query = Query.from_sample("lpat-n-m")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e{'id': 3, 'name': 1, 'price': 1}, e{'id': 4, 'name': 1, 'price': 2}]}, {'al': [e{'id': 3, 'name': 1, 'price': 1}, e{'id': 4, 'name': 1, 'price': 2}, e{'id': 5, 'name': 1, 'price': 3}]}, {'al': [e{'id': 4, 'name': 1, 'price': 2}, e{'id': 5, 'name': 1, 'price': 3}]}]",
        )

    def test_itercndt_lap(self):
        query = Query.from_sample("lpat-n-m-ic")
        input = ese_from_list([(1, 1), (1, 4), (1, 1), (1, 2), (1, 3)])
        output, oes = run_query(query, input, with_raw=True)
        for match in oes:
            sum = 0
            for event in match["al"]:
                sum += event["price"]
            self.assertLessEqual(sum, 5)
        self.assertEqual(
            output,
            "[{'al': [e{'id': 1, 'name': 1, 'price': 1}, e{'id': 2, 'name': 1, 'price': 4}]}, {'al': [e{'id': 2, 'name': 1, 'price': 4}, e{'id': 3, 'name': 1, 'price': 1}]}, {'al': [e{'id': 3, 'name': 1, 'price': 1}, e{'id': 4, 'name': 1, 'price': 2}]}, {'al': [e{'id': 4, 'name': 1, 'price': 2}, e{'id': 5, 'name': 1, 'price': 3}]}]",
        )
