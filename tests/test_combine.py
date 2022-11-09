import logging
import os
from typing import Union
import unittest

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.executor import MatchStream, Match
from reflinkcep.operator import CEPOperator

from .utils import run_query_raw

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "WARNING").upper())


def EventE(name: int, price: int = 0) -> Event:
    if not hasattr(EventE, "id"):
        EventE.id = 0
    EventE.id += 1
    return Event("e", {"id": EventE.id, "name": name, "price": price})


def ese_from_list(input: list[tuple[int, int]]) -> EventStream:
    EventE.id = 0
    return EventStream(EventE(n, p) for n, p in input)


def run_query(
    query: Query, input: EventStream, with_raw=False, with_fancy_output=False
) -> Union[str, tuple[str, MatchStream]]:
    output = run_query_raw(query, input)
    fancy_output = "\n".join(str(x) for x in output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
    logger.info("output: %s", output)
    logger.info("fancy output: %d matches\n%s", len(output), fancy_output)

    output_str = fancy_output if with_fancy_output else str(output)
    if with_raw:
        return output_str, output
    return output_str


class TestCombinePattern(unittest.TestCase):
    def test_hello(self):
        query = Query.from_sample("cat-strict")
        input = ese_from_list([(1, 0), (1, 5), (2, 0), (1, 2), (2, 8)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'a': [e(2,1,5)], 'b': [e(3,2,0)]}, {'a': [e(4,1,2)], 'b': [e(5,2,8)]}]",
        )

    def test_strict_nested(self):
        query = Query.from_sample("cat-strict-2")
        input = ese_from_list([(1, 0), (1, 5), (2, 0), (3, 2), (2, 8)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'a': [e(2,1,5)], 'b': [e(3,2,0)], 'c': [e(4,3,2)]}]",
        )

    def test_strict_looping(self):
        query = Query.from_sample("cat-strict-3")
        input = ese_from_list([(1, 0), (1, 5), (2, 0), (3, 2), (1, 8), (2, 8), (2, 8)])
        output = run_query(query, input, with_fancy_output=True)
        self.assertEqual(
            output,
            """{'a': [e(1,1,0), e(2,1,5)], 'b': [e(3,2,0), e(6,2,8)]}
{'a': [e(2,1,5)], 'b': [e(3,2,0), e(6,2,8)]}
{'a': [e(1,1,0), e(2,1,5), e(5,1,8)], 'b': [e(6,2,8), e(7,2,8)]}
{'a': [e(1,1,0), e(2,1,5)], 'b': [e(3,2,0), e(6,2,8), e(7,2,8)]}
{'a': [e(1,1,0), e(5,1,8)], 'b': [e(6,2,8), e(7,2,8)]}
{'a': [e(2,1,5), e(5,1,8)], 'b': [e(6,2,8), e(7,2,8)]}
{'a': [e(2,1,5)], 'b': [e(3,2,0), e(6,2,8), e(7,2,8)]}
{'a': [e(5,1,8)], 'b': [e(6,2,8), e(7,2,8)]}""",
        )

    def test_relaxed(self):
        query = Query.from_sample("cat-relaxed")
        input = ese_from_list([(1, 0), (1, 1), (3, 0), (2, 0), (2, 1)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'a': [e(1,1,0), e(2,1,1)], 'b': [e(4,2,0)]}, {'a': [e(1,1,0)], 'b': [e(4,2,0)]}, {'a': [e(2,1,1)], 'b': [e(4,2,0)]}, {'a': [e(1,1,0), e(2,1,1)], 'b': [e(4,2,0), e(5,2,1)]}, {'a': [e(1,1,0)], 'b': [e(4,2,0), e(5,2,1)]}, {'a': [e(2,1,1)], 'b': [e(4,2,0), e(5,2,1)]}]",
        )

    def test_ndrelaxed(self):
        query = Query.from_sample("cat-ndrelaxed")
        input = ese_from_list([(1, 0), (1, 1), (3, 0), (2, 0), (2, 1)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'a': [e(1,1,0), e(2,1,1)], 'b': [e(4,2,0)]}, {'a': [e(1,1,0)], 'b': [e(4,2,0)]}, {'a': [e(2,1,1)], 'b': [e(4,2,0)]}, {'a': [e(1,1,0), e(2,1,1)], 'b': [e(4,2,0), e(5,2,1)]}, {'a': [e(1,1,0), e(2,1,1)], 'b': [e(5,2,1)]}, {'a': [e(1,1,0)], 'b': [e(4,2,0), e(5,2,1)]}, {'a': [e(1,1,0)], 'b': [e(5,2,1)]}, {'a': [e(2,1,1)], 'b': [e(4,2,0), e(5,2,1)]}, {'a': [e(2,1,1)], 'b': [e(5,2,1)]}]",
        )
