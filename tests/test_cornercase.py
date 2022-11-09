import logging
import os
from typing import Union
import unittest

from reflinkcep.ast import Query
from reflinkcep.event import Event, EventStream
from reflinkcep.executor import MatchStream, Match

from .utils import run_query_raw, match_repr

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
    fancy_output = "\n".join(match_repr(match) for match in output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
    logger.info("output: %s", output)
    logger.info("fancy output: %d matches\n%s", len(output), fancy_output)

    output_str = fancy_output if with_fancy_output else str(output)
    if with_raw:
        return output_str, output
    return output_str


class TestCornerCase(unittest.TestCase):
    def test_corner_case_01(self):
        query = Query.from_sample("corner-case-01")
        input = ese_from_list([(1, 0), (1, 1), (1, 2)])
        output = run_query(query, input, with_fancy_output=True)
        self.assertEqual(
            output,
            """p: e(1,1,0), e(2,1,1)
p: e(1,1,0), e(2,1,1), e(3,1,2)
p: e(1,1,0), e(3,1,2)
p: e(2,1,1), e(3,1,2)""",
        )
