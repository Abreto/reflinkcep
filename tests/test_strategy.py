import logging
import os
from typing import Union
import unittest

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.executor import Match, MatchStream
from reflinkcep.operator import CEPOperator

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


def echo(*args):
    print(*args, sep="")


def run_query_raw(query: Query, input: EventStream) -> MatchStream:
    operator = CEPOperator.from_query(query)
    logger.debug("dst:\n%s", operator.executor.dst._print_trans_map())
    output = operator << input
    return output


def run_query(
    query: Query, input: EventStream, with_raw=False, with_fancy_output=False
) -> Union[str, tuple[str, MatchStream]]:
    def match_repr(match: Match) -> str:
        return "; ".join(
            "{}: {}".format(name, ", ".join(str(ev) for ev in evs))
            for name, evs in match.items()
        )

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


class TestAfterMatchStrategy(unittest.TestCase):
    def test_noskip(self):
        query = Query.from_sample("ams-noskip")
        input = ese_from_list([(2, 0), (2, 1), (2, 2), (3, 0)])
        output = run_query(query, input, with_fancy_output=True)
        self.assertEqual(
            output,
            """b: e(1,2,0), e(2,2,1), e(3,2,2); c: e(4,3,0)
b: e(1,2,0), e(2,2,1); c: e(4,3,0)
b: e(1,2,0); c: e(4,3,0)
b: e(2,2,1), e(3,2,2); c: e(4,3,0)
b: e(2,2,1); c: e(4,3,0)
b: e(3,2,2); c: e(4,3,0)""",
        )

    def test_skiptonext(self):
        query = Query.from_sample("ams-skiptonext")
        input = ese_from_list([(2, 0), (2, 1), (2, 2), (3, 0)])
        output = run_query(query, input, with_fancy_output=True)
        self.assertEqual(
            output,
            """b: e(1,2,0), e(2,2,1), e(3,2,2); c: e(4,3,0)
b: e(2,2,1), e(3,2,2); c: e(4,3,0)
b: e(3,2,2); c: e(4,3,0)""",
        )

    def test_skippastlastevent(self):
        query = Query.from_sample("ams-skippastlastevent")
        input = ese_from_list([(2, 0), (2, 1), (2, 2), (3, 0)])
        output = run_query(query, input, with_fancy_output=True)
        self.assertEqual(
            output,
            """b: e(1,2,0), e(2,2,1), e(3,2,2); c: e(4,3,0)""",
        )
