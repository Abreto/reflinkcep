import logging
import os
from typing import Union
import unittest

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.executor import MatchStream
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
    logger.debug("dst: %s", operator.executor.dst.edge_map)
    output = operator << input
    return output


def run_query(
    query: Query, input: EventStream, with_raw=False
) -> Union[str, tuple[str, MatchStream]]:
    output = run_query_raw(query, input)
    # echo("query: ", query)
    # echo("input: ", input)
    # echo("output: ", output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
    logger.info("output: %s", output)
    logger.info("fancy output:\n%s", "\n".join(str(x) for x in output))
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
            "[{'a1': [e(1,1,0)]}, {'a1': [e(4,1,2)]}]",
        )

    def test_lpat_nn(self):
        query = Query.from_sample("lpat-n-n")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(4,1,2), e(5,1,3)]}]",
        )

    def test_lpat_nm(self):
        query = Query.from_sample("lpat-n-m")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(4,1,2), e(5,1,3)]}]",
        )

    def test_lpat_nm_relaxed(self):
        query = Query.from_sample("lpat-n-m-relaxed")
        input = ese_from_list([(1, 0), (1, 5), (2, 1), (1, 2)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e(1,1,0), e(2,1,5)]}, {'al': [e(1,1,0), e(2,1,5), e(4,1,2)]}, {'al': [e(2,1,5), e(4,1,2)]}]",
        )

    def test_lpat_nm_ndrelaxed(self):
        query = Query.from_sample("lpat-n-m-ndrelaxed")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2)])
        output = run_query(query, input)
        self.assertEqual(
            output,
            "[{'al': [e(1,1,0), e(3,1,1)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2)]}, {'al': [e(1,1,0), e(4,1,2)]}, {'al': [e(3,1,1), e(4,1,2)]}]",
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
            "[{'al': [e(1,1,1), e(2,1,4)]}, {'al': [e(2,1,4), e(3,1,1)]}, {'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(4,1,2), e(5,1,3)]}]",
        )

    def test_lpat_n_inf(self):
        query = Query.from_sample("lpat-n-inf")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3), (1, 3)])
        output, mstream = run_query(query, input, with_raw=True)
        for match in mstream:
            self.assertGreaterEqual(len(match["al"]), 2)
        self.assertEqual(
            output,
            "[{'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(4,1,2), e(5,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(5,1,3), e(6,1,3)]}]",
        )

    def test_lpat_n_inf_relaxed(self):
        query = Query.from_sample("lpat-n-inf-relaxed")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3), (1, 3)])
        output, mstream = run_query(query, input, with_raw=True)
        for match in mstream:
            self.assertGreaterEqual(len(match["al"]), 2)
        self.assertEqual(
            output,
            "[{'al': [e(1,1,0), e(3,1,1)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2)]}, {'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(4,1,2), e(5,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(5,1,3), e(6,1,3)]}]",
        )

    def test_lpat_n_inf_ndrelaxed(self):
        query = Query.from_sample("lpat-n-inf-ndrelaxed")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3), (1, 3)])
        output, mstream = run_query(query, input, with_raw=True)
        for match in mstream:
            self.assertGreaterEqual(len(match["al"]), 3)
        self.assertEqual(
            output,
            "[{'al': [e(1,1,0), e(3,1,1), e(4,1,2)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(5,1,3)]}, {'al': [e(1,1,0), e(4,1,2), e(5,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2), e(6,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(5,1,3), e(6,1,3)]}, {'al': [e(1,1,0), e(3,1,1), e(6,1,3)]}, {'al': [e(1,1,0), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(1,1,0), e(4,1,2), e(6,1,3)]}, {'al': [e(1,1,0), e(5,1,3), e(6,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(5,1,3), e(6,1,3)]}, {'al': [e(3,1,1), e(4,1,2), e(6,1,3)]}, {'al': [e(3,1,1), e(5,1,3), e(6,1,3)]}, {'al': [e(4,1,2), e(5,1,3), e(6,1,3)]}]",
        )

    def test_lpat_n_inf_until(self):
        query = Query.from_sample("lpat-n-inf-until")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3), (1, 3)])
        output, mstream = run_query(query, input, with_raw=True)
        for match in mstream:
            self.assertGreaterEqual(len(match["al"]), 2)
            self.assertLess(sum([e["price"] for e in match["al"]]), 6)
        self.assertEqual(
            output,
            "[{'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(4,1,2), e(5,1,3)]}]",
        )

    def test_lpat_n_inf_until_relaxed(self):
        query = Query.from_sample("lpat-n-inf-until-relaxed")
        input = ese_from_list([(1, 0), (1, 5), (1, 1), (1, 2), (1, 3), (1, 3)])
        output, mstream = run_query(query, input, with_raw=True)
        for match in mstream:
            self.assertGreaterEqual(len(match["al"]), 2)
            self.assertLess(sum([e["price"] for e in match["al"]]), 6)
        self.assertEqual(
            output,
            "[{'al': [e(1,1,0), e(3,1,1)]}, {'al': [e(1,1,0), e(3,1,1), e(4,1,2)]}, {'al': [e(3,1,1), e(4,1,2)]}, {'al': [e(4,1,2), e(5,1,3)]}]",
        )
