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
    query: Query, input: EventStream, with_raw=False, with_fancy_output=True
) -> Union[str, tuple[str, MatchStream]]:
    output = run_query_raw(query, input)
    fancy_output = "\n".join(match_repr(match) for match in output)
    logger.info("query: %s", query)
    logger.info("input: %s", input)
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

    def test_circ_until(self):
        query = Query.from_yaml(
            """
type: "query"
patseq:
  type: "combine"
  contiguity: "relaxed"
  left:
    type: "spat"
    name: "c"
    event: "e"
    cndt:
      expr: name == 3
  right:
    type: "lpat-inf"
    name: "a"
    event: "e"
    cndt:
      expr: name == 1
    loop:
      contiguity: relaxed
      from: 1
    until:
      expr: name == 2
context:
  schema:
    e: ["id", "name", "price"]
""",
            "CircUntil",
        )
        input = ese_from_list([(3, 0), (2, 0), (1, 0), (1, 0)])
        output = run_query(query, input)

        self.assertEqual(
            output,
            """c: e(1,3,0); a: e(3,1,0)
c: e(1,3,0); a: e(3,1,0), e(4,1,0)""",
        )

    def test_circ_until_optional(self):
        query = Query.from_yaml(
            """
type: "query"
patseq:
  type: "combine"
  contiguity: "relaxed"
  left:
    type: "spat"
    name: "c"
    event: "e"
    cndt:
      expr: name == 3
  right:
    type: "lpat-inf"
    name: "a"
    event: "e"
    cndt:
      expr: name == 1
    loop:
      contiguity: relaxed
      from: 0
    until:
      expr: name == 2
context:
  schema:
    e: ["id", "name", "price"]
""",
            "CircUntilOptional",
        )
        input = ese_from_list([(3, 0), (2, 0), (1, 0), (1, 0)])
        output = run_query(query, input)

        self.assertEqual(
            output,
            """c: e(1,3,0)
c: e(1,3,0); a: e(3,1,0)
c: e(1,3,0); a: e(3,1,0), e(4,1,0)""",
        )

    def test_circ_lpat_nm_until_optional(self):
        query = Query.from_yaml(
            """
type: "query"
patseq:
  type: "combine"
  contiguity: "relaxed"
  left:
    type: "spat"
    name: "c"
    event: "e"
    cndt:
      expr: name == 3
  right:
    type: "lpat"
    name: "a"
    event: "e"
    cndt:
      expr: name == 1
    loop:
      contiguity: relaxed
      from: 0
      to: 1
    until:
      expr: name == 2
context:
  schema:
    e: ["id", "name", "price"]
""",
            "CircLPatNMUntilOptional",
        )
        input = ese_from_list([(3, 0), (2, 0), (1, 0), (1, 0)])
        output = run_query(query, input)

        self.assertEqual(
            output,
            """c: e(1,3,0)
c: e(1,3,0); a: e(3,1,0)""",
        )

    def test_nested_until(self):
        query = Query.from_yaml(
            """
type: "query"
patseq:
  type: "gpat-inf"
  child:
    type: "gpat-inf"
    child:
      type: "spat"
      name: "a"
      event: "e"
      cndt:
        expr: name == 1
    loop:
      from: 1
      to: inf
    until:
      expr: name == 2
  loop:
    from: 1
    to: inf
  until:
    expr: name == 3
context:
  schema:
    e: ["id", "name", "price"]
""",
            "NestedUntil",
        )
        input = ese_from_list([(i, 0) for i in [1, 1, 3, 1, 2, 3]])
        output = run_query(query, input)

        self.assertEqual(
            output,
            """a: e(1,1,0)
a: e(1,1,0), e(2,1,0)
a: e(1,1,0), e(2,1,0)
a: e(2,1,0)
a: e(4,1,0)""",
        )
