import logging
import os
import unittest
from dataclasses import dataclass
from pathlib import Path

import yaml

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.operator import CEPOperator

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())


EXAMPLE_ASTS_PATH = Path(__file__).parent.parent / "example-patseq-asts"


def load_query(name: str) -> Query:
    with open(EXAMPLE_ASTS_PATH / "{}.yml".format(name)) as f:
        queryobj = yaml.load(f, Loader=yaml.SafeLoader)
    return Query.from_dict(queryobj)


def EventE(name: str, price: float = 0) -> Event:
    if not hasattr(EventE, "id"):
        EventE.id = 0
    EventE.id += 1
    return Event("e", {"id": EventE.id, "name": name, "price": price})


def echo(*args):
    print(*args, sep="")


class TestBasicPatternSequence(unittest.TestCase):
    def test_hello(self):
        query = load_query("00-hello")
        input = EventStream(
            EventE(n, p) for n, p in [(1, 0), (1, 5), (2, 0), (1, 2), (1, 6)]
        )
        operator = CEPOperator.from_query(query)
        echo("query: ", query)
        echo("input: ", input)
        echo("output: ", operator << input)
