import unittest
from pathlib import Path

import yaml
from reflinkcep.ast import Query

from reflinkcep.compile import compile

EXAMPLE_ASTS_PATH = Path(__file__).parent.parent / "example-patseq-asts"
with open(EXAMPLE_ASTS_PATH / "00-hello.yml") as f:
    queryobj = yaml.load(f, Loader=yaml.SafeLoader)
query = Query.from_dict(queryobj)


class TestBasicPatternSequence(unittest.TestCase):
    def test_naive(self):
        executor = compile(query)
        print(executor)
