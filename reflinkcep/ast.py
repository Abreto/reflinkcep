from pathlib import Path

import yaml

AST = dict
QueryContext = dict

EXAMPLE_ASTS_PATH = Path(__file__).parent.parent / "example-patseq-asts"


class Query:
    type: str = "query"

    @staticmethod
    def from_dict(obj: dict):
        return Query(obj["patseq"], obj["context"], obj["raw"])

    @staticmethod
    def from_sample(name: str):
        with open(EXAMPLE_ASTS_PATH / "{}.yml".format(name)) as f:
            queryobj = yaml.load(f, Loader=yaml.SafeLoader)
        return Query.from_dict(queryobj)

    def __init__(self, patseq: AST, context: QueryContext, raw: str = "") -> None:
        self.patseq = patseq
        self.context = context
        self.raw = raw

    def __repr__(self) -> str:
        return self.raw
