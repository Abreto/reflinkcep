from pathlib import Path

import yaml

AST = dict
QueryContext = dict

EXAMPLE_ASTS_PATH = Path(__file__).parent.parent / "example-patseq-asts"


def ast_repr(ast: AST) -> str:
    if ast["type"] == "spat":
        return "{}:{}:[{}]".format(ast["name"], ast["event"], ast["cndt"]["expr"])
    raise Exception("Not supported AST node {}".format(ast["type"]))


class Query:
    type: str = "query"

    @staticmethod
    def from_dict(obj: dict):
        return Query(obj["patseq"], obj["context"])

    @staticmethod
    def from_sample(name: str):
        with open(EXAMPLE_ASTS_PATH / "{}.yml".format(name)) as f:
            queryobj = yaml.load(f, Loader=yaml.SafeLoader)
        return Query.from_dict(queryobj)

    def __init__(self, patseq: AST, context: QueryContext, raw: str = "") -> None:
        self.patseq = patseq
        self.context = context

    def __repr__(self) -> str:
        return ast_repr(self.patseq)
