from pathlib import Path

import yaml

AST = dict
VariableDescribe = dict
Variables = dict[str, VariableDescribe]
QueryContext = dict

EXAMPLE_ASTS_PATH = Path(__file__).parent.parent / "example-patseq-asts"


CONTIGUITY_REPR_MAP = {"strict": "⋅", "relaxed": "∘", "nd-relaxed": "⊙"}


def ast_repr(ast: AST) -> str:
    if ast["type"] == "spat":
        return "{}:{}:[{}]".format(ast["name"], ast["event"], ast["cndt"]["expr"])
    elif ast["type"] == "lpat":
        # TODO: how to reprenset iterative condition?
        return "{}:{}:[{}]_{}{{{},{}}}".format(
            ast["name"],
            ast["event"],
            ast["cndt"]["expr"],
            CONTIGUITY_REPR_MAP[ast["loop"]["contiguity"]],
            ast["loop"]["from"],
            ast["loop"]["to"],
        )
    elif ast["type"] == "lpat-inf":
        # TODO: how to reprenset iterative condition?
        until_suffix = "U({})".format(ast["until"]["expr"]) if "until" in ast else ""
        return "{}:{}:[{}]_{}{{{},{}}}{}".format(
            ast["name"],
            ast["event"],
            ast["cndt"]["expr"],
            CONTIGUITY_REPR_MAP[ast["loop"]["contiguity"]],
            ast["loop"]["from"],
            "inf",
            until_suffix,
        )
    elif ast["type"] == "combine":
        return "{}{}{}".format(
            ast_repr(ast["left"]),
            CONTIGUITY_REPR_MAP[ast["contiguity"]],
            ast_repr(ast["right"]),
        )
    elif ast["type"] == "gpat":
        return "({})".format(ast_repr(ast["child"]))
    elif ast["type"] == "gpat-times":
        return "({}){{{},{}}}".format(
            ast_repr(ast["child"]), ast["loop"]["from"], ast["loop"]["to"]
        )
    elif ast["type"] == "gpat-inf":
        until_suffix = "U({})".format(ast["until"]["expr"]) if "until" in ast else ""
        return "({}){{{},inf}}{}".format(
            ast_repr(ast["child"]), ast["loop"]["from"], until_suffix
        )
    raise ValueError("Not supported AST node {}".format(ast["type"]))


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

    @staticmethod
    def from_yaml(stream):
        queryobj = yaml.load(stream, Loader=yaml.SafeLoader)
        return Query.from_dict(queryobj)

    def __init__(self, patseq: AST, context: QueryContext, raw: str = "") -> None:
        self.patseq = patseq
        self.context = context

    def __repr__(self) -> str:
        strategy = self.context["strategy"] if "strategy" in self.context else "NoSkip"
        return "{}({})".format(strategy, ast_repr(self.patseq))
