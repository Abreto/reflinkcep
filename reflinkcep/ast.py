from dataclasses import dataclass

AST = dict
QueryContext = dict


class Query:
    type: str = "query"

    @staticmethod
    def from_dict(obj: dict):
        return Query(obj["patseq"], obj["context"], obj["raw"])

    def __init__(self, patseq: AST, context: QueryContext, raw: str = "") -> None:
        self.patseq = patseq
        self.context = context
        self.raw = raw

    def __repr__(self) -> str:
        return self.raw
