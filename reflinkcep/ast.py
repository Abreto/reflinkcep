from dataclasses import dataclass

AST = dict
QueryContext = dict


@dataclass
class Query:
    type: str = "query"

    @staticmethod
    def from_dict(obj: dict):
        return Query(obj["patseq"], obj["context"])

    def __init__(self, patseq: AST, context: QueryContext) -> None:
        self.patseq = patseq
        self.context = context
