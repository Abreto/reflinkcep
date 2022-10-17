from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream, Stream
from reflinkcep.executor import Executor, Match


class CEPOperator:
    @staticmethod
    def from_query(query: Query):
        return CEPOperator(compile(query))

    def __init__(self, executor: Executor) -> None:
        self.executor = executor

    def __lshift__(self, input: EventStream) -> Stream[Match]:
        self.executor.reset()
        output = Stream()
        for event in input:
            output.extend(self.executor.feed(event))
        return output
