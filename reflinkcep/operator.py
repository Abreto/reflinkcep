from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, Stream, EventStream
from reflinkcep.executor import Executor, Match


class CEPOperator:
    def __init__(self, query: Query) -> None:
        self.executor = compile(query)

    def __lshift__(self, input: EventStream) -> Stream[Match]:
        self.executor.reset()
        output = Stream()
        for event in input:
            output.extend(self.executor.feed(event))
        return output
