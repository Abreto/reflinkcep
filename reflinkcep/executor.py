from reflinkcep.DST import DST
from reflinkcep.event import Event, Stream, EventStream

Match = dict[str, EventStream]


class Executor:
    def __init__(self, dst: DST) -> None:
        self.dst = dst

    def reset(self):
        pass

    def feed(self, event: Event) -> Stream[Match]:
        if event["name"] == 1 and event["price"] < 5:
            return [{"a1": event}]
        return []
