from reflinkcep.DST import DST
from reflinkcep.event import Event


class Executor:
    def __init__(self, dst: DST) -> None:
        self.dst = dst

    def reset(self):
        pass

    def feed(self, event: Event) -> list[dict[str, Event]]:
        pass
