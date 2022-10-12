from event import Event

class Executor:
    def reset(self):
        pass

    def feed(self, event: Event) -> list[dict[str, Event]]:
        pass
