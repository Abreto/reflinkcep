from dataclasses import dataclass


@dataclass(repr=False)
class Event:
    type: str
    attrs: dict[str, int]

    def __getitem__(self, key: str) -> int:
        return self.attrs[key]

    def __repr__(self) -> str:
        return "{}{}".format(self.type, self.attrs)


Stream = list
EventStream = Stream[Event]
