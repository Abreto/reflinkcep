from dataclasses import dataclass
from reflinkcep.defs import value_t

EventAttrMap = dict[str, value_t]


@dataclass(repr=False)
class Event:
    type: str
    attrs: EventAttrMap

    def get_attrs(self) -> EventAttrMap:
        return self.attrs

    def __getitem__(self, key: str) -> int:
        return self.attrs[key]

    def __repr__(self) -> str:
        return "{}{}".format(self.type, self.attrs)


Stream = list
EventStream = Stream[Event]
