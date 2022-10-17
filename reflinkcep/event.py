from dataclasses import dataclass


@dataclass
class Event:
    type: str
    attrs: dict[str, int]
