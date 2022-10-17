from dataclasses import dataclass
from typing import Callable

from reflinkcep.defs import value_t


class State:
    _internal_counter: int = 0

    @classmethod
    def _get_counter(cls) -> None:
        ret = cls._internal_counter
        cls._step()
        return ret

    @classmethod
    def _step(cls) -> None:
        cls._internal_counter += 1

    def __init__(self, name: str, out: Callable[[str], str] = None) -> None:
        self.name = "{}:{}".format(name, self._get_counter())
        self.out = out


@dataclass
class Predicte:
    ev_type: str
    cndt: dict

    def __post_init__(self):
        pass


class DataUpdate:
    pass


@dataclass
class EventStreamUpdate:
    sink: str  # variable to append current event


@dataclass
class Transition:
    q1: State
    p: Predicte
    q2: State
    alpha: DataUpdate
    beta: EventStreamUpdate


class DST:
    def __init__(
        self,
        Sigma: set[str],
        Pi: set[str],
        X: set[str],
        Y: set[str],
        Q: set[State],
        q0: State,
        eta: Callable[[str], value_t],
        Delta: list[Transition],
    ) -> None:
        self.Sigma = Sigma
        self.Pi = Pi
        self.X = X
        self.Y = Y
        self.Q = Q
        self.q0 = q0
        self.eta = eta
        self.Delta = Delta
