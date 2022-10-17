from copy import deepcopy
from dataclasses import dataclass
from typing import Callable

from reflinkcep.defs import value_t
from reflinkcep.event import Event, EventAttrMap, EventStream

Val = value_t
Set = set
DataVariable = str
StreamVariable = str
Func = dict

Condition = dict
DataEnv = Func[DataVariable, Val]
Context = Func[StreamVariable, EventStream]

TrueCondition = {"expr": "True"}


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

    def __init__(self, name: str, out: Func[str, str] = None) -> None:
        self.name = "{}:{}".format(name, self._get_counter())
        self.out = out

    def __repr__(self) -> str:
        return "State({},{})".format(self.name, 0 if self.out is None else 1)


@dataclass
class Configuration:
    q: State
    eta: DataEnv
    ctx: Context

    def get_state(self) -> State:
        return self.q


class ConditionEvaluator:
    def __init__(self, cndt: Condition) -> None:
        self.obj = compile(cndt["expr"], filename="<condition>", mode="eval")

    def eval(self, env: DataEnv, attrs: EventAttrMap) -> bool:
        return eval(self.obj, {**env, **attrs, "__builtins__": None})


@dataclass
class Predicte:
    ev_type: str
    cndt: Condition

    def __post_init__(self):
        self.evaluator = ConditionEvaluator(self.cndt)
        self.epsilon = self.ev_type is None

    def evaluate(self, conf: Configuration, event: Event) -> bool:
        attrs = {} if event is None else event.attrs
        return self.evaluator.eval(conf.eta, attrs)


class DataUpdate:
    def update(self, eta: Func, event: Event) -> Func:
        return eta

    @staticmethod
    def Id():
        return DataUpdate()  # TODO


@dataclass
class EventStreamUpdate:
    sink: StreamVariable  # variable to append current event

    def update(self, ctx: Context, event: Event):
        # Ignore the event
        if self.sink is None:
            return ctx
        assert event is not None, "Trying to take epsilon"

        # Take the event
        newctx = deepcopy(ctx)
        if not self.sink in newctx:
            newctx[self.sink] = []
        newctx[self.sink].append(event)
        return newctx

    @staticmethod
    def Id():
        return EventStreamUpdate(None)


@dataclass
class Transition:
    q1: State
    p: Predicte
    q2: State
    alpha: DataUpdate
    beta: EventStreamUpdate

    def predict(self, conf: Configuration, event: Event) -> bool:
        """If this edge can go with (conf, event)"""
        return self.p.evaluate(conf, event)

    def advance(self, conf: Configuration, event: Event) -> Configuration:
        """Calculate next configuration"""
        return Configuration(
            self.q2,
            self.alpha.update(conf.eta, event),
            self.beta.update(conf.ctx, event),
        )

    def is_epsilon(self) -> bool:
        return self.p.epsilon


TransitionCollection = list


@dataclass
class DST:
    Sigma: Set[str]
    Pi: Set[str]
    X: Set[DataVariable]
    Y: Set[StreamVariable]
    Q: Set[State]
    q0: State
    eta: Func[DataVariable, Val]
    Delta: TransitionCollection[Transition]

    def __post_init__(self) -> None:
        self.edge_map: dict[str, TransitionCollection[Transition]] = {}
        for edge in self.Delta:
            q1 = edge.q1.name
            if not q1 in self.edge_map:
                self.edge_map[q1] = []
            self.edge_map[q1].append(edge)

    def initial_configuration(self) -> Configuration:
        return Configuration(self.q0, self.eta, {})

    def start_from(self, q: State) -> TransitionCollection[Transition]:
        qname = q.name
        if not qname in self.edge_map:
            return []
        return self.edge_map[qname]

    def find_accepted(self, conf: Configuration) -> Configuration:
        """Find accepted configuration from conf via epsilon-transition"""
        q = conf.get_state()
        for edge in self.start_from(q):
            if edge.is_epsilon() and edge.predict(conf, None):
                newconf = edge.advance(conf, None)
                if self.accept(newconf):
                    return newconf
                newtry = self.find_accepted(newconf)
                if newtry is not None:
                    return newtry
        return None

    def accept(self, conf: Configuration) -> bool:
        return conf.get_state().out is not None

    def output(self, conf: Configuration) -> bool:
        qout = conf.get_state().out
        return dict([(key, conf.ctx[var]) for key, var in qout.items()])
