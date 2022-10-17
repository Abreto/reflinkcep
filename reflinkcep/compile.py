from reflinkcep.ast import AST, Query
from reflinkcep.DST import (DST, DataUpdate, EventStreamUpdate, Predicte,
                            State, Transition)
from reflinkcep.executor import Executor


def compile_spat(ast: AST) -> DST:
    assert ast["type"] == "spat"
    name = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]

    S = set([ev])
    P = set([name])
    X = set()
    Y = set([name])
    q0 = State(f"{name}0")
    qf = State(f"{name}f", lambda p: name if p == name else None)
    Q = set([q0, qf])
    D = [Transition(q0, Predicte(ev, cndt), qf, DataUpdate(), EventStreamUpdate(name))]

    return DST(S, P, X, Y, Q, q0, lambda x: 0, D)


def compile_impl(ast: AST) -> DST:
    if ast["type"] == "spat":
        # single pattern
        return compile_spat(ast)


def compile(query: Query) -> Executor:
    dst = compile_impl(query.patseq)
    return Executor(dst)
