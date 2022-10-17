from reflinkcep.ast import AST, Query
from reflinkcep.DST import (DST, DataUpdate, EventStreamUpdate, Predicte, Set,
                            State, Transition)
from reflinkcep.executor import Executor


def compile_spat(ast: AST) -> DST:
    assert ast["type"] == "spat"
    name: str = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]

    S = Set([ev])
    P = Set([name])
    X = Set()
    Y = Set([name])
    q0 = State(f"{name}-0")
    qf = State(f"{name}-f", {name: name})
    Q = set([q0, qf])
    D = [Transition(q0, Predicte(ev, cndt), qf, DataUpdate(), EventStreamUpdate(name))]

    return DST(S, P, X, Y, Q, q0, {}, D)


def compile_impl(ast: AST) -> DST:
    if ast["type"] == "spat":
        # single pattern
        return compile_spat(ast)


def compile(query: Query) -> Executor:
    dst = compile_impl(query.patseq)
    return Executor(dst)
