"""Compile ast to executor"""

from typing import Callable
from reflinkcep.ast import AST, Query, Variables
from reflinkcep.DST import (
    DST,
    DataUpdate,
    EventStreamUpdate,
    Func,
    Predicte,
    Set,
    State,
    Transition,
    TransitionCollection,
    TrueCondition,
)
from reflinkcep.executor import Executor


def get_take_dataupdate(ast: AST) -> tuple[DataUpdate, Func]:
    variables: Variables = ast.get("variables", {})
    X = variables.keys()
    du = DataUpdate(dict((k, v["update"]) for k, v in variables.items()))
    eta0 = Func((k, v["initial"]) for k, v in variables.items())
    return X, du, eta0


class ASTCompiler:
    compiler_map = dict()

    @classmethod
    def register(cls, ast_type: str):
        def wrapper(compiler: Callable[[AST], DST]):
            cls.compiler_map[ast_type] = compiler
            return compiler

        return wrapper

    @classmethod
    def get_compiler(cls, ast_type: str):
        if ast_type in cls.compiler_map:
            return cls.compiler_map[ast_type]
        raise ValueError("Not supported AST type: {}".format(ast_type))

    @classmethod
    def compile(cls, ast: AST) -> DST:
        return cls.get_compiler(ast["type"])(ast)


@ASTCompiler.register("spat")
def compile_spat(ast: AST) -> DST:
    assert ast["type"] == "spat"
    name: str = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]

    X, tdu, eta0 = get_take_dataupdate(ast)

    S = Set([ev])
    P = Set([name])
    # X = Set()
    Y = Set([name])
    q0 = State(f"{name}-0")
    qf = State(f"{name}-f", {name: name})
    Q = set([q0, qf])
    D = [Transition(q0, Predicte(ev, cndt), qf, tdu, EventStreamUpdate(name))]

    return DST(S, P, X, Y, Q, q0, eta0, D)


@ASTCompiler.register("lpat")
def compile_lpat(ast: AST) -> DST:
    assert ast["type"] == "lpat", "Wrong ast type with {}".format(ast["type"])
    name: str = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]
    loop = ast["loop"]
    theta = loop["contiguity"]
    n = loop["from"]
    m = loop["to"]

    X, tdu, eta0 = get_take_dataupdate(ast)

    S = Set(ev)
    P = Set(name)
    # X = Set(variables.keys())
    Y = Set(name)
    q0 = State(f"{name}-0")
    qf = State(f"{name}-f", {name: name})
    q = [State(f"{name}-{i+1}") for i in range(m)]
    q.insert(0, q0)
    Q = Set([*q, qf])
    D = TransitionCollection()

    # take transitions
    esu = EventStreamUpdate(name)
    D.extend(
        [Transition(q[i], Predicte(ev, cndt), q[i + 1], tdu, esu) for i in range(m)]
    )

    # proceed transitions
    D.extend(
        [
            Transition(
                q[i],
                Predicte(None, TrueCondition),
                qf,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
            for i in range(n, m + 1)
        ]
    )

    return DST(S, P, X, Y, Q, q0, eta0, D)


def compile_impl(ast: AST) -> DST:
    return ASTCompiler.compile(ast)


def compile(query: Query) -> Executor:
    dst = compile_impl(query.patseq)
    return Executor(dst)
