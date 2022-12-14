"""Compile ast to executor"""

from typing import Callable
from reflinkcep.ast import AST, Query, QueryContext, Variables
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
    func_merge,
    transitions_union,
)
from reflinkcep.executor import AfterMatchStrategy, Executor


def get_take_dataupdate(ast: AST) -> tuple[Set, DataUpdate, Func]:
    variables: Variables = ast.get("variables", {})
    X = Set(variables.keys())
    du = DataUpdate(dict((k, v["update"]) for k, v in variables.items()))
    eta0 = Func((k, v["initial"]) for k, v in variables.items())
    return X, du, eta0


class ASTCompiler:
    compiler_map = dict()

    @classmethod
    def register(cls, ast_type: str):
        def wrapper(compiler: Callable[[AST, QueryContext], DST]):
            cls.compiler_map[ast_type] = compiler
            return compiler

        return wrapper

    @classmethod
    def get_compiler(cls, ast_type: str):
        if ast_type in cls.compiler_map:
            return cls.compiler_map[ast_type]
        raise ValueError("Not supported AST type: {}".format(ast_type))

    @classmethod
    def compile(cls, ast: AST, ctx: QueryContext) -> DST:
        return cls.get_compiler(ast["type"])(ast, ctx)


@ASTCompiler.register("spat")
def compile_spat(ast: AST, ctx: QueryContext = None) -> DST:
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
def compile_lpat(ast: AST, ctx: QueryContext) -> DST:
    assert ast["type"] == "lpat", "Wrong ast type with {}".format(ast["type"])
    name: str = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]
    loop = ast["loop"]
    theta = loop["contiguity"]
    n = loop["from"]
    m = loop["to"]

    X, tdu, eta0 = get_take_dataupdate(ast)

    S = Set([ev])
    P = Set([name])
    # X = Set(variables.keys())
    Y = Set([name])
    q0 = State(f"{name}-0")
    qf = State(f"{name}-f", {name: name})
    q = [State(f"{name}-{i+1}") for i in range(m)]
    q_ignore = [State(f"{name}-ig-{i}") for i in range(1, m)]
    q.insert(0, q0)  # q_ignore[i] ~ q[i+1], 0 <= i < m - 1
    Q = Set([*q, qf, *q_ignore])
    D = TransitionCollection()

    # take transitions
    esu = EventStreamUpdate(name)
    D.extend(
        [Transition(q[i], Predicte(ev, cndt), q[i + 1], tdu, esu) for i in range(m)]
    )
    D.extend(
        [
            Transition(q_ignore[i], Predicte(ev, cndt), q[i + 2], tdu, esu)
            for i in range(m - 1)
        ]
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

    def compose_ignore_transitions(q1: State, q2: State, pred: Predicte):
        return [Transition(q1, pred, q2, DataUpdate.Id(), EventStreamUpdate.Id())]

    # ignore transitions
    def compute_ignore_transitions():
        if theta == "strict":
            return []
        if theta == "relaxed":
            total_evtypes = ctx["schema"].keys()
            ret = [
                Transition(
                    q[i],
                    Predicte(ev, cndt).neg(),
                    q_ignore[i - 1],
                    DataUpdate.Id(),
                    EventStreamUpdate.Id(),
                )
                for i in range(1, m)
            ]
            ret.extend(
                [
                    Transition(
                        q_ignore[i - 1],
                        Predicte(ev, cndt).neg(),
                        q_ignore[i - 1],
                        DataUpdate.Id(),
                        EventStreamUpdate.Id(),
                    )
                    for i in range(1, m)
                ]
            )
            for e in total_evtypes:
                if e != ev:
                    ret.extend(
                        [
                            Transition(
                                q[i],
                                Predicte(e, TrueCondition),
                                q_ignore[i - 1],
                                DataUpdate.Id(),
                                EventStreamUpdate.Id(),
                            )
                            for i in range(1, m)
                        ]
                    )
                    ret.extend(
                        [
                            Transition(
                                q_ignore[i - 1],
                                Predicte(e, TrueCondition),
                                q_ignore[i - 1],
                                DataUpdate.Id(),
                                EventStreamUpdate.Id(),
                            )
                            for i in range(1, m)
                        ]
                    )
            return ret
        assert theta == "nd-relaxed", "Incorrect theta: {}".format(theta)
        return [
            Transition(
                q[i],
                Predicte(Predicte.ANY_TYPE, TrueCondition),
                q_ignore[i - 1],
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
            for i in range(1, m)
        ] + [
            Transition(
                q_ignore[i - 1],
                Predicte(Predicte.ANY_TYPE, TrueCondition),
                q_ignore[i - 1],
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
            for i in range(1, m)
        ]

    D.extend(compute_ignore_transitions())

    return DST(S, P, X, Y, Q, q0, eta0, D)


@ASTCompiler.register("lpat-inf")
def compile_lpat_inf(ast: AST, ctx: QueryContext) -> DST:
    name: str = ast["name"]
    ev = ast["event"]
    cndt = ast["cndt"]
    loop = ast["loop"]
    theta = loop["contiguity"]
    n = loop["from"]

    optional = False
    if 0 == n:
        optional = True
        n = 1

    X, tdu, eta0 = get_take_dataupdate(ast)

    S = Set([ev])
    P = Set([name])
    # X = Set(variables.keys())
    Y = Set([name])
    q0 = State(f"{name}-0")
    qf = State(f"{name}-f", {name: name})
    q = [State(f"{name}-{i+1}") for i in range(n)]
    qnp = State(f"{name}-np")
    q.insert(0, q0)
    Q = Set([*q, qnp, qf])
    D: TransitionCollection[Transition] = TransitionCollection()

    # take transitions
    esu = EventStreamUpdate(name)
    D.extend(
        [Transition(q[i], Predicte(ev, cndt), q[i + 1], tdu, esu) for i in range(n)]
    )
    D.extend(
        [
            Transition(q[n], Predicte(ev, cndt), q[n], tdu, esu),
            Transition(qnp, Predicte(ev, cndt), q[n], tdu, esu),
        ]
    )

    # proceed transitions
    D.append(
        Transition(
            q[n],
            Predicte(None, TrueCondition),
            qf,
            DataUpdate.Id(),
            EventStreamUpdate.Id(),
        )
    )

    # ignore transitions
    def compute_ignore_transitions():
        if theta == "strict":
            return []
        if theta == "relaxed":
            total_evtypes = ctx["schema"].keys()
            negpred = Predicte(ev, cndt).neg()
            ret = [
                Transition(
                    q[i],
                    Predicte(ev, cndt).neg(),
                    q[i],
                    DataUpdate.Id(),
                    EventStreamUpdate.Id(),
                )
                for i in range(1, n)
            ]
            for e in total_evtypes:
                if e != ev:
                    ret.extend(
                        [
                            Transition(
                                q[i],
                                Predicte(e, TrueCondition),
                                q[i],
                                DataUpdate.Id(),
                                EventStreamUpdate.Id(),
                            )
                            for i in range(1, n)
                        ]
                    )
            ret.extend(
                [
                    Transition(
                        q[n], negpred, qnp, DataUpdate.Id(), EventStreamUpdate.Id()
                    ),
                    Transition(
                        qnp, negpred, qnp, DataUpdate.Id(), EventStreamUpdate.Id()
                    ),
                ]
            )
            for e in total_evtypes:
                if e != ev:
                    ret.extend(
                        [
                            Transition(
                                q[n],
                                Predicte(e, TrueCondition),
                                qnp,
                                DataUpdate.Id(),
                                EventStreamUpdate.Id(),
                            ),
                            Transition(
                                qnp,
                                Predicte(e, TrueCondition),
                                qnp,
                                DataUpdate.Id(),
                                EventStreamUpdate.Id(),
                            ),
                        ]
                    )
            return ret
        assert theta == "nd-relaxed", "Incorrect theta: {}".format(theta)
        ret = [
            Transition(
                q[i],
                Predicte(Predicte.ANY_TYPE, TrueCondition),
                q[i],
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
            for i in range(1, n)
        ]
        ret.extend(
            [
                Transition(
                    q[n],
                    Predicte(Predicte.ANY_TYPE, TrueCondition),
                    qnp,
                    DataUpdate.Id(),
                    EventStreamUpdate.Id(),
                ),
                Transition(
                    qnp,
                    Predicte(Predicte.ANY_TYPE, TrueCondition),
                    qnp,
                    DataUpdate.Id(),
                    EventStreamUpdate.Id(),
                ),
            ]
        )
        return ret

    D.extend(compute_ignore_transitions())

    if "until" in ast:
        # p:e:[cndt]_x{n,inf}Ucndt'
        cndtp = ast["until"]
        for trans in D:
            if not trans.is_epsilon():  # take or ignore
                trans.update_predict(trans.get_predict().with_until(cndtp))

    if optional:
        D.append(
            Transition(
                q0,
                Predicte(None, TrueCondition),
                qf,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )

    return DST(S, P, X, Y, Q, q0, eta0, D)


@ASTCompiler.register("combine")
def compile_combine(ast: AST, ctx: QueryContext) -> DST:
    contiguity: str = ast["contiguity"]
    left = ASTCompiler.compile(ast["left"], ctx)
    right = ASTCompiler.compile(ast["right"], ctx)

    S = left.Sigma.union(right.Sigma)
    P = left.Pi.union(right.Pi)
    X = left.X.union(right.X)
    Y = left.Y.union(right.Y)
    # TODO: Q?, with output
    Q = left.Q.union(right.Q)
    q01 = left.q0
    eta0 = func_merge(left.eta, right.eta)  # suupose X1 /\ X2 = {}
    D: TransitionCollection[Transition] = transitions_union(left.Delta, right.Delta)

    for q in left.final_states():
        for q2 in right.final_states():
            q2.extend_output(q.out)

    q02 = right.q0
    for q in left.final_states():
        D.append(
            Transition(
                q,
                Predicte(None, TrueCondition),
                q02,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )
        q.clear_output()

    if contiguity != "strict":
        q02_ignore = State(f"{q02.name}-ignore")
        Q.add(q02_ignore)
        for edge in right.start_from(q02):
            if edge.is_take():
                D.append(Transition(q02_ignore, edge.p, edge.q2, edge.alpha, edge.beta))
    if contiguity == "relaxed":
        right_ast = ast["right"]
        D.append(
            Transition(
                q02,
                Predicte(right_ast["event"], right_ast["cndt"]).neg(),
                q02_ignore,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )
        D.append(
            Transition(
                q02_ignore,
                Predicte(right_ast["event"], right_ast["cndt"]).neg(),
                q02_ignore,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )
        for e in ctx["schema"].keys():
            if e != right_ast["event"]:
                D.append(
                    Transition(
                        q02,
                        Predicte(e, TrueCondition),
                        q02_ignore,
                        DataUpdate.Id(),
                        EventStreamUpdate.Id(),
                    )
                )
                D.append(
                    Transition(
                        q02_ignore,
                        Predicte(e, TrueCondition),
                        q02_ignore,
                        DataUpdate.Id(),
                        EventStreamUpdate.Id(),
                    )
                )
    elif contiguity == "nd-relaxed":
        D.append(
            Transition(
                q02,
                Predicte(Predicte.ANY_TYPE, TrueCondition),
                q02_ignore,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )
        D.append(
            Transition(
                q02_ignore,
                Predicte(Predicte.ANY_TYPE, TrueCondition),
                q02_ignore,
                DataUpdate.Id(),
                EventStreamUpdate.Id(),
            )
        )

    return DST(S, P, X, Y, Q, q01, eta0, D)


@ASTCompiler.register("gpat")
def compile_gpat(ast: AST, ctx: QueryContext) -> DST:
    return ASTCompiler.compile(ast["child"], ctx)


@ASTCompiler.register("gpat-times")
def compile_gpat_times(ast: AST, ctx: QueryContext) -> DST:
    loop = ast["loop"]
    n = loop["from"]
    m = loop["to"]

    dst0 = ASTCompiler.compile(ast["child"], ctx)
    dst = [ASTCompiler.compile(ast["child"], ctx) for _ in range(m)]

    S = dst0.Sigma
    P = dst0.Pi
    X = dst0.X
    Y = dst0.Y
    q0 = State("gpat-0")
    qf = State("gpat-f")
    Q = Set([q0, qf])
    D: TransitionCollection[Transition] = TransitionCollection()

    for i in range(m):
        Q |= dst[i].Q
        D.extend(dst[i].Delta)

    eps = Predicte.epsilon()
    duid = DataUpdate.Id()
    esuid = EventStreamUpdate.Id()

    # proceed
    D.append(Transition(q0, eps, dst[0].q0, duid, esuid))
    for i in range(m - 1):
        for q in dst[i].final_states():
            D.append(Transition(q, eps, dst[i + 1].q0, duid, esuid))

    # output
    for i in range(n - 1, m):
        for q in dst[i].final_states():
            D.append(Transition(q, eps, qf, duid, esuid))
    for i in range(m):
        for q in dst[i].final_states():
            q.clear_output()
    for q in dst0.final_states():
        qf.extend_output(q.out)

    return DST(
        S, P, X, Y, Q, q0, dst0.eta, D
    )  # TODO: initialize variable on every group begining?


@ASTCompiler.register("gpat-inf")
def compile_gpat_inf(ast: AST, ctx: QueryContext) -> DST:
    loop = ast["loop"]
    n = loop["from"]
    optional = n == 0

    n = max(n, 1)
    dst0 = ASTCompiler.compile(ast["child"], ctx)
    dst = [ASTCompiler.compile(ast["child"], ctx) for _ in range(n)]

    S = dst0.Sigma
    P = dst0.Pi
    X = dst0.X
    Y = dst0.Y
    q0 = State("gpat-inf-0")
    qf = State("gpat-inf-f")
    Q = Set([q0, qf])
    D: TransitionCollection[Transition] = TransitionCollection()

    for i in range(n):
        Q |= dst[i].Q
        D.extend(dst[i].Delta)

    eps = Predicte.epsilon()
    duid = DataUpdate.Id()
    esuid = EventStreamUpdate.Id()

    # proceed
    D.append(Transition(q0, eps, dst[0].q0, duid, esuid))
    for i in range(n - 1):
        for q in dst[i].final_states():
            D.append(Transition(q, eps, dst[i + 1].q0, duid, esuid))
    for q in dst[n - 1].final_states():
        D.append(Transition(q, eps, dst[n - 1].q0, duid, esuid))

    # output
    for i in range(n - 1, n):
        for q in dst[i].final_states():
            D.append(Transition(q, eps, qf, duid, esuid))
    for i in range(n):
        for q in dst[i].final_states():
            q.clear_output()
    for q in dst0.final_states():
        qf.extend_output(q.out)

    if "until" in ast:
        cndt = ast["until"]
        for trans in D:
            if not trans.is_epsilon():
                trans.update_predict(trans.get_predict().with_until(cndt))

    if optional:
        D.append(Transition(q0, eps, qf, duid, esuid))

    return DST(
        S, P, X, Y, Q, q0, dst0.eta, D
    )  # TODO: initialize variable on every group begining?


def compile_impl(ast: AST, ctx: QueryContext) -> tuple[DST, AfterMatchStrategy]:
    strategy = AfterMatchStrategy.from_context(ctx)
    return ASTCompiler.compile(ast, ctx), strategy


def compile(query: Query) -> Executor:
    dst, strategy = compile_impl(query.patseq, query.context)
    return Executor(dst, strategy)
