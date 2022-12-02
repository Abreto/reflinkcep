#!/usr/bin/env python3
from pathlib import Path

import yaml

from reflinkcep.ast import ast_repr
from reflinkcep.event import Event, EventStream

DIV = "gpat-combine"
DIV_ROOT = Path(__file__).parent.resolve() / DIV
TC_DEST = DIV_ROOT / "testcases"


class TestcaseWriter:
    def __init__(self) -> None:
        self.count = 0
        self.dest = TC_DEST
        self.init()

    def init(self) -> None:
        if not self.dest.exists():
            self.dest.mkdir(parents=True)

    def write(self, tc: dict) -> None:
        fp = self.dest / "{}.yml".format(self.count)
        self.count += 1

        with open(fp, "w+") as f:
            yaml.dump(tc, f)


class TestcaseGenerator:
    def __init__(self, writer: TestcaseWriter) -> None:
        self.writer = writer

    def gen_spat(self):
        return {
            "type": "spat",
            "name": "ps",
            "event": "e",
            "cndt": {"expr": "name == 1"},  # a
        }

    def iter_lpat(self):
        base = {
            "name": "pl",
            "event": "e",
        }

        scndt = {
            "cndt": {"expr": "name == 2"},  # b
        }
        icndt = {
            "cndt": {"expr": "z1 + price <= 10"},  # sum(price) <= 10
            "variables": {
                "z1": {
                    "update": "z1 + price",
                    "initial": 0,
                }
            },
        }
        cndt_choices = [scndt, icndt]

        loop_choices = []
        until_choices = [{}, {"until": {"expr": "name == 3"}}]  # c
        for contiguity in ["strict", "relaxed", "nd-relaxed"]:
            for n, m in [(0, 3), (1, 3), (3, 3)]:
                loop_choices.append(
                    {
                        "type": "lpat",
                        "loop": {
                            "contiguity": contiguity,
                            "from": n,
                            "to": m,
                        },
                    }
                )
            for until in until_choices:
                for n in [0, 1]:
                    loop_choices.append(
                        {
                            "type": "lpat-inf",
                            "loop": {
                                "contiguity": contiguity,
                                "from": n,
                            },
                            **until,
                        }
                    )

        for cndt in cndt_choices:
            for loop in loop_choices:
                yield {**base, **cndt, **loop}

    def iter_contiguity(self):
        for theta in ["strict", "relaxed", "nd-relaxed"]:
            yield theta

    def ast_combine(self, left, right, theta):
        return {
            "type": "combine",
            "contiguity": theta,
            "left": left,
            "right": right,
        }

    def gen_query(self, ast):
        ret = []
        for ams in ["NoSkip", "SkipToNext", "SkipPastLastEvent"]:
            ret.append(
                {
                    "type": "Query",
                    "patseq": ast,
                    "context": {
                        "strategy": ams,
                        "schema": {"e": ["id", "name", "price"]},
                        "repr": ast_repr(ast),
                    },
                }
            )
        return ret

    def evstream_from_list(self, events):
        evstream = []
        for i, event in enumerate(events):
            evstream.append(
                {
                    "type": "e",
                    "attrs": {"id": i + 1, "name": event[0], "price": event[1]},
                }
            )
        return evstream

    def gen_stream_in(self, query):
        # TODO: return different input by query
        return self.evstream_from_list(
            [(1, 0), (2, 5), (1, 0), (2, 2), (1, 0), (3, 2), (1, 0), (2, 5), (1, 8)]
        )

    def construct_testcase(self, query, stream_in):
        if not isinstance(query, list):
            query = [query]
        for q in query:
            tc = {"query": q, "input": stream_in}
            self.writer.write(tc)

    def gen_combine(self) -> None:
        spat = self.gen_spat()
        for lpat in self.iter_lpat():
            for theta in self.iter_contiguity():
                ast = self.ast_combine(spat, lpat, theta)
                query = self.gen_query(ast)  # considering after-match strategy
                stream_in = self.gen_stream_in(query)
                self.construct_testcase(query, stream_in)

    def looping_choices(self):
        yield {"type": "gpat"}
        for n, m in [(0, 3), (1, 3), (3, 3)]:
            yield {
                "type": "gpat-times",
                "loop": {
                    "from": n,
                    "to": m,
                },
            }

        until_choices = [{}, {"until": {"expr": "name == 3"}}]  # c
        for until in until_choices:
            for n in [0, 1]:
                yield {"type": "gpat-inf", "loop": {"from": n}, **until}

    def ast_group(self, child, looping):
        return {"child": child, **looping}

    def gen_group(self) -> None:
        spat = self.gen_spat()
        for lpat in self.iter_lpat():
            for theta in self.iter_contiguity():
                child = self.ast_combine(spat, lpat, theta)
                for looping in self.looping_choices():
                    ast = self.ast_group(child, looping)
                    query = self.gen_query(ast)  # considering after-match strategy
                    stream_in = self.gen_stream_in(query)
                    self.construct_testcase(query, stream_in)

    def generate(self) -> None:
        # self.gen_combine()
        self.gen_group()


def generate_testcases():
    writer = TestcaseWriter()
    generator = TestcaseGenerator(writer=writer)
    generator.generate()


def main():
    generate_testcases()


if __name__ == "__main__":
    main()
