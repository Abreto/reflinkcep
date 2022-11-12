import logging
import os
import time

from reflinkcep.ast import Query
from reflinkcep.compile import compile
from reflinkcep.event import Event, EventStream
from reflinkcep.executor import MatchStream, Match
from reflinkcep.operator import CEPOperator

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "WARNING").upper())


class stopwatch:
    def __init__(self):
        self.start_time = time.time()

    def elapsed_ms(self):
        return (time.time() - self.start_time) * 1000

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


def match_repr(match: Match) -> str:
    return "; ".join(
        "{}: {}".format(name, ", ".join(str(ev) for ev in evs))
        for name, evs in match.items()
    )


class TestRecorder:
    CSV_HEAD = "#,from,pattern,input,output,elapsed_ms\n"
    dest = "results-{}.csv".format(time.strftime("%Y%m%d-%H%M%S"))
    idx = 0
    should_record = os.environ.get("GENCSV", "0") == "1"
    if should_record:
        with open(dest, "w") as f:
            f.write(CSV_HEAD)

    @classmethod
    def record(cls, line: str) -> None:
        if not cls.should_record:
            return
        if not line.endswith("\n"):
            line += "\n"
        with open(cls.dest, "a") as f:
            f.write(line)

    @classmethod
    def log(
        cls,
        query: Query,
        input: EventStream,
        output: MatchStream,
        elapsed_ms: float,
    ):
        fancy_output = "\n".join(match_repr(x) for x in output)
        cls.record(
            '{},"{}","{}","{}","{}",{}'.format(
                cls.idx, query._get_from(), query, input, fancy_output, elapsed_ms
            )
        )
        cls.idx += 1


def run_query_raw(query: Query, input: EventStream) -> MatchStream:
    with stopwatch() as sw:
        operator = CEPOperator.from_query(query)
        logger.debug(
            "dst: from %s\n%s",
            operator.executor.dst.q0,
            operator.executor.dst._print_trans_map(),
        )
        output = operator << input
        elapsed_ms = sw.elapsed_ms()
    TestRecorder.log(query, input, output, elapsed_ms)
    return output
