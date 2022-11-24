import os
import logging
import random
import time
from pathlib import Path
import multiprocessing

from loguru import logger
from tqdm import tqdm
import yaml

from reflinkcep.event import Event
from reflinkcep.ast import Query
from reflinkcep.executor import Match
from reflinkcep.operator import CEPOperator

from generate import TC_DEST

RS_DEST = Path(__file__).parent.resolve() / "results"
RS_DEST.mkdir(exist_ok=True)

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


def find_testcases():
    return TC_DEST.glob("*.yml")


def transform_input(rawes):
    return list(map(lambda e: Event(e["type"], e["attrs"]), rawes))


@logger.catch()
def run_test(tc: Path):
    with open(tc) as f:
        tcdef = yaml.load(f, Loader=yaml.FullLoader)

    with stopwatch() as sw:
        query = Query.from_dict(tcdef["query"])
        ein = transform_input(tcdef["input"])

        op = CEPOperator.from_query(query)
        eout = op << ein
        elapsed_ms = sw.elapsed_ms()

    if "results" not in tcdef:
        tcdef["results"] = {}
    tcdef["results"]["reflinkcep"] = {
        "output": "\n".join(match_repr(x) for x in eout),
        "elapsed_ms": elapsed_ms,
    }
    with open(RS_DEST / tc.name, "w") as f:
        yaml.dump(tcdef, f)


def run_tests(tcs):
    filterd = []
    already = set(map(lambda f: f.name, RS_DEST.glob("*.yml")))
    for tc in tcs:
        if tc.name not in already:
            filterd.append(tc)
    tcs = filterd
    # run_test(tcs[0])
    random.shuffle(tcs)
    with multiprocessing.Pool(4) as pool:
        pool.map(run_test, tcs)
    # for tc in tqdm(tcs):
    #     try:
    #         run_test(tc)
    #     except Exception as e:
    #         print(f"Error running {tc}: {e}")


def main():
    testcases = find_testcases()
    run_tests(testcases)


if __name__ == "__main__":
    main()
