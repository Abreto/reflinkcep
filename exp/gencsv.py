#!/usr/bin/env python3
from pathlib import Path

import pandas as pd
import yaml
from loguru import logger
from tqdm import tqdm

from expsetup import DivInfo

DIVS = ["div-no-gpat", "gpat-combine", "gpat-single"]
RESULTS_COLLECTION_DEST_DIR = Path(__file__).parent.resolve() / "results-collection"


class ExpCollector:
    HEADER = [
        "div",
        "no",
        "query",
        "skip_strategy",
        "input",
        "result",
        "our",
        "flinkcep",
        "our_ms",
        "flinkcep_ms",
    ]

    def __init__(self, dest: Path) -> None:
        self.dest = dest

        self.total = pd.DataFrame(columns=self.HEADER)

        self.divres = None
        self.setup()

    def collect(self, div: DivInfo) -> None:
        self.divres = []
        tcs = sorted(list(div.find_testcases()), key=lambda x: int(x.stem))
        for tc in tqdm(tcs):
            self.collect_tc(div, tc)

        df = pd.DataFrame(data=self.divres, columns=self.HEADER)
        self.total = pd.concat([self.total, df], ignore_index=True)

        df.to_excel(self.dest / "{}.xlsx".format(div.div))

    def collect_tc(self, div: DivInfo, tc: Path) -> None:
        no = tc.stem

        with open(div.get_our_result(tc)) as f:
            our_result = yaml.full_load(f)
        input_repr = ",".join(
            "{}({})".format(e["type"], ",".join(str(v) for _, v in e["attrs"].items()))
            for e in our_result["input"]
        )
        our_result_str = our_result["results"]["reflinkcep"]["output"].strip()
        our_result_ms = float(
            "{:.3f}".format(our_result["results"]["reflinkcep"]["elapsed_ms"])
        )

        with open(div.get_flink_result(tc)) as f:
            flinkcep_result_str = f.read().strip()
        with open(div.get_flink_stat(tc)) as f:
            flinkcep_result_ms = float(f.read())

        same = our_result_str == flinkcep_result_str

        self.divres.append(
            (
                div.div,
                no,
                our_result["query"]["context"]["repr"],
                our_result["query"]["context"]["strategy"],
                input_repr,
                "SAME" if same else "DIFF",
                our_result_str,
                flinkcep_result_str,
                our_result_ms,
                flinkcep_result_ms,
            )
        )

    def setup(self):
        self.dest.mkdir(parents=True, exist_ok=True)

    def close(self):
        self.total.to_excel(self.dest / "total.xlsx")


def main() -> None:
    exp = ExpCollector(dest=RESULTS_COLLECTION_DEST_DIR)

    for div in DIVS:
        logger.info("Collecting {}", div)
        exp.collect(DivInfo(div))

    exp.close()


if __name__ == "__main__":
    main()
