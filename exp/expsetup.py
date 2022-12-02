import os
from pathlib import Path
from typing import Iterable

DIR_PATH = Path(__file__).parent.resolve()

DIV = os.getenv("DIV", ".")
DIV_ROOT = Path(__file__).parent.resolve() / DIV
TC_DEST = DIV_ROOT / "testcases"
RS_DEST = DIV_ROOT / "ourresults"
JAVA_DEST = DIV_ROOT / "javapackage"

FLINK_RESULT_DIR = "flinkresults"
FLINK_RESULT_DEST = DIV_ROOT / FLINK_RESULT_DIR

RS_DEST.mkdir(exist_ok=True)


def find_testcases() -> Iterable[Path]:
    return TC_DEST.glob("*.yml")


class DivInfo:
    def __init__(self, div: str):
        self.div = div
        self.div_root = DIR_PATH / div
        self.tc_dest = self.div_root / "testcases"
        self.rs_dest = self.div_root / "ourresults"
        self.java_dest = self.div_root / "javapackage"
        self.flink_result_dir = "flinkresults"
        self.flink_result_dest = self.div_root / self.flink_result_dir

    def find_testcases(self) -> Iterable[Path]:
        return self.tc_dest.glob("*.yml")

    def get_our_result(self, tc: Path) -> Path:
        return self.rs_dest / (tc.stem + ".yml")

    def get_flink_result(self, tc: Path) -> Path:
        return self.flink_result_dest / (tc.stem + ".txt")

    def get_flink_stat(self, tc: Path) -> Path:
        return self.div_root / "massivestat" / (tc.stem + ".ms")

    def find_flink_results(self) -> Iterable[Path]:
        return self.flink_result_dest.glob("*.txt")

    def find_our_results(self) -> Iterable[Path]:
        return self.rs_dest.glob("*.txt")

    def find_java_files(self) -> Iterable[Path]:
        return self.java_dest.glob("*.java")

    def find_jar_files(self) -> Iterable[Path]:
        return self.java_dest.glob("*.jar")
