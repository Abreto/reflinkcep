from pathlib import Path
from typing import Any

from loguru import logger as llogger
from tqdm import tqdm
import yaml

from run import find_testcases

DEST = Path(__file__).parent.resolve() / "javapackage"
RESULTS_DIR = "results"

JAVA_PACKAGE_ROOT = "science.abreto.flinkcep"
JAVA_PACKAGE = f"{JAVA_PACKAGE_ROOT}.massive"
JAVA_GENERAL_TEMPLATE = """/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package {package};

{custom_import}

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.operator.CepOperator;

import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.lang.reflect.Field;

{class_code}
"""
JAVA_CLASS_TEMPLATE = """public class {testname} {{
    {methods_code}
}}
"""
FLINKCEP_TEMPLATE = """public static void main(String[] args) throws Exception {{
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String pattern_string = "{pattern_repr}";
        List<Event> input_events = Arrays.asList(
            {events_code}
        );

        DataStream<String> exp_title = env.fromElements(
            "pattern: " + pattern_string,
            "input: " + input_events
        );

        DataStream<Event> input = env.fromCollection(input_events);


        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.{skip_strategy_member}();
        Pattern<Event, ?> pattern =
            {pattern_code}
        ;

        System.out.println("pattern: " + pattern.toString());

        PatternStream<Event> patternStream = CEP.pattern(input, pattern);

        //////////////////////// inProcessingTime() is of vital IMPORTANCE \\\\\\\\\\\\\\\\\\\\\\\\\\\\
        DataStream<String> result = patternStream.inProcessingTime().select(
            new PatternSelectFunction<Event, String>() {{
                @Override
                public String select(Map<String, List<Event>> pattern) throws Exception {{
                    return String.join("; ", pattern.entrySet().stream().map(
                        entry -> entry.getKey() + ": " + String.join(
                            ", ",
                            entry.getValue().stream().map(Event::toString).toArray(String[]::new)
                        )
                    ).toArray(String[]::new));
                }}
            }}
        );

        // System.out.println("resultStream.transformation: " + result.getTransformation().toString());
        CepOperator cepop = (CepOperator) ((OneInputTransformation) result.getTransformation()).getOperator();
        // System.out.println("resultStream.transformation.operator: " + cepop.toString());
        Field nfaFactory = cepop.getClass().getDeclaredField("nfaFactory");
        nfaFactory.setAccessible(true);
        // System.out.println("nfaFactory: " + nfaFactory.get(cepop).toString());
        Field states = nfaFactory.get(cepop).getClass().getDeclaredField("states");
        states.setAccessible(true);
        System.out.println(pattern_string + "'s states:");
        System.out.println(states.get(nfaFactory.get(cepop)).toString());

        exp_title.print();
        input.print();
        result.print();
        result.writeAsText("{result_dest}", WriteMode.OVERWRITE);

        // execute program
        env.execute("Flink CEP Experiment");
    }}
"""

JAVA_EVENT_CLASS_TEMPLATE = """/**
 * General Event used to do experiments.
 */

package {package};

public class Event {{
    public int id;
    public int name;
    public int price;

    public Event(int id, int name, int price) {{
        this.id = id;
        this.name = name;
        this.price = price;
    }}

    public int getPrice() {{
        return price;
    }}

    public int getId() {{
        return id;
    }}

    public int getName() {{
        return name;
    }}

    @Override
    public String toString() {{
        return "e(" + id + "," + name + "," + price + ")";
    }}
}}
"""


def read_yaml(file: Path):
    with open(file) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def write_class(class_name, class_code):
    with open(DEST / f"{class_name}.java", "w") as f:
        f.write(class_code)


def generate_auxiliary_classes():
    write_class("Event", JAVA_EVENT_CLASS_TEMPLATE.format(package=JAVA_PACKAGE))


def translate_input_events(input_events):
    events = map(lambda e: e["attrs"], input_events)
    return ",\n            ".join(
        f"new Event({event['id']}, {event['name']}, {event['price']})"
        for event in events
    )


def translate_skip_strategy(ss: str) -> str:
    return ss[0].lower() + ss[1:]


class ASTTranslator:
    class TranslateHandler:
        @classmethod
        def register(cls, name: str):
            def decorator(func):
                setattr(cls, name, func)
                return func

            return decorator

    def is_gpat(self, ast: dict):
        nodetype: str = ast["type"]
        return nodetype.startswith("gpat")

    def is_inf(self, ast: dict) -> bool:
        nodetype: str = ast["type"]
        return nodetype.endswith("-inf")

    COMBINING_CONTIGUITY_MAP = {
        "strict": "next",
        "relaxed": "followedBy",
        "nd-relaxed": "followedByAny",
    }

    def compose_condition(
        self, prefix: str, cndt: dict, variables: dict = None, pattern_name: str = ""
    ) -> str:
        self.indent_in()
        where = ".{}(".format(prefix) + self.nl_indent()
        where += self.translate_condition(cndt, variables, pattern_name)
        self.indent_out()
        where += self.nl_indent() + ")"
        return where

    def compose_where(
        self, cndt: dict, variables: dict = None, pattern_name: str = ""
    ) -> str:
        return self.compose_condition("where", cndt, variables, pattern_name)

    def compose_suffix(
        self,
        loop: dict,
        until: dict,
        inf: bool = False,
        variables: dict = None,
        pattern_name: str = "",
        default_contiguity: str = "relaxed",
    ) -> str:
        suffix = ""
        n = loop["from"]
        m = loop.get("to", None)
        contiguity = loop.get("contiguity", default_contiguity)

        if n == 0:
            n = 1
            suffix = ".optional()"

        if inf:
            # inf
            if n == 1:
                suffix = ".oneOrMore()" + suffix
            else:
                suffix = ".tiemsOrMore({})".format(n) + suffix
            if until is not None:
                suffix += self.compose_condition(
                    "until", until, variables, pattern_name
                )
        else:
            assert m is not None
            suffix = ".times({},{})".format(n, m) + suffix

        if contiguity == "strict":
            suffix += ".consecutive()"
        elif contiguity == "nd-relaxed":
            suffix += ".allowCombinations()"

        return suffix

    def compose_skipstrategy(self):
        sstr = ""
        if self.first:
            sstr = ", skipStrategy"
        self.first = False
        return sstr

    @TranslateHandler.register("spat")
    def spat_handler(self, ast: dict) -> str:
        if self.first_of_sequence():
            head_tpl = 'Pattern.<Event>begin("{{name}}"{})'.format(
                self.compose_skipstrategy()
            )
        else:
            head_tpl = f".{self.get_concat()}({{name}})"
        head = head_tpl.format(name=ast["name"])

        where = self.compose_where(ast["cndt"], ast.get("variables", None))

        return head + where

    @TranslateHandler.register("lpat")
    @TranslateHandler.register("lpat-inf")
    def lpat_handler(self, ast: dict) -> str:
        if self.first_of_sequence():
            head_tpl = 'Pattern.<Event>begin("{{name}}"{})'.format(
                self.compose_skipstrategy()
            )
        else:
            head_tpl = f'.{self.get_concat()}("{{name}}")'
        head = head_tpl.format(name=ast["name"])

        where = self.compose_where(ast["cndt"], ast.get("variables", None), ast["name"])

        suffix = self.compose_suffix(
            ast["loop"],
            ast.get("until", None),
            inf=self.is_inf(ast),
            variables=ast.get("variables", None),
            pattern_name=ast["name"],
        )

        return head + where + suffix

    @TranslateHandler.register("combine")
    def combine_handler(self, ast: dict) -> str:
        left_code = self.translate_pattern(ast["left"])
        code = left_code

        right = ast["right"]
        concat_fun = self.COMBINING_CONTIGUITY_MAP[ast["contiguity"]]
        if self.is_gpat(right):
            """group pattern"""
            raise NotImplementedError(
                f"Concat with a group pattern is not supported yet."
            )
        else:
            self.set_concat(concat_fun)
            right_code = self.translate_pattern(right)
            code += self.nl_indent() + right_code

        return code

    @TranslateHandler.register("gpat")
    @TranslateHandler.register("gpat-times")
    @TranslateHandler.register("gpat-inf")
    def gpat_handler(self, ast: dict) -> str:
        assert self.first_of_sequence()  # currently only support gpat at first

        template = "Pattern.begin(" + self.in_nl_indent()
        template += "{child_code}"
        if self.first_of_sequence():
            template += self.compose_skipstrategy()
        template += self.out_nl_indent() + ")"

        suffix = ""
        nodetype = ast["type"]
        if nodetype == "gpat-times" or nodetype == "gpat-inf":
            suffix += self.compose_suffix(
                loop=ast["loop"],
                until=ast.get("until", None),
                inf=self.is_inf(ast),
                pattern_name="",
                default_contiguity="strict",
            )

        self.indent_in()
        self.set_first_of_seq(True)
        child_code = self.translate_pattern(ast["child"])
        self.indent_out()

        mainpart = template.format(child_code=child_code)
        return mainpart + suffix

    def translate_condition(
        self, cndt: dict, variables: dict = None, pattern_name: str = ""
    ) -> str:
        iterative = (
            variables is not None
        )  # TODO: This is buggy (what about variable used in cndt but not in variables?)

        def filter_expr(expr: str) -> str:
            expr = expr.replace("and", "&&")
            expr = expr.replace("or", "||")
            expr = expr.replace("not", "!")

            for mb in ["id", "name", "price"]:
                expr = expr.replace(f"{mb}", f"value.get{mb.capitalize()}()")

            return expr

        def compose_filter_body():
            code = ""

            if iterative:
                for var, vardef in variables.items():
                    code += f"int {var} = {vardef['initial']};" + self.nl_indent()

                code += (
                    'for (Event event: ctx.getEventsForPattern("{}")) {{'.format(
                        pattern_name
                    )
                    + self.in_nl_indent()
                )
                code += self.nl_indent().join(
                    [
                        f"{var} = {filter_expr(vardef['update'])};"
                        for var, vardef in variables.items()
                    ]
                )
                code += self.out_nl_indent() + "}" + self.nl_indent()

            code += "return " + filter_expr(cndt["expr"]) + ";"

            return code

        def compose_filter():
            code = "@Override" + self.nl_indent()
            code += (
                "public boolean filter(Event value{} {{".format(
                    ", Context<Event> ctx) throws Exception" if iterative else ")"
                )
                + self.in_nl_indent()
            )
            code += compose_filter_body()
            code += self.out_nl_indent() + "}"

            return code

        def compose_tostring():
            code = "@Override" + self.nl_indent()
            code += "public String toString() {" + self.in_nl_indent()
            code += 'return "{}";'.format(cndt["expr"]) + self.out_nl_indent()
            code += "}"
            return code

        code = (
            "new {}Condition<Event>() {{".format("Iterative" if iterative else "Simple")
            + self.in_nl_indent()
        )

        code += compose_filter()
        code += self.nl_indent()
        code += compose_tostring()

        code += self.out_nl_indent() + "}"

        return code

    def translate_pattern(self, ast: dict, *args, **kwargs) -> str:
        nodetype = ast["type"]
        if not hasattr(self.TranslateHandler, nodetype):
            return "[UnImplementedPart {}]".format(nodetype)
            # raise NotImplementedError(f"Unknown node type: {nodetype}")

        handler = getattr(self.TranslateHandler, nodetype)
        return handler(self, ast, *args, **kwargs)

    def __init__(self) -> None:
        self.first = True
        self.indent = 3
        self.first_of_seq = True
        self.concat_fun: str = ""

    def set_first_of_seq(self, yesorno: bool = True):
        self.first_of_seq = yesorno

    def first_of_sequence(self) -> bool:
        return self.first_of_seq

    def set_concat(self, cf):
        self.concat_fun = cf
        self.set_first_of_seq(False)

    def get_concat(self):
        return self.concat_fun

    def indent_offset(self, offset):
        self.indent += offset

    def indent_in(self):
        return self.indent_offset(1)

    def indent_out(self):
        return self.indent_offset(-1)

    def nl_indent(self) -> str:
        return "\n{}".format(" " * self.indent * 4)

    def in_nl_indent(self) -> str:
        self.indent_in()
        return self.nl_indent()

    def out_nl_indent(self) -> str:
        self.indent_out()
        return self.nl_indent()

    def __call__(self, ast: dict) -> str:
        return self.translate_pattern(ast)


def translate_pattern(ast: dict) -> str:
    ast_translater = ASTTranslator()
    return ast_translater(ast)


def generate_test_class(classname: str, tcdef: dict, result_dest: str) -> str:
    input_events = tcdef["input"]
    query = tcdef["query"]
    context = query["context"]

    methods_code = FLINKCEP_TEMPLATE.format(
        pattern_repr=context["repr"],
        events_code=translate_input_events(input_events),
        skip_strategy_member=translate_skip_strategy(context["strategy"]),
        pattern_code=translate_pattern(query["patseq"]),
        result_dest=result_dest,
    )

    return JAVA_CLASS_TEMPLATE.format(testname=classname, methods_code=methods_code)


def generate_testcase(tc: Path):
    name = tc.stem
    tcdef = read_yaml(tc)

    classname = f"Test_{name}"
    classcode = generate_test_class(classname, tcdef, f"{RESULTS_DIR}/{name}.txt")

    write_class(
        classname,
        JAVA_GENERAL_TEMPLATE.format(
            package=JAVA_PACKAGE,
            custom_import=f"import {JAVA_PACKAGE}.Event;",
            class_code=classcode,
        ),
    )


def generate_javapkg(testcases):
    generate_auxiliary_classes()

    for tc in tqdm(list(testcases)):
        try:
            generate_testcase(tc)
        except Exception as e:
            llogger.exception(e)


def main() -> None:
    DEST.mkdir(exist_ok=True)
    testcases = find_testcases()
    generate_javapkg(testcases)


if __name__ == "__main__":
    main()
