# FlinkCEP Recreate

## Install
```
pip3 install -e .
```

## Generate results in csv
```
GENCSV=1 python3 -m unittest discover
```

## Tests and Coverage
```
python3 -m coverage run -m unittest
python3 -m coverage report
```

## Architecture

```
PatternSequence AST
-(compile)-> CEPExecutor
-(with input)-> Match Result
```

## Structure of PatternSequence ASTs
```
PS := Pattern | PatternConcat
```
