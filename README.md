# FlinkCEP Recreate

## Generate results in csv
```
GENCSV=1 python3 -m unittest discover
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
