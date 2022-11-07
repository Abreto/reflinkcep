# FlinkCEP Recreate

## Test
```
LOGLEVEL=info python3 -m unittest discover -s tests
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
