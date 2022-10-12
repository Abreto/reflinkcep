# FlinkCEP Recreate

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
