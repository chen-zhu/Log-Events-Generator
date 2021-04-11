from grammar import *

if __name__ == "__main__":
    Syntax_parser = grammar()
    expr = Syntax_parser.Syntax()
    results = expr.parseFile("rules/ForkWorkflow")
    print(results)
