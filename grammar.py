from pyparsing import Literal, Word, ZeroOrMore, Forward, nums, oneOf, Group, alphas, alphanums, Optional, OneOrMore, \
    Group, Combine, CaselessKeyword


class grammar:

    def __init__(self):
        self.Naming = Word(alphas + "_", alphanums + "_")
        self.Separator = Literal(",").suppress()
        self.Sugar = Literal("-:").suppress()
        self.FNumber = Combine(Word("+-" + nums, nums)
                               + Optional("." + Optional(Word(nums)))
                               + Optional(CaselessKeyword("E") + Word("+-" + nums, nums)))

    def Body(self):
        return Group(self.Table()) + ZeroOrMore(Group(self.Separator + self.Table()))

    def Table(self):
        return self.Naming + Literal("(") + self.TableColumn() + ZeroOrMore(
            self.Separator + self.TableColumn()) + Literal(")")

    def TableColumn(self):
        return (self.Naming + Literal(":") + Group(self.Condition())) | self.Naming

    def Condition(self):
        return (Combine(Literal('"') + self.Naming + Literal('"'))) | self.Naming | self.FNumber

    def Header(self):
        return OneOrMore(Group(self.Sugar + self.Event()))

    def Event(self):
        return self.Naming + "(" + self.EventColumn() + ZeroOrMore(self.Separator + self.EventColumn()) + ")"

    def EventColumn(self):
        return (self.Naming + Literal(":") + Group(self.Condition())) | Group(self.NestedTable()) | self.Naming

    def NestedTable(self):
        return self.Naming + "<" + self.Naming + ">(" + self.NestedTableColumn() + ZeroOrMore(
            self.Separator + self.NestedTableColumn()) + Literal(")")

    def NestedTableColumn(self):
        return self.Naming + Optional(Literal(":") + self.Naming)

    def Syntax(self):
        mapping_rules = self.Naming + Literal("=").suppress() + Group(self.Body()) + Group(self.Header())
        return mapping_rules


