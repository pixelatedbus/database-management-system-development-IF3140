from abc import ABC, abstractmethod

class Rows:
    def __init__(self, rows: list[dict]):
        self.rows = rows

class Condition:
    def __init__(self, column: str, operation: str, operand: str):
        self.column = column
        self.operator = operation
        self.operand = operand
    
    def __repr__(self):
        return f"Condition({self.column} {self.operator} {self.operand})"
    
    def evaluate(self, row: dict) -> bool:
        if self.operator == "=":
            return row.get(self.column) == self.operand
        elif self.operator == "<>":
            return row.get(self.column) != self.operand
        elif self.operator == "<":
            return row.get(self.column) < self.operand
        elif self.operator == "<=":
            return row.get(self.column) <= self.operand
        elif self.operator == ">":
            return row.get(self.column) > self.operand
        elif self.operator == ">=":
            return row.get(self.column) >= self.operand
        else:
            raise ValueError(f"Unknown operator: {self.operator}")

class ConditionNode(ABC):
    @abstractmethod
    def evaluate(self, row: dict) -> bool:
        pass

class ComparisonNode(ConditionNode):
    def __init__(self, column: str, operator: str, operand: str):
        self.column = column
        self.operator = operator
        self.operand = operand
    
    def evaluate(self, row: dict) -> bool:
        if self.operator == "=":
            return row.get(self.column) == self.operand
        elif self.operator == "<>":
            return row.get(self.column) != self.operand
        elif self.operator == "<":
            return row.get(self.column) < self.operand
        elif self.operator == "<=":
            return row.get(self.column) <= self.operand
        elif self.operator == ">":
            return row.get(self.column) > self.operand
        elif self.operator == ">=":
            return row.get(self.column) >= self.operand
        else:
            raise ValueError(f"Unknown operator: {self.operator}")
        
class ANDNode(ConditionNode):
    def __init__(self, children: list[ConditionNode]):
        self.children = children
    
    def evaluate(self, row: dict) -> bool:
        return all(child.evaluate(row) for child in self.children)

class ORNode(ConditionNode):
    def __init__(self, children: list[ConditionNode]):
        self.children = children
    
    def evaluate(self, row: dict) -> bool:
        return any(child.evaluate(row) for child in self.children)
    

class NOTNode(ConditionNode):
    def __init__(self, child: ConditionNode):
        self.child = child
    
    def evaluate(self, row: dict) -> bool:
        return not self.child.evaluate(row)

        
class DataRetrieval:
    def __init__(self, tables: list[str], columns: list[str], conditions: list[Condition], search_type: str):
        self.tables = tables
        self.columns = columns
        self.conditions = conditions
        if not search_type:
            self.search_type = "AUTO"
        else:
            self.search_type = search_type

class DataWrite:
    def __init__(self, table: list[str], columns: list[str], conditions: list[Condition], values: list[str]):
        self.table = table
        self.columns = columns
        self.conditions = conditions
        self.values = values

class DataDeletion:
    def __init__(self, table: list[str], conditions: list[Condition]):
        self.table = table
        self.conditions = conditions