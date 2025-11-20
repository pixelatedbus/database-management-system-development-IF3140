class Rows:
    def __init__(self, rows: list[dict]):
        self.rows = rows

class Condition:
    def __init__(self, column: str, operation: str, operand: str):
        self.column = column
        self.operator = operation
        self.operand = operand

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