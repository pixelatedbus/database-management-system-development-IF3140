import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from misc.optimizer import *
from misc.storage import Rows, Condition
from query_optimizer import *
from query_processor import QueryProcessor

"""
TODO List:
- UNARY OPERATORS
    - PROJECT (SELECT columns) -> DONE
    - FILTER (WHERE condition) -> DONE (naive)
    - SORT (ORDER BY columns) -> DONE (naive)
- BINARY OPERATORS
    - JOIN (JOIN relations) -> TODO: implementasi JOIN, diskusikan dengan storage manager
- LEAF NODES
    - RELATION (Base table/relation) -> DONE (test only), implementasi interaksi dengan storage manager
    - LIMIT (LIMIT value) -> TODO: implementasi LIMIT, gunakan variabel global
- SPECIAL OPERATORS
    - UPDATE: TODO: implementasi UPDATE, perbaiki SET. Mungkin coba menggunakan AST dari parser
- INSIGHTS:
    - Rows sebaiknya diberikan fungsionalitas tambahan untuk memudahkan manipulasi data
    - Parser harus memisahkan kondisi string dan numerik
    - Diskusikan dengan query optimizer apakah penggabungan kondisi AND/OR dan juga penggabungan expressions di SET pada UPDATE dapat mempengaruhi proses optimisasi
"""
def execute_node(query_tree: QueryTree) -> Rows | None:
    type = query_tree.type
    results = {}

    for child in query_tree.childs:
        child_result = execute_node(child)
        results[child] = child_result

    if type in UNARY_OPERATORS:
        # TODO: implementasi eksekusi operator unary
        return execute_unary_node(query_tree)

    elif type in BINARY_OPERATORS:
        # TODO: implementasi eksekusi JOIN, diskusikan dengan storage manager
        return None

    elif type in LEAF_NODES:
        return execute_leaf_node(query_tree)

    elif type in SPECIAL_OPERATORS:
        print(f"Executing special operator: <{type}>")
        # TODO: implementasi eksekusi operator khusus
        return execute_special_node(query_tree)

    else:
        raise ValueError(f"Tipe node tidak dikenali: <{type}>")
    
def execute_unary_node(query_tree: QueryTree):
    if len(query_tree.childs) != 1:
        raise ValueError(f"Unary operator <{query_tree.type}> harus punya tepat 1 anak")
    
    type = query_tree.type
    value = query_tree.val
    child_result: Rows | None = execute_node(query_tree.childs[0])
    processed_result: list[dict] = []
    if type == "PROJECT":
        if value == "*":
            return child_result 
        columns = value.split(",") if value else []
        for row in child_result.rows:
            projected_row = {col: row[col] for col in columns if col in row}
            processed_result.append(projected_row)

        # TESTING: print hasil projection
        print(f"Executing PROJECT on columns: {columns}")
        print(f"Projected Result: {processed_result}")
        return Rows(processed_result)
    
    elif type == "FILTER":
        # TODO: handle AND/OR conditions properly, for now just single condition
        # TODO: handle same column name different relations, determine ambiguity
        naive_filtering = []
        condition_string = value
        condition_split = value.split()
        if len(condition_split) != 4:
            raise ValueError("Not supported complex conditions yet!")
        # condition = Condition(condition_split[0], condition_split[1], condition_split[2])
        # if query_tree.childs[0].type == "RELATION":
        #     # TODO: SEND TO STORAGE MANAGER TO FILTER
        #     print(f"Executing FILTER on condition: {condition_string}, condition parsed: {condition}")
        #     return child_result  # placeholder

        _, col, op, val = condition_split

        # TODO: handle different data types properly, differentiate string and numeric
        if val.isdigit():
            val = int(val)

        for row in child_result.rows:
            if op == "=" and row.get(col) == val:
                naive_filtering.append(row)
            elif op == "!=" and row.get(col) != val:
                naive_filtering.append(row)
            elif op == "<" and row.get(col) < val:
                naive_filtering.append(row)
            elif op == "<=" and row.get(col) <= val:
                naive_filtering.append(row)
            elif op == ">" and row.get(col) > val:
                naive_filtering.append(row)
            elif op == ">=" and row.get(col) >= val:
                naive_filtering.append(row)

        print(f"Executing FILTER on condition: {condition_string}, result filtered")
        print(f"Filtered Result: {naive_filtering}")
        return Rows(naive_filtering)
    
    elif type == "SORT":
        columns = value.split(",") if value else []
        # TODO: naive sorting for now, implement proper sorting logic later
        sorted_result = sorted(child_result, key=lambda row: tuple(row[col] for col in columns if col in row))
        print(f"Executing SORT on columns: {columns}, result sorted")
        print(f"Sorted Result: {sorted_result}")
        return Rows(sorted_result)
    
def execute_leaf_node(query_tree: QueryTree) -> Rows | None:
    type = query_tree.type
    if type == "RELATION":
        table_name = query_tree.val
        # TODO: interact with storage manager to get table data. TEST FOR NOW!

        test_rows = Rows([{"id": 1, "name": "mifune"}, {"id": 2, "name": "alice"}, {"id": 3, "name": "bob"}])
        print(f"Executing RELATION node for table: {table_name}")
        print(f"Retrieved Rows: {test_rows}")
        return test_rows
    elif type == "LIMIT":
        # TODO: implement LIMIT logic, use global variable
        limit_value = int(query_tree.val)
        print(f"Executing LIMIT node with value: {limit_value}")
        return None
    
def execute_special_node(query_tree: QueryTree) -> None:
    type = query_tree.type
    value = query_tree.val
    if type == "UPDATE":
        # TODO: implement UPDATE logic, mungkin gunakan AST dari parser
        print(f"Executing UPDATE with value: {value}")
    elif type == "INSERT":
        relation: Rows = execute_node(query_tree.childs[0]) if query_tree.childs else None
        if not relation:
            raise ValueError("INSERT node harus punya child RELATION yang menyatakan tabel tujuan")
        
        upcoming_row = {}
        insert_values = value.split(",") if value else []
        print(f"Executing INSERT with values: {insert_values}")
        for val in insert_values:
            column, _, value = val.strip().partition("=")
            column = column.strip()
            value = value.strip()
            upcoming_row[column] = value

        first_element = relation.rows[0] if relation.rows else {}
        if set(upcoming_row.keys()) != set(first_element.keys()):
            print(upcoming_row.keys(), first_element.keys())
            raise ValueError("Kolom pada INSERT tidak sesuai dengan kolom pada tabel tujuan")
        print(f"Inserting Row: {upcoming_row} into Relation")
        # TODO: interact with storage manager to insert the row
        return upcoming_row

    elif type == "DELETE":
        # TODO: implement DELETE yang berinteraksi dengan storage manager
        rows_to_delete = execute_node(query_tree.childs[0]) if query_tree.childs else None
        print(f"Rows to delete: {rows_to_delete}")
        print(f"Executing DELETE operation (simulate delete)")
        return None
    
    elif type == "BEGIN_TRANSACTION":
        # TODO: implement transaction handling
        print("Executing BEGIN_TRANSACTION")
    elif type == "COMMIT":
        # TODO: implement commit handling
        print("Executing COMMIT")
    return None
    
def main():
    print("Hello from execute_node module")
    processor = QueryProcessor()
    # tree = processor._get_query_tree("SELECT * FROM users WHERE name='mifune';")
    # tree = processor._get_query_tree("INSERT INTO users (id, name) VALUES (100, 'didd');")
    tree = processor._get_query_tree("DELETE FROM users WHERE id=2;")
    result = execute_node(tree)
    print(f"Final Result: {result}")

if __name__ == "__main__":
    main()