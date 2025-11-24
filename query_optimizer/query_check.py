from __future__ import annotations
from query_optimizer.query_tree import QueryTree

class QueryValidationError(Exception):
    """Exception raised when query tree validation fails"""
    pass

# Dummy function
def get_statistic():
    """
    Dummy function yang mengembalikan informasi tentang database.
    Tolong returns dict dengan:
    - tables: list nama tabel yang ada
    - columns: dict mapping table_name -> list of column names
    """
    return {
        "tables": ["users", "profiles", "orders", "products", "employees", "accounts", "logs", "payroll"],
        "columns": {
            "users": ["id", "name", "email"],
            "profiles": ["id", "user_id", "bio"],
            "orders": ["id", "user_id", "total"],
            "products": ["id", "category", "price", "stock", "description", "discount"],
            "employees": ["id", "name", "salary", "department", "bonus"],
            "accounts": ["id", "balance"],
            "logs": ["id", "message"],
            "payroll": ["salary"]
        }
    }

ATOMIC_NODES = {
    "IDENTIFIER",
    "LITERAL_NUMBER",
    "LITERAL_STRING",
    "LITERAL_BOOLEAN",
    "LITERAL_NULL",
}

WRAPPER_NODES = {
    "COLUMN_NAME",
    "TABLE_NAME",
}

SOURCE_NODES = {
    "RELATION",
    "ALIAS",
    "PROJECT",
    "FILTER",
    "JOIN",
    "SORT",
    "LIMIT",
}

CONDITION_NODES = {
    "OPERATOR",
    "COMPARISON",
    "IN_EXPR",
    "NOT_IN_EXPR",
    "EXISTS_EXPR",
    "NOT_EXISTS_EXPR",
    "BETWEEN_EXPR",
    "NOT_BETWEEN_EXPR",
    "IS_NULL_EXPR",
    "IS_NOT_NULL_EXPR",
}

DML_NODES = {
    "UPDATE_QUERY",
    "INSERT_QUERY", 
    "DELETE_QUERY",
    "ASSIGNMENT",
}

DDL_NODES = {
    "CREATE_TABLE",
    "DROP_TABLE",
    "COLUMN_DEF",
    "DATA_TYPE",
    "PRIMARY_KEY",
    "FOREIGN_KEY",
    "REFERENCES",
}

TRANSACTION_NODES = {
    "BEGIN_TRANSACTION",
    "COMMIT",
}

UTILITY_NODES = {
    "COLUMN_LIST",
    "VALUES_CLAUSE",
    "COLUMN_DEF_LIST",
}

def check_query(node: QueryTree) -> None:
    """
    1. Semua child harus valid (rekursif)
    2. Validasi struktur berdasarkan node type
    3. Validasi value sesuai konteks
    """
    
    for child in node.childs:
        check_query(child)
    
    num_children = len(node.childs)
    
    if node.type in ATOMIC_NODES:
        if num_children != 0:
            raise QueryValidationError(f"Atomic node <{node.type}> tidak boleh punya child, dapat {num_children}")
    
    elif node.type in WRAPPER_NODES:
        if num_children != 1:
            raise QueryValidationError(f"Wrapper node <{node.type}> harus punya 1 child, dapat {num_children}")

        if node.childs[0].type != "IDENTIFIER":
            raise QueryValidationError(f"<{node.type}> child harus IDENTIFIER, dapat {node.childs[0].type}")
    
    elif node.type == "COLUMN_REF":
        if num_children < 1 or num_children > 2:
            raise QueryValidationError(f"<COLUMN_REF> harus punya 1-2 children, dapat {num_children}")
        # Child 0 must be COLUMN_NAME
        if node.childs[0].type != "COLUMN_NAME":
            raise QueryValidationError(f"<COLUMN_REF> child 0 harus COLUMN_NAME, dapat {node.childs[0].type}")
        # Child 1 (optional) must be TABLE_NAME
        if num_children == 2 and node.childs[1].type != "TABLE_NAME":
            raise QueryValidationError(f"<COLUMN_REF> child 1 harus TABLE_NAME, dapat {node.childs[1].type}")
    
    elif node.type in SOURCE_NODES:
        if node.type == "RELATION":
            # RELATION is a leaf node (0 children)
            if num_children != 0:
                raise QueryValidationError(f"<RELATION> tidak boleh punya child, dapat {num_children}")
        
        elif node.type == "ALIAS":
            # ALIAS has 1 child (what is being aliased)
            if num_children != 1:
                raise QueryValidationError(f"<ALIAS> harus punya 1 child, dapat {num_children}")
        
        elif node.type == "PROJECT":
            # PROJECT has 1+ children (last child is source, others are COLUMN_REF or value="*")
            if num_children < 1:
                raise QueryValidationError(f"<PROJECT> harus punya minimal 1 child (source), dapat {num_children}")
        
        elif node.type == "FILTER":
            # FILTER has 2 children (source + condition)
            if num_children != 2:
                raise QueryValidationError(f"<FILTER> harus punya 2 children (source + condition), dapat {num_children}")
        
        elif node.type == "JOIN":
            # JOIN has 2-3 children (rel1, rel2, optional condition for INNER JOIN)
            if num_children < 2 or num_children > 3:
                raise QueryValidationError(f"<JOIN> harus punya 2-3 children, dapat {num_children}")
        
        elif node.type == "SORT":
            # SORT has 2 children (order_expr, source)
            # order_expr bisa berupa COLUMN_REF atau expression lain
            if num_children != 2:
                raise QueryValidationError(f"<SORT> harus punya 2 children (order_expr + source), dapat {num_children}")
            # Child 0 harus expression (COLUMN_REF, ARITH_EXPR, dll)
            if node.childs[0].type not in {"COLUMN_REF", "ARITH_EXPR", "FUNCTION_CALL", "LITERAL_NUMBER", "LITERAL_STRING"}:
                raise QueryValidationError(f"<SORT> child 0 harus expression yang valid, dapat {node.childs[0].type}")
        
        elif node.type == "LIMIT":
            # LIMIT has 1 child (source)
            if num_children != 1:
                raise QueryValidationError(f"<LIMIT> harus punya 1 child (source), dapat {num_children}")
    
    elif node.type in CONDITION_NODES:
        if node.type == "OPERATOR":
            # AND/OR: 2+ children, NOT: 1 child
            if num_children < 1:
                raise QueryValidationError(f"<OPERATOR> minimal 1 child, dapat {num_children}")
        
        elif node.type == "COMPARISON":
            # COMPARISON has 2 children (left, right expressions)
            if num_children != 2:
                raise QueryValidationError(f"<COMPARISON> harus punya 2 children, dapat {num_children}")
        
        elif node.type in {"IN_EXPR", "NOT_IN_EXPR"}:
            # IN_EXPR has 2 children (column_ref + LIST or subquery)
            if num_children != 2:
                raise QueryValidationError(f"<{node.type}> harus punya 2 children, dapat {num_children}")
        
        elif node.type in {"EXISTS_EXPR", "NOT_EXISTS_EXPR"}:
            # EXISTS has 1 child (subquery)
            if num_children != 1:
                raise QueryValidationError(f"<{node.type}> harus punya 1 child (subquery), dapat {num_children}")
        
        elif node.type in {"BETWEEN_EXPR", "NOT_BETWEEN_EXPR"}:
            # BETWEEN has 3 children (value, lower, upper)
            if num_children != 3:
                raise QueryValidationError(f"<{node.type}> harus punya 3 children, dapat {num_children}")
        
        elif node.type in {"IS_NULL_EXPR", "IS_NOT_NULL_EXPR"}:
            # IS NULL has 1 child (column_ref)
            if num_children != 1:
                raise QueryValidationError(f"<{node.type}> harus punya 1 child, dapat {num_children}")
    
    elif node.type == "ARITH_EXPR":
        # ARITH_EXPR has 2 children (left, right)
        if num_children != 2:
            raise QueryValidationError(f"<ARITH_EXPR> harus punya 2 children, dapat {num_children}")
    
    elif node.type == "LIST":
        # LIST can have 0+ children (list items)
        pass
    
    elif node.type in DML_NODES:
        if node.type == "UPDATE_QUERY":
            # UPDATE has 2-3 children (relation, assignment(s), optional filter)
            if num_children < 2:
                raise QueryValidationError(f"<UPDATE_QUERY> minimal 2 children, dapat {num_children}")
        
        elif node.type == "INSERT_QUERY":
            # INSERT has 3 children (relation, column_list, values_clause)
            if num_children != 3:
                raise QueryValidationError(f"<INSERT_QUERY> harus punya 3 children, dapat {num_children}")
        
        elif node.type == "DELETE_QUERY":
            # DELETE has 1-2 children (relation, optional filter)
            if num_children < 1 or num_children > 2:
                raise QueryValidationError(f"<DELETE_QUERY> harus punya 1-2 children, dapat {num_children}")
        
        elif node.type == "ASSIGNMENT":
            # ASSIGNMENT has 2 children (column_ref, value_expr)
            if num_children != 2:
                raise QueryValidationError(f"<ASSIGNMENT> harus punya 2 children, dapat {num_children}")
    
    elif node.type in DDL_NODES:
        # DDL validation can be added here if needed
        pass
    
    elif node.type in TRANSACTION_NODES:
        # BEGIN_TRANSACTION can have 0+ children (statements)
        # COMMIT has 0 children
        pass
    
    elif node.type in UTILITY_NODES:
        # COLUMN_LIST, VALUES_CLAUSE, COLUMN_DEF_LIST can have 0+ children
        pass
    
    # Validasi value
    check_value(node)

def check_value(node: QueryTree) -> None:
    stats = get_statistic()
    
    if node.type == "IDENTIFIER":
        if not node.val:
            raise QueryValidationError("<IDENTIFIER> harus punya value")
    
    if node.type in ATOMIC_NODES:
        if node.type == "LITERAL_NULL":
            pass
        else:
            # Literals lain harus punya value
            if not node.val and node.val != 0 and node.val != False:
                raise QueryValidationError(f"<{node.type}> harus punya value")
    
    if node.type == "RELATION":
        if not node.val:
            raise QueryValidationError("<RELATION> harus punya nama tabel")
        if node.val not in stats["tables"]:
            raise QueryValidationError(f"Tabel '{node.val}' tidak ditemukan. Tersedia: {stats['tables']}")
    
    if node.type == "ALIAS":
        if not node.val:
            raise QueryValidationError("<ALIAS> harus punya alias name")
    
    if node.type == "JOIN":
        if not node.val:
            raise QueryValidationError("<JOIN> harus punya join type (INNER/NATURAL)")
        if node.val not in {"INNER", "NATURAL"}:
            raise QueryValidationError(f"<JOIN> value harus 'INNER' atau 'NATURAL', dapat '{node.val}'")
        
        num_children = len(node.childs)
        if node.val == "NATURAL" and num_children != 2:
            raise QueryValidationError(f"NATURAL JOIN harus punya 2 children, dapat {num_children}")
        elif node.val == "INNER" and num_children != 3:
            raise QueryValidationError(f"INNER JOIN harus punya 3 children (2 relations + condition), dapat {num_children}")
    
    if node.type == "COMPARISON":
        if not node.val:
            raise QueryValidationError("<COMPARISON> harus punya operator")
        valid_ops = {"=", "<>", "!=", ">", ">=", "<", "<="}
        if node.val not in valid_ops:
            raise QueryValidationError(f"<COMPARISON> operator tidak valid: '{node.val}'. Valid: {valid_ops}")
    
    if node.type == "ARITH_EXPR":
        if not node.val:
            raise QueryValidationError("<ARITH_EXPR> harus punya operator")
        valid_ops = {"+", "-", "*", "/", "%"}
        if node.val not in valid_ops:
            raise QueryValidationError(f"<ARITH_EXPR> operator tidak valid: '{node.val}'. Valid: {valid_ops}")
    
    if node.type == "OPERATOR":
        if not node.val:
            raise QueryValidationError("<OPERATOR> harus punya value (AND/OR/NOT)")
        
        operator_type = node.val.strip()
        if operator_type not in {"AND", "OR", "NOT"}:
            raise QueryValidationError(f"<OPERATOR> value harus 'AND', 'OR', atau 'NOT', dapat '{operator_type}'")
        
        num_children = len(node.childs)
        if operator_type == "NOT" and num_children != 1:
            raise QueryValidationError(f"<OPERATOR NOT> harus punya 1 child, dapat {num_children}")
        elif operator_type in {"AND", "OR"} and num_children < 2:
            raise QueryValidationError(f"<OPERATOR {operator_type}> minimal 2 children, dapat {num_children}")
    
    if node.type == "SORT":
        if node.val and node.val not in {"ASC", "DESC"}:
            raise QueryValidationError(f"<SORT> direction harus 'ASC' atau 'DESC', dapat '{node.val}'")
    
    if node.type == "PROJECT":
        # If value is "*", seharusnya tidak ada COLUMN_REF children (hanya source)
        if node.val == "*" and len(node.childs) > 1:
            raise QueryValidationError("<PROJECT> dengan value='*' hanya boleh punya 1 child (source)")
