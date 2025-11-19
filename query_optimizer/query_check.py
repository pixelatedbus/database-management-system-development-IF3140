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
        "tables": ["users", "profiles", "orders"],
        "columns": {
            "users": ["id", "name", "email"],
            "profiles": ["id", "user_id", "bio"],
            "orders": ["id", "user_id", "total"]
        }
    }

# Operator types:
# UNARY operators (1 child)
# BINARY operators (2 children)
# LEAF nodes (0 children)
# SPECIAL: UPDATE, INSERT, DELETE, BEGIN_TRANSACTION, COMMIT

UNARY_OPERATORS = {
    "PROJECT",    # π - projection (SELECT columns)
    "SORT",       # ORDER BY
}

BINARY_OPERATORS = {
    "JOIN",       # ⋈ - join (butuh 2 relasi)
}

LEAF_NODES = {
    "RELATION",   # Base table/relation
    "ARRAY",      # Array value
    "LIMIT",      # LIMIT value
}

LOGICAL_OPERATORS = {
    "OPERATOR",    # Logical operator nested (AND/OR/NOT tanpa explicit source)
    "OPERATOR_S",  # Logical operator dengan explicit source tree
}

SPECIAL_OPERATORS = {
    "UPDATE",     # DML operation
    "INSERT",     # DML operation
    "DELETE",     # DML operation
    "BEGIN_TRANSACTION",  # Transaction start
    "COMMIT",             # Transaction commit
    "FILTER",       # σ - selection (WHERE condition, IN value, EXIST)
    "OPERATOR",     # Logical operators nested (AND/OR/NOT)
    "OPERATOR_S",   # Logical operators dengan source (AND_S/OR_S)
}

VALID_JOIN_TYPES = {"NATURAL", "ON"}

def check_query(node: QueryTree) -> None:
    """
    Validasi query tree berdasarkan operator relational algebra:
    1. Semua child harus valid
    2. Cek jumlah children sesuai tipe operator: UNARY, BINARY, LEAF
    3. Special operators (UPDATE, INSERT, DELETE, transactions) punya aturan sendiri
    4. Value node harus valid sesuai konteks
    """
    
    # Rekursif cek semua child
    for child in node.childs:
        check_query(child)
    
    # Validasi jumlah children berdasarkan tipe operator
    num_children = len(node.childs)
    
    if node.type in UNARY_OPERATORS:
        if num_children != 1:
            raise QueryValidationError(f"Operator unary <{node.type}> butuh 1 anak, dapat {num_children}")
    
    elif node.type in BINARY_OPERATORS:
        if num_children != 2:
            raise QueryValidationError(f"Operator binary <{node.type}> butuh 2 anak, dapat {num_children}")
    
    elif node.type in LEAF_NODES:
        if num_children != 0:
            raise QueryValidationError(f"Leaf node <{node.type}> tidak boleh punya anak, dapat {num_children}")
    
    elif node.type in SPECIAL_OPERATORS:
        # UPDATE/DELETE/INSERT butuh 1 child (relation)
        # BEGIN_TRANSACTION dan COMMIT bisa punya 0 atau lebih children (statements dalam transaction)
        if node.type in {"UPDATE", "DELETE", "INSERT"}:
            if num_children != 1:
                raise QueryValidationError(f"<{node.type}> butuh 1 anak (relation), dapat {num_children}")
        
        if node.type == "FILTER":
            # FILTER bisa jadi condition leaf (0 children) atau operator node (>= 1 children)
            pass
        
        if node.type == "OPERATOR":
            # OPERATOR: Logical operators nested (AND/OR/NOT) tanpa explicit source
            # - AND/OR: minimum 2 children (semua harus FILTER atau OPERATOR)
            # - NOT: exactly 1 child (harus FILTER atau OPERATOR)
            if num_children < 1:
                raise QueryValidationError(f"<OPERATOR> minimal 1 child, dapat {num_children}")
        
        if node.type == "OPERATOR_S":
            # OPERATOR_S: Logical operators dengan explicit source tree
            # - Child 0: source tree yang menghasilkan data
            #   * RELATION, JOIN, SORT, PROJECT, dll (operators yang produce data)
            #   * OPERATOR_S (logical operator dengan source, pasti produce data)
            #   * FILTER dengan >= 1 children (filter dengan source, produce data)
            #   * TIDAK boleh: OPERATOR (nested logic, tidak produce data)
            #   * TIDAK boleh: FILTER leaf/0 children (condition saja, tidak produce data)
            # - Child 1 dan seterusnya: conditions (FILTER atau OPERATOR)
            # - Minimum 3 children (1 source + 2 conditions)
            if num_children < 3:
                raise QueryValidationError(f"<OPERATOR_S> minimal 3 children (1 source + 2 conditions), dapat {num_children}")
            
            # Validate first child
            first_child = node.childs[0]
            first_child_type = first_child.type
            
            if first_child_type == "OPERATOR":
                raise QueryValidationError(
                    f"<OPERATOR_S> child pertama tidak boleh OPERATOR (nested logic tanpa source). Gunakan OPERATOR_S sebagai source."
                )
            
            if first_child_type == "FILTER" and len(first_child.childs) == 0:
                raise QueryValidationError(
                    f"<OPERATOR_S> child pertama FILTER harus punya children (filter dengan source) untuk produce data. FILTER leaf (0 children) tidak produce data."
                )
            
            # Validate remaining children
            for i in range(1, num_children):
                child_type = node.childs[i].type
                if child_type not in {"FILTER", "OPERATOR", "OPERATOR_S"}:
                    raise QueryValidationError(
                        f"<OPERATOR_S> child {i} harus FILTER/OPERATOR/OPERATOR_S, dapat {child_type}"
                    )

        # BEGIN_TRANSACTION dan COMMIT tidak ada batasan jumlah children
    
    # Validasi value
    check_value(node)

def check_value(node: QueryTree) -> None:
    stats = get_statistic()
    
    # Validasi JOIN
    if node.type == "JOIN":
        if node.val:
            join_parts = node.val.split(maxsplit=1)
            join_type = join_parts[0]
            
            if len(join_parts) == 1:
                if join_type != "NATURAL":
                    raise QueryValidationError(f"JOIN dengan 1 kata harus 'NATURAL', dapat '{join_type}'")
            elif len(join_parts) >= 2:
                if join_type != "ON":
                    raise QueryValidationError(f"JOIN dengan kondisi harus diawali 'ON', dapat '{join_type}'")
    
    # Validasi FILTER (WHERE/IN/EXIST conditions only)
    if node.type == "FILTER":
        num_children = len(node.childs)
        if node.val:
            filter_parts = node.val.split(maxsplit=1)
            filter_type = filter_parts[0]
            
            if len(filter_parts) == 1:
                # Single word: EXIST only (AND/OR/NOT dipindah ke OPERATOR)
                if filter_type != "EXIST":
                    raise QueryValidationError(f"FILTER dengan 1 kata harus 'EXIST', dapat '{filter_type}'. Gunakan OPERATOR untuk AND/OR/NOT.")
            
            elif len(filter_parts) >= 2:
                if filter_type not in {"WHERE", "IN"}:
                    raise QueryValidationError(f"FILTER dengan kondisi harus diawali 'WHERE' atau 'IN', dapat '{filter_type}'")
                
                # WHERE/IN bisa jadi:
                # 1. Condition leaf (0 children) - digunakan dalam OPERATOR
                # 2. Filter operator (1 child) - aplikasi filter ke source
                # 3. Filter dengan value (2 children) - IN dengan array/subquery
                if num_children == 2:
                    # Validasi pattern IN/WHERE dengan value
                    second_child_type = node.childs[1].type
                    if second_child_type not in {"ARRAY", "RELATION", "PROJECT"}:
                        raise QueryValidationError(
                            f"FILTER '{filter_type}' dengan 2 children, child kedua harus ARRAY/RELATION/PROJECT untuk value, dapat {second_child_type}"
                        )
                elif num_children > 2:
                    raise QueryValidationError(f"FILTER '{filter_type}' maksimal 2 children, dapat {num_children}")
    
    # Validasi OPERATOR
    if node.type == "OPERATOR":
        num_children = len(node.childs)
        if not node.val:
            raise QueryValidationError("<OPERATOR> harus punya value (AND/OR/NOT)")
        
        operator_type = node.val.strip()
        
        if operator_type not in {"AND", "OR", "NOT"}:
            raise QueryValidationError(f"<OPERATOR> value harus 'AND', 'OR', atau 'NOT', dapat '{operator_type}'")
        
        if operator_type == "NOT":
            # NOT: exactly 1 child (FILTER atau OPERATOR)
            if num_children != 1:
                raise QueryValidationError(f"<OPERATOR> 'NOT' harus punya 1 child, dapat {num_children}")
            
            child_type = node.childs[0].type
            if child_type not in {"FILTER", "OPERATOR", "OPERATOR_S"}:
                raise QueryValidationError(
                    f"<OPERATOR> 'NOT' child harus FILTER/OPERATOR/OPERATOR_S, dapat {child_type}"
                )
        
        elif operator_type in {"AND", "OR"}:
            # AND/OR: minimum 2 children (semua harus FILTER atau OPERATOR)
            if num_children < 2:
                raise QueryValidationError(f"<OPERATOR> '{operator_type}' minimal 2 children, dapat {num_children}")
            
            for i in range(num_children):
                child_type = node.childs[i].type
                if child_type not in {"FILTER", "OPERATOR", "OPERATOR_S"}:
                    raise QueryValidationError(
                        f"<OPERATOR> '{operator_type}' child {i} harus FILTER/OPERATOR/OPERATOR_S, dapat {child_type}"
                    )
    
    # Validasi OPERATOR_S (Logical operators dengan source)
    if node.type == "OPERATOR_S":
        num_children = len(node.childs)
        if not node.val:
            raise QueryValidationError("<OPERATOR_S> harus punya value (AND/OR)")
        
        operator_type = node.val.strip()
        
        if operator_type not in {"AND", "OR"}:
            raise QueryValidationError(f"<OPERATOR_S> value harus 'AND' atau 'OR', dapat '{operator_type}' (NOT tidak bisa punya explicit source)")


    # Validasi RELATION
    if node.type == "RELATION":
        if not node.val:
            raise QueryValidationError(f"<{node.type}> harus punya nama tabel")
        if node.val not in stats["tables"]:
            raise QueryValidationError(f"Tabel '{node.val}' tidak ditemukan. Tersedia: {stats['tables']}")