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

SPECIAL_OPERATORS = {
    "UPDATE",     # DML operation
    "INSERT",     # DML operation
    "DELETE",     # DML operation
    "BEGIN_TRANSACTION",  # Transaction start
    "COMMIT",             # Transaction commit
    "FILTER"        # σ - selection (WHERE value IN RELATION, WHERE condition, WHERE EXIST RELATION)
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
            # Validasi lebih detail ada di check_value
            if num_children == 2:
                # Anak pertama: continuation tree (bisa PROJECT, JOIN, FILTER, dll)
                # Anak kedua: value (harus ARRAY atau RELATION untuk subquery)
                second_child_type = node.childs[1].type
                if second_child_type not in {"ARRAY", "RELATION", "PROJECT"}:
                    raise QueryValidationError(f"<{node.type}> dengan 2 anak, anak kedua harus ARRAY/RELATION/PROJECT untuk value, dapat <{second_child_type}>")
            elif num_children > 2:
                # Multiple children valid untuk AND/OR/EXIST dengan source
                pass

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
    
    # Validasi FILTER
    if node.type == "FILTER":
        num_children = len(node.childs)
        if node.val:
            filter_parts = node.val.split(maxsplit=1)
            filter_type = filter_parts[0]
            
            if len(filter_parts) == 1:
                # Single word: EXIST, AND, OR, or NOT
                if filter_type not in {"EXIST", "AND", "OR", "NOT"}:
                    raise QueryValidationError(f"FILTER dengan 1 kata harus 'EXIST', 'AND', 'OR', atau 'NOT', dapat '{filter_type}'")
                
                if filter_type in {"AND", "OR"}:
                    if num_children < 2:
                        raise QueryValidationError(f"FILTER '{filter_type}' butuh minimum 2 children")
                    
                    if node.childs[0].type != "FILTER":
                        if num_children < 3:
                            raise QueryValidationError(
                                f"FILTER '{filter_type}' dengan source butuh >= 3 children (1 source + 2+ conditions)"
                            )
                        for i in range(1, num_children):
                            if node.childs[i].type != "FILTER":
                                raise QueryValidationError(
                                    f"FILTER '{filter_type}' child {i} harus FILTER, dapat {node.childs[i].type}"
                                )
                    else:
                        for i in range(num_children):
                            if node.childs[i].type != "FILTER":
                                raise QueryValidationError(
                                    f"FILTER '{filter_type}' nested child {i} harus FILTER, dapat {node.childs[i].type}"
                                )
                
                elif filter_type == "NOT":
                    if num_children != 1:
                        raise QueryValidationError(
                            f"FILTER 'NOT' harus punya tepat 1 child, dapat {num_children}"
                        )
                    if node.childs[0].type != "FILTER":
                        raise QueryValidationError(
                            f"FILTER 'NOT' child harus FILTER, dapat {node.childs[0].type}"
                        )
            
            elif len(filter_parts) >= 2:
                if filter_type not in {"WHERE", "IN"}:
                    raise QueryValidationError(f"FILTER dengan kondisi harus diawali 'WHERE' atau 'IN', dapat '{filter_type}'")
                
                # WHERE/IN bisa jadi:
                # 1. Condition leaf (0 children) - digunakan dalam AND/OR
                # 2. Filter operator (1 child) - aplikasi filter ke source
                # 3. Filter dengan value (2 children) - IN dengan array/subquery
                if num_children > 2:
                    raise QueryValidationError(f"FILTER '{filter_type}' maksimal 2 children, dapat {num_children}")


    # Validasi RELATION
    if node.type == "RELATION":
        if not node.val:
            raise QueryValidationError(f"<{node.type}> harus punya nama tabel")
        if node.val not in stats["tables"]:
            raise QueryValidationError(f"Tabel '{node.val}' tidak ditemukan. Tersedia: {stats['tables']}")