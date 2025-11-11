from __future__ import annotations

class QueryValidationError(Exception):
    """Exception raised when query tree validation fails"""
    pass

# Dummy QueryTree class for context
class QueryTree:
    def __init__(self, type: str, val: str = "", parent: QueryTree | None = None):
        self.type: str = type
        self.val: str = val
        self.childs: list[QueryTree] = []
        self.parent: QueryTree | None = parent
    def add_child(self, child_node: QueryTree):
        child_node.parent = self
        self.childs.append(child_node)
    def __repr__(self) -> str:
        return f"QueryTree(type='{self.type}', val='{self.val}')"

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
            if num_children < 1:
                raise QueryValidationError(f"<{node.type}> butuh minimum 1 anak, dapat {num_children}")
            elif num_children == 2:
                # Anak pertama: continuation tree (bisa PROJECT, JOIN, FILTER, dll)
                # Anak kedua: value (harus ARRAY atau RELATION untuk subquery)
                second_child_type = node.childs[1].type
                if second_child_type not in {"ARRAY", "RELATION", "PROJECT"}:
                    raise QueryValidationError(f"<{node.type}> dengan 2 anak, anak kedua harus ARRAY/RELATION/PROJECT untuk value, dapat <{second_child_type}>")
            elif num_children > 2:
                raise QueryValidationError(f"<{node.type}> maksimal 2 anak (tree, value), dapat {num_children}")

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
        if node.val:
            filter_parts = node.val.split(maxsplit=1)
            filter_type = filter_parts[0]
            
            if len(filter_parts) == 1:
                # Single word: EXIST (untuk subquery check)
                if filter_type not in {"EXIST"}:
                    raise QueryValidationError(f"FILTER dengan 1 kata harus 'EXIST', dapat '{filter_type}'")
            elif len(filter_parts) >= 2:
                # Multiple words: WHERE condition atau IN column
                if filter_type not in {"WHERE", "IN"}:
                    raise QueryValidationError(f"FILTER dengan kondisi harus diawali 'WHERE' atau 'IN', dapat '{filter_type}'")


    # Validasi RELATION
    if node.type == "RELATION":
        if not node.val:
            raise QueryValidationError(f"<{node.type}> harus punya nama tabel")
        if node.val not in stats["tables"]:
            raise QueryValidationError(f"Tabel '{node.val}' tidak ditemukan. Tersedia: {stats['tables']}")