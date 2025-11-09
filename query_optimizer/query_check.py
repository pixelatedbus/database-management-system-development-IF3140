from __future__ import annotations

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
    "FILTER",     # σ - selection (WHERE condition)
    "SORT",       # ORDER BY
}

BINARY_OPERATORS = {
    "JOIN",       # ⋈ - join (butuh 2 relasi)
}

LEAF_NODES = {
    "RELATION",   # Base table/relation
    "LIMIT",      # LIMIT value
}

SPECIAL_OPERATORS = {
    "UPDATE",     # DML operation
    "INSERT",     # DML operation
    "DELETE",     # DML operation
    "BEGIN_TRANSACTION",  # Transaction start
    "COMMIT",             # Transaction commit
}

VALID_JOIN_TYPES = {"NATURAL", "ON"}

def check_query(node: QueryTree) -> bool:
    """
    Validasi query tree berdasarkan operator relational algebra:
    1. Semua child harus valid
    2. Cek jumlah children sesuai tipe operator: UNARY, BINARY, LEAF
    3. Special operators (UPDATE, INSERT, DELETE, transactions) punya aturan sendiri
    4. Value node harus valid sesuai konteks
    """
    
    # Rekursif cek semua child
    if not all(check_query(child) for child in node.childs):
        return False
    
    # Validasi jumlah children berdasarkan tipe operator
    num_children = len(node.childs)
    
    if node.type in UNARY_OPERATORS:
        if num_children != 1:
            print(f"-> Validasi GAGAL di <{node.type}>: Operator unary butuh 1 anak, dapat {num_children}")
            return False
    
    elif node.type in BINARY_OPERATORS:
        if num_children != 2:
            print(f"-> Validasi GAGAL di <{node.type}>: Operator binary butuh 2 anak, dapat {num_children}")
            return False
    
    elif node.type in LEAF_NODES:
        if num_children != 0:
            print(f"-> Validasi GAGAL di <{node.type}>: Leaf node tidak boleh punya anak, dapat {num_children}")
            return False
    
    elif node.type in SPECIAL_OPERATORS:
        # UPDATE/DELETE/INSERT butuh 1 child (relation)
        # BEGIN_TRANSACTION dan COMMIT bisa punya 0 atau lebih children (statements dalam transaction)
        if node.type in {"UPDATE", "DELETE", "INSERT"}:
            if num_children != 1:
                print(f"-> Validasi GAGAL di <{node.type}>: Butuh 1 anak (relation), dapat {num_children}")
                return False
        # BEGIN_TRANSACTION dan COMMIT tidak ada batasan jumlah children
    
    if not check_value(node):
        return False

    return True

def check_value(node: QueryTree) -> bool:
    stats = get_statistic()
    
    # Validasi JOIN
    if node.type == "JOIN":
        if node.val:
            join_parts = node.val.split(maxsplit=1)
            join_type = join_parts[0]
            if join_parts.__len__() == 1:
                return join_type == "NATURAL"
            if join_parts.__len__() >= 2:
                return join_type == "ON"
            else:
                print(f"-> Validasi GAGAL di <{node.type}>: jumlah value > 2")
                return False
            
    # Validasi RELATION
    if node.type == "RELATION":
        if not node.val:
            print(f"-> Validasi GAGAL di <{node.type}>: Relasi harus punya nama tabel")
            return False
        if node.val not in stats["tables"]:
            print(f"-> Validasi GAGAL di <{node.type}>: Tabel '{node.val}' tidak ditemukan. Tersedia: {stats['tables']}")
            return False
    
    return True