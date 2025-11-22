"""
Rule 8: Projection Operation over Theta Join Operation

Equivalency Rules:
1. Jika π hanya melibatkan atribut dari L1 ∪ L2:
   π_L1∪L2(E1 ⋈_θ E2) = (π_L1(E1)) ⋈_θ (π_L2(E2))

2. Untuk kasus umum:
   π_L1∪L2(E1 ⋈_θ E2) = π_L1∪L2((π_L1∪L3(E1)) ⋈_θ (π_L2∪L4(E2)))
   
   Dimana:
   - L1 = atribut dari E1 yang ada di proyeksi final
   - L2 = atribut dari E2 yang ada di proyeksi final
   - L3 = atribut dari E1 yang diperlukan untuk join condition, tapi tidak di L1
   - L4 = atribut dari E2 yang diperlukan untuk join condition, tapi tidak di L2

Benefit:
- Mengurangi ukuran relasi sebelum join dengan proyeksi awal
- Meningkatkan performa join dengan data yang lebih kecil
- Tetap mempertahankan atribut yang diperlukan untuk join condition

Example:
    Before:
        PROJECT
        ├── COLUMN_REF
        │   ├── COLUMN_NAME
        │   │   └── IDENTIFIER("name")
        │   └── TABLE_NAME
        │       └── IDENTIFIER("employee")
        ├── COLUMN_REF
        │   ├── COLUMN_NAME
        │   │   └── IDENTIFIER("department")
        │   └── TABLE_NAME
        │       └── IDENTIFIER("department")
        └── JOIN("INNER")
            ├── RELATION("employee")     # cols: id, name, dept_id, salary
            ├── RELATION("department")   # cols: id, department, location
            └── COMPARISON("=")
                ├── COLUMN_REF
                │   ├── COLUMN_NAME
                │   │   └── IDENTIFIER("dept_id")
                │   └── TABLE_NAME
                │       └── IDENTIFIER("employee")
                └── COLUMN_REF
                    ├── COLUMN_NAME
                    │   └── IDENTIFIER("id")
                    └── TABLE_NAME
                        └── IDENTIFIER("department")
    
    After (Rule 8 applied):
        PROJECT
        ├── COLUMN_REF
        │   ├── COLUMN_NAME
        │   │   └── IDENTIFIER("name")
        │   └── TABLE_NAME
        │       └── IDENTIFIER("employee")
        ├── COLUMN_REF
        │   ├── COLUMN_NAME
        │   │   └── IDENTIFIER("department")
        │   └── TABLE_NAME
        │       └── IDENTIFIER("department")
        └── JOIN("INNER")
            ├── PROJECT                  # Projection pushed to left side
            │   ├── COLUMN_REF
            │   │   └── COLUMN_NAME
            │   │       └── IDENTIFIER("name")
            │   ├── COLUMN_REF
            │   │   └── COLUMN_NAME
            │   │       └── IDENTIFIER("dept_id")
            │   └── RELATION("employee")
            ├── PROJECT                  # Projection pushed to right side
            │   ├── COLUMN_REF
            │   │   └── COLUMN_NAME
            │   │       └── IDENTIFIER("id")
            │   ├── COLUMN_REF
            │   │   └── COLUMN_NAME
            │   │       └── IDENTIFIER("department")
            │   └── RELATION("department")
            └── COMPARISON("=")
                ├── COLUMN_REF
                │   ├── COLUMN_NAME
                │   │   └── IDENTIFIER("dept_id")
                │   └── TABLE_NAME
                │       └── IDENTIFIER("employee")
                └── COLUMN_REF
                    ├── COLUMN_NAME
                    │   └── IDENTIFIER("id")
                    └── TABLE_NAME
                        └── IDENTIFIER("department")

IMPORTANT: Rule ini dijalankan SEKALI di awal proses optimasi (sebelum genetic algorithm),
TIDAK dimasukkan dalam parameter space GA.

- Bersifat deterministik: untuk setiap pattern PROJECT → JOIN, selalu optimal untuk push projection
- Always beneficial: mengurangi ukuran data sebelum join selalu lebih baik
- Tidak ada trade-off yang perlu dieksplor dengan GA
- Push projection selalu menghasilkan improvement yang sama
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
from typing import Set, Dict, List, Tuple


def push_projection_over_joins(query: ParsedQuery) -> ParsedQuery:
    """
    Apply Rule 8 ke semua opportunities yang ditemukan.
    
    Fungsi ini akan:
    1. Analisis query tree untuk menemukan pattern PROJECT → JOIN
    2. Untuk setiap JOIN yang bisa dioptimasi, push projection ke child
    3. Return hasil optimasi
    """
    # Analisis opportunities
    opportunities = analyze_projection_over_join(query)
    
    # Filter hanya yang can_optimize
    valid_opportunities = {
        join_id: info 
        for join_id, info in opportunities.items() 
        if info.get('can_optimize', False)
    }
    
    if not valid_opportunities:
        return query  # Tidak ada yang bisa dioptimasi
    
    # Apply push projection untuk setiap opportunity
    result = query
    for join_id, info in valid_opportunities.items():
        result = push_projection_to_join(result)
    
    return result


def analyze_projection_over_join(query: ParsedQuery) -> Dict[int, Dict[str, any]]:
    """
    Analisis query untuk menemukan pattern PROJECT → JOIN yang dapat dioptimasi.
    
    Returns:
        Dict dengan join_id sebagai key dan metadata sebagai value:
        {
            join_id: {
                'project_node': QueryTree,
                'join_node': QueryTree,
                'projected_cols': Set[str],
                'left_source': QueryTree,
                'right_source': QueryTree,
                'join_condition': QueryTree,
                'can_optimize': bool
            }
        }
    """
    opportunities = {}
    
    def traverse(node: QueryTree):
        if node is None:
            return
        
        # Check for PROJECT → JOIN pattern
        if node.type == "PROJECT":
            # Check if child is JOIN
            join_child = None
            for child in node.childs:
                if child.type == "JOIN" and child.val == "INNER":
                    join_child = child
                    break
            
            if join_child and len(join_child.childs) >= 3:
                # Found PROJECT → JOIN pattern
                left_source = join_child.childs[0]
                right_source = join_child.childs[1]
                join_condition = join_child.childs[2] if len(join_child.childs) > 2 else None
                
                # Extract projected columns
                projected_cols = extract_projected_columns(node)
                
                opportunities[join_child.id] = {
                    'project_node': node,
                    'join_node': join_child,
                    'projected_cols': projected_cols,
                    'left_source': left_source,
                    'right_source': right_source,
                    'join_condition': join_condition,
                    'can_optimize': len(projected_cols) > 0 and join_condition is not None
                }
        
        for child in node.childs:
            traverse(child)
    
    traverse(query.query_tree)
    return opportunities


def extract_projected_columns(project_node: QueryTree) -> Set[str]:
    """
    Ekstrak nama kolom yang diproyeksikan dari PROJECT node.
    
    Returns:
        Set of column names (simple names, tanpa table prefix)
    """
    columns = set()
    
    def extract_from_node(node: QueryTree):
        if node is None:
            return
        
        if node.type == "COLUMN_NAME":
            # Get IDENTIFIER child
            for child in node.childs:
                if child.type == "IDENTIFIER":
                    columns.add(child.val)
        elif node.type == "IDENTIFIER" and node.parent and node.parent.type == "COLUMN_NAME":
            columns.add(node.val)
        
        for child in node.childs:
            extract_from_node(child)
    
    # Extract from all non-source children (skip JOIN/RELATION/FILTER)
    for child in project_node.childs:
        if child.type not in ["JOIN", "RELATION", "FILTER", "SORT", "LIMIT"]:
            extract_from_node(child)
    
    return columns


def extract_join_condition_columns(condition_node: QueryTree) -> Tuple[Set[str], Set[str]]:
    """
    Ekstrak kolom yang digunakan dalam join condition.
    
    Returns:
        Tuple of (left_table_cols, right_table_cols)
        Atau (all_cols, set()) jika tidak bisa membedakan
    """
    columns = set()
    
    def extract_cols(node: QueryTree):
        if node is None:
            return
        
        if node.type == "COLUMN_NAME":
            for child in node.childs:
                if child.type == "IDENTIFIER":
                    columns.add(child.val)
        elif node.type == "IDENTIFIER" and node.parent and node.parent.type == "COLUMN_NAME":
            columns.add(node.val)
        
        for child in node.childs:
            extract_cols(child)
    
    extract_cols(condition_node)
    
    # Untuk simplifikasi, return semua kolom di kedua set
    # Implementasi lebih canggih bisa membedakan berdasarkan table prefix
    return (columns, columns)


def push_projection_to_join(
    query: ParsedQuery,
    join_opportunities: Dict[int, Dict[str, any]] = None
) -> ParsedQuery:
    """
    Transformation:
        PROJECT(L1 ∪ L2)
        └── JOIN
            ├── E1
            └── E2
    
    Menjadi:
        PROJECT(L1 ∪ L2)
        └── JOIN
            ├── PROJECT(L1 ∪ L3)  # L3 = join condition cols from E1
            │   └── E1
            └── PROJECT(L2 ∪ L4)  # L4 = join condition cols from E2
                └── E2
    """
    if join_opportunities is None:
        join_opportunities = analyze_projection_over_join(query)
    
    if not join_opportunities:
        return query
    
    from query_optimizer.rule_1 import clone_tree
    cloned_tree = clone_tree(query.query_tree)
    
    # Re-analyze opportunities on cloned tree (IDs will be different after clone)
    cloned_query = ParsedQuery(cloned_tree, query.query)
    cloned_opportunities = analyze_projection_over_join(cloned_query)
    
    # Apply transformation for each opportunity
    def transform(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        # Check if this is a PROJECT node with JOIN child
        if node.type == "PROJECT":
            join_child = None
            for child in node.childs:
                if child.type == "JOIN" and child.id in cloned_opportunities:
                    join_child = child
                    break
            
            if join_child:
                opp = cloned_opportunities[join_child.id]
                if opp['can_optimize']:
                    # Apply Rule 8 transformation
                    return apply_rule8_transformation(node, opp)
        
        # Recursively transform children
        for i, child in enumerate(node.childs):
            node.childs[i] = transform(child)
            if node.childs[i]:
                node.childs[i].parent = node
        
        return node
    
    transformed_tree = transform(cloned_tree)
    return ParsedQuery(transformed_tree, query.query)


def apply_rule8_transformation(
    project_node: QueryTree,
    opportunity: Dict[str, any]
) -> QueryTree:
    """
    Apply Rule 8: Push projection to both sides of join.
    
    Creates:
    PROJECT(original columns)
    └── JOIN
        ├── PROJECT(needed cols from left)
        │   └── left source
        └── PROJECT(needed cols from right)
            └── right source
    """
    join_node = opportunity['join_node']
    projected_cols = opportunity['projected_cols']
    left_source = opportunity['left_source']
    right_source = opportunity['right_source']
    join_condition = opportunity['join_condition']
    
    # Extract join condition columns
    left_join_cols, right_join_cols = extract_join_condition_columns(join_condition)
    
    # Determine which projected columns belong to which table
    # For simplicity, assume half go to left, half go to right
    # Better implementation would use schema information
    projected_list = list(projected_cols)
    mid = len(projected_list) // 2
    left_projected = set(projected_list[:mid])
    right_projected = set(projected_list[mid:])
    
    # L1 ∪ L3: projected cols from left + join cols from left
    left_needed_cols = left_projected | left_join_cols
    
    # L2 ∪ L4: projected cols from right + join cols from right  
    right_needed_cols = right_projected | right_join_cols
    
    # Create new PROJECT nodes for left and right
    if len(left_needed_cols) > 0:
        left_project = create_project_node(left_needed_cols, left_source)
    else:
        left_project = left_source
    
    if len(right_needed_cols) > 0:
        right_project = create_project_node(right_needed_cols, right_source)
    else:
        right_project = right_source
    
    # Update join children
    join_node.childs[0] = left_project
    join_node.childs[1] = right_project
    left_project.parent = join_node
    right_project.parent = join_node
    
    return project_node


def create_project_node(columns: Set[str], source: QueryTree) -> QueryTree:
    """
    Buat PROJECT node dengan kolom yang ditentukan.
    """
    project = QueryTree("PROJECT", "")
    
    # Add column references
    for col_name in sorted(columns):  # Sort for consistency
        col_ref = QueryTree("COLUMN_REF", "")
        col_name_node = QueryTree("COLUMN_NAME", "")
        identifier = QueryTree("IDENTIFIER", col_name)
        
        col_name_node.add_child(identifier)
        col_ref.add_child(col_name_node)
        project.add_child(col_ref)
    
    # Add source
    project.add_child(source)
    source.parent = project
    
    return project


def can_apply_rule8(project_node: QueryTree) -> bool:
    """
    Check apakah Rule 8 bisa diterapkan pada PROJECT node ini.
    
    Kondisi:
    1. Node harus PROJECT
    2. Child langsung harus JOIN (INNER)
    3. JOIN harus punya condition
    4. PROJECT harus punya kolom yang jelas (bukan SELECT *)
    """
    if not project_node or project_node.type != "PROJECT":
        return False
    
    # Check for SELECT *
    if project_node.val and project_node.val.strip() == "*":
        return False
    
    # Check if has JOIN child
    join_child = None
    for child in project_node.childs:
        if child.type == "JOIN" and child.val == "INNER":
            join_child = child
            break
    
    if not join_child:
        return False
    
    # Check if JOIN has condition (at least 3 children)
    if len(join_child.childs) < 3:
        return False
    
    # Check if JOIN has proper sources
    left = join_child.childs[0]
    right = join_child.childs[1]
    
    # Sources should be RELATION, FILTER, or another JOIN/PROJECT
    valid_types = {"RELATION", "FILTER", "JOIN", "PROJECT"}
    if left.type not in valid_types or right.type not in valid_types:
        return False
    
    return True


def undo_rule8(query: ParsedQuery) -> ParsedQuery:
    """
    Undo Rule 8 transformation: hapus projection di bawah join jika ada.
    
    Transformation:
        PROJECT(L1 ∪ L2)
        └── JOIN
            ├── PROJECT(L1 ∪ L3)
            │   └── E1
            └── PROJECT(L2 ∪ L4)
                └── E2
    
    Menjadi:
        PROJECT(L1 ∪ L2)
        └── JOIN
            ├── E1
            └── E2
    """
    from query_optimizer.rule_1 import clone_tree
    cloned_tree = clone_tree(query.query_tree)
    
    def remove_projections(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        # If this is a JOIN node
        if node.type == "JOIN":
            # Check if children are PROJECT nodes
            for i in range(min(2, len(node.childs))):  # Left and right children
                child = node.childs[i]
                if child.type == "PROJECT":
                    # Remove PROJECT, use its source instead
                    source = None
                    for sub_child in child.childs:
                        if sub_child.type in {"RELATION", "FILTER", "JOIN", "PROJECT"}:
                            source = sub_child
                            break
                    
                    if source:
                        node.childs[i] = source
                        source.parent = node
        
        # Recursively process children
        for i, child in enumerate(node.childs):
            node.childs[i] = remove_projections(child)
            if node.childs[i]:
                node.childs[i].parent = node
        
        return node
    
    transformed_tree = remove_projections(cloned_tree)
    return ParsedQuery(transformed_tree, query.query)


# DEPRECATED: Rule 8 tidak lagi memerlukan parameter functions
# karena rule ini dijalankan sekali di awal, tidak di genetic algorithm.
# Functions berikut hanya disimpan untuk backward compatibility dengan tests.

def generate_random_rule8_params(num_joins: int) -> Dict[int, bool]:
    """DEPRECATED: Rule 8 tidak digunakan di GA."""
    return {}


def copy_rule8_params(params: Dict[int, bool]) -> Dict[int, bool]:
    """DEPRECATED: Rule 8 tidak digunakan di GA."""
    return {}


def mutate_rule8_params(params: Dict[int, bool]) -> Dict[int, bool]:
    """DEPRECATED: Rule 8 tidak digunakan di GA."""
    return {}


def validate_rule8_params(params: Dict[int, bool]) -> bool:
    """DEPRECATED: Rule 8 tidak digunakan di GA."""
    return True
