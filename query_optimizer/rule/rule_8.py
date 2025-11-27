"""
Pushdown Projections over Joins

Input:
PROJECT
├── columns
└── JOIN
    ├── left_relation
    ├── right_relation
    └── join_condition

Output:
PROJECT
├── columns
└── JOIN
    ├── PROJECT (left columns + join keys)
    │   └── left_relation
    ├── PROJECT (right columns + join keys)
    │   └── right_relation
    └── join_condition
"""

from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree
from typing import Set, Dict, Tuple


def push_projection_over_joins(query: ParsedQuery) -> ParsedQuery:
    from query_optimizer.rule.rule_1 import clone_tree
    cloned_tree = clone_tree(query.query_tree)
    changed = [False]
    
    def transform(node: QueryTree, parent_projections: Set[str] = None) -> QueryTree:
        if node is None:
            return None
        
        current_projections = parent_projections
        if node.type == "PROJECT":
            current_projections = extract_projected_columns(node)
        
        if node.type in ["FILTER", "SORT"] and parent_projections:
            current_projections = parent_projections
        
        if node.type == "JOIN" and node.val == "INNER" and current_projections:
            if len(node.childs) >= 3:
                left_source = node.childs[0]
                right_source = node.childs[1]
                join_condition = node.childs[2]
                
                left_join_cols, right_join_cols = extract_join_condition_columns(join_condition)
                
                left_project = create_smart_project(current_projections, left_join_cols, left_source)
                right_project = create_smart_project(current_projections, right_join_cols, right_source)
                
                if left_project != left_source:
                    node.childs[0] = left_project
                    left_project.parent = node
                    changed[0] = True
                if right_project != right_source:
                    node.childs[1] = right_project
                    right_project.parent = node
                    changed[0] = True
        
        for i, child in enumerate(node.childs):
            node.childs[i] = transform(child, current_projections)
            if node.childs[i]:
                node.childs[i].parent = node
        
        return node
    
    transformed_tree = transform(cloned_tree)
    
    if changed[0]:
        return ParsedQuery(transformed_tree, query.query)
    else:
        return query


def analyze_projection_over_join(query: ParsedQuery) -> Dict[int, Dict[str, any]]:
    opportunities = {}
    
    def traverse(node: QueryTree):
        if node is None:
            return
        
        if node.type == "PROJECT":
            join_child = None
            for child in node.childs:
                if child.type == "JOIN" and child.val == "INNER":
                    join_child = child
                    break
            
            if join_child and len(join_child.childs) >= 3:
                left_source = join_child.childs[0]
                right_source = join_child.childs[1]
                join_condition = join_child.childs[2] if len(join_child.childs) > 2 else None
                
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
    
    return (columns, columns)

def push_projection_to_join(
    query: ParsedQuery,
    join_opportunities: Dict[int, Dict[str, any]] = None
) -> ParsedQuery:
    if join_opportunities is None:
        join_opportunities = analyze_projection_over_join(query)
    
    if not join_opportunities:
        return query
    
    from query_optimizer.rule.rule_1 import clone_tree
    cloned_tree = clone_tree(query.query_tree)
    cloned_query = ParsedQuery(cloned_tree, query.query)
    cloned_opportunities = analyze_projection_over_join(cloned_query)
    
    def transform(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        if node.type == "PROJECT":
            join_child = None
            for child in node.childs:
                if child.type == "JOIN" and child.id in cloned_opportunities:
                    join_child = child
                    break
            
            if join_child:
                opp = cloned_opportunities[join_child.id]
                if opp['can_optimize']:
                    return apply_rule8_transformation(node, opp)
        
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
    join_node = opportunity['join_node']
    projected_cols = opportunity['projected_cols']
    left_source = opportunity['left_source']
    right_source = opportunity['right_source']
    join_condition = opportunity['join_condition']
    
    left_join_cols, right_join_cols = extract_join_condition_columns(join_condition)
    projected_list = list(projected_cols)
    mid = len(projected_list) // 2
    left_projected = set(projected_list[:mid])
    right_projected = set(projected_list[mid:])
    left_needed_cols = left_projected | left_join_cols 
    right_needed_cols = right_projected | right_join_cols

    if len(left_needed_cols) > 0:
        left_project = create_project_node(left_needed_cols, left_source)
    else:
        left_project = left_source
    
    if len(right_needed_cols) > 0:
        right_project = create_project_node(right_needed_cols, right_source)
    else:
        right_project = right_source
    
    join_node.childs[0] = left_project
    join_node.childs[1] = right_project
    left_project.parent = join_node
    right_project.parent = join_node
    
    return project_node

def create_project_node(columns: Set[str], source: QueryTree) -> QueryTree:
    project = QueryTree("PROJECT", "")
    
    for col_name in sorted(columns):
        col_ref = QueryTree("COLUMN_REF", "")
        col_name_node = QueryTree("COLUMN_NAME", "")
        identifier = QueryTree("IDENTIFIER", col_name)
        
        col_name_node.add_child(identifier)
        col_ref.add_child(col_name_node)
        project.add_child(col_ref)
    
    project.add_child(source)
    source.parent = project
    
    return project

def create_smart_project(projected_cols: Set[str], join_cols: Set[str], source: QueryTree) -> QueryTree:
    from query_optimizer.query_check import get_metadata
    
    table_name = get_table_name_from_source(source)
    if not table_name:
        needed_cols = projected_cols | join_cols
        if not needed_cols:
            return source
        return create_project_node(needed_cols, source)
    
    metadata = get_metadata()
    table_columns = set(metadata["columns"].get(table_name, []))
    
    needed_cols = (projected_cols | join_cols) & table_columns
    
    if not needed_cols:
        return source
    
    return create_project_node(needed_cols, source)

def get_table_name_from_source(node: QueryTree) -> str | None:
    if node is None:
        return None
    
    if node.type == "RELATION":
        return node.val
    
    for child in node.childs:
        table_name = get_table_name_from_source(child)
        if table_name:
            return table_name
    
    return None

def can_apply_rule8(project_node: QueryTree) -> bool:
    if not project_node or project_node.type != "PROJECT":
        return False
    
    if project_node.val and project_node.val.strip() == "*":
        return False
    
    join_child = None
    for child in project_node.childs:
        if child.type == "JOIN" and child.val == "INNER":
            join_child = child
            break
    
    if not join_child:
        return False
    
    if len(join_child.childs) < 3:
        return False
    
    left = join_child.childs[0]
    right = join_child.childs[1]
    
    valid_types = {"RELATION", "FILTER", "JOIN", "PROJECT"}
    if left.type not in valid_types or right.type not in valid_types:
        return False
    
    return True

def undo_rule8(query: ParsedQuery) -> ParsedQuery:
    from query_optimizer.rule.rule_1 import clone_tree
    cloned_tree = clone_tree(query.query_tree)
    
    def remove_projections(node: QueryTree) -> QueryTree:
        if node is None:
            return None
        
        if node.type == "JOIN":
            for i in range(min(2, len(node.childs))):
                child = node.childs[i]
                if child.type == "PROJECT":
                    source = None
                    for sub_child in child.childs:
                        if sub_child.type in {"RELATION", "FILTER", "JOIN", "PROJECT"}:
                            source = sub_child
                            break
                    
                    if source:
                        node.childs[i] = source
                        source.parent = node
        
        for i, child in enumerate(node.childs):
            node.childs[i] = remove_projections(child)
            if node.childs[i]:
                node.childs[i].parent = node
        
        return node
    
    transformed_tree = remove_projections(cloned_tree)
    return ParsedQuery(transformed_tree, query.query)

def generate_random_rule8_params(num_joins: int) -> Dict[int, bool]:
    return {}

def copy_rule8_params(params: Dict[int, bool]) -> Dict[int, bool]:
    return {}

def mutate_rule8_params(params: Dict[int, bool]) -> Dict[int, bool]:
    return {}

def validate_rule8_params(params: Dict[int, bool]) -> bool:
    return True
