from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree

def seleksi_proyeksi(query: ParsedQuery) -> ParsedQuery:
    """
    Mengeliminasi proyeksi redundant yang bertumpuk.
    Rule: PROJECT_1(PROJECT_2(Source)) -> PROJECT_1(Source)
    """
    transformed_tree = seleksi_proyeksi_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)

def seleksi_proyeksi_rec(node: QueryTree) -> QueryTree:

    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = seleksi_proyeksi_rec(node.childs[i])
        if node.childs[i]:
            node.childs[i].parent = node
            
    if is_nested_projection(node):
        return collapse_projection(node)
        
    return node

def is_nested_projection(node: QueryTree) -> bool:
    """
    Mendeteksi apakah node saat ini adalah PROJECT 
    dan memiliki child langsung yang juga PROJECT.
    """

    if not node.is_node_type("PROJECT"):
        return False
    
    if len(node.childs) == 0:
        return False
        
    first_child = node.get_child(0)
    
    if first_child and first_child.is_node_type("PROJECT"):
        return True
        
    return False

def collapse_projection(outer_node: QueryTree) -> QueryTree:
    """
    Melakukan penggabungan dua node project.

    """
    inner_node = outer_node.get_child(0)

    if outer_node.val.strip() == "*":
        outer_node.val = inner_node.val
    
    if len(inner_node.childs) > 0:
        source = inner_node.get_child(0)
        
        outer_node.childs = [] 
        
        outer_node.add_child(source)
        
        inner_node.parent = None

        
    return outer_node