from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree

def seleksi_proyeksi(query: ParsedQuery) -> ParsedQuery:
    """
    Mengeliminasi proyeksi redundant yang bertumpuk.
    Rule: PROJECT_1(PROJECT_2(Source)) -> PROJECT_1(Source)
    
    NOTE: Fungsi ini dipanggil di optimize_query() SEBELUM genetic algorithm.
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
    
    # Check all children untuk nested PROJECT
    for child in node.childs:
        if child and child.is_node_type("PROJECT"):
            return True
        
    return False

def collapse_projection(outer_node: QueryTree) -> QueryTree:
    """
    Melakukan penggabungan dua node project.
    Menghilangkan inner projection, outer projection langsung ke source inner projection.
    """
    # Find inner PROJECT node
    inner_node = None
    inner_idx = -1
    
    for i, child in enumerate(outer_node.childs):
        if child and child.is_node_type("PROJECT"):
            inner_node = child
            inner_idx = i
            break
    
    if inner_node is None:
        return outer_node
    
    # If outer is *, inherit inner's column selection
    if outer_node.val.strip() == "*":
        outer_node.val = inner_node.val
        # Copy inner's column children to outer
        new_childs = []
        for child in inner_node.childs:
            if not child.is_node_type("PROJECT") and not child.is_node_type("RELATION") and not child.is_node_type("FILTER"):
                new_childs.append(child)
        # Get source from inner (last child that is PROJECT/RELATION/FILTER)
        for child in inner_node.childs:
            if child.is_node_type("PROJECT") or child.is_node_type("RELATION") or child.is_node_type("FILTER") or child.is_node_type("JOIN"):
                new_childs.append(child)
                break
        outer_node.childs = new_childs
    else:
        # Outer has specific columns, just replace inner PROJECT with its source
        # Find source in inner_node (RELATION, FILTER, or another PROJECT)
        source = None
        for child in inner_node.childs:
            if child.is_node_type("RELATION") or child.is_node_type("FILTER") or child.is_node_type("PROJECT") or child.is_node_type("JOIN"):
                source = child
                break
        
        if source:
            # Replace inner_node with its source in outer's children
            outer_node.childs[inner_idx] = source
            source.parent = outer_node
    
    inner_node.parent = None
    return outer_node