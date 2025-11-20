from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_tree import QueryTree

def join_komutatif(query: ParsedQuery) -> ParsedQuery:
    """
    Mengubah urutan tabel pada operasi JOIN.
    Rule: JOIN(A, B) ≡ JOIN(B, A)
    """
    transformed_tree = join_komutatif_rec(query.query_tree)
    return ParsedQuery(transformed_tree, query.query)

def join_komutatif_rec(node: QueryTree) -> QueryTree:

    if node is None:
        return None
    
    for i in range(len(node.childs)):
        node.childs[i] = join_komutatif_rec(node.childs[i])
        if node.childs[i]:
            node.childs[i].parent = node
            
    if node.is_node_type("JOIN"):
        node.childs[0], node.childs[1] = node.childs[1], node.childs[0]
        
    return node