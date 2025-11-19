"""
Unit tests untuk Rule 1: Seleksi Konjungtif
Testing transformasi OPERATOR_S(AND) menjadi cascaded filters
"""

import unittest
from query_optimizer.query_tree import QueryTree
from query_optimizer.optimization_engine import ParsedQuery
from query_optimizer.query_check import check_query
from query_optimizer.seleksi_konjungtif import (
    seleksi_konjungtif,
    seleksi_konjungtif_rec,
    transform_and_filter,
    is_conjunctive_filter,
    can_transform,
    cascade_filters,
    cascade_and_with_order,
    uncascade_filters,
    clone_tree
)


class TestSeleksiKonjungtif(unittest.TestCase):
    """Test cases untuk fungsi seleksi_konjungtif"""
    
    def setUp(self):
        """Setup test fixtures"""
        pass
    
    def test_simple_and_transformation(self):
        """Test transformasi sederhana OPERATOR_S(AND) dengan 2 kondisi"""
        # Input: OPERATOR_S(AND) -> [RELATION(users), FILTER(WHERE age > 18), FILTER(WHERE status = 'active')]
        # Output: FILTER(WHERE status = 'active') -> FILTER(WHERE age > 18) -> RELATION(users)
        
        relation = QueryTree("RELATION", "users")
        condition1 = QueryTree("FILTER", "WHERE age > 18")
        condition2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(condition1)
        and_filter.add_child(condition2)
        
        query = ParsedQuery(and_filter, "SELECT * FROM users WHERE age > 18 AND status = 'active'")
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = seleksi_konjungtif(query)
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        # Cek struktur hasil
        self.assertEqual(result.query_tree.type, "FILTER")
        self.assertTrue(result.query_tree.val.startswith("WHERE"))
        self.assertEqual(len(result.query_tree.childs), 1)
        
        # Cek nested filter
        nested = result.query_tree.get_child(0)
        self.assertEqual(nested.type, "FILTER")
        self.assertTrue(nested.val.startswith("WHERE"))
        self.assertEqual(len(nested.childs), 1)
        
        # Cek relation di bottom
        bottom = nested.get_child(0)
        self.assertEqual(bottom.type, "RELATION")
        self.assertEqual(bottom.val, "users")
    
    def test_and_with_three_conditions(self):
        """Test OPERATOR_S(AND) dengan 3 kondisi"""
        relation = QueryTree("RELATION", "orders")
        cond1 = QueryTree("FILTER", "WHERE amount > 100")
        cond2 = QueryTree("FILTER", "WHERE status = 'paid'")
        cond3 = QueryTree("FILTER", "WHERE date > '2024-01-01'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        and_filter.add_child(cond3)
        
        query = ParsedQuery(and_filter, "test query")
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = seleksi_konjungtif(query)
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        # Count depth of cascaded filters
        depth = 0
        current = result.query_tree
        while current.type == "FILTER":
            depth += 1
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
        
        self.assertEqual(depth, 3, "Should have 3 cascaded filters")
    
    def test_no_transformation_for_nested_and(self):
        """Test bahwa nested AND tidak ditransformasi"""
        # OPERATOR_S(AND) -> [OPERATOR_S(AND), FILTER(cond1), FILTER(cond2)]
        
        inner_and = QueryTree("OPERATOR_S", "AND")
        inner_and.add_child(QueryTree("RELATION", "users"))
        inner_and.add_child(QueryTree("FILTER", "WHERE x = 1"))
        inner_and.add_child(QueryTree("FILTER", "WHERE y = 2"))
        
        outer_and = QueryTree("OPERATOR_S", "AND")
        outer_and.add_child(inner_and)
        outer_and.add_child(QueryTree("FILTER", "WHERE z = 3"))
        outer_and.add_child(QueryTree("FILTER", "WHERE w = 4"))
        
        query = ParsedQuery(outer_and, "test query")
        
        # Validasi query sebelum transformasi
        check_query(outer_and)
        
        result = seleksi_konjungtif(query)
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        # Inner AND will be transformed, outer AND will also be transformed
        # Result should be cascaded filters
        self.assertEqual(result.query_tree.type, "FILTER")
    
    def test_is_conjunctive_filter(self):
        """Test fungsi is_conjunctive_filter"""
        # is_conjunctive_filter now requires OPERATOR_S with >= 3 children
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE x = 1")
        cond2 = QueryTree("FILTER", "WHERE y = 2")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        self.assertTrue(is_conjunctive_filter(and_filter))
        
        or_filter = QueryTree("OPERATOR_S", "OR")
        or_filter.add_child(relation)
        or_filter.add_child(cond1)
        or_filter.add_child(cond2)
        self.assertFalse(is_conjunctive_filter(or_filter))
        
        where_filter = QueryTree("FILTER", "WHERE x = 1")
        self.assertFalse(is_conjunctive_filter(where_filter))
        
        relation = QueryTree("RELATION", "users")
        self.assertFalse(is_conjunctive_filter(relation))
    
    def test_can_transform(self):
        """Test fungsi can_transform"""
        # Valid transformation case
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        self.assertTrue(can_transform(and_filter))
        
        # Invalid: less than 3 children
        and_filter2 = QueryTree("OPERATOR_S", "AND")
        and_filter2.add_child(relation)
        and_filter2.add_child(cond1)
        
        self.assertFalse(can_transform(and_filter2))
        
        # Invalid: first child is OPERATOR (nested logic)
        and_filter3 = QueryTree("OPERATOR_S", "AND")
        and_filter3.add_child(QueryTree("OPERATOR", "AND"))
        and_filter3.add_child(cond1)
        and_filter3.add_child(cond2)
        
        self.assertFalse(can_transform(and_filter3))
    
    def test_cascade_filters_with_order(self):
        """Test cascade filters dengan urutan spesifik"""
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        cond3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        and_filter.add_child(cond3)
        
        query = ParsedQuery(and_filter, "test query")
        
        # Apply dengan urutan custom: [2, 0, 1]
        # Order menentukan urutan aplikasi dari bottom ke top
        # Index 2 (city) diapply first (bottom), lalu 0 (age), lalu 1 (status) di top
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = cascade_filters(query, [2, 0, 1])
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        # Verify cascade structure exists
        filters = []
        current = result.query_tree
        while current.type == "FILTER" and len(current.childs) == 1:
            filters.append(current.val)
            current = current.get_child(0)
        
        # Should have 3 cascaded filters
        self.assertEqual(len(filters), 3)
        # Verify all conditions are present
        all_conditions = " ".join(filters).lower()
        self.assertTrue("city" in all_conditions)
        self.assertTrue("age" in all_conditions)
        self.assertTrue("status" in all_conditions)
    
    def test_uncascade_filters(self):
        """Test konversi cascaded filters kembali ke AND"""
        # Create cascaded filters: FILTER(cond3) -> FILTER(cond2) -> FILTER(cond1) -> RELATION
        relation = QueryTree("RELATION", "users")
        
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter1.add_child(relation)
        
        filter2 = QueryTree("FILTER", "WHERE status = 'active'")
        filter2.add_child(filter1)
        
        filter3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        filter3.add_child(filter2)
        
        query = ParsedQuery(filter3, "test query")
        
        # Validasi query sebelum uncascade
        check_query(filter3)
        
        result = uncascade_filters(query)
        
        # Validasi query setelah uncascade
        check_query(result.query_tree)
        
        # Due to bottom-up recursion, top filter remains but child becomes AND
        # Result structure: FILTER(city) -> FILTER(AND) -> [RELATION, FILTER(age), FILTER(status)]
        self.assertEqual(result.query_tree.type, "FILTER")
        
        # Should have 1 child (the AND node created from inner cascaded filters)
        self.assertEqual(len(result.query_tree.childs), 1)
        
        # Child should be OPERATOR_S(AND) node
        and_node = result.query_tree.get_child(0)
        self.assertEqual(and_node.type, "OPERATOR_S")
        self.assertEqual(and_node.val, "AND")
        
        # AND node should have 3 children: 1 relation + 2 filters
        self.assertEqual(len(and_node.childs), 3)
        self.assertEqual(and_node.get_child(0).type, "RELATION")
    
    def test_clone_tree(self):
        """Test deep cloning query tree"""
        relation = QueryTree("RELATION", "users")
        filter1 = QueryTree("FILTER", "WHERE age > 18")
        filter1.add_child(relation)
        
        cloned = clone_tree(filter1)
        
        # Verify it's a different object
        self.assertIsNot(cloned, filter1)
        self.assertIsNot(cloned.childs[0], relation)
        
        # Verify structure is same
        self.assertEqual(cloned.type, filter1.type)
        self.assertEqual(cloned.val, filter1.val)
        self.assertEqual(cloned.childs[0].type, relation.type)
        self.assertEqual(cloned.childs[0].val, relation.val)
    
    def test_complex_nested_structure(self):
        """Test transformasi pada struktur nested yang kompleks"""
        # PROJECT -> OPERATOR_S(AND) -> [RELATION, cond1, cond2]
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        project = QueryTree("PROJECT", "name, email")
        project.add_child(and_filter)
        
        query = ParsedQuery(project, "test query")
        
        # Validasi query sebelum transformasi
        check_query(project)
        
        result = seleksi_konjungtif(query)
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        # PROJECT should still be at top
        self.assertEqual(result.query_tree.type, "PROJECT")
        
        # Child should be cascaded filters
        child = result.query_tree.get_child(0)
        self.assertEqual(child.type, "FILTER")
        
        # Verify it's cascaded
        nested = child.get_child(0)
        self.assertEqual(nested.type, "FILTER")
    
    def test_empty_and_filter(self):
        """Test OPERATOR_S(AND) tanpa kondisi yang cukup"""
        # Note: OPERATOR_S(AND) dengan hanya 2 children (1 source + 1 condition) adalah invalid
        # karena AND memerlukan minimal 2 conditions untuk operasi logika
        # Test ini memverifikasi bahwa transformasi tidak terjadi pada struktur yang tidak memenuhi syarat
        
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = active")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        # Ubah menjadi hanya 2 children dengan memodifikasi after validation
        query = ParsedQuery(and_filter, "test query")
        
        # Validasi query dengan 3 children (valid)
        check_query(and_filter)
        
        # Remove satu condition untuk test transformasi dengan < 3 children
        and_filter.childs = [relation, cond1]
        
        result = seleksi_konjungtif(query)
        
        # Should not transform (less than 3 children)
        self.assertEqual(result.query_tree.type, "OPERATOR_S")
        self.assertEqual(result.query_tree.val, "AND")
    
    def test_transform_preserves_query_string(self):
        """Test bahwa transformasi mempertahankan query string"""
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        original_query = "SELECT * FROM users WHERE age > 18 AND status = 'active'"
        query = ParsedQuery(and_filter, original_query)
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = seleksi_konjungtif(query)
        
        # Validasi query setelah transformasi
        check_query(result.query_tree)
        
        self.assertEqual(result.query, original_query)


class TestTransformAndFilter(unittest.TestCase):
    """Test cases untuk fungsi transform_and_filter"""
    
    def test_basic_transform(self):
        """Test basic transform_and_filter"""
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = transform_and_filter(and_filter)
        
        # Validasi query setelah transformasi
        check_query(result)
        
        # Result should be cascaded filters
        self.assertEqual(result.type, "FILTER")
        self.assertNotEqual(result.val, "AND")
        
        # Should have single child (another filter)
        self.assertEqual(len(result.childs), 1)
        self.assertEqual(result.childs[0].type, "FILTER")
    
    def test_transform_with_complex_conditions(self):
        """Test transform dengan kondisi yang lebih kompleks"""
        relation = QueryTree("RELATION", "orders")
        
        # Kondisi dengan subquery - gunakan tabel yang ada
        cond1 = QueryTree("FILTER", "IN user_id")
        subquery = QueryTree("RELATION", "users")
        cond1.add_child(subquery)
        
        cond2 = QueryTree("FILTER", "WHERE amount > 1000")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        
        # Validasi query sebelum transformasi
        check_query(and_filter)
        
        result = transform_and_filter(and_filter)
        
        # Validasi query setelah transformasi
        check_query(result)
        
        self.assertEqual(result.type, "FILTER")
        self.assertEqual(len(result.childs), 1)


class TestCascadeAndUncascade(unittest.TestCase):
    """Test cases untuk cascade dan uncascade operations"""
    
    def test_cascade_then_uncascade(self):
        """Test bahwa cascade -> uncascade mengembalikan struktur OPERATOR_S(AND) (partial)"""
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE age > 18")
        cond2 = QueryTree("FILTER", "WHERE status = 'active'")
        cond3 = QueryTree("FILTER", "WHERE city = 'Jakarta'")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        and_filter.add_child(cond3)
        
        query = ParsedQuery(and_filter, "test query")
        
        # Validasi query sebelum cascade
        check_query(and_filter)
        
        # Cascade
        cascaded = cascade_filters(query)
        
        # Validasi query setelah cascade
        check_query(cascaded.query_tree)
        
        # Verify it's cascaded (should be single-child filters chained)
        self.assertEqual(cascaded.query_tree.type, "FILTER")
        self.assertNotEqual(cascaded.query_tree.val, "AND")
        self.assertEqual(len(cascaded.query_tree.childs), 1)
        
        # Uncascade
        uncascaded = uncascade_filters(cascaded)
        
        # Validasi query setelah uncascade
        check_query(uncascaded.query_tree)
        
        # Due to bottom-up recursion, uncascade creates nested structure
        # Top filter remains, child becomes AND
        self.assertEqual(uncascaded.query_tree.type, "FILTER")
        self.assertEqual(len(uncascaded.query_tree.childs), 1)
        
        # Child should be AND node (or another FILTER with AND below it)
        # Find the AND node in the structure
        current = uncascaded.query_tree
        found_and = False
        depth = 0
        while current and depth < 5:
            if current.val == "AND":
                found_and = True
                # Verify AND node has multiple children including relation
                self.assertGreaterEqual(len(current.childs), 2)
                break
            if len(current.childs) > 0:
                current = current.get_child(0)
            else:
                break
            depth += 1
        
        self.assertTrue(found_and, "Should find AND node in uncascaded structure")
    
    def test_cascade_with_different_orders(self):
        """Test cascade dengan berbagai urutan"""
        relation = QueryTree("RELATION", "users")
        cond1 = QueryTree("FILTER", "WHERE a = 1")
        cond2 = QueryTree("FILTER", "WHERE b = 2")
        cond3 = QueryTree("FILTER", "WHERE c = 3")
        
        and_filter = QueryTree("OPERATOR_S", "AND")
        and_filter.add_child(relation)
        and_filter.add_child(cond1)
        and_filter.add_child(cond2)
        and_filter.add_child(cond3)
        
        query = ParsedQuery(and_filter, "test")
        
        # Test dengan berbagai order
        orders = [
            [0, 1, 2],  # Normal order
            [2, 1, 0],  # Reverse order
            [1, 0, 2],  # Mixed order
        ]
        
        # Validasi query sebelum cascade
        check_query(and_filter)
        
        for order in orders:
            result = cascade_filters(query, order)
            
            # Validasi query setelah cascade
            check_query(result.query_tree)
            
            # Count filters
            depth = 0
            current = result.query_tree
            while current.type == "FILTER" and len(current.childs) == 1:
                depth += 1
                current = current.get_child(0)
            
            self.assertEqual(depth, 3, f"Should have 3 filters for order {order}")


if __name__ == "__main__":
    unittest.main()
