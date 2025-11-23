"""
Comprehensive Query Test Suite
Tests 3 different queries with various parameters to verify query execution functionality
Uses real storage manager with test data
"""
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from query_optimizer.query_tree import QueryTree
from query_execution import QueryExecution
from storage_manager import StorageManager, ColumnDefinition

def setup_test_data():
    """Create test tables and populate with data using real storage manager"""
    # Use test data directory
    data_dir = os.path.join(os.path.dirname(__file__), 'data', 'test_queries')
    os.makedirs(data_dir, exist_ok=True)
    
    storage_manager = StorageManager(data_dir)
    
    # Create employees table
    print("Setting up test data...")
    print("Creating employees table...")
    table_created = False
    try:
        storage_manager.create_table(
            table_name='employees',
            columns=[
                ColumnDefinition(name='id', data_type='INTEGER', is_primary_key=True),
                ColumnDefinition(name='name', data_type='VARCHAR', size=100),
                ColumnDefinition(name='department', data_type='VARCHAR', size=50),
                ColumnDefinition(name='salary', data_type='INTEGER'),
                ColumnDefinition(name='years', data_type='INTEGER'),
            ],
            primary_keys=['id']
        )
        table_created = True
    except Exception as e:
        print(f"Note: employees table already exists: {e}")
    
    # Insert employee data (whether table was just created or already existed)
    employees_data = [
        (1, 'John Doe', 'Engineering', 75000, 5),
        (2, 'Jane Smith', 'Marketing', 65000, 3),
        (3, 'Bob Johnson', 'Engineering', 85000, 7),
        (4, 'Alice Williams', 'Sales', 70000, 4),
        (5, 'Charlie Brown', 'Engineering', 90000, 10),
        (6, 'Diana Prince', 'Marketing', 72000, 6),
        (7, 'Eve Davis', 'Sales', 68000, 2),
        (8, 'Frank Miller', 'Engineering', 95000, 12),
    ]
    
    from storage_manager import DataWrite
    inserted_count = 0
    for emp in employees_data:
        try:
            data_write = DataWrite(
                table='employees',
                column=['id', 'name', 'department', 'salary', 'years'],
                new_value=list(emp),
                conditions=[]
            )
            storage_manager.write_block(data_write)
            inserted_count += 1
        except Exception as e:
            if table_created:
                print(f"Warning: Failed to insert employee {emp[0]}: {e}")
    
    print(f"✓ Inserted {inserted_count} employees")
    
    # Create projects table
    print("Creating projects table...")
    table_created = False
    try:
        storage_manager.create_table(
            table_name='projects',
            columns=[
                ColumnDefinition(name='project_id', data_type='INTEGER', is_primary_key=True),
                ColumnDefinition(name='employee_id', data_type='INTEGER'),
                ColumnDefinition(name='project_name', data_type='VARCHAR', size=100),
                ColumnDefinition(name='budget', data_type='INTEGER'),
            ],
            primary_keys=['project_id']
        )
        table_created = True
    except Exception as e:
        print(f"Note: projects table already exists: {e}")
    
    # Insert project data (whether table was just created or already existed)
    projects_data = [
        (101, 1, 'Alpha', 100000),
        (102, 3, 'Beta', 150000),
        (103, 5, 'Gamma', 200000),
        (104, 8, 'Delta', 175000),
        (105, 2, 'Epsilon', 80000),
    ]
    
    from storage_manager import DataWrite
    inserted_count = 0
    for proj in projects_data:
        try:
            data_write = DataWrite(
                table='projects',
                column=['project_id', 'employee_id', 'project_name', 'budget'],
                new_value=list(proj),
                conditions=[]
            )
            storage_manager.write_block(data_write)
            inserted_count += 1
        except Exception as e:
            if table_created:
                print(f"Warning: Failed to insert project {proj[0]}: {e}")
    
    print(f"✓ Inserted {inserted_count} projects")
    
    print("✓ Test data setup complete\n")
    return storage_manager

def build_query_tree(query_type, **params):
    """Build query trees for different test scenarios"""
    
    if query_type == "filtered_projection":
        # Query: SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 80000 LIMIT 3
        # Build bottom-up: RELATION -> FILTER -> PROJECT -> LIMIT
        
        relation = QueryTree(type="RELATION", val="employees")
        
        # Build condition: department = 'Engineering' AND salary > 80000
        comp1 = QueryTree(type="COMPARISON", val="=")
        col_ref1 = QueryTree(type="COLUMN_REF")
        col_name1 = QueryTree(type="COLUMN_NAME")
        col_name1.childs.append(QueryTree(type="IDENTIFIER", val="department"))
        col_ref1.childs.append(col_name1)
        comp1.childs.extend([col_ref1, QueryTree(type="LITERAL_STRING", val="Engineering")])
        
        comp2 = QueryTree(type="COMPARISON", val=">")
        col_ref2 = QueryTree(type="COLUMN_REF")
        col_name2 = QueryTree(type="COLUMN_NAME")
        col_name2.childs.append(QueryTree(type="IDENTIFIER", val="salary"))
        col_ref2.childs.append(col_name2)
        comp2.childs.extend([col_ref2, QueryTree(type="LITERAL_NUMBER", val="80000")])
        
        and_op = QueryTree(type="OPERATOR", val="AND")
        and_op.childs.extend([comp1, comp2])
        
        filter_node = QueryTree(type="FILTER")
        filter_node.childs.extend([relation, and_op])
        
        # Build projection
        project = QueryTree(type="PROJECT", val="columns")
        col_ref_name = QueryTree(type="COLUMN_REF")
        col_ref_name.childs.append(QueryTree(type="COLUMN_NAME"))
        col_ref_name.childs[0].childs.append(QueryTree(type="IDENTIFIER", val="name"))
        
        col_ref_salary = QueryTree(type="COLUMN_REF")
        col_ref_salary.childs.append(QueryTree(type="COLUMN_NAME"))
        col_ref_salary.childs[0].childs.append(QueryTree(type="IDENTIFIER", val="salary"))
        
        project.childs.extend([col_ref_name, col_ref_salary, filter_node])
        
        # Add LIMIT
        limit = QueryTree(type="LIMIT", val="3")
        limit.childs.append(project)
        
        return limit
    
    elif query_type == "join_with_alias":
        # Query: SELECT e.name, p.project_name FROM employees AS e, projects AS p WHERE e.id = p.employee_id
        
        # Build relations with aliases
        emp_relation = QueryTree(type="RELATION", val="employees")
        emp_alias = QueryTree(type="ALIAS", val="e")
        emp_alias.childs.append(emp_relation)
        
        proj_relation = QueryTree(type="RELATION", val="projects")
        proj_alias = QueryTree(type="ALIAS", val="p")
        proj_alias.childs.append(proj_relation)
        
        # Build join condition: e.id = p.employee_id
        join_condition = QueryTree(type="COMPARISON", val="=")
        
        left_col = QueryTree(type="COLUMN_REF")
        left_col_name = QueryTree(type="COLUMN_NAME")
        left_col_name.childs.append(QueryTree(type="IDENTIFIER", val="id"))
        left_col.childs.append(left_col_name)
        
        right_col = QueryTree(type="COLUMN_REF")
        right_col_name = QueryTree(type="COLUMN_NAME")
        right_col_name.childs.append(QueryTree(type="IDENTIFIER", val="employee_id"))
        right_col.childs.append(right_col_name)
        
        join_condition.childs.extend([left_col, right_col])
        
        # Build INNER JOIN
        join = QueryTree(type="JOIN", val="INNER")
        join.childs.extend([emp_alias, proj_alias, join_condition])
        
        # Build projection: e.name, p.project_name
        project = QueryTree(type="PROJECT", val="columns")
        
        col_ref1 = QueryTree(type="COLUMN_REF")
        col_ref1.childs.append(QueryTree(type="COLUMN_NAME"))
        col_ref1.childs[0].childs.append(QueryTree(type="IDENTIFIER", val="name"))
        
        col_ref2 = QueryTree(type="COLUMN_REF")
        col_ref2.childs.append(QueryTree(type="COLUMN_NAME"))
        col_ref2.childs[0].childs.append(QueryTree(type="IDENTIFIER", val="project_name"))
        
        project.childs.extend([col_ref1, col_ref2, join])
        
        return project
    
    elif query_type == "aggregate_filter":
        # Query: SELECT * FROM employees WHERE years >= 5 AND salary < 90000 ORDER BY salary DESC LIMIT 4
        
        relation = QueryTree(type="RELATION", val="employees")
        
        # Build condition: years >= 5 AND salary < 90000
        comp1 = QueryTree(type="COMPARISON", val=">=")
        col_ref1 = QueryTree(type="COLUMN_REF")
        col_name1 = QueryTree(type="COLUMN_NAME")
        col_name1.childs.append(QueryTree(type="IDENTIFIER", val="years"))
        col_ref1.childs.append(col_name1)
        comp1.childs.extend([col_ref1, QueryTree(type="LITERAL_NUMBER", val="5")])
        
        comp2 = QueryTree(type="COMPARISON", val="<")
        col_ref2 = QueryTree(type="COLUMN_REF")
        col_name2 = QueryTree(type="COLUMN_NAME")
        col_name2.childs.append(QueryTree(type="IDENTIFIER", val="salary"))
        col_ref2.childs.append(col_name2)
        comp2.childs.extend([col_ref2, QueryTree(type="LITERAL_NUMBER", val="90000")])
        
        and_op = QueryTree(type="OPERATOR", val="AND")
        and_op.childs.extend([comp1, comp2])
        
        filter_node = QueryTree(type="FILTER")
        filter_node.childs.extend([relation, and_op])
        
        # Build SORT
        sort = QueryTree(type="SORT", val="DESC")
        sort_col = QueryTree(type="COLUMN_REF")
        sort_col.childs.append(QueryTree(type="COLUMN_NAME"))
        sort_col.childs[0].childs.append(QueryTree(type="IDENTIFIER", val="salary"))
        sort.childs.extend([sort_col, filter_node])
        
        # Build PROJECT (SELECT *)
        project = QueryTree(type="PROJECT", val="*")
        project.childs.append(sort)
        
        # Add LIMIT
        limit = QueryTree(type="LIMIT", val="4")
        limit.childs.append(project)
        
        return limit
    
    else:
        raise ValueError(f"Unknown query type: {query_type}")

def run_test(test_num, query_name, query_tree, description, storage_manager, transaction_id=None):
    """Run a single test case"""
    print(f"\n{'='*70}")
    print(f"TEST {test_num}: {query_name}")
    print(f"{'='*70}")
    print(f"Description: {description}")
    print(f"Transaction ID: {transaction_id}")
    print()
    
    executor = QueryExecution(storage_manager=storage_manager)
    
    try:
        result = executor.execute_node(query_tree, transaction_id=transaction_id)
        
        print(f"\n{'='*70}")
        print(f"RESULTS: {len(result)} rows returned")
        print(f"{'='*70}")
        for i, row in enumerate(result, 1):
            print(f"{i}. {row}")
        
        print(f"\n✓ Test {test_num} PASSED!")
        return True
        
    except Exception as e:
        print(f"\n✗ Test {test_num} FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all test queries"""
    print("="*70)
    print("COMPREHENSIVE QUERY TEST SUITE")
    print("="*70)
    print("Testing query execution with various parameters and features")
    print("Using REAL storage manager with persistent data")
    print()
    
    # Setup real storage manager and test data
    storage_manager = setup_test_data()
    
    results = []
    
    # TEST 1: Filtered projection with LIMIT
    query1 = build_query_tree("filtered_projection")
    results.append(run_test(
        test_num=1,
        query_name="Filtered Projection with LIMIT",
        query_tree=query1,
        description="SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 80000 LIMIT 3",
        storage_manager=storage_manager,
        transaction_id=1001
    ))
    
    # TEST 2: JOIN with aliases
    query2 = build_query_tree("join_with_alias")
    results.append(run_test(
        test_num=2,
        query_name="JOIN with Table Aliases",
        query_tree=query2,
        description="SELECT e.name, p.project_name FROM employees AS e INNER JOIN projects AS p ON e.id = p.employee_id",
        storage_manager=storage_manager,
        transaction_id=1002
    ))
    
    # TEST 3: Complex filter with ORDER BY and LIMIT
    query3 = build_query_tree("aggregate_filter")
    results.append(run_test(
        test_num=3,
        query_name="Complex Filter with ORDER BY and LIMIT",
        query_tree=query3,
        description="SELECT * FROM employees WHERE years >= 5 AND salary < 90000 ORDER BY salary DESC LIMIT 4",
        storage_manager=storage_manager,
        transaction_id=1003
    ))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("\n✓ ALL TESTS PASSED!")
    else:
        print(f"\n✗ {total - passed} test(s) failed")
    
    print("="*70)
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
