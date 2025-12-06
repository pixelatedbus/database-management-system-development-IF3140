from query_optimizer.cost import CostCalculator
from query_optimizer.optimization_engine import OptimizationEngine
from storage_manager.models import Statistic
from storage_manager.storage_manager import StorageManager

def main():

    test_queries = [
    # Query 1: Join dengan Filter dan Projection (menggunakan index)
    """
    SELECT s.student_id, s.name, e.grade
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id
    WHERE s.gpa > 3.5
    """,
    
    # Query 2: Multiple Joins dengan Aggregation-like filter
    """
    SELECT s.name, c.course_name, p.name
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id
    JOIN courses c ON e.course_id = c.course_id
    JOIN professors p ON c.professor_id = p.professor_id
    WHERE s.major = 'Computer Science' AND c.department = 'CS'
    """,
    
    # Query 3: Subquery di WHERE dengan EXISTS
    """
    SELECT s.student_id, s.name, s.gpa
    FROM students s
    WHERE EXISTS (
        SELECT 1 
        FROM enrollments e 
        WHERE e.student_id = s.student_id AND e.grade = 'A'
    )
    """,
    
    # Query 4: Subquery di WHERE dengan IN
    """
    SELECT p.professor_id, p.name, p.department
    FROM professors p
    WHERE p.professor_id IN (
        SELECT c.professor_id 
        FROM courses c 
        WHERE c.department = 'Mathematics'
    )
    """,
    
    # Query 5: Join dengan Filter di kedua tabel (optimal untuk push-down)
    """
    SELECT s.name, e.grade, e.semester
    FROM students s
    JOIN enrollments e ON s.student_id = e.student_id
    WHERE s.age > 20 AND e.semester = 1
    """,
    
    # Query 6: Cross Product dengan Filter (bisa dioptimasi jadi join)
    """
    SELECT s.name, c.course_name
    FROM students s, courses c
    WHERE s.major = c.department AND s.gpa > 3.0
    """,
    
    # Query 7: Natural Join dengan Projection
    """
    SELECT student_id, course_id, grade
    FROM students
    NATURAL JOIN enrollments
    WHERE age < 25
    """,
    
    # Query 8: Complex dengan OR dan Multiple Indexes
    """
    SELECT s.student_id, s.name, s.major, s.gpa
    FROM students s
    WHERE s.gpa > 3.8 OR s.major = 'Physics'
    """
]
    query =    """
    SELECT s.student_id, s.name, s.gpa
    FROM students s
    WHERE EXISTS (
        SELECT 1 
        FROM enrollments e 
        WHERE e.student_id = s.student_id AND e.grade = 'A'
    )
    """
    sm = StorageManager()
    calcu = CostCalculator(sm.get_stats())
    engine = OptimizationEngine()
    print(engine.parse_query(query).query_tree.tree())
    print(calcu.get_cost(engine.parse_query(query).query_tree))
    print(engine.statistics)

    for i, query in enumerate(test_queries):
        # if (i != 0):
        #     continue
        print(f"--- Query {i+1} ---")
        print(f"SQL: {query}")
        parse_query = engine.parse_query(query)
        cost = engine.get_cost(parse_query)
        print(cost)
        print()

if __name__ == "__main__":
    main()