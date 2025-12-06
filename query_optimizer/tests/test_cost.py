from query_optimizer.cost import CostCalculator
from query_optimizer.optimization_engine import OptimizationEngine
from storage_manager.models import Statistic

def main():
    dummy_stats: dict[str, Statistic] = {

        "users": Statistic(
            n_r=1000,       
            l_r=100,        
            f_r=40,
            b_r=25,
            V_a_r={
                "id": 1000, 
                "name": 900,
                "email": 1000
            },
            indexes={
                "id": {"type": "hash"},
                "name": {"type": "btree", "height": 3}
            }
        ),

        "profiles": Statistic(
            n_r=800,
            l_r=250,
            f_r=16,         
            b_r=50,         
            V_a_r={
                "id": 800,
                "user_id": 800,
                "bio": 800
            },
            indexes={
                "user_id": {"type": "btree", "height": 2}
            }
        ),


        "orders": Statistic(
            n_r=10000,      
            l_r=60,
            f_r=68,
            b_r=148,        
            V_a_r={
                "id": 10000,
                "user_id": 1000,
                "total": 500 
            },
            indexes={
                "id": {"type": "hash"},
                "user_id": {"type": "btree", "height": 3},
                "total": {"type": "btree", "height": 3}
            }
        ),

        "products": Statistic(
            n_r=2000,
            l_r=150,
            f_r=27,
            b_r=75,
            V_a_r={
                "id": 2000,
                "category": 20,
                "price": 500,
                "stock": 100,
                "discount": 10
            },
            indexes={
                "id": {"type": "hash"},
                "category": {"type": "btree", "height": 2},
                "price": {"type": "btree", "height": 3}
            }
        ),

        "employees": Statistic(
            n_r=50,
            l_r=120,
            f_r=34,
            b_r=2,
            V_a_r={
                "id": 50,
                "department": 5,
                "salary": 30
            },
            indexes={
                "department": {"type": "hash"}
            }
        ),

        "accounts": Statistic(
            n_r=5000,
            l_r=50,
            f_r=80,
            b_r=63,
            V_a_r={
                "id": 5000,
                "balance": 4000
            },
            indexes={
                "id": {"type": "hash"}
            }
        ),

        "logs": Statistic(
            n_r=50000,
            l_r=200,
            f_r=20,
            b_r=2500,
            V_a_r={
                "id": 50000,
                "message": 45000
            },
            indexes={}
        ),

        "payroll": Statistic(
            n_r=50,
            l_r=20,
            f_r=200,
            b_r=1,
            V_a_r={
                "salary": 20
            },
            indexes={}
        )
    }

    test_queries = [

    "SELECT id, name FROM users WHERE id = 101;",

    "SELECT category, description FROM products WHERE price > 5000000;",

    "SELECT message FROM logs WHERE message = 'Error 404';",

    "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id;",

    "SELECT users.name, profiles.bio, orders.total FROM users JOIN profiles ON users.id = profiles.user_id JOIN orders ON users.id = orders.user_id;",

    "SELECT name, bonus FROM employees WHERE department = 'IT' AND salary > 10000000;",

    "SELECT * FROM products WHERE category = 'Electronics' OR stock < 5;",

    "UPDATE accounts SET balance = balance + 50000 WHERE id = 10;",

    "DELETE FROM logs WHERE id < 1000;",

    "SELECT id, name, bio FROM users NATURAL JOIN profiles ORDER BY name LIMIT 10;"
    ]

    engine = OptimizationEngine()
    calculator = CostCalculator(dummy_stats)

    for i, query in enumerate(test_queries):
        # if (i != 0):
        #     continue
        print(f"--- Query {i+1} ---")
        print(f"SQL: {query}")
        parse_query = engine.parse_query(query)
        cost = calculator.get_cost(parse_query.query_tree)
        print(cost)
        print()

if __name__ == "__main__":
    main()