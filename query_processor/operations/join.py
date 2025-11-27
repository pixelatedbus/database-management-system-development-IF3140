from storage_manager import ConditionNode, Rows

def nested_loop_join(relation1: Rows, relation2: Rows, join_condition: ConditionNode) -> Rows:
    result_data = []
    for row1 in relation1.rows:
        for row2 in relation2.rows:
            if join_condition.evaluate({**row1, **row2}):
                combined_row = {**row1, **row2}
                result_data.append(combined_row)
    return Rows(result_data)