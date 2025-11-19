class log:
    def __init__(self, transaction_id, action, timestamp, old_data = None, new_data = None, table_name : str = None) -> None:
        self.transaction_id = transaction_id
        self.action = action
        self.timestamp = timestamp
        self.old_data = old_data
        self.new_data = new_data
        self.table_name = table_name
    
    def __str__(self):
        return(
            f"transaction_id:{self.transaction_id}, "
            f"action:{self.action}, "
            f"timestamp:{self.timestamp}, "
            f"old_data:{self.old_data}, "
            f"new_data:{self.new_data}, "
            f"table_name:{self.table_name}, "
        )


class actiontype(enumerate):
    start = 0
    write = 1
    commit = 2
    abort = 3