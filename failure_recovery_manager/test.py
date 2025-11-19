from logFile import logFile
from log import log
from datetime import datetime
import random

lf = logFile()

for i in range (10):
        transaction_id = i + 1
        action = (i + 1) % 4
        timestamp = datetime.now()
        old_data = {
                "attr1": f"string:{random.randint(1, 2000)}",
                "attr2": f"{random.randint(1, 2000)}_char",
                "attr3": random.randint(1, 1000),
                "attr4": random.randint(1001, 2000)
        }
        new_data = {
                "attr1": f"string:{random.randint(2001, 4000)}",
                "attr2": f"{random.randint(2001, 4000)}_char",
                "attr3": random.randint(2001, 3000),
                "attr4": random.randint(3001, 4000)
        }
        # random.seed(10)
        table_name = f"table_{random.random()*(10**10)}"

        l = log(
                transaction_id=transaction_id,
                action=action,
                timestamp=timestamp,
                old_data=old_data,
                new_data=new_data,
                table_name=table_name
        )

        lf.write_log(l)

        # l.display()

lf.load_file()
ll = lf.get_logs()

for l in ll:
        l.display()


