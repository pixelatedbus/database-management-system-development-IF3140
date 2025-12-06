from failure_recovery_manager import FailureRecovery
from logFile import logFile
from log import log
from datetime import datetime
import random
from log import actiontype
from recovery_criteria import RecoveryCriteria

fr = FailureRecovery()

lf = logFile()

for i in range(1, 6):
    table_name = f"table_{random.randint(1, 3)}"

    # start log
    l = log(
        transaction_id=i,
        action=actiontype.start,
        timestamp=datetime.now(),
        old_data={},
        new_data={},
        table_name=table_name
    )
    lf.write_log(l)

    # 1-3 write logs
    for _ in range(random.randint(1, 3)):
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

        l = log(
            transaction_id=i,
            action=actiontype.write,
            timestamp=datetime.now(),
            old_data=old_data,
            new_data=new_data,
            table_name=table_name
        )
        lf.write_log(l)

    if i == 4: # so there's no commit log for abort test
        continue
    
    # commit log
    l = log(
        transaction_id=i,
        action=actiontype.commit,
        timestamp=datetime.now(),
        old_data={},
        new_data={},
        table_name=table_name
    )
    lf.write_log(l)

lf.load_file()
ll = lf.get_logs()

print(50 * "-")
for l in ll:
        l.display()

fr.logFile = lf
rc = RecoveryCriteria(transaction_id=4)
fr.recover(rc)
ll = fr.logFile.get_logs()

print(50 * "-")
for l in ll:
        l.display()




