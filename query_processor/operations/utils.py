from misc.optimizer import *
from misc.storage import Rows

def limit(r:Rows, n:int) -> Rows:
    res = r.rows[:n]
    return Rows(res)

# sort berasumsi bahwa pemrosesan dapat dilakukan sepenuhnya di memori
# saat ini, sort berasumsi bahwa parameter yang diberikan kepadanya VALID
def sort(r:Rows, keys:list, is_ascend:list) -> Rows:
    sorted_data = r.rows[:] # copy data dari rows
    count = len(keys)

    # sorting dimulai di sini, dari key paling belakang dalam list
    idx = count-1
    while idx >= 0:
        key_name = keys[idx]
        ascending = is_ascend[idx]

        # python magic
        sorted_data.sort(key=lambda row: row[key_name], reverse=(not ascending))

        idx-=1
    
    return Rows(sorted_data)