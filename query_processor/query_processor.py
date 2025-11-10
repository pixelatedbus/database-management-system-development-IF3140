from misc.storage import DataRetrieval, DataWrite, DataDeletion, Condition
from misc.optimizer import QueryTree, ParsedQuery

class QueryProcessor:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(QueryProcessor, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        pass