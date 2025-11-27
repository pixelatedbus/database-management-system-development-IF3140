from .buffer import buffer, table
from .log import actiontype, log
from .recovery_criteria import RecoveryCriteria
from .logFile import logFile
import threading
from typing import List

# replace with the correct one later
from .fake_exec_result import ExecutionResult 

# TODO: MAKE THIS SINGLETON BRUH
class FailureRecovery:

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                # Double-checked locking to ensure thread safety
                if not cls._instance:
                    cls._instance = super(FailureRecovery, cls).__new__(cls)
        return cls._instance

    def __init__(self, wal_size=50):
        
        # singleton check if init or not
        if getattr(self, "_initialized", False):
            return

        self.lock = threading.RLock()
        self.buffer: buffer = []
        self.mem_wal: List[ExecutionResult] = [] # List of Execution Results, this is the write-ahead log
        self.logFile: logFile = logFile()
        self.wal_size: int = wal_size
        self.undo_list: List[int] = []

        # singleton class attr
        self._initialized = True
    
    def write_log(self, info: ExecutionResult):
        '''
        Important notice: this only updates the memory's write-ahead log, maybe some adjustment is needed in the future? idk
        for now lets say the following:
        
        - if a commit is to occur, write from mem_wal to .log file via logFile
        '''

        try:
            with self.lock:
                self.mem_wal.append(info)

                if "commit" in info.query.lower() or info.action.lower() == "commit":
                    for infos in self.mem_wal:
                        self.logFile.write_log_execRes(infos)
                
                # handles if mem_wal is over the determined log size
                elif len(self.mem_wal) > self.wal_size:
                    for infos in self.mem_wal:
                        self.logFile.write_log_execRes(infos)
                
                # to be more efficient, we can also insert the transaction ID into the undo list
                if "commit" not in info.query.lower() or info.action.lower() != "commit":
                    if info.transaction_id not in self.undo_list:
                        self.undo_list.append(info.transaction_id)

        except Exception as e:
            print(f"Error in Failure Recovery Manager;write_log: {e}")
        
        pass

    def _save_checkpoint(self):
        pass
    
    def recover(self, criteria: RecoveryCriteria = None):
        pass

