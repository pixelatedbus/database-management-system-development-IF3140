from .buffer import buffer, table
from .log import actiontype, log
from .recovery_criteria import RecoveryCriteria
from .logFile import logFile
from storage_manager.models import (
    DataWrite 
)
import threading
from typing import List
from datetime import datetime

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

        self.current_transaction_id: int = 0
        self.current_table_name: str = ""

        # singleton class attr
        self._initialized = True
    
    def _flush_mem_wal(self):
        with self.lock:
            for infos in self.mem_wal:
                logFile.write_log_execRes(infos)
        self.mem_wal = []

    def write_log(self, info: ExecutionResult):
        '''
        Important notice: this only updates the memory's write-ahead log, maybe some adjustment is needed in the future? idk
        for now lets say the following:
        
        - if a commit is to occur, write from mem_wal to .log file via logFile
        '''

        # Maybe set the current transaction id here?
        self.current_transaction_id = info.transaction_id
        self.current_table_name = info.table_name

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

        checkpoint_log = log(
            transaction_id=self.current_transaction_id, 
            action=actiontype.checkpoint,
            timestamp=datetime.now(),
            old_data=self.undo_list,
            new_data=None,
            table_name=self.current_table_name
        )

        # Dalam method ini, semua entri dalam write-ahead log sejak checkpoint terakhir akan digunakan untuk memperbarui data di physical storage, agar data tetap sinkron.
        # Assume this is the part where we renew the .dat file

        self.logFile.write_log(checkpoint_log)

        pass

    def _recover_transaction(self, transaction_id: int) -> List[DataWrite]:
        self._flush_mem_wal()

        l_list = self.logFile.get_logs()

        dws = [] # datawrites to return
        found_checkpoint = False
        for i in range(len(l_list) - 1, -1, -1):
            l = l_list[i]
            if l.action == actiontype.checkpoint:
                found_checkpoint= True
            if l.transaction_id == transaction_id:
                if l.action == actiontype.write:
                    if not found_checkpoint:
                        continue

                    undo_log = log(
                        transaction_id=l.transaction_id,
                        action=actiontype.write,
                        timestamp=datetime.now(),
                        old_data=l.new_data,
                        new_data=l.old_data,
                        table_name=l.table_name
                    )
                    self.logFile.write_log(undo_log)
                    
                    datawrite_undo = l.to_datawrite_undo(pks=None)
                    dws.append(datawrite_undo)
                else: # start, commit, abort.
                    break
        abort_log = log(
            transaction_id=transaction_id,
            action=actiontype.abort,
            timestamp=datetime.now(),
            old_data={},
            new_data={}
        )
        self.logFile.write_log(abort_log)
        return dws
    
    def recover_system_crash(self) -> List[DataWrite]:
        self._flush_mem_wal()

        l_list = self.logFile.get_logs()
        dws = [] # datawrites to return

        i = len(l_list) - 1
        i_checkpoint = -1

        # find checkpoint phase
        while i >= 0:
            l = l_list[i]
            if l.action == actiontype.checkpoint:
                i_checkpoint = i
                break
            i -= 1

        if i_checkpoint == -1:
            self.undo_list = []
        else:
            self.undo_list = l_list[i].old_data
        i += 1

        # redo phase
        while i < len(l_list):
            l = l_list[i]
            if l.action == actiontype.write:
                datawrite_redo = l.to_datawrite_redo(pks=None)
                dws.append(datawrite_redo)
            elif l.action == actiontype.start:
                self.undo_list.append(l.transaction_id)
            elif l.action == actiontype.commit or l.action == actiontype.abort:
                self.undo_list.remove(l.transaction_id)

            i += 1

        i -= 1

        # undo phase
        while len(self.undo_list) > 0:
            l = l_list[i]
            if l.transaction_id in self.undo_list:
                if l.action == actiontype.write:
                    undo_log = log(
                        transaction_id=l.transaction_id,
                        action=actiontype.write,
                        timestamp=datetime.now(),
                        old_data=l.new_data,
                        new_data=l.old_data,
                        table_name=l.table_name
                    )
                    self.logFile.write_log(undo_log)
                    
                    datawrite_undo = l.to_datawrite_undo(pks=None)
                    dws.append(datawrite_undo)
                elif l.action == actiontype.start:
                    self.undo_list.remove(l.transaction_id)

                    abort_log = log(
                        transaction_id=l.transaction_id,
                        action=actiontype.abort,
                        timestamp=datetime.now(),
                        old_data={},
                        new_data={}
                    )
                    self.logFile.write_log(abort_log)
            
            i -= 1
        
        return dws

    
    def recover(self, criteria: RecoveryCriteria = None) -> List[DataWrite]:
        '''
        Implemented so far: transactional recovery
        TODO: add recovery for system crash (?)
        '''
        if criteria == None:
            raise("Recovery Criteria cannot be None")
        
        # For now we assume its transactional
        self._recover_transaction(criteria.transaction_id)
        


