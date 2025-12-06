from .buffer import buffer, table
from .log import actiontype, log
from .recovery_criteria import RecoveryCriteria
from .logFile import logFile
from storage_manager.models import (
    DataWrite,
    DataDeletion,
    DataUpdate
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

    def __init__(self, wal_size=10):
        
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
                self.logFile.write_log_execRes(infos)
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

                # Check if action is commit (can be int 2 or string "commit")
                is_commit = ("commit" in info.query.lower()) or (info.action == 2) or (isinstance(info.action, str) and info.action.lower() == "commit")
                
                if is_commit:
                    for infos in self.mem_wal:
                        self.logFile.write_log_execRes(infos)
                
                # handles if mem_wal is over the determined log size
                elif len(self.mem_wal) > self.wal_size:
                    for infos in self.mem_wal:
                        self.logFile.write_log_execRes(infos)
                
                # to be more efficient, we can also insert the transaction ID into the undo list
                if not is_commit:
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
            old_data=self.undo_list.copy(),  # Store copy of undo list
            new_data=None,
            table_name=self.current_table_name
        )

        # Dalam method ini, semua entri dalam write-ahead log sejak checkpoint terakhir akan digunakan untuk memperbarui data di physical storage, agar data tetap sinkron.
        # Assume this is the part where we renew the .dat file

        self.logFile.write_log(checkpoint_log)
        
        # Clear mem_wal after checkpoint since all operations are now in stable storage
        self.mem_wal = []

        pass

    def _recover_transaction(self, transaction_id: int) -> List[DataWrite | DataDeletion | DataUpdate]:
        # Flush current memory logs first
        self._flush_mem_wal()
        l_list = self.logFile.get_logs()

        data_execs = [] # data executions for requesting to Storage Manager
        found_checkpoint = False

        # traverse the log list in reverse (newest to oldest)
        for i in range(len(l_list) - 1, -1, -1):
            l = l_list[i]

            # Track checkpoints but don't stop collecting - there may be multiple checkpoints
            if l.action == actiontype.checkpoint:
                found_checkpoint = True
            
            if l.transaction_id == transaction_id:
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

                    # If ANY checkpoint was found, this operation was flushed to storage
                    # and needs to be undone. Only operations still in buffer (no checkpoint
                    # encountered yet while going backwards) should be skipped.
                    if found_checkpoint:
                        # This operation was flushed during some checkpoint - undo it
                        data_undo = l.to_data_undo()
                        data_execs += data_undo
                    # else: still in buffer, QP will discard it

                elif l.action == actiontype.start:
                    # Reached transaction start, stop traversing
                    break
                # else: commit, abort, or checkpoint for this transaction - continue traversing

        abort_log = log(
            transaction_id=transaction_id,
            action=actiontype.abort,
            timestamp=datetime.now(),
            old_data={},
            new_data={}
        )
        self.logFile.write_log(abort_log)

        return data_execs
    
    def recover_system_crash(self) -> List[DataWrite | DataDeletion | DataUpdate]:
        # Flush current memory logs first (just in case but its probably empty anyways)
        self._flush_mem_wal()

        # Find and load most recent log files for recovery
        try:
            self.logFile.find_most_recent_logs()
        except FileNotFoundError as e:
            # No log files found - clean start
            return []

        l_list = self.logFile.get_logs()
        data_execs = [] # datawrites to return

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
                data_redo = l.to_data_redo()
                data_execs += data_redo
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
                    
                    data_undo = l.to_data_undo()
                    data_execs += data_undo
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
        
        return data_execs

    
    def recover(self, criteria: RecoveryCriteria = None) -> List[DataWrite | DataDeletion | DataUpdate]:
        '''
        Implemented so far: transactional recovery
        TODO: add recovery for system crash (?)
        '''
        if criteria == None:
            raise("Recovery Criteria cannot be None")
        
        # For now we assume its transactional
        self._recover_transaction(criteria.transaction_id)
        


