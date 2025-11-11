"""
Log handling for transaction operations
"""

from datetime import datetime
from typing import List, Optional
from .enums import ActionType
import os


class LogEntry:
    """Represents a single log entry"""
    
    def __init__(self, transaction_id: int, object_id: str, action: Optional[ActionType],
                 timestamp: datetime, status: str, event_type: str):
        self.transaction_id: int = transaction_id
        self.object_id: str = object_id
        self.action: Optional[ActionType] = action
        self.timestamp: datetime = timestamp
        self.status: str = status
        self.event_type: str = event_type
    
    def __str__(self) -> str:
        """String representation of log entry"""
        action_str = self.action.value if self.action else "N/A"
        return (f"[{self.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}] "
                f"T{self.transaction_id} | {self.event_type} | "
                f"Object: {self.object_id} | Action: {action_str} | Status: {self.status}")


class LogHandler:
    """Handles logging of transaction operations"""
    
    def __init__(self, log_file: str):
        self.log_entries: List[LogEntry] = []
        self.log_file: str = log_file
        self._ensure_log_directory()
    
    def _ensure_log_directory(self) -> None:
        """Ensure the directory for log file exists"""
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
    
    def log_access(self, transaction_id: int, object_id: str, 
                   action: ActionType, status: str) -> None:
        """Log an access operation"""
        timestamp = datetime.now()
        log_entry = LogEntry(
            transaction_id=transaction_id,
            object_id=object_id,
            action=action,
            timestamp=timestamp,
            status=status,
            event_type="ACCESS"
        )
        self.log_entries.append(log_entry)
        self._write_to_file(log_entry)
    
    def log_transaction_event(self, transaction_id: int, event: str) -> None:
        """Log a transaction event"""
        timestamp = datetime.now()
        log_entry = LogEntry(
            transaction_id=transaction_id,
            object_id="",
            action=None,
            timestamp=timestamp,
            status="",
            event_type=event
        )
        self.log_entries.append(log_entry)
        self._write_to_file(log_entry)
    
    def _write_to_file(self, log_entry: LogEntry) -> None:
        """Write a single log entry to file"""
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(str(log_entry) + '\n')
        except Exception as e:
            print(f"Warning: Failed to write to log file: {e}")
    
    def flush(self) -> None:
        """Flush logs to file"""
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                for entry in self.log_entries:
                    f.write(str(entry) + '\n')
            # Clear in-memory entries after flushing
            self.log_entries.clear()
        except Exception as e:
            print(f"Warning: Failed to flush logs to file: {e}")
    
    def get_logs_for_transaction(self, transaction_id: int) -> List[LogEntry]:
        """Get all logs for a specific transaction"""
        return [entry for entry in self.log_entries if entry.transaction_id == transaction_id]
    
    def clear(self) -> None:
        """Clear all log entries from memory"""
        self.log_entries.clear()
    
    def get_all_logs(self) -> List[LogEntry]:
        """Get all log entries"""
        return self.log_entries.copy()
