"""
Row class representing a database object
"""

from typing import Dict, Any


class Row:
    """Represents a database row/object"""
    
    def __init__(self, object_id: str, table_name: str, data: Dict[str, Any]):
        self.object_id: str = object_id
        self.table_name: str = table_name
        self.data: Dict[str, Any] = data
