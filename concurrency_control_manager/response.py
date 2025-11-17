"""
Response interface for concurrency control operations
"""

from typing import Protocol


class Response(Protocol):
    """Response interface for concurrency control decisions"""
    def __init__(self, allowed: bool, message: str):
        self.allowed: bool = allowed
        self.message: str = message
