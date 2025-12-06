"""
Response interface for concurrency control operations
"""

from typing import Protocol

class Response(Protocol):
    """Response interface for concurrency control decisions"""
    allowed: bool
    message: str
    value: any

class AlgorithmResponse:
    def __init__(self, allowed: bool, message: str, value: any = None, waiting: bool = False):
        self.allowed = allowed
        self.message = message
        self.value = value
        self.waiting = waiting  # True if transaction should wait and retry
