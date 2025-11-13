"""
Response interface for concurrency control operations
"""

from typing import Protocol


class Response(Protocol):
    """Response interface for concurrency control decisions"""
    allowed: bool
    message: str
