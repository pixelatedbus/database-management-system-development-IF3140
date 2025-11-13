from typing import TypeVar, Generic, List, Optional
from .action import Action


T = TypeVar('T')


class Queue(Generic[T]):
    """Generic queue implementation"""

    def __init__(self):
        self.items: List[T] = []

    def enqueue(self, item: T) -> None:
        """Add item to the queue"""
        self.items.append(item)

    def dequeue(self) -> T:
        """Remove and return item from the queue"""
        if self.is_empty():
            raise IndexError("Cannot dequeue from empty queue")
        return self.items.pop(0)

    def is_empty(self) -> bool:
        """Check if queue is empty"""
        return len(self.items) == 0

    def size(self) -> int:
        """Get the size of the queue"""
        return len(self.items)

    def peek(self) -> Optional[T]:
        """Peek at the front item without removing it"""
        if self.is_empty():
            return None
        return self.items[0]

    def clear(self) -> None:
        """Clear all items from the queue"""
        self.items.clear()


class PriorityItem(Generic[T]):
    """Item wrapper for priority queue"""

    def __init__(self, item: T, priority: float):
        self.item: T = item
        self.priority: float = priority

    def __lt__(self, other: 'PriorityItem') -> bool:
        """Compare priority items - lower priority value = higher priority"""
        return self.priority < other.priority

    def __le__(self, other: 'PriorityItem') -> bool:
        """Less than or equal comparison"""
        return self.priority <= other.priority

    def __gt__(self, other: 'PriorityItem') -> bool:
        """Greater than comparison"""
        return self.priority > other.priority

    def __ge__(self, other: 'PriorityItem') -> bool:
        """Greater than or equal comparison"""
        return self.priority >= other.priority

    def __eq__(self, other: object) -> bool:
        """Equality comparison"""
        if not isinstance(other, PriorityItem):
            return False
        return self.priority == other.priority

    def __repr__(self) -> str:
        """String representation"""
        return f"PriorityItem(item={self.item}, priority={self.priority})"


class PriorityQueue(Generic[T]):
    """Priority queue implementation using min-heap"""

    def __init__(self):
        self.heap: List[PriorityItem[T]] = []

    def enqueue(self, item: T, priority: float) -> None:
        """Add item to the priority queue with priority"""
        priority_item = PriorityItem(item, priority)
        self.heap.append(priority_item)
        self._heapify_up(len(self.heap) - 1)

    def dequeue(self) -> T:
        """Remove and return highest priority item (lowest priority value)"""
        if self.is_empty():
            raise IndexError("Cannot dequeue from empty priority queue")

        # Swap first and last elements
        self.heap[0], self.heap[-1] = self.heap[-1], self.heap[0]

        # Remove and get the last element (originally first)
        priority_item = self.heap.pop()

        # Heapify down from root if heap is not empty
        if not self.is_empty():
            self._heapify_down(0)

        return priority_item.item

    def is_empty(self) -> bool:
        """Check if priority queue is empty"""
        return len(self.heap) == 0

    def size(self) -> int:
        """Get the size of the priority queue"""
        return len(self.heap)

    def peek(self) -> Optional[T]:
        """Peek at the highest priority item without removing it"""
        if self.is_empty():
            return None
        return self.heap[0].item

    def clear(self) -> None:
        """Clear all items from the priority queue"""
        self.heap.clear()

    def _heapify_up(self, index: int) -> None:
        """Move element up to maintain heap property"""
        if index == 0:
            return

        parent_index = (index - 1) // 2

        if self.heap[index] < self.heap[parent_index]:
            # Swap with parent
            self.heap[index], self.heap[parent_index] = self.heap[parent_index], self.heap[index]
            # Recursively heapify up
            self._heapify_up(parent_index)

    def _heapify_down(self, index: int) -> None:
        """Move element down to maintain heap property"""
        smallest = index
        left_child = 2 * index + 1
        right_child = 2 * index + 2

        # Check if left child exists and is smaller
        if left_child < len(self.heap) and self.heap[left_child] < self.heap[smallest]:
            smallest = left_child

        # Check if right child exists and is smaller
        if right_child < len(self.heap) and self.heap[right_child] < self.heap[smallest]:
            smallest = right_child

        # If smallest is not current index, swap and continue
        if smallest != index:
            self.heap[index], self.heap[smallest] = self.heap[smallest], self.heap[index]
            self._heapify_down(smallest)


class Schedule:
    """Manages scheduling of transaction actions"""

    def __init__(self):
        self.input_queue: Queue[Action] = Queue()
        self.ready_list: List[Action] = []
        self.blocked_queue: PriorityQueue[Action] = PriorityQueue()
        self.max_retry_count: int = 3

    def enqueue(self, action: Action) -> None:
        """Add action to the input queue"""
        self.input_queue.enqueue(action)

    def get_next_action(self) -> Optional[Action]:
        """Get the next action to execute

        Priority order:
        1. Actions in ready_list (already validated as ready)
        2. Actions in input_queue (newly arrived)
        3. Actions in blocked_queue (previously blocked, with priority)
        """
        # First, try to get from ready_list
        if self.ready_list:
            return self.ready_list.pop(0)

        # Second, try to get from input_queue
        if not self.input_queue.is_empty():
            return self.input_queue.dequeue()

        # Third, try to get from blocked_queue
        if not self.blocked_queue.is_empty():
            return self.blocked_queue.dequeue()

        # No actions available
        return None

    def mark_ready(self, action: Action) -> None:
        """Mark action as ready to be executed"""
        if action not in self.ready_list:
            self.ready_list.append(action)

    def mark_blocked(self, action: Action) -> None:
        """Mark action as blocked and add to blocked queue

        Priority is based on:
        - Wait time (longer wait = higher priority)
        - Retry count (fewer retries = higher priority)
        """
        action.mark_blocked()

        # Calculate priority: lower value = higher priority
        # Priority based on wait time and retry count
        wait_time = action.get_wait_time()
        priority = action.retry_count * 10 - wait_time

        self.blocked_queue.enqueue(action, priority)

    def remove_transaction_actions(self, transaction_id: int) -> None:
        """Remove all actions for a transaction from all queues"""
        # Remove from input_queue
        new_input_items = []
        while not self.input_queue.is_empty():
            action = self.input_queue.dequeue()
            if action.transaction_id != transaction_id:
                new_input_items.append(action)

        # Re-enqueue remaining items
        for action in new_input_items:
            self.input_queue.enqueue(action)

        # Remove from ready_list
        self.ready_list = [
            action for action in self.ready_list
            if action.transaction_id != transaction_id
        ]

        # Remove from blocked_queue
        new_blocked_items = []
        while not self.blocked_queue.is_empty():
            action = self.blocked_queue.dequeue()
            if action.transaction_id != transaction_id:
                # Store with priority for re-insertion
                wait_time = action.get_wait_time()
                priority = action.retry_count * 10 - wait_time
                new_blocked_items.append((action, priority))

        # Re-enqueue remaining items
        for action, priority in new_blocked_items:
            self.blocked_queue.enqueue(action, priority)

    def is_empty(self) -> bool:
        """Check if schedule is empty (no actions in any queue)"""
        return (
            self.input_queue.is_empty() and
            len(self.ready_list) == 0 and
            self.blocked_queue.is_empty()
        )

    def get_blocked_count(self) -> int:
        """Get count of blocked actions"""
        return self.blocked_queue.size()

    def get_queue_size(self) -> int:
        """Get the total size of all queues"""
        return (
            self.input_queue.size() +
            len(self.ready_list) +
            self.blocked_queue.size()
        )

    def clear_ready_list(self) -> None:
        """Clear the ready list"""
        self.ready_list.clear()

    def retry_blocked_actions(self) -> None:
        """Move blocked actions back to input queue for retry"""
        actions_to_retry = []

        while not self.blocked_queue.is_empty():
            action = self.blocked_queue.dequeue()
            action.increment_retry()

            if action.should_abort():
                # Skip actions that have exceeded max retries
                continue

            actions_to_retry.append(action)

        # Re-enqueue actions for retry
        for action in actions_to_retry:
            self.input_queue.enqueue(action)
