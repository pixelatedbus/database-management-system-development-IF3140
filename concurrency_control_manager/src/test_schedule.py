import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import unittest
from datetime import datetime
from schedule import Queue, PriorityItem, PriorityQueue, Schedule
from action import Action
from enums import ActionType, ActionStatus


class TestQueue(unittest.TestCase):
    """Test cases for Queue class"""

    def setUp(self):
        """Set up test fixtures"""
        self.queue = Queue()

    def test_enqueue_dequeue(self):
        """Test basic enqueue and dequeue operations"""
        self.queue.enqueue(1)
        self.queue.enqueue(2)
        self.queue.enqueue(3)

        self.assertEqual(self.queue.dequeue(), 1)
        self.assertEqual(self.queue.dequeue(), 2)
        self.assertEqual(self.queue.dequeue(), 3)

    def test_is_empty(self):
        """Test is_empty method"""
        self.assertTrue(self.queue.is_empty())
        self.queue.enqueue(1)
        self.assertFalse(self.queue.is_empty())
        self.queue.dequeue()
        self.assertTrue(self.queue.is_empty())

    def test_size(self):
        """Test size method"""
        self.assertEqual(self.queue.size(), 0)
        self.queue.enqueue(1)
        self.assertEqual(self.queue.size(), 1)
        self.queue.enqueue(2)
        self.assertEqual(self.queue.size(), 2)
        self.queue.dequeue()
        self.assertEqual(self.queue.size(), 1)

    def test_peek(self):
        """Test peek method"""
        self.assertIsNone(self.queue.peek())
        self.queue.enqueue(1)
        self.queue.enqueue(2)
        self.assertEqual(self.queue.peek(), 1)
        self.assertEqual(self.queue.size(), 2)  # Peek should not remove

    def test_dequeue_empty_raises_error(self):
        """Test that dequeue on empty queue raises IndexError"""
        with self.assertRaises(IndexError):
            self.queue.dequeue()


class TestPriorityItem(unittest.TestCase):
    """Test cases for PriorityItem class"""

    def test_comparison(self):
        """Test comparison operators"""
        item1 = PriorityItem("A", 1.0)
        item2 = PriorityItem("B", 2.0)
        item3 = PriorityItem("C", 1.0)

        self.assertTrue(item1 < item2)
        self.assertFalse(item2 < item1)
        self.assertTrue(item1 <= item3)
        self.assertTrue(item2 > item1)
        self.assertTrue(item2 >= item1)
        self.assertTrue(item1 == item3)


class TestPriorityQueue(unittest.TestCase):
    """Test cases for PriorityQueue class"""

    def setUp(self):
        """Set up test fixtures"""
        self.pq = PriorityQueue()

    def test_enqueue_dequeue_priority_order(self):
        """Test that items are dequeued in priority order"""
        self.pq.enqueue("Low", 10.0)
        self.pq.enqueue("High", 1.0)
        self.pq.enqueue("Medium", 5.0)

        self.assertEqual(self.pq.dequeue(), "High")   # Priority 1.0
        self.assertEqual(self.pq.dequeue(), "Medium") # Priority 5.0
        self.assertEqual(self.pq.dequeue(), "Low")    # Priority 10.0

    def test_is_empty(self):
        """Test is_empty method"""
        self.assertTrue(self.pq.is_empty())
        self.pq.enqueue("item", 1.0)
        self.assertFalse(self.pq.is_empty())
        self.pq.dequeue()
        self.assertTrue(self.pq.is_empty())

    def test_size(self):
        """Test size method"""
        self.assertEqual(self.pq.size(), 0)
        self.pq.enqueue("A", 1.0)
        self.assertEqual(self.pq.size(), 1)
        self.pq.enqueue("B", 2.0)
        self.assertEqual(self.pq.size(), 2)
        self.pq.dequeue()
        self.assertEqual(self.pq.size(), 1)

    def test_peek(self):
        """Test peek method"""
        self.assertIsNone(self.pq.peek())
        self.pq.enqueue("Low", 10.0)
        self.pq.enqueue("High", 1.0)
        self.assertEqual(self.pq.peek(), "High")
        self.assertEqual(self.pq.size(), 2)  # Peek should not remove

    def test_dequeue_empty_raises_error(self):
        """Test that dequeue on empty priority queue raises IndexError"""
        with self.assertRaises(IndexError):
            self.pq.dequeue()


class TestSchedule(unittest.TestCase):
    """Test cases for Schedule class"""

    def setUp(self):
        """Set up test fixtures"""
        self.schedule = Schedule()
        self.action1 = Action(1, 1, "obj1", ActionType.READ, datetime.now())
        self.action2 = Action(2, 2, "obj2", ActionType.WRITE, datetime.now())
        self.action3 = Action(3, 1, "obj3", ActionType.READ, datetime.now())

    def test_enqueue_and_get_next_action(self):
        """Test enqueueing and getting next action"""
        self.schedule.enqueue(self.action1)
        self.schedule.enqueue(self.action2)

        next_action = self.schedule.get_next_action()
        self.assertEqual(next_action, self.action1)

        next_action = self.schedule.get_next_action()
        self.assertEqual(next_action, self.action2)

    def test_mark_ready(self):
        """Test marking action as ready"""
        self.schedule.mark_ready(self.action1)
        self.assertEqual(len(self.schedule.ready_list), 1)
        self.assertEqual(self.schedule.ready_list[0], self.action1)

    def test_mark_blocked(self):
        """Test marking action as blocked"""
        self.schedule.mark_blocked(self.action1)
        self.assertEqual(self.action1.status, ActionStatus.Blocked)
        self.assertEqual(self.schedule.get_blocked_count(), 1)

    def test_get_next_action_priority(self):
        """Test that get_next_action respects priority order"""
        # Add to different queues
        self.schedule.mark_ready(self.action1)  # Highest priority
        self.schedule.enqueue(self.action2)      # Medium priority
        self.schedule.mark_blocked(self.action3) # Lowest priority

        # Should get from ready_list first
        self.assertEqual(self.schedule.get_next_action(), self.action1)
        # Then from input_queue
        self.assertEqual(self.schedule.get_next_action(), self.action2)
        # Finally from blocked_queue
        self.assertEqual(self.schedule.get_next_action(), self.action3)

    def test_remove_transaction_actions(self):
        """Test removing all actions for a transaction"""
        self.schedule.enqueue(self.action1)
        self.schedule.enqueue(self.action2)
        self.schedule.enqueue(self.action3)

        # Remove transaction 1 actions
        self.schedule.remove_transaction_actions(1)

        # Should only have action2 left (transaction 2)
        next_action = self.schedule.get_next_action()
        self.assertEqual(next_action.transaction_id, 2)
        self.assertIsNone(self.schedule.get_next_action())

    def test_is_empty(self):
        """Test is_empty method"""
        self.assertTrue(self.schedule.is_empty())
        self.schedule.enqueue(self.action1)
        self.assertFalse(self.schedule.is_empty())

    def test_get_queue_size(self):
        """Test get_queue_size method"""
        self.assertEqual(self.schedule.get_queue_size(), 0)
        self.schedule.enqueue(self.action1)
        self.assertEqual(self.schedule.get_queue_size(), 1)
        self.schedule.mark_ready(self.action2)
        self.assertEqual(self.schedule.get_queue_size(), 2)
        self.schedule.mark_blocked(self.action3)
        self.assertEqual(self.schedule.get_queue_size(), 3)


if __name__ == '__main__':
    unittest.main()
