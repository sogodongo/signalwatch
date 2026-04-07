import time
import math
from collections import defaultdict, deque
from dotenv import load_dotenv
import os

load_dotenv()

WINDOW_SIZE_SECONDS = int(os.getenv("WINDOW_SIZE_SECONDS", "300"))
# Minimum transactions needed before anomaly detection kicks in —
# can't compute meaningful statistics from 1 or 2 data points
MIN_WINDOW_SIZE = 5


class UserWindow:
    """
    Maintains a rolling time window of transactions for a single user.
    Uses a deque for O(1) append and popleft operations.
    """

    def __init__(self, window_seconds: int = WINDOW_SIZE_SECONDS):
        self.window_seconds = window_seconds
        # Each entry is (timestamp, amount)
        self._entries: deque[tuple[float, float]] = deque()

    def add(self, amount: float, timestamp: float = None):
        ts = timestamp or time.time()
        self._entries.append((ts, amount))
        self._evict_expired(ts)

    def _evict_expired(self, current_time: float):
        """Remove entries older than the window size."""
        cutoff = current_time - self.window_seconds
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.popleft()

    def stats(self) -> dict:
        """
        Computes rolling statistics over the current window.
        Returns None fields when there's insufficient data.
        """
        self._evict_expired(time.time())
        amounts = [amt for _, amt in self._entries]
        n = len(amounts)

        if n < MIN_WINDOW_SIZE:
            return {
                "count":  n,
                "mean":   None,
                "stddev": None,
                "min":    min(amounts) if amounts else None,
                "max":    max(amounts) if amounts else None,
                "ready":  False,
            }

        mean   = sum(amounts) / n
        stddev = math.sqrt(sum((x - mean) ** 2 for x in amounts) / n)

        return {
            "count":  n,
            "mean":   round(mean, 2),
            "stddev": round(stddev, 2),
            "min":    round(min(amounts), 2),
            "max":    round(max(amounts), 2),
            "ready":  True,
        }

    def __len__(self):
        return len(self._entries)


class WindowProcessor:
    """
    Maintains sliding windows for all active users.
    Thread-safe for single-process use — add a lock if using multiple threads.
    """

    def __init__(self, window_seconds: int = WINDOW_SIZE_SECONDS):
        self.window_seconds = window_seconds
        self._windows: dict[str, UserWindow] = defaultdict(
            lambda: UserWindow(window_seconds)
        )

    def process(self, user_id: str, amount: float, timestamp: float = None) -> dict:
        """
        Adds a transaction to the user's window and returns updated stats.
        This is called for every transaction before anomaly detection.
        """
        self._windows[user_id].add(amount, timestamp)
        stats = self._windows[user_id].stats()
        stats["user_id"] = user_id
        return stats

    def get_stats(self, user_id: str) -> dict:
        """Returns current window stats for a user without adding a transaction."""
        if user_id not in self._windows:
            return {"user_id": user_id, "count": 0, "ready": False}
        stats = self._windows[user_id].stats()
        stats["user_id"] = user_id
        return stats

    def active_users(self) -> list[str]:
        """Returns list of users with active windows."""
        return list(self._windows.keys())
