import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

import Utils

def test_safe_print_locking():
    """Verifies that safe_print outputs correctly through the lock."""
    # Cannot mock Lock.acquire directly (read-only).
    # Instead, verify it prints to stdout without crashing.
    buf = StringIO()
    with patch("sys.stdout", buf):
        Utils.safe_print("Test message")
    assert "Test message" in buf.getvalue()

def test_safe_print_concurrency():
    """Stress test safe_print with multiple threads to ensure no crashes."""
    errors = []
    def worker():
        try:
            for i in range(10):
                Utils.safe_print(f"Thread {threading.current_thread().name} - line {i}")
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()
    
    assert len(errors) == 0
