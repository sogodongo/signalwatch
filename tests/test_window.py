import sys
import time
sys.path.insert(0, ".")

from detection.window_processor import WindowProcessor

processor = WindowProcessor(window_seconds=10)

print("=== Building window for user_001 ===\n")

# Simulate 8 normal transactions
normal_amounts = [42.0, 38.0, 51.0, 45.0, 40.0, 47.0, 43.0, 39.0]
for i, amount in enumerate(normal_amounts):
    stats = processor.process("user_001", amount)
    print(f"Transaction {i+1}: KES {amount:.0f} | "
          f"window count={stats['count']} | "
          f"mean={stats['mean']} | "
          f"stddev={stats['stddev']} | "
          f"ready={stats['ready']}")

print(f"\nFinal window stats: {processor.get_stats('user_001')}")

# Now simulate a high-amount anomaly
print(f"\n=== Injecting high-amount anomaly ===\n")
anomaly_stats = processor.process("user_001", 850.0)
print(f"Anomaly transaction: KES 850 | "
      f"mean={anomaly_stats['mean']} | "
      f"stddev={anomaly_stats['stddev']}")

# Test window expiry
print(f"\n=== Testing window expiry (waiting 12 seconds) ===\n")
print("Waiting for window to expire...")
time.sleep(12)
expired_stats = processor.get_stats("user_001")
print(f"After expiry: count={expired_stats['count']} ready={expired_stats['ready']}")
