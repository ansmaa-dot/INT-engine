import json
from core.queue import PersistentQueue
from core.message import Envelope
from engine.config_loader import ChannelConfigRegistry

# 1. Inspect Config & Destination
registry = ChannelConfigRegistry("configs")
registry.load_all_configs()
print("1. Configs Loaded:", list(registry.configs.keys()))

# 2. Check Queue Connection
queue = PersistentQueue("queue.db")

# 3. Instantiate Runner
runner = registry.build_runner("his_to_lis", queue)
print("2. Runner Destination Target:", runner.destination.endpoint_url if hasattr(runner, 'destination') else "NO DESTINATION")

# 4. Enqueue & Process Synchronously
env = Envelope(channel_id="his_to_lis", raw=json.dumps({"order_id": 1234, "doctor_username": "dr_house", "test_code": "CBC"}))
queue.enqueue(env)
print("3. Enqueued Trace ID:", env.trace_id)

processed = runner.process_one()
print("4. Process One Result:", processed)