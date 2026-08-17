# test_runner.py
from core.message import Envelope
from core.queue import PersistentQueue
from nodes.transform.field_mapper import FieldMapper
from engine.runner import ChannelRunner

queue = PersistentQueue("queue.db")

# Define mapping rules for channel 'his_to_lis'
mapper = FieldMapper([
    {"source": "order_id", "target": "accession_num", "required": True},
    {"source": "test", "target": "test_code", "required": False}
])

runner = ChannelRunner("his_to_lis", queue, mapper)

# 1. Enqueue 1 valid message and 1 bad message missing 'order_id'
queue.enqueue(Envelope(channel_id="his_to_lis", raw_payload={"order_id": 5001, "test": "URINALYSIS"}))
queue.enqueue(Envelope(channel_id="his_to_lis", raw_payload={"test": "METABOLIC_PANEL"}))

print("Processing queue batch...")
runner.process_one()  # Should succeed
runner.process_one()  # Should hit DLQ due to missing order_id