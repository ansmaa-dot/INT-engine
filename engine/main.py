import time
import threading
import sys
import os

# Ensure core modules are visible
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.queue import PersistentQueue
from engine.config_loader import ChannelConfigRegistry

ACTIVE_WORKER_COUNTS = {}   # channel_id -> number of worker threads currently running for it
ACTIVE_THREADS = {}         # channel_id -> list of worker threads
ACTIVE_INGESTION = {}       # channel_id -> ingestion node instance (http_poller/mllp_server/file_watcher)
BACKGROUND_INGESTION_TYPES = {"http_poller", "mllp_server", "file_watcher", "db_poller"}
RECLAIM_INTERVAL_S = 30
STALE_PROCESSING_THRESHOLD_S = 120

registry = ChannelConfigRegistry("queue.db")


def start_ingestion_node(channel_id, config, queue):
    """Starts a background ingestion source for channels configured with an
    active ingestion_type that isn't 'http_webhook' (webhooks are handled by
    api/app.py since they need to live on the Flask process)."""
    if channel_id in ACTIVE_INGESTION:
        return
    itype = config.get("ingestion_type")
    if itype not in BACKGROUND_INGESTION_TYPES:
        return
    try:
        node = registry.build_ingestion(channel_id, queue)
        if node:
            node.start()
            ACTIVE_INGESTION[channel_id] = node
            print(f"===> [INGESTION STARTED] {itype} for channel: {channel_id}", flush=True)
    except Exception as e:
        print(f"===> [INGESTION ERROR] Failed to start ingestion for {channel_id}: {e}", flush=True)


def stop_ingestion_node(channel_id):
    node = ACTIVE_INGESTION.pop(channel_id, None)
    if node:
        try:
            node.stop()
        except Exception:
            pass


def start_channel_workers(channel_id, concurrency: int = 1):
    """Starts `concurrency` worker threads for a channel, each independently
    draining the shared persistent queue. Safe because PersistentQueue's
    dequeue_available() atomically claims a row before returning it — no two
    threads (or, if this daemon is ever run as multiple OS processes against
    the same queue.db, no two processes either) can pull the same message."""
    existing = ACTIVE_WORKER_COUNTS.get(channel_id, 0)
    if existing >= concurrency:
        return
    to_start = concurrency - existing
    ACTIVE_WORKER_COUNTS[channel_id] = concurrency
    ACTIVE_THREADS.setdefault(channel_id, [])

    for i in range(to_start):
        worker_index = existing + i
        thread = threading.Thread(
            target=_worker_loop, args=(channel_id, worker_index), daemon=True,
            name=f"worker-{channel_id}-{worker_index}",
        )
        ACTIVE_THREADS[channel_id].append(thread)
        thread.start()


def _worker_loop(channel_id, worker_index):
    thread_queue = PersistentQueue("queue.db")
    print(f"===> [WORKER STARTED] {channel_id} worker #{worker_index}", flush=True)

    while True:
        config = registry.load_config(channel_id)

        if not config or not config.get("enabled", True):
            print(f"===> [WORKER STOPPED] Channel {channel_id} worker #{worker_index}: deleted or disabled.", flush=True)
            ACTIVE_WORKER_COUNTS.pop(channel_id, None)
            ACTIVE_THREADS.pop(channel_id, None)
            stop_ingestion_node(channel_id)
            break

        if config.get("status") == "paused":
            time.sleep(1)
            continue

        try:
            runner = registry.build_runner(channel_id, thread_queue)
            processed = runner.process_one()
            if processed:
                print(f"===> [WORKER] Processed payload for channel: {channel_id} (worker #{worker_index})", flush=True)
            else:
                time.sleep(1)
        except Exception as e:
            print(f"===> [WORKER ERROR] Channel {channel_id} worker #{worker_index}: {e}", flush=True)
            time.sleep(1)


def _reclaim_loop():
    """Runs alongside the config-sync loop: puts back any message stuck in
    PROCESSING because the worker that claimed it died mid-flight, so it
    doesn't sit invisible forever."""
    reclaim_queue = PersistentQueue("queue.db")
    while True:
        try:
            n = reclaim_queue.reclaim_stale_processing(STALE_PROCESSING_THRESHOLD_S)
            if n:
                print(f"===> [RECLAIM] {n} stale PROCESSING message(s) put back to QUEUED", flush=True)
        except Exception as e:
            print(f"===> [RECLAIM ERROR] {e}", flush=True)
        time.sleep(RECLAIM_INTERVAL_S)


def main():
    print("[INIT] Starting Channel Worker Engine Daemon...", flush=True)
    ingestion_queue = PersistentQueue("queue.db")

    threading.Thread(target=_reclaim_loop, daemon=True, name="reclaim-loop").start()

    while True:
        try:
            configs = registry.load_all_configs()
            for cid, conf in configs.items():
                if conf.get("enabled", True):
                    concurrency = max(1, int(conf.get("concurrency") or 1))
                    start_channel_workers(cid, concurrency)
                    if cid not in ACTIVE_INGESTION:
                        start_ingestion_node(cid, conf, ingestion_queue)
                else:
                    stop_ingestion_node(cid)
        except Exception as e:
            print(f"[WORKER DAEMON ERROR] Config sync failed: {e}", flush=True)

        time.sleep(2)  # Check for newly registered, enabled, or reconfigured channels every 2s


if __name__ == "__main__":
    main()
