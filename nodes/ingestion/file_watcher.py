import os
import shutil
import threading

from core.transport import TransportMessage, to_envelope
from nodes.base import IngestionNode


class FileWatcher(IngestionNode):
    """Case 1 inbound: watches a directory for new files (.csv, .hl7, .txt)
    and enqueues their raw content. Polling-based (no `watchdog` dependency)
    — checks the directory every interval_s.

    The transport is format-agnostic: each file is delivered as one
    TransportMessage of raw content + filename metadata. No CSV/JSON parsing
    happens here — that belongs to a codec in the pipeline.

    A file is claimed by renaming it into a `.processing` suffix before
    reading, so a crash mid-read doesn't leave a file that gets silently
    reprocessed *or* silently skipped: on restart, any leftover
    `.processing` file is picked back up on the next scan.

    `csv_mode` is accepted for config compatibility but no longer used.
    """

    def __init__(self, directory: str, channel_id: str, queue,
                 interval_s: float = 5, extensions: tuple = (".csv", ".hl7", ".txt"),
                 max_queue_depth: int | None = None, csv_mode: str = "auto",
                 inbound_codec: str = "json"):
        if interval_s < 1:
            raise ValueError("interval_s must be >= 1")
        self.directory = directory
        self.channel_id = channel_id
        self.queue = queue
        self.interval_s = interval_s
        self.extensions = tuple(e.lower() for e in extensions)
        self.max_queue_depth = max_queue_depth
        self.csv_mode = csv_mode  # retained for config compatibility, unused
        self.inbound_codec = inbound_codec

        self.processed_dir = os.path.join(directory, "processed")
        self.failed_dir = os.path.join(directory, "failed")
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, on_message_callback=None) -> None:
        os.makedirs(self.directory, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.failed_dir, exist_ok=True)

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                         name=f"file-watcher-{self.channel_id}")
        self._thread.start()
        self._requeue_orphaned_processing_files()

    def stop(self) -> None:
        self._stop_event.set()

    def _requeue_orphaned_processing_files(self):
        """Files left mid-'.processing' from a crash go back to plain
        filenames so the next scan picks them up again."""
        for fname in os.listdir(self.directory):
            if fname.endswith(".processing"):
                original = fname[: -len(".processing")]
                try:
                    os.rename(os.path.join(self.directory, fname),
                              os.path.join(self.directory, original))
                except OSError:
                    pass

    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._scan_once()
            except Exception as e:
                print(f"[FileWatcher:{self.channel_id}] error: {e}", flush=True)
            self._stop_event.wait(self.interval_s)

    def _scan_once(self):
        if self.max_queue_depth is not None:
            depth = self.queue.queue_depth(self.channel_id)
            if depth >= self.max_queue_depth:
                return  # backpressure: leave files in place, try again next tick

        try:
            candidates = sorted(os.listdir(self.directory))
        except FileNotFoundError:
            return

        for fname in candidates:
            if not fname.lower().endswith(self.extensions):
                continue
            full_path = os.path.join(self.directory, fname)
            if not os.path.isfile(full_path):
                continue
            self._claim_and_process(full_path, fname)

    def _claim_and_process(self, full_path: str, fname: str):
        claimed_path = full_path + ".processing"
        try:
            os.rename(full_path, claimed_path)
        except OSError:
            return  # another process/thread claimed it first, or file vanished

        try:
            content = self._read_file(claimed_path)
            msg = TransportMessage(
                raw=content,
                source=self.channel_id,
                filename=fname,
            )
            self.queue.enqueue(to_envelope(self.channel_id, msg, self.inbound_codec))
            shutil.move(claimed_path, os.path.join(self.processed_dir, fname))
        except Exception as e:
            print(f"[FileWatcher:{self.channel_id}] failed to process {fname}: {e}", flush=True)
            try:
                shutil.move(claimed_path, os.path.join(self.failed_dir, fname))
            except OSError:
                pass

    def _read_file(self, path: str) -> str:
        """Reads the file's raw content. No format interpretation — CSV/JSON/
        HL7 structure is a codec concern, not the watcher's. ``newline=""``
        keeps every character intact (e.g. ``\\r`` segment separators in HL7
        are NOT translated to ``\\n`` by universal-newline handling)."""
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            return f.read()
