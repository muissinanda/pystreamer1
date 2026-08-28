import subprocess
import os
import uuid
import sqlite3
import threading
import time
import asyncio
from typing import Dict, Any, List, Tuple

class StreamManager:
    def __init__(self, db_path: str = "restreamer.db"):
        base_dir = os.path.dirname(__file__)
        self.db_path = os.path.join(base_dir, db_path)
        self.processes: Dict[str, subprocess.Popen] = {}
        self.subscribers: Dict[str, List[Tuple[asyncio.Queue, asyncio.AbstractEventLoop]]] = {}
        self.last_read_time: Dict[str, float] = {}
        self._init_db()
        self._start_monitor_thread()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT, input_url TEXT, target_status TEXT)''')
            conn.commit()

    def _start_monitor_thread(self):
        def monitor():
            time.sleep(2)
            while True:
                try: self._check_and_restart_streams()
                except Exception: pass
                time.sleep(10)
        threading.Thread(target=monitor, daemon=True).start()

    def _check_and_restart_streams(self):
        with sqlite3.connect(self.db_path) as conn:
            active_ids = [row[0] for row in conn.cursor().execute("SELECT id FROM channels WHERE target_status = 'active'").fetchall()]

        for ch_id in active_ids:
            proc = self.processes.get(ch_id)
            needs_restart = False
            if proc is None or proc.poll() is not None:
                needs_restart = True
            else:
                last_time = self.last_read_time.get(ch_id, 0)
                if last_time > 0 and (time.time() - last_time > 30):
                    needs_restart = True
                    try:
                        proc.terminate()
                        proc.wait(timeout=2)
                    except: proc.kill()
            if needs_restart:
                self._spawn_ffmpeg(ch_id)

    def _spawn_ffmpeg(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.cursor().execute("SELECT input_url FROM channels WHERE id = ?", (channel_id,)).fetchone()
            if not row: return
            input_url = row[0]

        # PURE PASSTHROUGH TO PIPE
        cmd = [
            "ffmpeg", "-y", "-reconnect", "1", "-reconnect_at_eof", "1", 
            "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
            "-rw_timeout", "10000000", "-fflags", "+genpts+discardcorrupt", 
            "-i", input_url, "-c", "copy", "-f", "mpegts", "pipe:1"
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.processes[channel_id] = process
        self.last_read_time[channel_id] = time.time()
        threading.Thread(target=self._broadcast_stream, args=(channel_id, process), daemon=True).start()

    def _broadcast_stream(self, channel_id: str, process: subprocess.Popen):
        try:
            while process.poll() is None:
                chunk = process.stdout.read(65536)
                if not chunk: break
                self.last_read_time[channel_id] = time.time()
                
                subs = self.subscribers.get(channel_id, [])
                for q, loop in subs.copy():
                    def _put(q=q, chunk=chunk):
                        try: q.put_nowait(chunk)
                        except asyncio.QueueFull: pass
                    if not loop.is_closed(): loop.call_soon_threadsafe(_put)
        except Exception: pass

    def add_subscriber(self, channel_id: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        if channel_id not in self.subscribers: self.subscribers[channel_id] = []
        self.subscribers[channel_id].append((queue, loop))

    def remove_subscriber(self, channel_id: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        if channel_id in self.subscribers:
            try: self.subscribers[channel_id].remove((queue, loop))
            except ValueError: pass

    def add_channel(self, name: str, input_url: str) -> str:
        channel_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.cursor().execute("INSERT INTO channels (id, name, input_url, target_status) VALUES (?, ?, ?, 'stopped')", (channel_id, name, input_url))
        return channel_id
    def start_stream(self, channel_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn: conn.cursor().execute("UPDATE channels SET target_status = 'active' WHERE id = ?", (channel_id,))
        self._spawn_ffmpeg(channel_id)
        return True
    def stop_stream(self, channel_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn: conn.cursor().execute("UPDATE channels SET target_status = 'stopped' WHERE id = ?", (channel_id,))
        process = self.processes.get(channel_id)
        if process:
            process.terminate()
            try: process.wait(timeout=3)
            except: process.kill()
            del self.processes[channel_id]
        return True
    def restart_stream(self, channel_id: str) -> bool:
        self.stop_stream(channel_id)
        time.sleep(0.5) 
        self.start_stream(channel_id)
        return True
    def edit_channel(self, channel_id: str, name: str, input_url: str) -> bool:
        channel = self.get_channel(channel_id)
        if not channel: return False
        with sqlite3.connect(self.db_path) as conn: conn.cursor().execute("UPDATE channels SET name = ?, input_url = ? WHERE id = ?", (name, input_url, channel_id))
        if channel["status"] == "active": self.restart_stream(channel_id)
        return True
    def get_channel(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.cursor().execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
            if not row: return None
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(channel_id)
                status = "active" if proc and proc.poll() is None else "error"
            return {"id": row["id"], "name": row["name"], "input_url": row["input_url"], "status": status}
    def get_all_channels(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.cursor().execute("SELECT * FROM channels").fetchall()
        result = []
        for row in rows:
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(row["id"])
                status = "active" if proc and proc.poll() is None else "error"
            result.append({"id": row["id"], "name": row["name"], "input_url": row["input_url"], "status": status})
        return result
    def delete_channel(self, channel_id: str) -> bool:
        self.stop_stream(channel_id)
        with sqlite3.connect(self.db_path) as conn: conn.cursor().execute("DELETE FROM channels WHERE id = ?", (channel_id,))
        return True
