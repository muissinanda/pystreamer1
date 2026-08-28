import subprocess
import os
import uuid
import sqlite3
import threading
import time
import asyncio
from typing import Dict, Any, List, Tuple
from collections import deque

class StreamManager:
    def __init__(self, db_path: str = "restreamer.db"):
        self.db_path = os.path.join(os.path.dirname(__file__), db_path)
        self.processes: Dict[str, subprocess.Popen] = {}
        self.subscribers: Dict[str, List[Tuple[asyncio.Queue, asyncio.AbstractEventLoop]]] = {}
        self.last_read_time: Dict[str, float] = {}
        self.backlogs: Dict[str, deque] = {} 
        self._init_db()
        threading.Thread(target=self._monitor, daemon=True).start()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT, input_url TEXT, target_status TEXT)''')
            conn.commit()

    def _monitor(self):
        while True:
            time.sleep(3)
            try:
                with sqlite3.connect(self.db_path) as conn:
                    active_ids = [r[0] for r in conn.execute("SELECT id FROM channels WHERE target_status = 'active'").fetchall()]
                for ch_id in active_ids:
                    proc = self.processes.get(ch_id)
                    needs_restart = False
                    if not proc or proc.poll() is not None:
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
                        self._start_ffmpeg(ch_id)
            except Exception: pass

    def _start_ffmpeg(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT input_url FROM channels WHERE id = ?", (channel_id,)).fetchone()
            if not row: return
            input_url = row[0]

        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-reconnect", "1", "-reconnect_at_eof", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
            "-rw_timeout", "10000000",
            "-i", input_url, 
            "-c", "copy", 
            "-f", "mpegts", 
            "pipe:1"
        ]
        
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        self.processes[channel_id] = proc
        self.last_read_time[channel_id] = time.time()
        
        # Simpan 80 chunk x 64KB = ~5MB backlog
        self.backlogs[channel_id] = deque(maxlen=80) 
            
        threading.Thread(target=self._read_stdout, args=(channel_id, proc), daemon=True).start()

    def _read_stdout(self, channel_id, proc):
        backlog = self.backlogs.get(channel_id)
        try:
            while proc.poll() is None:
                chunk = proc.stdout.read(65536)
                if not chunk: break
                
                self.last_read_time[channel_id] = time.time()
                if backlog is not None:
                    backlog.append(chunk)
                
                subs = self.subscribers.get(channel_id, [])
                for q, loop in subs.copy():
                    def _put(q=q, chunk=chunk):
                        try: q.put_nowait(chunk)
                        except asyncio.QueueFull: pass
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(_put)
        except: pass

    def add_subscriber(self, channel_id: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        if channel_id not in self.subscribers:
            self.subscribers[channel_id] = []
        self.subscribers[channel_id].append((queue, loop))
        
        # Tembakkan backlog ke queue pemirsa baru secara instan
        if channel_id in self.backlogs:
            backlog_copy = list(self.backlogs[channel_id])
            def _inject():
                for c in backlog_copy:
                    try: queue.put_nowait(c)
                    except asyncio.QueueFull: pass
            if not loop.is_closed():
                loop.call_soon_threadsafe(_inject)

    def remove_subscriber(self, channel_id: str, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        if channel_id in self.subscribers:
            try: self.subscribers[channel_id].remove((queue, loop))
            except ValueError: pass

    def get_channel(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM channels WHERE id = ?", (channel_id,)).fetchone()
            if not row: return None
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(channel_id)
                status = "active" if proc and proc.poll() is None else "error"
            return {"id": row["id"], "name": row["name"], "input_url": row["input_url"], "status": status}

    def get_all_channels(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM channels").fetchall()
        result = []
        for row in rows:
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(row["id"])
                status = "active" if proc and proc.poll() is None else "error"
            result.append({"id": row["id"], "name": row["name"], "input_url": row["input_url"], "status": status})
        return result

    def add_channel(self, name: str, input_url: str):
        channel_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT INTO channels (id, name, input_url, target_status) VALUES (?, ?, ?, 'stopped')", (channel_id, name, input_url))
            conn.commit()

    def delete_channel(self, channel_id: str):
        self.stop_stream(channel_id)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
            conn.commit()

    def start_stream(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE channels SET target_status = 'active' WHERE id = ?", (channel_id,))
            conn.commit()
        self._start_ffmpeg(channel_id)

    def stop_stream(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE channels SET target_status = 'stopped' WHERE id = ?", (channel_id,))
            conn.commit()
        proc = self.processes.get(channel_id)
        if proc:
            proc.terminate()
            try: proc.wait(timeout=2)
            except: proc.kill()
            del self.processes[channel_id]

    def restart_stream(self, channel_id: str):
        self.stop_stream(channel_id)
        time.sleep(1)
        self.start_stream(channel_id)
