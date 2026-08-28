import subprocess
import os
import uuid
import sqlite3
import threading
import time
import shutil

class StreamManager:
    def __init__(self, db_path: str = "restreamer.db"):
        base_dir = os.path.dirname(__file__)
        self.db_path = os.path.join(base_dir, db_path)
        
        # RAM-Disk Directory (Kembali ke RAM karena sangat ringan dengan list_size 5)
        self.output_dir = "/dev/shm/pystreamer_hls"
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.processes = {}
        
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
            
            hls_file = os.path.join(self.output_dir, ch_id, "stream.m3u8")
            
            if proc is None or proc.poll() is not None:
                needs_restart = True
            else:
                if os.path.exists(hls_file):
                    mtime = os.path.getmtime(hls_file)
                    if (time.time() - mtime) > 15:
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

        channel_dir = os.path.join(self.output_dir, channel_id)
        if os.path.exists(channel_dir):
            shutil.rmtree(channel_dir)
        os.makedirs(channel_dir, exist_ok=True)
        
        output_file = os.path.join(channel_dir, "stream.m3u8")

        # KEMBALI KE PENGATURAN SUPER STABIL (hls_time 2, list_size 5)
        cmd = [
            "ffmpeg", "-y", "-reconnect", "1", "-reconnect_at_eof", "1", 
            "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
            "-rw_timeout", "10000000", "-fflags", "+genpts+discardcorrupt", 
            "-i", input_url, "-c", "copy",
            "-f", "hls", 
            "-hls_time", "2", 
            "-hls_list_size", "5", 
            "-hls_flags", "delete_segments+omit_endlist",
            "-hls_segment_type", "mpegts",
            "-strftime", "1",
            "-hls_segment_filename", os.path.join(channel_dir, "seg-%s-%%04d.ts"),
            output_file
        ]

        process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes[channel_id] = process

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
        channel_dir = os.path.join(self.output_dir, channel_id)
        if os.path.exists(channel_dir): shutil.rmtree(channel_dir)
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
