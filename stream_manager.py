import subprocess
import os
import uuid
import shutil
import sqlite3
import threading
import time
from typing import Dict, Any, List

class StreamManager:
    def __init__(self, output_dir: str = "hls_output", db_path: str = "restreamer.db"):
        self.output_dir = output_dir
        # Store DB relative to this file
        base_dir = os.path.dirname(__file__)
        self.db_path = os.path.join(base_dir, db_path)
        self.processes: Dict[str, subprocess.Popen] = {}
        
        os.makedirs(os.path.join(base_dir, self.output_dir), exist_ok=True)
        self._init_db()
        self._start_monitor_thread()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    input_url TEXT,
                    target_status TEXT
                )
            ''')
            conn.commit()

    def _start_monitor_thread(self):
        def monitor():
            # Wait a few seconds on startup before restarting streams
            time.sleep(2)
            while True:
                try:
                    self._check_and_restart_streams()
                except Exception as e:
                    print(f"Monitor error: {e}")
                time.sleep(10)

        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()

    def _check_and_restart_streams(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM channels WHERE target_status = 'active'")
            active_ids = [row[0] for row in cursor.fetchall()]

        for ch_id in active_ids:
            proc = self.processes.get(ch_id)
            if proc is None or proc.poll() is not None:
                # Process is dead or never started, restart it
                print(f"Auto-restarting channel {ch_id}")
                self._spawn_ffmpeg(ch_id)

    def _spawn_ffmpeg(self, channel_id: str):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT input_url FROM channels WHERE id = ?", (channel_id,))
            row = cursor.fetchone()
            if not row:
                return
            input_url = row[0]

        channel_dir = os.path.join(os.path.dirname(__file__), self.output_dir, channel_id)
        os.makedirs(channel_dir, exist_ok=True)
        output_file = os.path.join(channel_dir, "stream.m3u8")

        cmd = [
            "ffmpeg",
            "-y",
            # Konfigurasi Anti-Hang & Auto-Reconnect untuk input HTTP (Sangat penting untuk IPTV)
            "-reconnect", "1",
            "-reconnect_at_eof", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "2",
            "-rw_timeout", "10000000", # Timeout 10 detik agar FFmpeg tidak hang jika koneksi sumber putus
            "-fflags", "+genpts+discardcorrupt", # Memperbaiki timestamp yang rusak dan membuang frame korup
            "-i", input_url,
            "-c", "copy",
            "-f", "hls",
            "-hls_time", "6",
            "-hls_list_size", "15", 
            "-hls_flags", "delete_segments",
            output_file
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        self.processes[channel_id] = process

    def add_channel(self, name: str, input_url: str) -> str:
        channel_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO channels (id, name, input_url, target_status) VALUES (?, ?, ?, 'stopped')",
                           (channel_id, name, input_url))
            conn.commit()
        return channel_id

    def start_stream(self, channel_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE channels SET target_status = 'active' WHERE id = ?", (channel_id,))
            conn.commit()
        self._spawn_ffmpeg(channel_id)
        return True

    def stop_stream(self, channel_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE channels SET target_status = 'stopped' WHERE id = ?", (channel_id,))
            conn.commit()

        process = self.processes.get(channel_id)
        if process:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
            del self.processes[channel_id]
            
        # Hapus semua file video (m3u8 & ts) agar player (seperti VLC) langsung terputus (Error 404)
        channel_dir = os.path.join(os.path.dirname(__file__), self.output_dir, channel_id)
        if os.path.exists(channel_dir):
            shutil.rmtree(channel_dir, ignore_errors=True)
            
        return True

    def restart_stream(self, channel_id: str) -> bool:
        self.stop_stream(channel_id)
        time.sleep(0.5) # Beri sedikit waktu agar file terhapus dan port benar-benar bersih
        self.start_stream(channel_id)
        return True

    def edit_channel(self, channel_id: str, name: str, input_url: str) -> bool:
        channel = self.get_channel(channel_id)
        if not channel:
            return False
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE channels SET name = ?, input_url = ? WHERE id = ?", (name, input_url, channel_id))
            conn.commit()
            
        if channel["status"] == "active":
            self.restart_stream(channel_id)
        return True

    def get_channel(self, channel_id: str) -> Any:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM channels WHERE id = ?", (channel_id,))
            row = cursor.fetchone()
            if not row:
                return None
            
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(channel_id)
                if proc and proc.poll() is None:
                    status = "active"
                else:
                    status = "error"

            return {
                "id": row["id"],
                "name": row["name"],
                "input_url": row["input_url"],
                "status": status
            }

    def get_all_channels(self) -> List[Any]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM channels")
            rows = cursor.fetchall()

        result = []
        for row in rows:
            channel_id = row["id"]
            status = "stopped"
            if row["target_status"] == "active":
                proc = self.processes.get(channel_id)
                if proc and proc.poll() is None:
                    status = "active"
                else:
                    status = "error"
            
            result.append({
                "id": channel_id,
                "name": row["name"],
                "input_url": row["input_url"],
                "status": status
            })
        return result

    def delete_channel(self, channel_id: str) -> bool:
        self.stop_stream(channel_id)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM channels WHERE id = ?", (channel_id,))
            conn.commit()

        # Clean up files
        channel_dir = os.path.join(os.path.dirname(__file__), self.output_dir, channel_id)
        if os.path.exists(channel_dir):
            shutil.rmtree(channel_dir, ignore_errors=True)
            
        return True
