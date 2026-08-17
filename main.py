from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from datetime import datetime, timedelta
import secrets
import uvicorn
import os
from dotenv import load_dotenv

load_dotenv()

from stream_manager import StreamManager

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    yield
    # Shutdown: Stop all ffmpeg processes
    print("Shutting down... Terminating all ffmpeg streams.")
    for channel_id, process in list(manager.processes.items()):
        try:
            process.terminate()
            process.wait(timeout=2)
        except:
            process.kill()
    manager.processes.clear()

app = FastAPI(title="Nano Stream", lifespan=lifespan)
security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    env_user = os.getenv("APP_USERNAME", "muis24")
    env_pass = os.getenv("APP_PASSWORD", "master123")
    
    correct_username = secrets.compare_digest(credentials.username, env_user)
    correct_password = secrets.compare_digest(credentials.password, env_pass)
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# Setup Stream Manager
# Menggunakan /dev/shm agar file HLS disimpan di RAM (Memory) bukan di Hard Disk
OUTPUT_DIR = "/dev/shm/nanostream_hls"
manager = StreamManager(output_dir=OUTPUT_DIR, db_path="restreamer.db")

# Setup Static Files (for logo)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Analytics Tracking
viewer_tracking = {}  # channel_id: { ip_address: timestamp }
traffic_tracking = {} # channel_id: total_bytes_served

def get_channel_metrics(channel_id: str):
    now = datetime.now()
    timeout = timedelta(seconds=25)
    active_viewers = 0
    
    if channel_id in viewer_tracking:
        # Clean up old viewers
        active_ips = {ip: ts for ip, ts in viewer_tracking[channel_id].items() if now - ts < timeout}
        viewer_tracking[channel_id] = active_ips
        active_viewers = len(active_ips)
    
    bytes_served = traffic_tracking.get(channel_id, 0)
    
    # Format bytes
    if bytes_served < 1024 * 1024:
        traffic_str = f"{bytes_served / 1024:.1f} KB"
    elif bytes_served < 1024 * 1024 * 1024:
        traffic_str = f"{bytes_served / (1024 * 1024):.1f} MB"
    else:
        traffic_str = f"{bytes_served / (1024 * 1024 * 1024):.2f} GB"
        
    return {
        "viewers": active_viewers,
        "traffic": traffic_str
    }

# Setup Templates
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
os.makedirs(templates_dir, exist_ok=True)
templates = Jinja2Templates(directory=templates_dir)

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(verify_credentials)):
    channels = manager.get_all_channels()
    for c in channels:
        c["metrics"] = get_channel_metrics(c["id"])
        
    return templates.TemplateResponse(
        request=request, 
        name="index.html", 
        context={"channels": channels}
    )

@app.get("/api/metrics")
async def api_metrics(username: str = Depends(verify_credentials)):
    channels = manager.get_all_channels()
    result = {}
    for c in channels:
        result[c["id"]] = get_channel_metrics(c["id"])
    return result

@app.get("/hls/{channel_id}/{filename}")
async def serve_hls(request: Request, channel_id: str, filename: str):
    file_path = os.path.join(OUTPUT_DIR, channel_id, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    # Track viewer
    client_ip = request.client.host
    now = datetime.now()
    
    if channel_id not in viewer_tracking:
        viewer_tracking[channel_id] = {}
    viewer_tracking[channel_id][client_ip] = now
    
    # Track traffic
    file_size = os.path.getsize(file_path)
    traffic_tracking[channel_id] = traffic_tracking.get(channel_id, 0) + file_size
    
    headers = {}
    if filename.endswith(".m3u8"):
        headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        
    return FileResponse(file_path, headers=headers)

@app.post("/add")
async def add_channel(name: str = Form(...), input_url: str = Form(...), username: str = Depends(verify_credentials)):
    manager.add_channel(name, input_url)
    return RedirectResponse(url="/", status_code=303)

@app.post("/edit/{channel_id}")
async def edit_channel(channel_id: str, name: str = Form(...), input_url: str = Form(...), username: str = Depends(verify_credentials)):
    manager.edit_channel(channel_id, name, input_url)
    return RedirectResponse(url="/", status_code=303)

@app.post("/start/{channel_id}")
async def start_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.start_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/restart/{channel_id}")
async def restart_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.restart_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/stop/{channel_id}")
async def stop_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.stop_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/delete/{channel_id}")
async def delete_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.delete_channel(channel_id)
    if channel_id in viewer_tracking:
        del viewer_tracking[channel_id]
    if channel_id in traffic_tracking:
        del traffic_tracking[channel_id]
    return RedirectResponse(url="/", status_code=303)

@app.get("/play/{channel_id}", response_class=HTMLResponse)
async def play_channel(request: Request, channel_id: str, username: str = Depends(verify_credentials)):
    channel = manager.get_channel(channel_id)
    if not channel:
        return HTMLResponse("Channel not found", status_code=404)
        
    return templates.TemplateResponse(
        request=request,
        name="player.html", 
        context={"channel": channel}
    )

if __name__ == "__main__":
    port = int(os.getenv("APP_PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
