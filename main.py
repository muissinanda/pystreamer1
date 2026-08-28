from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from stream_manager import StreamManager
import os
from dotenv import load_dotenv
import asyncio

load_dotenv()
app = FastAPI(title="MPEGTS Restreamer")
security = HTTPBasic()
manager = StreamManager(db_path="restreamer.db")

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != os.environ.get("ADMIN_USER", "admin") or credentials.password != os.environ.get("ADMIN_PASS", "admin"):
        raise HTTPException(status_code=401, detail="Incorrect credentials", headers={"WWW-Authenticate": "Basic"})
    return credentials.username

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(verify_credentials)):
    return templates.TemplateResponse(request=request, name="index.html", context={"channels": manager.get_all_channels()})

@app.post("/add")
async def add_channel(name: str = Form(...), input_url: str = Form(...), username: str = Depends(verify_credentials)):
    manager.add_channel(name, input_url)
    return RedirectResponse(url="/", status_code=303)

@app.post("/delete/{channel_id}")
async def delete_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.delete_channel(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/start/{channel_id}")
async def start_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.start_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/stop/{channel_id}")
async def stop_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.stop_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.post("/restart/{channel_id}")
async def restart_channel(channel_id: str, username: str = Depends(verify_credentials)):
    manager.restart_stream(channel_id)
    return RedirectResponse(url="/", status_code=303)

@app.get("/play/{channel_id}", response_class=HTMLResponse)
async def play_channel(request: Request, channel_id: str):
    channel = manager.get_channel(channel_id)
    if not channel: raise HTTPException(status_code=404)
    return templates.TemplateResponse(request=request, name="player.html", context={"channel": channel})

@app.get("/stream/{channel_id}")
async def stream_video(request: Request, channel_id: str):
    channel = manager.get_channel(channel_id)
    if not channel or channel["status"] != "active": raise HTTPException(status_code=404)
    
    cond = manager.conditions.get(channel_id)
    data = manager.stream_data.get(channel_id)
    if not cond or data is None: raise HTTPException(status_code=500)

    async def stream_generator():
        # Kirim backlog pertama kali secara penuh
        with cond:
            yield bytes(data)
            last_len = len(data)
            
        while True:
            if await request.is_disconnected(): break
            
            # Gunakan asyncio.sleep agar tidak memblokir event loop FastAPI
            await asyncio.sleep(0.1)
            
            with cond:
                current_len = len(data)
                if current_len > last_len:
                    # Ambil data baru
                    chunk = bytes(data[last_len:current_len])
                    last_len = current_len
                    yield chunk
                elif current_len < last_len:
                    # Terjadi reset buffer (FFmpeg restart atau data terpotong)
                    last_len = current_len

    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
        "Connection": "keep-alive"
    }
    return StreamingResponse(stream_generator(), media_type="video/mp2t", headers=headers)
