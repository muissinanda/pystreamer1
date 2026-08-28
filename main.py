from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, HTMLResponse, FileResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import asyncio
from stream_manager import StreamManager
import os
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="Python Restreamer")
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

@app.get("/hls/{channel_id}/{filename}")
async def serve_hls(channel_id: str, filename: str):
    file_path = os.path.join(manager.output_dir, channel_id, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
        
    headers = {}
    # MATIKAN TOTAL SEMUA CACHE CLOUDFLARE UNTUK SEMUA FILE (M3U8 & TS)
    headers["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0, s-maxage=0"
    headers["Pragma"] = "no-cache"
    headers["Expires"] = "0"
    headers["CDN-Cache-Control"] = "no-store"
        
    if filename.endswith(".ts"):
        headers["Content-Type"] = "video/mp2t"
        
    return FileResponse(file_path, headers=headers)
