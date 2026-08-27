from fastapi import FastAPI, Request, Form, Depends, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import asyncio
from stream_manager import StreamManager
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Python Restreamer")

security = HTTPBasic()

def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    USERNAME = os.environ.get("ADMIN_USER", "admin")
    PASSWORD = os.environ.get("ADMIN_PASS", "admin")
    
    if credentials.username != USERNAME or credentials.password != PASSWORD:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

manager = StreamManager(db_path="restreamer.db")

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, username: str = Depends(verify_credentials)):
    channels = manager.get_all_channels()
    return templates.TemplateResponse(
        request=request, 
        name="index.html", 
        context={"channels": channels}
    )

@app.post("/add")
async def add_channel(
    name: str = Form(...), 
    input_url: str = Form(...),
    username: str = Depends(verify_credentials)
):
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

@app.get("/play/{channel_id}", response_class=HTMLResponse)
async def play_channel(request: Request, channel_id: str):
    channel = manager.get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
        
    return templates.TemplateResponse(
        request=request, 
        name="player.html", 
        context={"channel": channel}
    )

@app.get("/stream/{channel_id}")
async def stream_video(request: Request, channel_id: str):
    channel = manager.get_channel(channel_id)
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
        
    if channel["status"] != "active":
        raise HTTPException(status_code=400, detail="Channel is not active")

    loop = asyncio.get_running_loop()
    q = asyncio.Queue(maxsize=100)
    manager.add_subscriber(channel_id, q, loop)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                chunk = await q.get()
                yield chunk
        except asyncio.CancelledError:
            pass
        finally:
            manager.remove_subscriber(channel_id, q, loop)

    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
        "Content-Type": "video/mp2t",
        "Transfer-Encoding": "chunked"
    }
    
    return StreamingResponse(
        event_generator(),
        media_type="video/mp2t",
        headers=headers
    )
