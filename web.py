import os
import json
import subprocess
from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import HTTPException, status
from dotenv import dotenv_values, load_dotenv

load_dotenv("/config/.env")

ENV_PATH = "/config/.env"
STATS_PATH = "/data/stats.json"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")

app = FastAPI()
templates = Jinja2Templates(directory="templates")
security = HTTPBasic()


# ---------- AUTH ----------

def auth(credentials: HTTPBasicCredentials = Depends(security)):

    if credentials.password != ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Wrong password",
            headers={"WWW-Authenticate": "Basic"},
        )

# ---------- HELPERS ----------

def get_stats():
    if not os.path.exists(STATS_PATH):
        return {"messages": 0, "status": "unknown"}

    with open(STATS_PATH) as f:
        return json.load(f)


# ---------- ROUTES ----------

@app.get("/", response_class=HTMLResponse)
def index(request: Request, user=Depends(auth)):

    cfg = dotenv_values(ENV_PATH)
    stats = get_stats()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "cfg": cfg,
            "stats": stats
        }
    )


@app.post("/save")
def save(
    api_id: str = Form(...),
    api_hash: str = Form(...),
    session_string: str = Form(...),
    dest_chat: str = Form(...),
    source_chats: str = Form(...),
    user=Depends(auth)
):

    with open(ENV_PATH, "w") as f:
        f.write(f"API_ID={api_id}\n")
        f.write(f"API_HASH={api_hash}\n")
        f.write(f"SESSION_STRING={session_string}\n")
        f.write(f"DEST_CHAT={dest_chat}\n")
        f.write(f"SOURCE_CHATS={source_chats}\n")
        f.write(f"ADMIN_PASSWORD={ADMIN_PASSWORD}\n")

    return RedirectResponse("/", status_code=303)


@app.post("/restart")
def restart(user=Depends(auth)):

    subprocess.Popen(["docker", "compose", "restart"])

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(user=Depends(auth)):

    db = "/data/state.db"
    if os.path.exists(db):
        os.remove(db)

    return RedirectResponse("/", status_code=303)
