# web.py
import os
import json
import signal
import requests

from fastapi import FastAPI, Request, Form, Depends, HTTPException, status
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from dotenv import dotenv_values, load_dotenv

# ---------- CONFIG ----------

ENV_PATH = "/config/.env"
STATS_PATH = "/data/stats.json"

load_dotenv(ENV_PATH)

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

if not ADMIN_PASSWORD:
    raise RuntimeError("ADMIN_PASSWORD not set in .env")

BOT_API = "http://localhost:9000/internal/restart"


# ---------- APP ----------

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
        try:
            return json.load(f)
        except:
            return {"messages": 0, "status": "error"}


def save_env(data: dict):

    with open(ENV_PATH, "w") as f:
        for k, v in data.items():
            if v is not None:
                f.write(f"{k}={v}\n")


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
    session_string: str = Form(""),
    dest_chat: str = Form(...),
    source_chats: str = Form(...),
    user=Depends(auth)
):

    env = dotenv_values(ENV_PATH)

    env["API_ID"] = api_id
    env["API_HASH"] = api_hash
    env["SESSION_STRING"] = session_string
    env["DEST_CHAT"] = dest_chat
    env["SOURCE_CHATS"] = source_chats
    env["ADMIN_PASSWORD"] = ADMIN_PASSWORD

    save_env(env)

    return RedirectResponse("/", status_code=303)


@app.post("/restart")
def restart(user=Depends(auth)):

    try:
        requests.post(BOT_API, timeout=3)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to restart bot: {e}"
        )

    return RedirectResponse("/", status_code=303)


@app.post("/clear-db")
def clear_db(user=Depends(auth)):

    db = "/data/state.db"

    if os.path.exists(db):
        os.remove(db)

    return RedirectResponse("/", status_code=303)
