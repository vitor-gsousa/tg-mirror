import os
import subprocess
from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from dotenv import dotenv_values

ENV_PATH = "/config/.env"

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    config = dotenv_values(ENV_PATH)
    return templates.TemplateResponse(
        "index.html", {"request": request, "cfg": config}
    )

@app.post("/save")
def save(
    api_id: str = Form(...),
    api_hash: str = Form(...),
    session: str = Form(...),
    dest_chat: str = Form(...),
    source_chats: str = Form(...),
):
    with open(ENV_PATH, "w") as f:
        f.write(f"API_ID={api_id}\n")
        f.write(f"API_HASH={api_hash}\n")
        f.write(f"SESSION={session}\n")
        f.write(f"DEST_CHAT={dest_chat}\n")
        f.write(f"SOURCE_CHATS={source_chats}\n")

    # Reinicia o container (Docker Compose)
    subprocess.Popen(["/usr/bin/supervisorctl", "restart", "mirror"])

    return RedirectResponse("/", status_code=303)
