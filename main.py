from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

import requests
import os
import json
import threading

from telegram.ext import Updater, MessageHandler, Filters

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

# Using Railway Volume path for persistent storage
DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# DATABASE FUNCTIONS
# ---------------------------------------------------
def load_movies():
    """Reads directly from disk every time it is called."""
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Error loading DB: {e}")
        return {}

def save_movies(data):
    """Saves data to disk, ensuring the directory exists."""
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)

# ---------------------------------------------------
# TELEGRAM MESSAGE HANDLER
# ---------------------------------------------------
def handle_movie(update, context):
    if update.effective_chat.id != CHANNEL_ID:
        return

    message = update.message
    if not message:
        return

    media = message.video or message.document
    if not media:
        return

    file_id = media.file_id
    
    # Load the freshest database from disk
    current_files = load_movies()
    
    filename = getattr(media, "file_name", None)
    if not filename:
        filename = f"Movie_{len(current_files)+1}"

    movie_id = filename.replace(" ", "_").lower()

    # Append the new movie
    current_files[movie_id] = {
        "name": filename,
        "poster": "https://via.placeholder.com/300x450.png?text=Telegram+Movie",
        "description": filename,
        "file_id": file_id
    }

    # Save it back to the disk
    save_movies(current_files)
    print(f"Added movie: {filename}")

# ---------------------------------------------------
# START TELEGRAM BOT
# ---------------------------------------------------
def start_bot():
    print("Starting Telegram polling...")
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(
        MessageHandler(Filters.video | Filters.document, handle_movie)
    )

    updater.start_polling()

# ---------------------------------------------------
# FASTAPI LIFESPAN
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Run bot in a background thread
    print("Initializing App Lifespan...")
    bot_thread = threading.Thread(target=start_bot, daemon=True)
    bot_thread.start()
    
    yield # Yield control back to FastAPI
    
    # Shutdown
    print("Shutting down App...")

# ---------------------------------------------------
# APP INIT
# ---------------------------------------------------
app = FastAPI(lifespan=lifespan)

# CORS FIX FOR STREMIO WEB
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# STREMIO MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "1.0.0",
    "name": "Telegram Stream Addon",
    "description": "Telegram streaming addon",
    "resources": ["catalog", "meta", "stream"],
    "types": ["movie"],
    "catalogs": [
        {
            "type": "movie",
            "id": "telegrammovies",
            "name": "Telegram Movies"
        }
    ]
}

@app.get("/manifest.json")
async def get_manifest():
    return JSONResponse(manifest)

# ---------------------------------------------------
# CATALOG
# ---------------------------------------------------
@app.get("/catalog/movie/telegrammovies.json")
async def catalog():
    current_files = load_movies()
    metas = []

    for movie_id, movie in current_files.items():
        metas.append({
            "id": movie_id,
            "type": "movie",
            "name": movie["name"],
            "poster": movie["poster"]
        })

    return JSONResponse({"metas": metas})

# ---------------------------------------------------
# META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):
    current_files = load_movies()
    movie = current_files.get(id)

    if not movie:
        return {"meta": {}}

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie["name"],
            "poster": movie["poster"],
            "description": movie["description"]
        }
    })

# ---------------------------------------------------
# STREAM
# ---------------------------------------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):
    current_files = load_movies()
    
    if id not in current_files:
        return {"streams": []}

    return JSONResponse({
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": current_files[id]["name"],
                "url": f"{BASE_URL}/watch/{id}"
            }
        ]
    })

# ---------------------------------------------------
# WATCH
# ---------------------------------------------------
@app.get("/watch/{id}")
async def watch(id: str):
    current_files = load_movies()
    movie = current_files.get(id)

    if not movie:
        return {"error": "Movie not found"}

    file_id = movie["file_id"]

    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={"file_id": file_id}
        ).json()

        if not r.get("ok"):
            return JSONResponse({"error": "Telegram API Error. File might be too large (>20MB)."}, status_code=400)

        file_path = r["result"]["file_path"]
        tg_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"

        return RedirectResponse(tg_url)
    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------
# HOME
# ---------------------------------------------------
@app.get("/")
async def home():
    return {
        "status": "running",
        "addon": "Telegram Stream Addon",
        "movies_in_db": len(load_movies()),
        "storage_path": DB_FILE
    }
