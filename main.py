from fastapi import FastAPI
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

import requests
import os
import json
import threading

from telegram.ext import Updater, MessageHandler, Filters

app = FastAPI()

# ---------------------------------------------------
# CORS FIX FOR STREMIO WEB
# ---------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
# Added default 0 to prevent crashes if the var is missing
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0)) 

DB_FILE = "movies.json"

# ---------------------------------------------------
# DATABASE
# ---------------------------------------------------
def load_movies():
    """Reads directly from disk every time it is called."""
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "r") as f:
                return json.load(f)
        return {}
    except:
        return {}

def save_movies(data):
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)

# ---------------------------------------------------
# TELEGRAM AUTO IMPORT
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
    
    # 1. Load the freshest database from disk
    current_files = load_movies()
    
    filename = getattr(media, "file_name", None)
    if not filename:
        filename = f"Movie_{len(current_files)+1}"

    movie_id = filename.replace(" ", "_").lower()

    # 2. Append the new movie
    current_files[movie_id] = {
        "name": filename,
        "poster": "https://via.placeholder.com/300x450.png?text=Telegram+Movie",
        "description": filename,
        "file_id": file_id
    }

    # 3. Save it back to the disk
    save_movies(current_files)
    print(f"Added movie: {filename}")

# ---------------------------------------------------
# START TELEGRAM BOT
# ---------------------------------------------------
def start_bot():
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(
        MessageHandler(Filters.video | Filters.document, handle_movie)
    )

    updater.start_polling()

# Start bot in background thread (daemon=True ensures it closes when the app closes)
threading.Thread(target=start_bot, daemon=True).start()

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
    current_files = load_movies() # Fetch fresh data!
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
    current_files = load_movies() # Fetch fresh data!
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
    current_files = load_movies() # Fetch fresh data!
    
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
    current_files = load_movies() # Fetch fresh data!
    movie = current_files.get(id)

    if not movie:
        return {"error": "Movie not found"}

    file_id = movie["file_id"]

    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={"file_id": file_id}
        ).json()

        # Catch the 20MB file size limit error
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
    # Shows you how many movies are currently loaded in the database
    return {
        "status": "running",
        "addon": "Telegram Stream Addon",
        "movies_in_db": len(load_movies())
    }
