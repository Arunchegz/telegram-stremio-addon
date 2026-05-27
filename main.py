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
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

DB_FILE = "movies.json"

# ---------------------------------------------------
# DATABASE
# ---------------------------------------------------

def load_movies():
    try:
        with open(DB_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_movies(data):
    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)

FILES = load_movies()

# ---------------------------------------------------
# TELEGRAM AUTO IMPORT
# ---------------------------------------------------

def handle_movie(update, context):

    # Only allow your channel
    if update.effective_chat.id != CHANNEL_ID:
        return

    message = update.message

    if not message:
        return

    media = None

    if message.video:
        media = message.video

    elif message.document:
        media = message.document

    if not media:
        return

    file_id = media.file_id

    filename = getattr(media, "file_name", None)

    if not filename:
        filename = f"Movie_{len(FILES)+1}"

    movie_id = filename.replace(" ", "_").lower()

    FILES[movie_id] = {
        "name": filename,
        "poster": "https://via.placeholder.com/300x450.png?text=Telegram+Movie",
        "description": filename,
        "file_id": file_id
    }

    save_movies(FILES)

    print(f"Added movie: {filename}")

# ---------------------------------------------------
# START TELEGRAM BOT
# ---------------------------------------------------

def start_bot():

    updater = Updater(BOT_TOKEN, use_context=True)

    dp = updater.dispatcher

    dp.add_handler(
        MessageHandler(
            Filters.video | Filters.document,
            handle_movie
        )
    )

    updater.start_polling()

# Start bot in background thread
threading.Thread(target=start_bot).start()

# ---------------------------------------------------
# STREMIO MANIFEST
# ---------------------------------------------------

manifest = {
    "id": "org.arun.telegram",
    "version": "1.0.0",
    "name": "Telegram Stream Addon",
    "description": "Telegram streaming addon",
    "resources": [
        "catalog",
        "meta",
        "stream"
    ],
    "types": [
        "movie"
    ],
    "catalogs": [
        {
            "type": "movie",
            "id": "telegrammovies",
            "name": "Telegram Movies"
        }
    ]
}

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------

@app.get("/manifest.json")
async def get_manifest():
    return JSONResponse(manifest)

# ---------------------------------------------------
# CATALOG
# ---------------------------------------------------

@app.get("/catalog/movie/telegrammovies.json")
async def catalog():

    metas = []

    for movie_id, movie in FILES.items():

        metas.append({
            "id": movie_id,
            "type": "movie",
            "name": movie["name"],
            "poster": movie["poster"]
        })

    return JSONResponse({
        "metas": metas
    })

# ---------------------------------------------------
# META
# ---------------------------------------------------

@app.get("/meta/movie/{id}.json")
async def meta(id: str):

    movie = FILES.get(id)

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

    if id not in FILES:
        return {"streams": []}

    return JSONResponse({
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": FILES[id]["name"],
                "url": f"{BASE_URL}/watch/{id}"
            }
        ]
    })

# ---------------------------------------------------
# WATCH
# ---------------------------------------------------

@app.get("/watch/{id}")
async def watch(id: str):

    movie = FILES.get(id)

    if not movie:
        return {"error": "Movie not found"}

    file_id = movie["file_id"]

    r = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
        params={"file_id": file_id}
    ).json()

    file_path = r["result"]["file_path"]

    tg_url = (
        f"https://api.telegram.org/file/bot"
        f"{BOT_TOKEN}/{file_path}"
    )

    return RedirectResponse(tg_url)

# ---------------------------------------------------
# HOME
# ---------------------------------------------------

@app.get("/")
async def home():
    return {
        "status": "running",
        "addon": "Telegram Stream Addon"
    }
