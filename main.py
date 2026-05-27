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

# Railway persistent volume path
DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# DATABASE FUNCTIONS
# ---------------------------------------------------
def load_movies():
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        print(f"Error loading DB: {e}")
        return {}

def save_movies(data):
    try:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4)

    except Exception as e:
        print(f"Error saving DB: {e}")

# ---------------------------------------------------
# TELEGRAM HANDLER
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

    # Load latest DB
    current_files = load_movies()

    filename = getattr(media, "file_name", None)

    if not filename:
        filename = f"Movie_{len(current_files)+1}"

    movie_id = filename.replace(" ", "_").replace(".", "_").lower()

    # Save movie
    current_files[movie_id] = {
        "name": filename,
        "poster": "https://via.placeholder.com/300x450.png?text=Telegram+Movie",
        "background": "https://via.placeholder.com/1280x720.png?text=Telegram+Movie",
        "description": filename,
        "file_id": file_id
    }

    save_movies(current_files)

    print(f"Added movie: {filename}")

# ---------------------------------------------------
# START BOT
# ---------------------------------------------------
def start_bot():

    print("Starting Telegram Bot Polling...")

    updater = Updater(BOT_TOKEN, use_context=True)

    dp = updater.dispatcher

    dp.add_handler(
        MessageHandler(
            Filters.video | Filters.document,
            handle_movie
        )
    )

    updater.start_polling()

# ---------------------------------------------------
# FASTAPI LIFESPAN
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):

    print("Starting FastAPI App...")

    bot_thread = threading.Thread(
        target=start_bot,
        daemon=True
    )

    bot_thread.start()

    yield

    print("Shutting down App...")

# ---------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------
app = FastAPI(lifespan=lifespan)

# ---------------------------------------------------
# CORS
# ---------------------------------------------------
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
    "version": "1.0.1",
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

    "idPrefixes": [
        "tg"
    ],

    "behaviorHints": {
        "configurable": False
    },

    "catalogs": [
        {
            "type": "movie",
            "id": "telegrammovies",
            "name": "Telegram Movies"
        }
    ]
}

# ---------------------------------------------------
# MANIFEST ROUTE
# ---------------------------------------------------
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
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie["name"],
            "poster": movie["poster"],
            "background": movie["background"],
            "description": movie["description"],
            "posterShape": "poster"
        })

    return JSONResponse({
        "metas": metas
    })

# ---------------------------------------------------
# META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):

    clean_id = id.replace("tg:", "")

    current_files = load_movies()

    movie = current_files.get(clean_id)

    if not movie:
        return JSONResponse({
            "meta": {}
        })

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie["name"],
            "poster": movie["poster"],
            "background": movie["background"],
            "description": movie["description"],
            "posterShape": "poster"
        }
    })

# ---------------------------------------------------
# STREAM
# ---------------------------------------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):

    clean_id = id.replace("tg:", "")

    current_files = load_movies()

    if clean_id not in current_files:
        return JSONResponse({
            "streams": []
        })

    movie = current_files[clean_id]

    return JSONResponse({
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": movie["name"],
                "url": f"{BASE_URL}/watch/{clean_id}"
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
        return JSONResponse({
            "error": "Movie not found"
        })

    file_id = movie["file_id"]

    try:

        r = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
            params={
                "file_id": file_id
            }
        ).json()

        if not r.get("ok"):

            return JSONResponse({
                "error": "Telegram API Error"
            }, status_code=400)

        file_path = r["result"]["file_path"]

        tg_url = (
            f"https://api.telegram.org/file/"
            f"bot{BOT_TOKEN}/{file_path}"
        )

        return RedirectResponse(tg_url)

    except Exception as e:

        return JSONResponse({
            "error": str(e)
        })

# ---------------------------------------------------
# HOME
# ---------------------------------------------------
@app.get("/")
async def home():

    current_files = load_movies()

    return JSONResponse({
        "status": "running",
        "addon": "Telegram Stream Addon",
        "movies_in_db": len(current_files),
        "storage_path": DB_FILE
    })
