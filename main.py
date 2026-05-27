from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

import requests
import os
import json

from telegram import Update
from telegram.ext import (
    Application,
    MessageHandler,
    ContextTypes,
    filters
)

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
BASE_URL = os.getenv("BASE_URL")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))

# ---------------------------------------------------
# DATABASE FILE
# ---------------------------------------------------
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

        print(f"DB Load Error: {e}")
        return {}

def save_movies(data):

    try:

        os.makedirs(
            os.path.dirname(DB_FILE),
            exist_ok=True
        )

        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4)

    except Exception as e:

        print(f"DB Save Error: {e}")

# ---------------------------------------------------
# TELEGRAM HANDLER
# ---------------------------------------------------
async def handle_movie(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    try:

        if update.effective_chat.id != CHANNEL_ID:
            return

        message = update.message

        if not message:
            return

        media = message.video or message.document

        if not media:
            return

        file_id = media.file_id

        current_files = load_movies()

        filename = getattr(media, "file_name", None)

        if not filename:
            filename = f"Movie_{len(current_files)+1}"

        movie_id = (
            filename
            .replace(" ", "_")
            .replace(".", "_")
            .lower()
        )

        current_files[movie_id] = {
            "name": filename,
            "poster": "https://via.placeholder.com/300x450.png?text=Telegram+Movie",
            "background": "https://via.placeholder.com/1280x720.png?text=Telegram+Movie",
            "description": filename,
            "file_id": file_id
        }

        save_movies(current_files)

        print(f"Added movie: {filename}")

    except Exception as e:

        print(f"Telegram Handler Error: {e}")

# ---------------------------------------------------
# TELEGRAM APPLICATION
# ---------------------------------------------------
telegram_app = (
    Application.builder()
    .token(BOT_TOKEN)
    .build()
)

telegram_app.add_handler(
    MessageHandler(
        filters.VIDEO | filters.Document.ALL,
        handle_movie
    )
)

# ---------------------------------------------------
# FASTAPI LIFESPAN
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):

    try:

        await telegram_app.initialize()

        await telegram_app.start()

        webhook_url = (
            f"{BASE_URL}/telegram-webhook"
        )

        await telegram_app.bot.set_webhook(
            url=webhook_url
        )

        print(f"Webhook Set: {webhook_url}")

    except Exception as e:

        print(f"Lifespan Error: {e}")

    yield

    try:

        await telegram_app.stop()

        await telegram_app.shutdown()

    except Exception as e:

        print(f"Shutdown Error: {e}")

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
# TELEGRAM WEBHOOK
# ---------------------------------------------------
@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):

    try:

        data = await request.json()

        update = Update.de_json(
            data,
            telegram_app.bot
        )

        await telegram_app.process_update(update)

        return JSONResponse({
            "ok": True
        })

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
        "storage_path": DB_FILE,
        "base_url": BASE_URL
    })
