from fastapi import FastAPI, HTTPException
from fastapi.responses import (
    JSONResponse,
    RedirectResponse
)
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.types import Message

import os
import json

# ---------------------------------------------------
# ENV
# ---------------------------------------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

BOT_TOKEN = os.getenv("BOT_TOKEN")

BASE_URL = os.getenv("BASE_URL")

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# PYROGRAM
# ---------------------------------------------------
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True
)

# ---------------------------------------------------
# FASTAPI
# ---------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------
# DATABASE
# ---------------------------------------------------
def load_movies():

    try:

        if os.path.exists(DB_FILE):

            with open(DB_FILE, "r") as f:
                return json.load(f)

        return {}

    except:
        return {}


def save_movies(data):

    os.makedirs(
        os.path.dirname(DB_FILE),
        exist_ok=True
    )

    with open(DB_FILE, "w") as f:
        json.dump(data, f, indent=4)

# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():

    await tg.start()

    print("✅ Pyrogram started")

# ---------------------------------------------------
# HOME
# ---------------------------------------------------
@app.get("/")
async def home():

    movies = load_movies()

    return {
        "status": "running",
        "movies": len(movies)
    }

# ---------------------------------------------------
# RESET
# ---------------------------------------------------
@app.get("/reset")
async def reset():

    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    return {
        "status": "database deleted"
    }

# ---------------------------------------------------
# SYNC CHANNEL
# ---------------------------------------------------
@app.get("/sync")
async def sync_movies():

    current = {}

    chat = await tg.get_chat(
        CHANNEL_USERNAME
    )

    print("✅ Channel:", chat.title)

    async for msg in tg.get_chat_history(
        CHANNEL_USERNAME
    ):

        try:

            media = msg.video or msg.document

            if not media:
                continue

            filename = getattr(
                media,
                "file_name",
                None
            )

            if not filename:
                continue

            movie_id = (
                filename
                .replace(" ", "_")
                .replace(".", "_")
                .lower()
            )

            current[movie_id] = {
                "message_id": msg.id,
                "file_name": filename
            }

        except:
            continue

    save_movies(current)

    return {
        "movies": len(current)
    }

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "10.0.0",
    "name": "Telegram Movies",
    "description": "Telegram CDN Streaming",
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

    movies = load_movies()

    metas = []

    for movie_id, movie in movies.items():

        movie_name = movie.get(
            "file_name",
            "Unknown Movie"
        )

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": movie_name,
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

    clean_id = id.replace(
        "tg:",
        ""
    )

    movies = load_movies()

    movie = movies.get(clean_id)

    if not movie:
        return JSONResponse({
            "meta": {}
        })

    movie_name = movie.get(
        "file_name",
        "Unknown Movie"
    )

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": movie_name,
            "posterShape": "poster"
        }
    })

# ---------------------------------------------------
# STREAM
# ---------------------------------------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):

    clean_id = id.replace(
        "tg:",
        ""
    )

    movies = load_movies()

    movie = movies.get(clean_id)

    if not movie:
        return JSONResponse({
            "streams": []
        })

    movie_name = movie.get(
        "file_name",
        "Unknown Movie"
    )

    return JSONResponse({
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": movie_name,
                "url": (
                    f"{BASE_URL}/watch/"
                    f"{clean_id}"
                )
            }
        ]
    })

# ---------------------------------------------------
# WATCH
# ---------------------------------------------------
@app.get("/watch/{movie_id}")
async def watch(movie_id: str):

    movies = load_movies()

    movie = movies.get(movie_id)

    if not movie:
        raise HTTPException(
            status_code=404,
            detail="Movie not found"
        )

    message_id = movie["message_id"]

    # GET TELEGRAM MESSAGE
    msg: Message = await tg.get_messages(
        CHANNEL_USERNAME,
        message_id
    )

    media = msg.video or msg.document

    if not media:
        raise HTTPException(
            status_code=404,
            detail="Media not found"
        )

    # FILE ID
    file_id = media.file_id

    # GET FILE PATH
    file = await tg.get_file(file_id)

    # TELEGRAM CDN URL
    file_url = (
        f"https://api.telegram.org/file/bot"
        f"{BOT_TOKEN}/"
        f"{file.file_path}"
    )

    # REDIRECT TO CDN
    return RedirectResponse(
        url=file_url,
        status_code=302
    )
