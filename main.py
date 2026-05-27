from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import (
    JSONResponse,
    StreamingResponse
)
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.types import Message

import os
import json
import math

# ---------------------------------------------------
# ENV
# ---------------------------------------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

BASE_URL = os.getenv("BASE_URL")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# PYROGRAM CLIENT
# ---------------------------------------------------
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING
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

    print("Pyrogram started")

# ---------------------------------------------------
# INDEX MOVIES
# ---------------------------------------------------
@app.get("/sync")
async def sync_movies():

    current = {}

    async for msg in tg.get_chat_history(
        CHANNEL_ID
    ):

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
            "file_name": filename,
            "file_size": media.file_size
        }

    save_movies(current)

    return {
        "movies": len(current)
    }

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "2.0.0",
    "name": "Telegram Stream Addon",
    "description": "Telegram MTProto streaming",

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

        metas.append({
            "id": movie_id,
            "type": "movie",
            "name": movie["file_name"],
            "poster": (
                "https://via.placeholder.com/"
                "300x450.png?text=Telegram"
            )
        })

    return {
        "metas": metas
    }

# ---------------------------------------------------
# META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):

    movies = load_movies()

    movie = movies.get(id)

    if not movie:

        return {
            "meta": {}
        }

    return {
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie["file_name"],
            "poster": (
                "https://via.placeholder.com/"
                "300x450.png?text=Telegram"
            )
        }
    }

# ---------------------------------------------------
# STREAM
# ---------------------------------------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):

    movies = load_movies()

    movie = movies.get(id)

    if not movie:

        return {
            "streams": []
        }

    return {
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": movie["file_name"],

                "url": (
                    f"{BASE_URL}/watch/{id}"
                ),

                "behaviorHints": {
                    "notWebReady": False
                }
            }
        ]
    }

# ---------------------------------------------------
# RANGE PARSER
# ---------------------------------------------------
def parse_range(
    range_header,
    file_size
):

    start = 0
    end = file_size - 1

    if range_header:

        bytes_range = (
            range_header
            .replace("bytes=", "")
        )

        parts = bytes_range.split("-")

        if parts[0]:
            start = int(parts[0])

        if parts[1]:
            end = int(parts[1])

    return start, end

# ---------------------------------------------------
# STREAM FILE
# ---------------------------------------------------
@app.get("/watch/{movie_id}")
async def watch(
    movie_id: str,
    request: Request
):

    movies = load_movies()

    movie = movies.get(movie_id)

    if not movie:

        raise HTTPException(
            status_code=404,
            detail="Movie not found"
        )

    message_id = movie["message_id"]

    msg: Message = await tg.get_messages(
        CHANNEL_ID,
        message_id
    )

    media = msg.video or msg.document

    file_size = media.file_size

    range_header = request.headers.get(
        "range"
    )

    start, end = parse_range(
        range_header,
        file_size
    )

    chunk_size = (
        end - start
    ) + 1

    async def file_stream():

        downloaded = 0

        async for chunk in tg.stream_media(
            message=msg,
            offset=start
        ):

            downloaded += len(chunk)

            yield chunk

            if downloaded >= chunk_size:
                break

    headers = {
        "Accept-Ranges": "bytes",

        "Content-Range":
            f"bytes {start}-{end}/{file_size}",

        "Content-Length":
            str(chunk_size),

        "Content-Type":
            "video/mp4"
    }

    return StreamingResponse(
        file_stream(),
        status_code=206,
        headers=headers
    )

# ---------------------------------------------------
# HOME
# ---------------------------------------------------
@app.get("/")
async def home():

    movies = load_movies()

    return {
        "movies": len(movies),
        "status": "running"
    }
