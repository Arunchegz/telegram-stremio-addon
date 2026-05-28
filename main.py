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

# ---------------------------------------------------
# ENV VARIABLES
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
# FASTAPI APP
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
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():

    await tg.start()

    print("Pyrogram started")

# ---------------------------------------------------
# RESET DATABASE
# ---------------------------------------------------
@app.get("/reset")
async def reset():

    try:

        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

        return {
            "status": "database deleted"
        }

    except Exception as e:

        return {
            "error": str(e)
        }

# ---------------------------------------------------
# SYNC TELEGRAM CHANNEL
# ---------------------------------------------------
@app.get("/sync")
async def sync_movies():

    try:

        current = {}

        # ---------------------------------------------------
        # FORCE CHANNEL RESOLVE
        # ---------------------------------------------------
        chat = await tg.get_chat(
            CHANNEL_ID
        )

        print(
            "Resolved Chat:",
            chat.title
        )

        # ---------------------------------------------------
        # READ CHANNEL HISTORY
        # ---------------------------------------------------
        async for msg in tg.get_chat_history(
            CHANNEL_ID
        ):

            media = (
                msg.video or
                msg.document
            )

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

    except Exception as e:

        return {
            "error": str(e)
        }

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "2.0.0",
    "name": "Telegram Stream Addon",
    "description": "Telegram MTProto Streaming",

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

    movies = load_movies()

    metas = []

    for movie_id, movie in movies.items():

        movie_name = (
            movie.get("file_name") or
            movie.get("name") or
            "Unknown Movie"
        )

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie_name,

            "poster": (
                "https://via.placeholder.com/"
                "300x450.png?text=Telegram"
            ),

            "background": (
                "https://via.placeholder.com/"
                "1280x720.png?text=Telegram"
            ),

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

    movie_name = (
        movie.get("file_name") or
        movie.get("name") or
        "Unknown Movie"
    )

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie_name,

            "poster": (
                "https://via.placeholder.com/"
                "300x450.png?text=Telegram"
            ),

            "background": (
                "https://via.placeholder.com/"
                "1280x720.png?text=Telegram"
            ),

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

    movie_name = (
        movie.get("file_name") or
        movie.get("name") or
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
                ),

                "behaviorHints": {
                    "notWebReady": True
                }
            }
        ]
    })

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

        if len(parts) > 1 and parts[1]:
            end = int(parts[1])

    return start, end

# ---------------------------------------------------
# WATCH / STREAM
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

    # ---------------------------------------------------
    # OLD DB CHECK
    # ---------------------------------------------------
    if "message_id" not in movie:

        raise HTTPException(
            status_code=400,
            detail=(
                "Old DB format detected. "
                "Run /reset then /sync"
            )
        )

    message_id = movie["message_id"]

    # ---------------------------------------------------
    # FORCE CHAT RESOLVE
    # ---------------------------------------------------
    await tg.get_chat(CHANNEL_ID)

    # ---------------------------------------------------
    # GET MESSAGE
    # ---------------------------------------------------
    msg: Message = await tg.get_messages(
        CHANNEL_ID,
        message_id
    )

    media = (
        msg.video or
        msg.document
    )

    if not media:

        raise HTTPException(
            status_code=404,
            detail="Media not found"
        )

    file_size = media.file_size

    # ---------------------------------------------------
    # RANGE HEADER
    # ---------------------------------------------------
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

    # ---------------------------------------------------
    # STREAM GENERATOR
    # ---------------------------------------------------
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

    # ---------------------------------------------------
    # HEADERS
    # ---------------------------------------------------
    headers = {

        "Accept-Ranges": "bytes",

        "Content-Range":
            f"bytes {start}-{end}/{file_size}",

        "Content-Length":
            str(chunk_size),

        "Content-Type":
            "video/x-matroska"
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
        "status": "running",
        "movies": len(movies),
        "db_path": DB_FILE,
        "channel_id": CHANNEL_ID
    }
