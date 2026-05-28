from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import (
    JSONResponse,
    StreamingResponse
)
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.types import Message

from math import floor

import os
import json

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
API_ID = int(os.getenv("API_ID"))

API_HASH = os.getenv("API_HASH")

SESSION_STRING = os.getenv(
    "SESSION_STRING"
)

BASE_URL = os.getenv("BASE_URL")

CHANNEL_USERNAME = os.getenv(
    "CHANNEL_USERNAME"
)

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
            json.dump(
                data,
                f,
                indent=4
            )
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

        # ----------------------------------------
        # RESOLVE CHANNEL
        # ----------------------------------------
        chat = await tg.get_chat(CHANNEL_USERNAME)
        print("Resolved Chat:", chat.title)

        # ----------------------------------------
        # GET CHAT HISTORY
        # ----------------------------------------
        async for msg in tg.get_chat_history(CHANNEL_USERNAME):
            try:
                media = msg.video or msg.document
                if not media:
                    continue

                filename = getattr(media, "file_name", None)
                if not filename:
                    continue

                if media.file_size is None:
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

            except Exception as inner_error:
                print("Skipped Message:", inner_error)
                continue

        save_movies(current)
        return {"movies": len(current)}

    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "6.0.0",
    "name": "Telegram Stream Addon",
    "description": "Telegram MTProto Streaming",
    "resources": [
        "catalog",
        "meta",
        "stream"
    ],
    "types": ["movie"],
    "idPrefixes": ["tg"],
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
        movie_name = movie.get("file_name") or "Unknown Movie"

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": movie_name,
            "posterShape": "poster"
        })

    return JSONResponse({"metas": metas})

# ---------------------------------------------------
# META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):
    clean_id = id.replace("tg:", "")
    movies = load_movies()
    movie = movies.get(clean_id)

    if not movie:
        return JSONResponse({"meta": {}})

    movie_name = movie.get("file_name") or "Unknown Movie"

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
    clean_id = id.replace("tg:", "")
    movies = load_movies()
    movie = movies.get(clean_id)

    if not movie:
        return JSONResponse({"streams": []})

    movie_name = movie.get("file_name") or "Unknown Movie"

    return JSONResponse({
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": movie_name,
                "url": f"{BASE_URL}/watch/{clean_id}",
                "behaviorHints": {
                    "notWebReady": True
                }
            }
        ]
    })

# ---------------------------------------------------
# WATCH / SEEKABLE STREAM
# ---------------------------------------------------
@app.get("/watch/{movie_id}")
async def watch(movie_id: str, request: Request):
    movies = load_movies()
    movie = movies.get(movie_id)

    if not movie:
        raise HTTPException(
            status_code=404,
            detail="Movie not found"
        )

    message_id = movie["message_id"]

    # ----------------------------------------
    # RESOLVE CHANNEL & MESSAGE
    # ----------------------------------------
    await tg.get_chat(CHANNEL_USERNAME)
    msg: Message = await tg.get_messages(CHANNEL_USERNAME, message_id)

    media = msg.video or msg.document
    if not media:
        raise HTTPException(
            status_code=404,
            detail="Media not found"
        )

    file_size = media.file_size
    filename = movie.get("file_name", "").lower()

    # ----------------------------------------
    # CONTENT TYPE
    # ----------------------------------------
    content_type = "video/mp4"
    if filename.endswith(".mkv"):
        content_type = "video/x-matroska"
    elif filename.endswith(".webm"):
        content_type = "video/webm"

    # ----------------------------------------
    # RANGE HEADER PARSING
    # ----------------------------------------
    range_header = request.headers.get("range", None)
    
    start = 0
    end = file_size - 1

    if range_header:
        bytes_range = range_header.replace("bytes=", "").split("-")
        
        if bytes_range[0]:
            start = int(bytes_range[0])
            if len(bytes_range) > 1 and bytes_range[1]:
                end = int(bytes_range[1])
        elif len(bytes_range) > 1 and bytes_range[1]:
            # Handle suffix ranges (e.g., bytes=-1024)
            start = max(0, file_size - int(bytes_range[1]))

    # ----------------------------------------
    # STRICT BOUNDARY VALIDATION
    # ----------------------------------------
    # Reject invalid start bytes immediately
    if start >= file_size or start > end:
        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"}
        )

    # Clamp the requested end byte to the actual end of the file
    if end >= file_size:
        end = file_size - 1

    content_length = (end - start) + 1

    # ----------------------------------------
    # TELEGRAM CHUNK ALIGNMENT
    # ----------------------------------------
    telegram_chunk_size = 1024 * 1024

    # Offset expects chunks, not bytes
    chunk_index = start // telegram_chunk_size
    # Calculate bytes to skip inside the chunk
    skip_bytes = start % telegram_chunk_size

    # ----------------------------------------
    # STREAM GENERATOR
    # ----------------------------------------
    async def file_stream():
        sent = 0
        first_chunk = True

        try:
            async for chunk in tg.stream_media(msg, offset=chunk_index):
                # --------------------------------
                # SKIP EXTRA BYTES
                # --------------------------------
                if first_chunk:
                    chunk = chunk[skip_bytes:]
                    first_chunk = False

                remaining = content_length - sent
                
                if remaining <= 0:
                    break
                
                # Truncate the final chunk if it exceeds requested range
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

                sent += len(chunk)
                yield chunk
                
        except Exception:
            # Gracefully handle disconnects during seek operations
            pass

    # ----------------------------------------
    # RESPONSE HEADERS
    # ----------------------------------------
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(content_length),
        "Content-Type": content_type
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
        "storage_path": DB_FILE,
        "base_url": BASE_URL,
        "channel_username": CHANNEL_USERNAME
    }
