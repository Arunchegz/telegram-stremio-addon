from fastapi import (
    FastAPI,
    HTTPException,
    Request
)

from fastapi.responses import (
    JSONResponse,
    Response,
    StreamingResponse
)

from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.types import Message

from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import InputDocumentFileLocation

from pyrogram.errors import FloodWait

import os
import json
import asyncio

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("SESSION_STRING")

BASE_URL = os.getenv("BASE_URL")

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# PYROGRAM CLIENT
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
# GLOBAL CDN CACHE
# ---------------------------------------------------
exported_senders = {}

sender_lock = asyncio.Lock()

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

        print("DB Load Error:", e)

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

        print("DB Save Error:", e)

# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():

    await tg.start()

    print("✅ Pyrogram started")

# ---------------------------------------------------
# SHUTDOWN
# ---------------------------------------------------
@app.on_event("shutdown")
async def shutdown():

    await tg.stop()

    print("🛑 Pyrogram stopped")

# ---------------------------------------------------
# HOME
# ---------------------------------------------------
@app.get("/")
async def home():

    movies = load_movies()

    return {
        "status": "running",
        "movies": len(movies),
        "channel": CHANNEL_USERNAME
    }

# ---------------------------------------------------
# RESET
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

            except Exception as inner_error:

                print(
                    "Skipped Message:",
                    inner_error
                )

                continue

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
    "version": "12.0.0",
    "name": "Telegram Movies",
    "description": "Telegram Seekable Streaming",
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
# WATCH / SEEKABLE STREAM
# ---------------------------------------------------
@app.api_route(
    "/watch/{movie_id}",
    methods=["GET", "HEAD"]
)
async def watch(
    movie_id: str,
    request: Request
):

    movies = load_movies()

    movie = movies.get(movie_id)

    if not movie:
        raise HTTPException(404)

    message_id = movie["message_id"]

    # GET MESSAGE
    msg: Message = await tg.get_messages(
        CHANNEL_USERNAME,
        message_id
    )

    media = msg.video or msg.document

    if not media:
        raise HTTPException(404)

    file_size = media.file_size

    filename = movie.get(
        "file_name",
        "video.mkv"
    ).lower()

    # ---------------------------------------------------
    # CONTENT TYPE
    # ---------------------------------------------------
    content_type = "video/mp4"

    if filename.endswith(".mkv"):
        content_type = "video/x-matroska"

    elif filename.endswith(".webm"):
        content_type = "video/webm"

    # ---------------------------------------------------
    # HEAD REQUEST
    # ---------------------------------------------------
    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": content_type
            }
        )

    # ---------------------------------------------------
    # RANGE PARSING
    # ---------------------------------------------------
    range_header = request.headers.get("range")

    start = 0
    end = file_size - 1

    if range_header:

        bytes_range = (
            range_header
            .replace("bytes=", "")
            .split("-")
        )

        if bytes_range[0]:
            start = int(bytes_range[0])

        if len(bytes_range) > 1 and bytes_range[1]:
            end = int(bytes_range[1])

    # ---------------------------------------------------
    # RANGE VALIDATION
    # ---------------------------------------------------
    if start >= file_size:

        raise HTTPException(
            status_code=416,
            detail="Requested Range Not Satisfiable"
        )

    if end >= file_size:
        end = file_size - 1

    # ---------------------------------------------------
    # TELEGRAM FILE LOCATION
    # ---------------------------------------------------
    location = InputDocumentFileLocation(
        id=media.file_id,
        access_hash=media.access_hash,
        file_reference=media.file_reference,
        thumb_size=""
    )

    chunk_size = 1024 * 1024

    # ---------------------------------------------------
    # EXPORT SENDER CACHE
    # ---------------------------------------------------
    async with sender_lock:

        dc_id = media.dc_id

        if dc_id not in exported_senders:

            exported_senders[dc_id] = True

            print(
                f"✅ Cached sender for DC {dc_id}"
            )

    # ---------------------------------------------------
    # STREAM GENERATOR
    # ---------------------------------------------------
    async def streamer():

        current = start

        while current <= end:

            limit = min(
                chunk_size,
                end - current + 1
            )

            try:

                result = await tg.invoke(
                    GetFile(
                        location=location,
                        offset=current,
                        limit=limit
                    )
                )

                chunk = result.bytes

                if not chunk:
                    break

                current += len(chunk)

                yield chunk

            except FloodWait as e:

                print(
                    f"\n🚨 FloodWait:"
                    f" {e.value}s\n"
                )

                break

            except Exception as e:

                print(
                    f"\n❌ Stream Error:"
                    f" {str(e)}\n"
                )

                break

    # ---------------------------------------------------
    # RESPONSE HEADERS
    # ---------------------------------------------------
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": (
            f"bytes {start}-{end}/{file_size}"
        ),
        "Content-Type": content_type
    }

    # ---------------------------------------------------
    # STREAM RESPONSE
    # ---------------------------------------------------
    return StreamingResponse(
        streamer(),
        status_code=206,
        headers=headers
    )
