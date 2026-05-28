from fastapi import (
    FastAPI,
    HTTPException,
    Request
)

from fastapi.responses import (
    JSONResponse,
    Response,
    RedirectResponse,
    StreamingResponse
)

from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import FloodWait

import os
import json
import time
import asyncio

# ---------------------------------------------------
# ENV VARIABLES
# ---------------------------------------------------
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

BASE_URL = os.getenv("BASE_URL", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")

DB_FILE = "/app/data/movies.json"

# ---------------------------------------------------
# PYROGRAM CLIENT
# ---------------------------------------------------
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=False,
    workers=8
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
# MEMORY CACHE
# ---------------------------------------------------
MOVIES_CACHE = {}
MESSAGES_CACHE = {}
URL_CACHE = {}

# Prevent Telegram FloodWait
STREAM_LIMITER = asyncio.Semaphore(2)

# ---------------------------------------------------
# URL TTL
# ---------------------------------------------------
URL_TTL = 3000

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------
def human_size(size):

    if not size:
        return "Unknown"

    for unit in ["B", "KB", "MB", "GB", "TB"]:

        if size < 1024:
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size:.2f} PB"


def detect_quality(filename: str):

    lower_name = filename.lower()

    if "2160p" in lower_name or "4k" in lower_name:
        return "4K"

    elif "1440p" in lower_name:
        return "1440p"

    elif "1080p" in lower_name:
        return "1080p"

    elif "720p" in lower_name:
        return "720p"

    elif "480p" in lower_name:
        return "480p"

    elif "360p" in lower_name:
        return "360p"

    return "Unknown"


def detect_resolution(quality: str):

    mapping = {
        "4K": "3840x2160",
        "1440p": "2560x1440",
        "1080p": "1920x1080",
        "720p": "1280x720",
        "480p": "854x480",
        "360p": "640x360"
    }

    return mapping.get(quality, "Unknown")


# ---------------------------------------------------
# DATABASE FUNCTIONS
# ---------------------------------------------------
def load_movies():

    global MOVIES_CACHE

    if MOVIES_CACHE:
        return MOVIES_CACHE

    try:

        if os.path.exists(DB_FILE):

            with open(DB_FILE, "r") as f:
                MOVIES_CACHE = json.load(f)

            return MOVIES_CACHE

        return {}

    except Exception as e:

        print("DB Load Error:", e)
        return {}


def save_movies(data):

    global MOVIES_CACHE

    try:

        os.makedirs(
            os.path.dirname(DB_FILE),
            exist_ok=True
        )

        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=4)

        MOVIES_CACHE = data

    except Exception as e:

        print("DB Save Error:", e)


# ---------------------------------------------------
# GET MESSAGE
# ---------------------------------------------------
async def get_message(
    movie_id: str,
    message_id: int
) -> Message:

    msg = MESSAGES_CACHE.get(movie_id)

    if not msg:

        msg = await tg.get_messages(
            CHANNEL_USERNAME,
            message_id
        )

        MESSAGES_CACHE[movie_id] = msg

    return msg


# ---------------------------------------------------
# GET CDN URL
# ---------------------------------------------------
async def get_cdn_url(
    movie_id: str,
    msg: Message
) -> str:

    cached = URL_CACHE.get(movie_id)

    if cached and time.time() < cached["expires"]:

        print(f"✅ URL Cache Hit: {movie_id}")

        return cached["url"]

    media = msg.video or msg.document

    if not media:
        raise Exception("Media not found")

    url = f"{BASE_URL}/proxy/{movie_id}"

    URL_CACHE[movie_id] = {
        "url": url,
        "expires": time.time() + URL_TTL
    }

    print(f"🔗 Generated Proxy URL: {movie_id}")

    return url


# ---------------------------------------------------
# AUTO SYNC NEW FILES
# ---------------------------------------------------
@tg.on_message()
async def auto_sync(_, msg: Message):

    try:

        if not msg.chat:
            return

        # Only target channel
        if (
            str(msg.chat.username).lower()
            != str(CHANNEL_USERNAME).replace("@", "").lower()
        ):
            return

        media = msg.video or msg.document

        if not media:
            return

        filename = getattr(media, "file_name", None)

        if not filename:
            return

        movies = load_movies()

        movie_id = (
            filename
            .replace(" ", "_")
            .replace(".", "_")
            .lower()
        )

        # -----------------------------------
        # FILE SIZE
        # -----------------------------------
        file_size_bytes = media.file_size or 0

        readable_size = human_size(file_size_bytes)

        # -----------------------------------
        # QUALITY
        # -----------------------------------
        quality = detect_quality(filename)

        # -----------------------------------
        # RESOLUTION
        # -----------------------------------
        resolution = detect_resolution(quality)

        # -----------------------------------
        # SAVE MOVIE
        # -----------------------------------
        movies[movie_id] = {
            "message_id": msg.id,
            "file_name": filename,
            "file_size": file_size_bytes,
            "readable_size": readable_size,
            "quality": quality,
            "resolution": resolution
        }

        save_movies(movies)

        # Cache message
        MESSAGES_CACHE[movie_id] = msg

        print(f"✅ Auto Synced: {filename}")

    except Exception as e:

        print(f"❌ Auto Sync Error: {e}")


# ---------------------------------------------------
# AUTO REMOVE DELETED FILES
# ---------------------------------------------------
@tg.on_deleted_messages()
async def auto_remove(_, deleted_messages):

    try:

        movies = load_movies()

        removed = []

        for movie_id, movie in list(movies.items()):

            if movie.get("message_id") in deleted_messages.messages:

                removed.append(movie.get("file_name"))

                movies.pop(movie_id, None)

                MESSAGES_CACHE.pop(movie_id, None)
                URL_CACHE.pop(movie_id, None)

        if removed:

            save_movies(movies)

            print(f"🗑 Removed {len(removed)} deleted files")

    except Exception as e:

        print(f"❌ Delete Sync Error: {e}")


# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():

    await tg.start()

    try:

        await tg.get_chat(CHANNEL_USERNAME)

        print("✅ Pyrogram started + DC warmed up")

    except Exception as e:

        print(f"⚠️ Warm-up warning: {e}")


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
        "cached_messages": len(MESSAGES_CACHE),
        "cached_urls": len(URL_CACHE),
        "channel": CHANNEL_USERNAME
    }


# ---------------------------------------------------
# RESET
# ---------------------------------------------------
@app.get("/reset")
async def reset():

    global MOVIES_CACHE
    global MESSAGES_CACHE
    global URL_CACHE

    try:

        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

        MOVIES_CACHE = {}
        MESSAGES_CACHE = {}
        URL_CACHE = {}

        return {
            "status": "database deleted"
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
    "version": "19.0.0",
    "name": "Telegram Movies",
    "description": "Fast Telegram Seekable Streaming",
    "resources": ["catalog", "meta", "stream"],
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

        quality = movie.get(
            "quality",
            "Unknown"
        )

        resolution = movie.get(
            "resolution",
            "Unknown"
        )

        size = movie.get(
            "readable_size",
            "Unknown"
        )

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": (
                f"📺 {quality} | "
                f"🖥 {resolution} | "
                f"💾 {size}"
            ),
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

    movies = load_movies()

    movie = movies.get(clean_id)

    if not movie:
        return JSONResponse({"meta": {}})

    movie_name = movie.get(
        "file_name",
        "Unknown Movie"
    )

    quality = movie.get(
        "quality",
        "Unknown"
    )

    resolution = movie.get(
        "resolution",
        "Unknown"
    )

    size = movie.get(
        "readable_size",
        "Unknown"
    )

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": (
                f"📺 Quality: {quality}\n"
                f"🖥 Resolution: {resolution}\n"
                f"💾 Size: {size}"
            ),
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

    movie_name = movie.get(
        "file_name",
        "Unknown Movie"
    )

    quality = movie.get(
        "quality",
        "Unknown"
    )

    resolution = movie.get(
        "resolution",
        "Unknown"
    )

    size = movie.get(
        "readable_size",
        "Unknown"
    )

    stream_title = (
        f"{movie_name}\n"
        f"📺 {quality} | "
        f"🖥 {resolution} | "
        f"💾 {size}"
    )

    try:

        msg = await get_message(
            clean_id,
            movie["message_id"]
        )

        cdn_url = await get_cdn_url(
            clean_id,
            msg
        )

        return JSONResponse({
            "streams": [
                {
                    "name": "⚡ Telegram CDN",
                    "title": stream_title,
                    "url": cdn_url
                }
            ]
        })

    except Exception as e:

        print(f"❌ Stream URL Error: {e}")

        return JSONResponse({
            "streams": [
                {
                    "name": "☁️ Telegram Proxy",
                    "title": stream_title,
                    "url": f"{BASE_URL}/proxy/{clean_id}"
                }
            ]
        })


# ---------------------------------------------------
# WATCH REDIRECT
# ---------------------------------------------------
@app.api_route(
    "/watch/{movie_id}",
    methods=["GET", "HEAD"]
)
async def watch(movie_id: str, request: Request):

    cdn_url = f"{BASE_URL}/proxy/{movie_id}"

    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={
                "Location": cdn_url
            }
        )

    return RedirectResponse(
        url=cdn_url,
        status_code=302
    )


# ---------------------------------------------------
# PROXY STREAM
# ---------------------------------------------------
@app.api_route(
    "/proxy/{movie_id}",
    methods=["GET", "HEAD"]
)
async def proxy_stream(movie_id: str, request: Request):

    movies = load_movies()

    movie = movies.get(movie_id)

    if not movie:
        raise HTTPException(
            status_code=404,
            detail="Movie not found"
        )

    msg = await get_message(
        movie_id,
        movie["message_id"]
    )

    media = msg.video or msg.document

    if not media:
        raise HTTPException(
            status_code=404,
            detail="Media not found"
        )

    file_size = movie.get(
        "file_size",
        media.file_size
    )

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

    elif filename.endswith(".avi"):
        content_type = "video/x-msvideo"

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

        try:

            bytes_range = (
                range_header
                .replace("bytes=", "")
                .split("-")
            )

            if bytes_range[0]:
                start = int(bytes_range[0])

            if len(bytes_range) > 1 and bytes_range[1]:
                end = int(bytes_range[1])

        except:
            pass

    if end >= file_size:
        end = file_size - 1

    # ---------------------------------------------------
    # TELEGRAM CHUNK SIZE
    # ---------------------------------------------------
    TG_CHUNK_SIZE = 1024 * 1024

    # ---------------------------------------------------
    # STREAMER
    # ---------------------------------------------------
    async def streamer():

        sent = 0
        first_chunk = True

        chunk_offset = start // TG_CHUNK_SIZE
        skip_bytes = start % TG_CHUNK_SIZE

        async with STREAM_LIMITER:

            try:

                async for chunk in tg.stream_media(
                    msg,
                    offset=chunk_offset
                ):

                    if await request.is_disconnected():
                        break

                    if first_chunk:
                        chunk = chunk[skip_bytes:]
                        first_chunk = False

                    remaining = (
                        (end - start + 1)
                        - sent
                    )

                    if remaining <= 0:
                        break

                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]

                    sent += len(chunk)

                    yield chunk

            except FloodWait as e:

                print(f"\n🚨 FloodWait: {e.value}s\n")

            except Exception:
                pass

    # ---------------------------------------------------
    # HEADERS
    # ---------------------------------------------------
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Type": content_type,
        "Cache-Control": "public, max-age=3600",
        "Connection": "keep-alive"
    }

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers=headers
    )


# ---------------------------------------------------
# DUMMY TELEGRAM WEBHOOK
# ---------------------------------------------------
@app.post("/telegram-webhook")
async def telegram_webhook():
    return {"status": "ok"}


# ---------------------------------------------------
# FAVICON
# ---------------------------------------------------
@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)