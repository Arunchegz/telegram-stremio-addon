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
    no_updates=True,
    workers=1
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

# ---------------------------------------------------
# STREAM LIMITER
# ---------------------------------------------------
STREAM_LIMITER = asyncio.Semaphore(2)

# ---------------------------------------------------
# URL CACHE TTL
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

    name = filename.lower()

    if "2160p" in name or "4k" in name:
        return "4K"

    elif "1440p" in name:
        return "1440p"

    elif "1080p" in name:
        return "1080p"

    elif "720p" in name:
        return "720p"

    elif "480p" in name:
        return "480p"

    elif "360p" in name:
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
# DATABASE
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
# BACKGROUND AUTO SYNC
# ---------------------------------------------------
async def background_sync():

    while True:

        try:

            movies = load_movies()

            updated = False

            async for msg in tg.get_chat_history(
                CHANNEL_USERNAME,
                limit=50
            ):

                media = msg.video or msg.document

                if not media:
                    continue

                filename = getattr(media, "file_name", None)

                if not filename:
                    continue

                movie_id = (
                    filename
                    .replace(" ", "_")
                    .replace(".", "_")
                    .lower()
                )

                # Skip already synced
                if movie_id in movies:
                    continue

                file_size_bytes = media.file_size or 0

                readable_size = human_size(file_size_bytes)

                quality = detect_quality(filename)

                resolution = detect_resolution(quality)

                movies[movie_id] = {
                    "message_id": msg.id,
                    "file_name": filename,
                    "file_size": file_size_bytes,
                    "readable_size": readable_size,
                    "quality": quality,
                    "resolution": resolution
                }

                MESSAGES_CACHE[movie_id] = msg

                updated = True

                print(f"✅ Auto Synced: {filename}")

            if updated:
                save_movies(movies)

        except Exception as e:

            print(f"⚠️ Background Sync Error: {e}")

        await asyncio.sleep(30)


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
# GET STREAM URL
# ---------------------------------------------------
async def get_cdn_url(
    movie_id: str,
    msg: Message
) -> str:

    cached = URL_CACHE.get(movie_id)

    if cached and time.time() < cached["expires"]:

        print(f"✅ URL Cache Hit: {movie_id}")

        return cached["url"]

    url = f"{BASE_URL}/proxy/{movie_id}"

    URL_CACHE[movie_id] = {
        "url": url,
        "expires": time.time() + URL_TTL
    }

    print(f"🔗 Generated Proxy URL: {movie_id}")

    return url


# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():

    try:

        if not tg.is_connected:

            await tg.start()

            await tg.get_chat(CHANNEL_USERNAME)

            asyncio.create_task(background_sync())

            print("✅ Pyrogram started + DC warmed up")

    except Exception as e:

        print(f"⚠️ Startup Error: {e}")


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
# RESET DATABASE
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
    "version": "24.0.0",
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

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie["file_name"],
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": (
                f"📺 {movie['quality']} | "
                f"🖥 {movie['resolution']} | "
                f"💾 {movie['readable_size']}"
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

    return JSONResponse({
        "meta": {
            "id": id,
            "type": "movie",
            "name": movie["file_name"],
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": (
                f"📺 Quality: {movie['quality']}\n"
                f"🖥 Resolution: {movie['resolution']}\n"
                f"💾 Size: {movie['readable_size']}"
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

    msg = await get_message(
        clean_id,
        movie["message_id"]
    )

    url = await get_cdn_url(
        clean_id,
        msg
    )

    return JSONResponse({
        "streams": [
            {
                "name": "⚡ Telegram Stream",
                "title": (
                    f"{movie['file_name']}\n"
                    f"📺 {movie['quality']} | "
                    f"🖥 {movie['resolution']} | "
                    f"💾 {movie['readable_size']}"
                ),
                "url": url
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

    url = f"{BASE_URL}/proxy/{movie_id}"

    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={"Location": url}
        )

    return RedirectResponse(
        url=url,
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

    file_size = media.file_size

    filename = movie["file_name"].lower()

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
    # RANGE
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
    # CHUNK SIZE
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

                print(f"🚨 FloodWait: {e.value}s")

            except Exception as e:

                print(f"⚠️ Stream Error: {e}")

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
# TELEGRAM WEBHOOK
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