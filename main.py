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

# =========================================================
# ENV VARIABLES
# =========================================================
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")

BASE_URL = os.getenv("BASE_URL", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")

DB_FILE = "/app/data/movies.json"

# =========================================================
# PYROGRAM CLIENT
# =========================================================
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
    workers=8
)

# =========================================================
# FASTAPI
# =========================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# CACHE
# =========================================================
MOVIES_CACHE = {}
MESSAGES_CACHE = {}
URL_CACHE = {}

STREAM_LIMITER = asyncio.Semaphore(2)

URL_TTL = 3000

# =========================================================
# DATABASE
# =========================================================
def load_movies():

    global MOVIES_CACHE

    if MOVIES_CACHE:
        return MOVIES_CACHE

    try:

        if os.path.exists(DB_FILE):

            with open(DB_FILE, "r") as f:
                MOVIES_CACHE = json.load(f)

        return MOVIES_CACHE

    except Exception as e:

        print("❌ DB Load Error:", e)

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

        print("❌ DB Save Error:", e)

# =========================================================
# GET MESSAGE
# =========================================================
async def get_message(
    movie_id: str,
    message_id: int
) -> Message:

    cached = MESSAGES_CACHE.get(movie_id)

    if cached:
        return cached

    msg = await tg.get_messages(
        CHANNEL_USERNAME,
        message_id
    )

    MESSAGES_CACHE[movie_id] = msg

    return msg

# =========================================================
# GET STREAM URL
# =========================================================
async def get_cdn_url(
    movie_id: str,
    msg: Message
) -> str:

    cached = URL_CACHE.get(movie_id)

    if cached and time.time() < cached["expires"]:
        return cached["url"]

    media = (
        msg.video
        or msg.document
        or msg.animation
        or msg.audio
        or msg.voice
        or msg.video_note
    )

    if not media:
        raise Exception("Media not found")

    url = f"{BASE_URL}/proxy/{movie_id}"

    URL_CACHE[movie_id] = {
        "url": url,
        "expires": time.time() + URL_TTL
    }

    return url

# =========================================================
# STARTUP
# =========================================================
@app.on_event("startup")
async def startup():

    await tg.start()

    try:

        me = await tg.get_me()

        print(f"✅ Logged in as: {me.first_name}")

        chat = await tg.get_chat(CHANNEL_USERNAME)

        print(f"✅ Connected to channel: {chat.title}")
        print(f"🆔 Channel ID: {chat.id}")

    except Exception as e:

        print("❌ Startup Error:", e)

# =========================================================
# SHUTDOWN
# =========================================================
@app.on_event("shutdown")
async def shutdown():

    await tg.stop()

    print("🛑 Pyrogram stopped")

# =========================================================
# HOME
# =========================================================
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

# =========================================================
# TEST
# =========================================================
@app.get("/test")
async def test():

    try:

        me = await tg.get_me()

        chat = await tg.get_chat(CHANNEL_USERNAME)

        return {
            "status": "success",
            "account": me.first_name,
            "channel_title": chat.title,
            "channel_id": chat.id
        }

    except Exception as e:

        return {
            "status": "error",
            "error": str(e)
        }

# =========================================================
# DEBUG
# =========================================================
@app.get("/debug")
async def debug():

    results = []

    chat = await tg.get_chat(CHANNEL_USERNAME)

    async for msg in tg.get_chat_history(
        chat.id,
        limit=20
    ):

        results.append({
            "id": msg.id,
            "text": msg.text,
            "video": bool(msg.video),
            "document": bool(msg.document),
            "animation": bool(msg.animation),
            "photo": bool(msg.photo),
            "audio": bool(msg.audio),
            "voice": bool(msg.voice),
            "video_note": bool(msg.video_note),
        })

    return results

# =========================================================
# RESET
# =========================================================
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

# =========================================================
# SYNC
# =========================================================
@app.get("/sync")
async def sync_movies():

    global MESSAGES_CACHE

    try:

        current = {}

        print(f"📡 Syncing: {CHANNEL_USERNAME}")

        chat = await tg.get_chat(CHANNEL_USERNAME)

        print(f"✅ Connected: {chat.title}")

        total_messages = 0
        media_messages = 0

        async for msg in tg.get_chat_history(
            chat.id,
            limit=5000
        ):

            total_messages += 1

            try:

                media = (
                    msg.video
                    or msg.document
                    or msg.animation
                    or msg.audio
                    or msg.voice
                    or msg.video_note
                )

                if not media:
                    continue

                media_messages += 1

                filename = getattr(
                    media,
                    "file_name",
                    None
                )

                if not filename:

                    mime = getattr(
                        media,
                        "mime_type",
                        ""
                    )

                    ext = "mp4"

                    if "mkv" in mime:
                        ext = "mkv"

                    elif "webm" in mime:
                        ext = "webm"

                    elif "mp3" in mime:
                        ext = "mp3"

                    filename = f"telegram_{msg.id}.{ext}"

                movie_id = (
                    filename
                    .replace(" ", "_")
                    .replace(".", "_")
                    .replace("-", "_")
                    .lower()
                )

                current[movie_id] = {
                    "message_id": msg.id,
                    "file_name": filename,
                    "file_size": getattr(
                        media,
                        "file_size",
                        0
                    )
                }

                MESSAGES_CACHE[movie_id] = msg

                print(f"✅ Added: {filename}")

            except Exception as inner_error:

                print(
                    f"⚠️ Skipped {msg.id}: {inner_error}"
                )

        save_movies(current)

        return {
            "status": "success",
            "total_messages": total_messages,
            "media_messages": media_messages,
            "synced": len(current),
            "messages_cached": len(MESSAGES_CACHE)
        }

    except Exception as e:

        print("❌ Sync Error:", e)

        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "error": str(e)
            }
        )

# =========================================================
# MANIFEST
# =========================================================
manifest = {
    "id": "org.arun.telegram",
    "version": "19.0.0",
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

# =========================================================
# CATALOG
# =========================================================
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

# =========================================================
# META
# =========================================================
@app.get("/meta/movie/{id}.json")
async def meta(id: str):

    clean_id = id.replace("tg:", "")

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

# =========================================================
# STREAM
# =========================================================
@app.get("/stream/movie/{id}.json")
async def stream(id: str):

    clean_id = id.replace("tg:", "")

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
                    "title": movie_name,
                    "url": cdn_url
                }
            ]
        })

    except Exception as e:

        print("❌ Stream Error:", e)

        return JSONResponse({
            "streams": [
                {
                    "name": "☁️ Telegram Proxy",
                    "title": movie_name,
                    "url": f"{BASE_URL}/proxy/{clean_id}"
                }
            ]
        })

# =========================================================
# WATCH
# =========================================================
@app.api_route(
    "/watch/{movie_id}",
    methods=["GET", "HEAD"]
)
async def watch(
    movie_id: str,
    request: Request
):

    proxy_url = f"{BASE_URL}/proxy/{movie_id}"

    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={
                "Location": proxy_url
            }
        )

    return RedirectResponse(
        url=proxy_url,
        status_code=302
    )

# =========================================================
# PROXY STREAM
# =========================================================
@app.api_route(
    "/proxy/{movie_id}",
    methods=["GET", "HEAD"]
)
async def proxy_stream(
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

    msg = await get_message(
        movie_id,
        movie["message_id"]
    )

    media = (
        msg.video
        or msg.document
        or msg.animation
        or msg.audio
        or msg.voice
        or msg.video_note
    )

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
        "video.mp4"
    ).lower()

    content_type = "video/mp4"

    if filename.endswith(".mkv"):
        content_type = "video/x-matroska"

    elif filename.endswith(".webm"):
        content_type = "video/webm"

    elif filename.endswith(".mp3"):
        content_type = "audio/mpeg"

    # =========================================================
    # HEAD
    # =========================================================
    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": content_type
            }
        )

    # =========================================================
    # RANGE
    # =========================================================
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

            if (
                len(bytes_range) > 1
                and bytes_range[1]
            ):
                end = int(bytes_range[1])

        except:
            pass

    if end >= file_size:
        end = file_size - 1

    TG_CHUNK_SIZE = 1024 * 1024

    # =========================================================
    # STREAMER
    # =========================================================
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

                print(
                    f"🚨 FloodWait: {e.value}s"
                )

            except Exception as e:

                print(
                    f"❌ Stream Exception: {e}"
                )

    # =========================================================
    # HEADERS
    # =========================================================
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
        "Content-Type": content_type,
        "Cache-Control": "public, max-age=3600",
        "Connection": "keep-alive"
    }

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers=headers
    )