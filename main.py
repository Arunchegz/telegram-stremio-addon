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
from pyrogram.errors import FloodWait, BadRequest

import os
import json
import time
import asyncio
import re

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
# MEMORY CACHE & LIMITS
# ---------------------------------------------------
MOVIES_CACHE = {}
URL_CACHE = {}

# Allow video players to probe metadata without deadlocking
STREAM_LIMITER = asyncio.Semaphore(5)

# ---------------------------------------------------
# URL TTL
# ---------------------------------------------------
URL_TTL = 3000
SYNC_LOCK = asyncio.Lock()


# ---------------------------------------------------
# QUALITY HELPERS
# ---------------------------------------------------
def format_size(size):
    if not size:
        return "Unknown"
    size = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"

def detect_quality(filename):
    if not filename:
        return "Unknown"
    name = filename.lower()
    for p in ["2160p","4k","1440p","1080p","720p","480p","360p"]:
        if p in name:
            return p.upper()
    return "Unknown"

def detect_source(filename):
    if not filename:
        return ""
    name = filename.lower()
    for s in ["bluray","bdrip","web-dl","webdl","webrip","hdrip","dvdrip","hdtv","remux"]:
        if s in name:
            return s.upper()
    return ""


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

def remove_movie_from_db(movie_id: str):
    global MOVIES_CACHE
    if movie_id in MOVIES_CACHE:
        del MOVIES_CACHE[movie_id]
        save_movies(MOVIES_CACHE)
        print(f"🗑️ Removed deleted Telegram media from DB: {movie_id}")


# ---------------------------------------------------
# GET CACHED MESSAGE
# ---------------------------------------------------
async def get_message(message_id: int) -> Message:
    return await tg.get_messages(CHANNEL_USERNAME, message_id)


# ---------------------------------------------------
# GET CDN URL
# ---------------------------------------------------
async def get_cdn_url(movie_id: str, msg: Message) -> str:
    cached = URL_CACHE.get(movie_id)
    # Cache hit
    if cached and time.time() < cached["expires"]:
        print(f"✅ URL Cache Hit: {movie_id}")
        return cached["url"]

    media = msg.video or msg.document
    if not media:
        raise Exception("Media not found")

    # Internal proxy URL
    url = f"{BASE_URL}/proxy/{movie_id}"
    URL_CACHE[movie_id] = {
        "url": url,
        "expires": time.time() + URL_TTL
    }
    print(f"🔗 Generated Proxy URL: {movie_id}")
    return url


# ---------------------------------------------------
# INSTANT AUTO SYNC (NO TIMER)
# ---------------------------------------------------
async def auto_sync():
    print("📚 AUTO_SYNC CALLED (INSTANT)")

    async with SYNC_LOCK:
        print("🔄 STARTING TELEGRAM SYNC")
        current = {}

        async for msg in tg.get_chat_history(CHANNEL_USERNAME):
            try:
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

                quality = detect_quality(filename)
                source = detect_source(filename)

                current[movie_id] = {
                    "message_id": msg.id,
                    "file_name": filename,
                    "file_size": media.file_size,
                    "file_size_text": format_size(media.file_size),
                    "quality": quality,
                    "source": source
                }

            except Exception:
                continue

        save_movies(current)
        print(f"✅ INSTANT SYNC COMPLETE: {len(current)} MOVIES")


# ---------------------------------------------------
# SHARED SYNC (MANUAL OVERRIDE)
# ---------------------------------------------------
async def sync_channel():
    current = {}
    async for msg in tg.get_chat_history(CHANNEL_USERNAME):
        try:
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

            quality = detect_quality(filename)
            source = detect_source(filename)

            current[movie_id] = {
                "message_id": msg.id,
                "file_name": filename,
                "file_size": media.file_size,
                "file_size_text": format_size(media.file_size),
                "quality": quality,
                "source": source
            }

        except Exception:
            continue

    save_movies(current)
    print(f"✅ MANUAL SYNC COMPLETE: {len(current)} MOVIES")


# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------
@app.on_event("startup")
async def startup():
    await tg.start()
    try:
        await tg.get_chat(CHANNEL_USERNAME)
        print("✅ Pyrogram started + DC warmed up (TgCrypto active)")
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
        "cached_urls": len(URL_CACHE),
        "channel": CHANNEL_USERNAME
    }

# ---------------------------------------------------
# RESET
# ---------------------------------------------------
@app.get("/reset")
async def reset():
    global MOVIES_CACHE
    global URL_CACHE
    try:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        MOVIES_CACHE = {}
        URL_CACHE = {}
        return {"status": "database deleted"}
    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------
# SYNC CHANNEL
# ---------------------------------------------------
@app.get("/sync")
async def sync_movies():
    await sync_channel()
    return {"status": "ok", "movies": len(load_movies())}

# ---------------------------------------------------
# DEBUG
# ---------------------------------------------------
@app.get("/debug")
async def debug():
    movies = load_movies()
    return {
        "movies": len(movies),
        "channel": CHANNEL_USERNAME
    }

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------
manifest = {
    "id": "org.arun.telegram",
    "version": "16.0.0",
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
    print("🎬 CATALOG REQUEST")
    
    # Always fetch latest data from Telegram
    await auto_sync()
    
    movies = load_movies()
    print(f"📁 MOVIES IN DB: {len(movies)}")
    
    metas = []
    for movie_id, movie in movies.items():
        movie_name = movie.get("file_name", "Unknown Movie")
        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie_name,
            "poster": "https://placehold.co/300x450?text=Telegram",
            "background": "https://placehold.co/1280x720?text=Telegram",
            "description": movie_name,
            "posterShape": "poster"
        })
        
    # Prevent Stremio from caching deleted items
    return JSONResponse(
        content={"metas": metas},
        headers={
            "Cache-Control": "max-age=0, no-cache, no-store, must-revalidate"
        }
    )

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

    movie_name = movie.get("file_name", "Unknown Movie")
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

    # Pull details from the database
    movie_name = movie.get("file_name", "Unknown Movie")
    quality = movie.get("quality", "Unknown")
    size = movie.get("file_size_text", "Unknown")
    source = movie.get("source", "")
    
    # ⚡ NEW: Format the Stremio button text using newlines (\n)
    source_text = f" | 🏷️ {source}" if source else ""
    stream_title = f"{movie_name}\n⚙️ {quality}{source_text} | 💾 {size}"

    try:
        msg = await get_message(movie["message_id"])
        
        # Dynamic Deletion Check
        if getattr(msg, "empty", False) or not (getattr(msg, "video", None) or getattr(msg, "document", None)):
            remove_movie_from_db(clean_id)
            return JSONResponse({"streams": []})

        cdn_url = await get_cdn_url(clean_id, msg)
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
            headers={"Location": cdn_url}
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
        raise HTTPException(status_code=404, detail="Movie not found in database")

    msg = await get_message(movie["message_id"])
    
    # Dynamic Deletion Check for the proxy
    if getattr(msg, "empty", False) or not (getattr(msg, "video", None) or getattr(msg, "document", None)):
        remove_movie_from_db(movie_id)
        raise HTTPException(status_code=404, detail="Media deleted from Telegram channel")

    media = msg.video or msg.document

    file_size = movie.get("file_size", media.file_size)
    filename = movie.get("file_name", "video.mkv").lower()

    content_type = "video/mp4"
    if filename.endswith(".mkv"):
        content_type = "video/x-matroska"
    elif filename.endswith(".webm"):
        content_type = "video/webm"

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
            bytes_range = range_header.replace("bytes=", "").split("-")
            if bytes_range[0]:
                start = int(bytes_range[0])
            if len(bytes_range) > 1 and bytes_range[1]:
                end = int(bytes_range[1])
        except:
            pass

    if end >= file_size:
        end = file_size - 1

    # ---------------------------------------------------
    # TELEGRAM NATIVE CHUNK SIZE (DO NOT CHANGE)
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

        # Calculate exactly how many 1MB chunks we need from Telegram
        bytes_to_fetch = (end - start + 1) + skip_bytes
        chunks_to_fetch = (bytes_to_fetch + TG_CHUNK_SIZE - 1) // TG_CHUNK_SIZE

        # Prevents parallel DC connection flooding
        async with STREAM_LIMITER:
            try:
                async for chunk in tg.stream_media(
                    msg,
                    offset=chunk_offset,
                    limit=chunks_to_fetch
                ):
                    # Kill stream immediately if user skipped forward
                    if await request.is_disconnected():
                        break

                    if first_chunk:
                        chunk = chunk[skip_bytes:]
                        first_chunk = False

                    remaining = (end - start + 1) - sent

                    if remaining <= 0:
                        break

                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]

                    sent += len(chunk)
                    yield chunk

            except FloodWait as e:
                print(f"\n🚨 FloodWait: {e.value}s\n")
            except Exception:
                # Safely ignore client disconnect exceptions to keep terminal clean
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
