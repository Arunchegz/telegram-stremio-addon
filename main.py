from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client
from pyrogram.errors import FloodWait

import os
import json
import asyncio
import re
import requests

# ---------------------------------------------------
# ENV
# ---------------------------------------------------
API_ID        = int(os.getenv("API_ID", "0"))
API_HASH      = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
BASE_URL      = os.getenv("BASE_URL", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
DB_FILE       = "/app/data/movies.json"

# ---------------------------------------------------
# PYROGRAM CLIENT
# ---------------------------------------------------
tg = Client(
    "streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    no_updates=True,
    workers=8,
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
# STATE
# ---------------------------------------------------
MOVIES_CACHE: dict = {}
SYNC_LOCK = asyncio.Lock()
STREAM_LIMITER = asyncio.Semaphore(5)   # limit parallel Telegram DC connections

TG_CHUNK_SIZE = 1024 * 1024             # Telegram's native 1 MB chunk (do not change)

# ---------------------------------------------------
# HELPERS (Formatting & Pyrogram)
# ---------------------------------------------------
def format_size(size) -> str:
    if not size:
        return "Unknown"
    size = float(size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def detect_quality(filename: str) -> str:
    name = (filename or "").lower()
    for tag in ["2160p", "4k", "1440p", "1080p", "720p", "480p", "360p"]:
        if tag in name:
            return tag.upper()
    return "Unknown"


def detect_source(filename: str) -> str:
    name = (filename or "").lower()
    for tag in ["bluray", "bdrip", "web-dl", "webdl", "webrip", "hdrip", "dvdrip", "hdtv", "remux"]:
        if tag in name:
            return tag.upper()
    return ""


def make_movie_id(filename: str) -> str:
    return filename.replace(" ", "_").replace(".", "_").lower()


def content_type_for(filename: str) -> str:
    name = filename.lower()
    if name.endswith(".mkv"):
        return "video/x-matroska"
    if name.endswith(".webm"):
        return "video/webm"
    return "video/mp4"

# ---------------------------------------------------
# HELPERS (IMDb Matching & String parsing)
# ---------------------------------------------------
def normalize(text: str):
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def flexible_match(title: str, filename: str):
    title_n = normalize(title)
    file_n = normalize(filename)
    words = title_n.split()
    
    if not words:
        return False

    matched = sum(1 for w in words if w in file_n)

    if len(words) <= 2:
        required = len(words)
    else:
        required = max(2, len(words) // 2)

    return matched >= required


def get_cinemeta(type_name: str, imdb_id: str):
    url = f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json"
    try:
        r = requests.get(url, timeout=10)
        meta = r.json().get("meta", {})
        return (
            meta.get("name", ""),
            str(meta.get("year", ""))
        )
    except Exception:
        return ("", "")


# ---------------------------------------------------
# DB
# ---------------------------------------------------
def load_movies() -> dict:
    global MOVIES_CACHE
    if MOVIES_CACHE:
        return MOVIES_CACHE
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE) as f:
                MOVIES_CACHE = json.load(f)
    except Exception as e:
        print("DB load error:", e)
    return MOVIES_CACHE


def save_movies(data: dict) -> None:
    global MOVIES_CACHE
    try:
        os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
        with open(DB_FILE, "w") as f:
            json.dump(data, f, indent=2)
        MOVIES_CACHE = data
    except Exception as e:
        print("DB save error:", e)


def remove_movie(movie_id: str) -> None:
    movies = load_movies()
    if movie_id in movies:
        del movies[movie_id]
        save_movies(movies)
        print(f"🗑️  Removed deleted media: {movie_id}")


# ---------------------------------------------------
# TELEGRAM HELPERS
# ---------------------------------------------------
def get_media(msg):
    """Return video or document from a message, or None."""
    return msg.video or msg.document or None


def is_empty(msg) -> bool:
    return getattr(msg, "empty", False) or get_media(msg) is None


async def fetch_message(message_id: int):
    return await tg.get_messages(CHANNEL_USERNAME, message_id)


# ---------------------------------------------------
# SYNC
# ---------------------------------------------------
async def sync_channel() -> int:
    """Fetch all media from the Telegram channel and persist to DB."""
    async with SYNC_LOCK:
        current: dict = {}
        async for msg in tg.get_chat_history(CHANNEL_USERNAME):
            try:
                media = get_media(msg)
                if not media:
                    continue
                filename = getattr(media, "file_name", None)
                if not filename:
                    continue
                movie_id = make_movie_id(filename)
                current[movie_id] = {
                    "message_id":    msg.id,
                    "file_name":     filename,
                    "file_size":     media.file_size,
                    "file_size_text": format_size(media.file_size),
                    "quality":       detect_quality(filename),
                    "source":        detect_source(filename),
                }
            except Exception:
                continue

        save_movies(current)
        print(f"✅ Sync complete: {len(current)} movies")
        return len(current)


# ---------------------------------------------------
# LIFECYCLE
# ---------------------------------------------------
@app.on_event("startup")
async def startup():
    await tg.start()
    try:
        await tg.get_chat(CHANNEL_USERNAME)
        print("✅ Pyrogram started")
    except Exception as e:
        print(f"⚠️  Startup warning: {e}")


@app.on_event("shutdown")
async def shutdown():
    await tg.stop()
    print("🛑 Pyrogram stopped")


# ---------------------------------------------------
# UTILITY ROUTES
# ---------------------------------------------------
@app.get("/")
async def home():
    movies = load_movies()
    return {"status": "running", "movies": len(movies), "channel": CHANNEL_USERNAME}


@app.get("/sync")
async def sync_route():
    count = await sync_channel()
    return {"status": "ok", "movies": count}


@app.get("/reset")
async def reset():
    global MOVIES_CACHE
    try:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        MOVIES_CACHE = {}
        return {"status": "database cleared"}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------
# STREMIO MANIFEST
# ---------------------------------------------------
MANIFEST = {
    "id": "org.arun.telegram",
    "version": "1.0.0",
    "name": "Telegram Movies",
    "description": "Telegram Movie Catalog",
    "resources": ["catalog", "meta", "stream"],
    "types": ["movie"],
    "idPrefixes": ["tg"],
    "catalogs": [
        {
            "type": "movie",
            "id": "telegrammovies",
            "name": "Telegram Movies"
        }
    ],
    "behaviorHints": {
        "configurable": False,
        "configurationRequired": False
    }
}


@app.get("/manifest.json")
async def get_manifest():
    return JSONResponse(MANIFEST)


# ---------------------------------------------------
# STREMIO CATALOG
# ---------------------------------------------------
@app.get("/catalog/movie/telegrammovies.json")
async def catalog():
    await sync_channel()          # always refresh before serving
    movies = load_movies()

    metas = [
        {
            "id":          f"tg:{mid}",
            "type":        "movie",
            "name":        m.get("file_name", "Unknown"),
            "poster":      "https://placehold.co/300x450?text=Telegram",
            "background":  "https://placehold.co/1280x720?text=Telegram",
            "description": m.get("file_name", ""),
            "posterShape": "poster",
        }
        for mid, m in movies.items()
    ]

    return JSONResponse(
        {"metas": metas},
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------
# STREMIO META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):
    # Handle IMDb requests
    if id.startswith("tt"):
        title, year = get_cinemeta("movie", id)
        return JSONResponse({
            "meta": {
                "id": id,
                "type": "movie",
                "name": title,
                "year": year,
                "poster": "https://placehold.co/300x450?text=Telegram",
            }
        })

    # Handle internal Telegram Catalog requests
    clean_id = id[3:] if id.startswith("tg:") else id
    movie = load_movies().get(clean_id)
    if not movie:
        return JSONResponse({"meta": {}})

    name = movie.get("file_name", "Unknown")
    return JSONResponse({
        "meta": {
            "id":          id,
            "type":        "movie",
            "name":        name,
            "poster":      "https://placehold.co/300x450?text=Telegram",
            "background":  "https://placehold.co/1280x720?text=Telegram",
            "description": name,
            "posterShape": "poster",
        }
    })


# ---------------------------------------------------
# STREMIO STREAM
# ---------------------------------------------------
@app.get("/stream/movie/{id}.json")
async def stream(id: str):
    movies = load_movies()
    
    # ---------------------------------------------------
    # IMDb Matcher (Discover Page)
    # ---------------------------------------------------
    if id.startswith("tt"):
        movie_title, movie_year = get_cinemeta("movie", id)
        if not movie_title:
            return JSONResponse({"streams": []})

        streams = []
        for mid, m in movies.items():
            name = m.get("file_name", "")
            
            if not flexible_match(movie_title, name):
                continue
            
            # Year validation
            if movie_year and movie_year not in name:
                continue

            quality = m.get("quality", "Unknown")
            size    = m.get("file_size_text", "Unknown")
            source  = m.get("source", "")
            src_tag = f" | 🏷️ {source}" if source else ""
            title   = f"{name}\n⚙️ {quality}{src_tag} | 💾 {size}"

            streams.append({
                "name":  "⚡ Telegram",
                "title": title,
                "url":   f"{BASE_URL}/proxy/{mid}",
            })
            
        return JSONResponse({"streams": streams})


    # ---------------------------------------------------
    # Internal Catalog Streamer (Telegram Addon Page)
    # ---------------------------------------------------
    clean_id = id[3:] if id.startswith("tg:") else id
    movie = movies.get(clean_id)
    if not movie:
        return JSONResponse({"streams": []})

    name    = movie.get("file_name", "Unknown")
    quality = movie.get("quality", "Unknown")
    size    = movie.get("file_size_text", "Unknown")
    source  = movie.get("source", "")
    src_tag = f" | 🏷️ {source}" if source else ""
    title   = f"{name}\n⚙️ {quality}{src_tag} | 💾 {size}"

    try:
        msg = await fetch_message(movie["message_id"])
        if is_empty(msg):
            remove_movie(clean_id)
            return JSONResponse({"streams": []})
    except Exception as e:
        print(f"❌ Stream fetch error: {e}")
        return JSONResponse({"streams": []})

    return JSONResponse({
        "streams": [
            {
                "name":  "⚡ Telegram",
                "title": title,
                "url":   f"{BASE_URL}/proxy/{clean_id}",
            }
        ]
    })


# ---------------------------------------------------
# PROXY / RANGE STREAMING
# ---------------------------------------------------
@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy_stream(movie_id: str, request: Request):
    movie = load_movies().get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    msg = await fetch_message(movie["message_id"])
    if is_empty(msg):
        remove_movie(movie_id)
        raise HTTPException(status_code=404, detail="Media deleted from channel")

    media      = get_media(msg)
    file_size  = movie.get("file_size") or media.file_size
    filename   = movie.get("file_name", "video.mp4")
    ctype      = content_type_for(filename)

    # HEAD — used by players to probe file metadata
    if request.method == "HEAD":
        return Response(
            status_code=200,
            headers={
                "Accept-Ranges":  "bytes",
                "Content-Length": str(file_size),
                "Content-Type":   ctype,
            },
        )

    # Parse Range header
    start, end = 0, file_size - 1
    range_header = request.headers.get("range")
    if range_header:
        try:
            parts = range_header[6:].split("-") if range_header.startswith("bytes=") else range_header.split("-")
            if parts[0]:
                start = int(parts[0])
            if len(parts) > 1 and parts[1]:
                end = int(parts[1])
        except ValueError:
            pass

    end = min(end, file_size - 1)

    # Align to Telegram chunk boundaries
    chunk_offset  = start // TG_CHUNK_SIZE
    skip_bytes    = start % TG_CHUNK_SIZE
    bytes_needed  = (end - start + 1) + skip_bytes
    chunks_needed = (bytes_needed + TG_CHUNK_SIZE - 1) // TG_CHUNK_SIZE

    async def streamer():
        sent       = 0
        first      = True
        total_want = end - start + 1

        async with STREAM_LIMITER:
            try:
                async for chunk in tg.stream_media(
                    msg,
                    offset=chunk_offset,
                    limit=chunks_needed,
                ):
                    if await request.is_disconnected():
                        break

                    if first:
                        chunk = chunk[skip_bytes:]
                        first = False

                    remaining = total_want - sent
                    if remaining <= 0:
                        break

                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]

                    sent += len(chunk)
                    yield chunk

            except FloodWait as e:
                print(f"🚨 FloodWait: {e.value}s — backing off")
                await asyncio.sleep(e.value)
            except Exception:
                pass   # client disconnect or Telegram hiccup — exit cleanly

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers={
            "Accept-Ranges":  "bytes",
            "Content-Range":  f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(end - start + 1),
            "Content-Type":   ctype,
            "Cache-Control":  "public, max-age=3600",
            "Connection":     "keep-alive",
        },
    )

# Required by Vercel
application = app
