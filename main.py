from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client, filters
from pyrogram.types import Message

import os
import re
import json
import asyncio
from urllib.parse import quote

# ---------------------------------------------------
# CONFIG
# ---------------------------------------------------

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

BASE_URL = os.getenv(
    "BASE_URL",
    "https://your-app.up.railway.app"
)

CATALOG_FILE = "catalog.json"

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
# PYROGRAM
# ---------------------------------------------------

tg = Client(
    "telegram-session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=4
)

# ---------------------------------------------------
# CACHE
# ---------------------------------------------------

catalog = []

# ---------------------------------------------------
# HELPERS
# ---------------------------------------------------

def load_catalog():
    global catalog

    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE, "r") as f:
            catalog = json.load(f)
    else:
        catalog = []


def save_catalog():
    with open(CATALOG_FILE, "w") as f:
        json.dump(catalog, f, indent=2)


def format_size(size_bytes):

    if not size_bytes:
        return "Unknown"

    size = float(size_bytes)

    for unit in ["B", "KB", "MB", "GB", "TB"]:

        if size < 1024:
            return f"{size:.2f} {unit}"

        size /= 1024

    return f"{size:.2f} PB"


def detect_quality(filename: str):

    name = filename.lower()

    if "2160p" in name or "4k" in name:
        return "2160p"

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


def detect_resolution(filename: str):

    name = filename.lower()

    patterns = [
        r'3840x2160',
        r'2560x1440',
        r'1920x1080',
        r'1280x720',
        r'854x480',
        r'640x360'
    ]

    for pattern in patterns:

        match = re.search(pattern, name)

        if match:
            return match.group(0)

    if "2160p" in name or "4k" in name:
        return "3840x2160"

    elif "1440p" in name:
        return "2560x1440"

    elif "1080p" in name:
        return "1920x1080"

    elif "720p" in name:
        return "1280x720"

    elif "480p" in name:
        return "854x480"

    elif "360p" in name:
        return "640x360"

    return "Unknown"


def clean_title(filename):

    title = os.path.splitext(filename)[0]

    title = title.replace(".", " ")
    title = title.replace("_", " ")
    title = re.sub(r"\s+", " ", title)

    return title.strip()


def create_stream(movie):

    return {
        "name": "Telegram",
        "title": (
            f"{movie['title']}\n"
            f"💿 {movie['quality']}\n"
            f"📺 {movie['resolution']}\n"
            f"📦 {movie['size']}"
        ),
        "url": f"{BASE_URL}/proxy/{quote(movie['id'])}"
    }


# ---------------------------------------------------
# SYNC
# ---------------------------------------------------

async def add_movie(message: Message):

    media = message.video or message.document

    if not media:
        return

    filename = media.file_name or f"file_{message.id}.mkv"

    existing = next(
        (x for x in catalog if x["id"] == filename),
        None
    )

    if existing:
        return

    quality = detect_quality(filename)

    resolution = detect_resolution(filename)

    size = format_size(media.file_size)

    movie = {
        "id": filename,
        "title": clean_title(filename),
        "filename": filename,
        "message_id": message.id,
        "quality": quality,
        "resolution": resolution,
        "size": size
    }

    catalog.append(movie)

    save_catalog()

    print(f"✅ Added: {filename}")


async def full_sync():

    print("🔄 Full sync started")

    async for message in tg.get_chat_history(CHANNEL_ID):

        try:
            await add_movie(message)

        except Exception as e:
            print("SYNC ERROR:", e)

    print("✅ Full sync completed")


# ---------------------------------------------------
# AUTO SYNC NEW FILES
# ---------------------------------------------------

@tg.on_message(filters.chat(CHANNEL_ID))
async def new_message_handler(client, message):

    try:

        await add_movie(message)

    except Exception as e:

        print("NEW MESSAGE ERROR:", e)


# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------

@app.on_event("startup")
async def startup():

    load_catalog()

    await tg.start()

    print("✅ Pyrogram started")

    try:
        await tg.get_chat(CHANNEL_ID)
        print("✅ Channel verified")

    except Exception as e:
        print("❌ CHANNEL ERROR:", e)

    asyncio.create_task(full_sync())


@app.on_event("shutdown")
async def shutdown():

    try:

        await tg.stop()

        print("🛑 Pyrogram stopped")

    except RuntimeError as e:

        print(f"⚠️ Shutdown loop warning: {e}")


# ---------------------------------------------------
# WEBHOOK
# ---------------------------------------------------

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):

    return {"ok": True}


# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------

@app.get("/manifest.json")
async def manifest():

    return {
        "id": "org.telegram.stream",
        "version": "1.0.0",
        "name": "Telegram Movies",
        "description": "Telegram Streaming Addon",
        "resources": ["catalog", "meta", "stream"],
        "types": ["movie"],
        "catalogs": [
            {
                "type": "movie",
                "id": "telegrammovies",
                "name": "Telegram Movies"
            }
        ]
    }


# ---------------------------------------------------
# CATALOG
# ---------------------------------------------------

@app.get("/catalog/movie/telegrammovies.json")
async def get_catalog():

    metas = []

    for movie in reversed(catalog[-200:]):

        metas.append({
            "id": f"tg:{movie['id']}",
            "type": "movie",
            "name": movie["title"],
            "poster": "https://stremio.github.io/stremio-art/logo.png"
        })

    return {"metas": metas}


# ---------------------------------------------------
# META
# ---------------------------------------------------

@app.get("/meta/movie/{movie_id}.json")
async def get_meta(movie_id: str):

    movie_id = movie_id.replace("tg:", "")

    movie = next(
        (x for x in catalog if x["id"] == movie_id),
        None
    )

    if not movie:
        raise HTTPException(404)

    return {
        "meta": {
            "id": f"tg:{movie['id']}",
            "type": "movie",
            "name": movie["title"],
            "description": (
                f"Quality: {movie['quality']}\n"
                f"Resolution: {movie['resolution']}\n"
                f"Size: {movie['size']}"
            ),
            "poster": "https://stremio.github.io/stremio-art/logo.png"
        }
    }


# ---------------------------------------------------
# STREAM
# ---------------------------------------------------

@app.get("/stream/movie/{movie_id}.json")
async def stream(movie_id: str):

    movie_id = movie_id.replace("tg:", "")

    movie = next(
        (x for x in catalog if x["id"] == movie_id),
        None
    )

    if not movie:
        raise HTTPException(404)

    return {
        "streams": [
            create_stream(movie)
        ]
    }


# ---------------------------------------------------
# PROXY STREAM
# ---------------------------------------------------

@app.api_route("/proxy/{movie_id:path}", methods=["GET", "HEAD"])
async def proxy(movie_id: str, request: Request):

    movie = next(
        (x for x in catalog if x["id"] == movie_id),
        None
    )

    if not movie:
        raise HTTPException(404)

    message = await tg.get_messages(
        CHANNEL_ID,
        movie["message_id"]
    )

    media = message.video or message.document

    if not media:
        raise HTTPException(404)

    file_size = media.file_size

    range_header = request.headers.get("range")

    start = 0
    end = file_size - 1

    if range_header:

        match = re.match(r"bytes=(\d+)-(\d*)", range_header)

        if match:

            start = int(match.group(1))

            if match.group(2):
                end = int(match.group(2))

    chunk_size = end - start + 1

    async def file_stream():

        downloaded = 0

        async for chunk in tg.stream_media(
            message,
            offset=start
        ):

            if downloaded + len(chunk) > chunk_size:

                remaining = chunk_size - downloaded

                yield chunk[:remaining]

                break

            yield chunk

            downloaded += len(chunk)

            if downloaded >= chunk_size:
                break

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "video/mp4",
    }

    return StreamingResponse(
        file_stream(),
        status_code=206 if range_header else 200,
        headers=headers
    )


# ---------------------------------------------------
# ROOT
# ---------------------------------------------------

@app.get("/")
async def root():

    return {
        "status": "running",
        "movies": len(catalog)
    }