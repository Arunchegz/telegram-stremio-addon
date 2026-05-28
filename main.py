from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client, filters
from pyrogram.types import Message

import os
import re
import json
import asyncio
from urllib.parse import quote

# ---------------------------------------------------
# ENV
# ---------------------------------------------------

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

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


def clean_title(filename):

    title = os.path.splitext(filename)[0]

    title = title.replace(".", " ")
    title = title.replace("_", " ")

    return re.sub(r"\s+", " ", title).strip()


def detect_quality(filename):

    name = filename.lower()

    for q in [
        "2160p",
        "1440p",
        "1080p",
        "720p",
        "480p",
        "360p"
    ]:

        if q in name:
            return q

    if "4k" in name:
        return "2160p"

    return "Unknown"


def detect_resolution(filename):

    name = filename.lower()

    mapping = {
        "2160p": "3840x2160",
        "1440p": "2560x1440",
        "1080p": "1920x1080",
        "720p": "1280x720",
        "480p": "854x480",
        "360p": "640x360"
    }

    for key, value in mapping.items():

        if key in name:
            return value

    return "Unknown"


def format_size(size):

    if not size:
        return "Unknown"

    power = 1024
    n = 0
    labels = ["B", "KB", "MB", "GB", "TB"]

    while size > power:

        size /= power
        n += 1

    return f"{size:.2f} {labels[n]}"


async def add_movie(message: Message):

    media = message.video or message.document

    if not media:
        return

    filename = media.file_name or f"{message.id}.mkv"

    movie_id = re.sub(r'[^a-zA-Z0-9]', '_', filename).lower()

    exists = next(
        (x for x in catalog if x["id"] == movie_id),
        None
    )

    if exists:
        return

    movie = {
        "id": movie_id,
        "filename": filename,
        "message_id": message.id,
        "title": clean_title(filename),
        "quality": detect_quality(filename),
        "resolution": detect_resolution(filename),
        "size": format_size(media.file_size)
    }

    catalog.append(movie)

    save_catalog()

    print(f"✅ Auto Synced: {filename}")


# ---------------------------------------------------
# FULL SYNC
# ---------------------------------------------------

async def full_sync():

    print("🔄 Full sync started")

    try:

        async for message in tg.get_chat_history(CHANNEL_USERNAME):

            try:
                await add_movie(message)

            except Exception as e:
                print("SYNC ITEM ERROR:", e)

        print("✅ Full sync completed")

    except Exception as e:

        print("FULL SYNC ERROR:", e)


# ---------------------------------------------------
# AUTO NEW FILE SYNC
# ---------------------------------------------------

@tg.on_message(filters.chat(CHANNEL_USERNAME))
async def new_files(client, message):

    try:

        await add_movie(message)

    except Exception as e:

        print("NEW FILE ERROR:", e)


# ---------------------------------------------------
# STARTUP
# ---------------------------------------------------

@app.on_event("startup")
async def startup():

    load_catalog()

    try:

        await asyncio.sleep(2)

        await tg.start()

        await tg.get_me()

        print("✅ Pyrogram started")

        chat = await tg.get_chat(CHANNEL_USERNAME)

        print(f"✅ Connected Channel: {chat.title}")
        print(f"✅ Channel Username: {CHANNEL_USERNAME}")

        asyncio.create_task(full_sync())

    except Exception as e:

        print(f"❌ CHANNEL ERROR: {e}")


# ---------------------------------------------------
# SHUTDOWN
# ---------------------------------------------------

@app.on_event("shutdown")
async def shutdown():

    try:

        await tg.stop()

    except Exception as e:

        print(f"⚠️ Shutdown loop warning: {e}")


# ---------------------------------------------------
# ROOT
# ---------------------------------------------------

@app.get("/")
async def root():

    return {
        "status": "running",
        "movies": len(catalog)
    }


# ---------------------------------------------------
# RESET
# ---------------------------------------------------

@app.get("/reset")
async def reset():

    global catalog

    catalog = []

    save_catalog()

    return {
        "status": "reset completed"
    }


# ---------------------------------------------------
# MANUAL SYNC
# ---------------------------------------------------

@app.get("/sync")
async def sync():

    asyncio.create_task(full_sync())

    return {
        "status": "sync started"
    }


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
async def meta(movie_id: str):

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
                f"🎬 {movie['quality']}\n"
                f"📺 {movie['resolution']}\n"
                f"📦 {movie['size']}"
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

    proxy_url = f"{BASE_URL}/proxy/{quote(movie['id'])}"

    print(f"🔗 Generated Proxy URL: {movie['id']}")

    return {
        "streams": [
            {
                "name": "Telegram",
                "title": (
                    f"{movie['quality']} | "
                    f"{movie['resolution']} | "
                    f"{movie['size']}"
                ),
                "url": proxy_url
            }
        ]
    }


# ---------------------------------------------------
# PROXY
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
        CHANNEL_USERNAME,
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

                remain = chunk_size - downloaded

                yield chunk[:remain]

                break

            yield chunk

            downloaded += len(chunk)

            if downloaded >= chunk_size:
                break

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": "video/mp4"
    }

    return StreamingResponse(
        file_stream(),
        status_code=206 if range_header else 200,
        headers=headers
    )