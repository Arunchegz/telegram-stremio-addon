from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pyrogram import Client, filters
from pyrogram.types import Message

import asyncio
import os
import re
import json
import time
import mimetypes

# =========================================================
# ENV
# =========================================================

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Use channel username now
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")

BASE_URL = os.getenv("BASE_URL")

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
# PYROGRAM
# =========================================================

tg = Client(
    "telegram-stream-bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=1
)

# =========================================================
# DATABASE
# =========================================================

DB_FILE = "movies.json"

if os.path.exists(DB_FILE):
    with open(DB_FILE, "r") as f:
        MOVIES = json.load(f)
else:
    MOVIES = {}

# =========================================================
# HELPERS
# =========================================================

def save_db():
    with open(DB_FILE, "w") as f:
        json.dump(MOVIES, f, indent=2)

def clean_name(name):
    name = re.sub(r"\.(mkv|mp4|avi)$", "", name, flags=re.I)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    return name.lower().strip("_")

def extract_resolution(filename):

    filename = filename.lower()

    resolutions = [
        "2160p",
        "4k",
        "1440p",
        "1080p",
        "720p",
        "480p",
        "360p"
    ]

    for r in resolutions:
        if r in filename:
            return r.upper()

    return "HD"

def extract_year(filename):

    match = re.search(r"(19\d{2}|20\d{2})", filename)

    if match:
        return match.group(1)

    return ""

async def add_movie(message: Message):

    if not message.video and not message.document:
        return

    media = message.video or message.document

    file_name = media.file_name or f"{message.id}.mp4"

    movie_id = clean_name(file_name)

    resolution = extract_resolution(file_name)

    year = extract_year(file_name)

    MOVIES[movie_id] = {
        "message_id": message.id,
        "file_name": file_name,
        "size": media.file_size,
        "resolution": resolution,
        "year": year
    }

    save_db()

    print(f"✅ Auto Synced: {file_name}")

# =========================================================
# TELEGRAM LISTENER
# =========================================================

@tg.on_message(filters.chat(CHANNEL_USERNAME))
async def new_post(_, message):

    try:
        await add_movie(message)

    except Exception as e:
        print("ADD MOVIE ERROR:", e)

# =========================================================
# STARTUP / SHUTDOWN
# =========================================================

@app.on_event("startup")
async def startup():

    try:

        await tg.start()

        print("✅ Pyrogram started")

        chat = await tg.get_chat(CHANNEL_USERNAME)

        print(f"✅ Connected Channel: {chat.title}")
        print(f"✅ Channel Username: {chat.username}")

    except Exception as e:

        print(f"STARTUP ERROR: {e}")

@app.on_event("shutdown")
async def shutdown():

    try:

        await tg.stop()

    except Exception as e:

        print(f"⚠️ Shutdown loop warning: {e}")

# =========================================================
# CATALOG
# =========================================================

@app.get("/catalog/movie/telegrammovies.json")
async def catalog():

    metas = []

    for movie_id, movie in MOVIES.items():

        metas.append({
            "id": f"tg:{movie_id}",
            "type": "movie",
            "name": movie["file_name"],
            "poster": "https://via.placeholder.com/300x450.png?text=Telegram",
            "description": movie["file_name"]
        })

    return JSONResponse({"metas": metas})

# =========================================================
# META
# =========================================================

@app.get("/meta/movie/{movie_id}.json")
async def meta(movie_id: str):

    movie_id = movie_id.replace("tg:", "")

    if movie_id not in MOVIES:
        raise HTTPException(404)

    movie = MOVIES[movie_id]

    meta = {
        "id": f"tg:{movie_id}",
        "type": "movie",
        "name": movie["file_name"],
        "poster": "https://via.placeholder.com/300x450.png?text=Telegram",
        "description": movie["file_name"],
        "releaseInfo": movie["year"]
    }

    return JSONResponse({"meta": meta})

# =========================================================
# STREAM
# =========================================================

@app.get("/stream/movie/{movie_id}.json")
async def stream(movie_id: str):

    movie_id = movie_id.replace("tg:", "")

    if movie_id not in MOVIES:
        raise HTTPException(404)

    proxy_url = f"{BASE_URL}/proxy/{movie_id}"

    print(f"🔗 Generated Proxy URL: {movie_id}")

    movie = MOVIES[movie_id]

    streams = [{
        "name": f"Telegram {movie['resolution']}",
        "title": movie["file_name"],
        "url": proxy_url
    }]

    return JSONResponse({"streams": streams})

# =========================================================
# PROXY STREAMING
# =========================================================

@app.get("/proxy/{movie_id}")
async def proxy_stream(movie_id: str, request: Request):

    if movie_id not in MOVIES:
        raise HTTPException(404)

    movie = MOVIES[movie_id]

    message_id = movie["message_id"]

    msg = await tg.get_messages(CHANNEL_USERNAME, message_id)

    media = msg.video or msg.document

    file_size = media.file_size

    mime_type = mimetypes.guess_type(movie["file_name"])[0] or "video/mp4"

    range_header = request.headers.get("range", None)

    start = 0
    end = file_size - 1

    if range_header:

        match = re.search(r"bytes=(\d+)-(\d*)", range_header)

        if match:

            start = int(match.group(1))

            if match.group(2):
                end = int(match.group(2))

    chunk_size = end - start + 1

    async def streamer():

        offset = start

        while offset <= end:

            limit = min(1024 * 1024, end - offset + 1)

            chunk = await tg.stream_media(
                message=msg,
                offset=offset,
                limit=limit
            )

            if not chunk:
                break

            yield chunk

            offset += len(chunk)

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(chunk_size),
        "Content-Type": mime_type
    }

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers=headers
    )

# =========================================================
# MANIFEST
# =========================================================

@app.get("/manifest.json")
async def manifest():

    return {
        "id": "org.arun.telegram",
        "version": "1.0.0",
        "name": "Telegram Stream Addon",
        "description": "Telegram streaming addon",
        "resources": [
            "catalog",
            "meta",
            "stream"
        ],
        "types": [
            "movie"
        ],
        "catalogs": [
            {
                "type": "movie",
                "id": "telegrammovies",
                "name": "Telegram Movies"
            }
        ]
    }