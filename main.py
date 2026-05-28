from fastapi import FastAPI, HTTPException, Request
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
no_updates=True,
workers=16
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

# CACHE

# ---------------------------------------------------

MOVIES_CACHE = {}
MESSAGES_CACHE = {}

# ---------------------------------------------------

# DATABASE FUNCTIONS

# ---------------------------------------------------

def load_movies():

```
global MOVIES_CACHE

if MOVIES_CACHE:
    return MOVIES_CACHE

try:

    if os.path.exists(DB_FILE):

        with open(DB_FILE, "r") as f:
            MOVIES_CACHE = json.load(f)

    else:
        MOVIES_CACHE = {}

    return MOVIES_CACHE

except Exception as e:

    print("DB Load Error:", e)

    return {}
```

def save_movies(data):

```
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
```

# ---------------------------------------------------

# GET MESSAGE

# ---------------------------------------------------

async def get_message(
movie_id: str,
message_id: int
) -> Message:

```
msg = MESSAGES_CACHE.get(movie_id)

if not msg:

    msg = await tg.get_messages(
        CHANNEL_USERNAME,
        message_id
    )

    MESSAGES_CACHE[movie_id] = msg

return msg
```

# ---------------------------------------------------

# STARTUP

# ---------------------------------------------------

@app.on_event("startup")
async def startup():

```
await tg.start()

try:

    await tg.get_chat(
        CHANNEL_USERNAME
    )

    print("✅ Pyrogram started + DC warmed up")

except Exception as e:

    print(f"⚠️ Warm-up warning: {e}")
```

# ---------------------------------------------------

# SHUTDOWN

# ---------------------------------------------------

@app.on_event("shutdown")
async def shutdown():

```
await tg.stop()

print("🛑 Pyrogram stopped")
```

# ---------------------------------------------------

# HOME

# ---------------------------------------------------

@app.get("/")
async def home():

```
movies = load_movies()

return {
    "status": "running",
    "movies": len(movies),
    "cached_messages": len(MESSAGES_CACHE),
    "channel": CHANNEL_USERNAME
}
```

# ---------------------------------------------------

# RESET

# ---------------------------------------------------

@app.get("/reset")
async def reset():

```
global MOVIES_CACHE
global MESSAGES_CACHE

try:

    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    MOVIES_CACHE = {}
    MESSAGES_CACHE = {}

    return {
        "status": "database deleted"
    }

except Exception as e:

    return {
        "error": str(e)
    }
```

# ---------------------------------------------------

# SYNC CHANNEL

# ---------------------------------------------------

@app.get("/sync")
async def sync_movies():

```
global MESSAGES_CACHE

try:

    current = {}

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
                "file_name": filename,
                "file_size": media.file_size
            }

            MESSAGES_CACHE[movie_id] = msg

        except Exception as inner_error:

            print(
                "Skipped Message:",
                inner_error
            )

            continue

    save_movies(current)

    return {
        "synced": len(current)
    }

except Exception as e:

    return {
        "error": str(e)
    }
```

# ---------------------------------------------------

# MANIFEST

# ---------------------------------------------------

manifest = {
"id": "org.arun.telegram",
"version": "18.0.0",
"name": "Telegram Movies",
"description": "Fast Telegram Seekable Streaming",
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

```
return JSONResponse(manifest)
```

# ---------------------------------------------------

# CATALOG

# ---------------------------------------------------

@app.get("/catalog/movie/telegrammovies.json")
async def catalog():

```
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
```

# ---------------------------------------------------

# META

# ---------------------------------------------------

@app.get("/meta/movie/{id}.json")
async def meta(id: str):

```
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
```

# ---------------------------------------------------

# STREAM

# ---------------------------------------------------

@app.get("/stream/movie/{id}.json")
async def stream(id: str):

```
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

return JSONResponse({
    "streams": [
        {
            "name": "⚡ Telegram CDN",
            "title": movie_name,
            "url": f"{BASE_URL}/proxy/{clean_id}"
        }
    ]
})
```

# ---------------------------------------------------

# WATCH

# ---------------------------------------------------

@app.api_route(
"/watch/{movie_id}",
methods=["GET", "HEAD"]
)
async def watch(
movie_id: str,
request: Request
):

```
url = f"{BASE_URL}/proxy/{movie_id}"

if request.method == "HEAD":

    return Response(
        status_code=200,
        headers={
            "Location": url
        }
    )

return RedirectResponse(
    url=url,
    status_code=302
)
```

# ---------------------------------------------------

# PROXY STREAM

# ---------------------------------------------------

@app.api_route(
"/proxy/{movie_id}",
methods=["GET", "HEAD"]
)
async def proxy_stream(
movie_id: str,
request: Request
):

```
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
            "Content-Type": content_type,
            "Cache-Control": "public, max-age=3600",
            "Access-Control-Expose-Headers":
                "Content-Range, Accept-Ranges, Content-Length"
        }
    )

# ---------------------------------------------------
# RANGE HEADER
# ---------------------------------------------------
range_header = request.headers.get("range")

start = 0
end = file_size - 1

if range_header:

    try:

        range_data = (
            range_header
            .replace("bytes=", "")
            .split("-")
        )

        if range_data[0]:
            start = int(range_data[0])

        if len(range_data) > 1 and range_data[1]:
            end = int(range_data[1])

    except Exception:
        pass

if end >= file_size:
    end = file_size - 1

content_length = end - start + 1

print(f"📺 Seek Request: {start}-{end}")

# ---------------------------------------------------
# STREAMER
# ---------------------------------------------------
async def streamer():

    downloaded = 0

    try:

        async for chunk in tg.stream_media(
            msg,
            offset=start
        ):

            if not chunk:
                break

            remaining = content_length - downloaded

            if remaining <= 0:
                break

            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            downloaded += len(chunk)

            yield chunk

    except FloodWait as e:

        print(f"🚨 FloodWait: {e.value}s")

    except Exception as e:

        print(f"❌ Streaming Error: {e}")

# ---------------------------------------------------
# RESPONSE HEADERS
# ---------------------------------------------------
headers = {
    "Accept-Ranges": "bytes",
    "Content-Type": content_type,
    "Content-Length": str(content_length),
    "Content-Range":
        f"bytes {start}-{end}/{file_size}",
    "Cache-Control": "public, max-age=3600",
    "Connection": "keep-alive",
    "Access-Control-Expose-Headers":
        "Content-Range, Accept-Ranges, Content-Length"
}

return StreamingResponse(
    streamer(),
    status_code=206,
    headers=headers
)
```
