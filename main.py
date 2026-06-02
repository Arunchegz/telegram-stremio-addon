from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from pyrogram import Client
from pyrogram.errors import FloodWait

import os
import json
import asyncio
import re
import httpx
import time

# ---------------------------------------------------
# ENV
# ---------------------------------------------------
API_ID           = int(os.getenv("API_ID", "0"))
API_HASH         = os.getenv("API_HASH", "")
SESSION_STRING   = os.getenv("SESSION_STRING", "")
BASE_URL         = os.getenv("BASE_URL", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "")
DB_FILE          = "movies.json"

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
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await tg.start()
    try:
        await tg.get_chat(CHANNEL_USERNAME)
        print("âœ… Pyrogram started")
    except Exception as e:
        print(f"âš ï¸  Startup warning: {e}")
    yield
    # Shutdown
    await tg.stop()
    print("ðŸ›‘ Pyrogram stopped")

app = FastAPI(lifespan=lifespan)

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
SYNC_LOCK     = asyncio.Lock()
STREAM_LIMITER = asyncio.Semaphore(6)   # limit parallel Telegram DC connections

# Cache variables
STARTUP_CACHE: dict = {}
STARTUP_LOCKS: dict = {}
TAIL_CACHE: dict    = {}
TAIL_LOCKS: dict    = {}
CACHE_MAX_ITEMS     = 5

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
    file_n  = normalize(filename)
    words   = title_n.split()

    if not words:
        return False

    matched = sum(1 for w in words if w in file_n)

    if len(words) <= 2:
        required = len(words)
    else:
        required = max(2, len(words) // 2)

    return matched >= required


async def get_cinemeta(type_name: str, imdb_id: str):
    url = f"https://v3-cinemeta.strem.io/meta/{type_name}/{imdb_id}.json"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r    = await client.get(url)
            meta = r.json().get("meta", {})
            return (
                meta.get("name", ""),
                str(meta.get("year", "")),
            )
    except Exception:
        return ("", "")


# ---------------------------------------------------
# POSTER HELPERS
# ---------------------------------------------------
POSTER_CACHE: dict = {}

def parse_title_year(filename: str):
    """Extract clean title and year from a release filename."""
    name = filename
    # strip extension
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", name)
    # replace dots/underscores with spaces
    name = re.sub(r"[._]", " ", name)
    # find year
    year_match = re.search(r"\b(19|20)\d{2}\b", name)
    year = year_match.group(0) if year_match else ""
    # cut title at year or common release tags
    cut = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE
    )[0]
    title = re.sub(r"\s+", " ", cut).strip().title()
    return title, year


async def fetch_tmdb_poster(filename: str) -> str:
    """Return a TMDB poster URL for the given filename, or fallback placeholder."""
    if filename in POSTER_CACHE:
        return POSTER_CACHE[filename]

    title, year = parse_title_year(filename)
    if not title:
        return "https://via.placeholder.com/300x450?text=No+Poster"

    try:
        query = f"{title} {year}".strip()
        url   = f"https://v3-cinemeta.strem.io/catalog/movie/top/search={query}.json"
        async with httpx.AsyncClient(timeout=8) as client:
            r    = await client.get(url)
            metas = r.json().get("metas", [])
            if metas and metas[0].get("poster"):
                poster = metas[0]["poster"]
                POSTER_CACHE[filename] = poster
                return poster
    except Exception:
        pass

    fallback = f"https://via.placeholder.com/300x450?text={title.replace(' ', '+')}"
    POSTER_CACHE[filename] = fallback
    return fallback


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
        print(f"ðŸ—‘ï¸  Removed deleted media: {movie_id}")


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
                    "message_id":     msg.id,
                    "file_name":      filename,
                    "file_size":      media.file_size,
                    "file_size_text": format_size(media.file_size),
                    "quality":        detect_quality(filename),
                    "source":         detect_source(filename),
                }
            except Exception:
                continue

        save_movies(current)
        print(f"âœ… Sync complete: {len(current)} movies")
        return len(current)


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


@app.get("/debug")
async def debug():
    return load_movies()


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
    "idPrefixes": ["tg:"],
    "catalogs": [
        {
            "type": "movie",
            "id":   "telegrammovies",
            "name": "Telegram Movies",
        }
    ],
    "behaviorHints": {
        "configurable":            False,
        "configurationRequired":   False,
    },
}


@app.get("/manifest.json")
async def get_manifest():
    return JSONResponse(MANIFEST)


# ---------------------------------------------------
# STREMIO CATALOG
# ---------------------------------------------------
@app.get("/catalog/movie/telegrammovies.json")
async def catalog():
    movies = load_movies()

    async def build_meta(mid, m):
        filename = m.get("file_name", "Unknown")
        poster   = await fetch_tmdb_poster(filename)
        return {
            "id":          f"tg:{mid}",
            "type":        "movie",
            "name":        filename,
            "poster":      poster,
            "posterShape": "poster",
        }

    metas = await asyncio.gather(*[build_meta(mid, m) for mid, m in movies.items()])

    return JSONResponse(
        {"metas": list(metas)},
        headers={"Cache-Control": "no-store"},
    )


# ---------------------------------------------------
# STREMIO META
# ---------------------------------------------------
@app.get("/meta/movie/{id}.json")
async def meta(id: str):
    # Handle IMDb requests
    if id.startswith("tt"):
        title, year = await get_cinemeta("movie", id)
        return JSONResponse({
            "meta": {
                "id":     id,
                "type":   "movie",
                "name":   title,
                "year":   year,
            }
        })

    # Handle internal Telegram Catalog requests
    clean_id = id[3:] if id.startswith("tg:") else id
    movie    = load_movies().get(clean_id)
    if not movie:
        return JSONResponse({"meta": {}})

    name   = movie.get("file_name", "Unknown")
    poster = await fetch_tmdb_poster(name)
    title, year = parse_title_year(name)
    return JSONResponse({
        "meta": {
            "id":          id,
            "type":        "movie",
            "name":        title or name,
            "year":        year,
            "poster":      poster,
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
        movie_title, movie_year = await get_cinemeta("movie", id)
        if not movie_title:
            return JSONResponse({"streams": []})

        streams = []
        for mid, m in movies.items():
            name = m.get("file_name", "")

            if not flexible_match(movie_title, name):
                continue

            # Year validation: allow Â±1 year tolerance (encode/release year drift)
            if movie_year:
                try:
                    my = int(movie_year)
                    year_ok = any(str(my + d) in name for d in (-1, 0, 1))
                except ValueError:
                    year_ok = movie_year in name
                if not year_ok:
                    continue

            quality = m.get("quality", "Unknown")
            size    = m.get("file_size_text", "Unknown")
            source  = m.get("source", "")
            src_tag = f" | ðŸ·ï¸ {source}" if source else ""
            title   = f"{name}\nâš™ï¸ {quality}{src_tag} | ðŸ’¾ {size}"

            streams.append({
                "name":  "âš¡ Telegram",
                "title": title,
                "url":   f"{BASE_URL}/proxy/{mid}",
            })

        return JSONResponse({"streams": streams})

    # ---------------------------------------------------
    # Internal Catalog Streamer (Telegram Addon Page)
    # ---------------------------------------------------
    else:
        clean_id = id[3:] if id.startswith("tg:") else id
        movie    = movies.get(clean_id)
        if not movie:
            return JSONResponse({"streams": []})

        name    = movie.get("file_name", "Unknown")
        quality = movie.get("quality", "Unknown")
        size    = movie.get("file_size_text", "Unknown")
        source  = movie.get("source", "")
        src_tag = f" | ðŸ·ï¸ {source}" if source else ""
        title   = f"{name}\nâš™ï¸ {quality}{src_tag} | ðŸ’¾ {size}"

        try:
            msg = await fetch_message(movie["message_id"])
            if is_empty(msg):
                remove_movie(clean_id)
                return JSONResponse({"streams": []})

            # Warm startup cache in background
            if clean_id not in STARTUP_CACHE:
                asyncio.create_task(
                    get_startup_cache(msg, clean_id)
                )

            # Warm tail cache in background
            file_size = movie.get("file_size")
            if file_size and clean_id not in TAIL_CACHE:
                asyncio.create_task(
                    get_tail_cache(msg, clean_id, file_size)
                )
                
        except Exception as e:
            print(f"âŒ Stream fetch error: {e}")
            return JSONResponse({"streams": []})

        return JSONResponse({
            "streams": [
                {
                    "name":  "âš¡ Telegram",
                    "title": title,
                    "url":   f"{BASE_URL}/proxy/{clean_id}",
                }
            ]
        })


# ---------------------------------------------------
# STARTUP CACHE FUNCTION
# ---------------------------------------------------
async def get_startup_cache(msg, movie_id: str):
    if movie_id in STARTUP_CACHE:
        return STARTUP_CACHE[movie_id]

    lock = STARTUP_LOCKS.setdefault(movie_id, asyncio.Lock())

    async with lock:
        if movie_id in STARTUP_CACHE:
            return STARTUP_CACHE[movie_id]

        data = bytearray()

        async for chunk in tg.stream_media(
            msg,
            offset=0,
            limit=8,
        ):
            data.extend(chunk)

        STARTUP_CACHE[movie_id] = bytes(data)

        while len(STARTUP_CACHE) > CACHE_MAX_ITEMS:
            oldest = next(iter(STARTUP_CACHE))
            del STARTUP_CACHE[oldest]

        print(
            f"[{movie_id}] STARTUP_CACHE_BUILT "
            f"{len(data)/1024/1024:.2f}MB"
        )

        return STARTUP_CACHE[movie_id]

# ---------------------------------------------------
# TAIL CACHE FUNCTION
# ---------------------------------------------------
async def get_tail_cache(msg, movie_id: str, file_size: int):
    if movie_id in TAIL_CACHE:
        return TAIL_CACHE[movie_id]
        
    lock = TAIL_LOCKS.setdefault(movie_id, asyncio.Lock())
    
    async with lock:
        if movie_id in TAIL_CACHE:
            return TAIL_CACHE[movie_id]
            
        offset = max(0, (file_size // TG_CHUNK_SIZE) - 8)
        data = bytearray()
        
        async for chunk in tg.stream_media(
            msg,
            offset=offset,
            limit=8,
        ):
            data.extend(chunk)
            
        TAIL_CACHE[movie_id] = {
            "start": offset * TG_CHUNK_SIZE,
            "data": bytes(data),
        }
        
        while len(TAIL_CACHE) > CACHE_MAX_ITEMS:
            oldest = next(iter(TAIL_CACHE))
            del TAIL_CACHE[oldest]
            
        print(
            f"[{movie_id}] TAIL_CACHE_BUILT "
            f"{len(data)/1024/1024:.2f}MB"
        )
        
        return TAIL_CACHE[movie_id]

# ---------------------------------------------------
# PREFETCH STREAMER
# ---------------------------------------------------
class PrefetchStreamer:
    def __init__(self, media_generator, queue_max_size: int = 4):
        self.generator = media_generator
        self.queue = asyncio.Queue(maxsize=queue_max_size)
        self.worker_task = None
        self.error = None

    async def _producer(self):
        try:
            async for chunk in self.generator:
                await self.queue.put(chunk)
            await self.queue.put(None)
        except Exception as e:
            self.error = e
            await self.queue.put(None)

    def start(self):
        self.worker_task = asyncio.create_task(self._producer())

    async def chunk_generator(self, request: Request, skip_bytes: int, total_want: int, movie_id: str):
        sent = 0
        first = True
        
        try:
            while sent < total_want:
                if await request.is_disconnected():
                    print(f"[{movie_id}] PREFETCH: CLIENT_DISCONNECTED")
                    break

                chunk = await self.queue.get()
                
                if chunk is None:
                    if self.error:
                        print(f"[{movie_id}] PREFETCH: Producer error: {self.error}")
                        raise self.error
                    break

                if first:
                    chunk = chunk[skip_bytes:]
                    first = False

                remaining = total_want - sent
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]

                if chunk:
                    sent += len(chunk)
                    yield chunk
                    
                self.queue.task_done()

        finally:
            if self.worker_task and not self.worker_task.done():
                self.worker_task.cancel()
                try:
                    await self.worker_task
                except asyncio.CancelledError:
                    pass


# ---------------------------------------------------
# PROXY / RANGE STREAMING
# ---------------------------------------------------
@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy_stream(movie_id: str, request: Request):
    movie = load_movies().get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    t0  = time.time()
    msg = await fetch_message(movie["message_id"])
    print(f"[{movie_id}] FETCH_MSG: {round(time.time() - t0, 3)}s")

    if is_empty(msg):
        remove_movie(movie_id)
        raise HTTPException(status_code=404, detail="Media deleted from channel")

    media     = get_media(msg)
    file_size = movie.get("file_size") or media.file_size
    filename  = movie.get("file_name", "video.mp4")
    ctype     = content_type_for(filename)

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

    print(f"RAW RANGE HEADER: {range_header}")

    if range_header:
        try:
            parts = (
                range_header[6:].split("-")
                if range_header.startswith("bytes=")
                else range_header.split("-")
            )
            if parts[0]:
                start = int(parts[0])
            if len(parts) > 1 and parts[1]:
                end = int(parts[1])
        except ValueError:
            pass

    if not range_header:
        end = min((8 * 1024 * 1024) - 1, file_size - 1)
    elif range_header.endswith("-"):
        end = min(start + (8 * 1024 * 1024) - 1, file_size - 1)
    else:
        end = min(end, file_size - 1)

    print(f"Player requested range: {start}-{end}")
    
    # ---------------------------------------------------
    # STARTUP CACHE INTERCEPT
    # ---------------------------------------------------
    if start < 8 * 1024 * 1024:
        cache = await get_startup_cache(msg, movie_id)
        cache_end = min(end, len(cache) - 1)

        if cache_end >= start:
            print(f"[{movie_id}] CACHE_HIT")
            return Response(
                content=cache[start:cache_end + 1],
                status_code=206,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{cache_end}/{file_size}",
                    "Content-Length": str(cache_end - start + 1),
                    "Content-Type": ctype,
                },
            )

    # ---------------------------------------------------
    # TAIL CACHE INTERCEPT
    # ---------------------------------------------------
    if start > file_size - (8 * 1024 * 1024):
        tail = await get_tail_cache(msg, movie_id, file_size)
        tail_start = tail["start"]
        tail_data = tail["data"]
        
        rel_start = start - tail_start
        rel_end = min(end - tail_start, len(tail_data) - 1)
        
        if rel_start >= 0 and rel_end >= rel_start:
            print(f"[{movie_id}] TAIL_CACHE_HIT")
            return Response(
                content=tail_data[rel_start:rel_end + 1],
                status_code=206,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(rel_end - rel_start + 1),
                    "Content-Type": ctype,
                },
            )

    # ---------------------------------------------------
    # MID-FILE: Stream from Telegram via Pyrogram
    # ---------------------------------------------------
    chunk_size  = TG_CHUNK_SIZE
    offset      = start // chunk_size
    skip_bytes  = start % chunk_size
    total_want  = end - start + 1
    num_chunks  = (skip_bytes + total_want + chunk_size - 1) // chunk_size

    print(
        f"[{movie_id}] RANGE=bytes={start}-{end} "
        f"START={start} END={end} SIZE={total_want/1024/1024:.2f}MB"
    )
    print(f"[{movie_id}] OFFSET={offset} SKIP={skip_bytes} CHUNKS={num_chunks}")

    async def _stream():
        t_start = time.time()
        sent    = 0
        async with STREAM_LIMITER:
            print(f"[{movie_id}] PREFETCH: Pipeline started actively.")
            media_gen = tg.stream_media(msg, offset=offset, limit=num_chunks)
            streamer  = PrefetchStreamer(media_gen)
            streamer.start()
            async for chunk in streamer.chunk_generator(request, skip_bytes, total_want, movie_id):
                sent += len(chunk)
                yield chunk
        elapsed = round(time.time() - t_start, 3)
        print(f"[{movie_id}] STREAM_COMPLETE: {elapsed}s SENT={sent/1024/1024:.2f}MB")

    return StreamingResponse(
        _stream(),
        status_code=206,
        headers={
            "Accept-Ranges":  "bytes",
            "Content-Range":  f"bytes {start}-{end}/{file_size}",
            "Content-Length": str(total_want),
            "Content-Type":   ctype,
        },
        media_type=ctype,
    )


# ---------------------------------------------------
# TELEGRAM WEBHOOK (placeholder â€“ set via Bot API)
# ---------------------------------------------------
@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    """
    Receives Telegram Bot API webhook updates.
    Extend this handler to process bot commands or messages.
    """
    try:
        update = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # TODO: add your bot update handling logic here
    return JSONResponse({"ok": True})# Required by Vercel
application = app
