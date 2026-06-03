from dotenv import load_dotenv
load_dotenv()
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from pyrogram import Client, utils, raw
from pyrogram.errors import FloodWait, AuthBytesInvalid
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType, ThumbnailSource

import os
import json
import asyncio
import math
import re
import httpx
import time
from typing import AsyncGenerator, Dict, Union

# ---------------------------------------------------------------------------
# Pyrogram internal-API version guard
# ---------------------------------------------------------------------------
import importlib.metadata as _meta
try:
    _pyro_ver = tuple(int(x) for x in _meta.version("pyrogram").split(".")[:2])
except Exception:
    _pyro_ver = (0, 0)

_PYROGRAM_MIN = (2, 0)
_PYROGRAM_MAX = (2, 99)

if not (_PYROGRAM_MIN <= _pyro_ver <= _PYROGRAM_MAX):
    raise RuntimeError(
        f"Pyrogram {'.'.join(str(x) for x in _pyro_ver)} is outside the "
        f"tested range {_PYROGRAM_MIN}–{_PYROGRAM_MAX}. "
        "ByteStreamer uses private internals (Session, Auth, file_id). "
        "Review and re-test before widening this range."
    )

# ---------------------------------------------------
# ENV
# ---------------------------------------------------
API_ID             = int(os.getenv("API_ID", "0"))
API_HASH           = os.getenv("API_HASH", "")
SESSION_STRING     = os.getenv("SESSION_STRING", "")
BASE_URL           = os.getenv("BASE_URL", "")
CHANNEL_USERNAME   = os.getenv("CHANNEL_USERNAME", "")
SYNC_STALE_MINUTES = int(os.getenv("SYNC_STALE_MINUTES", "60"))
DB_FILE            = "movies.json"

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
# FASTAPI LIFESPAN
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global byte_streamer
    # Startup
    await tg.start()
    byte_streamer = ByteStreamer(tg)
    try:
        await tg.get_chat(CHANNEL_USERNAME)
        print("✅ Pyrogram started")
    except Exception as e:
        print(f"⚠️ Startup warning: {e}")
    yield
    # Shutdown
    for dc_id, session in list(tg.media_sessions.items()):
        try:
            await session.stop()
        except Exception as e:
            print(f"Warning: could not close media session DC{dc_id}: {e}")
    tg.media_sessions.clear()
    await tg.stop()
    print("🛑 Pyrogram stopped")

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
STREAM_LIMITER = asyncio.Semaphore(3)

STARTUP_CACHE: dict = {}
STARTUP_LOCKS: dict = {}
TAIL_CACHE: dict    = {}
TAIL_LOCKS: dict    = {}
CACHE_MAX_ITEMS     = 5

last_sync_time: float = 0.0  # Unix timestamp, 0 = never synced
TG_CHUNK_SIZE = 1024 * 1024

# ---------------------------------------------------
# BYTE STREAMER
# ---------------------------------------------------
class ByteStreamer:
    """
    Streams Telegram media directly via raw MTProto GetFile calls.
    """
    def __init__(self, client: Client):
        self.client: Client = client
        self._file_id_cache: Dict[int, FileId] = {}
        self._cache_lock: asyncio.Lock = asyncio.Lock()

    async def get_file_id(self, msg) -> FileId:
        msg_id = msg.id
        async with self._cache_lock:
            if msg_id in self._file_id_cache:
                return self._file_id_cache[msg_id]
            file_id = self._extract_file_id(msg)
            self._file_id_cache[msg_id] = file_id
            if len(self._file_id_cache) > 200:
                oldest = next(iter(self._file_id_cache))
                del self._file_id_cache[oldest]
            return file_id

    async def yield_file(
        self,
        msg,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int = TG_CHUNK_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        file_id = await self.get_file_id(msg)
        media_session = await self._get_media_session(file_id)
        location = self._get_location(file_id)

        current_part = 1
        current_offset = offset

        r = await media_session.invoke(
            raw.functions.upload.GetFile(
                location=location,
                offset=current_offset,
                limit=chunk_size,
            )
        )

        if not isinstance(r, raw.types.upload.File):
            return

        while True:
            chunk = r.bytes
            if not chunk:
                break

            if part_count == 1:
                yield chunk[first_part_cut:last_part_cut]
            elif current_part == 1:
                yield chunk[first_part_cut:]
            elif current_part == part_count:
                yield chunk[:last_part_cut]
            else:
                yield chunk

            current_part += 1
            current_offset += chunk_size

            if current_part > part_count:
                break

            r = await media_session.invoke(
                raw.functions.upload.GetFile(
                    location=location,
                    offset=current_offset,
                    limit=chunk_size,
                )
            )

    @staticmethod
    def _extract_file_id(msg) -> FileId:
        media = msg.video or msg.document
        if media is None:
            raise ValueError("Message contains no streamable media")
        return FileId.decode(media.file_id)

    @staticmethod
    def _get_location(file_id: FileId):
        file_type = file_id.file_type
        if file_type == FileType.CHAT_PHOTO:
            if file_id.chat_id > 0:
                peer = raw.types.InputPeerUser(
                    user_id=file_id.chat_id,
                    access_hash=file_id.chat_access_hash,
                )
            elif file_id.chat_access_hash == 0:
                peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
            else:
                peer = raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(file_id.chat_id),
                    access_hash=file_id.chat_access_hash,
                )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            return raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

    async def _get_media_session(self, file_id: FileId) -> Session:
        client = self.client
        dc_id = file_id.dc_id

        media_session = client.media_sessions.get(dc_id)
        if media_session is not None:
            return media_session

        if dc_id != await client.storage.dc_id():
            media_session = Session(
                client,
                dc_id,
                await Auth(client, dc_id, await client.storage.test_mode()).create(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()

            for _ in range(6):
                exported = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                try:
                    await media_session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported.id, bytes=exported.bytes
                        )
                    )
                    break
                except AuthBytesInvalid:
                    continue
            else:
                await media_session.stop()
                raise AuthBytesInvalid
        else:
            media_session = Session(
                client,
                dc_id,
                await client.storage.auth_key(),
                await client.storage.test_mode(),
                is_media=True,
            )
            await media_session.start()

        client.media_sessions[dc_id] = media_session
        return media_session


byte_streamer: ByteStreamer = None

# ---------------------------------------------------
# HELPERS
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


def normalize(text: str):
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"[._\-–—+]", " ", text)
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def flexible_match(title: str, filename: str):
    title_n = normalize(title)
    file_n = normalize(filename)

    if not title_n or not file_n:
        return False

    if title_n in file_n:
        return True

    title_words = title_n.split()
    file_words = file_n.split()

    matches = sum(
        1 for word in title_words
        if word in file_words
    )

    return matches >= max(1, len(title_words) * 0.7)


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


POSTER_CACHE: dict = {}

def parse_title_year(filename: str):
    name = filename
    name = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", name)
    name = re.sub(r"[._]", " ", name)
    year_match = re.search(r"\b(19|20)\d{2}\b", name)
    year = year_match.group(0) if year_match else ""
    cut = re.split(
        r"\b(?:19|20)\d{2}\b|\b(?:1080p|2160p|720p|480p|bluray|webrip|web dl|bdrip|hdrip|remux|x264|x265|hevc|avc|h264|h265|aac|dts|atmos|10bit)\b",
        name, maxsplit=1, flags=re.IGNORECASE
    )[0]
    title = re.sub(r"\s+", " ", cut).strip().title()
    return title, year


async def fetch_tmdb_poster(filename: str) -> str:
    if filename in POSTER_CACHE:
        return POSTER_CACHE[filename]

    title, year = parse_title_year(filename)
    if not title:
        return "https://via.placeholder.com/300x450?text=No+Poster"

    try:
        query = f"{title} {year}".strip()
        url   = f"https://v3-cinemeta.strem.io/catalog/movie/top/search={query}.json"
        async with httpx.AsyncClient(timeout=8) as client:
            r     = await client.get(url)
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
        print(f"🗑️ Removed deleted media: {movie_id}")


def get_media(msg):
    return msg.video or msg.document or None


def is_empty(msg) -> bool:
    return getattr(msg, "empty", False) or get_media(msg) is None


async def fetch_message(message_id: int):
    return await tg.get_messages(CHANNEL_USERNAME, message_id)


async def sync_channel() -> int:
    global last_sync_time
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
        last_sync_time = time.time()
        print(f"✅ Sync complete: {len(current)} movies")
        return len(current)


# ---------------------------------------------------
# ROUTES
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
    global MOVIES_CACHE, last_sync_time
    try:
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
        MOVIES_CACHE = {}
        last_sync_time = 0.0
        return {"status": "database cleared"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug")
async def debug():
    return load_movies()


MANIFEST = {
    "id": "org.arun.telegram",
    "version": "1.0.0",
    "name": "Telegram Movies",
    "description": "Telegram Movie Catalog",
    "resources": ["catalog", "meta", "stream"],
    "types": ["movie"],
    "idPrefixes": ["tg:", "tt"],
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


@app.get("/catalog/movie/telegrammovies.json")
async def catalog():
    global last_sync_time
    movies = load_movies()
    now = time.time()
    stale = (now - last_sync_time) > (SYNC_STALE_MINUTES * 60)

    if not movies:
        # First time or empty DB — block until synced
        count = await sync_channel()
        movies = load_movies()

# Sync when catalog is opened, but only once every 2 minutes
now = time.time()

if (now - last_sync_time) > 120:
    count = await sync_channel()
    movies = load_movies()
    print(f"🔄 Catalog sync: {count} movies")

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


@app.get("/meta/movie/{id}.json")
async def meta(id: str):
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


@app.get("/stream/movie/{id}.json")
async def stream(id: str):
    movies = load_movies()

    if id.startswith("tt"):
        movie_title, movie_year = await get_cinemeta("movie", id)
        if not movie_title:
            return JSONResponse({"streams": []})

        streams = []
        for mid, m in movies.items():
            name = m.get("file_name", "")

            if not flexible_match(movie_title, name):
                continue

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
            src_tag = f" | {source}" if source else ""
            title   = f"{name}\n{quality}{src_tag} | {size}"

            streams.append({
                "name":  "Telegram",
                "title": title,
                "url":   f"{BASE_URL}/proxy/{mid}",
            })

        return JSONResponse({"streams": streams})

    else:
        clean_id = id[3:] if id.startswith("tg:") else id
        movie    = movies.get(clean_id)
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

            if clean_id not in STARTUP_CACHE:
                asyncio.create_task(get_startup_cache(msg, clean_id))

            file_size = movie.get("file_size")
            if file_size and clean_id not in TAIL_CACHE:
                asyncio.create_task(get_tail_cache(msg, clean_id, file_size))
                
        except Exception as e:
            print(f"❌ Stream fetch error: {e}")
            return JSONResponse({"streams": []})

        return JSONResponse({
            "streams": [
                {
                    "name": "Telegram",
                    "title": title,
                    "url": f"{BASE_URL}/proxy/{clean_id}",
                }
            ]
        })


async def get_startup_cache(msg, movie_id: str):
    if movie_id in STARTUP_CACHE:
        return STARTUP_CACHE[movie_id]

    lock = STARTUP_LOCKS.setdefault(movie_id, asyncio.Lock())

    async with lock:
        if movie_id in STARTUP_CACHE:
            return STARTUP_CACHE[movie_id]

        data = bytearray()
        async for chunk in byte_streamer.yield_file(
            msg,
            offset=0,
            first_part_cut=0,
            last_part_cut=TG_CHUNK_SIZE,
            part_count=8,
        ):
            data.extend(chunk)

        STARTUP_CACHE[movie_id] = bytes(data)

        while len(STARTUP_CACHE) > CACHE_MAX_ITEMS:
            oldest = next(iter(STARTUP_CACHE))
            del STARTUP_CACHE[oldest]

        print(f"[{movie_id}] STARTUP_CACHE_BUILT {len(data)/1024/1024:.2f}MB")
        return STARTUP_CACHE[movie_id]


async def get_tail_cache(msg, movie_id: str, file_size: int):
    if movie_id in TAIL_CACHE:
        return TAIL_CACHE[movie_id]
        
    lock = TAIL_LOCKS.setdefault(movie_id, asyncio.Lock())
    
    async with lock:
        if movie_id in TAIL_CACHE:
            return TAIL_CACHE[movie_id]
            
        offset = max(0, (file_size // TG_CHUNK_SIZE) - 8)
        data = bytearray()

        async for chunk in byte_streamer.yield_file(
            msg,
            offset=offset * TG_CHUNK_SIZE,
            first_part_cut=0,
            last_part_cut=TG_CHUNK_SIZE,
            part_count=8,
        ):
            data.extend(chunk)
            
        TAIL_CACHE[movie_id] = {
            "start": offset * TG_CHUNK_SIZE,
            "data": bytes(data),
        }
        
        while len(TAIL_CACHE) > CACHE_MAX_ITEMS:
            oldest = next(iter(TAIL_CACHE))
            del TAIL_CACHE[oldest]
            
        print(f"[{movie_id}] TAIL_CACHE_BUILT {len(data)/1024/1024:.2f}MB")
        return TAIL_CACHE[movie_id]


@app.api_route("/proxy/{movie_id}", methods=["GET", "HEAD"])
async def proxy_stream(movie_id: str, request: Request):
    movie = load_movies().get(movie_id)
    if not movie:
        raise HTTPException(status_code=404, detail="Movie not found")

    t0  = time.time()
    try:
        msg = await fetch_message(movie["message_id"])
    except FloodWait as e:
        raise HTTPException(status_code=503, detail=f"Telegram FloodWait {e.value}s")
    except Exception:
        raise HTTPException(status_code=502, detail="Telegram unavailable")
    print(f"[{movie_id}] FETCH_MSG: {round(time.time() - t0, 3)}s")

    if is_empty(msg):
        remove_movie(movie_id)
        raise HTTPException(status_code=404, detail="Media deleted from channel")

    media     = get_media(msg)
    file_size = movie.get("file_size") or media.file_size
    filename  = movie.get("file_name", "video.mp4")
    ctype     = content_type_for(filename)
    etag      = f'"{movie["message_id"]}-{file_size}"'

    if request.method == "HEAD":
        return Response(
            status_code=206,
            headers={
                "Accept-Ranges":  "bytes",
                "Content-Range":  f"bytes 0-{file_size - 1}/{file_size}",
                "Content-Length": str(file_size),
                "Content-Type":   ctype,
                "Cache-Control":  "public, max-age=3600",
                "ETag":           etag,
                "Vary":           "Range",
            },
        )

    start, end = 0, file_size - 1
    range_header = request.headers.get("range")
    print(f"RAW RANGE HEADER: {range_header}")

    if range_header and range_header.startswith("bytes="):
        try:
            range_spec = range_header[6:]
            if range_spec.startswith("-"):
                suffix = int(range_spec[1:])
                start = max(0, file_size - suffix)
                end = file_size - 1
            else:
                parts = range_spec.split("-")
                if parts[0]:
                    start = int(parts[0])
                if len(parts) > 1 and parts[1]:
                    end = int(parts[1])
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid Range header")

    if not range_header:
        end = min((8 * 1024 * 1024) - 1, file_size - 1)
    elif range_header.endswith("-"):
        end = min(start + (8 * 1024 * 1024) - 1, file_size - 1)
    else:
        end = min(end, file_size - 1)

    print(f"Player requested range: {start}-{end}")
    
    if start < 8 * 1024 * 1024:
        cache = STARTUP_CACHE.get(movie_id)
        if cache is None:
            asyncio.create_task(get_startup_cache(msg, movie_id))
        else:
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
                        "Cache-Control": "public, max-age=3600",
                        "ETag":          etag,
                        "Vary":          "Range",
                    },
                )

    if start >= file_size - (8 * 1024 * 1024):
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
                    "Cache-Control": "public, max-age=3600",
                    "ETag":          etag,
                    "Vary":          "Range",
                },
            )

    chunk_size      = TG_CHUNK_SIZE
    aligned_offset  = (start // chunk_size) * chunk_size
    first_part_cut  = start - aligned_offset
    last_part_cut   = (end % chunk_size) + 1
    part_count      = math.ceil((end + 1) / chunk_size) - (aligned_offset // chunk_size)
    total_want      = end - start + 1

    print(f"[{movie_id}] RANGE=bytes={start}-{end} START={start} END={end} SIZE={total_want/1024/1024:.2f}MB")
    print(f"[{movie_id}] OFFSET={aligned_offset} FIRST_CUT={first_part_cut} LAST_CUT={last_part_cut} PARTS={part_count}")

    async def _stream():
        t_start = time.time()
        sent    = 0
        async with STREAM_LIMITER:
            print(f"[{movie_id}] BYTESTREAMER: Pipeline started.")
            async for chunk in byte_streamer.yield_file(
                msg,
                offset=aligned_offset,
                first_part_cut=first_part_cut,
                last_part_cut=last_part_cut,
                part_count=part_count,
                chunk_size=chunk_size,
            ):
                if await request.is_disconnected():
                    print(f"[{movie_id}] BYTESTREAMER: CLIENT_DISCONNECTED")
                    break
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
            "Cache-Control":  "public, max-age=3600",
            "ETag":           etag,
            "Vary":           "Range",
        },
        media_type=ctype,
    )


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    return JSONResponse({"ok": True})

application = app
