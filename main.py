from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

# ---------------------------------------------------
# MANIFEST
# ---------------------------------------------------

manifest = {
    "id": "org.arun.tgaddon",
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


@app.get("/manifest.json")
async def get_manifest():
    return JSONResponse(manifest)


# ---------------------------------------------------
# CATALOG
# ---------------------------------------------------

@app.get("/catalog/movie/telegrammovies.json")
async def catalog():

    movies = {
        "metas": [
            {
                "id": "movie1",
                "type": "movie",
                "name": "Sample Movie",
                "poster": "https://via.placeholder.com/300x450.png?text=Movie"
            }
        ]
    }

    return JSONResponse(movies)


# ---------------------------------------------------
# META
# ---------------------------------------------------

@app.get("/meta/movie/{id}.json")
async def meta(id: str):

    meta = {
        "meta": {
            "id": id,
            "type": "movie",
            "name": "Sample Movie",
            "poster": "https://via.placeholder.com/300x450.png?text=Movie",
            "description": "Streaming from Telegram"
        }
    }

    return JSONResponse(meta)


# ---------------------------------------------------
# STREAM
# ---------------------------------------------------

@app.get("/stream/movie/{id}.json")
async def stream(id: str):

    streams = {
        "streams": [
            {
                "name": "☁️ Telegram",
                "title": "1080p",
                "url": "https://samplelib.com/lib/preview/mp4/sample-5s.mp4"
            }
        ]
    }

    return JSONResponse(streams)