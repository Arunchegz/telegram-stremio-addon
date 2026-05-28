```python
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
    # HEAD SUPPORT
    # ---------------------------------------------------
    if request.method == "HEAD":

        return Response(
            status_code=200,
            headers={
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": content_type,
                "Cache-Control": "public, max-age=3600"
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
    # SMALLER CHUNK SIZE
    # ---------------------------------------------------
    chunk_size = 512 * 1024

    # ---------------------------------------------------
    # STREAMER
    # ---------------------------------------------------
    async def streamer():

        downloaded = 0

        try:

            async for chunk in tg.stream_media(
                msg,
                offset=start,
                limit=content_length
            ):

                if not chunk:
                    break

                downloaded += len(chunk)

                yield chunk

                if downloaded >= content_length:
                    break

        except FloodWait as e:

            print(f"🚨 FloodWait: {e.value}s")

        except Exception as e:

            print(f"❌ Streaming Error: {e}")

    # ---------------------------------------------------
    # HEADERS
    # ---------------------------------------------------
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
        "Content-Length": str(content_length),
        "Content-Range": (
            f"bytes {start}-{end}/{file_size}"
        ),
        "Cache-Control": "public, max-age=3600",
        "Connection": "keep-alive",
    }

    return StreamingResponse(
        streamer(),
        status_code=206,
        headers=headers
    )
```
