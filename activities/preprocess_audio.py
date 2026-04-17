# activities/preprocess_audio.py
import os
import subprocess
import uuid

import azure.durable_functions as df

bp_preprocess_audio = df.Blueprint()

MAX_BYTES = 25 * 1024 * 1024


def _size_ok(path: str) -> bool:
    return os.path.getsize(path) <= MAX_BYTES


def _bin(name: str, default: str) -> str:
    return os.getenv(name, default)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


@bp_preprocess_audio.activity_trigger(input_name="arg")
def PreprocessAudio(arg: dict) -> dict:
    src = arg["local_path"]

    ffmpeg = _bin("FFMPEG_BIN", "ffmpeg")

    # 1) already <=25MB -> single
    if _size_ok(src):
        return {"mode": "single", "paths": [src]}

    # 2) compress to mp3 (16kHz/mono/64kbps)
    mp3 = f"/tmp/{uuid.uuid4()}.mp3"
    _run([
        ffmpeg, "-y", "-i", src,
        "-ac", "1", "-ar", "16000",
        "-b:a", "64k",
        mp3
    ])

    if _size_ok(mp3):
        return {"mode": "single", "paths": [mp3]}

    # 3) still >25MB -> split into 5-min segments
    out_dir = f"/tmp/{uuid.uuid4()}"
    os.makedirs(out_dir, exist_ok=True)
    pattern = os.path.join(out_dir, "chunk_%03d.mp3")

    _run([
        ffmpeg, "-y", "-i", mp3,
        "-f", "segment",
        "-segment_time", os.getenv("SEGMENT_SECONDS", "300"),
        "-reset_timestamps", "1",
        pattern
    ])

    chunks = [os.path.join(out_dir, f) for f in sorted(os.listdir(out_dir)) if f.endswith(".mp3")]

    # safety check
    if any(os.path.getsize(c) > MAX_BYTES for c in chunks):
        raise Exception("Chunk still exceeds 25MB. Reduce SEGMENT_SECONDS.")

    return {"mode": "chunks", "paths": chunks}
