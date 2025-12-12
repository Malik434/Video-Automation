import os
import re
import json
import tempfile
import subprocess
import argparse
from pathlib import Path
from typing import Tuple

import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.cloud import storage

# --- Environment ---
GCS_ASSETS_BUCKET = os.environ["GCS_ASSETS_BUCKET"]
ASSETS_PREFIX = os.environ.get("ASSETS_PREFIX", "assets")
DRIVE_OUTPUT_FOLDER_ID = os.environ.get("DRIVE_OUTPUT_FOLDER_ID")
SKIP_DRIVE_UPLOAD = os.environ.get("SKIP_DRIVE_UPLOAD", "0") == "1"
GCS_OUTPUTS_BUCKET = os.environ.get("GCS_OUTPUTS_BUCKET", "")
GCS_OUTPUTS_PREFIX = os.environ.get("GCS_OUTPUTS_PREFIX", "")

# --- API clients ---
def creds_with_scopes(scopes):
    creds, _ = google.auth.default(scopes=scopes)
    return creds

def drive():
    return build(
        "drive", "v3",
        credentials=creds_with_scopes([
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/drive.file"
        ]),
        cache_discovery=False
    )

def youtube():
    return build(
        "youtube", "v3",
        credentials=creds_with_scopes([
            "https://www.googleapis.com/auth/youtube.upload"
        ]),
        cache_discovery=False
    )

def storage_client():
    return storage.Client()

# --- Helpers ---
def run_ffmpeg(args, debug_report: bool = False, timeout: int = None):
    if debug_report:
        cmd = ["ffmpeg", "-report", "-loglevel", "debug"] + list(args)
    else:
        cmd = ["ffmpeg"] + list(args)
    print("Running ffmpeg:", " ".join(cmd))
    p = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if p.returncode == 0:
        return p
    if p.returncode < 0:
        sig = -p.returncode
        raise RuntimeError(f"ffmpeg terminated by signal {sig} (returncode={p.returncode}).\n\nSTDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}")
    raise RuntimeError(f"ffmpeg failed (returncode={p.returncode}).\n\nSTDOUT:\n{p.stdout}\n\nSTDERR:\n{p.stderr}")

def parse_drive_id(url_or_id: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]{20,}", url_or_id):
        return url_or_id
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", url_or_id) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", url_or_id)
    if not m:
        raise ValueError("Could not parse Google Drive fileId")
    return m.group(1)

def dl_drive(file_id: str, dest: Path):
    req = drive().files().get_media(fileId=file_id, supportsAllDrives=True)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, req, chunksize=32*1024*1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    if not dest.exists() or dest.stat().st_size == 0:
        raise RuntimeError("Downloaded 0 bytes from Drive")

def dl_gcs(obj: str, dest: Path):
    blob = storage_client().bucket(GCS_ASSETS_BUCKET).blob(obj)
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))
    if not dest.exists() or dest.stat().st_size == 0:
        raise RuntimeError(f"GCS object empty: gs://{GCS_ASSETS_BUCKET}/{obj}")

def run_ffprobe_json(path: Path):
    p = subprocess.run([
        "ffprobe", "-v", "error",
        "-show_entries", "format:stream",
        "-of", "json",
        str(path)
    ], text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed:\n{p.stderr}")
    return json.loads(p.stdout or "{}")

def get_video_resolution(path: Path) -> Tuple[int, int]:
    info = run_ffprobe_json(path)
    streams = info.get("streams") or []
    vs = [s for s in streams if s.get("codec_type") == "video"]
    if not vs:
        raise RuntimeError(f"No video stream found in {path}")
    s = vs[0]
    return int(s.get("width")), int(s.get("height"))

def has_audio(path: Path) -> bool:
    info = run_ffprobe_json(path)
    streams = info.get("streams") or []
    return any(s.get("codec_type") == "audio" for s in streams)

def add_silent_audio(src: Path, dst: Path):
    args = [
        "-y",
        "-i", str(src),
        "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        "-shortest",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        str(dst)
    ]
    run_ffmpeg(args)

def upload_drive(local: Path, out_name: str):
    if not DRIVE_OUTPUT_FOLDER_ID:
        raise RuntimeError("DRIVE_OUTPUT_FOLDER_ID unset and SKIP_DRIVE_UPLOAD != 1")
    media = MediaFileUpload(str(local), mimetype="video/mp4", resumable=True)
    body = {"name": out_name, "parents": [DRIVE_OUTPUT_FOLDER_ID]}
    req = drive().files().create(
        body=body,
        media_body=media,
        supportsAllDrives=True,
        fields="id,name,webViewLink"
    )
    resp = None
    while resp is None:
        _, resp = req.next_chunk()
    return resp

def upload_gcs(local: Path, object_name: str):
    if not GCS_OUTPUTS_BUCKET:
        return None
    blob = storage_client().bucket(GCS_OUTPUTS_BUCKET).blob(object_name)
    blob.upload_from_filename(str(local), content_type="video/mp4")
    return f"gs://{blob.bucket.name}/{blob.name}"

def upload_youtube(local: Path, title: str, description: str, thumbnail: str = None):
    yt = youtube()
    body = {
        "snippet": {"title": title, "description": description, "categoryId": "22"},
        "status": {"privacyStatus": "unlisted"}
    }
    media = MediaFileUpload(str(local), mimetype="video/mp4", resumable=True)
    request = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    response = None
    while response is None:
        status, response = request.next_chunk()
        if status:
            print(f"Upload progress: {int(status.progress() * 100)}%")
    video_id = response["id"]
    if thumbnail:
        try:
            yt.thumbnails().set(videoId=video_id, media_body=thumbnail).execute()
        except Exception:
            pass
    return {"id": video_id, "url": f"https://youtu.be/{video_id}"}

# --- Minimal sequence process function ---
def process(drive_url: str, output_name: str, yt_title=None, yt_desc=None, yt_thumb=None):
    fid = parse_drive_id(drive_url)
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        main = tmp / "main.mp4"
        bg = tmp / "background.mp4"
        outro = tmp / "outro.mp4"
        music = tmp / "music.mp3"
        out_video = tmp / "composed_video.mp4"   # video-only composed file
        out_audio = tmp / "composed_audio.aac"   # audio-only mixed file
        final_out = tmp / "final_output.mp4"

        print("Downloading main video from Drive...")
        dl_drive(fid, main)
        pref = ASSETS_PREFIX.strip("/") + "/"
        print("Downloading assets from GCS...")
        dl_gcs(pref + "background.mp4", bg)
        dl_gcs(pref + "outro.mp4", outro)
        dl_gcs(pref + "music.mp3", music)

        # Basic checks
        for p in (main, bg, outro, music):
            if not p.exists() or p.stat().st_size == 0:
                raise RuntimeError(f"Required asset missing or empty: {p}")

        # Ensure main has audio (the final audio mixing step expects some main audio)
        if not has_audio(main):
            print("Adding silent audio to main (no audio present).")
            main_with_audio = tmp / "main_with_audio.mp4"
            add_silent_audio(main, main_with_audio)
            main = main_with_audio

        # If main is not 720p, create a 1080p scaled version for composition (so later steps only see 1080p main)
        w, h = get_video_resolution(main)
        print(f"Main resolution: {w}x{h}")
        if h != 720:
            scaled_main = tmp / "main_1080.mp4"
            vf = "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p"
            print("Scaling main to 1920x1080 ->", scaled_main)
            run_ffmpeg([
                "-y",
                "-i", str(main),
                "-vf", vf,
                "-c:v", "libx264", "-crf", "18", "-preset", "superfast",
                "-c:a", "aac", "-b:a", "192k",
                str(scaled_main)
            ])
            main = scaled_main

        # Pre-scale background and outro independently (each a small job)
        bg_1080 = tmp / "background_1080.mp4"
        outro_1080 = tmp / "outro_1080.mp4"

        print("Pre-scaling background to 1920x1080 ->", bg_1080)
        run_ffmpeg([
            "-y",
            "-i", str(bg),
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
            "-c:v", "libx264", "-crf", "22", "-preset", "fast",  # slightly cheaper settings for assets
            "-an",
            str(bg_1080)
        ])

        print("Pre-scaling outro to 1920x1080 ->", outro_1080)
        # Keep audio from outro if any (-c:a aac) otherwise -an and add later; here we preserve if present
        run_ffmpeg([
            "-y",
            "-i", str(outro),
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
            "-c:v", "libx264", "-crf", "22", "-preset", "fast",
            "-c:a", "aac", "-b:a", "128k",
            str(outro_1080)
        ])

        # Now compose the video (overlay + concat) WITHOUT audio to reduce memory
        # Inputs: 0=main (already 1080), 1=bg_1080, 2=outro_1080
        # Simpler filter: scale main to target overlay width and overlay onto bg, concat with outro.
        vc_filter = (
            "[1:v]setpts=PTS-STARTPTS[bg];"
            "[0:v]scale=1400:-1[fg];"
            "[bg][fg]overlay=(W-w)/2:(H-h)/2:shortest=1[comp];"
            "[2:v]setpts=PTS-STARTPTS[outro];"
            "[comp][outro]concat=n=2:v=1:a=0[vout]"
        )

        print("Composing video-only output (overlay + concat) ->", out_video)
        run_ffmpeg([
            "-y",
            "-i", str(main),
            "-i", str(bg_1080),
            "-i", str(outro_1080),
            "-filter_complex", vc_filter,
            "-map", "[vout]",
            "-c:v", "libx264", "-crf", "18", "-preset", "superfast",
            "-an",  # video-only
            str(out_video)
        ], debug_report=True)

        # Audio: mix main audio and music into a single audio track (do simpler mix)
        # Approach: loop music to at least main duration and then mix with main's audio.
        print("Preparing audio-only track by mixing main audio with music ->", out_audio)
        # Measure durations
        def get_duration(path: Path) -> float:
            p = run_ffprobe_json(path)
            fmt = p.get("format") or {}
            d = fmt.get("duration")
            return float(d) if d else 0.0

        main_dur = get_duration(main)
        music_dur = get_duration(music)
        # If music shorter, loop it with -stream_loop. We'll create music_looped file.
        music_looped = tmp / "music_looped.mp3"
        if music_dur >= main_dur or main_dur == 0:
            # just copy or transcode to aac for reliable mixing
            run_ffmpeg([
                "-y",
                "-i", str(music),
                "-c:a", "aac", "-b:a", "192k",
                str(music_looped)
            ])
        else:
            loops = max(1, int((main_dur // max(1.0, music_dur)) + 1))
            run_ffmpeg([
                "-y",
                "-stream_loop", str(loops),
                "-i", str(music),
                "-t", str(main_dur + 1),  # a little extra
                "-c:a", "aac", "-b:a", "192k",
                str(music_looped)
            ])

        # Mix main audio (input 0) with music_looped (input 1) into a stereo aac audio
        # We'll use amix with 2 inputs and normalize by 2 to avoid clipping
        run_ffmpeg([
            "-y",
            "-i", str(main),
            "-i", str(music_looped),
            "-filter_complex", "amix=inputs=2:normalize=0",
            "-c:a", "aac", "-b:a", "192k",
            "-vn",
            str(out_audio)
        ])

        # Finally mux composed video with composed audio
        print("Muxing video and audio ->", final_out)
        run_ffmpeg([
            "-y",
            "-i", str(out_video),
            "-i", str(out_audio),
            "-c:v", "copy",
            "-c:a", "copy",
            str(final_out)
        ])

        # Uploads (same as before)
        result = {"drive": None, "gcs": None, "youtube": None}

        if not SKIP_DRIVE_UPLOAD:
            result["drive"] = upload_drive(final_out, output_name)

        if GCS_OUTPUTS_BUCKET and GCS_OUTPUTS_PREFIX:
            oid = (result["drive"]["id"] if result["drive"] else final_out.stem) + ".mp4"
            key = f'{GCS_OUTPUTS_PREFIX.strip("/")}/{oid}'
            result["gcs"] = upload_gcs(final_out, key)

        if yt_title:
            result["youtube"] = upload_youtube(final_out, yt_title, yt_desc or "", yt_thumb)

        print(json.dumps(result, indent=2))

# --- CLI ---
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--drive-url", required=True)
    ap.add_argument("--output-name", required=True)
    ap.add_argument("--yt-title", help="YouTube title")
    ap.add_argument("--yt-desc", help="YouTube description")
    ap.add_argument("--yt-thumb", help="Path to thumbnail image")
    args = ap.parse_args()
    process(args.drive_url, args.output_name, args.yt_title, args.yt_desc, args.yt_thumb)