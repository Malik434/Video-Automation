import os, re, json, tempfile, subprocess, argparse
from pathlib import Path
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.cloud import storage
from google.oauth2.credentials import Credentials # Added for YouTube

# --- CONFIGURATION ---
# We use the Mount Path to avoid OOM crashes (RAM usage)
MOUNT_PATH = Path("/mnt/townhall-bucket") 
ASSETS_PREFIX = os.environ.get("ASSETS_PREFIX", "assets")
DRIVE_OUTPUT_FOLDER_ID = os.environ.get("DRIVE_OUTPUT_FOLDER_ID")
SKIP_DRIVE_UPLOAD = os.environ.get("SKIP_DRIVE_UPLOAD", "0") == "1"
GCS_OUTPUTS_BUCKET = os.environ.get("GCS_OUTPUTS_BUCKET", "")
GCS_OUTPUTS_PREFIX = os.environ.get("GCS_OUTPUTS_PREFIX", "")

# --- CLIENTS ---
def creds_with_scopes(scopes):
    creds, _ = google.auth.default(scopes=scopes)
    return creds

def drive():
    return build("drive", "v3", credentials=creds_with_scopes([
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/drive.file"
    ]), cache_discovery=False)

def youtube():
    # Added: OAuth Logic for User Channel Uploads
    if "YOUTUBE_REFRESH_TOKEN" in os.environ:
        c = Credentials(
            None, 
            refresh_token=os.environ["YOUTUBE_REFRESH_TOKEN"],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=os.environ["YOUTUBE_CLIENT_ID"],
            client_secret=os.environ["YOUTUBE_CLIENT_SECRET"],
            scopes=["https://www.googleapis.com/auth/youtube.upload"]
        )
        return build("youtube", "v3", credentials=c, cache_discovery=False)
    return None

def storage_client():
    return storage.Client()

# --- HELPERS ---
def parse_drive_id(url_or_id: str) -> str:
    if re.fullmatch(r"[A-Za-z0-9_-]{20,}", url_or_id): return url_or_id
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", url_or_id) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", url_or_id)
    if not m: raise ValueError("Could not parse Google Drive fileId")
    return m.group(1)

def dl_drive(file_id: str, dest: Path):
    req = drive().files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(dest, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, req, chunksize=32*1024*1024)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    
    # ADDED: Critical Safety Check for "Corrupt" Downloads
    size_mb = dest.stat().st_size / (1024*1024)
    if size_mb < 1.0:
        raise RuntimeError(f"Downloaded file is too small ({size_mb:.2f}MB). Check Drive permissions!")

def run_ffmpeg(args: list):
    print(f"Running FFmpeg: {' '.join(args)}")
    # Added 'check=True' to raise error immediately if it fails
    subprocess.run(["ffmpeg"] + args, text=True, check=True)

def upload_drive(local: Path, out_name: str):
    if not DRIVE_OUTPUT_FOLDER_ID: return None
    media = MediaFileUpload(str(local), mimetype="video/mp4", resumable=True)
    body = {"name": out_name, "parents": [DRIVE_OUTPUT_FOLDER_ID]}
    req = drive().files().create(body=body, media_body=media, supportsAllDrives=True, fields="id")
    resp = None
    while resp is None: _, resp = req.next_chunk()
    return resp

def upload_gcs(local: Path, object_name: str):
    if not GCS_OUTPUTS_BUCKET: return None
    blob = storage_client().bucket(GCS_OUTPUTS_BUCKET).blob(object_name)
    blob.upload_from_filename(str(local), content_type="video/mp4")
    return f"gs://{blob.bucket.name}/{blob.name}"

def upload_yt(local: Path, title: str, desc: str):
    yt = youtube()
    if not yt: return "Skipped (No Auth)"
    body = {
        "snippet": {"title": title, "description": desc, "categoryId": "22"},
        "status": {"privacyStatus": "unlisted"}
    }
    media = MediaFileUpload(str(local), mimetype="video/mp4", resumable=True)
    req = yt.videos().insert(part="snippet,status", body=body, media_body=media)
    resp = None
    while resp is None: _, resp = req.next_chunk()
    return f"https://youtu.be/{resp['id']}"

# --- MAIN PROCESS ---
def process(drive_url: str, output_name: str, yt_title=None, yt_desc=None):
    fid = parse_drive_id(drive_url)
    
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        main = tmp/"main.mp4"
        out  = tmp/"final_output.mp4"

        # 1) Inputs
        print("Downloading from Drive...")
        dl_drive(fid, main)
        
        # CHANGED: Use Volume Mounts instead of dl_gcs (Fixes Memory Crash)
        assets = MOUNT_PATH / ASSETS_PREFIX
        bg = assets/"background.mp4"
        outro = assets/"outro.mp4"
        music = assets/"music.mp3"

        # Verify assets exist
        for p in [bg, outro, music]:
            if not p.exists(): raise RuntimeError(f"Missing asset on mount: {p}")

        # 2) FFmpeg Graph (Your Original Filter Logic)
        # Note: I added 'pad' to the scale filters to prevent aspect ratio crashes
        filter_complex = (
          "[1:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[bg];"
          "[0:v]scale=1400:-1[fg];"
          "[bg][fg]overlay=(W-w)/2:(H-h)/2:shortest=1[comp];"
          "[2:v]scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2[outro];"
          "[comp][outro]concat=n=2:v=1:a=0[vout];"
          "[3:a]asplit=2[a_intro_src][a_outro_src];"
          "[a_intro_src]atrim=0:5,afade=t=in:st=0:d=2,afade=t=out:st=3:d=2[aintro];"
          "[a_outro_src]atrim=0:12,afade=t=in:st=0:d=3,afade=t=out:st=9:d=3[aoutro];"
          "[0:a]anull[main];" # NOTE: Ensure main.mp4 actually has audio or this 'anull' might fail if stream missing
          "[aintro][main]amix=inputs=2[intro_mix];"
          "[intro_mix][aoutro]concat=n=2:v=0:a=1[aout]"
        )

        run_ffmpeg([
          "-y","-i",str(main),"-i",str(bg),"-i",str(outro),"-i",str(music),
          "-filter_complex", filter_complex, 
          "-map","[vout]","-map","[aout]",
          "-c:v","libx264","-crf","18","-preset","superfast","-threads","0",
          "-c:a","aac",
          str(out)
        ])

        # 3) Outputs
        result = {"drive": None, "gcs": None, "youtube": None}
        
        if not SKIP_DRIVE_UPLOAD:
            result["drive"] = upload_drive(out, output_name)
            
        if GCS_OUTPUTS_BUCKET and GCS_OUTPUTS_PREFIX:
            oid = (result["drive"]["id"] if result["drive"] else out.stem) + ".mp4"
            key = f'{GCS_OUTPUTS_PREFIX.strip("/")}/{oid}'
            result["gcs"] = upload_gcs(out, key)

        if yt_title:
            result["youtube"] = upload_yt(out, yt_title, yt_desc or "")
            
        print(json.dumps(result, indent=2))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--drive-url", required=True)
    ap.add_argument("--output-name", required=True)
    ap.add_argument("--yt-title")
    ap.add_argument("--yt-desc")
    args = ap.parse_args()
    process(args.drive_url, args.output_name, args.yt_title, args.yt_desc)