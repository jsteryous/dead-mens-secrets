#!/usr/bin/env python3
"""
20th Century Dark — Local Assembler

Runs on your local machine. Polls Supabase for pending jobs from Railway brain.
Downloads assets, assembles video with full quality FFmpeg, uploads to YouTube.

Run once manually or schedule via Task Scheduler to run hourly.
Your machine needs to be on for this to work — run it overnight.

Setup:
    pip install requests numpy
    Set env vars below or in run_assembler.ps1
"""

import os, re, json, base64, datetime, tempfile, subprocess, shutil, textwrap
import requests
from pathlib import Path

# ── ENVIRONMENT ───────────────────────────────────────────────────────────────
SUPABASE_URL          = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY          = os.environ.get("SUPABASE_KEY", "")
YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID", "")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET", "")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN", "")

# Video dimensions
W, H     = 1080, 1920
HOOK_DUR = 0.55
FONT     = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

# On Windows, use a bundled font or system font
if os.name == "nt":
    # Try common Windows font paths
    for fp in [
        "C:/Windows/Fonts/arialbd.ttf",
        "C:/Windows/Fonts/Arial Bold.ttf",
        "C:/Windows/Fonts/calibrib.ttf",
    ]:
        if Path(fp).exists():
            FONT = fp
            break

CAPTION_Y = int(H * 0.72)

VISUAL_STYLES = {
    "oil_painting":           "oil painting, dramatic chiaroscuro, Rembrandt lighting, museum quality",
    "cold_war_photo":         "cold war era photograph, 35mm film grain, desaturated, 1960s documentary",
    "daguerreotype":          "daguerreotype photograph, 1860s-1900s, sepia toned, high contrast",
    "illuminated_manuscript": "medieval illuminated manuscript, gold leaf, intricate borders, Gothic",
    "noir_photograph":        "noir photograph, high contrast black and white, dramatic shadows, 1940s",
    "renaissance_painting":   "Renaissance oil painting, classical composition, Italian masters style",
    "gritty_documentary":     "gritty documentary photograph, photojournalism, raw and unfiltered",
}

STYLE_GRADE = {
    "oil_painting":           "colorchannelmixer=rr=0.85:gg=0.80:bb=0.95,eq=contrast=1.15:brightness=-0.04:saturation=0.80",
    "cold_war_photo":         "colorchannelmixer=rr=0.75:gg=0.78:bb=0.78,eq=contrast=1.30:brightness=-0.06:saturation=0.20",
    "daguerreotype":          "colorchannelmixer=rr=0.90:gg=0.82:bb=0.65,eq=contrast=1.20:brightness=-0.03:saturation=0.35",
    "illuminated_manuscript": "colorchannelmixer=rr=0.92:gg=0.85:bb=0.60,eq=contrast=1.10:brightness=0.02:saturation=0.90",
    "noir_photograph":        "colorchannelmixer=rr=0.70:gg=0.72:bb=0.72,eq=contrast=1.40:brightness=-0.08:saturation=0.10",
    "renaissance_painting":   "colorchannelmixer=rr=0.88:gg=0.82:bb=0.72,eq=contrast=1.12:brightness=-0.02:saturation=0.85",
    "gritty_documentary":     "colorchannelmixer=rr=0.82:gg=0.80:bb=0.78,eq=contrast=1.25:brightness=-0.05:saturation=0.65",
}

MOTION_PROFILES = [
    lambda f: f"zoompan=z='min(zoom+0.0002,1.05)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={f}:s={W}x{H}:fps=30",
    lambda f: f"zoompan=z='if(eq(on,1),1.06,max(1.0,zoom-0.0002))':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d={f}:s={W}x{H}:fps=30",
    lambda f: f"zoompan=z='1.04':x='iw/2-(iw/zoom/2)+(on/{max(f,1)})*{W//8}':y='ih/2-(ih/zoom/2)':d={f}:s={W}x{H}:fps=30",
    lambda f: f"zoompan=z='min(zoom+0.0001,1.04)':x='iw/2-(iw/zoom/2)-((on/{max(f,1)})*{W//10})':y='ih/2-(ih/zoom/2)':d={f}:s={W}x{H}:fps=30",
    lambda f: f"zoompan=z='1.04':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)-((on/{max(f,1)})*{H//12})':d={f}:s={W}x{H}:fps=30",
    lambda f: f"zoompan=z='1.04':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)+((on/{max(f,1)})*{H//12})':d={f}:s={W}x{H}:fps=30",
]


# ── HELPERS ───────────────────────────────────────────────────────────────────
def run(cmd, check=True):
    return subprocess.run(cmd, capture_output=True, text=True, check=check)

def get_duration(path):
    r = run(["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", path], check=False)
    try:
        for s in json.loads(r.stdout).get("streams", []):
            if "duration" in s:
                return float(s["duration"])
    except Exception:
        pass
    return 60.0

def esc(text):
    return text.replace("'", "\u2019").replace(":", "\\:").replace("\\", "\\\\")


# ── SUPABASE ──────────────────────────────────────────────────────────────────
def _sb_headers(prefer="return=minimal"):
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        prefer,
    }

def get_pending_jobs():
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/jobs?status=eq.pending&order=created_at.asc&limit=5",
        headers=_sb_headers("return=representation"),
        timeout=15
    )
    return r.json() if r.status_code == 200 else []

def mark_job(job_id, status, video_id=None):
    data = {"status": status, "processed_at": datetime.datetime.utcnow().isoformat()}
    if video_id:
        data["video_id"] = video_id
    requests.patch(
        f"{SUPABASE_URL}/rest/v1/jobs?id=eq.{job_id}",
        headers=_sb_headers(), json=data, timeout=15
    )

def log_video(video_id, title, topic):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/performance",
        headers=_sb_headers(),
        json={"video_id": video_id, "title": title, "topic": topic,
              "posted_at": datetime.datetime.utcnow().isoformat()},
        timeout=15
    )

def download_asset(storage_path, local_path):
    url = f"{SUPABASE_URL}/storage/v1/object/public/{storage_path}"
    r = requests.get(url, timeout=60)
    if r.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(r.content)
        return True
    print(f"  Asset download failed {r.status_code}: {storage_path}")
    return False

def get_clip_for_scene(scene_prompt, used, tmpdir, idx):
    """Try to get a pre-rendered clip for this scene."""
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/clip_library?select=filename,description,storage_path&storage_path=not.is.null&limit=500",
            headers=_sb_headers("return=representation"), timeout=15
        )
        rows = [row for row in (r.json() if r.status_code == 200 else [])
                if row["filename"] not in used]
        if not rows:
            return None
        # Keyword match
        prompt_words = set(re.sub(r'[^\w\s]', '', scene_prompt).lower().split())
        best, best_score = None, -1
        for row in rows:
            desc_words = set(re.sub(r'[^\w\s]', '', row["description"]).lower().split())
            score = len(prompt_words & desc_words)
            if score > best_score:
                best_score, best = score, row
        if not best:
            return None
        used.add(best["filename"])
        local = f"{tmpdir}/clip_{idx}.mp4"
        if download_asset(best["storage_path"], local):
            return local
    except Exception as e:
        print(f"  Clip fetch error: {e}")
    return None


# ── VIDEO ASSEMBLY ────────────────────────────────────────────────────────────
def build_cinematic_clip(img_path, dur, motion_idx, visual_style, out_path):
    """Full quality cinematic clip — no time pressure on local machine."""
    frames = max(int(dur * 30), 1)
    motion = MOTION_PROFILES[motion_idx % len(MOTION_PROFILES)](frames)
    grade  = STYLE_GRADE.get(visual_style, STYLE_GRADE["cold_war_photo"])
    flicker = "noise=alls=3:allf=t,lutyuv=y=val+random(0)*8-4"

    vf = (
        f"scale={W*4}:{H*4}:force_original_aspect_ratio=increase,"
        f"crop={W*4}:{H*4},"
        f"{motion},"
        f"{grade},"
        f"vignette=PI/3.5,"
        f"{flicker}"
    )
    r = run(
        ["ffmpeg", "-y", "-loop", "1", "-i", img_path,
         "-vf", vf, "-t", str(dur + 0.5),
         "-c:v", "libx264", "-preset", "medium", "-crf", "18",
         "-pix_fmt", "yuv420p", "-an", out_path],
        check=False
    )
    return r.returncode == 0 and Path(out_path).exists()

def build_background(image_paths, clip_paths, beat_times, voice_dur, visual_style, tmpdir):
    """
    Build background video.
    Prefers pre-rendered clips. Falls back to cinematic still images.
    Full quality — no shortcuts since we're on local machine.
    """
    bg = f"{tmpdir}/bg.mp4"

    # Use pre-rendered clips if available
    if clip_paths:
        print(f"  Using {len(clip_paths)} pre-rendered clips")
        all_clips = (clip_paths * (int(voice_dur // 5) + 2))
        lst = f"{tmpdir}/clips_list.txt"
        with open(lst, "w") as f:
            for c in all_clips:
                f.write(f"file '{c}'\n")
        r = run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", lst,
             "-t", str(voice_dur),
             "-vf", f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H}",
             "-c:v", "libx264", "-preset", "medium", "-crf", "18",
             "-pix_fmt", "yuv420p", bg],
            check=False
        )
        if r.returncode == 0:
            return bg

    if not image_paths:
        run(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c=0x08080f:size={W}x{H}:duration={voice_dur}:rate=30",
             "-vf", "noise=alls=6:allf=t+u,vignette=PI/3",
             "-c:v", "libx264", "-preset", "medium", "-crf", "28",
             "-pix_fmt", "yuv420p", bg])
        return bg

    # Cinematic still images — full quality
    pts  = sorted(set([0.0] + beat_times + [1.0]))
    durs = [(pts[i+1] - pts[i]) * voice_dur for i in range(len(pts) - 1)]
    imgs = (image_paths * (len(durs) // len(image_paths) + 1))[:len(durs)]

    clips = []
    for i, (img, dur) in enumerate(zip(imgs, durs)):
        out = f"{tmpdir}/cin_{i}.mp4"
        ok  = build_cinematic_clip(img, dur, i, visual_style, out)
        if ok:
            clips.append((out, dur))
        else:
            sout = f"{tmpdir}/static_{i}.mp4"
            run(["ffmpeg", "-y", "-loop", "1", "-i", img,
                 "-t", str(dur + 0.5),
                 "-vf", f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H}",
                 "-c:v", "libx264", "-preset", "medium", "-crf", "22",
                 "-pix_fmt", "yuv420p", "-an", sout], check=False)
            if Path(sout).exists():
                clips.append((sout, dur))

    if not clips:
        return build_background([], [], beat_times, voice_dur, visual_style, tmpdir)

    if len(clips) == 1:
        run(["ffmpeg", "-y", "-i", clips[0][0],
             "-t", str(voice_dur), "-c:v", "libx264", "-preset", "medium",
             "-crf", "18", "-pix_fmt", "yuv420p", bg])
        return bg

    # Full crossfade transitions
    xfade  = 0.5
    inputs = sum([["-i", c] for c, _ in clips], [])
    parts  = []
    label  = "[0:v]"
    offset = 0.0

    for i in range(1, len(clips)):
        offset += clips[i-1][1] - xfade
        offset  = max(offset, 0.01)
        nxt     = f"[v{i}]" if i < len(clips) - 1 else "[vout]"
        parts.append(
            f"{label}[{i}:v]xfade=transition=fade"
            f":duration={xfade}:offset={offset:.3f}{nxt}"
        )
        label = nxt

    r = run(
        ["ffmpeg", "-y"] + inputs + [
            "-filter_complex", ";".join(parts),
            "-map", "[vout]",
            "-t", str(voice_dur),
            "-c:v", "libx264", "-preset", "medium", "-crf", "18",
            "-pix_fmt", "yuv420p", bg
        ], check=False
    )
    if r.returncode != 0:
        # Concat fallback
        lst = f"{tmpdir}/list.txt"
        with open(lst, "w") as f:
            for c, _ in clips:
                f.write(f"file '{c}'\n")
        run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", lst,
             "-t", str(voice_dur), "-vf", f"scale={W}:{H}",
             "-c:v", "libx264", "-preset", "medium", "-crf", "22",
             "-pix_fmt", "yuv420p", bg], check=False)
    return bg

def assemble_video(word_timings, hook_word, audio_path, bg_path,
                   music_path, beat_times, output_path):
    print("  Assembling final video...")
    voice_dur = get_duration(audio_path)
    total_dur = voice_dur + HOOK_DUR

    # Caption chunks
    chunks = []
    i = 0
    while i < len(word_timings):
        group = word_timings[i:i+3]
        if group:
            text = " ".join(w[0] for w in group)
            t0   = group[0][1] + HOOK_DUR
            t1   = group[-1][2] + HOOK_DUR
            if i + 3 < len(word_timings):
                t1 = min(t1 + 0.04, word_timings[i+3][1] + HOOK_DUR)
            chunks.append((text, t0, t1))
        i += 3

    beat_secs = [b * voice_dur + HOOK_DUR for b in beat_times]

    filters = [
        "vignette=PI/3.5",
        f"fade=t=in:st={HOOK_DUR:.2f}:d=0.4",
        f"drawtext=text='20TH CENTURY DARK'"
        f":fontfile={FONT}:fontsize=38:fontcolor=#FFD700"
        f":x=(w-text_w)/2:y=88:borderw=3:bordercolor=black@0.95",
        f"drawtext=text='{esc(hook_word)}'"
        f":fontfile={FONT}:fontsize=170:fontcolor=white"
        f":x=(w-text_w)/2:y=(h-text_h)/2"
        f":borderw=8:bordercolor=black"
        f":enable='between(t,0,{HOOK_DUR-0.05:.2f})'",
    ]

    for text, t0, t1 in chunks:
        char_count = len(text)
        font_size  = max(44, int(66 * 24 / char_count)) if char_count > 24 else 66
        filters.append(
            f"drawtext=text='{esc(text)}'"
            f":fontfile={FONT}:fontsize={font_size}:fontcolor=white"
            f":x=max(48,(w-text_w)/2):y={CAPTION_Y}"
            f":borderw=5:bordercolor=black@0.92"
            f":enable='between(t,{t0:.3f},{t1:.3f})'"
        )

    for bt in beat_secs:
        filters.append(
            f"drawbox=x=0:y=0:w={W}:h={H}:color=white@0.06:t=fill"
            f":enable='between(t,{bt:.2f},{bt+0.08:.2f})'"
        )

    vf = ",".join(filters)

    if music_path and Path(music_path).exists():
        af = (f"[1:a]volume=1.0[voice];"
              f"[2:a]volume=0.08,"
              f"afade=t=in:st={HOOK_DUR}:d=2.5,"
              f"afade=t=out:st={total_dur-2.0}:d=1.8[music];"
              f"[voice][music]amix=inputs=2:duration=first[aout]")
        cmd = ["ffmpeg", "-y",
               "-i", bg_path, "-i", audio_path, "-i", music_path,
               "-filter_complex", af, "-vf", vf,
               "-map", "0:v", "-map", "[aout]",
               "-c:v", "libx264", "-preset", "medium", "-crf", "18",
               "-c:a", "aac", "-b:a", "192k",
               "-t", str(total_dur), "-movflags", "+faststart",
               "-pix_fmt", "yuv420p", output_path]
    else:
        cmd = ["ffmpeg", "-y",
               "-i", bg_path, "-i", audio_path,
               "-vf", vf,
               "-c:v", "libx264", "-preset", "medium", "-crf", "18",
               "-c:a", "aac", "-b:a", "192k",
               "-shortest", "-movflags", "+faststart",
               "-pix_fmt", "yuv420p", output_path]

    r = run(cmd, check=False)
    if r.returncode != 0:
        print(f"  Assembly retry without music")
        run(["ffmpeg", "-y", "-i", bg_path, "-i", audio_path,
             "-vf", vf, "-c:v", "libx264", "-preset", "medium", "-crf", "18",
             "-c:a", "aac", "-b:a", "192k", "-shortest",
             "-movflags", "+faststart", "-pix_fmt", "yuv420p", output_path])

    size = Path(output_path).stat().st_size / 1048576
    print(f"  Video: {size:.1f}MB, {total_dur:.1f}s")

def build_thumbnail(thumb_text, image_paths, tmpdir):
    TW, TH     = 1280, 720
    thumb_path = f"{tmpdir}/thumbnail.jpg"
    bg_frame   = f"{tmpdir}/thumb_bg.jpg"
    got_frame  = False

    if image_paths:
        r = run(["ffmpeg", "-y", "-i", image_paths[0],
                 "-vf", f"scale={TW}:{TH}:force_original_aspect_ratio=increase,crop={TW}:{TH}",
                 "-frames:v", "1", bg_frame], check=False)
        got_frame = r.returncode == 0 and Path(bg_frame).exists()

    if not got_frame:
        run(["ffmpeg", "-y", "-f", "lavfi",
             "-i", f"color=c=0x08080f:size={TW}x{TH}",
             "-frames:v", "1", bg_frame])

    lines       = textwrap.wrap(thumb_text.upper(), width=16)
    text_filters = []
    y_start     = TH // 2 - len(lines) * 70
    for li, line in enumerate(lines):
        safe = esc(line)
        y    = y_start + li * 130
        text_filters.append(f"drawtext=text='{safe}':fontfile={FONT}:fontsize=110"
                             f":fontcolor=white:x=(w-text_w)/2:y={y}"
                             f":borderw=8:bordercolor=black@0.9")
        text_filters.append(f"drawtext=text='{safe}':fontfile={FONT}:fontsize=110"
                             f":fontcolor=white:x=(w-text_w)/2:y={y}"
                             f":shadowx=4:shadowy=4:shadowcolor=black@0.8")
    text_filters.append(
        f"drawtext=text='20TH CENTURY DARK':fontfile={FONT}:fontsize=42"
        f":fontcolor=#FFD700:x=(w-text_w)/2:y=28:borderw=3:bordercolor=black"
    )
    tf = ",".join(text_filters)
    run(["ffmpeg", "-y", "-i", bg_frame,
         "-vf", f"eq=contrast=1.25:brightness=-0.08,vignette=PI/3,{tf}",
         "-frames:v", "1", thumb_path], check=False)
    return thumb_path


# ── YOUTUBE UPLOAD ────────────────────────────────────────────────────────────
def get_youtube_token():
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     YOUTUBE_CLIENT_ID,
        "client_secret": YOUTUBE_CLIENT_SECRET,
        "refresh_token": YOUTUBE_REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    }, timeout=15)
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"YouTube token failed: {r.text[:200]}")
    return token

def upload_to_youtube(output_path, thumb_path, title, script, tags, token):
    print("  Uploading to YouTube...")
    desc = f"{script}\n\nFollow 20th Century Dark — true history buried for a reason.\n\n#" + " #".join(tags)
    meta = {
        "snippet": {
            "title":       title[:100],
            "description": desc[:5000],
            "tags":        tags[:15],
            "categoryId":  "27",
        },
        "status": {"privacyStatus": "public", "selfDeclaredMadeForKids": False},
    }
    init = requests.post(
        "https://www.googleapis.com/upload/youtube/v3/videos"
        "?uploadType=resumable&part=snippet,status",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type":  "application/json",
                 "X-Upload-Content-Type": "video/mp4"},
        json=meta, timeout=30
    )
    if init.status_code != 200:
        raise RuntimeError(f"YouTube init {init.status_code}: {init.text[:200]}")
    upload_url = init.headers.get("Location")
    with open(output_path, "rb") as f:
        video_data = f.read()
    up = requests.put(upload_url,
                      headers={"Content-Type": "video/mp4",
                               "Content-Length": str(len(video_data))},
                      data=video_data, timeout=300)
    if up.status_code not in (200, 201):
        raise RuntimeError(f"YouTube upload {up.status_code}: {up.text[:200]}")
    video_id = up.json().get("id")
    print(f"  Live: https://youtube.com/shorts/{video_id}")

    # Thumbnail
    if thumb_path and Path(thumb_path).exists():
        try:
            with open(thumb_path, "rb") as f:
                td = f.read()
            tr = requests.post(
                f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set"
                f"?videoId={video_id}&uploadType=media",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type":  "image/jpeg",
                         "Content-Length": str(len(td))},
                data=td, timeout=30
            )
            print(f"  Thumbnail: {'uploaded' if tr.status_code == 200 else f'failed ({tr.status_code})'}")
        except Exception as e:
            print(f"  Thumbnail error: {e}")
    return video_id


# ── MUSIC ─────────────────────────────────────────────────────────────────────
def get_music(tmpdir):
    """Try Supabase music bucket, fall back to ambient_fallback.mp3."""
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            r = requests.post(
                f"{SUPABASE_URL}/storage/v1/object/list/music",
                headers={"apikey": SUPABASE_KEY,
                         "Authorization": f"Bearer {SUPABASE_KEY}",
                         "Content-Type": "application/json"},
                json={"limit": 100, "offset": 0}, timeout=10
            )
            tracks = [t for t in (r.json() if r.status_code == 200 else [])
                      if t.get("name", "").endswith(".mp3")]
            if tracks:
                import random
                track = random.choice(tracks)
                url   = f"{SUPABASE_URL}/storage/v1/object/public/music/{track['name']}"
                dl    = requests.get(url, timeout=30)
                if dl.status_code == 200:
                    path = f"{tmpdir}/music.mp3"
                    with open(path, "wb") as f:
                        f.write(dl.content)
                    print(f"  Music: {track['name']}")
                    return path
        except Exception:
            pass

    # Ambient fallback
    fallback = Path(__file__).parent / "ambient_fallback.mp3"
    if fallback.exists():
        path = f"{tmpdir}/music.mp3"
        shutil.copy(str(fallback), path)
        print("  Music: ambient fallback")
        return path
    return None


# ── PROCESS JOB ──────────────────────────────────────────────────────────────
def process_job(job, tmpdir):
    job_id = job["id"]
    print(f"\nProcessing job: {job_id}")
    print(f"Topic: {job['topic'][:80]}")
    print(f"Title: {job['title']}")

    # Parse job data
    word_timings  = json.loads(job["word_timings"])
    image_paths_s = json.loads(job["image_paths"])  # Supabase storage paths
    beat_times    = json.loads(job["beat_times"])
    scene_prompts = json.loads(job["scene_prompts"])
    tags          = json.loads(job["tags"])
    hook_word     = job["hook_word"]
    visual_style  = job["visual_style"]
    thumb_text    = job["thumb_text"]
    title         = job["title"]
    script        = job["script"]
    audio_path_s  = job["audio_path"]

    # Download audio
    audio_path = f"{tmpdir}/voice.mp3"
    print("  Downloading audio...")
    if not download_asset(audio_path_s, audio_path):
        raise RuntimeError("Audio download failed")

    # Download images
    print("  Downloading images...")
    image_paths = []
    for i, sp in enumerate(image_paths_s):
        local = f"{tmpdir}/img_{i}.jpg"
        if download_asset(sp, local):
            image_paths.append(local)

    # Try to get pre-rendered clips
    print("  Checking clip library...")
    clip_paths = []
    used_clips = set()
    for i, prompt in enumerate(scene_prompts):
        clip = get_clip_for_scene(prompt, used_clips, tmpdir, i)
        if clip:
            clip_paths.append(clip)
    print(f"  Clips: {len(clip_paths)}/{len(scene_prompts)} found")

    # Get music
    music_path = get_music(tmpdir)

    # Build background
    print("  Building background...")
    bg_path = build_background(image_paths, clip_paths, beat_times,
                               get_duration(audio_path), visual_style, tmpdir)

    # Assemble
    output_path = f"{tmpdir}/short.mp4"
    assemble_video(word_timings, hook_word, audio_path, bg_path,
                   music_path, beat_times, output_path)

    # Thumbnail
    thumb_path = build_thumbnail(thumb_text, image_paths, tmpdir)

    # Upload to YouTube
    token    = get_youtube_token()
    video_id = upload_to_youtube(output_path, thumb_path, title, script, tags, token)

    # Log and mark done
    log_video(video_id, title, job["topic"])
    mark_job(job_id, "done", video_id)

    print(f"\n{'═'*60}")
    print(f"LIVE:  https://youtube.com/shorts/{video_id}")
    print(f"TITLE: {title}")
    print(f"{'═'*60}")
    return video_id


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'═'*60}\n20th Century Dark — Assembler — {now}\n{'═'*60}\n")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL / SUPABASE_KEY not set")
        return

    jobs = get_pending_jobs()
    if not jobs:
        print("No pending jobs. Railway hasn't run yet today.")
        return

    print(f"Found {len(jobs)} pending job(s)")

    for job in jobs:
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                mark_job(job["id"], "processing")
                process_job(job, tmpdir)
            except Exception as e:
                print(f"\nJob {job['id']} failed: {e}")
                mark_job(job["id"], "failed")

    print("\nAll jobs processed.")


if __name__ == "__main__":
    main()
