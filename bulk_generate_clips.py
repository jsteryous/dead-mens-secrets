#!/usr/bin/env python3
"""
Dead Men's Secrets — Bulk Video Clip Generator

Generates 5-second cinematic video clips from existing image library.
Uses Replicate minimax/video-01 — image + motion prompt → real video clip.
Stores clips in Supabase Storage `clips` bucket.
Records in `clip_library` table with same semantic description as source image.

Run locally (Railway network can't reach Replicate for long generations):
    python bulk_generate_clips.py

Cost: ~$0.05-0.10 per clip. Start with 20 clips to test quality.
171 images × $0.07 avg = ~$12 for full library.

Prerequisites:
    pip install requests sentence-transformers
    Supabase: create `clips` bucket (public) and `clip_library` table (see below)

Supabase SQL to create clip_library table:
    CREATE TABLE clip_library (
        id          bigserial PRIMARY KEY,
        filename    text UNIQUE NOT NULL,
        description text,
        style       text,
        storage_path text,
        source_image text,
        embedding   vector(384),
        created_at  timestamptz DEFAULT now()
    );
    CREATE INDEX ON clip_library USING ivfflat (embedding vector_cosine_ops);
"""

import os, re, sys, time, json, datetime, requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── CONFIG ────────────────────────────────────────────────────────────────────
REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY", "")
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "")

# Replicate video model — takes image + prompt, returns 5s clip
# minimax/video-01 is the best quality/cost balance right now
VIDEO_MODEL = "minimax/video-01"

# How many clips to generate per run (cost control)
# 20 = ~$1.40, 50 = ~$3.50, 171 = ~$12 (full library)
BATCH_SIZE = 20

# Workers — video generation is slow, 1-2 is enough
MAX_WORKERS = 1

# Motion prompt templates — matched to image style
# These tell the video model how to move the camera
MOTION_PROMPTS = {
    "cold_war_photo":         "slow push in, handheld, surveillance documentary style, desaturated, gritty",
    "gritty_documentary":     "slow zoom in, documentary camera movement, handheld, naturalistic",
    "noir_photograph":        "slow drift right, noir film style, moody shadows, low angle",
    "daguerreotype":          "slow pull back, vintage film grain, sepia toned, static weight",
    "oil_painting":           "slow tilt up, painterly, dramatic lighting, classical",
    "renaissance_painting":   "slow push in, renaissance style, candlelit, dramatic chiaroscuro",
    "illuminated_manuscript": "slow zoom, medieval aesthetic, torchlit, aged parchment feel",
}
DEFAULT_MOTION = "slow cinematic push in, dramatic, historical documentary"

# ── SUPABASE ──────────────────────────────────────────────────────────────────
def sb_get(table, params=""):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}?{params}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=15
    )
    return r.json() if r.status_code == 200 else []

def sb_upsert(table, data, on_conflict="filename"):
    requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "resolution=merge-duplicates",
        },
        json=data,
        timeout=15
    )

def upload_clip(clip_path, filename):
    """Upload video clip to Supabase Storage clips bucket."""
    with open(clip_path, "rb") as f:
        data = f.read()
    r = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/clips/{filename}",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "video/mp4",
        },
        data=data,
        timeout=60
    )
    if r.status_code in (200, 201):
        return f"clips/{filename}"
    print(f"  Upload failed {r.status_code}: {r.text[:100]}")
    return None

def get_existing_clips():
    """Return set of source image filenames already converted to clips."""
    rows = sb_get("clip_library", "select=source_image")
    return {r["source_image"] for r in rows if r.get("source_image")}

def get_image_library():
    """Return all images from library that have a storage_path."""
    rows = sb_get("image_library", "select=filename,description,style,storage_path&storage_path=not.is.null&limit=500")
    return [r for r in rows if r.get("storage_path")]

# ── REPLICATE VIDEO GENERATION ────────────────────────────────────────────────
def get_image_url(storage_path):
    """Build public Supabase URL for an image."""
    return f"{SUPABASE_URL}/storage/v1/object/public/{storage_path}"

def generate_clip(image_row, out_dir):
    """
    Generate a 5-second video clip from a library image.
    Returns local path to downloaded clip on success, None on failure.
    """
    filename    = image_row["filename"]
    description = image_row["description"]
    style       = image_row["style"]
    storage_path = image_row["storage_path"]

    image_url    = get_image_url(storage_path)
    motion       = MOTION_PROMPTS.get(style, DEFAULT_MOTION)
    full_prompt  = f"{description}. {motion}. No text overlays. Cinematic quality."

    # Clip filename mirrors image filename but with .mp4
    clip_name = Path(filename).stem + ".mp4"
    clip_path = Path(out_dir) / clip_name

    print(f"  Generating: {filename[:60]}")

    try:
        # Start prediction
        r = requests.post(
            f"https://api.replicate.com/v1/models/{VIDEO_MODEL}/predictions",
            headers={
                "Authorization": f"Bearer {REPLICATE_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "input": {
                    "prompt":       full_prompt,
                    "first_frame_image": image_url,
                    "duration":     5,
                }
            },
            timeout=30
        )

        if r.status_code == 429:
            print(f"  Rate limited — waiting 60s")
            time.sleep(60)
            return generate_clip(image_row, out_dir)  # retry once

        if r.status_code not in (200, 201):
            print(f"  Replicate {r.status_code}: {r.text[:150]}")
            return None

        pred = r.json()
        pred_id = pred.get("id")
        if not pred_id:
            print(f"  No prediction ID")
            return None

        # Poll until complete — video generation takes 2-4 minutes
        print(f"  Polling {pred_id[:12]}...", end="", flush=True)
        for attempt in range(240):  # up to 4 minutes
            time.sleep(3)
            poll = requests.get(
                f"https://api.replicate.com/v1/predictions/{pred_id}",
                headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"},
                timeout=15
            )
            if poll.status_code != 200:
                continue
            pd = poll.json()
            status = pd.get("status")
            if status == "succeeded":
                print(" done")
                output = pd.get("output")
                if not output:
                    return None
                # Output is a URL to the video file
                video_url = output if isinstance(output, str) else output[0]
                break
            elif status == "failed":
                print(f" FAILED: {pd.get('error', 'unknown')}")
                return None
            elif attempt % 10 == 9:
                print(".", end="", flush=True)
        else:
            print(" TIMEOUT")
            return None

        # Download the clip
        dl = requests.get(video_url, timeout=120)
        if dl.status_code != 200:
            print(f"  Download failed {dl.status_code}")
            return None

        with open(clip_path, "wb") as f:
            f.write(dl.content)

        size_kb = clip_path.stat().st_size // 1024
        print(f"  Downloaded: {clip_name} ({size_kb}KB)")
        return str(clip_path)

    except Exception as e:
        print(f"  Error: {e}")
        return None

# ── EMBEDDING ─────────────────────────────────────────────────────────────────
def compute_embedding(text):
    """Compute sentence embedding for semantic search."""
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        return model.encode(text).tolist()
    except Exception as e:
        print(f"  Embedding error: {e}")
        return None

# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("\n" + "═"*60)
    print("Dead Men's Secrets — Bulk Clip Generator")
    print(f"Model: {VIDEO_MODEL}")
    print(f"Batch size: {BATCH_SIZE} clips")
    print("═"*60 + "\n")

    if not REPLICATE_API_KEY:
        print("ERROR: REPLICATE_API_KEY not set")
        sys.exit(1)
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL / SUPABASE_KEY not set")
        sys.exit(1)

    # Check what's already been generated
    existing   = get_existing_clips()
    all_images = get_image_library()

    # Filter to images not yet converted
    todo = [img for img in all_images if img["filename"] not in existing]

    print(f"Library images: {len(all_images)}")
    print(f"Already have clips: {len(existing)}")
    print(f"To generate: {min(len(todo), BATCH_SIZE)} clips (batch of {BATCH_SIZE})")
    print(f"Est. cost: ~${min(len(todo), BATCH_SIZE) * 0.07:.2f}\n")

    if not todo:
        print("All images already have clips. Done.")
        return

    # Work in batches
    batch = todo[:BATCH_SIZE]

    # Temp directory for downloads
    out_dir = Path("clip_downloads")
    out_dir.mkdir(exist_ok=True)

    # Load embedding model once
    print("Loading embedding model...")
    try:
        from sentence_transformers import SentenceTransformer
        embed_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        print("  Model ready\n")
    except Exception as e:
        print(f"  Warning: embeddings unavailable ({e})\n")
        embed_model = None

    generated = 0
    failed    = 0
    cost      = 0.0

    for i, image_row in enumerate(batch):
        print(f"[{i+1}/{len(batch)}] {image_row['filename'][:55]}")

        clip_path = generate_clip(image_row, out_dir)
        if not clip_path:
            failed += 1
            print(f"  ✗ Failed\n")
            continue

        # Upload to Supabase
        clip_name    = Path(clip_path).name
        storage_path = upload_clip(clip_path, clip_name)
        if not storage_path:
            failed += 1
            continue

        # Compute embedding
        embedding = None
        if embed_model:
            try:
                embedding = embed_model.encode(image_row["description"]).tolist()
            except Exception:
                pass

        # Save to clip_library
        record = {
            "filename":     clip_name,
            "description":  image_row["description"],
            "style":        image_row["style"],
            "storage_path": storage_path,
            "source_image": image_row["filename"],
            "created_at":   datetime.datetime.utcnow().isoformat(),
        }
        if embedding:
            record["embedding"] = json.dumps(embedding)

        sb_upsert("clip_library", record)

        # Clean up local file
        Path(clip_path).unlink(missing_ok=True)

        generated += 1
        cost      += 0.07
        print(f"  ✓ Saved: {clip_name[:55]}")
        print(f"  Running cost: ~${cost:.2f}\n")

        # Rate limit pause between clips
        if i < len(batch) - 1:
            time.sleep(5)

    print("\n" + "═"*60)
    print(f"Complete: {generated} generated, {failed} failed")
    print(f"Clip library now has {len(existing) + generated} clips")
    print(f"Total cost: ~${cost:.2f}")
    print("═"*60)

if __name__ == "__main__":
    main()
