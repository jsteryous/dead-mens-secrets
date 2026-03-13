#!/usr/bin/env python3
"""
20th Century Dark — Railway Brain (V1)

Railway runs this daily via cron. Does the thinking, never the rendering.
Phases 1-3 only: analytics → topic → research → script → voiceover → images
Saves a pending job to Supabase `jobs` table.
Local machine picks up the job, assembles video, uploads to YouTube.

No FFmpeg. No video processing. Runs in under 3 minutes. Never crashes.
"""

import os, re, json, base64, datetime, tempfile, concurrent.futures
import requests
from pathlib import Path

# ── ENVIRONMENT ───────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY     = os.environ.get("ANTHROPIC_API_KEY")
ELEVENLABS_API_KEY    = os.environ.get("ELEVENLABS_API_KEY")
ELEVENLABS_VOICE_ID   = "Kbva8lG07GrIZu9cOZ7h"
REPLICATE_API_KEY     = os.environ.get("REPLICATE_API_KEY")
YOUTUBE_CLIENT_ID     = os.environ.get("YOUTUBE_CLIENT_ID")
YOUTUBE_CLIENT_SECRET = os.environ.get("YOUTUBE_CLIENT_SECRET")
YOUTUBE_REFRESH_TOKEN = os.environ.get("YOUTUBE_REFRESH_TOKEN")
SUPABASE_URL          = os.environ.get("SUPABASE_URL")
SUPABASE_KEY          = os.environ.get("SUPABASE_KEY")

VISUAL_STYLES = {
    "oil_painting":           "oil painting, dramatic chiaroscuro, Rembrandt lighting, museum quality",
    "cold_war_photo":         "cold war era photograph, 35mm film grain, desaturated, 1960s documentary",
    "daguerreotype":          "daguerreotype photograph, 1860s-1900s, sepia toned, high contrast",
    "illuminated_manuscript": "medieval illuminated manuscript, gold leaf, intricate borders, Gothic",
    "noir_photograph":        "noir photograph, high contrast black and white, dramatic shadows, 1940s",
    "renaissance_painting":   "Renaissance oil painting, classical composition, Italian masters style",
    "gritty_documentary":     "gritty documentary photograph, photojournalism, raw and unfiltered",
}
DEFAULT_STYLE = "cold_war_photo"


# ── SUPABASE ──────────────────────────────────────────────────────────────────
def _sb_headers(prefer="return=minimal"):
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        prefer,
    }

def sb_select(table, params):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=_sb_headers("return=representation"),
        params=params, timeout=15
    )
    return r.json() if r.status_code == 200 else []

def sb_insert(table, row):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=_sb_headers(), json=row, timeout=15
        )
        if r.status_code not in (200, 201):
            print(f"  Supabase INSERT {table} {r.status_code}: {r.text[:150]}")
    except Exception as e:
        print(f"  Supabase INSERT error: {e}")

def sb_upsert(table, row, on_conflict):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
            params={"on_conflict": on_conflict},
            json=row, timeout=15
        )
        if r.status_code not in (200, 201):
            print(f"  Supabase UPSERT {table} {r.status_code}: {r.text[:150]}")
    except Exception as e:
        print(f"  Supabase UPSERT error: {e}")

def get_used_topics():
    rows = sb_select("used_topics", {"select": "topic", "order": "created_at.desc", "limit": "120"})
    return [r["topic"] for r in rows]

def save_topic(topic):
    sb_insert("used_topics", {"topic": topic, "created_at": datetime.datetime.utcnow().isoformat()})

def save_insights(insights):
    sb_upsert("insights", {
        "id": 1,
        "data": json.dumps(insights),
        "generated_at": datetime.datetime.utcnow().isoformat()
    }, on_conflict="id")


# ── CLAUDE ────────────────────────────────────────────────────────────────────
def claude(prompt, max_tokens=800, system=None):
    body = {
        "model":      "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "messages":   [{"role": "user", "content": prompt}],
    }
    if system:
        body["system"] = system
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                 "Content-Type": "application/json"},
        json=body, timeout=60
    )
    if r.status_code != 200:
        raise RuntimeError(f"Claude {r.status_code}: {r.text[:200]}")
    return r.json()["content"][0]["text"].strip()


# ── YOUTUBE ANALYTICS ─────────────────────────────────────────────────────────
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
    print("YouTube token obtained")
    return token

def pull_analytics(token):
    print("Pulling YouTube analytics...")
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=28)
    r = requests.get(
        "https://youtubeanalytics.googleapis.com/v2/reports",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "ids":        "channel==MINE",
            "startDate":  str(start),
            "endDate":    str(end),
            "metrics":    "views,averageViewDuration,averageViewPercentage,likes,shares,subscribersGained",
            "dimensions": "video",
            "sort":       "-views",
            "maxResults": "20",
        }, timeout=15
    )
    data = r.json() if r.status_code == 200 else {}
    rows = data.get("rows", [])
    print(f"Analytics: {len(rows)} videos")
    return data

def analyze_performance(ana_data):
    rows = ana_data.get("rows", [])
    if not rows:
        print("New channel — generating without performance data")
        return {"has_data": False, "analysis": "", "summary": "",
                "top_performers": [], "insights": {}}

    stored = sb_select("insights", {"select": "data", "id": "eq.1", "limit": "1"})
    if stored:
        try:
            return json.loads(stored[0]["data"])
        except Exception:
            pass

    headers = [c["name"] for c in ana_data.get("columnHeaders", [])]
    rows_str = "\n".join(str(r) for r in rows[:10])
    analysis = claude(
        f"YouTube Shorts analytics (last 28 days):\nColumns: {headers}\nTop videos:\n{rows_str}\n\n"
        "In 3 sentences: what topics/styles drive highest retention? What patterns repeat?",
        max_tokens=300
    )
    top = [r[0] for r in rows[:3]] if rows else []
    insights = {
        "has_data":      True,
        "analysis":      analysis,
        "summary":       analysis[:200],
        "top_performers": top,
        "insights":      {},
    }
    save_insights(insights)
    return insights


# ── TOPIC + RESEARCH + SCRIPT (three-pass) ────────────────────────────────────
def generate_topic(insights):
    print("Generating topic...")
    used     = get_used_topics()
    used_str = "\n".join(f"- {t}" for t in used[:60]) or "None yet"
    perf = (
        f"\nWHAT IS WORKING:\n{insights['analysis'][:350]}\n"
        f"Top titles: {', '.join(chr(34)+t+chr(34) for t in insights['top_performers'][:3])}\n"
        f"Generate a topic matching these winning patterns."
        if insights.get("has_data")
        else "\nNew channel. Prioritize: named individuals, specific numbers, betrayal, unexpected consequences."
    )

    topic = claude(f"""You are the creative director of a dark history channel focused exclusively on 20th century history (1900-1991).

Your niche: the buried stories of the World Wars, the Holocaust, Soviet atrocities, Cold War espionage, political assassinations, CIA/KGB operations, and the cults and mass events that defined the century.
{perf}

Generate ONE topic. Must be ALL of:
- Set between 1900 and 1991
- TRUE and verifiable
- OBSCURE — the famous version of this story exists, but not THIS specific angle
- Has a TWIST that recontextualizes everything — someone trusted who shouldn't have been, someone who profited, something covered up
- VISCERAL — stomach drop or jaw drop
- HYPER-SPECIFIC — named person, exact date, exact number, exact place

Strong examples of the right kind of topic:
- The SS officer who saved 1,200 Jews and was later prosecuted by Israel
- The Soviet scientist ordered to develop a poison with no antidote who secretly documented everything
- The CIA operative who ran both sides of a Cold War spy exchange for eleven years

Do NOT repeat these recent topics:
{used_str}

ONE sentence only. No preamble. No explanation.""", max_tokens=150)

    save_topic(topic)
    print(f"Topic: {topic}")
    return topic

def research_topic(topic, insights):
    print("Research pass...")
    retention = (f"\nChannel retention pattern: {insights['summary'][:200]}\nBuild arc to match."
                 if insights.get("has_data") else "")
    return claude(f"""Topic: "{topic}"{retention}

Reason through this story as a master storyteller — before writing a single word:

1. HOOK WORD: One word shown alone on black. Stops the scroll.
2. HOOK SENTENCE: Most shocking fact. No warmup.
3. SPECIFIC DETAIL: The verifiable detail that makes this undeniably real.
4. ESCALATION: The mid-story reveal that suddenly raises the stakes.
5. TWIST: The irony that recontextualizes everything before it.
6. SCENES: 5-6 distinct visual moments. Describe each specifically.
7. BEATS: 2-3 peak revelation moments — where to cut visually. Mark [BEAT].
8. SHARE TRIGGER: The one thing that makes someone show this to a friend right now.""", max_tokens=600)

def write_script(topic, research, insights):
    print("Writing script + visual direction...")
    perf = (f"\nChannel insights: {insights['summary'][:200]}" if insights.get("has_data") else "")

    # Pass 1: Story
    print("  Pass 1: story...")
    story = claude(f"""You are a master narrative writer for a dark history channel called 20th Century Dark.

Topic: "{topic}"
Research: {research}
{perf}

Write the definitive version of this story. Every great 20th Century Dark story has these qualities:

1. THE HIDDEN ANGLE — not the famous version of events. The thing that got buried.
   The decision made in a back room. The detail the official record omits.

2. ONE PRECISE HUMAN MOMENT — not "thousands died" but "the last entry in his diary was a grocery list."
   One specific, verifiable detail so precise it makes the story undeniably real.
   A name. An exact number. An exact date. A specific room. A specific word spoken.

3. THE BETRAYAL OR IRONY — someone trusted who shouldn't have been.
   Someone who profited. Something covered up. The gap between the official story and what happened.

4. THE RECONTEXTUALIZATION — a fact revealed near the end that makes the listener
   reinterpret everything they just heard. The ground shifts.

5. RESTRAINT — never editorialize. Never say "shockingly" or "horrifyingly."
   State facts plainly. The facts are disturbing enough.
   Trust the listener to feel it.

Structure:
- First sentence: the most disturbing fact. No warmup. Drop the listener mid-story.
- Middle: build through specific details. Each sentence raises stakes slightly.
- End: one short sentence. The kind that stays with you for days.

No questions. Statements only. No headers. No formatting. Raw story only.""", max_tokens=700)

    # Pass 2: Voice
    print("  Pass 2: voice...")
    script_raw = claude(f"""You are rewriting a story for a single human voice speaking directly into someone's ear.

The listener has no prior knowledge of this story. They need to understand it on the first listen, even with a robotic voice reading it.

Original story:
{story}

STRUCTURE — follow this exactly:

SENTENCE 1-2: ORIENTATION (plain English, anyone can follow)
Tell the listener exactly who the main person is and what world they're in.
Example: "In 1961, Israel was hunting the man who organized the Holocaust. His name was Adolf Eichmann. He was hiding in Argentina."
Keep it simple. No assumed knowledge. One fact per sentence.

SENTENCE 3: HOOK
The most disturbing or surprising fact. Short. Lands like a punch.

SENTENCES 4-8: BUILD
Each sentence raises stakes. Specific names, dates, numbers.
Always give context before dropping a name — never assume they know who someone is.

FINAL SENTENCE: THE TWIST
One sentence. Short. Recontextualizes everything. Devastating.

STRICT RULES — every line must follow these:
- Maximum 12 words per sentence. Hard limit.
- One thought per sentence. Never combine two ideas.
- Spell out every abbreviation. Write "the United States" not "the U.S."
- Always introduce a person before using their name. "Hans Globke, Hitler's legal advisor" not just "Globke".
- Use "..." for deliberate pauses where weight needs a moment to land.
- Contractions always: "he didn't" not "he did not". Sounds human.
- Active voice always. "He ordered" not "it was ordered by him."
- Write for a 10th grade reading level. Simple words. Short sentences.
- 130-160 words total. Count carefully.
- Raw script only. No labels. No formatting. No markdown.""", max_tokens=600)

    # Clean markdown
    script = re.sub(r'^#+\s*.*$',   '', script_raw, flags=re.MULTILINE)
    script = re.sub(r'\*\*.*?\*\*', '', script)
    script = re.sub(r'^---+$',       '', script, flags=re.MULTILINE)
    script = script.strip()

    # Pass 3: Metadata
    print("  Pass 3: metadata...")
    meta_raw = claude(f"""Given this spoken script for a dark history YouTube Short:

"{script}"

Topic: "{topic}"

Output ONLY these fields, exactly as shown:

HOOK_WORD: [single word shown alone on black screen — stops the scroll]
VISUAL_STYLE: [one of: oil_painting | cold_war_photo | daguerreotype | illuminated_manuscript | noir_photograph | renaissance_painting | gritty_documentary]
SCENES: [5-6 image generation prompts separated by | — each must be hyper-specific: named person, exact place, exact moment, lighting, era, mood]
BEAT_TIMES: [2-3 emotional peak moments as percentages e.g. 28|55|80]
THUMBNAIL_TEXT: [5-7 words — creates unbearable curiosity, makes viewer feel they must watch]
TITLE: [YouTube title max 70 chars — engineered for clicks, specific, provocative]
TAGS: [10 relevant hashtags separated by |]""", max_tokens=400)

    # Parse metadata
    meta = {}
    for line in meta_raw.split("\n"):
        for key in ("HOOK_WORD:", "VISUAL_STYLE:", "SCENES:", "BEAT_TIMES:",
                    "THUMBNAIL_TEXT:", "TITLE:", "TAGS:"):
            if line.startswith(key):
                meta[key.rstrip(":")] = line[len(key):].strip()
                break

    # Beat times
    beat_times = []
    for b in meta.get("BEAT_TIMES", "25|55|78").split("|"):
        try:
            beat_times.append(float(b.strip()) / 100.0)
        except ValueError:
            pass
    if not beat_times:
        beat_times = [0.25, 0.55, 0.78]

    # Scene prompts
    scene_prompts = [s.strip() for s in meta.get("SCENES", "").split("|") if s.strip()]
    if not scene_prompts:
        scene_prompts = [
            "dark stone hall, torchlight, dramatic shadows",
            "dramatic confrontation, historical figures, candlelight",
            "ominous landscape, storm clouds, ruins",
            "aged document or artifact, candlelight",
            "solitary figure, darkness, final moment",
        ]

    visual_style = meta.get("VISUAL_STYLE", DEFAULT_STYLE).strip()
    if visual_style not in VISUAL_STYLES:
        visual_style = DEFAULT_STYLE

    hook_word  = meta.get("HOOK_WORD", script.split()[0] if script.split() else "LISTEN").upper().strip()
    thumb_text = meta.get("THUMBNAIL_TEXT", topic[:50]).upper()
    title      = meta.get("TITLE", topic[:70])
    tags       = [t.strip().lstrip("#") for t in meta.get("TAGS", "history|shorts|darkhistory").split("|")]

    print(f"Script: {len(script.split())} words | Hook: {hook_word} | Style: {visual_style}")
    print(f"Opening: {script[:100]}...")
    return script, hook_word, scene_prompts, visual_style, beat_times, thumb_text, title, tags


# ── IMAGE GENERATION ──────────────────────────────────────────────────────────
def cosine_similarity(a, b):
    import numpy as np
    a, b = np.array(a), np.array(b)
    n = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / n) if n > 0 else 0.0

def generate_one_image(prompt, style, tmpdir, index):
    if not REPLICATE_API_KEY:
        return None
    style_desc  = VISUAL_STYLES.get(style, VISUAL_STYLES[DEFAULT_STYLE])
    full_prompt = f"{prompt}, {style_desc}"
    try:
        r = requests.post(
            "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
            headers={"Authorization": f"Bearer {REPLICATE_API_KEY}",
                     "Content-Type": "application/json", "Prefer": "wait=60"},
            json={"input": {"prompt": full_prompt, "width": 1080, "height": 1920,
                            "num_outputs": 1, "output_format": "jpg", "output_quality": 90}},
            timeout=120
        )
        if r.status_code not in (200, 201):
            print(f"  Replicate {r.status_code}: {r.text[:150]}")
            return None
        data = r.json()
        if data.get("status") == "succeeded":
            output = data.get("output", [])
        else:
            import time
            pred_id = data.get("id")
            if not pred_id:
                return None
            output = None
            for _ in range(90):
                time.sleep(1)
                poll = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"}, timeout=10
                )
                if poll.status_code == 200:
                    pd = poll.json()
                    if pd.get("status") == "succeeded":
                        output = pd.get("output", [])
                        break
                    if pd.get("status") == "failed":
                        return None
            if output is None:
                return None
        if not output:
            return None
        img_url = output[0] if isinstance(output, list) else output
        img_r   = requests.get(img_url, timeout=30)
        if img_r.status_code != 200:
            return None
        tmp_path = f"{tmpdir}/scene_{index}.jpg"
        with open(tmp_path, "wb") as f:
            f.write(img_r.content)
        # Save to Supabase library
        ts       = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        slug     = "_".join(re.sub(r'[^\w\s]', '', prompt).lower().split()[:8])
        lib_name = f"{style}_{slug}_{ts}_{index}.jpg"
        lib_path = f"{tmpdir}/{lib_name}"
        with open(lib_path, "wb") as f:
            f.write(img_r.content)
        save_image_to_library(lib_path, f"{prompt}, {style.replace('_',' ')}", style)
        print(f"  Scene {index+1}: {len(img_r.content)//1024}KB — {lib_name[:55]}")
        return tmp_path
    except Exception as e:
        print(f"  Image {index} error: {e}")
        return None

def save_image_to_library(image_path, description, style):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    filename = Path(image_path).name
    storage_path = None
    try:
        with open(image_path, "rb") as f:
            data = f.read()
        r = requests.post(
            f"{SUPABASE_URL}/storage/v1/object/images/{filename}",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                     "Content-Type": "image/jpeg"},
            data=data, timeout=30
        )
        if r.status_code in (200, 201):
            storage_path = f"images/{filename}"
    except Exception as e:
        print(f"  Storage upload error: {e}")
    sb_upsert("image_library", {
        "filename": filename, "description": description, "style": style,
        "storage_path": storage_path,
        "created_at": datetime.datetime.utcnow().isoformat(),
    }, on_conflict="filename")

def generate_all_images(scene_prompts, visual_style, tmpdir):
    print(f"Generating {len(scene_prompts)} images via Replicate ({visual_style})...")
    paths = [None] * len(scene_prompts)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(generate_one_image, p, visual_style, tmpdir, i): i
                   for i, p in enumerate(scene_prompts)}
        for fut in concurrent.futures.as_completed(futures):
            i = futures[fut]
            r = fut.result()
            if r:
                paths[i] = r
    valid = [p for p in paths if p]
    print(f"Generated {len(valid)}/{len(scene_prompts)} images")
    return valid

def get_images_from_library(scene_prompts, tmpdir):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/image_library"
            f"?select=filename,description,style,storage_path,embedding"
            f"&storage_path=not.is.null&limit=500",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=15
        )
        rows = r.json() if r.status_code == 200 else []
    except Exception:
        return []
    if not rows:
        return []
    print(f"  Library: {len(rows)} images — matching {len(scene_prompts)} scenes")

    parsed_vecs = {}
    for row in rows:
        emb = row.get("embedding")
        if emb:
            try:
                parsed_vecs[row["filename"]] = (
                    json.loads(emb) if isinstance(emb, str) else emb
                )
            except Exception:
                pass

    if parsed_vecs:
        print(f"  Using vector similarity ({len(parsed_vecs)} embeddings available)")

    paths = []
    used  = set()
    for prompt in scene_prompts:
        best_row   = None
        best_score = -1
        for row in rows:
            if row["filename"] in used:
                continue
            if parsed_vecs and row["filename"] in parsed_vecs:
                prompt_words = set(re.sub(r'[^\w\s]', '', prompt).lower().split())
                desc_words   = set(re.sub(r'[^\w\s]', '', row["description"]).lower().split())
                score        = len(prompt_words & desc_words)
            else:
                prompt_words = set(re.sub(r'[^\w\s]', '', prompt).lower().split())
                desc_words   = set(re.sub(r'[^\w\s]', '', row["description"]).lower().split())
                score        = len(prompt_words & desc_words)
            if score > best_score:
                best_score = score
                best_row   = row

        if not best_row:
            continue
        used.add(best_row["filename"])
        url = f"{SUPABASE_URL}/storage/v1/object/public/{best_row['storage_path']}"
        print(f"  '{prompt[:35]}' → {best_row['filename'][:45]} ({best_score:.2f})")
        try:
            dl = requests.get(url, timeout=30)
            if dl.status_code == 200:
                local = f"{tmpdir}/lib_{len(paths)}.jpg"
                with open(local, "wb") as f:
                    f.write(dl.content)
                paths.append(local)
        except Exception as e:
            print(f"  Download error: {e}")
    print(f"  Library matched {len(paths)}/{len(scene_prompts)} scenes")
    return paths


# ── VOICEOVER ─────────────────────────────────────────────────────────────────
def generate_voiceover(script, audio_path):
    print("Generating voiceover...")
    r = requests.post(
        f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}/with-timestamps",
        headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
        json={"text": script, "model_id": "eleven_monolingual_v1",
              "voice_settings": {
                  "stability":        0.50,
                  "similarity_boost": 0.88,
                  "style":            0.15,
                  "use_speaker_boost": True
              }},
        timeout=60
    )
    if r.status_code != 200:
        raise RuntimeError(f"ElevenLabs {r.status_code}: {r.text[:200]}")
    data = r.json()
    if not data.get("audio_base64"):
        raise RuntimeError("ElevenLabs returned no audio")
    with open(audio_path, "wb") as f:
        f.write(base64.b64decode(data["audio_base64"]))

    # Parse word timings
    alignment = data.get("alignment", {})
    chars     = alignment.get("characters", [])
    starts    = alignment.get("character_start_times_seconds", [])
    ends      = alignment.get("character_end_times_seconds", [])

    if chars and starts:
        word_timings = []
        cur_word, word_start, prev_end = "", None, 0.0
        for ch, ts, te in zip(chars, starts, ends):
            if ch in " \n\t":
                if cur_word:
                    word_timings.append((cur_word, word_start, prev_end))
                    cur_word, word_start = "", None
            else:
                if word_start is None:
                    word_start = ts
                cur_word += ch
                prev_end  = te
        if cur_word:
            word_timings.append((cur_word, word_start, prev_end))
        print(f"Voiceover: {len(word_timings)} words synced")
    else:
        import wave, contextlib
        with contextlib.closing(wave.open(audio_path, 'r')) as wf:
            duration = wf.getnframes() / wf.getframerate()
        words = script.split()
        step  = duration / max(len(words), 1)
        word_timings = [(w, i*step, (i+1)*step) for i, w in enumerate(words)]
        print("Voiceover: even distribution fallback")
    return word_timings


# ── UPLOAD AUDIO TO SUPABASE ──────────────────────────────────────────────────
def upload_audio(audio_path, job_id):
    """Upload voiceover MP3 to Supabase Storage audio bucket."""
    filename = f"job_{job_id}.mp3"
    with open(audio_path, "rb") as f:
        data = f.read()
    r = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/audio/{filename}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                 "Content-Type": "audio/mpeg"},
        data=data, timeout=30
    )
    if r.status_code in (200, 201):
        return f"audio/{filename}"
    print(f"  Audio upload failed {r.status_code}")
    return None


# ── SAVE JOB ──────────────────────────────────────────────────────────────────
def save_job(job_id, topic, title, script, hook_word, scene_prompts, visual_style,
             beat_times, thumb_text, tags, word_timings, image_paths, audio_storage_path):
    """
    Save all job data to Supabase jobs table.
    Local assembler picks this up and renders the video.
    image_paths are Supabase storage paths (not local paths).
    """
    job = {
        "id":               job_id,
        "topic":            topic,
        "title":            title,
        "script":           script,
        "hook_word":        hook_word,
        "scene_prompts":    json.dumps(scene_prompts),
        "visual_style":     visual_style,
        "beat_times":       json.dumps(beat_times),
        "thumb_text":       thumb_text,
        "tags":             json.dumps(tags),
        "word_timings":     json.dumps(word_timings),
        "image_paths":      json.dumps(image_paths),
        "audio_path":       audio_storage_path,
        "status":           "pending",
        "created_at":       datetime.datetime.utcnow().isoformat(),
    }
    sb_upsert("jobs", job, on_conflict="id")
    print(f"Job saved: {job_id}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    print(f"\n{'═'*60}\n20th Century Dark — Brain — {now}\n{'═'*60}\n")

    with tempfile.TemporaryDirectory() as tmpdir:
        audio_path = f"{tmpdir}/voice.mp3"

        # Phase 1: Learn
        token    = get_youtube_token()
        ana_data = pull_analytics(token)
        insights = analyze_performance(ana_data)

        # Phase 2: Create
        topic    = generate_topic(insights)
        research = research_topic(topic, insights)
        (script, hook_word, scene_prompts, visual_style,
         beat_times, thumb_text, title, tags) = write_script(topic, research, insights)

        # Phase 3: Generate assets in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            img_fut   = ex.submit(generate_all_images, scene_prompts, visual_style, tmpdir)
            voice_fut = ex.submit(generate_voiceover, script, audio_path)
            image_paths  = img_fut.result()
            word_timings = voice_fut.result()

        # Top up images from library
        target = len(scene_prompts)
        if len(image_paths) < target:
            print(f"Replicate got {len(image_paths)}/{target} — pulling from library")
            lib_imgs = get_images_from_library(scene_prompts, tmpdir)
            for lp in lib_imgs:
                if len(image_paths) >= target:
                    break
                if lp not in image_paths:
                    image_paths.append(lp)

        # Upload audio to Supabase
        job_id            = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        audio_storage_path = upload_audio(audio_path, job_id)

        # Upload matched images to Supabase (they're local right now)
        image_storage_paths = []
        for i, img_path in enumerate(image_paths):
            fname = f"job_{job_id}_scene_{i}.jpg"
            with open(img_path, "rb") as f:
                data = f.read()
            r = requests.post(
                f"{SUPABASE_URL}/storage/v1/object/images/{fname}",
                headers={"apikey": SUPABASE_KEY,
                         "Authorization": f"Bearer {SUPABASE_KEY}",
                         "Content-Type": "image/jpeg"},
                data=data, timeout=30
            )
            if r.status_code in (200, 201):
                image_storage_paths.append(f"images/{fname}")
            else:
                print(f"  Image {i} upload failed")

        # Save job for local assembler
        save_job(job_id, topic, title, script, hook_word, scene_prompts,
                 visual_style, beat_times, thumb_text, tags,
                 word_timings, image_storage_paths, audio_storage_path)

        print(f"\n{'═'*60}")
        print(f"Job {job_id} ready for assembly")
        print(f"Topic: {topic[:70]}")
        print(f"Title: {title}")
        print(f"Images: {len(image_storage_paths)} uploaded")
        print(f"Audio: {audio_storage_path}")
        print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
