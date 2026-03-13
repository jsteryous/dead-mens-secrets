#!/usr/bin/env python3
"""
Dead Men's Secrets — Bulk Image Library Builder

Generates 200 highly specific historical images via Replicate Flux
and saves them directly to Supabase Storage + image_library table.

Run once to seed the library. Each image costs ~$0.003.
Total cost: ~$0.60 for all 200 images.

Usage:
    Set environment variables, then:
    python3 bulk_generate_images.py

    Or run on Railway as a one-off job (set START_CMD to this file temporarily).

Progress is saved — if interrupted, re-running skips already-generated images.
"""

import os, re, json, time, datetime, requests, concurrent.futures
from pathlib import Path

REPLICATE_API_KEY = os.environ.get("REPLICATE_API_KEY")
SUPABASE_URL      = os.environ.get("SUPABASE_URL")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY")

W, H = 1080, 1920

VISUAL_STYLES = {
    "oil_painting":           "dark oil painting style, dramatic chiaroscuro lighting, cinematic, rich shadows, historically detailed, masterpiece quality, moody atmosphere",
    "cold_war_photo":         "gritty cold war era photography, grainy black and white, surveillance aesthetic, stark contrast, Soviet brutalist architecture, documentary style",
    "daguerreotype":          "Victorian daguerreotype photograph style, sepia tones, aged, formal composition, 19th century aesthetic, antique photograph",
    "illuminated_manuscript": "medieval illuminated manuscript style, rich gold leaf, intricate borders, gothic lettering, candlelit parchment, dark ages aesthetic",
    "noir_photograph":        "1940s film noir photography, deep shadows, venetian blind light, cigarette smoke, black and white, expressionist angles",
    "renaissance_painting":   "Renaissance oil painting style, chiaroscuro, dramatic religious lighting, classical composition, Caravaggio influence, rich jewel tones",
    "gritty_documentary":     "gritty documentary photography, raw, unflinching, high contrast, photojournalism style, harsh flash lighting, modern realism",
}

# ── 200 HIGHLY SPECIFIC PROMPTS ───────────────────────────────────────────────
# Format: (description, style)
# Description = exactly what the image should show. Specific person, place,
# moment, lighting, era. The filename becomes the semantic search key.
# Err toward over-specificity — "German SS officer oak desk Berlin 1942"
# beats "Nazi soldier" every time for semantic matching.

PROMPTS = [
    # ── WWII / NAZI GERMANY ──────────────────────────────────────────────────
    ("German SS officer signing execution orders at oak desk Berlin 1942 single lamp", "cold_war_photo"),
    ("Adolf Hitler alone at window of Berghof mountain retreat 1939 contemplating", "gritty_documentary"),
    ("Nuremberg war crimes trial defendants in dock 1945 guards watching", "gritty_documentary"),
    ("Jewish prisoners in striped uniforms assembled at Auschwitz gate dawn roll call", "gritty_documentary"),
    ("Nazi propaganda minister at podium Berlin rally torchlight crowd shadows", "cold_war_photo"),
    ("Heinrich Himmler inspecting concentration camp guards forest 1942", "gritty_documentary"),
    ("Allied soldiers discovering mass grave forest Eastern Europe 1945 horror", "gritty_documentary"),
    ("German U-boat commander peering through periscope dark Atlantic 1942", "oil_painting"),
    ("Gestapo agents arresting Jewish family apartment Berlin night 1938", "noir_photograph"),
    ("Eva Braun alone in bunker corridor Berlin April 1945 waiting", "noir_photograph"),
    ("Hiroshima atomic bomb mushroom cloud viewed from distance August 1945", "gritty_documentary"),
    ("Japanese kamikaze pilots receiving final orders airfield Pacific 1944", "gritty_documentary"),
    ("D-Day soldiers storming Normandy beach under fire June 1944 carnage", "gritty_documentary"),
    ("Holocaust survivor liberation Dachau skeletal figure barbed wire 1945", "gritty_documentary"),
    ("Mengele medical experiment prisoner strapped to table Auschwitz 1943", "noir_photograph"),

    # ── SOVIET / COLD WAR ────────────────────────────────────────────────────
    ("Stalin reviewing execution list by lamplight Moscow office midnight 1937", "cold_war_photo"),
    ("Soviet secret police NKVD agents dragging man from apartment Leningrad 1936", "cold_war_photo"),
    ("Gulag prisoners marching through Siberian blizzard armed guards 1950", "cold_war_photo"),
    ("KGB interrogation room single hanging bulb suspect strapped to chair Moscow", "noir_photograph"),
    ("Beria secret police chief Lubyanka prison corridor with folder 1940s", "cold_war_photo"),
    ("Soviet nuclear test explosion Kazakhstan desert 1949 observers watching", "gritty_documentary"),
    ("CIA spy photographing documents with hidden camera East Berlin 1960", "noir_photograph"),
    ("Berlin Wall construction East German soldiers laying barbed wire August 1961", "cold_war_photo"),
    ("Defector crossing frozen river night border guards dogs 1962", "cold_war_photo"),
    ("Cuban Missile Crisis Kennedy alone Oval Office October 1962 documents", "gritty_documentary"),
    ("Chernobyl reactor building exploding night sky Ukraine April 1986", "gritty_documentary"),
    ("Soviet dissident in Siberian prison cell writing banned manuscript 1970s", "cold_war_photo"),
    ("Rosenbergs execution electric chair Sing Sing Prison 1953 guards", "noir_photograph"),
    ("McCarthy hearing Communist accused witness Washington 1953 pointing", "gritty_documentary"),
    ("Trotsky assassination ice pick Mexico City study 1940 blood", "noir_photograph"),

    # ── ANCIENT ROME ─────────────────────────────────────────────────────────
    ("Julius Caesar stabbed in Roman Senate marble floor blood toga 44 BC", "renaissance_painting"),
    ("Gladiator kneeling in Colosseum sand crowd Emperor thumb down Rome", "oil_painting"),
    ("Roman Emperor Caligula on throne surrounded by terrified senators gold light", "renaissance_painting"),
    ("Roman crucifixion on hill outside Jerusalem crowds watching dusk", "renaissance_painting"),
    ("Poisoning of Roman Emperor Claudius feast table wife Agrippina watching", "renaissance_painting"),
    ("Nero watching Rome burn from palace balcony 64 AD flames sky", "oil_painting"),
    ("Roman legionaries marching through conquered Gaul burning village 52 BC", "oil_painting"),
    ("Cleopatra entering Rome triumphant procession crowds Caesar watching", "renaissance_painting"),
    ("Roman slave market chained prisoners buyers inspecting Ostia 1st century", "oil_painting"),
    ("Vestal Virgin breaking vow discovered condemned buried alive Rome", "renaissance_painting"),
    ("Spartacus crucified slave revolt leader Roman road Appian Way 71 BC", "oil_painting"),
    ("Pompeii eruption Mount Vesuvius citizens fleeing ash cloud 79 AD", "oil_painting"),
    ("Roman torture chamber Palatine Hill prisoners interrogation torchlight", "oil_painting"),
    ("Marcus Aurelius dying in military camp Danube frontier 180 AD soldiers", "renaissance_painting"),
    ("Roman Emperor Domitian assassination bedroom guards conspirators 96 AD", "renaissance_painting"),

    # ── MEDIEVAL EUROPE ──────────────────────────────────────────────────────
    ("Medieval inquisition tribunal condemning heretic torch stake crowd", "illuminated_manuscript"),
    ("Joan of Arc burning at stake Rouen marketplace crowd 1431 flames", "oil_painting"),
    ("Black Death plague cart London bodies stacked at night 1348", "illuminated_manuscript"),
    ("Medieval dungeon iron maiden torture device stone walls torchlight", "oil_painting"),
    ("Crusader knights massacring Jerusalem civilians 1099 blood streets", "illuminated_manuscript"),
    ("King Henry VIII signing Anne Boleyn execution warrant quill 1536", "oil_painting"),
    ("Tower of London prisoner writing last letter cell candle 1483", "oil_painting"),
    ("Medieval hanging public execution town square crowd jeering 14th century", "oil_painting"),
    ("Vlad the Impaler feasting surrounded by impaled bodies forest Wallachia", "oil_painting"),
    ("Witchcraft trial Salem courthouse accused woman pointing crowd 1692", "daguerreotype"),
    ("Borgias poisoning rival cardinal dinner table Venice 15th century", "renaissance_painting"),
    ("Richard III ordering murder of Princes Tower London guards 1483", "oil_painting"),
    ("Bubonic plague doctor beak mask examining dying patient 1348 Paris", "illuminated_manuscript"),
    ("Medieval executioner with axe prisoner kneeling block castle courtyard", "oil_painting"),
    ("Catherine de Medici ordering St Bartholomew's Day Massacre Paris 1572", "renaissance_painting"),

    # ── VICTORIAN / 19TH CENTURY ─────────────────────────────────────────────
    ("Jack the Ripper victim Whitechapel alley London gas lamp fog 1888", "daguerreotype"),
    ("Victorian asylum patient strapped to chair doctors observing London 1880s", "daguerreotype"),
    ("Opium den London East End Victorian addicts pipes dim lamp 1870s", "daguerreotype"),
    ("Child labor Victorian coal mine boy underground shaft candle 1840", "daguerreotype"),
    ("Public hanging Newgate Prison London crowd cheering gallows 1860", "daguerreotype"),
    ("Victorian grave robber medical school dissection body night 1820", "daguerreotype"),
    ("Burke and Hare body snatchers selling corpse Edinburgh anatomy school", "daguerreotype"),
    ("Elephant Man Joseph Merrick hospital bed Victorian London 1887", "daguerreotype"),
    ("Victorian serial killer H H Holmes murder hotel Chicago 1893 blueprint", "daguerreotype"),
    ("Lincoln assassination Ford's Theatre box seat Booth pistol 1865", "daguerreotype"),
    ("Donner Party survivors cannibalism Sierra Nevada snowstorm 1847", "daguerreotype"),
    ("American Civil War field hospital amputations surgeon blood 1863", "daguerreotype"),
    ("Triangle Shirtwaist Factory fire women jumping windows New York 1911", "daguerreotype"),
    ("Titanic sinking passengers on tilting deck lifeboats night 1912", "daguerreotype"),
    ("Lizzie Borden axe murder parents Fall River Massachusetts 1892 house", "daguerreotype"),

    # ── ANCIENT WORLD ────────────────────────────────────────────────────────
    ("Ancient Egyptian pharaoh ordering slave construction pyramids desert heat", "oil_painting"),
    ("Cleopatra holding asp cobra to breast Alexandria palace suicide 30 BC", "renaissance_painting"),
    ("Carthage burning Roman soldiers destroying city 146 BC flames smoke", "oil_painting"),
    ("Alexander the Great weeping at Babylon death bed fever 323 BC generals", "renaissance_painting"),
    ("Socrates drinking hemlock prison cell disciples watching Athens 399 BC", "renaissance_painting"),
    ("Human sacrifice Aztec pyramid heart removal priest sun ceremony", "oil_painting"),
    ("Mongol horde burning Baghdad massacre Tigris River red 1258 AD", "oil_painting"),
    ("Genghis Khan ordering execution of city population Central Asia 1220", "oil_painting"),
    ("Ancient Roman arena Christians fed to lions crowd Emperor 100 AD", "renaissance_painting"),
    ("Hannibal crossing Alps with war elephants snow soldiers dying", "oil_painting"),
    ("Caligula feeding prisoners crocodiles Roman palace garden 38 AD", "renaissance_painting"),
    ("Persian King Xerxes ordering Thermopylae troops forward 480 BC", "oil_painting"),
    ("Greek philosopher Hypatia dragged from chariot by mob Alexandria 415 AD", "renaissance_painting"),
    ("Nero poisoning half-brother Britannicus feast Rome 55 AD wine cup", "renaissance_painting"),
    ("Roman orgy debauchery Caligula palace gold torchlight senators", "renaissance_painting"),

    # ── ESPIONAGE / ASSASSINATIONS ───────────────────────────────────────────
    ("JFK assassination Dallas motorcade Grassy Knoll crowd 1963 moment of shot", "gritty_documentary"),
    ("Robert Kennedy shot Ambassador Hotel kitchen floor Los Angeles 1968", "gritty_documentary"),
    ("Martin Luther King balcony Lorraine Motel shot Memphis 1968", "gritty_documentary"),
    ("Archduke Franz Ferdinand assassination Sarajevo open car 1914", "daguerreotype"),
    ("Leon Trotsky desk Mexico City moments before assassination 1940", "noir_photograph"),
    ("Spy exchanged Glienicke Bridge Berlin fog both sides waiting 1962", "cold_war_photo"),
    ("Mata Hari blindfolded execution post French soldiers rifles 1917", "daguerreotype"),
    ("Agent dropped by parachute occupied France dark field 1943", "noir_photograph"),
    ("Double agent meeting handler Vienna cafe corner table documents 1955", "noir_photograph"),
    ("Umbrella assassination Bulgarian defector Georgi Markov London bridge 1978", "gritty_documentary"),
    ("Kim Jong-nam poisoned Kuala Lumpur airport CCTV surveillance 2017", "gritty_documentary"),
    ("Polonium poisoning Alexander Litvinenko hospital London deathbed 2006", "gritty_documentary"),
    ("CIA black site interrogation hooded prisoner undisclosed location 2002", "gritty_documentary"),
    ("Mossad agents monitoring target hotel Jerusalem operation 1972", "cold_war_photo"),
    ("Watergate burglars arrested Democratic headquarters Washington 1972", "gritty_documentary"),

    # ── CULTS / MASS EVENTS ──────────────────────────────────────────────────
    ("Jonestown mass suicide bodies field Guyana jungle 1978 aftermath", "gritty_documentary"),
    ("Jim Jones preaching Peoples Temple congregation San Francisco 1977", "gritty_documentary"),
    ("Heaven's Gate cult members bodies discovered Rancho Santa Fe 1997", "gritty_documentary"),
    ("Charles Manson trial defendant laughing courthouse 1970 swastika forehead", "gritty_documentary"),
    ("Branch Davidian Waco compound burning Texas April 1993 smoke", "gritty_documentary"),
    ("Aum Shinrikyo Tokyo subway sarin attack victims sprawled 1995", "gritty_documentary"),
    ("Ku Klux Klan burning cross field night robes gathered 1920s", "daguerreotype"),
    ("Witch hunter Matthew Hopkins condemning women England 1645 pointing", "oil_painting"),
    ("Salem witch hanging Gallows Hill crowd Puritan minister 1692", "daguerreotype"),
    ("Taiping Rebellion massacre Shanghai bodies river China 1860s", "daguerreotype"),

    # ── PRISONS / EXECUTIONS ─────────────────────────────────────────────────
    ("Electric chair execution Sing Sing Prison guard strapping condemned 1930s", "noir_photograph"),
    ("Gas chamber execution prisoner face pressed glass California 1960s", "gritty_documentary"),
    ("Alcatraz solitary confinement cell prisoner alone darkness 1940s", "noir_photograph"),
    ("Death row inmates last meal prison cell Texas modern", "gritty_documentary"),
    ("French guillotine execution public Paris crowd 1793 Revolution", "oil_painting"),
    ("Prisoner in stocks public humiliation Puritan New England town square 1670", "daguerreotype"),
    ("Siberian gulag prisoner in punishment cell ice walls Stalin era", "cold_war_photo"),
    ("Hanged man discovered prison cell morning guard shock 1920s", "noir_photograph"),
    ("Firing squad execution blindfolded prisoner wall dawn 1918", "daguerreotype"),
    ("Condemned man walking to gallows prison courtyard chaplain 1900s", "daguerreotype"),

    # ── SCIENCE / EXPERIMENTS ────────────────────────────────────────────────
    ("Tuskegee syphilis study doctor injecting unknowing Black patient 1940s", "gritty_documentary"),
    ("Unit 731 Japanese biological warfare laboratory Manchuria 1940 prisoners", "cold_war_photo"),
    ("MK-Ultra CIA mind control experiment subject strapped chair 1950s", "noir_photograph"),
    ("Nazi human experiment hypothermia test prisoner ice water Dachau 1942", "cold_war_photo"),
    ("Lobotomy operation patient Walter Freeman ice pick eye socket 1940s", "daguerreotype"),
    ("Electric shock therapy psychiatric patient 1950s hospital strapped table", "noir_photograph"),
    ("Radium factory girls painting watch dials dying glow New Jersey 1922", "daguerreotype"),
    ("Thalidomide child deformed limbless mother hospital Germany 1960", "gritty_documentary"),
    ("Marie Curie alone in laboratory radiation glowing vials night 1900", "daguerreotype"),
    ("Stanford Prison Experiment guard abusing prisoner basement 1971", "gritty_documentary"),

    # ── BETRAYALS / POLITICAL INTRIGUE ───────────────────────────────────────
    ("Judas receiving thirty pieces of silver high priests night Jerusalem", "renaissance_painting"),
    ("Thomas Becket murdered altar Canterbury Cathedral knights swords 1170", "illuminated_manuscript"),
    ("Macbeth daggers bloody hands bedchamber murdered king Shakespeare", "oil_painting"),
    ("Cardinal Richelieu whispering to French King Louis XIII plotting", "renaissance_painting"),
    ("Rasputin assassination Yusupov palace basement Petersburg 1916", "daguerreotype"),
    ("Benedict Arnold meeting British spy night boat Hudson River 1780", "oil_painting"),
    ("Guy Fawkes discovered gunpowder Parliament cellar London 1605", "oil_painting"),
    ("Brutus weeping with bloody dagger after Caesar assassination Rome", "renaissance_painting"),
    ("Mary Queen of Scots receiving death warrant Fotheringhay Castle 1587", "oil_painting"),
    ("Anne Boleyn Tower of London cell night before execution candle 1536", "oil_painting"),

    # ── WAR ATROCITIES ───────────────────────────────────────────────────────
    ("My Lai massacre US soldiers Vietnamese village 1968 bodies road", "gritty_documentary"),
    ("Rwandan genocide machete bodies church Nyamata 1994", "gritty_documentary"),
    ("Nanking massacre Japanese soldiers Chinese civilians 1937 river", "gritty_documentary"),
    ("Srebrenica massacre Bosnian men blindfolded lined forest 1995", "gritty_documentary"),
    ("Armenian genocide deportation march desert dying columns 1915", "daguerreotype"),
    ("Stalin forced collectivization Ukrainian famine children dying 1932", "cold_war_photo"),
    ("Pol Pot Khmer Rouge mass grave Cambodia S21 prison 1977", "gritty_documentary"),
    ("Trench warfare WWI soldiers going over top barbed wire gas masks 1917", "daguerreotype"),
    ("Firebombing Dresden burning church civilians fleeing 1945", "gritty_documentary"),
    ("Atomic bomb Nagasaki survivor burns hospital Japan August 1945", "gritty_documentary"),

    # ── DARK PORTRAITS / ISOLATED FIGURES ────────────────────────────────────
    ("Solitary figure standing at edge of abyss cliff fog night alone", "oil_painting"),
    ("Hooded executioner carrying axe castle tower dawn mist", "oil_painting"),
    ("King alone on throne empty dark hall crown heavy burden", "renaissance_painting"),
    ("Prisoner hands pressed against iron prison door darkness only candle", "oil_painting"),
    ("Doctor holding skull candlelight laboratory night contemplating mortality", "renaissance_painting"),
    ("Old man alone writing confession by dying fire shadow long", "oil_painting"),
    ("Woman discovering body husband home Victorian hallway candle horror", "daguerreotype"),
    ("Spy burning documents fireplace apartment before escape Berlin night", "noir_photograph"),
    ("Condemned man alone in cell writing last letter dawn execution day", "oil_painting"),
    ("Tyrant king alone after ordering massacre moonlit balcony guilt", "renaissance_painting"),

    # ── DOCUMENTS / ARTIFACTS ────────────────────────────────────────────────
    ("Ancient parchment death warrant seal quill ink bloodstained", "illuminated_manuscript"),
    ("Classified document stamped top secret eyes only hands holding", "noir_photograph"),
    ("Confession letter stained blood Victorian writing desk candle", "daguerreotype"),
    ("Poison bottle antique glass skull crossbones label candlelight", "daguerreotype"),
    ("Ransom note letters cut from newspaper table lamp noir", "noir_photograph"),
    ("Execution order stamped approved Nazi letterhead 1942 desk", "cold_war_photo"),
    ("Evidence photograph crime scene black white detective 1940s table", "noir_photograph"),
    ("Forged identity papers WWII resistance underground printing press", "noir_photograph"),
    ("Stolen nuclear secrets microfilm Soviet agent dark room 1953", "cold_war_photo"),
    ("Last will testament dying Victorian aristocrat servants gathered", "daguerreotype"),

    # ── AFTERMATH / RUINS ────────────────────────────────────────────────────
    ("Bombed Dresden ruins cathedral skeleton walls 1945 survivors walking", "gritty_documentary"),
    ("Hiroshima shadows burned into steps stone August 1945", "gritty_documentary"),
    ("Auschwitz abandoned barracks shoes pile liberation 1945", "gritty_documentary"),
    ("Chernobyl abandoned classroom Pripyat gas masks desks 1986", "gritty_documentary"),
    ("Pompeii plaster cast body preserved ash Vesuvius victim", "daguerreotype"),
    ("Colosseum Rome interior dusk ruins gladiator history weight", "oil_painting"),
    ("Abandoned gulag watchtower Siberia collapsed barbed wire", "cold_war_photo"),
    ("Mass grave excavation forensic anthropologists bones field", "gritty_documentary"),
    ("Concentration camp gate arbeit macht frei fog dawn", "gritty_documentary"),
    ("Berlin bunker Hitler final days underground corridor guards", "cold_war_photo"),
]

# ── SUPABASE HELPERS ──────────────────────────────────────────────────────────

def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def get_indexed_filenames():
    """Return set of filenames already in image_library."""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/image_library",
        headers={**sb_headers(), "Prefer": "return=representation"},
        params={"select": "filename", "limit": "1000"},
        timeout=15
    )
    if r.status_code == 200:
        return {row["filename"] for row in r.json()}
    return set()

def upload_to_storage(image_path, filename):
    """Upload image file to Supabase Storage images/ bucket."""
    with open(image_path, "rb") as f:
        data = f.read()
    r = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/images/{filename}",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "image/jpeg",
        },
        data=data,
        timeout=30
    )
    return r.status_code in (200, 201)

def get_embedding(text):
    """Get 384-dim embedding. Returns pgvector literal string or None."""
    try:
        from sentence_transformers import SentenceTransformer
        global _model
        if not hasattr(get_embedding, '_model') or get_embedding._model is None:
            get_embedding._model = SentenceTransformer("all-MiniLM-L6-v2")
        vec = get_embedding._model.encode(text).tolist()
        return f"[{','.join(str(round(v,6)) for v in vec)}]"
    except Exception as e:
        print(f"  Embedding error: {e}")
        return None

def upsert_image_record(filename, description, style, storage_ok):
    """Insert image metadata + embedding into image_library."""
    storage_path = f"images/{filename}" if storage_ok else None
    vec          = get_embedding(description)
    row = {
        "filename":     filename,
        "description":  description,
        "style":        style,
        "storage_path": storage_path,
        "created_at":   datetime.datetime.utcnow().isoformat(),
    }
    if vec:
        row["embedding"] = vec

    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/image_library",
        headers={**sb_headers(), "Prefer": "resolution=merge-duplicates"},
        params={"on_conflict": "filename"},
        json=row,
        timeout=15
    )
    return r.status_code in (200, 201)


# ── REPLICATE IMAGE GENERATION ────────────────────────────────────────────────

def generate_image(description, style, tmpdir, idx):
    """
    Generate one image via Replicate Flux Schnell.
    Returns local path on success, None on failure.
    """
    style_desc  = VISUAL_STYLES[style]
    full_prompt = f"{description}, {style_desc}"

    try:
        r = requests.post(
            "https://api.replicate.com/v1/models/black-forest-labs/flux-schnell/predictions",
            headers={
                "Authorization": f"Bearer {REPLICATE_API_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "wait=60",
            },
            json={"input": {
                "prompt":         full_prompt,
                "width":          W,
                "height":         H,
                "num_outputs":    1,
                "output_format":  "jpg",
                "output_quality": 90,
            }},
            timeout=120
        )

        if r.status_code not in (200, 201):
            print(f"  [{idx}] Replicate {r.status_code}: {r.text[:100]}")
            return None

        data = r.json()

        if data.get("status") == "succeeded":
            output = data.get("output", [])
        else:
            pred_id = data.get("id")
            if not pred_id:
                return None
            output = None
            for _ in range(90):
                time.sleep(1)
                poll = requests.get(
                    f"https://api.replicate.com/v1/predictions/{pred_id}",
                    headers={"Authorization": f"Bearer {REPLICATE_API_KEY}"},
                    timeout=10
                )
                if poll.status_code == 200:
                    pd = poll.json()
                    if pd.get("status") == "succeeded":
                        output = pd.get("output", [])
                        break
                    if pd.get("status") == "failed":
                        return None
            if not output:
                return None

        img_url = output[0] if isinstance(output, list) else output
        img_r   = requests.get(img_url, timeout=30)
        if img_r.status_code != 200:
            return None

        path = f"{tmpdir}/bulk_{idx}.jpg"
        with open(path, "wb") as f:
            f.write(img_r.content)
        return path

    except Exception as e:
        print(f"  [{idx}] Error: {e}")
        return None


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    import tempfile

    print(f"\n{'═'*60}")
    print(f"Dead Men's Secrets — Bulk Image Generator")
    print(f"{len(PROMPTS)} images @ ~$0.003 each = ~${len(PROMPTS)*0.003:.2f} total")
    print(f"{'═'*60}\n")

    if not REPLICATE_API_KEY:
        print("ERROR: REPLICATE_API_KEY not set")
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL or SUPABASE_KEY not set")
        return

    # Skip already-indexed images
    already_done = get_indexed_filenames()
    print(f"Already in library: {len(already_done)} images")

    # Build work list — skip anything already done
    work = []
    for i, (desc, style) in enumerate(PROMPTS):
        slug     = re.sub(r'[^\w\s]', '', desc).lower().split()
        filename = f"{style}_{'_'.join(slug[:10])}_{i:03d}.jpg"
        if filename not in already_done:
            work.append((i, desc, style, filename))

    print(f"To generate: {len(work)} images\n")

    if not work:
        print("Library already complete!")
        return

    success = 0
    fail    = 0

    with tempfile.TemporaryDirectory() as tmpdir:
        # Process in batches of 5 parallel workers
        # Replicate handles concurrent requests well; 5 is safe
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            futures = {
                ex.submit(generate_image, desc, style, tmpdir, i): (i, desc, style, filename)
                for i, desc, style, filename in work
            }

            for fut in concurrent.futures.as_completed(futures):
                i, desc, style, filename = futures[fut]
                img_path = fut.result()

                if not img_path:
                    print(f"  ✗ [{i:03d}] FAILED: {desc[:50]}")
                    fail += 1
                    continue

                # Upload to Supabase Storage
                storage_ok = upload_to_storage(img_path, filename)

                # Index with embedding
                record_ok = upsert_image_record(filename, desc, style, storage_ok)

                status = "✓" if (storage_ok and record_ok) else "~"
                print(f"  {status} [{i:03d}] {desc[:55]}")
                success += 1

    print(f"\n{'═'*60}")
    print(f"Complete: {success} generated, {fail} failed")
    print(f"Library now has {len(already_done) + success} images")
    print(f"Cost: ~${success * 0.003:.2f}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()