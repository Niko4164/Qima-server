"""
QIMA Life Sciences — LatAm Trials Server v3.1
Architecture simplifiée : cache mémoire + sync à la demande
"""
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import httpx, asyncio, re, os, json
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════
# APP
# ══════════════════════════════════════════════
app = FastAPI(title="QIMA LatAm Trials API", version="3.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ══════════════════════════════════════════════
# CACHE MÉMOIRE (pas de SQLite — plus simple)
# ══════════════════════════════════════════════
CACHE = {
    "studies":    [],
    "updated_at": None,
    "is_syncing": False,
    "total":      0,
    "be_total":   0,
}
CACHE_TTL_HOURS = 12

def cache_is_fresh():
    if not CACHE["updated_at"]:
        return False
    age = datetime.now() - CACHE["updated_at"]
    return age < timedelta(hours=CACHE_TTL_HOURS)

# ══════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════
LATAM = [
    "Brazil", "Argentina", "Mexico", "Chile",
    "Colombia", "Peru", "Uruguay", "Bolivia",
    "Paraguay", "Ecuador",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; QIMA-Bot/3.1)",
    "Accept":     "text/html,application/json,*/*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

def client(timeout=30):
    return httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=True,
        headers=HEADERS,
    )

# ══════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════
BE_PATTERN = re.compile(
    r"bioequivalen|bioavailabilit|biodisponibilid|"
    r"pharmacokineti|farmacocineti|ba/be|"
    r"equival.ncia farmac", re.I
)

def is_be(text):
    return bool(BE_PATTERN.search(text or ""))

EU_PATTERN = re.compile(
    r"roche|novartis|bayer|astrazeneca|sanofi|gsk|ucb|"
    r"boehringer|servier|ipsen|chiesi|recordati|lundbeck|"
    r"novo nordisk|ferring|leo pharma|GmbH|N\.V\.|S\.r\.l", re.I
)

CRO_MAP = [
    (re.compile(r"IQVIA|Quintiles", re.I),         "IQVIA"),
    (re.compile(r"Syneos|INC Research", re.I),      "Syneos Health"),
    (re.compile(r"ICON plc|ICON Clinical", re.I),   "ICON plc"),
    (re.compile(r"PAREXEL|Parexel", re.I),          "PAREXEL"),
    (re.compile(r"Covance|Labcorp Drug", re.I),     "Labcorp"),
    (re.compile(r"Medpace", re.I),                  "Medpace"),
    (re.compile(r"PPD ", re.I),                     "PPD"),
]

def detect_cro(text):
    for pattern, label in CRO_MAP:
        if pattern.search(text or ""):
            return label
    return None

AREA_MAP = [
    ("be",        re.compile(r"bioequivalen|bioavailab|pharmacokineti|ba/be", re.I)),
    ("onco",      re.compile(r"cancer|tumor|oncol|carcinom|lymphoma|leukemia|melanoma|sarcoma|myeloma|neoplasm", re.I)),
    ("cardio",    re.compile(r"cardiovascular|cardiac|heart failure|hypertension|cholesterol|coronary|myocardial", re.I)),
    ("endocrino", re.compile(r"diabetes|insulin|obesity|endocrin|thyroid|metabolic", re.I)),
    ("neuro",     re.compile(r"neurolog|alzheimer|parkinson|sclerosis|epilepsy|dementia|migraine|stroke", re.I)),
    ("psiq",      re.compile(r"psychiatric|depression|anxiety|schizophrenia|bipolar|adhd|autism", re.I)),
    ("gastro",    re.compile(r"gastro|hepatitis|\bliver\b|crohn|colitis|colon|cirrhosis|nafld|nash", re.I)),
    ("resp",      re.compile(r"respiratory|asthma|copd|pulmonary|lung|bronchial|fibrosis", re.I)),
    ("infec",     re.compile(r"hiv|aids|tuberculosis|malaria|dengue|covid|sars|influenza|infection|zika|chagas", re.I)),
    ("vacinas",   re.compile(r"vaccine|vaccination|immunization", re.I)),
]

def classify(title, conditions):
    txt = (title or "") + " " + " ".join(conditions or [])
    for area_id, pat in AREA_MAP:
        if pat.search(txt):
            return area_id
    return "outros"

def norm_status(s):
    s = (s or "").lower()
    if any(x in s for x in ["recruit", "aberto", "ativo", "open"]):       return "RECRUITING"
    if any(x in s for x in ["not yet", "não iniciado", "planned"]):        return "NOT_YET_RECRUITING"
    if any(x in s for x in ["active, not", "fechado"]):                    return "ACTIVE_NOT_RECRUITING"
    if any(x in s for x in ["complet", "concluí", "encerrado"]):           return "COMPLETED"
    if any(x in s for x in ["terminat", "interrompido", "cancelado"]):     return "TERMINATED"
    if any(x in s for x in ["suspend", "suspenso"]):                       return "SUSPENDED"
    return "UNKNOWN"

# ══════════════════════════════════════════════
# SOURCE 1: ClinicalTrials.gov (avec pagination)
# ══════════════════════════════════════════════
async def fetch_ctgov_country(country):
    studies = []
    next_token = None
    page = 0
    print(f"  CTgov: {country}...")
    async with client(40) as c:
        while page < 8:
            params = {"query.locn": country, "pageSize": "1000"}
            if next_token:
                params["pageToken"] = next_token
            try:
                r = await c.get("https://clinicaltrials.gov/api/v2/studies", params=params)
                if r.status_code != 200:
                    break
                data = r.json()
                batch = data.get("studies", [])
                next_token = data.get("nextPageToken")
                page += 1
                for raw in batch:
                    s = parse_ctgov(raw, country)
                    if s:
                        studies.append(s)
                print(f"    {country} p{page}: +{len(batch)} → {len(studies)}")
                if not next_token:
                    break
                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"  CTgov {country} error: {e}")
                break
    return studies

def parse_ctgov(raw, country):
    try:
        p   = raw.get("protocolSection", {})
        id_ = p.get("identificationModule", {})
        st_ = p.get("statusModule", {})
        sp_ = p.get("sponsorCollaboratorsModule", {})
        de_ = p.get("designModule", {})
        co_ = p.get("conditionsModule", {})
        cl_ = p.get("contactsLocationsModule", {})

        nct_id     = id_.get("nctId", "")
        if not nct_id:
            return None

        title      = id_.get("briefTitle", "N/A")
        conditions = co_.get("conditions", [])
        sponsor    = sp_.get("leadSponsor", {}).get("name", "—")
        collabs    = [c.get("name","") for c in sp_.get("collaborators",[])]
        phases     = de_.get("phases", [])
        locs       = cl_.get("locations", [])
        latam_c    = list({l.get("country","") for l in locs if l.get("country","") in LATAM})
        phase      = phases[0].replace("PHASE","").strip() if phases else "NA"
        txt        = title + " " + " ".join(conditions) + " " + sponsor
        is_be_flag = is_be(txt)
        cro        = detect_cro(" ".join(collabs + [sponsor]))

        return {
            "id":            nct_id,
            "source":        "ClinicalTrials.gov",
            "title":         title,
            "status":        st_.get("overallStatus","UNKNOWN"),
            "phase":         phase,
            "sponsor":       sponsor,
            "country":       country,
            "studyType":     de_.get("studyType","N/A"),
            "isBE":          is_be_flag,
            "isEU":          bool(EU_PATTERN.search(sponsor)),
            "cro":           cro,
            "area":          classify(title, conditions),
            "conditions":    conditions,
            "enrollment":    de_.get("enrollmentInfo",{}).get("count"),
            "primEnd":       st_.get("primaryCompletionDateStruct",{}).get("date"),
            "firstPosted":   st_.get("studyFirstSubmitDate"),
            "latamCountries":latam_c or [country],
        }
    except Exception as e:
        print(f"  parse error: {e}")
        return None

# ══════════════════════════════════════════════
# SOURCE 2: REBEC
# ══════════════════════════════════════════════
async def fetch_rebec():
    studies = []
    terms = ["bioequivalencia","bioequivalência","biodisponibilidade","farmacocinética"]
    try:
        async with client(30) as c:
            for term in terms:
                try:
                    r = await c.get(
                        "https://ensaiosclinicos.gov.br/api/v1/rg/",
                        params={"q": term, "format": "json", "page_size": 500},
                    )
                    if r.status_code != 200:
                        continue
                    data  = r.json()
                    items = data.get("results", []) if isinstance(data, dict) else []
                    for item in items:
                        rid   = str(item.get("registro_anvisa") or item.get("id","")).strip()
                        title = item.get("titulo_publico") or item.get("scientific_title") or "N/A"
                        if not rid:
                            continue
                        studies.append({
                            "id":            f"REBEC-{rid}",
                            "source":        "REBEC",
                            "title":         title,
                            "status":        norm_status(item.get("recrutamento","")),
                            "phase":         "NA",
                            "sponsor":       item.get("patrocinador_primario","—"),
                            "country":       "Brazil",
                            "studyType":     "INTERVENTIONAL",
                            "isBE":          True,
                            "isEU":          False,
                            "cro":           None,
                            "area":          "be",
                            "conditions":    item.get("condicao_saude_primaria",[]),
                            "enrollment":    item.get("tamanho_amostra"),
                            "primEnd":       item.get("data_conclusao"),
                            "firstPosted":   item.get("data_registro"),
                            "latamCountries":["Brazil"],
                        })
                    await asyncio.sleep(0.2)
                except Exception as e:
                    print(f"  REBEC '{term}' error: {e}")
    except Exception as e:
        print(f"  REBEC error: {e}")
    print(f"  REBEC: {len(studies)} studies")
    return studies

# ══════════════════════════════════════════════
# SOURCE 3: WHO ICTRP
# ══════════════════════════════════════════════
async def fetch_ictrp(country, term="bioequivalence"):
    studies = []
    try:
        async with client(35) as c:
            r = await c.get(
                "https://trialsearch.who.int/Trial2.aspx",
                params={"SearchTerms": term, "Country": country},
            )
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            rows = soup.select("table#GridView1 tr")[1:] or soup.select("table tr")[1:]
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                tid = cells[0].get_text(strip=True)
                if not tid or tid.lower() in ("trial id","id",""):
                    continue
                title  = cells[1].get_text(strip=True) if len(cells)>1 else "N/A"
                status = cells[3].get_text(strip=True) if len(cells)>3 else ""
                sponsor= cells[4].get_text(strip=True) if len(cells)>4 else "—"
                is_be_flag = is_be(title + " " + sponsor)
                studies.append({
                    "id":            tid,
                    "source":        "WHO ICTRP",
                    "title":         title,
                    "status":        norm_status(status),
                    "phase":         "NA",
                    "sponsor":       sponsor,
                    "country":       country,
                    "studyType":     "INTERVENTIONAL",
                    "isBE":          is_be_flag,
                    "isEU":          bool(EU_PATTERN.search(sponsor)),
                    "cro":           detect_cro(sponsor),
                    "area":          "be" if is_be_flag else "outros",
                    "conditions":    [],
                    "enrollment":    None,
                    "primEnd":       None,
                    "firstPosted":   None,
                    "latamCountries":[country],
                })
        print(f"  ICTRP {country}/{term}: {len(studies)}")
    except Exception as e:
        print(f"  ICTRP {country} error: {e}")
    return studies

# ══════════════════════════════════════════════
# SYNC PRINCIPALE
# ══════════════════════════════════════════════
async def run_sync():
    if CACHE["is_syncing"]:
        print("Sync already running, skipping")
        return

    CACHE["is_syncing"] = True
    started = datetime.now()
    print(f"\n{'='*50}")
    print(f"SYNC START — {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    seen = {}

    def add(lst):
        for s in (lst or []):
            if not s or not s.get("id"):
                continue
            sid = s["id"].strip().upper()
            if sid not in seen:
                seen[sid] = s
            else:
                # Merge: keep most complete data
                ex = seen[sid]
                for k, v in s.items():
                    if v and not ex.get(k):
                        ex[k] = v
                # Merge latamCountries
                for c in s.get("latamCountries", []):
                    if c not in ex.get("latamCountries", []):
                        ex.setdefault("latamCountries", []).append(c)

    try:
        # 1. ClinicalTrials.gov — pays un par un pour ne pas surcharger
        print("\n[1/3] ClinicalTrials.gov (avec pagination)...")
        for country in LATAM:
            try:
                results = await fetch_ctgov_country(country)
                add(results)
                await asyncio.sleep(0.5)  # pause entre pays
            except Exception as e:
                print(f"  {country} failed: {e}")
        print(f"  → Après CTgov: {len(seen)} études uniques")

        # 2. REBEC
        print("\n[2/3] REBEC...")
        add(await fetch_rebec())
        print(f"  → Après REBEC: {len(seen)} études uniques")

        # 3. WHO ICTRP (BE uniquement pour les principaux pays)
        print("\n[3/3] WHO ICTRP...")
        ictrp_tasks = []
        for country in ["Brazil","Argentina","Mexico","Chile","Colombia","Peru"]:
            ictrp_tasks.append(fetch_ictrp(country, "bioequivalence"))
            ictrp_tasks.append(fetch_ictrp(country, "bioavailability"))
        results = await asyncio.gather(*ictrp_tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                add(r)
        print(f"  → Après ICTRP: {len(seen)} études uniques")

        # Tri final
        status_order = {
            "RECRUITING":0,"NOT_YET_RECRUITING":1,"ACTIVE_NOT_RECRUITING":2,
            "UNKNOWN":3,"COMPLETED":4,"TERMINATED":5,"SUSPENDED":6,
        }
        all_studies = sorted(
            seen.values(),
            key=lambda x: (
                status_order.get(x.get("status","UNKNOWN"),99),
                x.get("firstPosted") or "0000"
            )
        )

        # Mise à jour du cache
        CACHE["studies"]    = all_studies
        CACHE["updated_at"] = datetime.now()
        CACHE["total"]      = len(all_studies)
        CACHE["be_total"]   = sum(1 for s in all_studies if s.get("isBE"))

        duration = (datetime.now() - started).seconds
        print(f"\n{'='*50}")
        print(f"SYNC DONE — {len(all_studies)} studies ({CACHE['be_total']} BE) — {duration}s")
        print(f"{'='*50}\n")

    except Exception as e:
        print(f"SYNC ERROR: {e}")
    finally:
        CACHE["is_syncing"] = False


async def nightly_scheduler():
    """Relance la sync toutes les nuits à 2h00 UTC."""
    await asyncio.sleep(10)  # attendre que le serveur soit prêt
    while True:
        now      = datetime.now()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait = (next_run - now).total_seconds()
        print(f"Next nightly sync: {next_run.strftime('%Y-%m-%d %H:%M')} (in {wait/3600:.1f}h)")
        await asyncio.sleep(wait)
        await run_sync()


@app.on_event("startup")
async def startup():
    print("QIMA Server v3.1 starting...")
    # Sync initiale en arrière-plan (ne bloque pas le démarrage)
    asyncio.create_task(run_sync())
    # Scheduler nocturne
    asyncio.create_task(nightly_scheduler())
    print("Startup OK — sync running in background")


# ══════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {
        "status":     "ok",
        "version":    "3.1",
        "time":       datetime.now().isoformat(),
        "studies":    CACHE["total"],
        "be_studies": CACHE["be_total"],
        "updated_at": CACHE["updated_at"].isoformat() if CACHE["updated_at"] else None,
        "is_syncing": CACHE["is_syncing"],
    }


@app.get("/api/studies")
async def get_studies(
    country:  str  = Query("all"),
    area:     str  = Query("all"),
    per_page: int  = Query(5000),
    page:     int  = Query(1),
):
    # Si le cache est vide ET pas de sync en cours → déclencher sync
    if not CACHE["studies"] and not CACHE["is_syncing"]:
        asyncio.create_task(run_sync())
        return {
            "status":  "syncing",
            "message": "Initial sync in progress, please retry in ~2 minutes",
            "data":    [],
        }

    studies = CACHE["studies"]

    # Filtres
    if country != "all":
        studies = [s for s in studies
                   if s.get("country") == country
                   or country in s.get("latamCountries", [])]
    if area != "all":
        studies = [s for s in studies if s.get("area") == area]

    # Pagination
    total  = len(studies)
    offset = (page - 1) * per_page
    paged  = studies[offset:offset + per_page]

    return {
        "status":     "ok",
        "total":      total,
        "page":       page,
        "per_page":   per_page,
        "count":      len(paged),
        "updated_at": CACHE["updated_at"].isoformat() if CACHE["updated_at"] else None,
        "is_syncing": CACHE["is_syncing"],
        "data":       paged,
    }


@app.post("/api/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    if CACHE["is_syncing"]:
        return {"status": "already_running"}
    background_tasks.add_task(run_sync)
    return {"status": "started", "message": "Sync launched in background"}


@app.get("/api/stats")
async def stats():
    studies = CACHE["studies"]
    from collections import Counter
    return {
        "total":      len(studies),
        "be_total":   sum(1 for s in studies if s.get("isBE")),
        "by_country": dict(Counter(s.get("country") for s in studies).most_common()),
        "by_area":    dict(Counter(s.get("area")    for s in studies).most_common()),
        "by_source":  dict(Counter(s.get("source")  for s in studies).most_common()),
        "by_status":  dict(Counter(s.get("status")  for s in studies).most_common()),
        "updated_at": CACHE["updated_at"].isoformat() if CACHE["updated_at"] else None,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

