"""
QIMA Life Sciences — LatAm Trials Intelligence Server v3.0
Serveur stable avec base de données SQLite + scheduler nocturne.
Le frontend n'interroge QUE ce serveur — plus de requêtes directes depuis le navigateur.
"""
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import httpx, asyncio, re, os, json, sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from contextlib import contextmanager
import threading

app = FastAPI(title="QIMA LatAm Trials API", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══════════════════════════════════════════════════════════════
# BASE DE DONNÉES SQLITE
# Stocke les études de façon persistante entre les redémarrages.
# Plus besoin de tout re-fetcher à chaque refresh navigateur.
# ══════════════════════════════════════════════════════════════
DB_PATH = os.environ.get("DB_PATH", "/tmp/qima_trials.db")

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS studies (
                id          TEXT PRIMARY KEY,
                source      TEXT,
                title       TEXT,
                status      TEXT,
                phase       TEXT,
                sponsor     TEXT,
                country     TEXT,
                study_type  TEXT,
                is_be       INTEGER DEFAULT 0,
                area        TEXT,
                conditions  TEXT,
                enrollment  INTEGER,
                prim_end    TEXT,
                first_posted TEXT,
                latam_countries TEXT,
                updated_at  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at  TEXT,
                finished_at TEXT,
                status      TEXT,
                studies_found INTEGER,
                error       TEXT
            )
        """)
        conn.commit()

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def upsert_studies(studies: list):
    """Insère ou met à jour les études en base."""
    now = datetime.now().isoformat()
    with get_db() as conn:
        for s in studies:
            conn.execute("""
                INSERT INTO studies 
                    (id, source, title, status, phase, sponsor, country,
                     study_type, is_be, area, conditions, enrollment,
                     prim_end, first_posted, latam_countries, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                    source=excluded.source,
                    title=excluded.title,
                    status=excluded.status,
                    phase=excluded.phase,
                    sponsor=excluded.sponsor,
                    study_type=excluded.study_type,
                    is_be=excluded.is_be,
                    area=excluded.area,
                    conditions=excluded.conditions,
                    enrollment=excluded.enrollment,
                    prim_end=excluded.prim_end,
                    first_posted=excluded.first_posted,
                    latam_countries=excluded.latam_countries,
                    updated_at=excluded.updated_at
            """, (
                s.get("id"), s.get("source"), s.get("title"),
                s.get("status"), s.get("phase"), s.get("sponsor"),
                s.get("country"), s.get("studyType"),
                1 if s.get("isBE") else 0,
                s.get("area"),
                json.dumps(s.get("conditions", [])),
                s.get("enrollment"),
                s.get("primEnd"), s.get("firstPosted"),
                json.dumps(s.get("latamCountries", [])),
                now,
            ))
        conn.commit()

def db_to_study(row) -> dict:
    """Convertit une ligne SQLite en dict compatible frontend."""
    return {
        "id":             row["id"],
        "source":         row["source"],
        "title":          row["title"],
        "status":         row["status"],
        "phase":          row["phase"],
        "sponsor":        row["sponsor"],
        "country":        row["country"],
        "studyType":      row["study_type"],
        "isBE":           bool(row["is_be"]),
        "area":           row["area"],
        "conditions":     json.loads(row["conditions"] or "[]"),
        "enrollment":     row["enrollment"],
        "primEnd":        row["prim_end"],
        "firstPosted":    row["first_posted"],
        "latamCountries": json.loads(row["latam_countries"] or "[]"),
    }

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.9",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
}

LATAM = [
    {"name": "Brazil",    "code": "BR"},
    {"name": "Argentina", "code": "AR"},
    {"name": "Mexico",    "code": "MX"},
    {"name": "Chile",     "code": "CL"},
    {"name": "Colombia",  "code": "CO"},
    {"name": "Peru",      "code": "PE"},
    {"name": "Uruguay",   "code": "UY"},
    {"name": "Bolivia",   "code": "BO"},
    {"name": "Paraguay",  "code": "PY"},
    {"name": "Ecuador",   "code": "EC"},
]
LATAM_NAMES = [c["name"] for c in LATAM]

def make_client(timeout=35):
    return httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, headers=HEADERS
    )

# ══════════════════════════════════════════════════════════════
# UTILITAIRES
# ══════════════════════════════════════════════════════════════
def is_be(text: str) -> bool:
    t = (text or "").lower()
    return any(p in t for p in [
        "bioequivalen", "bioequivalên",
        "bioavailabilit", "biodisponibilid",
        "pharmacokineti", "farmacocinéti", "farmacocineti",
        "equivalência farmacêutica", "equivalencia farmaceutica",
        "ba/be", "generic drug study",
    ])

def norm_status(s: str) -> str:
    s = (s or "").lower()
    if any(x in s for x in ["recruit", "recrutando", "aberto", "open", "ativo"]):
        return "RECRUITING"
    if any(x in s for x in ["not yet", "não iniciado", "planned", "aprovado"]):
        return "NOT_YET_RECRUITING"
    if any(x in s for x in ["active, not", "fechado para recrutamento"]):
        return "ACTIVE_NOT_RECRUITING"
    if any(x in s for x in ["complet", "concluí", "encerrado", "finalizado"]):
        return "COMPLETED"
    if any(x in s for x in ["terminat", "interrompido", "cancelado"]):
        return "TERMINATED"
    if any(x in s for x in ["suspend", "suspenso"]):
        return "SUSPENDED"
    return "UNKNOWN"

EU_PHARMA = re.compile(
    r"roche|novartis|bayer|astrazeneca|sanofi|glaxosmithkline|gsk|ucb|"
    r"boehringer|merck kgaa|servier|ipsen|pierre fabre|almirall|chiesi|"
    r"recordati|menarini|bracco|grunenthal|lundbeck|novo nordisk|ferring|"
    r"leo pharma|stallergenes|haleon|genmab|bavarian nordic|seagen|"
    r"GmbH|S\.A\.S|N\.V\.|S\.r\.l\.|A/S\b", re.I
)

CROS = {
    r"IQVIA|Quintiles|IMS Health": "IQVIA",
    r"Syneos|INC Research": "Syneos Health",
    r"ICON plc|ICON Clinical": "ICON plc",
    r"PAREXEL|Parexel": "PAREXEL",
    r"PPD |Pharmaceutical Product Dev": "PPD",
    r"Covance|Labcorp Drug": "Labcorp Drug Dev.",
    r"Medpace": "Medpace",
    r"PRA Health": "PRA Health Sciences",
    r"WuXi|Wuxi": "WuXi AppTec",
    r"Premier Research": "Premier Research",
}

def detect_cro(text: str):
    for pattern, label in CROS.items():
        if re.search(pattern, text, re.I):
            return label
    return None

def classify_area(title: str, conditions: list) -> str:
    txt = (title + " " + " ".join(conditions)).lower()
    areas = [
        ("onco",      r"cancer|tumor|oncol|carcinom|lymphoma|leukemia|melanoma|sarcoma|myeloma|neoplasm"),
        ("cardio",    r"cardiovascular|cardiac|heart failure|hypertension|cholesterol|coronary|myocardial"),
        ("endocrino", r"diabetes|insulin|obesity|endocrin|thyroid|metabolic|hypoglycemi"),
        ("neuro",     r"neurolog|alzheimer|parkinson|multiple sclerosis|epilepsy|dementia|migraine|stroke"),
        ("psiq",      r"psychiatric|depression|anxiety|schizophrenia|bipolar|adhd|autism"),
        ("gastro",    r"gastro|hepatitis|liver|crohn|colitis|ibd|colon|pancrea|cirrhosis|nafld|nash"),
        ("resp",      r"respiratory|asthma|copd|pulmonary|lung|bronchial|fibrosis"),
        ("infec",     r"hiv|aids|tuberculosis|malaria|dengue|covid|sars|influenza|infection|zika|chagas"),
        ("vacinas",   r"vaccine|vaccination|immunization|immunogen"),
        ("be",        r"bioequivalen|bioavailabilit|pharmacokineti|ba/be"),
    ]
    for area_id, pattern in areas:
        if re.search(pattern, txt, re.I):
            return area_id
    return "outros"

# ══════════════════════════════════════════════════════════════
# SOURCE 1 : CLINICALTRIALS.GOV — fetch complet avec pagination
# ══════════════════════════════════════════════════════════════
async def fetch_ctgov_country(country: str) -> list:
    """
    Récupère TOUTES les études d'un pays via pagination.
    ClinicalTrials.gov v2 limite à 1000 par page — on pagine jusqu'à la fin.
    """
    all_studies = []
    next_token = None
    page = 0
    MAX_PAGES = 10  # sécurité — 10 pages × 1000 = 10 000 études max par pays

    print(f"  Fetching ClinicalTrials.gov: {country}...")

    async with make_client(40) as client:
        while True:
            params = {
                "query.locn": country,
                "pageSize":   "1000",
            }
            if next_token:
                params["pageToken"] = next_token

            try:
                resp = await client.get(
                    "https://clinicaltrials.gov/api/v2/studies",
                    params=params,
                )
                if resp.status_code != 200:
                    print(f"  ClinicalTrials.gov {country} HTTP {resp.status_code}")
                    break

                data       = resp.json()
                studies    = data.get("studies", [])
                next_token = data.get("nextPageToken")
                page      += 1

                for raw in studies:
                    s = parse_ctgov(raw, country)
                    if s:
                        all_studies.append(s)

                print(f"  {country} page {page}: {len(studies)} studies (total: {len(all_studies)})")

                if not next_token:
                    break  # Plus de pages

                if page >= MAX_PAGES:
                    print(f"  {country}: MAX_PAGES ({MAX_PAGES}) reached")
                    break

                await asyncio.sleep(0.2)  # Courtoisie envers l'API

            except Exception as e:
                print(f"  ClinicalTrials.gov {country} error: {e}")
                break

    return all_studies


def parse_ctgov(raw: dict, primary_country: str) -> dict:
    """Parse un résultat brut ClinicalTrials.gov v2."""
    try:
        p   = raw.get("protocolSection", {})
        id_ = p.get("identificationModule", {})
        st_ = p.get("statusModule", {})
        sp_ = p.get("sponsorCollaboratorsModule", {})
        de_ = p.get("designModule", {})
        co_ = p.get("conditionsModule", {})
        cl_ = p.get("contactsLocationsModule", {})

        nct_id  = id_.get("nctId", "")
        if not nct_id:
            return None

        title      = id_.get("briefTitle", "N/A")
        conditions = co_.get("conditions", [])
        locs       = cl_.get("locations", [])
        sponsor    = sp_.get("leadSponsor", {}).get("name", "—")
        collabs    = [c.get("name", "") for c in sp_.get("collaborators", [])]
        phases     = de_.get("phases", [])

        latam_c    = list({l.get("country", "") for l in locs if l.get("country", "") in LATAM_NAMES})
        phase      = phases[0].replace("PHASE", "").strip() if phases else "NA"
        txt        = title + " " + " ".join(conditions) + " " + sponsor

        is_be_flag = is_be(txt)
        area       = "be" if is_be_flag else classify_area(title, conditions)
        cro        = detect_cro(" ".join(collabs + [sponsor]))

        return {
            "id":             nct_id,
            "source":         "ClinicalTrials.gov",
            "title":          title,
            "status":         st_.get("overallStatus", "UNKNOWN"),
            "phase":          phase,
            "sponsor":        sponsor,
            "country":        primary_country,
            "studyType":      de_.get("studyType", "N/A"),
            "isBE":           is_be_flag,
            "isEU":           bool(EU_PHARMA.search(sponsor)),
            "cro":            cro,
            "area":           area,
            "conditions":     conditions,
            "enrollment":     de_.get("enrollmentInfo", {}).get("count"),
            "primEnd":        st_.get("primaryCompletionDateStruct", {}).get("date"),
            "firstPosted":    st_.get("studyFirstSubmitDate"),
            "latamCountries": latam_c or [primary_country],
        }
    except Exception as e:
        print(f"  parse_ctgov error: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# SOURCE 2 : PLATAFORMA BRASIL (CONEP/CEP)
# Système officiel d'approbation éthique brésilien.
# Source la plus précoce pour détecter les nouvelles études BA/BE.
# ══════════════════════════════════════════════════════════════
async def fetch_plataforma(term: str) -> list:
    studies = []
    url = "https://plataformabrasil.saude.gov.br/visao/publico/indexPublico.jsf"
    try:
        async with make_client(40) as c:
            r = await c.get(url)
            if r.status_code != 200:
                return []
            soup = BeautifulSoup(r.text, "html.parser")
            vs   = soup.find("input", {"name": "javax.faces.ViewState"})
            viewstate = vs["value"] if vs else ""

            post_data = {
                "javax.faces.partial.ajax":    "true",
                "javax.faces.source":          "formBusca:btnBuscar",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render":  "formBusca:tabelaPesquisas",
                "formBusca:btnBuscar":         "formBusca:btnBuscar",
                "formBusca":                   "formBusca",
                "formBusca:tituloPesquisa":    term,
                "formBusca:situacao":          "",
                "javax.faces.ViewState":       viewstate,
            }
            hdrs = {**HEADERS,
                "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Faces-Request":    "partial/ajax",
                "Referer":          url,
            }
            r2 = await c.post(url, data=post_data, headers=hdrs)
            if r2.status_code != 200:
                return []

            soup2 = BeautifulSoup(r2.text, "xml")
            html_frag = ""
            for upd in soup2.find_all("update"):
                if "tabelaPesquisas" in upd.get("id", ""):
                    html_frag = upd.get_text()
                    break
            if not html_frag:
                html_frag = r2.text

            soup3 = BeautifulSoup(html_frag, "html.parser")
            for row in soup3.select("table tbody tr"):
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                caae   = cells[0].get_text(strip=True)
                title  = cells[1].get_text(strip=True)
                status = cells[2].get_text(strip=True) if len(cells) > 2 else "Aprovado"
                sponsor= cells[3].get_text(strip=True) if len(cells) > 3 else "—"
                if not caae or not re.match(r"\d{8}\.\d\.\d{4}\.\d{4}", caae):
                    continue
                is_be_flag = is_be(title + " " + sponsor) or is_be(term)
                studies.append({
                    "id":             f"CAAE-{caae}",
                    "source":         "Plataforma Brasil (CONEP)",
                    "title":          title,
                    "status":         norm_status(status),
                    "phase":          "NA",
                    "sponsor":        sponsor,
                    "country":        "Brazil",
                    "studyType":      "INTERVENTIONAL",
                    "isBE":           is_be_flag,
                    "isEU":           False,
                    "cro":            None,
                    "area":           "be" if is_be_flag else "outros",
                    "conditions":     [],
                    "enrollment":     None,
                    "primEnd":        None,
                    "firstPosted":    None,
                    "latamCountries": ["Brazil"],
                })
        print(f"  Plataforma Brasil '{term}' → {len(studies)}")
    except Exception as e:
        print(f"  Plataforma Brasil error '{term}': {e}")
    return studies

# ══════════════════════════════════════════════════════════════
# SOURCE 3 : REBEC
# ══════════════════════════════════════════════════════════════
async def fetch_rebec(term: str) -> list:
    studies = []
    try:
        async with make_client() as c:
            r = await c.get(
                "https://ensaiosclinicos.gov.br/api/v1/rg/",
                params={"q": term, "format": "json", "page_size": 500},
            )
            if r.status_code != 200:
                return []
            data  = r.json()
            items = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                rid   = str(item.get("registro_anvisa") or item.get("registro") or item.get("id", "")).strip()
                title = item.get("titulo_publico") or item.get("scientific_title") or "N/A"
                if not rid:
                    continue
                is_be_flag = is_be(title) or is_be(term)
                studies.append({
                    "id":             f"REBEC-{rid}",
                    "source":         "REBEC",
                    "title":          title,
                    "status":         norm_status(item.get("recrutamento", "")),
                    "phase":          "NA",
                    "sponsor":        item.get("patrocinador_primario", "—"),
                    "country":        "Brazil",
                    "studyType":      "INTERVENTIONAL",
                    "isBE":           is_be_flag,
                    "isEU":           False,
                    "cro":            None,
                    "area":           "be" if is_be_flag else "outros",
                    "conditions":     item.get("condicao_saude_primaria", []),
                    "enrollment":     item.get("tamanho_amostra"),
                    "primEnd":        item.get("data_conclusao"),
                    "firstPosted":    item.get("data_registro"),
                    "latamCountries": ["Brazil"],
                })
        print(f"  REBEC '{term}' → {len(studies)}")
    except Exception as e:
        print(f"  REBEC error '{term}': {e}")
    return studies

# ══════════════════════════════════════════════════════════════
# SOURCE 4 : WHO ICTRP
# ══════════════════════════════════════════════════════════════
async def fetch_ictrp(country: str, term: str = "bioequivalence") -> list:
    studies = []
    try:
        async with make_client(40) as c:
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
                if not tid or tid.lower() in ("trial id", "id", ""):
                    continue
                title  = cells[1].get_text(strip=True) if len(cells) > 1 else "N/A"
                status = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                sponsor= cells[4].get_text(strip=True) if len(cells) > 4 else "—"
                is_be_flag = is_be(title + " " + sponsor) or is_be(term)
                studies.append({
                    "id":             tid,
                    "source":         f"WHO ICTRP",
                    "title":          title,
                    "status":         norm_status(status),
                    "phase":          "NA",
                    "sponsor":        sponsor,
                    "country":        country,
                    "studyType":      "INTERVENTIONAL",
                    "isBE":           is_be_flag,
                    "isEU":           bool(EU_PHARMA.search(sponsor)),
                    "cro":            detect_cro(sponsor),
                    "area":           "be" if is_be_flag else "outros",
                    "conditions":     [],
                    "enrollment":     None,
                    "primEnd":        None,
                    "firstPosted":    None,
                    "latamCountries": [country],
                })
        print(f"  WHO ICTRP {country}/'{term}' → {len(studies)}")
    except Exception as e:
        print(f"  WHO ICTRP error ({country}/{term}): {e}")
    return studies

# ══════════════════════════════════════════════════════════════
# COLLECTE PRINCIPALE — appelée au démarrage + toutes les nuits
# ══════════════════════════════════════════════════════════════
_sync_running = False
_sync_status  = {"last_run": None, "last_count": 0, "status": "never_run"}

async def full_sync():
    """
    Collecte complète de toutes les sources.
    - ClinicalTrials.gov : tous les pays LatAm avec pagination complète
    - REBEC + Plataforma Brasil : études brésiliennes BA/BE
    - WHO ICTRP : complément pour tous les pays

    Résultat stocké en base SQLite → réponses stables et instantanées.
    """
    global _sync_running, _sync_status
    if _sync_running:
        print("Sync already running, skipping")
        return

    _sync_running = True
    started = datetime.now()
    print(f"\n{'='*60}")
    print(f"QIMA SYNC STARTED — {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    all_studies = {}

    def add(studies):
        for s in studies:
            if s and s.get("id"):
                sid = s["id"].strip().upper()
                if sid not in all_studies:
                    all_studies[sid] = s
                else:
                    # Fusionner : garder les données les plus complètes
                    existing = all_studies[sid]
                    merged = {**existing}
                    for k, v in s.items():
                        if v and not merged.get(k):
                            merged[k] = v
                    all_studies[sid] = merged

    try:
        # ── ClinicalTrials.gov : tous pays en parallèle ──────────
        print("\n[1/4] ClinicalTrials.gov — pagination complète par pays")
        ct_tasks = [fetch_ctgov_country(c["name"]) for c in LATAM]
        ct_results = await asyncio.gather(*ct_tasks, return_exceptions=True)
        for r in ct_results:
            if isinstance(r, list):
                add(r)
        print(f"  → ClinicalTrials.gov total: {len(all_studies)} études uniques")

        # ── REBEC ────────────────────────────────────────────────
        print("\n[2/4] REBEC — registre officiel brésilien")
        rebec_terms = ["bioequivalencia", "bioequivalência", "biodisponibilidade",
                       "farmacocinética", "equivalência farmacêutica"]
        rebec_tasks = [fetch_rebec(t) for t in rebec_terms]
        rebec_results = await asyncio.gather(*rebec_tasks, return_exceptions=True)
        for r in rebec_results:
            if isinstance(r, list):
                add(r)
        print(f"  → Total après REBEC: {len(all_studies)} études uniques")

        # ── Plataforma Brasil ────────────────────────────────────
        print("\n[3/4] Plataforma Brasil (CONEP/CEP)")
        pb_terms = ["bioequivalência", "bioequivalencia", "biodisponibilidade",
                    "equivalência farmacêutica", "farmacocinética"]
        pb_tasks = [fetch_plataforma(t) for t in pb_terms]
        pb_results = await asyncio.gather(*pb_tasks, return_exceptions=True)
        for r in pb_results:
            if isinstance(r, list):
                add(r)
        print(f"  → Total après Plataforma Brasil: {len(all_studies)} études uniques")

        # ── WHO ICTRP ────────────────────────────────────────────
        print("\n[4/4] WHO ICTRP — tous pays LatAm")
        ictrp_tasks = []
        for c in LATAM:
            ictrp_tasks.append(fetch_ictrp(c["name"], "bioequivalence"))
            ictrp_tasks.append(fetch_ictrp(c["name"], "bioavailability"))
        ictrp_results = await asyncio.gather(*ictrp_tasks, return_exceptions=True)
        for r in ictrp_results:
            if isinstance(r, list):
                add(r)
        print(f"  → Total après WHO ICTRP: {len(all_studies)} études uniques")

        # ── Sauvegarde en base ───────────────────────────────────
        print(f"\nSaving {len(all_studies)} studies to database...")
        upsert_studies(list(all_studies.values()))

        finished = datetime.now()
        duration = (finished - started).seconds
        _sync_status = {
            "last_run":    finished.isoformat(),
            "last_count":  len(all_studies),
            "status":      "ok",
            "duration_s":  duration,
        }
        print(f"\n{'='*60}")
        print(f"SYNC COMPLETE — {len(all_studies)} studies — {duration}s")
        print(f"{'='*60}\n")

        with get_db() as conn:
            conn.execute(
                "INSERT INTO sync_log (started_at, finished_at, status, studies_found) VALUES (?,?,?,?)",
                (started.isoformat(), finished.isoformat(), "ok", len(all_studies))
            )
            conn.commit()

    except Exception as e:
        print(f"SYNC ERROR: {e}")
        _sync_status["status"] = f"error: {e}"
        with get_db() as conn:
            conn.execute(
                "INSERT INTO sync_log (started_at, status, error) VALUES (?,?,?)",
                (started.isoformat(), "error", str(e))
            )
            conn.commit()
    finally:
        _sync_running = False

def schedule_nightly():
    """Lance la synchronisation chaque nuit à 2h00 UTC."""
    import time
    while True:
        now = datetime.now()
        # Prochaine exécution à 2h00
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        wait_seconds = (next_run - now).total_seconds()
        print(f"Next sync scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {wait_seconds/3600:.1f}h)")
        time.sleep(wait_seconds)
        asyncio.run(full_sync())

# ══════════════════════════════════════════════════════════════
# ENDPOINTS API
# ══════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    """Au démarrage : initialise la DB et lance une sync si nécessaire."""
    init_db()
    # Vérifier si la base est vide
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0]

    if count == 0:
        print("Database empty — launching initial sync...")
        asyncio.create_task(full_sync())
    else:
        print(f"Database has {count} studies — ready to serve")
        _sync_status["last_count"] = count
        _sync_status["status"] = "ok (from db)"

    # Lancer le scheduler nocturne dans un thread séparé
    t = threading.Thread(target=schedule_nightly, daemon=True)
    t.start()


@app.get("/api/health")
async def health():
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0]
        be_count = conn.execute("SELECT COUNT(*) FROM studies WHERE is_be=1").fetchone()[0]
        last_log = conn.execute(
            "SELECT * FROM sync_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "status":        "ok",
        "version":       "3.0",
        "time":          datetime.now().isoformat(),
        "db_studies":    count,
        "db_be_studies": be_count,
        "sync_running":  _sync_running,
        "sync_status":   _sync_status,
        "last_sync":     dict(last_log) if last_log else None,
    }


@app.get("/api/studies")
async def get_studies(
    country:  str  = Query("all"),
    area:     str  = Query("all"),
    is_be:    bool = Query(None),
    status:   str  = Query("all"),
    page:     int  = Query(1),
    per_page: int  = Query(2000),
):
    """
    Endpoint principal — retourne les études depuis la base SQLite.
    Données stables, cohérentes, mises à jour toutes les nuits.
    """
    with get_db() as conn:
        where = ["1=1"]
        params = []

        if country != "all":
            where.append("(country=? OR latam_countries LIKE ?)")
            params += [country, f"%{country}%"]

        if area != "all":
            where.append("area=?")
            params.append(area)

        if is_be is not None:
            where.append("is_be=?")
            params.append(1 if is_be else 0)

        if status != "all":
            where.append("status=?")
            params.append(status)

        sql = f"""
            SELECT * FROM studies
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE status
                    WHEN 'RECRUITING' THEN 0
                    WHEN 'NOT_YET_RECRUITING' THEN 1
                    WHEN 'ACTIVE_NOT_RECRUITING' THEN 2
                    WHEN 'UNKNOWN' THEN 3
                    WHEN 'COMPLETED' THEN 4
                    ELSE 5
                END,
                first_posted DESC
            LIMIT ? OFFSET ?
        """
        params += [per_page, (page - 1) * per_page]

        rows   = conn.execute(sql, params).fetchall()
        total  = conn.execute(
            f"SELECT COUNT(*) FROM studies WHERE {' AND '.join(where)}",
            params[:-2]
        ).fetchone()[0]

    studies = [db_to_study(r) for r in rows]

    return {
        "status":     "ok",
        "total":      total,
        "page":       page,
        "per_page":   per_page,
        "count":      len(studies),
        "sync_info":  _sync_status,
        "data":       studies,
    }


@app.post("/api/sync")
async def trigger_sync(background_tasks: BackgroundTasks):
    """Déclenche manuellement une synchronisation complète."""
    if _sync_running:
        return {"status": "already_running", "message": "A sync is already in progress"}
    background_tasks.add_task(full_sync)
    return {"status": "started", "message": "Full sync launched in background"}


@app.get("/api/stats")
async def get_stats():
    """Statistiques détaillées par source, pays et aire thérapeutique."""
    with get_db() as conn:
        by_country = conn.execute(
            "SELECT country, COUNT(*) as n FROM studies GROUP BY country ORDER BY n DESC"
        ).fetchall()
        by_area = conn.execute(
            "SELECT area, COUNT(*) as n FROM studies GROUP BY area ORDER BY n DESC"
        ).fetchall()
        by_source = conn.execute(
            "SELECT source, COUNT(*) as n FROM studies GROUP BY source ORDER BY n DESC"
        ).fetchall()
        by_status = conn.execute(
            "SELECT status, COUNT(*) as n FROM studies GROUP BY status ORDER BY n DESC"
        ).fetchall()
        be_count = conn.execute(
            "SELECT COUNT(*) FROM studies WHERE is_be=1"
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0]

    return {
        "total":      total,
        "be_studies": be_count,
        "by_country": [dict(r) for r in by_country],
        "by_area":    [dict(r) for r in by_area],
        "by_source":  [dict(r) for r in by_source],
        "by_status":  [dict(r) for r in by_status],
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    print(f"Starting QIMA LatAm Trials API v3.0 on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)

