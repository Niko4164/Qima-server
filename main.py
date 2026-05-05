"""
QIMA Life Sciences — LatAm Trials Server v4.0
Stratégie : mini-syncs espacées toutes les 20 min durant la nuit
Aucune source perdue — mémoire toujours sous contrôle
"""
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import httpx, asyncio, re, os, gc
from datetime import datetime, timedelta
from collections import Counter
from bs4 import BeautifulSoup

from fastapi.middleware.gzip import GZipMiddleware

app = FastAPI(title="QIMA LatAm Trials API", version="4.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ══════════════════════════════════════════════════════════════
# CACHE GLOBAL — s'accumule au fil des mini-syncs
# Jamais reconstruit depuis zéro — mis à jour incrémentalement
# ══════════════════════════════════════════════════════════════
CACHE = {
    "studies":      {},        # dict: id -> study (dédupliqué)
    "updated_at":   None,
    "current_job":  None,      # nom du job en cours
    "jobs_done":    [],        # log des jobs terminés
    "next_job_at":  None,
}

# ══════════════════════════════════════════════════════════════
# PLANNING DES MINI-SYNCS
# Chaque job = une petite tâche, exécutée l'une après l'autre
# avec une pause de 20 minutes entre chaque
# ══════════════════════════════════════════════════════════════
JOBS = [
    # (nom, type, paramètre)
    ("CTgov Brazil",                    "ctgov",      ["Brazil"]),
    ("CTgov Argentina + Mexico",        "ctgov",      ["Argentina","Mexico"]),
    ("CTgov Chile + Colombia + Peru",   "ctgov",      ["Chile","Colombia","Peru"]),
    ("CTgov Uruguay + Bolivia + Others","ctgov",      ["Uruguay","Bolivia","Paraguay","Ecuador"]),
    ("REBEC Brazil",                    "rebec",      None),
    ("WHO ICTRP Brazil + Argentina",    "ictrp",      ["Brazil","Argentina"]),
    ("WHO ICTRP Mexico + Chile + CO",   "ictrp",      ["Mexico","Chile","Colombia","Peru"]),
    ("Plataforma Brasil CONEP",         "plataforma", None),
    ("ANMAT / REPEC Argentina",         "anmat",      None),
    ("INS REPEC Peru",                  "ins_repec",  None),
]

# Interval entre deux mini-syncs (en secondes)
JOB_INTERVAL = 20 * 60  # 20 minutes

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
LATAM = ["Brazil","Argentina","Mexico","Chile","Colombia",
         "Peru","Uruguay","Bolivia","Paraguay","Ecuador"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Accept":     "text/html,application/xhtml+xml,application/json,*/*;q=0.9",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

# ══════════════════════════════════════════════════════════════
# PATTERNS
# ══════════════════════════════════════════════════════════════
RE_BE  = re.compile(r"bioequivalen|bioavailab|biodisponib|pharmacokineti|ba/be|farmacocineti|equivalência farmac", re.I)
RE_EU  = re.compile(r"roche|novartis|bayer|astrazeneca|sanofi|gsk|boehringer|servier|ipsen|lundbeck|novo nordisk|ferring|GmbH|N\.V\.|S\.r\.l\.", re.I)
CRO_LIST = [
    (re.compile(r"IQVIA|Quintiles",    re.I), "IQVIA"),
    (re.compile(r"Syneos",             re.I), "Syneos Health"),
    (re.compile(r"ICON plc",           re.I), "ICON plc"),
    (re.compile(r"PAREXEL",            re.I), "PAREXEL"),
    (re.compile(r"Covance|Labcorp",    re.I), "Labcorp"),
    (re.compile(r"Medpace",            re.I), "Medpace"),
    (re.compile(r"\bPPD\b",            re.I), "PPD"),
]
AREA_LIST = [
    ("be",        re.compile(r"bioequivalen|bioavailab|pharmacokineti|ba/be", re.I)),
    ("onco",      re.compile(r"cancer|tumor|oncol|lymphoma|leukemia|sarcoma|neoplasm", re.I)),
    ("cardio",    re.compile(r"cardiovascular|cardiac|hypertension|coronary|myocardial", re.I)),
    ("endocrino", re.compile(r"diabetes|insulin|obesity|thyroid|metabolic", re.I)),
    ("neuro",     re.compile(r"neurolog|alzheimer|parkinson|epilepsy|dementia|stroke", re.I)),
    ("psiq",      re.compile(r"depression|anxiety|schizophrenia|bipolar|adhd", re.I)),
    ("gastro",    re.compile(r"hepatitis|liver|crohn|colitis|cirrhosis|nash", re.I)),
    ("resp",      re.compile(r"asthma|copd|pulmonary|lung|fibrosis", re.I)),
    ("infec",     re.compile(r"hiv|aids|tuberculosis|malaria|dengue|covid|zika", re.I)),
    ("vacinas",   re.compile(r"vaccine|vaccination|immunization", re.I)),
]

def classify(txt):
    for aid, pat in AREA_LIST:
        if pat.search(txt): return aid
    return "outros"

def norm_status(s):
    s = (s or "").lower()
    if any(x in s for x in ["recruit","aberto","open","ativo"]):   return "RECRUITING"
    if any(x in s for x in ["not yet","não iniciado","planned"]):  return "NOT_YET_RECRUITING"
    if any(x in s for x in ["active, not","fechado"]):             return "ACTIVE_NOT_RECRUITING"
    if any(x in s for x in ["complet","concluí","encerrado"]):     return "COMPLETED"
    if any(x in s for x in ["terminat","interrompido"]):           return "TERMINATED"
    return "UNKNOWN"

def add_to_cache(studies):
    """Ajoute les études au cache global de façon incrémentale."""
    added = 0
    for s in studies:
        sid = (s.get("id") or "").strip().upper()
        if not sid:
            continue
        if sid not in CACHE["studies"]:
            CACHE["studies"][sid] = s
            added += 1
        else:
            # Enrichir les données existantes si on a de nouvelles infos
            ex = CACHE["studies"][sid]
            for k, v in s.items():
                if v and not ex.get(k):
                    ex[k] = v
            for c in s.get("latamCountries", []):
                if c not in ex.get("latamCountries", []):
                    ex.setdefault("latamCountries", []).append(c)
    CACHE["updated_at"] = datetime.now()
    return added

# ══════════════════════════════════════════════════════════════
# SOURCE 1 : ClinicalTrials.gov
# Pagination complète par pays — traitement immédiat étude par étude
# ══════════════════════════════════════════════════════════════
async def job_ctgov(countries):
    total_added = 0
    for country in countries:
        studies = []
        next_token = None
        page = 0
        print(f"  CTgov: {country}...")
        async with httpx.AsyncClient(timeout=25, headers=HEADERS) as c:
            while page < 5:  # max 5 pages × 1000 = 5000 par pays
                params = {"query.locn": country, "pageSize": "1000"}
                if next_token:
                    params["pageToken"] = next_token
                try:
                    r = await c.get("https://clinicaltrials.gov/api/v2/studies", params=params)
                    if r.status_code != 200:
                        break
                    data       = r.json()
                    batch      = data.get("studies", [])
                    next_token = data.get("nextPageToken")
                    page += 1

                    for raw in batch:
                        s = _parse_ctgov(raw, country)
                        if s:
                            studies.append(s)

                    # Libérer la réponse HTTP immédiatement
                    del data, batch
                    gc.collect()

                    if not next_token:
                        break
                    await asyncio.sleep(0.3)

                except Exception as e:
                    print(f"    {country} p{page} error: {e}")
                    break

        added = add_to_cache(studies)
        total_added += added
        print(f"  CTgov {country}: {len(studies)} fetched, {added} new (cache: {len(CACHE['studies'])})")
        del studies
        gc.collect()
        await asyncio.sleep(1)  # pause entre pays

    return total_added

def _parse_ctgov(raw, country):
    try:
        p   = raw.get("protocolSection", {})
        id_ = p.get("identificationModule", {})
        nct = id_.get("nctId", "")
        if not nct:
            return None
        st_ = p.get("statusModule", {})
        sp_ = p.get("sponsorCollaboratorsModule", {})
        de_ = p.get("designModule", {})
        co_ = p.get("conditionsModule", {})
        cl_ = p.get("contactsLocationsModule", {})

        sponsor  = (sp_.get("leadSponsor") or {}).get("name", "—")
        collabs  = " ".join(c.get("name","") for c in sp_.get("collaborators",[]))
        phases   = de_.get("phases", [])
        conds    = co_.get("conditions", [])[:5]
        locs     = cl_.get("locations", [])
        latam_c  = list({l["country"] for l in locs if l.get("country","") in LATAM})[:6]
        txt      = f"{id_.get('briefTitle','')} {' '.join(conds)} {sponsor}"
        is_be    = bool(RE_BE.search(txt))
        cro      = next((l for pat,l in CRO_LIST if pat.search(collabs+" "+sponsor)), None)

        return {
            "id":            nct,
            "source":        "ClinicalTrials.gov",
            "title":         id_.get("briefTitle","N/A")[:200],
            "status":        st_.get("overallStatus","UNKNOWN"),
            "phase":         phases[0].replace("PHASE","").strip() if phases else "NA",
            "sponsor":       sponsor[:100],
            "country":       country,
            "studyType":     de_.get("studyType","N/A"),
            "isBE":          is_be,
            "isEU":          bool(RE_EU.search(sponsor)),
            "cro":           cro,
            "area":          classify(txt),
            "conditions":    conds,
            "enrollment":    (de_.get("enrollmentInfo") or {}).get("count"),
            "primEnd":       (st_.get("primaryCompletionDateStruct") or {}).get("date"),
            "firstPosted":   st_.get("studyFirstSubmitDate"),
            "latamCountries":latam_c or [country],
        }
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════
# SOURCE 2 : REBEC — API JSON officielle
# ══════════════════════════════════════════════════════════════
async def job_rebec():
    studies = []
    terms = ["bioequivalencia","bioequivalência","biodisponibilidade",
             "farmacocinética","equivalência farmacêutica","fase I"]
    async with httpx.AsyncClient(timeout=25, headers=HEADERS) as c:
        for term in terms:
            try:
                r = await c.get(
                    "https://ensaiosclinicos.gov.br/api/v1/rg/",
                    params={"q":term,"format":"json","page_size":500},
                )
                if r.status_code != 200:
                    continue
                data  = r.json()
                items = data.get("results",[]) if isinstance(data,dict) else []
                for item in items:
                    rid   = str(item.get("registro_anvisa") or item.get("id","")).strip()
                    title = (item.get("titulo_publico") or item.get("scientific_title") or "N/A")
                    if not rid:
                        continue
                    studies.append({
                        "id":            f"REBEC-{rid}",
                        "source":        "REBEC",
                        "title":         title[:200],
                        "status":        norm_status(item.get("recrutamento","")),
                        "phase":         "NA",
                        "sponsor":       (item.get("patrocinador_primario") or "—")[:100],
                        "country":       "Brazil",
                        "studyType":     "INTERVENTIONAL",
                        "isBE":          True,
                        "isEU":          False,
                        "cro":           None,
                        "area":          "be",
                        "conditions":    item.get("condicao_saude_primaria",[])[:5],
                        "enrollment":    item.get("tamanho_amostra"),
                        "primEnd":       item.get("data_conclusao"),
                        "firstPosted":   item.get("data_registro"),
                        "latamCountries":["Brazil"],
                    })
                del data, items
                gc.collect()
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"  REBEC '{term}' error: {e}")

    added = add_to_cache(studies)
    print(f"  REBEC: {len(studies)} fetched, {added} new (cache: {len(CACHE['studies'])})")
    del studies
    gc.collect()
    return added

# ══════════════════════════════════════════════════════════════
# SOURCE 3 : WHO ICTRP — scraping HTML avec BeautifulSoup
# Traité séparément, libéré après chaque pays
# ══════════════════════════════════════════════════════════════
async def job_ictrp(countries):
    total_added = 0
    for country in countries:
        studies = []
        for term in ["bioequivalence", "bioavailability", "pharmacokinetics"]:
            try:
                async with httpx.AsyncClient(timeout=30, headers=HEADERS) as c:
                    r = await c.get(
                        "https://trialsearch.who.int/Trial2.aspx",
                        params={"SearchTerms": term, "Country": country},
                    )
                if r.status_code != 200:
                    continue

                # BeautifulSoup sur réponse limitée
                soup = BeautifulSoup(r.text, "html.parser")
                rows = (soup.select("table#GridView1 tr")[1:] or
                        soup.select("table tr")[1:])

                for row in rows[:150]:
                    cells = row.find_all("td")
                    if len(cells) < 3:
                        continue
                    tid = cells[0].get_text(strip=True)
                    if not tid or tid.lower() in ("trial id","id",""):
                        continue
                    title  = cells[1].get_text(strip=True) if len(cells)>1 else "N/A"
                    status = cells[3].get_text(strip=True) if len(cells)>3 else ""
                    sponsor= cells[4].get_text(strip=True) if len(cells)>4 else "—"
                    is_be  = bool(RE_BE.search(title+" "+sponsor))
                    studies.append({
                        "id":            tid,
                        "source":        "WHO ICTRP",
                        "title":         title[:200],
                        "status":        norm_status(status),
                        "phase":         "NA",
                        "sponsor":       sponsor[:100],
                        "country":       country,
                        "studyType":     "INTERVENTIONAL",
                        "isBE":          is_be,
                        "isEU":          bool(RE_EU.search(sponsor)),
                        "cro":           next((l for pat,l in CRO_LIST if pat.search(sponsor)), None),
                        "area":          "be" if is_be else classify(title),
                        "conditions":    [],
                        "enrollment":    None,
                        "primEnd":       None,
                        "firstPosted":   None,
                        "latamCountries":[country],
                    })

                # Libérer BeautifulSoup immédiatement
                del soup, r
                gc.collect()
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"  ICTRP {country}/{term} error: {e}")

        added = add_to_cache(studies)
        total_added += added
        print(f"  ICTRP {country}: {len(studies)} fetched, {added} new")
        del studies
        gc.collect()
        await asyncio.sleep(1)

    return total_added

# ══════════════════════════════════════════════════════════════
# SOURCE 4 : PLATAFORMA BRASIL (CONEP/CEP)
# JSF avec ViewState — scraping par terme, libéré après chaque terme
# ══════════════════════════════════════════════════════════════
PLATAFORMA_URL = "https://plataformabrasil.saude.gov.br/visao/publico/indexPublico.jsf"
PB_TERMS = ["bioequivalência","bioequivalencia","biodisponibilidade",
             "equivalência farmacêutica","farmacocinética"]

async def job_plataforma():
    total_added = 0
    for term in PB_TERMS:
        studies = []
        try:
            async with httpx.AsyncClient(timeout=40, headers=HEADERS) as c:
                # Étape 1: GET → ViewState
                r1 = await c.get(PLATAFORMA_URL)
                if r1.status_code != 200:
                    continue
                soup1    = BeautifulSoup(r1.text, "html.parser")
                vs_input = soup1.find("input", {"name": "javax.faces.ViewState"})
                viewstate= vs_input["value"] if vs_input else ""
                del soup1, r1
                gc.collect()

                # Étape 2: POST AJAX
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
                hdrs2 = {
                    **HEADERS,
                    "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Faces-Request":    "partial/ajax",
                    "Referer":          PLATAFORMA_URL,
                }
                r2 = await c.post(PLATAFORMA_URL, data=post_data, headers=hdrs2)
                if r2.status_code != 200:
                    continue

                # Étape 3: Parser la réponse AJAX
                soup2     = BeautifulSoup(r2.text, "xml")
                html_frag = ""
                for upd in soup2.find_all("update"):
                    if "tabelaPesquisas" in upd.get("id",""):
                        html_frag = upd.get_text()
                        break
                del soup2, r2
                gc.collect()

                if not html_frag:
                    continue

                # Étape 4: Extraire les lignes
                soup3 = BeautifulSoup(html_frag, "html.parser")
                for row in soup3.select("table tbody tr"):
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    caae   = cells[0].get_text(strip=True)
                    title  = cells[1].get_text(strip=True)
                    status = cells[2].get_text(strip=True) if len(cells)>2 else "Aprovado"
                    sponsor= cells[3].get_text(strip=True) if len(cells)>3 else "—"
                    if not caae or not re.match(r"\d{8}\.\d\.\d{4}\.\d{4}", caae):
                        continue
                    is_be = bool(RE_BE.search(title+" "+sponsor)) or bool(RE_BE.search(term))
                    studies.append({
                        "id":            f"CAAE-{caae}",
                        "source":        "Plataforma Brasil (CONEP)",
                        "title":         title[:200],
                        "status":        norm_status(status),
                        "phase":         "NA",
                        "sponsor":       sponsor[:100],
                        "country":       "Brazil",
                        "studyType":     "INTERVENTIONAL",
                        "isBE":          is_be,
                        "isEU":          False,
                        "cro":           None,
                        "area":          "be" if is_be else "outros",
                        "conditions":    [],
                        "enrollment":    None,
                        "primEnd":       None,
                        "firstPosted":   None,
                        "latamCountries":["Brazil"],
                    })
                del soup3, html_frag
                gc.collect()

        except Exception as e:
            print(f"  Plataforma Brasil '{term}' error: {e}")

        added = add_to_cache(studies)
        total_added += added
        print(f"  Plataforma Brasil '{term}': {len(studies)} fetched, {added} new")
        del studies
        gc.collect()
        await asyncio.sleep(2)

    return total_added


# ══════════════════════════════════════════════════════════════
# SOURCE 5 : ANMAT / REPEC (Argentine)
# Administración Nacional de Medicamentos
# ══════════════════════════════════════════════════════════════
async def job_anmat():
    studies = []
    urls = [
        "http://www.anmat.gov.ar/comunicados/REGISTRO_ENSAYOS_CLINICOS.asp",
        "https://www.argentina.gob.ar/anmat/ensayosclinicos",
    ]
    terms = ["bioequivalencia", "biodisponibilidad", "farmacocinetica"]
    for term in terms:
        for url in urls:
            try:
                async with httpx.AsyncClient(timeout=30, headers=HEADERS) as c:
                    r = await c.get(url, params={"termino": term})
                    if r.status_code != 200:
                        continue
                    soup = BeautifulSoup(r.text, "html.parser")
                    for row in soup.select("table tr")[1:]:
                        cells = row.find_all("td")
                        if len(cells) < 2:
                            continue
                        sid   = cells[0].get_text(strip=True)
                        title = cells[1].get_text(strip=True)
                        if not sid:
                            continue
                        status = cells[2].get_text(strip=True) if len(cells)>2 else ""
                        sponsor= cells[3].get_text(strip=True) if len(cells)>3 else "—"
                        is_be  = bool(RE_BE.search(title+" "+sponsor))
                        studies.append({
                            "id":            f"ANMAT-{sid}",
                            "source":        "ANMAT/REPEC (Argentina)",
                            "title":         title[:200],
                            "status":        norm_status(status),
                            "phase":         "NA",
                            "sponsor":       sponsor[:100],
                            "country":       "Argentina",
                            "studyType":     "INTERVENTIONAL",
                            "isBE":          is_be,
                            "isEU":          bool(RE_EU.search(sponsor)),
                            "cro":           None,
                            "area":          "be" if is_be else classify(title),
                            "conditions":    [],
                            "enrollment":    None,
                            "primEnd":       None,
                            "firstPosted":   None,
                            "latamCountries":["Argentina"],
                        })
                    del soup, r
                    gc.collect()
                    break  # URL found, skip fallback
            except Exception as e:
                print(f"  ANMAT {url}/{term} error: {e}")
                continue
        await asyncio.sleep(0.5)

    added = add_to_cache(studies)
    print(f"  ANMAT: {len(studies)} fetched, {added} new (cache: {len(CACHE['studies'])})")
    del studies
    gc.collect()
    return added


# ══════════════════════════════════════════════════════════════
# SOURCE 6 : INS REPEC (Pérou)
# Instituto Nacional de Salud — registre péruvien officiel
# ══════════════════════════════════════════════════════════════
async def job_ins_repec():
    studies = []
    terms = ["bioequivalencia", "biodisponibilidad", "farmacocinetica"]
    for term in terms:
        try:
            async with httpx.AsyncClient(timeout=30, headers=HEADERS) as c:
                r = await c.get(
                    "https://ensayosclinicos-repec.ins.gob.pe/",
                    params={"search": term},
                )
                if r.status_code != 200:
                    continue
                soup = BeautifulSoup(r.text, "html.parser")
                rows = soup.select("table.table-striped tr, table.resultados tr")[1:]
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 2:
                        continue
                    sid   = cells[0].get_text(strip=True)
                    title = cells[1].get_text(strip=True)
                    if not sid:
                        continue
                    status = cells[2].get_text(strip=True) if len(cells)>2 else ""
                    sponsor= cells[3].get_text(strip=True) if len(cells)>3 else "—"
                    is_be  = bool(RE_BE.search(title+" "+sponsor))
                    studies.append({
                        "id":            f"REPEC-PE-{sid}",
                        "source":        "INS REPEC (Peru)",
                        "title":         title[:200],
                        "status":        norm_status(status),
                        "phase":         "NA",
                        "sponsor":       sponsor[:100],
                        "country":       "Peru",
                        "studyType":     "INTERVENTIONAL",
                        "isBE":          is_be,
                        "isEU":          bool(RE_EU.search(sponsor)),
                        "cro":           None,
                        "area":          "be" if is_be else classify(title),
                        "conditions":    [],
                        "enrollment":    None,
                        "primEnd":       None,
                        "firstPosted":   None,
                        "latamCountries":["Peru"],
                    })
                del soup, r
                gc.collect()
        except Exception as e:
            print(f"  INS REPEC '{term}' error: {e}")
        await asyncio.sleep(0.5)

    added = add_to_cache(studies)
    print(f"  INS REPEC: {len(studies)} fetched, {added} new (cache: {len(CACHE['studies'])})")
    del studies
    gc.collect()
    return added

# ══════════════════════════════════════════════════════════════
# SCHEDULER — exécute chaque job à 20 min d'intervalle
# ══════════════════════════════════════════════════════════════
async def run_job(job_name, job_type, param):
    """Exécute un seul mini-job et enregistre le résultat."""
    CACHE["current_job"] = job_name
    started = datetime.now()
    print(f"\n[JOB] {job_name} — {started:%H:%M:%S}")

    try:
        if job_type == "ctgov":
            added = await job_ctgov(param)
        elif job_type == "rebec":
            added = await job_rebec()
        elif job_type == "ictrp":
            added = await job_ictrp(param)
        elif job_type == "plataforma":
            added = await job_plataforma()
        elif job_type == "anmat":
            added = await job_anmat()
        elif job_type == "ins_repec":
            added = await job_ins_repec()
        else:
            added = 0

        duration = (datetime.now() - started).seconds
        log = {
            "job":      job_name,
            "added":    added,
            "total":    len(CACHE["studies"]),
            "duration": duration,
            "at":       started.isoformat(),
        }
        CACHE["jobs_done"].append(log)
        if len(CACHE["jobs_done"]) > 50:  # garder seulement les 50 derniers
            CACHE["jobs_done"] = CACHE["jobs_done"][-50:]
        print(f"[JOB DONE] {job_name}: +{added} études en {duration}s (total cache: {len(CACHE['studies'])})")

    except Exception as e:
        print(f"[JOB ERROR] {job_name}: {e}")
    finally:
        CACHE["current_job"] = None
        gc.collect()


# Verrou global — empêche deux cycles de tourner en même temps
_CYCLE_LOCK = asyncio.Lock()

async def run_full_cycle(interval_between_jobs=30):
    """
    Exécute tous les jobs en séquence avec un verrou global.
    interval_between_jobs : secondes entre chaque job
    """
    if _CYCLE_LOCK.locked():
        print("Cycle already running — skipping")
        return
    async with _CYCLE_LOCK:
        print(f"\n=== CYCLE START {datetime.now():%Y-%m-%d %H:%M:%S} ({len(JOBS)} jobs, {interval_between_jobs}s apart) ===")
        for i, (job_name, job_type, param) in enumerate(JOBS):
            await run_job(job_name, job_type, param)
            gc.collect()
            if i < len(JOBS) - 1:
                print(f"  Waiting {interval_between_jobs}s before next job...")
                await asyncio.sleep(interval_between_jobs)
        print(f"=== CYCLE DONE — {len(CACHE['studies'])} études au total ===\n")


async def nightly_scheduler():
    """
    Scheduler principal.
    - Au démarrage : cycle complet immédiat (30s entre jobs)
    - Chaque nuit à 2h00 UTC : cycle complet (20 min entre jobs)
    Le verrou _CYCLE_LOCK empêche tout chevauchement.
    """
    # Attendre que le serveur soit prêt
    await asyncio.sleep(5)

    # Premier remplissage au démarrage (30s entre jobs)
    await run_full_cycle(interval_between_jobs=30)

    # Scheduler nocturne : tous les jours à 2h00 UTC
    while True:
        now      = datetime.now()
        next_run = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)

        wait = (next_run - now).total_seconds()
        CACHE["next_job_at"] = next_run.isoformat()
        print(f"Next nightly cycle: {next_run:%Y-%m-%d %H:%M} (in {wait/3600:.1f}h)")
        await asyncio.sleep(wait)

        # Cycle nocturne (20 min entre jobs)
        await run_full_cycle(interval_between_jobs=JOB_INTERVAL)


@app.on_event("startup")
async def startup():
    print("QIMA Server v4.0 starting — mini-sync scheduler")
    asyncio.create_task(nightly_scheduler())


# ══════════════════════════════════════════════════════════════
# ENDPOINTS
# ══════════════════════════════════════════════════════════════
@app.get("/api/health")
async def health():
    s = list(CACHE["studies"].values())
    return {
        "status":      "ok",
        "version":     "4.0",
        "studies":     len(s),
        "be_studies":  sum(1 for x in s if x.get("isBE")),
        "updated_at":  CACHE["updated_at"].isoformat() if CACHE["updated_at"] else None,
        "current_job": CACHE["current_job"],
        "next_job_at": CACHE.get("next_job_at"),
        "jobs_done":   len(CACHE["jobs_done"]),
        "last_jobs":   CACHE["jobs_done"][-5:],
    }

@app.get("/api/studies")
async def get_studies(
    country:  str = Query("all"),
    area:     str = Query("all"),
    per_page: int = Query(5000),
    page:     int = Query(1),
):
    if not CACHE["studies"]:
        return {
            "status":  "syncing",
            "message": "Initial sync in progress — please retry in ~5 minutes",
            "data":    [],
        }

    s = list(CACHE["studies"].values())

    # Tri stable : recruiting first, puis par date
    STATUS_ORDER = {"RECRUITING":0,"NOT_YET_RECRUITING":1,"ACTIVE_NOT_RECRUITING":2,
                    "UNKNOWN":3,"COMPLETED":4,"TERMINATED":5}
    s.sort(key=lambda x:(STATUS_ORDER.get(x.get("status","UNKNOWN"),9),
                         x.get("firstPosted") or ""))

    if country != "all":
        s = [x for x in s if x.get("country")==country
             or country in x.get("latamCountries",[])]
    if area != "all":
        s = [x for x in s if x.get("area")==area]

    total  = len(s)
    offset = (page-1)*per_page
    return {
        "status":      "ok",
        "total":       total,
        "count":       len(s[offset:offset+per_page]),
        "updated_at":  CACHE["updated_at"].isoformat() if CACHE["updated_at"] else None,
        "current_job": CACHE["current_job"],
        "data":        s[offset:offset+per_page],
    }

@app.post("/api/sync")
async def trigger_sync(bg: BackgroundTasks):
    """Déclenche manuellement un cycle complet (10s entre jobs)."""
    bg.add_task(run_full_cycle, 10)
    return {"status":"started","jobs":len(JOBS)}

@app.post("/api/sync/{job_index}")
async def trigger_one_job(job_index: int, bg: BackgroundTasks):
    """Déclenche manuellement un seul job (0-7)."""
    if job_index < 0 or job_index >= len(JOBS):
        return {"error": f"job_index must be 0-{len(JOBS)-1}"}
    name, type_, param = JOBS[job_index]
    bg.add_task(run_job, name, type_, param)
    return {"status":"started","job":name}

@app.get("/api/stats")
async def stats():
    s = list(CACHE["studies"].values())
    return {
        "total":      len(s),
        "be":         sum(1 for x in s if x.get("isBE")),
        "eu":         sum(1 for x in s if x.get("isEU")),
        "with_cro":   sum(1 for x in s if x.get("cro")),
        "by_country": dict(Counter(x.get("country") for x in s).most_common(15)),
        "by_area":    dict(Counter(x.get("area")    for x in s).most_common(15)),
        "by_source":  dict(Counter(x.get("source")  for x in s).most_common(10)),
        "by_status":  dict(Counter(x.get("status")  for x in s).most_common(10)),
        "jobs_log":   CACHE["jobs_done"][-10:],
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT",8000)))
