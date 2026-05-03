"""
QIMA Life Sciences — LatAm BA/BE + Clinical Trials API
Scrapes: REBEC (Brazil), WHO ICTRP, ANMAT (Argentina), ClinicalTrials.gov
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import os

app = FastAPI(title="QIMA LatAm Trials API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Simple in-memory cache ───────────────────────────────
_cache: dict = {}
CACHE_TTL = timedelta(hours=12)

def get_cache(key: str):
    if key in _cache:
        ts, data = _cache[key]
        if datetime.now() - ts < CACHE_TTL:
            return data
    return None

def set_cache(key: str, data):
    _cache[key] = (datetime.now(), data)

# ─── HTTP client factory ──────────────────────────────────
def make_client():
    return httpx.AsyncClient(
        timeout=30,
        follow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; QIMA-LifeSciences-Bot/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/json,*/*",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
    )

# ═══════════════════════════════════════════════════════════
# SOURCE 1: REBEC (Brazilian Clinical Trials Registry)
# ═══════════════════════════════════════════════════════════
async def fetch_rebec_api(query: str = "bioequivalencia") -> list:
    """Try REBEC REST API first"""
    studies = []
    try:
        async with make_client() as client:
            url = f"https://ensaiosclinicos.gov.br/api/v1/rg/"
            params = {"q": query, "format": "json", "page_size": 500}
            resp = await client.get(url, params=params)

            if resp.status_code == 200:
                data = resp.json()
                items = data.get("results", data) if isinstance(data, dict) else data
                if isinstance(items, list):
                    for r in items:
                        s = normalize_rebec(r)
                        if s: studies.append(s)
                    print(f"REBEC API → {len(studies)} studies")
    except Exception as e:
        print(f"REBEC API failed: {e}")
    return studies


async def fetch_rebec_web(query: str = "bioequivalencia") -> list:
    """Scrape REBEC web interface as fallback"""
    studies = []
    try:
        async with make_client() as client:
            pages_to_try = [1, 2, 3, 4, 5]
            for page in pages_to_try:
                url = "https://ensaiosclinicos.gov.br/rg/"
                params = {"q": query, "page": page}
                resp = await client.get(url, params=params)
                if resp.status_code != 200:
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select("table.table tbody tr, .result-item, li.trial")

                if not rows:
                    # Try alternative selectors
                    rows = soup.select("tr")

                found_on_page = 0
                for row in rows:
                    cells = row.select("td")
                    if len(cells) < 3:
                        continue

                    try:
                        reg_id = cells[0].get_text(strip=True)
                        title  = cells[1].get_text(strip=True) if len(cells) > 1 else "N/A"
                        status = cells[2].get_text(strip=True) if len(cells) > 2 else "Unknown"

                        if not reg_id or not re.match(r"RBR-|U\d{4}", reg_id):
                            continue

                        studies.append({
                            "id":         reg_id,
                            "source":     "REBEC",
                            "title":      title,
                            "status":     map_rebec_status(status),
                            "phase":      "NA",
                            "sponsor":    cells[3].get_text(strip=True) if len(cells) > 3 else "—",
                            "country":    "Brazil",
                            "studyType":  "INTERVENTIONAL",
                            "isBE":       True,
                            "area":       "be",
                            "conditions": [],
                            "enrollment": None,
                            "primEnd":    None,
                            "firstPosted": None,
                            "latamCountries": ["Brazil"],
                        })
                        found_on_page += 1
                    except Exception:
                        continue

                print(f"REBEC web page {page} → {found_on_page} studies")
                if found_on_page == 0:
                    break

                await asyncio.sleep(0.5)  # Be polite

    except Exception as e:
        print(f"REBEC web scrape failed: {e}")
    return studies


def normalize_rebec(r: dict) -> dict | None:
    """Normalize a REBEC API record"""
    try:
        reg_id = (r.get("registro_anvisa") or r.get("registro") or
                  r.get("trial_id") or r.get("id") or "")
        if not reg_id:
            return None

        raw_status = (r.get("recrutamento") or r.get("recruitment_status") or
                      r.get("status") or "Unknown")

        return {
            "id":           str(reg_id),
            "source":       "REBEC",
            "title":        (r.get("titulo_publico") or r.get("scientific_title") or
                             r.get("public_title") or "N/A"),
            "status":       map_rebec_status(raw_status),
            "phase":        (r.get("fase") or r.get("phase") or "NA"),
            "sponsor":      (r.get("patrocinador_primario") or r.get("sponsor") or
                             r.get("primary_sponsor") or "—"),
            "country":      "Brazil",
            "studyType":    "INTERVENTIONAL",
            "isBE":         True,
            "area":         "be",
            "conditions":   r.get("condicao_saude_primaria", []),
            "enrollment":   r.get("tamanho_amostra") or r.get("target_size"),
            "primEnd":      r.get("data_conclusao") or r.get("primary_completion_date"),
            "firstPosted":  r.get("data_registro") or r.get("date_registration"),
            "latamCountries": ["Brazil"],
        }
    except Exception as e:
        print(f"REBEC normalize error: {e}")
        return None


def map_rebec_status(s: str) -> str:
    s = (s or "").lower()
    if any(x in s for x in ["recrutando", "recruiting", "aberto", "open"]):
        return "RECRUITING"
    if any(x in s for x in ["não iniciado", "nao iniciado", "not yet", "planned"]):
        return "NOT_YET_RECRUITING"
    if any(x in s for x in ["concluido", "concluído", "completed", "encerrado"]):
        return "COMPLETED"
    if any(x in s for x in ["suspenso", "suspended"]):
        return "SUSPENDED"
    if any(x in s for x in ["interrompido", "terminated"]):
        return "TERMINATED"
    return "UNKNOWN"


# ═══════════════════════════════════════════════════════════
# SOURCE 2: WHO ICTRP (covers all LatAm registries)
# ═══════════════════════════════════════════════════════════
ICTRP_COUNTRIES = {
    "Brazil":    "BR",
    "Argentina": "AR",
    "Mexico":    "MX",
    "Chile":     "CL",
    "Colombia":  "CO",
    "Peru":      "PE",
    "Uruguay":   "UY",
    "Bolivia":   "BO",
    "Paraguay":  "PY",
    "Ecuador":   "EC",
}

async def fetch_ictrp(country: str = "Brazil", query: str = "bioequivalence") -> list:
    """Scrape WHO ICTRP for LatAm BA/BE studies"""
    studies = []
    try:
        async with make_client() as client:
            url = "https://trialsearch.who.int/Default.aspx"
            params = {
                "Query":   f"{query}",
                "Country": country,
            }
            resp = await client.get(url, params=params, timeout=40)
            if resp.status_code != 200:
                print(f"ICTRP HTTP {resp.status_code} for {country}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # Try different table selectors
            rows = (soup.select("table#GridView1 tr") or
                    soup.select("table.trials tr") or
                    soup.select("tr.trialRow") or
                    soup.select("table tr")[1:])  # skip header

            for row in rows:
                cells = row.select("td")
                if len(cells) < 4:
                    continue
                try:
                    trial_id = cells[0].get_text(strip=True)
                    if not trial_id or trial_id.lower() in ("trial id", "id"):
                        continue

                    title    = cells[1].get_text(strip=True) if len(cells) > 1 else "N/A"
                    status   = cells[3].get_text(strip=True) if len(cells) > 3 else "Unknown"
                    sponsor  = cells[4].get_text(strip=True) if len(cells) > 4 else "—"

                    is_be = any(t in (trial_id + title).lower()
                                for t in ["bioequivalen", "bioavailab", "pharmacokinetic"])

                    studies.append({
                        "id":           trial_id,
                        "source":       "WHO ICTRP",
                        "title":        title,
                        "status":       map_ictrp_status(status),
                        "phase":        "NA",
                        "sponsor":      sponsor,
                        "country":      country,
                        "studyType":    "INTERVENTIONAL",
                        "isBE":         is_be,
                        "area":         "be" if is_be else "outros",
                        "conditions":   [],
                        "enrollment":   None,
                        "primEnd":      None,
                        "firstPosted":  None,
                        "latamCountries": [country],
                    })
                except Exception:
                    continue

            print(f"WHO ICTRP {country}/{query} → {len(studies)} studies")

    except Exception as e:
        print(f"WHO ICTRP error ({country}): {e}")
    return studies


def map_ictrp_status(s: str) -> str:
    s = (s or "").lower()
    if "recruit" in s:        return "RECRUITING"
    if "not yet" in s:        return "NOT_YET_RECRUITING"
    if "complet" in s:        return "COMPLETED"
    if "terminat" in s:       return "TERMINATED"
    if "suspend" in s:        return "SUSPENDED"
    return "UNKNOWN"


# ═══════════════════════════════════════════════════════════
# SOURCE 3: ClinicalTrials.gov (server-side — no CORS limit)
# ═══════════════════════════════════════════════════════════
async def fetch_ct_gov(country: str, term: str = "bioequivalence", size: int = 500) -> list:
    """Fetch from ClinicalTrials.gov API v2"""
    studies = []
    try:
        async with make_client() as client:
            params = {
                "query.locn": country,
                "query.term": term,
                "pageSize":   str(size),
            }
            resp = await client.get("https://clinicaltrials.gov/api/v2/studies", params=params)
            if resp.status_code != 200:
                return []

            raw_studies = resp.json().get("studies", [])
            for r in raw_studies:
                p  = r.get("protocolSection", {})
                id_m  = p.get("identificationModule", {})
                st_m  = p.get("statusModule", {})
                sp_m  = p.get("sponsorCollaboratorsModule", {})
                de_m  = p.get("designModule", {})
                co_m  = p.get("conditionsModule", {})
                cl_m  = p.get("contactsLocationsModule", {})

                locs    = cl_m.get("locations", [])
                countries = list({l.get("country","") for l in locs if l.get("country")})
                lat_c   = [c for c in countries if c in ICTRP_COUNTRIES]

                nct_id  = id_m.get("nctId", "")
                title   = id_m.get("briefTitle", "N/A")
                txt     = (title + " " + " ".join(co_m.get("conditions", []))).lower()
                is_be   = bool(re.search(r"bioequivalen|bioavailab|pharmacokinetic", txt))

                studies.append({
                    "id":           nct_id,
                    "source":       "ClinicalTrials.gov",
                    "title":        title,
                    "status":       st_m.get("overallStatus", "UNKNOWN"),
                    "phase":        (de_m.get("phases") or ["NA"])[0].replace("PHASE",""),
                    "sponsor":      sp_m.get("leadSponsor", {}).get("name", "—"),
                    "country":      country,
                    "studyType":    de_m.get("studyType", "N/A"),
                    "isBE":         is_be,
                    "area":         "be" if is_be else "outros",
                    "conditions":   co_m.get("conditions", []),
                    "enrollment":   de_m.get("enrollmentInfo", {}).get("count"),
                    "primEnd":      st_m.get("primaryCompletionDateStruct", {}).get("date"),
                    "firstPosted":  st_m.get("studyFirstSubmitDate"),
                    "latamCountries": lat_c or [country],
                })

            print(f"ClinicalTrials.gov {country}/{term} → {len(studies)} studies")
    except Exception as e:
        print(f"ClinicalTrials.gov error ({country}): {e}")
    return studies


# ═══════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════
@app.get("/api/health")
async def health():
    cached_keys = [k for k in _cache if datetime.now() - _cache[k][0] < CACHE_TTL]
    return {
        "status":    "ok",
        "time":      datetime.now().isoformat(),
        "cached":    cached_keys,
    }


@app.get("/api/be-trials")
async def get_be_trials(
    country: str = Query("all", description="Country or 'all'"),
    refresh: bool = Query(False),
    source: str  = Query("all", description="rebec | ictrp | ctgov | all")
):
    cache_key = f"be_{country}_{source}"

    if not refresh:
        cached = get_cache(cache_key)
        if cached is not None:
            return {"status": "cached", "count": len(cached), "data": cached}

    countries = list(ICTRP_COUNTRIES.keys()) if country == "all" else [country]

    tasks = []

    # Always include REBEC for Brazil (most important source)
    if source in ("all", "rebec") and "Brazil" in countries:
        tasks.append(fetch_rebec_api("bioequivalencia"))
        tasks.append(fetch_rebec_api("bioequivalência"))
        tasks.append(fetch_rebec_api("equivalência farmacêutica"))
        tasks.append(fetch_rebec_web("bioequivalencia"))

    # WHO ICTRP for all countries
    if source in ("all", "ictrp"):
        for c in countries:
            tasks.append(fetch_ictrp(c, "bioequivalence"))
            tasks.append(fetch_ictrp(c, "bioavailability"))

    # ClinicalTrials.gov server-side (no CORS limitation here)
    if source in ("all", "ctgov"):
        be_terms = ["bioequivalence", "bioavailability pharmacokinetic", "BA/BE"]
        for c in countries:
            for t in be_terms:
                tasks.append(fetch_ct_gov(c, t, 300))

    print(f"Running {len(tasks)} parallel tasks...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Merge + deduplicate
    seen_ids: set = set()
    all_studies: list = []

    for result in results:
        if isinstance(result, list):
            for s in result:
                if s and s.get("id"):
                    norm_id = s["id"].strip().upper()
                    if norm_id not in seen_ids:
                        seen_ids.add(norm_id)
                        all_studies.append(s)

    # Sort: recruiting first, then by date
    status_order = {"RECRUITING": 0, "NOT_YET_RECRUITING": 1,
                    "ACTIVE_NOT_RECRUITING": 2, "UNKNOWN": 3,
                    "COMPLETED": 4, "TERMINATED": 5}
    all_studies.sort(key=lambda x: (
        status_order.get(x.get("status", "UNKNOWN"), 99),
        x.get("firstPosted") or ""
    ), reverse=False)

    set_cache(cache_key, all_studies)

    return {
        "status":     "live",
        "count":      len(all_studies),
        "fetched_at": datetime.now().isoformat(),
        "data":       all_studies,
    }


@app.get("/api/clinical-trials")
async def get_clinical_trials(
    country: str = Query("Brazil"),
    term:    str = Query(""),
    size:    int = Query(500),
):
    """Proxy endpoint for ClinicalTrials.gov — avoids CORS on client side"""
    cache_key = f"ct_{country}_{term}_{size}"
    cached = get_cache(cache_key)
    if cached is not None:
        return {"status": "cached", "count": len(cached), "data": cached}

    studies = await fetch_ct_gov(country, term, size)
    set_cache(cache_key, studies)
    return {"status": "live", "count": len(studies), "data": studies}


# Run with: uvicorn main:app --host 0.0.0.0 --port $PORT
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
