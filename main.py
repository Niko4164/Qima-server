"""
QIMA Life Sciences — LatAm BA/BE + Clinical Trials Intelligence Server v2.0
"""
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx, asyncio, re, os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

app = FastAPI(title="QIMA LatAm Trials API", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_cache: dict = {}
CACHE_TTL = timedelta(hours=12)

def get_cache(key):
    e = _cache.get(key)
    if e and datetime.now() - e["ts"] < CACHE_TTL:
        return e["data"]
    return None

def set_cache(key, data):
    _cache[key] = {"ts": datetime.now(), "data": data}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/json,*/*;q=0.9",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
}

def make_client(timeout=30):
    return httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=HEADERS)

LATAM = ["Brazil","Argentina","Mexico","Chile","Colombia","Peru","Uruguay","Bolivia","Paraguay","Ecuador"]

def is_be(text):
    t = text.lower()
    return any(p in t for p in [
        "bioequivalen","bioavailabilit","biodisponibilid",
        "pharmacokineti","farmacocinéti","farmacocineti",
        "equivalência farmacêutica","equivalencia farmaceutica","ba/be",
    ])

def norm_status(s):
    s = (s or "").lower()
    if any(x in s for x in ["recruit","recrutando","aberto","open","ativo","em andamento"]): return "RECRUITING"
    if any(x in s for x in ["not yet","não iniciado","planned","aprovado"]): return "NOT_YET_RECRUITING"
    if any(x in s for x in ["active, not","fechado para recrutamento"]): return "ACTIVE_NOT_RECRUITING"
    if any(x in s for x in ["complet","concluí","encerrado","finalizado"]): return "COMPLETED"
    if any(x in s for x in ["terminat","interrompido","cancelado"]): return "TERMINATED"
    if any(x in s for x in ["suspend","suspenso"]): return "SUSPENDED"
    return "UNKNOWN"

def mk(sid, source, title, status, sponsor, country, phase="NA",
       enrollment=None, prim_end=None, first_posted=None, conditions=None, is_be_=True):
    return {
        "id": sid, "source": source, "title": title or "N/A",
        "status": norm_status(status), "phase": phase,
        "sponsor": sponsor or "—", "country": country,
        "studyType": "INTERVENTIONAL", "isBE": is_be_,
        "area": "be" if is_be_ else "outros",
        "conditions": conditions or [],
        "enrollment": enrollment, "primEnd": prim_end,
        "firstPosted": first_posted, "latamCountries": [country],
    }

# ══ SOURCE 1 : PLATAFORMA BRASIL (CONEP/CEP) ══════════════════════════════
# Plataforma Brasil est le système officiel brésilien d'approbation éthique.
# Toute étude BA/BE doit y être approuvée AVANT d'être conduite au Brésil.
# C'est la source la plus précoce — les études y apparaissent avant REBEC.
# Techniquement c'est du JSF (JavaServer Faces) : on récupère d'abord le
# ViewState (jeton de session) puis on envoie un POST AJAX.

async def fetch_plataforma(term):
    studies = []
    url = "https://plataformabrasil.saude.gov.br/visao/publico/indexPublico.jsf"
    try:
        async with make_client(40) as c:
            # Étape 1 : GET initial → récupération du ViewState JSF
            r = await c.get(url)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            vs_inp = soup.find("input", {"name": "javax.faces.ViewState"})
            viewstate = vs_inp["value"] if vs_inp else ""

            # Étape 2 : POST AJAX avec les paramètres de recherche
            data = {
                "javax.faces.partial.ajax": "true",
                "javax.faces.source": "formBusca:btnBuscar",
                "javax.faces.partial.execute": "@all",
                "javax.faces.partial.render": "formBusca:tabelaPesquisas formBusca:msgErro",
                "formBusca:btnBuscar": "formBusca:btnBuscar",
                "formBusca": "formBusca",
                "formBusca:tituloPesquisa": term,
                "formBusca:situacao": "",
                "formBusca:areaTematica": "",
                "javax.faces.ViewState": viewstate,
            }
            hdrs = {**HEADERS,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Faces-Request": "partial/ajax",
                "Referer": url,
            }
            r2 = await c.post(url, data=data, headers=hdrs)
            if r2.status_code != 200: return []

            # Étape 3 : Parsing du XML AJAX → extraction du HTML interne
            soup2 = BeautifulSoup(r2.text, "xml")
            html_fragment = ""
            for upd in soup2.find_all("update"):
                if "tabelaPesquisas" in upd.get("id", ""):
                    html_fragment = upd.get_text()
                    break
            if not html_fragment:
                html_fragment = r2.text

            # Étape 4 : Extraction des lignes du tableau
            soup3 = BeautifulSoup(html_fragment, "html.parser")
            for row in soup3.select("table tbody tr, tr.rich-table-row"):
                cells = row.find_all("td")
                if len(cells) < 2: continue
                caae   = cells[0].get_text(strip=True)
                title  = cells[1].get_text(strip=True)
                status = cells[2].get_text(strip=True) if len(cells) > 2 else "Aprovado"
                sponsor= cells[3].get_text(strip=True) if len(cells) > 3 else "—"
                # Validation du format CAAE : 8 chiffres.1 chiffre.4 chiffres.4 chiffres
                if not caae or not re.match(r"\d{8}\.\d\.\d{4}\.\d{4}", caae):
                    continue
                studies.append(mk(f"CAAE-{caae}", "Plataforma Brasil (CONEP)", title,
                                  status, sponsor, "Brazil",
                                  is_be_=is_be(title+" "+sponsor) or is_be(term)))
        print(f"Plataforma Brasil '{term}' → {len(studies)}")
    except Exception as e:
        print(f"Plataforma Brasil error '{term}': {e}")
    return studies

# ══ SOURCE 2 : REBEC ══════════════════════════════════════════════════════
# REBEC = Registro Brasileiro de Ensaios Clínicos.
# Registre prospectif officiel brésilien géré par l'ANVISA.
# Dispose d'une API REST semi-officielle accessible sans authentification.

async def fetch_rebec(term):
    studies = []
    try:
        async with make_client() as c:
            r = await c.get("https://ensaiosclinicos.gov.br/api/v1/rg/",
                            params={"q": term, "format": "json", "page_size": 500})
            if r.status_code != 200: return await _rebec_web(term)
            data = r.json()
            items = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                rid = str(item.get("registro_anvisa") or item.get("registro") or item.get("id","")).strip()
                if not rid: continue
                title = item.get("titulo_publico") or item.get("scientific_title") or "N/A"
                studies.append(mk(
                    f"REBEC-{rid}", "REBEC", title,
                    item.get("recrutamento",""), item.get("patrocinador_primario","—"), "Brazil",
                    enrollment=item.get("tamanho_amostra"),
                    prim_end=item.get("data_conclusao"),
                    first_posted=item.get("data_registro"),
                    conditions=item.get("condicao_saude_primaria",[]),
                    is_be_=is_be(title) or is_be(term),
                ))
        print(f"REBEC '{term}' → {len(studies)}")
    except Exception as e:
        print(f"REBEC error '{term}': {e}")
        return await _rebec_web(term)
    return studies

async def _rebec_web(term):
    studies = []
    try:
        async with make_client() as c:
            for page in range(1, 6):
                r = await c.get("https://ensaiosclinicos.gov.br/rg/", params={"q":term,"page":page})
                if r.status_code != 200: break
                soup = BeautifulSoup(r.text, "html.parser")
                rows = soup.select("table tbody tr")
                if not rows: break
                found = 0
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 2: continue
                    rid = cells[0].get_text(strip=True)
                    if not re.match(r"RBR-|U\d{4}|REC", rid): continue
                    title = cells[1].get_text(strip=True) if len(cells)>1 else "N/A"
                    studies.append(mk(f"REBEC-{rid}","REBEC",title,
                                      cells[2].get_text(strip=True) if len(cells)>2 else "",
                                      cells[3].get_text(strip=True) if len(cells)>3 else "—",
                                      "Brazil", is_be_=is_be(title)))
                    found += 1
                if found == 0: break
                await asyncio.sleep(0.3)
    except Exception as e:
        print(f"REBEC web fallback error: {e}")
    return studies

# ══ SOURCE 3 : WHO ICTRP ══════════════════════════════════════════════════
# WHO ICTRP agrège 17 registres nationaux (REPEC Argentine, RPCEC Équateur/Cuba...)
# C'est la source clé pour les pays LatAm sans registre public API dédié.

async def fetch_ictrp(country, term="bioequivalence"):
    studies = []
    try:
        async with make_client(40) as c:
            r = await c.get("https://trialsearch.who.int/Trial2.aspx",
                            params={"SearchTerms": term, "Country": country})
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            rows = (soup.select("table#GridView1 tr")[1:] or
                    soup.select("tr.searchResultsRow") or
                    soup.select("table tr")[1:])
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3: continue
                tid = cells[0].get_text(strip=True)
                if not tid or tid.lower() in ("trial id","id",""): continue
                title   = cells[1].get_text(strip=True) if len(cells)>1 else "N/A"
                status  = cells[3].get_text(strip=True) if len(cells)>3 else ""
                sponsor = cells[4].get_text(strip=True) if len(cells)>4 else "—"
                studies.append(mk(tid, f"WHO ICTRP ({country})", title, status,
                                  sponsor, country,
                                  is_be_=is_be(tid+" "+title+" "+sponsor) or is_be(term)))
        print(f"WHO ICTRP {country}/'{term}' → {len(studies)}")
    except Exception as e:
        print(f"WHO ICTRP error ({country}/{term}): {e}")
    return studies

# ══ SOURCE 4 : ANMAT / REPEC Argentine ════════════════════════════════════
# ANMAT = Administración Nacional de Medicamentos, Argentine.
# Équivalent argentin de l'ANVISA brésilienne.

async def fetch_anmat(term="bioequivalencia"):
    studies = []
    try:
        async with make_client(35) as c:
            urls = [
                "http://www.anmat.gov.ar/comunicados/REGISTRO_ENSAYOS_CLINICOS.asp",
                "https://www.argentina.gob.ar/anmat/ensayosclinicos",
            ]
            for url in urls:
                try:
                    r = await c.get(url, params={"termino":term})
                    if r.status_code == 200:
                        soup = BeautifulSoup(r.text, "html.parser")
                        for row in soup.select("table tr")[1:]:
                            cells = row.find_all("td")
                            if len(cells) < 2: continue
                            sid   = cells[0].get_text(strip=True)
                            title = cells[1].get_text(strip=True)
                            if not sid: continue
                            studies.append(mk(f"ANMAT-{sid}","ANMAT/REPEC (Argentina)",
                                              title,
                                              cells[2].get_text(strip=True) if len(cells)>2 else "",
                                              cells[3].get_text(strip=True) if len(cells)>3 else "—",
                                              "Argentina", is_be_=is_be(title)))
                        break
                except Exception:
                    continue
        print(f"ANMAT '{term}' → {len(studies)}")
    except Exception as e:
        print(f"ANMAT error: {e}")
    return studies

# ══ SOURCE 5 : INS REPEC Pérou ════════════════════════════════════════════
# Instituto Nacional de Salud du Pérou — registre officiel péruvien.

async def fetch_ins_repec(term="bioequivalencia"):
    studies = []
    try:
        async with make_client(35) as c:
            r = await c.get("https://ensayosclinicos-repec.ins.gob.pe/",
                            params={"search":term})
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            for row in soup.select("table.table-striped tr, table.resultados tr")[1:]:
                cells = row.find_all("td")
                if len(cells) < 2: continue
                sid   = cells[0].get_text(strip=True)
                title = cells[1].get_text(strip=True)
                if not sid: continue
                studies.append(mk(f"REPEC-PE-{sid}","INS REPEC (Peru)",title,
                                  cells[2].get_text(strip=True) if len(cells)>2 else "",
                                  cells[3].get_text(strip=True) if len(cells)>3 else "—",
                                  "Peru", is_be_=is_be(title)))
        print(f"INS REPEC '{term}' → {len(studies)}")
    except Exception as e:
        print(f"INS REPEC error: {e}")
    return studies

# ══ SOURCE 6 : ClinicalTrials.gov (proxy serveur) ═════════════════════════
# Interrogé côté serveur pour éviter les restrictions CORS du navigateur.
# Un navigateur ne peut pas appeler l'API CTgov directement depuis certains hébergements.

CT_BE_TERMS = ["bioequivalence","bioavailability","pharmacokinetics generic","BA/BE","bioequivalência"]

async def fetch_ctgov(country, term="", size=500):
    studies = []
    try:
        async with make_client(30) as c:
            params = {"query.locn": country, "pageSize": str(size)}
            if term: params["query.term"] = term
            r = await c.get("https://clinicaltrials.gov/api/v2/studies", params=params)
            if r.status_code != 200: return []
            for raw in r.json().get("studies", []):
                p  = raw.get("protocolSection",{})
                id_= p.get("identificationModule",{})
                st_= p.get("statusModule",{})
                sp_= p.get("sponsorCollaboratorsModule",{})
                de_= p.get("designModule",{})
                co_= p.get("conditionsModule",{})
                cl_= p.get("contactsLocationsModule",{})
                locs = cl_.get("locations",[])
                latam_c = list({l.get("country","") for l in locs if l.get("country","") in LATAM})
                phases  = de_.get("phases",[])
                nct_id  = id_.get("nctId","")
                title   = id_.get("briefTitle","N/A")
                txt     = (title+" "+" ".join(co_.get("conditions",[]))).lower()
                phase   = phases[0].replace("PHASE","").strip() if phases else "NA"
                studies.append({
                    "id": nct_id, "source": "ClinicalTrials.gov", "title": title,
                    "status": st_.get("overallStatus","UNKNOWN"), "phase": phase,
                    "sponsor": sp_.get("leadSponsor",{}).get("name","—"),
                    "country": country, "studyType": de_.get("studyType","N/A"),
                    "isBE": is_be(txt) or (term and is_be(term)),
                    "area": "be" if (is_be(txt) or (term and is_be(term))) else "outros",
                    "conditions": co_.get("conditions",[]),
                    "enrollment": de_.get("enrollmentInfo",{}).get("count"),
                    "primEnd":    st_.get("primaryCompletionDateStruct",{}).get("date"),
                    "firstPosted":st_.get("studyFirstSubmitDate"),
                    "latamCountries": latam_c or [country],
                })
        print(f"ClinicalTrials.gov {country}/'{term}' → {len(studies)}")
    except Exception as e:
        print(f"ClinicalTrials.gov error ({country}/{term}): {e}")
    return studies

# ══ FUSION & DÉDUPLICATION ════════════════════════════════════════════════
SOURCE_PRIORITY = {
    "Plataforma Brasil (CONEP)": 1,
    "REBEC": 2,
    "ClinicalTrials.gov": 3,
    "WHO ICTRP": 4,
    "ANMAT/REPEC (Argentina)": 5,
    "INS REPEC (Peru)": 6,
}

def merge(all_results):
    seen = {}
    for lst in all_results:
        if not isinstance(lst, list): continue
        for s in lst:
            if not s or not s.get("id"): continue
            nid = s["id"].strip().upper()
            if nid not in seen:
                seen[nid] = s
            else:
                ep = SOURCE_PRIORITY.get(seen[nid].get("source",""), 99)
                np = SOURCE_PRIORITY.get(s.get("source",""), 99)
                if np < ep:
                    seen[nid] = {**seen[nid], **{k: v for k, v in s.items() if v}}
    order = {"RECRUITING":0,"NOT_YET_RECRUITING":1,"ACTIVE_NOT_RECRUITING":2,
             "UNKNOWN":3,"COMPLETED":4,"TERMINATED":5,"SUSPENDED":6}
    return sorted(seen.values(),
                  key=lambda x:(order.get(x.get("status","UNKNOWN"),99), x.get("firstPosted") or ""))

# ══ ENDPOINTS ═════════════════════════════════════════════════════════════

@app.get("/api/health")
async def health():
    return {
        "status": "ok", "version": "2.0",
        "time": datetime.now().isoformat(),
        "cached_keys": list(_cache.keys()),
        "sources": ["Plataforma Brasil (CONEP/CEP)","REBEC","WHO ICTRP (17 registres)",
                    "ANMAT/REPEC Argentina","INS REPEC Peru","ClinicalTrials.gov (proxy)"],
    }

@app.get("/api/be-trials")
async def get_be_trials(
    country: str  = Query("all"),
    refresh: bool = Query(False),
    source:  str  = Query("all"),
):
    cache_key = f"be_v2_{country}_{source}"
    if not refresh:
        cached = get_cache(cache_key)
        if cached is not None:
            return {"status":"cached","count":len(cached),
                    "cached_at":_cache[cache_key]["ts"].isoformat(),"data":cached}

    countries = LATAM if country == "all" else [country]
    tasks = []

    for c in countries:
        # Brésil — toutes sources disponibles
        if c == "Brazil" and source in ("all","plataforma"):
            for t in ["bioequivalência","biodisponibilidade","equivalência farmacêutica",
                      "farmacocinética","bioequivalencia"]:
                tasks.append(fetch_plataforma(t))
        if c == "Brazil" and source in ("all","rebec"):
            for t in ["bioequivalencia","bioequivalência","biodisponibilidade","farmacocinética"]:
                tasks.append(fetch_rebec(t))

        # Argentine — ANMAT
        if c == "Argentina" and source in ("all","anmat"):
            for t in ["bioequivalencia","biodisponibilidad"]:
                tasks.append(fetch_anmat(t))

        # Pérou — INS REPEC
        if c == "Peru" and source in ("all","ins"):
            for t in ["bioequivalencia","biodisponibilidad"]:
                tasks.append(fetch_ins_repec(t))

        # WHO ICTRP — tous pays
        if source in ("all","ictrp"):
            tasks.append(fetch_ictrp(c, "bioequivalence"))
            tasks.append(fetch_ictrp(c, "bioavailability"))
            if c in ("Brazil","Argentina","Mexico","Chile","Colombia","Peru"):
                tasks.append(fetch_ictrp(c, "pharmacokinetics"))

        # ClinicalTrials.gov — tous pays via proxy
        if source in ("all","ctgov"):
            for t in CT_BE_TERMS[:3]:
                tasks.append(fetch_ctgov(c, t, 300))

    print(f"Running {len(tasks)} parallel tasks across {len(countries)} countries...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_studies = merge([r for r in results if isinstance(r, list)])
    errors = [str(r) for r in results if isinstance(r, Exception)]
    if errors: print(f"Errors: {errors[:3]}")

    set_cache(cache_key, all_studies)
    return {
        "status": "live", "count": len(all_studies),
        "fetched_at": datetime.now().isoformat(),
        "tasks_run": len(tasks), "tasks_ok": len(tasks)-len(errors),
        "data": all_studies,
    }

@app.get("/api/clinical-trials")
async def get_clinical_trials(
    country: str = Query("Brazil"),
    term:    str = Query(""),
    size:    int = Query(500),
):
    cache_key = f"ct_{country}_{term}_{size}"
    cached = get_cache(cache_key)
    if cached: return {"status":"cached","count":len(cached),"data":cached}
    studies = await fetch_ctgov(country, term, size)
    set_cache(cache_key, studies)
    return {"status":"live","count":len(studies),"data":studies}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
