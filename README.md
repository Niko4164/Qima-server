# QIMA Life Sciences — LatAm Trials API

Serveur Python qui agrège les données BA/BE de :
- **REBEC** — Registro Brasileiro de Ensaios Clínicos
- **WHO ICTRP** — International Clinical Trials Registry Platform
- **ClinicalTrials.gov** — via proxy server-side (pas de CORS)

---

## Déploiement sur Railway (5 minutes)

### Étape 1 — Mettre les fichiers sur GitHub
1. Va sur **github.com** → New repository → nomme-le `qima-server`
2. Upload les 3 fichiers : `main.py`, `requirements.txt`, `Procfile`
3. Clique **Commit changes**

### Étape 2 — Déployer sur Railway
1. Va sur **railway.app** → Sign up (gratuit, avec ton compte GitHub)
2. Clique **New Project** → **Deploy from GitHub repo**
3. Sélectionne `qima-server`
4. Railway détecte automatiquement Python et lance le déploiement
5. Attends 2 minutes → clique sur ton projet → onglet **Settings** → **Domains**
6. Clique **Generate Domain** → tu obtiens une URL comme `qima-server-production.up.railway.app`

### Étape 3 — Connecter l'app
Dans le fichier `index.html` de ton app principale, remplace :
```
const BACKEND_URL = null;
```
par :
```
const BACKEND_URL = "https://qima-server-production.up.railway.app";
```

---

## Endpoints disponibles

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Statut du serveur |
| `GET /api/be-trials` | Toutes les études BA/BE LatAm |
| `GET /api/be-trials?country=Brazil` | Filtré par pays |
| `GET /api/be-trials?refresh=true` | Force le rechargement (bypass cache) |
| `GET /api/clinical-trials?country=Argentina` | Proxy ClinicalTrials.gov |

---

## Plan gratuit Railway
- 500h compute/mois (largement suffisant)
- Cache 12h intégré — le serveur ne surcharge pas les sources
- Auto-restart si crash
