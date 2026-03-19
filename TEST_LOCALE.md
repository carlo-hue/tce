# Test Locale

## Avvio

Apri PowerShell ed entra nella cartella del modulo:

```powershell
cd "C:\Users\CarloMarino\OneDrive - camarino59\OneDrive\CODICE\Agata_nuovo\moduli\tess_tce"
python run.py
```

Se `python` non fosse riconosciuto:

```powershell
cd "C:\Users\CarloMarino\OneDrive - camarino59\OneDrive\CODICE\Agata_nuovo\moduli\tess_tce"
py run.py
```

## URL di test

Apri nel browser:

- `http://127.0.0.1:5000/tess-tce/`
- `http://127.0.0.1:5000/tess-tce/api/health`

## Esito atteso

- `/tess-tce/` apre la UI del modulo
- `/tess-tce/api/health` restituisce un JSON simile a:

```json
{"module":"tess_tce","status":"ok"}
```

## Arresto server

Per fermare il server:

```powershell
Ctrl+C
```
