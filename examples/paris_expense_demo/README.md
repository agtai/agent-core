# Multi-source collect → merge → approve (Core example)

A terminal travel assistant: **plan sources → ask permission → collect/merge → missing/policy tips → approve → submit**.  
Paris expense is the default case; also reconcile / trip brief / media pack.  
Static web pages mirror order data (amounts and IDs match the YAML fixtures).

---

## 1. Start the web pages

Static HTML — start a local HTTP server from **`web_mocks/`** (**do not** open via `file://`, and **do not** run `http.server` from `agent-core` root).

### Start (recommended)

```bash
cd /path/to/agent-core/examples/paris_expense_demo/web_mocks
bash start-web.sh
# or: python3 -m http.server 8765 --bind 127.0.0.1
```

Keep that terminal open.

### Open

**http://127.0.0.1:8765/**  
Hotel order: **http://127.0.0.1:8765/hotel.html**

| Entry | Content |
| --- | --- |
| LiliMall / OrangeStay / FlyWay / QuickBite / OrangeRide | Shopping, hotel, flight, food, rides |
| Email / Photos / Booking / Rides / Expense | Sources and expense claim |

### Stop

`Ctrl+C` in the server terminal.

### If pages won’t open / “Address already in use”

```bash
# See who holds 8765
lsof -nP -iTCP:8765 -sTCP:LISTEN

# Free the port, then start again from web_mocks/
lsof -tiTCP:8765 -sTCP:LISTEN | xargs kill
cd /path/to/agent-core/examples/paris_expense_demo/web_mocks
bash start-web.sh
```

Or use another port: `bash start-web.sh 8766` → http://127.0.0.1:8766/

**Tip:** keep the web hub open while running the agent in another terminal.

---

## 2. Run the agent

### Prerequisites

- Repo: `agent-core`  
- Local `.venv` (`uv sync` if needed)  
- Run from **agent-core root** with `PYTHONPATH=.`

### Default: Paris expense (fully automatic)

```bash
cd /path/to/agent-core

PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main \
  --case paris_expense --yes
```

Plans sources → auto-grants reads → prints approval card (incl. policy tips) → submits → prints claim id.

### Interactive

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main --case paris_expense
```

1. Per source: allow read? `y` / `n`  
2. Approval card: submit? `y` / `n`

### List cases

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main --list-cases
```

| case id | What it does |
| --- | --- |
| `paris_expense` | Expense the Paris trip (default) |
| `berlin_reconcile` | Berlin ops reconcile (email vs DB, dedupe) |
| `seoul_trip_summary` | Seoul trip brief |
| `nyc_media_pack` | NYC launch media archive |

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main --case berlin_reconcile --yes
```

### Flags

| Flag | Meaning |
| --- | --- |
| `--case <id>` | Case pack (default `paris_expense`) |
| `--yes` | Auto-grant sources + auto-submit |
| `--no` | Auto-grant sources, cancel final submit |
| `--grant-all` | Skip per-source prompts only |
| `--deny-sources photos,booking` | Force-deny sources (missing-receipt tips) |
| `--plan auto` | Default; LLM if `API_KEY`, else heuristic |
| `--plan heuristic` / `llm` / `fixed` | Source planning mode |
| `--list-cases` / `--list-sources` | List cases or adapters |

Missing-receipt demo:

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main \
  --case paris_expense --yes --deny-sources photos,booking
```

### Suggested walkthrough

1. Terminal A: start web server → http://localhost:8765/  
2. Terminal B: run Paris `--yes`, compare flight/hotel/ride amounts  
3. Run again with `--deny-sources photos` to see missing tips  

---

## 3. Capabilities (short)

**Flow:** plan → permission → collect/merge → policy tips → approve → submit.

**A1–A5:** case YAML, source registry, Evidence/DraftLine, templates, merge config.  
**B1–B3:** heuristic/optional LLM planning, read permission gate, missing/over-policy tips.

Sources: `email` · `photos` · `booking` · `rides` · `browser` · `db`  
New case: add YAML under `cases/` (optional `policy:`).

Paris policy example: meal ≤ 80 EUR, taxi ≤ 50 EUR; denied sources / missing categories tip on the card.

---

## 4. Out of scope (for later)

WorkSwarm chat UI, live email/expense APIs, DeepAgent permission rails end-to-end.
