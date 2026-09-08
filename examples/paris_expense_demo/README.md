# Multi-source collect → merge → approve (Core example)

A terminal travel assistant: **recall memory → plan sources → ask permission → collect/merge → fill mock ERP → edit on the page → approve → submit**.  
Paris expense is the default case; also reconcile / trip brief / media pack.

---

## 1. Start the web pages

Use **`web_mocks/start-web.sh`** (live ERP API — **do not** use `python -m http.server` or `file://`).

```bash
cd /path/to/agent-core/examples/paris_expense_demo/web_mocks
bash start-web.sh
```

Keep that terminal open. Open **http://127.0.0.1:8765/expense.html**.

If you skip this step, `main.py` will try to start the ERP on port 8765 itself.

### Stop

`Ctrl+C` in the server terminal, or: `lsof -tiTCP:8765 -sTCP:LISTEN | xargs kill`

---

## 2. Run the agent

From **agent-core root** with `PYTHONPATH=.`:

### Demo: approve on the web page (default)

```bash
cd /path/to/agent-core

PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main \
  --case paris_expense --grant-all --plan heuristic
```

`--grant-all` skips source `y/n` so you can watch the form fill. Then **on the page**:

1. Edit a line (amount / description) or delete one  
2. **Add a receipt** if something is missing  
3. Click **Submit to finance** (or Cancel)

The terminal waits for that click.

### Fully automatic (scripts / CI)

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main \
  --case paris_expense --yes --fast --no-open --plan heuristic
```

### What you should see

1. **Memory recall** — cost center CC-1402, Paris trip from Slack, usual meal cap **80 EUR** (the case file says 100; memory wins). Dinner at 86.50 is flagged.  
2. Collect from email / photos / booking / rides.  
3. **http://127.0.0.1:8765/expense.html** fills line by line, then unlocks edit / add / submit.

### Flags

| Flag | Meaning |
| --- | --- |
| `--case <id>` | Case pack (default `paris_expense`) |
| `--yes` | Auto-grant sources + auto-submit (no page wait) |
| `--no` | Auto-grant sources, cancel final submit |
| `--grant-all` | Skip per-source prompts; still wait on the ERP page |
| `--cli-approve` | Ask `y/n` in the terminal instead of the page |
| `--deny-sources photos,booking` | Force-deny sources (missing-receipt tips) |
| `--plan auto` | Default; LLM if `API_KEY`, else heuristic |
| `--skip-erp` | CLI only; do not fill the web form |
| `--no-open` | Do not open a browser tab |
| `--no-autostart` | Do not spawn the ERP server if it is down |
| `--fast` | Fill the ERP form without the typing pause |
| `--list-cases` / `--list-sources` | List cases or adapters |

Missing-receipt demo (then add the hotel on the page):

```bash
PYTHONPATH=. .venv/bin/python -m examples.paris_expense_demo.main \
  --case paris_expense --grant-all --plan heuristic --deny-sources booking
```

---

## 3. Capabilities (short)

**Flow:** recall → plan → permission → collect/merge → **fill ERP** → **edit / add receipt / submit on the page**.

**Memory:** YAML packs under `memory/` (profile, policy, Slack-like notes, past claims).  
**Browser use:** mock ERP at `/expense.html`.  
**Approvals:** the claim card is the ERP page (terminal `--cli-approve` still works).

Sources: `email` · `photos` · `booking` · `rides` · `browser` · `db`  
New case: add YAML under `cases/` (optional `policy:`). New user memory: `memory/<first_last>.yaml`.

---

## 4. Out of scope (for later)

WorkSwarm chat UI, live email/expense APIs, Playwright against a real ERP, DeepAgent permission rails, document OCR.
