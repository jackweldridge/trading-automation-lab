# Trading Automation Lab — reference files

Clone-and-use code and config for the **Trading Automation Lab** course. Each
folder here matches a part of the course; clone the whole repo onto your server
and follow the lessons.

## Contents

- **`ib-stack/`** — IB Gateway + HTTP bridge (Module 2b). A paper-trading stack:
  the official gateway container plus a small FastAPI bridge that exposes simple
  HTTP endpoints (`/health`, `/account`, `/positions`, `/order`, …) for n8n.
- **`db/`** — database schema and roles (Module 3): `schema.sql` (tables and
  views, run once as the owner) and `roles.sql` (the read-only `claude_ro` role).
- **`webapp/`** — the private positions site (Module 7): `build.py`,
  `queries.sql`, `template.html` and `nginx-site.conf`.
- **`scripts/`** — operational helpers (Module 8): `backup.sh`, a template that
  exports your n8n workflows and dumps the database to a private backup repo.
- **`workflows/`** — ready-to-import n8n workflows (data loads, politician trades,
  trade approval & logging, FX, plus helper sub-workflows). Import them and connect
  your own credentials — see `workflows/README.md`.

## Using it

Clone the repo onto your server:

```
git clone https://github.com/jackweldridge/trading-automation-lab.git ~/trading-automation-lab
```

Then follow the relevant lesson. For the gateway stack:

```
cd ~/trading-automation-lab/ib-stack
```

and see that folder's own `README.md`.

## Disclaimer

Educational software, provided as-is with no warranty. Automated trading can
lose real money. You are responsible for anything your systems do. Nothing here
is financial advice.
