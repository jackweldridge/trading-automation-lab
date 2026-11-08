# n8n workflows

Importable n8n workflows for the Trading Automation Lab course. These are **sanitized exports of the real workflows** — every key, token, chat id, account number and personal id has been replaced with a `YOUR_…` placeholder, and credentials are referenced by name only (n8n keeps the actual secret values in its own encrypted store, never in these files).

## What's here

| File | What it does | Course area |
|---|---|---|
| `eodhd-data-load.json` | Daily EOD price load (bulk + incremental), orchestrates the helpers below | Module 3 — storing data |
| `fundamentals.json` | Fundamentals load (batched) | Module 3 |
| `fx-tracking.json` | FX rate load (e.g. GBPUSD) | Module 3 |
| `political-trades-periodic-disclosures.json` | US House periodic transaction reports → DB + LLM summary → Telegram | Politician-trades module |
| `trade-approval-and-logging.json` | Telegram approve/reject → place via the IB bridge → log the trade | Module 5 — approval & logging |
| `gateway-reconnect.json` | *(helper)* safe IB gateway reconnect | Module 6 |
| `sub-batch-price-update.json` | *(helper)* batched price upsert | Module 3 |
| `sub-batch-fundamentals-update.json` | *(helper)* batched fundamentals upsert | Module 3 |

The last three are **helper sub-workflows** that the others call via Execute Workflow nodes.

## Import & connect (once)

1. In n8n: **Workflows → Import from File**, and import **all eight** (helpers included).
2. Create your own **credentials** and attach them wherever a node shows "credential not found":
   - **Postgres** — your `market_data` database (most nodes)
   - **Telegram** — your BotFather token (alert / approval nodes)
   - **EODHD** — the data-load nodes (or paste your key wherever you see `YOUR_EODHD_KEY`)
   - **Google Sheets** and **OpenAI** — Trade-Approval logging / politician-trades summary (optional)
3. Re-link the **Execute Workflow** nodes: open each one and re-select the imported helper from the list. n8n assigns new ids on import, so the original links won't resolve automatically.
4. Replace the remaining placeholders: `YOUR_EODHD_KEY`, `YOUR_TELEGRAM_CHAT_ID`, `YOUR_GOOGLE_DOC_ID`, `YOUR_SERVER_IP`, `YOUR_ACCOUNT`.

## Notes

- `eodhd-data-load.json` contains one **disabled** node — "Industry Classification … (build your own classification)". The original stack enriches tickers with a classification step that isn't shipped; build your own or leave it disabled.
- These workflows call the IB bridge at `http://172.17.0.1:8080` (Module 2b) and expect the schema from `db/schema.sql` (Module 3).
- Provided as a learning aid, **as-is**. Review every node before running anything that places orders, and stay on a **paper** account first.
