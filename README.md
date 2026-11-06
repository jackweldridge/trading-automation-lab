# Trading Automation Lab — reference files

Clone-and-use code and config for the **Trading Automation Lab** course. Each
folder here matches a part of the course; clone the whole repo onto your server
and follow the lessons.

## Contents

- **`ib-stack/`** — IB Gateway + HTTP bridge (Module 2b). A paper-trading stack:
  the official gateway container plus a small FastAPI bridge that exposes simple
  HTTP endpoints (`/health`, `/account`, `/positions`, `/order`, …) for n8n.

More modules' reference files will be added here over time.

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
