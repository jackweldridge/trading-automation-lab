# IB Gateway + HTTP Bridge (Trading Automation Lab)

A small Docker stack that logs in to Interactive Brokers and exposes a simple
HTTP API your automation (n8n) can call. Two containers:

- **ib-gateway** — the official [gnzsnz/ib-gateway](https://github.com/gnzsnz/ib-gateway-docker)
  image, which runs IB Gateway and logs it in.
- **ib-http-bridge** — a small FastAPI app that turns Gateway's native interface
  into plain HTTP calls (`/health`, `/account`, `/positions`, `/order`, …).

It is configured for **paper trading** out of the box. Going live is a one-line
change (see below) — but keep it on paper until your system is proven.

## Prerequisites

- A Linux server with Docker and the Docker Compose plugin installed.
- An Interactive Brokers account with **paper trading** enabled, and the IBKR
  Mobile app set up for two-factor login.

## Setup

1. Clone this repo into a folder on your server, e.g. `~/ib-stack`, and `cd` into it.

2. Create your secrets file from the template:

   ```
   cp .env.example .env
   nano .env
   ```

   Fill in your IBKR paper username, password, paper account number (starts with
   `DU`), and your timezone. Save and close.

3. Build and start it:

   ```
   sudo docker compose up -d --build
   sudo docker compose ps
   ```

4. Watch it log in, then approve the 2FA push on your phone:

   ```
   sudo docker compose logs -f ib-gateway
   ```

   Nothing connects until you approve the push.

## Check it's working

From the same server:

```
curl http://127.0.0.1:8080/health
```

It reports whether the gateway is connected. From **inside n8n** (which runs on
Docker's own network) the bridge is reached at `http://172.17.0.1:8080` instead.

## Common endpoints

- `GET  /health` — is the gateway connected?
- `GET  /account` · `GET /nav` — balances, buying power, net liquidation value
- `GET  /positions` · `GET /portfolio` — holdings (with market values)
- `GET  /open-orders` — resting orders
- `POST /order` — place an order (JSON body: `symbol`, `exchange`, `action`,
  `quantity`, `orderType`, `limitPrice`/`stopPrice`/`trailingPercent`, `tif`, …)
- `POST /cancel-order?orderId=...` — cancel a resting order

## Paper vs live

In `docker-compose.yml`, paper uses `TRADING_MODE: paper` and `IB_PORT: 4002`.
Live is `TRADING_MODE: live` and `IB_PORT: 4001`, with your real IBKR login.
Nothing else changes.

## Safety notes

- Never commit your `.env`. It holds your IBKR credentials.
- The API binds to `127.0.0.1` only — it is not exposed to the internet.
- This is educational software provided as-is, with no warranty. Automated
  trading can lose money. You are responsible for anything your system does.
