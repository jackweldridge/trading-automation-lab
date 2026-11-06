"""
IB HTTP Bridge — V2 Architecture

Per-request IB connections. No global state. No shared event loop.
No monitor task. Each endpoint creates its own IB instance, does its
work, and disconnects cleanly.

Eliminates the entire class of "loop wedging" failures from V1.

Trade-off: each request adds ~500ms-1s of connect overhead. Acceptable
for n8n polling at minute-or-greater intervals.
"""

import os
import time
import asyncio
import json
import socket
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List, AsyncIterator

import requests_unixsocket
from fastapi import FastAPI, HTTPException, Query
from pydantic import ConfigDict, BaseModel, Field
from ib_insync import IB, Stock, MarketOrder, LimitOrder, Order, StopOrder, ExecutionFilter


# ============================================================
# Config
# ============================================================
def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v else default
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v else default
    except ValueError:
        return default


IB_HOST = os.getenv("IB_HOST", "ib-gateway")
IB_PORT = env_int("IB_PORT", 4002)
IB_CLIENT_ID = env_int("IB_CLIENT_ID", 1001)

_IBKR_UNSET = 1.7976931348623157e+308  # IB's UNSET_DOUBLE / sys.float_info.max


def _clean_float(v, default=None):
    """Map IBKR's UNSET_DOUBLE sentinel, None, and invalid values to default."""
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return default if f >= _IBKR_UNSET else f
IB_ACCOUNT = os.getenv("IB_ACCOUNT", "").strip() or None
CONNECT_TIMEOUT = env_float("IB_CONNECT_TIMEOUT", 15.0)
SUBSCRIBE_FLUSH_SEC = env_float("IB_SUBSCRIBE_FLUSH_SEC", 2.0)
ORDER_CALLBACK_MAX_WAIT_SEC = env_float("IB_ORDER_CALLBACK_MAX_WAIT_SEC", 5.0)
BRACKET_CONFIRM_TIMEOUT_SEC = env_float("IB_BRACKET_CONFIRM_TIMEOUT", 30.0)


# Module-level cache. Read by /portfolio. Survives across requests
# because we don't recreate the cache when we recreate the IB instance.
CONTRACT_NAME_CACHE: Dict[int, str] = {}


# ============================================================
# Per-request IB session — the new architecture
# ============================================================
@asynccontextmanager
async def ib_session(
    subscribe_account: bool = False,
    timeout: float = None,
    client_id: int | None = None,
    rotate: bool = True,
) -> AsyncIterator[IB]:
    """
    Create a fresh IB connection for the lifetime of one request.
    
    Connects with clientId rotation (configured + 5 alternates) so
    transient "client id in use" errors self-heal.
    
    If subscribe_account=True, subscribes to account updates and waits
    SUBSCRIBE_FLUSH_SEC so portfolio() / accountValues() return data.
    
    Always disconnects on exit, regardless of success or exception.
    """
    if timeout is None:
        timeout = CONNECT_TIMEOUT
    
    ib = IB()
    connected_id = None
    last_err: Optional[Exception] = None
    
    base_id = IB_CLIENT_ID if client_id is None else client_id
    attempts = 20 if rotate else 1
    for offset in range(attempts):
        cid = base_id + offset
        try:
            await asyncio.wait_for(
                ib.connectAsync(
                    host=IB_HOST,
                    port=IB_PORT,
                    clientId=cid,
                    timeout=timeout,
                ),
                timeout=timeout + 2,
            )
            if ib.isConnected():
                # isConnected() can be true on a timeout-truncated handshake
                # where nextValidId never arrived -> orders place into the void
                # with no permId and no error event. Require a valid order-id
                # sequence before accepting the connection.
                try:
                    _ = ib.client.getReqId()
                except Exception:
                    last_err = RuntimeError("connected but nextValidId not ready")
                    try:
                        ib.disconnect()
                    except Exception:
                        pass
                    await asyncio.sleep(1.0)
                    continue
                connected_id = cid
                ib._captured_errs = []
                def _ib_err(*a):
                    ib._captured_errs.append(list(a))
                    print(f"[IB_ERROR] {a}", flush=True)
                ib.errorEvent += _ib_err
                break
            last_err = RuntimeError("connectAsync returned but not connected")
        except Exception as e:
            last_err = e
            try:
                if ib.isConnected():
                    ib.disconnect()
            except Exception:
                pass
            # Rotate clientId on collision-type errors. A TimeoutError under
            # concurrency is almost always a clientId collision (gateway accepts
            # the socket but never completes the handshake because the id is
            # contended), so we MUST rotate on timeout too -- not just on the
            # explicit "326 / already in use" message.
            msg = str(e).lower()
            import asyncio as _asyncio
            is_collision = (
                "326" in msg
                or "already in use" in msg
                or isinstance(e, _asyncio.TimeoutError)
                or "timeout" in msg
            )
            if not is_collision:
                break
    
    if connected_id is None:
        raise HTTPException(
            status_code=503,
            detail=f"IB connect failed after rotation: {last_err!r}",
        )
    
    # Note: we do NOT subscribe to account updates here.
    # reqAccountUpdatesAsync hangs in this ib_insync version inside FastAPI's
    # request loop. Endpoints that need portfolio data use reqPositionsAsync
    # and reqContractDetailsAsync directly instead.
    
    try:
        yield ib
    finally:
        try:
            if ib.isConnected():
                ib.disconnect()
                # Gateway needs ~1s to actually release the clientId.
                # Without this, rapid sequential calls collide on the same id.
                await asyncio.sleep(1.0)
        except Exception:
            pass


async def _resolve_account(ib: IB, override: Optional[str] = None) -> str:
    """Determine which account to act on. Override > IB_ACCOUNT > first."""
    accts = ib.managedAccounts()
    if not accts:
        raise HTTPException(status_code=503, detail="No managed accounts")
    
    if override:
        if override not in accts:
            raise HTTPException(
                status_code=400,
                detail=f"Account {override} not in managed accounts {accts}",
            )
        return override
    
    if IB_ACCOUNT:
        if IB_ACCOUNT not in accts:
            raise HTTPException(
                status_code=503,
                detail=f"IB_ACCOUNT={IB_ACCOUNT} not in managed accounts {accts}",
            )
        return IB_ACCOUNT
    
    return accts[0]


async def _qualify_stock(ib: IB, symbol: str, exchange: str) -> Stock:
    exchange = exchange.upper()
    if exchange == "LSE":
        contract = Stock(symbol, "SMART", "GBP", primaryExchange="LSE")
    else:
        contract = Stock(symbol, "SMART", "USD")
    qualified = await ib.qualifyContractsAsync(contract)
    if not qualified:
        raise HTTPException(
            status_code=400,
            detail=f"Could not qualify {symbol} on {exchange}",
        )
    return qualified[0]


# ============================================================
# Models
# ============================================================
class HealthResponse(BaseModel):
    ib_connected: bool
    ib_host: str
    ib_port: int
    client_id: int
    accounts: List[str] = []
    duration_ms: float = 0.0


class OrderRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    symbol: str
    exchange: str = Field(...)
    action: str = Field(...)
    quantity: float = Field(..., gt=0)
    order_type: str = Field("MKT", alias="orderType")
    limit_price: Optional[float] = Field(None, alias="limitPrice")
    stop_loss: Optional[float] = Field(None, alias="stopLoss")
    take_profit: Optional[float] = Field(None, alias="takeProfit")
    tif: str = Field("DAY")
    child_tif: Optional[str] = Field(None, alias="childTif", description="TIF for SL/TP children. Defaults to GTC.")
    outside_rth: bool = Field(False, alias="outsideRth")
    account: Optional[str] = None
    trailing_percent: Optional[float] = Field(None, alias="trailingPercent", gt=0)
    trail_stop_price: Optional[float] = Field(None, alias="trailStopPrice", gt=0)
    stop_price: Optional[float] = Field(None, alias="stopPrice", gt=0, description="Trigger price for a standalone STP (protective stop). Distinct from stop_loss, which triggers the bracket path.")
    sec_type: Optional[str] = Field(None, alias="secType")
    currency: Optional[str] = None


# ============================================================
# App
# ============================================================
app = FastAPI(title="IB HTTP Bridge V2")


# ============================================================
# Read endpoints
# ============================================================

@app.get("/health", response_model=HealthResponse)
async def health():
    """
    Verifies a fresh IB connection works end-to-end.
    Slower than V1's stale-state health check, but actually meaningful:
    if /health returns 200, subsequent endpoints WILL work.
    """
    t0 = time.time()
    try:
        async with ib_session(subscribe_account=False) as ib:
            return HealthResponse(
                ib_connected=True,
                ib_host=IB_HOST,
                ib_port=IB_PORT,
                client_id=IB_CLIENT_ID,
                accounts=ib.managedAccounts(),
                duration_ms=(time.time() - t0) * 1000,
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"health check failed: {e!r}")


@app.get("/accounts")
async def accounts():
    async with ib_session(subscribe_account=False) as ib:
        return {"connected": True, "accounts": ib.managedAccounts()}


@app.get("/account")
async def account(account: Optional[str] = Query(None)):
    async with ib_session(subscribe_account=False) as ib:
        acct = await _resolve_account(ib, account)
        vals = await ib.accountSummaryAsync(acct)
        result = {v.tag: v.value for v in vals}

        # Pending parent BUY commitments grouped by instrument currency.
        # Excludes child orders (parentId != 0) since exits don't consume cash.
        # Excludes SELLs since they release cash, not reserve it.
        # Pricing falls back lmtPrice -> auxPrice; no-price orders are skipped.
        pending_by_currency = {}
        pending_orders = []

        for trade in ib.openTrades():
            order = trade.order
            contract = trade.contract

            if getattr(order, "parentId", 0) != 0:
                continue
            if order.action != "BUY":
                continue

            remaining = getattr(order, "remaining", order.totalQuantity or 0) or 0
            if remaining <= 0:
                continue

            lmt = getattr(order, "lmtPrice", 0) or 0
            aux = getattr(order, "auxPrice", 0) or 0
            price = lmt if lmt > 0 else aux
            if price <= 0:
                continue

            currency = (getattr(contract, "currency", "") or "USD").upper()
            notional = remaining * price

            pending_by_currency[currency] = pending_by_currency.get(currency, 0.0) + notional
            pending_orders.append({
                "symbol":    getattr(contract, "symbol", ""),
                "orderId":   order.orderId,
                "action":    order.action,
                "orderType": order.orderType,
                "quantity":  remaining,
                "price":     price,
                "currency":  currency,
                "notional":  round(notional, 2),
            })

        result["PendingParentBuyValueByCurrency"] = {k: round(v, 2) for k, v in pending_by_currency.items()}
        result["PendingParentBuyOrders"] = pending_orders
        result["PendingParentBuyCount"] = len(pending_orders)

        return result


@app.get("/positions")
async def positions(
    symbol: Optional[str] = Query(None),
    primaryExchange: Optional[str] = Query(None),
    account: Optional[str] = Query(None),
):
    async with ib_session(subscribe_account=False) as ib:
        pos = await ib.reqPositionsAsync()
        results = []
        for p in pos:
            c = p.contract
            record = {
                "symbol": getattr(c, "symbol", ""),
                "secType": getattr(c, "secType", ""),
                "conId": getattr(c, "conId", None),
                "currency": getattr(c, "currency", ""),
                "exchange": getattr(c, "exchange", ""),
                "primaryExchange": getattr(c, "primaryExchange", ""),
                "localSymbol": getattr(c, "localSymbol", ""),
                "tradingClass": getattr(c, "tradingClass", ""),
                "secIdType": getattr(c, "secIdType", ""),
                "secId": getattr(c, "secId", ""),
                "description": getattr(c, "description", ""),
                "lastTradeDateOrContractMonth": getattr(c, "lastTradeDateOrContractMonth", ""),
                "strike": float(getattr(c, "strike", 0) or 0),
                "right": getattr(c, "right", ""),
                "multiplier": getattr(c, "multiplier", ""),
                "position": float(p.position),
                "avgCost": float(p.avgCost),
                "account": p.account,
            }
            if symbol and record["symbol"].upper() != symbol.upper():
                continue
            if primaryExchange and record["primaryExchange"].upper() != primaryExchange.upper():
                continue
            if account and record["account"] != account:
                continue
            results.append(record)
        return results


# ---------------------------------------------------------------------------
# Position pricing for /portfolio and /snapshot.
# The account has no real-time subscription, so prices come from historical
# bars. Previously one position at a time with 5s timeouts: ~17 holdings on a
# bad HMDS day ran 85s+, blowing n8n's 60s timeout and driving the Reconnect
# loop. Now: bounded-parallel, 1-min bar first, daily close as fallback, and a
# circuit breaker that stops asking for 1-min bars once they start timing out.
# ---------------------------------------------------------------------------
PRICE_CONCURRENCY = env_int("PRICE_CONCURRENCY", 6)
PRICE_TIMEOUT_SEC = 5.0
PRICE_MINUTE_TRIP = 2   # 1-min timeouts before skipping 1-min bars for the rest


async def _price_contracts(ib, contracts) -> Dict[int, Dict[str, Any]]:
    """conId -> {"price": float, "source": "1min"|"daily"|"none"}."""
    sem = asyncio.Semaphore(PRICE_CONCURRENCY)
    state = {"minute_timeouts": 0}

    async def bars(c, duration, size):
        return await asyncio.wait_for(
            ib.reqHistoricalDataAsync(c, "", duration, size, "TRADES", useRTH=False, formatDate=1),
            timeout=PRICE_TIMEOUT_SEC,
        )

    async def one(c):
        async with sem:
            if state["minute_timeouts"] < PRICE_MINUTE_TRIP:
                try:
                    b = await bars(c, "60 S", "1 min")
                    if b:
                        return c.conId, {"price": float(b[-1].close), "source": "1min"}
                except asyncio.TimeoutError:
                    state["minute_timeouts"] += 1
                    print(f"!!! 1-min bar timed out for {c.symbol}", flush=True)
                except Exception as e:
                    print(f"!!! 1-min bar failed for {c.symbol}: {e!r}", flush=True)
            try:
                b = await bars(c, "1 D", "1 day")
                if b:
                    return c.conId, {"price": float(b[-1].close), "source": "daily"}
            except Exception as e:
                print(f"!!! historical data fetch failed for {c.symbol}: {e!r}", flush=True)
            return c.conId, {"price": 0.0, "source": "none"}

    t0 = time.time()
    results = await asyncio.gather(*(one(c) for c in contracts))
    print(f"priced {len(contracts)} positions in {time.time() - t0:.1f}s "
          f"(1-min timeouts={state['minute_timeouts']})", flush=True)
    return dict(results)


@app.get("/portfolio")
async def portfolio():
    """
    Return enriched position data for the configured account.
    
    Uses reqPositionsAsync (positions + avgCost) plus reqMktData (live prices)
    to compose the same data shape as V1's ib.portfolio() — without needing
    reqAccountUpdates (which hangs in this ib_insync version inside FastAPI).
    """
    async with ib_session() as ib:
        positions = await ib.reqPositionsAsync()
        target_acct = IB_ACCOUNT or (ib.managedAccounts()[0] if ib.managedAccounts() else None)
        positions = [p for p in positions if (not target_acct or p.account == target_acct)]
        
        prices = await _price_contracts(ib, [p.contract for p in positions])
        results = []
        for p in positions:
            c = p.contract
            
            # Resolve company name (cached across requests)
            name = CONTRACT_NAME_CACHE.get(c.conId)
            if not name:
                try:
                    details = await ib.reqContractDetailsAsync(c)
                    if details:
                        name = details[0].longName or c.symbol
                        CONTRACT_NAME_CACHE[c.conId] = name
                    else:
                        name = c.symbol
                except Exception:
                    name = c.symbol
            
            # Price resolved up front by _price_contracts (1-min bar, else daily close).
            px = prices.get(c.conId, {"price": 0.0, "source": "none"})
            market_price = px["price"]
            
            position = float(p.position)
            avg_cost = float(p.avgCost)
            market_value = position * market_price if market_price else 0.0
            unrealized_pnl = (market_price - avg_cost) * position if market_price else 0.0
            
            results.append({
                "symbol": getattr(c, "symbol", ""),
                "name": name,
                "secType": getattr(c, "secType", ""),
                "exchange": getattr(c, "exchange", ""),
                "primaryExchange": getattr(c, "primaryExchange", ""),
                "currency": getattr(c, "currency", ""),
                "conId": getattr(c, "conId", None),
                "position": position,
                "avgCost": avg_cost,
                "marketPrice": market_price,
                "priceSource": px["source"],
                "marketValue": market_value,
                "unrealizedPnL": unrealized_pnl,
                "realizedPnL": 0.0,  # not available without account subscription
            })
        return results


@app.get("/orders")
async def orders(
    symbol: Optional[str] = Query(None),
    orderId: Optional[int] = Query(None),
    permId: Optional[int] = Query(None),
    clientId: Optional[int] = Query(None),
    account: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    orderType: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    parentId: Optional[int] = Query(None),
    conId: Optional[int] = Query(None),
):
    async with ib_session(subscribe_account=False) as ib:
        await ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)  # let trades populate
        trades = ib.trades()
        results = []
        for t in trades:
            o = t.order
            c = t.contract
            s = t.orderStatus
            record = {
                "symbol": getattr(c, "symbol", ""),
                "secType": getattr(c, "secType", ""),
                "exchange": getattr(c, "exchange", ""),
                "primaryExchange": getattr(c, "primaryExchange", ""),
                "currency": getattr(c, "currency", ""),
                "conId": getattr(c, "conId", None),
                "orderId": int(getattr(o, "orderId", 0)),
                "permId": int(getattr(o, "permId", 0)),
                "clientId": int(getattr(o, "clientId", 0)),
                "account": getattr(o, "account", ""),
                "orderRef": getattr(o, "orderRef", ""),
                "action": getattr(o, "action", ""),
                "orderType": getattr(o, "orderType", ""),
                "totalQuantity": _clean_float(getattr(o, "totalQuantity", None), default=0),
                "lmtPrice": _clean_float(getattr(o, "lmtPrice", None)),
                "auxPrice": _clean_float(getattr(o, "auxPrice", None)),
                "trailingPercent": _clean_float(getattr(o, "trailingPercent", None)),
                "trailStopPrice": _clean_float(getattr(o, "trailStopPrice", None)),
                "tif": getattr(o, "tif", ""),
                "outsideRth": bool(getattr(o, "outsideRth", False)),
                "transmit": bool(getattr(o, "transmit", True)),
                "parentId": int(getattr(o, "parentId", 0) or 0),
                "status": getattr(s, "status", ""),
                "filled": _clean_float(getattr(s, "filled", 0), default=0),
                "remaining": _clean_float(getattr(s, "remaining", 0), default=0),
                "avgFillPrice": _clean_float(getattr(s, "avgFillPrice", None)),
                "lastFillPrice": _clean_float(getattr(s, "lastFillPrice", None)),
                "whyHeld": getattr(s, "whyHeld", ""),
                "fills": [
                    {
                        "execId": f.execution.execId,
                        "executionTime": (f.execution.time.isoformat() if f.execution.time else None),
                        "side": f.execution.side,
                        "shares": float(f.execution.shares),
                        "price": float(f.execution.price),
                        "cumQty": float(f.execution.cumQty),
                        "avgPrice": float(f.execution.avgPrice),
                        "exchange": f.execution.exchange,
                        "commission": float(getattr(f.commissionReport, "commission", 0) or 0),
                        "realizedPNL": float(getattr(f.commissionReport, "realizedPNL", 0) or 0),
                        "commissionCurrency": getattr(f.commissionReport, "currency", ""),
                    }
                    for f in t.fills
                ],
            }
            if symbol and record["symbol"] != symbol.upper(): continue
            if orderId and record["orderId"] != orderId: continue
            if permId and record["permId"] != permId: continue
            if clientId and record["clientId"] != clientId: continue
            if account and record["account"] != account: continue
            if action and record["action"] != action.upper(): continue
            if orderType and record["orderType"] != orderType.upper(): continue
            if status and record["status"] != status: continue
            if parentId and record["parentId"] != parentId: continue
            if conId and record["conId"] != conId: continue
            results.append(record)
        return results


ACTIVE_ORDER_STATES = {"ApiPending", "PendingSubmit", "PreSubmitted", "Submitted"}


def _match_symbol_order(t, sym: str, account: str | None) -> bool:
    if getattr(t.contract, "symbol", "").upper() != sym:
        return False
    if account and getattr(t.order, "account", "") != account:
        return False
    return getattr(t.orderStatus, "status", "") in ACTIVE_ORDER_STATES


@app.api_route("/cancel-symbol", methods=["POST"])
async def cancel_symbol(symbol: str, account: str | None = None):
    """Cheap path: cancel active orders for a symbol that THIS client can
    address by id. Cannot touch manual orphans (orderId 0). Verifies by re-poll.
    """
    sym = symbol.upper()
    async with ib_session(subscribe_account=False) as ib:
        await ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)
        targets     = [t for t in ib.openTrades() if _match_symbol_order(t, sym, account)]
        cancellable = [t for t in targets if t.order.orderId != 0]
        orphaned    = [t for t in targets if t.order.orderId == 0]
        for t in cancellable:
            ib.cancelOrder(t.order)
        await asyncio.sleep(1.5)
        await ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)
        still_live = [t for t in ib.openTrades() if _match_symbol_order(t, sym, account)]
        return {
            "mode": "client_owned",
            "symbol": sym,
            "targeted": len(targets),
            "cancel_requested": [t.order.permId for t in cancellable],
            "uncancellable_orderId0": [t.order.permId for t in orphaned],
            "still_live_after_verify": [t.order.permId for t in still_live],
            "fully_cleared": len(still_live) == 0,
            "needs_escalation": len(orphaned) > 0 or len(still_live) > 0,
        }


@app.api_route("/force-cancel-symbol", methods=["POST"])
async def force_cancel_symbol(symbol: str, account: str | None = None, dry_run: bool = True):
    """Escalation path: connect clientId=0 to BIND manual TWS orders, then cancel
    all active orders for a symbol (manual included). Verifies by re-poll.

    REQUIRES TWS setting 'Use negative numbers to bind automatic orders' = ON.
    dry_run defaults True: binds and reports what WOULD be cancelled, sends nothing.
    Pass dry_run=false to actually cancel.
    """
    sym = symbol.upper()
    async with ib_session(client_id=0, rotate=False, subscribe_account=False) as ib:
        await ib.reqAllOpenOrdersAsync()   # clientId=0 binds manual orders
        await asyncio.sleep(1.5)
        targets     = [t for t in ib.openTrades() if _match_symbol_order(t, sym, account)]
        cancellable = [t for t in targets if t.order.orderId != 0]   # incl. negative
        unbound     = [t for t in targets if t.order.orderId == 0]   # bind failed
        if dry_run:
            return {
                "mode": "bind_dry_run",
                "symbol": sym,
                "would_cancel": [
                    {"permId": t.order.permId, "orderId": t.order.orderId,
                     "orderType": t.order.orderType, "clientId": t.order.clientId}
                    for t in cancellable
                ],
                "binding_failed_orderId0": [t.order.permId for t in unbound],
                "note": "No cancels sent. Re-run with dry_run=false to execute.",
            }
        for t in cancellable:
            ib.cancelOrder(t.order)
        await asyncio.sleep(1.5)
        await ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)
        still_live = [t for t in ib.openTrades() if _match_symbol_order(t, sym, account)]
        owners = sorted({int(getattr(t.order, "clientId", 0)) for t in still_live
                         if int(getattr(t.order, "clientId", 0)) != 0})
    # Escalation 2: a real-orderId order placed by a ROTATED bridge clientId (the
    # bridge rotates base+N on "client id in use", which orphans those orders from
    # the base client) can only be cancelled by that owning client. Connect AS each
    # owning clientId and cancel its orders for this symbol, then verify on the base.
    owner_cancels = {}
    for oc in owners:
        try:
            async with ib_session(client_id=oc, rotate=False, subscribe_account=False) as ib2:
                await ib2.reqOpenOrdersAsync()
                await asyncio.sleep(1.0)
                mine = [t for t in ib2.openTrades() if _match_symbol_order(t, sym, account)]
                for t in mine:
                    ib2.cancelOrder(t.order)
                await asyncio.sleep(1.5)
                owner_cancels[oc] = [t.order.permId for t in mine]
        except Exception as ex:
            owner_cancels[oc] = f"error: {ex}"
    async with ib_session(subscribe_account=False) as ibv:
        await ibv.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)
        still_final = [t for t in ibv.openTrades() if _match_symbol_order(t, sym, account)]
    return {
        "mode": "bind_execute",
        "symbol": sym,
        "targeted": len(targets),
        "cancel_requested": [t.order.permId for t in cancellable],
        "binding_failed_orderId0": [t.order.permId for t in unbound],
        "owner_cancels": owner_cancels,
        "still_live_after_verify": [t.order.permId for t in still_final],
        "fully_cleared": len(still_final) == 0,
    }



@app.get("/open-orders")
async def open_orders(symbol: str | None = None, account: str | None = None):
    """Active orders only (Submitted, PreSubmitted, PendingSubmit).
    
    Same field set as /orders for downstream compatibility — but uses
    openTrades() rather than trades(), so cancelled and filled orders
    are excluded automatically. No status= query needed for "live only".
    """
    async with ib_session(subscribe_account=False) as ib:
        # Pull across all clients (captures manual TWS orders)...
        await ib.reqAllOpenOrdersAsync()
        # ...and this client's own orders, in case the prior call drops them.
        await ib.reqOpenOrdersAsync()
        await asyncio.sleep(1.0)  # let both sets of callbacks populate
        seen = {}
        for t in ib.openTrades():
            seen[t.order.permId] = t
        results = []
        for t in seen.values():
            if symbol and getattr(t.contract, "symbol", "").upper() != symbol.upper():
                continue
            if account and getattr(t.order, "account", "") != account:
                continue
            o = t.order
            c = t.contract
            s = t.orderStatus
            results.append({
                "symbol": getattr(c, "symbol", ""),
                "secType": getattr(c, "secType", ""),
                "exchange": getattr(c, "exchange", ""),
                "primaryExchange": getattr(c, "primaryExchange", ""),
                "currency": getattr(c, "currency", ""),
                "conId": getattr(c, "conId", None),
                "orderId": int(getattr(o, "orderId", 0)),
                "permId": int(getattr(o, "permId", 0)),
                "clientId": int(getattr(o, "clientId", 0)),
                "account": getattr(o, "account", ""),
                "orderRef": getattr(o, "orderRef", ""),
                "action": getattr(o, "action", ""),
                "orderType": getattr(o, "orderType", ""),
                "totalQuantity": _clean_float(getattr(o, "totalQuantity", None), default=0),
                "lmtPrice": _clean_float(getattr(o, "lmtPrice", None)),
                "auxPrice": _clean_float(getattr(o, "auxPrice", None)),
                "trailingPercent": _clean_float(getattr(o, "trailingPercent", None)),
                "trailStopPrice": _clean_float(getattr(o, "trailStopPrice", None)),
                "tif": getattr(o, "tif", ""),
                "outsideRth": bool(getattr(o, "outsideRth", False)),
                "transmit": bool(getattr(o, "transmit", True)),
                "parentId": int(getattr(o, "parentId", 0) or 0),
                "status": getattr(s, "status", ""),
                "filled": _clean_float(getattr(s, "filled", 0), default=0),
                "remaining": _clean_float(getattr(s, "remaining", 0), default=0),
                "avgFillPrice": _clean_float(getattr(s, "avgFillPrice", None)),
                "lastFillPrice": _clean_float(getattr(s, "lastFillPrice", None)),
                "whyHeld": getattr(s, "whyHeld", ""),
                "fills": [
                    {
                        "execId": f.execution.execId,
                        "executionTime": (f.execution.time.isoformat() if f.execution.time else None),
                        "side": f.execution.side,
                        "shares": float(f.execution.shares),
                        "price": float(f.execution.price),
                        "cumQty": float(f.execution.cumQty),
                        "avgPrice": float(f.execution.avgPrice),
                        "exchange": f.execution.exchange,
                        "commission": float(getattr(f.commissionReport, "commission", 0) or 0),
                        "realizedPNL": float(getattr(f.commissionReport, "realizedPNL", 0) or 0),
                        "commissionCurrency": getattr(f.commissionReport, "currency", ""),
                    }
                    for f in t.fills
                ],
            })
        return results


@app.get("/executions")
async def executions():
    async with ib_session(subscribe_account=False) as ib:
        execs = await ib.reqExecutionsAsync()
        return [
            {
                "execId": e.execution.execId,
                "symbol": e.contract.symbol,
                "side": e.execution.side,
                "shares": float(e.execution.shares),
                "price": float(e.execution.price),
                "time": str(e.execution.time),
            }
            for e in execs
        ]


@app.get("/trade-history")
async def trade_history(days: int = 7, account: str | None = None):
    async with ib_session(subscribe_account=False) as ib:
        start_time = (datetime.utcnow() - timedelta(days=days)).strftime("%Y%m%d-%H:%M:%S")
        filt = ExecutionFilter()
        filt.time = start_time
        if account:
            filt.acctCode = account
        execs = await ib.reqExecutionsAsync(filt)
        trades = {}
        for e in execs:
            contract = e.contract
            ex = e.execution
            report = e.commissionReport
            key = (contract.symbol, ex.permId)
            if key not in trades:
                trades[key] = {
                    "symbol": contract.symbol,
                    "secType": contract.secType,
                    "currency": contract.currency,
                    "exchange": contract.exchange,
                    "permId": ex.permId,
                    "side": ex.side,
                    "fills": [],
                    "totalShares": 0,
                    "avgPrice": 0,
                }
            trades[key]["fills"].append({
                "execId": ex.execId,
                "time": str(ex.time),
                "shares": float(ex.shares),
                "price": float(ex.price),
                "exchange": ex.exchange,
                "cumQty": float(ex.cumQty),
                "avgPrice": float(ex.avgPrice),
                "commission": float(getattr(report, "commission", 0.0) or 0.0),
                "realizedPNL": float(getattr(report, "realizedPNL", 0.0) or 0.0),
            })
            trades[key]["totalShares"] += float(ex.shares)
            trades[key]["avgPrice"] = float(ex.avgPrice)
        return list(trades.values())


@app.get("/search")
async def search(
    symbol: str = Query(..., min_length=1),
    exchange: Optional[str] = Query(None),
):
    ALLOWED_EXCHANGES = {"NYSE", "NASDAQ", "LSE", "CBOE", "LSEETF"}
    async with ib_session(subscribe_account=False) as ib:
        contracts = await ib.reqMatchingSymbolsAsync(symbol)
        results = []
        seen = set()
        requested_symbol = symbol.strip().upper()
        requested_exch = exchange.upper() if exchange else None
        for c in contracts:
            contract = c.contract
            if not contract.symbol or contract.conId <= 0: continue
            if contract.symbol.strip().upper() != requested_symbol: continue
            exch = ((contract.primaryExchange or contract.exchange) or "").strip().upper()
            if not exch: continue
            if requested_exch:
                if not exch.startswith(requested_exch): continue
            else:
                if exch not in ALLOWED_EXCHANGES: continue
            if contract.conId in seen: continue
            seen.add(contract.conId)
            results.append({
                "symbol": contract.symbol,
                "secType": contract.secType,
                "currency": contract.currency,
                "exchange": exch,
                "conId": contract.conId,
            })
        return results


# ============================================================
# Order placement
# ============================================================

async def _wait_for_callbacks(trades: List, max_wait: float) -> None:
    """Poll until every trade has a non-zero permId, or timeout."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        if all(t.order.permId != 0 for t in trades):
            return
        await asyncio.sleep(0.2)


# Statuses that mean IB has accepted the order onto its book.
LIVE_STATUSES = {"PreSubmitted", "Submitted", "Filled"}


async def _confirm_orders_live(trades: List, timeout: float) -> bool:
    """
    Wait until EVERY trade is accepted by IB: permId assigned (non-zero)
    AND orderStatus in a live state.

    Unlike _wait_for_callbacks, a False return is a hard failure the caller
    MUST act on -- never disconnect on a half-live bracket.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(
            t.order.permId != 0 and t.orderStatus.status in LIVE_STATUSES
            for t in trades
        ):
            return True
        await asyncio.sleep(0.25)
    return False


def _serialize_trade(trade) -> Dict:
    return {
        "orderId": int(trade.order.orderId),
        "permId": int(trade.order.permId),
        "clientId": int(trade.order.clientId),
        "action": trade.order.action,
        "orderType": trade.order.orderType,
        "quantity": _clean_float(trade.order.totalQuantity, default=0),
        "lmtPrice": _clean_float(getattr(trade.order, "lmtPrice", None)),
        "auxPrice": _clean_float(getattr(trade.order, "auxPrice", None)),
        "trailingPercent": _clean_float(getattr(trade.order, "trailingPercent", None)),
        "trailStopPrice": _clean_float(getattr(trade.order, "trailStopPrice", None)),
        "tif": getattr(trade.order, "tif", ""),
        "parentId": int(getattr(trade.order, "parentId", 0) or 0),
        "transmit": bool(getattr(trade.order, "transmit", True)),
        "status": trade.orderStatus.status,
        "filled": _clean_float(trade.orderStatus.filled, default=0),
        "remaining": _clean_float(trade.orderStatus.remaining, default=0),
        "whyHeld": getattr(trade.orderStatus, "whyHeld", "") or "",
        "log": [
            {
                "time": str(getattr(entry, "time", "")),
                "status": getattr(entry, "status", ""),
                "message": getattr(entry, "message", "") or "",
                "errorCode": getattr(entry, "errorCode", 0),
            }
            for entry in getattr(trade, "log", [])
        ],
    }


@app.post("/order")
async def place_order(req: OrderRequest):
    action = req.action.strip().upper()
    if action not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="action must be BUY or SELL")
    
    otype = req.order_type.strip().upper()
    if otype not in ("MKT", "LMT", "TRAIL", "STP"):
        raise HTTPException(status_code=400, detail="order_type must be MKT, LMT, TRAIL, or STP")

    if otype == "TRAIL" and (req.stop_loss is not None or req.take_profit is not None):
        raise HTTPException(status_code=400, detail="bracket orders cannot be combined with TRAIL")

    if otype == "TRAIL" and req.trailing_percent is None and req.trail_stop_price is None:
        raise HTTPException(status_code=400, detail="TRAIL requires trailing_percent or trail_stop_price")

    if otype == "STP" and (req.stop_loss is not None or req.take_profit is not None):
        raise HTTPException(status_code=400, detail="STP is a standalone protective stop; do not send stopLoss/takeProfit (those trigger the bracket path). Use stopPrice for the trigger.")

    if otype == "STP" and req.stop_price is None:
        raise HTTPException(status_code=400, detail="STP requires stopPrice (the trigger price)")
    
    async with ib_session(subscribe_account=False) as ib:
        acct = await _resolve_account(ib, req.account)
        contract = await _qualify_stock(
            ib,
            req.symbol.strip().upper(),
            req.exchange.strip().upper(),
        )
        
        trades = []
        
        if req.stop_loss is not None or req.take_profit is not None:
            # ---- BRACKET ORDER ----
            # MKT brackets are unstable in ib_insync (entry_price=None produces
            # malformed parent orders). Force LMT for bracketed entries.
            if otype != "LMT":
                raise HTTPException(
                    status_code=400,
                    detail="bracket orders require order_type=LMT (MKT bracket parents are unreliable)",
                )
            if req.limit_price is None:
                raise HTTPException(status_code=400, detail="limit_price required for LMT bracket")
            entry_price = req.limit_price

            bracket = ib.bracketOrder(
                action,
                req.quantity,
                entry_price,
                req.take_profit,
                req.stop_loss,
            )

            # Set account, RTH, tif on each leg.
            # GTC on children so they survive overnight session close (DAY
            # children get auto-cancelled at 4pm ET, leaving positions naked).
            child_tif = req.child_tif or "GTC"
            for i, o in enumerate(bracket):
                o.account = acct
                o.outsideRth = bool(req.outside_rth)
                o.tif = req.tif if i == 0 else child_tif

            # Canonical transmit pattern: parent + first child are queued with
            # transmit=False; only the final leg carries transmit=True, which
            # releases the whole bracket to IBKR atomically.
            bracket.parent.transmit = False
            bracket.takeProfit.transmit = False
            bracket.stopLoss.transmit = True

            for order in bracket:
                trades.append(ib.placeOrder(contract, order))

            # ---- HARD CONFIRMATION GATE ----
            # The per-request architecture disconnects the instant /order
            # returns. A bracket is only safe to disconnect from once IB has
            # ACCEPTED all three legs (permId assigned + live status). If the
            # children are still settling when we disconnect, IB drops them
            # and the parent is left to fill naked. Block here until every
            # leg is confirmed; treat failure as critical.
            confirmed = await _confirm_orders_live(
                trades, timeout=BRACKET_CONFIRM_TIMEOUT_SEC
            )

            if not confirmed:
                # Local Trade state can lag -- cross-check IB's own open-order
                # book, the source of truth, before acting.
                await ib.reqAllOpenOrdersAsync()
                await asyncio.sleep(1.0)
                live_ids = {
                    t.order.orderId
                    for t in ib.trades()
                    if t.orderStatus.status in LIVE_STATUSES
                }
                parent_live = bracket.parent.orderId in live_ids
                children_live = all(
                    o.orderId in live_ids
                    for o in (bracket.takeProfit, bracket.stopLoss)
                )

                if not children_live:
                    parent_filled = float(trades[0].orderStatus.filled) > 0

                    if parent_live and parent_filled:
                        # Worst case: entry filled, no protection on the book.
                        raise HTTPException(
                            status_code=500,
                            detail=(
                                "CRITICAL: parent filled but bracket children "
                                "not confirmed on IB - NAKED POSITION. Place a "
                                "stop and target manually NOW."
                            ),
                        )

                    if parent_live:
                        # Parent live but unfilled - cancel every leg so
                        # nothing can fill without protection.
                        for o in bracket:
                            try:
                                ib.cancelOrder(o)
                            except Exception:
                                pass
                        await asyncio.sleep(1.0)
                        raise HTTPException(
                            status_code=502,
                            detail=(
                                "bracket children failed to confirm; parent "
                                "cancelled - no position opened, safe to retry."
                            ),
                        )

                    raise HTTPException(
                        status_code=502,
                        detail=f"bracket failed to confirm; captured_errs={getattr(ib,'_captured_errs',None)}; parent_status={trades[0].orderStatus.status if trades else None}; parent_permId={trades[0].order.permId if trades else None}",
                    )
        else:
            # ---- SIMPLE ORDER ----
            if otype == "MKT":
                order = MarketOrder(action, req.quantity, tif=req.tif)
            elif otype == "TRAIL":
                order = Order(action=action, totalQuantity=req.quantity, orderType="TRAIL", tif=req.tif)
                if req.trailing_percent is not None:
                    order.trailingPercent = req.trailing_percent
                if req.trail_stop_price is not None:
                    order.auxPrice = req.trail_stop_price
            elif otype == "STP":
                # Standalone protective stop: rests below market, triggers a
                # market SELL when price falls through stop_price. Correct order
                # type for the R-floor (was mis-placed as LMT, which resting
                # above market behaves as a take-profit).
                order = StopOrder(action, req.quantity, req.stop_price, tif=req.tif)
            else:
                if req.limit_price is None:
                    raise HTTPException(status_code=400, detail="limit_price required for LMT")
                order = LimitOrder(action, req.quantity, req.limit_price, tif=req.tif)
            order.account = acct
            order.outsideRth = bool(req.outside_rth)
            trade = ib.placeOrder(contract, order)
            trades.append(trade)
        
        # Wait for callbacks to populate permIds, fills, log
        await _wait_for_callbacks(trades, ORDER_CALLBACK_MAX_WAIT_SEC)
        
        parent_trade = trades[0]
        
        return {
            "contract": {
                "symbol": contract.symbol,
                "secType": contract.secType,
                "exchange": contract.exchange,
                "currency": contract.currency,
                "conId": contract.conId,
                "primaryExchange": getattr(contract, "primaryExchange", ""),
            },
            "orders": [_serialize_trade(t) for t in trades],
            "execution": {
                "status": parent_trade.orderStatus.status,
                "filled": _clean_float(parent_trade.orderStatus.filled, default=0),
                "remaining": _clean_float(parent_trade.orderStatus.remaining, default=0),
                "avgFillPrice": _clean_float(parent_trade.orderStatus.avgFillPrice),
                "lastFillPrice": _clean_float(parent_trade.orderStatus.lastFillPrice),
            },
            "fills": [
                {
                    "execId": f.execution.execId,
                    "time": str(f.execution.time),
                    "shares": float(f.execution.shares),
                    "price": float(f.execution.price),
                    "avgPrice": float(f.execution.avgPrice),
                    "side": f.execution.side,
                    "cumQty": float(f.execution.cumQty),
                    "commission": float(getattr(f.commissionReport, "commission", 0.0) or 0.0),
                    "realizedPNL": float(getattr(f.commissionReport, "realizedPNL", 0.0) or 0.0),
                }
                for f in parent_trade.fills
            ],
        }


# ============================================================
# Order management — cancel
# ============================================================

@app.post("/cancel-order")
async def cancel_order(orderId: int = Query(..., description="The orderId returned from /order")):
    """
    Cancel an open order by orderId. New endpoint for V2 since we don't
    have shared trade state anymore — the order must be re-fetched.
    """
    async with ib_session(subscribe_account=False) as ib:
        await ib.reqAllOpenOrdersAsync()
        await asyncio.sleep(1.0)
        for t in ib.trades():
            if t.order.orderId == orderId:
                ib.cancelOrder(t.order)
                await asyncio.sleep(1.0)
                return {
                    "ok": True,
                    "orderId": orderId,
                    "status": t.orderStatus.status,
                }
        raise HTTPException(status_code=404, detail=f"Order {orderId} not found in open orders")


@app.post("/protect-oca")
async def protect_oca(
    symbol: str = Query(...),
    quantity: float = Query(..., gt=0),
    stopPrice: float = Query(..., gt=0, description="SELL STP trigger"),
    limitPrice: float = Query(..., gt=0, description="SELL LMT take-profit"),
    exchange: str = Query("SMART"),
    account: str | None = Query(None),
    tif: str = Query("GTC"),
    outsideRth: bool = Query(False),
):
    """Place a protective OCA pair for an EXISTING long (no parent entry):
    SELL STP @ stopPrice + SELL LMT @ limitPrice, one-cancels-all. Rebuilds a
    bracket after the originals were cancelled (e.g. raising the stop to breakeven
    while keeping the take-profit). A fill on either leg auto-cancels the other, so
    there is no orphan-stop short risk. Hard confirmation gate like /order brackets."""
    import time as _t
    async with ib_session(subscribe_account=False) as ib:
        acct = await _resolve_account(ib, account)
        contract = await _qualify_stock(ib, symbol.strip().upper(), exchange.strip().upper())
        oca = f"PROT_{symbol.upper()}_{int(_t.time())}"
        stp = Order(action="SELL", totalQuantity=quantity, orderType="STP", auxPrice=stopPrice,
                    tif=tif, account=acct, outsideRth=bool(outsideRth))
        lmt = Order(action="SELL", totalQuantity=quantity, orderType="LMT", lmtPrice=limitPrice,
                    tif=tif, account=acct, outsideRth=bool(outsideRth))
        for o in (stp, lmt):
            o.ocaGroup = oca
            o.ocaType = 1   # cancel remaining block on fill
        # Both legs transmit immediately; the shared ocaGroup links them. There is NO parent order
        # here, so the bracket "queue transmit=False, release with the last leg" pattern does NOT
        # apply - a queued transmit=False leg never gets released, never goes live, and the confirm
        # gate times out -> a false 502. For a standalone OCA pair, both must be transmit=True.
        stp.transmit = True
        lmt.transmit = True
        trades = [ib.placeOrder(contract, stp), ib.placeOrder(contract, lmt)]
        confirmed = await _confirm_orders_live(trades, timeout=BRACKET_CONFIRM_TIMEOUT_SEC)
        if not confirmed:
            await ib.reqAllOpenOrdersAsync()
            await asyncio.sleep(1.0)
            live_ids = {t.order.orderId for t in ib.trades() if t.orderStatus.status in LIVE_STATUSES}
            if not all(o.orderId in live_ids for o in (stp, lmt)):
                for o in (stp, lmt):
                    try:
                        ib.cancelOrder(o)
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
                raise HTTPException(
                    status_code=502,
                    detail="protective OCA pair failed to confirm; partial cancelled - POSITION UNPROTECTED, place stop+target manually NOW",
                )
        return {
            "ok": True, "symbol": symbol.upper(), "account": acct, "ocaGroup": oca, "qty": quantity,
            "stop": {"orderId": stp.orderId, "stopPrice": stopPrice, "permId": stp.permId},
            "target": {"orderId": lmt.orderId, "limitPrice": limitPrice, "permId": lmt.permId},
        }


# ============================================================
# Backward-compat stubs for V1 connection-mgmt endpoints
# These were needed in V1 to manage shared connection state.
# In V2 there's no shared state, so they just succeed trivially.
# ============================================================

@app.api_route("/disconnect", methods=["GET", "POST"])
async def disconnect():
    return {"status": "noop_in_v2", "note": "V2 has no shared connection to disconnect"}


@app.api_route("/reconnect", methods=["GET", "POST"])
async def reconnect():
    """Verify gateway is reachable by trying a fresh connect."""
    t0 = time.time()
    async with ib_session(subscribe_account=False) as ib:
        return {
            "status": "verified_via_fresh_session",
            "ib_connected": True,
            "duration_ms": (time.time() - t0) * 1000,
            "accounts": ib.managedAccounts(),
        }


@app.api_route("/reset-session", methods=["GET", "POST"])
async def reset_session():
    return await reconnect()


@app.api_route("/hard-reset", methods=["GET", "POST"])
async def hard_reset():
    return await reconnect()


@app.api_route("/nuclear", methods=["GET", "POST"])
async def nuclear():
    return await reconnect()


# ============================================================
# Gateway docker control endpoints — preserved verbatim from V1
# ============================================================

DOCKER_BASE = "http+unix://%2Fvar%2Frun%2Fdocker.sock"
DOCKER_CONTAINER = "ib-gateway"

# Minimum seconds between gateway restarts. A restart is a HARD container
# restart: it kills the JVM along with any 2FA challenge that has not yet been
# answered, and because IB Gateway only writes its autorestart token during its
# OWN scheduled self-restart, every such start demands full authentication.
#
# 17 n8n workflows call the Gateway Reconnect sub-workflow on any IBKR failure.
# While the gateway waits for 2FA it is not connected, so those workflows fail
# and call it again - a feedback loop that produced 445 restarts and 92 MFA
# prompts (for 5 successful logins) on 2026-09-02, peaking at 98/hour.
#
# The Postgres gateway_restart_lock is a mutex: it stops two restarts at once,
# not two hundred in sequence. This is the rate limit. It lives here rather than
# in the workflows so one guard covers all 17 callers.
#
# Set 0 to disable. POST /gateway/restart?force=true bypasses it deliberately.
GATEWAY_RESTART_COOLDOWN_SEC = env_int("GATEWAY_RESTART_COOLDOWN_SEC", 900)


def _parse_docker_ts(raw: str) -> Optional[datetime]:
    """Docker stamps RFC3339 with NANOsecond precision; fromisoformat takes at
    most microseconds, so the fraction is truncated to 6 digits."""
    if not raw:
        return None
    raw = raw.strip().replace("Z", "+00:00")
    if "." in raw:
        head, rest = raw.split(".", 1)
        digits = ""
        for ch in rest:
            if ch.isdigit():
                digits += ch
            else:
                break
        raw = f"{head}.{digits[:6]}{rest[len(digits):]}"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _gateway_started_at() -> Optional[datetime]:
    """When the gateway container last started, per Docker itself. Using the
    container as the source of truth means the cooldown survives a bridge
    restart and still holds if the gateway was restarted by any other route."""
    try:
        r = docker_session().get(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/json")
        if r.status_code != 200:
            return None
        return _parse_docker_ts((r.json().get("State") or {}).get("StartedAt"))
    except Exception:
        return None


def docker_session():
    return requests_unixsocket.Session()


@app.get("/gateway/status")
def gateway_status():
    s = docker_session()
    r = s.get(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/json")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=r.text)
    data = r.json()
    state = data.get("State", {})
    return {
        "name": data.get("Name", "").lstrip("/"),
        "status": state.get("Status"),
        "running": state.get("Running"),
        "started_at": state.get("StartedAt"),
    }


@app.post("/gateway/start")
def gateway_start():
    s = docker_session()
    r = s.post(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/start")
    return {"ok": r.status_code in (204, 304), "status_code": r.status_code}


@app.post("/gateway/stop")
def gateway_stop():
    s = docker_session()
    r = s.post(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/stop")
    return {"ok": r.status_code in (204, 304), "status_code": r.status_code}


# ---------------------------------------------------------------------------
# Restart safety. Every gateway restart forces a fresh IBKR 2FA prompt, so a
# restart is only ever useful when (a) the gateway is actually down and (b) a
# human is available to approve the 2FA. Two guards enforce that:
#   * connected-guard: refuse to restart a gateway whose API port is open. A
#     transient data error (an HMDS timeout, "no data") must never kill a live,
#     authenticated session - that is exactly what blew the connection out.
#   * circuit breaker: cap restarts per rolling window. Once a restart has fired
#     and the 2FA is left unanswered (overnight), restarting again just spawns
#     another prompt; the breaker stops that turning into a storm.
# force=true bypasses everything for a deliberate manual restart.
GATEWAY_RESTART_MAX_PER_WINDOW = env_int("GATEWAY_RESTART_MAX_PER_WINDOW", 2)
GATEWAY_RESTART_WINDOW_SEC = env_int("GATEWAY_RESTART_WINDOW_SEC", 21600)
_RESTART_STATE_FILE = os.environ.get(
    "GATEWAY_RESTART_STATE_FILE", "/state/gw_restart_history.json"
)


async def _ib_healthy() -> bool:
    """True only if a REAL IB API session can be established right now. Unlike a
    bare TCP check, a gateway stuck mid-login (port 4001 bound but not
    authenticated) reads as NOT healthy, so it stays restartable. One short,
    non-rotating probe on a dedicated client id: fails fast, never churns the
    main session's ids."""
    try:
        async with ib_session(subscribe_account=False, rotate=False,
                              client_id=IB_CLIENT_ID + 7, timeout=4) as ib:
            return bool(ib.isConnected())
    except Exception:
        return False


def _load_restart_history() -> list:
    try:
        with open(_RESTART_STATE_FILE) as fh:
            data = json.load(fh)
        return [float(t) for t in data] if isinstance(data, list) else []
    except Exception:
        return []


def _save_restart_history(hist: list) -> None:
    try:
        os.makedirs(os.path.dirname(_RESTART_STATE_FILE), exist_ok=True)
        with open(_RESTART_STATE_FILE, "w") as fh:
            json.dump(hist, fh)
    except Exception:
        pass


@app.post("/gateway/restart")
def gateway_restart(force: bool = Query(False, description="kept for compatibility; ignored")):
    # Reverted 2026-09-12 to the plain known-good behaviour: no guard, no cooldown,
    # no circuit breaker (those interfered with the reconnect flow's recovery).
    s = docker_session()
    r = s.post(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/restart")
    return {"ok": r.status_code in (204, 304), "status_code": r.status_code}


@app.get("/gateway/logs")
def gateway_logs(tail: int = 50):
    s = docker_session()
    r = s.get(
        f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/logs"
        f"?stdout=1&stderr=1&tail={tail}"
    )
    return {"logs": r.text}


@app.post("/gateway/kill")
def gateway_kill():
    s = docker_session()
    r = s.post(f"{DOCKER_BASE}/containers/{DOCKER_CONTAINER}/kill")
    return {"ok": r.status_code in (204, 304), "status_code": r.status_code}


@app.get("/snapshot")
async def snapshot(account: Optional[str] = Query(None)):
    """Combined account summary + enriched portfolio in one IB session.

    'positions' carries the same fields as /portfolio (name, marketPrice,
    marketValue, unrealizedPnL). 'account' is the /account summary. The
    optional 'account' query param scopes BOTH halves to that account id.
    """
    async with ib_session() as ib:
        # ---- resolve which account this call is scoped to ----
        acct = await _resolve_account(ib, account)

        # ---- account summary (logic identical to /account) ----
        vals = await ib.accountSummaryAsync(acct)
        account_result = {v.tag: v.value for v in vals}

        pending_by_currency = {}
        pending_orders = []
        for trade in ib.openTrades():
            order = trade.order
            contract = trade.contract
            if getattr(order, "parentId", 0) != 0:
                continue
            if order.action != "BUY":
                continue
            remaining = getattr(order, "remaining", order.totalQuantity or 0) or 0
            if remaining <= 0:
                continue
            lmt = getattr(order, "lmtPrice", 0) or 0
            aux = getattr(order, "auxPrice", 0) or 0
            price = lmt if lmt > 0 else aux
            if price <= 0:
                continue
            currency = (getattr(contract, "currency", "") or "USD").upper()
            notional = remaining * price
            pending_by_currency[currency] = pending_by_currency.get(currency, 0.0) + notional
            pending_orders.append({
                "symbol":    getattr(contract, "symbol", ""),
                "orderId":   order.orderId,
                "action":    order.action,
                "orderType": order.orderType,
                "quantity":  remaining,
                "price":     price,
                "currency":  currency,
                "notional":  round(notional, 2),
            })
        account_result["PendingParentBuyValueByCurrency"] = {k: round(v, 2) for k, v in pending_by_currency.items()}
        account_result["PendingParentBuyOrders"] = pending_orders
        account_result["PendingParentBuyCount"] = len(pending_orders)

        # ---- enriched portfolio (logic identical to /portfolio) ----
        positions = await ib.reqPositionsAsync()
        # Filter to the resolved account BEFORE enrichment so we don't
        # fetch contract details / historical bars for other accounts.
        positions = [p for p in positions if (not acct or p.account == acct)]

        prices = await _price_contracts(ib, [p.contract for p in positions])
        positions_result = []
        for p in positions:
            c = p.contract

            name = CONTRACT_NAME_CACHE.get(c.conId)
            if not name:
                try:
                    details = await ib.reqContractDetailsAsync(c)
                    if details:
                        name = details[0].longName or c.symbol
                        CONTRACT_NAME_CACHE[c.conId] = name
                    else:
                        name = c.symbol
                except Exception:
                    name = c.symbol

            px = prices.get(c.conId, {"price": 0.0, "source": "none"})
            market_price = px["price"]

            position = float(p.position)
            avg_cost = float(p.avgCost)
            market_value = position * market_price if market_price else 0.0
            unrealized_pnl = (market_price - avg_cost) * position if market_price else 0.0

            positions_result.append({
                "symbol": getattr(c, "symbol", ""),
                "name": name,
                "secType": getattr(c, "secType", ""),
                "exchange": getattr(c, "exchange", ""),
                "primaryExchange": getattr(c, "primaryExchange", ""),
                "currency": getattr(c, "currency", ""),
                "conId": getattr(c, "conId", None),
                "position": position,
                "avgCost": avg_cost,
                "marketPrice": market_price,
                "priceSource": px["source"],
                "marketValue": market_value,
                "unrealizedPnL": unrealized_pnl,
                "realizedPnL": 0.0,
            })

        return {"account": account_result, "positions": positions_result}


@app.get("/nav")
async def nav(account: Optional[str] = Query(None)):
    """Daily NAV snapshot per account. NetLiquidation in each account's base currency."""
    async with ib_session(subscribe_account=False) as ib:
        accounts = [account] if account else ib.managedAccounts()
        out = []
        for acct in accounts:
            vals = await ib.accountSummaryAsync(acct)
            for v in vals:
                if v.tag == "NetLiquidation":
                    out.append({
                        "account": v.account,
                        "nav": float(v.value),
                        "currency": v.currency,
                    })
        return {"navs": out}
