import os
import time
import sys
import logging
import threading
import signal
import json
import urllib.parse
from decimal import Decimal, getcontext, ROUND_HALF_UP, ROUND_FLOOR
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
from functools import wraps

# Libs
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError
from requests.exceptions import RequestException, ConnectionError, Timeout
from dotenv import load_dotenv

# ==========================================
# 0. CONFIG & PRECISION
# ==========================================
load_dotenv()
getcontext().prec = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("HEDGE_ULTIMATE")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")
    SYMBOLS = ["1000PEPEUSDC"]
    LEVERAGE = 16
    BASE_ORDER_SIZE = Decimal("8.0")
    GRID_LEVELS = 16
    FIB_STEP_BASE = Decimal("0.00015")
    VOL_COEFF = Decimal("80.0")
    TAKE_PROFIT_PCT = Decimal("0.0007")
    PAGEN = 3
    WATCHDOG_TIMEOUT = 60


@dataclass
class SymbolPrecision:
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    min_notional: Decimal
    price_precision: int
    qty_precision: int


@dataclass
class SymbolState:
    symbol: str
    info: SymbolPrecision
    last_price: Decimal = Decimal("0")
    long_amt: Decimal = Decimal("0")
    long_entry: Decimal = Decimal("0")
    short_amt: Decimal = Decimal("0")
    short_entry: Decimal = Decimal("0")
    long_grid_center: Decimal = Decimal("0")
    short_grid_center: Decimal = Decimal("0")
    trailing_threshold_pct: Decimal = Decimal("0")


# ==========================================
# 1. DECORATOR: RETRY LOGIC
# ==========================================
def retry_request(max_retries=3, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_err = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, Timeout, RequestException) as e:
                    time.sleep(delay);
                    last_err = e
                except ClientError as e:
                    if int(e.status_code) >= 500:
                        time.sleep(delay);
                        last_err = e
                    else:
                        raise e
            if last_err: raise last_err

        return wrapper

    return decorator


# ==========================================
# 2. BOT CLASS
# ==========================================
class HedgeBot:
    def __init__(self):
        self.running = True
        self.lock = threading.RLock()
        self.client: Optional[UMFutures] = None
        self.ws_client: Optional[UMFuturesWebsocketClient] = None
        self.states: Dict[str, SymbolState] = {}
        self.last_ws_update = time.time()
        self.listen_key = None

    def initialize(self):
        log.info("🔹 Starting Bot (Bypassing SDK bugs)...")
        if not Config.API_KEY: log.critical("❌ No API Keys"); sys.exit(1)
        try:
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)
            ex_info = self.client.exchange_info()
            all_info = {s['symbol']: s for s in ex_info['symbols']}
            for sym in Config.SYMBOLS:
                if sym not in all_info: continue
                s_info = all_info[sym]
                p_f = next(f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER')
                l_f = next(f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE')
                n_f = next((f for f in s_info['filters'] if f['filterType'] in ['MIN_NOTIONAL', 'NOTIONAL']), None)
                mn = Decimal("5.0")
                if n_f:
                    if 'notional' in n_f:
                        mn = Decimal(str(n_f['notional']))
                    elif 'minNotional' in n_f:
                        mn = Decimal(str(n_f['minNotional']))
                prec = SymbolPrecision(
                    tick_size=Decimal(str(p_f['tickSize'])), step_size=Decimal(str(l_f['stepSize'])),
                    min_qty=Decimal(str(l_f['minQty'])), min_notional=mn,
                    price_precision=int(s_info['pricePrecision']), qty_precision=int(s_info['quantityPrecision'])
                )
                dist = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.PAGEN))
                self.states[sym] = SymbolState(symbol=sym, info=prec, trailing_threshold_pct=dist)
                self._setup_account(sym)
                ticker = self.client.ticker_price(sym)
                self.states[sym].last_price = Decimal(str(ticker['price']))
            self._sync_all_positions_rest()
            log.info("✅ Sync position done")
        except Exception as e:
            log.critical(f"Init Fail: {e}"); sys.exit(1)

    def _setup_account(self, symbol):
        try:
            try:
                m = self.client.get_position_mode()
                if not m['dualSidePosition']: self.client.change_position_mode(dualSidePosition="true")
            except:
                pass
            self.client.change_leverage(symbol, Config.LEVERAGE)
        except Exception as e:
            log.warning(f"⚠️ Setup {symbol}: {e}")

    # --- MATH (DECIMAL) ---
    def _fib(self, n: int) -> List[int]:
        seq = [1, 1]
        for i in range(2, n): seq.append(seq[-1] + seq[-2])
        return seq[:n]

    def _calc_grid(self, base: Decimal, direction: str) -> List[Tuple[Decimal, Decimal, Decimal, Decimal]]:
        lvls = []
        fib_seq = self._fib(Config.GRID_LEVELS)
        cum_dist = Decimal("0")
        for i in range(Config.GRID_LEVELS):
            step = Config.FIB_STEP_BASE * Decimal(str(fib_seq[i]))
            cum_dist += step
            price = base * (Decimal("1.0") - cum_dist) if direction == "LONG" else base * (Decimal("1.0") + cum_dist)
            multiplier = max(Decimal("1.0"), Decimal("1.0") + (step * Config.VOL_COEFF))
            vol_usd = Config.BASE_ORDER_SIZE * multiplier
            qty = vol_usd / price
            lvls.append((price, qty, vol_usd, cum_dist))
        return lvls

    def _recon(self, avg_entry: Decimal, qty: Decimal, direction: str, symbol: str) -> Decimal:
        if qty == Decimal("0"): return self.states[symbol].last_price
        grid = self._calc_grid(avg_entry, direction)
        filled = []
        acc_vol = Decimal("0")
        target_vol = qty * avg_entry
        for p, q, v, d in grid:
            filled.append((v, d));
            acc_vol += v
            if acc_vol >= target_vol * Decimal("0.9"): break
        if not filled: return avg_entry
        num = sum(v * d for v, d in filled)
        den = sum(v for v, d in filled)
        avg_dist = num / den if den > 0 else Decimal("0")
        return avg_entry / (Decimal("1.0") - avg_dist) if direction == "LONG" else avg_entry / (
                    Decimal("1.0") + avg_dist)

    def _rp(self, p: Decimal, info: SymbolPrecision) -> str:
        val = (p / info.tick_size).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * info.tick_size
        return "{:.{prec}f}".format(val, prec=info.price_precision)

    def _rq(self, q: Decimal, info: SymbolPrecision) -> str:
        val = (q / info.step_size).quantize(Decimal('1'), rounding=ROUND_FLOOR) * info.step_size
        return "{:.{prec}f}".format(val, prec=info.qty_precision)

    # --- DIRECT API CALLS (BYPASSING SDK BUGS) ---
    @retry_request(max_retries=3)
    def _safe_get_open_orders(self, symbol):
        return self.client.sign_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    @retry_request(max_retries=3)
    def _cancel_side_orders(self, symbol, pos_side):
        all_open = self._safe_get_open_orders(symbol)
        ids = [o['orderId'] for o in all_open if o['positionSide'] == pos_side]
        if not ids: return
        for i in range(0, len(ids), 10):
            chunk = ids[i: i + 10]
            # Прямой вызов DELETE /fapi/v1/batchOrders
            params = {"symbol": symbol, "orderIdList": json.dumps(chunk)}
            self.client.sign_request("DELETE", "/fapi/v1/batchOrders", params)

    @retry_request(max_retries=3)
    def _place_batch(self, symbol, params_list):
        if not params_list: return
        for i in range(0, len(params_list), 5):
            chunk = params_list[i: i + 5]
            # Прямой вызов POST /fapi/v1/batchOrders
            # ВАЖНО: batchOrders должен быть JSON-строкой
            query = {"symbol": symbol, "batchOrders": json.dumps(chunk)}
            self.client.sign_request("POST", "/fapi/v1/batchOrders", query)

    @retry_request(max_retries=3)
    def _sync_all_positions_rest(self):
        pos_data = self.client.get_position_risk()
        with self.lock:
            for s in self.states.values():
                s.long_amt = Decimal("0");
                s.short_amt = Decimal("0")
            for p in pos_data:
                sym = p['symbol']
                if sym in self.states:
                    amt, ent = Decimal(str(p['positionAmt'])), Decimal(str(p['entryPrice']))
                    if p['positionSide'] == "LONG":
                        self.states[sym].long_amt, self.states[sym].long_entry = amt, ent
                    elif p['positionSide'] == "SHORT":
                        self.states[sym].short_amt, self.states[sym].short_entry = abs(amt), ent

    # --- CORE LOGIC ---
    def update_strategy_for_side(self, symbol, pos_side):
        if symbol not in self.states: return
        state = self.states[symbol]
        info = state.info
        try:
            with self.lock:
                self._cancel_side_orders(symbol, pos_side)
                is_l = (pos_side == "LONG")
                amt, entry = (state.long_amt, state.long_entry) if is_l else (state.short_amt, state.short_entry)

                if amt > info.min_qty:
                    tp_p = entry * (Decimal("1.0") + Config.TAKE_PROFIT_PCT) if is_l else entry * (
                                Decimal("1.0") - Config.TAKE_PROFIT_PCT)
                    tp_ps, tp_qs = self._rp(tp_p, info), self._rq(amt, info)
                    try:
                        self.client.new_order(symbol=symbol, side="SELL" if is_l else "BUY", positionSide=pos_side,
                                              type="LIMIT", quantity=tp_qs, price=tp_ps, timeInForce="GTC")
                        log.info(f"[{symbol}] 🎯 TP {pos_side} @ {tp_ps}")
                    except ClientError as e:
                        if e.error_code == -2022: amt = Decimal("0")

                    if amt > 0:
                        recon_base = self._recon(entry, amt, pos_side, symbol)
                        grid = self._calc_grid(recon_base, pos_side)
                        batch = []
                        for p, q, v, d in grid:
                            if (is_l and p < state.last_price * Decimal("0.9997")) or (
                                    not is_l and p > state.last_price * Decimal("1.0003")):
                                ps, qs = self._rp(p, info), self._rq(q, info)
                                if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                                    batch.append(
                                        {"symbol": symbol, "side": "BUY" if is_l else "SELL", "positionSide": pos_side,
                                         "type": "LIMIT", "quantity": qs, "price": ps, "timeInForce": "GTX"})
                        self._place_batch(symbol, batch)

                if amt <= info.min_qty:
                    center = state.last_price
                    if is_l:
                        state.long_grid_center = center
                    else:
                        state.short_grid_center = center
                    log.info(f"[{symbol}] 🆕 Start {pos_side} @ {center}")
                    grid = self._calc_grid(center, pos_side)
                    batch = []
                    for p, q, v, d in grid:
                        ps, qs = self._rp(p, info), self._rq(q, info)
                        if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                            batch.append({"symbol": symbol, "side": "BUY" if is_l else "SELL", "positionSide": pos_side,
                                          "type": "LIMIT", "quantity": qs, "price": ps, "timeInForce": "GTX"})
                    self._place_batch(symbol, batch)
        except Exception as e:
            log.error(f"[{symbol}] ❌ Strategy Error {pos_side}: {e}")

    # --- WEB SOCKET ---
    def on_ws_msg(self, _, m):
        try:
            msg = json.loads(m) if isinstance(m, str) else m
            self.last_ws_update = time.time()
            e = msg.get('e')
            if e == 'aggTrade':
                s = msg['s']
                if s in self.states:
                    price = Decimal(str(msg['p']))
                    with self.lock:
                        st = self.states[s];
                        st.last_price = price
                        th = st.trailing_threshold_pct
                        if st.long_amt == 0 and st.long_grid_center > 0:
                            if (price - st.long_grid_center) / st.long_grid_center > th:
                                st.long_grid_center = price
                                threading.Thread(target=self.update_strategy_for_side, args=(s, "LONG"),
                                                 daemon=True).start()
                        if st.short_amt == 0 and st.short_grid_center > 0:
                            if (st.short_grid_center - price) / st.short_grid_center > th:
                                st.short_grid_center = price
                                threading.Thread(target=self.update_strategy_for_side, args=(s, "SHORT"),
                                                 daemon=True).start()
            elif e == 'ORDER_TRADE_UPDATE':
                o = msg['o']
                if o['X'] == 'FILLED' and o['s'] in self.states:
                    log.info(f"[{o['s']}] ⚡ FILLED {o['ps']} {o['S']}")
                    threading.Thread(target=self.update_strategy_for_side, args=(o['s'], o['ps']), daemon=True).start()
            elif e == 'ACCOUNT_UPDATE':
                for p in msg['a']['P']:
                    sym = p['s']
                    if sym in self.states:
                        with self.lock:
                            amt, ent, ps = Decimal(str(p['pa'])), Decimal(str(p['ep'])), p['ps']
                            if ps == "LONG":
                                self.states[sym].long_amt, self.states[sym].long_entry = amt, ent
                            elif ps == "SHORT":
                                self.states[sym].short_amt, self.states[sym].short_entry = abs(amt), ent
        except:
            pass

    def run_maintenance(self):
        last_renew = last_audit = time.time()
        while self.running:
            try:
                if time.time() - last_renew > 1800:
                    self.client.renew_listen_key(self.listen_key);
                    last_renew = time.time()
                if time.time() - last_audit > 60:
                    self._sync_all_positions_rest();
                    last_audit = time.time()
                self.client.time()
            except:
                pass
            time.sleep(30)

    def run(self):
        log.info(f"🚀 Bot starting: {Config.SYMBOLS}")
        self.initialize()
        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_ws_msg)
        self.listen_key = self.client.new_listen_key()['listenKey']
        self.ws_client.user_data(listen_key=self.listen_key)
        for sym in Config.SYMBOLS: self.ws_client.agg_trade(symbol=sym.lower())
        threading.Thread(target=self.run_maintenance, daemon=True).start()

        def watchdog():
            while self.running:
                if time.time() - self.last_ws_update > Config.WATCHDOG_TIMEOUT:
                    log.critical("🚨 WebSocket Dead!");
                    os.kill(os.getpid(), signal.SIGINT)
                time.sleep(10)

        threading.Thread(target=watchdog, daemon=True).start()
        for sym in Config.SYMBOLS:
            self.update_strategy_for_side(sym, "LONG");
            self.update_strategy_for_side(sym, "SHORT")
        try:
            while self.running: time.sleep(1)
        except KeyboardInterrupt:
            self.running = False; self.ws_client.stop()


if __name__ == "__main__":
    HedgeBot().run()