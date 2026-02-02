import os
import time
import sys
import logging
import threading
import signal
import json
from decimal import Decimal, getcontext, ROUND_HALF_UP, ROUND_FLOOR
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field
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
log = logging.getLogger("HEDGE_ULTIMATE_15L_PRO")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    SYMBOLS = ["SUIUSDC", "DOGEUSDC", "1000PEPEUSDC", "XRPUSDC"]

    # --- РИСК-МЕНЕДЖМЕНТ (Для 15 уровней и 25 плеча ---- 10) ---
    BALANCE_PER_1_DOLLAR_ORDER = Decimal("6.5")

    MIN_ORDER_SIZE = Decimal("5.2")
    MAX_ORDER_SIZE = Decimal("25")

    GRID_LEVELS = 14
    FIB_STEP_BASE = Decimal("0.00015")
    VOL_COEFF = Decimal("80.0")
    TAKE_PROFIT_PCT = Decimal("0.0007")
    PAGEN = 3

    # --- СТОП-ЛОСС (1.5% за пределами 15-го уровня) ---
    STOP_LOSS_BEYOND_GRID_PCT = Decimal("0.02")

    WATCHDOG_TIMEOUT = 60
    AUDIT_INTERVAL = 60
    STATE_FILE = "bot_state.json"


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

    # Эти данные теперь сохраняются на диск
    long_grid_center: Decimal = Decimal("0")
    short_grid_center: Decimal = Decimal("0")
    current_long_order_size: Decimal = Config.MIN_ORDER_SIZE
    current_short_order_size: Decimal = Config.MIN_ORDER_SIZE

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
                        time.sleep(delay); last_err = e
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
        log.info("🔹 Starting Ultimate Bot (15 Levels + Full Memory + SL)...")
        if not Config.API_KEY: log.critical("❌ No API Keys"); sys.exit(1)
        try:
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)
            ex_info = self.client.exchange_info()
            all_info = {s['symbol']: s for s in ex_info['symbols']}

            # Загружаем "память" с диска
            saved_data = self._load_state_from_disk()

            for sym in Config.SYMBOLS:
                if sym not in all_info: continue
                s_info = all_info[sym]
                p_f = next(f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER')
                l_f = next(f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE')
                n_f = next((f for f in s_info['filters'] if f['filterType'] in ['MIN_NOTIONAL', 'NOTIONAL']), None)
                mn = Decimal("6.0")
                if n_f: mn = Decimal(str(n_f.get('notional', n_f.get('minNotional', 6.0))))

                prec = SymbolPrecision(
                    tick_size=Decimal(str(p_f['tickSize'])), step_size=Decimal(str(l_f['stepSize'])),
                    min_qty=Decimal(str(l_f['minQty'])), min_notional=mn,
                    price_precision=int(s_info['pricePrecision']), qty_precision=int(s_info['quantityPrecision'])
                )
                dist = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.PAGEN))

                st = SymbolState(symbol=sym, info=prec, trailing_threshold_pct=dist)

                # Восстанавливаем ВСЕ параметры сделки
                if sym in saved_data:
                    st.current_long_order_size = Decimal(str(saved_data[sym].get('l_size', Config.MIN_ORDER_SIZE)))
                    st.current_short_order_size = Decimal(str(saved_data[sym].get('s_size', Config.MIN_ORDER_SIZE)))
                    st.long_grid_center = Decimal(str(saved_data[sym].get('l_center', "0")))
                    st.short_grid_center = Decimal(str(saved_data[sym].get('s_center', "0")))
                    log.info(f"[{sym}] 💾 Full State Restored. SL logic active.")

                self.states[sym] = st
                self._setup_account(sym)
                ticker = self.client.ticker_price(sym)
                self.states[sym].last_price = Decimal(str(ticker['price']))

            self._sync_all_positions_rest()
            log.info("✅ Persistence ready. Trading started.")
        except Exception as e:
            log.critical(f"Init Fail: {e}"); sys.exit(1)

    # --- PERSISTENCE ---
    def _save_state_to_disk(self):
        try:
            data = {}
            with self.lock:
                for sym, st in self.states.items():
                    data[sym] = {
                        "l_size": str(st.current_long_order_size),
                        "s_size": str(st.current_short_order_size),
                        "l_center": str(st.long_grid_center),
                        "s_center": str(st.short_grid_center)
                    }
            with open(Config.STATE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            log.error(f"Save state error: {e}")

    def _load_state_from_disk(self) -> dict:
        if os.path.exists(Config.STATE_FILE):
            try:
                with open(Config.STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}

    # --- DYNAMIC RISK ---
    def _get_dynamic_order_size(self):
        try:
            acc = self.client.account()
            balance = Decimal(str(acc['totalWalletBalance']))
            calc = balance / (Decimal(str(len(Config.SYMBOLS))) * Config.BALANCE_PER_1_DOLLAR_ORDER)
            return max(Config.MIN_ORDER_SIZE, min(calc, Config.MAX_ORDER_SIZE))
        except:
            return Config.MIN_ORDER_SIZE

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

    def _fib(self, n: int) -> List[int]:
        seq = [1, 1]
        for i in range(2, n): seq.append(seq[-1] + seq[-2])
        return seq[:n]

    def _calc_grid(self, base: Decimal, direction: str, order_size: Decimal) -> List[
        Tuple[Decimal, Decimal, Decimal, Decimal]]:
        lvls = []
        fib_seq = self._fib(Config.GRID_LEVELS)
        cum_dist = Decimal("0")
        for i in range(Config.GRID_LEVELS):
            step = Config.FIB_STEP_BASE * Decimal(str(fib_seq[i]))
            cum_dist += step
            price = base * (Decimal("1.0") - cum_dist) if direction == "LONG" else base * (Decimal("1.0") + cum_dist)
            multiplier = max(Decimal("1.0"), Decimal("1.0") + (step * Config.VOL_COEFF))
            qty = (order_size * multiplier) / price
            lvls.append((price, qty, order_size * multiplier, cum_dist))
        return lvls

    def _recon(self, avg_entry: Decimal, qty: Decimal, direction: str, symbol: str, order_size: Decimal) -> Decimal:
        if qty == Decimal("0"): return self.states[symbol].last_price
        grid = self._calc_grid(avg_entry, direction, order_size)
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

    @retry_request()
    def _safe_get_open_orders(self, symbol):
        return self.client.sign_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    @retry_request()
    def _cancel_side_orders(self, symbol, pos_side):
        all_open = self._safe_get_open_orders(symbol)
        ids = [o['orderId'] for o in all_open if o['positionSide'] == pos_side]
        if not ids: return
        for i in range(0, len(ids), 10):
            params = {"symbol": symbol, "orderIdList": json.dumps(ids[i:i + 10])}
            self.client.sign_request("DELETE", "/fapi/v1/batchOrders", params)

    @retry_request()
    def _place_batch(self, symbol, params_list):
        if not params_list: return
        for i in range(0, len(params_list), 5):
            query = {"symbol": symbol, "batchOrders": json.dumps(params_list[i:i + 5])}
            self.client.sign_request("POST", "/fapi/v1/batchOrders", query)

    @retry_request()
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

    # --- CORE STRATEGY ---
    def update_strategy_for_side(self, symbol, pos_side):
        if symbol not in self.states: return
        state = self.states[symbol]
        info = state.info
        try:
            with self.lock:
                self._cancel_side_orders(symbol, pos_side)

                is_l = (pos_side == "LONG")
                amt, entry = (state.long_amt, state.long_entry) if is_l else (state.short_amt, state.short_entry)
                current_size = state.current_long_order_size if is_l else state.current_short_order_size

                # 1. АВАРИЙНЫЙ СТОП (Теперь работает всегда, т.к. center сохраняется)
                if amt > info.min_qty:
                    center = state.long_grid_center if is_l else state.short_grid_center
                    if center > 0:
                        grid_depth = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.GRID_LEVELS))
                        total_stop_threshold = grid_depth + Config.STOP_LOSS_BEYOND_GRID_PCT
                        price_move = (center - state.last_price) / center if is_l else (
                                                                                                   state.last_price - center) / center

                        if price_move > total_stop_threshold:
                            log.critical(f"[{symbol}] 🚨 SL TRIGGERED {pos_side}! Move: {price_move:.2%}. Closing.")
                            self.client.new_order(symbol=symbol, side="SELL" if is_l else "BUY",
                                                  positionSide=pos_side, type="MARKET", quantity=self._rq(amt, info))
                            return

                            # 2. ТЕЙК-ПРОФИТ
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

                    # 3. СЕТКА УСРЕДНЕНИЯ
                    if amt > 0:
                        recon_base = self._recon(entry, amt, pos_side, symbol, current_size)
                        grid = self._calc_grid(recon_base, pos_side, current_size)
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

                # 4. НОВАЯ СЕТКА (Старт)
                if amt <= info.min_qty:
                    new_dynamic_size = self._get_dynamic_order_size()
                    if is_l:
                        state.current_long_order_size = new_dynamic_size
                        state.long_grid_center = state.last_price
                    else:
                        state.current_short_order_size = new_dynamic_size
                        state.short_grid_center = state.last_price

                    self._save_state_to_disk()  # СОХРАНЯЕМ ВСЁ, включая center
                    log.info(f"[{symbol}] 🆕 Start {pos_side} @ {state.last_price}. Size: {new_dynamic_size}$")

                    grid = self._calc_grid(state.last_price, pos_side, new_dynamic_size)
                    batch = []
                    for p, q, v, d in grid:
                        ps, qs = self._rp(p, info), self._rq(q, info)
                        if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                            batch.append({"symbol": symbol, "side": "BUY" if is_l else "SELL", "positionSide": pos_side,
                                          "type": "LIMIT", "quantity": qs, "price": ps, "timeInForce": "GTX"})
                    self._place_batch(symbol, batch)

        except Exception as e:
            log.error(f"[{symbol}] ❌ Strategy Error {pos_side}: {e}")

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
                if time.time() - last_audit > Config.AUDIT_INTERVAL:
                    self._sync_all_positions_rest();
                    last_audit = time.time()
                self.client.time()
            except:
                pass
            time.sleep(10)

    def run(self):
        log.info(f"🚀 Starting Bot. Persistence + SL Active. Symbols: {Config.SYMBOLS}")
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
            self.running = False;
            self.ws_client.stop()


if __name__ == "__main__":
    HedgeBot().run()