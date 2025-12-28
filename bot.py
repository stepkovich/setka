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
# 0. CONFIG (SAFE SCALPING MODE)
# ==========================================
load_dotenv()
getcontext().prec = 28

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("SAFE_BOT")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    # ТОРГУЕМЫЕ ПАРЫ
    SYMBOLS = ["1000PEPEUSDC", "SUIUSDC", "1000SHIBUSDC", "ENAUSDC"]

    LEVERAGE = 10

    # Стартовый ордер
    BASE_ORDER_SIZE = 7.0

    # --- СЕТКА  ---
    GRID_LEVELS = 6

    # Шаг 0.2% (Фибоначчи раздвинет его: 0.1% -> 0.1% -> 0.2% -> 0.3% -> 0.5%)
    FIB_STEP_BASE = 0.002

    # Множитель объема
    VOL_COEFF = 1.25

    # --- ВЫХОД ---
    TAKE_PROFIT_PCT = 0.0016

    # --- ЗАЩИТА (STOP LOSS) ---
    STOP_LOSS_PCT = 0.04

    # Трейлинг входа шага
    PAGEN = 1

    RECONNECT_DELAY = 5
    WATCHDOG_TIMEOUT = 60


@dataclass
class SymbolPrecision:
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float
    price_precision: int
    qty_precision: int


@dataclass
class GridLevel:
    level_index: int
    price: float
    qty: float
    vol_usd: float
    dist_pct: float


@dataclass
class SymbolState:
    symbol: str
    info: SymbolPrecision

    last_price: float = 0.0

    long_amt: float = 0.0
    long_entry: float = 0.0
    short_amt: float = 0.0
    short_entry: float = 0.0

    long_grid_center: float = 0.0
    short_grid_center: float = 0.0

    trailing_threshold_pct: float = 0.0


# ==========================================
# 1. DECORATOR: RETRY LOGIC
# ==========================================
def retry_request(max_retries=3, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, Timeout, RequestException) as e:
                    if i == 0:
                        log.info(f"⚠️ Network glitch in {func.__name__}, retrying...")
                    else:
                        log.warning(f"⚠️ Retry {i + 1}/{max_retries}: {e}")
                    time.sleep(delay)
                    last_exception = e
                except ClientError as e:
                    raise e
                except Exception as e:
                    log.error(f"❌ Unknown Error in {func.__name__}: {e}")
                    raise e
            if last_exception:
                raise last_exception

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
        log.info("🔹 Init SAFE Bot (StopLoss 2.5%, Grid 3 Lvls)...")
        if not Config.API_KEY: log.critical("❌ No API Keys"); sys.exit(1)

        try:
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)

            # 1. Загружаем инфо
            exchange_info = self.client.exchange_info()
            all_symbols_info = {s['symbol']: s for s in exchange_info['symbols']}

            # 2. Инициализируем стейты
            for sym in Config.SYMBOLS:
                if sym not in all_symbols_info:
                    log.error(f"❌ Symbol {sym} not found!")
                    continue

                s_info = all_symbols_info[sym]
                p_f = next(f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER')
                l_f = next(f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE')
                n_f = next((f for f in s_info['filters'] if f['filterType'] in ['MIN_NOTIONAL', 'NOTIONAL']), None)
                mn = 5.0
                if n_f:
                    if 'notional' in n_f:
                        mn = float(n_f['notional'])
                    elif 'minNotional' in n_f:
                        mn = float(n_f['minNotional'])

                precision = SymbolPrecision(
                    tick_size=float(p_f['tickSize']), step_size=float(l_f['stepSize']),
                    min_qty=float(l_f['minQty']), min_notional=mn,
                    price_precision=s_info['pricePrecision'], qty_precision=s_info['quantityPrecision']
                )

                dist = sum(Config.FIB_STEP_BASE * f for f in self._fib(Config.PAGEN))

                self.states[sym] = SymbolState(
                    symbol=sym,
                    info=precision,
                    trailing_threshold_pct=dist
                )

                self._setup_account(sym)
                ticker = self.client.ticker_price(symbol=sym)
                self.states[sym].last_price = float(ticker['price'])
                log.info(f"✅ Loaded {sym}: Price={self.states[sym].last_price}")

            # 3. Синхронизируем позиции
            self._sync_all_positions()
            self._ping_rest()

        except Exception as e:
            log.critical(f"Init Fail: {e}");
            sys.exit(1)

    def _setup_account(self, symbol):
        try:
            try:
                m = self.client.get_position_mode()
                if not m['dualSidePosition']: self.client.change_position_mode(dualSidePosition="true")
            except:
                pass
            self.client.change_leverage(symbol=symbol, leverage=Config.LEVERAGE)
        except Exception as e:
            log.warning(f"⚠️ Setup account {symbol}: {e}")

    # --- MATH ---
    def _fib(self, n):
        seq = [1, 1]
        for i in range(2, n): seq.append(seq[-1] + seq[-2])
        return seq[:n]

    def _calc_grid(self, base, direction):
        lvls = []
        fib = self._fib(Config.GRID_LEVELS)
        cum = 0.0

        # Объем растет геометрически: Base -> Base*1.5 -> Base*1.5*1.5
        current_usd = Config.BASE_ORDER_SIZE

        for i in range(Config.GRID_LEVELS):
            step = Config.FIB_STEP_BASE * fib[i]
            cum += step
            p = base * (1.0 - cum) if direction == "LONG" else base * (1.0 + cum)

            # Новый объем = Старый * Коэффициент
            current_usd = current_usd * Config.VOL_COEFF

            qty = current_usd / p
            lvls.append(GridLevel(i + 1, p, qty, current_usd, cum))
        return lvls

    def _recon(self, avg, qty, direction, symbol):
        state = self.states[symbol]
        if qty == 0: return state.last_price

        temp = self._calc_grid(avg, direction)
        filled = [];
        acc = 0.0;
        tgt = qty * avg

        for l in temp:
            filled.append(l);
            acc += l.vol_usd
            if acc >= tgt * 0.9: break

        if not filled: return avg
        num = sum(l.vol_usd * l.dist_pct for l in filled)
        den = sum(l.vol_usd for l in filled)
        d = num / den if den > 0 else 0
        return avg / (1.0 - d) if direction == "LONG" else avg / (1.0 + d)

    def _rp(self, p, info: SymbolPrecision):
        return float(
            (Decimal(str(p)) / Decimal(str(info.tick_size))).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * Decimal(
                str(info.tick_size)))

    def _rq(self, q, info: SymbolPrecision):
        return float(
            (Decimal(str(q)) / Decimal(str(info.step_size))).quantize(Decimal('1'), rounding=ROUND_FLOOR) * Decimal(
                str(info.step_size)))

    # --- API ---
    @retry_request(max_retries=3)
    def _cancel_orders_for_side(self, symbol, pos_side):
        orders = self.client.get_orders(symbol=symbol)
        to_cancel = [o['orderId'] for o in orders if o['positionSide'] == pos_side]
        for oid in to_cancel:
            try:
                self.client.cancel_order(symbol=symbol, orderId=oid)
            except:
                pass

    @retry_request(max_retries=3)
    def _place_limit_order(self, symbol, side, pos_side, qty, price, is_maker=True, reduce_only=False):
        state = self.states[symbol]
        info = state.info
        fp = self._rp(price, info)
        fq = self._rq(qty, info)

        if fq < info.min_qty or (fp * fq) < info.min_notional:
            return False, 0

        params = {
            "symbol": symbol, "side": side, "positionSide": pos_side,
            "type": "LIMIT", "quantity": fq, "price": fp,
            "timeInForce": "GTX" if is_maker else "GTC"
        }
        try:
            self.client.new_order(**params)
            return True, 0
        except ClientError as e:
            return False, e.error_code

    @retry_request(max_retries=3)
    def _emergency_close(self, symbol, pos_side, qty):
        """Мгновенное закрытие по рынку (STOP LOSS)"""
        # Здесь нельзя использовать лимитки, т.к. при обвале рынка лимитка может не сработать.
        # Безопасность депозита важнее комиссии Taker.
        side = "SELL" if pos_side == "LONG" else "BUY"
        try:
            self.client.new_order(
                symbol=symbol, side=side, positionSide=pos_side,
                type="MARKET", quantity=qty
            )
            log.warning(f"[{symbol}] 🚨 EMERGENCY STOP LOSS EXECUTED ({pos_side})")
        except Exception as e:
            log.error(f"[{symbol}] ❌ Failed to exec Stop Loss: {e}")

    @retry_request(max_retries=3)
    def _sync_all_positions(self):
        positions = self.client.get_position_risk()
        # Сброс локальных данных
        for state in self.states.values():
            state.long_amt = 0.0
            state.short_amt = 0.0

        for p in positions:
            sym = p['symbol']
            if sym in self.states:
                state = self.states[sym]
                amt = float(p['positionAmt'])
                entry = float(p['entryPrice'])
                if p['positionSide'] == "LONG":
                    state.long_amt = amt;
                    state.long_entry = entry
                elif p['positionSide'] == "SHORT":
                    state.short_amt = abs(amt);
                    state.short_entry = entry

    @retry_request(max_retries=3)
    def _keep_alive_listen_key(self):
        if self.listen_key: self.client.renew_listen_key(listenKey=self.listen_key)

    @retry_request(max_retries=3)
    def _ping_rest(self):
        self.client.time()

    # --- STOP LOSS LOGIC ---
    def check_stop_loss(self, symbol):
        """Проверка Стоп-Лосса на каждом тике"""
        state = self.states[symbol]
        lp = state.last_price
        sl_pct = Config.STOP_LOSS_PCT
        triggered = False

        # LONG
        if state.long_amt > 0:
            drop = (state.long_entry - lp) / state.long_entry
            if drop >= sl_pct:
                log.warning(f"[{symbol}] 🛑 LONG SL Triggered! Drop: {drop * 100:.2f}%")
                self._emergency_close(symbol, "LONG", state.long_amt)
                self._cancel_orders_for_side(symbol, "LONG")
                triggered = True

        # SHORT
        if state.short_amt > 0:
            pump = (lp - state.short_entry) / state.short_entry
            if pump >= sl_pct:
                log.warning(f"[{symbol}] 🛑 SHORT SL Triggered! Pump: {pump * 100:.2f}%")
                self._emergency_close(symbol, "SHORT", state.short_amt)
                self._cancel_orders_for_side(symbol, "SHORT")
                triggered = True

        return triggered

    # --- STRATEGY ---
    def update_strategy_for_side(self, symbol, pos_side):
        if symbol not in self.states: return
        state = self.states[symbol]

        try:
            with self.lock:
                self._cancel_orders_for_side(symbol, pos_side)
                self._sync_all_positions()

                is_l = (pos_side == "LONG")
                amt = state.long_amt if is_l else state.short_amt
                entry = state.long_entry if is_l else state.short_entry
                force = False

                if amt > state.info.min_qty:
                    # Позиция есть -> Тейк + Сетка усреднения
                    tp_pct = Config.TAKE_PROFIT_PCT
                    tp_p = entry * (1.0 + tp_pct) if is_l else entry * (1.0 - tp_pct)
                    tp_s = "SELL" if is_l else "BUY"

                    ok, err = self._place_limit_order(symbol, tp_s, pos_side, amt, tp_p, is_maker=False,
                                                      reduce_only=True)

                    if err == -2022:
                        force = True  # Позиция закрыта на бирже
                    elif ok:
                        # Сетка усреднения (только лимитки)
                        base = self._recon(entry, amt, pos_side, symbol)
                        grid = self._calc_grid(base, pos_side)
                        for l in grid:
                            pl = (is_l and l.price < state.last_price * 0.9995) or \
                                 (not is_l and l.price > state.last_price * 1.0005)
                            if pl:
                                s_s = "BUY" if is_l else "SELL"
                                self._place_limit_order(symbol, s_s, pos_side, l.qty, l.price, is_maker=True)

                if amt <= state.info.min_qty or force:
                    # Позиции нет -> Стартовая сетка
                    if force:
                        if is_l:
                            state.long_amt = 0
                        else:
                            state.short_amt = 0

                    center = state.last_price
                    if is_l:
                        state.long_grid_center = center
                    else:
                        state.short_grid_center = center

                    grid = self._calc_grid(center, pos_side)
                    for l in grid:
                        valid = (is_l and l.price < center) or (not is_l and l.price > center)
                        if valid:
                            s_s = "BUY" if is_l else "SELL"
                            self._place_limit_order(symbol, s_s, pos_side, l.qty, l.price, is_maker=True)

        except Exception as e:
            log.error(f"[{symbol}] ❌ Logic Error in {pos_side}: {e}")

    def on_execution_event(self, d):
        sym = d['s']
        if sym not in self.states: return
        ps = d['ps']
        log.info(f"[{sym}] ⚡ EXEC {ps} {d['S']} {d['l']} @ {d['L']}")
        time.sleep(1.0)
        self.update_strategy_for_side(sym, ps)

        opp = "SHORT" if ps == "LONG" else "LONG"
        with self.lock:
            state = self.states[sym]
            oa = state.long_amt if opp == "LONG" else state.short_amt
            if oa == 0: self.update_strategy_for_side(sym, opp)

    def check_pagen_trailing(self, symbol):
        state = self.states[symbol]
        th = state.trailing_threshold_pct

        if state.long_amt == 0 and state.long_grid_center > 0:
            if (state.last_price - state.long_grid_center) / state.long_grid_center > th:
                log.info(f"[{symbol}] 🏃 LONG TRAIL")
                self.update_strategy_for_side(symbol, "LONG")

        if state.short_amt == 0 and state.short_grid_center > 0:
            if (state.short_grid_center - state.last_price) / state.short_grid_center > th:
                log.info(f"[{symbol}] 🏃 SHORT TRAIL")
                self.update_strategy_for_side(symbol, "SHORT")

    def on_ws_msg(self, _, m):
        try:
            msg = json.loads(m) if isinstance(m, str) else m
            if 'e' not in msg: return
            self.last_ws_update = time.time()

            if msg['e'] == 'aggTrade':
                sym = msg['s']
                if sym in self.states:
                    p = float(msg['p'])
                    with self.lock:
                        self.states[sym].last_price = p
                        # --- 1. Проверяем Стоп-Лосс ---
                        sl_triggered = self.check_stop_loss(sym)
                        # --- 2. Если живы, проверяем трейлинг ---
                        if not sl_triggered:
                            self.check_pagen_trailing(sym)

            elif msg['e'] == 'ORDER_TRADE_UPDATE':
                sym = msg['o']['s']
                if sym in self.states and msg['o']['X'] == 'FILLED':
                    threading.Thread(target=self.on_execution_event, args=(msg['o'],)).start()
        except:
            pass

    def run_maintenance(self):
        """Фоновый поток: пинг, аудит позиций, обновление ключей"""
        last_listen_key_renew = time.time()
        while self.running:
            if time.time() - last_listen_key_renew > 1800:
                try:
                    self._keep_alive_listen_key(); last_listen_key_renew = time.time()
                except:
                    pass

            # AUDIT: Если мы закрыли руками на бирже, бот должен узнать об этом
            try:
                old_states = {s: (st.long_amt, st.short_amt) for s, st in self.states.items()}
                self._sync_all_positions()
                for s, state in self.states.items():
                    old_l, old_s = old_states[s]
                    # Если позиция исчезла (закрыли руками)
                    if old_l > 0 and state.long_amt == 0:
                        log.info(f"[{s}] ♻️ MANUAL CLOSE LONG DETECTED")
                        self.update_strategy_for_side(s, "LONG")
                    if old_s > 0 and state.short_amt == 0:
                        log.info(f"[{s}] ♻️ MANUAL CLOSE SHORT DETECTED")
                        self.update_strategy_for_side(s, "SHORT")
            except Exception as e:
                log.error(f"Audit Error: {e}")

            try:
                self._ping_rest()
            except:
                pass

            for _ in range(45):
                time.sleep(1)
                if not self.running: return

    def run(self):
        log.info(f"🤖 BOT STARTED (SAFE MODE). Symbols: {Config.SYMBOLS}")
        self.initialize()

        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_ws_msg)
        lk = self.client.new_listen_key()['listenKey']
        self.listen_key = lk
        self.ws_client.user_data(listen_key=lk, id=1)

        for i, sym in enumerate(Config.SYMBOLS):
            self.ws_client.agg_trade(symbol=sym.lower(), id=i + 2)

        threading.Thread(target=self.run_maintenance, daemon=True).start()

        log.info("⏳ Waiting for prices...")
        while any(s.last_price == 0 for s in self.states.values()): time.sleep(1)

        for sym in Config.SYMBOLS:
            self.update_strategy_for_side(sym, "LONG")
            self.update_strategy_for_side(sym, "SHORT")

        threading.Thread(target=lambda: [time.sleep(10) or (os.kill(os.getpid(),
                                                                    signal.SIGINT) if time.time() - self.last_ws_update > Config.WATCHDOG_TIMEOUT else None)
                                         for _ in iter(int, 1)], daemon=True).start()

        try:
            while self.running: time.sleep(1)
        except KeyboardInterrupt:
            self.running = False;
            self.ws_client.stop();
            sys.exit(0)


if __name__ == "__main__":
    HedgeBot().run()
