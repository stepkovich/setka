import os
import time
import sys
import logging
import threading
import signal
import json
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
# 0. CONFIG
# ==========================================
load_dotenv()
getcontext().prec = 28

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("HEDGE_BOT")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")
    SYMBOL = "DOGEUSDC"
    LEVERAGE = 16
    BASE_ORDER_SIZE = 8.0
    GRID_LEVELS = 16
    FIB_STEP_BASE = 0.00015
    VOL_COEFF = 80.0
    TAKE_PROFIT_PCT = 0.0005
    PAGEN = 3
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
                    # Логируем только как Info, чтобы не пугать, если это первый сбой
                    if i == 0:
                        log.info(f"⚠️ Network glitch in {func.__name__}, retrying...")
                    else:
                        log.warning(f"⚠️ Retry {i + 1}/{max_retries} for {func.__name__}: {e}")
                    time.sleep(delay)
                    last_exception = e
                except ClientError as e:
                    raise e
                except Exception as e:
                    log.error(f"❌ Unknown Error in {func.__name__}: {e}")
                    raise e
            if last_exception:
                log.error(f"❌ Connection Failed: {last_exception}")
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
        self.symbol_info: Optional[SymbolPrecision] = None
        self.last_price = 0.0
        self.last_ws_update = time.time()

        self.long_amt = 0.0
        self.long_entry = 0.0
        self.short_amt = 0.0
        self.short_entry = 0.0

        self.long_grid_center = 0.0
        self.short_grid_center = 0.0
        self.trailing_threshold_pct = 0.0
        self.listen_key = None

    def initialize(self):
        log.info("🔹 Init...")
        if not Config.API_KEY: log.critical("❌ No API Keys"); sys.exit(1)
        try:
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)
            self._load_symbol_info()
            self._setup_account()

            # PAGEN Threshold
            dist = sum(Config.FIB_STEP_BASE * f for f in self._fib(Config.PAGEN))
            self.trailing_threshold_pct = dist
            log.info(f"📏 Trailing Threshold = {self.trailing_threshold_pct * 100:.4f}%")

            ticker = self.client.ticker_price(symbol=Config.SYMBOL)
            self.last_price = float(ticker['price'])
            self._sync_positions()
        except Exception as e:
            log.critical(f"Init Fail: {e}");
            sys.exit(1)

    @retry_request(max_retries=3)
    def _load_symbol_info(self):
        info = self.client.exchange_info()
        s_info = next(s for s in info['symbols'] if s['symbol'] == Config.SYMBOL)
        p_f = next(f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER')
        l_f = next(f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE')
        n_f = next((f for f in s_info['filters'] if f['filterType'] in ['MIN_NOTIONAL', 'NOTIONAL']), None)
        mn = 5.0
        if n_f:
            if 'notional' in n_f:
                mn = float(n_f['notional'])
            elif 'minNotional' in n_f:
                mn = float(n_f['minNotional'])

        self.symbol_info = SymbolPrecision(
            tick_size=float(p_f['tickSize']), step_size=float(l_f['stepSize']),
            min_qty=float(l_f['minQty']), min_notional=mn,
            price_precision=s_info['pricePrecision'], qty_precision=s_info['quantityPrecision']
        )

    def _setup_account(self):
        try:
            m = self.client.get_position_mode()
            if not m['dualSidePosition']: self.client.change_position_mode(dualSidePosition="true")
            self.client.change_leverage(symbol=Config.SYMBOL, leverage=Config.LEVERAGE)
        except:
            pass

    # --- MATH ---
    def _fib(self, n):
        seq = [1, 1]
        for i in range(2, n): seq.append(seq[-1] + seq[-2])
        return seq[:n]

    def _calc_grid(self, base, direction):
        lvls = []
        fib = self._fib(Config.GRID_LEVELS)
        cum = 0.0
        for i in range(Config.GRID_LEVELS):
            step = Config.FIB_STEP_BASE * fib[i]
            cum += step
            p = base * (1.0 - cum) if direction == "LONG" else base * (1.0 + cum)
            mult = max(1.0, 1.0 + (step * Config.VOL_COEFF))
            usd = Config.BASE_ORDER_SIZE * mult
            qty = usd / p
            lvls.append(GridLevel(i + 1, p, qty, usd, cum))
        return lvls

    def _recon(self, avg, qty, direction):
        if qty == 0: return self.last_price
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

    def _rp(self, p):
        return float((Decimal(str(p)) / Decimal(str(self.symbol_info.tick_size))).quantize(Decimal('1'),
                                                                                           rounding=ROUND_HALF_UP) * Decimal(
            str(self.symbol_info.tick_size)))

    def _rq(self, q):
        return float((Decimal(str(q)) / Decimal(str(self.symbol_info.step_size))).quantize(Decimal('1'),
                                                                                           rounding=ROUND_FLOOR) * Decimal(
            str(self.symbol_info.step_size)))

    # --- API ---
    @retry_request(max_retries=3)
    def _cancel_orders_for_side(self, pos_side):
        orders = self.client.get_orders(symbol=Config.SYMBOL)
        to_cancel = [o['orderId'] for o in orders if o['positionSide'] == pos_side]
        if to_cancel:
            for oid in to_cancel:
                try:
                    self.client.cancel_order(symbol=Config.SYMBOL, orderId=oid)
                except Exception:
                    pass
            log.info(f"🗑️ Cancelled {len(to_cancel)} for {pos_side}")

    @retry_request(max_retries=3)
    def _place_limit_order(self, side, pos_side, qty, price, is_maker=True, reduce_only=False):
        fp = self._rp(price);
        fq = self._rq(qty)
        if fq < self.symbol_info.min_qty or (fp * fq) < self.symbol_info.min_notional: return False, 0

        params = {
            "symbol": Config.SYMBOL, "side": side, "positionSide": pos_side,
            "type": "LIMIT", "quantity": fq, "price": fp,
            "timeInForce": "GTX" if is_maker else "GTC"
        }
        try:
            self.client.new_order(**params)
            return True, 0
        except ClientError as e:
            return False, e.error_code

    @retry_request(max_retries=3)
    def _sync_positions(self):
        pos = self.client.get_position_risk(symbol=Config.SYMBOL)
        for p in pos:
            amt = float(p['positionAmt']);
            entry = float(p['entryPrice'])
            if p['positionSide'] == "LONG":
                self.long_amt = amt; self.long_entry = entry
            elif p['positionSide'] == "SHORT":
                self.short_amt = abs(amt); self.short_entry = entry

    @retry_request(max_retries=3)
    def _keep_alive_listen_key(self):
        if self.listen_key:
            self.client.renew_listen_key(listenKey=self.listen_key)

    @retry_request(max_retries=3)
    def _ping_rest(self):
        """Легкий запрос для поддержания HTTP соединения"""
        self.client.time()

    # --- STRATEGY ---
    def update_strategy_for_side(self, pos_side):
        try:
            with self.lock:
                self._cancel_orders_for_side(pos_side)
                self._sync_positions()

                is_l = (pos_side == "LONG")
                amt = self.long_amt if is_l else self.short_amt
                entry = self.long_entry if is_l else self.short_entry
                force = False

                if amt > self.symbol_info.min_qty:
                    # Позиция есть (или мы так думаем). Пробуем поставить TP.
                    tp_pct = Config.TAKE_PROFIT_PCT
                    tp_p = entry * (1.0 + tp_pct) if is_l else entry * (1.0 - tp_pct)
                    tp_s = "SELL" if is_l else "BUY"

                    ok, err = self._place_limit_order(tp_s, pos_side, amt, tp_p, is_maker=False, reduce_only=True)

                    if err == -2022:
                        # Ошибка -2022 значит, что позиции на самом деле НЕТ.
                        # Это нормально при быстром закрытии сделки.
                        force = True
                    elif ok:
                        # Логируем, только если TP реально встал
                        log.info(f"⚙️ IN DEAL {pos_side}. Vol={amt}. TP @ {tp_p:.5f}")

                        base = self._recon(entry, amt, pos_side)
                        grid = self._calc_grid(base, pos_side)
                        c = 0
                        for l in grid:
                            pl = (is_l and l.price < self.last_price * 0.9995) or (
                                        not is_l and l.price > self.last_price * 1.0005)
                            if pl:
                                s_s = "BUY" if is_l else "SELL"
                                ok_o, _ = self._place_limit_order(s_s, pos_side, l.qty, l.price, is_maker=True)
                                if ok_o: c += 1
                        log.info(f"✅ Grid {pos_side}: {c} orders")

                if amt <= self.symbol_info.min_qty or force:
                    if force:
                        if is_l:
                            self.long_amt = 0
                        else:
                            self.short_amt = 0

                    center = self.last_price
                    if is_l:
                        self.long_grid_center = center
                    else:
                        self.short_grid_center = center

                    log.info(f"🆕 START {pos_side} @ {center}")
                    grid = self._calc_grid(center, pos_side)
                    c = 0
                    for l in grid:
                        valid = (is_l and l.price < center) or (not is_l and l.price > center)
                        if valid:
                            s_s = "BUY" if is_l else "SELL"
                            ok_o, _ = self._place_limit_order(s_s, pos_side, l.qty, l.price, is_maker=True)
                            if ok_o: c += 1
                    log.info(f"✅ New Grid {pos_side}: {c} orders")

        except Exception as e:
            log.error(f"❌ Critical Logic Error in {pos_side}: {e}")

    def on_execution_event(self, d):
        ps = d['ps']
        log.info(f"⚡ EXEC {ps} {d['S']} {d['l']} @ {d['L']}")
        time.sleep(1.0)
        self.update_strategy_for_side(ps)

        opp = "SHORT" if ps == "LONG" else "LONG"
        with self.lock:
            oa = self.long_amt if opp == "LONG" else self.short_amt
            if oa == 0: self.update_strategy_for_side(opp)

    def check_pagen_trailing(self):
        th = self.trailing_threshold_pct
        if self.long_amt == 0 and self.long_grid_center > 0:
            if (self.last_price - self.long_grid_center) / self.long_grid_center > th:
                log.info("🏃 LONG TRAIL")
                self.update_strategy_for_side("LONG")
        if self.short_amt == 0 and self.short_grid_center > 0:
            if (self.short_grid_center - self.last_price) / self.short_grid_center > th:
                log.info("🏃 SHORT TRAIL")
                self.update_strategy_for_side("SHORT")

    def on_ws_msg(self, _, m):
        try:
            msg = json.loads(m) if isinstance(m, str) else m
            if 'e' not in msg: return
            self.last_ws_update = time.time()
            if msg['e'] == 'aggTrade':
                with self.lock:
                    self.last_price = float(msg['p'])
                    self.check_pagen_trailing()
            elif msg['e'] == 'ORDER_TRADE_UPDATE':
                if msg['o']['X'] == 'FILLED' and msg['o']['s'] == Config.SYMBOL:
                    threading.Thread(target=self.on_execution_event, args=(msg['o'],)).start()
        except:
            pass

    def run_maintenance(self):
        """Фоновый поток для пинга и продления ключа"""
        while self.running:
            # Продление ListenKey раз в 30 мин
            try:
                self._keep_alive_listen_key()
            except:
                pass

            # Пинг REST API каждые 40 секунд (грелка для соединения)
            for _ in range(45):
                time.sleep(1)
                if not self.running: return

            try:
                self._ping_rest()
            except:
                pass

    def run(self):
        log.info(f"🤖 BOT START [{Config.SYMBOL}]")
        self.initialize()
        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_ws_msg)
        lk = self.client.new_listen_key()['listenKey']
        self.listen_key = lk
        self.ws_client.user_data(listen_key=lk, id=1)
        self.ws_client.agg_trade(symbol=Config.SYMBOL.lower(), id=2)

        threading.Thread(target=self.run_maintenance, daemon=True).start()

        while self.last_price == 0: time.sleep(1)
        self.update_strategy_for_side("LONG")
        self.update_strategy_for_side("SHORT")

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
