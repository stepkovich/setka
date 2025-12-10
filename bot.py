import os
import time
import json
import logging
import threading
import sys
import uuid
import random
from decimal import Decimal, getcontext, ROUND_FLOOR
from dataclasses import dataclass
from typing import Dict, List, Optional

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError
from dotenv import load_dotenv

# ==========================
# 0. SETUP & GLOBALS
# ==========================
load_dotenv()
getcontext().prec = 28

CONFIG = {
    "mode": "real",  # 'emulator' или 'real'
    "symbol": "DOGEUSDC",
    "api_key": os.getenv("BINANCE_API_KEY", ""),
    "api_secret": os.getenv("BINANCE_API_SECRET", ""),

    # Risk Management
    "leverage": 16,
    "max_margin_usage_pct": 0.95,

    # Grid Settings
    "base_order_size": 8,
    "peper_balance": 100,  # Баланс эмулятора
    "grid_levels": 16,
    "fib_step_base": 0.00015,
    "vol_coeff": 60.0,
    "pagen": 3,

    # Exit Strategy
    "take_profit_pct": 0.0005,
    "stop_loss_pct": 0.15,

    "log_level": "INFO"
}

logging.basicConfig(level=getattr(logging, CONFIG["log_level"]), format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("BOT_v11.2")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


# ==========================
# 1. MATH HELPERS (PRECISION)
# ==========================
def round_step(value, step):
    """Округляет значение до ближайшего шага (tickSize или stepSize)"""
    if step == 0: return value
    # Используем Decimal для точного деления
    val_d = Decimal(str(value))
    step_d = Decimal(str(step))
    # Округляем вниз (FLOOR) для кол-ва, чтобы не превысить баланс, или математически
    # Для цены лучше обычное округление, для кол-ва - вниз.
    # Но для универсальности здесь используем квантование.
    return float((val_d // step_d) * step_d)


def round_qty(value, step):
    """Округление количества (всегда вниз или мат. округление)"""
    return round_step(value, step)


def round_price(value, step):
    """Округление цены"""
    # Для цены важно попасть в сетку. round_step подходит.
    # Иногда нужно форматировать через format() чтобы не было 1.200000001
    rp = round_step(value, step)
    # Удаляем артефакты float
    return float(Decimal(str(rp)).quantize(Decimal(str(step))))


# ==========================
# 2. STATE
# ==========================
@dataclass
class PositionData:
    amount: float = 0.0
    entry_price: float = 0.0


class BotState:
    def __init__(self):
        self.lock = threading.RLock()
        self.last_price = 0.0
        self.wallet_balance = 0.0

        self.long = PositionData()
        self.short = PositionData()

        # Grid Memory
        self.long_grid_ids = []
        self.short_grid_ids = []
        self.long_tp_id = None
        self.short_tp_id = None

        # Trailing Anchors
        self.long_top_price = 0.0
        self.short_bottom_price = 0.0

        # Instrument Precision (Default)
        self.tick_size = 0.00001
        self.step_size = 1.0
        self.min_qty = 1.0
        self.min_notional = 5.0  # Мин сумма ордера
        self.price_precision = 5
        self.qty_precision = 0
        self.quote_asset = "USDT"

        # Emulator
        self.emu_orders = {}
        self.emu_balance = CONFIG["peper_balance"]


STATE = BotState()
CLIENT = None
if CONFIG["api_key"]: CLIENT = UMFutures(key=CONFIG["api_key"], secret=CONFIG["api_secret"])


# ==========================
# 3. API
# ==========================
class Emulator:
    def __init__(self):
        pass

    def get_position_risk(self, s):
        return [{"positionSide": "LONG", "positionAmt": STATE.long.amount, "entryPrice": STATE.long.entry_price},
                {"positionSide": "SHORT", "positionAmt": -STATE.short.amount, "entryPrice": STATE.short.entry_price}]

    def get_orders(self, s):
        return list(STATE.emu_orders.values())

    def account(self):
        return {"assets": [{"asset": STATE.quote_asset, "walletBalance": STATE.emu_balance}]}

    def new_order(self, **k):
        with STATE.lock:
            oid = str(uuid.uuid4())[:8];
            k['orderId'] = oid;
            k['status'] = 'NEW';
            STATE.emu_orders[oid] = k
            return {'orderId': oid}

    def cancel_order(self, symbol, orderId):
        with STATE.lock:
            if orderId in STATE.emu_orders: del STATE.emu_orders[orderId]
            return {'status': 'CANCELED'}

    def process_price(self, p):
        with STATE.lock:
            executed = [];
            to_rem = []
            for oid, o in STATE.emu_orders.items():
                filled = (o['side'] == "BUY" and p <= float(o['price'])) or (
                            o['side'] == "SELL" and p >= float(o['price']))
                if filled: executed.append(
                    (o['positionSide'], float(o['price']), float(o['quantity']), o['side'])); to_rem.append(oid)
            for oid in to_rem: del STATE.emu_orders[oid]
            return executed

    def exec_logic(self, ps, p, q, s):
        pos = STATE.long if ps == "LONG" else STATE.short
        is_open = (ps == "LONG" and s == "BUY") or (ps == "SHORT" and s == "SELL")
        if is_open:
            c = pos.amount * pos.entry_price + q * p;
            pos.amount += q;
            pos.entry_price = c / pos.amount if pos.amount > 0 else 0
        else:
            pnl = (p - pos.entry_price) if ps == "LONG" else (pos.entry_price - p)
            STATE.emu_balance += pnl * q;
            pos.amount -= q
            if pos.amount < STATE.min_qty: pos.amount = 0; pos.entry_price = 0


EMULATOR = Emulator()


def api_call(method, **kwargs):
    try:
        if CONFIG["mode"] == "real":
            if not CLIENT: return None
            return getattr(CLIENT, method)(**kwargs)
        else:
            return getattr(EMULATOR, method)(**kwargs)
    except ClientError as e:
        if "Unknown order" in e.error_message: return None
        log.error(f"API {method}: {e.error_message}")
    except Exception as e:
        log.error(f"Sys {method}: {e}")
    return None


# ==========================
# 4. LOGIC
# ==========================
def fetch_state_sync():
    """Синхронизация баланса и поз"""
    acct = api_call("account")
    if acct:
        with STATE.lock:
            for a in acct['assets']:
                if a['asset'] == STATE.quote_asset: STATE.wallet_balance = float(a['walletBalance'])
    pos = api_call("get_position_risk", symbol=CONFIG["symbol"])
    if pos:
        with STATE.lock:
            for p in pos:
                amt = float(p['positionAmt']);
                ent = float(p['entryPrice'])
                if p['positionSide'] == "LONG":
                    STATE.long.amount = amt; STATE.long.entry_price = ent
                elif p['positionSide'] == "SHORT":
                    STATE.short.amount = abs(amt); STATE.short.entry_price = ent


def clean_start():
    log.info("🧹 Clean Start...")
    orders = api_call("get_orders", symbol=CONFIG["symbol"])
    if orders:
        for o in orders: api_call("cancel_order", symbol=CONFIG["symbol"], orderId=o['orderId'])
    with STATE.lock:
        STATE.long_grid_ids = [];
        STATE.short_grid_ids = []
        STATE.long_tp_id = None;
        STATE.short_tp_id = None


def get_tp_price(entry, side):
    pct = CONFIG["take_profit_pct"]
    return entry * (1 + pct) if side == "LONG" else entry * (1 - pct)


def calculate_grid(center, side, pos_usd):
    levels = CONFIG["grid_levels"];
    base = CONFIG["base_order_size"];
    step_b = CONFIG["fib_step_base"];
    coef = CONFIG["vol_coeff"]
    struct = [];
    fib = [1, 1]
    for _ in range(levels): fib.append(fib[-1] + fib[-2])

    acc = 0.0;
    start_idx = 0
    for i in range(levels):
        step = step_b * fib[i]
        usd = base * (1.0 + step * coef)
        acc += usd
        if pos_usd >= (acc * 0.9): start_idx = i + 1
        struct.append({"s": step, "u": usd})

    orders = [];
    dist = 0.0
    for k in range(start_idx): dist += struct[k]["s"]
    for i in range(start_idx, levels):
        dist += struct[i]["s"]
        p = center * (1 - dist) if side == "LONG" else center * (1 + dist)
        orders.append((p, struct[i]["u"]))
    return orders


def cancel_grid(side):
    with STATE.lock:
        ids = list(STATE.long_grid_ids) if side == "LONG" else list(STATE.short_grid_ids)
        if side == "LONG":
            STATE.long_grid_ids = []; STATE.long_top_price = 0
        else:
            STATE.short_grid_ids = []; STATE.short_bottom_price = 0
    for oid in ids: api_call("cancel_order", symbol=CONFIG["symbol"], orderId=oid)


def update_tp(side, amt, entry):
    old_tp = None
    with STATE.lock:
        old_tp = STATE.long_tp_id if side == "LONG" else STATE.short_tp_id
        if side == "LONG":
            STATE.long_tp_id = None
        else:
            STATE.short_tp_id = None
    if old_tp: api_call("cancel_order", symbol=CONFIG["symbol"], orderId=old_tp)

    target = get_tp_price(entry, side)

    # PRECISION FIX
    final_price = round_price(target, STATE.tick_size)

    o_side = "SELL" if side == "LONG" else "BUY"
    res = api_call("new_order", symbol=CONFIG["symbol"], side=o_side, positionSide=side,
                   type="LIMIT", quantity=amt, price=final_price, timeInForce="GTC")
    if res:
        with STATE.lock:
            if side == "LONG":
                STATE.long_tp_id = str(res['orderId'])
            else:
                STATE.short_tp_id = str(res['orderId'])


def start_cycle(side):
    cancel_grid(side)
    with STATE.lock:
        center = STATE.last_price
        if side == "LONG":
            amt = STATE.long.amount; ent = STATE.long.entry_price
        else:
            amt = STATE.short.amount; ent = STATE.short.entry_price
        cap = STATE.wallet_balance * CONFIG["leverage"] * CONFIG["max_margin_usage_pct"]
        pos_usd = amt * ent

    if center == 0: return
    if amt > 0: update_tp(side, amt, ent)
    if pos_usd >= cap: return

    orders = calculate_grid(center, side, pos_usd)
    new_ids = [];
    prices = []
    o_side = "BUY" if side == "LONG" else "SELL"

    for p, usd in orders:
        if (pos_usd + usd) > cap: break

        # PRECISION FIX
        final_price = round_price(p, STATE.tick_size)
        qty_raw = usd / final_price
        final_qty = round_qty(qty_raw, STATE.step_size)

        # Min Filters
        if final_qty < STATE.min_qty: continue
        if (final_qty * final_price) < STATE.min_notional: continue

        if amt > 0:
            if side == "LONG" and final_price >= center: continue
            if side == "SHORT" and final_price <= center: continue

        res = api_call("new_order", symbol=CONFIG["symbol"], side=o_side, positionSide=side,
                       type="LIMIT", quantity=final_qty, price=final_price, timeInForce="GTC")
        if res:
            new_ids.append(str(res['orderId']))
            prices.append(final_price)

    with STATE.lock:
        if side == "LONG":
            STATE.long_grid_ids = new_ids
            STATE.long_top_price = max(prices) if prices else 0
        else:
            STATE.short_grid_ids = new_ids
            STATE.short_bottom_price = min(prices) if prices else 0
    log.info(f"✅ {side} Grid: {len(new_ids)} orders")


def on_execution_event(pos_side):
    log.info(f"⚡ Exec {pos_side}. Sync...")
    fetch_state_sync()
    start_cycle(pos_side)


def check_stop_loss(p):
    with STATE.lock:
        l = STATE.long; s = STATE.short
    if l.amount > 0 and p < l.entry_price * (1 - CONFIG["stop_loss_pct"]):
        log.critical("🛑 LONG SL")
        api_call("new_order", symbol=CONFIG["symbol"], side="SELL", positionSide="LONG", type="MARKET",
                 quantity=l.amount)
        time.sleep(1);
        clean_start()
    if s.amount > 0 and p > s.entry_price * (1 + CONFIG["stop_loss_pct"]):
        log.critical("🛑 SHORT SL")
        api_call("new_order", symbol=CONFIG["symbol"], side="BUY", positionSide="SHORT", type="MARKET",
                 quantity=s.amount)
        time.sleep(1);
        clean_start()


# ==========================
# 5. WS WORKER
# ==========================
def ws_worker():
    last_realign_ts = 0.0

    def on_m(_, m):
        nonlocal last_realign_ts
        try:
            d = json.loads(m)
            e = d.get('e')
            if e == 'aggTrade':
                p = float(d['p'])
                with STATE.lock:
                    STATE.last_price = p

                if CONFIG["mode"] == "emulator":
                    ex = EMULATOR.process_price(p)
                    for x in ex:
                        EMULATOR.exec_logic(x[0], p, x[2], x[3])
                        on_execution_event(x[0])

                now = time.time()
                if now - last_realign_ts > 1.0:
                    do_long = False;
                    do_short = False
                    with STATE.lock:
                        if STATE.long.amount == 0:
                            threshold = CONFIG["fib_step_base"] * CONFIG["pagen"] * p
                            if not STATE.long_grid_ids or (
                                    STATE.long_top_price > 0 and p > STATE.long_top_price + threshold):
                                do_long = True
                        if STATE.short.amount == 0:
                            threshold = CONFIG["fib_step_base"] * CONFIG["pagen"] * p
                            if not STATE.short_grid_ids or (
                                    STATE.short_bottom_price > 0 and p < STATE.short_bottom_price - threshold):
                                do_short = True
                    if do_long: start_cycle("LONG"); last_realign_ts = now
                    if do_short: start_cycle("SHORT"); last_realign_ts = now

            elif e == 'ORDER_TRADE_UPDATE':
                o = d['o']
                if o['X'] == 'FILLED' and o['s'] == CONFIG["symbol"]:
                    on_execution_event(o['ps'])
        except:
            pass

    ws = UMFuturesWebsocketClient(on_message=on_m)
    if CONFIG["mode"] == "real":
        k = CLIENT.new_listen_key()['listenKey']
        ws.user_data(listen_key=k, id=1)
        threading.Thread(target=lambda: (time.sleep(1800) or CLIENT.renew_listen_key(listenKey=k)) and None,
                         daemon=True).start()
    ws.agg_trade(symbol=CONFIG["symbol"].lower(), id=2)
    while True: time.sleep(10)


def main():
    log.info(f"🚀 BOT v11.2 [PRECISION FIX]. Mode: {CONFIG['mode']}")

    if CONFIG["mode"] == "real":
        try:
            info = CLIENT.exchange_info()
            s = next(s for s in info['symbols'] if s['symbol'] == CONFIG["symbol"])

            # FILTERS LOAD
            p_filter = next(f for f in s['filters'] if f['filterType'] == 'PRICE_FILTER')
            l_filter = next(f for f in s['filters'] if f['filterType'] == 'LOT_SIZE')
            notional = next((f for f in s['filters'] if f['filterType'] == 'MIN_NOTIONAL'), None)

            STATE.tick_size = float(p_filter['tickSize'])
            STATE.step_size = float(l_filter['stepSize'])
            STATE.min_qty = float(l_filter['minQty'])
            STATE.price_precision = s['pricePrecision']
            STATE.qty_precision = s['quantityPrecision']
            STATE.quote_asset = s['quoteAsset']

            if notional: STATE.min_notional = float(notional.get('minNotional', 5.0))

            log.info(f"Tick: {STATE.tick_size}, Step: {STATE.step_size}, Min$: {STATE.min_notional}")

        except Exception as e:
            log.error(f"Init Error: {e}");
            return

    threading.Thread(target=ws_worker, daemon=True).start()
    log.info("⏳ Waiting for price...")
    while STATE.last_price == 0: time.sleep(1)

    fetch_state_sync()
    if CONFIG["mode"] == "real": clean_start()

    start_cycle("LONG")
    start_cycle("SHORT")

    last_audit = time.time()
    while True:
        try:
            time.sleep(1)
            check_stop_loss(STATE.last_price)
            if time.time() - last_audit > 60:
                with STATE.lock:
                    old_l = STATE.long.amount; old_s = STATE.short.amount
                fetch_state_sync()
                with STATE.lock:
                    new_l = STATE.long.amount; new_s = STATE.short.amount
                if abs(new_l - old_l) > STATE.min_qty or (new_l > 0 and not STATE.long_tp_id): start_cycle("LONG")
                if abs(new_s - old_s) > STATE.min_qty or (new_s > 0 and not STATE.short_tp_id): start_cycle("SHORT")
                last_audit = time.time()
        except KeyboardInterrupt:
            sys.exit(0)
        except:
            time.sleep(5)


if __name__ == "__main__":
    main()
