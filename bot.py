import os
import time
import json
import logging
import threading
import random
import signal
import sys
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from decimal import Decimal, getcontext

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError
from dotenv import load_dotenv

load_dotenv()
getcontext().prec = 28

# ==========================
# CONFIGURATION
# ==========================
CONFIG: Dict[str, Any] = {
    "mode": "real",  # 'emulator' или 'real'
    "symbol": "DOGEUSDC",
    "api_key": os.getenv("BINANCE_API_KEY", ""),
    "api_secret": os.getenv("BINANCE_API_SECRET", ""),

    "peper balance": 20,
    "leverage": 16,
    "base_order_size": 10,  # Базовый лот ($)

    # --- Grid Settings ---
    "grid_levels": 16,
    "fib_step_base": 0.00015,  # 0.015% - база
    "pagen": 3,  # Подтягивание

    # --- Smart Martingale ---
    "vol_coeff": 60.0,  # Коэффициент множителя объема (1 + step% * 100)

    # --- TP / SL ---
    "take_profit_pct": 0.0003,  # 0.03%
    "stop_loss_pct": 1,  # 1%

    "log_level": "INFO",
}

# ==========================
# LOGGING
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s][%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("BINANCE_BOT")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.INFO)


# ==========================
# STATE
# ==========================
@dataclass
class Instrument:
    symbol: str
    tick_size: float
    qty_step: float
    min_qty: float
    price_precision: int
    qty_precision: int


@dataclass
class VirtualPosition:
    long_amt: float = 0.0
    long_cost: float = 0.0
    short_amt: float = 0.0
    short_cost: float = 0.0


class State:
    def __init__(self):
        self.symbol = CONFIG["symbol"]
        self.instrument: Optional[Instrument] = None
        self.client: Optional[UMFutures] = None
        self.ws_client: Optional[UMFuturesWebsocketClient] = None
        self.listen_key: Optional[str] = None

        self.long_grid_orders: Dict[str, Any] = {}
        self.short_grid_orders: Dict[str, Any] = {}
        self.long_tp_id: Optional[str] = None
        self.short_tp_id: Optional[str] = None

        self.emu_balance = CONFIG["peper balance"]
        self.emu_pos = VirtualPosition()
        self.last_price = 0.0
        self.ws_connected = False
        self.last_ws_update = time.time()  # For Watchdog
        self.is_shutting_down = False

        self.long_bottom_price = 0.0
        self.short_top_price = 0.0


STATE = State()


# ==========================
# WATCHDOG (DEAD MAN SWITCH)
# ==========================
class Watchdog(threading.Thread):
    def __init__(self, timeout=120):
        super().__init__(daemon=True)
        self.timeout = timeout
        self.running = True

    def run(self):
        log.info("🐕 Watchdog started")
        while self.running:
            time.sleep(10)
            if STATE.is_shutting_down: break

            # Если цена не обновлялась > timeout -> Убиваем процесс
            silence = time.time() - STATE.last_ws_update
            if silence > self.timeout:
                log.critical(f"💀 WATCHDOG: No data for {silence:.0f}s! Killing process for restart...")
                os._exit(1)  # Hard crash to trigger Docker restart


# ==========================
# UTILS
# ==========================
def safe_call(func, *args, max_retries=3, backoff=0.5, **kwargs):
    for i in range(1, max_retries + 1):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.error_code in (-1003, -2015, -2011):
                time.sleep(backoff * i);
                continue
            return None
        except Exception as e:
            log.error(f"Exception: {e}");
            time.sleep(backoff * i)
    return None


def round_step(value: float, step: float) -> float:
    if step <= 0: return value
    v = Decimal(str(value))
    s = Decimal(str(step))
    return float((v // s) * s)


def get_fib_levels(center_price: float, levels: int, base_step: float, direction: str):
    prices = []
    steps_pct = []
    fib = [1, 1]
    for i in range(2, levels): fib.append(fib[i - 1] + fib[i - 2])

    total_pct = 0.0
    for i in range(levels):
        step = base_step * fib[i]
        total_pct += step
        price = center_price * (1 - total_pct) if direction == "LONG" else center_price * (1 + total_pct)
        prices.append(price)
        steps_pct.append(step)
    return prices, steps_pct


# ==========================
# API
# ==========================
def init_api():
    if not CONFIG["api_key"]: raise ValueError("No API Key")
    STATE.client = UMFutures(key=CONFIG["api_key"], secret=CONFIG["api_secret"])
    log.info("REST Client OK")


def fetch_instrument():
    info = safe_call(STATE.client.exchange_info)
    target = next((s for s in info['symbols'] if s['symbol'] == STATE.symbol), None)
    p = next(f for f in target['filters'] if f['filterType'] == 'PRICE_FILTER')
    l = next(f for f in target['filters'] if f['filterType'] == 'LOT_SIZE')
    STATE.instrument = Instrument(
        symbol=STATE.symbol, tick_size=float(p['tickSize']), qty_step=float(l['stepSize']),
        min_qty=float(l['minQty']), price_precision=target['pricePrecision'], qty_precision=target['quantityPrecision']
    )
    log.info(f"Instrument: {STATE.instrument}")


def get_price():
    r = safe_call(STATE.client.ticker_price, symbol=STATE.symbol)
    return float(r['price']) if r else 0.0


def get_real_positions():
    pos = safe_call(STATE.client.get_position_risk, symbol=STATE.symbol)
    l_amt, l_ent, s_amt, s_ent = 0.0, 0.0, 0.0, 0.0
    if pos:
        for p in pos:
            if p['symbol'] == STATE.symbol:
                amt, prc = float(p['positionAmt']), float(p['entryPrice'])
                if p['positionSide'] == "LONG":
                    l_amt, l_ent = amt, prc
                elif p['positionSide'] == "SHORT":
                    s_amt, s_ent = amt, prc
    return l_amt, l_ent, s_amt, s_ent


def setup_account():
    try:
        res = safe_call(STATE.client.get_position_mode)
        if res and not res['dualSidePosition']:
            safe_call(STATE.client.change_position_mode, dualSidePosition="true")
        safe_call(STATE.client.change_leverage, symbol=STATE.symbol, leverage=CONFIG["leverage"])
    except Exception:
        pass


def cancel_grid_only(side: str):
    if CONFIG["mode"] == "emulator":
        if side == "LONG":
            STATE.long_grid_orders.clear()
        else:
            STATE.short_grid_orders.clear()
        return

    to_cancel = []
    if side == "LONG":
        to_cancel = list(STATE.long_grid_orders.keys())
        STATE.long_grid_orders.clear()
    else:
        to_cancel = list(STATE.short_grid_orders.keys())
        STATE.short_grid_orders.clear()

    if to_cancel:
        for oid in to_cancel: safe_call(STATE.client.cancel_order, symbol=STATE.symbol, orderId=oid)


def place_order(side: str, pos_side: str, price: float, qty: float, is_maker=True, reduce_only=False):
    i = STATE.instrument
    price = round(round_step(price, i.tick_size), i.price_precision)
    qty = round(round_step(qty, i.qty_step), i.qty_precision)
    if qty < i.min_qty: return None

    notional = price * qty
    if notional < 5.1:
        qty = round(round_step(5.5 / price, i.qty_step), i.qty_precision)
        if qty * price < 5.0: return None

    if CONFIG["mode"] == "emulator":
        oid = f"emu_{side}_{int(time.time() * 10000)}_{random.randint(10, 99)}"
        log.info(f"[EMU] {side} ({pos_side}) {qty} @ {price}")
        return oid

    params = {
        "symbol": STATE.symbol, "side": side, "positionSide": pos_side,
        "type": "LIMIT", "quantity": qty, "price": price,
        "timeInForce": "GTX" if is_maker else "GTC"
    }
    if reduce_only:
        params["timeInForce"] = "GTC"
        params["reduceOnly"] = "true"

    res = safe_call(STATE.client.new_order, **params)
    if not res and is_maker:
        params["timeInForce"] = "GTC"
        res = safe_call(STATE.client.new_order, **params)

    if res: return str(res['orderId'])
    return None


# ==========================
# LOGIC CORE
# ==========================
def start_cycle(pos_side: str):
    if STATE.is_shutting_down: return
    cancel_grid_only(pos_side)
    center = STATE.last_price
    if center == 0: return

    # Определяем, есть ли уже позиция, чтобы понять, как строить сетку
    has_pos = False
    pos_entry = 0.0

    if CONFIG["mode"] == "real":
        l, le, s, se = get_real_positions()
        if pos_side == "LONG" and l > 0: has_pos = True; pos_entry = le
        if pos_side == "SHORT" and abs(s) > 0: has_pos = True; pos_entry = se
    else:
        if pos_side == "LONG" and STATE.emu_pos.long_amt > 0:
            has_pos = True;
            pos_entry = STATE.emu_pos.long_cost / STATE.emu_pos.long_amt
        if pos_side == "SHORT" and STATE.emu_pos.short_amt > 0:
            has_pos = True;
            pos_entry = STATE.emu_pos.short_cost / STATE.emu_pos.short_amt

    log.info(f"♻️ STARTING {pos_side} CYCLE @ {center} (Has Pos: {has_pos}, Entry: {pos_entry})")

    # Получаем уровни цен И размеры шагов (в %)
    prices, steps_pct = get_fib_levels(center, CONFIG["grid_levels"], CONFIG["fib_step_base"], pos_side)

    # Save bounds for Stop Loss
    if pos_side == "LONG":
        STATE.long_bottom_price = prices[-1]
    else:
        STATE.short_top_price = prices[-1]

    count = 0
    for i, price in enumerate(prices):
        order_side = "BUY" if pos_side == "LONG" else "SELL"

        # УМНЫЙ ФИЛЬТР:
        # 1. Если позиции НЕТ -> Фильтруем "плохие" входы (слишком близко к центру против тренда).
        # 2. Если позиция ЕСТЬ -> Нам все равно на тренд, нам нужно УСРЕДНЯТЬ.
        #    Мы ставим ордера только те, которые "хуже" текущей цены (ниже для лонга, выше для шорта).

        should_place = True

        if not has_pos:
            # Классический старт: пропускаем первые уровни, если они "вдогонку" ушедшей цене
            if pos_side == "LONG" and price > center * 1.0005: should_place = False
            if pos_side == "SHORT" and price < center * 0.9995: should_place = False
        else:
            # Режим достройки/усреднения:
            # Для LONG: ставим только если цена ордера НИЖЕ текущей (чтобы усреднить)
            # Для SHORT: ставим только если цена ордера ВЫШЕ текущей
            if pos_side == "LONG" and price >= center: should_place = False
            if pos_side == "SHORT" and price <= center: should_place = False

            # Дополнительная защита: не ставить ордера СЛИШКОМ близко (меньше шага цены)
            if abs(price - center) / center < 0.0005: should_place = False

        if not should_place: continue

        # SMART MARTINGALE LOGIC
        step_p = steps_pct[i]
        multiplier = 1.0 + (step_p * CONFIG["vol_coeff"])
        multiplier = max(1.0, multiplier)

        order_qty_usd = CONFIG["base_order_size"] * multiplier
        qty = order_qty_usd / price

        oid = place_order(order_side, pos_side, price, qty, is_maker=True)
        if oid:
            count += 1
            grid_info = {"price": price, "qty": qty, "lvl": i + 1}
            if pos_side == "LONG":
                STATE.long_grid_orders[oid] = grid_info
            else:
                STATE.short_grid_orders[oid] = grid_info

    log.info(f"✅ Placed {count} orders for {pos_side}")


def update_tp(pos_side: str, amt: float, entry: float):
    if STATE.is_shutting_down: return
    if abs(amt) < STATE.instrument.min_qty: return

    profit_pct = CONFIG["take_profit_pct"]
    tp_price = entry * (1 + profit_pct) if pos_side == "LONG" else entry * (1 - profit_pct)
    order_side = "SELL" if pos_side == "LONG" else "BUY"
    old_tp_id = STATE.long_tp_id if pos_side == "LONG" else STATE.short_tp_id

    if old_tp_id and CONFIG["mode"] == "real":
        safe_call(STATE.client.cancel_order, symbol=STATE.symbol, orderId=old_tp_id)

    log.info(f"🔄 TP {pos_side}: Pos={amt} Entry={entry:.5f} -> TP={tp_price:.5f}")
    oid = place_order(order_side, pos_side, tp_price, abs(amt), is_maker=False, reduce_only=True)

    if pos_side == "LONG":
        STATE.long_tp_id = oid
    else:
        STATE.short_tp_id = oid


def on_execution(oid: str, side: str, pos_side: str, price: float, qty: float):
    is_long_grid = oid in STATE.long_grid_orders
    is_short_grid = oid in STATE.short_grid_orders

    if is_long_grid or is_short_grid:
        log.info(f"📉 GRID HIT ({pos_side}) @ {price}")
        if is_long_grid: del STATE.long_grid_orders[oid]
        if is_short_grid: del STATE.short_grid_orders[oid]

        if CONFIG["mode"] == "emulator":
            if pos_side == "LONG":
                val = STATE.emu_pos.long_cost + (price * qty)
                amt = STATE.emu_pos.long_amt + qty
                STATE.emu_pos.long_amt = amt;
                STATE.emu_pos.long_cost = val
                if amt > 0: update_tp("LONG", amt, val / amt)
            else:
                val = STATE.emu_pos.short_cost + (price * qty)
                amt = STATE.emu_pos.short_amt + qty
                STATE.emu_pos.short_amt = amt;
                STATE.emu_pos.short_cost = val
                if amt > 0: update_tp("SHORT", amt, val / amt)
        else:
            time.sleep(1)
            l, le, s, se = get_real_positions()
            if pos_side == "LONG":
                update_tp("LONG", l, le)
            else:
                update_tp("SHORT", abs(s), se)

    is_long_tp = (oid == STATE.long_tp_id)
    is_short_tp = (oid == STATE.short_tp_id)

    if is_long_tp or is_short_tp:
        log.info(f"🎉 PROFIT ({pos_side}) @ {price}! Restarting cycle...")
        if CONFIG["mode"] == "emulator":
            if pos_side == "LONG":
                pnl = (price * qty) - STATE.emu_pos.long_cost
                STATE.emu_pos.long_amt = 0;
                STATE.emu_pos.long_cost = 0;
                STATE.long_tp_id = None
            else:
                pnl = STATE.emu_pos.short_cost - (price * qty)
                STATE.emu_pos.short_amt = 0;
                STATE.emu_pos.short_cost = 0;
                STATE.short_tp_id = None
            STATE.emu_balance += pnl
            log.info(f"💰 EMU PnL: {pnl:.4f} Bal: {STATE.emu_balance:.2f}")

        time.sleep(2)
        start_cycle(pos_side)


def check_grid_realignment(pos_side: str, center_price: float):
    if STATE.is_shutting_down: return

    has_pos = False
    if CONFIG["mode"] == "emulator":
        if pos_side == "LONG":
            has_pos = STATE.emu_pos.long_amt > 0
        else:
            has_pos = STATE.emu_pos.short_amt > 0
    else:
        l, _, s, _ = get_real_positions()
        if pos_side == "LONG":
            has_pos = l > 0
        else:
            has_pos = abs(s) > 0

    # Если есть позиция, Realign не нужен (мы уже в рынке),
    # НО нам может понадобиться достройка сетки, если она пустая.
    # Это теперь обрабатывается внутри start_cycle() при рестарте.
    # Здесь просто следим, чтобы не запускать дубликаты.
    if has_pos:
        # Можно добавить проверку "а есть ли ордера усреднения?", но это сложнее.
        # Пока оставим простую логику: если есть поза - не двигаем сетку.
        return

    if pos_side == "LONG":
        if not STATE.long_grid_orders: start_cycle("LONG"); return
        top_order_price = max([o['price'] for o in STATE.long_grid_orders.values()])
        threshold = CONFIG["fib_step_base"] * int(CONFIG["pagen"])
        if center_price > top_order_price * (1 + threshold):
            log.info(f"🏃 Price moved UP ({center_price}). Realigning LONG Grid...")
            start_cycle("LONG")
    else:
        if not STATE.short_grid_orders: start_cycle("SHORT"); return
        bottom_order_price = min([o['price'] for o in STATE.short_grid_orders.values()])
        threshold = CONFIG["fib_step_base"] * int(CONFIG["pagen"])
        if center_price < bottom_order_price * (1 - threshold):
            log.info(f"🏃 Price moved DOWN ({center_price}). Realigning SHORT Grid...")
            start_cycle("SHORT")

def ensure_tp():
    if CONFIG["mode"] != "real":
        return
    l, le, s, se = get_real_positions()

    # LONG
    if l > 0:
        if not STATE.long_tp_id:
            log.warning("⚠️ LONG position without TP, recreating...")
            update_tp("LONG", l, le)

    # SHORT
    if abs(s) > 0:
        if not STATE.short_tp_id:
            log.warning("⚠️ SHORT position without TP, recreating...")
            update_tp("SHORT", abs(s), se)


def check_stop_loss(price: float):
    if STATE.is_shutting_down: return
    if CONFIG["mode"] != "real": return

    l, _, s, _ = get_real_positions()
    if l > 0 and STATE.long_bottom_price > 0:
        sl_price = STATE.long_bottom_price * (1 - CONFIG["stop_loss_pct"])
        if price < sl_price:
            log.critical(f"🚨 LONG STOP LOSS HIT! Price {price} < {sl_price}. CLOSING ALL!")
            safe_call(STATE.client.new_order, symbol=STATE.symbol, side="SELL", positionSide="LONG", type="MARKET",
                      quantity=l)
            cancel_grid_only("LONG");
            STATE.long_bottom_price = 0

    if abs(s) > 0 and STATE.short_top_price > 0:
        sl_price = STATE.short_top_price * (1 + CONFIG["stop_loss_pct"])
        if price > sl_price:
            log.critical(f"🚨 SHORT STOP LOSS HIT! Price {price} > {sl_price}. CLOSING ALL!")
            safe_call(STATE.client.new_order, symbol=STATE.symbol, side="BUY", positionSide="SHORT", type="MARKET",
                      quantity=abs(s))
            cancel_grid_only("SHORT");
            STATE.short_top_price = 0


def graceful_shutdown(signum, frame):
    log.warning("\n🛑 SHUTDOWN SIGNAL RECEIVED! Cleaning up...")
    STATE.is_shutting_down = True
    log.info("🧹 Cancelling Grid Orders...")
    cancel_grid_only("LONG");
    cancel_grid_only("SHORT")
    if STATE.ws_client: STATE.ws_client.stop()
    sys.exit(0)


# ==========================
# SYNC STATE
# ==========================
def sync_initial_state():
    log.info("🔄 Syncing state with exchange...")
    l, le, s, se = get_real_positions()
    orders = safe_call(STATE.client.get_open_orders, symbol=STATE.symbol) or []

    long_tp_found = False
    short_tp_found = False

    for o in orders:
        oid = str(o['orderId'])
        if o['positionSide'] == "LONG" and o['side'] == "SELL" and o['reduceOnly']:
            if l > 0:
                STATE.long_tp_id = oid;
                long_tp_found = True;
                log.info(f"✅ Found active LONG TP: {oid}")
            else:
                safe_call(STATE.client.cancel_order, symbol=STATE.symbol, orderId=oid)
        elif o['positionSide'] == "SHORT" and o['side'] == "BUY" and o['reduceOnly']:
            if abs(s) > 0:
                STATE.short_tp_id = oid;
                short_tp_found = True;
                log.info(f"✅ Found active SHORT TP: {oid}")
            else:
                safe_call(STATE.client.cancel_order, symbol=STATE.symbol, orderId=oid)
        else:
            safe_call(STATE.client.cancel_order, symbol=STATE.symbol, orderId=oid)

    if l > 0:
        log.info(f"Resuming LONG: {l} @ {le}")
        if not long_tp_found: update_tp("LONG", l, le)
        start_cycle("LONG")
    else:
        start_cycle("LONG")

    if abs(s) > 0:
        log.info(f"Resuming SHORT: {abs(s)} @ {se}")
        if not short_tp_found: update_tp("SHORT", abs(s), se)
        start_cycle("SHORT")
    else:
        start_cycle("SHORT")


# ==========================
# MAIN new
# ==========================
def on_message(_, msg):
    try:
        e = json.loads(msg) if isinstance(msg, str) else msg
        if 'e' not in e: return
        if not STATE.ws_connected: STATE.ws_connected = True; log.info("WS Connected")

        STATE.last_ws_update = time.time()  # <--- Update Heartbeat

        if e['e'] == 'aggTrade':
            price = float(e['p'])
            STATE.last_price = price
            if CONFIG["mode"] == "real": check_stop_loss(price)
            if CONFIG["mode"] == "emulator": check_emu_exec(price)

        elif e['e'] == 'ORDER_TRADE_UPDATE':
            o = e['o']
            if o['X'] == 'FILLED' and o['s'] == STATE.symbol:
                oid, side, pos_side = str(o['i']), o['S'], o['ps']
                price, qty = float(o['L']), float(o['l'])
                on_execution(oid, side, pos_side, price, qty)
    except Exception:
        pass


def check_emu_exec(price: float):
    for oid in list(STATE.long_grid_orders.keys()):
        o = STATE.long_grid_orders[oid]
        if price <= o['price']: on_execution(oid, "BUY", "LONG", o['price'], o['qty'])
    for oid in list(STATE.short_grid_orders.keys()):
        o = STATE.short_grid_orders[oid]
        if price >= o['price']: on_execution(oid, "SELL", "SHORT", o['price'], o['qty'])
    if STATE.long_tp_id and STATE.emu_pos.long_amt > 0:
        tp = STATE.emu_pos.long_cost / STATE.emu_pos.long_amt * (1 + CONFIG["take_profit_pct"])
        if price >= tp: on_execution(STATE.long_tp_id, "SELL", "LONG", tp, STATE.emu_pos.long_amt)
    if STATE.short_tp_id and STATE.emu_pos.short_amt > 0:
        tp = STATE.emu_pos.short_cost / STATE.emu_pos.short_amt * (1 - CONFIG["take_profit_pct"])
        if price <= tp: on_execution(STATE.short_tp_id, "BUY", "SHORT", tp, STATE.emu_pos.short_amt)


def main():
    print("--- DUAL-SIDE HEDGE BOT v6.1 [SMART REBUILD + WATCHDOG] ---")
    signal.signal(signal.SIGINT, graceful_shutdown)

    init_api();
    fetch_instrument()
    STATE.ws_client = UMFuturesWebsocketClient(on_message=on_message)

    if CONFIG["mode"] == "real":
        log.warning("⚠️ REAL MODE")
        setup_account()
        STATE.last_price = get_price()
        sync_initial_state()

        res = safe_call(STATE.client.new_listen_key)
        STATE.listen_key = res['listenKey']
        STATE.ws_client.user_data(listen_key=STATE.listen_key, id=1)
        STATE.ws_client.agg_trade(symbol=STATE.symbol.lower(), id=2)
        threading.Thread(target=keep_alive, daemon=True).start()
    else:
        log.info("🎮 EMULATOR MODE")
        STATE.last_price = get_price()
        start_cycle("LONG");
        start_cycle("SHORT")
        STATE.ws_client.agg_trade(symbol=STATE.symbol.lower(), id=1)

    # Запуск Сторожевого Пса
    wd = Watchdog(timeout=120)
    wd.start()

    try:
        while True:
            time.sleep(10)
            if STATE.last_price > 0:
                check_grid_realignment("LONG", STATE.last_price)
                check_grid_realignment("SHORT", STATE.last_price)
                ensure_tp()
    except Exception as e:
        log.error(f"Loop Error: {e}")


def keep_alive():
    while True: time.sleep(1800); safe_call(STATE.client.renew_listen_key, listenKey=STATE.listen_key)


if __name__ == "__main__":
    main()