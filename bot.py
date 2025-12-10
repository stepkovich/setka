import os
import time
import json
import logging
import threading
import sys
import uuid
from decimal import Decimal, getcontext
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError
from dotenv import load_dotenv

# ==========================
# 0. SETUP & GLOBALS
# ==========================
load_dotenv()
getcontext().prec = 28

# ==========================
# 1. CONFIGURATION
# ==========================
CONFIG = {
    "mode": "real",  # 'emulator' или 'real'
    "symbol": "DOGEUSDC",  # Торгуемая пара
    "api_key": os.getenv("BINANCE_API_KEY", ""),
    "api_secret": os.getenv("BINANCE_API_SECRET", ""),

    # Риск-менеджмент (ДИНАМИЧЕСКИЙ)
    "leverage": 16,
    "max_margin_usage_pct": 0.95,  # Использовать не более 95% от доступной мощности

    # Сетка
    "base_order_size": 10,  # Базовый лот ($)
    "peper_balance": 100,  # Баланс для эмулятора
    "grid_levels": 16,
    "fib_step_base": 0.00015,  # 0.015%
    "vol_coeff": 50.0,  # Множитель объема (Агрессивный)
    "pagen": 3,  # Подтягивание (шагов)
    "active_orders_window": 5,  # Сколько ордеров держать на бирже одновременно

    # Выход
    "take_profit_pct": 0.001,  # 0.1%
    "stop_loss_pct": 0.15,  # 15% (Аварийный)

    "audit_interval": 60,  # Полная сверка раз в минуту (редко, т.к. есть сокет)
    "log_level": "INFO"
}

# ==========================
# 2. LOGGING
# ==========================
logging.basicConfig(level=getattr(logging, CONFIG["log_level"]), format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("BOT_v10")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


# ==========================
# 3. DATA STRUCTURES
# ==========================
@dataclass
class PositionData:
    amount: float = 0.0
    entry_price: float = 0.0
    unrealized_pnl: float = 0.0

    @property
    def usd_value(self):
        return abs(self.amount * self.entry_price)


@dataclass
class Instrument:
    tick_size: float = 0.00001
    qty_step: float = 1.0
    min_qty: float = 1.0
    price_precision: int = 5
    qty_precision: int = 0
    quote_asset: str = "USDT"


class BotState:
    def __init__(self):
        self.lock = threading.RLock()
        self.instrument = Instrument()
        self.last_price = 0.0
        self.wallet_balance = 0.0

        # Timestamp последнего обновления через сокет (для защиты от конфликтов)
        self.last_event_ts = 0.0

        self.long = PositionData()
        self.short = PositionData()

        self.long_tp_oid: Optional[str] = None
        self.short_tp_oid: Optional[str] = None

        self.grid_orders_long: Dict[str, float] = {}  # {oid: price}
        self.grid_orders_short: Dict[str, float] = {}  # {oid: price}

        self.emu_balance = CONFIG["peper_balance"]
        self.emu_orders: Dict[str, dict] = {}


STATE = BotState()


# ==========================
# 4. EMULATOR ENGINE
# ==========================
class Emulator:
    def __init__(self):
        pass

    def get_position_risk(self, symbol):
        res = []
        res.append({"positionSide": "LONG", "positionAmt": STATE.long.amount, "entryPrice": STATE.long.entry_price,
                    "unRealizedProfit": STATE.long.unrealized_pnl})
        res.append({"positionSide": "SHORT", "positionAmt": -STATE.short.amount, "entryPrice": STATE.short.entry_price,
                    "unRealizedProfit": STATE.short.unrealized_pnl})
        return res

    def get_orders(self, symbol):
        return list(STATE.emu_orders.values())

    def account(self):
        return {"assets": [{"asset": STATE.instrument.quote_asset, "walletBalance": STATE.emu_balance}]}

    def new_order(self, **kwargs):
        with STATE.lock:
            oid = str(uuid.uuid4())[:8]
            order = kwargs.copy();
            order['orderId'] = oid;
            order['status'] = 'NEW'
            STATE.emu_orders[oid] = order
            return {'orderId': oid}

    def cancel_order(self, symbol, orderId):
        with STATE.lock:
            if orderId in STATE.emu_orders: del STATE.emu_orders[orderId]
            return {'status': 'CANCELED'}

    def process_price_update(self, price: float):
        with STATE.lock:
            to_remove = []
            executed = []
            for oid, o in STATE.emu_orders.items():
                side = o['side'];
                pos_side = o['positionSide']
                limit = float(o['price']);
                qty = float(o['quantity'])
                filled = (side == "BUY" and price <= limit) or (side == "SELL" and price >= limit)
                if filled:
                    self._execute_trade(side, pos_side, qty, limit)
                    to_remove.append(oid)
                    executed.append((oid, side, pos_side, limit, qty))
            for oid in to_remove: del STATE.emu_orders[oid]
            return executed  # Возвращаем исполненные ордера для симуляции сокета

    def _execute_trade(self, side, pos_side, qty, price):
        pos = STATE.long if pos_side == "LONG" else STATE.short
        is_open = (pos_side == "LONG" and side == "BUY") or (pos_side == "SHORT" and side == "SELL")
        if is_open:
            cost = pos.amount * pos.entry_price + qty * price
            pos.amount += qty
            pos.entry_price = cost / pos.amount if pos.amount > 0 else 0
        else:
            pnl = (price - pos.entry_price) if pos_side == "LONG" else (pos.entry_price - price)
            STATE.emu_balance += pnl * qty
            pos.amount -= qty
            if pos.amount < STATE.instrument.min_qty: pos.amount = 0; pos.entry_price = 0


EMULATOR = Emulator()
REAL_CLIENT = None
if CONFIG["api_key"]: REAL_CLIENT = UMFutures(key=CONFIG["api_key"], secret=CONFIG["api_secret"])


# ==========================
# 5. API ABSTRACTION
# ==========================
def api_call(method_name, **kwargs):
    try:
        if CONFIG["mode"] == "real":
            if not REAL_CLIENT: return None
            return getattr(REAL_CLIENT, method_name)(**kwargs)
        else:
            if hasattr(EMULATOR, method_name): return getattr(EMULATOR, method_name)(**kwargs)
            return None
    except ClientError as e:
        log.error(f"API Error [{method_name}]: {e.error_message}")
        return None
    except Exception as e:
        log.error(f"Sys Error [{method_name}]: {e}")
        return None


# ==========================
# 6. MATH & HELPERS
# ==========================
def get_tp_price(entry, side):
    pct = CONFIG["take_profit_pct"]
    if side == "LONG":
        return entry * (1 + pct)
    else:
        return entry * (1 - pct)


def get_grid_structure():
    levels = CONFIG["grid_levels"]
    base_step = CONFIG["fib_step_base"]
    structure = []
    fib = [1, 1]
    for _ in range(levels): fib.append(fib[-1] + fib[-2])
    total_step_pct = 0.0
    for i in range(levels):
        step = base_step * fib[i]
        total_step_pct += step
        mult = 1.0 + (step * CONFIG["vol_coeff"])
        qty_usd = CONFIG["base_order_size"] * mult
        structure.append({"step_pct": step, "qty_usd": qty_usd})
    return structure


def calculate_smart_recovery_orders(center_price, side, current_pos_usd):
    structure = get_grid_structure()
    accumulated_usd = 0.0
    current_level_idx = -1
    for i, level_data in enumerate(structure):
        accumulated_usd += level_data["qty_usd"]
        if current_pos_usd >= (accumulated_usd * 0.9):
            current_level_idx = i
        else:
            break

    next_level_idx = current_level_idx + 1
    orders = []
    for i in range(next_level_idx, len(structure)):
        lvl = structure[i]
        local_dist = 0.0
        for k in range(next_level_idx, i + 1): local_dist += structure[k]["step_pct"]
        if side == "LONG":
            p = center_price * (1 - local_dist)
        else:
            p = center_price * (1 + local_dist)
        orders.append((p, lvl["qty_usd"]))
    return orders


# ==========================
# 7. LOGIC CONTROLLER
# ==========================
def fetch_full_state_audit():
    """Редкий полный аудит для синхронизации"""
    # Защита от конфликта: Если сокет был активен недавно (5 сек), пропускаем аудит
    if time.time() - STATE.last_event_ts < 5:
        return

    acct = api_call("account")
    if acct:
        with STATE.lock:
            q = STATE.instrument.quote_asset
            for a in acct['assets']:
                if a['asset'] == q: STATE.wallet_balance = float(a['walletBalance']); break

    pos_res = api_call("get_position_risk", symbol=CONFIG["symbol"])
    if pos_res:
        with STATE.lock:
            # Двойная проверка времени внутри лока
            if time.time() - STATE.last_event_ts < 5: return

            for p in pos_res:
                amt = float(p['positionAmt']);
                entry = float(p['entryPrice'])
                if p['positionSide'] == "LONG":
                    STATE.long.amount = amt; STATE.long.entry_price = entry
                elif p['positionSide'] == "SHORT":
                    STATE.short.amount = abs(amt); STATE.short.entry_price = entry

    orders_res = api_call("get_orders", symbol=CONFIG["symbol"])
    if orders_res is not None:
        with STATE.lock:
            if time.time() - STATE.last_event_ts < 5: return

            STATE.long_tp_oid = None;
            STATE.short_tp_oid = None
            STATE.grid_orders_long.clear();
            STATE.grid_orders_short.clear()
            for o in orders_res:
                oid = str(o['orderId']);
                side = o['side'];
                pos_side = o['positionSide'];
                reduce = o.get('reduceOnly', False)
                is_tp = (pos_side == "LONG" and side == "SELL" and reduce) or (
                            pos_side == "SHORT" and side == "BUY" and reduce)
                is_grid = (o['type'] == 'LIMIT' and not reduce)
                if is_tp:
                    if pos_side == "LONG":
                        STATE.long_tp_oid = oid
                    else:
                        STATE.short_tp_oid = oid
                elif is_grid:
                    price = float(o['price'])
                    if pos_side == "LONG":
                        STATE.grid_orders_long[oid] = price
                    else:
                        STATE.grid_orders_short[oid] = price


def reconcile_side(side):
    """Основной мозг: проверяет, нужно ли ставить/отменять ордера"""
    with STATE.lock:
        pos_data = STATE.long if side == "LONG" else STATE.short
        tp_oid = STATE.long_tp_oid if side == "LONG" else STATE.short_tp_oid
        grid_map = STATE.grid_orders_long if side == "LONG" else STATE.grid_orders_short
        current_price = STATE.last_price
        has_pos = pos_data.amount > STATE.instrument.min_qty

    if has_pos:
        # A. Проверяем Тейк
        if not tp_oid:
            target = get_tp_price(pos_data.entry_price, side)
            o_side = "SELL" if side == "LONG" else "BUY"
            res = api_call("new_order", symbol=CONFIG["symbol"], side=o_side, positionSide=side,
                           type="LIMIT", quantity=pos_data.amount, price=round(target, 5),
                           timeInForce="GTC", reduceOnly="true")
            if res:
                with STATE.lock:
                    if side == "LONG":
                        STATE.long_tp_oid = str(res['orderId'])
                    else:
                        STATE.short_tp_oid = str(res['orderId'])

        # B. Проверяем Сетку (Усреднение)
        # Если ордеров меньше чем окно, достраиваем
        if len(grid_map) < CONFIG["active_orders_window"]:
            place_grid(side, current_price, is_averaging=True)

    else:
        # C. Чистим Тейк
        if tp_oid: api_call("cancel_order", symbol=CONFIG["symbol"], orderId=tp_oid)

        # D. Trailing Grid (Подтягивание)
        if not grid_map:
            place_grid(side, current_price, is_averaging=False)
        else:
            # Логика подтягивания
            prices = list(grid_map.values())
            if not prices: return
            should_realign = False
            threshold = CONFIG["fib_step_base"] * CONFIG["pagen"]

            if side == "LONG":
                top = max(prices)
                if current_price > top * (1 + threshold): should_realign = True
            else:
                bot = min(prices)
                if current_price < bot * (1 - threshold): should_realign = True

            if should_realign:
                log.info(f"🏃 Price moved ({current_price}). Realigning {side}...")
                for oid in list(grid_map.keys()): api_call("cancel_order", symbol=CONFIG["symbol"], orderId=oid)
                # На следующем такте поставится новая сетка


def place_grid(side, center_price, is_averaging):
    wallet_cap = STATE.wallet_balance * CONFIG["leverage"] * CONFIG["max_margin_usage_pct"]
    if wallet_cap == 0 and CONFIG["mode"] == "real": return

    orders = []
    if is_averaging:
        pos_usd = 0.0
        with STATE.lock:
            if side == "LONG":
                pos_usd = STATE.long.usd_value
            else:
                pos_usd = STATE.short.usd_value
        if pos_usd >= wallet_cap:
            log.warning(f"🛑 {side} Max Margin. Stop.")
            return
        orders = calculate_smart_recovery_orders(center_price, side, pos_usd)
    else:
        orders = calculate_smart_recovery_orders(center_price, side, 0.0)

    # Ставим только нужное количество (Window)
    limit_cnt = CONFIG["active_orders_window"]
    o_side = "BUY" if side == "LONG" else "SELL"

    with STATE.lock:
        existing_prices = set(STATE.grid_orders_long.values()) if side == "LONG" else set(
            STATE.grid_orders_short.values())
        current_count = len(existing_prices)

    placed_in_this_run = 0
    for p, qty_usd in orders:
        if current_count + placed_in_this_run >= limit_cnt: break

        # Проверка на дубликаты (огрубленно)
        is_duplicate = False
        for ep in existing_prices:
            if abs(ep - p) / p < 0.0005: is_duplicate = True; break  # Если цена отличается < 0.05%
        if is_duplicate: continue

        # Проверка лимита маржи
        with STATE.lock:
            curr_val = STATE.long.usd_value if side == "LONG" else STATE.short.usd_value
        if (curr_val + qty_usd) > wallet_cap: continue

        # Фильтр направления
        if is_averaging:
            if side == "LONG" and p >= center_price: continue
            if side == "SHORT" and p <= center_price: continue

        qty = round(qty_usd / p, 0)
        if qty < STATE.instrument.min_qty: continue

        res = api_call("new_order", symbol=CONFIG["symbol"], side=o_side, positionSide=side,
                       type="LIMIT", quantity=qty, price=round(p, 5), timeInForce="GTX")
        if res:
            placed_in_this_run += 1
            # Сразу добавляем в стейт, не дожидаясь сокета (оптимистично)
            with STATE.lock:
                oid = str(res['orderId'])
                if side == "LONG":
                    STATE.grid_orders_long[oid] = p
                else:
                    STATE.grid_orders_short[oid] = p


def handle_execution(oid, side, pos_side, price, qty):
    """Обработчик события исполнения ордера (из сокета)"""
    with STATE.lock:
        STATE.last_event_ts = time.time()  # Обновляем метку времени

        # 1. Удаляем из сетки (если это грид)
        if pos_side == "LONG":
            if oid in STATE.grid_orders_long: del STATE.grid_orders_long[oid]
        else:
            if oid in STATE.grid_orders_short: del STATE.grid_orders_short[oid]

        # 2. Обновляем позицию (локально)
        pos = STATE.long if pos_side == "LONG" else STATE.short
        is_open = (pos_side == "LONG" and side == "BUY") or (pos_side == "SHORT" and side == "SELL")

        old_amt = pos.amount
        if is_open:
            cost = pos.amount * pos.entry_price + qty * price
            pos.amount += qty
            pos.entry_price = cost / pos.amount if pos.amount > 0 else 0
        else:
            pos.amount -= qty
            if pos.amount < STATE.instrument.min_qty: pos.amount = 0; pos.entry_price = 0

        log.info(f"⚡ EXECUTION {pos_side}: {old_amt} -> {pos.amount} @ {price}")

    # 3. Мгновенная реакция (вне лока)
    # Если изменилась поза -> надо пересчитать TP и добавить ордера в сетку
    reconcile_side(pos_side)


def check_stop_loss(price):
    if STATE.long.amount > 0:
        sl = STATE.long.entry_price * (1 - CONFIG["stop_loss_pct"])
        if price < sl:
            log.critical("🛑 LONG SL HIT!")
            api_call("new_order", symbol=CONFIG["symbol"], side="SELL", positionSide="LONG", type="MARKET",
                     quantity=STATE.long.amount)
    if STATE.short.amount > 0:
        sl = STATE.short.entry_price * (1 + CONFIG["stop_loss_pct"])
        if price > sl:
            log.critical("🛑 SHORT SL HIT!")
            api_call("new_order", symbol=CONFIG["symbol"], side="BUY", positionSide="SHORT", type="MARKET",
                     quantity=STATE.short.amount)


# ==========================
# 8. MAIN ENTRY
# ==========================
def ws_worker():
    def on_m(_, m):
        try:
            d = json.loads(m)
            e_type = d.get('e')

            # A. Обновление цены
            if e_type == 'aggTrade':
                p = float(d['p'])
                with STATE.lock:
                    STATE.last_price = p
                if CONFIG["mode"] == "emulator":
                    # В эмуляторе сводим ордера тут же
                    executed = EMULATOR.process_price_update(p)
                    for exc in executed:
                        handle_execution(exc[0], exc[1], exc[2], exc[3], exc[4])

            # B. Исполнение ордера (ORDER_TRADE_UPDATE)
            elif e_type == 'ORDER_TRADE_UPDATE':
                o = d['o']
                if o['X'] == 'FILLED' and o['s'] == CONFIG["symbol"]:
                    oid = str(o['i']);
                    side = o['S'];
                    pos_side = o['ps']
                    price = float(o['L']);
                    qty = float(o['l'])
                    handle_execution(oid, side, pos_side, price, qty)
        except Exception as e:
            pass  # log.error(f"WS Error: {e}")

    ws = UMFuturesWebsocketClient(on_message=on_m)
    if CONFIG["mode"] == "real":
        # Получаем ListenKey для приватных событий (ордера)
        try:
            res = REAL_CLIENT.new_listen_key()
            key = res['listenKey']
            ws.user_data(listen_key=key, id=1)

            # Запускаем поток продления ключа
            def keep_alive():
                while True: time.sleep(1800); REAL_CLIENT.renew_listen_key(listenKey=key)

            threading.Thread(target=keep_alive, daemon=True).start()
        except:
            log.error("ListenKey Error"); sys.exit(1)

    ws.agg_trade(symbol=CONFIG["symbol"].lower(), id=2)
    while True: time.sleep(10)


def main():
    log.info(f"🤖 BOT v10.0 [HYBRID ULTIMATE]. Mode: {CONFIG['mode']}")
    if CONFIG["mode"] == "real":
        try:
            info = REAL_CLIENT.exchange_info()
            s = next(s for s in info['symbols'] if s['symbol'] == CONFIG["symbol"])
            STATE.instrument.min_qty = float(next(f for f in s['filters'] if f['filterType'] == 'LOT_SIZE')['minQty'])
            STATE.instrument.price_precision = s['pricePrecision']
            STATE.instrument.quote_asset = s['quoteAsset']
        except Exception as e:
            log.error(f"Init Error: {e}"); return

    t = threading.Thread(target=ws_worker, daemon=True);
    t.start()
    log.info("⏳ Waiting for price stream...")
    while STATE.last_price == 0: time.sleep(1)

    # Первичная синхронизация
    fetch_full_state_audit()

    last_audit = time.time()
    while True:
        try:
            # Регулярный цикл
            check_stop_loss(STATE.last_price)

            # Логика (если не было событий сокета, она отработает "вхолостую" или подтянет сетку)
            reconcile_side("LONG")
            reconcile_side("SHORT")

            # Редкий Аудит
            if time.time() - last_audit > CONFIG["audit_interval"]:
                fetch_full_state_audit()
                last_audit = time.time()

            time.sleep(1)
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            log.error(f"Main Loop Error: {e}"); time.sleep(5)


if __name__ == "__main__":
    main()
