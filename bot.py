import os
import time
import sys
import logging
import threading
import signal
import json
import collections
from decimal import Decimal, getcontext, ROUND_HALF_UP, ROUND_FLOOR
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field
from functools import wraps

# Lib
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
log = logging.getLogger("HEDGE_BOT_v2.1")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    SYMBOLS = ["DOGEUSDC"]
    LEVERAGE = 75

    # --- РИСК-МЕНЕДЖМЕНТ ---
    # Расчет ордера от ДОСТУПНОГО БАЛАНСА
    BALANCE_PER_1_DOLLAR_ORDER = Decimal("1")

    # Максимальный множитель объема позиции (Защита Recon от перегруза)
    MAX_EXPOSURE_MULTIPLIER = Decimal("100.0")

    MIN_ORDER_SIZE = Decimal("5.2")
    MAX_ORDER_SIZE = Decimal("5.2")

    GRID_LEVELS = 9
    FIB_STEP_BASE = Decimal("0.00015")
    VOL_COEFF = Decimal("100.0")
    TAKE_PROFIT_PCT = Decimal("0.0007")
    PAGEN = 3

    STOP_LOSS_BEYOND_GRID_PCT = Decimal("0.0174")

    WATCHDOG_TIMEOUT = 60
    AUDIT_INTERVAL = 30
    STATE_FILE = "bot_state.json"

    # --- ДЕТЕКЦИЯ ТРЕНДА ---
    # Количество тиков в буфере для анализа (кольцевой буфер)
    TREND_BUFFER_SIZE = 500
    # Минимальное количество тиков перед началом анализа
    TREND_MIN_SAMPLES = 200
    # Порог тренда в % за весь буфер (0.15 = 0.15% за ~500 тиков)
    TREND_THRESHOLD_PCT = Decimal("0.15")
    # --- СОХРАНЕНИЕ СОСТОЯНИЯ ---
    # Минимальный интервал между сохранениями на диск (секунды)
    STATE_SAVE_INTERVAL = 5.0


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
    current_long_order_size: Decimal = Config.MIN_ORDER_SIZE
    current_short_order_size: Decimal = Config.MIN_ORDER_SIZE

    long_sl_price: Decimal = Decimal("0")
    short_sl_price: Decimal = Decimal("0")
    last_sl_attempt: float = 0  # Время последней попытки закрытия по стопу

    trailing_threshold_pct: Decimal = Decimal("0")

    # --- ДЕТЕКЦИЯ ТРЕНДА ---
    price_buffer: collections.deque = field(
        default_factory=lambda: collections.deque(maxlen=Config.TREND_BUFFER_SIZE)
    )
    trend_direction: str = "RANGE"   # "UPTREND", "DOWNTREND", "RANGE"
    trend_strength: float = 0.0      # -1.0 ... +1.0 (отрицательное = даунтренд)

    # --- ЗАКРЫТИЕ ПО ТРЕНДУ ---
    # Время последней попытки закрытия позиции по тренду (троттлинг)
    last_trend_close_attempt: float = 0


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
        self._last_state_save_time: float = 0.0  # Таймер троттлинга сохранений

    def initialize(self):
        log.info("🔹 Запуск бота v2.1 (SL + тренд-детекция + закрытие по тренду + восстановление)")
        if not Config.API_KEY: log.critical("❌ API ключи не найдены"); sys.exit(1)
        try:
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)
            ex_info = self.client.exchange_info()
            all_info = {s['symbol']: s for s in ex_info['symbols']}
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

                if sym in saved_data:
                    # --- Существующие поля (размеры ордеров и центры сеток) ---
                    st.current_long_order_size = Decimal(str(saved_data[sym].get('l_size', Config.MIN_ORDER_SIZE)))
                    st.current_short_order_size = Decimal(str(saved_data[sym].get('s_size', Config.MIN_ORDER_SIZE)))
                    st.long_grid_center = Decimal(str(saved_data[sym].get('l_center', "0")))
                    st.short_grid_center = Decimal(str(saved_data[sym].get('s_center', "0")))

                    # --- Восстановление позиций (из последнего сохранения) ---
                    # Эти данные будут перезаписаны _sync_all_positions_rest(),
                    # но до того момента бот уже знает о позициях для SL-мониторинга
                    st.long_amt = Decimal(str(saved_data[sym].get('l_amt', "0")))
                    st.long_entry = Decimal(str(saved_data[sym].get('l_entry', "0")))
                    st.short_amt = Decimal(str(saved_data[sym].get('s_amt', "0")))
                    st.short_entry = Decimal(str(saved_data[sym].get('s_entry', "0")))

                    # --- Восстановление стоп-лосс цен ---
                    st.long_sl_price = Decimal(str(saved_data[sym].get('l_sl', "0")))
                    st.short_sl_price = Decimal(str(saved_data[sym].get('s_sl', "0")))

                    # --- Восстановление последней цены ---
                    saved_last_price = saved_data[sym].get('last_price', "0")
                    if saved_last_price and Decimal(str(saved_last_price)) > Decimal("0"):
                        st.last_price = Decimal(str(saved_last_price))

                    # --- Восстановление тренда ---
                    st.trend_direction = saved_data[sym].get('trend_direction', "RANGE")
                    saved_strength = saved_data[sym].get('trend_strength', "0")
                    if saved_strength:
                        st.trend_strength = float(saved_strength)

                    # --- Восстановление буфера цен (для тренда) ---
                    saved_buffer = saved_data[sym].get('price_buffer', None)
                    if saved_buffer and isinstance(saved_buffer, list) and len(saved_buffer) > 0:
                        for p in saved_buffer[-Config.TREND_BUFFER_SIZE:]:
                            try:
                                st.price_buffer.append(float(p))
                            except (ValueError, TypeError):
                                pass

                    log.info(
                        f"[{sym}] 📂 Состояние восстановлено: "
                        f"LONG {st.long_amt}@{st.long_entry} SL={st.long_sl_price} | "
                        f"SHORT {st.short_amt}@{st.short_entry} SL={st.short_sl_price} | "
                        f"цена={st.last_price} тренд={st.trend_direction} "
                        f"буфер={len(st.price_buffer)}"
                    )

                self.states[sym] = st
                self._setup_account(sym)
                ticker = self.client.ticker_price(sym)
                self.states[sym].last_price = Decimal(str(ticker['price']))

            self._sync_all_positions_rest()

            # --- Принудительное сохранение после синхронизации ---
            # Теперь в файле актуальные данные из REST API
            self._force_save_state_to_disk()

            log.info("✅ Инициализация завершена")
        except Exception as e:
            log.critical(f"❌ Ошибка инициализации: {e}"); sys.exit(1)

    def _save_state_to_disk(self):
        """Сохраняет полное состояние бота на диск.

        Троттлируется: не чаще чем раз в STATE_SAVE_INTERVAL секунд,
        чтобы не спамить диск при активной торговле.
        """
        try:
            now = time.time()
            if now - self._last_state_save_time < Config.STATE_SAVE_INTERVAL:
                return
            self._last_state_save_time = now

            self._write_state_to_file()
        except Exception as e:
            log.error(f"Ошибка сохранения: {e}")

    def _force_save_state_to_disk(self):
        """Принудительное сохранение состояния на диск (без троттлинга).

        Используется в критических моментах: инициализация, смена SL, и т.д.
        """
        try:
            self._last_state_save_time = time.time()
            self._write_state_to_file()
        except Exception as e:
            log.error(f"Ошибка принудительного сохранения: {e}")

    def _write_state_to_file(self):
        """Непосредственно записывает состояние в файл.

        Выносится в отдельный метод, чтобы избежать дублирования кода
        между _save_state_to_disk и _force_save_state_to_disk.
        """
        data = {}
        with self.lock:
            for sym, st in self.states.items():
                data[sym] = {
                    # --- Существующие поля: размеры ордеров и центры сеток ---
                    "l_size": str(st.current_long_order_size),
                    "s_size": str(st.current_short_order_size),
                    "l_center": str(st.long_grid_center),
                    "s_center": str(st.short_grid_center),

                    # --- Позиции (для мгновенного SL-мониторинга после перезапуска) ---
                    "l_amt": str(st.long_amt),
                    "l_entry": str(st.long_entry),
                    "s_amt": str(st.short_amt),
                    "s_entry": str(st.short_entry),

                    # --- Стоп-лосс цены ---
                    "l_sl": str(st.long_sl_price),
                    "s_sl": str(st.short_sl_price),

                    # --- Последняя известная цена ---
                    "last_price": str(st.last_price),

                    # --- Состояние тренда ---
                    "trend_direction": st.trend_direction,
                    "trend_strength": str(st.trend_strength),

                    # --- Буфер цен (для восстановления тренда без "слепого" периода) ---
                    "price_buffer": list(st.price_buffer)
                }
        with open(Config.STATE_FILE, 'w') as f:
            json.dump(data, f)

    def _load_state_from_disk(self) -> dict:
        if os.path.exists(Config.STATE_FILE):
            try:
                with open(Config.STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def _get_dynamic_order_size(self):
        """Считает ордер от Available Balance для минимизации риска перегруза"""
        try:
            acc = self.client.account()
            available = Decimal(str(acc['availableBalance']))
            wallet = Decimal(str(acc['totalWalletBalance']))

            if available < (wallet * Decimal("0.01")):
                log.warning("🛑 Мало свободной маржи, новые сделки отменены")
                return Decimal("0")

            calc = available / (Decimal(str(len(Config.SYMBOLS))) * Config.BALANCE_PER_1_DOLLAR_ORDER)
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
            log.warning(f"⚠️ Настройка {symbol}: {e}")

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
            vol_usd = order_size * multiplier
            qty = vol_usd / price
            lvls.append((price, qty, vol_usd, cum_dist))
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
            if acc_vol >= target_vol * Decimal("1"): break
        if not filled: return avg_entry
        num = sum(v * d for v, d in filled);
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
                    amt, ent = Decimal(str(p['pa'] if 'pa' in p else p['positionAmt'])), Decimal(
                        str(p['ep'] if 'ep' in p else p['entryPrice']))
                    if p['positionSide'] == "LONG":
                        self.states[sym].long_amt, self.states[sym].long_entry = amt, ent
                    elif p['positionSide'] == "SHORT":
                        self.states[sym].short_amt, self.states[sym].short_entry = abs(amt), ent

    # ==========================================
    # 2.1 ДЕТЕКЦИЯ ТРЕНДА (линейная регрессия)
    # ==========================================
    def _detect_trend(self, symbol: str) -> str:
        """Определяет тренд линейной регрессией по кольцевому буферу цен.

        Возвращает: "UPTREND", "DOWNTREND" или "RANGE".
        Также обновляет поля trend_direction и trend_strength в SymbolState.
        """
        st = self.states[symbol]
        buf = st.price_buffer

        if len(buf) < Config.TREND_MIN_SAMPLES:
            st.trend_direction = "RANGE"
            st.trend_strength = 0.0
            return "RANGE"

        prices = list(buf)
        n = len(prices)

        # --- Линейная регрессия: y = slope * x + intercept ---
        # x — индекс тика (0, 1, 2, ..., n-1)
        # y — цена
        x_mean = (n - 1) / 2.0
        y_mean = sum(prices) / n

        num = 0.0
        den = 0.0
        for i in range(n):
            dx = i - x_mean
            dy = prices[i] - y_mean
            num += dx * dy
            den += dx * dx

        if den == 0:
            st.trend_direction = "RANGE"
            st.trend_strength = 0.0
            return "RANGE"

        slope = num / den  # изменение цены за 1 тик

        # Нормализуем к проценту за весь буфер
        slope_pct = (slope * n) / y_mean * 100.0  # % изменения за N тиков

        threshold = float(Config.TREND_THRESHOLD_PCT)

        if slope_pct > threshold:
            # Тренд вверх: сила = нормализованное значение (0.0 ... 1.0)
            st.trend_direction = "UPTREND"
            st.trend_strength = min(slope_pct / (threshold * 3.0), 1.0)
        elif slope_pct < -threshold:
            # Тренд вниз: сила = отрицательная нормализация (-1.0 ... 0.0)
            st.trend_direction = "DOWNTREND"
            st.trend_strength = max(slope_pct / (threshold * 3.0), -1.0)
        else:
            st.trend_direction = "RANGE"
            st.trend_strength = 0.0

        return st.trend_direction

    def _is_trend_against(self, symbol: str, pos_side: str) -> bool:
        """Проверяет, направлен ли тренд ПРОТИВ указанной стороны позиции.

        Возвращает True, если тренд идёт против нас:
          - LONG при DOWNTREND → True (цена падает, а мы покупаем)
          - SHORT при UPTREND → True (цена растёт, а мы продаём)
        """
        st = self.states[symbol]
        trend = st.trend_direction

        if trend == "RANGE":
            return False

        if pos_side == "LONG" and trend == "DOWNTREND":
            return True

        if pos_side == "SHORT" and trend == "UPTREND":
            return True

        return False

    # --- ОСНОВНАЯ ЛОГИКА ---
    def update_strategy_for_side(self, symbol, pos_side):
        if symbol not in self.states: return
        state = self.states[symbol]
        info = state.info
        is_l = (pos_side == "LONG")
        try:
            with self.lock:
                self._cancel_side_orders(symbol, pos_side)
                amt, entry = (state.long_amt, state.long_entry) if is_l else (state.short_amt, state.short_entry)
                current_size = state.current_long_order_size if is_l else state.current_short_order_size

                # 1. ОБНОВЛЕНИЕ ЦЕНЫ СТОП-ЛОССА (для мониторинга)
                center = state.long_grid_center if is_l else state.short_grid_center
                if amt > info.min_qty and center > 0:
                    grid_depth = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.GRID_LEVELS))
                    total_threshold = grid_depth + Config.STOP_LOSS_BEYOND_GRID_PCT
                    sl_p = center * (Decimal("1.0") - total_threshold) if is_l else center * (
                                Decimal("1.0") + total_threshold)
                    if is_l:
                        state.long_sl_price = sl_p
                    else:
                        state.short_sl_price = sl_p
                    log.info(f"[{symbol}] 🛡️ SL {pos_side} → {self._rp(sl_p, info)}")
                    # --- Сохраняем SL на диск (критично для восстановления) ---
                    self._save_state_to_disk()

                # 2. ТЕЙК-ПРОФИТ
                if amt > info.min_qty:
                    tp_p = entry * (Decimal("1.0") + Config.TAKE_PROFIT_PCT) if is_l else entry * (
                                Decimal("1.0") - Config.TAKE_PROFIT_PCT)
                    tp_ps, tp_qs = self._rp(tp_p, info), self._rq(amt, info)
                    try:
                        self.client.new_order(symbol=symbol, side="SELL" if is_l else "BUY", positionSide=pos_side,
                                              type="LIMIT", quantity=tp_qs, price=tp_ps, timeInForce="GTC")
                        log.info(f"[{symbol}] 🎯 TP {pos_side} → {tp_ps}")
                    except ClientError as e:
                        if e.error_code == -2022: amt = Decimal("0")

                    # 3. СЕТКА УСРЕДНЕНИЯ (с лимитом накопления + проверка тренда)
                    if amt > 0:
                        if (amt * entry) < (current_size * Config.MAX_EXPOSURE_MULTIPLIER):
                            # --- ПРОВЕРКА ТРЕНДА: не усредняться ПРОТИВ тренда ---
                            if self._is_trend_against(symbol, pos_side):
                                log.info(
                                    f"[{symbol}] ⏸️ Тренд {state.trend_direction} "
                                    f"(сила {state.trend_strength:.2f}) "
                            f"→ усреднение {pos_side} приостановлено"
                                )
                            else:
                                recon_base = self._recon(entry, amt, pos_side, symbol, current_size)
                                grid = self._calc_grid(recon_base, pos_side, current_size)
                                batch = []
                                for p, q, v, d in grid:
                                    if (is_l and p < state.last_price * Decimal("0.9997")) or (
                                            not is_l and p > state.last_price * Decimal("1.0003")):
                                        ps, qs = self._rp(p, info), self._rq(q, info)
                                        if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                                            batch.append({"symbol": symbol, "side": "BUY" if is_l else "SELL",
                                                          "positionSide": pos_side,
                                                          "type": "LIMIT", "quantity": qs, "price": ps,
                                                          "timeInForce": "GTX"})
                                self._place_batch(symbol, batch)
                        else:
                            log.warning(f"[{symbol}] 🛑 Лимит экспозиции {pos_side}, усреднение остановлено")

                # 4. НОВАЯ СЕТКА (СТАРТ)
                if amt <= info.min_qty:
                    # --- ПРОВЕРКА ТРЕНДА: не стартовать ПРОТИВ тренда ---
                    # Сторона ПО тренду (LONG при UPTREND, SHORT при DOWNTREND)
                    # разрешена: трейлинг подтягивает сетку за ценой, прибыль на откатах
                    if self._is_trend_against(symbol, pos_side):
                        log.info(
                            f"[{symbol}] ⏸️ Тренд {state.trend_direction} "
                            f"(сила {state.trend_strength:.2f}) "
                            f"→ старт {pos_side} отложен"
                        )
                    else:
                        new_size = self._get_dynamic_order_size()
                        if new_size > 0:
                            if is_l:
                                state.current_long_order_size, state.long_grid_center = new_size, state.last_price
                            else:
                                state.current_short_order_size, state.short_grid_center = new_size, state.last_price
                            self._save_state_to_disk()
                            log.info(f"[{symbol}] 🆕 Старт {pos_side} от {self._rp(state.last_price, info)} | объём {new_size:.2f}$")
                            grid = self._calc_grid(state.last_price, pos_side, new_size)
                            batch = []
                            for p, q, v, d in grid:
                                ps, qs = self._rp(p, info), self._rq(q, info)
                                if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                                    batch.append(
                                        {"symbol": symbol, "side": "BUY" if is_l else "SELL", "positionSide": pos_side,
                                         "type": "LIMIT", "quantity": qs, "price": ps, "timeInForce": "GTX"})
                            self._place_batch(symbol, batch)

        except Exception as e:
            log.error(f"[{symbol}] ❌ Ошибка стратегии {pos_side}: {e}")

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
                        st.price_buffer.append(float(price))
                        self._detect_trend(s)
                        now = time.time()

                        # --- СТОП-ЛОСС ---
                        if st.long_amt > 0 and st.long_sl_price > 0 and price <= st.long_sl_price:
                            if now - st.last_sl_attempt > 2.0:
                                st.last_sl_attempt = now
                                log.critical(f"[{s}] 🚨 СТОП-ЛОСС LONG! Закрытие позиции...")
                                try:
                                    self._cancel_side_orders(s, "LONG")
                                    self.client.new_order(symbol=s, side="SELL", positionSide="LONG", type="MARKET",
                                                          quantity=self._rq(st.long_amt, st.info))
                                except:
                                    pass

                        if st.short_amt > 0 and st.short_sl_price > 0 and price >= st.short_sl_price:
                            if now - st.last_sl_attempt > 2.0:
                                st.last_sl_attempt = now
                                log.critical(f"[{s}] 🚨 СТОП-ЛОСС SHORT! Закрытие позиции...")
                                try:
                                    self._cancel_side_orders(s, "SHORT")
                                    self.client.new_order(symbol=s, side="BUY", positionSide="SHORT", type="MARKET",
                                                          quantity=self._rq(st.short_amt, st.info))
                                except:
                                    pass

                        # --- ЗАКРЫТИЕ ПО ТРЕНДУ ---
                        # Если тренд направлен ПРОТИВ открытой позиции и нереализованный
                        # убыток превышает размер TP — закрываем позицию по рынку.
                        # Логика: если мы уже потеряли больше чем могли бы заработать (TP),
                        # нет смысла держать позицию против тренда — цена продолжит идти против.
                        # Троттлинг: не чаще 1 раза в 5 секунд.
                        if now - st.last_trend_close_attempt > 5.0:
                            tp_pct = float(Config.TAKE_PROFIT_PCT)

                            if st.long_amt > 0 and self._is_trend_against(s, "LONG") and st.long_entry > 0:
                                # Для LONG: убыток = (entry - price) / entry
                                unrealized_loss_pct = float((st.long_entry - price) / st.long_entry)
                                if unrealized_loss_pct > tp_pct:
                                    st.last_trend_close_attempt = now
                                    log.warning(
                                        f"[{s}] 🔄 Тренд {st.trend_direction} против LONG, "
                                        f"убыток {unrealized_loss_pct:.4f}% > TP {tp_pct:.4f}% — "
                                        f"закрытие по рынку"
                                    )
                                    try:
                                        self._cancel_side_orders(s, "LONG")
                                        self.client.new_order(
                                            symbol=s, side="SELL", positionSide="LONG",
                                            type="MARKET", quantity=self._rq(st.long_amt, st.info)
                                        )
                                    except:
                                        pass

                            if st.short_amt > 0 and self._is_trend_against(s, "SHORT") and st.short_entry > 0:
                                # Для SHORT: убыток = (price - entry) / entry
                                unrealized_loss_pct = float((price - st.short_entry) / st.short_entry)
                                if unrealized_loss_pct > tp_pct:
                                    st.last_trend_close_attempt = now
                                    log.warning(
                                        f"[{s}] 🔄 Тренд {st.trend_direction} против SHORT, "
                                        f"убыток {unrealized_loss_pct:.4f}% > TP {tp_pct:.4f}% — "
                                        f"закрытие по рынку"
                                    )
                                    try:
                                        self._cancel_side_orders(s, "SHORT")
                                        self.client.new_order(
                                            symbol=s, side="BUY", positionSide="SHORT",
                                            type="MARKET", quantity=self._rq(st.short_amt, st.info)
                                        )
                                    except:
                                        pass

                        # --- ТРЕЙЛИНГ ---
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
                    log.info(f"[{o['s']}] ⚡ Исполнен {o['ps']} {o['S']} @ {o['p']} qty={o['q']}")
                    threading.Thread(target=self.update_strategy_for_side, args=(o['s'], o['ps']), daemon=True).start()
            elif e == 'ACCOUNT_UPDATE':
                for p in msg['a']['P']:
                    sym = p['s']
                    if sym in self.states:
                        with self.lock:
                            amt, ent, ps = Decimal(str(p['pa'])), Decimal(str(p['ep'])), p['ps']
                            if ps == "LONG":
                                self.states[sym].long_amt, self.states[sym].long_entry = amt, ent
                                if amt == 0: self.states[sym].long_sl_price = Decimal("0")
                            elif ps == "SHORT":
                                self.states[sym].short_amt, self.states[sym].short_entry = abs(amt), ent
                                if amt == 0: self.states[sym].short_sl_price = Decimal("0")
                        # --- Сохраняем изменение позиции на диск ---
                        self._save_state_to_disk()
        except:
            pass

    def run_maintenance(self):
        last_renew = last_audit = time.time()
        while self.running:
            try:
                # 1. Продление жизни сокета
                if time.time() - last_renew > 1800:
                    self.client.renew_listen_key(self.listen_key)
                    last_renew = time.time()

                # 2. Периодический аудит (раз в AUDIT_INTERVAL сек)
                if time.time() - last_audit > Config.AUDIT_INTERVAL:
                    # А) Синхронизируем позиции
                    self._sync_all_positions_rest()

                    # А.1) Сохраняем синхронизированные позиции на диск
                    self._save_state_to_disk()

                    # Б) Проверяем, не зависла ли какая-то сторона из-за нехватки маржи
                    new_size = self._get_dynamic_order_size()
                    if new_size > 0:  # Если деньги на счету появились
                        for sym in Config.SYMBOLS:
                            # Получаем реальные открытые ордера по монете
                            all_open = self._safe_get_open_orders(sym)

                            for side in ["LONG", "SHORT"]:
                                amt = self.states[sym].long_amt if side == "LONG" else self.states[sym].short_amt
                                side_orders = [o for o in all_open if o['positionSide'] == side]

                                # Если позиции нет И ордеров нет — сторона "заглохла"
                                if amt == 0 and not side_orders:
                                    # Не перезапускать, если тренд идёт против этой стороны
                                    if not self._is_trend_against(sym, side):
                                        log.info(
                                            f"[{sym}] ♻️ Перезапуск {side} (маржа восстановлена)"
                                        )
                                        threading.Thread(target=self.update_strategy_for_side, args=(sym, side),
                                                         daemon=True).start()
                                    else:
                                        log.info(
                                            f"[{sym}] ⏸️ {side} на паузе "
                                            f"(тренд {self.states[sym].trend_direction}), не перезапускается"
                                        )

                    last_audit = time.time()

                self.client.time()  # Пинг REST сессии
            except Exception as e:
                log.error(f"Ошибка обслуживания: {e}")

            time.sleep(10)

    def run(self):
        log.info("🚀 Бот v2.1 | SL + тренд-детекция + закрытие по тренду + 6 уровней")
        self.initialize()
        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_ws_msg)
        self.listen_key = self.client.new_listen_key()['listenKey']
        self.ws_client.user_data(listen_key=self.listen_key)
        for sym in Config.SYMBOLS: self.ws_client.agg_trade(symbol=sym.lower())
        threading.Thread(target=self.run_maintenance, daemon=True).start()

        def watchdog():
            while self.running:
                if time.time() - self.last_ws_update > Config.WATCHDOG_TIMEOUT:
                    log.critical("🚨 WebSocket не отвечает! Перезапуск...")
                    os.kill(os.getpid(), signal.SIGINT)
                time.sleep(10)

        threading.Thread(target=watchdog, daemon=True).start()

        # --- ПРОГРЕВ: накопление тиков для детекции тренда ---
        # Пропускаем, если буфер уже заполнен (восстановление из файла)
        # или есть открытые позиции (нужен немедленный SL/TP мониторинг)
        warmup_needed = False
        for sym in Config.SYMBOLS:
            current_len = len(self.states[sym].price_buffer)
            has_position = (self.states[sym].long_amt > 0 or self.states[sym].short_amt > 0)
            if current_len < Config.TREND_MIN_SAMPLES and not has_position:
                warmup_needed = True
                break

        if warmup_needed:
            log.info(
                f"⏳ Прогрев: ожидание {Config.TREND_MIN_SAMPLES} тиков перед размещением сеток..."
            )
            for sym in Config.SYMBOLS:
                while self.running and len(self.states[sym].price_buffer) < Config.TREND_MIN_SAMPLES:
                    time.sleep(0.5)
                if not self.running:
                    break
                log.info(
                    f"[{sym}] 🔥 Прогрев завершён: "
                    f"тренд={self.states[sym].trend_direction} "
                    f"сила={self.states[sym].trend_strength:.2f} "
                    f"буфер={len(self.states[sym].price_buffer)}"
                )
        else:
            log.info("⏩ Прогрев пропущен (буфер восстановлен / есть позиции)")

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