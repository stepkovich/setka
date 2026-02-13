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

# Импорт библиотек Binance
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
from binance.error import ClientError
from requests.exceptions import RequestException, ConnectionError, Timeout
from dotenv import load_dotenv

# ==========================================
# 0. CONFIG & PRECISION+
# ==========================================
load_dotenv()  # Загружает ключи из файла .env
getcontext().prec = 50  # Устанавливаем точность вычислений (50 знаков), чтобы избежать ошибок округления денег

# Настройка логирования (вывод сообщений в консоль)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("HEDGE_ULTIMATE")
# Отключаем лишний мусор в логах от библиотек
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("binance.websocket").setLevel(logging.WARNING)


class Config:
    """Конфигурация бота. Все настройки в одном месте."""
    API_KEY = os.getenv("BINANCE_API_KEY", "")
    API_SECRET = os.getenv("BINANCE_API_SECRET", "")

    SYMBOLS = ["1000PEPEUSDC", "DOGEUSDC"]  # Торговые пары
    LEVERAGE = 25  # Кредитное плечо

    # --- РИСК-МЕНЕДЖМЕНТ ---
    # Делитель баланса. Определяет, какую часть депозита выделять на одну сетку.
    # Если баланс 1000$, монет 2, коэф 10 -> 1000 / (2 * 10) = 50$ (Максимальный первый ордер)
    BALANCE_PER_1_DOLLAR_ORDER = Decimal("10")

    # Жесткие лимиты размера первого ордера (в долларах)
    MIN_ORDER_SIZE = Decimal("5.2")
    MAX_ORDER_SIZE = Decimal("10")

    GRID_LEVELS = 13  # Сколько всего усредняющих ордеров в сетке
    FIB_STEP_BASE = Decimal("0.00017")  # Базовый шаг сетки (0.017% * Фибоначчи)
    VOL_COEFF = Decimal("150.0")  # Множитель мартингейла (агрессивность увеличения объема)

    TAKE_PROFIT_PCT = Decimal("0.001")  # Тейк-профит 0.1% от средней цены входа

    # PAGEN - параметр для трейлинга (подтягивания сетки за ценой до входа)
    PAGEN = 3

    # На сколько % цена должна уйти за пределы всей сетки, чтобы сработал Стоп-Лосс
    STOP_LOSS_BEYOND_GRID_PCT = Decimal("0.03")

    # Технические настройки
    WATCHDOG_TIMEOUT = 60  # Если нет данных от биржи 60 сек - перезапуск
    AUDIT_INTERVAL = 60  # Как часто сверять позиции с API (раз в минуту)
    STATE_FILE = "bot_state.json"  # Файл для сохранения памяти бота


@dataclass
class SymbolPrecision:
    """Хранит правила округления для конкретной пары (из API биржи)"""
    tick_size: Decimal  # Мин. шаг цены (напр. 0.000001)
    step_size: Decimal  # Мин. шаг количества (напр. 1.0 монет)
    min_qty: Decimal  # Мин. кол-во для ордера
    min_notional: Decimal  # Мин. стоимость ордера в $ (обычно 5$)
    price_precision: int  # Кол-во знаков после запятой в цене
    qty_precision: int  # Кол-во знаков после запятой в кол-ве


@dataclass
class SymbolState:
    """Память бота для одной пары. Хранит текущую ситуацию."""
    symbol: str
    info: SymbolPrecision

    # Рыночные данные
    last_price: Decimal = Decimal("0")  # Последняя цена сделки с рынка

    # Данные позиции (из API)
    long_amt: Decimal = Decimal("0")  # Кол-во монет в лонге
    long_entry: Decimal = Decimal("0")  # Средняя цена входа лонга
    short_amt: Decimal = Decimal("0")  # Кол-во монет в шорте
    short_entry: Decimal = Decimal("0")  # Средняя цена входа шорта

    # Внутренние данные стратегии (самое важное!)
    long_grid_center: Decimal = Decimal("0")  # Цена, ОТКУДА началась сетка Long
    short_grid_center: Decimal = Decimal("0")  # Цена, ОТКУДА началась сетка Short

    current_long_order_size: Decimal = Config.MIN_ORDER_SIZE  # Текущий размер первого ордера
    current_short_order_size: Decimal = Config.MIN_ORDER_SIZE

    # Цены стоп-лосса (рассчитываются ботом)
    long_sl_price: Decimal = Decimal("0")
    short_sl_price: Decimal = Decimal("0")

    # Порог для трейлинга (смещения сетки за ценой)
    trailing_threshold_pct: Decimal = Decimal("0")


def retry_request(max_retries=3, delay=1.0):
    """Декоратор: если интернет моргнул, пробуем снова 3 раза перед ошибкой."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_err = None
            for i in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, Timeout, RequestException) as e:
                    time.sleep(delay)
                    last_err = e
                except ClientError as e:
                    # Если ошибка 5xx (сервер биржи упал) - ждем и повторяем
                    if int(e.status_code) >= 500:
                        time.sleep(delay)
                        last_err = e
                    else:
                        raise e  # Если ошибка наша (неверные параметры) - падаем сразу
            if last_err: raise last_err

        return wrapper

    return decorator


class HedgeBot:
    def __init__(self):
        """Конструктор класса бота."""
        self.running = True  # Флаг работы главного цикла
        self.lock = threading.RLock()  # Блокировка для защиты данных от одновременного доступа потоков
        self.client: Optional[UMFutures] = None  # REST клиент (запросы)
        self.ws_client: Optional[UMFuturesWebsocketClient] = None  # WebSocket клиент (поток данных)
        self.states: Dict[str, SymbolState] = {}  # Словарь состояний всех пар
        self.last_ws_update = time.time()  # Время последнего сигнала жизни от биржи
        self.listen_key = None  # Ключ для подписки на данные аккаунта

    def initialize(self):
        """Первичная настройка и запуск."""
        log.info(f"🔹 Starting Bot (GRID_LEVEL: {Config.GRID_LEVELS}, SYMBOLS: {Config.SYMBOLS})")

        # Проверка ключей
        if not Config.API_KEY:
            log.critical("❌ No API Keys")
            sys.exit(1)

        try:
            # Подключение к API
            self.client = UMFutures(key=Config.API_KEY, secret=Config.API_SECRET)

            # Получаем правила торговли (точность, мин. лот)
            ex_info = self.client.exchange_info()
            all_info = {s['symbol']: s for s in ex_info['symbols']}

            # Загружаем память с диска (если бот перезапущен)
            saved_data = self._load_state_from_disk()

            # Настраиваем каждую пару
            for sym in Config.SYMBOLS:
                if sym not in all_info: continue
                s_info = all_info[sym]

                # Парсим фильтры (Price Filter, Lot Size, Min Notional)
                p_f = next(f for f in s_info['filters'] if f['filterType'] == 'PRICE_FILTER')
                l_f = next(f for f in s_info['filters'] if f['filterType'] == 'LOT_SIZE')
                n_f = next((f for f in s_info['filters'] if f['filterType'] in ['MIN_NOTIONAL', 'NOTIONAL']), None)
                mn = Decimal("6.0")  # Дефолтное значение
                if n_f: mn = Decimal(str(n_f.get('notional', n_f.get('minNotional', 6.0))))

                prec = SymbolPrecision(
                    tick_size=Decimal(str(p_f['tickSize'])),
                    step_size=Decimal(str(l_f['stepSize'])),
                    min_qty=Decimal(str(l_f['minQty'])),
                    min_notional=mn,
                    price_precision=int(s_info['pricePrecision']),
                    qty_precision=int(s_info['quantityPrecision'])
                )

                # Рассчитываем порог трейлинга (на основе PAGEN)
                dist = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.PAGEN))

                st = SymbolState(symbol=sym, info=prec, trailing_threshold_pct=dist)

                # Если есть сохраненные данные - восстанавливаем их
                if sym in saved_data:
                    st.current_long_order_size = Decimal(str(saved_data[sym].get('l_size', Config.MIN_ORDER_SIZE)))
                    st.current_short_order_size = Decimal(str(saved_data[sym].get('s_size', Config.MIN_ORDER_SIZE)))
                    st.long_grid_center = Decimal(str(saved_data[sym].get('l_center', "0")))
                    st.short_grid_center = Decimal(str(saved_data[sym].get('s_center', "0")))
                    log.info(f"[{sym}] 💾 Full State Restored.")

                self.states[sym] = st
                self._setup_account(sym)  # Ставим плечо и хедж-режим

                # Получаем первую цену (чтобы не ждать вебсокет)
                ticker = self.client.ticker_price(sym)
                self.states[sym].last_price = Decimal(str(ticker['price']))

            # Синхронизируем реальные позиции с биржи
            self._sync_all_positions_rest()
            log.info("✅ Persistence ready. Trading started.")

        except Exception as e:
            log.critical(f"Init Fail: {e}");
            sys.exit(1)

    def _save_state_to_disk(self):
        """Сохраняет важные переменные (центр сетки, размер ордера) в файл JSON."""
        try:
            data = {}
            with self.lock:  # Блокируем доступ, чтобы данные не изменились во время записи
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
        """Читает файл JSON с состоянием."""
        if os.path.exists(Config.STATE_FILE):
            try:
                with open(Config.STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {}

    def _get_dynamic_order_size(self):
        """Рассчитывает размер первого ордера исходя из баланса."""
        try:
            acc = self.client.account()
            balance = Decimal(str(acc['totalWalletBalance']))
            # Формула: Баланс / (Кол-во пар * Коэффициент)
            calc = balance / (Decimal(str(len(Config.SYMBOLS))) * Config.BALANCE_PER_1_DOLLAR_ORDER)
            # Обрезаем лимитами MIN и MAX
            return max(Config.MIN_ORDER_SIZE, min(calc, Config.MAX_ORDER_SIZE))
        except:
            return Config.MIN_ORDER_SIZE

    def _setup_account(self, symbol):
        """Включает хедж-режим (Dual Side) и выставляет плечо."""
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
        """Генерирует последовательность Фибоначчи [1, 1, 2, 3, 5...]"""
        seq = [1, 1]
        for i in range(2, n): seq.append(seq[-1] + seq[-2])
        return seq[:n]

    def _calc_grid(self, base: Decimal, direction: str, order_size: Decimal) -> List[
        Tuple[Decimal, Decimal, Decimal, Decimal]]:
        """
        Математическое сердце бота. Рассчитывает координаты всех ордеров сетки.
        Возвращает список: [(Цена, Кол-во, Объем_USD, Дистанция), ...]
        """
        lvls = []
        fib_seq = self._fib(Config.GRID_LEVELS)
        cum_dist = Decimal("0")

        for i in range(Config.GRID_LEVELS):
            # Шаг цены увеличивается по Фибоначчи
            step = Config.FIB_STEP_BASE * Decimal(str(fib_seq[i]))
            cum_dist += step

            # Цена уровня
            price = base * (Decimal("1.0") - cum_dist) if direction == "LONG" else base * (Decimal("1.0") + cum_dist)

            # Объем ордера увеличивается (Мартингейл)
            multiplier = max(Decimal("1.0"), Decimal("1.0") + (step * Config.VOL_COEFF))
            qty = (order_size * multiplier) / price

            lvls.append((price, qty, order_size * multiplier, cum_dist))
        return lvls

    def _rp(self, p: Decimal, info: SymbolPrecision) -> str:
        """Округляет цену до нужного знака (Price Precision)."""
        val = (p / info.tick_size).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * info.tick_size
        return "{:.{prec}f}".format(val, prec=info.price_precision)

    def _rq(self, q: Decimal, info: SymbolPrecision) -> str:
        """Округляет кол-во до нужного знака (Quantity Precision) ВНИЗ."""
        val = (q / info.step_size).quantize(Decimal('1'), rounding=ROUND_FLOOR) * info.step_size
        return "{:.{prec}f}".format(val, prec=info.qty_precision)

    @retry_request()
    def _safe_get_open_orders(self, symbol):
        """Безопасный запрос открытых ордеров."""
        return self.client.sign_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    @retry_request()
    def _cancel_side_orders(self, symbol, pos_side):
        """Удаляет ВСЕ ордера указанной стороны (LONG или SHORT) для конкретной пары."""
        all_open = self._safe_get_open_orders(symbol)
        ids = [o['orderId'] for o in all_open if o['positionSide'] == pos_side]
        if not ids: return
        # Удаляем пачками по 10 штук
        for i in range(0, len(ids), 10):
            params = {"symbol": symbol, "orderIdList": json.dumps(ids[i:i + 10])}
            self.client.sign_request("DELETE", "/fapi/v1/batchOrders", params)

    @retry_request()
    def _place_batch(self, symbol, params_list):
        """Отправляет пачку ордеров (до 5 штук за раз) на биржу."""
        if not params_list: return
        for i in range(0, len(params_list), 5):
            query = {"symbol": symbol, "batchOrders": json.dumps(params_list[i:i + 5])}
            self.client.sign_request("POST", "/fapi/v1/batchOrders", query)

    @retry_request()
    def _sync_all_positions_rest(self):
        """Запрашивает у биржи точные размеры позиций и цены входа."""
        pos_data = self.client.get_position_risk()
        with self.lock:
            # Сбрасываем локальные данные в 0 перед обновлением
            for s in self.states.values():
                s.long_amt = Decimal("0")
                s.short_amt = Decimal("0")
            # Заполняем актуальными данными
            for p in pos_data:
                sym = p['symbol']
                if sym in self.states:
                    amt = Decimal(str(p['pa'] if 'pa' in p else p['positionAmt']))
                    ent = Decimal(str(p['ep'] if 'ep' in p else p['entryPrice']))
                    if p['positionSide'] == "LONG":
                        self.states[sym].long_amt, self.states[sym].long_entry = amt, ent
                    elif p['positionSide'] == "SHORT":
                        self.states[sym].short_amt, self.states[sym].short_entry = abs(amt), ent

    # ------------------------------------------------------------------
    # [NEW] ФУНКЦИЯ ВОССТАНОВЛЕНИЯ
    # ------------------------------------------------------------------
    def _recon(self, avg_entry: Decimal, qty: Decimal, direction: str, symbol: str,
               base_order_size: Decimal) -> Decimal:
        """
        Восстанавливает цену старта сетки (Base Price), если память бота была утеряна.
        Использует объем позиции в долларах для определения текущего уровня сетки.
        """
        # Если позиция мизерная - считаем, что сетка начинается прямо здесь
        if qty < self.states[symbol].info.min_qty:
            return self.states[symbol].last_price

        # Считаем текущий объем позиции в долларах
        current_notional = qty * avg_entry

        # Симулируем идеальную сетку
        fib_seq = self._fib(Config.GRID_LEVELS)
        accumulated_notional = Decimal("0")
        cum_dist_pct = Decimal("0")
        effective_dist_pct = Decimal("0")

        # Проходим по уровням, пока не наберем такой же объем
        for i in range(Config.GRID_LEVELS):
            step_fib = Config.FIB_STEP_BASE * Decimal(str(fib_seq[i]))
            cum_dist_pct += step_fib

            # Множитель объема на этом уровне
            multiplier = max(Decimal("1.0"), Decimal("1.0") + (step_fib * Config.VOL_COEFF))
            accumulated_notional += base_order_size * multiplier

            # Если теоретический объем догнал реальный (с допуском 15%)
            if accumulated_notional >= current_notional * Decimal("0.85"):
                # Мы нашли уровень! Считаем, что цена входа отстает от базы на 60% пройденного пути
                effective_dist_pct = cum_dist_pct * Decimal("0.6")
                break

        # Если цикл прошел, а уровень не найден (огромная позиция), берем максимальную дистанцию
        if effective_dist_pct == Decimal("0"): effective_dist_pct = cum_dist_pct

        # Восстанавливаем Base Price: AvgEntry / (1 +/- dist)
        if direction == "LONG":
            return avg_entry / (Decimal("1.0") - effective_dist_pct)
        else:
            return avg_entry / (Decimal("1.0") + effective_dist_pct)

    # ------------------------------------------------------------------
    # [NEW] ОСНОВНАЯ ЛОГИКА ТОРГОВЛИ
    # ------------------------------------------------------------------
    def update_strategy_for_side(self, symbol, pos_side):
        """
        Проверяет состояние и обновляет ордера.
        Реализует логику: "Снести старое -> Поставить правильное новое".
        """
        # --- ФАЗА 1: Чтение данных (ПОД ЗАМКОМ) ---
        with self.lock:
            if symbol not in self.states: return
            st = self.states[symbol]
            info = st.info
            is_long = (pos_side == "LONG")
            last_price = st.last_price

            # Копируем нужные переменные, чтобы освободить лок
            if is_long:
                amt, entry = st.long_amt, st.long_entry
                grid_center = st.long_grid_center
                order_size = st.current_long_order_size
            else:
                amt, entry = st.short_amt, st.short_entry
                grid_center = st.short_grid_center
                order_size = st.current_short_order_size

        # --- ФАЗА 2: Выполнение действий (БЕЗ ЗАМКА - сеть свободна) ---
        try:
            # СЦЕНАРИЙ А: Есть открытая позиция
            if amt > info.min_qty:

                # 1. Определяем, ОТКУДА должна строиться сетка
                base_price = Decimal("0")
                if grid_center > 0:
                    # Идеально: мы помним центр сетки
                    base_price = grid_center
                else:
                    # Плохо: мы забыли центр, восстанавливаем через _recon
                    base_price = self._recon(entry, amt, pos_side, symbol, order_size)
                    with self.lock:
                        if is_long:
                            self.states[symbol].long_grid_center = base_price
                        else:
                            self.states[symbol].short_grid_center = base_price
                    log.warning(f"[{symbol}] ⚠️ {pos_side} Base restored to {base_price}")

                # 2. УДАЛЯЕМ ВСЕ ОРДЕРА (ЧИСТКА)
                self._cancel_side_orders(symbol, pos_side)

                # 3. СТАВИМ ТЕЙК-ПРОФИТ
                tp_price = entry * (Decimal("1.0") + Config.TAKE_PROFIT_PCT) if is_long else entry * (
                            Decimal("1.0") - Config.TAKE_PROFIT_PCT)
                try:
                    self.client.new_order(
                        symbol=symbol, side="SELL" if is_long else "BUY", positionSide=pos_side,
                        type="LIMIT", quantity=self._rq(amt, info), price=self._rp(tp_price, info),
                        timeInForce="GTC", reduceOnly="true"  # reduceOnly - закрывает позицию, не открывая новую
                    )
                except Exception as e:
                    # Ошибка может быть, если цена уже проскочила TP
                    log.error(f"[{symbol}] TP Error: {e}")

                # 4. РАССЧИТЫВАЕМ ЦЕНУ СТОП-ЛОССА (Только в памяти)
                grid_depth = sum(Config.FIB_STEP_BASE * Decimal(str(f)) for f in self._fib(Config.GRID_LEVELS))
                total_sl_dist = grid_depth + Config.STOP_LOSS_BEYOND_GRID_PCT
                sl_val = base_price * (Decimal("1.0") - total_sl_dist) if is_long else base_price * (
                            Decimal("1.0") + total_sl_dist)

                # Сохраняем SL в состояние (для проверки в on_ws_msg)
                with self.lock:
                    if is_long:
                        self.states[symbol].long_sl_price = sl_val
                    else:
                        self.states[symbol].short_sl_price = sl_val

                # 5. СТРОИМ СЕТКУ ЛИМИТОК
                grid = self._calc_grid(base_price, pos_side, order_size)
                batch = []

                for p, q, v, d in grid:
                    # Фильтр: ставим только те ордера, до которых цена еще НЕ дошла
                    # (те, что "глубже" в минус)
                    is_pending_order = False
                    if is_long:
                        if p < last_price * Decimal("0.9995"): is_pending_order = True
                    else:
                        if p > last_price * Decimal("1.0005"): is_pending_order = True

                    if is_pending_order:
                        ps, qs = self._rp(p, info), self._rq(q, info)
                        # Проверка на минимальный размер ордера
                        if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                            batch.append({
                                "symbol": symbol, "side": "BUY" if is_long else "SELL",
                                "positionSide": pos_side, "type": "LIMIT",
                                "quantity": qs, "price": ps, "timeInForce": "GTX"  # GTX = Post Only
                            })

                # Отправляем пачкой
                if batch:
                    self._place_batch(symbol, batch)

            # СЦЕНАРИЙ Б: Позиции нет (или закрылась)
            elif amt <= info.min_qty:
                # Сносим старые ордера (если остались хвосты)
                self._cancel_side_orders(symbol, pos_side)

                # Считаем новый размер ордера
                new_size = self._get_dynamic_order_size()

                # Обновляем память: Центр = Текущая цена
                with self.lock:
                    if is_long:
                        self.states[symbol].current_long_order_size = new_size
                        self.states[symbol].long_grid_center = last_price
                        self.states[symbol].long_sl_price = Decimal("0")  # Сброс стопа
                    else:
                        self.states[symbol].current_short_order_size = new_size
                        self.states[symbol].short_grid_center = last_price
                        self.states[symbol].short_sl_price = Decimal("0")

                # Сохраняем на диск
                self._save_state_to_disk()
                log.info(f"[{symbol}] 🆕 Fresh Start {pos_side} @ {last_price}")

                # Ставим ПОЛНУЮ сетку
                grid = self._calc_grid(last_price, pos_side, new_size)
                batch = []
                for p, q, v, d in grid:
                    ps, qs = self._rp(p, info), self._rq(q, info)
                    if Decimal(qs) >= info.min_qty and (Decimal(ps) * Decimal(qs)) >= info.min_notional:
                        batch.append({
                            "symbol": symbol, "side": "BUY" if is_long else "SELL",
                            "positionSide": pos_side, "type": "LIMIT",
                            "quantity": qs, "price": ps, "timeInForce": "GTX"
                        })
                self._place_batch(symbol, batch)

        except Exception as e:
            log.error(f"[{symbol}] ❌ Strategy Error {pos_side}: {e}")

    def on_ws_msg(self, _, m):
        """Обработчик сообщений от WebSocket (вызывается биржей)."""
        try:
            msg = json.loads(m) if isinstance(m, str) else m
            self.last_ws_update = time.time()  # Обновляем "пульс" бота
            e = msg.get('e')  # Тип события

            # --- СОБЫТИЕ: ИЗМЕНЕНИЕ ЦЕНЫ ---
            if e == 'aggTrade':
                s = msg['s']
                if s in self.states:
                    price = Decimal(str(msg['p']))
                    with self.lock:
                        st = self.states[s]
                        st.last_price = price  # Обновляем цену в памяти

                        # --- ПРОВЕРКА СТОП-ЛОССА (Программная) ---
                        # Если цена пересекла уровень SL - закрываем по рынку
                        if st.long_amt > 0 and st.long_sl_price > 0 and price <= st.long_sl_price:
                            log.critical(f"[{s}] 🚨 SL LONG HIT! {price} <= {st.long_sl_price}")
                            st.long_sl_price = Decimal("0")  # Чтобы не сработало дважды
                            try:
                                self._cancel_side_orders(s, "LONG")
                                self.client.new_order(symbol=s, side="SELL", positionSide="LONG", type="MARKET",
                                                      quantity=self._rq(st.long_amt, st.info))
                            except Exception as ex:
                                log.error(f"SL LONG FAIL: {ex}")

                        if st.short_amt > 0 and st.short_sl_price > 0 and price >= st.short_sl_price:
                            log.critical(f"[{s}] 🚨 SL SHORT HIT! {price} >= {st.short_sl_price}")
                            st.short_sl_price = Decimal("0")
                            try:
                                self._cancel_side_orders(s, "SHORT")
                                self.client.new_order(symbol=s, side="BUY", positionSide="SHORT", type="MARKET",
                                                      quantity=self._rq(st.short_amt, st.info))
                            except Exception as ex:
                                log.error(f"SL SHORT FAIL: {ex}")

                        # --- ТРЕЙЛИНГ (Подтягивание сетки до входа) ---
                        # Если нет позиции, но цена ушла в нашу сторону -> двигаем "центр" за ценой
                        th = st.trailing_threshold_pct
                        if st.long_amt == 0 and st.long_grid_center > 0:
                            if (price - st.long_grid_center) / st.long_grid_center > th:
                                st.long_grid_center = price  # Смещаем центр вверх
                                # Запускаем обновление стратегии в фоне
                                threading.Thread(target=self.update_strategy_for_side, args=(s, "LONG"),
                                                 daemon=True).start()

                        if st.short_amt == 0 and st.short_grid_center > 0:
                            if (st.short_grid_center - price) / st.short_grid_center > th:
                                st.short_grid_center = price  # Смещаем центр вниз
                                threading.Thread(target=self.update_strategy_for_side, args=(s, "SHORT"),
                                                 daemon=True).start()

            # --- СОБЫТИЕ: ИЗМЕНЕНИЕ ОРДЕРА (Исполнение) ---
            elif e == 'ORDER_TRADE_UPDATE':
                o = msg['o']
                # Если ордер полностью исполнен (FILLED)
                if o['X'] == 'FILLED' and o['s'] in self.states:
                    log.info(f"[{o['s']}] ⚡ FILLED {o['ps']} {o['S']}")
                    # Пересчитываем сетку (удаляем старое, ставим новое)
                    threading.Thread(target=self.update_strategy_for_side, args=(o['s'], o['ps']), daemon=True).start()

            # --- СОБЫТИЕ: ОБНОВЛЕНИЕ БАЛАНСА/ПОЗИЦИИ ---
            elif e == 'ACCOUNT_UPDATE':
                for p in msg['a']['P']:  # P = Positions
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
        """Фоновый поток: продление ключа и редкая сверка балансов."""
        last_renew = last_audit = time.time()
        while self.running:
            try:
                # Каждые 30 минут продлеваем ListenKey (иначе сокет умрет)
                if time.time() - last_renew > 1800:
                    self.client.renew_listen_key(self.listen_key)
                    last_renew = time.time()

                # Раз в минуту полная сверка позиций через REST API (на всякий случай)
                if time.time() - last_audit > Config.AUDIT_INTERVAL:
                    self._sync_all_positions_rest()
                    last_audit = time.time()

                # Пинг сервера (чтобы соединение не висело)
                self.client.time()
            except:
                pass
            time.sleep(10)

    def run(self):
        """Запуск всего бота."""
        log.info(f"🚀 Starting Bot! GRID_LEVEL: {Config.GRID_LEVELS}, SYMBOLS: {Config.SYMBOLS}")
        self.initialize()  # 1. Инициализация

        # 2. Запуск WebSocket
        self.ws_client = UMFuturesWebsocketClient(on_message=self.on_ws_msg)
        self.listen_key = self.client.new_listen_key()['listenKey']
        self.ws_client.user_data(listen_key=self.listen_key)

        # Подписка на цены для всех пар
        for sym in Config.SYMBOLS: self.ws_client.agg_trade(symbol=sym.lower())

        # 3. Запуск фонового обслуживания
        threading.Thread(target=self.run_maintenance, daemon=True).start()

        # 4. Watchdog (Сторожевой пес) - следит, не завис ли сокет
        def watchdog():
            while self.running:
                # Если данных нет больше 60 секунд - убиваем процесс (Docker/Systemd перезапустит)
                if time.time() - self.last_ws_update > Config.WATCHDOG_TIMEOUT:
                    log.critical("🚨 WebSocket Dead!");
                    os.kill(os.getpid(), signal.SIGINT)
                time.sleep(10)

        threading.Thread(target=watchdog, daemon=True).start()

        # 5. При старте сразу обновляем стратегии (чтобы выставить сетки)
        for sym in Config.SYMBOLS:
            self.update_strategy_for_side(sym, "LONG")
            self.update_strategy_for_side(sym, "SHORT")

        # 6. Бесконечный цикл ожидания (работаем, пока не нажмут Ctrl+C)
        try:
            while self.running: time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            self.ws_client.stop()


if __name__ == "__main__":
    HedgeBot().run()