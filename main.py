import os
import json
import asyncio
import logging
from datetime import datetime, timedelta, time as dtime

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

# --------------------- Константы и настройки путей ---------------------

DATA_FOLDER = "zakupki44fz_data"
SETTINGS_FOLDER = "settings"  # Новая папка для файлов настроек и логов
STATE_FILE = os.path.join(SETTINGS_FOLDER, "state.json")

# Проверяем наличие папки settings и создаем её, если не существует
os.makedirs(SETTINGS_FOLDER, exist_ok=True)

# --------------------- Синхронные функции для работы с файлами ---------------------

def save_state(state, state_file=STATE_FILE):
    try:
        # Убедимся, что папка для файла существует
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=4, ensure_ascii=False)
        # logging.info(f"Состояние успешно сохранено в {state_file}: {state}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении состояния в {state_file}: {str(e)}")

def load_state(state_file=STATE_FILE):
    if os.path.exists(state_file):
        with open(state_file, "r", encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {}
    return state

def format_code(code):
    try:
        return format(float(code), ".2f")
    except Exception:
        return str(code)

def extract_titles(html_str):
    """
    Извлекает значения атрибута title из всех span-элементов с классом 
    "grid-view-character-values__value" в переданном HTML-коде.
    Возвращает строку, где найденные значения объединены через "; ".
    """
    try:
        soup = BeautifulSoup(html_str, "html.parser")
        spans = soup.find_all("span", class_="grid-view-character-values__value")
        titles = [span.get("title") for span in spans if span.get("title")]
        return "; ".join(titles)
    except Exception as e:
        logging.error(f"Ошибка при извлечении title: {e}")
        return ""

def save_csv(okpd_code, page, data, date_from, date_to):
    """
    Сохраняет данные в CSV, добавляя столбец с информацией о диапазоне выгрузки.
    """
    os.makedirs(DATA_FOLDER, exist_ok=True)
    items = data.get("items", [])
    date_range_str = f"{date_from} to {date_to}"
    # Если в элементе присутствует characteristicValues, извлекаем title для full и preview
    for item in items:
        if "characteristicValues" in item:
            char_vals = item["characteristicValues"]
            if "full" in char_vals:
                item["characteristic_full_extracted"] = extract_titles(char_vals["full"])
            if "preview" in char_vals:
                item["characteristic_preview_extracted"] = extract_titles(char_vals["preview"])
        # Добавляем информацию о диапазоне выгрузки
        item["date_range"] = date_range_str
    df = pd.DataFrame(items)
    file_name = os.path.join(DATA_FOLDER, f"{okpd_code}_{page}.csv")
    df.to_csv(file_name, index=False)
    logging.info(f"Сохранён файл {file_name} с диапазоном {date_range_str}")

# --------------------- АСИНХРОННЫЕ функции для работы с API ---------------------

async def auth_async(session: aiohttp.ClientSession, retries=3):
    url = "https://auth.zakupki44fz.ru/api/v1/Login/LoginByEeoShortCode"
    params = {
        "code": "27969f60750e4ab8b3315f352d3e801d",
        "fingerprint": "notUsed",
        "audiences": [30, 1]
    }
    for attempt in range(1, retries + 1):
        try:
            async with session.post(url, json=params) as response:
                data = await response.json()
                logging.info("Авторизация прошла успешно.")
                return data['refreshToken'], data['jwtToken']
        except Exception as e:
            logging.error(f"Ошибка авторизации, попытка {attempt}: {e}")
            await asyncio.sleep(1)
    raise Exception("Не удалось авторизоваться после 3 попыток.")

async def update_jwt_token_async(session: aiohttp.ClientSession, jwt_token, rf_token, retries=3):
    url = "https://auth.zakupki44fz.ru/api/v1/User/UpdateUser"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://zakupki44fz.ru/",
        "Accept": "application/json, text/plain, */*",
    }
    params = {
        "eeoLicense": {},
        "token": rf_token,
        "fingerprint": "NotUsed"
    }
    for attempt in range(1, retries + 1):
        try:
            async with session.post(url, json=params, headers=headers) as response:
                data = await response.json()
                logging.info("Токен успешно обновлён.")
                return data['refreshToken'], data['jwtToken']
        except Exception as e:
            logging.error(f"Ошибка обновления токена, попытка {attempt}: {e}")
            await asyncio.sleep(1)
    raise Exception("Не удалось обновить токен после 3 попыток.")

async def get_purchases_async(session: aiohttp.ClientSession, jwt_token, okpd2_code, date_from, date_to, page):
    url = "https://zakupki44fz.ru/app/api/purchasesDocs/get"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://zakupki44fz.ru/",
        "Accept": "application/json, text/plain, */*",
    }
    params = {
        "name": None,
        "okpd2": okpd2_code,
        "dateFrom": date_from,
        "dateTo": date_to,
        "regionCodes": [],
        "limit": 1000,
        "page": page,
        "customerRegNums": [],
        "customersExcluded": False,
        "skipSuggest": False
    }
    async with session.post(url, json=params, headers=headers) as response:
        return await response.json()

async def get_purchases_with_retry(session: aiohttp.ClientSession, jwt_token, okpd2_code, date_from, date_to, page, retries=3):
    """
    Выполняет запрос get_purchases_async с до 3 попытками при ошибке.
    Если запрос не удаётся выполнить после 3-х попыток, возвращает {"items": []}.
    """
    for attempt in range(1, retries + 1):
        try:
            result = await get_purchases_async(session, jwt_token, okpd2_code, date_from, date_to, page)
            return result
        except Exception as e:
            logging.error(f"Ошибка запроса для кода {okpd2_code}, страница {page}, попытка {attempt}: {e}")
            await asyncio.sleep(1)
    logging.error(f"Не удалось получить данные для кода {okpd2_code}, страница {page} после {retries} попыток. Пропускаем запрос.")
    return {"items": []}

# --------------------- Функции обработки данных ---------------------

# Обработка исторических данных для кода (если дата в состоянии меньше сегодняшней)
async def process_historical(code, session, state, jwt_token):
    state_info = state[code]
    current_state_date = datetime.strptime(state_info["time"], "%Y-%m-%d").date()
    date_from_dt = datetime.combine(current_state_date, datetime.min.time())
    # Изменяем интервал с 1 до 7 дней
    date_to_dt = date_from_dt + timedelta(days=7)
    page = state_info["page"]

    while True:
        date_from_iso = date_from_dt.isoformat() + "Z"
        date_to_iso   = date_to_dt.isoformat() + "Z"
        data = await get_purchases_with_retry(session, jwt_token, code, date_from_iso, date_to_iso, page)
        items = data.get("items", [])
        logging.info(f"Исторически: ОКПД {code} за период {date_from_iso} - {date_to_iso}, страница {page}: получено {len(items)} элементов")
        # Задержка 5 секунд между запросами
        await asyncio.sleep(5)
        if not items:
            # Обновляем состояние на следующий 7-дневный интервал
            new_date = current_state_date + timedelta(days=7)
            state[code] = {"time": new_date.strftime("%Y-%m-%d"), "page": 1}
            save_state(state)
            break
        else:
            save_csv(code, page, data, date_from_iso, date_to_iso)
            page += 1
            state[code]["page"] = page
            save_state(state)

# Обработка данных за сегодняшний день для одного кода (если state.date == сегодня)
async def process_today(code, session, state, jwt_token):
    today = datetime.today().date()
    date_from_dt = datetime.combine(today, datetime.min.time())
    # Изменяем интервал с 1 до 7 дней
    date_to_dt = date_from_dt + timedelta(days=7)
    page = state[code]["page"]

    while True:
        date_from_iso = date_from_dt.isoformat() + "Z"
        date_to_iso = date_to_dt.isoformat() + "Z"
        data = await get_purchases_with_retry(session, jwt_token, code, date_from_iso, date_to_iso, page)
        items = data.get("items", [])
        logging.info(f"Сегодня: ОКПД {code} за период {date_from_iso} - {date_to_iso}, страница {page}: получено {len(items)} элементов")
        await asyncio.sleep(5)
        if not items:
            # Обновляем состояние для сегодняшнего интервала: следующий интервал начинается через 7 дней
            new_date = today + timedelta(days=7)
            state[code] = {"time": new_date.strftime("%Y-%m-%d"), "page": 1}
            save_state(state)
            break
        else:
            save_csv(code, page, data, date_from_iso, date_to_iso)
            page += 1
            state[code]["page"] = page
            save_state(state)

# Функция для каждого кода, которая обрабатывает исторические данные, если дата меньше сегодняшней;
# если дата равна сегодняшней, задача проста ждёт – централизованную обработку выполняет шедуллер.
async def process_code(code, session, state):
    global jwt_token
    while True:
        state_info = state.get(code)
        if not state_info:
            await asyncio.sleep(1)
            continue
        current_state_date = datetime.strptime(state_info["time"], "%Y-%m-%d").date()
        today = datetime.today().date()
        if current_state_date < today:
            await process_historical(code, session, state, jwt_token)
        else:
            # Если для кода сегодня уже состояние, ждем обновления (обработка через централизованный шедуллер)
            await asyncio.sleep(60)

# Централизованная задача для обработки всех кодов, у которых state.date == сегодня.
# Эта функция вычисляет время ожидания до 18:00 и затем запускает обработку за сегодняшний день единоразово.
async def scheduled_today_job(session, state):
    while True:
        now = datetime.now()
        today = datetime.today().date()
        target = datetime.combine(today, dtime(18, 0))
        if now >= target:
            logging.info("Запуск централизованной обработки сегодняшних данных для всех кодов.")
            tasks = []
            for code, info in state.items():
                state_date = datetime.strptime(info["time"], "%Y-%m-%d").date()
                if state_date == today:
                    logging.info(f"Запуск обработки для ОКПД {code} (начиная со страницы {info['page']}).")
                    tasks.append(process_today(code, session, state, jwt_token))
            if tasks:
                await asyncio.gather(*tasks)
            await asyncio.sleep(10)  # Небольшая задержка для завершения операций
            next_target = datetime.combine(today + timedelta(days=1), dtime(18, 0))
            sleep_time = (next_target - datetime.now()).total_seconds()
            logging.info(f"Ожидание {sleep_time:.0f} секунд до следующего централизованного запуска.")
            await asyncio.sleep(sleep_time)
        else:
            wait_time = (target - now).total_seconds()
            logging.info(f"Ожидание {wait_time:.0f} секунд до запуска обработки за сегодняшний день.")
            await asyncio.sleep(wait_time)

# Фоновая задача для обновления JWT-токена каждые 4 минуты
async def token_update_loop(session: aiohttp.ClientSession, update_interval=240):
    global jwt_token, rf_token, last_token_update
    while True:
        await asyncio.sleep(update_interval)
        try:
            rf_token, jwt_token = await update_jwt_token_async(session, jwt_token, rf_token)
            last_token_update = datetime.now()
            logging.info("JWT-токен обновлён в фоне.")
        except Exception as e:
            logging.error(f"Ошибка обновления токена в фоне: {str(e)}")

# --------------------- Основная функция ---------------------

async def main():
    global jwt_token, rf_token, last_token_update

    # Настройки логирования с записью в папку settings
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=[
                            logging.FileHandler(os.path.join(SETTINGS_FOLDER, "scraper.log"), encoding="utf-8"),
                            logging.StreamHandler()
                        ])
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(base_dir, "okpd2_code", "okpd2.xlsx")
        df = pd.read_excel(excel_path)
        okpd_codes = [format_code(x) for x in df['Код']]
    except Exception as e:
        logging.error(f"Ошибка загрузки Excel: {str(e)}")
        return

    state = load_state()
    default_start_date = "2012-10-10"
    for code in okpd_codes:
        if code not in state:
            state[code] = {"time": default_start_date, "page": 1}
    save_state(state)

    async with aiohttp.ClientSession() as session:
        try:
            rf_token, jwt_token = await auth_async(session)
            last_token_update = datetime.now()
        except Exception as e:
            logging.error(f"Ошибка авторизации: {str(e)}")
            return

        token_task = asyncio.create_task(token_update_loop(session))
        code_tasks = [asyncio.create_task(process_code(code, session, state)) for code in okpd_codes]
        scheduler_task = asyncio.create_task(scheduled_today_job(session, state))
        await asyncio.gather(token_task, scheduler_task, *code_tasks)

if __name__ == "__main__":
    asyncio.run(main())