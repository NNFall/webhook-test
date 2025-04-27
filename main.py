import os
import logging
import asyncio
import json
import re
from fastapi import FastAPI, Request, HTTPException
import requests

# --- Конфигурация (лучше использовать переменные окружения) ---
AMO_ACCESS_TOKEN = os.environ.get('AMO_ACCESS_TOKEN', 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjU4OWJiODVmNDYyYjZkNWIwOGUwNzRmYTkyZDUzOWY4MjFkMzAzZTE3YTMwNWY4MTc1NTVmZTY5YWNiYTk1NjcwMjAxMzA3ZDk4MDE0NTg4In0.eyJhdWQiOiIxM2QwNGJmZS0wM2I5LTRhZWYtYTgyNS0xZGE4YTc3OGM0ZTMiLCJqdGkiOiI1ODliYjg1ZjQ2MmI2ZDViMDhlMDc0ZmE5MmQ1MzlmODIxZDMwM2UxN2EzMDVmODE3NTU1ZmU2OWFjYmE5NTY3MDIwMTMwN2Q5ODAxNDU4OCIsImlhdCI6MTc0NDYzMDg3OCwibmJmIjoxNzQ0NjMwODc4LCJleHAiOjE4MzkxMTA0MDAsInN1YiI6IjExOTA1OTE4IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyMTI2NDkwLCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJjcm0iLCJmaWxlcyIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiLCJwdXNoX25vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiZTlmYTAwOWEtNjVhOC00ZjY2LTg4Y2YtODFlZDI1NjI5YTYwIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.jMJy8xHeePGzjW2jrqEaB2r2vYPUjGAcTsGzSiL0wR94SqKFWmAdP6mHkiC09UN3zvRD9xa_dgI-J6w0GrAEucg3d-cBfc7Q3Vx3pzzHQitoefVv55KH9J3TyRV8y0cPfdnbNBmxSDjoHIEWAt0-e-0iITxTSI7iqvcNg-o8yRqJNi6-s3WGOI7QN87_UQmUu2MNL1tXq7MSrp-mqExl5rP0VU_1mHv62-PDXTmU4K64bG02z331yobU1Z0uO-mrvFH7_J-4i7Fp2Y8-awqHC-NKg29l3OTG7PTDM9uFRma75N2V5rs2JWVj-hBzDXOp34TTdCaRf0giE34aJFv4vg')
AMO_SUBDOMAIN = os.environ.get('AMO_SUBDOMAIN', 'new1734359770')
AMO_PIPELINE_ID = int(os.environ.get('AMO_PIPELINE_ID', '9400586'))
AMO_PHONE_FIELD_ID = int(os.environ.get('AMO_PHONE_FIELD_ID', '783193'))
AMO_COMMENT_FIELD_ID = int(os.environ.get('AMO_COMMENT_FIELD_ID', '783191'))
AMO_DIALOG_FIELD_ID = int(os.environ.get('AMO_DIALOG_FIELD_ID', '783595'))
AMO_AVITO_ID_FIELD_ID = int(os.environ.get('AMO_AVITO_ID_FIELD_ID', '784185')) # Поле для хранения Avito ID

NN_API_URL_BASE = 'https://us1.api.pro-talk.ru/api/v1.0/ask/'
NN_BOT_TOKEN = os.environ.get('NN_BOT_TOKEN', 'rcqmZub9LjRXOYClifrxBRbCcXEcT8zE')
NN_BOT_ID = int(os.environ.get('NN_BOT_ID', '20830'))


# URL для отправки сообщений обратно в Avito через Apimonster
# ЭТОТ URL НУЖНО ВЗЯТЬ ИЗ НАСТРОЕК ВАШЕЙ ПРОМЕЖУТОЧНОЙ ПЛАТФОРМЫ APIMONSTER!
# (Ваш пример: "https://api.apimonster.ru/webhooks/145993/26123/18/c687f06f61f5622678694316c1ed6118/")
AVITO_SEND_MESSAGE_URL = os.environ.get('AVITO_SEND_MESSAGE_URL', 'https://api.apimonster.ru/webhooks/145993/26123/18/c687f06f61f5622678694316c1ed6118/')
# Возможно, Apimonster требует какой-то ключ или ID отправителя в теле или заголовках для этого webhook'а.
# Уточните по документации Apimonster, если простого POST JSON недостаточно.


# Плейсхолдеры для имен
NAME_PLACEHOLDERS = ["Неизвестно", "Клиент Avito", "777"]

# --- Логирование ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Вспомогательная функция для парсинга текстового ответа нейросети ---
def parse_nn_text_response(text_response: str):
    """
    Парсит текстовый ответ от нейросети, извлекая параметры для AmoCRM.
    Ожидаемый формат: "Ответ на вопрос\n[ключ: значение, ключ: значение, ...]"
    """
    params = {}
    nn_text_part = text_response # По умолчанию весь текст - это ответ нейросети

    # Ищем блок с параметрами в квадратных скобках []
    # Используем нежадный поиск (.*?) чтобы не захватить несколько блоков []
    match = re.search(r'\[(.*?)\]', text_response)
    if match:
        params_string = match.group(1) # Извлекаем содержимое скобок
        nn_text_part = text_response[:match.start()].strip() # Текст до скобок - это ответ
        logger.debug(f"Найден блок параметров: '{params_string}'")

        # Разбиваем строку параметров на отдельные пары ключ: значение
        # Использование lookbehind для разделения только по запятой, если перед ней не стоит экранированный символ (более надежно)
        # Или просто split, если уверены, что запятые внутри значений не экранируются
        param_pairs = params_string.split(',') # Простой split
        # param_pairs = re.split(r'(?<!\\),', params_string) # Более продвинутый split

        for pair in param_pairs:
            # Разбиваем каждую пару по первому вхождению ':'
            parts = pair.split(':', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                params[key] = value
                logger.debug(f"Распарсено: {key} = '{value}'")
            else:
                logger.warning(f"Некорректный формат пары ключ:значение в ответе нейросети: '{pair}'. Пропускаем.")
                continue # Пропускаем некорректную пару

    else:
        logger.warning("В ответе нейросети не найден блок параметров в квадратных скобках.")
        # Если блок параметров не найден, возвращаем только текстовый ответ и пустые параметры

    return nn_text_part, params

# --- Вспомогательная функция для вызова нейросети Pro-Talk.ru ---
async def call_pro_talk_api(message_text: str, avito_chat_id: str):
    """
    Отправляет сообщение пользователя на API Pro-Talk.ru, получает текстовый ответ,
    парсит его и возвращает извлеченные данные (текст ответа + параметры для AmoCRM).
    """
    if not NN_BOT_TOKEN:
        logger.error("NN_BOT_TOKEN для Pro-Talk не настроен.")
        raise ValueError("API нейросети не настроен: отсутствует токен.")

    api_url = f"{NN_API_URL_BASE}{NN_BOT_TOKEN}"

    payload = {
        "bot_id": NN_BOT_ID,
        "chat_id": avito_chat_id,
        "message": message_text
    }

    headers = {'Content-Type': 'application/json'}

    logger.info(f"Отправка сообщения в Pro-Talk API для chat_id {avito_chat_id}: '{message_text[:100]}...'")

    raw_response_text = None
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None, # Использует default ThreadPoolExecutor
            lambda: requests.post(api_url, json=payload, headers=headers, timeout=45) # Добавьте таймаут!
        )

        response.raise_for_status() # Выбросит исключение для кодов 4xx/5xx

        raw_response_text = response.text # Получаем сырой текст ответа
        logger.info(f"Получен raw текст ответа от Pro-Talk API для chat_id {avito_chat_id}. Частичный текст: '{raw_response_text[:200]}...'")

        # Парсим текстовый ответ
        nn_text_part, nn_params = parse_nn_text_response(raw_response_text)

        # Преобразуем типы данных из спарсенных параметров
        # Используем .get() с None по умолчанию, чтобы избежать KeyError
        status_id = nn_params.get('status', None)
        if status_id is not None:
            try:
                status_id = int(status_id)
            except (ValueError, TypeError):
                logger.warning(f"Невалидный 'status' ({status_id}) в ответе нейросети для chat_id {avito_chat_id}. Игнорируем.")
                status_id = None # Устанавливаем None, если не удалось преобразовать

        # Возвращаем словарь с всеми данными
        return {
            'nn_text_response': nn_text_part, # Текстовая часть ответа
            'status_id': status_id, # int или None
            'comment': nn_params.get('comment', None), # str или None
            'dialog': nn_params.get('dialog', None), # str или None
            'client_name': nn_params.get('client_name', None), # str или None
            'phone': nn_params.get('phone', None), # str или None
            # Добавьте другие поля, если они появятся в формате ответа NN
        }

    except requests.exceptions.Timeout:
         logger.error(f"Таймаут при запросе к Pro-Talk API для chat_id {avito_chat_id}.")
         raise HTTPException(status_code=504, detail="API нейросети не ответил вовремя (таймаут).")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка HTTP-запроса к Pro-Talk API для chat_id {avito_chat_id}: {e}")
        if e.response is not None:
            logger.error(f"Ответ Pro-Talk API при ошибке ({e.response.status_code}): {e.response.text}")
        raise HTTPException(status_code=500, detail=f"Ошибка при обращении к API нейросети: {e}")
    except ValueError as e: # Ошибки при парсинге или обработке данных из ответа
         logger.error(f"Ошибка парсинга или обработки данных из ответа Pro-Talk API для chat_id {avito_chat_id}: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Ошибка обработки ответа нейросети: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при работе с Pro-Talk API для chat_id {avito_chat_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при работе с API нейросети: {e}")


# --- НОВАЯ ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ОТПРАВКИ СООБЩЕНИЯ В АВИТО ЧЕРЕЗ APIMONSTER ---

async def send_message_to_apimonster(avito_user_id: str, avito_chat_id: str, message_text: str):
    """
    Отправляет сообщение в чат Avito через API Apimonster.

    Args:
        avito_user_id (str): ID пользователя Avito.
        avito_chat_id (str): ID чата Avito, куда отправить сообщение.
        message_text (str): Текст сообщения для отправки.

    Raises:
        HTTPException: Если произошла ошибка при отправке.
    """
    if not AVITO_SEND_MESSAGE_URL or AVITO_SEND_MESSAGE_URL == 'YOUR_APIMONSTER_SEND_MESSAGE_URL':
        logger.error("AVITO_SEND_MESSAGE_URL для отправки сообщений через Apimonster не настроен.")
        # Это критическая ошибка для отправки ответа ботом.
        raise ValueError("URL платформы для отправки сообщений не настроен.")
    if not avito_user_id or not avito_chat_id or not message_text or not message_text.strip():
        logger.warning(f"Пропуск отправки сообщения через Apimonster: Неполные данные (user_id: {avito_user_id}, chat_id: {avito_chat_id}, text: '{message_text[:50]}...').")
        # Не бросаем ошибку, просто логируем, т.к. возможно, просто не было ответа бота
        return # Ничего не отправляем

    # Формирование тела запроса для Apimonster по предоставленному формату
    payload = {
        "userid": avito_user_id,
        "chatid": avito_chat_id,
        "text": message_text
    }

    headers = {
        'Content-Type': 'application/json; charset=UTF-8'
        # Возможно, здесь нужен заголовок авторизации или другой параметр для Apimonster
    }

    logger.info(f"Попытка отправить сообщение в Avito чат {avito_chat_id} (user: {avito_user_id}) через Apimonster. Текст: '{message_text[:100]}...'")

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: requests.post(AVITO_SEND_MESSAGE_URL, json=payload, headers=headers, timeout=30) # Таймаут!
        )

        response.raise_for_status() # Выбросит исключение для 4xx/5xx

        # Apimonster, возможно, возвращает какой-то ответ при успехе
        logger.info(f"Сообщение в Avito чат {avito_chat_id} успешно отправлено через Apimonster. Статус: {response.status_code}. Ответ: {response.text[:100]}")

    except requests.exceptions.Timeout:
         logger.error(f"Таймаут при отправке сообщения в Avito чат {avito_chat_id} через Apimonster.")
         # Бросаем HTTPException, т.к. отправка ответа клиенту - важный шаг.
         raise HTTPException(status_code=504, detail="Платформа для отправки сообщений не ответила вовремя.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка HTTP-запроса при отправке сообщения в Avito чат {avito_chat_id}: {e}")
        if e.response is not None:
            logger.error(f"Ответ Apimonster при ошибке ({e.response.status_code}): {e.response.text}")
        # Бросаем HTTPException
        raise HTTPException(status_code=500, detail=f"Ошибка при отправке сообщения через платформу: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке сообщения в Avito чат {avito_chat_id}: {e}", exc_info=True)
        # Бросаем HTTPException
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при отправке сообщения через платформу: {e}")


# --- Вспомогательные функции для работы с AmoCRM ---
# (Без существенных изменений, кроме исправления type hints)

def _get_amo_headers():
    if not AMO_ACCESS_TOKEN:
        logger.error("AMO_ACCESS_TOKEN не настроен.")
        raise ValueError("AMO_ACCESS_TOKEN не настроен.")
    return {
        "Authorization": f"Bearer {AMO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def _amo_request(method, endpoint, **kwargs):
    if not AMO_SUBDOMAIN:
        logger.error("AMO_SUBDOMAIN не настроен.")
        raise ValueError("AMO_SUBDOMAIN не настроен.")

    url = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4{endpoint}"
    headers = _get_amo_headers()
    params_log = kwargs.get('params', {})
    # Маскируем тело запроса в логах, т.к. может содержать чувствительные данные
    json_log_summary = list(kwargs.get('json', {}).keys()) # Логируем только ключи

    logger.debug(f"AMO Request: {method.upper()} {url}, Params: {params_log}, Body keys: {json_log_summary}")

    try:
        response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        response.raise_for_status()

        if response.status_code == 204:
            logger.debug(f"AMO Response (204 No Content): {method.upper()} {url}")
            return None

        response_json = response.json()
        logger.debug(f"AMO Response ({response.status_code}): {method.upper()} {url}, Body keys: {list(response_json.keys())}")
        return response_json

    except requests.exceptions.Timeout:
         logger.error(f"Таймаут при запросе к AmoCRM ({method.upper()} {url}).")
         raise ConnectionError(f"Таймаут при обращении к AmoCRM ({method.upper()} {url}).") from None
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса к AmoCRM ({method.upper()} {url}): {e}")
        if e.response is not None:
             logger.error(f"Ответ AmoCRM при ошибке ({e.response.status_code}): {e.response.text}")
             if e.response.status_code == 404:
                 raise ConnectionError(f"AmoCRM Not Found (404): {method.upper()} {url}") from e
             if e.response.status_code == 400:
                  logger.error(f"AmoCRM Bad Request (400): {e.response.text}")
                  raise ValueError(f"Некорректный запрос к AmoCRM (400): {e.response.text}") from e
        raise ConnectionError(f"Ошибка при обращении к AmoCRM ({method.upper()} {url}): {e}") from e
    except ValueError as e:
         logger.error(f"Ошибка парсинга JSON от AmoCRM: {e}")
         raise ValueError(f"Ошибка парсинга ответа AmoCRM: {e}") from e
    except Exception as e:
        logger.error(f"Неожиданная ошибка при работе с AmoCRM: {e}", exc_info=True)
        raise RuntimeError(f"Неожиданная ошибка AmoCRM: {e}") from e


def find_lead_by_avito_id(avito_id: str | None):
    """
    Ищет сделку по сохраненному ID чата/пользователя Avito в кастомном поле.
    Использует Avito ID (предполагается, что это chat_id или user_id) для поиска.
    Возвращает ID сделки или None, если не найдено или ошибка.
    """
    if not avito_id or not AMO_AVITO_ID_FIELD_ID:
         if not avito_id: logger.warning("find_lead_by_avito_id вызван с пустым Avito ID.")
         if not AMO_AVITO_ID_FIELD_ID: logger.error("AMO_AVITO_ID_FIELD_ID не настроен. Поиск сделки по Avito ID невозможен.")
         return None

    endpoint = "/leads"
    # Параметры фильтрации по кастомному полю (актуально для AmoCRM API v4)
    # Ищем по точному значению поля, где хранится Avito ID
    params = {
        f'filter[custom_fields_values][{AMO_AVITO_ID_FIELD_ID}][values][0][value]': avito_id,
        'order[created_at]': 'desc', # Ищем последние созданные/обновленные на всякий случай
        'limit': 1 # Нам нужна только одна сделка
    }
    # Можно добавить фильтр по воронке, если Avito-сделки всегда в одной воронке
    if AMO_PIPELINE_ID and AMO_PIPELINE_ID > 0:
         params[f'filter[pipeline_id]'] = AMO_PIPELINE_ID


    logger.debug(f"Поиск сделки по Avito ID {avito_id} в поле {AMO_AVITO_ID_FIELD_ID}")

    try:
        response_data = _amo_request('get', endpoint, params=params)

        if response_data and '_embedded' in response_data and 'leads' in response_data['_embedded']:
            leads = response_data['_embedded']['leads']
            if leads:
                found_lead_id = leads[0]['id']
                logger.info(f"Найдена существующая сделка {found_lead_id} для Avito ID {avito_id}.")
                return found_lead_id
            else:
                logger.info(f"Сделка для Avito ID {avito_id} не найдена в AmoCRM по полю {AMO_AVITO_ID_FIELD_ID}.")
                return None
        else:
             if response_data is not None:
                 logger.warning(f"Поиск сделки по Avito ID {avito_id} вернул unexpected формат ответа: {response_data}")
             return None

    except ConnectionError as e:
         # Пробрасываем ConnectionError из _amo_request, если это не 404.
         # Если это 404 (или другая ошибка, которую мы решили игнорировать для поиска), возвращаем None.
         if "404 Client Error" not in str(e) and "AmoCRM Not Found (404)" not in str(e):
             logger.error(f"Ошибка при поиске сделки по Avito ID {avito_id} из-за проблемы с AmoCRM API: {e}")
             # Можно пробросить исключение дальше, если ошибка AmoCRM должна прерывать процесс
             # raise
         return None # Считаем, что сделка не найдена из-за ошибки


def create_amo_lead(name: str | None, status_id: int, phone: str | None, comment: str | None, dialog_text: str | None, avito_id: str):
    """Создает новую сделку в AmoCRM с начальными данными."""
    if not AMO_PIPELINE_ID or not AMO_AVITO_ID_FIELD_ID:
         logger.error("Константы AmoCRM (PIPELINE_ID или AVITO_ID_FIELD_ID) не настроены для создания сделки.")
         raise ValueError("Конфигурация AmoCRM неполная.")

    endpoint = "/leads"
    custom_fields_values = []

    if AMO_PHONE_FIELD_ID and phone and phone.lower() != "номер не известен":
        custom_fields_values.append({
            "field_id": AMO_PHONE_FIELD_ID,
            "values": [{"value": phone}]
        })

    if AMO_COMMENT_FIELD_ID and comment:
        custom_fields_values.append({
            "field_id": AMO_COMMENT_FIELD_ID,
            "values": [{"value": comment}]
        })

    # Добавляем диалог при создании, если поле настроено
    if AMO_DIALOG_FIELD_ID and dialog_text:
         custom_fields_values.append({
             "field_id": AMO_DIALOG_FIELD_ID,
             "values": [{"value": dialog_text}]
         })

    # Обязательно сохраняем Avito ID в кастомном поле для последующего поиска
    if AMO_AVITO_ID_FIELD_ID and avito_id:
         custom_fields_values.append({
             "field_id": AMO_AVITO_ID_FIELD_ID,
             "values": [{"value": avito_id}]
         })
    else:
         logger.warning("Avito ID или AMO_AVITO_ID_FIELD_ID отсутствует при создании сделки. Повторный поиск будет невозможен.")

    # Определяем имя сделки, как в вашем GAS, добавим Avito ID если имя по умолчанию
    lead_name = name if name and name not in NAME_PLACEHOLDERS and name.strip() else f"Заявка Avito ({avito_id})"

    data = [{
        "name": lead_name,
        "pipeline_id": AMO_PIPELINE_ID,
        "status_id": status_id,
        "custom_fields_values": custom_fields_values if custom_fields_values else None
    }]

    logger.info(f"Попытка создания новой сделки для Avito ID {avito_id} с именем '{lead_name}' и статусом {status_id}.")

    try:
        response_data = _amo_request('post', endpoint, json=data)
        if response_data and '_embedded' in response_data and 'leads' in response_data['_embedded']:
            new_lead = response_data['_embedded']['leads'][0]
            logger.info(f"Сделка успешно создана. ID: {new_lead['id']} для Avito ID {avito_id}.")
            return new_lead['id']
        else:
             logger.error(f"Ошибка создания сделки: Не удалось получить ID из ответа. Ответ: {response_data}. Avito ID: {avito_id}")
             raise RuntimeError("Не удалось получить ID созданной сделки из ответа AmoCRM.")

    except Exception as e:
        logger.error(f"Исключение при создании сделки для Avito ID {avito_id}: {e}", exc_info=True)
        raise


def update_amo_lead(lead_id: int, status_id: int | None, phone: str | None, comment: str | None, dialog_text: str | None, client_name: str | None, avito_id: str):
    """Обновляет существующую сделку в AmoCRM."""
    if not lead_id:
        logger.warning("Попытка обновить сделку с пустым lead_id.")
        return

    endpoint = f"/leads/{lead_id}"
    update_data = {}
    custom_fields_values = []

    if status_id is not None:
        update_data['status_id'] = status_id
        logger.debug(f"Обновляем статус сделки {lead_id} на {status_id}.")

    if client_name is not None and client_name not in NAME_PLACEHOLDERS and client_name.strip():
         update_data['name'] = client_name
         logger.debug(f"Обновляем имя сделки {lead_id} на: '{client_name}'")
    elif client_name is not None:
         logger.debug(f"Имя '{client_name}' является плейсхолдером, имя сделки {lead_id} не обновляется.")


    if AMO_PHONE_FIELD_ID and phone is not None and phone.lower() != "номер не известен":
         custom_fields_values.append({
             "field_id": AMO_PHONE_FIELD_ID,
             "values": [{"value": phone}]
         })
         logger.debug(f"Добавляем/обновляем телефон для сделки {lead_id}: {phone}")

    if AMO_COMMENT_FIELD_ID and comment is not None:
        custom_fields_values.append({
            "field_id": AMO_COMMENT_FIELD_ID,
            "values": [{"value": comment}]
        })
        logger.debug(f"Добавляем/обновляем комментарий для сделки {lead_id}")

    if AMO_DIALOG_FIELD_ID and dialog_text is not None:
        custom_fields_values.append({
            "field_id": AMO_DIALOG_FIELD_ID,
            "values": [{"value": dialog_text}]
        })
        logger.debug(f"Добавляем/обновляем диалог для сделки {lead_id}")

    # Обновляем Avito ID на всякий случай, если поле настроено
    if AMO_AVITO_ID_FIELD_ID and avito_id:
        custom_fields_values.append({
            "field_id": AMO_AVITO_ID_FIELD_ID,
            "values": [{"value": avito_id}]
        })
        logger.debug(f"Обновляем/подтверждаем Avito ID {avito_id} для сделки {lead_id}")


    if custom_fields_values:
        update_data['custom_fields_values'] = custom_fields_values

    if update_data:
        data_payload = [update_data]
        logger.info(f"Попытка обновления сделки {lead_id}. Данные: {update_data}")
        try:
            _amo_request('patch', endpoint, json=data_payload)
            logger.info(f"Сделка {lead_id} успешно обновлена.")
        except Exception as e:
            logger.error(f"Ошибка при обновлении сделки {lead_id}: {e}", exc_info=True)
    else:
        logger.info(f"Нет данных для обновления сделки {lead_id} (статус, имя, поля). Пропуск PATCH запроса.")


def add_note_to_lead(lead_id: int, note_text: str):
    if not lead_id or not note_text or not note_text.strip():
        logger.warning(f"Пропуск добавления заметки: lead_id пустой ({lead_id}) или текст заметки пустой.")
        return

    endpoint = f"/leads/{lead_id}/notes"
    note_type = 'common'

    data = [{
        "note_type": note_type,
        "params": {
            "text": note_text
        }
    }]

    logger.debug(f"Попытка добавить заметку к сделке {lead_id}. Текст: '{note_text[:100]}...'")

    try:
        _amo_request('post', endpoint, json=data)
        logger.info(f"Заметка успешно добавлена к сделке {lead_id}.")
    except Exception as e:
        logger.error(f"Ошибка при добавлении заметки к сделке {lead_id}: {e}", exc_info=True)


# --- Главный обработчик Webhook ---

# Уточните реальные имена параметров, которые присылает промежуточная платформа!
# Используем имена из вашего самого первого описания, если они отличаются от имен для отправки.
AVITO_USER_ID_PARAM_IN = 'param2'
AVITO_CHAT_ID_PARAM_IN = 'param1'
AVITO_MESSAGE_PARAM_IN = 'param3' # Или 'message' как в ProTalk/Apimonster? Уточните!


@app.post("/")
async def webhook_receiver(request: Request):
    avito_chat_id_in = "N/A"
    avito_user_id_in = "N/A"
    message_text_in = "" # Добавим для логирования

    try:
        if request.headers.get('content-type') != 'application/json':
             logger.error(f"Получен webhook с не-JSON Content-Type: {request.headers.get('content-type')}")
             raise HTTPException(status_code=415, detail="Ожидается Content-Type: application/json")

        incoming_data = await request.json()
        logger.info(f"🚀 Пришел входящий webhook. Частичные данные: {str(incoming_data)[:300]}...")

        # --- 1. Извлечение данных из входящего webhook ---
        try:
            # Получаем данные, убеждаемся, что они строки
            avito_user_id_in = str(incoming_data.get(AVITO_USER_ID_PARAM_IN)) if incoming_data.get(AVITO_USER_ID_PARAM_IN) is not None else None
            avito_chat_id_in = str(incoming_data.get(AVITO_CHAT_ID_PARAM_IN)) if incoming_data.get(AVITO_CHAT_ID_PARAM_IN) is not None else None
            message_text_in = incoming_data.get(AVITO_MESSAGE_PARAM_IN)

            if not avito_chat_id_in or not avito_user_id_in or not message_text_in or not message_text_in.strip():
                missing_params = [p for p, val in {AVITO_USER_ID_PARAM_IN: avito_user_id_in, AVITO_CHAT_ID_PARAM_IN: avito_chat_id_in, AVITO_MESSAGE_PARAM_IN: message_text_in}.items() if val is None or (isinstance(val, str) and not val.strip())]
                logger.error(f"Входящий webhook не содержит все обязательные параметры. Отсутствуют: {', '.join(missing_params)}. Данные: {incoming_data}")
                raise HTTPException(status_code=400, detail=f"Отсутствуют обязательные параметры в webhook: {', '.join(missing_params)}")

            logger.info(f"Извлечены данные из входящего webhook: user_id={avito_user_id_in}, chat_id={avito_chat_id_in}, message_text='{message_text_in[:100]}...'")

        except HTTPException:
             raise
        except Exception as e:
             logger.error(f"Ошибка при извлечении параметров из входящего webhook: {e}", exc_info=True)
             raise HTTPException(status_code=400, detail=f"Ошибка при парсинге входящих данных: {e}")


        # --- 2. Вызов API нейросети Pro-Talk.ru и парсинг ответа ---
        # call_pro_talk_api вернет словарь с извлеченными данными
        nn_processed_data = await call_pro_talk_api(message_text_in, avito_chat_id_in)

        # Извлекаем данные из результата парсинга
        nn_text_response = nn_processed_data.get('nn_text_response', '') # Текстовая часть ответа нейросети (может быть пустой строкой)
        status_id_nn = nn_processed_data.get('status_id') # int или None
        comment_nn = nn_processed_data.get('comment') # str или None
        dialog_nn = nn_processed_data.get('dialog') # str или None
        client_name_nn = nn_processed_data.get('client_name') # str или None
        phone_nn = nn_processed_data.get('phone') # str или None

        logger.info(f"Данные от нейросети для chat_id {avito_chat_id_in}: Статус: {status_id_nn}, Имя: {client_name_nn}, Тел: {phone_nn}, Коммент: '{str(comment_nn)[:50]}...', Диалог: '{str(dialog_nn)[:50]}...', Текст ответа: '{nn_text_response[:50]}...'")


        # --- 3. ОТПРАВКА ОТВЕТА БОТА ОБРАТНО В АВИТО ЧЕРЕЗ APIMONSTER ---
        # Этот шаг выполняется сразу после получения ответа нейросети.
        try:
            # Отправляем только текст ответа нейросети, если он не пустой
            if nn_text_response and nn_text_response.strip():
                 # Используем user_id и chat_id из ВХОДЯЩЕГО webhook'а
                 await send_message_to_apimonster(avito_user_id_in, avito_chat_id_in, nn_text_response)
                 logger.info(f"Ответ бота ('{nn_text_response[:50]}...') успешно отправлен обратно в чат Avito {avito_chat_id_in}.")
            else:
                 logger.warning(f"Текстовый ответ нейросети пуст или отсутствует для chat_id {avito_chat_id_in}. Пропускаем отправку сообщения в Avito.")

        except HTTPException as e:
            # Если произошла ошибка при отправке сообщения через Apimonster,
            # пробрасываем ее дальше. Залогировано уже внутри send_message_to_apimonster.
            raise e
        except Exception as e:
             logger.error(f"Неожиданная ошибка при попытке отправить ответ бота в Avito для chat_id {avito_chat_id_in}: {e}", exc_info=True)
             # Можно обернуть в HTTPException, если это критично
             raise HTTPException(status_code=500, detail=f"Ошибка при отправке сообщения в Avito: {e}")


        # --- 4. Работа с AmoCRM (поиск, создание, обновление) ---
        lead_id_for_response = None
        operation_status = ""

        # Используем ID чата Avito как идентификатор для AmoCRM
        # (или user_id, если одна сделка = один пользователь, а не один чат)
        avito_crm_id_to_search = avito_chat_id_in # Решите, что использовать: chat_id или user_id

        # Поиск существующей сделки в AmoCRM по сохраненному Avito ID (chat_id или user_id)
        current_lead_id = find_lead_by_avito_id(avito_crm_id_to_search)


        if current_lead_id:
            # --- СДЕЛКА СУЩЕСТВУЕТ -> ОБНОВЛЯЕМ ---
            lead_id_for_response = current_lead_id
            operation_status = "обновлено"
            logger.info(f"Обновление сделки {current_lead_id} для Avito ID {avito_crm_id_to_search}.")

            # Обновляем сделку данными от нейросети
            update_amo_lead(
                lead_id=current_lead_id,
                status_id=status_id_nn, # Передаем статус (может быть None)
                phone=phone_nn, # Передаем телефон (может быть None)
                comment=comment_nn, # Передаем комментарий (может быть None)
                dialog_text=dialog_nn, # Передаем полный диалог (может быть None) - ВАЖНО: поле должно быть ТЕКСТОВЫМ и достаточно длинным!
                client_name=client_name_nn, # Передаем имя клиента (может быть None)
                avito_id=avito_crm_id_to_search # Передаем Avito ID на всякий случай
            )

            # Добавляем оригинальное сообщение пользователя как заметку в ленту сделки.
            # Текст ответа бота уже отправлен клиенту и записан в поле "Диалог".
            # В заметку можно добавить только входящее сообщение.
            note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
            add_note_to_lead(current_lead_id, note_content)


        else:
            # --- СДЕЛКА НЕ НАЙДЕНА -> СОЗДАЕМ НОВУЮ ---
            logger.info(f"Сделка для Avito ID {avito_crm_id_to_search} не найдена. Создание новой.")
            operation_status = "создано"

            # Создаем новую сделку с данными от нейросети и Avito ID
            try:
                 new_lead_id = create_amo_lead(
                     name=client_name_nn, # Имя от нейросети или по умолчанию
                     status_id=status_id_nn if status_id_nn is not None else (AMO_PIPELINE_ID or 0), # Статус от нейросети или дефолт (0 - Неразобранное/первичный контакт)
                     phone=phone_nn, # Телефон от нейросети или None
                     comment=comment_nn, # Комментарий от нейросети или None
                     dialog_text=dialog_nn, # Диалог от нейросети или None
                     avito_id=avito_crm_id_to_search # Обязательно сохраняем Avito ID
                 )
                 lead_id_for_response = new_lead_id

                 logger.info(f"Создана новая сделка {new_lead_id} и связана с Avito ID {avito_crm_id_to_search}.")

                 # Добавляем оригинальное сообщение пользователя как заметку к новой сделке
                 note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
                 add_note_to_lead(new_lead_id, note_content)

            except Exception as e:
                 logger.error(f"Ошибка при создании сделки для Avito ID {avito_crm_id_to_search}: {e}", exc_info=True)
                 raise HTTPException(status_code=500, detail=f"Ошибка при создании сделки в AmoCRM: {e}")


        logger.info(f"Обработка Avito ID {avito_crm_id_to_search} завершена. ID сделки: {lead_id_for_response}, Статус: {operation_status}")

        # --- 5. Возвращаем успешный ответ webhook'у ---
        # Важно: Если отправка сообщения в Apimonster завершилась ошибкой (HTTPException),
        # этот блок не выполнится, и ошибка будет проброшена на уровень выше,
        # вернув соответствующий HTTP статус вызывающей стороне.
        # Если отправка в Apimonster прошла успешно, возвращаем OK.
        return {"status": "ok", "result": operation_status, "leadId": lead_id_for_response}

    except HTTPException as http_exc:
         # Перехватываем HTTPException, которые мы сами бросали (в т.ч. из send_message_to_apimonster)
         logger.error(f"Обработка запроса для chat_id {avito_chat_id_in} user_id {avito_user_id_in} (msg='{message_text_in[:50]}...') прервана с HTTPException: {http_exc.status_code} - {http_exc.detail}")
         return {"status": "error", "message": http_exc.detail}, http_exc.status_code
    except Exception as e:
        # Критическая необработанная ошибка
        logger.critical(f"Критическая необработанная ошибка в webhook_receiver для chat_id {avito_chat_id_in} user_id {avito_user_id_in} (msg='{message_text_in[:50]}...'): {e}", exc_info=True)
        return {"status": "error", "message": f"Внутренняя ошибка сервера: {e}"}, 500


@app.get("/")
async def root():
    """Простой эндпоинт для проверки, что сервер запущен."""
    return {"message": "Сервер интеграции Avito-ProTalk-AmoCRM работает!"}

# --- Запуск сервера (для разработки) ---
# Для продакшена используйте gunicorn или uvicorn напрямую:
# uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
