import os
import logging
import asyncio
import json
import re
import requests

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks # Импортируем BackgroundTasks
from starlette.datastructures import FormData # Импортируем FormData


# --- Конфигурация (Используйте переменные окружения!) ---
# Установите эти переменные в вашей среде выполнения, не хардкодьте секреты в код.
# Если переменная окружения не установлена, будут использоваться значения по умолчанию (ваши примеры).

# Настройки AmoCRM
AMO_ACCESS_TOKEN = os.environ.get('AMO_ACCESS_TOKEN', 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjU4OWJiODVmNDYyYjZkNWIwOGUwNzRmYTkyZDUzOWY4MjFkMzAzZTE3YTMwNWY4MTc1NTVmZTY5YWNiYTk1NjcwMjAxMzA3ZDk4MDE0NTg4In0.eyJhdWQiOiIxM2QwNGJmZS0wM2I5LTRhZWYtYTgyNS0xZGE4YTc3OGM0ZTMiLCJqdGkiOiI1ODliYjg1ZjQ2MmI2ZDViMDhlMDc0ZmE5MmQ1MzlmODIxZDMwM2UxN2EzMDVmODE3NTU1ZmU2OWFjYmE5NTY3MDIwMTMwN2Q5ODAxNDU4OCIsImlhdCI6MTc0NDYzMDg3OCwibmJmIjoxNzQ0NjMwODc4LCJleHAiOjE4MzkxMTA0MDAsInN1YiI6IjExOTA1OTE4IiwiZ3JhbnRfdHlwZSI6IiIsImFjY291bnRfaWQiOjMyMTI2NDkwLCJiYXNlX2RvbWFpbiI6ImFtb2NybS5ydSIsInZlcnNpb24iOjIsInNjb3BlcyI6WyJjcm0iLCJmaWxlcyIsImZpbGVzX2RlbGV0ZSIsIm5vdGlmaWNhdGlvbnMiLCJwdXNoX25vdGlmaWNhdGlvbnMiXSwiaGFzaF91dWlkIjoiZTlmYTAwOWEtNjVhOC00ZjY2LTg4Y2YtODFlZDI1NjI5YTYwIiwiYXBpX2RvbWFpbiI6ImFwaS1iLmFtb2NybS5ydSJ9.jMJy8xHeePGzjW2jrqEaB2r2vYPUjGAcTsGzSiL0wR94SqKFWmAdP6mHkiC09UN3zvRD9xa_dgI-J6w0GrAEucg3d-cBfc7Q3Vx3pzzHQitoefVv55KH9J3TyRV8y0cPfdnbNBmxSDjoHIEWAt0-e-0iITxTSI7iqvcNg-o8yRqJNi6-s3WGOI7QN87_UQmUu2MNL1tXq7MSrp-mqExl5rP0VU_1mHv62-PDXTmU4K64bG02z331yobU1Z0uO-mrvFH7_J-4i7Fp2Y8-awqHC-NKg29l3OTG7PTDM9uFRma75N2V5rs2JWVj-hBzDXOp34TTdCaRf0giE34aJFv4vg')
AMO_SUBDOMAIN = os.environ.get('AMO_SUBDOMAIN', 'new1734359770')
AMO_PIPELINE_ID = int(os.environ.get('AMO_PIPELINE_ID', '9400586')) # ID вашей воронки. 0 или None, если не используется для поиска/создания.
AMO_PHONE_FIELD_ID = int(os.environ.get('AMO_PHONE_FIELD_ID', '783193')) # ID кастомного поля для телефона
AMO_COMMENT_FIELD_ID = int(os.environ.get('AMO_COMMENT_FIELD_ID', '783191')) # ID кастомного поля для комментария
AMO_DIALOG_FIELD_ID = int(os.environ.get('AMO_DIALOG_FIELD_ID', '783595')) # ID кастомного поля для диалога (для полного диалога)
# ID кастомного поля для хранения ID чата/пользователя Avito. Текстовый тип поля!
AMO_AVITO_ID_FIELD_ID = int(os.environ.get('AMO_AVITO_ID_FIELD_ID', '784203')) # Используем ID из вашего рабочего кода!

# Настройки API нейросети Pro-Talk.ru
NN_API_URL_BASE = os.environ.get('NN_API_URL_BASE', 'https://us1.api.pro-talk.ru/api/v1.0/ask/')
NN_BOT_TOKEN = os.environ.get('NN_BOT_TOKEN', 'rcqmZub9LjRXOYClifrxBRbCcXEcT8zE') # Ваш bot_token для Pro-Talk
NN_BOT_ID = int(os.environ.get('NN_BOT_ID', '20830')) # Ваш bot_id для Pro-Talk


AVITO_SEND_MESSAGE_URL = os.environ.get('AVITO_SEND_MESSAGE_URL', 'https://api.apimonster.ru/webhooks/145993/26123/18/c687f06f61f5622678694316c1ed6118/')

# Плейсхолдеры для имен, которые не нужно устанавливать как имя сделки в AmoCRM
NAME_PLACEHOLDERS = ["Неизвестно", "Клиент Avito", "777"]

# --- Логирование ---
# Уровень логирования можно менять (DEBUG, INFO, WARNING, ERROR, CRITICAL)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()


# --- Вспомогательная функция для парсинга текстовой части ответа нейросети ---
def parse_nn_text_response(text_response: str | None) -> tuple[str, dict]:
    """
    Парсит текстовую часть ответа (из поля 'done'), извлекая параметры для AmoCRM.
    Ожидаемый формат: "Ответ на вопрос\n[ключ: значение, ключ: значение, ...]"
    Возвращает: (текст_ответа_без_скобок, словарь_параметров)
    """
    params = {}
    # Убедимся, что входные данные - строка, иначе вернем пустые данные
    if not isinstance(text_response, str):
        logger.debug(f"parse_nn: получил не строку: {text_response}. Возвращаем пустые данные.")
        return "", {}

    nn_text_part = text_response # По умолчанию весь текст - это ответ нейросети

    # Ищем блок с параметрами в квадратных скобках [] в конце строки (опционально с пробелами после)
    match = re.search(r'\[(.*?)\]\s*$', text_response)

    if match:
        params_string = match.group(1).strip() # Извлекаем содержимое скобок и убираем пробелы по краям
        # Обрезаем строку ДО НАЧАЛА совпадения и убираем концевые пробелы/переносы
        nn_text_part = text_response[:match.start()].rstrip()
        logger.debug(f"parse_nn: Найден блок параметров: '{params_string[:100]}...'")
        logger.debug(f"parse_nn: Текст до параметров: '{nn_text_part[:100]}...'")

        # Разбиваем строку параметров на отдельные пары ключ: значение
        # Используем regex split по ', ' за которым следует типичное имя ключа (буквы/цифры/подчеркивание + \w*) и ':'
        # Используем lookahead `(?=...)` чтобы не включать следующий ключ в разделитель
        # Учитываем, что имена ключей могут содержать только буквы и подчеркивания
        # pattern = r',\s*(?=[a-zA-Z_]\w*\s*:)' # Старый паттерн, может не сработать с ключами типа client_name
        # Новый паттерн: `,` за которой следует пробелы, а затем слово из букв, цифр, подчеркиваний, а затем `:`.
        # Ищем только те совпадения, которые находятся перед словом_с_двоеточием.
        # Более простой и часто рабочий вариант: разбить по ', ' и надеяться, что ': ' разделяет ключ и значение.
        # Если ключи всегда выглядят как слово_слово:
        # param_list = re.split(r',\s*(?=\w+:\s*|\w+_\w+:\s*)', params_string) # Один из вариантов
        # Используем более общий паттерн для ключей
        param_list = re.split(r',\s*(?=[a-zA-Z_][a-zA-Z0-9_]*\s*:)', params_string)


        logger.debug(f"parse_nn: Строка параметров разбита на части ({len(param_list)}): {param_list}")

        for item in param_list:
            # Для каждой части ищем первое двоеточие для разделения на ключ и значение
            parts = item.split(':', 1)
            if len(parts) == 2:
                key = parts[0].strip()
                value = parts[1].strip()
                params[key] = value
                logger.debug(f"parse_nn: Распарсено: {key} = '{value[:50]}...'")
            else:
                # Если часть не содержит двоеточия (после разбиения), это некорректная часть
                logger.warning(f"parse_nn: Некорректная часть после re.split (нет ':') в ответе нейросети: '{item}'. Пропускаем.")
                continue # Пропускаем некорректную часть

    else:
        logger.debug("parse_nn: В ответе нейросети не найден блок параметров в квадратных скобках [].")
        # Если блок параметров не найден, nn_text_part остается исходным текстом, params пуст.

    return nn_text_part, params

# --- Вспомогательная функция для вызова нейросети Pro-Talk.ru ---
async def call_pro_talk_api(message_text: str, avito_chat_id: str) -> dict:
    """
    Отправляет сообщение пользователя на API Pro-Talk.ru, получает JSON ответ,
    извлекает строку из поля 'done', парсит ее и возвращает извлеченные данные
    (текст ответа БЕЗ параметров + словарь параметров для AmoCRM).
    """
    if not NN_BOT_TOKEN or NN_BOT_TOKEN == 'YOUR_PROTALK_BOT_TOKEN':
        logger.error("NN_BOT_TOKEN для Pro-Talk не настроен.")
        raise ValueError("API нейросети не настроен: отсутствует токен.")
    if not NN_BOT_ID or NN_BOT_ID == 0:
        logger.error("NN_BOT_ID для Pro-Talk не настроен.")
        raise ValueError("API нейросети не настроен: отсутствует Bot ID.")


    api_url = f"{NN_API_URL_BASE}{NN_BOT_TOKEN}"

    payload = {
        "bot_id": NN_BOT_ID,
        "chat_id": avito_chat_id, # Используем ID чата Avito
        "message": message_text # Используем текст сообщения пользователя
    }

    headers = {'Content-Type': 'application/json'}

    logger.info(f"Pro-Talk: Отправка сообщения для chat_id {avito_chat_id}: '{message_text[:100]}...'")
    logger.debug(f"Pro-Talk URL: {api_url}")
    # logger.debug(f"Pro-Talk Payload: {payload}") # Payload может содержать чувствительные данные


    text_with_params_from_done = "" # Переменная для хранения содержимого 'done'

    try:
        loop = asyncio.get_event_loop()
        # Выполняем блокирующий requests.post в отдельном потоке
        response = await loop.run_in_executor(
            None, # Использует default ThreadPoolExecutor
            lambda: requests.post(api_url, json=payload, headers=headers, timeout=45) # Добавьте таймаут!
        )

        response.raise_for_status() # Выбросит исключение для кодов 4xx/5xx

        # --- Парсим ответ как JSON и извлекаем поле 'done' ---
        try:
            response_json = response.json()
            logger.info(f"Pro-Talk: Получен JSON ответ. Статус {response.status_code}. Частичный ответ: {str(response_json)[:200]}...")

            # Получаем строку из поля 'done'. Это строка, которая может содержать [параметры]
            text_with_params_from_done = response_json.get('done', '') # Используем get с пустой строкой по умолчанию

            # Получаем данные об использовании (опционально)
            usage_data = response_json.get('usage', None)
            if usage_data:
                 logger.debug(f"Pro-Talk: Данные об использовании: {usage_data}")

        except json.JSONDecodeError:
            raw_response_text = response.text # Сохраняем сырой текст для лога ошибки
            logger.error(f"Pro-Talk: Ответ невалидный JSON для chat_id {avito_chat_id}. Сырой текст: '{raw_response_text[:200]}...'")
            # В случае ошибки парсинга JSON, считаем, что данные от нейросети не получены.
            # Пробрасываем исключение.
            raise ValueError("Ответ API нейросети не является валидным JSON.")

        # --- Передаем строку из 'done' в парсер для извлечения параметров AmoCRM ---
        # parse_nn_text_response теперь получает только строку из поля 'done'
        nn_text_response, nn_params = parse_nn_text_response(text_with_params_from_done)

        # --- Преобразуем типы данных из спарсенных параметров ---
        # Используем .get() с None по умолчанию, чтобы избежать KeyError
        status_id: int | None = None # Инициализируем None
        status_id_str = nn_params.get('status', None)
        if status_id_str is not None:
            try:
                status_id = int(status_id_str)
            except (ValueError, TypeError):
                logger.warning(f"Pro-Talk: Невалидный 'status' ({status_id_str}) в спарсенных параметрах для chat_id {avito_chat_id}. Игнорируем.")
                status_id = None # Устанавливаем None, если не удалось преобразовать

        # Возвращаем словарь с всеми данными
        return {
            'nn_text_response': nn_text_response, # Текстовая часть ответа (без скобок)
            'status_id': status_id, # int или None
            'comment': nn_params.get('comment', None), # str или None
            'dialog': nn_params.get('dialog', None), # str или None
            'client_name': nn_params.get('client_name', None), # str или None
            'phone': nn_params.get('phone', None), # str или None
        }

    except requests.exceptions.Timeout as e:
         logger.error(f"Pro-Talk: Таймаут при запросе для chat_id {avito_chat_id}: {e}")
         raise HTTPException(status_code=504, detail=f"API нейросети не ответил вовремя (таймаут): {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Pro-Talk: Ошибка HTTP-запроса для chat_id {avito_chat_id}: {e}")
        if e.response is not None:
            raw_response_text = e.response.text # Сохраняем сырой текст ошибки
            logger.error(f"Pro-Talk: Ответ при ошибке ({e.response.status_code}): {raw_response_text[:200]}")
        raise HTTPException(status_code=500, detail=f"Ошибка при обращении к API нейросети: {e}")
    except (ValueError, RuntimeError) as e:
        # Ошибки парсинга или другие проблемы после получения JSON
        logger.error(f"Pro-Talk: Ошибка обработки ответа для chat_id {avito_chat_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка обработки ответа нейросети: {e}")
    except Exception as e:
        logger.critical(f"Pro-Talk: Неожиданная КРИТИЧЕСКАЯ ошибка при работе с API для chat_id {avito_chat_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при работе с API нейросети: {e}")


# --- Вспомогательная функция для отправки сообщения в Avito через Apimonster ---

async def send_message_to_apimonster(avito_user_id: str | None, avito_chat_id: str | None, message_text: str | None):
    """
    Отправляет сообщение в чат Avito через API Apimonster.

    Args:
        avito_user_id (str | None): ID пользователя Avito.
        avito_chat_id (str | None): ID чата Avito, куда отправить сообщение.
        message_text (str | None): Текст сообщения для отправки (текстовая часть ответа нейросети).

    Raises:
        HTTPException: Если произошла ошибка при отправке, которая критична для процесса.
                      Если нет текста для отправки, просто завершается.
    """
    # Проверяем наличие URL для отправки
    if not AVITO_SEND_MESSAGE_URL or AVITO_SEND_MESSAGE_URL == 'YOUR_APIMONSTER_SEND_MESSAGE_URL':
        logger.error("Apimonster: URL для отправки сообщений не настроен.")
        # Это критическая ошибка для отправки ответа ботом.
        raise ValueError("Apimonster: URL платформы для отправки сообщений не настроен.")

    # Проверяем наличие данных для отправки сообщения
    # User ID может быть некритичен для отправки, но chat_id и текст обязательны
    if not avito_chat_id or not message_text or not message_text.strip():
        logger.warning(f"Apimonster: Пропуск отправки сообщения: Отсутствует chat_id ({avito_chat_id}) или текст сообщения пуст.")
        return # Ничего не отправляем, это не ошибка, просто нет текста для отправки

    # Формирование тела запроса для Apimonster по предоставленному формату
    # Используем имена параметров из вашего тестового примера отправки: userid, chatid, text
    payload = {
        "userid": avito_user_id if avito_user_id is not None else '', # Отправляем пустую строку, если user_id None
        "chatid": avito_chat_id,
        "text": message_text
    }

    headers = {
        'Content-Type': 'application/json; charset=UTF-8'
        # Возможно, здесь нужен заголовок авторизации или другой параметр для Apimonster
    }

    logger.info(f"Apimonster: Попытка отправить сообщение в чат {avito_chat_id} (user: {avito_user_id}). Текст: '{message_text[:100]}...'")

    try:
        loop = asyncio.get_event_loop()
        # Выполняем блокирующий requests.post в отдельном потоке
        response = await loop.run_in_executor(
            None,
            lambda: requests.post(AVITO_SEND_MESSAGE_URL, json=payload, headers=headers, timeout=30) # Таймаут!
        )

        response.raise_for_status() # Выбросит исключение для 4xx/5xx

        # Apimonster, возможно, возвращает какой-то ответ при успехе
        logger.info(f"Apimonster: Сообщение в чат {avito_chat_id} успешно отправлено. Статус: {response.status_code}. Ответ: {response.text[:100]}")

    except requests.exceptions.Timeout as e:
         logger.error(f"Apimonster: Таймаут при отправке сообщения в чат {avito_chat_id}: {e}")
         raise HTTPException(status_code=504, detail=f"Apimonster не ответил вовремя (таймаут): {e}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Apimonster: Ошибка HTTP-запроса при отправке сообщения в чат {avito_chat_id}: {e}")
        if e.response is not None:
            logger.error(f"Apimonster: Ответ при ошибке ({e.response.status_code}): {e.response.text[:200]}")
        raise HTTPException(status_code=500, detail=f"Apimonster: Ошибка при отправке сообщения: {e}")
    except Exception as e:
        logger.critical(f"Apimonster: Неожиданная КРИТИЧЕСКАЯ ошибка при отправке сообщения в чат {avito_chat_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Apimonster: Неожиданная ошибка при отправке сообщения: {e}")


# --- Вспомогательные функции для работы с AmoCRM ---

def _get_amo_headers():
    """Возвращает стандартные заголовки для запросов к AmoCRM API, включая авторизацию."""
    # Проверяем наличие токена перед использованием
    if not AMO_ACCESS_TOKEN or AMO_ACCESS_TOKEN == 'YOUR_AMO_ACCESS_TOKEN':
        logger.error("AmoCRM API: AMO_ACCESS_TOKEN не настроен.")
        raise ValueError("AmoCRM API: AMO_ACCESS_TOKEN не настроен.")

    return {
        "Authorization": f"Bearer {AMO_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json" # Явно запрашиваем JSON ответ
    }

def _amo_request(method: str, endpoint: str, **kwargs):
    """
    Выполняет запрос к AmoCRM API v4.

    Args:
        method (str): HTTP метод (GET, POST, PATCH, DELETE, PUT).
        endpoint (str): Конечная точка API v4 (например, '/leads', '/leads/notes').
        **kwargs: Дополнительные аргументы для requests.request (params, json, data, files, timeout и т.e.).

    Returns:
        dict | list | None: Распарсенный JSON ответ от AmoCRM, список (для get списка),
                            словарь (для get одной сущности), или None для 204 No Content.

    Raises:
        ValueError: Если не настроен поддомен AmoCRM, токен, или некорректный запрос (400 Bad Request),
                    или ошибка парсинга JSON.
        ConnectionError: При ошибках сети, таймауте, или ошибках 404 Not Found/других запросов (4xx/5xx).
        RuntimeError: Для других неожиданных ошибок.
    """
    if not AMO_SUBDOMAIN or AMO_SUBDOMAIN == 'YOUR_AMO_SUBDOMAIN':
        logger.error("AmoCRM API: AMO_SUBDOMAIN не настроен.")
        raise ValueError("AmoCRM API: AMO_SUBDOMAIN не настроен.")

    # Убеждаемся, что endpoint начинается с '/', если это не полный URL
    if not endpoint.startswith('/') and not endpoint.startswith('http'):
         endpoint = '/' + endpoint

    url = f"https://{AMO_SUBDOMAIN}.amocrm.ru/api/v4{endpoint}"

    # Получаем заголовки, включая авторизацию
    try:
        headers = _get_amo_headers()
    except ValueError as e:
        # Пробрасываем ошибку, если токен не настроен
        raise e

    # --- Подготовка информации для логирования запроса ---
    params_log = kwargs.get('params', {})
    json_log_summary = None

    # Логируем тело запроса, если оно передано в аргументе `json`
    if 'json' in kwargs and kwargs['json'] is not None:
        json_payload = kwargs['json']
        # Если это список (типично для POST/PATCH одной или нескольких сущностей в v4)
        if isinstance(json_payload, list):
            # Логируем ключи первого элемента, если список не пустой и начинается со словаря
            if json_payload and isinstance(json_payload[0], dict):
                 # Показываем, что это список, и ключи первого элемента
                 json_log_summary = f"List[0] keys: {list(json_payload[0].keys())}"
            else:
                 # Логируем тип списка или что он пустой
                 json_log_summary = f"List ({len(json_payload)} items, type: {type(json_payload[0]).__name__ if json_payload else 'empty'})"
        # Если это словарь (менее типично для тела POST/PATCH в v4, но возможно для других эндпоинтов)
        elif isinstance(json_payload, dict):
            json_log_summary = f"Dict keys: {list(json_payload.keys())}"
        else:
            # Для других типов данных в теле
            json_log_summary = f"Payload type: {type(json_payload).__name__}"
    # Логируем тело запроса, если оно передано в аргументе `data` (для форм или другого)
    elif 'data' in kwargs and kwargs['data'] is not None:
         # Data может быть строкой, словарем, байтами и т.д.
         data_payload = kwargs['data']
         if isinstance(data_payload, (dict, list)):
             # Пытаемся кратко представить
              data_log_summary = f"Data type: {type(data_payload).__name__}, keys/len: {list(data_payload.keys()) if isinstance(data_payload, dict) else len(data_payload)}"
         else:
              data_log_summary = f"Data type: {type(data_payload).__name__}, partial: {str(data_payload)[:100]}"
         json_log_summary = data_log_summary # Используем ту же переменную для краткости


    logger.debug(f"AmoCRM API: Request: {method.upper()} {url}")
    logger.debug(f"  Params: {params_log}")
    # Логируем только summary тела, если оно есть
    if json_log_summary is not None:
        logger.debug(f"  Body: {json_log_summary}")


    try:
        # Выполняем сам HTTP запрос
        # Добавляем таймаут по умолчанию, если он не указан в kwargs
        request_timeout = kwargs.pop('timeout', 30)
        # Добавляем headers из kwargs к стандартным заголовкам
        request_headers = headers.copy()
        request_headers.update(kwargs.pop('headers', {}))

        response = requests.request(method, url, headers=request_headers, timeout=request_timeout, **kwargs)

        # Выбрасываем исключение для кодов статуса 4xx/5xx.
        # Если исключение выброшено, остальная часть блока try не выполняется.
        response.raise_for_status()

        # --- Анализ успешного ответа ---
        if response.status_code == 204:
            # 204 No Content означает успешное выполнение без тела ответа (например, при DELETE)
            logger.debug(f"AmoCRM API: Response (204 No Content): {method.upper()} {endpoint}")
            return None # Возвращаем None, т.к. нет тела ответа

        # Если статус не 204, ожидаем тело ответа, которое должно быть JSON.
        # Пытаемся распарсить JSON.
        try:
            response_json = response.json()
            logger.debug(f"AmoCRM API: Response ({response.status_code}): {method.upper()} {endpoint}")
            # logger.debug(f"  Response body (partial): {str(response_json)[:200]}...") # Осторожно, может быть большим

            return response_json # Возвращаем распарсенный JSON

        except json.JSONDecodeError:
             # Если raise_for_status не выбросил ошибку (т.е., статус 2xx или 3xx),
             # но ответ не JSON, это неожиданный формат ответа.
             content_type = response.headers.get('Content-Type', '').lower()
             logger.error(f"AmoCRM API: Ошибка парсинга JSON ответа. Получен статус {response.status_code}, Content-Type: {content_type}. Сырой ответ: {response.text[:200]}...")
             # Пробрасываем исключение, так как ожидался JSON, но не пришел
             raise ValueError(f"AmoCRM API: Ошибка парсинга JSON ответа (статус: {response.status_code}, Content-Type: {content_type}).")


    # --- Обработка исключений ---
    except requests.exceptions.Timeout as e:
         # Специально обрабатываем таймаут
         logger.error(f"AmoCRM API: Таймаут при запросе ({method.upper()} {endpoint}): {e}")
         # Перебрасываем как ConnectionError для единообразия обработки сетевых проблем
         raise ConnectionError(f"AmoCRM API: Таймаут при обращении ({method.upper()} {endpoint}).") from e
    except requests.exceptions.RequestException as e:
        # Обработка других ошибок библиотеки requests (сетевые ошибки, ошибки статуса 4xx/5xx)
        logger.error(f"AmoCRM API: Ошибка запроса ({method.upper()} {endpoint}): {e}")
        if e.response is not None:
             logger.error(f"AmoCRM API: Ответ при ошибке ({e.response.status_code}): {e.response.text[:200]}...") # Логируем часть ответа при ошибке

             # Для специфических HTTP ошибок добавляем более информативные сообщения в исключение
             if e.response.status_code == 404:
                 # 404 Not Found
                 raise ConnectionError(f"AmoCRM API: Not Found (404) {method.upper()} {endpoint}") from e
             if e.response.status_code == 400:
                  # 400 Bad Request - пытаемся извлечь детали ошибки из JSON тела ответа
                  error_detail = "Нет деталей ошибки в ответе (не JSON или пустое)"
                  try:
                       error_json = e.response.json()
                       # Пытаемся получить поле 'detail' или весь JSON как строку
                       error_detail = error_json.get('detail', str(error_json))
                  except json.JSONDecodeError:
                       # Если тело ошибки не JSON
                       error_detail = e.response.text[:200] # Логируем часть сырого текста ошибки
                  logger.error(f"AmoCRM API: Bad Request (400) Details: {error_detail}")
                  # Пробрасываем как ValueError, чтобы было понятно, что это проблема данных/запроса,
                  # а не сетевая ошибка или "не найдено".
                  raise ValueError(f"AmoCRM API: Некорректный запрос (400): {error_detail}") from e

        # Для всех остальных ошибок requests.exceptions (сетевые, другие 4xx/5xx кроме 400/404)
        # Перебрасываем как ConnectionError
        raise ConnectionError(f"AmoCRM API: Ошибка при обращении ({method.upper()} {endpoint}): {e}") from e
    except (ValueError, RuntimeError) as e:
        # Обработка ошибок, которые мы сами пробросили (например, ошибка парсинга JSON ответа, ошибка получения токена)
        # Эти ошибки уже достаточно информативно залогированы внутри тех блоков, где они возникли.
        # Просто пробрасываем их дальше.
        raise e
    except Exception as e:
        # Обработка любых других неожиданных исключений
        logger.critical(f"AmoCRM API: Неожиданная КРИТИЧЕСКАЯ ошибка при работе ({method.upper()} {endpoint}): {e}", exc_info=True)
        # Пробрасываем как RuntimeError
        raise RuntimeError(f"AmoCRM API: Неожиданная ошибка: {e}") from e


def find_lead_by_avito_id(avito_id: str | None) -> int | None:
    """
    Ищет сделку по сохраненному ID чата/пользователя Avito в кастомном поле,
    используя логику пагинации и клиент-сайдового поиска по полям.
    Использует Avito ID (предполагается, что это chat_id или user_id) для поиска.
    Возвращает ID сделки (int) или None, если не найдено или ошибка.

    Адаптировано из предоставленного рабочего кода поиска по кастомному полю.
    """
    if not avito_id or not avito_id.strip():
        logger.warning("AmoCRM Search: find_lead_by_avito_id вызван с пустым Avito ID. Поиск невозможен.")
        return None

    if not AMO_AVITO_ID_FIELD_ID or AMO_AVITO_ID_FIELD_ID == 0:
        logger.error("AmoCRM Search: AMO_AVITO_ID_FIELD_ID не настроен (0 или None). Поиск сделки по Avito ID невозможен.")
        return None

    # PIPELINE_ID для фильтра при получении списка сделок (опционально)
    pipeline_filter_params = {}
    if AMO_PIPELINE_ID is not None and AMO_PIPELINE_ID > 0:
        pipeline_filter_params['filter[pipeline_id]'] = AMO_PIPELINE_ID
        logger.info(f"AmoCRM Search: Поиск сделки для Avito ID {avito_id} ограничен воронкой с ID: {AMO_PIPELINE_ID}")
    else:
        logger.warning("AmoCRM Search: AMO_PIPELINE_ID не настроен. Поиск будет выполняться по всем сделкам аккаунта (может быть очень медленно!).")


    logger.info(f"AmoCRM Search: Начало поиска сделки для Avito ID: {avito_id} в кастомном поле ID {AMO_AVITO_ID_FIELD_ID} (клиент-сайдовый поиск).")

    # Параметры для получения первой страницы списка сделок
    params = {
        'limit': 250,  # Максимум сделок на страницу
        **pipeline_filter_params # Добавляем фильтр по воронке, если он есть
    }

    # --- Логика пагинации и поиска ---
    found_lead_id = None
    page_count = 0
    # Endpoint всегда /leads для получения списка
    endpoint = "/leads"

    try:
        # Используем цикл while True и проверяем наличие 'next' ссылки для выхода
        while True:
            page_count += 1
            logger.debug(f"AmoCRM Search: Запрос страницы {page_count} списка сделок. Endpoint: {endpoint}, Params: {params}")

            # Выполняем GET запрос к AmoCRM API для получения списка сделок
            response_data = _amo_request('get', endpoint, params=params)

            if response_data is None:
                 # _amo_request возвращает None только для 204 No Content, что не ожидается для GET /leads
                 logger.error(f"AmoCRM Search: Получен неожиданно пустой ответ на странице {page_count} при получении списка сделок. Завершение поиска.")
                 break # Завершаем поиск

            if '_embedded' not in response_data or 'leads' not in response_data['_embedded']:
                # Это может быть валидный ответ 200 с пустой _embedded секцией, если сделок нет на этой странице
                if response_data.get('_embedded', {}).get('leads') is None:
                     logger.debug(f"AmoCRM Search: На странице {page_count} нет сделок в _embedded.leads. Завершение поиска пагинации.")
                     break # Больше нет сделок
                else:
                     logger.warning(f"AmoCRM Search: Ответ AmoCRM на странице {page_count} не содержит ожидаемой структуры '_embedded.leads'. Ответ: {response_data}")
                     break # Unexpected формат ответа - ошибка


            leads = response_data['_embedded']['leads']
            logger.debug(f"AmoCRM Search: Получено {len(leads)} сделок на странице {page_count}.")

            if not leads:
                logger.info(f"AmoCRM Search: На странице {page_count} нет сделок. Завершение поиска.")
                break # Больше нет сделок

            # Итерируем по сделкам на текущей странице
            for lead in leads:
                lead_id = lead.get('id')
                lead_name = lead.get('name', 'N/A')
                custom_fields = lead.get('custom_fields_values', [])

                logger.debug(f"AmoCRM Search: Проверка сделки ID: {lead_id}, Имя: {lead_name}")

                # Ищем наше кастомное поле в списке полей сделки
                for field in custom_fields:
                    # Убедимся, что поле имеет нужный ID и содержит массив values
                    if field.get('field_id') == AMO_AVITO_ID_FIELD_ID and 'values' in field and isinstance(field['values'], list) and field['values']:
                         # Итерируем по всем значениям поля (хотя обычно для Avito ID оно одно)
                         for value_data in field['values']:
                              field_value = value_data.get('value')
                              # Сравниваем значение поля со значением, которое мы ищем (Avito ID)
                              if field_value is not None and isinstance(field_value, str) and field_value.strip() == avito_id.strip():
                                  logger.info(f"AmoCRM Search: ✅ Сделка найдена по Avito ID: {avito_id}. ID сделки: {lead_id}")
                                  # Найдена нужная сделка, сохраняем ID и выходим из всех циклов
                                  found_lead_id = lead_id
                                  break # Выходим из цикла по значениям поля
                         if found_lead_id is not None:
                              break # Выходим из цикла по полям, если найдена

                if found_lead_id is not None:
                    break # Выходим из цикла по сделкам, если найдена

            if found_lead_id is not None:
                break # Выходим из основного цикла while, если найдена на этой странице


            # Проверяем наличие ссылки на следующую страницу для пагинации
            if '_links' in response_data and 'next' in response_data['_links']:
                # AmoCRM API v4 возвращает полный URL в 'next'
                next_link_full = response_data['_links']['next']['href']
                logger.debug(f"AmoCRM Search: Найден next link (пагинация). Продолжаем поиск.")
                # Для корректной пагинации с _amo_request, нужно извлечь offset или page из 'next' url
                # и добавить их к параметрам для следующего запроса.
                parsed_url = requests.utils.urlparse(next_link_full)
                next_params = requests.utils.parse_qs(parsed_url.query)
                # parse_qs возвращает списки значений, нужно взять первое и преобразовать в int
                offset_str_list = next_params.get('offset', [None])
                page_str_list = next_params.get('page', [None])

                if offset_str_list[0] is not None:
                    try:
                        params['offset'] = int(offset_str_list[0])
                        params.pop('page', None) # Убираем page, если есть offset
                    except (ValueError, TypeError):
                         logger.warning(f"AmoCRM Search: Невалидный 'offset' в next link: {offset_str_list[0]}. Завершение поиска пагинации.")
                         break
                elif page_str_list[0] is not None:
                    try:
                        params['page'] = int(page_str_list[0])
                        params.pop('offset', None) # Убираем offset, если есть page
                    except (ValueError, TypeError):
                         logger.warning(f"AmoCRM Search: Невалидный 'page' в next link: {page_str_list[0]}. Завершение поиска пагинации.")
                         break
                else:
                     logger.warning("AmoCRM Search: Найден next link, но не удалось извлечь параметры пагинации (offset/page). Завершение поиска.")
                     break # Не удалось извлечь параметры пагинации

                # Убедимся, что limit тоже передан для следующего запроса
                limit_str_list = next_params.get('limit', [None])
                if limit_str_list[0] is not None:
                     try:
                          params['limit'] = int(limit_str_list[0])
                     except (ValueError, TypeError):
                          logger.warning(f"AmoCRM Search: Невалидный 'limit' в next link: {limit_str_list[0]}. Используем предыдущий limit.")
                          # Оставляем старый limit в params

                # Endpoint остается '/leads', параметры пагинации и фильтры добавлены в `params`
                # params.update(pipeline_filter_params) # Фильтр по воронке уже в params, его не нужно добавлять снова, т.к. params сохраняется

            else:
                logger.info("AmoCRM Search: Нет ссылки на следующую страницу. Завершение поиска.")
                break # Завершаем цикл while

        # После завершения цикла
        if found_lead_id is None:
            logger.info(f"AmoCRM Search: ❌ Сделка для Avito ID: {avito_id} не найдена после проверки всех страниц.")
        # else: Сделка найдена, информация уже залогирована внутри цикла

        return found_lead_id

    except (ConnectionError, ValueError, RuntimeError) as e:
         # Эти исключения пробрасываются из _amo_request при ошибках API (4xx, 5xx, таймаут, невалидный JSON)
         logger.error(f"AmoCRM Search: Ошибка при поиске сделки для Avito ID {avito_id}: {e}")
         # Возвращаем None, чтобы дать возможность создать новую сделку
         return None
    except Exception as e:
        logger.critical(f"AmoCRM Search: Неожиданная КРИТИЧЕСКАЯ ошибка при поиске сделки для Avito ID {avito_id}: {e}", exc_info=True)
        # Для необработанных исключений, лучше вернуть None и залогировать как CRITICAL
        return None


def create_amo_lead(name: str | None, status_id: int | None, phone: str | None, comment: str | None, dialog_text: str | None, avito_id: str | None) -> int | None:
    """Создает новую сделку в AmoCRM с начальными данными. Возвращает ID созданной сделки или None."""
    if not AMO_PIPELINE_ID or AMO_PIPELINE_ID == 0:
         logger.error("AmoCRM Create: Константа AMO_PIPELINE_ID не настроена (0 или None). Сделка не будет создана в нужной воронке.")
         # Это критично, если сделки всегда создаются в определенной воронке.
         # Можно бросить исключение, если воронка обязательна.
         # raise ValueError("Конфигурация AmoCRM неполная: не настроен AMO_PIPELINE_ID.")
         pipeline_id_to_create = None # Создать без указания воронки (может попасть в дефолтную)
         logger.warning("AmoCRM Create: AMO_PIPELINE_ID не настроен, сделка будет создана без указания воронки.")
    else:
        pipeline_id_to_create = AMO_PIPELINE_ID

    if not AMO_AVITO_ID_FIELD_ID or AMO_AVITO_ID_FIELD_ID == 0:
         logger.warning("AmoCRM Create: AMO_AVITO_ID_FIELD_ID не настроен (0 или None). Avito ID не будет сохранен в кастомном поле при создании сделки.")
         # Не критично для создания самой сделки, но делает поиск по Avito ID в будущем невозможным для этой сделки.
         # Можно бросить исключение, если сохранение Avito ID обязательно.
         # raise ValueError("Конфигурация AmoCRM неполная: не настроен AMO_AVITO_ID_FIELD_ID.")


    # Статус обязателен для создания сделки в v4 API, если не 0 (Неразобранное)
    # Если статус от нейросети None, используем 0 (Неразобранное)
    status_id_to_create = status_id if status_id is not None else 0
    if status_id is None and status_id_to_create == 0:
        logger.warning("AmoCRM Create: status_id не получен от нейросети. Сделка будет создана в статусе 'Неразобранное' (ID 0).")
    elif status_id is None and status_id_to_create != 0:
         logger.warning(f"AmoCRM Create: status_id не получен от нейросети. Используется статус по умолчанию {status_id_to_create}.")


    endpoint = "/leads"
    custom_fields_values = []

    if AMO_PHONE_FIELD_ID and AMO_PHONE_FIELD_ID > 0 and phone is not None and phone.lower() != "номер не известен":
        custom_fields_values.append({
            "field_id": AMO_PHONE_FIELD_ID,
            "values": [{"value": phone}]
        })

    if AMO_COMMENT_FIELD_ID and AMO_COMMENT_FIELD_ID > 0 and comment is not None:
        custom_fields_values.append({
            "field_id": AMO_COMMENT_FIELD_ID,
            "values": [{"value": comment}]
        })

    # Добавляем диалог при создании, если поле настроено
    if AMO_DIALOG_FIELD_ID and AMO_DIALOG_FIELD_ID > 0 and dialog_text is not None:
         custom_fields_values.append({
             "field_id": AMO_DIALOG_FIELD_ID,
             "values": [{"value": dialog_text}]
         })

    # Обязательно сохраняем Avito ID в кастомном поле для последующего поиска
    if AMO_AVITO_ID_FIELD_ID and AMO_AVITO_ID_FIELD_ID > 0 and avito_id is not None:
         custom_fields_values.append({
             "field_id": AMO_AVITO_ID_FIELD_ID,
             "values": [{"value": avito_id}]
         })
    # Логирование warning уже сделано выше, если поле не настроено

    # Определяем имя сделки, как в вашем GAS, добавим Avito ID если имя по умолчанию
    lead_name = name if name is not None and name not in NAME_PLACEHOLDERS and name.strip() else f"Заявка Avito ({avito_id if avito_id else 'N/A'})"

    # Формируем данные для запроса создания
    data = [{
        "name": lead_name,
        "status_id": status_id_to_create,
        "custom_fields_values": custom_fields_values if custom_fields_values else None # Отправляем null если нет полей
    }]
    # Добавляем pipeline_id только если он настроен
    if pipeline_id_to_create is not None:
        data[0]["pipeline_id"] = pipeline_id_to_create


    logger.info(f"AmoCRM Create: Попытка создания новой сделки для Avito ID {avito_id} с именем '{lead_name[:50]}...' и статусом {status_id_to_create}.")
    # logger.debug(f"AmoCRM Create: Данные для создания: {json.dumps(data)}") # Осторожно с чувствительными данными/большим диалогом

    try:
        response_data = _amo_request('post', endpoint, json=data)
        if response_data and '_embedded' in response_data and 'leads' in response_data['_embedded']:
            new_lead = response_data['_embedded']['leads'][0]
            logger.info(f"AmoCRM Create: Сделка успешно создана. ID: {new_lead['id']} для Avito ID {avito_id}.")
            return new_lead['id']
        else:
             logger.error(f"AmoCRM Create: Ошибка создания сделки: Не удалось получить ID из ответа. Ответ: {response_data}. Avito ID: {avito_id}")
             # Возвращаем None при ошибке создания, но логируем как ошибку
             return None

    except (ConnectionError, ValueError, RuntimeError) as e:
        logger.error(f"AmoCRM Create: Ошибка при создании сделки для Avito ID {avito_id}: {e}")
        # Возвращаем None при ошибке создания
        return None
    except Exception as e:
        logger.critical(f"AmoCRM Create: Неожиданная КРИТИЧЕСКАЯ ошибка при создании сделки для Avito ID {avito_id}: {e}", exc_info=True)
        # Возвращаем None при критической ошибке
        return None


def update_amo_lead(lead_id: int, status_id: int | None, phone: str | None, comment: str | None, dialog_text: str | None, client_name: str | None, avito_id: str | None):
    """Обновляет существующую сделку в AmoCRM."""
    if not lead_id:
        logger.warning("AmoCRM Update: Попытка обновить сделку с пустым lead_id.")
        return

    endpoint = f"/leads/{lead_id}"
    update_data = {}
    custom_fields_values = []

    if status_id is not None: # Обновляем статус, если передан валидный status_id (может быть 0)
        update_data['status_id'] = status_id
        logger.debug(f"AmoCRM Update: Обновляем статус сделки {lead_id} на {status_id}.")

    if client_name is not None and client_name not in NAME_PLACEHOLDERS and client_name.strip():
         # Осторожно: может перезаписать существующее имя, даже если оно более подходящее.
         # Можно добавить логику проверки текущего имени, но это усложнит запрос.
         update_data['name'] = client_name
         logger.debug(f"AmoCRM Update: Обновляем имя сделки {lead_id} на: '{client_name}'")
    elif client_name is not None:
         logger.debug(f"AmoCRM Update: Имя '{client_name}' является плейсхолдером, имя сделки {lead_id} не обновляется.")


    if AMO_PHONE_FIELD_ID and AMO_PHONE_FIELD_ID > 0 and phone is not None and phone.lower() != "номер не известен":
         # Для многозначных полей типа "Телефон" простая передача нового значения может добавить его как новое.
         # Если нужно заменить или обновить конкретное значение, логика сложнее (нужно получить текущие, найти, изменить).
         # Здесь просто добавляем новое значение (или обновляем первое, если поле однозначное).
         custom_fields_values.append({
             "field_id": AMO_PHONE_FIELD_ID,
             "values": [{"value": phone}]
         })
         logger.debug(f"AmoCRM Update: Добавляем/обновляем телефон для сделки {lead_id}: {phone}")

    if AMO_COMMENT_FIELD_ID and AMO_COMMENT_FIELD_ID > 0 and comment is not None:
        # Для текстовых полей, передача нового значения обычно заменяет старое.
        custom_fields_values.append({
            "field_id": AMO_COMMENT_FIELD_ID,
            "values": [{"value": comment}]
        })
        logger.debug(f"AmoCRM Update: Добавляем/обновляем комментарий для сделки {lead_id}")

    if AMO_DIALOG_FIELD_ID and AMO_DIALOG_FIELD_ID > 0 and dialog_text is not None:
        # Для текстовых полей, передача нового значения обычно заменяет старое.
        # Убедитесь, что поле достаточно длинное!
        custom_fields_values.append({
            "field_id": AMO_DIALOG_FIELD_ID,
            "values": [{"value": dialog_text}]
        })
        logger.debug(f"AmoCRM Update: Добавляем/обновляем диалог для сделки {lead_id}")

    # Обновляем Avito ID на всякий случай, если поле настроено и значение пришло
    if AMO_AVITO_ID_FIELD_ID and AMO_AVITO_ID_FIELD_ID > 0 and avito_id is not None:
        # Предполагаем, что это текстовое, однозначное поле
        custom_fields_values.append({
            "field_id": AMO_AVITO_ID_FIELD_ID,
            "values": [{"value": avito_id}]
        })
        logger.debug(f"AmoCRM Update: Обновляем/подтверждаем Avito ID {avito_id} для сделки {lead_id}")


    if custom_fields_values:
        update_data['custom_fields_values'] = custom_fields_values

    if update_data: # Если есть что обновлять (хотя бы статус, имя или кастомные поля)
        # AmoCRM API v4 PATCH для одной сущности требует массив из одного элемента
        data_payload = [update_data]
        logger.info(f"AmoCRM Update: Попытка обновления сделки {lead_id}. Данные для обновления: {list(update_data.keys())}") # Логируем только ключи
        try:
            _amo_request('patch', endpoint, json=data_payload)
            logger.info(f"AmoCRM Update: Сделка {lead_id} успешно обновлена.")
        except (ConnectionError, ValueError, RuntimeError) as e:
            # Не бросаем исключение, чтобы основной процесс завершился, но логируем проблему
            logger.error(f"AmoCRM Update: Ошибка при обновлении сделки {lead_id}: {e}", exc_info=True)
            # Можно добавить raise, если ошибка обновления критична и должна прерывать webhook

    else:
        logger.info(f"AmoCRM Update: Нет данных для обновления сделки {lead_id} (статус, имя, поля). Пропуск PATCH запроса.")


def add_note_to_lead(lead_id: int, note_text: str | None):
    """Добавляет примечание (заметку) в ленту сделки."""
    if not lead_id:
        logger.warning("AmoCRM Note: Пропуск добавления заметки: lead_id пустой.")
        return
    if not note_text or not note_text.strip():
        logger.warning(f"AmoCRM Note: Пропуск добавления заметки к сделке {lead_id}: текст заметки пустой.")
        return

    endpoint = f"/leads/{lead_id}/notes"
    note_type = 'common' # Тип примечания 4 - standard note ('common')

    data = [{
        "note_type": note_type,
        "params": {
            "text": note_text
        }
    }]

    logger.debug(f"AmoCRM Note: Попытка добавить заметку к сделке {lead_id}. Текст: '{note_text[:100]}...'")

    try:
        _amo_request('post', endpoint, json=data)
        logger.info(f"AmoCRM Note: Заметка успешно добавлена к сделке {lead_id}.")
    except (ConnectionError, ValueError, RuntimeError) as e:
        logger.error(f"AmoCRM Note: Ошибка при добавлении заметки к сделке {lead_id}: {e}", exc_info=True)
        # Не бросаем исключение, т.к. добавление заметки не должно прерывать весь процесс.


# --- ФОНОВАЯ ФУНКЦИЯ для выполнения основной логики (вся тяжелая работа) ---
async def process_avito_message(avito_user_id_in: str, avito_chat_id_in: str, message_text_in: str):
    """
    Выполняет всю основную логику обработки сообщения Avito в фоновом режиме:
    вызов нейросети, отправка ответа клиенту, работа с AmoCRM.
    Логирует ошибки, но не пробрасывает их дальше, чтобы не завершить процесс.
    """
    # Генерируем ID для логирования в фоновой задаче (полезно для отслеживания)
    # Используем первые 8 символов chat_id + user_id
    log_id = f"BG-{avito_chat_id_in[:8]}-{avito_user_id_in[:8]}"

    logger.info(f"{log_id}: Начало обработки сообщения для chat_id {avito_chat_id_in}, user_id {avito_user_id_in}")

    # --- 1. Вызов API нейросети Pro-Talk.ru и парсинг ответа ---
    nn_text_response: str = ""
    status_id_nn: int | None = None
    comment_nn: str | None = None
    dialog_nn: str | None = None
    client_name_nn: str | None = None
    phone_nn: str | None = None

    try:
         # call_pro_talk_api может бросить HTTPException или ValueError. Ловим их здесь.
         nn_processed_data = await call_pro_talk_api(message_text_in, avito_chat_id_in)

         # Извлекаем данные из результата обработки нейросети
         nn_text_response = nn_processed_data.get('nn_text_response', '')
         status_id_nn = nn_processed_data.get('status_id')
         comment_nn = nn_processed_data.get('comment')
         dialog_nn = nn_processed_data.get('dialog')
         client_name_nn = nn_processed_data.get('client_name')
         phone_nn = nn_processed_data.get('phone')

         logger.info(f"{log_id}: Данные от нейросети: Статус: {status_id_nn}, Имя: {client_name_nn}, Тел: {phone_nn}, Коммент: '{str(comment_nn)[:50] if comment_nn else 'None'}...', Диалог: '{str(dialog_nn)[:50] if dialog_nn else 'None'}...', Текст ответа: '{nn_text_response[:50]}...'")

    except Exception as e:
         # Логирование уже произошло внутри call_pro_talk_api
         logger.error(f"{log_id}: Ошибка при вызове или обработке ответа нейросети: {e}")
         # Если ошибка на этом этапе, мы не можем получить данные для AmoCRM или текст для отправки.
         # Завершаем фоновую задачу.
         return # Завершаем фоновую задачу


    # --- 2. ОТПРАВКА ОТВЕТА БОТА ОБРАТНО В АВИТО ЧЕРЕЗ APIMONSTER ---
    try:
        # Отправляем только nn_text_response (текст БЕЗ параметров), если он не пустой
        if nn_text_response and nn_text_response.strip():
             await send_message_to_apimonster(avito_user_id_in, avito_chat_id_in, nn_text_response)
             logger.info(f"{log_id}: Ответ бота ('{nn_text_response[:50]}...') успешно отправлен обратно в чат Avito {avito_chat_id_in}.")
        else:
             logger.warning(f"{log_id}: Текстовый ответ нейросети (nn_text_response) пуст или отсутствует. Пропускаем отправку сообщения в Avito.")

    except Exception as e:
         # Логирование уже произошло внутри send_message_to_apimonster
         logger.error(f"{log_id}: Ошибка при отправке сообщения в Apimonster: {e}")
         # Ошибка отправки клиенту не должна прерывать работу с AmoCRM. Логируем и продолжаем.


    # --- 3. Работа с AmoCRM (поиск, создание, обновление) ---
    lead_id_for_response = None
    operation_status = ""

    avito_crm_id_to_search = avito_chat_id_in # Решите, что использовать: chat_id или user_id

    # Поиск существующей сделки
    # find_lead_by_avito_id может бросить исключение при ошибках AmoCRM. Ловим его.
    try:
        current_lead_id = find_lead_by_avito_id(avito_crm_id_to_search)
    except Exception as e:
         logger.error(f"{log_id}: Критическая ошибка при поиске сделки в AmoCRM для Avito ID {avito_crm_id_to_search}: {e}")
         # Ошибка поиска AmoCRM критична для создания/обновления. Логируем и завершаем фоновую задачу.
         return # Завершаем фоновую задачу


    if current_lead_id is not None:
        # --- СДЕЛКА СУЩЕСТВУЕТ -> ОБНОВЛЯЕМ ---
        lead_id_for_response = current_lead_id
        operation_status = "обновлено"
        logger.info(f"{log_id}: Обновление сделки {current_lead_id} для Avito ID {avito_crm_id_to_search}.")

        # Обновляем сделку
        try:
             update_amo_lead(
                 lead_id=current_lead_id,
                 status_id=status_id_nn, # int или None
                 phone=phone_nn, # str или None
                 comment=comment_nn, # str или None
                 dialog_text=dialog_nn, # str или None
                 client_name=client_name_nn, # str или None
                 avito_id=avito_crm_id_to_search # str (передаем Avito ID на всякий случай)
             )
        except Exception as e:
             logger.error(f"{log_id}: Ошибка при обновлении сделки {current_lead_id} для Avito ID {avito_crm_id_to_search}: {e}", exc_info=True)
             # Ошибка обновления не всегда критична. Логируем и продолжаем для добавления заметки.


        # Добавляем заметку
        try:
            note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
            # Можно также добавить информацию, что обработано ботом
            if nn_text_response:
                note_content += f"\nОтвет бота: {nn_text_response[:200]}..." # Добавим часть ответа бота в заметку
            add_note_to_lead(current_lead_id, note_content)
        except Exception as e:
             logger.error(f"{log_id}: Ошибка при добавлении заметки к сделке {current_lead_id}: {e}", exc_info=True)
             # Ошибка добавления заметки не критична.


    else:
        # --- СДЕЛКА НЕ НАЙДЕНА -> СОЗДАЕМ НОВУЮ ---
        logger.info(f"{log_id}: Сделка для Avito ID {avito_crm_id_to_search} не найдена. Создание новой.")
        operation_status = "создано"

        # Создаем новую сделку
        try:
             new_lead_id = create_amo_lead(
                 name=client_name_nn, # Имя от нейросети или по умолчанию
                 status_id=status_id_nn, # Статус от нейросети (может быть None, функция выберет дефолт)
                 phone=phone_nn, # Телефон от нейросети или None
                 comment=comment_nn, # Комментарий от нейросети или None
                 dialog_text=dialog_nn, # Диалог от нейросети или None
                 avito_id=avito_crm_id_to_search # Обязательно сохраняем Avito ID
             )
             lead_id_for_response = new_lead_id

             if new_lead_id is not None: # Убедимся, что создание прошло успешно
                 logger.info(f"{log_id}: Создана новая сделка {new_lead_id} и связана с Avito ID {avito_crm_id_to_search}.")

                 # Добавляем заметку к новой сделке
                 try:
                     note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
                     if nn_text_response:
                        note_content += f"\nОтвет бота: {nn_text_response[:200]}..." # Добавим часть ответа бота
                     add_note_to_lead(new_lead_id, note_content)
                 except Exception as e:
                     logger.error(f"{log_id}: Ошибка при добавлении заметки к новой сделке {new_lead_id}: {e}", exc_info=True)
                     # Ошибка добавления заметки не критична.
             else:
                  # Ошибка при создании уже залогирована внутри create_amo_lead
                  logger.error(f"{log_id}: Не удалось создать сделку для Avito ID {avito_crm_id_to_search}.")
                  # Завершаем фоновую задачу
                  return


        except Exception as e:
             # Ошибка при создании сделки критична для этой ветви логики
             logger.error(f"{log_id}: Ошибка при создании сделки для Avito ID {avito_crm_id_to_search}: {e}", exc_info=True)
             # Завершаем фоновую задачу
             return


    logger.info(f"{log_id}: Обработка для Avito ID {avito_crm_id_to_search} завершена. ID сделки: {lead_id_for_response}, Статус: {operation_status}")


# --- Главный обработчик Webhook (быстрый ответ) ---

# Уточните реальные имена параметров, которые присылает промежуточная платформа!
# По логам похоже, что это 'user_id', 'chat_id', 'message_text'
AVITO_USER_ID_PARAM_IN = 'param2'
AVITO_CHAT_ID_PARAM_IN = 'param1'
AVITO_MESSAGE_PARAM_IN = 'param3'
# Если есть 4-й параметр и он нужен, укажите его имя:
# AVITO_FOURTH_PARAM_IN = 'fourth_param_name_in_webhook'


@app.post("/")
async def webhook_receiver(request: Request, background_tasks: BackgroundTasks): # Добавляем background_tasks как зависимость
    # Используем имена переменных для входящих данных (для логирования в случае ранней ошибки)
    avito_chat_id_in: str | None = None
    avito_user_id_in: str | None = None
    message_text_in: str | None = None
    # fourth_param_in: str | None = None


    logger.info("🚀 Пришел входящий webhook.")

    # --- 1. Извлечение и базовая валидация входящего webhook (быстрый ответ) ---
    # Этот блок ДОЛЖЕН выполняться очень быстро, чтобы ответить Apimonster'у за < 5 сек.
    try:
        # Проверяем Content-Type
        content_type = request.headers.get('content-type', '').lower()
        if 'application/x-www-form-urlencoded' not in content_type and 'multipart/form-data' not in content_type:
             logger.error(f"Входящий webhook: Получен с не-FORM/Multipart Content-Type: {content_type}")
             raise HTTPException(status_code=415, detail=f"Ожидается Content-Type: application/x-www-form-urlencoded или multipart/form-data, получен {content_type}")

        # Парсим данные формы
        incoming_data: FormData = await request.form()
        logger.info(f"Входящий webhook: Данные получены. Ключи: {str(list(incoming_data.keys()))[:300]}...") # Логируем только ключи входящих данных

        # Получаем значения по именам параметров из объекта Form Data
        # Используем .get() с None по умолчанию
        avito_user_id_in = incoming_data.get(AVITO_USER_ID_PARAM_IN)
        avito_chat_id_in = incoming_data.get(AVITO_CHAT_ID_PARAM_IN)
        message_text_in = incoming_data.get(AVITO_MESSAGE_PARAM_IN)
        # fourth_param_in = incoming_data.get(AVITO_FOURTH_PARAM_IN) # Если нужен


        # Проверка на наличие обязательных данных (не None и не пустая строка после strip)
        if not avito_chat_id_in or not avito_user_id_in or not message_text_in or not isinstance(message_text_in, str) or not message_text_in.strip():
            missing_params = []
            if not avito_user_id_in or (isinstance(avito_user_id_in, str) and not avito_user_id_in.strip()): missing_params.append(AVITO_USER_ID_PARAM_IN)
            if not avito_chat_id_in or (isinstance(avito_chat_id_in, str) and not avito_chat_id_in.strip()): missing_params.append(AVITO_CHAT_ID_PARAM_IN)
            if not message_text_in or not isinstance(message_text_in, str) or not message_text_in.strip(): missing_params.append(AVITO_MESSAGE_PARAM_IN)

            logger.error(f"Входящий webhook: Базовая валидация не пройдена. Отсутствуют/пустые обязательные параметры: {', '.join(missing_params)}. Полученные ключи: {list(incoming_data.keys())}")
            # Возвращаем 400 Bad Request, т.к. проблема во входящих данных
            raise HTTPException(status_code=400, detail=f"Отсутствуют обязательные параметры в webhook: {', '.join(missing_params)}")

        # Убедимся, что user_id и chat_id точно строки
        avito_user_id_in = str(avito_user_id_in)
        avito_chat_id_in = str(avito_chat_id_in)
        # message_text_in уже проверен, что это не пустая строка


        logger.info(f"Входящий webhook ({avito_chat_id_in[:8]}...): Базовая валидация успешна. user_id={avito_user_id_in}, chat_id={avito_chat_id_in}, message_text='{message_text_in[:100]}...'")

    except HTTPException as http_exc:
         # Если базовая валидация не пройдена, сразу возвращаем ошибку
         logger.error(f"Входящий webhook: Базовая валидация не пройдена. Ошибка: {http_exc.detail}")
         return {"status": "error", "message": http_exc.detail}, http_exc.status_code
    except Exception as e:
         logger.error(f"Входящий webhook: Неожиданная ошибка при базовой валидации: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при валидации входящих данных: {e}")


    # --- 2. Планирование фоновой задачи для всей остальной логики ---
    # Передаем необходимые данные в фоновую функцию
    # Здесь нет дедупликации. Каждое сообщение пройдет дальше.
    background_tasks.add_task(
        process_avito_message,
        avito_user_id_in,
        avito_chat_id_in,
        message_text_in
        # Если нужен fourth_param_in, передайте его сюда и добавьте в сигнатуру process_avito_message
    )
    logger.info(f"Входящий webhook ({avito_chat_id_in[:8]}...): Фоновая задача запланирована (без дедупликации).")

    # --- 3. Сразу вернуть быстрый ответ 200 OK ---
    # Это сигнализирует Apimonster'у, что webhook принят.
    # Возвращаем 200, как будто все успешно, даже если будут повторы.
    return {"status": "ok", "message": "Processing in background (no deduplication)"}


@app.get("/")
async def root():
    """Простой эндпоинт для проверки, что сервер запущен."""
    return {"message": "Сервер интеграции Avito-ProTalk-AmoCRM работает!"}

# --- Блок запуска ---
if __name__ == "__main__":
    # !!! В ЭТОЙ ВЕРСИИ НЕТ ИНИЦИАЛИЗАЦИИ БД И ДЕДУПЛИКАЦИИ !!!
    # Если вы хотите дедупликацию, используйте предыдущую версию кода.

    # --- Запуск FastAPI сервера ---
    import uvicorn
    logger.info("Запуск FastAPI сервера...")
    # В режиме разработки reload=True удобен, в продакшене лучше использовать без него и с менеджером процессов (gunicorn)
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
