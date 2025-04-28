import os
import logging
import asyncio
import json
import re
import requests
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks # Импортируем BackgroundTasks
from starlette.datastructures import FormData

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

NAME_PLACEHOLDERS = ["Неизвестно", "Клиент Avito", "777"]

# --- Логирование ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

# --- Вспомогательные функции (parse_nn_text_response, call_pro_talk_api, send_message_to_apimonster, _get_amo_headers, _amo_request, find_lead_by_avito_id, create_amo_lead, update_amo_lead, add_note_to_lead) ---
# (Эти функции остаются БЕЗ ИЗМЕНЕНИЙ. Они будут вызываться из новой фоновой задачи.)
# ... КОД ФУНКЦИЙ parse_nn_text_response, call_pro_talk_api, send_message_to_apimonster, _get_amo_headers, _amo_request, find_lead_by_avito_id, create_amo_lead, update_amo_lead, add_note_to_lead ИДЕТ ЗДЕСЬ ...
# (Скопируйте их из предыдущего полного кода)

# --- НОВАЯ ФОНОВАЯ ФУНКЦИЯ для выполнения основной логики ---
async def process_avito_message(avito_user_id_in: str, avito_chat_id_in: str, message_text_in: str):
    """
    Выполняет всю основную логику обработки сообщения Avito в фоновом режиме:
    вызов нейросети, отправка ответа клиенту, работа с AmoCRM.
    """
    logger.info(f"ФОНОВАЯ ЗАДАЧА: Начало обработки сообщения для chat_id {avito_chat_id_in}, user_id {avito_user_id_in}")

    try:
        # --- 1. Вызов API нейросети Pro-Talk.ru и парсинг ответа ---
        # call_pro_talk_api может бросить HTTPException или ValueError. Ловим их здесь.
        try:
             nn_processed_data = await call_pro_talk_api(message_text_in, avito_chat_id_in)
        except Exception as e:
             # Логирование уже произошло внутри call_pro_talk_api
             logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при вызове или обработке ответа нейросети для chat_id {avito_chat_id_in}: {e}")
             # В фоновой задаче мы не можем вернуть ошибку клиенту. Просто логируем и, возможно, останавливаем обработку для этой задачи.
             # В зависимости от требований, можно решить, стоит ли продолжать работу с AmoCRM без данных от нейросети.
             # Давайте решим остановиться, так как данные от нейросети критичны для AmoCRM.
             return # Завершаем фоновую задачу

        # Извлекаем данные из результата обработки нейросети
        nn_text_response: str = nn_processed_data.get('nn_text_response', '')
        status_id_nn: int | None = nn_processed_data.get('status_id')
        comment_nn: str | None = nn_processed_data.get('comment')
        dialog_nn: str | None = nn_processed_data.get('dialog')
        client_name_nn: str | None = nn_processed_data.get('client_name')
        phone_nn: str | None = nn_processed_data.get('phone')

        logger.info(f"ФОНОВАЯ ЗАДАЧА: Данные от нейросети для chat_id {avito_chat_id_in}: Статус: {status_id_nn}, Имя: {client_name_nn}, Тел: {phone_nn}, Коммент: '{str(comment_nn)[:50] if comment_nn else 'None'}...', Диалог: '{str(dialog_nn)[:50] if dialog_nn else 'None'}...', Текст ответа: '{nn_text_response[:50]}...'")


        # --- 2. ОТПРАВКА ОТВЕТА БОТА ОБРАТНО В АВИТО ЧЕРЕЗ APIMONSTER ---
        # send_message_to_apimonster может бросить HTTPException или ValueError. Ловим их здесь.
        try:
            # Отправляем только nn_text_response (текст БЕЗ параметров), если он не пустой
            if nn_text_response and nn_text_response.strip():
                 await send_message_to_apimonster(avito_user_id_in, avito_chat_id_in, nn_text_response)
                 logger.info(f"ФОНОВАЯ ЗАДАЧА: Ответ бота ('{nn_text_response[:50]}...') успешно отправлен обратно в чат Avito {avito_chat_id_in}.")
            else:
                 logger.warning(f"ФОНОВАЯ ЗАДАЧА: Текстовый ответ нейросети (nn_text_response) пуст или отсутствует для chat_id {avito_chat_id_in}. Пропускаем отправку сообщения в Avito.")

        except Exception as e:
             # Логирование уже произошло внутри send_message_to_apimonster
             logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при отправке сообщения в Apimonster для chat_id {avito_chat_id_in}: {e}")
             # Ошибка отправки клиенту не должна прерывать работу с AmoCRM. Логируем и продолжаем.


        # --- 3. Работа с AmoCRM (поиск, создание, обновление) ---
        lead_id_for_response = None
        operation_status = ""

        avito_crm_id_to_search = avito_chat_id_in # Или avito_user_id_in

        # Поиск существующей сделки
        # find_lead_by_avito_id может бросить исключение при ошибках AmoCRM. Ловим его.
        try:
            current_lead_id = find_lead_by_avito_id(avito_crm_id_to_search)
        except Exception as e:
             logger.error(f"ФОНОВАЯ ЗАДАЧА: Критическая ошибка при поиске сделки в AmoCRM для Avito ID {avito_crm_id_to_search}: {e}")
             # Ошибка поиска AmoCRM критична для создания/обновления. Логируем и завершаем фоновую задачу.
             return


        if current_lead_id is not None:
            # --- СДЕЛКА СУЩЕСТВУЕТ -> ОБНОВЛЯЕМ ---
            lead_id_for_response = current_lead_id
            operation_status = "обновлено"
            logger.info(f"ФОНОВАЯ ЗАДАЧА: Обновление сделки {current_lead_id} для Avito ID {avito_crm_id_to_search}.")

            # Обновляем сделку
            try:
                 update_amo_lead(
                     lead_id=current_lead_id,
                     status_id=status_id_nn,
                     phone=phone_nn,
                     comment=comment_nn,
                     dialog_text=dialog_nn,
                     client_name=client_name_nn,
                     avito_id=avito_crm_id_to_search
                 )
            except Exception as e:
                 logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при обновлении сделки {current_lead_id} для Avito ID {avito_crm_id_to_search}: {e}", exc_info=True)
                 # Ошибка обновления не всегда критична. Логируем и продолжаем для добавления заметки.


            # Добавляем заметку
            try:
                note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
                # Можно также добавить информацию, что обработано ботом
                if nn_text_response:
                    note_content += f"\nОтвет бота отправлен." # Или добавить часть текста nn_text_response, если не очень длинный
                add_note_to_lead(current_lead_id, note_content)
            except Exception as e:
                 logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при добавлении заметки к сделке {current_lead_id}: {e}", exc_info=True)
                 # Ошибка добавления заметки не критична.


        else:
            # --- СДЕЛКА НЕ НАЙДЕНА -> СОЗДАЕМ НОВУЮ ---
            logger.info(f"ФОНОВАЯ ЗАДАЧА: Сделка для Avito ID {avito_crm_id_to_search} не найдена. Создание новой.")
            operation_status = "создано"

            # Создаем новую сделку
            try:
                 new_lead_id = create_amo_lead(
                     name=client_name_nn,
                     status_id=status_id_nn,
                     phone=phone_nn,
                     comment=comment_nn,
                     dialog_text=dialog_nn,
                     avito_id=avito_crm_id_to_search
                 )
                 lead_id_for_response = new_lead_id

                 logger.info(f"ФОНОВАЯ ЗАДАЧА: Создана новая сделка {new_lead_id} и связана с Avito ID {avito_crm_id_to_search}.")

                 # Добавляем заметку к новой сделке
                 try:
                     note_content = f"Входящее от клиента ({avito_user_id_in}): {message_text_in}"
                     if nn_text_response:
                        note_content += f"\nОтвет бота отправлен." # Или добавить часть текста nn_text_response
                     add_note_to_lead(new_lead_id, note_content)
                 except Exception as e:
                     logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при добавлении заметки к новой сделке {new_lead_id}: {e}", exc_info=True)
                     # Ошибка добавления заметки не критична.


            except Exception as e:
                 # Ошибка при создании сделки критична для этой ветви логики
                 logger.error(f"ФОНОВАЯ ЗАДАЧА: Ошибка при создании сделки для Avito ID {avito_crm_id_to_search}: {e}", exc_info=True)
                 # Завершаем фоновую задачу
                 return


        logger.info(f"ФОНОВАЯ ЗАДАЧА: Обработка для Avito ID {avito_crm_id_to_search} завершена. ID сделки: {lead_id_for_response}, Статус: {operation_status}")

    except Exception as e:
        # Логируем любые необработанные исключения в фоновой задаче
        logger.critical(f"ФОНОВАЯ ЗАДАЧА: Необработанная КРИТИЧЕСКАЯ ошибка в process_avito_message для chat_id {avito_chat_id_in}: {e}", exc_info=True)


# --- Главный обработчик Webhook ---

# Уточните реальные имена параметров, которые присылает промежуточная платформа!
AVITO_USER_ID_PARAM_IN = 'param2'
AVITO_CHAT_ID_PARAM_IN = 'param1'
AVITO_MESSAGE_PARAM_IN = 'param3'
# Если есть 4-й параметр и он нужен, укажите его имя:
# AVITO_FOURTH_PARAM_IN = 'fourth_param_name_in_webhook'


@app.post("/")
async def webhook_receiver(request: Request, background_tasks: BackgroundTasks): # Добавляем background_tasks как зависимость
    # Используем имена переменных для входящих данных
    avito_chat_id_in: str | None = None
    avito_user_id_in: str | None = None
    message_text_in: str | None = None
    # fourth_param_in: str | None = None


    logger.info("🚀 Пришел входящий webhook.")

    # --- 1. Базовая валидация входящего webhook (быстрый ответ) ---
    try:
        # Проверяем Content-Type
        content_type = request.headers.get('content-type', '').lower()
        if 'application/x-www-form-urlencoded' not in content_type and 'multipart/form-data' not in content_type:
             logger.error(f"Получен webhook с не-FORM/Multipart Content-Type: {content_type}")
             raise HTTPException(status_code=415, detail=f"Ожидается Content-Type: application/x-www-form-urlencoded или multipart/form-data, получен {content_type}")

        # Парсим данные формы
        incoming_data: FormData = await request.form()
        logger.info(f"Входящий webhook (FORM/Multipart) получен. Ключи данных: {str(list(incoming_data.keys()))[:300]}...") # Логируем только ключи входящих данных

        # Получаем значения по именам параметров из объекта Form Data
        # Используем .get() с None по умолчанию
        avito_user_id_in = incoming_data.get(AVITO_USER_ID_PARAM_IN)
        avito_chat_id_in = incoming_data.get(AVITO_CHAT_ID_PARAM_IN)
        message_text_in = incoming_data.get(AVITO_MESSAGE_PARAM_IN)
        # fourth_param_in = incoming_data.get(AVITO_FOURTH_PARAM_IN) # Если нужен


        # Проверка на наличие обязательных данных
        # Проверка также на то, что строка не пустая после strip()
        # Убедимся, что переменные не None перед strip() и что они строки
        if not avito_chat_id_in or not avito_user_id_in or not message_text_in or not isinstance(message_text_in, str) or not message_text_in.strip():
            missing_params = []
            if not avito_user_id_in or (isinstance(avito_user_id_in, str) and not avito_user_id_in.strip()): missing_params.append(AVITO_USER_ID_PARAM_IN)
            if not avito_chat_id_in or (isinstance(avito_chat_id_in, str) and not avito_chat_id_in.strip()): missing_params.append(AVITO_CHAT_ID_PARAM_IN)
            if not message_text_in or not isinstance(message_text_in, str) or not message_text_in.strip(): missing_params.append(AVITO_MESSAGE_PARAM_IN)

            logger.error(f"Входящий webhook (FORM/Multipart) не содержит все обязательные параметры или они пустые. Отсутствуют: {', '.join(missing_params)}. Полученные ключи: {list(incoming_data.keys())}")
            # Возвращаем 400 Bad Request, т.к. проблема во входящих данных
            raise HTTPException(status_code=400, detail=f"Отсутствуют обязательные параметры в webhook (FORM/Multipart): {', '.join(missing_params)}")

        # Убедимся, что user_id и chat_id точно строки
        avito_user_id_in = str(avito_user_id_in)
        avito_chat_id_in = str(avito_chat_id_in)
        # message_text_in уже проверен, что это не пустая строка

        logger.info(f"Входящий webhook: Базовая валидация успешна. user_id={avito_user_id_in}, chat_id={avito_chat_id_in}, message_text='{message_text_in[:100]}...'")

    except HTTPException as http_exc:
         # Если базовая валидация не пройдена, сразу возвращаем ошибку
         logger.error(f"Входящий webhook: Базовая валидация не пройдена. Ошибка: {http_exc.detail}")
         return {"status": "error", "message": http_exc.detail}, http_exc.status_code
    except Exception as e:
         logger.error(f"Входящий webhook: Неожиданная ошибка при базовой валидации: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Неожиданная ошибка при валидации входящих данных: {e}")


    # --- 2. Запланировать фоновую задачу для всей остальной логики ---
    # Передаем необходимые данные в фоновую функцию
    background_tasks.add_task(
        process_avito_message,
        avito_user_id_in,
        avito_chat_id_in,
        message_text_in
        # Если нужен fourth_param_in, передайте его сюда и добавьте в сигнатуру process_avito_message
    )
    logger.info(f"Фоновая задача для chat_id {avito_chat_id_in} запланирована.")

    # --- 3. Сразу вернуть быстрый ответ 200 OK ---
    # Это сигнализирует Apimonster'у, что webhook принят.
    return {"status": "ok", "message": "Processing in background"}


@app.get("/")
async def root():
    """Простой эндпоинт для проверки, что сервер запущен."""
    return {"message": "Сервер интеграции Avito-ProTalk-AmoCRM работает!"}

# --- Блок запуска ---
# if __name__ == "__main__":
#     # --- Запуск FastAPI сервера ---
#     import uvicorn
#     logger.info("Запуск FastAPI сервера...")
#     # В режиме разработки reload=True удобен, в продакшене лучше использовать без него и с менеджером процессов (gunicorn)
#     uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
