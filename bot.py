import logging
import time
import jwt
import os
import json
import aiohttp
import pandas as pd
import phonenumbers

from phonenumbers import timezone, carrier, geocoder
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram import F

# ------------------------------------------
# НАСТРОЙКИ — ЗАПОЛНИ СВОИ ДАННЫЕ
# ------------------------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")

# Загружаем сервисный аккаунт
SA = json.loads(os.getenv("SERVICE_ACCOUNT_JSON"))

SERVICE_ACCOUNT_ID = SA["service_account_id"]
KEY_ID = SA["id"]
PRIVATE_KEY = SA["private_key"]

CALL_DATA = pd.DataFrame(columns=["phone", "call_time", "duration"])


# ---------------------------------------------------
# IAM TOKEN
# ---------------------------------------------------
async def get_iam_token():
    now = int(time.time())

    payload = {
        "aud": "https://iam.api.cloud.yandex.net/iam/v1/tokens",
        "iss": SERVICE_ACCOUNT_ID,
        "iat": now,
        "exp": now + 360
    }

    jwt_token = jwt.encode(
        payload,
        PRIVATE_KEY,
        algorithm="PS256",
        headers={"kid": KEY_ID}
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://iam.api.cloud.yandex.net/iam/v1/tokens",
            json={"jwt": jwt_token}
        ) as resp:
            result = await resp.json()
            return result["iamToken"]


# ---------------------------------------------------
# YandexGPT
# ---------------------------------------------------
async def yandex_gpt(prompt: str) -> str:
    iam_token = await get_iam_token()

    headers = {
        "Authorization": f"Bearer {iam_token}",
        "Content-Type": "application/json"
    }

    data = {
        "modelUri": f"gpt://{YANDEX_FOLDER_ID}/yandexgpt/latest",
        "completionOptions": {
            "stream": False,
            "maxTokens": 700,
            "temperature": 0.2
        },
        "messages": [
            {
                "role": "system",
                "text": (
                    "Ты — модель, которая анализирует телефонные звонки.\n"
                    "У тебя есть список звонков: час, длительность, успех/неуспех.\n"
                    "Также у тебя есть оператор и часовой пояс абонента.\n\n"
                    "Задача:\n"
                    "1. Определи лучшее время для звонка.\n"
                    "2. Не учитывай часы, где длительность 0 — значит абонент не отвечал.\n"
                    "3. Учитывай рабочие часы региона (часовой пояс).\n"
                    "4. Учитывай особенности оператора (например: корпоративные номера чаще отвечают днём).\n"
                    "5. Дай итог в виде:\n"
                    "   • Лучшее время звонить\n"
                    "   • Часы, когда НЕ стоит звонить\n"
                    "   • Краткое объяснение\n"
                )
            },
            {"role": "user", "text": prompt}
        ]
    }

    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    async with aiohttp.ClientSession() as s:
        async with s.post(url, headers=headers, json=data) as r:
            out = await r.json()
            try:
                return out["result"]["alternatives"][0]["message"]["text"]
            except:
                return f"Ошибка ответа модели: {out}"


# ---------------------------------------------------
# TELEGRAM BOT
# ---------------------------------------------------
bot = Bot(BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer(
        "Привет! Загрузи Excel c полями:\n"
        "`phone`, `call_time`, `duration`\n\n"
        "После — напиши номер телефона, и я проанализирую звонки.\n"
    )


# ---------------------------------------------------
# ПРИЁМ EXCEL
# ---------------------------------------------------
@dp.message(F.document)
async def load_excel(message: types.Message):
    global CALL_DATA

    file = await bot.get_file(message.document.file_id)
    await bot.download_file(file.file_path, "calls.xlsx")

    try:
        df = pd.read_excel("calls.xlsx", usecols=["Время", "Номер клиента", "Длительность"], dtype={"Номер клиента": str})

        df.columns = ["call_time", "phone",  "duration"]

        preview = df.head().to_string()
        await message.answer(f"📌 Распознано (нужные колонки):\n```\n{preview}\n```")

        # Приводим названия колонок к нижнему регистру
        cols = {c.lower(): c for c in df.columns}

        # Возможные варианты колонок
        phone_cols = ["phone", "телефон", "номер клиента", "номер телефона"]
        time_cols = ["call_time", "время", "время звонка", "звонок"]
        dur_cols = ["duration", "длительность", "продолжительность", "сек", "секунды"]

        def find_col(possible, error_name):
            for p in possible:
                if p in cols:
                    return cols[p]
            raise Exception(f"Не найдена колонка: {error_name}")

        # Находим реальные названия колонок
        phone_col = find_col(phone_cols, "номер клиента")
        time_col = find_col(time_cols, "время")
        dur_col = find_col(dur_cols, "длительность")

        # Переименовываем в единый формат
        df = df.rename(columns={
            phone_col: "phone",
            time_col: "call_time",
            dur_col: "duration"
        })

        # Преобразования
        df["call_time"] = pd.to_datetime(df["call_time"], format="%H:%M:%S", errors="coerce")
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")

        CALL_DATA = df.dropna(subset=["call_time", "duration"])

        await message.answer("📁 Файл загружен! Обнаружены колонки:\n"
                             f"• Номер: {phone_col}\n"
                             f"• Время: {time_col}\n"
                             f"• Длительность: {dur_col}")

    except Exception as e:
        await message.answer("Ошибка обработки файла:\n" + str(e))


# ---------------------------------------------------
# АНАЛИЗ НОМЕРА
# ---------------------------------------------------
@dp.message(F.text)
async def analyze_number(message: types.Message):
    phone = message.text.strip()

    df = CALL_DATA[CALL_DATA["phone"].astype(str) == phone]

    if df.empty:
        await message.answer("❌ Нет данных по этому номеру.")
        return

    # 1. Определение часов, где нет ответа
    df["hour"] = df["call_time"].dt.hour
    unsuccessful_hours = sorted(df[df["duration"] == 0]["hour"].unique().tolist())
    short_hours = sorted(df[df["duration"] <= 10]["hour"].unique().tolist())
    success_hours = sorted(df[df["duration"] > 10]["hour"].unique().tolist())

    # 3. Определение часового пояса и оператора
    try:
        parsed = phonenumbers.parse(phone, "RU")

        tz_list = timezone.time_zones_for_number(parsed)
        tz = tz_list[0] if tz_list else "unknown"

        op = carrier.name_for_number(parsed, "ru")
        region = geocoder.description_for_number(parsed, "ru")
    except:
        tz = "unknown"
        op = "unknown"
        region = "unknown"

    call_records = df.to_dict(orient="records")

    prompt = f"""
Регион: {region}
Часовой пояс: {tz}

Список звонков:
{call_records}

Часы, когда абонент НЕ отвечал (длительность 0):
{unsuccessful_hours}

Часы, когда абоненту было не удобно говорить или отвечал автоответчик (длиетльность меньше 10 секунд):
{short_hours}

Часы, когда разговор состоялся:
{success_hours}

Проанализируй список звонков и определи по часам суток: когда номер чаще отвечает, когда чаще не отвечает,
лучшее время для звонка, время, когда звонить бессмысленно. 
Обрати внимание, если длительность звонка слишком короткая, то скорее всего абоненту неудобно говорить в это время.
Если есть данные о часовом поясе и операторе — учитывай их.
Имей в виду, что звонки совершаются по уральскому времени.
Сделай краткие рекомендации менеджеру по продажам, в какое время лучше звонить.
"""

    await message.answer("🔍 Анализирую звонки...")

    result = await yandex_gpt(prompt)

    await message.answer(f"""
    📞Номер: {phone}
    🏢Оператор: {op}
    🏙Регион: {region}
    🌍Часовой пояс: {tz}
        """)

    await message.answer("📊 *Результат анализа:*\n" + result)


# ---------------------------------------------------
# ЗАПУСК
# ---------------------------------------------------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())