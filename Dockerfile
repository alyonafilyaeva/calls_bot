# Исходный образ: Python 3.11 (или 3.12), на linux x86_64
FROM python:3.11-slim

# Установим зависимости системы (если понадобятся)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем файлы проекта
COPY . /app

# Устанавливаем зависимости
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Запуск бота
CMD ["python", "bot.py"]
