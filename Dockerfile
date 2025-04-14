# Используем лёгкий образ Python
# Используем официальный минимальный образ Python 3.11 (slim-вариант для меньшего размера)
FROM python:3.11-alpine as package

# Метаданные образа
LABEL maintainer="OXYCRiNGE" \
      description="Сервис для загрузки данных с zakupki44fz.ru по расписанию" \
      version="1.0.0"

# Устанавливаем переменную окружения для Python: отключаем создание .pyc файлов и буферизацию вывода
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Задаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл со списком зависимостей и устанавливаем их
COPY requirements.txt .
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt

# Копируем весь исходный код в рабочую директорию
FROM package
COPY main.py .
COPY okpd2_code .

# Определяем команду запуска контейнера (замените "script.py" на имя вашего файла с основным кодом, если необходимо)
ENTRYPOINT python -m main