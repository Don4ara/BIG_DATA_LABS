# Задание 2 — Docker + PostgreSQL

## Быстрый старт

```bash
# 1. Скопировать .env.example и заполнить своими данными
cp .env.example .env

# 2. Собрать и запустить контейнер
docker-compose up --build -d

# 3. Проверить переменные окружения внутри контейнера
docker exec postgres_logs_db env

# 4. Проверить проброс порта
docker port postgres_logs_db

# 5. Подключиться к БД внутри контейнера
docker exec -it postgres_logs_db psql -U idrisov -d my_db_idrisov

# 6. Остановить и удалить данные (volumes)
docker-compose down -v
```

## Генерация SECRET_HASH

```bash
# MD5-хэш от имени (macOS)
echo -n "Имя" | md5

# MD5-хэш от имени (Linux)
echo -n "Имя" | md5sum
```
