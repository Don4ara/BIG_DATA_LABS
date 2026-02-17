# Задание 1 — Docker + PostgreSQL

## Описание

Проект разворачивает контейнер PostgreSQL 13, создаёт базу данных `user_logs_db` и загружает в неё данные из CSV-файла (`aggrigation_logs_per_week.csv`).

## Запуск

```bash
docker compose up --build -d
```

## Работа с контейнером

Войти в контейнер:

```bash
docker exec -it postgres_logs_db bash
```

Подключиться к БД:

```bash
psql -U root -d user_logs_db
```

## Команды psql

| Команда | Описание |
|---|---|
| `\dt` | Список таблиц |
| `SELECT COUNT(*) FROM user_logs;` | Количество записей |
| `SELECT * FROM user_logs LIMIT 10;` | Первые 10 записей |
| `\q` | Выход из psql |

## Результаты

- Таблиц в БД: **1** (`user_logs`)
- Записей в таблице: **414 528**