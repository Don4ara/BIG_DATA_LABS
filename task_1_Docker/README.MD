# Задание 1 — Docker + PostgreSQL

## Описание

Проект разворачивает контейнер PostgreSQL 13, создаёт базу данных `user_logs_db` и загружает данные из CSV-файлов: логи пользователей и справочник кафедр, после преобразует в таблицы для БД.
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
| `SELECT COUNT(*) FROM user_logs;` | Количество записей в user_logs |
| `SELECT COUNT(*) FROM departments;` | Количество записей в departments |
| `SELECT * FROM user_logs LIMIT 10;` | Первые 10 записей логов |
| `SELECT * FROM departments;` | Все кафедры |
| `\q` | Выход из psql |

Вывод 10 записей с названием кафедры (JOIN):

```sql
SELECT ul.userid, ul.courseid, d.name AS department_name
FROM user_logs ul
JOIN departments d ON ul.Depart = d.id
LIMIT 10;
```

## Результаты

Было:
- Таблиц в БД: **1** (`user_logs`)
- Записей в таблице: **414 528**

Стало:
- Таблиц в БД: **2** (`user_logs`, `departments`)
- Записей в user_logs: **414 528**
- Записей в departments: **43**
- Тип поля Depart: **INTEGER** (ссылка на `departments.id`)