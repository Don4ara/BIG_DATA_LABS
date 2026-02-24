# Задание 3 — Docker + PostgreSQL + DBeaver


### 1. Вывод случайных строк

Выполните запрос, чтобы посмотреть на 5 случайных строк из логов:

```sql
SELECT * FROM user_logs ul 
ORDER BY RANDOM() 
LIMIT 5;
```

### 2. Исправление некорректных данных в колонке s_all_avg

Для начала исправим данные в отдельно взятой колонке `s_all_avg` (которая должна содержать среднюю активность). Проблема в том, что использован текстовый тип данных, а дробная часть отделена запятой, а не точкой.

**Чтобы заменить все запятые на точки в колонке s_all_avg:**

```sql
UPDATE user_logs SET s_all_avg = REPLACE(s_all_avg, ',', '.') WHERE s_all_avg LIKE '%,%';
```

**Чтобы преобразовать старые текстовые значения в новые числовые (REAL):**

```sql
ALTER TABLE user_logs ALTER COLUMN s_all_avg TYPE REAL USING s_all_avg::REAL;
```

### 3. Исправление некорректных данных в остальных столбцах

В таблице есть и другие колонки со средними значениями (`s_course_viewed_avg` и т.д.), которые также имеют текстовый тип `VARCHAR` и используют запятую в качестве разделителя вместо точки.

**Очистка данных от запятых (замена на точку) для остальных столбцов:**

```sql
UPDATE user_logs 

SET 
    s_course_viewed_avg = REPLACE(s_course_viewed_avg::text, ',', '.'),
    s_q_attempt_viewed_avg = REPLACE(s_q_attempt_viewed_avg::text, ',', '.'),
    s_a_course_module_viewed_avg = REPLACE(s_a_course_module_viewed_avg::text, ',', '.'),
    s_a_submission_status_viewed_avg = REPLACE(s_a_submission_status_viewed_avg::text, ',', '.')
WHERE 
    s_course_viewed_avg::text LIKE '%,%' OR 
    s_q_attempt_viewed_avg::text LIKE '%,%' OR 
    s_a_course_module_viewed_avg::text LIKE '%,%' OR 
    s_a_submission_status_viewed_avg::text LIKE '%,%';
```

**Изменение типа остальных колонок на числовой (REAL):**

*(Заметка: столбец `s_all_avg` уже имеет тип `REAL` из предыдущих шагов, поэтому мы игнорируем его)*

```sql
ALTER TABLE user_logs 
    ALTER COLUMN s_course_viewed_avg TYPE REAL USING NULLIF(s_course_viewed_avg::text, '')::REAL,
    ALTER COLUMN s_q_attempt_viewed_avg TYPE REAL USING NULLIF(s_q_attempt_viewed_avg::text, '')::REAL,
    ALTER COLUMN s_a_course_module_viewed_avg TYPE REAL USING NULLIF(s_a_course_module_viewed_avg::text, '')::REAL,
    ALTER COLUMN s_a_submission_status_viewed_avg TYPE REAL USING NULLIF(s_a_submission_status_viewed_avg::text, '')::REAL;
```

**Пример запроса — расчёт среднего значения общей активности:**

```sql
SELECT AVG(s_all_avg) AS average_activity FROM user_logs;
```



