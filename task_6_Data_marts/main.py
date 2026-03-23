"""
 Требуется создать отдельную схему dmr  (Data Mart Repository) для аналитических данных и 
 разместить в ней витрину analytics_student_performancee.

 Требования:
- Создать схему dmr если она не существует
- Создать витрину dmr.analytics_student_performance с агрегированными данными.
- Реализация через функции

Структура витрины: 
Поле	- Тип данных	- Описание
student_id	- INTEGER	- ID студента
course_id -	INTEGER	ID - курса
department_id -	INTEGER	- Код кафедры
department_name	 - VARCHAR - Название кафедры
education_level	- VARCHAR	- Уровень образования
education_base - VARCHAR -	Основа обучения
semester	- INTEGER	- Номер семестра
course_year	- INTEGER	- Курс обучения
final_grade -	INTEGER -	Итоговая оценка
total_events -	INTEGER	- Всего событий за семестр
avg_weekly_events	- DECIMAL(10,2)	- Среднее событий в неделю
total_course_views	- INTEGER	- Всего просмотров курса
total_quiz_views	- INTEGER	- Всего просмотров тестов
total_module_views -	INTEGER - Всего просмотров модулей
total_submissions	- INTEGER	- Всего отправленных заданий
peak_activity_week	- INTEGER	- Неделя с максимальной активностью
consistency_score	- DECIMAL(5,2)	- Коэффициент стабильности активности (0-1)
activity_category	- VARCHAR	- Категория активности (низкая/средняя/высокая)
last_update	- TIMESTAMP	- Дата обновления записи

"""


# Ниже представлен пример реализации витрины dmr.analytics_student
# Поле	- Тип данных	- Описание
# student_id	- INTEGER	- ID студента
# course_id -	INTEGER	ID - курса
# department_id -	INTEGER	- Код кафедры
# semester	- INTEGER	- Номер семестра
# course_year	- INTEGER	- Курс обучения
# final_grade -	INTEGER -	Итоговая оценка
# last_update	- TIMESTAMP	- Дата обновления записи

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Загружаем переменные из .env файла в текущей директории
load_dotenv()

# получение параметров подключения
def get_db_config():
    """
    Формирует словарь с параметрами подключения к БД.    
    """
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'educational_portal'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }  
    print (config)
    return config

# подключение к БД
def get_connection():
    """Устанавливает и возвращает соединение с БД."""
    try:
        config = get_db_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

# создание нового слоя в БД (схема dmr)
def create_schema(conn):
    """Создаёт схему dmr, если она ещё не существует."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dmr;")
        conn.commit()
        print("Схема dmr успешно создана (или уже существовала).")

# создание таблицы для витрины
def create_table(conn):
    """Создаёт таблицу dmr.analytics_student_performance с заданной структурой."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dmr.analytics_student_performance (
        student_id         INTEGER NOT NULL,
        course_id          INTEGER NOT NULL,
        department_id      INTEGER,
        department_name    VARCHAR(255),
        education_level    VARCHAR(255),
        education_base     VARCHAR(255),
        semester           INTEGER,
        course_year        INTEGER,
        final_grade        INTEGER,
        total_events       INTEGER,
        avg_weekly_events  DECIMAL(10,2),
        total_course_views INTEGER,
        total_quiz_views   INTEGER,
        total_module_views INTEGER,
        total_submissions  INTEGER,
        peak_activity_week INTEGER,
        consistency_score  DECIMAL(5,2),
        activity_category  VARCHAR(50),
        last_update        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (student_id, course_id)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
        print("Таблица dmr.analytics_student_performance успешно создана.")

# заполнение таблицы dmr.analytics_student_performance
def insert_data(conn):
    """
    Заполняет витрину данными из public.user_logs и public.departments.
    """
    select_query = """
    WITH week_activity AS (
        SELECT userid, courseid, num_week, COALESCE(s_all, 0) as s_all,
               ROW_NUMBER() OVER(PARTITION BY userid, courseid ORDER BY COALESCE(s_all, 0) DESC, num_week ASC) as rn
        FROM public.user_logs
    ),
    peak_weeks AS (
        SELECT userid, courseid, num_week
        FROM week_activity
        WHERE rn = 1
    ),
    student_stats AS (
        SELECT 
            ul.userid AS student_id,
            ul.courseid AS course_id,
            MAX(ul.Depart) AS department_id,
            MAX(d.name) AS department_name,
            MAX(ul.LevelEd) AS education_level,
            MAX(ul.Name_OsnO) AS education_base,
            MAX(ul.Num_Sem) AS semester,
            MAX(ul.Kurs) AS course_year,
            MAX(CASE WHEN ul.NameR_Level IN ('2','3','4','5') THEN CAST(ul.NameR_Level AS INTEGER) ELSE NULL END) AS final_grade,
            SUM(COALESCE(ul.s_all, 0)) AS total_events,
            ROUND(AVG(COALESCE(ul.s_all, 0)), 2) AS avg_weekly_events,
            SUM(COALESCE(ul.s_course_viewed, 0)) AS total_course_views,
            SUM(COALESCE(ul.s_q_attempt_viewed, 0)) AS total_quiz_views,
            SUM(COALESCE(ul.s_a_course_module_viewed, 0)) AS total_module_views,
            SUM(COALESCE(ul.s_a_submission_status_viewed, 0)) AS total_submissions,
            COUNT(NULLIF(COALESCE(ul.s_all, 0), 0)) * 1.0 / NULLIF(COUNT(ul.num_week), 0) AS raw_consistency
        FROM public.user_logs ul
        LEFT JOIN public.departments d ON ul.Depart = d.id
        GROUP BY ul.userid, ul.courseid
    )
    SELECT 
        s.student_id,
        s.course_id,
        s.department_id,
        s.department_name,
        s.education_level,
        s.education_base,
        s.semester,
        s.course_year,
        s.final_grade,
        s.total_events,
        s.avg_weekly_events,
        s.total_course_views,
        s.total_quiz_views,
        s.total_module_views,
        s.total_submissions,
        p.num_week AS peak_activity_week,
        ROUND(CAST(COALESCE(s.raw_consistency, 0) AS NUMERIC), 2) AS consistency_score,
        CASE 
            WHEN s.avg_weekly_events < 5 THEN 'низкая'
            WHEN s.avg_weekly_events < 20 THEN 'средняя'
            ELSE 'высокая'
        END AS activity_category
    FROM student_stats s
    LEFT JOIN peak_weeks p ON s.student_id = p.userid AND s.course_id = p.courseid;
    """

    insert_query = sql.SQL("""
        INSERT INTO dmr.analytics_student_performance 
        (student_id, course_id, department_id, department_name, education_level, education_base, 
         semester, course_year, final_grade, total_events, avg_weekly_events, total_course_views, 
         total_quiz_views, total_module_views, total_submissions, peak_activity_week, 
         consistency_score, activity_category)
        VALUES %s
        ON CONFLICT (student_id, course_id) 
        DO UPDATE SET
            department_id      = EXCLUDED.department_id,
            department_name    = EXCLUDED.department_name,
            education_level    = EXCLUDED.education_level,
            education_base     = EXCLUDED.education_base,
            semester           = EXCLUDED.semester,
            course_year        = EXCLUDED.course_year,
            final_grade        = EXCLUDED.final_grade,
            total_events       = EXCLUDED.total_events,
            avg_weekly_events  = EXCLUDED.avg_weekly_events,
            total_course_views = EXCLUDED.total_course_views,
            total_quiz_views   = EXCLUDED.total_quiz_views,
            total_module_views = EXCLUDED.total_module_views,
            total_submissions  = EXCLUDED.total_submissions,
            peak_activity_week = EXCLUDED.peak_activity_week,
            consistency_score  = EXCLUDED.consistency_score,
            activity_category  = EXCLUDED.activity_category,
            last_update        = CURRENT_TIMESTAMP;
    """)

    with conn.cursor() as cur:
        cur.execute(select_query)
        rows = cur.fetchall()
        
        if not rows:
            print("Нет данных для вставки.")
            return
        
        data_tuples = [
            (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
             row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
             row[16], row[17]) for row in rows
        ]
        execute_values(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()        
        print(f"Витрина заполнена. Добавлено/обновлено записей: {cur.rowcount}")

def main():
    """Последовательное выполнение шагов."""
    conn = None
    try:
        conn = get_connection()
        create_schema(conn)
        create_table(conn)
        insert_data(conn)
        print("\nВсе операции выполнены успешно!")
    except Exception as e:
        print(f"Ошибка в процессе выполнения: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Соединение с БД закрыто.")

if __name__ == "__main__":
    main()