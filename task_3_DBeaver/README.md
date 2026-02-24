# Задание 3 — Docker + PostgreSQL + DBeaver

SELECT * FROM user_logs ul 
ORDER BY RANDOM() 
LIMIT 5;



UPDATE user_logs SET s_all_avg = REPLACE(s_all_avg, ',', '.') WHERE s_all_avg LIKE '%,%';