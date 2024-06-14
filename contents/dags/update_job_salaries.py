import os
import re
import mysql.connector


def parse_salary_range(salary_range):
    if 'Thoả thuận' in salary_range:
        return None, None, None
    elif 'Tới' in salary_range:
        max_salary = float(re.findall(r'\d+', salary_range)[0])
        return 0, max_salary, max_salary / 2
    else:
        salaries = re.findall(r'\d+\.?\d*', salary_range)
        min_salary = float(salaries[0])
        max_salary = float(salaries[1])
        avg_salary = (min_salary + max_salary) / 2
        return min_salary, max_salary, avg_salary


def update_job_salaries():
    conn = mysql.connector.connect(
        host=os.getenv("CRAWLER_DATABASE_HOST"),
        user=os.getenv("CRAWLER_DATABASE_USERNAME"),
        password=os.getenv("CRAWLER_DATABASE_PASSWORD"),
        database=os.getenv("CRAWLER_DATABASE_NAME")
    )
    cursor = conn.cursor()
    cursor.execute("SELECT id, salary_range FROM jobs")
    rows = cursor.fetchall()

    for row in rows:
        job_id, salary_range = row
        min_salary, max_salary, avg_salary = parse_salary_range(salary_range)

        cursor.execute("""
            UPDATE jobs
            SET min_salary = %s, max_salary = %s, avg_salary = %s
            WHERE id = %s
        """, (min_salary, max_salary, avg_salary, job_id))

    conn.commit()
    cursor.close()
    conn.close()
