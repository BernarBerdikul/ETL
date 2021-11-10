import psycopg2
import os
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


def etl_data_migration(pg_connection: _connection):
    postgres_cursor = pg_connection.cursor()
    now = datetime(2021, 6, 17).isoformat()
    sql_query: str = f"""
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > '{now}'
        ORDER BY updated_at
        LIMIT 100;
    """
    postgres_cursor.execute(sql_query)
    persons = postgres_cursor.fetchall()
    print(persons)
    print(len(persons))
    ids: tuple = ("0031feab-8f53-412a-8f53-47098a60ac73", "009a900e-b9dc-4cd4-87a7-ca53d1b7dd24")
    sql_query2 = f"""
    SELECT fw.id, fw.updated_at
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    WHERE pfw.person_id IN {ids}
    ORDER BY fw.updated_at
    LIMIT 100; 
    """
    postgres_cursor.execute(sql_query2)
    film_works = postgres_cursor.fetchall()
    print(film_works)
    print(len(film_works))
    # person_ids: tuple = ('01cd80e2-5db8-4914-9a80-74f15a3a1a24',)
    # sql_query3 = f"""
    # SELECT
    #     fw.id as fw_id,
    #     fw.title,
    #     fw.description,
    #     fw.rating,
    #     fw.type,
    #     fw.created_at,
    #     fw.updated_at,
    #     pfw.role,
    #     p.id,
    #     p.full_name,
    #     g.name
    # FROM content.film_work fw
    # LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    # LEFT JOIN content.person p ON p.id = pfw.person_id
    # LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    # LEFT JOIN content.genre g ON g.id = gfw.genre_id
    # WHERE fw.id IN {person_ids};
    # """
    # print(postgres_cursor.execute(sql_query3))


if __name__ == "__main__":
    dsl = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": os.getenv("DB_PORT", 5432),
        "options": "-c search_path=content",
    }
    pg_conn = psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    )
    try:
        with pg_conn:
            etl_data_migration(pg_conn)
    except Exception as e:
        raise e
    finally:
        pg_conn.close()
