import psycopg2
from utils import log_message
import os
from dotenv import load_dotenv


load_dotenv()

# Конфигурация
DB_CONFIG = {
    "dbname": os.loadenv("DB_NAME"),
    "user": os.loadenv("DB_USER"),
    "password": os.loadenv("DB_PASSWORD"),
    "host": os.loadenv("DB_HOST"),
    "port": os.loadenv("DB_PORT"),
}
BATCH_SIZE = os.loadenv("BATCH_SIZE")


def connect_to_scopus_db() -> Optional[psycopg2.extensions.connection]:
    """Подключение к базе данных с обработкой ошибок"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        log_message("Успешное подключение к БД Scopus")
        return conn
    except Exception as e:
        log_message(f"Ошибка подключения: {str(e)}", "ERROR")
        return None
    

def get_publications_batch(conn: psycopg2.extensions.connection, limit: int = BATCH_SIZE) -> List[Dict[str, str]]:
    """Получение пакета публикаций из Scopus"""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT id, doi FROM publication 
            WHERE doi IS NOT NULL AND doi != ''
            ORDER BY id
            LIMIT %s;
        """, (limit,))
        return [{"id": row[0], "doi": row[1]} for row in cursor.fetchall()]
    

