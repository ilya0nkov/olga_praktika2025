import time
from dotenv import load_dotenv
import os
from utils import log_message
from database import *
from parser import process_publication, save_results, analyze_results


load_dotenv()

REQUEST_DELAY = os.loadenv("REQUEST_DELAY")


def main():
    """Основная функция выполнения"""
    log_message("Запуск обработки Scopus -> OpenAlex")
    conn = connect_to_scopus_db()
    if not conn:
        return

    results = []

    try:
        publications = get_publications_batch(conn)
        if not publications:
            log_message("Нет публикаций с DOI для обработки", "WARNING")
            return

        log_message(f"Начата обработка {len(publications)} публикаций")

        for pub in publications:
            result = process_publication(conn, pub)
            results.append(result)
            time.sleep(REQUEST_DELAY)

            # Периодическое сохранение промежуточных результатов
            if len(results) % 10 == 0:
                save_results([r for r in results if r], f"partial_{len(results)}.json")

        # Фильтрация None результатов перед сохранением
        valid_results = [r for r in results if r]
        save_results(valid_results)
        analyze_results(valid_results)

    except KeyboardInterrupt:
        log_message("Обработка прервана пользователем", "WARNING")
        if results:
            save_results([r for r in results if r], "interrupted_results.json")
    except Exception as e:
        log_message(f"Критическая ошибка: {str(e)}", "ERROR")
    finally:
        conn.close()
        log_message("Работа завершена")


if __name__ == "__main__":
    main()