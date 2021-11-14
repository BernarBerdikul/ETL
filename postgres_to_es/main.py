import logging
from time import sleep
from datetime import datetime, timedelta
from typing import List
from postgres_to_es.services import (
    PostgresService,
    PostgresCursor,
    ElasticsearchService,
    my_state
)
from postgres_to_es.settings_parser import app_data


logger = logging.getLogger(__name__)


def etl_data_migration(pg_cursor):
    cursor = PostgresCursor(
        cursor=pg_cursor, itersize=app_data.LIMIT
    )

    update_at = my_state.get_state(app_data.STATE_FIELD)
    person_ids: List[str] = cursor.get_persons_ids(update_at=update_at)

    print(person_ids[:10])
    film_work_ids:  List[str] = cursor.get_film_work_ids(person_ids=person_ids)
    print(film_work_ids[:10])

    film_works, state_time = cursor.get_film_work_instances(film_work_ids=film_work_ids)

    print(film_works)

    my_state.set_state(
        key=app_data.STATE_FIELD,
        value=state_time
    )


if __name__ == "__main__":
    es_conn = ElasticsearchService()
    while True:
        logger.info("Start migration")
        try:
            with PostgresService().conn_status as postgres_conn:
                etl_data_migration(pg_cursor=postgres_conn.cursor())
        except Exception as e:
            logger.error(e)
        finally:
            postgres_conn.close()
        logger.info(f"Sleep for {app_data.FETCH_DELAY} seconds")
        print(app_data.FETCH_DELAY)
        sleep(app_data.FETCH_DELAY)
