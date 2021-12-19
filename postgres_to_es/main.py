import logging
from time import sleep

from postgres_to_es.services import (
    ElasticsearchService,
    PostgresConnector,
    PostgresCursor,
    PostgresService,
    my_state,
)
from postgres_to_es.settings_parser import app_data

logger = logging.getLogger(__name__)


def load_data(cursor, es_conn, film_work_ids: tuple) -> None:
    if film_work_ids:
        actions: list = []
        for film_work in cursor.get_film_work_instances(film_work_ids=film_work_ids):
            actions.append(film_work)
            if len(actions) == app_data.LIMIT:
                es_conn.migrate_data(actions=actions)
                actions.clear()
        else:
            if actions:
                es_conn.migrate_data(actions=actions)


def etl_data_migration(pg_cursor, es_conn):
    """Function to migrate data from Postgres to ElasticSearch"""
    """ set DB cursor """
    person_film_work_ids: list[str] = []
    genre_film_work_ids: list[str] = []
    cursor = PostgresService(cursor=pg_cursor, stater=my_state)

    """ get film works ids by persons """
    person_ids, person_updated_at = cursor.get_person_ids()
    if person_ids:
        person_film_work_ids: list[str] = cursor.get_person_film_work_ids(
            person_ids=person_ids
        )

    """ get film works ids by genres """
    genre_ids, genre_updated_at = cursor.get_genre_ids()
    if genre_ids:
        genre_film_work_ids: list[str] = cursor.get_genre_film_work_ids(
            genre_ids=genre_ids
        )

    """ get film works ids """
    film_work_ids, film_work_updated_at = cursor.get_film_work_ids()
    all_film_work_ids: set = set(
        film_work_ids + genre_film_work_ids + person_film_work_ids
    )
    load_data(cursor=cursor, es_conn=es_conn, film_work_ids=tuple(all_film_work_ids))
    """set states"""
    if person_updated_at:
        my_state.set_state(key="person_updated_at", value=person_updated_at)
    if genre_updated_at:
        my_state.set_state(key="genre_updated_at", value=genre_updated_at)
    if film_work_updated_at:
        my_state.set_state(key="film_work_updated_at", value=film_work_updated_at)


if __name__ == "__main__":
    while True:
        logger.info("Start migration")
        try:
            es_conn = ElasticsearchService()
            with PostgresConnector().connection as postgres_conn:
                with PostgresCursor(connection=postgres_conn).cursor as postgres_cur:
                    etl_data_migration(pg_cursor=postgres_cur, es_conn=es_conn)
        except Exception as e:
            logger.error(e)
        else:
            logger.info("Close connections")
            es_conn.client.transport.close()
            if not postgres_cur.closed:
                postgres_cur.close()
            if not postgres_conn.closed:
                postgres_conn.close()
        logger.info(f"Sleep for {app_data.FETCH_DELAY} seconds")
        sleep(app_data.FETCH_DELAY)
