from typing import Optional, List
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
from postgres_to_es.decorators import backoff
from psycopg2.extensions import connection as pg_connection
from postgres_to_es.pydantic_schemas.schemas import (
    PersonSchema,
    PersonFilmWorkSchema,
    ESFilmWorkSchema,
)
from postgres_to_es.pydantic_schemas.enums import PersonTypeEnum
from postgres_to_es.services.db_quaries import (
    query_updated_persons,
    query_film_works_by_persons,
    query_film_works_by_ids,
)
from postgres_to_es.settings_parser import pg_data
from datetime import datetime


class PostgresCursor:
    def __init__(self, cursor, itersize: int):
        self.cursor = cursor
        self.itersize = itersize

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchmany(self.itersize)

    def get_persons_ids(self, update_at: datetime) -> List[str]:
        """ Return List of Person ids """
        persons = self.execute(
            query=query_updated_persons(datetime_value=update_at)
        )
        return [
            PersonSchema(**person).id
            for person in persons
        ]

    def get_film_work_ids(self, person_ids: List[str]) -> List[str]:
        """ Return List of Film Work ids """
        person_film_works = self.execute(
            query=query_film_works_by_persons(person_ids=person_ids)
        )
        return [
            PersonFilmWorkSchema(**person_film_work).id
            for person_film_work in person_film_works
        ]

    def get_film_work_instances(self, film_work_ids: List[str]):
        """ Return Film Work instances to migrate it in ES """
        film_works = self.execute(
            query=query_film_works_by_ids(film_work_ids=film_work_ids)
        )
        print(len(film_works))
        transformed_data = []
        for film_work in film_works:
            new_film_work = {
                "id": film_work.get("fw_id"),
                "imdb_rating": film_work.get("rating"),
                "genre": film_work.get("name"),
                "title": film_work.get("title"),
                "description": film_work.get("description"),
            }
            person_name = film_work.get("full_name")
            person_instance = {
                "id": film_work.get("id"),
                "name": person_name
            }
            if film_work.get("role") == PersonTypeEnum.director:
                new_film_work["director"] = [person_name]
            elif film_work.get("role") == PersonTypeEnum.actor:
                new_film_work["actors_names"] = [person_name]
                new_film_work["actors"] = [person_instance]
            elif film_work.get("role") == PersonTypeEnum.writer:
                new_film_work["writers_names"] = [person_name]
                new_film_work["writers"] = [person_instance]
            instance = ESFilmWorkSchema(**new_film_work).dict()
            print(instance)
            transformed_data.append(new_film_work)
            yield instance, instance.get("updated_at")


class PostgresService:
    """ Class to work with Postgres Database """
    def __init__(self, pg_settings=pg_data):
        self.dsl = {
            "dbname": pg_settings.DB_NAME,
            "user": pg_settings.DB_USER,
            "password": pg_settings.DB_PASSWORD,
            "host": pg_settings.DB_HOST,
            "port": pg_settings.DB_PORT,
            "options": pg_settings.DB_OPTIONS,
        }
        self.conn: Optional[pg_connection] = None

    @property
    def conn_status(self):
        """Check connection, if closed, reconnect to DB and return"""
        if self.conn and not self.conn.closed:
            conn = self.conn
        else:
            conn = self.create_conn()
        return conn

    @backoff(exceptions=[OperationalError])
    def create_conn(self) -> pg_connection:
        """ Create connection with Postgres """
        return psycopg2.connect(
            **self.dsl, cursor_factory=DictCursor
        )
