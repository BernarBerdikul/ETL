from typing import Optional, List, Dict
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor
from postgres_to_es.decorators import backoff
from psycopg2.extensions import connection as pg_connection
from postgres_to_es.pydantic_schemas.schemas import (
    PersonSchema,
    PersonFilmWorkSchema,
    ESFilmWorkSchema, GenreFilmWorkSchema, GenreSchema, FilmWorkSchema,
)
from postgres_to_es.pydantic_schemas.enums import PersonTypeEnum
from postgres_to_es.services.db_quaries import (
    query_updated_persons,
    query_film_works_by_persons,
    query_film_works_by_ids, query_updated_genres, query_film_works_by_genres,
    query_updated_film_works,
)
from postgres_to_es.settings_parser import pg_data, app_data


class PostgresCursor:
    def __init__(self, cursor, stater):
        self.cursor = cursor
        self.stater = stater

    def execute(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_film_work_ids(self) -> List[str]:
        """ Return List of Person ids """
        """ get state """
        key: str = f"film_work_{app_data.STATE_FIELD}"
        updated_at: str = self.stater.get_state(key=key)
        """ get persons """
        film_works = self.execute(
            query=query_updated_film_works(datetime_value=updated_at)
        )
        if film_works:
            """ set state """
            self.stater.set_state(
                key=key,
                value=str(film_works[-1].get(app_data.STATE_FIELD))
            )
            return [
                FilmWorkSchema(**film_work).id
                for film_work in film_works
            ]

    def get_person_ids(self) -> List[str]:
        """ Return List of Person ids """
        """ get state """
        key: str = f"person_{app_data.STATE_FIELD}"
        updated_at: str = self.stater.get_state(key=key)
        """ get persons """
        persons = self.execute(
            query=query_updated_persons(datetime_value=updated_at)
        )
        if persons:
            """ set state """
            self.stater.set_state(
                key=key,
                value=str(persons[-1].get(app_data.STATE_FIELD))
            )
            return [
                PersonSchema(**person).id
                for person in persons
            ]

    def get_person_film_work_ids(self, person_ids: List[str]) -> List[str]:
        """ Return List of Film Work ids """
        """ get person's film works """
        person_film_works = self.execute(
            query=query_film_works_by_persons(person_ids=person_ids)
        )
        return [
            PersonFilmWorkSchema(**person_film_work).id
            for person_film_work in person_film_works
        ]

    def get_genre_ids(self) -> List[str]:
        """ Return List of Person ids """
        """ get state """
        key: str = f"genre_{app_data.STATE_FIELD}"
        updated_at: str = self.stater.get_state(key=key)
        """ get genres """
        genres = self.execute(
            query=query_updated_genres(datetime_value=updated_at)
        )
        if genres:
            """ set state """
            self.stater.set_state(
                key=key,
                value=str(genres[-1].get(app_data.STATE_FIELD))
            )
            return [
                GenreSchema(**genre).id
                for genre in genres
            ]

    def get_genre_film_work_ids(self, genre_ids: List[str]) -> List[str]:
        """ Return List of Film Work ids """
        """ get genre's film works """
        genre_film_works = self.execute(
            query=query_film_works_by_genres(genre_ids=genre_ids)
        )
        return [
            GenreFilmWorkSchema(**genre_film_work).id
            for genre_film_work in genre_film_works
        ]

    def get_film_work_instances(self, film_work_ids: List[str]):
        """ Return Film Work instances to migrate it in ES """
        film_works = self.execute(
            query=query_film_works_by_ids(
                film_work_ids=film_work_ids
            )
        )
        """ get unique ids of film works """
        film_work_ids: set = {
            film_work.get("fw_id") for film_work in film_works
        }

        transformed_data: list = []
        """ combine duplicates rows """
        for film_work_id in film_work_ids:
            genres: Optional[List[str]] = []
            directors: Optional[List[str]] = []
            actors_names: Optional[List[str]] = []
            writers_names: Optional[List[str]] = []
            actors: Optional[List[Dict[str, str]]] = []
            writers: Optional[List[Dict[str, str]]] = []
            for film_work in film_works:
                if film_work.get("fw_id") == film_work_id:
                    imdb_rating = film_work.get("rating")
                    title = film_work.get("title")
                    description = film_work.get("description")
                    """ group film work genres """
                    if film_work.get("name") not in genres:
                        genres.append(film_work.get("name"))
                    person_name = film_work.get("full_name")
                    person_instance = {
                        "id": film_work.get("id"),
                        "name": person_name
                    }
                    """ group film work directors, actors, writers """
                    if film_work.get("role") == PersonTypeEnum.director:
                        """ group director's names """
                        if person_name not in directors:
                            directors.append(person_name)
                    elif film_work.get("role") == PersonTypeEnum.actor:
                        """ group actor's names """
                        if person_name not in actors_names:
                            actors_names.append(person_name)
                        """ group actor's instances """
                        if person_instance not in actors:
                            actors.append(person_instance)
                    elif film_work.get("role") == PersonTypeEnum.writer:
                        """ group writer's names """
                        if person_name not in writers_names:
                            writers_names.append(person_name)
                        """ group writer's instances """
                        if person_instance not in writers:
                            writers.append(person_instance)
            new_film_work = {
                "id": film_work_id,
                "imdb_rating": imdb_rating,
                "title": title,
                "description": description,
                "genre": genres,
                "director": directors,
                "actors_names": actors_names,
                "writers_names": writers_names,
                "actors": actors,
                "writers": writers
            }
            transformed_data.append(new_film_work)
        """ generate new_film_work data """
        for film_work in transformed_data:
            yield ESFilmWorkSchema(**film_work).dict()


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
    def connection(self):
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
