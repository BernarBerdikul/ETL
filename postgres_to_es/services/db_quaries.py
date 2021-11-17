from typing import Union

from postgres_to_es.settings_parser import app_data


def query_part_for_where(data: Union[list, tuple]) -> str:
    """Query constructor for WHERE operator"""
    if len(data) > 1:
        return f"IN {tuple(data)}"
    else:
        return f"= '{data[0]}'"


def query_updated_film_works(datetime_value) -> str:
    """Return updated film works, after specific datetime"""
    return f"""
        SELECT id, updated_at
        FROM content.film_work
        WHERE updated_at > '{datetime_value}'
        ORDER BY updated_at
        LIMIT {app_data.LIMIT};
    """


def query_updated_genres(datetime_value) -> str:
    """Return updated genres, after specific datetime"""
    return f"""
        SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > '{datetime_value}'
        ORDER BY updated_at
        LIMIT {app_data.LIMIT};
    """


def query_film_works_by_genres(genre_ids: list) -> str:
    """Return film_works by genres"""
    query_part = query_part_for_where(data=genre_ids)
    return f"""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE gfw.genre_id {query_part}
        ORDER BY fw.updated_at;
    """


def query_updated_persons(datetime_value) -> str:
    """Return updated persons, after specific datetime"""
    return f"""
        SELECT id, updated_at
        FROM content.person
        WHERE updated_at > '{datetime_value}'
        ORDER BY updated_at
        LIMIT {app_data.LIMIT};
    """


def query_film_works_by_persons(person_ids: list) -> str:
    """Return film_works, where persons were played"""
    query_part = query_part_for_where(data=person_ids)
    return f"""
        SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id {query_part}
        ORDER BY fw.updated_at;
    """


def query_film_works_by_ids(film_work_ids: tuple) -> str:
    """Return film works by film work ids
    where updated persons were played"""
    query_part = query_part_for_where(data=film_work_ids)
    return f"""
        SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created_at,
            fw.updated_at,
            pfw.role,
            p.id,
            p.full_name,
            g.name
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id {query_part};
    """
