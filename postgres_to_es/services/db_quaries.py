from postgres_to_es.settings_parser import app_data


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
    return f"""
    SELECT fw.id, fw.updated_at
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    WHERE pfw.person_id IN {tuple(person_ids)}
    ORDER BY fw.updated_at
    LIMIT {app_data.LIMIT}; 
    """


def query_film_works_by_ids(film_work_ids: list) -> str:
    """ Return film works by film work ids
        where updated persons were played """
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
    WHERE fw.id IN {tuple(film_work_ids)};
    """
