from typing import Dict, List, Optional

from .mixins import IdMixin, UpdatedMixin


class FilmWorkSchema(IdMixin, UpdatedMixin):
    """Schema describe updated Film Work in specific time"""


class GenreSchema(IdMixin, UpdatedMixin):
    """Schema describe updated Genre in specific time"""


class GenreFilmWorkSchema(IdMixin, UpdatedMixin):
    """Schema describe Film work by Genre"""


class PersonSchema(IdMixin, UpdatedMixin):
    """Schema describe updated Person in specific time"""


class PersonFilmWorkSchema(IdMixin, UpdatedMixin):
    """Schema describe Film work, where Person played"""


class ESFilmWorkSchema(IdMixin):
    """Schema describe Film work instance,
    which will migrate into ElasticSearch"""

    imdb_rating: Optional[float] = None
    genre: Optional[List[str]] = None
    title: str
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[Dict[str, str]]] = None
    writers: Optional[List[Dict[str, str]]] = None
