from typing import Optional, List, Dict
from .mixins import UpdatedMixin, IdMixin


class PersonSchema(IdMixin, UpdatedMixin):
    """ Schema describe updated Person in specific time """


class PersonFilmWorkSchema(IdMixin, UpdatedMixin):
    """ Schema describe Film work, where Person played """


class ESFilmWorkSchema(IdMixin):
    """ Schema describe Film work instance,
        which will migrate into ElasticSearch """
    imdb_rating: float
    genre: Optional[str] = None
    title: str
    description: str
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[Dict[str, str]]] = None
    writers: Optional[List[Dict[str, str]]] = None
