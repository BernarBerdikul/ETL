from enum import Enum


class PersonTypeEnum(str, Enum):
    actor = "actor"
    director = "director"
    writer = "writer"


class FilmWorkTypeEnum(str, Enum):
    movie = "movie"
    tv_show = "tv_show"
