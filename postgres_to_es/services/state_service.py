import abc
import json
from pathlib import Path
from typing import Any, Optional
from postgres_to_es.settings_parser import app_data


json_file_name: str = app_data.STATE_FILE_NAME
file_path: str = str(Path(__file__).resolve().parent)


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict = {}) -> None:
        """Сохранить состояние в постоянное хранилище"""
        print(f"{self.file_path}{json_file_name}")
        with open(f"{self.file_path}{json_file_name}", 'w', encoding='utf-8') \
                as storage:
            json.dump(state, storage, ensure_ascii=False, indent=4)

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        with open(f"{self.file_path}{json_file_name}", 'r', encoding='utf-8') \
                as storage:
            state: dict = json.load(storage)
            return state


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не
    перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или
    распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(state={key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state().get(key, None)


my_state = State(storage=JsonFileStorage(file_path=file_path))
# my_state.set_state(key="hello", value="world")
# print(my_state.get_state(key="hello"))
# print(my_state.get_state(key="bye"))
