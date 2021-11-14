import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from postgres_to_es.settings_parser import es_data


logger = logging.getLogger(__name__)


class ElasticsearchService:
    """ Class to work with Elasticsearch engine """
    def __init__(self, es_settings=es_data, index_name: str = "movies"):
        self.client = Elasticsearch(
            hosts=f"{es_settings.ETL_HOST}:{es_settings.ETL_PORT}"
        )
        self.index_name = index_name
        self.file_path = es_settings.ETL_FILE_PATH

    def create_index(self, ignore_http_response: int = 400) -> None:
        with open(self.file_path, 'r') as es_file:
            index_settings = json.load(es_file)
            self.client.indices.create(
                index=self.index_name,
                **index_settings,
                ignore=ignore_http_response
            )

    def migrate_data(self, data) -> None:
        print(data)
        lines, status = bulk(
            client=self.client, actions=data, index=self.index_name
        )
        print(lines, status)
        logger.info(f"Migrate data: {lines}")
