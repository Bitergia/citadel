# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>
#

import json
import logging
import urllib3

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
import elasticsearch.exceptions as es_exceptions

from citadel.storage_engine import StorageEngine
from citadel.errors import StorageEngineError

urllib3.disable_warnings()


PERCEVAL_MAPPING = """
    {
      "mappings": {
        "items": {
            "dynamic": false,
            "properties": {
                "backend_name" : {
                    "type" : "keyword"
                },
                "backend_version" : {
                    "type" : "keyword"
                },
                "category" : {
                    "type" : "keyword"
                },
                "classified_fields_filtered" : {
                    "type" : "keyword"
                },
                "data" : {
                    "properties":{}
                },
                "origin" : {
                    "type" : "keyword"
                },
                "perceval_version" : {
                    "type" : "keyword"
                },
                "tag" : {
                    "type" : "keyword"
                },
                "timestamp" : {
                    "type" : "long"
                },
                "updated_on" : {
                    "type" : "long"
                },
                "uuid" : {
                    "type" : "keyword"
                }
            }
        }
      }
    }
    """

logger = logging.getLogger(__name__)


class ElasticsearchStorage(StorageEngine):
    """ElasticSearch storage engine.

    This class allows to handle data stored in an ElasticSearch database.

    :param url: ElasticSearch URL
    """
    ITEMS = 'items'

    TIMEOUT = 3600
    MAX_RETRIES = 50
    RETRY_ON_TIMEOUT = True
    VERIFY_CERTS = False

    CHUNK_SIZE = 100

    def __init__(self, url):
        self.url = url
        self.elasticsearch = Elasticsearch([url],
                                           timeout=self.TIMEOUT,
                                           max_retries=self.MAX_RETRIES,
                                           retry_on_timeout=self.RETRY_ON_TIMEOUT,
                                           verify_certs=self.VERIFY_CERTS,
                                           connection_class=RequestsHttpConnection)

    def create_index(self, index_name, mapping):
        """Create an index in ElasticSearch database.

        :param index_name: name of the index
        :param mapping: mapping defined as a string

        :raises StorageEngineError: raised when the index already
            exists or it cannot be created
        """
        json_mapping = json.loads(mapping)

        if self.elasticsearch.indices.exists(index=index_name):
            msg = "Index {} already exists!".format(index_name)
            logger.error(msg)
            raise StorageEngineError(cause=msg)

        try:
            self.elasticsearch.indices.create(index=index_name, body=json_mapping)
        except es_exceptions.RequestError as ex:
            info_error = ex.info['error']
            msg = "Index {} not created, {}, {}".format(index_name, info_error['type'], info_error['reason'])
            logger.error(msg)
            raise StorageEngineError(cause=msg)

    def write(self, resource, data, item_type=ITEMS, chunk_size=CHUNK_SIZE, field_id=None):
        """Write the `data` in bulks of `chunk_size` to the index `resource`.

        :param resource: index name
        :param data: data to be written
        :param item_type: type of the item
        :param chunk_size: chunk size data
        :param field_id: field representing the ID of the item. If None the
            ID generation is delegated to ElasticSearch

        :return: number of written items

        :raises StorageEngineError: raised when an
            item is not written to the index
        """
        def stream_items(items):
            for item in items:
                es_item = {
                    '_index': resource,
                    '_type': item_type,
                    '_source': item
                }

                if field_id:
                    es_item['_id'] = item[field_id]

                yield es_item

        written = 0
        for ok, item in helpers.streaming_bulk(self.elasticsearch, actions=stream_items(data),
                                               chunk_size=chunk_size, raise_on_error=False):
            if ok:
                written += 1
            else:
                info_error = item['index']['error']
                msg = "Bulk error on {}, {}, {}".format(resource, info_error['type'], info_error['reason'])
                raise StorageEngineError(cause=msg)

        self.elasticsearch.indices.refresh(resource)
        return written
