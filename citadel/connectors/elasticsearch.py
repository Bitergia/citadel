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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Valerio Cosentino <valcos@bitergia.com>

import json
import logging
import urllib3

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers

from citadel.connector import Connector
from citadel.errors import ElasticError
from grimoirelab_toolkit.datetime import datetime_utcnow

urllib3.disable_warnings()


URL = 'https://admin:admin@localhost:9200'
INDEX_BASE = 'items_'
ALL_ITEMS = 'all_items'
ITEMS_TYPE = 'items'

SCROLL_TIME = '5m'
SCROLL_SIZE = 100

TIMEOUT = 3600
MAX_RETRIES = 50
RETRY_ON_TIMEOUT = True
VERIFY_CERTS = False

WRITE_BULK = 100


PERCEVAL_MAPPING = """
    {
      "mappings": {
        "items": {
            "dynamic": true,
            "properties": {
                "data" : {
                    "properties":{}
                }
            }
        }
      }
    }
    """

logger = logging.getLogger(__name__)


class ESConnector(Connector):

    def __init__(self, origin, timeout=TIMEOUT, max_retries=MAX_RETRIES,
                 retry_on_timeout=RETRY_ON_TIMEOUT, verify_certs=VERIFY_CERTS):
        self.origin = origin
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.verify_certs = verify_certs

        self.elasticsearch = Elasticsearch([origin], timeout=timeout, max_retries=max_retries,
                                           retry_on_timeout=retry_on_timeout, verify_certs=verify_certs)

    def create_index(self, index):
        """Create index if not exists"""

        mapping = json.loads(PERCEVAL_MAPPING)

        if self.elasticsearch.indices.exists(index=index):
            logger.info("Index %s already exists!", index)
            return

        res = self.elasticsearch.indices.create(index=index, body=mapping)

        if not res['acknowledged']:
            msg = "Index {} not created".format(index)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def set_alias(self, index):
        """Create alias for index"""

        res = self.elasticsearch.indices.update_aliases(
            {
                "actions": [
                    {"add": {"index": index, "alias": ALL_ITEMS}}
                ]
            }
        )

        if not res['acknowledged']:
            msg = "Alias {} not created".format(ALL_ITEMS)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def read(self, **kwargs):

        index = kwargs.get('index', None)

        if not index:
            index = ALL_ITEMS

        backend_name = kwargs.get('backend_name', None)
        backend_version = kwargs.get('backend_version', None)
        category = kwargs.get('category', None)
        origin = kwargs.get('origin', None)
        tag = kwargs.get('tag', None)
        timestamp = kwargs.get('timestamp', None)
        updated_on = kwargs.get('updated_on', None)

        filter_terms = []

        if backend_name:
            backend_name_fltr = self.__compose_filter_term("backend_name", backend_name)
            filter_terms.append(backend_name_fltr)

        if backend_version:
            backend_version_fltr = self.__compose_filter_term("backend_version", backend_version)
            filter_terms.append(backend_version_fltr)

        if category:
            category_fltr = self.__compose_filter_term("category", category)
            filter_terms.append(category_fltr)

        if origin:
            origin_fltr = self.__compose_filter_term("origin", origin)
            filter_terms.append(origin_fltr)

        if tag:
            tag_fltr = self.__compose_filter_term("tag", tag)
            filter_terms.append(tag_fltr)

        if timestamp:
            timestamp_fltr = self.__compose_filter_range("timestamp", timestamp)
            filter_terms.append(timestamp_fltr)

        if updated_on:
            updated_on_fltr = self.__compose_filter_range("updated_on", updated_on)
            filter_terms.append(updated_on_fltr)

        page = self.elasticsearch.search(
            index=index,
            scroll=SCROLL_TIME,
            size=SCROLL_SIZE,
            body={
                "query": {
                    "bool": {
                        "filter": filter_terms
                    }
                }
            }
        )

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']

        if scroll_size == 0:
            msg = "No data found for {}".format(index)
            logger.warning(msg)
            return

        while scroll_size > 0:
            for item in page['hits']['hits']:
                yield item['_source']

            page = self.elasticsearch.scroll(scroll_id=sid, scroll=SCROLL_TIME)
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

    def write(self, data, **kwargs):

        index = kwargs.get('index', None)

        if not index:
            index = self.__index_name()

        self.create_index(index)
        self.set_alias(index)

        to_process = []

        for item in data:
            to_process.append(item)

            if len(to_process) == WRITE_BULK:
                self.__process_items(index, to_process)
                to_process.clear()

        if to_process:
            self.__process_items(index, to_process)
            to_process.clear()

    def __compose_filter_term(self, attr_name, attr_value):
        filter = {
            "term": {
                attr_name: attr_value
            }
        }

        return filter

    def __compose_filter_range(self, attr_name, attr_value):
        filter = {
            "range": {
                attr_name: {
                    "gte": attr_value
                }
            }
        }

        return filter

    def __process_items(self, index, items):
        es_items = []

        for item in items:
            es_item = {
                '_index': index,
                '_type': ITEMS_TYPE,
                '_id': item['uuid'],
                '_source': item
            }

            es_items.append(es_item)

        errors = helpers.bulk(self.elasticsearch, es_items)[1]
        msg = "{} items written in {}".format(len(es_items), index)
        logger.info(msg)
        self.elasticsearch.indices.refresh(index=index)

        if errors:
            msg = "Items lost when writing to ES, index {}. Error {}".format(index, errors[0])
            logger.error(msg)
            raise ElasticError(cause=msg)

    def __index_name(self):
        current_time = datetime_utcnow().replace(tzinfo=None).strftime("%Y%m%d_%Hh%Mm%Ss")

        return INDEX_BASE + "_" + current_time
