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
urllib3.disable_warnings()

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers

from citadel.writer import Writer
from citadel.errors import ElasticError


URL = 'https://admin:admin@localhost:9200'
INDEX = 'xyz_index'
ALIAS = 'whatever'
ITEMS_TYPE = 'items'

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


class ESWriter(Writer):

    def __init__(self, url=URL, index=INDEX, alias=ALIAS, timeout=TIMEOUT,
                 max_retries=MAX_RETRIES, retry_on_timeout=RETRY_ON_TIMEOUT,
                 verify_certs=VERIFY_CERTS):
        super().__init__()
        self.url = url
        self.index = index
        self.alias = alias
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.verify_certs = verify_certs

        self.conn = Elasticsearch([url], timeout=timeout, max_retries=max_retries,
                                  retry_on_timeout=retry_on_timeout, verify_certs=verify_certs)

        self.create_index()
        self.set_alias()

    def create_index(self):
        """Create index if not exists"""

        mapping = json.loads(PERCEVAL_MAPPING)

        if self.conn.indices.exists(index=self.index):
            logger.info("Index %s already exists!", self.index)
            return

        res = self.conn.indices.create(index=self.index, body=mapping)

        if not res['acknowledged']:
            msg = "Index {} not created".format(self.index)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def set_alias(self):
        """Create alias for index"""

        res = self.conn.indices.update_aliases(
            {
                "actions": [
                    {"add": {"index": self.index, "alias": self.alias}}
                ]
            }
        )

        if not res['acknowledged']:
            msg = "Alias {} not created".format(self.alias)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def write(self, items):

        to_process = []

        for item in items:
            to_process.append(item)

            if len(to_process) == WRITE_BULK:
                self.__process_items(items)
                to_process.clear()

        if to_process:
            self.__process_items(to_process)
            to_process.clear()

    def __process_items(self, items):
        es_items = []

        for item in items:
            es_item = {
                '_index': self.index,
                '_type': ITEMS_TYPE,
                '_id': item['uuid'],
                '_source': item
            }

            es_items.append(es_item)

        errors = helpers.bulk(self.conn, es_items)[1]
        msg = "{} items written in {}".format(len(es_items), self.index)
        logger.info(msg)
        self.conn.indices.refresh(index=self.index)

        if errors:
            msg = "Lost items from Redis to ES, index {}. Error {}".format(self.index, errors[0])
            logger.error(msg)
            raise ElasticError(cause=msg)
