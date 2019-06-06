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


import asyncio
import hashlib
import json
import logging
import pickle
import redis
import time
import urllib3

from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from citadel.errors import ElasticError

# Redis params
REDIS_SERVER = 'redis://localhost/8'
KEEP_ALIVE = True
DELAY_TIME = 0
READ_DONE = 'read_done'
Q_STORAGE_ITEMS = 'items'

# ES connection params
ES_SERVER = 'https://admin:admin@localhost:9200'
ES_TIMEOUT = 3600
ES_MAX_RETRIES = 50
ES_RETRY_ON_TIMEOUT = True
ES_VERIFY_CERTS = False

# Transfer bulk
TRANSFER_BULK = 100

# ES params
ES_ITEMS_TYPE = 'items'

# Items data
BACKEND_NAME = 'backend_name'
BACKEND_CATEGORY = 'category'
BACKEND_VERSION = 'backend_version'
BACKEND_ORIGIN = 'origin'


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


def uuid(*args):
    """Generate a UUID based on the given parameters.

    The UUID will be the SHA1 of the concatenation of the values
    from the list. The separator bewteedn these values is ':'.
    Each value must be a non-empty string, otherwise, the function
    will raise an exception.

    :param *args: list of arguments used to generate the UUID

    :returns: a universal unique identifier

    :raises ValueError: when anyone of the values is not a string,
        is empty or `None`.
    """
    def check_value(v):
        if not isinstance(v, str):
            raise ValueError("%s value is not a string instance" % str(v))
        elif not v:
            raise ValueError("value cannot be None or empty")
        else:
            return v

    s = ':'.join(map(check_value, args))

    sha1 = hashlib.sha1(s.encode('utf-8', errors='surrogateescape'))
    uuid_sha1 = sha1.hexdigest()

    return uuid_sha1


class Citadel:

    def __init__(self, redis_url=REDIS_SERVER, es_url=ES_SERVER,
                 redis_queue_name=Q_STORAGE_ITEMS,
                 es_timeout=ES_TIMEOUT, es_max_retries=ES_MAX_RETRIES,
                 es_retry_on_timeout=ES_RETRY_ON_TIMEOUT, es_verify_certs=ES_VERIFY_CERTS):
        self.redis_conn = redis.StrictRedis.from_url(redis_url)
        self.redis_queue_name = redis_queue_name
        self.es_conn = Elasticsearch([es_url], timeout=es_timeout, max_retries=es_max_retries,
                                     retry_on_timeout=es_retry_on_timeout, verify_certs=es_verify_certs,
                                     connection_class=RequestsHttpConnection)

    def transfer(self, keep_alive=KEEP_ALIVE, delay=DELAY_TIME):
        """Transfer the data from Redis to the ElasticSearch.

        :param keep_alive: a flag to keeps listening to the source storage
        :param delay: the number of seconds to sleep between queue listenings
        """
        data_queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        while True:
            try:
                loop.create_task(self.read(data_queue))
                loop.run_until_complete(self.write(data_queue))

                if not keep_alive:
                    break

                if delay:
                    time.sleep(delay)

            except KeyboardInterrupt:
                if data_queue.qsize() != 0:
                    data_queue.put(READ_DONE)
                    loop.run_until_complete(self.write(data_queue))
                break

        if data_queue.qsize() != 0:
            logger.warning("%s items have been lost before closing the transfer", data_queue.qsize)

        loop.close()

    async def read(self, data_queue):
        """Read data from Redis queue"""

        pipe = self.redis_conn.pipeline()
        pipe.lrange(self.redis_queue_name, 0, -1)
        pipe.ltrim(self.redis_queue_name, 1, 0)
        items = pipe.execute()[0]

        for item in items:
            item = pickle.loads(item)
            await data_queue.put(item)

        await data_queue.put(READ_DONE)

    async def write(self, data_queue):
        """Write data to ElasticSearch"""

        items = []
        while True:
            item = await data_queue.get()

            if item == READ_DONE:
                break

            items.append(item)
            data_queue.task_done()

            if len(items) == TRANSFER_BULK:
                self.dispatch_items(items)
                items.clear()

        if items:
            self.dispatch_items(items)
            items.clear()

        data_queue.task_done()

    def dispatch_items(self, items):
        """Dispatch the items to the corresponding index"""

        index2items = self.items_per_index(items)
        for index in index2items.keys():

            items_in_index = index2items.get(index)

            index.create(self.es_conn)
            index.set_alias(self.es_conn)
            index.write(self.es_conn, items_in_index)

    def items_per_index(self, items):
        """Organize the items per index"""

        index2items = {}

        for item in items:
            index = Index(item[BACKEND_NAME],
                          item[BACKEND_CATEGORY],
                          item[BACKEND_VERSION],
                          item[BACKEND_ORIGIN])

            items_to_dispatch = index2items.get(index, [])
            items_to_dispatch.append(item)
            index2items.update({index: items_to_dispatch})

        return index2items


class Index:

    def __init__(self, backend_name, backend_category, backend_version, origin):
        self.backend_name = backend_name
        self.backend_category = backend_category
        self.backend_version = backend_version
        self.origin = origin

    def name(self):
        return uuid(self.backend_name, self.backend_category, self.backend_version, self.origin)

    def alias(self):
        return "{}_{}_{}".format(self.backend_name, self.backend_category, self.backend_version)

    def create(self, connection):
        """Create index if it doesn't exists"""

        mapping = json.loads(PERCEVAL_MAPPING)
        index_name = self.name()

        if connection.indices.exists(index=index_name):
            return

        res = connection.indices.create(index=index_name, body=mapping)

        if not res['acknowledged']:
            msg = "Index {} not created".format(index_name)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def set_alias(self, connection):
        """Create alias for index"""

        alias = self.alias()

        res = connection.indices.update_aliases(
            {
                "actions": [
                    {"add": {"index": self.name(), "alias": alias}}
                ]
            }
        )

        if not res['acknowledged']:
            msg = "Alias {} not created".format(alias)
            logger.error(msg)
            raise ElasticError(cause=msg)

    def write(self, connection, items):
        """Write items to an index"""

        index_name = self.name()
        es_items = []

        for item in items:
            es_item = {
                '_index': index_name,
                '_type': ES_ITEMS_TYPE,
                '_id': item['uuid'],
                '_source': item
            }

            es_items.append(es_item)

        errors = helpers.bulk(connection, es_items)[1]
        msg = "{} items written in {}".format(len(es_items), index_name)
        logger.info(msg)
        connection.indices.refresh(index=index_name)

        if errors:
            msg = "Lost items from Redis to ES, index {}. Error {}".format(index_name, errors[0])
            logger.error(msg)
            raise ElasticError(cause=msg)
