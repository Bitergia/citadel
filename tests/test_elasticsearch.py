#!/usr/bin/env python3
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

import configparser
import json
import os
import unittest

from elasticsearch import Elasticsearch

from citadel.errors import StorageEngineError
from citadel.storage_engines.elasticsearch import ElasticsearchStorage


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


INDEX_NAME = 'test_citadel'
WRONG_INDEX_NAME = '#test_citadel'
CONFIG_FILE = 'tests.conf'

ERROR_INDEX_ALREADY_EXISTS = "Index test_citadel already exists!"
ERROR_INDEX_NOT_VALID_NAME = "Index #test_citadel not created, invalid_index_name_exception, " \
                             "Invalid index name [#test_citadel], must not contain '#'"
ERROR_WRITE_IMMENSE_TERM = 'Bulk error on test_citadel, illegal_argument_exception, Document ' \
                           'contains at least one immense term in field="backend_name" (whose UTF8 ' \
                           'encoding is longer than the max length 32766)'

QUERY_MATCH_ALL = {
    "query": {
        "match_all": {}
    }
}


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


PERCEVAL_MAPPING_WRONG = """
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


class TestElasticsearchStorage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = configparser.ConfigParser()
        cls.config.read(CONFIG_FILE)
        cls.es_url = dict(cls.config.items('ElasticSearch'))['url']
        cls.index = None

    def tearDown(self):
        if self.index:
            storage = ElasticsearchStorage(self.es_url)
            storage.elasticsearch.indices.delete(self.index, ignore=[400, 404])

    def test_initialization(self):
        """Test whether attributes are initialized"""

        storage = ElasticsearchStorage(self.es_url)

        self.assertEqual(storage.url, self.es_url)
        self.assertIsInstance(storage.elasticsearch, Elasticsearch)

    def test_create_index(self):
        """Test whether an index is created"""

        self.index = INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)

        self.assertFalse(storage.elasticsearch.indices.exists(index=self.index))
        storage.create_index(self.index, PERCEVAL_MAPPING)
        self.assertTrue(storage.elasticsearch.indices.exists(index=self.index))

    def test_create_index_already_exists(self):
        """Test whether a StorageEngineError error is thrown when the index already exists"""

        self.index = INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)

        storage.create_index(self.index, PERCEVAL_MAPPING)

        with self.assertRaises(StorageEngineError) as ex:
            storage.create_index(self.index, PERCEVAL_MAPPING)
            self.assertEqual(ex.exception.msg, ERROR_INDEX_ALREADY_EXISTS)

    def test_create_index_error(self):
        """Test whether a StorageEngineError error is thrown when the index cannot be created"""

        self.index = WRONG_INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)

        with self.assertRaises(StorageEngineError) as ex:
            storage.create_index(self.index, PERCEVAL_MAPPING)

        self.assertEqual(ex.exception.msg, ERROR_INDEX_NOT_VALID_NAME)

    def test_write(self):
        """Test whether items are written to the index"""

        self.index = INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)
        storage.create_index(self.index, PERCEVAL_MAPPING)

        data = read_file('data/perceval_data.json')
        data_json = json.loads(data)

        written = storage.write(self.index, data_json, chunk_size=5)

        query = storage.elasticsearch.search(index=self.index,
                                             doc_type=storage.ITEMS,
                                             body=QUERY_MATCH_ALL,
                                             size=100)
        hits = query['hits']['hits']

        self.assertEqual(written, len(data_json))
        self.assertEqual(written, len(hits))

    def test_write_field_id(self):
        """Test whether items are written to the index when `field_id` is set"""

        self.index = INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)
        storage.create_index(self.index, PERCEVAL_MAPPING)

        data = read_file('data/perceval_data.json')
        data_json = json.loads(data)

        storage.write(self.index, data_json, chunk_size=5, field_id='uuid')

        query = storage.elasticsearch.search(index=self.index,
                                             doc_type=storage.ITEMS,
                                             body=QUERY_MATCH_ALL,
                                             size=100)

        for hit in query['hits']['hits']:
            self.assertEqual(hit['_id'], hit['_source']['uuid'])

    def test_write_wrong(self):
        """Test whether a StorageEngine error is thrown when an item is not written"""

        self.index = INDEX_NAME
        storage = ElasticsearchStorage(self.es_url)
        storage.create_index(self.index, PERCEVAL_MAPPING_WRONG)

        # load JSON with an attribute exceeding 32766 bytes
        data = read_file('data/perceval_data_wrong.json')
        data_json = json.loads(data)

        with self.assertRaises(StorageEngineError) as ex:
            storage.write(self.index, data_json, chunk_size=5)

        self.assertIn(ERROR_WRITE_IMMENSE_TERM, ex.exception.msg)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
