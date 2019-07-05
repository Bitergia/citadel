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
import datetime
import json
import os
import unittest
import unittest.mock

from citadel.collections.events import (BY_MINUTE,
                                        BY_HOUR,
                                        BY_DAY,
                                        BY_MONTH,
                                        Events)
from citadel.errors import EventsError
from citadel.storage_engines.elasticsearch import ElasticsearchStorage


CONFIG_FILE = 'tests.conf'

QUERY_MATCH_ALL = {
    "query": {
        "match_all": {}
    }
}


def read_file(filename, mode='r'):
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename), mode) as f:
        content = f.read()
    return content


class TestEvents(unittest.TestCase):

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

        events = Events(self.es_url)

        self.assertIsInstance(events.storage, ElasticsearchStorage)
        self.assertEqual(events.timeframe, BY_DAY)

    def test_initialization_unknown_timeframe(self):
        """Test whether an exception is thrown when the timeframe is unknown"""

        with self.assertRaises(EventsError):
            _ = Events(self.es_url, timeframe='seconds')

    @unittest.mock.patch('citadel.collections.events.datetime_utcnow')
    def test_index_name(self, mock_utcnow):
        """Test whether the index name is created properly"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1, 23, 59, 00)

        events = Events(self.es_url, timeframe=BY_MINUTE)
        self.assertEqual(events.index_name(), 'events_20170101_23h59m')

        events = Events(self.es_url, timeframe=BY_HOUR)
        self.assertEqual(events.index_name(), 'events_20170101_23h')

        events = Events(self.es_url, timeframe=BY_DAY)
        self.assertEqual(events.index_name(), 'events_20170101')

        events = Events(self.es_url, timeframe=BY_MONTH)
        self.assertEqual(events.index_name(), 'events_201701')

    @unittest.mock.patch('citadel.collections.events.datetime_utcnow')
    def test_store(self, mock_utcnow):
        """Test whether the method store works properly"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1, 23, 59, 00)
        self.index = 'events_20170101'

        data = read_file('data/perceval_data.json')
        data_json = json.loads(data)

        events = Events(self.es_url)
        written = events.store(data_json)

        query = events.storage.elasticsearch.search(index=self.index,
                                                    doc_type=events.storage.ITEMS,
                                                    body=QUERY_MATCH_ALL,
                                                    size=100)
        hits = query['hits']['hits']

        self.assertEqual(written, len(hits))


if __name__ == "__main__":
    unittest.main(warnings='ignore')
