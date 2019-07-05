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

from citadel.storage_engines.elasticsearch import ElasticsearchStorage


class Lookups:

    UUID_SEARCH = 'uuid'
    LOOKUPS = 'lookups'
    LOOKUPS_MAPPING = """
        {
          "mappings": {
            "%s": {
                "dynamic": false,
                "_source": {
                    "excludes": [
                        "data"
                    ]
                },
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
        """ % LOOKUPS

    def __init__(self, url, index=LOOKUPS):
        self.storage = ElasticsearchStorage(url)
        self.index = index

    def store(self, data):

        if not self.storage.elasticsearch.indices.exists(self.index):
            self.storage.create_index(self.index, self.LOOKUPS_MAPPING)

        written = self.storage.write(self.index, data, item_type=self.LOOKUPS, field_id=self.UUID_SEARCH)

        return written
