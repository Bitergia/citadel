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


from grimoirelab_toolkit.datetime import datetime_utcnow


from citadel.errors import EventsError
from citadel.storage_engines.elasticsearch import ElasticsearchStorage


EVENTS = 'events'

BY_MINUTE = 'minute'
BY_HOUR = 'hour'
BY_DAY = 'day'
BY_MONTH = 'month'

TIMEFRAMES = [BY_MINUTE, BY_HOUR, BY_DAY, BY_MONTH]


class Events:

    PERCEVAL_MAPPING = """
        {
          "mappings": {
            "%s": {
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
        """ % ElasticsearchStorage.ITEMS

    def __init__(self, url, base_index=EVENTS, timeframe=BY_DAY):
        self.storage = ElasticsearchStorage(url)

        if timeframe not in TIMEFRAMES:
            msg = "Unknown timeframe {}".format(timeframe)
            raise EventsError(cause=msg)

        self.timeframe = timeframe
        self.base_index = base_index

    def index_name(self):

        def __set_timeframe_format():
            frmt = "%Y%m"
            if self.timeframe == BY_MINUTE:
                frmt = "%Y%m%d_%Hh%Mm"
            if self.timeframe == BY_HOUR:
                frmt = "%Y%m%d_%Hh"
            elif self.timeframe == BY_DAY:
                frmt = "%Y%m%d"

            return frmt

        timeframe_format = __set_timeframe_format()
        timeframe = datetime_utcnow().replace(tzinfo=None).strftime(timeframe_format)

        index_name = self.base_index + "_" + timeframe

        return index_name

    def store(self, data, field_id=None):

        index = self.index_name()

        if not self.storage.elasticsearch.indices.exists(index):
            self.storage.create_index(index, self.PERCEVAL_MAPPING)
            self.storage.set_alias(self.base_index, index)

        written = self.storage.write(index, data, field_id=field_id)

        return written
