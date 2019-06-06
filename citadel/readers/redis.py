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

import logging
import pickle

import redis

from citadel.reader import Reader

URL = 'redis://localhost/8'
Q_STORAGE_ITEMS = 'items'

logger = logging.getLogger(__name__)


class RedisReader(Reader):

    def __init__(self, url=URL, qname=Q_STORAGE_ITEMS):
        super().__init__()
        self.url = url
        self.qname = qname
        self.conn = redis.StrictRedis.from_url(url)

    def read(self):
        """Read data from Redis queue"""

        pipe = self.conn.pipeline()
        pipe.lrange(Q_STORAGE_ITEMS, 0, -1)
        pipe.ltrim(Q_STORAGE_ITEMS, 1, 0)
        items = pipe.execute()[0]

        for item in items:
            item = pickle.loads(item)
            yield item
