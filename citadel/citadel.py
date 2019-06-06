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


import time

KEEP_ALIVE = True
DELAY_TIME = 0


class Citadel:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    def transfer(self, keep_alive=KEEP_ALIVE, delay=DELAY_TIME):
        """Transfer the data from Redis to the ElasticSearch.

        :param keep_alive: a flag to keeps listening to the source connection
        :param delay: the number of seconds to sleep between queue listenings
        """
        while True:
            try:
                items = self.reader.read()
                self.writer.write(items)

                if not keep_alive:
                    break

                if delay:
                    time.sleep(delay)

            except KeyboardInterrupt:
                items = self.reader.read()
                self.writer.write(items)
                break
