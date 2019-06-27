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


class StorageEngine:
    """Abstract class for storage engines.

    This class allows to perform write operations on storage engines.
    """
    def write(self, resource, data, chunk_size=None):
        """Write method to be redefined by subclasses

        :param resource: the location where the data is written
        :param data: the data to be written
        :param chunk_size: size of data chunks
        """
        raise NotImplementedError
