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

import unittest

from citadel.storage_engine import StorageEngine


class TestStorageEngine(unittest.TestCase):

    def test_write(self):
        """Test whether an NotImplementedError exception is thrown"""

        e = StorageEngine()

        with self.assertRaises(NotImplementedError):
            e.write("storage", data=None)


if __name__ == "__main__":
    unittest.main(warnings='ignore')
