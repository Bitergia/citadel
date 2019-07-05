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


import itertools
import time

from perceval.backends.core.git import Git
from citadel.collections.events import (BY_HOUR,
                                        Events)
from citadel.collections.lookups import Lookups


REPOS = [
    # ("https://github.com/chaoss/grimoirelab-perceval", "/tmp/a"),
    # ("https://github.com/chaoss/grimoirelab-toolkit", "/tmp/b"),
    # ("https://github.com/chaoss/grimoirelab-elk", "/tmp/c"),
    # ("https://github.com/chaoss/grimoirelab-sirmordred", "/tmp/d"),
    # ("https://github.com/chaoss/grimoirelab-graal", "/tmp/e"),
    ("https://github.com/elastic/elasticsearch", "/tmp/f")
]

URL = 'https://admin:admin@localhost:9200'
LOOPS = 4


def test_git():

    # clone repos
    for url, local_path in REPOS:
        msg = "Cloning repo {} at {}".format(url, local_path)
        print(msg)

        git = Git(url, local_path)
        commits = [i for i in git.fetch()]

        print(len(commits))

    print("Start test with writes")
    # writes
    events = Events(URL, base_index='events_writes', timeframe=BY_HOUR)
    lookups = Lookups(URL)
    c = 0
    while True:

        for url, local_path in REPOS:

            time_start = time.time()

            git = Git(url, local_path)
            items = git.fetch()
            items, items2 = itertools.tee(git.fetch())

            events_ = events.store(items, uuid=False)
            lookups_ = lookups.store(items2)

            spent_time = time.strftime("%H:%M:%S", time.gmtime(time.time() - time_start))
            msg = "Repo {} processed: time {}, events {}".format(url, spent_time, events_)
            print(msg)

        c = c + 1

        if c == LOOPS:
            break

    print("Start test with updates")
    # updates
    events = Events(URL, base_index='events_updates', timeframe=BY_HOUR)
    c = 0
    while True:

        for url, local_path in REPOS:

            time_start = time.time()

            git = Git(url, local_path)
            items = git.fetch()

            events_ = events.store(items, field_id="uuid")

            spent_time = time.strftime("%H:%M:%S", time.gmtime(time.time() - time_start))
            msg = "Repo {} processed: time {}, events {}".format(url, spent_time, events_)
            print(msg)

        c = c + 1

        if c == LOOPS:
            break


def main():
    test_git()


if __name__ == '__main__':
    main()
