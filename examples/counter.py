#!/usr/bin/env python
from pysyncobj import SyncObj, replicated
from __future__ import print_function

import sys
import time
from functools import partial

sys.path.append("../")


class TestObj(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(TestObj, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.__counter = 0

    @replicated
    def inc_counter(self):
        self.__counter += 1
        return self.__counter

    @replicated
    def add_value(self, value, cn):
        self.__counter += value
        return self.__counter, cn

    def get_counter(self):
        return self.__counter


def on_add(res, err, cnt):
    print('on_add %d:' % cnt, res, err)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
        sys.exit(-1)

    port = int(sys.argv[1])
    partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]
    o = TestObj('localhost:%d' % port, partners)
    n = 0
    old_value = -1
    while True:
        # time.sleep(0.005)
        time.sleep(0.5)
        if o.get_counter() != old_value:
            old_value = o.get_counter()
            print(old_value)
        if o._getLeader() is None:
            continue
        # if n < 2000:
        if n < 20:
            o.add_value(10, n, callback=partial(on_add, cnt=n))
        n += 1
        # if n % 200 == 0:
        # if True:
        #    print('Counter value:', o.get_counter(), o._getLeader(), o._getRaftLogSize(), o._getLastCommitIndex())
