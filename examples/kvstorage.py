#!/usr/bin/env python
from __future__ import print_function

import sys

from pysyncobj import SyncObj, SyncObjConf, replicated

sys.path.append("../")


class KVStorage(SyncObj):
    def __init__(self, self_address, partner_addrs):
        cfg = SyncObjConf(dynamicMembershipChange=True)
        super(KVStorage, self).__init__(self_address, partner_addrs, cfg)
        self.__data = {}

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)


_g_kvstorage = None


def main():
    if len(sys.argv) < 2:
        print('Usage: %s selfHost:port partner1Host:port partner2Host:port ...')
        sys.exit(-1)

    selfAddr = sys.argv[1]
    if selfAddr == 'readonly':
        selfAddr = None
    partners = sys.argv[2:]

    global _g_kvstorage
    _g_kvstorage = KVStorage(selfAddr, partners)

    while True:
        cmd = input(">> ").split()
        if not cmd:
            continue
        elif cmd[0] == 'set':
            _g_kvstorage.set(cmd[1], cmd[2])
        elif cmd[0] == 'get':
            print(_g_kvstorage.get(cmd[1]))
        elif cmd[0] == 'pop':
            print(_g_kvstorage.pop(cmd[1]))
        else:
            print('Wrong command')


if __name__ == '__main__':
    main()
