import time
import socket
import random
import logging


class DnsCachingResolver(object):
    def __init__(self, cache_time, fail_cache_time):
        self.__cache = {}
        self.__cacheTime = cache_time
        self.__failCacheTime = fail_cache_time
        self.__preferredAddrFamily = socket.AF_INET

    def set_timeouts(self, cache_time, fail_cache_time):
        self.__cacheTime = cache_time
        self.__failCacheTime = fail_cache_time

    def resolve(self, hostname):
        curr_time = time.time()
        cached_time, ips = self.__cache.get(hostname, (0, []))
        time_passed = curr_time - cached_time
        if (time_passed > self.__cacheTime) or (not ips and time_passed > self.__failCacheTime):
            prev_ips = ips
            ips = self.__do_resolve(hostname)
            if not ips:
                ips = prev_ips
            self.__cache[hostname] = (curr_time, ips)
        return None if not ips else random.choice(ips)

    def set_preferred_addr_family(self, preferred_addr_family):
        if preferred_addr_family is None:
            self.__preferredAddrFamily = None
        elif preferred_addr_family == 'ipv4':
            self.__preferredAddrFamily = socket.AF_INET
        elif preferred_addr_family == 'ipv6':
            self.__preferredAddrFamily = socket.AF_INET
        else:
            self.__preferredAddrFamily = preferred_addr_family

    def __do_resolve(self, hostname):
        try:
            addrs = socket.getaddrinfo(hostname, None)
            ips = []
            if self.__preferredAddrFamily is not None:
                ips = list(set([addr[4][0] for addr in addrs
                                if addr[0] == self.__preferredAddrFamily]))
            if not ips:
                ips = list(set([addr[4][0] for addr in addrs]))
        except socket.gaierror:
            logging.warning('failed to resolve host %s', hostname)
            ips = []
        return ips


_g_resolver = None


def global_dns_resolver():
    global _g_resolver
    if _g_resolver is None:
        _g_resolver = DnsCachingResolver(
            cache_time=600.0, fail_cache_time=30.0)
    return _g_resolver
