import collections
import heapq
import os
import socket
import threading
import time
import weakref

from .syncobj import SyncObjConsumer, replicated


class ReplCounter(SyncObjConsumer):
    def __init__(self):
        """
        Simple distributed counter. You can set, add, sub and inc counter value.
        """
        super(ReplCounter, self).__init__()
        self.__counter = int()

    @replicated
    def set(self, new_value):
        """
        Set new value to a counter.

        :param new_value: new value
        :return: new counter value
        """
        self.__counter = new_value
        return self.__counter

    @replicated
    def add(self, value):
        """
        Adds value to a counter.

        :param value: value to add
        :return: new counter value
        """
        self.__counter += value
        return self.__counter

    @replicated
    def sub(self, value):
        """
        Subtracts a value from counter.

        :param value: value to subtract
        :return: new counter value
        """
        self.__counter -= value
        return self.__counter

    @replicated
    def inc(self):
        """
        Increments counter value by one.

        :return: new counter value
        """
        self.__counter += 1
        return self.__counter

    def get(self):
        """
        :return: current counter value
        """
        return self.__counter


class ReplList(SyncObjConsumer):
    def __init__(self):
        """
        Distributed list - it has an interface similar to a regular list.
        """
        super(ReplList, self).__init__()
        self.__data = []

    @replicated
    def reset(self, new_data):
        """Replace list with a new one"""
        assert isinstance(new_data, list)
        self.__data = new_data

    @replicated
    def set(self, position, new_value):
        """Update value at given position."""
        self.__data[position] = new_value

    @replicated
    def append(self, item):
        """Append item to end"""
        self.__data.append(item)

    @replicated
    def extend(self, other):
        """Extend list by appending elements from the iterable"""
        self.__data.extend(other)

    @replicated
    def insert(self, position, element):
        """Insert object before position"""
        self.__data.insert(position, element)

    @replicated
    def remove(self, element):
        """
        Remove first occurrence of element.
        Raises ValueError if the value is not present.
        """
        self.__data.remove(element)

    @replicated
    def pop(self, position=None):
        """
        Remove and return item at position (default last).
        Raises IndexError if list is empty or index is out of range.
        """
        return self.__data.pop(position)

    @replicated
    def sort(self, reverse=False):
        """Stable sort *IN PLACE*"""
        self.__data.sort(reverse=reverse)

    def index(self, element):
        """
        Return first position of element.
        Raises ValueError if the value is not present.
        """
        return self.__data.index(element)

    def count(self, element):
        """ Return number of occurrences of element """
        return self.__data.count(element)

    def get(self, position):
        """ Return value at given position"""
        return self.__data[position]

    def __getitem__(self, position):
        """ Return value at given position"""
        return self.__data[position]

    @replicated(ver=1)
    def __setitem__(self, position, element):
        """Update value at given position."""
        self.__data[position] = element

    def __len__(self):
        """Return the number of items of a sequence or collection."""
        return len(self.__data)

    def raw_data(self):
        """Return internal list - use it carefully"""
        return self.__data


class ReplDict(SyncObjConsumer):
    def __init__(self):
        """
        Distributed dict - it has an interface similar to a regular dict.
        """
        super(ReplDict, self).__init__()
        self.__data = {}

    @replicated
    def reset(self, new_data):
        """Replace dict with a new one"""
        assert isinstance(new_data, dict)
        self.__data = new_data

    @replicated
    def __setitem__(self, key, value):
        """Set value for specified key"""
        self.__data[key] = value

    @replicated
    def set(self, key, value):
        """Set value for specified key"""
        self.__data[key] = value

    @replicated
    def setdefault(self, key, default):
        """Return value for specified key, set default value if key not exist"""
        return self.__data.setdefault(key, default)

    @replicated
    def update(self, other):
        """Adds all values from the other dict"""
        self.__data.update(other)

    @replicated
    def pop(self, key, default=None):
        """Remove and return value for given key, return default if key not exist"""
        return self.__data.pop(key, default)

    @replicated
    def clear(self):
        """Remove all items from dict"""
        self.__data.clear()

    def __getitem__(self, key):
        """Return value for given key"""
        return self.__data[key]

    def get(self, key, default=None):
        """Return value for given key, return default if key not exist"""
        return self.__data.get(key, default)

    def __len__(self):
        """Return size of dict"""
        return len(self.__data)

    def __contains__(self, key):
        """True if key exists"""
        return key in self.__data

    def keys(self):
        """Return all keys"""
        return self.__data.keys()

    def values(self):
        """Return all values"""
        return self.__data.values()

    def items(self):
        """Return all items"""
        return self.__data.items()

    def raw_data(self):
        """Return internal dict - use it carefully"""
        return self.__data


class ReplSet(SyncObjConsumer):
    def __init__(self):
        """
        Distributed set - it has an interface similar to a regular set.
        """
        super(ReplSet, self).__init__()
        self.__data = set()

    @replicated
    def reset(self, new_data):
        """Replace set with a new one"""
        assert isinstance(new_data, set)
        self.__data = new_data

    @replicated
    def add(self, item):
        """Add an element to a set"""
        self.__data.add(item)

    @replicated
    def remove(self, item):
        """
        Remove an element from a set; it must be a member.
        If the element is not a member, raise a KeyError.
        """
        self.__data.remove(item)

    @replicated
    def discard(self, item):
        """
        Remove an element from a set if it is a member.
        If the element is not a member, do nothing.
        """
        self.__data.discard(item)

    @replicated
    def pop(self):
        """
        Remove and return an arbitrary set element.
        Raises KeyError if the set is empty.
        """
        return self.__data.pop()

    @replicated
    def clear(self):
        """ Remove all elements from this set. """
        self.__data.clear()

    @replicated
    def update(self, other):
        """ Update a set with the union of itself and others. """
        self.__data.update(other)

    def raw_data(self):
        """Return internal dict - use it carefully"""
        return self.__data

    def __len__(self):
        """Return size of set"""
        return len(self.__data)

    def __contains__(self, item):
        """True if item exists"""
        return item in self.__data


class ReplQueue(SyncObjConsumer):
    def __init__(self, maxsize=0):
        """
        Replicated FIFO queue. Based on collections.deque.
        Has an interface similar to Queue.

        :param maxsize: Max queue size.
        :type maxsize: int
        """
        super(ReplQueue, self).__init__()
        self.__maxsize = maxsize
        self.__data = collections.deque()

    def qsize(self):
        """Return size of queue"""
        return len(self.__data)

    def empty(self):
        """True if queue is empty"""
        return len(self.__data) == 0

    def __len__(self):
        """Return size of queue"""
        return len(self.__data)

    def full(self):
        """True if queue is full"""
        return len(self.__data) == self.__maxsize

    @replicated
    def put(self, item):
        """Put an item into the queue.
        True - if item placed in queue.
        False - if queue is full and item can not be placed."""
        if self.__maxsize and len(self.__data) >= self.__maxsize:
            return False
        self.__data.append(item)
        return True

    @replicated
    def get(self, default=None):
        """Extract item from queue.
        Return default if queue is empty."""
        try:
            return self.__data.popleft()
        except IndexError:
            return default


class ReplPriorityQueue(SyncObjConsumer):
    def __init__(self, maxsize=0):
        """
        Replicated priority queue. Based on heapq.
        Has an interface similar to Queue.

        :param maxsize: Max queue size.
        :type maxsize: int
        """
        super(ReplPriorityQueue, self).__init__()
        self.__maxsize = maxsize
        self.__data = []

    def qsize(self):
        """Return size of queue"""
        return len(self.__data)

    def empty(self):
        """True if queue is empty"""
        return len(self.__data) == 0

    def __len__(self):
        """Return size of queue"""
        return len(self.__data)

    def full(self):
        """True if queue is full"""
        return len(self.__data) == self.__maxsize

    @replicated
    def put(self, item):
        """Put an item into the queue. Items should be comparable, eg. tuples.
        True - if item placed in queue.
        False - if queue is full and item can not be placed."""
        if self.__maxsize and len(self.__data) >= self.__maxsize:
            return False
        heapq.heappush(self.__data, item)
        return True

    @replicated
    def get(self, default=None):
        """Extract the smallest item from queue.
        Return default if queue is empty."""
        if not self.__data:
            return default
        return heapq.heappop(self.__data)


class _ReplLockManagerImpl(SyncObjConsumer):
    def __init__(self, auto_unlock_time):
        super(_ReplLockManagerImpl, self).__init__()
        self.__locks = {}
        self.__autoUnlockTime = auto_unlock_time

    @replicated
    def acquire(self, lock_id, client_id, current_time):
        existing_lock = self.__locks.get(lock_id, None)
        # Auto-unlock old lock
        if existing_lock is not None:
            if current_time - existing_lock[1] > self.__autoUnlockTime:
                existing_lock = None
        # Acquire lock if possible
        if existing_lock is None or existing_lock[0] == client_id:
            self.__locks[lock_id] = (client_id, current_time)
            return True
        # Lock already acquired by someone else
        return False

    @replicated
    def prolongate(self, client_id, current_time):
        for lockID in list(self.__locks):
            lock_client_id, lock_time = self.__locks[lockID]

            if current_time - lock_time > self.__autoUnlockTime:
                del self.__locks[lockID]
                continue

            if lock_client_id == client_id:
                self.__locks[lockID] = (client_id, current_time)

    @replicated
    def release(self, lock_id, client_id):
        existing_lock = self.__locks.get(lock_id, None)
        if existing_lock is not None and existing_lock[0] == client_id:
            del self.__locks[lock_id]

    def is_acquired(self, lock_id, client_id, current_time):
        existing_lock = self.__locks.get(lock_id, None)
        if existing_lock is not None:
            if existing_lock[0] == client_id:
                if current_time - existing_lock[1] < self.__autoUnlockTime:
                    return True
        return False


class ReplLockManager(object):
    def __init__(self, auto_unlock_time, self_id=None):
        """Replicated Lock Manager. Allow to acquire / release distributed locks.

        :param auto_unlock_time: lock will be released automatically
            if no response from holder for more than autoUnlockTime seconds
        :type auto_unlock_time: float
        :param self_id: (optional) - unique id of current lock holder.
        :type self_id: str
        """
        self.__lockImpl = _ReplLockManagerImpl(auto_unlock_time)
        if self_id is None:
            self_id = '%s:%d:%d' % (
                socket.gethostname(), os.getpid(), id(self))
        self.__selfID = self_id
        self.__autoUnlockTime = auto_unlock_time
        self.__mainThread = threading.current_thread()
        self.__initialised = threading.Event()
        self.__destroying = False
        self.__lastProlongateTime = 0
        self.__thread = threading.Thread(
            target=ReplLockManager._auto_acquire_thread, args=(weakref.proxy(self),))
        self.__thread.start()
        while not self.__initialised.is_set():
            pass

    def _consumer(self):
        return self.__lockImpl

    def destroy(self):
        """Destroy should be called before destroying ReplLockManager"""
        self.__destroying = True

    def _auto_acquire_thread(self):
        self.__initialised.set()
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    break
                time.sleep(0.1)
                if time.time() - self.__lastProlongateTime < float(self.__autoUnlockTime) / 4.0:
                    continue
                sync_obj = self.__lockImpl._syncObj
                if sync_obj is None:
                    continue
                if sync_obj._getLeader() is not None:
                    self.__lastProlongateTime = time.time()
                    self.__lockImpl.prolongate(self.__selfID, time.time())
        except ReferenceError:
            pass

    def try_acquire(self, lock_id, callback=None, sync=False, timeout=None):
        """Attempt to acquire lock.

        :param lock_id: unique lock identifier.
        :type lock_id: str
        :param sync: True - to wait until lock is acquired or failed to acquire.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        :return True if acquired, False - somebody else already acquired lock
        """
        return self.__lockImpl.acquire(lock_id, self.__selfID, time.time(), callback=callback, sync=sync,
                                       timeout=timeout)

    def is_acquired(self, lock_id):
        """Check if lock is acquired by ourselves.

        :param lock_id: unique lock identifier.
        :type lock_id: str
        :return True if lock is acquired by ourselves.
         """
        return self.__lockImpl.is_acquired(lock_id, self.__selfID, time.time())

    def release(self, lock_id, callback=None, sync=False, timeout=None):
        """
        Release previously-acquired lock.

        :param lock_id:  unique lock identifier.
        :type lock_id: str
        :param sync: True - to wait until lock is released or failed to release.
        :type sync: bool
        :param callback: if sync is False - callback will be called with operation result.
        :type callback: func(opResult, error)
        :param timeout: max operation time (default - unlimited)
        :type timeout: float
        """
        self.__lockImpl.release(lock_id, self.__selfID,
                                callback=callback, sync=sync, timeout=timeout)
