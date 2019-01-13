import collections
import functools
import logging
import os
import random
import struct
import sys
import threading
import time
import types
import weakref

import pysyncobj.pickle as pickle
from .dns_resolver import global_dns_resolver
from .poller import create_poller

try:
    from .pipe_notifier import PipeNotifier

    PIPE_NOTIFIER_ENABLED = True
except ImportError:
    PIPE_NOTIFIER_ENABLED = False
from .serializer import Serializer, SerializerState
from .node import Node
from pysyncobj.tcp_node import TCPNode
from .transport import Transport, TransportNotReadyError
from pysyncobj.tcp_transport import TCPTransport
from .journal import createJournal
from .config import SyncObjConf, FailReason
from .encryptor import HAS_CRYPTO, getEncryptor
from .version import VERSION
from .fast_queue import FastQueue
import queue

is_py3 = True
xrange = range


def iteritems(v):
    return v.items()


class RaftState:
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


class CommandType:
    REGULAR = 0
    NO_OP = 1
    MEMBERSHIP = 2
    VERSION = 3


_bchr = functools.partial(struct.pack, 'B')


class SyncObjException(Exception):
    def __init__(self, error_code, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
        self.errorCode = error_code


class SyncObjExceptionWrongVer(SyncObjException):
    def __init__(self, ver):
        SyncObjException.__init__(self, 'wrongVer')
        self.ver = ver


class SyncObjConsumer(object):
    def __init__(self):
        self._syncObj = None
        self.__properies = set()
        for key in self.__dict__:
            self.__properies.add(key)

    def _destroy(self):
        self._syncObj = None

    def _serialize(self):
        return dict([(k, v) for k, v in iteritems(self.__dict__) if k not in self.__properies])

    def _deserialize(self, data):
        for k, v in iteritems(data):
            self.__dict__[k] = v


# https://github.com/bakwc/PySyncObj

class SyncObj(object):
    def __init__(self, self_node, other_nodes, conf=None, consumers=None, node_class=TCPNode, transport=None,
                 transport_class=TCPTransport):
        """
        Main SyncObj class, you should inherit your own class from it.

        :param self_node: object representing the self-node or address of the current node server 'host:port'
        :type self_node: Node or str
        :param other_nodes: objects representing the other nodes or addresses of partner nodes ['host1:port1', 'host2:port2', ...]
        :type other_nodes: iterable of Node or iterable of str
        :param conf: configuration object
        :type conf: SyncObjConf
        :param consumers: objects to be replicated
        :type consumers: list of SyncObjConsumer inherited objects
        :param node_class: class used for representation of nodes
        :type node_class: class
        :param transport: transport object; if None, transportClass is used to initialise such an object
        :type transport: Transport or None
        :param transport_class: the Transport subclass to be used for transferring messages to and from other nodes
        :type transport_class: class
        """

        if conf is None:
            self.__conf = SyncObjConf()
        else:
            self.__conf = conf

        self.__conf.validate()

        if self.__conf.password is not None:
            if not HAS_CRYPTO:
                raise ImportError("Please install 'cryptography' module")
            self.__encryptor = getEncryptor(self.__conf.password)
        else:
            self.__encryptor = None

        consumers = consumers or []
        new_consumers = []
        for c in consumers:
            if not isinstance(c, SyncObjConsumer) and getattr(c, '_consumer', None):
                c = c._consumer()
            if not isinstance(c, SyncObjConsumer):
                raise SyncObjException(
                    'Consumers must be inherited from SyncObjConsumer')
            new_consumers.append(c)
        consumers = new_consumers

        self.__consumers = consumers

        if not isinstance(self_node, Node) and self_node is not None:
            self_node = node_class(self_node)
        self.__selfNode = self_node
        self.__otherNodes = set()  # set of Node
        for other_node in other_nodes:
            if not isinstance(other_node, Node):
                other_node = node_class(other_node)
            self.__otherNodes.add(other_node)
        self.__readonlyNodes = set()  # set of Node
        self.__connectedNodes = set()  # set of Node
        self.__nodeClass = node_class

        self.__raftState = RaftState.FOLLOWER
        self.__raftCurrentTerm = 0
        self.__votedForNodeId = None
        self.__votesCount = 0
        self.__raftLeader = None
        self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
        self.__raftLog = createJournal(self.__conf.journalFile)
        if len(self.__raftLog) == 0:
            self.__raftLog.add(_bchr(CommandType.NO_OP),
                               1, self.__raftCurrentTerm)
        self.__raftCommitIndex = 1
        self.__raftLastApplied = 1
        self.__raftNextIndex = {}
        self.__lastResponseTime = {}
        self.__raftMatchIndex = {}
        self.__lastSerializedTime = time.time()
        self.__lastSerializedEntry = None
        self.__forceLogCompaction = False
        self.__leaderCommitIndex = None
        self.__onReadyCalled = False
        self.__changeClusterIDx = None
        self.__noopIDx = None
        self.__destroying = False
        self.__recvTransmission = ''

        self.__onTickCallbacks = []
        self.__onTickCallbacksLock = threading.Lock()

        self.__startTime = time.time()
        global_dns_resolver().set_timeouts(
            self.__conf.dnsCacheTime, self.__conf.dnsFailCacheTime)
        global_dns_resolver().set_preferred_addr_family(self.__conf.preferredAddrType)
        self.__serializer = Serializer(self.__conf.fullDumpFile,
                                       self.__conf.logCompactionBatchSize,
                                       self.__conf.useFork,
                                       self.__conf.serializer,
                                       self.__conf.deserializer,
                                       self.__conf.serializeChecker)
        self.__lastInitTryTime = 0
        self._poller = create_poller(self.__conf.pollerType)

        if transport is not None:
            self.__transport = transport
        else:
            self.__transport = transport_class(
                self, self.__selfNode, self.__otherNodes)
        self.__transport.set_on_node_connected_callback(self.__onNodeConnected)
        self.__transport.set_on_node_disconnected_callback(
            self.__onNodeDisconnected)
        self.__transport.set_on_message_received_callback(
            self.__onMessageReceived)
        self.__transport.set_on_readonly_node_connected_callback(
            self.__onReadonlyNodeConnected)
        self.__transport.set_on_readonly_node_disconnected_callback(
            self.__onReadonlyNodeDisconnected)

        self._methodToID = {}
        self._idToMethod = {}
        self._idToConsumer = {}

        methods = [m for m in dir(self) if callable(getattr(self, m)) and
                   getattr(getattr(self, m), 'replicated', False) and
                   m != getattr(getattr(self, m), 'origName')]
        curr_method_id = 0
        self.__selfCodeVersion = 0
        self.__currentVersionFuncNames = {}

        methods_to_enumerate = []

        for method in methods:
            ver = getattr(getattr(self, method), 'ver')
            methods_to_enumerate.append((ver, 0, method, self))

        for consumerNum, consumer in enumerate(consumers):
            consumer_methods = [m for m in dir(consumer) if callable(getattr(consumer, m)) and
                                getattr(getattr(consumer, m), 'replicated', False) and
                                m != getattr(getattr(consumer, m), 'origName')]
            for method in consumer_methods:
                ver = getattr(getattr(consumer, method), 'ver')
                methods_to_enumerate.append(
                    (ver, consumerNum + 1, method, consumer))
            consumer._syncObj = self

        for ver, _, method, obj in sorted(methods_to_enumerate):
            self.__selfCodeVersion = max(self.__selfCodeVersion, ver)
            if obj is self:
                self._methodToID[method] = curr_method_id
            else:
                self._methodToID[(id(obj), method)] = curr_method_id
            self._idToMethod[curr_method_id] = getattr(obj, method)
            curr_method_id += 1

        self.__onSetCodeVersion(0)

        self.__thread = None
        self.__mainThread = None
        self.__initialised = None
        self.__commandsQueue = FastQueue(self.__conf.commandsQueueSize)
        if not self.__conf.appendEntriesUseBatch and PIPE_NOTIFIER_ENABLED:
            self.__pipeNotifier = PipeNotifier(self._poller)
        self.__needLoadDumpFile = True

        self.__lastReadonlyCheck = 0
        self.__newAppendEntriesTime = 0

        self.__commandsWaitingCommit = collections.defaultdict(
            list)  # logID => [(termID, callback), ...]
        self.__commandsLocalCounter = 0
        self.__commandsWaitingReply = {}  # commandLocalCounter => callback

        self.__properies = set()
        for key in self.__dict__:
            self.__properies.add(key)

        self.__enabledCodeVersion = 0

        if self.__conf.autoTick:
            self.__mainThread = threading.current_thread()
            self.__initialised = threading.Event()
            self.__thread = threading.Thread(
                target=SyncObj._autoTickThread, args=(weakref.proxy(self),))
            self.__thread.start()
            self.__initialised.wait()
            # while not self.__initialised.is_set():
            #     pass
        else:
            try:
                while not self.__transport.ready:
                    self.__transport.try_get_ready()
            except TransportNotReadyError:
                logging.exception('failed to perform initialization')
                raise SyncObjException('BindError')  # Backwards compatibility

    def destroy(self):
        """
        Correctly destroy SyncObj. Stop autoTickThread, close connections, etc.
        """
        if self.__conf.autoTick:
            self.__destroying = True
        else:
            self._doDestroy()

    def wait_ready(self):
        """
        Waits until the transport is ready for operation.

        :raises TransportNotReadyError: if the transport fails to get ready
        """
        self.__transport.wait_ready()

    def wait_binded(self):
        """
        Waits until initialized (binded port).
        If success - just returns.
        If failed to initialized after conf.maxBindRetries - raise SyncObjException.
        """
        try:
            self.__transport.wait_ready()
        except TransportNotReadyError:
            raise SyncObjException('BindError')
        if not self.__transport.ready:
            raise SyncObjException('BindError')

    def _destroy(self):
        self.destroy()

    def _doDestroy(self):
        self.__transport.destroy()
        for consumer in self.__consumers:
            consumer._destroy()
        self.__raftLog._destroy()

    def get_code_version(self):
        return self.__enabledCodeVersion

    def set_code_version(self, new_version, callback=None):
        """Switch to a new code version on all cluster nodes. You
        should ensure that cluster nodes are updated, otherwise they
        won't be able to apply commands.

        :param new_version: new code version
        :type int
        :param callback: will be called on cussess or fail
        :type callback: function(`FAIL_REASON <#pysyncobj.FAIL_REASON>`_, None)
        """
        assert isinstance(new_version, int)
        if new_version > self.__selfCodeVersion:
            raise Exception(
                'wrong version, current version is %d, requested version is %d' % (self.__selfCodeVersion, new_version))
        if new_version < self.__enabledCodeVersion:
            raise Exception('wrong version, enabled version is %d, requested version is %d' % (
                self.__enabledCodeVersion, new_version))
        self._applyCommand(pickle.dumps(new_version),
                           callback, CommandType.VERSION)

    def add_node_to_cluster(self, node, callback=None):
        """Add single node to cluster (dynamic membership changes). Async.
        You should wait until node successfully added before adding
        next node.

        :param node: node object or 'nodeHost:nodePort'
        :type node: Node | str
        :param callback: will be called on success or fail
        :type callback: function(`FAIL_REASON <#pysyncobj.FAIL_REASON>`_, None)
        """
        if not self.__conf.dynamicMembershipChange:
            raise Exception('dynamicMembershipChange is disabled')
        if not isinstance(node, Node):
            node = self.__nodeClass(node)
        self._applyCommand(pickle.dumps(
            ['add', node.id, node]), callback, CommandType.MEMBERSHIP)

    def remove_node_from_cluster(self, node, callback=None):
        """Remove single node from cluster (dynamic membership changes). Async.
        You should wait until node successfully added before adding
        next node.

        :param node: node object or 'nodeHost:nodePort'
        :type node: Node | str
        :param callback: will be called on success or fail
        :type callback: function(`FAIL_REASON <#pysyncobj.FAIL_REASON>`_, None)
        """
        if not self.__conf.dynamicMembershipChange:
            raise Exception('dynamicMembershipChange is disabled')
        if not isinstance(node, Node):
            node = self.__nodeClass(node)
        self._applyCommand(pickle.dumps(
            ['rem', node.id, node]), callback, CommandType.MEMBERSHIP)

    def _add_node_to_cluster(self, node, callback=None):
        self.add_node_to_cluster(node, callback)

    def _removeNodeFromCluster(self, node, callback=None):
        self.remove_node_from_cluster(node, callback)

    def __onSetCodeVersion(self, newVersion):
        methods = [m for m in dir(self) if callable(getattr(self, m)) and
                   getattr(getattr(self, m), 'replicated', False) and
                   m != getattr(getattr(self, m), 'origName')]

        self.__currentVersionFuncNames = {}

        funcVersions = collections.defaultdict(set)
        for method in methods:
            ver = getattr(getattr(self, method), 'ver')
            origFuncName = getattr(getattr(self, method), 'origName')
            funcVersions[origFuncName].add(ver)

        for consumer in self.__consumers:
            consumerID = id(consumer)
            consumerMethods = [m for m in dir(consumer) if callable(getattr(consumer, m)) and
                               getattr(getattr(consumer, m), 'replicated', False)]
            for method in consumerMethods:
                ver = getattr(getattr(consumer, method), 'ver')
                origFuncName = getattr(getattr(consumer, method), 'origName')
                funcVersions[(consumerID, origFuncName)].add(ver)

        for funcName, versions in iteritems(funcVersions):
            versions = sorted(list(versions))
            for v in versions:
                if v > newVersion:
                    break
                realFuncName = funcName[1] if isinstance(
                    funcName, tuple) else funcName
                self.__currentVersionFuncNames[funcName] = realFuncName + \
                    '_v' + str(v)

    def _getFuncName(self, funcName):
        return self.__currentVersionFuncNames[funcName]

    def _applyCommand(self, command, callback, commandType=None):
        try:
            if commandType is None:
                self.__commandsQueue.put_nowait((command, callback))
            else:
                self.__commandsQueue.put_nowait(
                    (_bchr(commandType) + command, callback))
            if not self.__conf.appendEntriesUseBatch and PIPE_NOTIFIER_ENABLED:
                self.__pipeNotifier.notify()
        except queue.Full:
            self.__callErrCallback(FailReason.QUEUE_FULL, callback)

    def _checkCommandsToApply(self):
        startTime = time.time()

        while time.time() - startTime < self.__conf.appendEntriesPeriod:
            if self.__raftLeader is None and self.__conf.commandsWaitLeader:
                break
            try:
                command, callback = self.__commandsQueue.get_nowait()
            except Queue.Empty:
                break

            requestNode, requestID = None, None
            if isinstance(callback, tuple):
                requestNode, requestID = callback

            if self.__raftState == RaftState.LEADER:
                idx, term = self.__getCurrentLogIndex() + 1, self.__raftCurrentTerm

                if self.__conf.dynamicMembershipChange:
                    changeClusterRequest = self.__parseChangeClusterRequest(
                        command)
                else:
                    changeClusterRequest = None

                if changeClusterRequest is None or self.__changeCluster(changeClusterRequest):

                    self.__raftLog.add(command, idx, term)

                    if requestNode is None:
                        if callback is not None:
                            self.__commandsWaitingCommit[idx].append(
                                (term, callback))
                    else:
                        self.__transport.send(requestNode, {
                            'type': 'apply_command_response',
                            'request_id': requestID,
                            'log_idx': idx,
                            'log_term': term,
                        })
                    if not self.__conf.appendEntriesUseBatch:
                        self.__sendAppendEntries()
                else:

                    if requestNode is None:
                        if callback is not None:
                            callback(None, FailReason.REQUEST_DENIED)
                    else:
                        self.__transport.send(requestNode, {
                            'type': 'apply_command_response',
                            'request_id': requestID,
                            'error': FailReason.REQUEST_DENIED,
                        })

            elif self.__raftLeader is not None:
                if requestNode is None:
                    message = {
                        'type': 'apply_command',
                        'command': command,
                    }

                    if callback is not None:
                        self.__commandsLocalCounter += 1
                        self.__commandsWaitingReply[self.__commandsLocalCounter] = callback
                        message['request_id'] = self.__commandsLocalCounter

                    self.__transport.send(self.__raftLeader, message)
                else:
                    self.__transport.send(requestNode, {
                        'type': 'apply_command_response',
                        'request_id': requestID,
                        'error': FailReason.NOT_LEADER,
                    })
            else:
                self.__callErrCallback(FailReason.MISSING_LEADER, callback)

    def _autoTickThread(self):
        try:
            self.__transport.try_get_ready()
        except TransportNotReadyError:
            logging.exception('failed to perform initialization')
            return
        finally:
            self.__initialised.set()
        time.sleep(0.1)
        try:
            while True:
                if not self.__mainThread.is_alive():
                    break
                if self.__destroying:
                    self._doDestroy()
                    break
                self._onTick(self.__conf.autoTickPeriod)
        except ReferenceError:
            pass

    def doTick(self, timeToWait=0.0):
        """Performs single tick. Should be called manually if `autoTick <#pysyncobj.SyncObjConf.autoTick>`_ disabled

        :param timeToWait: max time to wait for next tick. If zero - perform single tick without waiting for new events.
            Otherwise - wait for new socket event and return.
        :type timeToWait: float
        """
        assert not self.__conf.autoTick
        self._onTick(timeToWait)

    def _onTick(self, timeToWait=0.0):
        if not self.__transport.ready:
            try:
                self.__transport.try_get_ready()
            except TransportNotReadyError:
                # Implicitly handled in the 'if not self.__transport.ready' below
                pass

        if not self.__transport.ready:
            time.sleep(timeToWait)
            return

        if self.__needLoadDumpFile:
            if self.__conf.fullDumpFile is not None and os.path.isfile(self.__conf.fullDumpFile):
                self.__loadDumpFile(clearJournal=False)
            self.__needLoadDumpFile = False

        if self.__raftState in (RaftState.FOLLOWER, RaftState.CANDIDATE) and self.__selfNode is not None:
            if self.__raftElectionDeadline < time.time() and self.__connectedToAnyone():
                self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
                self.__raftLeader = None
                self.__setState(RaftState.CANDIDATE)
                self.__raftCurrentTerm += 1
                self.__votedForNodeId = self.__selfNode.id
                self.__votesCount = 1
                for node in self.__otherNodes:
                    self.__transport.send(node, {
                        'type': 'request_vote',
                        'term': self.__raftCurrentTerm,
                        'last_log_index': self.__getCurrentLogIndex(),
                        'last_log_term': self.__getCurrentLogTerm(),
                    })
                self.__onLeaderChanged()
                if self.__votesCount > (len(self.__otherNodes) + 1) / 2:
                    self.__onBecomeLeader()

        if self.__raftState == RaftState.LEADER:
            while self.__raftCommitIndex < self.__getCurrentLogIndex():
                nextCommitIndex = self.__raftCommitIndex + 1
                count = 1
                for node in self.__otherNodes:
                    if self.__raftMatchIndex[node] >= nextCommitIndex:
                        count += 1
                if count > (len(self.__otherNodes) + 1) / 2:
                    self.__raftCommitIndex = nextCommitIndex
                else:
                    break
            self.__leaderCommitIndex = self.__raftCommitIndex
            deadline = time.time() - self.__conf.leaderFallbackTimeout
            count = 1
            for node in self.__otherNodes:
                if self.__lastResponseTime[node] > deadline:
                    count += 1
            if count <= (len(self.__otherNodes) + 1) / 2:
                self.__setState(RaftState.FOLLOWER)
                self.__raftLeader = None

        needSendAppendEntries = False

        if self.__raftCommitIndex > self.__raftLastApplied:
            count = self.__raftCommitIndex - self.__raftLastApplied
            entries = self.__getEntries(self.__raftLastApplied + 1, count)
            for entry in entries:
                try:
                    currentTermID = entry[2]
                    subscribers = self.__commandsWaitingCommit.pop(
                        entry[1], [])
                    res = self.__doApplyCommand(entry[0])
                    for subscribeTermID, callback in subscribers:
                        if subscribeTermID == currentTermID:
                            callback(res, FailReason.SUCCESS)
                        else:
                            callback(None, FailReason.DISCARDED)

                    self.__raftLastApplied += 1
                except SyncObjExceptionWrongVer as e:
                    logging.error(
                        'request to switch to unsupported code version (self version: %d, requested version: %d)' %
                        (self.__selfCodeVersion, e.ver))

            if not self.__conf.appendEntriesUseBatch:
                needSendAppendEntries = True

        if self.__raftState == RaftState.LEADER:
            if time.time() > self.__newAppendEntriesTime or needSendAppendEntries:
                self.__sendAppendEntries()

        if not self.__onReadyCalled and self.__raftLastApplied == self.__leaderCommitIndex:
            if self.__conf.onReady:
                self.__conf.onReady()
            self.__onReadyCalled = True

        self._checkCommandsToApply()
        self.__tryLogCompaction()

        with self.__onTickCallbacksLock:
            for callback in self.__onTickCallbacks:
                callback()

        self._poller.poll(timeToWait)

    def addOnTickCallback(self, callback):
        with self.__onTickCallbacksLock:
            self.__onTickCallbacks.append(callback)

    def removeOnTickCallback(self, callback):
        with self.__onTickCallbacksLock:
            try:
                self.__onTickCallbacks.remove(callback)
            except ValueError:
                # callback not in list, ignore
                pass

    def getStatus(self):
        """Dumps different debug info about cluster to dict and return it"""

        status = {'version': VERSION, 'self': self.__selfNode, 'state': self.__raftState, 'leader': self.__raftLeader,
                  'partner_nodes_count': len(self.__otherNodes)}
        for node in self.__otherNodes:
            status['partner_node_status_server_' +
                   node.id] = 2 if node in self.__connectedNodes else 0
        status['readonly_nodes_count'] = len(self.__readonlyNodes)
        for node in self.__readonlyNodes:
            status['readonly_node_status_server_' +
                   node.id] = 2 if node in self.__connectedNodes else 0
        status['log_len'] = len(self.__raftLog)
        status['last_applied'] = self.__raftLastApplied
        status['commit_idx'] = self.__raftCommitIndex
        status['raft_term'] = self.__raftCurrentTerm
        status['next_node_idx_count'] = len(self.__raftNextIndex)
        for node, idx in iteritems(self.__raftNextIndex):
            status['next_node_idx_server_' + node.id] = idx
        status['match_idx_count'] = len(self.__raftMatchIndex)
        for node, idx in iteritems(self.__raftMatchIndex):
            status['match_idx_server_' + node.id] = idx
        status['leader_commit_idx'] = self.__leaderCommitIndex
        status['uptime'] = int(time.time() - self.__startTime)
        status['self_code_version'] = self.__selfCodeVersion
        status['enabled_code_version'] = self.__enabledCodeVersion
        return status

    def _getStatus(self):
        return self.getStatus()

    def printStatus(self):
        """Dumps different debug info about cluster to default logger"""
        status = self.getStatus()
        for k, v in iteritems(status):
            logging.info('%s: %s' % (str(k), str(v)))

    def _printStatus(self):
        self.printStatus()

    def forceLogCompaction(self):
        """Force to start log compaction (without waiting required time or required number of entries)"""
        self.__forceLogCompaction = True

    def _forceLogCompaction(self):
        self.forceLogCompaction()

    def __doApplyCommand(self, command):
        commandType = ord(command[:1])
        # Skip no-op and membership change commands
        if commandType == CommandType.VERSION:
            ver = pickle.loads(command[1:])
            if self.__selfCodeVersion < ver:
                raise SyncObjExceptionWrongVer(ver)
            oldVer = self.__enabledCodeVersion
            self.__enabledCodeVersion = ver
            callback = self.__conf.onCodeVersionChanged
            self.__onSetCodeVersion(ver)
            if callback is not None:
                callback(oldVer, ver)
            return
        if commandType != CommandType.REGULAR:
            return
        command = pickle.loads(command[1:])
        args = []
        kwargs = {
            '_doApply': True,
        }
        if not isinstance(command, tuple):
            funcID = command
        elif len(command) == 2:
            funcID, args = command
        else:
            funcID, args, newKwArgs = command
            kwargs.update(newKwArgs)

        return self._idToMethod[funcID](*args, **kwargs)

    def __onMessageReceived(self, node, message):

        if message['type'] == 'request_vote' and self.__selfNode is not None:

            if message['term'] > self.__raftCurrentTerm:
                self.__raftCurrentTerm = message['term']
                self.__votedForNodeId = None
                self.__setState(RaftState.FOLLOWER)
                self.__raftLeader = None

            if self.__raftState in (RaftState.FOLLOWER, RaftState.CANDIDATE):
                lastLogTerm = message['last_log_term']
                lastLogIdx = message['last_log_index']
                if message['term'] >= self.__raftCurrentTerm:
                    if lastLogTerm < self.__getCurrentLogTerm():
                        return
                    if lastLogTerm == self.__getCurrentLogTerm() and \
                            lastLogIdx < self.__getCurrentLogIndex():
                        return
                    if self.__votedForNodeId is not None:
                        return

                    self.__votedForNodeId = node.id

                    self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
                    self.__transport.send(node, {
                        'type': 'response_vote',
                        'term': message['term'],
                    })

        if message['type'] == 'append_entries' and message['term'] >= self.__raftCurrentTerm:
            self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
            if self.__raftLeader != node:
                self.__onLeaderChanged()
            self.__raftLeader = node
            if message['term'] > self.__raftCurrentTerm:
                self.__raftCurrentTerm = message['term']
                self.__votedForNodeId = None
            self.__setState(RaftState.FOLLOWER)
            newEntries = message.get('entries', [])
            serialized = message.get('serialized', None)
            self.__leaderCommitIndex = leaderCommitIndex = message['commit_index']

            # Regular append entries
            if 'prevLogIdx' in message:
                transmission = message.get('transmission', None)
                if transmission is not None:
                    if transmission == 'start':
                        self.__recvTransmission = message['data']
                        self.__sendNextNodeIdx(
                            node, success=False, reset=False)
                        return
                    elif transmission == 'process':
                        self.__recvTransmission += message['data']
                        self.__sendNextNodeIdx(
                            node, success=False, reset=False)
                        return
                    elif transmission == 'finish':
                        self.__recvTransmission += message['data']
                        newEntries = [pickle.loads(self.__recvTransmission)]
                        self.__recvTransmission = ''
                    else:
                        raise Exception('Wrong transmission type')

                prevLogIdx = message['prevLogIdx']
                prevLogTerm = message['prevLogTerm']
                prevEntries = self.__getEntries(prevLogIdx)
                if not prevEntries:
                    self.__sendNextNodeIdx(node, success=False, reset=True)
                    return
                if prevEntries[0][2] != prevLogTerm:
                    self.__sendNextNodeIdx(
                        node, nextNodeIdx=prevLogIdx, success=False, reset=True)
                    return
                if len(prevEntries) > 1:
                    # rollback cluster changes
                    if self.__conf.dynamicMembershipChange:
                        for entry in reversed(prevEntries[1:]):
                            clusterChangeRequest = self.__parseChangeClusterRequest(
                                entry[0])
                            if clusterChangeRequest is not None:
                                self.__doChangeCluster(
                                    clusterChangeRequest, reverse=True)

                    self.__deleteEntriesFrom(prevLogIdx + 1)
                for entry in newEntries:
                    self.__raftLog.add(*entry)

                # apply cluster changes
                if self.__conf.dynamicMembershipChange:
                    for entry in newEntries:
                        clusterChangeRequest = self.__parseChangeClusterRequest(
                            entry[0])
                        if clusterChangeRequest is not None:
                            self.__doChangeCluster(clusterChangeRequest)

                nextNodeIdx = prevLogIdx + 1
                if newEntries:
                    nextNodeIdx = newEntries[-1][1]

                self.__sendNextNodeIdx(
                    node, nextNodeIdx=nextNodeIdx, success=True)

            # Install snapshot
            elif serialized is not None:
                if self.__serializer.set_transmission_data(serialized):
                    self.__loadDumpFile(clearJournal=True)
                    self.__sendNextNodeIdx(node, success=True)

            self.__raftCommitIndex = min(
                leaderCommitIndex, self.__getCurrentLogIndex())

        if message['type'] == 'apply_command':
            if 'request_id' in message:
                self._applyCommand(
                    message['command'], (node, message['request_id']))
            else:
                self._applyCommand(message['command'], None)

        if message['type'] == 'apply_command_response':
            requestID = message['request_id']
            error = message.get('error', None)
            callback = self.__commandsWaitingReply.pop(requestID, None)
            if callback is not None:
                if error is not None:
                    callback(None, error)
                else:
                    idx = message['log_idx']
                    term = message['log_term']
                    assert idx > self.__raftLastApplied
                    self.__commandsWaitingCommit[idx].append((term, callback))

        if self.__raftState == RaftState.CANDIDATE:
            if message['type'] == 'response_vote' and message['term'] == self.__raftCurrentTerm:
                self.__votesCount += 1

                if self.__votesCount > (len(self.__otherNodes) + 1) / 2:
                    self.__onBecomeLeader()

        if self.__raftState == RaftState.LEADER:
            if message['type'] == 'next_node_idx':
                reset = message['reset']
                nextNodeIdx = message['next_node_idx']
                success = message['success']

                currentNodeIdx = nextNodeIdx - 1
                if reset:
                    self.__raftNextIndex[node] = nextNodeIdx
                if success:
                    self.__raftMatchIndex[node] = currentNodeIdx
                self.__lastResponseTime[node] = time.time()

    def __callErrCallback(self, err, callback):
        if callback is None:
            return
        if isinstance(callback, tuple):
            requestNode, requestID = callback
            self.__transport.send(requestNode, {
                'type': 'apply_command_response',
                'request_id': requestID,
                'error': err,
            })
            return
        callback(None, err)

    def __sendNextNodeIdx(self, node, reset=False, nextNodeIdx=None, success=False):
        if nextNodeIdx is None:
            nextNodeIdx = self.__getCurrentLogIndex() + 1
        self.__transport.send(node, {
            'type': 'next_node_idx',
            'next_node_idx': nextNodeIdx,
            'reset': reset,
            'success': success,
        })

    def __generateRaftTimeout(self):
        minTimeout = self.__conf.raftMinTimeout
        maxTimeout = self.__conf.raftMaxTimeout
        return minTimeout + (maxTimeout - minTimeout) * random.random()

    def __onReadonlyNodeConnected(self, node):
        self.__readonlyNodes.add(node)
        self.__connectedNodes.add(node)
        self.__raftNextIndex[node] = self.__getCurrentLogIndex() + 1
        self.__raftMatchIndex[node] = 0

    def __onReadonlyNodeDisconnected(self, node):
        self.__readonlyNodes.discard(node)
        self.__connectedNodes.discard(node)
        self.__raftNextIndex.pop(node, None)
        self.__raftMatchIndex.pop(node, None)
        node._destroy()

    def __onNodeConnected(self, node):
        self.__connectedNodes.add(node)

    def __onNodeDisconnected(self, node):
        self.__connectedNodes.discard(node)

    def __getCurrentLogIndex(self):
        return self.__raftLog[-1][1]

    def __getCurrentLogTerm(self):
        return self.__raftLog[-1][2]

    def __getPrevLogIndexTerm(self, nextNodeIndex):
        prevIndex = nextNodeIndex - 1
        entries = self.__getEntries(prevIndex, 1)
        if entries:
            return prevIndex, entries[0][2]
        return None, None

    def __getEntries(self, fromIDx, count=None, maxSizeBytes=None):
        firstEntryIDx = self.__raftLog[0][1]
        if fromIDx is None or fromIDx < firstEntryIDx:
            return []
        diff = fromIDx - firstEntryIDx
        if count is None:
            result = self.__raftLog[diff:]
        else:
            result = self.__raftLog[diff:diff + count]
        if maxSizeBytes is None:
            return result
        totalSize = 0
        i = 0
        for i, entry in enumerate(result):
            totalSize += len(entry[0])
            if totalSize >= maxSizeBytes:
                break
        return result[:i + 1]

    def _isLeader(self):
        """ Check if current node has a leader state.
        WARNING: there could be multiple leaders at the same time!

        :return: True if leader, False otherwise
        :rtype: bool
        """
        return self.__raftState == RaftState.LEADER

    def _getLeader(self):
        """ Returns last known leader.

        WARNING: this information could be outdated, eg. there could be another leader selected!
        WARNING: there could be multiple leaders at the same time!

        :return: the last known leader node.
        :rtype: Node
        """
        return self.__raftLeader

    def isReady(self):
        """Check if current node is initially synced with others and has an actual data.

        :return: True if ready, False otherwise
        :rtype: bool
        """
        return self.__onReadyCalled

    def _isReady(self):
        return self.isReady()

    def _getTerm(self):
        return self.__raftCurrentTerm

    def _getRaftLogSize(self):
        return len(self.__raftLog)

    def __deleteEntriesFrom(self, fromIDx):
        firstEntryIDx = self.__raftLog[0][1]
        diff = fromIDx - firstEntryIDx
        if diff < 0:
            return
        self.__raftLog.delete_entries_from(diff)

    def __deleteEntriesTo(self, toIDx):
        firstEntryIDx = self.__raftLog[0][1]
        diff = toIDx - firstEntryIDx
        if diff < 0:
            return
        self.__raftLog.delete_entries_to(diff)

    def __onBecomeLeader(self):
        self.__raftLeader = self.__selfNode
        self.__setState(RaftState.LEADER)

        self.__lastResponseTime.clear()
        for node in self.__otherNodes | self.__readonlyNodes:
            self.__raftNextIndex[node] = self.__getCurrentLogIndex() + 1
            self.__raftMatchIndex[node] = 0
            self.__lastResponseTime[node] = time.time()

        # No-op command after leader election.
        idx, term = self.__getCurrentLogIndex() + 1, self.__raftCurrentTerm
        self.__raftLog.add(_bchr(CommandType.NO_OP), idx, term)
        self.__noopIDx = idx
        if not self.__conf.appendEntriesUseBatch:
            self.__sendAppendEntries()

        self.__sendAppendEntries()

    def __setState(self, newState):
        oldState = self.__raftState
        self.__raftState = newState
        callback = self.__conf.onStateChanged
        if callback is not None and oldState != newState:
            callback(oldState, newState)

    def __onLeaderChanged(self):
        for id in sorted(self.__commandsWaitingReply):
            self.__commandsWaitingReply[id](None, FailReason.LEADER_CHANGED)
        self.__commandsWaitingReply = {}

    def __sendAppendEntries(self):
        self.__newAppendEntriesTime = time.time() + self.__conf.appendEntriesPeriod

        startTime = time.time()

        batchSizeBytes = self.__conf.appendEntriesBatchSizeBytes

        for node in self.__otherNodes | self.__readonlyNodes:
            if node not in self.__connectedNodes:
                self.__serializer.cancel_transmisstion(node)
                continue

            sendSingle = True
            sendingSerialized = False
            nextNodeIndex = self.__raftNextIndex[node]

            while nextNodeIndex <= self.__getCurrentLogIndex() or sendSingle or sendingSerialized:
                if nextNodeIndex > self.__raftLog[0][1]:
                    prevLogIdx, prevLogTerm = self.__getPrevLogIndexTerm(
                        nextNodeIndex)
                    entries = []
                    if nextNodeIndex <= self.__getCurrentLogIndex():
                        entries = self.__getEntries(
                            nextNodeIndex, None, batchSizeBytes)
                        self.__raftNextIndex[node] = entries[-1][1] + 1

                    if len(entries) == 1 and len(entries[0][0]) >= batchSizeBytes:
                        entry = pickle.dumps(entries[0])
                        for pos in xrange(0, len(entry), batchSizeBytes):
                            currData = entry[pos:pos + batchSizeBytes]
                            if pos == 0:
                                transmission = 'start'
                            elif pos + batchSizeBytes >= len(entries[0][0]):
                                transmission = 'finish'
                            else:
                                transmission = 'process'
                            message = {
                                'type': 'append_entries',
                                'transmission': transmission,
                                'data': currData,
                                'term': self.__raftCurrentTerm,
                                'commit_index': self.__raftCommitIndex,
                                'prevLogIdx': prevLogIdx,
                                'prevLogTerm': prevLogTerm,
                            }
                            self.__transport.send(node, message)
                    else:
                        message = {
                            'type': 'append_entries',
                            'term': self.__raftCurrentTerm,
                            'commit_index': self.__raftCommitIndex,
                            'entries': entries,
                            'prevLogIdx': prevLogIdx,
                            'prevLogTerm': prevLogTerm,
                        }
                        self.__transport.send(node, message)
                else:
                    transmissionData = self.__serializer.get_transmission_data(
                        node)
                    message = {
                        'type': 'append_entries',
                        'term': self.__raftCurrentTerm,
                        'commit_index': self.__raftCommitIndex,
                        'serialized': transmissionData,
                    }
                    self.__transport.send(node, message)
                    if transmissionData is not None:
                        isLast = transmissionData[2]
                        if isLast:
                            self.__raftNextIndex[node] = self.__raftLog[1][1] + 1
                            sendingSerialized = False
                        else:
                            sendingSerialized = True
                    else:
                        sendingSerialized = False

                nextNodeIndex = self.__raftNextIndex[node]

                sendSingle = False

                delta = time.time() - startTime
                if delta > self.__conf.appendEntriesPeriod:
                    break

    def __connectedToAnyone(self):
        return len(self.__connectedNodes) > 0 or len(self.__otherNodes) == 0

    def _getConf(self):
        return self.__conf

    @property
    def conf(self):
        return self.__conf

    def _getEncryptor(self):
        return self.__encryptor

    @property
    def encryptor(self):
        return self.__encryptor

    def __changeCluster(self, request):
        if self.__raftLastApplied < self.__noopIDx:
            # No-op entry was not commited yet
            return False

        if self.__changeClusterIDx is not None:
            if self.__raftLastApplied >= self.__changeClusterIDx:
                self.__changeClusterIDx = None

        # Previous cluster change request was not commited yet
        if self.__changeClusterIDx is not None:
            return False

        return self.__doChangeCluster(request)

    def __setCodeVersion(self, newVersion):
        self.__enabledCodeVersion = newVersion

    def __doChangeCluster(self, request, reverse=False):
        requestType = request[0]
        requestNodeId = request[1]
        if len(request) >= 3:
            requestNode = request[2]
            # Actually shouldn't be necessary, but better safe than sorry.
            if not isinstance(requestNode, Node):
                requestNode = self.__nodeClass(requestNode)
        else:
            requestNode = self.__nodeClass(requestNodeId)

        if requestType == 'add':
            adding = not reverse
        elif requestType == 'rem':
            adding = reverse
        else:
            return False

        if adding:
            newNode = requestNode
            # Node already exists in cluster
            if newNode is self.__selfNode or newNode in self.__otherNodes:
                return False
            self.__otherNodes.add(newNode)
            self.__raftNextIndex[newNode] = self.__getCurrentLogIndex() + 1
            self.__raftMatchIndex[newNode] = 0
            if self._isLeader():
                self.__lastResponseTime[newNode] = time.time()
            self.__transport.add_node(newNode)
            return True
        else:
            oldNode = requestNode
            if oldNode is self.__selfNode:
                return False
            if oldNode not in self.__otherNodes:
                return False
            self.__otherNodes.discard(oldNode)
            self.__raftNextIndex.pop(oldNode, None)
            self.__raftMatchIndex.pop(oldNode, None)
            self.__transport.drop_node(oldNode)
            return True

    @staticmethod
    def __parseChangeClusterRequest(command):
        commandType = ord(command[:1])
        if commandType != CommandType.MEMBERSHIP:
            return None
        return pickle.loads(command[1:])

    def __tryLogCompaction(self):
        currTime = time.time()
        serializeState, serializeID = self.__serializer.check_serializing()

        if serializeState == SerializerState.SUCCESS:
            self.__lastSerializedTime = currTime
            self.__deleteEntriesTo(serializeID)
            self.__lastSerializedEntry = serializeID

        if serializeState == SerializerState.FAILED:
            logging.warning('Failed to store full dump')

        if serializeState != SerializerState.NOT_SERIALIZING:
            return

        if len(self.__raftLog) <= self.__conf.logCompactionMinEntries and \
                currTime - self.__lastSerializedTime <= self.__conf.logCompactionMinTime and \
                not self.__forceLogCompaction:
            return

        if self.__conf.logCompactionSplit:
            allNodeIds = sorted([node.id for node in (
                self.__otherNodes | {self.__selfNode})])
            nodesCount = len(allNodeIds)
            selfIdx = allNodeIds.index(self.__selfNode.id)
            interval = self.__conf.logCompactionMinTime
            periodStart = int(currTime / interval) * interval
            nodeInterval = float(interval) / nodesCount
            nodeIntervalStart = periodStart + selfIdx * nodeInterval
            nodeIntervalEnd = nodeIntervalStart + 0.3 * nodeInterval
            if currTime < nodeIntervalStart or currTime >= nodeIntervalEnd:
                return

        self.__forceLogCompaction = False

        lastAppliedEntries = self.__getEntries(self.__raftLastApplied - 1, 2)
        if len(lastAppliedEntries) < 2 or lastAppliedEntries[0][1] == self.__lastSerializedEntry:
            self.__lastSerializedTime = currTime
            return

        if self.__conf.serializer is None:
            selfData = dict([(k, v) for k, v in iteritems(
                self.__dict__) if k not in self.__properies])
            data = selfData
            if self.__consumers:
                data = [selfData]
                for consumer in self.__consumers:
                    data.append(consumer._serialize())
        else:
            data = None
        cluster = self.__otherNodes | {self.__selfNode}
        self.__serializer.serialize((data, lastAppliedEntries[1], lastAppliedEntries[0], cluster),
                                    lastAppliedEntries[0][1])

    def __loadDumpFile(self, clearJournal):
        try:
            data = self.__serializer.deserialize()
            if data[0] is not None:
                if self.__consumers:
                    selfData = data[0][0]
                    consumersData = data[0][1:]
                else:
                    selfData = data[0]
                    consumersData = []

                for k, v in iteritems(selfData):
                    self.__dict__[k] = v

                for i, consumer in enumerate(self.__consumers):
                    consumer._deserialize(consumersData[i])

            if clearJournal or \
                    len(self.__raftLog) < 2 or \
                    self.__raftLog[0] != data[2] or \
                    self.__raftLog[1] != data[1]:
                self.__raftLog.clear()
                self.__raftLog.add(*data[2])
                self.__raftLog.add(*data[1])

            self.__raftLastApplied = data[1][1]

            if self.__conf.dynamicMembershipChange:
                self.__updateClusterConfiguration(
                    [node for node in data[3] if node != self.__selfNode])
            self.__onSetCodeVersion(0)
        except:
            logging.exception('failed to load full dump')

    def __updateClusterConfiguration(self, newNodes):
        # newNodes: list of Node or node ID
        newNodes = {self.__nodeClass(node) if not isinstance(
            node, Node) else node for node in newNodes}
        nodesToRemove = self.__otherNodes - newNodes
        nodesToAdd = newNodes - self.__otherNodes
        for node in nodesToRemove:
            self.__raftNextIndex.pop(node, None)
            self.__raftMatchIndex.pop(node, None)
            self.__transport.drop_node(node)
        self.__otherNodes = newNodes
        for node in nodesToAdd:
            self.__transport.add_node(node)
            self.__raftNextIndex[node] = self.__getCurrentLogIndex() + 1
            self.__raftMatchIndex[node] = 0


def __copy_func(f, name):
    if is_py3:
        res = types.FunctionType(
            f.__code__, f.__globals__, name, f.__defaults__, f.__closure__)
        res.__dict__ = f.__dict__
    else:
        res = types.FunctionType(
            f.func_code, f.func_globals, name, f.func_defaults, f.func_closure)
        res.func_dict = f.func_dict
    return res


class AsyncResult(object):
    def __init__(self):
        self.result = None
        self.error = None
        self.event = threading.Event()

    def onResult(self, res, err):
        self.result = res
        self.error = err
        self.event.set()


def replicated(*decArgs, **decKwargs):
    """Replicated decorator. Use it to mark your class members that modifies
    a class state. Function will be called asynchronously. Function accepts
    flowing additional parameters (optional):
        'callback': callback(result, failReason), failReason - `FAIL_REASON <#pysyncobj.FAIL_REASON>`_.
        'sync': True - to block execution and wait for result, False - async call. If callback is passed,
            'sync' option is ignored.
        'timeout': if 'sync' is enabled, and no result is available for 'timeout' seconds -
            SyncObjException will be raised.
    These parameters are reserved and should not be used in kwargs of your replicated method.

    :param func: arbitrary class member
    :type func: function
    :param ver: (optional) - code version (for zero deployment)
    :type ver: int
    """

    def replicatedImpl(func):
        def newFunc(self, *args, **kwargs):

            if kwargs.pop('_doApply', False):
                return func(self, *args, **kwargs)
            else:
                if isinstance(self, SyncObj):
                    applier = self._applyCommand
                    funcName = self._getFuncName(func.__name__)
                    funcID = self._methodToID[funcName]
                elif isinstance(self, SyncObjConsumer):
                    consumerId = id(self)
                    funcName = self._syncObj._getFuncName(
                        (consumerId, func.__name__))
                    funcID = self._syncObj._methodToID[(consumerId, funcName)]
                    applier = self._syncObj._applyCommand
                else:
                    raise SyncObjException(
                        "Class should be inherited from SyncObj or SyncObjConsumer")

                callback = kwargs.pop('callback', None)
                if kwargs:
                    cmd = (funcID, args, kwargs)
                elif args and not kwargs:
                    cmd = (funcID, args)
                else:
                    cmd = funcID
                sync = kwargs.pop('sync', False)
                if callback is not None:
                    sync = False

                if sync:
                    asyncResult = AsyncResult()
                    callback = asyncResult.onResult

                timeout = kwargs.pop('timeout', None)
                applier(pickle.dumps(cmd), callback, CommandType.REGULAR)

                if sync:
                    res = asyncResult.event.wait(timeout)
                    if not res:
                        raise SyncObjException('Timeout')
                    if not asyncResult.error == 0:
                        raise SyncObjException(asyncResult.error)
                    return asyncResult.result

        func_dict = newFunc.__dict__ if is_py3 else newFunc.func_dict
        func_dict['replicated'] = True
        func_dict['ver'] = int(decKwargs.get('ver', 0))
        func_dict['origName'] = func.__name__

        callframe = sys._getframe(1 if decKwargs else 2)
        namespace = callframe.f_locals
        newFuncName = func.__name__ + '_v' + str(func_dict['ver'])
        namespace[newFuncName] = __copy_func(newFunc, newFuncName)
        functools.update_wrapper(newFunc, func)
        return newFunc

    if len(decArgs) == 1 and len(decKwargs) == 0 and callable(decArgs[0]):
        return replicatedImpl(decArgs[0])

    return replicatedImpl


def replicated_sync(*decArgs, **decKwargs):
    def replicated_sync_impl(func, timeout=None):
        """Same as replicated, but synchronous by default.

        :param func: arbitrary class member
        :type func: function
        :param timeout: time to wait (seconds). Default: None
        :type timeout: float or None
        """

        def newFunc(self, *args, **kwargs):
            if kwargs.get('_doApply', False):
                return replicated(func)(self, *args, **kwargs)
            else:
                kwargs.setdefault('timeout', timeout)
                kwargs.setdefault('sync', True)
                return replicated(func)(self, *args, **kwargs)

        func_dict = newFunc.__dict__ if is_py3 else newFunc.func_dict
        func_dict['replicated'] = True
        func_dict['ver'] = int(decKwargs.get('ver', 0))
        func_dict['origName'] = func.__name__

        callframe = sys._getframe(1 if decKwargs else 2)
        namespace = callframe.f_locals
        newFuncName = func.__name__ + '_v' + str(func_dict['ver'])
        namespace[newFuncName] = __copy_func(newFunc, newFuncName)
        functools.update_wrapper(newFunc, func)
        return newFunc

    if len(decArgs) == 1 and len(decKwargs) == 0 and callable(decArgs[0]):
        return replicated_sync_impl(decArgs[0])

    return replicated_sync_impl
