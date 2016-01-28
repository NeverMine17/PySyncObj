import time
import random
import select
import socket
import cPickle
import zlib
import struct
import threading
import Queue
import weakref
import sys

class CONFIG:
	CONNECTION_RETRY_TIME = 5.0
	CONNECTION_TIMEOUT = 5.0
	DNS_CACHE_TIME = 600.0
	DNS_FAIL_CACHE_TIME = 30.0

class NODE_STATUS:
	DISCONNECTED = 0
	CONNECTING = 1
	CONNECTED = 2


class _DnsCachingResolver(object):

	def __init__(self):
		self.__cache = {}

	def resolve(self, hostname):
		currTime = time.time()
		cachedTime, ips = self.__cache.get(hostname, (0, []))
		timePassed = currTime - cachedTime
		if (timePassed > CONFIG.DNS_CACHE_TIME) or (not ips and timePassed > CONFIG.DNS_FAIL_CACHE_TIME):
			prevIps = ips
			ips = self.__doResolve(hostname)
			if not ips:
				ips = prevIps
			self.__cache[hostname] = (currTime, ips)
		return None if not ips else random.choice(ips)

	def __doResolve(self, hostname):
		try:
			ips = socket.gethostbyname_ex(hostname)[2]
		except socket.gaierror:
			print 'failed to resolve host %s' % hostname
			ips = []
		return ips


class _Connection(object):

	def __init__(self, socket = None):
		self.__socket = socket
		self.__readBuffer = ''
		self.__writeBuffer = ''
		self.__lastReadTime = time.time()
		self.__timeout = 10.0
		self.__disconnected = False

	def __del__(self):
		self.__socket = None

	def isDisconnected(self):
		return self.__disconnected or time.time() - self.__lastReadTime > self.__timeout

	def connect(self, host, port):
		self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.__socket.settimeout(CONFIG.CONNECTION_TIMEOUT)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 ** 13)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 ** 13)
		self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		self.__socket.setblocking(0)
		self.__readBuffer = ''
		self.__writeBuffer = ''
		self.__lastReadTime = time.time()

		try:
			self.__socket.connect((host, port))
		except socket.error as e:
			if e.errno != socket.errno.EINPROGRESS:
				return False
		self.__disconnected = False
		return True

	def send(self, message):
		data = zlib.compress(cPickle.dumps(message, -1), 3)
		data = struct.pack('i', len(data)) + data
		self.__writeBuffer += data
		self.trySendBuffer()

	def trySendBuffer(self):
		while self.processSend():
			pass

	def processSend(self):
		if not self.__writeBuffer:
			return False
		try:
			res = self.__socket.send(self.__writeBuffer)
			if res < 0:
				self.__writeBuffer = ''
				self.__readBuffer = ''
				self.__disconnected = True
				return False
			if res == 0:
				return False

			self.__writeBuffer = self.__writeBuffer[res:]
			return True
		except socket.error as e:
			if e.errno != socket.errno.EAGAIN:
				self.__writeBuffer = ''
				self.__readBuffer = ''
				self.__disconnected = True
			return False

	def getSendBufferSize(self):
		return len(self.__writeBuffer)

	def read(self):
		try:
			incoming = self.__socket.recv(2 ** 13)
		except socket.error as e:
			if e.errno != socket.errno.EAGAIN:
				self.__disconnected = True
			return
		if self.__socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
			self.__disconnected = True
			return
		if not incoming:
			return
		self.__lastReadTime = time.time()
		self.__readBuffer += incoming

	def getMessage(self):
		if len(self.__readBuffer) < 4:
			return None
		l = struct.unpack('i', self.__readBuffer[:4])[0]
		if len(self.__readBuffer) - 4 < l:
			return None
		data = self.__readBuffer[4:4 + l]
		message = cPickle.loads(zlib.decompress(data))
		self.__readBuffer = self.__readBuffer[4 + l:]
		return message

	def close(self):
		self.__socket.close()

	def socket(self):
		return self.__socket


class _Node(object):

	def __init__(self, syncObj, nodeAddr):
		self.__syncObj = weakref.ref(syncObj)
		self.__nodeAddr = nodeAddr
		self.__ip = syncObj._getResolver().resolve(nodeAddr.split(':')[0])
		self.__port = int(nodeAddr.split(':')[1])
		self.__conn = _Connection()
		self.__shouldConnect = syncObj._getSelfNodeAddr() > nodeAddr
		self.__lastConnectAttemptTime = 0
		self.__lastPingTime = 0
		self.__status = NODE_STATUS.DISCONNECTED

	def __del__(self):
		self.__conn = None

	def onPartnerConnected(self, conn):
		self.__conn = conn
		self.__status = NODE_STATUS.CONNECTED

	def onTickStage1(self):
		if self.__shouldConnect:
			if self.__status == NODE_STATUS.DISCONNECTED:
				self.__connect()

		if self.__shouldConnect and self.__status == NODE_STATUS.CONNECTING:
			return (self.__conn.socket(), self.__conn.socket(), self.__conn.socket())
		if self.__status == NODE_STATUS.CONNECTED:
			readyWriteSocket = None
			if self.__conn.getSendBufferSize() > 0:
				readyWriteSocket = self.__conn.socket()
			return (self.__conn.socket(), readyWriteSocket, self.__conn.socket())
		return None

	def getStatus(self):
		return self.__status

	def isConnected(self):
		return self.__status == NODE_STATUS.CONNECTED

	def getAddress(self):
		return self.__nodeAddr

	def getSendBufferSize(self):
		return self.__conn.getSendBufferSize()

	def onTickStage2(self, rlist, wlist, xlist):

		if self.__shouldConnect:
			if self.__status == NODE_STATUS.CONNECTING:
				self.__checkConnected(rlist, wlist, xlist)

		if self.__status == NODE_STATUS.CONNECTED:
			if self.__conn.socket() in rlist:
				self.__conn.read()
				while True:
					message = self.__conn.getMessage()
					if message is None:
						break
					self.__syncObj()._onMessageReceived(self.__nodeAddr, message)
			if self.__conn.socket() in wlist:
				self.__conn.trySendBuffer()

			if self.__conn.socket() in xlist or self.__conn.isDisconnected():
				self.__status = NODE_STATUS.DISCONNECTED
				self.__conn.close()

	def send(self, message):
		if self.__status != NODE_STATUS.CONNECTED:
			return False
		self.__conn.send(message)
		if self.__conn.isDisconnected():
			self.__status = NODE_STATUS.DISCONNECTED
			self.__conn.close()
			return False
		return True

	def __connect(self):
		if time.time() - self.__lastConnectAttemptTime < CONFIG.CONNECTION_RETRY_TIME:
			return

		self.__status = NODE_STATUS.CONNECTING

		self.__lastConnectAttemptTime = time.time()

		if not self.__conn.connect(self.__ip, self.__port):
			self.__status = NODE_STATUS.DISCONNECTED

	def __checkConnected(self, rlist, wlist, xlist):
		if self.__conn.socket() in xlist:
			self.__status = NODE_STATUS.DISCONNECTED
		if self.__conn.socket() in rlist or self.__conn.socket() in wlist:
			if self.__conn.socket().getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
				self.__status = NODE_STATUS.DISCONNECTED
			else:
				self.__status = NODE_STATUS.CONNECTED
				self.__conn.send(self.__syncObj()._getSelfNodeAddr())


class _RAFT_STATE:
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2


class SyncObj(object):

	def __init__(self, selfNodeAddr, otherNodesAddrs, autoTick = True):
		self.__selfNodeAddr = selfNodeAddr
		self.__otherNodesAddrs = otherNodesAddrs
		self.__unknownConnections = []
		self.__raftState = _RAFT_STATE.FOLLOWER
		self.__raftCurrentTerm = 0
		self.__votesCount = 0
		self.__raftLeader = None
		self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
		self.__raftLog = []
		self.__raftLog.append((None, 1, self.__raftCurrentTerm))
		self.__raftCommitIndex = 1
		self.__raftLastApplied = 1
		self.__raftNextIndex = {}
		self.__raftMatchIndex = {}
		self.__lastSerialized = None
		self.__lastSerializedTime = 0
		self.__outgoingSerializedData = {}
		self.__incomingSerializedData = None
		self.__socket = None

		self._methodToID = {}
		self._idToMethod = {}
		methods = sorted([m for m in dir(self) if callable(getattr(self, m))])
		for i, method in enumerate(methods):
			self._methodToID[method] = i
			self._idToMethod[i] = getattr(self, method)

		self.__thread = None
		self.__mainThread = None
		self.__initialised = None
		self.__commandsQueue = Queue.Queue(1000)
		self.__resolver = None
		self.__nodes = []
		self.__newAppendEntriesTime = 0

		self.__properies = set()
		for key in self.__dict__:
			self.__properies.add(key)

		if autoTick:
			self.__mainThread = threading.current_thread()
			self.__initialised = threading.Event()
			self.__thread = threading.Thread(target=SyncObj._autoTickThread, args=(weakref.proxy(self),))
			self.__thread.start()
			while not self.__initialised.is_set():
				pass
		else:
			self.__initInTickThread()

	def __initInTickThread(self):
		self.__resolver = _DnsCachingResolver()
		self.__bind()
		for nodeAddr in self.__otherNodesAddrs:
			self.__nodes.append(_Node(self, nodeAddr))
			self.__raftNextIndex[nodeAddr] = 0
			self.__raftMatchIndex[nodeAddr] = 0

	def _applyCommand(self, command):
		try:
			self.__commandsQueue.put_nowait(command)
		except Queue.Full:
			#todo: call fail callback
			pass

	def _checkCommandsToApply(self):
		while True:
			try:
				command = self.__commandsQueue.get_nowait()
			except Queue.Empty:
				break
			if self.__raftState == _RAFT_STATE.LEADER:
				self.__raftLog.append((command, self.__getCurrentLogIndex() + 1, self.__raftCurrentTerm))
				#self.__sendAppendEntries()
			elif self.__raftLeader is not None:
				self.__send(self.__raftLeader, {
					'type': 'apply_command',
					'command': command,
				})
			else:
				pass

	def _autoTickThread(self):
		self.__initInTickThread()
		self.__initialised.set()
		try:
			while True:
				if not self.__mainThread.is_alive():
					break
				self._onTick(0.05)
		except ReferenceError:
			pass

	def _onTick(self, timeToWait = 0.0):

		if self.__raftState in (_RAFT_STATE.FOLLOWER, _RAFT_STATE.CANDIDATE):
			if self.__raftElectionDeadline < time.time():
				self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
				self.__raftLeader = None
				self.__raftState = _RAFT_STATE.CANDIDATE
				self.__raftCurrentTerm += 1
				self.__votesCount = 1
				for node in self.__nodes:
					node.send({
						'type': 'request_vote',
						'term': self.__raftCurrentTerm,
						'last_log_index': self.__raftLog[-1][1],
						'last_log_term': self.__raftLog[-1][2],
					})

		if self.__raftState == _RAFT_STATE.LEADER:
			while self.__raftCommitIndex < self.__raftLog[-1][1]:
				nextCommitIndex = self.__raftCommitIndex + 1
				count = 1
				for node in self.__nodes:
					if self.__raftMatchIndex[node.getAddress()] >= nextCommitIndex:
						count += 1
				if count > (len(self.__nodes) + 1) / 2:
					self.__raftCommitIndex = nextCommitIndex
				else:
					break

			if time.time() > self.__newAppendEntriesTime:
				self.__sendAppendEntries()

		if self.__raftCommitIndex > self.__raftLastApplied:
			count = self.__raftCommitIndex - self.__raftLastApplied
			entries = self.__getEntries(self.__raftLastApplied + 1, count)
			for entry in entries:
				self.__doApplyCommand(entry[0])
				self.__raftLastApplied += 1

		self._checkCommandsToApply()
		self.__tryLogCompaction()

		socketsToCheckR = [self.__socket]
		socketsToCheckW = [self.__socket]
		socketsToCheckX = [self.__socket]
		for conn in self.__unknownConnections:
			socketsToCheckR.append(conn.socket())
			socketsToCheckX.append(conn.socket())

		for node in self.__nodes:
			socks = node.onTickStage1()
			if socks is not None:
				sockR, sockW, sockX = socks
				if sockR is not None:
					socketsToCheckR.append(sockR)
				if sockW is not None:
					socketsToCheckW.append(sockW)
				if sockX is not None:
					socketsToCheckX.append(sockX)

		rlist, wlist, xlist = select.select(socketsToCheckR, socketsToCheckW, socketsToCheckX, timeToWait)

		if self.__socket in rlist:
			try:
				sock, addr = self.__socket.accept()
				sock.settimeout(CONFIG.CONNECTION_TIMEOUT)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 ** 13)
				sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 ** 13)
				sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
				sock.setblocking(0)
				self.__unknownConnections.append(_Connection(sock))
			except socket.error as e:
				if e.errno != socket.errno.EAGAIN:
					raise e

		if self.__socket in xlist:
			print ' === some error for main socket'
			# todo: handle

		self.__processUnknownConnections(rlist, xlist)

		for node in self.__nodes:
			node.onTickStage2(rlist, wlist, xlist)

	def _getLastCommitIndex(self):
		return self.__raftCommitIndex

	def printStatus(self):
		print 'self:    ', self.__selfNodeAddr
		print 'leader:  ', self.__raftLeader
		print 'partner nodes:', len(self.__nodes)
		for n in self.__nodes:
			print n.getAddress(), n.getStatus()
		print 'log size:', len(zlib.compress(cPickle.dumps(self.__raftLog, -1)))
		print ''

	def __doApplyCommand(self, command):
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

		self._idToMethod[funcID](*args, **kwargs)

	def _onMessageReceived(self, nodeAddr, message):
		if self.__raftState in (_RAFT_STATE.FOLLOWER, _RAFT_STATE.CANDIDATE):
			if message['type'] == 'request_vote':
				lastLogTerm = message['last_log_term']
				lastLogIdx = message['last_log_index']
				if message['term'] > self.__raftCurrentTerm:
					if lastLogTerm < self.__raftLog[-1][2]:
						return
					if lastLogTerm == self.__raftLog[-1][2] and \
							lastLogIdx < self.__raftLog[-1][1]:
						return

					self.__raftCurrentTerm = message['term']
					self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
					self.__send(nodeAddr, {
						'type': 'response_vote',
						'term': message['term'],
					})

		if message['type'] == 'append_entries' and message['term'] >= self.__raftCurrentTerm:
			self.__raftElectionDeadline = time.time() + self.__generateRaftTimeout()
			self.__raftLeader = nodeAddr
			self.__raftState = _RAFT_STATE.FOLLOWER
			newEntries = message.get('entries', [])
			serialized = message.get('serialized', None)
			leaderCommitIndex = message['commit_index']
			if newEntries:
				prevLogIdx = message['prevLogIdx']
				prevLogTerm = message['prevLogTerm']
				prevEntries = self.__getEntries(prevLogIdx)
				if not prevEntries:
					self.__sendNextNodeIdx(nodeAddr, reset = True)
					return
				if prevEntries[0][2] != prevLogTerm:
					self.__deleteEntriesFrom(prevLogIdx)
					self.__sendNextNodeIdx(nodeAddr, reset = True)
					return
				if len(prevEntries) > 1:
					self.__deleteEntriesFrom(prevLogIdx + 1)
				self.__raftLog += newEntries

			if serialized is not None:
				isLast = message.get('is_last')
				isFirst = message.get('is_first')
				if isFirst:
					self.__incomingSerializedData = ''
				self.__incomingSerializedData += serialized
				if isLast:
					self.__lastSerialized = self.__incomingSerializedData
					self.__incomingSerializedData = ''
					self._deserialize()

			self.__sendNextNodeIdx(nodeAddr)

			self.__raftCommitIndex = min(leaderCommitIndex, self.__raftLog[-1][1])

		if message['type'] == 'apply_command':
			self._applyCommand(message['command'])

		if self.__raftState == _RAFT_STATE.CANDIDATE:
			if message['type'] == 'response_vote' and message['term'] == self.__raftCurrentTerm:
				self.__votesCount += 1

				if self.__votesCount > (len(self.__nodes) + 1) / 2:
					self.__onBecomeLeader()

		if self.__raftState == _RAFT_STATE.LEADER:
			if message['type'] == 'next_node_idx':
				reset = message['reset']
				nextNodeIdx = message['next_node_idx']

				currentNodeIdx = nextNodeIdx - 1
				if reset:
					self.__raftNextIndex[nodeAddr] = nextNodeIdx
				self.__raftMatchIndex[nodeAddr] = currentNodeIdx

	def __sendNextNodeIdx(self, nodeAddr, reset = False):
		self.__send(nodeAddr, {
			'type': 'next_node_idx',
			'next_node_idx': self.__raftLog[-1][1] + 1,
			'reset': reset,
		})

	def __generateRaftTimeout(self):
		return 1.0 + 2.0 * random.random()

	def __bind(self):
		self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 ** 13)
		self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 ** 13)
		self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		self.__socket.setblocking(0)
		host, port = self.__selfNodeAddr.split(':')
		self.__socket.bind((host, int(port)))
		self.__socket.listen(5)

	def __getCurrentLogIndex(self):
		return self.__raftLog[-1][1]

	def __getPrevLogIndexTerm(self, nextNodeIndex):
		prevIndex = nextNodeIndex - 1
		entries = self.__getEntries(prevIndex, 1)
		if entries:
			return prevIndex, entries[0][2]
		return None, None

	def __getEntries(self, fromIDx, count = None):
		firstEntryIDx = self.__raftLog[0][1]
		if fromIDx < firstEntryIDx:
			return []
		diff = fromIDx - firstEntryIDx
		if count is None:
			return self.__raftLog[diff:]
		return self.__raftLog[diff:diff + count]

	def _isLeader(self):
		return self.__raftState == _RAFT_STATE.LEADER

	def _getLeader(self):
		return self.__raftLeader

	def _getRaftLogSize(self):
		return len(self.__raftLog)

	def __deleteEntriesFrom(self, fromIDx):
		firstEntryIDx = self.__raftLog[0][1]
		diff = fromIDx - firstEntryIDx
		if diff < 0:
			return
		self.__raftLog = self.__raftLog[:diff]

	def __deleteEntriesTo(self, toIDx):
		firstEntryIDx = self.__raftLog[0][1]
		diff = toIDx - firstEntryIDx
		if diff < 0:
			return
		self.__raftLog = self.__raftLog[diff:]

	def __onBecomeLeader(self):
		self.__raftLeader = self.__selfNodeAddr
		self.__raftState = _RAFT_STATE.LEADER

		for node in self.__nodes:
			nodeAddr = node.getAddress()
			self.__raftNextIndex[nodeAddr] = self.__getCurrentLogIndex() + 1
			self.__raftMatchIndex[nodeAddr] = 0

		self.__sendAppendEntries()

	def __sendAppendEntries(self):
		self.__newAppendEntriesTime = time.time() + 0.3

		startTime = time.time()

		for node in self.__nodes:
			nodeAddr = node.getAddress()

			if not node.isConnected():
				self.__outgoingSerializedData.pop(nodeAddr, 0)
				continue

			sendSingle = True
			sendingSerialized = False
			nextNodeIndex = self.__raftNextIndex[nodeAddr]
			prevLogIdx, prevLogTerm = None, None
			entries = []

			while nextNodeIndex <= self.__getCurrentLogIndex() or sendSingle or sendingSerialized:
				if nextNodeIndex >= self.__raftLog[0][1]:
					if nextNodeIndex <= self.__getCurrentLogIndex():
						entries = self.__getEntries(nextNodeIndex, 1000)
						prevLogIdx, prevLogTerm = self.__getPrevLogIndexTerm(nextNodeIndex)
						self.__raftNextIndex[nodeAddr] = entries[-1][1] + 1

					message = {
						'type': 'append_entries',
						'term': self.__raftCurrentTerm,
						'commit_index': self.__raftCommitIndex,
						'entries': entries,
						'prevLogIdx': prevLogIdx,
						'prevLogTerm': prevLogTerm,
					}
					node.send(message)
					nextNodeIndex = self.__raftNextIndex[nodeAddr]
				else:
					alreadyTansmitted = self.__outgoingSerializedData.get(nodeAddr, 0)
					currentChunk = self.__lastSerialized[alreadyTansmitted:alreadyTansmitted + 2 ** 13]
					isLast = alreadyTansmitted + len(currentChunk) == len(self.__lastSerialized)
					isFirst = alreadyTansmitted == 0
					message = {
						'type': 'append_entries',
						'term': self.__raftCurrentTerm,
						'commit_index': self.__raftCommitIndex,
						'serialized': currentChunk,
						'is_last': isLast,
						'is_first': isFirst,
					}
					node.send(message)
					if isLast:
						nextNodeIndex = self.__raftLog[0][1]
						self.__outgoingSerializedData.pop(nodeAddr, 0)
						sendingSerialized = False
					else:
						sendingSerialized = True
						self.__outgoingSerializedData[nodeAddr] = alreadyTansmitted + len(currentChunk)

				sendSingle = False

				delta = time.time() - startTime
				if delta > 0.3:
					break


	def __send(self, nodeAddr, message):
		for node in self.__nodes:
			if node.getAddress() == nodeAddr:
				node.send(message)
				break

	def __processUnknownConnections(self, rlist, xlist):
		newUnknownConnections = []
		for conn in self.__unknownConnections:
			remove = False
			if conn.socket() in rlist:
				conn.read()
				nodeAddr = conn.getMessage()
				if nodeAddr is not None:
					for node in self.__nodes:
						if node.getAddress() == nodeAddr:
							node.onPartnerConnected(conn)
							break
					remove = True
			if conn.socket() in xlist:
				remove = True
			if not remove and not conn.isDisconnected():
				newUnknownConnections.append(conn)

		self.__unknownConnections = newUnknownConnections

	def _getSelfNodeAddr(self):
		return self.__selfNodeAddr

	def _getResolver(self):
		return self.__resolver

	def __tryLogCompaction(self):
		currTime = time.time()
		if len(self.__raftLog) > 5000 and currTime - self.__lastSerializedTime > 60:
			self._serialize()
			self.__lastSerializedTime = currTime

	def _serialize(self):
		data = dict([(k, self.__dict__[k]) for k in self.__dict__.keys() if k not in self.__properies])
		lastAppliedEntry = self.__getEntries(self.__raftLastApplied, 1)[0]
		self.__lastSerialized = zlib.compress(cPickle.dumps((data, lastAppliedEntry), -1), 3)
		self.__deleteEntriesTo(lastAppliedEntry[1])
		self.__outgoingSerializedData = {}

	def _deserialize(self):
		try:
			data = cPickle.loads(zlib.decompress(self.__lastSerialized))
		except:
			return
		for k, v in data[0].iteritems():
			self.__dict__[k] = v
		self.__raftLog = [data[1]]
		self.__raftLastApplied = data[1][1]

def replicated(func):
	def newFunc(self, *args, **kwargs):
		if kwargs.pop('_doApply', False):
			func(self, *args, **kwargs)
		else:
			if args and kwargs:
				cmd = (self._methodToID[func.__name__], args, kwargs)
			elif args and not kwargs:
				cmd = (self._methodToID[func.__name__], args)
			else:
				cmd = self._methodToID[func.__name__]
			self._applyCommand(cmd)
	return newFunc
