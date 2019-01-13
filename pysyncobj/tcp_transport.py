import functools
import os
import threading
import time

from pysyncobj import FailReason
from pysyncobj.dns_resolver import global_dns_resolver
from pysyncobj.node import Node
from pysyncobj.tcp_connection import CONNECTION_STATE, TCPConnection
from pysyncobj.tcp_node import TCPNode
from pysyncobj.tcp_server import TCPServer
from pysyncobj.transport import Transport, TransportNotReadyError


class TCPTransport(Transport):
    def __init__(self, syncObj, selfNode, otherNodes):
        """
        Initialise the TCP transport. On normal (non-read-only) nodes, this will start a TCP server. On all nodes, it will initiate relevant connections to other nodes.

        :param syncObj: SyncObj
        :type syncObj: SyncObj
        :param selfNode: current node (None if this is a read-only node)
        :type selfNode: TCPNode or None
        :param otherNodes: partner nodes
        :type otherNodes: iterable of TCPNode
        """

        super(TCPTransport, self).__init__(syncObj, selfNode, otherNodes)
        self._syncObj = syncObj
        self._server = None
        self._connections = {}  # Node object -> TcpConnection object
        self._unknownConnections = set()  # set of TcpConnection objects
        self._selfNode = selfNode
        self._selfIsReadonlyNode = selfNode is None
        self._nodes = set()  # set of TCPNode
        self._readonlyNodes = set()  # set of Node
        # node ID/address -> TCPNode (does not include read-only nodes)
        self._nodeAddrToNode = {}
        self._lastConnectAttempt = {}  # TPCNode -> float (seconds since epoch)
        # set of TCPNode to which no (re)connection should be triggered on
        self._preventConnectNodes = set()
        # _connectIfNecessary; used via drop_node and destroy to cleanly remove a node
        self._readonlyNodesCounter = 0
        self._lastBindAttemptTime = 0
        self._bindAttempts = 0
        # gets triggered either when the server has either been bound
        self._bindOverEvent = threading.Event()
        # correctly or when the number of bind attempts exceeds the config value maxBindRetries
        self._ready = False

        self._syncObj.addOnTickCallback(self._on_tick)

        for node in otherNodes:
            self.add_node(node)

        if not self._selfIsReadonlyNode:
            self._create_server()
        else:
            self._ready = True

    def _conn_to_node(self, conn):
        """
        Find the node to which a connection belongs.

        :param conn: connection object
        :type conn: TCPConnection
        :returns corresponding node or None if the node cannot be found
        :rtype Node or None
        """

        for node in self._connections:
            if self._connections[node] is conn:
                return node
        return None

    def try_get_ready(self):
        """
        Try to bind the server if necessary.

        :raises TransportNotReadyError if the server could not be bound
        """

        self._maybe_bind()

    @property
    def ready(self):
        return self._ready

    def _create_server(self):
        """
        Create the TCP server (but don't bind yet)
        """

        conf = self._syncObj.conf
        bind_addr = conf.bindAddress or getattr(self._selfNode, 'address')
        if not bind_addr:
            raise RuntimeError('Unable to determine bind address')
        host, port = bind_addr.rsplit(':', 1)
        host = global_dns_resolver().resolve(host)
        self._server = TCPServer(self._syncObj._poller, host, port, on_new_connection=self._on_new_incoming_connection,
                                 send_buffer_size=conf.sendBufferSize,
                                 recv_buffer_size=conf.recvBufferSize,
                                 connection_timeout=conf.connectionTimeout)

    def _maybe_bind(self):
        """
        Bind the server unless it is already bound, this is a read-only node, or the last attempt was too recently.

        :raises TransportNotReadyError if the bind attempt fails
        """

        if self._ready or self._selfIsReadonlyNode or time.time() < self._lastBindAttemptTime + self._syncObj.conf.bindRetryTime:
            return
        self._lastBindAttemptTime = time.time()
        try:
            self._server.bind()
        except Exception as e:
            self._bindAttempts += 1
            if self._syncObj.conf.maxBindRetries and self._bindAttempts >= self._syncObj.conf.maxBindRetries:
                self._bindOverEvent.set()
                raise TransportNotReadyError
        else:
            self._ready = True
            self._bindOverEvent.set()

    def _on_tick(self):
        """
        Tick callback. Binds the server and connects to other nodes as necessary.
        """

        try:
            self._maybe_bind()
        except TransportNotReadyError:
            pass
        self._connect_if_necessary()

    def _on_new_incoming_connection(self, conn):
        """
        Callback for connections initiated by the other side

        :param conn: connection object
        :type conn: TCPConnection
        """

        self._unknownConnections.add(conn)
        encryptor = self._syncObj.encryptor
        if encryptor:
            conn.encryptor = encryptor
        conn.setOnMessageReceivedCallback(functools.partial(
            self._on_incoming_message_received, conn))
        conn.setOnDisconnectedCallback(
            functools.partial(self._onDisconnected, conn))

    def _on_incoming_message_received(self, conn, message):
        """
        Callback for initial messages on incoming connections. Handles encryption, utility messages, and association of the connection with a Node.
        Once this initial setup is done, the relevant connected callback is executed, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TCPConnection
        :param message: received message
        :type message: any
        """

        if self._syncObj.encryptor and not conn.sendRandKey:
            conn.sendRandKey = message
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
            return

        # Utility messages
        if isinstance(message, list):
            done = False
            try:
                if message[0] == 'status':
                    conn.send(self._syncObj.getStatus())
                    done = True
                elif message[0] == 'add':
                    self._syncObj.add_node_to_cluster(message[1],
                                                      callback=functools.partial(self._utility_callback, conn=conn,
                                                                                 cmd='ADD', arg=message[1]))
                    done = True
                elif message[0] == 'remove':
                    if message[1] == self._selfNode.address:
                        conn.send('FAIL REMOVE ' + message[1])
                    else:
                        self._syncObj.remove_node_from_cluster(message[1],
                                                               callback=functools.partial(self._utility_callback,
                                                                                          conn=conn,
                                                                                          cmd='REMOVE', arg=message[1]))
                    done = True
                elif message[0] == 'set_version':
                    self._syncObj.set_code_version(message[1],
                                                   callback=functools.partial(self._utility_callback, conn=conn,
                                                                              cmd='SET_VERSION', arg=str(message[1])))
                    done = True
            except Exception as e:
                conn.send(str(e))
                done = True
            if done:
                return

        # At this point, message should be either a node ID (i.e. address) or 'readonly'
        node = self._nodeAddrToNode[message] if message in self._nodeAddrToNode else None

        if node is None and message != 'readonly':
            conn.disconnect()
            self._unknownConnections.discard(conn)
            return

        readonly = node is None
        if readonly:
            nodeId = str(self._readonlyNodesCounter)
            node = Node(nodeId)
            self._readonlyNodes.add(node)
            self._readonlyNodesCounter += 1

        self._unknownConnections.discard(conn)
        self._connections[node] = conn
        conn.setOnMessageReceivedCallback(
            functools.partial(self._on_message_received, node))
        if not readonly:
            self._on_node_connected(node)
        else:
            self._on_readonly_node_connected(node)

    @staticmethod
    def _utility_callback(res, err, conn, cmd, arg):
        """
        Callback for the utility messages

        :param res: result of the command
        :param err: error code (one of pysyncobj.config.FAIL_REASON)
        :param conn: utility connection
        :param cmd: command
        :param arg: command arguments
        """

        cmdResult = 'FAIL'
        if err == FailReason.SUCCESS:
            cmdResult = 'SUCCESS'
        conn.send(cmdResult + ' ' + cmd + ' ' + arg)

    def _should_connect(self, node):
        """
        Check whether this node should initiate a connection to another node

        :param node: the other node
        :type node: Node
        """

        return isinstance(node, TCPNode) and node not in self._preventConnectNodes and (
            self._selfIsReadonlyNode or self._selfNode.address > node.address)

    def _connect_if_necessary_single(self, node):
        """
        Connect to a node if necessary.

        :param node: node to connect to
        :type node: Node
        """

        if node in self._connections and self._connections[node].state != CONNECTION_STATE.DISCONNECTED:
            return True
        if not self._should_connect(node):
            return False
        # Since we "should connect" to this node, there should always be a connection
        assert node in self._connections
        # object already in place.
        if node in self._lastConnectAttempt and time.time() - self._lastConnectAttempt[
                node] < self._syncObj.conf.connectionRetryTime:
            return False
        self._lastConnectAttempt[node] = time.time()
        return self._connections[node].connect(node.ip, node.port)

    def _connect_if_necessary(self):
        """
        Connect to all nodes as necessary.
        """

        for node in self._nodes:
            self._connect_if_necessary_single(node)

    def _on_outgoing_connected(self, conn):
        """
        Callback for when a new connection from this to another node is established. Handles encryption and informs the other node which node this is.
        If encryption is disabled, this triggers the onNodeConnected callback and messages are deferred to the onMessageReceived callback.
        If encryption is enabled, the first message is handled by _on_outgoing_message_received.

        :param conn: connection object
        :type conn: TCPConnection
        """

        if self._syncObj.encryptor:
            conn.setOnMessageReceivedCallback(
                functools.partial(self._on_outgoing_message_received, conn))  # So we can process the sendRandKey
            conn.recvRandKey = os.urandom(32)
            conn.send(conn.recvRandKey)
        else:
            # The onMessageReceived callback is configured in add_node already.
            if not self._selfIsReadonlyNode:
                conn.send(self._selfNode.address)
            else:
                conn.send('readonly')
            self._on_node_connected(self._conn_to_node(conn))

    def _on_outgoing_message_received(self, conn, message):
        """
        Callback for receiving a message on a new outgoing connection. Used only if encryption is enabled to exchange the random keys.
        Once the key exchange is done, this triggers the onNodeConnected callback, and further messages are deferred to the onMessageReceived callback.

        :param conn: connection object
        :type conn: TCPConnection
        :param message: received message
        :type message: any
        """

        if not conn.sendRandKey:
            conn.sendRandKey = message
            conn.send(self._selfNode.address)

        node = self._conn_to_node(conn)
        conn.setOnMessageReceivedCallback(
            functools.partial(self._on_message_received, node))
        self._on_node_connected(node)

    def _onDisconnected(self, conn):
        """
        Callback for when a connection is terminated or considered dead. Initiates a reconnect if necessary.

        :param conn: connection object
        :type conn: TCPConnection
        """

        self._unknownConnections.discard(conn)
        node = self._conn_to_node(conn)
        if node is not None:
            if node in self._nodes:
                self._on_node_disconnected(node)
                self._connect_if_necessary_single(node)
            else:
                self._readonlyNodes.discard(node)
                self._on_readonly_node_disconnected(node)

    def wait_ready(self):
        """
        Wait for the TCP transport to become ready for operation, i.e. the server to be bound.
        This method should be called from a different thread than used for the SyncObj ticks.

        :raises TransportNotReadyError: if the number of bind tries exceeds the configured limit
        """

        self._bindOverEvent.wait()
        if not self._ready:
            raise TransportNotReadyError

    def add_node(self, node):
        """
        Add a node to the network

        :param node: node to add
        :type node: TCPNode
        """

        self._nodes.add(node)
        self._nodeAddrToNode[node.address] = node
        if self._should_connect(node):
            conn = TCPConnection(poller=self._syncObj._poller,
                                 timeout=self._syncObj.conf.connectionTimeout,
                                 sendBufferSize=self._syncObj.conf.sendBufferSize,
                                 recvBufferSize=self._syncObj.conf.recvBufferSize)
            conn.encryptor = self._syncObj.encryptor
            conn.setOnConnectedCallback(functools.partial(
                self._on_outgoing_connected, conn))
            conn.setOnMessageReceivedCallback(
                functools.partial(self._on_message_received, node))
            conn.setOnDisconnectedCallback(
                functools.partial(self._onDisconnected, conn))
            self._connections[node] = conn

    def drop_node(self, node):
        """
        Drop a node from the network

        :param node: node to drop
        :type node: Node
        """

        conn = self._connections.pop(node, None)
        if conn is not None:
            # Calling conn.disconnect() immediately triggers the on_disconnected callback if the connection
            # isn't already disconnected, so this is necessary to prevent the automatic reconnect.
            self._preventConnectNodes.add(node)
            conn.disconnect()
            self._preventConnectNodes.remove(node)
        if isinstance(node, TCPNode):
            self._nodes.discard(node)
            self._nodeAddrToNode.pop(node.address, None)
        else:
            self._readonlyNodes.discard(node)
        self._lastConnectAttempt.pop(node, None)

    def send(self, node, message):
        """
        Send a message to a node. Returns False if the connection appears to be dead either before or after actually trying to send the message.

        :param node: target node
        :type node: Node
        :param message: message
        :param message: any
        :returns success
        :rtype bool
        """

        if node not in self._connections or self._connections[node].state != CONNECTION_STATE.CONNECTED:
            return False
        self._connections[node].send(message)
        if self._connections[node].state != CONNECTION_STATE.CONNECTED:
            return False
        return True

    def destroy(self):
        """
        Destroy this transport
        """

        self.set_on_message_received_callback(None)
        self.set_on_node_connected_callback(None)
        self.set_on_node_disconnected_callback(None)
        self.set_on_readonly_node_connected_callback(None)
        self.set_on_readonly_node_disconnected_callback(None)
        for node in self._nodes | self._readonlyNodes:
            self.drop_node(node)
        if self._server is not None:
            self._server.unbind()
        for conn in self._unknownConnections:
            conn.disconnect()
        self._unknownConnections = set()
