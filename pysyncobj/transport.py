from .node import Node


class TransportNotReadyError(Exception):
    """Transport failed to get ready for operation."""


class Transport(object):
    """Base class for implementing a transport between PySyncObj nodes"""

    def __init__(self, syncObj, selfNode, otherNodes):
        """
        Initialise the transport

        :param syncObj: SyncObj
        :type syncObj: SyncObj
        :param selfNode: current server node, or None if this is a read-only node
        :type selfNode: Node or None
        :param otherNodes: partner nodes
        :type otherNodes: list of Node
        """

        self._onMessageReceivedCallback = None
        self._onNodeConnectedCallback = None
        self._onNodeDisconnectedCallback = None
        self._onReadonlyNodeConnectedCallback = None
        self._onReadonlyNodeDisconnectedCallback = None

    def setOnMessageReceivedCallback(self, callback):
        """
        Set the callback for when a message is received, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node, message: any) or None
        """

        self._onMessageReceivedCallback = callback

    def setOnNodeConnectedCallback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is established, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeConnectedCallback = callback

    def setOnNodeDisconnectedCallback(self, callback):
        """
        Set the callback for when the connection to a (non-read-only) node is terminated or is considered dead, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onNodeDisconnectedCallback = callback

    def setOnReadonlyNodeConnectedCallback(self, callback):
        """
        Set the callback for when a read-only node connects, or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeConnectedCallback = callback

    def setOnReadonlyNodeDisconnectedCallback(self, callback):
        """
        Set the callback for when a read-only node disconnects (or the connection is lost), or disable callback by passing None

        :param callback callback
        :type callback function(node: Node) or None
        """

        self._onReadonlyNodeDisconnectedCallback = callback

    # Helper functions so you don't need to check for the callbacks manually in subclasses
    def _onMessageReceived(self, node, message):
        if self._onMessageReceivedCallback is not None:
            self._onMessageReceivedCallback(node, message)

    def _onNodeConnected(self, node):
        if self._onNodeConnectedCallback is not None:
            self._onNodeConnectedCallback(node)

    def _onNodeDisconnected(self, node):
        if self._onNodeDisconnectedCallback is not None:
            self._onNodeDisconnectedCallback(node)

    def _onReadonlyNodeConnected(self, node):
        if self._onReadonlyNodeConnectedCallback is not None:
            self._onReadonlyNodeConnectedCallback(node)

    def _onReadonlyNodeDisconnected(self, node):
        if self._onReadonlyNodeDisconnectedCallback is not None:
            self._onReadonlyNodeDisconnectedCallback(node)

    def tryGetReady(self):
        """
        Try to get the transport ready for operation. This may for example mean binding a server to a port.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    @property
    def ready(self):
        """
        Whether the transport is ready for operation.

        :rtype bool
        """

        return True

    def waitReady(self):
        """
        Wait for the transport to be ready.

        :raises TransportNotReadyError: if the transport fails to get ready for operation
        """

    def addNode(self, node):
        """
        Add a node to the network

        :param node node to add
        :type node Node
        """

    def dropNode(self, node):
        """
        Remove a node from the network (meaning connections, buffers, etc. related to this node can be dropped)

        :param node node to drop
        :type node Node
        """

    def send(self, node, message):
        """
        Send a message to a node.
        The message should be picklable.
        The return value signifies whether the message is thought to have been sent successfully. It does not necessarily mean that the message actually arrived at the node.

        :param node target node
        :type node Node
        :param message message
        :type message any
        :returns success
        :rtype bool
        """

        raise NotImplementedError

    def destroy(self):
        """
        Destroy the transport
        """


