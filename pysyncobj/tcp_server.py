import socket

from .poller import PollEventType
from .tcp_connection import TCPConnection, _getAddrType


class SERVER_STATE:
    UNBINDED = 0,
    BINDED = 1


class TCPServer(object):

    def __init__(self, poller, host, port, on_new_connection,
                 send_buffer_size=2 ** 13,
                 recv_buffer_size=2 ** 13,
                 connection_timeout=3.5):
        self.__poller = poller
        self.__host = host
        self.__port = int(port)
        self.__hostAddrType = _getAddrType(host)
        self.__sendBufferSize = send_buffer_size
        self.__recvBufferSize = recv_buffer_size
        self.__socket = None
        self.__fileno = None
        self.__state = SERVER_STATE.UNBINDED
        self.__onNewConnectionCallback = on_new_connection
        self.__connectionTimeout = connection_timeout

    def bind(self):
        self.__socket = socket.socket(self.__hostAddrType, socket.SOCK_STREAM)
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_SNDBUF, self.__sendBufferSize)
        self.__socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, self.__recvBufferSize)
        self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.__socket.setblocking(False)
        self.__socket.bind((self.__host, self.__port))
        self.__socket.listen(5)
        self.__fileno = self.__socket.fileno()
        self.__poller.subscribe(self.__fileno,
                                self.__on_new_connection,
                                PollEventType.READ | PollEventType.ERROR)
        self.__state = SERVER_STATE.BINDED

    def unbind(self):
        self.__state = SERVER_STATE.UNBINDED
        if self.__fileno is not None:
            self.__poller.unsubscribe(self.__fileno)
            self.__fileno = None
        if self.__socket is not None:
            self.__socket.close()

    def __on_new_connection(self, descr, event):
        if event & PollEventType.READ:
            try:
                sock, addr = self.__socket.accept()
                sock.setsockopt(socket.SOL_SOCKET,
                                socket.SO_SNDBUF, self.__sendBufferSize)
                sock.setsockopt(socket.SOL_SOCKET,
                                socket.SO_RCVBUF, self.__recvBufferSize)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setblocking(0)
                conn = TCPConnection(poller=self.__poller,
                                     socket=sock,
                                     timeout=self.__connectionTimeout,
                                     sendBufferSize=self.__sendBufferSize,
                                     recvBufferSize=self.__recvBufferSize)
                self.__onNewConnectionCallback(conn)
            except socket.error as e:
                if e.errno not in (socket.errno.EAGAIN, socket.errno.EWOULDBLOCK):
                    self.unbind()
                    return

        if event & PollEventType.ERROR:
            self.unbind()
            return
