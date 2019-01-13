from pysyncobj.dns_resolver import globalDnsResolver
from pysyncobj.node import Node


class TCPNode(Node):
    """
    A node intended for communication over TCP/IP. Its id is the network address (host:port).
    """

    def __init__(self, address, **kwargs):
        """
        Initialise the TCPNode

        :param address: network address of the node in the format 'host:port'
        :type address: str
        :param **kwargs: any further information that should be kept about this node
        """

        super(TCPNode, self).__init__(address, **kwargs)
        self.address = address
        self.host, port = address.rsplit(':', 1)
        self.port = int(port)
        self.ip = globalDnsResolver().resolve(self.host)

    def __repr__(self):
        v = vars(self)
        filtered = ['_id', 'address', 'host', 'port', 'ip']
        formatted = ['{} = {}'.format(key, repr(v[key])) for key in v if key not in filtered]
        return '{}({}{})'.format(type(self).__name__, repr(self.id), (', ' + ', '.join(formatted)) if len(formatted) else '')