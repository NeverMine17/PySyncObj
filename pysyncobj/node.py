class Node(object):
    """
    A representation of any node in the network.

    The ID must uniquely identify a node. Node objects with the same ID will be treated as equal, i.e. as representing the same node.
    """

    def __init__(self, id_, **kwargs):
        """
        Initialise the Node; id must be immutable, hashable, and unique.

        :param id_: unique, immutable, hashable ID of a node
        :type id_: any
        :param **kwargs: any further information that should be kept about this node
        """

        self._id = id_
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def __setattr__(self, name, value):
        if name == 'id':
            raise AttributeError('Node id is not mutable')
        super(Node, self).__setattr__(name, value)

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id

    def __ne__(self, other):
        # In Python 3, __ne__ defaults to inverting the result of __eq__.
        # Python 2 isn't as sane. So for Python 2 compatibility, we also need to define the != operator explicitly.
        return not (self == other)

    def __hash__(self):
        return hash(self.id)

    def __str__(self):
        return self.id

    def __repr__(self):
        v = vars(self)
        return '{}({}{})'.format(type(self).__name__, repr(self.id),
                                 (', ' + ', '.join('{} = {}'.format(key, repr(v[key])) for key in v if key != '_id'))
                                 if len(v) > 1 else '')

    @property
    def id(self):
        return self._id
