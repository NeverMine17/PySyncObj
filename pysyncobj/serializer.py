import gzip
import logging
import os
from io import BytesIO

import pysyncobj.pickle as pickle
from .atomic_replace import atomic_replace
from .config import SerializerState


class Serializer(object):
    def __init__(self, file_name, transmission_batch_size, try_use_fork,
                 serializer, deserializer, serialize_checker):
        self.__useFork = try_use_fork and hasattr(
            os, 'fork') and serializer is None
        self.__fileName = file_name
        self.__transmissionBatchSize = transmission_batch_size
        self.__pid = 0
        self.__currentID = 0
        self.__transmissions = {}
        self.__incomingTransmissionFile = None
        self.__inMemorySerializedData = None
        self.__serializer = serializer
        self.__deserializer = deserializer
        self.__serializeChecker = serialize_checker

    def check_serializing(self):
        if self.__serializeChecker is not None:
            status = self.__serializeChecker()
            if status in (SerializerState.SUCCESS, SerializerState.FAILED):
                self.__pid = 0
            return status, self.__currentID

        # In-memory case
        if self.__fileName is None or not self.__useFork:
            if self.__pid in (-1, -2):
                serialize_state = SerializerState.SUCCESS if self.__pid == - \
                    1 else SerializerState.FAILED
                self.__pid = 0
                self.__transmissions = {}
                return serialize_state, self.__currentID
            return SerializerState.NOT_SERIALIZING, None

        # File case
        pid = self.__pid
        if pid == 0:
            return SerializerState.NOT_SERIALIZING, None
        try:
            rpid, status = os.waitpid(pid, os.WNOHANG)
        except OSError:
            self.__pid = 0
            return SerializerState.FAILED, self.__currentID
        if rpid == pid:
            if status == 0:
                self.__transmissions = {}
                self.__pid = 0
                return SerializerState.SUCCESS, self.__currentID
            self.__pid = 0
            return SerializerState.FAILED, self.__currentID
        return SerializerState.SERIALIZING, self.__currentID

    def serialize(self, data, id_):
        if self.__pid != 0:
            return

        self.__currentID = id_

        # In-memory case
        if self.__fileName is None:
            with BytesIO() as io:
                with gzip.GzipFile(fileobj=io, mode='wb') as g:
                    pickle.dump(data, g)
                self.__inMemorySerializedData = io.getvalue()
            self.__pid = -1
            return

        # File case
        if self.__useFork:
            pid = os.fork()
            if pid != 0:
                self.__pid = pid
                return

        try:
            tmp_file = self.__fileName + '.tmp'
            if self.__serializer is not None:
                self.__serializer(tmp_file, data[1:])
            else:
                with open(tmp_file, 'wb') as f:
                    with gzip.GzipFile(fileobj=f) as g:
                        pickle.dump(data, g)

            atomic_replace(tmp_file, self.__fileName)
            if self.__useFork:
                os._exit(0)
            else:
                self.__pid = -1
        except Exception as e:
            if self.__useFork:
                os._exit(-1)
            else:
                self.__pid = -2

    def deserialize(self):
        if self.__fileName is None:
            with BytesIO(self.__inMemorySerializedData) as io:
                with gzip.GzipFile(fileobj=io) as g:
                    return pickle.load(g)

        if self.__deserializer is not None:
            return (None,) + self.__deserializer(self.__fileName)
        else:
            with open(self.__fileName, 'rb') as f:
                with gzip.GzipFile(fileobj=f) as g:
                    return pickle.load(g)

    def get_transmission_data(self, transmission_id):
        if self.__pid != 0:
            return None
        transmission = self.__transmissions.get(transmission_id, None)
        if transmission is None:
            try:
                if self.__fileName is None:
                    data = self.__inMemorySerializedData
                    assert data is not None
                    self.__transmissions[transmission_id] = transmission = {
                        'transmitted': 0,
                        'data': data,
                    }
                else:
                    self.__transmissions[transmission_id] = transmission = {
                        'file': open(self.__fileName, 'rb'),
                        'transmitted': 0,
                    }
            except:
                logging.exception('Failed to open file for transmission')
                self.__transmissions.pop(transmission_id, None)
                return None
        is_first = transmission['transmitted'] == 0
        try:
            if self.__fileName is None:
                transmitted = transmission['transmitted']
                data = transmission['data'][transmitted:transmitted +
                                            self.__transmissionBatchSize]
            else:
                data = transmission['file'].read(self.__transmissionBatchSize)
        except:
            logging.exception('Error reading transmission file')
            self.__transmissions.pop(transmission_id, None)
            return False
        size = len(data)
        transmission['transmitted'] += size
        isLast = size == 0
        if isLast:
            self.__transmissions.pop(transmission_id, None)
        return data, is_first, isLast

    def set_transmission_data(self, data):
        if data is None:
            return False
        data, is_first, is_last = data

        # In-memory case
        if self.__fileName is None:
            if is_first:
                self.__incomingTransmissionFile = bytes()
            elif self.__incomingTransmissionFile is None:
                return False
            self.__incomingTransmissionFile += pickle.to_bytes(data)
            if is_last:
                self.__inMemorySerializedData = self.__incomingTransmissionFile
                self.__incomingTransmissionFile = None
                return True
            return False

        # File case
        tmpFile = self.__fileName + '.1.tmp'
        if is_first:
            if self.__incomingTransmissionFile is not None:
                self.__incomingTransmissionFile.close()
            try:
                self.__incomingTransmissionFile = open(tmpFile, 'wb')
            except:
                logging.exception(
                    'Failed to open file for incoming transition')
                self.__incomingTransmissionFile = None
                return False
        elif self.__incomingTransmissionFile is None:
            return False
        try:
            self.__incomingTransmissionFile.write(data)
        except:
            logging.exception('Failed to write incoming transition data')
            self.__incomingTransmissionFile = None
            return False
        if is_last:
            self.__incomingTransmissionFile.close()
            self.__incomingTransmissionFile = None
            try:
                atomic_replace(tmpFile, self.__fileName)
            except:
                logging.exception(
                    'Failed to rename temporary incoming transition file')
                return False
            return True
        return False

    def cancel_transmisstion(self, id_):
        self.__transmissions.pop(id_, None)
