# Copyright 2018 The Wallaroo Authors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.

import socket
import struct
import time


class SourceDriver(object):

    def __init__(self):
        self._conn = None

    def connect(self, host, port):
        conn = socket.socket()
        print "Attempting to connect to {}:{}".format(host, port)
        while True:
            try:
                conn.connect((host, port))
                self._conn = conn
                return
            except socket.error as err:
                if err.errno == socket.errno.ECONNREFUSED:
                    time.sleep(1)
                else:
                    raise

    def write(self, message):
        if self._conn == None:
            raise RuntimeError("Please call connect before writing")
        bytes = len(message)
        payload = struct.pack('>I', bytes) + message
        self._conn.sendall(payload)


class SinkDriver(object):

    def __init__(self, host, port, backlog = 0):
        acceptor = socket.socket()
        acceptor.bind((host, port))
        acceptor.listen(backlog)
        self._acceptor = acceptor
    
    def accept(self):
        conn, addr = self._acceptor.accept()
        SinkDriverConnection(self, conn, addr)


class SinkDriverConnection(object):

    def __init__(self, driver, conn, from_addr):
        self.driver = driver
        self._conn = conn
        self._from_addr = from_addr

    def read(self):
        # These waits should be buffered but there is tuning involved here
        # and it might be better to move this to python 3's asyncio anyway
        header = self._conn.recv(4, socket.MSG_WAITALL)
        if not header: return None
        expected = struct.unpack('>I', header)[0]
        return self._conn.recv(expected, socket.MSG_WAITALL)


class Session(object):
    # TODO: port over some kind of state management to this revision using
    # sqlite or some kind of simple pickle file until a plan is made for
    # managing this within wallaroo itself. For now we'll keep this empty
    # since it's not required for our current demonstration needs.
    pass