## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
#
#    This file is part of Meresco Oai.
#
#    Meresco Oai is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    Meresco Oai is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Meresco Oai; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from socket import socket, error as SocketError, SHUT_WR, SHUT_RD, SOL_SOCKET, SO_ERROR
from errno import EINPROGRESS, ECONNREFUSED
from lxml.etree import parse, ElementTree
from StringIO import StringIO
from traceback import format_exc
from os import makedirs, close, remove
from os.path import join, isfile, isdir
from urllib import urlencode

from meresco.core import Observable
from meresco.components.http.utils import CRLF
from weightless import compose

from sys import stderr, stdout
from time import time
from tempfile import mkstemp

class AlwaysReadable(object):
    def __init__(self):
        self._fd, self._name = mkstemp('periodicdownload')

    def fileno(self):
        return self._fd

    def cleanUp(self):
        close(self._fd)
        remove(self._name)


class PeriodicDownload(Observable):
    def __init__(self, reactor, host, port, period=1, verbose=False, prio=None, err=None):
        super(PeriodicDownload, self).__init__()
        self._reactor = reactor
        self._host = host
        self._port = port 
        self._period = period
        self._prio = prio
        self._err = err or stderr
        if not verbose:
            self._log = lambda x: None

    def observer_init(self):
        self._loop = compose(self.loop())
        self._reactor.addTimer(1, self._loop.next)

    def loop(self):
        while True:
            sok = yield self._tryConnect()
            sok.send(self.any.buildRequest())
            sok.shutdown(SHUT_WR)
            self._reactor.addReader(sok, self._loop.next, prio=self._prio)
            responses = []
            try:
                while True:
                    yield
                    response = sok.recv(4096)
                    if response == '':
                         break
                    responses.append(response)
            except SocketError, (errno, msg):
                yield self._retryAfterError("Receive error: %s: %s" % (errno, msg))
                continue
            self._reactor.removeReader(sok)
            sok.close()
            try:
                response = ''.join(responses)
                headers, body = response.split(2 * CRLF, 1)
                statusLine = headers.split(CRLF)[0]
                if not statusLine.strip().lower().endswith('200 ok'):
                    yield self._retryAfterError('Unexpected response: ' + statusLine)
                    continue
                lxmlNode = parse(StringIO(body))
                alwaysReadable = AlwaysReadable()
                self._reactor.addReader(alwaysReadable, self._loop.next, prio=self._prio)
                try:
                    result = self.any.handle(lxmlNode=lxmlNode)
                    yield result 
                finally:
                    self._reactor.removeReader(alwaysReadable)
                    alwaysReadable.cleanUp()
            except Exception:
                self._logError(format_exc())
            self._reactor.addTimer(self._period, self._loop.next)
            yield
    
    def _tryConnect(self):
        sok = socket()
        sok.setblocking(0)
        while True:
            try:
                sok.connect((self._host, self._port))
            except SocketError, (errno, msg):
                if errno != EINPROGRESS:
                    yield self._retryAfterError("%s: %s" % (errno, msg))
                    continue
            self._reactor.addWriter(sok, self._loop.next)
            yield
            self._reactor.removeWriter(sok)

            err = sok.getsockopt(SOL_SOCKET, SO_ERROR)
            if err == ECONNREFUSED:
                yield self._retryAfterError("Connection refused.")
                continue
            if err != 0:   # any other error
                raise IOError(err)
            break
        raise StopIteration(sok)

    def _retryAfterError(self, message):
        self._logError(message)
        self._reactor.addTimer(self._period, self._loop.next)
        yield
        
    def _logError(self, message):
        self._err.write("%s:%d: " % (self._host, self._port))
        self._err.write(message)
        if not message.endswith('\n'):
            self._err.write('\n')
        self._err.flush()

    def _log(self, message):
        stdout.write(message)
        if not message.endswith('\n'):
            stdout.write('\n')
        stdout.flush()

