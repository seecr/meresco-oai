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
from lxml.etree import parse
from StringIO import StringIO
from traceback import format_exc
from os import makedirs, close, remove
from os.path import join, isfile, isdir

from meresco.core import Observable
from weightless import compose

from sys import stderr, stdout
from time import time
from tempfile import mkstemp

class AlwaysReadable(object):
    def __init__(self):
        self._fd, self._name = mkstemp()

    def fileno(self):
        return self._fd

    def cleanUp(self):
        close(self._fd)
        remove(self._name)


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}

class OaiHarvester(Observable):
    def __init__(self, reactor, host, port, path, metadataPrefix, workingDir, xWait=True, verbose=False):
        super(OaiHarvester, self).__init__()
        self._reactor = reactor
        self._host = host
        self._port = port 
        self._path = path
        self._prefix = metadataPrefix
        isdir(workingDir) or makedirs(workingDir)
        self._xWait = xWait
        self._stateFilePath = join(workingDir, "harvester.state")
        if not verbose:
            self._log = lambda x: None
            self._logError = lambda x: None

    def observer_init(self):
        resumptionToken = self._readState()
        self._loop = compose(self.loop(resumptionToken))
        self._reactor.addTimer(1, self._loop.next)

    def loop(self, resumptionToken=None):
        while True:
            sok = yield self._tryConnect()
            sok.send(self._buildRequest(resumptionToken))
            sok.shutdown(SHUT_WR)
            self._reactor.addReader(sok, self._loop.next)
            responses = []
            while True:
                yield
                response = sok.recv(4096)
                if response == '':
                     break
                responses.append(response)
            self._reactor.removeReader(sok)
            sok.close()
            alwaysReadable = AlwaysReadable()
            try:
                response = ''.join(responses)
                headers, body = response.split("\r\n\r\n")
                lxmlNode = parse(StringIO(body))
                errors = lxmlNode.xpath("/oai:OAI-PMH/oai:error", namespaces=namespaces)
                if len(errors) > 0:
                    for error in errors:
                        self._logError("%s: %s" % (error.get("code"), error.text))
                    resumptionToken = None
                else:
                    self._reactor.addReader(alwaysReadable, self._loop.next)
                    try:
                        self.do.add(lxmlNode=lxmlNode)
                        yield
                    finally:
                        self._reactor.removeReader(alwaysReadable)
                    resumptionToken = head(lxmlNode.xpath("/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()", 
                                               namespaces=namespaces))
            except Exception:
                self._logError(format_exc())
            finally:
                open(self._stateFilePath, 'w').write("Resumptiontoken: %s" % resumptionToken)
                alwaysReadable.cleanUp()
            self._reactor.addTimer(1, self._loop.next)
            yield

    def _readState(self):
        state = []
        if isfile(self._stateFilePath):
            state = open(self._stateFilePath).read().split("Resumptiontoken: ")
        return state[1] if len(state)  == 2 else "" 

    def _buildRequest(self, resumptionToken):
        request = LISTRECORDS % self._path
        if resumptionToken:
            request += "&resumptionToken=%s" % resumptionToken
        else:
            request += "&metadataPrefix=%s" % self._prefix
        if self._xWait:
            request += "&x-wait=True"
        return STATUSLINE % request

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
                yield self._retryAfterError("Connection to %s:%s%s refused." % (self._host, self._port, self._path))
                continue
            if err != 0:   # any other error
                raise IOError(err)
            break
        raise StopIteration(sok)

    def _retryAfterError(self, message):
        self._logError(message)
        self._reactor.addTimer(1, self._loop.next)
        yield
        

    def _logError(self, message):
        stderr.write(message)
        stderr.flush()

    def _log(self, message):
        stdout.write(message)
        stdout.flush()


def head(l):
    return l[0] if l else ""


STATUSLINE = "GET %s HTTP/1.0\r\n\r\n"
LISTRECORDS = "%s?verb=ListRecords"
