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

namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}

class OaiDownloadProcessor(Observable):
    def __init__(self, path, metadataPrefix, workingDirectory, xWait=True, err=None):
        Observable.__init__(self)
        self._metadataPrefix = metadataPrefix
        self._resumptionToken = None
        self._xWait = xWait
        self._path = path
        self._err = err or stderr

        isdir(workingDirectory) or makedirs(workingDirectory)
        self._stateFilePath = join(workingDirectory, "harvester.state")
        self._readState()

    def buildRequest(self):
        arguments = [('verb', 'ListRecords')]
        if self._resumptionToken:
            arguments.append(('resumptionToken', self._resumptionToken))
        else:
            arguments.append(('metadataPrefix', self._metadataPrefix))
        if self._xWait:
            arguments.append(('x-wait', 'True'))
        statusline = "GET %s?%s HTTP/1.0\r\n\r\n"
        return statusline % (self._path, urlencode(arguments))

    def handle(self, lxmlNode):
        try:
            errors = xpath(lxmlNode, "/oai:OAI-PMH/oai:error")
            if len(errors) > 0:
                for error in errors:
                    self._logError("%s: %s" % (error.get("code"), error.text))
                self._resumptionToken = None
            else:
                records = xpath(lxmlNode, '/oai:OAI-PMH/oai:ListRecords/oai:record')
                for record in records:
                    self.do.add(lxmlNode=ElementTree(record))
                    yield
                self._resumptionToken = head(xpath(lxmlNode, "/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()"))
        finally:
            self._writeState()

    def _writeState(self):
        open(self._stateFilePath, 'w').write("%s%s" % (RESUMPTIONTOKEN_STATE, self._resumptionToken))

    def _readState(self):
        self._resumptionToken = ''
        if isfile(self._stateFilePath):
            state = open(self._stateFilePath).read()
            if RESUMPTIONTOKEN_STATE in state:
                self._resumptionToken = state.split(RESUMPTIONTOKEN_STATE)[-1].strip()

    def _logError(self, message):
        self._err.write(message)
        if not message.endswith('\n'):
            self._err.write('\n')
        self._err.flush()

def head(l):
    return l[0] if l else ""

def xpath(node, path):
    return node.xpath(path, namespaces=namespaces)

RESUMPTIONTOKEN_STATE = "Resumptiontoken: "

