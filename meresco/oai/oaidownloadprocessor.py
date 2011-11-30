## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# 
# This file is part of "Meresco Oai"
# 
# "Meresco Oai" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
# 
# "Meresco Oai" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with "Meresco Oai"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
from weightless.core import compose

from sys import stderr, stdout
from time import time
from tempfile import mkstemp

namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}

class OaiDownloadProcessor(Observable):
    def __init__(self, path, metadataPrefix, workingDirectory, set=None, xWait=True, err=None, verb=None):
        Observable.__init__(self)
        self._metadataPrefix = metadataPrefix
        self._resumptionToken = None
        self._set = set
        self._xWait = xWait
        self._path = path
        self._err = err or stderr
        self._verb = verb or 'ListRecords'
        isdir(workingDirectory) or makedirs(workingDirectory)
        self._stateFilePath = join(workingDirectory, "harvester.state")
        self._readState()

    def buildRequest(self):
        arguments = [('verb', self._verb)]
        if self._resumptionToken:
            arguments.append(('resumptionToken', self._resumptionToken))
        else:
            arguments.append(('metadataPrefix', self._metadataPrefix))
            if self._set:
                arguments.append(('set', self._set))
        if self._xWait:
            arguments.append(('x-wait', 'True'))
        statusline = "GET %s?%s HTTP/1.0\r\n\r\n"
        return statusline % (self._path, urlencode(arguments))

    def handle(self, lxmlNode):
        errors = xpath(lxmlNode, "/oai:OAI-PMH/oai:error")
        if len(errors) > 0:
            for error in errors:
                self._logError("%s: %s" % (error.get("code"), error.text))
            self._resumptionToken = None
            self._writeState()                
            return
        try:
            verbNode = xpath(lxmlNode, "/oai:OAI-PMH/oai:%s" % self._verb)[0]
            itemXPath, headerXPath = VERB_XPATHS[self._verb]
            for item in xpath(verbNode, itemXPath):
                header = xpath(item, headerXPath)[0]
                datestamp = xpath(header, 'oai:datestamp/text()')[0]
                identifier = xpath(header, 'oai:identifier/text()')[0]
                yield self.asyncdo.add(identifier=identifier, lxmlNode=ElementTree(item), datestamp=datestamp)
                yield # some room for others
            self._resumptionToken = head(xpath(verbNode, "oai:resumptionToken/text()"))
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

VERB_XPATHS = {
    'ListRecords': ('oai:record', 'oai:header'),
    'ListIdentifiers': ('oai:header', '.')
}
