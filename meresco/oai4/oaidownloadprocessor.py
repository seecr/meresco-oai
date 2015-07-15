## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011-2012, 2014-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011, 2014-2015 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from lxml.etree import ElementTree
from traceback import format_exc
from os import makedirs
from os.path import join, isfile, isdir
from urllib import urlencode

from meresco.core import Observable
try:
    from meresco.components import lxmltostring
except ImportError:
    from lxml.etree import tostring
    lxmltostring = lambda x: tostring(x, encoding="UTF-8")
from meresco.oai4 import VERSION

from simplejson import dump, loads
from uuid import uuid4
from sys import stderr


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}

class OaiDownloadProcessor(Observable):
    def __init__(self, path, metadataPrefix, workingDirectory, set=None, xWait=True, err=None, verb=None, autoCommit=True, userAgentAddition=None, name=None):
        Observable.__init__(self, name=name)
        self._userAgent = _USER_AGENT + ('' if userAgentAddition is None else ' (%s)' % userAgentAddition)
        self._metadataPrefix = metadataPrefix
        self._resumptionToken = None
        self._errorState = None
        self._set = set
        self._xWait = xWait
        self._path = path
        self._err = err or stderr
        self._verb = verb or 'ListRecords'
        self._autoCommit = autoCommit
        isdir(workingDirectory) or makedirs(workingDirectory)
        self._stateFilePath = join(workingDirectory, "harvester.state")
        self._readState()
        self._identifierFilePath = join(workingDirectory, "harvester.identifier")
        if isfile(self._identifierFilePath):
            self._identifier = open(self._identifierFilePath).read().strip()
        else:
            self._identifier = str(uuid4())
            open(self._identifierFilePath, 'w').write(self._identifier)

    def buildRequest(self, additionalHeaders=None):
        arguments = [('verb', self._verb)]
        if self._resumptionToken:
            arguments.append(('resumptionToken', self._resumptionToken))
        else:
            arguments.append(('metadataPrefix', self._metadataPrefix))
            if self._set:
                arguments.append(('set', self._set))
        if self._xWait:
            arguments.append(('x-wait', 'True'))
        request = "GET %s?%s HTTP/1.0\r\n%s\r\n"
        headers = "X-Meresco-Oai-Client-Identifier: %s\r\n" % self._identifier
        userAgent = self._userAgent
        if additionalHeaders:
            headers += ''.join("{0}: {1}\r\n".format(k, v) for k, v in additionalHeaders.items())
            userAgent = additionalHeaders.pop('User-Agent', self._userAgent)
        headers += "User-Agent: %s\r\n" % userAgent
        return request % (self._path, urlencode(arguments), headers)

    def handle(self, lxmlNode):
        errors = xpath(lxmlNode, "/oai:OAI-PMH/oai:error")
        if len(errors) > 0:
            for error in errors:
                self._errorState = "%s: %s" % (error.get("code"), error.text)
                self._logError(self._errorState)
            self._resumptionToken = None
            self._maybeCommit()
            return
        try:
            verbNode = xpath(lxmlNode, "/oai:OAI-PMH/oai:%s" % self._verb)[0]
            itemXPath, headerXPath = VERB_XPATHS[self._verb]
            for item in xpath(verbNode, itemXPath):
                header = xpath(item, headerXPath)[0]
                datestamp = xpath(header, 'oai:datestamp/text()')[0]
                identifier = xpath(header, 'oai:identifier/text()')[0]
                try:
                    yield self._add(identifier=identifier, lxmlNode=ElementTree(item), datestamp=datestamp)
                except Exception, e:
                    self._logError(format_exc())
                    self._logError("While processing:")
                    self._logError(lxmltostring(item))
                    self._errorState = "ERROR while processing '%s': %s" % (identifier, str(e))
                    raise
                self._errorState = None
                yield # some room for others
            self._resumptionToken = head(xpath(verbNode, "oai:resumptionToken/text()"))
        finally:
            self._maybeCommit()

    def _add(self, identifier, lxmlNode, datestamp):
        yield self.all.add(identifier=identifier, lxmlNode=lxmlNode, datestamp=datestamp)

    def _maybeCommit(self):
        if self._autoCommit:
            self.commit()

    def commit(self):
        with open(self._stateFilePath, 'w') as f:
            dump({
                'resumptionToken': self._resumptionToken,
                'errorState': self._errorState,
            },f)

    def handleShutdown(self):
        print 'handle shutdown: saving OaiDownloadProcessor %s' % self._stateFilePath
        from sys import stdout; stdout.flush()
        self.commit()

    def _readState(self):
        self._resumptionToken = ''
        self._errorState = None
        if isfile(self._stateFilePath):
            state = open(self._stateFilePath).read()
            if not state.startswith('{'):
                if RESUMPTIONTOKEN_STATE in state:
                    self._resumptionToken = state.split(RESUMPTIONTOKEN_STATE)[-1].strip()
                self._maybeCommit()
                return
            d = loads(state)
            self._resumptionToken = d['resumptionToken']
            self._errorState = d['errorState']

    def _logError(self, message):
        self._err.write(message)
        if not message.endswith('\n'):
            self._err.write('\n')
        self._err.flush()

    def getState(self):
        return HarvestStateView(self)

class HarvestStateView(object):
    def __init__(self, oaiDownloadProcessor):
        self._processor = oaiDownloadProcessor

    @property
    def errorState(self):
        return self._processor._errorState

    @property
    def resumptionToken(self):
        return self._processor._resumptionToken

    @property
    def from_(self):
        return None

    @property
    def name(self):
        return self._processor.observable_name()

    @property
    def path(self):
        return self._processor._path

    @property
    def metadataPrefix(self):
        return self._processor._metadataPrefix

    @property
    def set(self):
        return self._processor._set

def head(l):
    return l[0] if l else ""

def xpath(node, path):
    return node.xpath(path, namespaces=namespaces)

RESUMPTIONTOKEN_STATE = "Resumptiontoken: "

VERB_XPATHS = {
    'ListRecords': ('oai:record', 'oai:header'),
    'ListIdentifiers': ('oai:header', '.')
}
_USER_AGENT = "Meresco-Oai-DownloadProcessor/%s" % VERSION
