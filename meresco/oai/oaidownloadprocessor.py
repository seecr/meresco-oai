## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2011-2012, 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012, 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from sys import stderr
from os import makedirs, rename
from os.path import join, isfile, isdir
from traceback import format_exc
from time import time
from urllib import urlencode
from uuid import uuid4
from simplejson import dump, loads

from lxml.etree import ElementTree

from meresco.core import Observable
from meresco.xml import xpath, xpathFirst
from meresco.components import lxmltostring, Schedule


namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/"}

class OaiDownloadProcessor(Observable):
    def __init__(self, path, metadataPrefix, workingDirectory, set=None, xWait=True, err=None, verb=None, autoCommit=True, incrementalHarvestSchedule=None, restartAfterFinish=False, name=None):
        Observable.__init__(self, name=name)
        self._path = path
        self._metadataPrefix = metadataPrefix
        isdir(workingDirectory) or makedirs(workingDirectory)
        self._stateFilePath = join(workingDirectory, "harvester.state")
        self._set = set
        self._xWait = xWait
        self._err = err or stderr
        self._verb = verb or 'ListRecords'
        self._autoCommit = autoCommit
        self._incrementalHarvestSchedule = incrementalHarvestSchedule
        if restartAfterFinish and incrementalHarvestSchedule:
            raise ValueError("In case restartAfterFinish==True, incrementalHarvestSchedule must not be set")
        self._restartAfterFinish = restartAfterFinish
        if not restartAfterFinish and incrementalHarvestSchedule is None:
            self._incrementalHarvestSchedule = Schedule(timeOfDay='00:00')

        self._resumptionToken = None
        self._from = None
        self._errorState = None
        self._incrementalHarvestTime = None
        self._readState()
        self._identifierFilePath = join(workingDirectory, "harvester.identifier")
        if isfile(self._identifierFilePath):
            self._identifier = open(self._identifierFilePath).read().strip()
        else:
            self._identifier = str(uuid4())
            open(self._identifierFilePath, 'w').write(self._identifier)

    def setPath(self, path):
        self._path = path

    def setMetadataPrefix(self, metadataPrefix):
        self._metadataPrefix = metadataPrefix

    def buildRequest(self, additionalHeaders=None):
        arguments = [('verb', self._verb)]
        if self._resumptionToken:
            arguments.append(('resumptionToken', self._resumptionToken))
        else:
            if self._from:
                if not self._timeForIncrementalHarvest():
                    return None
                arguments.append(('from', self._from))
            arguments.append(('metadataPrefix', self._metadataPrefix))
            if self._set:
                arguments.append(('set', self._set))
        if self._xWait:
            arguments.append(('x-wait', 'True'))
        request = "GET %s?%s HTTP/1.0\r\n%s\r\n"
        headers = "X-Meresco-Oai-Client-Identifier: %s\r\n" % self._identifier
        if additionalHeaders:
            headers += ''.join("{0}: {1}\r\n".format(k, v) for k, v in additionalHeaders.items())
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
            self._from = xpathFirst(lxmlNode, '/oai:OAI-PMH/oai:responseDate/text()')
            self._resumptionToken = xpathFirst(verbNode, "oai:resumptionToken/text()")
            if self._resumptionToken is None:
                if self._restartAfterFinish:
                    self._from = None
                elif self._incrementalHarvestSchedule:
                    self._incrementalHarvestTime = self._time() + self._incrementalHarvestSchedule.secondsFromNow()
        finally:
            self._maybeCommit()

    def commit(self):
        tmpFilePath = self._stateFilePath + '.tmp'
        with open(tmpFilePath, 'w') as f:
            dump({
                'from': self._from,
                'resumptionToken': self._resumptionToken,
                'errorState': self._errorState,
            },f)
        rename(tmpFilePath, self._stateFilePath)

    def getState(self):
        return HarvestStateView(self)

    def handleShutdown(self):
        print 'handle shutdown: saving OaiDownloadProcessor %s' % self._stateFilePath
        from sys import stdout; stdout.flush()
        self.commit()

    def _add(self, identifier, lxmlNode, datestamp):
        yield self.all.add(identifier=identifier, lxmlNode=lxmlNode, datestamp=datestamp)

    def _maybeCommit(self):
        if self._autoCommit:
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
            self._from = d.get('from')
            self._resumptionToken = d['resumptionToken']
            self._errorState = d['errorState']

    def _logError(self, message):
        self._err.write(message)
        if not message.endswith('\n'):
            self._err.write('\n')
        self._err.flush()

    def _timeForIncrementalHarvest(self):
        if not self._incrementalHarvestTime:
            return False
        return self._time() >= self._incrementalHarvestTime

    def _time(self):
        return time()


class HarvestStateView(object):
    def __init__(self, oaiDownloadProcessor):
        self._processor = oaiDownloadProcessor
        self.name = oaiDownloadProcessor.observable_name()
        self.path = oaiDownloadProcessor._path

    @property
    def errorState(self):
        return self._processor._errorState

    @property
    def resumptionToken(self):
        return self._processor._resumptionToken

RESUMPTIONTOKEN_STATE = "Resumptiontoken: "

VERB_XPATHS = {
    'ListRecords': ('oai:record', 'oai:header'),
    'ListIdentifiers': ('oai:header', '.')
}
