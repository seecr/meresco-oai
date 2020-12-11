## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
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

from weightless.io import Suspend
from random import choice
import sys

class SuspendRegister(object):
    def __init__(self, maximumSuspendedConnections=None, batchMode=False):
        self._register = {}
        self._maximumSuspendedConnections = maximumSuspendedConnections or 100
        self._batchMode = batchMode
        self._lastStamp = None
        self._immediateState = _ImmediateState(self)
        self._postponedState = _PostponedState(self)
        self._state = self._immediateState

    def suspendAfterNoResult(self, **kwargs):
        yield self._suspend(**kwargs)

    def suspendBeforeSelect(self, continueAfter, **kwargs):
        if not self._state.shouldSuspendBeforeSelect(continueAfter=continueAfter):
            return
        yield self._suspend(continueAfter=continueAfter, **kwargs)

    def signalOaiUpdate(self, stamp, **kwargs):
        self._lastStamp = stamp
        self._state.signalOaiUpdate(stamp=stamp, **kwargs)

    def startOaiBatch(self):
        if self._batchMode:
            self._state.switchToPostponed()

    def stopOaiBatch(self):
        if self._batchMode:
            self._state.switchToImmediate()

    def _suspend(self, clientIdentifier, prefix, sets, **ignored):
        suspend = Suspend()
        suspend.oaiListResumeMask = dict(
                prefix=prefix,
                sets=set() if sets is None else set(sets)
            )
        if clientIdentifier in self._register:
            self._register.pop(clientIdentifier).throw(exc_type=ForcedResumeException, exc_value=ForcedResumeException("Aborting suspended request because of new request for the same OaiClient with identifier: %s." % clientIdentifier), exc_traceback=None)
        elif len(self._register) == self._maximumSuspendedConnections:
            self._register.pop(choice(self._register.keys())).throw(exc_type=ForcedResumeException, exc_value=ForcedResumeException("OAI x-wait connection has been forcefully resumed."), exc_traceback=None)
            sys.stderr.write("Too many suspended connections in SuspendRegister. One random connection has been resumed.\n")
        self._register[clientIdentifier] = suspend
        yield suspend
        suspend.getResult()

    def _handleOaiUpdateSignal(self, prefixAndSets):
        for clientId, suspend in list(self._register.items()): #[:]:
            sets = prefixAndSets.get(suspend.oaiListResumeMask['prefix'])
            if sets is None:
                continue
            setMasks = suspend.oaiListResumeMask['sets']
            if len(setMasks) > 0 and not setMasks.intersection(sets):
                continue
            del self._register[clientId]
            suspend.resume()

    # test helpers

    def __len__(self):
        """For testing"""
        return len(self._register)

    def __contains__(self, clientId):
        """For testing"""
        return clientId in self._register

    def _suspendObject(self, clientId):
        """For testing"""
        return self._register.get(clientId)

    def _setLastStamp(self, stamp):
        """For testing"""
        self._lastStamp = stamp


class _ImmediateState(object):
    def __init__(self, register):
        self._register = register

    def start(self):
        return self

    def signalOaiUpdate(self, metadataPrefixes, sets, **kwargs):
        self._register._handleOaiUpdateSignal(prefixAndSets=dict((k, sets) for k in metadataPrefixes))

    def switchToPostponed(self):
        self._register._state = self._register._postponedState.start()

    def shouldSuspendBeforeSelect(self, **kwargs):
        return False

class _PostponedState(object):
    def __init__(self, register):
        self._postponed = {}
        self._register = register
        self._batches = 0

    def start(self):
        self._lastStampBeforeBatch = self._register._lastStamp
        self._batches += 1
        return self

    def signalOaiUpdate(self, metadataPrefixes, sets, **ignored):
        for prefix in metadataPrefixes:
            prefixSets = self._postponed.setdefault(prefix, set())
            prefixSets.update(sets)

    def switchToPostponed(self):
        self._batches += 1

    def switchToImmediate(self):
        self._handlePostponed()
        self._batches -= 1
        if self._batches == 0:
            self._register._state = self._register._immediateState.start()

    def shouldSuspendBeforeSelect(self, continueAfter, **kwargs):
        if self._lastStampBeforeBatch is None:
            return False
        return int(continueAfter) >= self._lastStampBeforeBatch

    def _handlePostponed(self):
        self._register._handleOaiUpdateSignal(prefixAndSets=self._postponed)
        self._postponed = {}

class ForcedResumeException(Exception):
    pass

