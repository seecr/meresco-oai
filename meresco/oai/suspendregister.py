## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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
    def __init__(self, maximumSuspendedConnections=None):
        self._register = {}
        self._maximumSuspendedConnections = maximumSuspendedConnections or 100

    def suspendAfterNoResult(self, clientIdentifier, prefix, sets, **ignored):
        suspend = Suspend()
        suspend.oaiListResumeMask = dict(prefix=prefix, set_=next(iter(sets), None))
        if clientIdentifier in self._register:
            self._register.pop(clientIdentifier).throw(exc_type=ValueError, exc_value=ValueError("Aborting suspended request because of new request for the same OaiClient with identifier: %s." % clientIdentifier), exc_traceback=None)
        if len(self._register) == self._maximumSuspendedConnections:
            self._register.pop(choice(self._register.keys())).throw(exc_type=ForcedResumeException, exc_value=ForcedResumeException(), exc_traceback=None)
            sys.stderr.write("Too many suspended connections in SuspendRegister. One random connection has been resumed.\n")
        self._register[clientIdentifier] = suspend
        yield suspend
        suspend.getResult()

    def suspendBeforeSelect(self, **ignored):
        return
        yield

    def signalOaiUpdate(self, **kwargs):
        self._signalOaiUpdate(**kwargs)

    def _signalOaiUpdate(self, metadataPrefixes, sets, **ignored):
        for clientId, suspend in self._register.items()[:]:
            if suspend.oaiListResumeMask['prefix'] in metadataPrefixes:
                setMask = suspend.oaiListResumeMask['set_']
                if setMask and not setMask in sets:
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

class BatchSuspendRegister(SuspendRegister):
    def __init__(self, **kwargs):
        SuspendRegister.__init__(self, **kwargs)
        self._lastStamp = 0
        self._immediateState = ImmediateState(self)
        self._postponedState = PostponedState(self)
        self._state = self._immediateState

    def signalOaiUpdate(self, stamp, **kwargs):
        self._lastStamp = stamp
        self._state.signalOaiUpdate(stamp=stamp, **kwargs)

    def startOaiBatch(self):
        self._state.switchToPostponed()

    def stopOaiBatch(self):
        self._state.switchToImmediate()

class ImmediateState(object):
    def __init__(self, register):
        self._register = register

    def signalOaiUpdate(self, **kwargs):
        self._register._signalOaiUpdate(**kwargs)

    def switchToPostponed(self):
        self._register._state = self._register._postponedState

class PostponedState(object):
    def __init__(self, register):
        self._postponed = []
        self._register = register

    def signalOaiUpdate(self, metadataPrefixes, sets, **ignored):
        self._postponed.append(dict(metadataPrefixes=metadataPrefixes, sets=sets))

    def switchToImmediate(self):
        for prefixesAndSets in self._postponed:
            self._register._signalOaiUpdate(**prefixesAndSets)
        self._register._state = self._register._immediateState

class ForcedResumeException(Exception):
    pass

