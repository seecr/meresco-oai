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

from seecr.test import SeecrTestCase, CallTrace
from seecr.test.io import stderr_replaced
from meresco.oai.suspendregister import SuspendRegister, ForcedResumeException
from weightless.io import Suspend

class SuspendRegisterTest(SeecrTestCase):

    def testResume(self):
        register = SuspendRegister()
        reactor = CallTrace("reactor")
        suspend = register.suspendAfterNoResult(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        self.assertEquals(Suspend, type(suspend))
        resumed = []
        suspend(reactor, lambda: resumed.append(True))
        self.assertEquals([], resumed)
        register.resume(metadataPrefixes=['prefix'], sets=set())
        self.assertEquals([True], resumed)
        self.assertEquals(0, len(register))

    def testSuspendSameClientTwiceBeforeResuming(self):
        register = SuspendRegister()
        s1 = register.suspendAfterNoResult(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        s1(CallTrace('reactor'), lambda: None)
        register.suspendAfterNoResult(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        try:
            s1.getResult()
            self.fail()
        except ValueError, e:
            self.assertEquals("Aborting suspended request because of new request for the same OaiClient with identifier: a-client-id.", str(e))

    def testShouldResumeAPreviousSuspendAfterTooManySuspends(self):
        with stderr_replaced() as s:
            register = SuspendRegister(maximumSuspendedConnections=1)
            s1 = register.suspendAfterNoResult(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
            s1(CallTrace('reactor'), lambda: None)
            register.suspendAfterNoResult(clientIdentifier="another-client-id", metadataPrefix='prefix').next()
            try:
                s1.getResult()
                self.fail()
            except ForcedResumeException:
                self.assertEquals("Too many suspended connections in SuspendRegister. One random connection has been resumed.\n", s.getvalue())

    def testResumeOnlyMatchingSuspends(self):
        register = SuspendRegister()
        resumed = []

        def suspendAfterNoResult(clientIdentifier, metadataPrefix, set=None):
            if not clientIdentifier in register:
                suspendObject = register.suspendAfterNoResult(clientIdentifier=clientIdentifier, metadataPrefix=metadataPrefix, set=set).next()
                suspendObject(CallTrace('reactor'), lambda: resumed.append(clientIdentifier))

        def prepareSuspends():
            resumed[:] = []
            suspendAfterNoResult(clientIdentifier="client 1", metadataPrefix='prefix1')
            suspendAfterNoResult(clientIdentifier="client 2", metadataPrefix='prefix2')
            suspendAfterNoResult(clientIdentifier="client 3", metadataPrefix='prefix2', set='set_a')

        prepareSuspends()
        register.resume(metadataPrefixes=['prefix2'], sets=['set_b'])
        self.assertEquals(['client 2'], resumed)

        prepareSuspends()
        register.resume(metadataPrefixes=['prefix2'], sets=['set_a'])
        self.assertEquals(['client 2', 'client 3'], sorted(resumed))
