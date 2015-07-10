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

from seecr.test import SeecrTestCase

class SuspendRegisterTest(SeecrTestCase):

    def TODOtestAddOaiRecordResumes(self):
        reactor = CallTrace("reactor")
        suspend = self.jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))
        self.assertEquals([], resumed)
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])
        self.assertEquals([True], resumed)
        self.assertEquals({}, self.jazz._suspended)

    def TODOtestAddSuspendedListRecord(self):
        suspend = self.jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        self.assertTrue({'a-client-id': suspend}, self.jazz._suspended)
        self.assertEquals(Suspend, type(suspend))

    def testSuspendSameClientTwiceBeforeResuming(self):
        reactor = CallTrace("reactor")
        resumed = []

        suspendGen1 = self.jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix')
        suspend1 = suspendGen1.next()
        suspend1(reactor, lambda: resumed.append(True))
        suspend2 = self.jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix').next()

        try:
            suspendGen1.next()
            self.fail()
        except ValueError, e:
            self.assertTrue([True], resumed)
            self.assertEquals("Aborting suspended request because of new request for the same OaiClient with identifier: a-client-id.", str(e))

    def testShouldResumeAPreviousSuspendAfterTooManySuspends(self):
        reactor = CallTrace("reactor")
        resumed = []
        jazz = OaiJazz(self.tmpdir2("b"), maximumSuspendedConnections=1)
        suspendGen1 = jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix')
        suspend1 = suspendGen1.next()
        suspend1(reactor, lambda: resumed.append(True))
        with stderr_replaced() as s:
            suspend2 = jazz.suspend(clientIdentifier="another-client-id", metadataPrefix='prefix').next()

        self.assertRaises(ForcedResumeException, lambda: suspendGen1.next())
        self.assertTrue([True], resumed)
        self.assertEquals(1, len(jazz._suspended))
        self.assertEquals("Too many suspended connections in OaiJazz. One random connection has been resumed.\n", s.getvalue())

    def testDeleteResumes(self):
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix', 'schema', 'namespace')])
        reactor = CallTrace("reactor")
        suspend = self.jazz.suspend(clientIdentifier="a-client-id", metadataPrefix='prefix').next()
        resumed = []
        suspend(reactor, lambda: resumed.append(True))
        self.assertEquals([], resumed)
        list(compose(self.jazz.delete(identifier='identifier')))
        self.assertEquals([True], resumed)
        self.assertEquals({}, self.jazz._suspended)


    def testResumeOnlyMatchingSuspends(self):
        reactor = CallTrace("reactor")
        resumed = []

        def suspend(clientIdentifier, metadataPrefix, set=None):
            if not clientIdentifier in self.jazz._suspended:
                suspendObject = self.jazz.suspend(clientIdentifier=clientIdentifier, metadataPrefix=metadataPrefix, set=set).next()
                suspendObject(reactor, lambda: resumed.append(clientIdentifier))

        def prepareSuspends():
            resumed[:] = []
            suspend(clientIdentifier="client 1", metadataPrefix='prefix1')
            suspend(clientIdentifier="client 2", metadataPrefix='prefix2')
            suspend(clientIdentifier="client 3", metadataPrefix='prefix2', set='set_a')

        prepareSuspends()
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix2', 'schema', 'namespace')], sets=[('set_b', 'set B')])
        self.assertEquals(['client 2'], resumed)

        prepareSuspends()
        list(compose(self.jazz.delete(identifier='identifier')))
        self.assertEquals(['client 2'], resumed)

        prepareSuspends()
        self.jazz.addOaiRecord(identifier="identifier", metadataFormats=[('prefix2', 'schema', 'namespace')], sets=[('set_a', 'set A')])
        self.assertEquals(['client 2', 'client 3'], sorted(resumed))

        prepareSuspends()
        list(compose(self.jazz.delete(identifier='identifier')))
        self.assertEquals(['client 2', 'client 3'], sorted(resumed))
