## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015, 2017, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from weightless.core import compose, asList
from weightless.io import Suspend

from meresco.oai.suspendregister import SuspendRegister, ForcedResumeException, _PostponedState


class SuspendRegisterTest(SeecrTestCase):
    def testSignalOaiUpdate(self):
        def test(register):
            reactor = CallTrace("reactor")
            suspend = next(compose(register.suspendAfterNoResult(clientIdentifier="a-client-id", prefix='prefix', sets=[])))
            self.assertEqual(Suspend, type(suspend))
            resumed = []
            suspend(reactor, lambda: resumed.append(True))
            self.assertEqual([], resumed)
            register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=1000)
            self.assertEqual([True], resumed)
            self.assertEqual(0, len(register))
        test(SuspendRegister())
        test(SuspendRegister(batchMode=True)) # immediate resume state

    def testSuspendSameClientTwiceBeforeResuming(self):
        def test(suspendMethod):
            s1 = next(compose(suspendMethod(clientIdentifier="a-client-id", prefix='prefix', sets=[], continueAfter='9876')))
            s1(CallTrace('reactor'), lambda: None)
            next(compose(suspendMethod(clientIdentifier="a-client-id", prefix='prefix', sets=[], continueAfter='9876')))
            try:
                s1.getResult()
                self.fail()
            except ForcedResumeException as e:
                self.assertEqual("Aborting suspended request because of new request for the same OaiClient with identifier: a-client-id.", str(e))
        test(SuspendRegister().suspendAfterNoResult)
        test(SuspendRegister(batchMode=True).suspendAfterNoResult)
        batchRegister = SuspendRegister(batchMode=True)
        batchRegister.startOaiBatch()
        test(batchRegister.suspendAfterNoResult)
        batchRegister = SuspendRegister(batchMode=True)
        batchRegister._setLastStamp(1000)
        batchRegister.startOaiBatch()
        test(batchRegister.suspendBeforeSelect) #only suspend in batch (not outside)

    def testShouldResumeAPreviousSuspendAfterTooManySuspends(self):
        def test(suspendMethod):
            with stderr_replaced() as s:
                s1 = next(compose(suspendMethod(clientIdentifier="a-client-id", prefix='prefix', sets=[], continueAfter='9876')))
                s1(CallTrace('reactor'), lambda: None)
                next(compose(suspendMethod(clientIdentifier="another-client-id", prefix='prefix', sets=[], continueAfter='9876')))
                try:
                    s1.getResult()
                    self.fail()
                except ForcedResumeException:
                    self.assertEqual("Too many suspended connections in SuspendRegister. One random connection has been resumed.\n", s.getvalue())

        test(SuspendRegister(maximumSuspendedConnections=1).suspendAfterNoResult)
        test(SuspendRegister(batchMode=True, maximumSuspendedConnections=1).suspendAfterNoResult)
        batchRegister = SuspendRegister(batchMode=True, maximumSuspendedConnections=1)
        batchRegister.startOaiBatch()
        test(batchRegister.suspendAfterNoResult)
        batchRegister = SuspendRegister(batchMode=True, maximumSuspendedConnections=1)
        batchRegister._setLastStamp(1000)
        batchRegister.startOaiBatch()
        test(batchRegister.suspendBeforeSelect) #only suspend in batch (not outside)

    def testResumeOnlyMatchingSuspends(self):
        def test(register):
            resumed = []

            def suspendAfterNoResult(clientIdentifier, prefix, sets):
                if not clientIdentifier in register:
                    suspendObject = next(compose(register.suspendAfterNoResult(clientIdentifier=clientIdentifier, prefix=prefix, sets=sets)))
                    suspendObject(CallTrace('reactor'), lambda: resumed.append(clientIdentifier))

            def prepareSuspends():
                resumed[:] = []
                suspendAfterNoResult(clientIdentifier="client 1", prefix='prefix1', sets=[])
                suspendAfterNoResult(clientIdentifier="client 2", prefix='prefix2', sets=[])
                suspendAfterNoResult(clientIdentifier="client 3", prefix='prefix2', sets=['set_a'])

            prepareSuspends()
            register.signalOaiUpdate(metadataPrefixes=['prefix2'], sets=['set_b'], stamp=1000)
            self.assertEqual(['client 2'], resumed)

            prepareSuspends()
            register.signalOaiUpdate(metadataPrefixes=['prefix2'], sets=['set_a'], stamp=1001)
            self.assertEqual(['client 2', 'client 3'], sorted(resumed))
        test(SuspendRegister())
        test(SuspendRegister(batchMode=True)) # immediate resume state

    def testResumeOnlyMatchingSuspendsInBatchMode(self):
        register = SuspendRegister(batchMode=True)
        register._setLastStamp(999)
        register.startOaiBatch()
        resumed = []

        def suspendBeforeSelect(clientIdentifier, prefix, sets, continueAfter):
            suspendObject = next(compose(register.suspendBeforeSelect(clientIdentifier=clientIdentifier, prefix=prefix, sets=sets, continueAfter=continueAfter)))
            suspendObject(CallTrace('reactor'), lambda: resumed.append(clientIdentifier))

        suspendBeforeSelect('id0', prefix='p0', sets=['s0'], continueAfter='1000')
        suspendBeforeSelect('id1', prefix='p0', sets=['s1'], continueAfter='1000')
        suspendBeforeSelect('id2', prefix='p0', sets=['s1'], continueAfter='1000')
        suspendBeforeSelect('id3', prefix='p0', sets=[], continueAfter='1000')
        suspendBeforeSelect('id4', prefix='p1', sets=['s0'], continueAfter='1000')
        suspendBeforeSelect('id5', prefix='p1', sets=[], continueAfter='1000')

        register.signalOaiUpdate(metadataPrefixes=['p0'], sets=set(), stamp=1001)
        self.assertEqual([], resumed)
        register.stopOaiBatch()
        # nobody cared about the update
        self.assertEqual(['id3'], resumed)
        del resumed[:]
        register.startOaiBatch()
        register.signalOaiUpdate(metadataPrefixes=['p0'], sets=set(['s0', 's1']), stamp=1002)
        self.assertEqual([], resumed)
        register.stopOaiBatch()
        self.assertEqual(['id0', 'id1', 'id2'], sorted(resumed))
        del resumed[:]
        register.startOaiBatch()
        register.signalOaiUpdate(metadataPrefixes=['p1'], sets=set('s42'), stamp=1001)
        register.stopOaiBatch()
        self.assertEqual(['id5'], sorted(resumed))


    def testSuspendBeforeSelect(self):
        self.assertEqual([], asList(SuspendRegister().suspendBeforeSelect(continueAfter='9876', some='argument')))
        self.assertEqual([], asList(SuspendRegister(batchMode=True).suspendBeforeSelect(continueAfter='9876', some='argument')))

    def testInitialBatchSuspendAfterNoResult(self):
        reactor = CallTrace("reactor")
        register = SuspendRegister(batchMode=True)
        register.startOaiBatch()
        suspend = next(compose(register.suspendAfterNoResult(clientIdentifier="a-client-id", prefix='prefix', sets=[])))
        self.assertEqual(Suspend, type(suspend))
        resumed = []
        suspend(reactor, lambda: resumed.append(True))
        self.assertEqual([], resumed)
        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=1000)
        self.assertEqual([], resumed)
        self.assertEqual(1, len(register))
        register.stopOaiBatch()
        self.assertEqual([True], resumed)
        self.assertEqual(0, len(register))

    def testSuspendBeforeSelectWithContinueAfter(self):
        reactor = CallTrace("reactor")
        register = SuspendRegister(batchMode=True)
        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=1000)
        register.startOaiBatch()
        resumed = []

        suspend = next(compose(register.suspendBeforeSelect(clientIdentifier="id0", prefix='prefix', sets=[], continueAfter='1000')))
        suspend(reactor, lambda: resumed.append(True))
        suspend = next(compose(register.suspendBeforeSelect(clientIdentifier="id1", prefix='prefix', sets=[], continueAfter='1001')))
        suspend(reactor, lambda: resumed.append(True))
        self.assertEqual([], asList(register.suspendBeforeSelect(clientIdentifier="id2", prefix='prefix', sets=[], continueAfter='999')))

        self.assertEqual([], resumed)

        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=2000)
        self.assertEqual([], resumed)
        self.assertEqual(2, len(register))
        register.stopOaiBatch()
        self.assertEqual([True, True], resumed)
        self.assertEqual(0, len(register))

    def testEachBatchWillHaveItsOwnTimestamp(self):
        reactor = CallTrace("reactor")
        register = SuspendRegister(batchMode=True)
        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=1000)
        register.startOaiBatch()

        suspend = next(compose(register.suspendBeforeSelect(clientIdentifier="id0", prefix='prefix', sets=[], continueAfter='1000')))
        suspend(reactor, lambda: None)

        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=2000)
        self.assertEqual(1, len(register))
        register.stopOaiBatch()
        self.assertEqual(0, len(register))
        register.startOaiBatch()
        self.assertEqual([], asList(register.suspendBeforeSelect(clientIdentifier="id0", prefix='prefix', sets=[], continueAfter='1000')))
        self.assertEqual(0, len(register))

    def testStopStopBoom(self):
        register = SuspendRegister(batchMode=True)
        register.startOaiBatch()
        register.stopOaiBatch()
        self.assertRaises(AttributeError, lambda: register.stopOaiBatch())

    def testSuspendRegisterWithoutBatchIgnoreStartStop(self):
        register = SuspendRegister()
        register.startOaiBatch()
        register.startOaiBatch()
        register.startOaiBatch()

    def testSuspendBeforeSelectWithoutUpdates(self):
        reactor = CallTrace("reactor")
        register = SuspendRegister(batchMode=True)
        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=1000)
        register.startOaiBatch()
        resumed = []

        suspend = next(compose(register.suspendBeforeSelect(clientIdentifier="id0", prefix='prefix', sets=[], continueAfter='1000')))
        suspend(reactor, lambda: resumed.append(True))

        self.assertEqual([], resumed)
        self.assertEqual(1, len(register))
        register.stopOaiBatch()
        self.assertEqual([], resumed)
        self.assertEqual(1, len(register))

    def testInitialBatchSuspendBeforeResult(self):
        register = SuspendRegister(batchMode=True)
        register.startOaiBatch()
        self.assertEqual([], asList(register.suspendBeforeSelect(clientIdentifier="a-client-id", prefix='prefix', sets=[], continueAfter='0')))
        self.assertEqual([], asList(register.suspendBeforeSelect(clientIdentifier="a-client-id", prefix='prefix', sets=[], continueAfter='1000')))

    def testPostponedSignals(self):
        register = CallTrace('register')
        register._immediateState = CallTrace()
        state = _PostponedState(register)
        state.signalOaiUpdate(metadataPrefixes=set(['p0']), sets=set())
        state.signalOaiUpdate(metadataPrefixes=set(['p0']), sets=set(['s0']))
        state.signalOaiUpdate(metadataPrefixes=set(['p0']), sets=set(['s0']))
        state.signalOaiUpdate(metadataPrefixes=set(['p0']), sets=set(['s0']))
        state.signalOaiUpdate(metadataPrefixes=set(['p0']), sets=set(['s0', 's1']))
        state.signalOaiUpdate(metadataPrefixes=set(['p1']), sets=set(['s0']))
        state.signalOaiUpdate(metadataPrefixes=set(['p1']), sets=set(['s0']))
        state.signalOaiUpdate(metadataPrefixes=set(['p1', 'p2']), sets=set(['s0']))
        state.switchToImmediate()
        self.assertEqual(['_handleOaiUpdateSignal'], register.calledMethodNames())
        self.assertEqual({
                'p0': set(['s0', 's1']),
                'p1': set(['s0']),
                'p2': set(['s0']),
            }, register.calledMethods[0].kwargs['prefixAndSets'])

    def testTwoBatchesInterleave(self):
        register = SuspendRegister(batchMode=True)
        resumed = []
        def signalUpdate(stamp):
            register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=set(), otherKey='ignored', stamp=stamp)

        def suspendBeforeSelect(clientIdentifier, continueAfter='1000'):
            suspend = next(compose(register.suspendBeforeSelect(clientIdentifier=clientIdentifier, prefix='prefix', sets=[], continueAfter=continueAfter)))
            suspend(CallTrace('reactor'), lambda: resumed.append(clientIdentifier))

        signalUpdate(1000)
        register.startOaiBatch()
        suspendBeforeSelect('client0')
        self.assertEqual([], resumed)
        signalUpdate(1001)
        signalUpdate(1002)
        register.startOaiBatch()
        self.assertEqual([], resumed)
        suspendBeforeSelect('client1')
        signalUpdate(1003)
        register.stopOaiBatch()
        self.assertEqual(['client0', 'client1'], sorted(resumed))
        del resumed[:]
        suspendBeforeSelect('client2')
        signalUpdate(1004)
        register.stopOaiBatch()
        self.assertEqual(['client2'], sorted(resumed))
        signalUpdate(1005)

        self.assertRaises(AttributeError, lambda: register.stopOaiBatch())


