## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2012-2013, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Observable
from meresco.oai import OaiSetSelect
from weightless.core import be, compose
from mockoaijazz import MockRecord


class OaiSetSelectTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.observer = CallTrace()

        with stderr_replaced() as err:
            self.dna = be(
                (Observable(),
                    (OaiSetSelect(['set1', 'set2']),
                        (self.observer,)
                    )
                )
            )
            self.assertTrue(not err.getvalue() or \
                'warn("OaiSetSelect is deprecated;' in err.getvalue(), err.getvalue())


    def testOaiSelect(self):
        self.dna.call.oaiSelect()
        self.assertEqual(1, len(self.observer.calledMethods))
        methodCalled = self.observer.calledMethods[0]
        self.assertTrue('sets' in methodCalled.kwargs, methodCalled)
        self.assertEqual(['set1', 'set2'], self.observer.calledMethods[0].kwargs['sets'])

    def testGetUniqueInSet(self):
        self.observer.returnValues['getRecord'] = MockRecord('id', sets=['set1'])
        self.dna.call.getRecord('xyz')
        self.assertEqual(['getRecord'], self.observer.calledMethodNames())
        getRecordCall = self.observer.calledMethods[0]
        self.assertEqual(('xyz',), getRecordCall.args)

    def testGetUniqueNotInSet(self):
        self.observer.returnValues['getRecord'] = MockRecord('id', sets=['set4'])
        self.dna.call.getRecord('xyz')
        self.assertEqual(['getRecord'], self.observer.calledMethodNames())

    def testOtherMethodsArePassed(self):
        self.observer.methods['getAllMetadataFormats'] = lambda *a, **kw: (x for x in [])
        list(compose(self.dna.all.getAllMetadataFormats()))
        self.assertEqual(1, len(self.observer.calledMethods))
        self.assertEqual('getAllMetadataFormats', self.observer.calledMethods[0].name)

    def testSetsIsNone(self):
        self.dna.call.oaiSelect(sets=None)
        self.assertEqual(1, len(self.observer.calledMethods))
        methodCalled = self.observer.calledMethods[0]
        self.assertTrue('sets' in methodCalled.kwargs, methodCalled)
        self.assertEqual(['set1', 'set2'], self.observer.calledMethods[0].kwargs['sets'])

