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
# Copyright (C) 2012, 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from meresco.core import Observable
from meresco.oai4 import OaiSetMask
from weightless.core import be, compose

class OaiSetMaskTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.observer = CallTrace()

        self.dna = be(
            (Observable(),
                (OaiSetMask(['set1', 'set2'], name="set1|set2 mask"),
                    (self.observer,)
                )
            )
        )

    def testOaiSelect(self):
        self.dna.call.oaiSelect()
        self.assertEquals(1, len(self.observer.calledMethods))
        methodCalled = self.observer.calledMethods[0]
        self.assertEquals(set(['set1', 'set2']), self.observer.calledMethods[0].kwargs['setsMask'])

    def testOaiSelectWithSetsMask(self):
        self.dna.call.oaiSelect(setsMask=['set3'])
        self.assertEquals(1, len(self.observer.calledMethods))
        methodCalled = self.observer.calledMethods[0]
        self.assertEquals(set(['set1', 'set2', 'set3']), self.observer.calledMethods[0].kwargs['setsMask'])

    def testGetUniqueInSets(self):
        self.observer.returnValues['getSets'] = ['set1', 'set2', 'set3']
        self.dna.call.getUnique('xyz')
        self.assertEquals(['getSets', 'getUnique'], [m.name for m in self.observer.calledMethods])
        getUniqueCall = self.observer.calledMethods[1]
        self.assertEquals(('xyz',), getUniqueCall.args)

    def testGetUniqueNotInSets(self):
        self.observer.returnValues['getSets'] = ['set1']
        self.dna.call.getUnique('xyz')
        self.assertEquals(['getSets'], [m.name for m in self.observer.calledMethods])

    def testGetUniqueWithSetsMask(self):
        self.observer.returnValues['getSets'] = ['set1', 'set2', 'set3']
        self.dna.call.getUnique('xyz', setsMask=['set3'])
        self.assertEquals(['getSets', 'getUnique'], [m.name for m in self.observer.calledMethods])
        getUniqueCall = self.observer.calledMethods[1]
        self.assertEquals(('xyz',), getUniqueCall.args)

        self.observer.calledMethods.reset()
        self.observer.returnValues['getSets'] = ['set1', 'set2']
        self.dna.call.getUnique('xyz', setsMask=['set3'])
        self.assertEquals(['getSets'], [m.name for m in self.observer.calledMethods])

    def testOtherMethodsArePassed(self):
        self.observer.methods['getAllMetadataFormats'] = lambda *a, **kw: (x for x in [])
        list(compose(self.dna.all.getAllMetadataFormats()))
        self.assertEquals(1, len(self.observer.calledMethods))
        self.assertEquals('getAllMetadataFormats', self.observer.calledMethods[0].name)

