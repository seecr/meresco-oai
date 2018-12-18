## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015, 2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2018 Stichting Kennisnet https://www.kennisnet.nl
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
from seecr.test.io import stdout_replaced
from meresco.oai import OaiJazz
from meresco.oai.tools.removesetsfromoai import removeSetsFromOai

class RemoveSetsFromOaiTest(SeecrTestCase):

    @stdout_replaced
    def testRemoveSet(self):
        oaiJazz = OaiJazz(self.tempdir)
        oaiJazz.updateSet('a:b', 'set A/B')
        oaiJazz.updateSet('a:c', 'set A/C')
        oaiJazz.addOaiRecord('id:0', setSpecs=['a:b', 'a:c'], metadataFormats=[('prefix', '', '')])
        oaiJazz.addOaiRecord('id:1', setSpecs=['a:b'], metadataFormats=[('prefix', '', '')])
        oaiJazz.addOaiRecord('id:2', setSpecs=['a:c'], metadataFormats=[('prefix', '', '')])

        self.assertEquals([
                ('id:0', set([u'a', u'a:b', u'a:c']), False),
                ('id:1', set([u'a', u'a:b']), False),
                ('id:2', set([u'a', u'a:c']), False),
            ],
            [(r.identifier, r.sets, r.isDeleted) for r in oaiJazz.oaiSelect(prefix='prefix').records])
        self.assertEquals(set(['a:b', 'a', 'a:c']), oaiJazz.getAllSets())

        oaiJazz.close()

        removeSetsFromOai(self.tempdir, sets=['a:b'], prefix='prefix', batchSize=1)

        oaiJazz = OaiJazz(self.tempdir)
        self.assertEquals([
                ('id:2', set([u'a', u'a:c']), False),
                ('id:0', set([u'a', u'a:c']), False),
                ('id:1', set([]), False), 
            ],
            [(r.identifier, r.sets, r.isDeleted) for r in oaiJazz.oaiSelect(prefix='prefix').records])
        self.assertEquals(set(['a', 'a:c']), oaiJazz.getAllSets())
