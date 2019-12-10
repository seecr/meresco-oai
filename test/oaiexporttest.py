## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2019 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2019 Seecr (Seek You Too B.V.) https://seecr.nl
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

from meresco.oai import OaiJazz
from json import loads
from os.path import join
from time import time

class OaiExportTest(SeecrTestCase):
    def testExport(self):
        jazz = OaiJazz(join(self.tempdir, 'oai'), deleteInSets=True)
        jazz.updateMetadataFormat(prefix='someprefix', schema='https://example.org/schema.xsd', namespace='urn:ns')
        jazz.updateMetadataFormat(prefix='prefix', schema='schema', namespace='namespace')
        jazz.updateSet(setSpec='a', setName='A')
        jazz.updateSet(setSpec='setSpec', setName='setName')
        jazz.addOaiRecord(identifier='id:0', metadataPrefixes=['prefix'])
        jazz.addOaiRecord(identifier='id:1', metadataPrefixes=['prefix'], setSpecs=['a', 'a:b', 'd:e:f'])
        jazz.addOaiRecord(identifier='id:2', metadataPrefixes=['prefix', 'someprefix'], setSpecs=['a', 'a:b', 'd:e:f'])
        jazz.addOaiRecord(identifier='id:3', metadataPrefixes=['prefix', 'someprefix'], setSpecs=['a', 'a:b', 'd:e:f'])
        for i in xrange(4,3000):
            jazz.addOaiRecord(identifier='id:{}'.format(i), metadataPrefixes=['prefix'])

        jazz.deleteOaiRecordInSets(identifier='id:3', setSpecs=['d:e:f'])
        jazz.deleteOaiRecord(identifier='id:7')

        dumpfile = join(self.tempdir, 'dump')
        jazz.export(dumpfile)

        d = open(dumpfile).readlines()
        self.assertEqual(3003, len(d))
        self.assertEqual('META:\n', d[0])
        self.assertEqual('RECORDS:\n', d[2])
        meta = loads(d[1].strip())
        self.assertEqual({
            u'export_version': 1,
            u'metadataPrefixes': {
                u'someprefix': {u'schema': u'https://example.org/schema.xsd', u'namespace': u'urn:ns'},
                u'prefix': {u'schema': u'schema', u'namespace': u'namespace'},
            },
            u'sets': {
                u'a': {u'setName': u'A'},
                u'a:b': {u'setName': u''},
                u'd': {u'setName': u''},
                u'd:e': {u'setName': u''},
                u'd:e:f': {u'setName': u''},
                u'setSpec': {u'setName': u'setName'},
            }
        }, meta)
        record0 = loads(d[3].strip())
        self.assertAlmostEqual(time(), record0['timestamp'] / 10.0 ** 6, delta=3)
        record0['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            u'identifier': u'id:0',
            u'timestamp': 'TIMESTAMP',
            u'tombstone': False,
            u'prefixes': ['prefix'],
            u'deletedSets': [],
            u'sets': [],}, record0)
        record2 = loads(d[5].strip())
        record2['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            u'identifier': u'id:2',
            u'timestamp': 'TIMESTAMP',
            u'tombstone': False,
            u'prefixes': ['prefix', 'someprefix'],
            u'deletedSets': [],
            u'sets': ['a', 'a:b', 'd', 'd:e', 'd:e:f'],}, record2)
        record3 = loads(d[-2].strip())
        record3['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            u'identifier': u'id:3',
            u'timestamp': 'TIMESTAMP',
            u'tombstone': False,
            u'prefixes': ['prefix', 'someprefix'],
            u'deletedSets': ['d:e:f'],
            u'sets': ['a', 'a:b', 'd', 'd:e', 'd:e:f'],}, record3)
        record7 = loads(d[-1].strip())
        record7['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            u'identifier': u'id:7',
            u'timestamp': 'TIMESTAMP',
            u'tombstone': True,
            u'prefixes': ['prefix'],
            u'deletedSets': [],
            u'sets': [],}, record7)



# TODO:
# - import from dump
