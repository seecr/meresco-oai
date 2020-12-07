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
from os.path import join, dirname, abspath
from time import time

mydir = dirname(abspath(__file__))
datadir = join(mydir, 'data')

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
        for i in range(4,3000):
            jazz.addOaiRecord(identifier='id:{}'.format(i), metadataPrefixes=['prefix'])

        jazz.deleteOaiRecordInPrefixes(identifier='id:2', metadataPrefixes=['someprefix'])
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
            'export_version': 1,
            'metadataPrefixes': {
                'someprefix': {'schema': 'https://example.org/schema.xsd', 'namespace': 'urn:ns'},
                'prefix': {'schema': 'schema', 'namespace': 'namespace'},
            },
            'sets': {
                'a': {'setName': 'A'},
                'a:b': {'setName': ''},
                'd': {'setName': ''},
                'd:e': {'setName': ''},
                'd:e:f': {'setName': ''},
                'setSpec': {'setName': 'setName'},
            }
        }, meta)
        record0 = loads(d[3].strip())
        self.assertAlmostEqual(time(), record0['timestamp'] / 10.0 ** 6, delta=3)
        record0['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            'identifier': 'id:0',
            'timestamp': 'TIMESTAMP',
            'tombstone': False,
            'deletedPrefixes': [],
            'prefixes': ['prefix'],
            'deletedSets': [],
            'sets': [],}, record0)
        record2 = loads(d[-3].strip())
        record2['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            'identifier': 'id:2',
            'timestamp': 'TIMESTAMP',
            'tombstone': False,
            'deletedPrefixes': ['someprefix'],
            'prefixes': ['prefix', 'someprefix'],
            'deletedSets': [],
            'sets': ['a', 'a:b', 'd', 'd:e', 'd:e:f'],}, record2)
        record3 = loads(d[-2].strip())
        record3['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            'identifier': 'id:3',
            'timestamp': 'TIMESTAMP',
            'tombstone': False,
            'deletedPrefixes': [],
            'prefixes': ['prefix', 'someprefix'],
            'deletedSets': ['d:e:f'],
            'sets': ['a', 'a:b', 'd', 'd:e', 'd:e:f'],}, record3)
        record7 = loads(d[-1].strip())
        record7['timestamp'] = 'TIMESTAMP'
        self.assertEqual({
            'identifier': 'id:7',
            'timestamp': 'TIMESTAMP',
            'tombstone': True,
            'deletedPrefixes': ['prefix'],
            'prefixes': ['prefix'],
            'deletedSets': [],
            'sets': [],}, record7)

    def testOaiJazzImport(self):
        dumpfile = join(datadir, 'oaiexport.dump')
        result = OaiJazz.importDump(join(self.tempdir, 'oai'), dumpfile)
        self.assertTrue(result)
        jazz = OaiJazz(join(self.tempdir, 'oai'), deleteInSets=True)
        r = jazz.oaiSelect(prefix=None)
        self.assertEqual(7, r.numberOfRecordsInBatch)
        records = list(r.records)
        self.assertEqual([
                ('id:0', False, '2019-12-10T09:49:09Z', {'prefix'}),
                ('id:1', False, '2019-12-10T09:49:29Z', {'prefix'}),
                ('id:4', False, '2019-12-10T09:50:49Z', {'prefix'}),
                ('id:5', False, '2019-12-10T10:05:49Z', {'prefix'}),
                ('id:2', False, '2019-12-10T10:07:29Z', {'prefix', 'someprefix'}),
                ('id:3', False, '2019-12-10T10:17:29Z', {'prefix', 'someprefix'}),
                ('id:7', True, '2019-12-10T10:22:29Z', {'prefix'}),
            ], [(rec.identifier, rec.isDeleted, rec.getDatestamp(), rec.prefixes) for rec in records])

        r2 = records[-3]
        r3 = records[-2]
        r7 = records[-1]
        self.assertEqual([
            ('id:2', {'prefix', 'someprefix'}, {'someprefix'}, {'a', 'a:b', 'd', 'd:e', 'd:e:f'}, set()),
            ('id:3', {'prefix', 'someprefix'}, set(), {'a', 'a:b', 'd', 'd:e', 'd:e:f'}, {'d:e:f'}),
            ('id:7', {'prefix'}, {'prefix'}, set(), set()),
            ], [(rec.identifier, rec.prefixes, rec.deletedPrefixes, rec.sets, rec.deletedSets) for rec in [r2, r3, r7]])



# TODO:
# - import from dump
