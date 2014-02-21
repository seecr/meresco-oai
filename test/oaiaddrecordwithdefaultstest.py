## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from seecr.test import CallTrace, SeecrTestCase
from unittest import TestCase
from weightless.core import compose, consume
from meresco.oai import OaiAddRecordWithDefaults, OaiJazz, SequentialMultiStorage
from os import makedirs
from lxml.etree import XML as parseLxml

class OaiAddRecordWithDefaultsTest(SeecrTestCase):
    def testAdd(self):
        subject = OaiAddRecordWithDefaults(sets=[('setSpec', 'setName')], metadataFormats=[('prefix','schema','namespace')])
        observer = CallTrace('oaijazz')
        subject.addObserver(observer)

        list(compose(subject.add('id', ignored="kwarg", lxmlNode="na")))

        self.assertEquals(['addOaiRecord'], [m.name for m in observer.calledMethods])
        self.assertEquals({'identifier':'id',
            'sets': [('setSpec', 'setName')],
            'metadataFormats': [('prefix','schema','namespace')]},
            observer.calledMethods[0].kwargs)

    def testAddWithMethods(self):
        methodObject = CallTrace(returnValues={'sets':[('setSpec', 'setName')], 'metadataFormats': [('prefix','schema','namespace')]})
        subject = OaiAddRecordWithDefaults(sets=methodObject.sets , metadataFormats=methodObject.metadataFormats)
        observer = CallTrace('oaijazz')
        subject.addObserver(observer)

        list(compose(subject.add('id', ignored="kwarg", lxmlNode="data")))

        self.assertEquals(['addOaiRecord'], [m.name for m in observer.calledMethods])
        self.assertEquals({'identifier':'id',
            'sets': [('setSpec', 'setName')],
            'metadataFormats': [('prefix','schema','namespace')]},
            observer.calledMethods[0].kwargs)
        self.assertEquals(['sets', 'metadataFormats'], methodObject.calledMethodNames())
        for method in methodObject.calledMethods:
            self.assertEquals({'identifier':'id', 'ignored':'kwarg', 'lxmlNode':"data"}, method.kwargs)

    def testUseSequentialStorageWithOaiAddRecordWithDefaults(self):
        addrecord = OaiAddRecordWithDefaults(metadataFormats=[('part1', '?', '?'), ('part2', '?', '?')], useSequentialStorage=True)
        jazz =  OaiJazz(self.tempdir)
        addrecord.addObserver(jazz)
        makedirs(self.tempdir + '/1')
        storage = SequentialMultiStorage(self.tempdir + '/1')
        addrecord.addObserver(storage)
        consume(addrecord.add("id0", lxmlNode=parseLxml("<xml/>")))
        t, data = storage.iterData("part1", 0, 9999999999999).next()
        self.assertEquals("<xml/>", data)
        t, data = storage.iterData("part2", 0, 9999999999999).next()
        self.assertEquals("<xml/>", data)


