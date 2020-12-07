# -*- coding: utf-8 -*-
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
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.core import Observable, TransactionScope, ResourceManager
from weightless.core import be, compose
from meresco.oai import Fields2OaiRecord

class Fields2OaiRecordTest(SeecrTestCase):
    def testOne(self):
        __callstack_var_tx__ = CallTrace('TX')
        __callstack_var_tx__.locals = {'id': 'identifier'}
        intercept = CallTrace()
        fields2OaiRecord = Fields2OaiRecord()
        fields2OaiRecord.addObserver(intercept)
        def f():
            f = yield fields2OaiRecord.beginTransaction()
            yield f
        tx = next(compose(f()))
        
        tx.addField('set', ('setSpec', 'setName'))
        tx.addField('metadataFormat', ('prefix', 'schema', 'namespace'))
        list(compose(tx.commit()))

        self.assertEqual(1, len(intercept.calledMethods))
        self.assertEqual('addOaiRecord', intercept.calledMethods[0].name)
        self.assertEqual({'identifier':'identifier',
                'metadataFormats': set([('prefix', 'schema', 'namespace')]),
                'sets': set([('setSpec', 'setName')])},
            intercept.calledMethods[0].kwargs)

    def testNothing(self):
        __callstack_var_tx__ = CallTrace('TX')
        __callstack_var_tx__.locals = {'id': 'identifier'}
        intercept = CallTrace()
        fields2OaiRecord = Fields2OaiRecord()
        fields2OaiRecord.addObserver(intercept)
        def f():
            f = yield fields2OaiRecord.beginTransaction()
            yield f
        tx = next(compose(f()))
        tx.addField('set', ('setSpec', 'setName'))
        tx.commit()
        self.assertEqual(0, len(intercept.calledMethods))

    def testWorksWithRealTransactionScope(self):
        intercept = CallTrace('Intercept', ignoredAttributes=['begin', 'commit', 'rollback'])
        class MockVenturi(Observable):
            def all_unknown(self, message, *args, **kwargs):
                self.ctx.tx.locals['id'] = 'an:identifier'
                yield self.all.unknown(message, *args, **kwargs)
        class MockMultiFielder(Observable):
            def add(self, *args, **kwargs):
                self.do.addField('set', ('setSpec', 'setName'))
                self.do.addField('metadataFormat', ('prefix', 'schema', 'namespace'))
                yield 'ok'
        root = be( 
            (Observable(),
                (TransactionScope(transactionName="oaiRecord"),
                    (MockVenturi(),
                        (MockMultiFielder(),
                            (ResourceManager("oaiRecord"),
                                (Fields2OaiRecord(),
                                    (intercept,),
                                )   
                            )   
                        )   
                    )   
                )   
            )   
        )
        list(compose(root.all.add('some', 'arguments')))
        self.assertEqual(['addOaiRecord'], [m.name for m in intercept.calledMethods])
        method = intercept.calledMethods[0]
        self.assertEqual(((), {'identifier': 'an:identifier', 'metadataFormats': set([('prefix', 'schema', 'namespace')]), 'sets': set([('setSpec', 'setName')])}), (method.args, method.kwargs))        
