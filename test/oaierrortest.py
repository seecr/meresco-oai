## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2016 SURFmarket https://surf.nl
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

from weightless.core import compose, be, Yield
from meresco.core import Observable

from meresco.oai.oaierror import OaiError
from meresco.oai.oairepository import OaiRepository


class OaiErrorTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)

        self.data = []
        def responder(**kwargs):
            while self.data:
                yield self.data.pop(0)
        self.observer = CallTrace('Observer', methods={'someMessage': responder})

        self.dna = be((Observable(),
            (OaiError(repository=OaiRepository()),
                (self.observer,)
            )
        ))


    def testShouldGiveOaiErrorOnNoData(self):
        result = ''.join(compose(self.dna.all.someMessage(arguments={'http': 'arguments'}, Headers={}, path='/some/path', port=0, otherKwargs='kwargs')))
        self.assertTrue('HTTP/1.0 200 OK\r\nContent-Type: text/xml; charset=utf-8\r\n\r\n', result)
        self.assertTrue('<error code="badArgument">' in result, result)

    def testShouldNotCountInitialCallablesOrYieldsAsNonErrorCondition(self):
        callablesAndYields = [Yield, callable, Yield, lambda: None]
        self.data.extend(callablesAndYields)

        result = list(compose(self.dna.all.someMessage(arguments={'http': 'arguments'}, Headers={}, path='/some/path', port=0, otherKwargs='kwargs')))
        initial, remaining = result[:4], result[4:]
        remaining = ''.join(remaining)

        self.assertEqual(callablesAndYields, initial)
        self.assertTrue('HTTP/1.0 200 OK\r\nContent-Type: text/xml; charset=utf-8\r\n\r\n', remaining)
        self.assertTrue('<error code="badArgument">' in remaining, remaining)

    def testShouldConsiderEverythingFineAfterFistPieceOfData(self):
        # Data only
        callablesAndYields = ['data']
        self.data.extend(callablesAndYields)

        result = ''.join(compose(self.dna.all.someMessage(arguments={'http': 'arguments'}, Headers={}, path='/some/path', port=0, otherKwargs='kwargs')))
        self.assertEqual('data', result)

        # Callables, Yields and then data
        callablesAndYields = [Yield, callable, Yield, lambda: None]
        self.data.extend(callablesAndYields)
        self.data.append('data')

        result = list(compose(self.dna.all.someMessage(arguments={'http': 'arguments'}, Headers={}, path='/some/path', port=0, otherKwargs='kwargs')))
        initial, remaining = result[:4], result[4:]
        remaining = ''.join(remaining)

        self.assertEqual(callablesAndYields, initial)
        self.assertEqual('data', remaining)

