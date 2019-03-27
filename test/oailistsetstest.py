## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2017, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
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

from weightless.core import Observable, be, asString

from meresco.oai.oailistsets import OaiListSets


class OaiListSetsTest(SeecrTestCase):
    def testSetNameEscaped(self):
        repository = CallTrace(returnValues=dict(requestUrl='http://example.org'))
        observer = CallTrace(returnValues=dict(getAllSets=[('123&abc', '123 & abc')]), emptyGeneratorMethods=['oaiWatermark'])
        top = be(
            (Observable(),
                (OaiListSets(repository),
                    (observer,)
                )
            )
        )
        response = asString(top.all.listSets(arguments=dict(verb=['ListSets'])))
        self.assertTrue('<ListSets><set><setSpec>123&amp;abc</setSpec><setName>123 &amp; abc</setName></set></ListSets>' in response, response)
