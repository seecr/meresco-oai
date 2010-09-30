## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
#    Copyright (C) 2007-2009 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
#    Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
#    Copyright (C) 2009 Tilburg University http://www.uvt.nl
#    Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
#
#    This file is part of Meresco Oai.
#
#    Meresco Oai is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    Meresco Oai is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Meresco Oai; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from oaitestcase import OaiTestCase

from mockoaijazz import MockOaiJazz

from meresco.core import ObserverFunction
from meresco.oai.oaigetrecord import OaiGetRecord
from meresco.components.http.utils import CRLF

from cq2utils.calltrace import CallTrace

class OaiGetRecordTest(OaiTestCase):
    def getSubject(self):
        oaigetrecord = OaiGetRecord()
        oaigetrecord.addObserver(ObserverFunction(lambda: ['oai_dc'], 'getAllPrefixes'))
        return oaigetrecord


    def testGetRecordNotAvailable(self):
        self.request.args = {'verb':['GetRecord'], 'metadataPrefix': ['oai_dc'], 'identifier': ['oai:ident']}

        observer = CallTrace('RecordAnswering')
        notifications = []
        def isAvailable(id, partName):
            notifications.append((id, partName))
            return False, False
        observer.isAvailable = isAvailable
        observer.returnValues['getDatestamp'] = 'DATESTAMP_FOR_TEST'
        self.subject.addObserver(observer)

        list(self.observable.all.getRecord(self.request))
        body = self.stream.getvalue().split(CRLF*2)[-1]
        self.assertTrue("""<request identifier="oai:ident" metadataPrefix="oai_dc" verb="GetRecord">http://server:9000/path/to/oai</request>""" in body, body)
        self.assertTrue("""<error code="idDoesNotExist">The value of the identifier argument is unknown or illegal in this repository.</error>""" in body, body)
        self.assertValidString(body)

        self.assertEquals([('oai:ident', 'oai_dc')], notifications)

    def testGetRecord(self):
        self.request.args = {'verb':['GetRecord'], 'metadataPrefix': ['oai_dc'], 'identifier': ['oai:ident']}

        self.subject.addObserver(MockOaiJazz(
            isAvailableDefault=(True, False),
            isAvailableAnswer=[(None, 'oai_dc', (True,True))]))
        list(self.observable.all.getRecord(self.request))
        body = self.stream.getvalue().split(CRLF*2)[-1]
        self.assertTrue("<GetRecord>" in body, body)
        self.assertTrue("<identifier>oai:ident</identifier>" in body, body)
        self.assertTrue("""<some:recorddata xmlns:some="http://some.example.org" id="oai:ident"/>""" in body, body)

    def testDeletedRecord(self):
        self.request.args = {'verb':['GetRecord'], 'metadataPrefix': ['oai_dc'], 'identifier': ['oai:ident']}

        self.subject.addObserver(MockOaiJazz(
            isAvailableDefault=(True, False),
            isAvailableAnswer=[(None, "oai_dc", (True, False))],
            deleted=['oai:ident']))
        list(self.observable.all.getRecord(self.request))
        self.assertTrue("deleted" in self.stream.getvalue(), self.stream.getvalue())
