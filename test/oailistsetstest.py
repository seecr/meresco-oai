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

from os.path import join
from cq2utils.calltrace import CallTrace

from oaitestcase import OaiTestCase
from meresco.oai.oailistsets import OaiListSets
from meresco.components.facetindex import LuceneIndex
from meresco.components import StorageComponent

class OaiListSetsTest(OaiTestCase):
    def getSubject(self):
        return OaiListSets()



    def testListSetsNoArguments(self):
        mockJazz = CallTrace(returnValues = {'getAllSets': ['some:name:id_0', 'some:name:id_1']})
        self.request.args = {'verb':['ListSets']}
        self.subject.addObserver(mockJazz)
        list(self.subject.listSets(self.request))
        self.assertEqualsWS(self.OAIPMH % """
<request verb="ListSets">http://server:9000/path/to/oai</request>
 <ListSets>
   <set><setSpec>some:name:id_0</setSpec><setName>set some:name:id_0</setName></set>
   <set><setSpec>some:name:id_1</setSpec><setName>set some:name:id_1</setName></set>
 </ListSets>""", self.stream.getvalue())
        self.assertFalse('<resumptionToken' in self.stream.getvalue())

