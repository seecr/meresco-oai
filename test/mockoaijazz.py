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

class MockOaiJazz(object):
    def __init__(self, selectAnswer=None, setsAnswer=None, deleted=None, isAvailableDefault=(True,True), isAvailableAnswer=None, selectTotal=5000):
        self._selectAnswer = selectAnswer if selectAnswer else []
        self._setsAnswer = setsAnswer if setsAnswer else []
        self._deleted = deleted if deleted else []
        self._isAvailableDefault = isAvailableDefault
        self._isAvailableAnswer = isAvailableAnswer if isAvailableAnswer else []
        self.oaiSelectArguments = {}

    def oaiSelect(self, sets=None, prefix=None, continueAfter=None, oaiFrom=None, oaiUntil=None, batchSize=0):
        self.oaiSelectArguments = (sets if sets else [], prefix, continueAfter, oaiFrom, oaiUntil, batchSize)
        return iter(self._selectAnswer) 

    def getUnique(self, id):
        return 'Unique for test'

    def getSets(self, id):
        return self._setsAnswer

    def getDatestamp(self, id):
        return 'DATESTAMP_FOR_TEST'

    def getPrefixes(self, id):
        raise "STOP"

    def isDeleted(self, id):
        return id in self._deleted

    def getAllMetadataFormats(self):
        return [('oai_dc',None,None)]
    
    def getAllPrefixes(self):
        return ['oai_dc']

    def yieldRecord(self, id, partName):
        if partName == 'oai_dc':
            yield '<some:recorddata xmlns:some="http://some.example.org" id="%s"/>' % id.replace('&', '&amp;')
        elif partName == 'meta':
            yield """<meta>
  <repository>
    <baseurl>META_BASEURL</baseurl>
    <harvestdate>META_HARVESTDATE</harvestdate>
    <metadataNamespace>META_METADATANAMESPACE</metadataNamespace>
  </repository>
</meta>"""
        elif partName == 'header':
            yield """<header xmlns="http://www.openarchives.org/OAI/2.0/">
            <identifier>HEADER_IDENTIFIER</identifier>
            <datestamp>HEADER_DATESTAMP</datestamp>
        </header>"""
        elif partName == '__stamp__':
            yield """<__stamp__>
    <datestamp>DATESTAMP_FOR_TEST</datestamp>
</__stamp__>"""
        elif partName == '__sets__':
            yield """<__sets__><set><setSpec>one:two:three</setSpec><setName>Three Piggies</setName></set><set><setSpec>one:two:four</setSpec><setName>Four Chickies</setName></set></__sets__>"""
        else:
            self.fail(partName + ' is unexpected')

    def isAvailable(self, id, partName):
        result = self._isAvailableDefault
        for (aId, aPartname, answer) in self._isAvailableAnswer:
            if (aId == None or aId == id) and aPartname == partName:
                result = answer
                break
        return result
