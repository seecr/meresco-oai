## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2011 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
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


from meresco.core import Transparant
from xml.sax.saxutils import escape as xmlEscape
from meresco.core.generatorutils import decorate

class OaiRecord(Transparant):
    def _oaiRecordHeader(self, recordId, isDeleted):
        isDeletedStr = ' status="deleted"' if isDeleted else ''
        datestamp = self.any.getDatestamp(recordId)
        yield '<header%s>' % isDeletedStr
        yield '<identifier>%s</identifier>' % xmlEscape(recordId.encode('utf-8'))
        yield '<datestamp>%s</datestamp>' % datestamp
        yield self._getSetSpecs(recordId)
        yield '</header>'

    def oaiRecordHeader(self, recordId, **kwargs):
        yield self._oaiRecordHeader(recordId, self.any.isDeleted(recordId))

    def oaiRecord(self, recordId, metadataPrefix):
        yield '<record>'
        isDeleted = self.any.isDeleted(recordId)
        yield self._oaiRecordHeader(recordId, isDeleted)

        if not isDeleted:
            yield '<metadata>'
            yield self.all.yieldRecord(recordId, metadataPrefix)
            yield '</metadata>'
            
            provenance = self.all.provenance(recordId)
            for line in decorate('<about>', provenance, '</about>'):
                yield line

        yield '</record>'

    def _getSetSpecs(self, recordId):
        sets = self.any.getSets(recordId)
        if sets:
            return ''.join('<setSpec>%s</setSpec>' % xmlEscape(setSpec) for setSpec in sets)
        return ''

