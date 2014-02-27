## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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


from meresco.core import Transparent
from meresco.core.generatorutils import decorate
from weightless.core import compose
from xml.sax.saxutils import escape as xmlEscape

class OaiRecord(Transparent):
    def _oaiRecordHeader(self, record):
        isDeletedStr = ' status="deleted"' if record.isDeleted else ''
        datestamp = record.getDatestamp()
        yield '<header%s>' % isDeletedStr
        yield '<identifier>%s</identifier>' % xmlEscape(record.identifier.encode('utf-8'))
        yield '<datestamp>%s</datestamp>' % datestamp
        yield self._getSetSpecs(record)
        yield '</header>'

    def oaiRecordHeader(self, record, **kwargs):
        yield self._oaiRecordHeader(record=record)

    def oaiRecord(self, record, metadataPrefix, data):
        yield '<record>'
        yield self._oaiRecordHeader(record)

        if not record.isDeleted:
            yield '<metadata>'
            data = None if data is None else data.get(str(record.stamp))
            if not data is None:
                yield data
            else:
                yield self.all.yieldRecord(record.identifier, metadataPrefix)
            yield '</metadata>'

            provenance = compose(self.all.provenance(record.identifier))
            for line in decorate('<about>', provenance, '</about>'):
                yield line

        yield '</record>'

    def _getSetSpecs(self, record):
        if record.sets:
            return ''.join('<setSpec>%s</setSpec>' % xmlEscape(setSpec) for setSpec in record.sets)
        return ''

