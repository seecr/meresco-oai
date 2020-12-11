## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2011, 2018 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2012-2014, 2016-2018 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from xml.sax.saxutils import escape as xmlEscape

from weightless.core import compose, NoneOfTheObserversRespond
from meresco.core import Transparent
from meresco.core.generatorutils import decorate


class OaiRecord(Transparent):
    def __init__(self, repository=None, preciseDatestamp=False, deleteInSets=False, **kwargs):
        Transparent.__init__(self, **kwargs)
        self._repository = repository
        self._preciseDatestamp = preciseDatestamp
        self._deleteInSetsSupport = deleteInSets

    def oaiRecordHeader(self, record, **kwargs):
        isDeletedStr = ' status="deleted"' if record.isDeleted else ''
        datestamp = record.getDatestamp(preciseDatestamp=self._preciseDatestamp)
        identifier = record.identifier
        if self._repository:
            identifier = self._repository.prefixIdentifier(identifier)
        yield '<header%s>' % isDeletedStr
        yield '<identifier>%s</identifier>' % xmlEscape(identifier)
        yield '<datestamp>%s</datestamp>' % datestamp
        yield self._getSetSpecs(record)
        yield '</header>'

    def oaiRecord(self, record, metadataPrefix, fetchedRecords=None):
        yield '<record>'
        yield self.oaiRecordHeader(record)

        if not record.isDeleted:
            yield '<metadata>'
            if not fetchedRecords is None:
                try:
                    yield fetchedRecords[record.identifier]
                except KeyError:
                    pass
            else:
                try:
                    yield self.call.getData(identifier=record.identifier, name=metadataPrefix)
                except NoneOfTheObserversRespond:
                    data = yield self.any.retrieveData(identifier=record.identifier, name=metadataPrefix)
                    yield data
            yield '</metadata>'

            provenance = compose(self.all.provenance(record.identifier))
            for line in decorate('<about>', provenance, '</about>'):
                yield line

        yield '</record>'

    def _getSetSpecs(self, record):
        if record.sets:
            deletedSets = set()
            if self._deleteInSetsSupport:
                deletedSets = record.deletedSets
            return ''.join('<setSpec{1}>{0}</setSpec>'.format(
                    xmlEscape(setSpec),
                    ' status="deleted"' if setSpec in deletedSets else ''
                ) for setSpec in sorted(record.sets))
        return ''
