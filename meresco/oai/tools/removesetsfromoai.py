## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2017 SURFmarket https://surf.nl
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


def removeSetsFromOai(jazzDir, sets, prefix, batchSize=None):
    """Will remove sets from OaiJazz.
Records with this set will be updated.

BEWARE: Oai does not support deleted sets. Harvesters for specifically this set will not receive updates.

We assume there is no active oaiJazz in this directory

Usage: removeSetsFromOai(jazzDir, sets=['a:b'], prefix='your_prefix')"""
    from meresco.oai.oaijazz import OaiJazz, lazyImport
    lazyImport()
    from meresco.oai.oaijazz import Document, StringField, IDENTIFIER_FIELD, Field, PREFIX_FIELD, SETS_FIELD, SETS_DELETED_FIELD
    from meresco.oai.oaijazz import _setSpecAndSubsets

    class SetsDeletingOaiJazz(OaiJazz):
        def _getNewDocument(self, identifier, oldDoc):
            doc = Document()
            doc.add(StringField(IDENTIFIER_FIELD, identifier, Field.Store.YES))
            oldDeletedSets = set()
            if oldDoc is not None:
                for oldPrefix in oldDoc.getValues(PREFIX_FIELD):
                    doc.add(StringField(PREFIX_FIELD, oldPrefix, Field.Store.YES))
                oldSets = set(oldDoc.getValues(SETS_FIELD))
                allDeletedSets = set()
                for aSet in sets:
                    allDeletedSets.update(set(_setSpecAndSubsets(aSet)))
                if allDeletedSets != oldSets:
                    for oldSet in oldSets:
                        if oldSet in sets:
                            continue
                        doc.add(StringField(SETS_FIELD, oldSet, Field.Store.YES))
                oldDeletedSets.update(oldDoc.getValues(SETS_DELETED_FIELD))
                oldDeletedSets.difference_update(sets)
            return doc, oldDeletedSets
    deletingOaiJazz = SetsDeletingOaiJazz(jazzDir)
    goOn = True
    while goOn:
        select = deletingOaiJazz.oaiSelect(prefix=prefix, sets=sets, batchSize=batchSize)
        print 'Removing set for %s records' % select.numberOfRecordsInBatch
        for record in select.records:
            deletingOaiJazz.addOaiRecord(identifier=record.identifier, metadataPrefixes=[prefix])
        goOn = select.numberOfRecordsInBatch
    for aSet in sets:
        if aSet in deletingOaiJazz._sets:
            del deletingOaiJazz._sets[aSet]
    deletingOaiJazz.close()
