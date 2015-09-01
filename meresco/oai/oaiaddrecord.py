## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2010-2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2011-2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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
from lxml.etree import iselement
from meresco.xml import xpath
from meresco.xml.namespaces import xpathFirst, expandNs

class OaiAddDeleteRecordWithPrefixesAndSetSpecs(Transparent):
    def __init__(self, metadataPrefixes=None, setSpecs=None, name=None):
        Transparent.__init__(self, name=name)
        self._setSpecs = _prepare(setSpecs)
        self._metadataPrefixes = _prepare(metadataPrefixes)

    def add(self, identifier, **kwargs):
        self.call.addOaiRecord(
            identifier=identifier,
            setSpecs=self._setSpecs(identifier=identifier, **kwargs),
            metadataPrefixes=self._metadataPrefixes(identifier=identifier, **kwargs))
        return
        yield

    def delete(self, identifier, **kwargs):
        self.call.deleteOaiRecord(
            identifier=identifier,
            setSpecs=self._setSpecs(identifier=identifier, **kwargs),
            metadataPrefixes=self._metadataPrefixes(identifier=identifier, **kwargs))
        return
        yield

class OaiAddRecordWithDefaults(Transparent):
    def __init__(self, metadataFormats=None, sets=None, name=None):
        Transparent.__init__(self, name=name)
        self._sets = _prepare(sets)
        self._metadataFormats = _prepare(metadataFormats)

    def add(self, identifier, **kwargs):
        self.call.addOaiRecord(
            identifier=identifier,
            sets=self._sets(identifier=identifier, **kwargs),
            metadataFormats=self._metadataFormats(identifier=identifier, **kwargs))
        return
        yield

def _prepare(iterableOrCallable):
    if iterableOrCallable is None:
        return lambda **kwargs: []
    return iterableOrCallable if callable(iterableOrCallable) else lambda **kwargs: iterableOrCallable


class OaiAddRecord(Transparent):
    def add(self, identifier, partname, lxmlNode):
        record = lxmlNode if iselement(lxmlNode) else lxmlNode.getroot()
        oaiHeader = xpathFirst(record, 'oai:header')
        if oaiHeader is None:
            oaiHeader = xpathFirst(record, '/oai:header')

        setSpecs = [] if oaiHeader is None else xpath(oaiHeader, 'oai:setSpec/text()')
        sets = set((str(s), str(s)) for s in setSpecs)

        namespace = record.nsmap.get(record.prefix or None, '')
        schemaLocation = record.attrib.get(expandNs('xsi:schemaLocation'), '')
        ns2xsd = schemaLocation.split()
        schema = dict(zip(ns2xsd[::2],ns2xsd[1::2])).get(namespace, '')
        schema, namespace = self._magicSchemaNamespace(record.prefix, partname, schema, namespace)
        metadataFormats=[(partname, schema, namespace)]
        self.call.addOaiRecord(identifier=identifier, sets=sets, metadataFormats=metadataFormats)
        return
        yield

    def _magicSchemaNamespace(self, prefix, name, schema, namespace):
        searchForPrefix = prefix or name
        for oldprefix, oldschema, oldnamespace in self.call.getAllMetadataFormats():
            if searchForPrefix == oldprefix:
                return schema or oldschema, namespace or oldnamespace
        return schema, namespace
