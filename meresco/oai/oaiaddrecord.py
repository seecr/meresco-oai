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
# Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2011 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.core import Transparent, asyncreturn
from lxml.etree import iselement

namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'xsi': "http://www.w3.org/2001/XMLSchema-instance",
}

class OaiAddRecord(Transparent):

    @asyncreturn
    def add(self, identifier, partname, lxmlNode):
        record = lxmlNode if iselement(lxmlNode) else lxmlNode.getroot()
        setSpecs = record.xpath('//oai:header/oai:setSpec/text()', namespaces=namespaces)
        sets = set((str(s), str(s)) for s in setSpecs)
        
        namespace = record.nsmap.get(record.prefix or None, '') 
        schemaLocations = record.xpath('@xsi:schemaLocation', namespaces=namespaces)
        ns2xsd = ''.join(schemaLocations).split()
        schema = dict(zip(ns2xsd[::2],ns2xsd[1::2])).get(namespace, '')
        schema, namespace = self._magicSchemaNamespace(record.prefix, partname, schema, namespace)
        metadataFormats=[(partname, schema, namespace)]

        self.do.addOaiRecord(identifier=identifier, sets=sets, metadataFormats=metadataFormats)

    def _magicSchemaNamespace(self, prefix, name, schema, namespace):
        searchForPrefix = prefix or name
        for oldprefix, oldschema, oldnamespace in self.call.getAllMetadataFormats():
            if searchForPrefix == oldprefix:
                return schema or oldschema, namespace or oldnamespace
        return schema, namespace

class OaiAddRecordWithDefaults(Transparent):
    def __init__(self, metadataFormats=None, sets=None):
        Transparent.__init__(self)
        self._metadataFormats = metadataFormats if metadataFormats else []
        self._sets = sets if sets else []

    @asyncreturn
    def add(self, identifier, partname, lxmlNode):
        self.do.addOaiRecord(identifier=identifier, sets=self._sets, metadataFormats=self._metadataFormats)

