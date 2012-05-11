## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
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

from oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, oaiFooter, oaiHeader, oaiRequestArgs, OaiException, zuluTime
from oaierror import oaiError
from meresco.core import Observable
from xml.sax.saxutils import escape as xmlEscape

class OaiListMetadataFormats(Observable):
    """4.4 ListMetadataFormats
Summary and Usage Notes

This verb is used to retrieve the metadata formats available from a repository. An optional argument restricts the request to the formats available for a specific item.
Arguments

    * identifier an optional argument that specifies the unique identifier of the item for which available metadata formats are being requested. If this argument is omitted, then the response includes all metadata formats supported by this repository. Note that the fact that a metadata format is supported by a repository does not mean that it can be disseminated from all items in the repository.

Error and Exception Conditions

    * badArgument - The request includes illegal arguments or is missing required arguments.
    * idDoesNotExist - The value of the identifier argument is unknown or illegal in this repository.
    * noMetadataFormats - There are no metadata formats available for the specified item.
    """

    def __init__(self):
        Observable.__init__(self)

    def listMetadataFormats(self, arguments, **httpkwargs):
        responseDate = zuluTime()
        verb = arguments.get('verb', [None])[0]
        if not verb == 'ListMetadataFormats':
            return

        try:
            validatedArguments = self._validateArguments(arguments)
            metadataFormats = self.any.getAllMetadataFormats()
            if 'identifier' in validatedArguments:
                identifier = validatedArguments['identifier']
                if not self.any.getUnique(identifier):
                    raise OaiException('idDoesNotExist')
                prefixes = set(self.any.getPrefixes(identifier))
                metadataFormats = [(prefix, xsd, ns) for prefix, xsd, ns in metadataFormats if prefix in prefixes]
            displayedMetadataFormats = sorted(metadataFormats)
        except OaiException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, **httpkwargs)
            return

        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(arguments, **httpkwargs)
        yield '<%s>' % verb
        for metadataPrefix, schema, metadataNamespace in displayedMetadataFormats:
            yield '<metadataFormat>'
            yield '<metadataPrefix>%s</metadataPrefix>' % xmlEscape(metadataPrefix)
            yield '<schema>%s</schema>' % xmlEscape(schema)
            yield '<metadataNamespace>%s</metadataNamespace>' % xmlEscape(metadataNamespace)
            yield '</metadataFormat>'
        yield '</%s>' % verb
        yield oaiFooter()

    def _validateArguments(self, arguments):
        arguments = dict(arguments)
        validatedArguments = {}
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        checkArgument(arguments, 'identifier', validatedArguments)
        checkNoMoreArguments(arguments)
        return validatedArguments

