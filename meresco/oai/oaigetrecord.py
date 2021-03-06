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
# Copyright (C) 2012-2014, 2016, 2018, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012, 2018, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2016 SURFmarket https://surf.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from meresco.core.observable import Observable

from .oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, OaiBadArgumentException, oaiFooter, oaiHeader, oaiRequestArgs, OaiException, zuluTime
from .oaierror import oaiError


class OaiGetRecord(Observable):
    """4.1 GetRecord
Summary and Usage Notes

This verb is used to retrieve an individual metadata record from a repository. Required arguments specify the identifier of the item from which the record is requested and the format of the metadata that should be included in the record. Depending on the level at which a repository tracks deletions, a header with a "deleted" value for the status attribute may be returned, in case the metadata format specified by the metadataPrefix is no longer available from the repository or from the specified item.

Arguments

    * identifier a required argument that specifies the unique identifier of the item in the repository from which the record must be disseminated.
    * metadataPrefix a required argument that specifies the metadataPrefix of the format that should be included in the metadata part of the returned record . A record should only be returned if the format specified by the metadataPrefix can be disseminated from the item identified by the value of the identifier argument. The metadata formats supported by a repository and for a particular record can be retrieved using the ListMetadataFormats request.

Error and Exception Conditions

    * badArgument - The request includes illegal arguments or is missing required arguments.
    * cannotDisseminateFormat - The value of the metadataPrefix argument is not supported by the item identified by the value of the identifier argument.
    * idDoesNotExist - The value of the identifier argument is unknown or illegal in this repository.
"""

    def __init__(self, repository, **kwargs):
        super(OaiGetRecord, self).__init__(**kwargs)
        self._repository = repository

    def getRecord(self, arguments, **httpkwargs):
        responseDate = zuluTime()
        verb = arguments.get('verb', [None])[0]
        if not verb == 'GetRecord':
            return

        try:
            validatedArguments = self._validateArguments(arguments)
            identifier = validatedArguments['identifier']
            if self._repository:
                identifier = self._repository.unprefixIdentifier(identifier)
            metadataPrefix = validatedArguments['metadataPrefix']
            record = self.call.getRecord(identifier=identifier, metadataPrefix=metadataPrefix)
            self._validateValues(record, metadataPrefix)
        except OaiException as e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, requestUrl=self._repository.requestUrl(**httpkwargs), **httpkwargs)
            return

        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(arguments, requestUrl=self._repository.requestUrl(**httpkwargs), **httpkwargs)
        yield '<%s>' % verb
        yield self.all.oaiRecord(record=record, metadataPrefix=metadataPrefix)
        yield '</%s>' % verb
        yield oaiFooter()

    def _validateValues(self, record, metadataPrefix):
        if not self.call.isKnownPrefix(metadataPrefix):
            raise OaiException('cannotDisseminateFormat')

        if not record:
            raise OaiException('idDoesNotExist')

        hasPartname = metadataPrefix in set(record.prefixes)
        if not record.isDeleted and not hasPartname:
            raise OaiException('cannotDisseminateFormat')

    def _validateArguments(self, arguments):
        arguments = dict(arguments)
        validatedArguments = {}
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        missing = []
        if not checkArgument(arguments, 'identifier', validatedArguments):
            missing.append('"identifier"')
        if not checkArgument(arguments, 'metadataPrefix', validatedArguments):
            missing.append('"metadataPrefix"')
        if missing:
            raise OaiBadArgumentException('Missing argument(s) ' + \
                " and ".join(missing) + ".")
        checkNoMoreArguments(arguments)
        return validatedArguments
