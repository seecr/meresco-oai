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
# Copyright (C) 2012, 2015-2017, 2019 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from meresco.core.observable import Observable

from oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, oaiFooter, oaiHeader, oaiRequestArgs, OaiException, zuluTime, OaiBadArgumentException
from oaierror import oaiError


class OaiListSets(Observable):
    """4.6 ListSets
Summary and Usage Notes

This verb is used to retrieve the set structure of a repository, useful for selective harvesting.
Arguments

    * resumptionToken an exclusive argument with a value that is the flow control token returned by a previous ListSets request that issued an incomplete list.

Error and Exception Conditions

    * badArgument - The request includes illegal arguments or is missing required arguments.
    * badResumptionToken - The value of the resumptionToken argument is invalid or expired.
    * noSetHierarchy - The repository does not support sets."""

    def __init__(self, repository):
        Observable.__init__(self)
        self._repository = repository

    def listSets(self, arguments, **httpkwargs):
        responseDate = zuluTime()
        verb = arguments.get('verb', [None])[0]
        if not verb == 'ListSets':
            return

        try:
            validatedArguments = self._validateArguments(arguments)
            if 'resumptionToken' in validatedArguments:
                raise OaiException('badResumptionToken')

            sets = self.call.getAllSets(includeSetNames=True)
            if len(sets) == 0:
                raise OaiException('noSetHierarchy')
        except OaiException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, requestUrl=self._repository.requestUrl(**httpkwargs), **httpkwargs)
            return

        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(arguments, requestUrl=self._repository.requestUrl(**httpkwargs), **httpkwargs)
        yield '<%s>' % verb
        for setSpec, setName in sets:
            yield '<set>'
            yield '<setSpec>%s</setSpec>' % xmlEscape(setSpec)
            yield '<setName>%s</setName>' % xmlEscape(setName)
            yield '</set>'
        yield '</%s>' % verb
        yield oaiFooter()

    def _validateArguments(self, arguments):
        arguments = dict(arguments)
        validatedArguments = {}
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        if checkArgument(arguments, 'resumptionToken', validatedArguments):
            if len(arguments) > 0:
                raise OaiBadArgumentException('"resumptionToken" argument may only be used exclusively.')
        checkNoMoreArguments(arguments)
        return validatedArguments
