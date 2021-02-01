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
# Copyright (C) 2012, 2014, 2016, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
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

from meresco.core import Observable
from weightless.core import compose, Yield
from .oaiutils import oaiHeader, oaiFooter, oaiRequestArgs, zuluTime


class OaiError(Observable):
    def __init__(self, repository, **kwargs):
        Observable.__init__(self, **kwargs)
        self._repository = repository

    def all_unknown(self, message, **kwargs):
        result = compose(self.all.unknown(message, **kwargs))
        try:
            firstDataResult = next(result)
            while callable(firstDataResult) or firstDataResult is Yield:
                yield firstDataResult
                firstDataResult = next(result)
        except StopIteration:
            yield self._error(**kwargs)
            return
        yield firstDataResult
        for remainder in result:
            yield remainder

    def _error(self, arguments, **kwargs):
        verbs = arguments.get('verb', [None])
        if verbs[0] is None or verbs[0] == '':
            yield oaiError('badArgument', 'No "verb" argument found.', arguments=arguments, requestUrl=self._repository.requestUrl(**kwargs), **kwargs)
        elif len(verbs) > 1:
            yield oaiError('badArgument', 'More than one "verb" argument found.', arguments=arguments, requestUrl=self._repository.requestUrl(**kwargs), **kwargs)
        else:
            yield oaiError('badVerb', 'Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.', arguments=arguments, requestUrl=self._repository.requestUrl(**kwargs), **kwargs)


def oaiError(statusCode, additionalMessage, arguments, requestUrl, **httpkwargs):
    responseDate = zuluTime()
    space = additionalMessage and ' ' or ''
    message = ERROR_CODES[statusCode] + space + additionalMessage

    yield oaiHeader(responseDate=responseDate)
    if statusCode in ["badArgument", "badResumptionToken", "badVerb"]:
        """in these cases it is illegal to echo the arguments back; since the arguments are not valid in the first place the response will not validate either"""
        yield oaiRequestArgs({}, requestUrl=requestUrl, **httpkwargs)
    else:
        yield oaiRequestArgs(arguments, requestUrl=requestUrl, **httpkwargs)

    yield """<error code="%(statusCode)s">%(message)s</error>""" % locals()

    yield oaiFooter()

# http://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
ERROR_CODES = {
    'badArgument': 'The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax.',
    'badResumptionToken': 'The value of the resumptionToken argument is invalid or expired.',
    'badVerb': 'Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.',
    'cannotDisseminateFormat': 'The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository.',
    'idDoesNotExist': 'The value of the identifier argument is unknown or illegal in this repository.',
    'noRecordsMatch': 'The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.',
    'noMetadataFormats': 'There are no metadata formats available for the specified item.',
    'noSetHierarchy': 'The repository does not support sets.'
}
