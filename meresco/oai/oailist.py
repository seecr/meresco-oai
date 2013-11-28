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
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.components.http.utils import serverErrorPlainText, successNoContentPlainText
from meresco.core.observable import Observable

from resumptiontoken import resumptionTokenFromString, ResumptionToken
from oaitool import ISO8601, ISO8601Exception
from itertools import chain, islice
from oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, OaiBadArgumentException, oaiFooter, oaiHeader, oaiRequestArgs, OaiException, zuluTime
from oaierror import oaiError
from oaijazz import ForcedResumeException, DEFAULT_BATCH_SIZE
from uuid import uuid4
import sys


class OaiList(Observable):
    """4.3 ListIdentifiers
Summary and Usage Notes

This verb is an abbreviated form of ListRecords, retrieving only headers rather than records. Optional arguments permit selective harvesting of headers based on set membership and/or datestamp. Depending on the repository's support for deletions, a returned header may have a status attribute of "deleted" if a record matching the arguments specified in the request has been deleted.
Arguments

    * from an optional argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.
    * until an optional argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.
    * metadataPrefix a required argument, which specifies that headers should be returned only if the metadata format matching the supplied metadataPrefix is available or, depending on the repository's support for deletions, has been deleted. The metadata formats supported by a repository and for a particular item can be retrieved using the ListMetadataFormats request.
    * set an optional argument with a setSpec value , which specifies set criteria for selective harvesting.
    * resumptionToken an exclusive argument with a value that is the flow control token returned by a previous ListIdentifiers request that issued an incomplete list.

Error and Exception Conditions

    * badArgument - The request includes illegal arguments or is missing required arguments.
    * badResumptionToken - The value of the resumptionToken argument is invalid or expired.
    * cannotDisseminateFormat - The value of the metadataPrefix argument is not supported by the repository.
    * noRecordsMatch- The combination of the values of the from, until, and set arguments results in an empty list.
    * noSetHierarchy - The repository does not support sets.

4.5 ListRecords
Summary and Usage Notes

This verb is used to harvest records from a repository. Optional arguments permit selective harvesting of records based on set membership and/or datestamp. Depending on the repository's support for deletions, a returned header may have a status attribute of "deleted" if a record matching the arguments specified in the request has been deleted. No metadata will be present for records with deleted status.
Arguments

    * from an optional argument with a UTCdatetime value, which specifies a lower bound for datestamp-based selective harvesting.
    * until an optional argument with a UTCdatetime value, which specifies a upper bound for datestamp-based selective harvesting.
    * set an optional argument with a setSpec value , which specifies set criteria for selective harvesting.
    * resumptionToken an exclusive argument with a value that is the flow control token returned by a previous ListRecords request that issued an incomplete list.
    * metadataPrefix a required argument (unless the exclusive argument resumptionToken is used) that specifies the metadataPrefix of the format that should be included in the metadata part of the returned records. Records should be included only for items from which the metadata format
      matching the metadataPrefix can be disseminated. The metadata formats supported by a repository and for a particular item can be retrieved using the ListMetadataFormats request.

Error and Exception Conditions

    * badArgument - The request includes illegal arguments or is missing required arguments.
    * badResumptionToken - The value of the resumptionToken argument is invalid or expired.
    * cannotDisseminateFormat - The value of the metadataPrefix argument is not supported by the repository.
    * noRecordsMatch - The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.
    * noSetHierarchy - The repository does not support sets.
"""

    def __init__(self, batchSize=DEFAULT_BATCH_SIZE, supportXWait=False):
        self._supportedVerbs = ['ListIdentifiers', 'ListRecords']
        Observable.__init__(self)
        self._batchSize = batchSize
        self._supportXWait = supportXWait

    def listRecords(self, arguments, **httpkwargs):
        yield self._doProcess(arguments, **httpkwargs)

    def listIdentifiers(self, arguments, **httpkwargs):
        yield self._doProcess(arguments, **httpkwargs)

    def _doProcess(self, arguments, **httpkwargs):
        verb = arguments.get('verb', [None])[0]
        if not verb in self._supportedVerbs:
            return

        try:
            validatedArguments = self._validateArguments(arguments)
        except OaiBadArgumentException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, **httpkwargs)
            return

        while True:
            try:
                responseDate = zuluTime()
                records = self._preProcess(validatedArguments, **httpkwargs)
                break
            except OaiException, e:
                if validatedArguments.get("x-wait", 'False') == 'True' and \
                        e.statusCode in ["noRecordsMatch", "cannotDisseminateFormat"]:
                    clientId = httpkwargs['Headers'].get('X-Meresco-Oai-Client-Identifier')
                    if clientId is None:
                        clientId = str(uuid4())
                        sys.stderr.write("X-Meresco-Oai-Client-Identifier not found in HTTP Headers. Generated a uuid for OAI client from %s\n" % httpkwargs['Client'][0])
                        sys.stderr.flush()
                    try:
                        yield self.any.suspend(clientIdentifier=clientId)
                    except ForcedResumeException, e:
                        yield successNoContentPlainText + str(e)
                        return
                    except Exception, e:
                        yield serverErrorPlainText + str(e)
                        raise e
                else:
                    yield oaiError(e.statusCode, e.additionalMessage, arguments, **httpkwargs)
                    return

        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(arguments, **httpkwargs)
        yield '<%s>' % verb
        yield self._process(verb, records, validatedArguments, **httpkwargs)
        yield '</%s>' % verb

        yield oaiFooter()

    def _validateArguments(self, arguments):
        arguments = dict(arguments)
        validatedArguments = {}
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        if self._supportXWait:
            checkArgument(arguments, 'x-wait', validatedArguments)
            if validatedArguments.get('x-wait', None) not in ['True', None]:
                raise OaiBadArgumentException("The argument 'x-wait' only supports 'True' as valid value.")
        checkArgument(arguments, 'x-count', validatedArguments)
        if validatedArguments.get('x-count', None) not in ['True', None]:
            raise OaiBadArgumentException("The argument 'x-count' only supports 'True' as valid value.")
        if checkArgument(arguments, 'resumptionToken', validatedArguments):
            if len(arguments) > 0:
                raise OaiBadArgumentException('"resumptionToken" argument may only be used exclusively.')
        else:
            if not checkArgument(arguments, 'metadataPrefix', validatedArguments):
                raise OaiBadArgumentException('Missing argument(s) "resumptionToken" or "metadataPrefix".')
            for name in ['from', 'until', 'set']:
                checkArgument(arguments, name, validatedArguments)
            checkNoMoreArguments(arguments)
        return validatedArguments

    def _preProcess(self, validatedArguments, **httpkwargs):
        if validatedArguments.get('resumptionToken', None):
            token = resumptionTokenFromString(validatedArguments['resumptionToken'])
            if not token:
                raise OaiException("badResumptionToken")
            continueAfter = token.continueAfter
            from_ = token.from_
            until = token.until
            set_ = token.set_
            metadataPrefix = token.metadataPrefix
        else:
            continueAfter = '0'
            from_ = validatedArguments.get('from', None)
            until = validatedArguments.get('until', None)
            set_ = validatedArguments.get('set', None)
            metadataPrefix = validatedArguments.get('metadataPrefix', None)

            try:
                from_ = from_ and ISO8601(from_)
                until = until and ISO8601(until)
                if from_ and until:
                    if from_.isShort() != until.isShort():
                        raise OaiBadArgumentException('From and/or until arguments must match in length.')
                    if str(from_) > str(until):
                        raise OaiBadArgumentException('From argument must be smaller than until argument.')
                from_ = from_ and from_.floor()
                until = until and until.ceil()
            except ISO8601Exception:
                raise OaiBadArgumentException('From and/or until arguments are faulty.')

        if not metadataPrefix in set(self.call.getAllPrefixes()):
            raise OaiException('cannotDisseminateFormat')

        validatedArguments['from'] = from_
        validatedArguments['until'] = until
        validatedArguments['set'] = set_
        validatedArguments['metadataPrefix'] = metadataPrefix
        records = self.call.oaiSelect(
            sets=[set_] if set_ else None,
            prefix=metadataPrefix,
            continueAfter=continueAfter,
            oaiFrom=from_,
            oaiUntil=until,
            batchSize=self._batchSize + 1,# +1 so we can see if there is more for a resumptionToken
            shouldCountHits='x-count' in validatedArguments)
        try:
            firstRecord = records.next()
            return chain(iter([firstRecord]), records)
        except StopIteration:
            raise OaiException('noRecordsMatch')

    def _process(self, verb, records, validatedArguments, **httpkwargs):
        message = "oaiRecord" if verb == 'ListRecords' else "oaiRecordHeader"
        for record in islice(records, 0, self._batchSize):
            yield self.all.unknown(message, record=record, metadataPrefix=validatedArguments['metadataPrefix'])

        try:
            if not 'x-wait' in validatedArguments:
                records.next()
            if 'x-count' in validatedArguments:
                yield '<resumptionToken recordsRemaining="%s">' % record.recordsRemaining
            else:
                yield '<resumptionToken>'
            yield '%s</resumptionToken>' % ResumptionToken(
                    metadataPrefix=validatedArguments['metadataPrefix'],
                    continueAfter=record.stamp,
                    from_=validatedArguments['from'],
                    until=validatedArguments['until'],
                    set_=validatedArguments['set'])
        except StopIteration:
            if 'resumptionToken' in validatedArguments:
                yield '<resumptionToken/>'

