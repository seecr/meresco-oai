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
# Copyright (C) 2012-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from weightless.core import NoneOfTheObserversRespond

from resumptiontoken import resumptionTokenFromString, ResumptionToken
from oaitool import ISO8601, ISO8601Exception
from oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, checkBooleanArgument, OaiBadArgumentException, oaiFooter, oaiHeader, oaiRequestArgs, OaiException, zuluTime
from oaierror import oaiError
from oaijazz import ForcedResumeException, DEFAULT_BATCH_SIZE
from uuid import uuid4
import sys
from time import time
from traceback import print_exc


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
        yield self._list(arguments, **httpkwargs)

    def listIdentifiers(self, arguments, **httpkwargs):
        yield self._list(arguments, **httpkwargs)

    def _list(self, requestArguments, **httpkwargs):
        verb = requestArguments.get('verb', [None])[0]
        if not verb in self._supportedVerbs:
            return

        try:
            selectArguments = self._validateAndParseArguments(requestArguments)
        except (OaiBadArgumentException, OaiException), e:
            yield oaiError(e.statusCode, e.additionalMessage, requestArguments, **httpkwargs)
            return

        while True:
            try:
                responseDate = zuluTime()
                result = self._oaiSelect(**selectArguments)
                break
            except OaiException, e:
                if selectArguments['x-wait'] and \
                        e.statusCode in ["noRecordsMatch", "cannotDisseminateFormat"]:
                    try:
                        yield self._suspend(selectArguments, httpkwargs)
                    except ForcedResumeException, e:
                        yield successNoContentPlainText + str(e)
                        return
                    except Exception, e:
                        print_exc()
                        yield serverErrorPlainText + str(e)
                        raise e
                else:
                    yield oaiError(e.statusCode, e.additionalMessage, requestArguments, **httpkwargs)
                    return

        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(requestArguments, **httpkwargs)
        yield '<%s>' % verb
        yield self._renderRecords(verb, result, selectArguments)
        yield self._renderResumptionToken(result, selectArguments)
        yield '</%s>' % verb

        yield oaiFooter()

    def _validateAndParseArguments(self, arguments):
        selectArguments = {}
        arguments = dict(arguments)
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        selectArguments['x-wait'] = self._supportXWait and checkBooleanArgument(arguments, 'x-wait', selectArguments)
        checkBooleanArgument(arguments, 'x-count', selectArguments)
        if checkArgument(arguments, 'resumptionToken', selectArguments):
            if len(arguments) > 0:
                raise OaiBadArgumentException('"resumptionToken" argument may only be used exclusively.')
        else:
            if not checkArgument(arguments, 'metadataPrefix', selectArguments):
                raise OaiBadArgumentException('Missing argument(s) "resumptionToken" or "metadataPrefix".')
            for name in ['from', 'until', 'set']:
                checkArgument(arguments, name, selectArguments)
            checkNoMoreArguments(arguments)

        resumptionToken = selectArguments.get('resumptionToken')
        if not resumptionToken is None:
            token = resumptionTokenFromString(resumptionToken)
            if not token:
                raise OaiException("badResumptionToken")
            continueAfter = token.continueAfter
            metadataPrefix = token.metadataPrefix
            from_ = token.from_
            until = token.until
            set_ = token.set_
        else:
            continueAfter = '0'
            metadataPrefix = selectArguments.pop('metadataPrefix')
            from_ = selectArguments.pop('from', None)
            until = selectArguments.pop('until', None)
            set_ = selectArguments.pop('set', None)
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

        selectArguments['continueAfter'] = continueAfter
        selectArguments['from_'] = from_
        selectArguments['until'] = until
        selectArguments['set_'] = set_
        selectArguments['metadataPrefix'] = metadataPrefix
        return selectArguments

    def _oaiSelect(self, metadataPrefix, continueAfter, from_, until, set_, **kwargs):
        if not metadataPrefix in set(self.call.getAllPrefixes()):
            raise OaiException('cannotDisseminateFormat')
        result = self.call.oaiSelect(
            sets=[set_] if set_ else None,
            prefix=metadataPrefix,
            continueAfter=continueAfter,
            oaiFrom=from_,
            oaiUntil=until,
            batchSize=self._batchSize,
            shouldCountHits=kwargs['x-count'])
        if result.numberOfRecordsInBatch == 0:
            raise OaiException('noRecordsMatch')
        return result

    def _suspend(self, selectArguments, httpkwargs):
        clientId = httpkwargs['Headers'].get('X-Meresco-Oai-Client-Identifier')
        if clientId is None:
            clientId = str(uuid4())
            sys.stderr.write("X-Meresco-Oai-Client-Identifier not found in HTTP Headers. Generated a uuid for OAI client from %s\n" % httpkwargs['Client'][0])
            sys.stderr.flush()
        yield self.any.suspend(
            clientIdentifier=clientId,
            metadataPrefix=selectArguments['metadataPrefix'],
            set=selectArguments.get('set_'))

    def _renderRecords(self, verb, result, selectArguments):
        records = list(result.records)
        metadataPrefix = selectArguments['metadataPrefix']
        fetchedRecords = None
        try:
            t0 = time()
            fetchedRecords = dict(
                self.call.getMultipleData(
                    name=metadataPrefix,
                    identifiers=(r.identifier for r in records if not r.isDeleted),
                    ignoreMissing=True
                )
            )
            deltaT = time() - t0
            if deltaT > 10.0:
                sys.stderr.write("SequentialStorage.getMultipleData for {0} records took {1:.1f} seconds.\n".format(
                    result.numberOfRecordsInBatch, deltaT
                ))
                sys.stderr.flush()
        except NoneOfTheObserversRespond:
            pass
        message = "oaiRecord" if verb == 'ListRecords' else "oaiRecordHeader"
        for record in records:
            yield self.all.unknown(message, record=record, metadataPrefix=selectArguments['metadataPrefix'], fetchedRecords=fetchedRecords)

    def _renderResumptionToken(self, result, selectArguments):
        if result.moreRecordsAvailable or selectArguments['x-wait']:
            if selectArguments['x-count']:
                yield '<resumptionToken recordsRemaining="%s">' % result.recordsRemaining
            else:
                yield '<resumptionToken>'
            yield '%s</resumptionToken>' % ResumptionToken(
                    metadataPrefix=selectArguments['metadataPrefix'],
                    continueAfter=result.continueAfter,
                    from_=selectArguments['from_'],
                    until=selectArguments['until'],
                    set_=selectArguments['set_'])
        elif 'resumptionToken' in selectArguments:
            yield '<resumptionToken/>'

MAX_RATIO = 1.1
