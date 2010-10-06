## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
#    Copyright (C) 2007-2009 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
#    Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
#    Copyright (C) 2009 Tilburg University http://www.uvt.nl
#    Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
#
#    This file is part of Meresco Oai.
#
#    Meresco Oai is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    Meresco Oai is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with Meresco Oai; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from meresco.core.observable import Observable

from resumptiontoken import resumptionTokenFromString, ResumptionToken
from oaitool import ISO8601, ISO8601Exception
from oairecordverb import OaiRecordVerb
from itertools import chain, islice
from oaiutils import checkNoRepeatedArguments, checkNoMoreArguments, checkArgument, OaiBadArgumentException, oaiFooter, oaiHeader, oaiRequestArgs, OaiException
from oaierror import oaiError
from xml.sax.saxutils import escape as xmlEscape
from meresco.core.generatorutils import decorate

BATCH_SIZE = 200

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
    def __init__(self, batchSize=BATCH_SIZE):
        self._supportedVerbs = ['ListIdentifiers', 'ListRecords']
        Observable.__init__(self)
        self._batchSize = batchSize

    def listRecords(self, arguments, **httpkwargs):
        yield self.doProcess(arguments, **httpkwargs)

    def listIdentifiers(self, arguments, **httpkwargs):
        yield self.doProcess(arguments, **httpkwargs)

    def doProcess(self, arguments, **httpkwargs):
        self._verb = arguments.get('verb', [None])[0]
        if not self._verb in self._supportedVerbs:
            return

        try:
            validatedArguments = self._validateArguments(arguments)
        except OaiBadArgumentException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, **httpkwargs)
            return

        while True:
            try:
                results = self.preProcess(validatedArguments, **httpkwargs)
                break
            except OaiException, e:
                if e.statusCode == "noRecordsMatch" and validatedArguments.get("x-wait", 'False') == 'True':
                    yield self.all.suspend()
                else:
                    yield oaiError(e.statusCode, e.additionalMessage, arguments, **httpkwargs)
                    return
        
        yield oaiHeader()
        yield oaiRequestArgs(arguments, **httpkwargs)
        yield '<%s>' % self._verb
        yield self.process(results, validatedArguments, **httpkwargs)
        yield '</%s>' % self._verb

        yield oaiFooter()

    def _validateArguments(self, arguments):
        arguments = dict(arguments)
        validatedArguments = {}
        checkNoRepeatedArguments(arguments)
        arguments.pop('verb')
        checkArgument(arguments, 'x-wait', validatedArguments)
        if checkArgument(arguments, 'resumptionToken', validatedArguments):
            checkNoMoreArguments(arguments, '"resumptionToken" argument may only be used exclusively.')
        else:
            if not checkArgument(arguments, 'metadataPrefix', validatedArguments):
                raise OaiBadArgumentException('Missing argument(s) "resumptionToken" or "metadataPrefix".')
            for name in ['from', 'until', 'set']:
                checkArgument(arguments, name, validatedArguments)
            checkNoMoreArguments(arguments, 'Argument(s) ' + ', '.join('"%s"' % t for t in arguments.keys()) + ' is/are illegal.')
        return validatedArguments

    def preProcess(self, validatedArguments, **httpkwargs):
        if validatedArguments.get('resumptionToken', None):
            token = resumptionTokenFromString(validatedArguments['resumptionToken'])
            if not token:
                raise OaiException("badResumptionToken")
            self._continueAfter = token._continueAfter
            _from = token._from
            _until = token._until
            _set = token._set
            _metadataPrefix = token._metadataPrefix
        else:
            self._continueAfter = '0'
            _from = validatedArguments.get('from', None)
            _until = validatedArguments.get('until', None)
            _set = validatedArguments.get('set', None)
            _metadataPrefix = validatedArguments.get('metadataPrefix', None)

            try:
                _from = _from and ISO8601(_from)
                _until = _until and ISO8601(_until) 
                if _from and _until:
                    if _from.isShort() != _until.isShort():
                        raise OaiBadArgumentException('from and/or until arguments must match in length')
                    if str(_from) > str(_until):
                        raise OaiBadArgumentException('from argument must be smaller than until argument')
                _from = _from and _from.floor()
                _until = _until and _until.ceil()
            except ISO8601Exception, e:
                raise OaiBadArgumentException('from and/or until arguments are faulty')

        if not _metadataPrefix in set(self.any.getAllPrefixes()):
            raise OaiException('cannotDisseminateFormat')

        validatedArguments['from'] = _from
        validatedArguments['until'] = _until
        validatedArguments['set'] = _set
        validatedArguments['metadataPrefix'] = _metadataPrefix
        result = self.any.oaiSelect(
            sets = [_set] if _set else None,
            prefix = _metadataPrefix,
            continueAfter  = self._continueAfter,
            oaiFrom = _from,
            oaiUntil = _until)
        try:
            firstRecord = result.next()
            return chain(iter([firstRecord]), result)
        except StopIteration:
            raise OaiException('noRecordsMatch')

    def process(self, results, validatedArguments, **httpkwargs):
        for id in islice(results, 0, self._batchSize):
            yield self.oaiRecord(validatedArguments, id, self._verb == "ListRecords")

        try:
            if not 'x-wait' in validatedArguments:
                results.next()
            yield '<resumptionToken>%s</resumptionToken>' % ResumptionToken(
                validatedArguments['metadataPrefix'],
                self.any.getUnique(id),
                validatedArguments['from'],
                validatedArguments['until'],
                validatedArguments['set'])
            return
        except StopIteration:
            pass

        if 'resumptionToken' in validatedArguments:
            yield '<resumptionToken/>'

    def oaiRecord(self, validatedArguments, recordId, writeBody=True):
        isDeletedStr = self.any.isDeleted(recordId) and ' status="deleted"' or ''
        datestamp = self.any.getDatestamp(recordId)
        setSpecs = self._getSetSpecs(recordId)
        if writeBody:
            yield '<record>'

        yield """<header%s>
            <identifier>%s</identifier>
            <datestamp>%s</datestamp>
            %s
        </header>""" % (isDeletedStr, xmlEscape(recordId.encode('utf-8')), datestamp, setSpecs)

        if writeBody and not isDeletedStr:
            yield '<metadata>'
            yield self.all.yieldRecord(recordId, validatedArguments['metadataPrefix'])
            yield '</metadata>'

        if writeBody:
            provenance = self.all.provenance(recordId)
            for line in decorate('<about>', provenance, '</about>'):
                yield line

        if writeBody:
            yield '</record>'

    def _getSetSpecs(self, recordId):
        sets = self.any.getSets(recordId)
        if sets:
            return ''.join('<setSpec>%s</setSpec>' % xmlEscape(setSpec) for setSpec in sets)
        return ''

