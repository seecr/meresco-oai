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
from itertools import chain
from oaiutils import OaiBadArgumentException, doElementaryArgumentsValidation, oaiFooter, oaiHeader, oaiRequestArgs, OaiException
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
        self._argsDef = {
            'from': 'optional',
            'until': 'optional',
            'set': 'optional',
            'resumptionToken': 'exclusive',
            'metadataPrefix': 'required'}
        Observable.__init__(self)
        self._batchSize = batchSize

    def listRecords(self, arguments, **kwargs):
        #self.startProcessing(webrequest)
        #yield webrequest.generateResponse()
        self._verb = arguments.get('verb', [None])[0]
        if not self._verb in self._supportedVerbs:
            return

        try:
            validatedArguments = doElementaryArgumentsValidation(arguments, self._argsDef)
            for k,v in validatedArguments.items():
                setattr(self, "_" + k, v)
        except OaiBadArgumentException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, **kwargs)

        try:
            self.preProcess(arguments, **kwargs)
        except OaiException, e:
            yield oaiError(e.statusCode, e.additionalMessage, arguments, **kwargs)
            return

        yield oaiHeader()
        yield oaiRequestArgs(arguments, **kwargs)

        yield '<%s>' % self._verb
        yield self.process(arguments, **kwargs)
        yield '</%s>' % self._verb

        yield oaiFooter()

    def listIdentifiers(self, webrequest, **kwargs):
        self.startProcessing(webrequest)
        yield webrequest.generateResponse()

    def preProcess(self, arguments, **kwargs):
        if self._resumptionToken:
            token = resumptionTokenFromString(self._resumptionToken)
            if not token:
                raise OaiException("badResumptionToken")
            self._continueAfter = token._continueAfter
            self._metadataPrefix = token._metadataPrefix
            self._from = token._from
            self._until = token._until
            self._set = token._set
        else:
            self._continueAfter = '0'
            try:
                self._from = self._from and ISO8601(self._from)
                self._until  = self._until and ISO8601(self._until)
                if self._from and self._until:
                    if self._from.isShort() != self._until.isShort():
                        raise OaiBadArgumentException('from and/or until arguments must match in length')
                    if str(self._from) > str(self._until):
                        raise OaiBadArgumentException('from argument must be smaller than until argument')
                self._from = self._from and self._from.floor()
                self._until = self._until and self._until.ceil()
            except ISO8601Exception, e:
                raise OaiBadArgumentException('from and/or until arguments are faulty')

        if not self._metadataPrefix in set(self.any.getAllPrefixes()):
            raise OaiException('cannotDisseminateFormat')

        result = self.any.oaiSelect(
            sets=self._set and [self._set] or None,
            prefix=self._metadataPrefix,
            continueAfter=self._continueAfter,
            oaiFrom=self._from,
            oaiUntil=self._until,
            batchSize = self._batchSize)
        try:
            firstRecord = result.next()
            self._queryRecordIds = chain(iter([firstRecord]), result)
        except StopIteration:
            self._queryRecordIds = iter([])
            raise OaiException('noRecordsMatch')

    def process(self, arguments, **kwargs):
        for i, id in enumerate(self._queryRecordIds):
            if i == self._batchSize:
                yield('<resumptionToken>%s</resumptionToken>' % ResumptionToken(
                    self._metadataPrefix,
                    self.any.getUnique(prevId),
                    self._from,
                    self._until,
                    self._set))
            yield self.oaiRecord(id, self._verb == "ListRecords")
            prevId = id

        if self._resumptionToken:
            yield '<resumptionToken/>'

    def oaiRecord(self, recordId, writeBody=True):
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
            yield self.any.write(None, recordId, self._metadataPrefix)
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

