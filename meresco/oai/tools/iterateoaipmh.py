## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2013-2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2013-2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from lxml.etree import parse, tostring
from urllib import urlencode, urlopen
from meresco.xml import xpathFirst, xpath


def iterateOaiPmh(*args, **kwargs):
    oaiListRequest = OaiListRequest(*args, **kwargs)
    while oaiListRequest:
        oaiBatch = oaiListRequest.retrieveBatch()
        for item in oaiBatch.items:
            yield item
        oaiListRequest = oaiBatch.nextRequest()


class OaiListRequest(object):
    def __init__(self, baseurl, metadataPrefix=None, set=None, from_=None, until=None, resumptionToken=None, verb='ListRecords'):
        if metadataPrefix is None and resumptionToken is None:
            raise ValueError('One of metadataPrefix or resumptionToken is required.')
        if verb not in VERB_XPATHS.keys():
            raise ValueError('Expected verb to be one of: ' + repr(list(VERB_XPATHS.keys())))
        self.baseurl = baseurl
        self.metadataPrefix = metadataPrefix
        self.set = set
        self.from_ = from_
        self.until = until
        self.resumptionToken = resumptionToken
        self.verb = verb

    def buildUrl(self):
        parameters = {}
        if self.resumptionToken:
            parameters['resumptionToken'] = self.resumptionToken
        else:
            parameters['metadataPrefix'] = self.metadataPrefix
            if self.set:
                parameters['set'] = self.set
            if self.from_:
                parameters['from'] = self.from_
            if self.until:
                parameters['until'] = self.until
        return self.baseurl + '?' + urlencode([('verb', self.verb)] + sorted(parameters.items()))

    def retrieveBatch(self):
        url = self.buildUrl()
        return OaiBatch(self, parse(self._urlopen(url)))

    def _nextRequest(self, resumptionToken):
        return OaiListRequest(baseurl=self.baseurl, verb=self.verb, resumptionToken=resumptionToken)

    def _urlopen(self, url):
        return urlopen(url)

    def __repr__(self):
        return _repr(self)


class OaiBatch(object):
    def __init__(self, request, response):
        self.request = request
        self.response = response
        self.items = []
        verbNode = xpathFirst(self.response, "/oai:OAI-PMH/oai:%s" % self.request.verb)
        if verbNode is None:
            errorNode = xpathFirst(self.response, "/oai:OAI-PMH/oai:error")
            if errorNode is None:
                raise ValueError('Not a OAI-PMH %s response from %s. Got:\n%s' % (self.request.verb, self.request.buildUrl(), tostring(response, pretty_print=True)))
            errorCode = xpathFirst(errorNode, '@code')
            msg = xpathFirst(errorNode, 'text()')
            raise ValueError('Got OAI-PMH response with error (%s): %s' % (errorCode, msg))
        itemXPath, headerXPath = VERB_XPATHS[self.request.verb]
        for item in xpath(verbNode, itemXPath):
            record = item if self.request.verb == 'ListRecords' else None
            self.items.append(OaiItem(record, header=xpathFirst(item, headerXPath), oaiBatch=self))

    @property
    def resumptionToken(self):
        return xpathFirst(self.response, "//oai:resumptionToken/text()")

    @property
    def responseDate(self):
        return xpathFirst(self.response, '/oai:OAI-PMH/oai:responseDate/text()')

    @property
    def completeListSize(self):
        return xpathFirst(self.response, '//oai:resumptionToken/@completeListSize')

    def nextRequest(self):
        resumptionToken = self.resumptionToken
        if resumptionToken:
            return self.request._nextRequest(resumptionToken=resumptionToken)


class OaiItem(object):
    def __init__(self, record, header, oaiBatch=None):
        self.record = record
        self.header = header
        self.oaiBatch = oaiBatch
        self.identifier = xpathFirst(header, 'oai:identifier/text()')
        self.datestamp = xpathFirst(header, 'oai:datestamp/text()')
        self.deleted = (xpathFirst(header, '@status') == 'deleted')
        self.setSpecs = xpath(header, 'oai:setSpec/text()')
        self.metadata = None
        if not record is None:
            self.metadata = xpathFirst(record, 'oai:metadata/*')

    def __repr__(self):
        return _repr(self)

def _repr(self):
    return "%s(%s)" % (self.__class__.__name__, ", ".join(
        "%s=%s" % (k, repr(v))
        for k, v in self.__dict__.items()))


VERB_XPATHS = {
    'ListRecords': ('oai:record', 'oai:header'),
    'ListIdentifiers': ('oai:header', '.')
}
