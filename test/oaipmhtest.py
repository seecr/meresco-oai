## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2010-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010-2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2011 Nederlands Instituut voor Beeld en Geluid http://instituut.beeldengeluid.nl
# Copyright (C) 2011-2016, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015-2016 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from seecr.test import SeecrTestCase, CallTrace
from oaischema import assertValidOai

from io import BytesIO
from lxml.etree import parse, XML
from os.path import join
from socket import gethostname
from time import sleep
from urllib.parse import urlencode

from meresco.core import Observable
from meresco.components import lxmltostring, RetrieveToGetDataAdapter
from meresco.components.http.utils import parseResponse, CRLF
from meresco.sequentialstore import MultiSequentialStorage
from meresco.xml import namespaces

from meresco.oai import OaiPmh, OaiJazz, OaiBranding, SuspendRegister
from weightless.core import be, compose, asBytes


namespaces = namespaces.copyUpdate({
    'toolkit': 'http://oai.dlib.vt.edu/OAI/metadata/toolkit',
    'branding': 'http://www.openarchives.org/OAI/2.0/branding/',
    'identifier': 'http://www.openarchives.org/OAI/2.0/oai-identifier',
})
xpath = namespaces.xpath
xpathFirst = namespaces.xpathFirst

BATCHSIZE = 10
HOSTNAME = gethostname()


class _OaiPmhTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.jazz = jazz = OaiJazz(join(self.tempdir, 'jazz'))
        self.storage = MultiSequentialStorage(join(self.tempdir, 'sequential-store'))
        self.oaipmh = self.getOaiPmh()
        self.root = be((Observable(),
            (self.oaipmh,
                (jazz, ),
                (RetrieveToGetDataAdapter(),
                    (self.storage,)
                )
            )
        ))
        for i in range(20):
            identifier = recordId = 'record:id:%02d' % i
            metadataFormats = [('oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://www.openarchives.org/OAI/2.0/oai_dc/')]
            if i >= 10:
                metadataFormats.append(('prefix2', 'http://example.org/prefix2/?format=xsd&prefix=2','http://example.org/prefix2/'))
            sets = []
            if i >= 5:
                sets.append(('setSpec%s' % ((i//5)*5), ('' if ((i//5)*5) == 10 else 'setName')))  # empty string becomes 'set <setSpec>'.
            if 5 <= i < 10:
                sets.append(('hierarchical:set', 'hierarchical set'))
            if 10 <= i < 15:
                sets.append(('hierarchical', 'hierarchical toplevel only'))
            sleep(0.001) # avoid timestamps being equals on VMs

            setSpecs = []
            for spec, name in sets:
                setSpecs.append(spec)
                jazz.updateSet(setSpec=spec, setName=name)
            formats = []
            for prefix,schema,namespace in metadataFormats:
                formats.append(prefix)
                jazz.updateMetadataFormat(prefix=prefix, schema=schema, namespace=namespace)

            jazz.addOaiRecord(recordId, setSpecs=setSpecs, metadataPrefixes=formats)
            if i % 5 == 0:
                list(compose(jazz.delete(recordId)))

            self.storage.addData(
                identifier=identifier,
                name='oai_dc',
                data=b'<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier>%b</dc:identifier></oai_dc:dc>' % bytes(recordId, encoding="utf-8"))
            if i >= 10:
                self.storage.addData(
                    identifier=identifier,
                    name='prefix2',
                    data=b'<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:subject>%b</dc:subject></oai_dc:dc>' %  bytes(recordId, encoding="utf-8"))

    def tearDown(self):
        self.jazz.close()
        SeecrTestCase.tearDown(self)

    def _request(self, from_=None, path=None, xcount=None, validate=True, **arguments):
        httpMethod = getattr(self, 'httpMethod', 'GET')
        if from_:
            arguments['from'] = from_
        if xcount:
            arguments['x-count'] = xcount
        RequestURI = 'http://example.org/oai'
        queryString = urlencode(arguments, doseq=True)
        if httpMethod == 'GET':
            RequestURI += '?' + queryString
            Body = None
        else:
            Body = bytes(queryString, encoding="utf-8")
            arguments = {}
        header, body = parseResponse(asBytes(compose(self.root.all.handleRequest(
                RequestURI=RequestURI,
                Headers={},
                Body=Body,
                Client=('127.0.0.1', 1324),
                Method=httpMethod,
                port=9000,
                arguments=arguments,
                path='/oai' if path is None else path,
            ))))
        parsedBody = XML(body)
        if validate:
            assertValidOai(parsedBody)
        return header, parsedBody

    def testBugListRecordsReturnsDoubleValueOnNoRecordsMatch(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'], from_=['9999-01-01'])
        self.assertEqual(['noRecordsMatch'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), lxmltostring(body, pretty_print=True))

    def testBadPathIsEscaped(self):
        header, body = self._request(path='/oai&verb=Identify')
        self.assertEqual(['http://%s:9000/oai&verb=Identify' % HOSTNAME], xpath(body, '/oai:OAI-PMH/oai:request/text()'))

    def testListRecords(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['prefix2'])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEqual(10, len(records))
        self.assertEqual([self.prefix + 'record:id:11'], xpath(records[1], 'oai:header/oai:identifier/text()'))
        self.assertEqual(['record:id:11'], xpath(records[1], 'oai:metadata/oai_dc:dc/dc:subject/text()'), lxmltostring(records[1]))
        self.assertEqual(['hierarchical', 'setSpec10'], sorted(xpath(records[1], 'oai:header/oai:setSpec/text()')))
        deletedRecords = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record[oai:header/@status="deleted"]')
        self.assertEqual(2, len(deletedRecords))
        self.assertEqual([0,0], [len(xpath(r, 'oai:metadata')) for r in deletedRecords])
        self.assertEqual(['hierarchical', 'setSpec10'], sorted(xpath(deletedRecords[0], 'oai:header/oai:setSpec/text()')))

    def testListRecordsWithResumptionToken(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEqual(10, len(records))
        resumptionToken = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')[0]
        header, body = self._request(verb=['ListRecords'], resumptionToken=[resumptionToken])
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEqual(10, len(records))
        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/text()')))

    def testListRecordsWithXCount(self):
        header, body = self._request(verb=['ListRecords'], metadataPrefix=['oai_dc'], xcount=['True'], validate=False)
        records = xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:record')
        self.assertEqual(10, len(records))
        recordsRemaining = int(xpath(body, '/oai:OAI-PMH/oai:ListRecords/oai:resumptionToken/@recordsRemaining')[0])
        self.assertEqual(10, recordsRemaining)

    def testGetRecordNotAvailable(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=['doesNotExist'])

        error = xpath(body, '/oai:OAI-PMH/oai:error')[0]
        self.assertEqual('idDoesNotExist', error.attrib['code'])
        self.assertEqual('The value of the identifier argument is unknown or illegal in this repository.', error.text)

    def testGetRecord(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=[self.prefix + 'record:id:11'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        records = xpath(body, '/oai:OAI-PMH/oai:GetRecord/oai:record')
        self.assertEqual(1, len(records))
        self.assertEqual([self.prefix + 'record:id:11'], xpath(records[0], 'oai:header/oai:identifier/text()'))
        self.assertEqual(['record:id:11'], xpath(records[0], 'oai:metadata/oai_dc:dc/dc:identifier/text()'), lxmltostring(records[0]))
        self.assertEqual(['hierarchical', 'setSpec10'], sorted(xpath(records[0], 'oai:header/oai:setSpec/text()')))

    def testGetRecordDeleted(self):
        header, body = self._request(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=[self.prefix + 'record:id:10'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        records = xpath(body, '/oai:OAI-PMH/oai:GetRecord/oai:record')
        self.assertEqual(1, len(records))
        self.assertEqual([self.prefix + 'record:id:10'], xpath(records[0], 'oai:header/oai:identifier/text()'))
        self.assertEqual(0, len(xpath(records[0], 'oai:metadata')))
        self.assertEqual(['hierarchical', 'setSpec10'], sorted(xpath(records[0], 'oai:header/oai:setSpec/text()')))

    def testListAllMetadataFormats(self):
        header, body = self._request(verb=['ListMetadataFormats'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        formats = xpath(body, '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat')
        self.assertEqual(2, len(formats), lxmltostring(body, pretty_print=True))
        self.assertEqual(['oai_dc', 'prefix2'], [xpath(f, 'oai:metadataPrefix/text()')[0] for f in formats])
        self.assertEqual(['http://www.openarchives.org/OAI/2.0/oai_dc.xsd', 'http://example.org/prefix2/?format=xsd&prefix=2'], [xpath(f, 'oai:schema/text()')[0] for f in formats])
        self.assertEqual(['http://www.openarchives.org/OAI/2.0/oai_dc/', 'http://example.org/prefix2/'], [xpath(f, 'oai:metadataNamespace/text()')[0] for f in formats])

    def testListMetadataFormatsForIdentifier(self):
        header, body = self._request(verb=['ListMetadataFormats'], identifier=[self.prefix + 'record:id:01'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')), lxmltostring(body, pretty_print=True))
        formats = xpath(body, '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat')
        self.assertEqual(1, len(formats), lxmltostring(body, pretty_print=True))
        self.assertEqual(['oai_dc'], xpath(formats[0], 'oai:metadataPrefix/text()'))

    def testListMetadataFormatsForWrongIdentifier(self):
        header, body = self._request(verb=['ListMetadataFormats'], identifier=['does:not:exist'])

        self.assertEqual(['idDoesNotExist'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), lxmltostring(body, pretty_print=True))

    def testListAllSets(self):
        header, body = self._request(verb=['ListSets'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        setsNodes = xpath(body, '/oai:OAI-PMH/oai:ListSets/oai:set')
        sets = [(xpathFirst(n, 'oai:setSpec/text()'), xpathFirst(n, 'oai:setName/text()')) for n in setsNodes]
        self.assertEqual(set([
                ('setSpec5', 'setName'),
                ('setSpec10', None),
                ('setSpec15', 'setName'),
                ('hierarchical', 'hierarchical toplevel only'),
                ('hierarchical:set', 'hierarchical set'),
            ]),
            set(sets),
            lxmltostring(body, pretty_print=True)
        )

    def testListSetsWithoutSets(self):
        self.root = be((Observable(),
            (OaiPmh(repositoryName='Repository', adminEmail='admin@cq2.nl', batchSize=BATCHSIZE),
                (OaiJazz(join(self.tempdir, 'empty'),),)
            )
        ))

        header, body = self._request(verb=['ListSets'])

        self.assertEqual(['noSetHierarchy'], xpath(body, '/oai:OAI-PMH/oai:error/@code'), lxmltostring(body, pretty_print=True))

    def testIdentify(self):
        statusAndHeaders, body = self._request(verb=['Identify'])
       
        headers = statusAndHeaders['Headers']
        self.assertEqual("text/xml; charset=utf-8", headers['Content-Type'])
        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        self.assertEqual(['http://%s:9000/oai' % HOSTNAME], xpath(body, '/oai:OAI-PMH/oai:request/text()'))
        identify = xpath(body, '/oai:OAI-PMH/oai:Identify')[0]
        self.assertEqual(['The Repository Name'], xpath(identify, 'oai:repositoryName/text()'))
        self.assertEqual(['admin@meresco.org'], xpath(identify, 'oai:adminEmail/text()'))
        self.assertEqual(['YYYY-MM-DDThh:mm:ssZ'], xpath(identify, 'oai:granularity/text()'))
        self.assertEqual(['1970-01-01T00:00:00Z'], xpath(identify, 'oai:earliestDatestamp/text()'))
        self.assertEqual(['persistent'], xpath(identify, 'oai:deletedRecord/text()'))

        descriptions = xpath(body, '/oai:OAI-PMH/oai:Identify/oai:description')
        if self.prefix:
            self.assertEqual(2, len(descriptions))
            self.assertEqual(['%s5324' % self.prefix], xpath(descriptions[0], 'identifier:oai-identifier/identifier:sampleIdentifier/text()'))
        else:
            self.assertEqual(1, len(descriptions))
        self.assertEqual(['Meresco'], xpath(descriptions[-1], 'toolkit:toolkit/toolkit:title/text()'))

    def testIdentifyWithTransientDeleteRecord(self):
        jazz = OaiJazz(join(self.tempdir, 'otherjazz'), persistentDelete=False)
        self.oaipmh = self.getOaiPmh()
        self.root = be((Observable(),
            (self.oaipmh,
                (jazz,),
            )
        ))
        header, body = self._request(verb=['Identify'])
        self.assertEqual(['transient'], xpath(body, '/oai:OAI-PMH/oai:Identify/oai:deletedRecord/text()'))

    def testIdentifyWithDescription(self):
        self.oaipmh.addObserver(OaiBranding('http://meresco.org/files/images/meresco-logo-small.png', 'http://www.meresco.org/', 'Meresco'))
        header, body = self._request(verb=['Identify'])

        self.assertEqual(0, len(xpath(body, '/oai:OAI-PMH/oai:error')))
        descriptions = xpath(body, '/oai:OAI-PMH/oai:Identify/oai:description')
        if self.prefix:
            self.assertEqual(3, len(descriptions))
            self.assertEqual(['%s5324' % self.prefix], xpath(descriptions[0], 'identifier:oai-identifier/identifier:sampleIdentifier/text()'))
        else:
            self.assertEqual(2, len(descriptions))
        self.assertEqual(['Meresco'], xpath(descriptions[-2], 'toolkit:toolkit/toolkit:title/text()'))
        self.assertEqual(['Meresco'], xpath(descriptions[-1], 'branding:branding/branding:collectionIcon/branding:title/text()'))

    def testWatermarking(self):
        class OaiWatermark(object):
            def oaiWatermark(this):
                yield "<!-- Watermarked by Seecr -->"
        self.oaipmh.addObserver(OaiWatermark())

        def assertWaterMarked(**oaiArgs):
            header, body = self._request(**oaiArgs)
            try:
                comment = xpath(body, "/oai:OAI-PMH/comment()")[0]
            except:
                print(lxmltostring(body, pretty_print=True))
                raise
            self.assertEqual(" Watermarked by Seecr ", comment.text)
        assertWaterMarked(verb=["Identify"])
        assertWaterMarked(verb=['ListRecords'], metadataPrefix=['prefix2'])
        assertWaterMarked(verb=['ListIdentifiers'], metadataPrefix=['prefix2'])
        assertWaterMarked(verb=['ListSets'])
        assertWaterMarked(verb=['ListMetadataFormats'])
        assertWaterMarked(verb=['GetRecord'], metadataPrefix=['oai_dc'], identifier=[self.prefix + 'record:id:11'])

    def testNoVerb(self):
        self.assertOaiError({}, additionalMessage='No "verb" argument found.', errorCode='badArgument')

    def testNVerbs(self):
        self.assertOaiError({'verb': ['ListRecords', 'Indentify']}, additionalMessage='Argument "verb" may not be repeated.', errorCode='badArgument')

    def testWrongVerb(self):
        self.assertOaiError({'verb': ['Nonsense']}, additionalMessage='Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.', errorCode='badVerb')

    def testIllegalIdentifyArguments(self):
        self.assertOaiError({'verb': ['Identify'], 'metadataPrefix': ['oai_dc']}, additionalMessage='Argument(s) "metadataPrefix" is/are illegal.', errorCode='badArgument')

    def testIllegalVerbListRecords(self):
        self.assertOaiError({'verb': ['listRecords'], 'metadataPrefix': ['oai_dc']}, additionalMessage='Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.', errorCode='badVerb')

    def testNoArgumentsListRecords(self):
        self.assertOaiError({'verb': ['ListRecords']}, additionalMessage='Missing argument(s) "resumptionToken" or "metadataPrefix"', errorCode='badArgument')

    def testTokenNotUsedExclusivelyListRecords(self):
        self.assertOaiError({'verb': ['ListRecords'], 'resumptionToken': ['aToken'], 'from': ['aDate']}, additionalMessage='"resumptionToken" argument may only be used exclusively.', errorCode='badArgument')

    def testNeitherTokenNorMetadataPrefixListRecords(self):
        self.assertOaiError({'verb': ['ListRecords'], 'from': ['aDate']}, additionalMessage='Missing argument(s) "resumptionToken" or "metadataPrefix"', errorCode='badArgument')

    def testNonsenseArgumentsListRecords(self):
        self.assertOaiError({'verb': ['ListRecords'], 'metadataPrefix': ['aDate'], 'nonsense': ['more nonsense'], 'bla': ['b']}, additionalMessage='Argument(s) "bla", "nonsense" is/are illegal.', errorCode='badArgument')

    def testDoubleArgumentsListRecords(self):
        self.assertOaiError({'verb':['ListRecords'], 'metadataPrefix': ['oai_dc', '2']}, additionalMessage='Argument "metadataPrefix" may not be repeated.', errorCode='badArgument')

    def testGetRecordNoArgumentsGetRecord(self):
        self.assertOaiError({'verb': ['GetRecord']}, additionalMessage='Missing argument(s) "identifier" and "metadataPrefix".', errorCode='badArgument')

    def testGetNoMetadataPrefixGetRecord(self):
        self.assertOaiError({'verb': ['GetRecord'], 'identifier': ['oai:ident']}, additionalMessage='Missing argument(s) "metadataPrefix".', errorCode='badArgument')

    def testGetNoIdentifierArgumentGetRecord(self):
        self.assertOaiError({'verb': ['GetRecord'], 'metadataPrefix': ['oai_dc']}, additionalMessage='Missing argument(s) "identifier".', errorCode='badArgument')

    def testNonsenseArgumentGetRecord(self):
        self.assertOaiError({'verb': ['GetRecord'], 'metadataPrefix': ['aPrefix'], 'identifier': ['anIdentifier'], 'nonsense': ['bla']}, additionalMessage='Argument(s) "nonsense" is/are illegal.', errorCode='badArgument')

    def testDoubleArgumentsGetRecord(self):
        self.assertOaiError({'verb':['GetRecord'], 'metadataPrefix': ['oai_dc'], 'identifier': ['oai:ident', '2']}, additionalMessage='Argument "identifier" may not be repeated.', errorCode='badArgument')

    def testResumptionTokensNotSupportedListSets(self):
        self.assertOaiError({'verb': ['ListSets'], 'resumptionToken': ['someResumptionToken']}, errorCode="badResumptionToken")

    def testNonsenseArgumentsListSets(self):
        self.assertOaiError({'verb': ['ListSets'], 'nonsense': ['aDate'], 'nonsense': ['more nonsense'], 'bla': ['b']}, additionalMessage='Argument(s) "bla", "nonsense" is/are illegal.', errorCode='badArgument')

    def testRottenTokenListRecords(self):
        self.assertOaiError({'verb': ['ListRecords'], 'resumptionToken': ['someResumptionToken']}, errorCode="badResumptionToken")

    def testEmptyResumptionTokenEdgeCase(self):
        self.assertOaiError({'verb': ['ListIdentifiers'], 'resumptionToken': ['']}, errorCode="badResumptionToken")

    def testIllegalArgumentsListMetadataFormats(self):
        self.assertOaiError({'verb': ['ListMetadataFormats'], 'somethingElse': ['illegal']}, errorCode='badArgument')

    def testObserverInit(self):
        observer = CallTrace()
        root = be((Observable(),
            (OaiPmh(repositoryName='Repository', adminEmail='admin@cq2.nl', batchSize=BATCHSIZE),
                (observer,),
            )
        ))
        list(compose(root.once.observer_init()))
        self.assertEqual(['observer_init'], [m.name for m in observer.calledMethods])

    def assertOaiError(self, arguments, errorCode, additionalMessage = ''):
        header, body = self._request(**arguments)

        self.assertEqual([errorCode], xpath(body, '/oai:OAI-PMH/oai:error/@code'), lxmltostring(body, pretty_print=True))
        errorText = xpath(body, '/oai:OAI-PMH/oai:error/text()')[0]
        self.assertTrue(additionalMessage in errorText, 'Expected "%s" in "%s"' % (additionalMessage, errorText))


class OaiPmhTest(_OaiPmhTest):
    def setUp(self):
        _OaiPmhTest.setUp(self)
        self.prefix=''

    def getOaiPmh(self):
        return OaiPmh(repositoryName='The Repository Name', adminEmail='admin@meresco.org', batchSize=BATCHSIZE)

    def testExceptionOnInvalidRepositoryIdentifier(self):
        try:
            OaiPmh(repositoryName="Repository", adminEmail="admin@example.org", repositoryIdentifier="repoId")
            self.fail()
        except ValueError as e:
            self.assertEqual("Invalid repository identifier: repoId", str(e))

        OaiPmh(repositoryName="Repository", adminEmail="admin@example.org", repositoryIdentifier="repoId.cq2.org")
        OaiPmh(repositoryName="Repository", adminEmail="admin@example.org", repositoryIdentifier="a.aa")


class OaiPmhWithIdentifierTest(_OaiPmhTest):
    def setUp(self):
        _OaiPmhTest.setUp(self)
        self.prefix='oai:www.example.org:'

    def getOaiPmh(self):
        return OaiPmh(repositoryName='The Repository Name', adminEmail='admin@meresco.org', batchSize=BATCHSIZE, repositoryIdentifier='www.example.org')

    def testBadRepositoryIdentifier(self):
        def oaipmh(repositoryIdentifier):
            return OaiPmh(repositoryName='The Repository Name', adminEmail='admin@meresco.org', batchSize=BATCHSIZE, repositoryIdentifier=repositoryIdentifier)
        self.assertRaises(ValueError, lambda: oaipmh('01234'))
        self.assertRaises(ValueError, lambda: oaipmh('a*'))
        self.assertRaises(ValueError, lambda: oaipmh('a34.0834'))


class HttpPostOaiPmhTest(OaiPmhTest):
    def setUp(self):
        OaiPmhTest.setUp(self)
        self.httpMethod = 'POST'
