from lxml.etree import XML

from seecr.test import SeecrTestCase

from weightless.core import asString
from meresco.xml import xpath

from meresco.oai.oailistmetadataformats import OaiListMetadataFormats
from meresco.oai.oaijazz import OaiJazz
from meresco.oai.oairepository import OaiRepository


class OaiListMetadataFormatsTest(SeecrTestCase):
    def setUp(self):
        SeecrTestCase.setUp(self)
        self.httpkwargs = {
            'path': '/path/to/oai',
            'Headers':{'Host':'server'},
            'port':9000,
        }

    def testListMetadataFormats(self):
        listMetadataFormats = OaiListMetadataFormats()
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        listMetadataFormats.addObserver(oaijazz)
        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        oaijazz.addOaiRecord(identifier="id1", sets=(), metadataFormats=[('rdf', '', '')])
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc', 'rdf'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))

    def testListMetadataFormatsWithRepositoryIdentifier(self):
        listMetadataFormats = OaiListMetadataFormats(repository=OaiRepository('example.org'))
        oaijazz = OaiJazz(self.tempdir + '/jazz')
        listMetadataFormats.addObserver(oaijazz)
        oaijazz.addOaiRecord(identifier="id0", sets=(), metadataFormats=[('oai_dc', '', '')])
        oaijazz.addOaiRecord(identifier="id1", sets=(), metadataFormats=[('rdf', '', '')])
        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertTrue(xpath(XML(body), '/oai:OAI-PMH/oai:error[@code="idDoesNotExist"]'), body)

        response = listMetadataFormats.listMetadataFormats(arguments=dict(
                verb=['ListMetadataFormats'],
                identifier=['oai:example.org:id0'],
            ),
            **self.httpkwargs)
        _, body = asString(response).split("\r\n\r\n")
        self.assertEquals(['oai_dc'], xpath(XML(body), '/oai:OAI-PMH/oai:ListMetadataFormats/oai:metadataFormat/oai:metadataPrefix/text()'))
