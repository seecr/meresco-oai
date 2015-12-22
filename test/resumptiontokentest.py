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
# Copyright (C) 2012, 2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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

from meresco.oai.resumptiontoken import ResumptionToken, resumptionTokenFromString
from meresco.oai.oaijazz import PartHash
from seecr.test import SeecrTestCase


class ResumptionTokenTest(SeecrTestCase):
    def assertResumptionToken(self, token):
        aTokenString = str(token)
        token2 = resumptionTokenFromString(aTokenString)
        self.assertEquals(token, token2)

    def testResumptionToken(self):
        self.assertResumptionToken(ResumptionToken())
        resumptionToken = ResumptionToken(metadataPrefix='oai:dc', continueAfter='100', from_='2002-06-01T19:20:30Z', until='2002-06-01T19:20:39Z', set_='some:set:name')
        self.assertResumptionToken(resumptionToken)
        self.assertEquals('oai:dc', resumptionToken.metadataPrefix)
        self.assertEquals('100', resumptionToken.continueAfter)
        self.assertEquals('2002-06-01T19:20:30Z', resumptionToken.from_)
        self.assertEquals('2002-06-01T19:20:39Z', resumptionToken.until)
        self.assertEquals('some:set:name', resumptionToken.set_)
        self.assertEquals(None, resumptionToken.parthash)
        self.assertResumptionToken(ResumptionToken(set_=None))

    def testResumptionTokenHacked(self):
        r = ResumptionToken.fromString('caap|f|m|u|s')
        # complete nonsense is accepted (for now ????)
        self.assertEquals('aap', r.continueAfter)

    def testParthash(self):
        r = ResumptionToken(metadataPrefix='prefix', continueAfter='3', parthash=PartHash.create('1/2'))
        self.assertEquals('1/2', str(r.parthash))
        self.assertResumptionToken(r)