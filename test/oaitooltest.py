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
# Copyright (C) 2011-2012, 2016, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
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

from meresco.oai.oaitool import ISO8601Exception, ISO8601
from meresco.oai.oaiutils import oaiRequestArgs, validSetSpecName
from seecr.test import SeecrTestCase

class OaiToolTest(SeecrTestCase):

    def testWriteRequestArgs(self):
        result = ''.join(oaiRequestArgs({'identifier': ['with a "']}, requestUrl='https://example.org/oai', Headers={'Host':'localhost'}, port=8000, path='/oai'))

        self.assertEqual('<request identifier="with a &quot;">https://example.org/oai</request>', result)

    def testSetSpecName(self):
        self.assertTrue(validSetSpecName("name"))
        self.assertTrue(validSetSpecName("123name"))
        self.assertTrue(validSetSpecName("n-a-m-e"))
        self.assertTrue(validSetSpecName("(._~-'!*!'-~_.)"))
        self.assertFalse(validSetSpecName("separated:name"))
        self.assertFalse(validSetSpecName("separated name"))
        self.assertFalse(validSetSpecName("separated#/name"))


    def testISO8601(self):
        """http://www.w3.org/TR/NOTE-datetime
   Below is the complete spec by w3. OAI-PMH only allows for
   YYYY-MM-DD and
   YYYY-MM-DDThh:mm:ssZ

   Year:
      YYYY (eg 1997)
   Year and month:
      YYYY-MM (eg 1997-07)
   Complete date:
      YYYY-MM-DD (eg 1997-07-16)
   Complete date plus hours and minutes:
      YYYY-MM-DDThh:mmTZD (eg 1997-07-16T19:20+01:00)
   Complete date plus hours, minutes and seconds:
      YYYY-MM-DDThh:mm:ssTZD (eg 1997-07-16T19:20:30+01:00)
   Complete date plus hours, minutes, seconds and a decimal fraction of a
second
      YYYY-MM-DDThh:mm:ss.sTZD (eg 1997-07-16T19:20:30.45+01:00)"""

        def right(s):
            ISO8601(s)

        def wrong(s):
            try:
                ISO8601(s)
                self.fail()
            except ISO8601Exception as e:
                pass

        wrong('2000')
        wrong('2000-01')
        right('2000-01-01')
        wrong('aaaa-bb-cc')
        wrong('2000-01-32')
        wrong('2000-01-01T00:00Z')
        right('2000-01-01T00:00:00Z')
        right('2000-12-31T23:59:59Z')
        wrong('2000-01-01T00:00:61Z')
        wrong('2000-01-01T00:00:00+01:00')
        wrong('2000-01-01T00:00:00.000Z')

        iso8601 = ISO8601('2000-01-01T00:00:00Z')
        self.assertFalse(iso8601.isShort())
        self.assertEqual('2000-01-01T00:00:00Z', str(iso8601))
        self.assertEqual('2000-01-01T00:00:00Z', iso8601.floor())
        self.assertEqual('2000-01-01T00:00:00Z', iso8601.ceil())

        iso8601 = ISO8601('2000-01-01')
        self.assertTrue(iso8601.isShort())
        self.assertEqual('2000-01-01T00:00:00Z', str(iso8601))
        self.assertEqual('2000-01-01T00:00:00Z', iso8601.floor())
        self.assertEqual('2000-01-01T23:59:59Z', iso8601.ceil())
