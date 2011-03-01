## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010-2011 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2010 Maastricht University Library
#        http://www.maastrichtuniversity.nl/web/Library/home.htm
#    Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
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

from cq2utils import CQ2TestCase
from meresco.oai import OaiBranding
from weightless.core import compose

class OaiBrandingTest(CQ2TestCase):

    def testOne(self):
        branding = OaiBranding(url="http://example.org/icon.png", link="http://www.example.org", title="Example")
        description = ''.join(compose(branding.description()))
        self.assertEqualsWS(BRANDING_RESULT, description)

    def testBrandingWithoutLinkAndTitle(self):
        branding = OaiBranding(url="http://example.org/icon.png")
        description = ''.join(compose(branding.description()))
        self.assertEqualsWS(BRANDING_RESULT_URL, description)

BRANDING_RESULT="""<description xmlns="http://www.openarchives.org/OAI/2.0/">
    <branding xmlns="http://www.openarchives.org/OAI/2.0/branding/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/branding/
                          http://www.openarchives.org/OAI/2.0/branding.xsd">
      <collectionIcon>
        <url>http://example.org/icon.png</url>
        <link>http://www.example.org</link>
        <title>Example</title>
      </collectionIcon>
    </branding>
</description>"""

BRANDING_RESULT_URL="""<description xmlns="http://www.openarchives.org/OAI/2.0/">
    <branding xmlns="http://www.openarchives.org/OAI/2.0/branding/"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/branding/
                          http://www.openarchives.org/OAI/2.0/branding.xsd">
      <collectionIcon>
        <url>http://example.org/icon.png</url>
      </collectionIcon>
    </branding>
</description>"""
