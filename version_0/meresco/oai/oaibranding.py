# -*- coding: utf-8 -*-
## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2010 Maastricht University Library
#        http://www.maastrichtuniversity.nl/web/Library/home.htm
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

from meresco.core import Observable
from xml.sax.saxutils import escape as escapeXml

class OaiBranding(Observable):
   
    def __init__(self, url, link=None, title=None):
        self._url = url
        self._link = link
        self._title = title

    def description(self):
        yield """<description xmlns="http://www.openarchives.org/OAI/2.0/">"""
        yield """<branding xmlns="http://www.openarchives.org/OAI/2.0/branding/"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/branding/
                                http://www.openarchives.org/OAI/2.0/branding.xsd">"""
        yield "<collectionIcon>"
        yield "     <url>%s</url>" % escapeXml(self._url)
        yield "     <link>%s</link>" % escapeXml(self._link) if self._link else ""
        yield "     <title>%s</title>" % self._title if self._title else ""
        yield "</collectionIcon>"
        yield "</branding>"
        yield "</description>"
