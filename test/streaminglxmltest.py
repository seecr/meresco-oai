## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from unittest import TestCase
from lxml.etree import XMLParser, TreeBuilder
from meresco.components import lxmltostring

class Target(object):

    def __init__(self, tagname):
        self._tagname = tagname
        self._builder = TreeBuilder()
        self._rootbuilder = self._builder
        self._depth = 0

    def start(self, tagname, attrs, nsmap=None):
        if tagname == self._tagname:
            if self._depth == 0:
                self._builder = TreeBuilder()
            self._depth += 1
        self._builder.start(tagname, attrs, nsmap)
    
    def data(self, *args, **kwargs):
        self._builder.data(*args, **kwargs)

    def end(self, tagname):
        self._builder.end(tagname)
        if tagname == self._tagname:
            self._depth -= 1
            if self._depth == 0:
                self.root = self._builder.close()
                self._builder = self._rootbuilder

class StreamingLxmlTest(TestCase):
    def testTwoTags(self):
        target = Target('aap')
        p = XMLParser(target = target)
        p.feed("<aap>")
        p.feed("noot")
        p.feed("</aap>")
        self.assertEquals("<aap>noot</aap>", lxmltostring(target.root))

    def testFilterTag(self):
        target = Target('mies')
        p = XMLParser(target = target)
        p.feed("<aap><mies>")
        p.feed("noot")
        p.feed("</mies>")
        p.feed("</aap>")
        self.assertEquals("<mies>noot</mies>", lxmltostring(target.root))


    def testFilterTag(self):
        target = Target('mies')
        p = XMLParser(target = target)
        p.feed("<mies><mies>")
        p.feed("noot")
        p.feed("</mies>")
        p.feed("</mies>")
        self.assertEquals("<mies><mies>noot</mies></mies>", lxmltostring(target.root))
