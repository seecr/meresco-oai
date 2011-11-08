from unittest import TestCase
from lxml.etree import XMLParser, TreeBuilder, tostring

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
        self.assertEquals("<aap>noot</aap>", tostring(target.root))

    def testFilterTag(self):
        target = Target('mies')
        p = XMLParser(target = target)
        p.feed("<aap><mies>")
        p.feed("noot")
        p.feed("</mies>")
        p.feed("</aap>")
        self.assertEquals("<mies>noot</mies>", tostring(target.root))


    def testFilterTag(self):
        target = Target('mies')
        p = XMLParser(target = target)
        p.feed("<mies><mies>")
        p.feed("noot")
        p.feed("</mies>")
        p.feed("</mies>")
        self.assertEquals("<mies><mies>noot</mies></mies>", tostring(target.root))
