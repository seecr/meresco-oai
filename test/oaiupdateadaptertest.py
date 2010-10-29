## begin license ##
#
#  Edurep is a service for searching in educational repositories.
#  Edurep is developed for Stichting Kennisnet (http://www.kennisnet.nl) by
#  Seek You Too (http://www.cq2.nl). The project is based on the opensource
#  project Meresco (http://www.meresco.com).
#  Copyright (C) 2010 Stichting Kennisnet http://www.kennisnet.nl
#  Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
#
#  This file is part of Edurep
#
#  Edurep is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  Edurep is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Edurep; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

from cq2utils import CQ2TestCase, CallTrace

from lxml.etree import parse
from StringIO import StringIO

from edurep import namespaces
from edurep.search.oaiupdateadapter import UpdateAdapterFromOaiHarvester

class OaiUpdateAdapterTest(CQ2TestCase):
    def testDelete(self):
        adapter = UpdateAdapterFromOaiHarvester()
        observer = CallTrace('observer')
        adapter.addObserver(observer)

        list(adapter.add(lxmlNode=parse(StringIO(OAI_DELETED_RECORD))))

        self.assertEquals(['delete'], [m.name for m in observer.calledMethods])
        self.assertEquals({'identifier':'oai:test:identifier'}, observer.calledMethods[0].kwargs)

    def testAdd(self):
        adapter = UpdateAdapterFromOaiHarvester()
        observer = CallTrace('observer')
        adapter.addObserver(observer)

        recordNode = parse(StringIO(OAI_RECORD))
        list(adapter.add(lxmlNode=recordNode))

        self.assertEquals(['add'], [m.name for m in observer.calledMethods])
        kwargs = observer.calledMethods[0].kwargs
        self.assertEquals({'identifier': 'oai:test:identifier',
            'partname': 'record',
            'lxmlNode': recordNode}, kwargs)

    def testRaiseErrorOnBadArguments(self):
        adapter = UpdateAdapterFromOaiHarvester()
        observer = CallTrace('observer')
        adapter.addObserver(observer)

        self.assertRaises(ValueError, adapter.add, lxmlNode=parse(StringIO('<nooairecord/>')))


OAI_DELETED_RECORD = """<oai:record xmlns:oai="http://www.openarchives.org/OAI/2.0/">
    <oai:header status="deleted">
            <oai:identifier>oai:test:identifier</oai:identifier>
            <oai:datestamp>2010-10-19T09:57:32Z</oai:datestamp>
    </oai:header>
</oai:record>"""

OAI_RECORD = """<oai:record xmlns:oai="http://www.openarchives.org/OAI/2.0/">
    <oai:header>
            <oai:identifier>oai:test:identifier</oai:identifier>
            <oai:datestamp>2010-10-19T09:57:32Z</oai:datestamp>
    </oai:header>
    <oai:metadata><somedata xmlns="http://example.org">DATA</somedata></oai:metadata>
</oai:record>"""

