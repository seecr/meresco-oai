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

from meresco.core import Observable

namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/'
}

class UpdateAdapterFromOaiHarvester(Observable):

    def add(self, lxmlNode):
        header = xpath(lxmlNode, '/oai:record/oai:header')
        if not header:
            raise ValueError("Expected /{%(oai)s}record/{%(oai)s}header" % namespaces)
        header = header[0]
        identifier = xpath(header, 'oai:identifier/text()')[0]
        if xpath(header, 'self::node()[@status="deleted"]'):
            print 'delete', identifier
            
            return self.all.delete(identifier=identifier)
        else:
            print 'add', identifier
            return self.all.add(identifier=identifier, partname='record', lxmlNode=lxmlNode)


def xpath(node, path):
    return node.xpath(path, namespaces=namespaces)
