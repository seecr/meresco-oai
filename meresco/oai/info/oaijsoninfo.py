## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Observable
from simplejson import dumps

class OaiJsonInfo(Observable):

    def handleRequest(self, path, arguments, **kwargs):
        method = path.rpartition('/')[-1]
        yield 'HTTP/1.0 200 OK\r\n'
        yield 'Content-Type: application/json\r\n'
        yield '\r\n'
        try:
            yield dumps(getattr(self, method)(**arguments))
        except:
            yield dumps({})

    def sets(self):
        return list(sorted(self.call.getAllSets()))

    def prefixes(self):
        return list(sorted(self.call.getAllPrefixes()))

    def prefix(self, prefix):
        prefix = prefix[0]
        for aPrefix, schema, namespace in self.call.getAllMetadataFormats():
            if aPrefix == prefix:
                break
        else:
            return {}
        nrOfRecords = self.call.getNrOfRecords(prefix=prefix)
        return dict(prefix=prefix, schema=schema, namespace=namespace, nrOfRecords=nrOfRecords)