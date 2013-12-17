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
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from meresco.core import Transparent

import re
repositoryIdentifierRe = re.compile(r"[a-zA-Z][a-zA-Z0-9\-]*(\.[a-zA-Z][a-zA-Z0-9\-]+)+")

class OaiIdentifierRename(Transparent):
    def __init__(self, repositoryIdentifier):
        Transparent.__init__(self)
        if not repositoryIdentifierRe.match(repositoryIdentifier):
            raise ValueError("Invalid repositoryIdentifier: %s" % repositoryIdentifier)

        self._repositoryIdentifier = repositoryIdentifier
        self._prefix = 'oai:%s:' % self._repositoryIdentifier

    def _strip(self, identifier):
        return identifier[len(self._prefix):]

    def _append(self, record):
        if record:
            record.identifier = self._prefix + record.identifier
        return record

    def write(self, sink, id, partName):
        return self.call.write(sink, self._strip(id), partName)

    def yieldRecord(self, identifier, partname):
        return self.call.yieldRecord(self._strip(identifier), partname)

    def getStream(self, id, partName):
        return self.call.getStream(self._strip(id), partName)

    def getRecord(self, identifier):
        return self._append(self.call.getRecord(self._strip(identifier)))

    def oaiSelect(self, *args, **kwargs):
        result = self.call.oaiSelect(*args, **kwargs)
        result.records = (self._append(record) for record in result.records)
        return result

