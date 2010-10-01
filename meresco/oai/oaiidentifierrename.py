## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
#    Copyright (C) 2007-2009 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
#    Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
#    Copyright (C) 2009 Tilburg University http://www.uvt.nl
#    Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
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
from oaijazz import RecordId, WrapIterable

import re
repositoryIdentifierRe = re.compile(r"[a-zA-Z][a-zA-Z0-9\-]*(\.[a-zA-Z][a-zA-Z0-9\-]+)+")

class OaiIdentifierRename(Observable):
    def __init__(self, repositoryIdentifier):
        Observable.__init__(self)
        if not repositoryIdentifierRe.match(repositoryIdentifier):
            raise ValueError("Invalid repositoryIdentifier: %s" % repositoryIdentifier)

        self._repositoryIdentifier = repositoryIdentifier
        self._prefix = 'oai:%s:' % self._repositoryIdentifier

    def _strip(self, identifier):
        return identifier[len(self._prefix):]

    def _append(self, identifier):
        if hasattr(identifier, 'stamp'):
            return RecordId(self._prefix + identifier, identifier.stamp)
        return self._prefix + identifier


    def isDeleted(self, identifier):
        return self.any.isDeleted(self._strip(identifier))

    def getUnique(self, identifier):
        return self.any.getUnique(self._strip(identifier))

    def getDatestamp(self, identifier):
        return self.any.getDatestamp(self._strip(identifier))
    
    def isAvailable(self, id, partName):
        return self.any.isAvailable(self._strip(id), partName)
    
    def getPrefixes(self, identifier):
        return self.any.getPrefixes(self._strip(identifier))
    
    def getSets(self, identifier):
        return self.any.getSets(self._strip(identifier))

    def write(self, sink, id, partName):
        return self.any.write(sink, self._strip(id), partName)

    def yieldRecord(self, identifier, partname):
        return self.any.yieldRecord(self._strip(identifier), partname)

    def getStream(self, id, partName):
        return self.any.getStream(self._strip(id), partName)
    
    def unknown(self, message, *args, **kwargs):
        return self.all.unknown(message, *args, **kwargs)

    def oaiSelect(self, *args, **kwargs):
        return WrapIterable((self._append(recordId) for recordId in self.any.oaiSelect(*args, **kwargs)))
            
