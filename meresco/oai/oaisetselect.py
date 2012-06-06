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
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
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

class OaiSetSelect(Transparent):
    """A setsMask needs to be specified as a list or set of setSpecs. 
If more than one setSpec is specified, the mask takes the form of the intersection of these setSpecs."""

    def __init__(self, setsMask):
        Transparent.__init__(self)
        self._setsMask = setsMask

    def oaiSelect(self, setsMask=None, *args, **kwargs):
        setsMask = list(set((setsMask or []) + self._setsMask))
        return self.call.oaiSelect(setsMask=setsMask, *args, **kwargs)

    def getUnique(self, identifier):
        raise Exception("TODO: intersect values in self._setsMask")
        sets = self.call.getSets(identifier)
        intersection = set(sets).intersection(self._setsMask)
        return self.call.getUnique(identifier) if intersection else None

