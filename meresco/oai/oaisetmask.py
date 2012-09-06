## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2012 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Nederlands Instituut voor Beeld en Geluid  http://www.beeldengeluid.nl
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

class OaiSetMask(Transparent):
    """A setsMask needs to be specified as a list or set of setSpecs. 
If more than one setSpec is specified (in a single instance or by chaining), 
the mask takes the form of the intersection of these setSpecs."""

    def __init__(self, setsMask, name=None):
        Transparent.__init__(self, name=name)
        self._setsMask = set(setsMask)

    def oaiSelect(self, setsMask=None, *args, **kwargs):
        return self.call.oaiSelect(setsMask=self._combinedSetsMask(setsMask), *args, **kwargs)

    def getUnique(self, identifier, setsMask=None):
        sets = self.call.getSets(identifier)
        if self._combinedSetsMask(setsMask).issubset(sets):
            return self.call.getUnique(identifier)
        return None

    def _combinedSetsMask(self, setsMask):
        return self._setsMask.union(setsMask or [])
