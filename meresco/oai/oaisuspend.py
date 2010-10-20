## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Stichting Kennisnet Ict op school.
#       http://www.kennisnetictopschool.nl
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

from meresco.core import Transparant
from weightless import Suspend

class OaiSuspend(Transparant):

    def __init__(self):
        Transparant.__init__(self)
        self._suspended = []

    def suspend(self):
        suspend = Suspend()
        self._suspended.append(suspend) 
        yield suspend
        suspend.getResult()

    def addOaiRecord(self, **kwargs):
        self.do.addOaiRecord(**kwargs)
        self._resume()

    def delete(self, **kwargs):
        self.do.delete(**kwargs)
        self._resume()

    def _resume(self):
        while len(self._suspended) > 0:
            self._suspended.pop().resume()

