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

from time import strptime

class ISO8601Exception(Exception):
    pass

shortDate = '%Y-%m-%d'
longDate = '%Y-%m-%dT%H:%M:%SZ'

class ISO8601:
    short, long = [len('YYYY-MM-DD'), len('YYYY-MM-DDThh:mm:ssZ')]

    def __init__(self, s):
        if not len(s) in [self.short, self.long]:
            raise ISO8601Exception(s)
        
        if not self._matchesDateTimeFormat(shortDate, s) and not self._matchesDateTimeFormat(longDate, s):
          raise ISO8601Exception(s)
        self.s = s
    
    def _extend(self, extension):
        if not self.isShort():
            return self.s
        return self.s + extension

    def _matchesDateTimeFormat(self, aDateFormat, aDateString):
      result = True
      try:
        year, month, day, hour, minute, seconds, wday, yday, isdst = strptime(aDateString, aDateFormat)
        if seconds > 59:
          result = False
      except ValueError:
        result = False
      return result

    def floor(self):
        return self._extend("T00:00:00Z")
    
    def ceil(self):
        return self._extend("T23:59:59Z")
    
    def __str__(self):
        return self.floor()
   
    def __repr__(self):
        return str(self.s)

    def isShort(self):
        return len(self.s) == self.short

