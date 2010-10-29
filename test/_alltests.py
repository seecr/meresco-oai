# -*- coding: utf-8 -*-
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

from os import getuid
assert getuid() != 0, "Do not run tests as 'root'"

from os import system                             #DO_NOT_DISTRIBUTE
from sys import path as sysPath                   #DO_NOT_DISTRIBUTE
system('find .. -name "*.pyc" | xargs rm -f')     #DO_NOT_DISTRIBUTE
                                                  #DO_NOT_DISTRIBUTE
from glob import glob                             #DO_NOT_DISTRIBUTE
for path in glob('../deps.d/*'):                  #DO_NOT_DISTRIBUTE
    sysPath.insert(0, path)                       #DO_NOT_DISTRIBUTE
sysPath.insert(0,'..')                            #DO_NOT_DISTRIBUTE

import unittest

from fields2oairecordtest import Fields2OaiRecordTest
from oaiaddrecordtest import OaiAddRecordTest
from oaigetrecordtest import OaiGetRecordTest
from oaiharvestertest import OaiHarvesterTest
from oaijazzimplementationstest import OaiJazzImplementationsTest
from oaijazztest import OaiJazzTest
from oailistmetadataformatstest import OaiListMetadataFormatsTest
from oailistsetstest import OaiListSetsTest
from oailisttest import OaiListTest
from oaipmhjazztest import OaiPmhJazzTest
from oaipmhtest import OaiPmhTest2
from oaipmhtest import OaiPmhTest, OaiPmhWithIdentifierTest
from oaiprovenancetest import OaiProvenanceTest
from oaisetselecttest import OaiSetSelectTest
from oaisuspendtest import OaiSuspendTest
from oaitooltest import OaiToolTest
from resumptiontokentest import ResumptionTokenTest
from updateadaptertest import UpdateAdapterTest

if __name__ == '__main__':
    unittest.main()
