# -*- coding: utf-8 -*-
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
# Copyright (C) 2010 Maastricht University Library http://www.maastrichtuniversity.nl/web/Library/home.htm
# Copyright (C) 2011 Nederlands Instituut voor Beeld en Geluid http://instituut.beeldengeluid.nl
# Copyright (C) 2011-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011-2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2012-2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from warnings import simplefilter
simplefilter('default')

from convertoaiv1tov2test import ConvertOaiV1ToV2Test
from convertoaiv2tov3test import ConvertOaiV2ToV3Test
from convertoaiv3tov4test import ConvertOaiV3ToV4Test
from convertoaiv5tov6test import ConvertOaiV5ToV6Test
from fields2oairecordtest import Fields2OaiRecordTest
from oaiaddrecordtest import OaiAddRecordTest
from oaiaddrecordwithdefaultstest import OaiAddRecordWithDefaultsTest
from oaibrandingtest import OaiBrandingTest
from oaidownloadprocessortest import OaiDownloadProcessorTest
from oaierrortest import OaiErrorTest
from oaiintegrationtest import OaiIntegrationTest
from oaijazztest import OaiJazzTest
from oailisttest import OaiListTest
from oaipmhtest import OaiPmhTest, OaiPmhWithIdentifierTest, HttpPostOaiPmhTest
from oaiprovenancetest import OaiProvenanceTest
from oairecordtest import OaiRecordTest
from oaisetmasktest import OaiSetMaskTest
from oaisetselecttest import OaiSetSelectTest
from oaitooltest import OaiToolTest
from resumptiontokentest import ResumptionTokenTest
from streaminglxmltest import StreamingLxmlTest
from updateadaptertest import UpdateAdapterTest
from sequentialstoragetest import SequentialStorageTest

if __name__ == '__main__':
    unittest.main()
