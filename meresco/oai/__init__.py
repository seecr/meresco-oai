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
# Copyright (C) 2010-2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2011-2015, 2018-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2019 SURFmarket https://surf.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from os.path import dirname, abspath, join, isfile, basename      #DO_NOT_DISTRIBUTE
from os import stat, system                                       #DO_NOT_DISTRIBUTE
from glob import glob                                             #DO_NOT_DISTRIBUTE
from sys import exit, path as sysPath                             #DO_NOT_DISTRIBUTE
mydir = dirname(abspath(__file__))                                #DO_NOT_DISTRIBUTE
srcDir = join(dirname(dirname(mydir)), 'src')                     #DO_NOT_DISTRIBUTE
libDir = join(dirname(dirname(mydir)), 'lib')                     #DO_NOT_DISTRIBUTE
sofiles = glob(join(libDir, "meresco_oai", "*.so"))               #DO_NOT_DISTRIBUTE
sofile = [each for each in sofiles if basename(each).startswith("_meresco_oai")][0]               #DO_NOT_DISTRIBUTE
#sofile = join(libDir, 'meresco_oai', '_meresco_oai.so')           #DO_NOT_DISTRIBUTE
merescoOaiFiles = join(srcDir, 'org','meresco','oai', '*.java')   #DO_NOT_DISTRIBUTE
lastMtime = max(stat(f).st_mtime for f in glob(merescoOaiFiles))  #DO_NOT_DISTRIBUTE
if not isfile(sofile) or stat(sofile).st_mtime < lastMtime:       #DO_NOT_DISTRIBUTE
    result = system('cd %s; ./build.sh' % srcDir)                 #DO_NOT_DISTRIBUTE
    if result:                                                    #DO_NOT_DISTRIBUTE
        exit(result)                                              #DO_NOT_DISTRIBUTE
sysPath.insert(0, libDir)                                         #DO_NOT_DISTRIBUTE

from .__version__ import VERSION
from .oaipmh import OaiPmh
from .oaiprovenance import OaiProvenance
from .oaisetmask import OaiSetMask
from .oaisetselect import OaiSetSelect # deprecated
from .fields2oairecord import Fields2OaiRecord
from .oaijazz import OaiJazz, allHierarchicalSetSpecs
from .oaiaddrecord import OaiAddRecord, OaiAddDeleteRecordWithPrefixesAndSetSpecs
from .oaibranding import OaiBranding
from .suspendregister import SuspendRegister

# backwards compatible imports
from meresco.oaicommon import stamp2zulutime, ResumptionToken, OaiDownloadProcessor, UpdateAdapterFromOaiDownloadProcessor
