## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2018 Seecr (Seek You Too B.V.) https://seecr.nl
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

from os.path import isfile, join
from os import remove
from meresco.oai import OaiJazz

class Convert11to12(object):
    def __init__(self, dataDir):
        self._dataDir = dataDir

    def go(self, verbose=False):
        self._preflightCheck()
        self._convert(verbose)

    def _preflightCheck(self):
        oaiversion = join(self._dataDir, 'oai.version')
        if not isfile(oaiversion) or open(oaiversion).read() != '11':
            raise ValueError('Expected version 11 of meresco oai in "{}"'.format(self._dataDir))
        if isfile(join(self._dataDir, 'converting')):
            raise ValueError('A conversion is still busy or has crashed')

    def _convert(self, verbose=False):
        with open(join(self._dataDir, 'converting'), 'w') as f:
            f.write('CONVERTING')
        try:
            with open(join(self._dataDir, 'oai.version'), 'w') as v:
                v.write('12')
            o = OaiJazz(self._dataDir)
            try:
                continueAfter = 0
                while continueAfter is not None:
                    if continueAfter == 0:
                        continueAfter = None
                    result = o.oaiSelect(prefix=None, continueAfter=continueAfter)
                    continueAfter = result.continueAfter
                    for record in result.records:
                        if record.isDeleted and record.prefixes != record.deletedPrefixes:
                            if verbose:
                                print 'Converting', record.identifier
                            o.deleteOaiRecord(identifier=record.identifier)
            finally:
                o.close()
        finally:
            remove(join(self._dataDir, 'converting'))

