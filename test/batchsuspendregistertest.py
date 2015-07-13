## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2015 Seecr (Seek You Too B.V.) http://seecr.nl
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

from seecr.test import SeecrTestCase
from meresco.oai import BatchSuspendRegister
from weightless.io import Suspend

class BatchSuspendRegisterTest(SeecrTestCase):

    def testExplicitResume(self):
        register = BatchSuspendRegister()
        register.signalOaiUpdate(metadataPrefixes=['prefix'], sets=['set_a'], stamp=1000)
        register.startOaiBatch()
        result = list(register.suspendBeforeSelect(continueAfter='1000', otherKey='other'))
        self.assertEquals([Suspend], [type(e) for e in result])
        register.suspendAfterNoResult()

