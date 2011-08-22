## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2011 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.core import Observable
from warnings import warn

namespaces = {
    'oai': 'http://www.openarchives.org/OAI/2.0/'
}

class UpdateAdapterFromOaiDownloadProcessor(Observable):

    def add(self, identifier, lxmlNode, datestamp):
        __callstack_var_identifier = identifier
        if xpath(lxmlNode, '/oai:record/oai:header[@status="deleted"]'):
            return self.all.delete(identifier=identifier)
        else:
            return self.all.add(identifier=identifier, partname='record', lxmlNode=lxmlNode)

class UpdateAdapterFromOaiHarvester(UpdateAdapterFromOaiDownloadProcessor):
    def __init__(self, *args, **kwargs):
        warn("UpdateAdapterFromOaiHarvester is deprecated, please use UpdateAdapterFromOaiDownloadProcessor,", DeprecationWarning)
        UpdateAdapterFromOaiDownloadProcessor.__init__(self, *args, **kwargs)

def xpath(node, path):
    return node.xpath(path, namespaces=namespaces)
