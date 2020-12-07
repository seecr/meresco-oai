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
# Copyright (C) 2012, 2015 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
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


class Fields2OaiRecord(Observable):
    def beginTransaction(self):
        return Fields2OaiRecord.Fields2OaiRecordTx(self)
        yield

    class Fields2OaiRecordTx(object):
        def __init__(self, resource):
            self._sets = set()
            self._metadataFormats = set()
            self._resource = resource

        def addField(self, name, value):
            if name == 'set':
                self._sets.add(value)
            elif name == 'metadataFormat':
                self._metadataFormats.add(value)

        def commit(self):
            if self._metadataFormats:
                identifier = self._resource.ctx.tx.locals['id']
                self._resource.do.addOaiRecord(identifier=identifier, sets=self._sets, metadataFormats=self._metadataFormats)
            return
            yield

        def rollback(self):
            return
            yield

