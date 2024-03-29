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
# Copyright (C) 2011-2017, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2015 Koninklijke Bibliotheek (KB) http://www.kb.nl
# Copyright (C) 2016 SURFmarket https://surf.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
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

from urllib.parse import parse_qs
import re

from weightless.core import be, compose
from meresco.core import Transparent, Observable

from .oaiidentify import OaiIdentify
from .oailist import OaiList
from .oaijazz import DEFAULT_BATCH_SIZE
from .oaigetrecord import OaiGetRecord
from .oailistmetadataformats import OaiListMetadataFormats
from .oailistsets import OaiListSets
from .oaierror import OaiError
from .oairecord import OaiRecord
from .oairepository import OaiRepository


class OaiPmh(object):
    def __init__(self, repositoryName, adminEmail, repositoryIdentifier=None, batchSize=DEFAULT_BATCH_SIZE, supportXWait=False, externalUrl=None, preciseDatestamp=False, deleteInSets=False):
        self._repository = OaiRepository(
            identifier=repositoryIdentifier,
            name=repositoryName,
            adminEmail=adminEmail,
            externalUrl=externalUrl,
        )
        outside = Transparent()
        self.addObserver = outside.addObserver
        self.addStrand = outside.addStrand
        self._internalObserverTree = be(
            (Observable(),
                (OaiError(self._repository),
                    (OaiIdentify(self._repository),
                        (outside,)
                    ),
                    (OaiList(repository=self._repository, batchSize=batchSize, supportXWait=supportXWait),
                        (OaiRecord(self._repository, preciseDatestamp=preciseDatestamp, deleteInSets=deleteInSets),
                            (outside,)
                        )
                    ),
                    (OaiGetRecord(self._repository),
                        (OaiRecord(self._repository, preciseDatestamp=preciseDatestamp, deleteInSets=deleteInSets),
                            (outside,)
                        )
                    ),
                    (OaiListMetadataFormats(self._repository),
                        (outside,)
                    ),
                    (OaiListSets(self._repository),
                        (outside,)
                    ),
                )
            )
        )

    def updateRepositoryInfo(self, name=None, adminEmail=None):
        if name is not None:
            self._repository.updateName(name=name)
        if adminEmail is not None:
            self._repository.updateAdminEmail(adminEmail=adminEmail)

    def observer_init(self):
        list(compose(self._internalObserverTree.once.observer_init()))

    def handleRequest(self, Method, arguments, Body=None, **kwargs):
        if Method == 'POST':
            arguments.update(parse_qs(str(Body, encoding="utf-8"), keep_blank_values=True))
        verb = arguments.get('verb', [None])[0]
        message = verb[0].lower() + verb[1:] if verb else ''
        yield self._internalObserverTree.all.unknown(message, arguments=arguments, **kwargs)


