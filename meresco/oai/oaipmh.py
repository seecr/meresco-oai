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

from meresco.core import be, Transparant, Observable
from oaiidentify import OaiIdentify
from oailist import OaiList
from oaigetrecord import OaiGetRecord
from oailistmetadataformats import OaiListMetadataFormats
from oailistsets import OaiListSets
from oaisink import OaiSink
from oaiidentifierrename import OaiIdentifierRename
from webrequest import WebRequest

class OaiPmh(object):
    def __init__(self, repositoryName, adminEmail, repositoryIdentifier=None):
        outside = Transparant() if repositoryIdentifier == None else OaiIdentifierRename(repositoryIdentifier)
        self.addObserver = outside.addObserver
        self.addStrand = outside.addStrand
        self._internalObserverTree = be(
            (Observable(),
                (OaiIdentify(repositoryName=repositoryName, adminEmail=adminEmail, repositoryIdentifier=repositoryIdentifier), ),
                (OaiList(),
                    (outside,)
                ),
                (OaiGetRecord(),
                    (outside,)
                ),
                (OaiListMetadataFormats(),
                    (outside,)
                ),
                (OaiListSets(),
                    (outside,)
                ),
                (OaiSink(), )
            )
        )


    def handleRequest(self, **kwargs):
        # if 'ListRecords'
        #   yield self._internalObserverTree.any.unknown("ListRecords", **kwargs)
        # else:
        webrequest = WebRequest(**kwargs)
        verb = webrequest.args.get('verb',[None])[0]
        message = verb and verb[0].lower() + verb[1:] or ''
        self._internalObserverTree.any.unknown(message, webrequest)
        return webrequest.generateResponse()
