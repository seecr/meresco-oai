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
# Copyright (C) 2010, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2012, 2014, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

from meresco.components import XmlCompose
from lxml.etree import parse
from io import BytesIO

PROVENANCE_TEMPLATE = """<provenance xmlns="http://www.openarchives.org/OAI/2.0/provenance"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/provenance
                      http://www.openarchives.org/OAI/2.0/provenance.xsd">

<originDescription harvestDate="%(harvestDate)s" altered="true">
    <baseURL>%(baseURL)s</baseURL>
    <identifier>%(identifier)s</identifier>
    <datestamp>%(datestamp)s</datestamp>
    <metadataNamespace>%(metadataNamespace)s</metadataNamespace>
</originDescription>
</provenance>
"""

class OaiProvenance(XmlCompose):
    def __init__(self, nsMap, baseURL, harvestDate, metadataNamespace, identifier, datestamp):
        XmlCompose.__init__(self,
            PROVENANCE_TEMPLATE,
            nsMap,
            baseURL=baseURL,
            harvestDate=harvestDate,
            metadataNamespace=metadataNamespace,
            identifier=identifier,
            datestamp=datestamp)

    def provenance(self, identifier):
        yield self.getRecord(identifier=identifier)

    def _getPart(self, identifier, partname):
        data = self.call.getData(identifier=identifier, name=partname)
        return parse(BytesIO(data))
