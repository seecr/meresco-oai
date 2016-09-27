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
# Copyright (C) 2011-2012, 2014, 2016 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011-2012 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2016 SURFmarket https://surf.nl
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

from xml.sax.saxutils import escape as escapeXml

from meresco.core import Observable

from oaiutils import oaiHeader, oaiFooter, oaiRequestArgs, zuluTime
from oaierror import oaiError


class OaiIdentify(Observable):
    """
http://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
4.2 Identify
Summary and Usage Notes

This verb is used to retrieve information about a repository. Some of the information returned is required as part of the OAI-PMH. Repositories may also employ the Identify verb to return additional descriptive information.
Arguments

None
Error and Exception Conditions

    * badArgument - The request includes illegal arguments.

Response Format

The response must include one instance of the following elements:

    * repositoryName : a human readable name for the repository;
    * baseURL : the base URL of the repository;
    * protocolVersion : the version of the OAI-PMH supported by the repository;
    * earliestDatestamp : a UTCdatetime that is the guaranteed lower limit of all datestamps recording changes, modifications, or deletions in the repository. A repository must not use datestamps lower than the one specified by the content of the earliestDatestamp element. earliestDatestamp must be expressed at the finest granularity supported by the repository.
    * deletedRecord : the manner in which the repository supports the notion of deleted records. Legitimate values are no ; transient ; persistent with meanings defined in the section on deletion.
    * granularity: the finest harvesting granularity supported by the repository. The legitimate values are YYYY-MM-DD and YYYY-MM-DDThh:mm:ssZ with meanings as defined in ISO8601.

The response must include one or more instances of the following element:

    * adminEmail : the e-mail address of an administrator of the repository.

The response may include multiple instances of the following optional elements:

    * compression : a compression encoding supported by the repository. The recommended values are those defined for the Content-Encoding header in Section 14.11 of RFC 2616 describing HTTP 1.1. A compression element should not be included for the identity encoding, which is implied.
    * description : an extensible mechanism for communities to describe their repositories. For example, the description container could be used to include collection-level metadata in the response to the Identify request. Implementation Guidelines are available to give directions with this respect. Each description container must be accompanied by the URL of an XML schema describing the structure of the description container.

    """
    def __init__(self, repository):
        Observable.__init__(self)
        self._repository = repository

    def identify(self, arguments, **httpkwargs):
        responseDate = zuluTime()
        if arguments.keys() != ['verb']:
            additionalMessage = 'Argument(s) %s is/are illegal.' % ", ".join('"%s"' % key for key in arguments.keys() if key != 'verb')
            yield oaiError('badArgument',
                    additionalMessage=additionalMessage,
                    arguments=arguments,
                    requestUrl=self._repository.requestUrl(**httpkwargs),
                    **httpkwargs)
            return

        repositoryIdentifier = self._repository.identifier
        descriptionRepositoryIdentifier = '' if not repositoryIdentifier else DESCRIPTION_REPOSITORY_IDENTIFIER % {'repositoryIdentifier': escapeXml(repositoryIdentifier)}

        values = {
            'repositoryName': escapeXml(self._repository.name),
            'baseURL': escapeXml(self._repository.requestUrl(**httpkwargs)),
            'adminEmails': ''.join([ADMIN_EMAIL % escapeXml(email) for email in [self._repository.adminEmail]]),
            'deletedRecord': self.call.getDeletedRecordType(),
        }
        values.update(hardcoded_values)
        yield oaiHeader(self, responseDate)
        yield oaiRequestArgs(arguments, requestUrl=self._repository.requestUrl(**httpkwargs), **httpkwargs)
        yield '<Identify>'
        yield IDENTIFY % values
        yield descriptionRepositoryIdentifier
        yield TOOLKIT_DESCRIPTION
        yield self.all.description()
        yield '</Identify>'
        yield oaiFooter()


hardcoded_values = {
    'protocolVersion': '2.0',
    'earliestDatestamp': '1970-01-01T00:00:00Z',
    'granularity': 'YYYY-MM-DDThh:mm:ssZ'
}


REQUEST = """<request verb="Identify">%s</request>"""

ADMIN_EMAIL = """<adminEmail>%s</adminEmail>"""

IDENTIFY = """<repositoryName>%(repositoryName)s</repositoryName>
<baseURL>%(baseURL)s</baseURL>
<protocolVersion>%(protocolVersion)s</protocolVersion>
    %(adminEmails)s
<earliestDatestamp>%(earliestDatestamp)s</earliestDatestamp>
<deletedRecord>%(deletedRecord)s</deletedRecord>
<granularity>%(granularity)s</granularity>"""

TOOLKIT_DESCRIPTION = """<description>
    <toolkit xmlns="http://oai.dlib.vt.edu/OAI/metadata/toolkit"
             xsi:schemaLocation="http://oai.dlib.vt.edu/OAI/metadata/toolkit http://oai.dlib.vt.edu/OAI/metadata/toolkit.xsd">
        <title>Meresco</title>
        <author>
            <email>info@seecr.nl</email>
            <institution>Seecr with the Meresco community</institution>
        </author>
        <toolkitIcon>http://meresco.org/files/images/meresco-logo-small.png</toolkitIcon>
        <URL>http://www.meresco.org</URL>
    </toolkit>
</description>"""

DESCRIPTION_REPOSITORY_IDENTIFIER = """<description>
  <oai-identifier xmlns="http://www.openarchives.org/OAI/2.0/oai-identifier"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai-identifier
      http://www.openarchives.org/OAI/2.0/oai-identifier.xsd">
    <scheme>oai</scheme>
    <repositoryIdentifier>%(repositoryIdentifier)s</repositoryIdentifier>
    <delimiter>:</delimiter>
    <sampleIdentifier>oai:%(repositoryIdentifier)s:5324</sampleIdentifier>
  </oai-identifier>
  </description>"""
