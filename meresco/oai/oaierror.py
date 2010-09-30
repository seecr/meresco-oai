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

from meresco.core import Observable
from oaiutils import oaiHeader, oaiFooter, REQUEST, requestUrl
from xml.sax.saxutils import escape as xmlEscape

class OaiError(Observable):
    def unknown(self, message, **kwargs):
        result = self.all.unknown(message, **kwargs)
        try:
            firstResult = result.next()
        except StopIteration:
            yield self._error(**kwargs)
            return
        yield firstResult
        for remainder in result:
            yield remainder

    def _error(self, arguments, **kwargs):
        verbs = arguments.get('verb', [None])
        if verbs[0] is None or verbs[0] == '':
            yield oaiError('badArgument', 'No "verb" argument found.', arguments=arguments, **kwargs)
        elif len(verbs) > 1:
            yield oaiError('badArgument', 'More than one "verb" argument found.', arguments=arguments, **kwargs)
        else:
            yield oaiError('badVerb', 'Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.', arguments=arguments, **kwargs)


def oaiError(statusCode, addionalMessage, arguments, **kwargs):
    space = addionalMessage and ' ' or ''
    message = ERROR_CODES[statusCode] + space + addionalMessage

    yield oaiHeader()

    url =  requestUrl(**kwargs)

    args = ''
    if statusCode not in ["badArgument", "badResumptionToken", "badVerb"]:
        """in these cases it is illegal to echo the arguments back; since the arguments are not valid in the first place the responce will not validate either"""
        args = ' '.join(['%s="%s"' % (xmlEscape(k), xmlEscape(v[0]).replace('"', '&quot;')) for k,v in sorted(arguments.items())])
    yield REQUEST % locals()
    yield """<error code="%(statusCode)s">%(message)s</error>""" % locals()

    yield oaiFooter()

# http://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
ERROR_CODES = {
    'badArgument': 'The request includes illegal arguments, is missing required arguments, includes a repeated argument, or values for arguments have an illegal syntax.',
    'badResumptionToken': 'The value of the resumptionToken argument is invalid or expired.',
    'badVerb': 'Value of the verb argument is not a legal OAI-PMH verb, the verb argument is missing, or the verb argument is repeated.',
    'cannotDisseminateFormat': 'The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository.',
    'idDoesNotExist': 'The value of the identifier argument is unknown or illegal in this repository.',
    'noRecordsMatch': 'The combination of the values of the from, until, set and metadataPrefix arguments results in an empty list.',
    'noMetadataFormats': 'There are no metadata formats available for the specified item.',
    'noSetHierarchy': 'The repository does not support sets.'
}
