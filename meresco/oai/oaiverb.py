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

from time import gmtime, strftime
from xml.sax.saxutils import escape as xmlEscape
from oaierror import ERROR_CODES, oaiError
from oaiutils import RESPONSE_DATE, REQUEST, OAIHEADER, OAIFOOTER, zuluTime, doElementaryArgumentsValidation, OaiBadArgumentException
from weightless import compose

class OaiVerb(object):

    def __init__(self, supportedVerbs, argsDef):
        self._supportedVerbs = supportedVerbs
        self._argsDef = argsDef

    def startProcessing(self, webRequest):
        self._verb = webRequest.args.get('verb', [None])[0]
        if not self._verb in self._supportedVerbs:
            return

        try:
            validatedArguments = doElementaryArgumentsValidation(webRequest.args, self._argsDef)
            for k,v in validatedArguments.items():
                setattr(self, "_" + k, v)
        except OaiBadArgumentException, e:
            return self.writeError(webRequest, e.statusCode, e.additionalMessage)

        error = self.preProcess(webRequest)
        if error:
            return error

        self.writeHeader(webRequest)
        self.writeRequestArgs(webRequest)

        webRequest.write('<%s>' % self._verb)
        self.process(webRequest)
        webRequest.write('</%s>' % self._verb)

        self.writeFooter(webRequest)

    def preProcess(self, webRequest):
        """Hook"""
        pass

    def process(self, webRequest):
        """Hook"""
        pass

    def getTime(self):
        return strftime('%Y-%m-%dT%H:%M:%SZ', gmtime())

    def getRequestUrl(self, webRequest):
        return 'http://%s:%s' % (webRequest.getRequestHostname(), webRequest.getHost().port) + webRequest.path

    def writeHeader(self, webRequest):
        webRequest.setHeader('content-type', 'text/xml; charset=utf-8')
        webRequest.write(OAIHEADER)
        webRequest.write(RESPONSE_DATE % self.getTime())

    def writeRequestArgs(self, webRequest):
        url = self.getRequestUrl(webRequest)
        args = ' '.join(['%s="%s"' % (xmlEscape(k), xmlEscape(v[0]).replace('"', '&quot;')) for k,v in sorted(webRequest.args.items())])
        webRequest.write(REQUEST % locals())

    def writeError(self, webRequest, statusCode, additionalMessage = '', echoArgs = True):
        for line in compose(oaiError(statusCode, additionalMessage, arguments=webRequest.args, **webRequest.kwargs)):
            webRequest.write(line)
        #        space = additionalMessage and ' ' or ''
        #        message = ERROR_CODES[statusCode] + space + additionalMessage
        #        self.writeHeader(webRequest)
        #        url = self.getRequestUrl(webRequest)
        #        if statusCode in ["badArgument", "badResumptionToken", "badVerb"]:
        #            """in these cases it is illegal to echo the arguments back; since the arguments are not valid in the first place the responce will not validate either"""
        #            args = ''
        #            webRequest.write(REQUEST % locals())
        #        else:
        #            self.writeRequestArgs(webRequest)
        #        webRequest.write("""<error code="%(statusCode)s">%(message)s</error>""" % locals())
        #        self.writeFooter(webRequest)
        return statusCode

    def writeFooter(self, webRequest):
        webRequest.write(OAIFOOTER)



