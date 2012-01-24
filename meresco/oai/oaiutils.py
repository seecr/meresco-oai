## begin license ##
# 
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components". 
# 
# Copyright (C) 2011 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2011 Stichting Kennisnet http://www.kennisnet.nl
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

from time import strftime, gmtime
from socket import gethostname
from meresco.components.http.utils import okXml
from xml.sax.saxutils import escape as xmlEscape

HOSTNAME = gethostname()

class OaiException(Exception):
    def __init__(self, statusCode, additionalMessage=""):
        Exception.__init__(self, additionalMessage)
        self.additionalMessage = additionalMessage
        self.statusCode = statusCode

class OaiBadArgumentException(OaiException):
    def __init__(self, additionalMessage):
        OaiException.__init__(self, 'badArgument', additionalMessage)

def zuluTime():
    return strftime('%Y-%m-%dT%H:%M:%SZ', gmtime())

def requestUrl(Headers, path, port, **kwargs):
    hostname = Headers.get('Host', HOSTNAME).split(':')[0]
    return 'http://%s:%s%s' % (hostname, port, path)

def oaiHeader():
    yield okXml
    yield OAIHEADER
    yield RESPONSE_DATE % zuluTime()

def oaiFooter():
    yield OAIFOOTER

def oaiRequestArgs(arguments, **httpkwargs):
    url = requestUrl(**httpkwargs)
    args = ' '.join(['%s="%s"' % (xmlEscape(k), xmlEscape(v[0]).replace('"', '&quot;')) for k,v in sorted(arguments.items())])
    yield REQUEST % locals()


def doElementaryArgumentsValidation(arguments, argsDef):
    validatedArguments = {}

    checkNoRepeatedArguments(arguments)
    exclusiveArguments = _select('exclusive', argsDef)
    for exclusiveArgument in exclusiveArguments:
        if exclusiveArgument in arguments.keys():
            if set(arguments.keys()) != set(['verb', exclusiveArgument]):
                raise OaiBadArgumentException('"%s" argument may only be used exclusively.' % exclusiveArgument)
            validatedArguments[exclusiveArgument] = arguments[exclusiveArgument][0]
            return validatedArguments
        else:
            validatedArguments[exclusiveArgument] = None

    missing = []
    for requiredArgument in _select('required', argsDef):
        if not requiredArgument in arguments.keys():
            missing.append(requiredArgument)
        else:
            validatedArguments[requiredArgument] = arguments[requiredArgument][0]
    quote = lambda l: (map(lambda s: '"%s"' % s, l))
    if missing:
        raise OaiBadArgumentException('Missing argument(s) ' + \
            " or ".join(quote(exclusiveArguments) + \
            [" and ".join(quote(missing))]) + ".")

    for optionalArgument in _select('optional', argsDef):
        validatedArguments[optionalArgument] = arguments.get(optionalArgument, [None])[0]

    tooMuch = set(arguments.keys()).difference(argsDef.keys() + ['verb'])
    if tooMuch:
        raise OaiBadArgumentException('Argument(s) %s is/are illegal.' % ", ".join(map(lambda s: '"%s"' %s, tooMuch)))

    return validatedArguments

def checkNoRepeatedArguments(arguments):
    for k, v in arguments.items():
        if len(v) > 1:
            raise OaiBadArgumentException('Argument "%s" may not be repeated.' % k)

def checkNoMoreArguments(arguments):
    if len(arguments) > 0:
        raise OaiBadArgumentException('Argument(s) ' +\
                ', '.join('"%s"' % t for t in arguments.keys()) +\
                ' is/are illegal.')

def checkArgument(arguments, name, validatedArguments):
    try:
        value = arguments.pop(name)
    except KeyError:
        return False
    validatedArguments[name] = value[0]
    return True

def _select(neededNess, argsDef):
    result = []
    for arg, definition in argsDef.items():
        if definition == neededNess:
            result.append(arg)
    return result

def validSetSpecName(name):
    return len([l for l in name if not l.isalnum() and not l in "-_.!~*'()"]) == 0

OAIHEADER = '<?xml version="1.0" encoding="UTF-8"?>\n' +\
    '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" ' +\
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' +\
    'xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">'

RESPONSE_DATE = """<responseDate>%s</responseDate>"""

REQUEST = """<request %(args)s>%(url)s</request>"""

OAIFOOTER = """</OAI-PMH>"""


