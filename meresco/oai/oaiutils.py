
from time import strftime, gmtime
from socket import gethostname
from meresco.components.http.utils import okXml

HOSTNAME = gethostname()

class OaiBadArgumentException(Exception):
    def __init__(self, additionalMessage):
        Exception.__init__(self, additionalMessage)
        self.additionalMessage = additionalMessage
        self.statusCode = 'badArgument'

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

def doElementaryArgumentsValidation(arguments, argsDef):
    validatedArguments = {}

    if _isArgumentRepeated(arguments):
        raise OaiBadArgumentException('Argument "%s" may not be repeated.' % _isArgumentRepeated(arguments))

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

def _isArgumentRepeated(arguments):
    for k, v in arguments.items():
        if len(v) > 1:
            return k
    return False

def _select(neededNess, argsDef):
    result = []
    for arg, definition in argsDef.items():
        if definition == neededNess:
            result.append(arg)
    return result

OAIHEADER = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
"""

RESPONSE_DATE = """<responseDate>%s</responseDate>"""

REQUEST = """<request %(args)s>%(url)s</request>"""

OAIFOOTER = """</OAI-PMH>"""


