
from time import strftime, gmtime
from socket import gethostname
from meresco.components.http.utils import okXml

HOSTNAME = gethostname()

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

OAIHEADER = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
"""

RESPONSE_DATE = """<responseDate>%s</responseDate>"""

REQUEST = """<request %(args)s>%(url)s</request>"""

OAIFOOTER = """</OAI-PMH>"""


