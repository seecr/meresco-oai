## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from traceback import print_exc

from simplejson import dumps

from meresco.core import Observable

from meresco.oai import ResumptionToken


class OaiJsonInfo(Observable):
    def handleRequest(self, path, arguments, **kwargs):
        method = path.rpartition('/')[-1]
        method = 'info' if method in ['json', ''] else method
        yield 'HTTP/1.0 200 OK\r\n'
        yield 'Content-Type: application/json\r\n'
        yield '\r\n'
        try:
            yield dumps(getattr(self, method)(**arguments))
        except:
            print_exc()
            yield dumps({})

    def info(self):
        totalRecords = self.call.getNrOfRecords(prefix=None, setSpec=None)
        lastStamp = self.call.getLastStampId(prefix=None)
        return {'totalRecords': totalRecords, 'lastStamp': lastStamp}

    def sets(self):
        return list(sorted(self.call.getAllSets()))

    def set(self, set):
        setSpec = set[0]
        for spec, name in self.call.getAllSets(includeSetNames=True):
            if spec == setSpec:
                break
        nrOfRecords = self.call.getNrOfRecords(prefix=None, setSpec=setSpec)
        lastStamp = self.call.getLastStampId(prefix=None, setSpec=setSpec)
        return dict(setSpec=setSpec, name=name, nrOfRecords=nrOfRecords, lastStamp=lastStamp)

    def prefixes(self):
        return list(sorted(self.call.getAllPrefixes()))

    def prefix(self, prefix):
        prefix = prefix[0]
        for aPrefix, schema, namespace in self.call.getAllMetadataFormats():
            if aPrefix == prefix:
                break
        else:
            return {}
        nrOfRecords = self.call.getNrOfRecords(prefix=prefix, setSpec=None)
        lastStamp = self.call.getLastStampId(prefix=prefix, setSpec=None)
        return dict(prefix=prefix, schema=schema, namespace=namespace, nrOfRecords=nrOfRecords, lastStamp=lastStamp)

    def resumptiontoken(self, resumptionToken):
        resumptionToken = ResumptionToken.fromString(resumptionToken[0])
        kwargs = dict(
                prefix=resumptionToken.metadataPrefix or None,
                setSpec=resumptionToken.set_ if resumptionToken.set_ else None,
                oaiFrom=resumptionToken.from_,
                oaiUntil=resumptionToken.until
            )
        nrOfRecords = self.call.getNrOfRecords(**kwargs)
        nrOfRemainingRecords = self.call.getNrOfRecords(continueAfter=resumptionToken.continueAfter, **kwargs)

        return {
                'prefix': resumptionToken.metadataPrefix,
                'set': resumptionToken.set_ or None,
                'from': resumptionToken.from_ or None,
                'until': resumptionToken.until or None,
                'nrOfRecords': nrOfRecords or None,
                'nrOfRemainingRecords': nrOfRemainingRecords,
                'timestamp': int(resumptionToken.continueAfter),
            }