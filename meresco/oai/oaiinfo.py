## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
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


class OaiInfo(Observable):
    def __init__(self, reactor, **kwargs):
        Observable.__init__(self, **kwargs)
        self._dynamicHtml = DynamicHtml([dynamicPath],
                reactor=reactor,
                notFoundPage='notFound',
                additionalGlobals={
                    'VERSION': version,
                    'allCoreInfo': self._allCoreInfo,
                    'parseCql': parseString,
                    'cql2string': cql2string,
                    'dumps': dumps,
                }
            )
        self._internalTree = be((Observable(),
            (PathFilter('/', excluding=['/static', '/version']),
                (self._dynamicHtml,)
            ),
            (PathFilter('/static'),
                (PathRename(lambda path: path[len('/static'):]),
                    (FileServer(staticPath),)
                )
            ),
            (PathFilter('/version'),
                (StringServer("Meresco Lucene version %s" % version, ContentTypePlainText),)
            ),
        ))

    def addObserver(self, *args, **kwargs):
        Observable.addObserver(self, *args, **kwargs)
        self._dynamicHtml.addObserver(*args, **kwargs)

    def addStrand(self, *args, **kwargs):
        Observable.addStrand(self, *args, **kwargs)
        self._dynamicHtml.addStrand(*args, **kwargs)

    def handleRequest(self, path, Method, Body=None, **kwargs):
        if Method == 'POST' and path.endswith('/__lucene_remote__'):
            yield self._handleQuery(Body)
        elif '/info' in path:
            originalPath = path
            _, _, path = path.partition('/info')
            yield self._internalTree.all.handleRequest(path=path or '/', originalPath=originalPath, Method=Method, Body=Body, **kwargs)

    def _handleQuery(self, Body):
        try:
            message, kwargs = jsonLoadMessage(Body)
            if message not in _ALLOWED_METHODS:
                raise ValueError('Expected %s' % (' or '.join('"%s"' % m for m in _ALLOWED_METHODS)))
            response = yield self.any.unknown(message=message, **kwargs)
        except Exception, e:
            x = format_exc() # returns 'None' if e is a Java Error
            yield serverErrorPlainText
            yield x if x and x.strip() != 'None' else repr(e)
            return
        yield Ok
        yield ContentTypeHeader + 'application/json' + CRLF
        yield CRLF
        yield response.asJson()

    def _allCoreInfo(self):
        return list(compose(self.all.coreInfo()))
)