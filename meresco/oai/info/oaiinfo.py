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

from meresco.core import Observable, Transparent
from seecr.html import DynamicHtml
from os.path import abspath, dirname, join
from meresco.oai import VERSION
from meresco.components.http import StringServer, PathFilter, PathRename, FileServer
from meresco.components.http.utils import ContentTypePlainText
from weightless.core import be

from oaijsoninfo import OaiJsonInfo
from time import localtime, strftime

mydir = dirname(abspath(__file__))
dynamicPath = join(mydir, 'dynamic')
usrSharePath = '/usr/share/meresco-oai'
usrSharePath = join(dirname(dirname(dirname(mydir))), 'usr-share') #DO_NOT_DISTRIBUTE
staticPath = join(usrSharePath, 'oai-info', 'static')

class OaiInfo(Observable):
    def __init__(self, reactor, oaiPath, **kwargs):
        Observable.__init__(self, **kwargs)
        self._outside = Transparent()
        oaiJsonInfo = OaiJsonInfo()
        self._dynamicHtml = DynamicHtml([dynamicPath],
                reactor=reactor,
                notFoundPage='notFound',
                additionalGlobals={
                    'VERSION': VERSION,
                    'oaiPath': oaiPath,
                    'strftime': strftime,
                    'localtime': localtime,
                }
            )
        self._internalTree = be((Observable(),
            (PathFilter('/', excluding=['/static', '/version', '/json']),
                (self._dynamicHtml,
                    (oaiJsonInfo,
                        (self._outside,),
                    ),
                )
            ),
            (PathFilter('/json'),
                (oaiJsonInfo,
                    (self._outside,),
                )
            ),
            (PathFilter('/static'),
                (PathRename(lambda path: path[len('/static'):]),
                    (FileServer(staticPath),)
                )
            ),
            (PathFilter('/version'),
                (StringServer("Meresco Oai version %s" % VERSION, ContentTypePlainText),)
            ),
        ))

    def addObserver(self, *args, **kwargs):
        Observable.addObserver(self, *args, **kwargs)
        self._outside.addObserver(*args, **kwargs)

    def addStrand(self, *args, **kwargs):
        Observable.addStrand(self, *args, **kwargs)
        self._outside.addStrand(*args, **kwargs)

    def handleRequest(self, path, **kwargs):
        if '/info' in path:
            originalPath = path
            pathPrefix, _, path = path.rpartition('/info')
            yield self._internalTree.all.handleRequest(path=path or '/', originalPath=originalPath, pathPrefix=pathPrefix, **kwargs)

