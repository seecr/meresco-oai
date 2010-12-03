#!/usr/bin/env python
# -*- coding: utf-8 -*-
## begin license ##
#
#    Meresco Oai are components to build Oai repositories, based on Meresco
#    Core and Meresco Components.
#    Copyright (C) 2010 Seek You Too (CQ2) http://www.cq2.nl
#    Copyright (C) 2010 Stichting Kennisnet http://www.kennisnet.nl
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

from os import system                             #DO_NOT_DISTRIBUTE
from sys import path as sysPath                   #DO_NOT_DISTRIBUTE
system('find .. -name "*.pyc" | xargs rm -f')     #DO_NOT_DISTRIBUTE
                                                  #DO_NOT_DISTRIBUTE
from glob import glob                             #DO_NOT_DISTRIBUTE
for path in glob('../deps.d/*'):                  #DO_NOT_DISTRIBUTE
    sysPath.insert(0, path)                       #DO_NOT_DISTRIBUTE
sysPath.insert(0,'..')                            #DO_NOT_DISTRIBUTE

import sys
from os import listdir, remove, rename
from os.path import join, isdir
from meresco.components.facetindex import IntegerList

def convert(path):
    iList = IntegerList(0, use64bits=True)
    iList.extendFrom(path)
    iListDeleted = IntegerList(0, use64bits=True)
    iListDeleted.extendFrom(path + '.deleted')
    deleted = sorted(iListDeleted, reverse=True)
    for position in deleted:
        del iList[position]
    iList.save(path + '~', offset=0, append=False)
    remove(path + '.deleted')
    rename(path + '~', path)

def convertDir(directory):
    for path in listdir(directory):
        fullpath = join(directory, path)
        if path.endswith('.list'):
            convert(fullpath)
        if isdir(fullpath):
            convertDir(fullpath)

def main():
    if len(sys.argv) != 2:
        print 'Usage: %s [OAI directory]' % sys.argv[0]
        exit(1)
    directory = sys.argv[1]
    convertDir(directory)
    print "Finished converting %s to OAI data format v2." % directory

if __name__ == '__main__':
    main()
