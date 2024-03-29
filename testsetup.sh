#!/bin/bash
## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2012-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

set -o errexit
rm -rf tmp build
mydir=$(cd $(dirname $0); pwd)
source /usr/share/seecr-tools/functions.d/test

definePythonVars
$PYTHON setup.py install --root tmp
removeDoNotDistribute tmp
cp -r test tmp/test

VERSION="x.y.z"
find tmp -name '*.py' -exec sed -r -e "
    s,^binDir.*$,binDir='${SEECRTEST_USR_BIN}',;
    s/\\\$Version:[^\\\$]*\\\$/\\\$Version: ${VERSION}\\\$/;
    " -i '{}' \;

if [ -z "$@" ]; then
    runtests "alltests.sh"
else
    runtests "$@"
fi
rm -rf tmp build
