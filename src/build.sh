#!/bin/bash
## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2013-2014, 2017 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2013-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
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

mydir=$(cd $(dirname $0); pwd)
buildDir=$mydir/build
libDir=$1
if [ -z "$libDir" ]; then
    libDir=$(dirname $mydir)/lib
fi

export PYTHONPATH=~/EG/pyl/dist/usr/local/lib/python3.7/dist-packages/:${PYTHONPATH}

pythonVersion=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f-2)
pythonPackagesDir=/usr/lib64/python${pythonVersion}/site-packages
if [ -f /etc/debian_version ]; then
    pythonPackagesDir=/usr/lib/python3/dist-packages
fi

#JCC_VERSION=3.6
#if ! grep -q "VERSION=\"${JCC_VERSION}\"" ${pythonPackagesDir}/jcc/config.py; then
#    echo "JCC ${JCC_VERSION} is required."
#    exit 1
#fi

JAVA_HOME=
test -f /etc/debian_version && JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
test -f /etc/redhat_version && JAVA_HOME=/usr/lib/jvm/java
if [ -z "${JAVA_HOME}" ]; then
    echo "Unable to determine JAVA_HOME"
    exit 0
fi

if [ ! -d "${JAVA_HOME}" ]; then
    echo "${JAVA_HOME} does not exist"
    exit 0
fi
export JAVA_HOME
javac=${JAVA_HOME}/bin/javac

luceneJarDir=${pythonPackagesDir}/lucene

LUCENE_VERSION=8.6.1
classpath=${luceneJarDir}/lucene-core-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-analyzers-common-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-facet-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-queries-${LUCENE_VERSION}.jar:${luceneJarDir}/lucene-misc-${LUCENE_VERSION}.jar

rm -rf $buildDir $libDir
mkdir --parents $buildDir $libDir

${javac} -cp ${classpath} -d ${buildDir} org/meresco/oai/*.java
(cd $buildDir; jar -c org > $buildDir/meresco-oai.jar)


python3 -m jcc.__main__ \
    --shared \
    --use_full_names \
    --import lucene \
    --arch x86_64 \
    --jar $buildDir/meresco-oai.jar \
    --python meresco_oai \
    --build \
    --install \
    --root $mydir/root 

rootLibDir=$mydir/root/usr/lib64/python${pythonVersion}/site-packages/meresco_oai
if [ -f /etc/debian_version ]; then
    rootLibDir=$mydir/root/usr/local/lib/python${pythonVersion}/dist-packages/meresco_oai
fi

mv ${rootLibDir} $libDir/

rm -rf $buildDir $mydir/root $mydir/meresco_oai.egg-info
