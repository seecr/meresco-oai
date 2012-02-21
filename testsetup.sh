#!/bin/bash
set -e
mydir=$(cd $(dirname $0); pwd)

fullPythonVersion=python2.6

VERSION="x.y.z"

rm -rf tmp build
${fullPythonVersion} setup.py install --root tmp

if [ -f /etc/debian_version ]; then
    USR_DIR=`pwd`/tmp/usr/local
    SITE_PACKAGE_DIR=${USR_DIR}/lib/${fullPythonVersion}/dist-packages
else
    USR_DIR=`pwd`/tmp/usr
    SITE_PACKAGE_DIR=${USR_DIR}/lib/${fullPythonVersion}/site-packages
fi

cp meresco/__init__.py ${SITE_PACKAGE_DIR}/meresco
export PYTHONPATH=${SITE_PACKAGE_DIR}:${PYTHONPATH}

cp -r test tmp/test

find tmp -type f -exec sed -r -e \
    "/DO_NOT_DISTRIBUTE/d;
    s,^binDir.*$,binDir='${USR_DIR}/bin',;
    s/\\\$Version:[^\\\$]*\\\$/\\\$Version: ${VERSION}\\\$/" -i {} \;


set +o errexit
(
cd tmp/test
./alltests.sh
)
set -o errexit

rm -rf tmp build
