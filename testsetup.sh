#!/bin/bash
set -e
mydir=$(cd $(dirname $0); pwd)

fullPythonVersion=python2.6

VERSION="x.y.z"

rm -rf tmp build
${fullPythonVersion} setup.py install --root tmp

cp -r test tmp/test
find tmp -type f -exec sed -r -e \
    "/DO_NOT_DISTRIBUTE/d;
    s,^binDir.*$,binDir='$mydir/tmp/usr/local/bin',;
    s/\\\$Version:[^\\\$]*\\\$/\\\$Version: ${VERSION}\\\$/" -i {} \;

cp meresco/__init__.py tmp/usr/local/lib/${fullPythonVersion}/dist-packages/meresco
export PYTHONPATH=`pwd`/tmp/usr/local/lib/${fullPythonVersion}/dist-packages:${PYTHONPATH}

set +o errexit
(
cd tmp/test
./alltests.sh
)
set -o errexit

rm -rf tmp build
