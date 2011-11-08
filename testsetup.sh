#!/bin/bash
set -e
mydir=$(cd $(dirname $0); pwd)

rm -rf tmp build
python2.5 setup.py install --root tmp

cp meresco/__init__.py tmp/usr/lib/python2.5/site-packages/meresco
export PYTHONPATH=`pwd`/tmp/usr/lib/python2.5/site-packages
cp -r test tmp/test
find tmp -type f -exec sed -e \
    "/DO_NOT_DISTRIBUTE/d;
    s,^binDir.*$,binDir='$mydir/tmp/usr/bin'," -i {} \;

(
cd tmp/test
./alltests.sh
)

rm -rf tmp build
