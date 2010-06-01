#!/bin/bash
set -e

rm -rf tmp build
python2.5 setup.py install --root tmp

cp meresco/__init__.py tmp/usr/lib/python2.5/site-packages/meresco
export PYTHONPATH=`pwd`/tmp/usr/lib/python2.5/site-packages
cp -r test tmp/test

(
cd tmp/test
./alltests.sh
)

rm -rf tmp build
