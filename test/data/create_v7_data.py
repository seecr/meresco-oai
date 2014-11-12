#!/usr/bin/env python
## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2014 Netherlands Institute for Sound and Vision http://instituut.beeldengeluid.nl/
# Copyright (C) 2014 Seecr (Seek You Too B.V.) http://seecr.nl
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

from meresco.oai import OaiJazz

def main():
    if OaiJazz.version != '7':
        print 'Please run with version 7 of OaiJazz'
        exit(1)

    j = OaiJazz('oai_conversion_v7_to_v8')
    j.addOaiRecord('oai:1', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:2', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:3', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:4', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:5', sets=[('setSpec', 'setName')], metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:4', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:4', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:4', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    j.addOaiRecord('oai:4', metadataFormats=[('oai_dc', 'schema', 'namespace')])
    list(j.delete('oai:2'))
    j.close()

if __name__ == '__main__':
    main()