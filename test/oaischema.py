## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2011 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2010 Maastricht University Library http://www.maastrichtuniversity.nl/web/Library/home.htm
# Copyright (C) 2011, 2020-2021 Stichting Kennisnet https://www.kennisnet.nl
# Copyright (C) 2012, 2020-2021 Seecr (Seek You Too B.V.) https://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
# Copyright (C) 2020-2021 Data Archiving and Network Services https://dans.knaw.nl
# Copyright (C) 2020-2021 SURF https://www.surf.nl
# Copyright (C) 2020-2021 The Netherlands Institute for Sound and Vision https://beeldengeluid.nl
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

from lxml.etree import parse, XMLSchema, XMLSchemaParseError
from meresco.components import lxmltostring
from io import BytesIO
from os.path import join, dirname, abspath
from glob import glob

schemaLocation = join(abspath(dirname(__file__)), 'schemas')

rootSchema = '<?xml version="1.0" encoding="utf-8"?><schema targetNamespace="http://www.meresco.org/XML" \
            xmlns="http://www.w3.org/2001/XMLSchema" \
            elementFormDefault="qualified">\n' \
 + '\n'.join('<import namespace="%s" schemaLocation="%s"/>' %
    (parse(xsd).getroot().get('targetNamespace'), xsd)
        for xsd in glob(join(schemaLocation,'*.xsd'))) \
+ '</schema>'

schemaXml = parse(BytesIO(rootSchema.encode()))

schema = None

def getSchema():
    global schema
    if not schema:
        try:
            schema = XMLSchema(schemaXml)
        except XMLSchemaParseError as e:
            print(e.error_log.last_error)
            raise
    return schema

def assertValidOai(lxmlTree=None, aXmlString=None):
    schema = getSchema()
    aXmlString = lxmltostring(lxmlTree, pretty_print=True) if aXmlString == None else aXmlString
    tree = parse(BytesIO(aXmlString.encode()))
    schema.validate(tree)
    if schema.error_log:
        for nr, line in enumerate(aXmlString.split('\n')):
            print(nr+1, line)
        raise AssertionError(schema.error_log.last_error)
    return tree
