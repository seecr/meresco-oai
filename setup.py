# -*- coding: utf-8 -*-
## begin license ##
#
# "Meresco Oai" are components to build Oai repositories, based on
# "Meresco Core" and "Meresco Components".
#
# Copyright (C) 2007-2008 SURF Foundation. http://www.surf.nl
# Copyright (C) 2007-2010 Seek You Too (CQ2) http://www.cq2.nl
# Copyright (C) 2007-2009 Stichting Kennisnet Ict op school. http://www.kennisnetictopschool.nl
# Copyright (C) 2009-2010 Delft University of Technology http://www.tudelft.nl
# Copyright (C) 2009 Tilburg University http://www.uvt.nl
# Copyright (C) 2012-2014 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

from distutils.core import setup

setup(
    name = 'meresco-oai4',
    packages = [
        'meresco',                  #DO_NOT_DISTRIBUTE
        'meresco.oai4',
    ],
    scripts=[
        'bin/convert_oai_v1_to_v2',
        'bin/convert_oai_v2_to_v3',
        'bin/convert_oai_v3_to_v4',
    ],
    version = '%VERSION%',
    url = 'http://www.cq2.nl',
    author = 'Seek You Too',
    author_email = 'info@cq2.nl',
    description = 'Meresco Oai are components to build Oai repositories, based on Meresco Core and Meresco Components.',
    long_description = 'Meresco Oai are components to build Oai repositories, based on Meresco Core and Meresco Components.',
    license = 'GPL',
    platforms='all',
)
