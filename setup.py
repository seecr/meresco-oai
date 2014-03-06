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
# Copyright (C) 2012-2014 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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
from os import walk
from os.path import join

data_files = []
for path, dirs, files in walk('usr-share'):
        data_files.append((path.replace('usr-share', '/usr/share/meresco-oai', 1), [join(path, f) for f in files]))

packages = []
for path, dirs, files in walk('meresco'):
    if '__init__.py' in files and path != 'meresco':
        packages.append(path.replace('/', '.'))

scripts = []
for path, dirs, files in walk('bin'):
    for file in files:
        scripts.append(join(path, file))

setup(
    name = 'meresco-oai',
    packages = [
        'meresco',                  #DO_NOT_DISTRIBUTE
    ] + packages,
    package_data={
        'meresco.oai.info': [
            'dynamic/*.sf'
        ]
    },
    scripts=scripts,
    data_files=data_files,
    version = '%VERSION%',
    url = 'http://www.cq2.nl',
    author = 'Seek You Too',
    author_email = 'info@cq2.nl',
    description = 'Meresco Oai are components to build Oai repositories, based on Meresco Core and Meresco Components.',
    long_description = 'Meresco Oai are components to build Oai repositories, based on Meresco Core and Meresco Components.',
    license = 'GPL',
    platforms='all',
)
