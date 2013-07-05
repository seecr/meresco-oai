# -*- coding: utf-8 -*-
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
# Copyright (C) 2010-2011 Stichting Kennisnet http://www.kennisnet.nl
# Copyright (C) 2011-2013 Seecr (Seek You Too B.V.) http://seecr.nl
# Copyright (C) 2012 Stichting Bibliotheek.nl (BNL) http://www.bibliotheek.nl
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

import sys
from sys import maxint
from os.path import isdir, join, isfile
from os import makedirs, listdir, rename, remove
from bisect import bisect_left
from time import time, strftime, gmtime, strptime
from calendar import timegm
from json import dumps, load as jsonLoad
from bsddb import btopen
from traceback import print_exc
from random import choice

from escaping import escapeFilename, unescapeFilename
from meresco.components.sorteditertools import OrIterator, AndIterator
from meresco.components import PersistentSortedIntegerList
from meresco.core import asyncreturn
from weightless.io import Suspend


class OaiJazz(object):

    version = '4'

    def __init__(self, aDirectory, alwaysDeleteInPrefixes=None, preciseDatestamp=False, persistentDelete=True, maximumSuspendedConnections=100, autoCommit=True, name=None):
        self._directory = _ensureDir(aDirectory)
        self._versionFormatCheck()
        self._deletePrefixes = alwaysDeleteInPrefixes or []
        self._preciseDatestamp = preciseDatestamp
        self._persistentDelete = persistentDelete
        self._maximumSuspendedConnections = maximumSuspendedConnections
        self._autoCommit = autoCommit
        self._name = name
        self._suspended = {}

        self._identifierDict = btopen(join(aDirectory, 'stamp2identifier2setSpecs.bd'))
        self._tombStones = PersistentSortedIntegerList(
            join(self._directory, 'tombStones.list'),
            autoCommit=self._autoCommit)
        self._prefixesInfoDir = _ensureDir(join(aDirectory, 'prefixesInfo'))
        self._prefixesDir = _ensureDir(join(aDirectory, 'prefixes'))
        self._prefixes = {}
        self._setsDir = _ensureDir(join(aDirectory, 'sets'))
        self._sets = {}
        self._newestStamp = 0
        self._changeFile = join(self._directory, 'change.json')
        self._read()

    def observable_name(self):
        return self._name

    def addOaiRecord(self, identifier, sets=None, metadataFormats=None):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        identifier = safeString(identifier)
        sets = sets or []
        metadataFormats = metadataFormats or []
        assert [prefix for prefix, schema, namespace in metadataFormats], 'No metadataFormat specified for record with identifier "%s"' % identifier
        for setSpec, setName in sets:
            assert SETSPEC_SEPARATOR not in setSpec, 'SetSpec "%s" contains illegal characters' % setSpec

        newStamp = self._newStamp()
        self._storeMetadataFormats(metadataFormats)
        oldStamp, oldPrefixes, oldSets = self._lookupExisting(identifier)
        prefixes = set(prefix for prefix, schema, namespace in metadataFormats)
        prefixes.update(oldPrefixes)
        setSpecs = _flattenSetHierarchy((setSpec for setSpec, setName in sets))
        setSpecs.update(oldSets)
        self._applyChange(
            identifier=identifier,
            oldStamp=oldStamp,
            oldSets=list(oldSets),
            newStamp=newStamp,
            delete=False,
            prefixes=list(prefixes),
            newSets=list(setSpecs))
        self._resume()

    @asyncreturn
    def delete(self, identifier):
        if not identifier:
            raise ValueError("Empty identifier not allowed.")
        identifier = safeString(identifier)
        oldStamp, oldPrefixes, oldSets = self._lookupExisting(identifier)
        if not oldStamp and not self._deletePrefixes:
            return
        self._applyChange(
            identifier=identifier,
            oldStamp=oldStamp,
            oldSets=list(oldSets),
            newStamp=self._newStamp(),
            delete=True,
            prefixes=list(set(oldPrefixes + self._deletePrefixes)),
            newSets=list(oldSets))
        self._resume()

    def purge(self, identifier):
        if self._persistentDelete:
            raise KeyError("Purging of records is not allowed with persistent deletes.")
        identifier = safeString(identifier)
        oldStamp, oldPrefixes, oldSets = self._lookupExisting(identifier)
        if not oldStamp:
            return
        self._applyChange(
            identifier=identifier,
            oldStamp=oldStamp,
            oldSets=list(oldSets),
            newStamp=None)

    def oaiSelect(self, sets=None, prefix='oai_dc', continueAfter='0', oaiFrom=None, oaiUntil=None, setsMask=None):
        setsMask = setsMask or []
        sets = sets or []
        start = max(int(continueAfter)+1, self._fromTime(oaiFrom))
        stop = self._untilTime(oaiUntil)
        stampIds = self._sliceStampIds(self._prefixes.get(prefix, []), start, stop)
        setsStampIds = dict(
            (setSpec, self._sliceStampIds(self._sets.get(setSpec, []), start, stop))
            for setSpec in set(setsMask).union(sets)
        )
        if setsMask:
            stampIds = AndIterator(stampIds,
                reduce(AndIterator, (setsStampIds[setSpec] for setSpec in setsMask)))
        if sets:
            stampIds = AndIterator(stampIds,
                reduce(OrIterator, (setsStampIds[setSpec] for setSpec in sets)))
        idAndStamps = ((self._getIdentifier(stampId), stampId) for stampId in stampIds)
        return (RecordId(identifier, stampId) for identifier, stampId in idAndStamps if not identifier is None)

    def getDatestamp(self, identifier):
        stamp = self.getUnique(identifier)
        if stamp is None:
            return None
        return _stamp2zulutime(stamp=stamp, preciseDatestamp=self._preciseDatestamp)

    def getUnique(self, identifier):
        if hasattr(identifier, 'stamp'):
            return identifier.stamp
        return self._getStamp(identifier)

    def isDeleted(self, identifier):
        stamp = self.getUnique(identifier)
        if stamp is None:
            return False
        return stamp in self._tombStones

    def getAllMetadataFormats(self):
        for prefix, schema, namespace in self._metadataFormats.values():
            yield (prefix, schema, namespace)

    def getAllPrefixes(self):
        return self._prefixes.keys()

    def getSets(self, identifier):
        identifier = safeString(identifier)
        value = self._identifierDict.get(IDENTIFIER2SETSPEC + identifier)
        return value.split(SETSPEC_SEPARATOR) if value else []

    def getPrefixes(self, identifier):
        stamp = self.getUnique(safeString(identifier))
        if not stamp:
            return []
        return (prefix for prefix, stampIds in self._prefixes.items() if stamp in stampIds)

    def getAllSets(self):
        return self._sets.keys()

    def getNrOfRecords(self, prefix='oai_dc'):
        return len(self._prefixes.get(prefix, []))

    def getLastStampId(self, prefix='oai_dc'):
        if prefix in self._prefixes and self._prefixes[prefix]:
            stampIds = self._prefixes[prefix]
            return stampIds[-1] if stampIds else None

    def getDeletedRecordType(self):
        return "persistent" if self._persistentDelete else "transient"

    def suspend(self, clientIdentifier):
        suspend = Suspend()
        if clientIdentifier in self._suspended:
            self._suspended.pop(clientIdentifier).throw(exc_type=ValueError, exc_value=ValueError("Aborting suspended request because of new request for the same OaiClient with identifier: %s." % clientIdentifier), exc_traceback=None)
        if len(self._suspended) == self._maximumSuspendedConnections:
            self._suspended.pop(choice(self._suspended.keys())).throw(ForcedResumeException, ForcedResumeException("OAI x-wait connection has been forcefully resumed."), None)
            sys.stderr.write("Too many suspended connections in OaiJazz. One random connection has been resumed.\n")
        self._suspended[clientIdentifier] = suspend
        yield suspend
        suspend.getResult()

    def commit(self):
        self._identifierDict.sync()
        for l in self._sets.values() + self._prefixes.values():
            l.commit()

    def handleShutdown(self):
        self.commit()

    # private methods

    def _read(self):
        self._metadataFormats = {}
        for prefix in (unescapeFilename(name[:-len('.list')]) for name in listdir(self._prefixesDir) if name.endswith('.list')):
            self._getPrefixList(prefix)
            schema = open(join(self._prefixesInfoDir, '%s.schema' % escapeFilename(prefix))).read()
            namespace = open(join(self._prefixesInfoDir, '%s.namespace' % escapeFilename(prefix))).read()
            self._metadataFormats[prefix] = (prefix, schema, namespace)
        for setSpec in (unescapeFilename(name[:-len('.list')]) for name in listdir(self._setsDir) if name.endswith('.list')):
            self._getSetList(setSpec)

    def _getSetList(self, setSpec):
        if setSpec not in self._sets:
            filename = join(self._setsDir, '%s.list' % escapeFilename(setSpec))
            l = self._sets[setSpec] = PersistentSortedIntegerList(filename, autoCommit=self._autoCommit)
            self._removeInvalidStamp(l)
            self._newestStampFromList(l)
        return self._sets[setSpec]

    def _getPrefixList(self, prefix):
        if prefix not in self._prefixes:
            filename = join(self._prefixesDir, '%s.list' % escapeFilename(prefix))
            l = self._prefixes[prefix] = PersistentSortedIntegerList(filename, autoCommit=self._autoCommit)
            self._removeInvalidStamp(l)
            self._newestStampFromList(l)
        return self._prefixes[prefix]

    def _newestStampFromList(self, l):
        if len(l):
            self._newestStamp = max(self._newestStamp, l[-1])

    def _lookupExisting(self, identifier):
        stamp = self.getUnique(identifier)
        oldPrefixes = []
        oldSets = []
        if not stamp is None:
            for prefix, prefixStamps in self._prefixes.items():
                if stamp in prefixStamps:  # Relatively expensive...
                    oldPrefixes.append(prefix)
            oldSets = [
                setSpec
                for setSpec in self._identifierDict.get(IDENTIFIER2SETSPEC + identifier, '').split(SETSPEC_SEPARATOR)
                if setSpec]
        return stamp, oldPrefixes, oldSets

    def _applyChange(self, identifier, oldStamp=None, oldSets=None, newStamp=None, delete=False, prefixes=None, newSets=None):
        identifier = safeString(identifier)
        try:
            if not oldStamp is None:
                self._purge(identifier, oldStamp, oldSets)
            if not newStamp is None:
                self._add(identifier, newStamp, prefixes, newSets)
                if delete:
                    self._appendIfNotYet(newStamp, self._tombStones)
            else:
                self._purge(identifier, oldStamp, oldSets)
            if self._autoCommit:
                self._identifierDict.sync()
            if not oldStamp is None:
                self._purgeLists(identifier, oldStamp, oldSets)
        except:
            print_exc()
            raise SystemExit("OaiJazz: FATAL error committing change to disk.")

    def _purge(self, identifier, oldStamp, oldSets):
        self._identifierDict.pop(STAMP2IDENTIFIER + "id:" + identifier, None)
        self._identifierDict.pop(IDENTIFIER2SETSPEC + identifier, None)
        self._identifierDict.pop(STAMP2IDENTIFIER + str(oldStamp), None)

    def _purgeLists(self, identifier, oldStamp, oldSets):
        self._removeIfInList(oldStamp, self._tombStones)
        for prefix, prefixStamps in self._prefixes.items():
            self._removeIfInList(oldStamp, prefixStamps)
        for setSpec in oldSets:
            self._removeIfInList(oldStamp, self._sets[setSpec])

    def _add(self, identifier, newStamp, prefixes, newSets):
        self._newestStamp = newStamp
        self._identifierDict[STAMP2IDENTIFIER + "id:" + identifier] = str(newStamp)
        self._identifierDict[STAMP2IDENTIFIER + str(newStamp)] = identifier
        for prefix in prefixes:
            self._appendIfNotYet(newStamp, self._getPrefixList(prefix))
        for setSpec in newSets:
            self._appendIfNotYet(newStamp, self._getSetList(setSpec))
        if newSets:
            self._identifierDict[IDENTIFIER2SETSPEC + identifier] = SETSPEC_SEPARATOR.join(newSets)

    def _sliceStampIds(self, stampIds, start, stop):
        if stop:
            return stampIds[bisect_left(stampIds, start):bisect_left(stampIds, stop)]
        return stampIds[bisect_left(stampIds, start):]

    def _fromTime(self, oaiFrom):
        if not oaiFrom:
            return 0
        return self._timeToNumber(oaiFrom)

    def _untilTime(self, oaiUntil):
        if not oaiUntil:
            return None
        UNTIL_IS_INCLUSIVE = 1 # Add one second to 23:59:59
        return self._timeToNumber(oaiUntil) + UNTIL_IS_INCLUSIVE

    @staticmethod
    def _timeToNumber(time):
        try:
            return int(timegm(strptime(time, '%Y-%m-%dT%H:%M:%SZ')) * DATESTAMP_FACTOR)
        except (ValueError, OverflowError):
            return maxint * DATESTAMP_FACTOR

    def _getIdentifier(self, stamp):
        return self._identifierDict.get(STAMP2IDENTIFIER + str(stamp), None)

    def _getStamp(self, identifier):
        result = self._identifierDict.get(STAMP2IDENTIFIER + "id:" + safeString(identifier), None)
        if result != None:
            result = int(result)
        return result

    def _storeMetadataFormats(self, metadataFormats):
        for prefix, schema, namespace in metadataFormats:
            if (prefix, schema, namespace) != self._metadataFormats.get(prefix):
                self._metadataFormats[prefix] = (prefix, schema, namespace)
                _write(join(self._prefixesInfoDir, '%s.schema' % escapeFilename(prefix)), schema)
                _write(join(self._prefixesInfoDir, '%s.namespace' % escapeFilename(prefix)), namespace)

    def _newStamp(self):
        """time in microseconds"""
        newStamp = int(time() * DATESTAMP_FACTOR)
        if newStamp <= self._newestStamp:
            raise ValueError("Timestamp error: new stamp '%s' lower than existing ('%s')" % (newStamp, self._newestStamp))
        return newStamp

    def _versionFormatCheck(self):
        self._versionFile = join(self._directory, "oai.version")
        assert listdir(self._directory) == [] or (isfile(self._versionFile) and open(self._versionFile).read() == self.version), "The OAI index at %s need to be converted to the current version (with 'convert_oai_v3_to_v4' in meresco-oai/bin)" % self._directory
        with open(join(self._directory, "oai.version"), 'w') as f:
            f.write(self.version)

    def _resume(self):
        while len(self._suspended) > 0:
            clientId, suspend = self._suspended.popitem()
            suspend.resume()

    def _removeInvalidStamp(self, l):
        # Last or second last could be unused stamps due to crashes
        if l:
            for stamp in l[-2:]:
                if self._getIdentifier(str(stamp)) is None:
                    l.remove(stamp)

    def _removeIfInList(self, item, l):
        try:
            l.remove(item)
        except ValueError:
            pass

    def _appendIfNotYet(self, item, l):
        if len(l) == 0 or l[-1] != item:
            l.append(item)


# helper methods

class RecordId(str):
    def __new__(self, identifier, stamp):
        return str.__new__(self, identifier)
    def __init__(self, identifier, stamp):
        self.stamp = stamp
    def __getslice__(self, *args, **kwargs):
        return RecordId(str.__getslice__(self, *args, **kwargs), self.stamp)

def _writeLines(filename, lines):
    with open(filename + '.tmp', 'w') as f:
        for line in lines:
            f.write('%s\n' % line)
    rename(filename + '.tmp', filename)

def _write(filename, content):
    with open(filename + '.tmp', 'w') as f:
        f.write(content)
    rename(filename + '.tmp', filename)

def _flattenSetHierarchy(sets):
    """"[1:2:3, 1:2:4] => [1, 1:2, 1:2:3, 1:2:4]"""
    result = set()
    for setSpec in sets:
        parts = setSpec.split(':')
        for i in range(1, len(parts) + 1):
            result.add(':'.join(parts[:i]))
    return result

def safeString(aString):
    return str(aString) if isinstance(aString, unicode) else aString

def stamp2zulutime(stamp):
    if stamp is None:
        return ''
    return _stamp2zulutime(int(stamp))

def _stamp2zulutime(stamp, preciseDatestamp=False):
    microseconds = ".%s" % (stamp % DATESTAMP_FACTOR) if preciseDatestamp else ""
    return "%s%sZ" % (strftime('%Y-%m-%dT%H:%M:%S', gmtime(stamp / DATESTAMP_FACTOR)), microseconds)

def _ensureDir(directory):
    isdir(directory) or makedirs(directory)
    return directory


class ForcedResumeException(Exception):
    pass

SETSPEC_SEPARATOR = ','
DATESTAMP_FACTOR = 1000000

IDENTIFIER2SETSPEC = 'ss:'
STAMP2IDENTIFIER = 'st:'
