# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

class var:
    def __init__(self,name):
        self._name = name
        self._values = []

    def append( self, value ):
        self._values.append(value)

    def has_val(self,val):
        return val in self._values

    def __repr__(self):
        res = self._name + ": " + repr(self._values)
        return res

class groupvar(var):
    def __init__(self,name):
        self._name = name
        self._values = {}

    def add(self,key,value):
        self._values[key] = value

    def keys(self):
        return self._values.keys()

    def items(self):
        return self._values.items()

    def has_key(self,key):
        return self._values.has_key(key)

    def __getitem__(self,key):
        return self._values[key]

    def __setitem__(self,key,value):
        self._values[key] = value

    def __delitem__(self,key):
        del self._values[key]

    def copy(self):
        g = groupvar(self._name)
        g._values = self._values.copy()
        return g
