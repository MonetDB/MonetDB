# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

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

    def __contains__(self,key):
        return key in self._values

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
