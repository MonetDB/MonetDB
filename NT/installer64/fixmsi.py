#!/usr/bin/python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

# Fix a .msi (Windows Installer) file for a 64-bit registry search.
# Microsoft refuses to fix a bug in Visual Studio so that for a 64-bit
# build, the registry search will look in the 32-bit part of the
# registry instead of the 64-bit part of the registry.  This script
# fixes the .msi to look in the correct part.

import msilib
import sys
import glob

def fixmsi(f):
    db = msilib.OpenDatabase(f, msilib.MSIDBOPEN_DIRECT)
    v = db.OpenView('UPDATE RegLocator SET Type = 18 WHERE Type = 2')
    v.Execute(None)
    v.Close()
    db.Commit()

if __name__ == '__main__':
    for f in sys.argv[1:]:
        for g in glob.glob(f):
            fixmsi(g)
