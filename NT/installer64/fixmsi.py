#!/usr/bin/python

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
# Copyright August 2008-2014 MonetDB B.V.
# All Rights Reserved.

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
