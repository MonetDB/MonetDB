# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the Monet Database System.
# 
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2004 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

import os
import fileinput
import string

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR)
DATADIR = os.path.join(SRCDIR,"SF-0.01")
SQL_CLIENT = os.environ['SQL_CLIENT']

f = open("load.sql","w")
for i in fileinput.input(os.path.join(SRCDIR,"load.sql")):
    x = string.split(i,"PWD/")
    if len(x) == 2:
        f.write(x[0]+DATADIR+os.sep+x[1])
    if len(x) == 1:
    	f.write(x[0])
f.close()

CALL = SQL_CLIENT+" < load.sql"

os.system("Mlog '%s'" % CALL)
os.system(CALL)

