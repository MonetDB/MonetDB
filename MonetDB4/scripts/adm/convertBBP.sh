#!/bin/sh

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

#
# After patch gdk_bbp.dir revision 1.23.4.2
# on 2001/09/15 you'd loose existing persistent
# bats if you do not run this script;
#   Arjen - arjen@acm.org
#
DBFARM=$1
[ ! -n "$DBFARM" ] && { 
  echo Usage: $0 DBFARM
  exit -1 
}
#
# Note: 
#   refCnt in BBP.dir was usually 0, possibly 1.
#   existing nested persistent bats (latter case)
#   had problems; just in case people used this
#   anyway, we simply increment the old value
#   to 1 resp. 2.
#
cd $DBFARM
for db in *
do
  [ -d $db ] && echo Converting $db
  find $db -name BBP.dir | xargs tar uf $DBFARM/oldBBP.tar
  find $db -name BBP.dir | xargs \
    perl -npi -e 's/1\s+]/2 ]/g; s/0\s+]/1 ]/g'
done
echo backed up BBPs are stored in $DBFARM/oldBBP.tar

