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
# Portions created by CWI are Copyright (C) 1997-2007 CWI.
# All Rights Reserved.

#
# ! this file should be kept identical in                                               !
# ! MonetDB, MonetDB4, monet5, clients, template, sql, pathfinder, amdb, xml, pruning !
#

if [ -n "$BUILDTOOLS_PREFIX" ]; then
    CONFBASH="$BUILDTOOLS_PREFIX"/share/MonetDB/conf.bash
else
    BOOTSTRAP=`which Mbootstrap 2>/dev/null`
    if [ -z "$BOOTSTRAP" ]; then
	echo "$0: cannot find MonetDB buildtools installation"
	return 1
    fi
    CONFBASH="${BOOTSTRAP%/bin/Mbootstrap}"/share/MonetDB/conf.bash
fi

if [ ! -f "$CONFBASH" ]; then
    echo "$0: cannot find conf.bash from installed buildtools"
    return 1
fi

source "$CONFBASH"
