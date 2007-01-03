#! /bin/sh

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

# fetches the source of the first given argument

if [ ! $1 ]; then
	echo "$0 expects an argument, ie. $0 http://monetdb.cwi.nl/" > /dev/stderr
	exit -1
fi

if [ `which lynx >& /dev/null; echo $?` -eq 0 ]; then
	lynx -source $1
elif [ `which links >& /dev/null; echo $?` -eq 0 ]; then
	links -source $1
elif [ `which curl >& /dev/null; echo $?` -eq 0 ]; then
	# most sun/apple systems are shipped with this
	curl -s $1
elif [ `which wget >& /dev/null; echo $?` -eq 0 ]; then
	# wget is a more sophisticated and friendly version of curl
	wget -q -O - $1
else
	echo "No available method for fetching HTTP data found" > /dev/stderr
	exit -1
fi
