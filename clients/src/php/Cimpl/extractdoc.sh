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

# Fabian Groffen, May 8th 2005
# This simple awk script extracts the documentation out of the
# php_monetdb.c file and currently formats it into an HTML list.  In
# order to make this work well it probably has to be altered a bit.
# Currently `./extractdoc.sh | lynx -stdin' produces a nice list of
# function prototypes and their (limited) available documentation.

awk '
BEGIN { printdoc = 0; print "<ul>"; }
	{
		if ($2 == "{{{" && $3 == "proto") {
			printdoc = 1;
			print "<li><b>"
			for (i = 3; i < NF; i++) print $i " ";
			print $NF "</b><br />\n";
		} else if (printdoc != 0 && $0 ~ /*\//) {
			printdoc = 0;
			for (i = 1; $i != "*/"; i++) print $i " ";
			print "</li>\n";
		} else if (printdoc != 0) {
			print $0 "\n";
		}
	}
END { print "</ul>"; }
' php_monetdb.c
