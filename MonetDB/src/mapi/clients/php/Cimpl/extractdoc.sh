#!/bin/sh

# Fabian Groffen, May 8th 2005
# This simple awk script extracts the documentation out of the
# php_monetdb.c file and currently formats it into an HTML list.  In
# order to make this work well it probably has to be altered a bit.
# Currently `./extractdoc.sh | lynx -stdin' produces a nice list of
# function prototypes and their (limited) available documentation.

awk '
BEGIN { printdoc = 0; msg = "<ul>"; }
	{
		if ($2 == "{{{" && $3 == "proto") {
			printdoc = 1;
			msg = msg "<li><b>"
			for (i = 3; i < NF; i++) msg = msg $i " ";
			msg = msg $NF "</b><br />\n";
		} else if (printdoc != 0 && $0 ~ /*\//) {
			printdoc = 0;
			for (i = 1; $i != "*/"; i++) msg = msg $i " ";
			msg = msg "</li>\n";
		} else if (printdoc != 0) {
			msg = msg $0 "\n";
		}
	}
END { print msg "</ul>"; }
' php_monetdb.c
