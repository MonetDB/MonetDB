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

# make rules to generate sql's documentation

# entry point
docs: \
	$(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Unix/index.html \
	$(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Windows/index.html

$(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Unix/index.html: $(top_srcdir)/HowToStart-SQL
	mkdir -p $(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Unix
	rst2html.py --stylesheet http://monetdb.cwi.nl/MonetDB.css --link-stylesheet $(top_srcdir)/HowToStart-SQL | xsltproc --html $(top_srcdir)/body.xsl - > $(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Unix/index.html

$(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Windows/index.html: $(top_srcdir)/HowToStart-SQL-Win32.txt
	mkdir -p $(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Windows
	rst2html.py --stylesheet http://monetdb.cwi.nl/MonetDB.css --link-stylesheet $(top_srcdir)/HowToStart-SQL-Win32.txt | xsltproc --html $(top_srcdir)/body.xsl - > $(prefix)/doc/MonetDB/GetGoing/Setup/SQL/Windows/index.html
