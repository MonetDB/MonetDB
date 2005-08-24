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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

# make rules to generate MonetDB's documentation

$(prefix)/doc/MonetDB/TechDocs/Core/Mx/mxdoc.tex:	$(top_srcdir)/doc/mxdoc.tex
	-@mkdir -p $(prefix)/doc/MonetDB/TechDocs/Core/Mx
	cp $< $@

$(prefix)/doc/MonetDB/TechDocs/Core/Mx/mxdoc.aux:	$(prefix)/doc/MonetDB/TechDocs/Core/Mx/mxdoc.tex
	cd $(prefix)/doc/MonetDB/TechDocs/Core/Mx; latex mxdoc.tex; latex mxdoc.tex

$(prefix)/doc/MonetDB/TechDocs/Core/Mx/index.html:	$(prefix)/doc/MonetDB/TechDocs/Core/Mx/mxdoc.aux
	cd $(prefix); latex2html -math -ascii_mode -no_images -address '' -style http://monetdb.cwi.nl/MonetDB.css -dir doc/MonetDB/TechDocs/Core/Mx doc/MonetDB/TechDocs/Core/Mx/mxdoc.tex -noinfo

$(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation/index.html:	\
		$(top_srcdir)/src/mapi/clients/java/MapiClient.java	\
		$(top_srcdir)/src/mapi/clients/java/mapi/Mapi.java	\
		$(top_srcdir)/src/mapi/clients/java/mapi/MapiException.java
	-@mkdir -p $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation
	sh $(top_srcdir)/src/utils/http_get.sh http://monetdb.cwi.nl/MonetDB.css > $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation/MonetDB.css
	javadoc -d $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation -stylesheetfile $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation/MonetDB.css\
		$(top_srcdir)/src/mapi/clients/java/MapiClient.java       	\
	        $(top_srcdir)/src/mapi/clients/java/mapi/Mapi.java        	\
	        $(top_srcdir)/src/mapi/clients/java/mapi/MapiException.java

docs:	$(prefix)/doc/MonetDB/TechDocs/Core/Mx/index.html	\
	$(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation/index.html	\
	$(top_srcdir)/doc/mkdoc.py
	mv $(prefix)/doc/MonetDB/TechDocs/Core/Mx $(prefix)/doc/
	mv $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/Documentation $(prefix)/doc/
	python $(top_srcdir)/doc/mkdoc.py $(top_srcdir) $(MONETDB_BUILD) $(prefix)
	mkdir -p $(prefix)/doc/MonetDB/TechDocs/Core $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java
	mv $(prefix)/doc/Mx $(prefix)/doc/MonetDB/TechDocs/Core/
	mv $(prefix)/doc/Documentation $(prefix)/doc/MonetDB/TechDocs/APIs/Mapi/Java/

$(top_srcdir)/HowToStart.html: $(top_srcdir)/HowToStart $(top_srcdir)/HowToStart.css
	rst2html.py --stylesheet $(top_srcdir)/HowToStart.css $(top_srcdir)/HowToStart > $(top_srcdir)/HowToStart.html
