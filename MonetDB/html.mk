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
# Portions created by CWI are Copyright (C) 1997-2003 CWI.
# All Rights Reserved.
# 
# Contributor(s):
# 		Martin Kersten <Martin.Kersten@cwi.nl>
# 		Peter Boncz <Peter.Boncz@cwi.nl>
# 		Niels Nes <Niels.Nes@cwi.nl>
# 		Stefan Manegold  <Stefan.Manegold@cwi.nl>

# make rules to generate MonetDB'\''s documentation

$(prefix)/doc/Mx/content.shtml:	$(top_srcdir)/src/utils/Mx/content.shtml
	-@mkdir -p $(prefix)/doc/Mx
	cp $< $@

$(prefix)/doc/Mx/title.txt:	$(top_srcdir)/src/utils/Mx/title.txt
	-@mkdir -p $(prefix)/doc/Mx
	cp $< $@

$(prefix)/doc/Mx/mxdoc.tex:	$(top_srcdir)/doc/mxdoc.tex
	-@mkdir -p $(prefix)/doc/Mx
	-@chmod g+w,g-s $(prefix)/doc/Mx
	cp $< $@

$(prefix)/doc/Mx/mxdoc.aux:	$(prefix)/doc/Mx/mxdoc.tex
	(cd $(prefix)/doc/Mx; latex mxdoc.tex; latex mxdoc.tex)

$(prefix)/doc/Mx/index.html:	$(prefix)/doc/Mx/mxdoc.aux
	(cd $(prefix); latex2html -ascii_mode -no_images -address '' -style http://monetdb.cwi.nl/MonetDB.css -dir doc/Mx doc/Mx/mxdoc.tex)

$(prefix)/doc/MapiJava/content.shtml:	$(top_srcdir)/src/mapi/clients/java/content.shtml
	-@mkdir -p $(prefix)/doc/MapiJava
	cp $< $@

$(prefix)/doc/MapiJava/title.txt:	$(top_srcdir)/src/mapi/clients/java/title.txt
	-@mkdir -p $(prefix)/doc/MapiJava
	cp $< $@

$(prefix)/doc/MapiJava/index.html:	$(top_srcdir)/src/mapi/clients/java/MapiClient.java	\
					$(top_srcdir)/src/mapi/clients/java/mapi/Mapi.java	\
					$(top_srcdir)/src/mapi/clients/java/mapi/MapiException.java
	-@mkdir -p $(prefix)/doc/MapiJava
	-@chmod g+w,g-s $(prefix)/doc/MapiJava
	lynx -source http://monetdb.cwi.nl/MonetDB.css > $(prefix)/doc/MapiJava/MonetDB.css
	javadoc -d $(prefix)/doc/MapiJava -stylesheetfile $(prefix)/doc/MapiJava/MonetDB.css\
		$(top_srcdir)/src/mapi/clients/java/MapiClient.java       	\
	        $(top_srcdir)/src/mapi/clients/java/mapi/Mapi.java        	\
	        $(top_srcdir)/src/mapi/clients/java/mapi/MapiException.java

html:	$(prefix)/doc/Mx/content.shtml	\
	$(prefix)/doc/Mx/title.txt	\
	$(prefix)/doc/Mx/index.html	\
	$(prefix)/doc/MapiJava/content.shtml	\
	$(prefix)/doc/MapiJava/title.txt	\
	$(prefix)/doc/MapiJava/index.html	\
	$(top_srcdir)/doc/mkdoc.py
	python $(top_srcdir)/doc/mkdoc.py $(top_srcdir) $(prefix)

