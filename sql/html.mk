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
# Portions created by CWI are Copyright (C) 1997-2005 CWI.
# All Rights Reserved.

# make rules to generate sql's documentation

$(prefix)/doc/jdbcmanual.html:	$(top_srcdir)/jdbcmanual.html
	-@mkdir -p $(prefix)/doc
	cp $< $@

$(prefix)/doc/jdbcmanual/jdbcmanual.tex:	$(top_srcdir)/src/jdbc/jdbcmanual.tex
	-@mkdir -p $(prefix)/doc/jdbcmanual
	cp $< $@

$(prefix)/doc/jdbcmanual/jdbcmanual.aux:	$(prefix)/doc/jdbcmanual/jdbcmanual.tex
	(cd $(prefix)/doc/jdbcmanual; latex jdbcmanual.tex; latex jdbcmanual.tex)

$(prefix)/doc/jdbcmanual/jdbcmanual.html:	$(prefix)/doc/jdbcmanual/jdbcmanual.aux
	(cd $(prefix); latex2html -ascii_mode -address '' -style http://monetdb.cwi.nl/MonetDB.css -dir doc/jdbcmanual doc/jdbcmanual/jdbcmanual.tex -noinfo)


$(prefix)/doc/SQLsessionDemo.html:	$(top_srcdir)/SQLsessionDemo.html
	-@mkdir -p $(prefix)/doc
	cp $< $@

$(prefix)/doc/SQLfeatures/SQLfeatures.tex:	$(top_srcdir)/SQLfeatures.tex
	-@mkdir -p $(prefix)/doc/SQLfeatures
	cp $< $@

$(prefix)/doc/SQLfeatures/SQLfeatures.aux:	$(prefix)/doc/SQLfeatures/SQLfeatures.tex
	(cd $(prefix)/doc/SQLfeatures; latex SQLfeatures.tex; latex SQLfeatures.tex)

$(prefix)/doc/SQLfeatures/SQLfeatures.html:	$(prefix)/doc/SQLfeatures/SQLfeatures.aux
	(cd $(prefix); latex2html -ascii_mode -address '' -style http://monetdb.cwi.nl/MonetDB.css -dir doc/SQLfeatures doc/SQLfeatures/SQLfeatures.tex -noinfo)

docs: $(prefix)/doc/SQLsessionDemo.html $(prefix)/doc/SQLfeatures/SQLfeatures.html $(prefix)/doc/jdbcmanual/jdbcmanual.html
