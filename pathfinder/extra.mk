# Copyright Notice:
# -----------------
# 
# The contents of this file are subject to the MonetDB Public
# License Version 1.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of
# the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
# 
# Software distributed under the License is distributed on an "AS
# IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
# implied. See the License for the specific language governing
# rights and limitations under the License.
# 
# The Original Code is the ``Pathfinder'' system. The Initial
# Developer of the Original Code is the Database & Information
# Systems Group at the University of Konstanz, Germany. Portions
# created by U Konstanz are Copyright (C) 2000-2004 University
# of Konstanz. All Rights Reserved.
#
# Contributors:
#         Niels Nes <Niels.Nes@cwi.nl>
#

# extra make rules for Pathfinder

# Twig checks its argument to end in `.mt'. Unfortunately
# it does that the wrong way and will fail if the pathname
# of the argument contains dots (which is the case in the
# automated test environment).
# We thus copy the source file into the ``safe'' filename
# `tmp.mt' and run twig on that. The proper solution would
# of course be fixing Twig.
%.symbols.h %.c : %.mt
	$(LOCKFILE) waiting_for_twig
	$(CP) $< tmp.mt
	$(TWIG) -t tmp.mt
	mv -f symbols.h $*.symbols.h
	sed 's/^short\(.*\)=/static short\1=/' walker.c > $*.c
	$(RM) walker.c
	$(RM) tmp.mt
	$(RM) waiting_for_twig

% :: %.m4
	$(RM) -f $@
	$(M4) $< >$@
	chmod =r $@

%.c : %.brg
	$(RM) -f $@
	$(top_builddir)/burg/burg -c 1000 -d -I -p PF$* $< -o $@
	chmod =r $@

#
# If Burg fails, it will still produce some output. If somebody
# invoked `make' again compilation will produce strange results
# with these corrupt .c files. We thus advise `make' to delete
# them on error.
#
.DELETE_ON_ERROR: $(top_srcdir)/compiler/mil/ma_gen.c \
                  $(top_srcdir)/compiler/mil/ma_opt.c \
                  $(top_srcdir)/compiler/mil/milgen.c


# Some files need to be preprocessed with the stream editor `sed'.
# The required sed expressions are contained in the source files
# themselves; they carry a special marker that we use during the
# build.
# We first ``grep'' for all lines in the source file that contain
# the marker. We translate this marker into an sed expression
# (using sed; we map `*!sed 'pattern'' to `-e 'pattern'', as it
# can be passed to sed on the command line). The resulting sed
# expression is the used as the sed command line argument when
# we feed the source file through sed.
% :: %.sed
	$(RM) -f $@
	sed `grep '\*!sed' $< | \
	  sed 's/\*!sed *'\''\(.*\)'\''/\-e \1/g'` $< > $@
	chmod =r $@

.PHONY: doc html

#
# We have a little shell script that generates documentation with
# help of the doxygen source code documentation tool.
#
doc:
	cd $(top_srcdir) ; doc/gen_doc.sh

#
# We do not only produce HTML documentation, so `html' is actually
# the wrong make target name. Stefan's automated test system, however
# uses `html', so we provide it here.
#
html: doc
