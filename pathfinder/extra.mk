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
# created by U Konstanz are Copyright (C) 2000-2003 University
# of Konstanz. All Rights Reserved.
#
# Contributors:
#         Niels Nes <Niels.Nes@cwi.nl>
#

# extra make rules for Pathfinder

%.symbols.h %.c : %.mt
	$(CP) $< tmp.mt
	$(TWIG) -t tmp.mt
	mv -f symbols.h $*.symbols.h
	sed 's/^short\(.*\)=/static short\1=/' walker.c > $@
	$(RM) -f walker.c
	$(RM) -f tmp.mt

% :: %.m4
	$(RM) -f $@
	$(M4) $< >$@
	chmod =r $@

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

