# Copyright Notice:
# -----------------
#
# The contents of this file are subject to the Pathfinder Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
# the License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the Pathfinder system.
#
# The Initial Developer of the Original Code is the Database &
# Information Systems Group at the University of Konstanz, Germany.
# Portions created by the University of Konstanz are Copyright (C)
# 2000-2005 University of Konstanz.  All Rights Reserved.
#

# extra make rules for Pathfinder

#
# -c 1000 makes burg abort if costs exceed 1000. This essentially
#         detects recursion in burg rules and avoids infinite loops
#         at build time (or segfaults).
# -d      reports some statistics and reports on unused rules and
#         terminals. (Please look at this output when making
#         Pathfinder if you have modified any burg files. Unused
#         rules indicate errors in your grammar!)
# -I      makes burg emit standard code for some burg internal functions.
#         (Otherwise we'd have to provide them ourselves.)
# -p...   adds a prefix to each burg internal function. We base this
#         prefix on the file name to avoid linker problems (as we use
#         burg allover Pathfinder).
#
%.c : %.brg
	$(RM) -f $@
	$(top_builddir)/burg/burg -c 1000 -d -I -p PF$* $< -o $@
	chmod =r $@

#
# If Burg fails, it will still produce some output. If somebody
# invoked `make' again compilation would produce strange results
# with these corrupt .c files. We thus advise `make' to delete
# them on error.
#
.DELETE_ON_ERROR: $(top_srcdir)/compiler/mil/milgen.c \
                  $(top_srcdir)/compiler/core/coreopt.c \
                  $(top_srcdir)/compiler/core/fs.c \
                  $(top_srcdir)/compiler/core/simplify.c \
                  $(top_srcdir)/compiler/semantics/normalize.c \
                  $(top_srcdir)/compiler/semantics/typecheck.c \
                  $(top_srcdir)/compiler/algebra/algopt.c \
                  $(top_srcdir)/compiler/algebra/core2alg.c


.PHONY: doc html

#
# We have a little shell script that generates documentation with
# help of the doxygen source code documentation tool.
#
doc:
	cd $(top_srcdir) ; doc/gen_doc.sh

#
# Stefan's automated test system uses `make docs', not `make doc'.
# Make him happy as well.
#
docs: doc
