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
#         Torsten Grust <torsten.grust@uni-konstanz.de>
#         Maurice van Keulen <M.van.Keulen@bigfoot.com>
#         Jens Teubner <jens.teubner@uni-konstanz.de>
#

#
# We put ALL our include files in here.
#
INCLUDES = -I$(top_srcdir)/compiler/include

#
# Existence of the libxml2 XML parser library or the Hans Boehm
# garbage collection library might give additional compiler flags
pf_CFLAGS = $(AM_CFLAGS) $(LIBXML2_CFLAGS) $(LIBGC_CFLAGS)
pf_LDFLAGS = $(AM_LDFLAGS) $(LIBXML2_LIBS) $(LIBGC_LIBS)
pf_LDADD = $(LIBGC_LDADD) $(LIBXML2_LDADD) $(LEXLIB)

#
# These are sources that appear as dependancies of other source
# files, but are generated during compilation. We specify them
# here, so that automake can resolve all the dependancies.
#
BUILT_SOURCES = parser/parser.h \
                semantics/normalize.symbols.h \
                semantics/normalize.c \
                semantics/typecheck.symbols.h \
                semantics/typecheck.c \
                semantics/typecheck_impl.c \
                core/fs.symbols.h \
                core/fs.c \
                core/fs_impl.c \
                core/simplify.symbols.h \
                core/simplify.c

# Yacc has to generate a header files with the tokens we need in the
# scanner (parser/parser.h).
# Note that with this flag (together with the above BUILT_SOURCES)
# automake knows what to do. automake has some special built-ins for
# that.
AM_YFLAGS = -d

#
# The Pathfinder compiler, 'pf' is the only binary we create
#
bin_PROGRAMS = pf

#
# These are all the source files required to build 'pf'
#
pf_SOURCES = daemon/daemon.c \
             schema/import.c \
             debug/abssynprint.c \
             debug/coreprint.c \
	     debug/mildebug.c \
	     debug/prettyp.c \
	     mem/array.c \
	     mem/mem.c \
	     parser/scanner.l \
	     parser/parser.y \
	     parser/abssyn.c \
	     semantics/functions.c \
	     semantics/normalize.c \
	     semantics/ns.c \
	     semantics/subtyping.c \
	     semantics/typecheck.c \
	     semantics/types.c \
	     semantics/varscope.c \
	     semantics/xquery_fo.c \
	     core/fs.c \
             core/core.c \
	     core/simplify.c \
	     mil/core2mil.c \
	     mil/milprint.c \
	     mil/miltype.c \
	     pathfinder/env.c \
	     pathfinder/main.c \
	     pathfinder/oops.c \
	     pathfinder/pfstrings.c \
	     pathfinder/qname.c \
	     pathfinder/scope.c \
	     pathfinder/timer.c \
	     pathfinder/variable.c

#
# How to create `foo' from `foo.m4'
#
% :: %.m4
	rm -f $@
	$(M4) $< >$@
	chmod =r $@

#
# And how to create `foo.symbols.h' and `foo.c' from `foo.mt'
#
%.symbols.h %.c : %.mt
	$(TWIG) -t $<
	mv -f symbols.h $*.symbols.h
	sed 's/^short\(.*\)=/static short\1=/' walker.c > $@
	rm walker.c
