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
#         Torsten Grust <torsten.grust@uni-konstanz.de>
#
# $Id$

CORE = Core

GHC = ghc
GHCPACKAGES = 

GHCFLAGS = -O -syslib lang -cpp -fallow-overlapping-instances -fglasgow-exts $(GHCPACKAGES)
GHCIFLAGS = --interactive

SRCS = Core.hs Item.hs Algb.hs Ty.hs Dag.hs XMark.hs

OBJS = $(SRCS:.hs=.o)

$(CORE): $(SRCS)
	$(GHC) $(GHCFLAGS) -o $@ --make $@
	strip $@
ps: $(CORE)
	./$(CORE) | dot -Tps > dot.ps
	latex dot
	dvips -ta4 dot -o Core.ps
	rm -f dot.{aux,dvi,log,ps}

.PHONY: debug
debug: $(CORE)
	$(GHC) $(GHCIFLAGS) $<

.PHONY: clean
clean:
	rm -f $(CORE) $(OBJS) *.hi __rm_food
	rm -f *~ __rm_food
