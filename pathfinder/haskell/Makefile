# Pathfinder Haskell ``prototype''
#
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

XQUERY = XQuery

DOT   = dot
DVIPS = dvips

GHC = ghc
GHCPACKAGES = 

GHCFLAGS = $(GHCPACKAGES)
GHCIFLAGS = --interactive

SRCS = Algb.hs CSE.hs Compile.hs Core.hs DM.hs DMGraph.hs Dot.hs \
       Eval.hs Graph.hs GraphFM.hs Item.hs Ty.hs UDF.hs XMark.hs XQuery.hs
OBJS = $(SRCS:.hs=.o)
HIS  = $(SRCS:.hs=.hi)

$(XQUERY): $(SRCS)
	$(GHC) $(GHCFLAGS) -o $@ --make $@
	strip $@

ps: $(XQUERY)
	./$(XQUERY) | $(DOT) -Tps > dot.ps
	latex dot
	$(DVIPS) dot.dvi -o $(XQUERY).ps
	$(RM) dot.{aux,dvi,log,ps}

.PHONY: debug
debug: $(XQUERY)
	$(GHC) $(GHCIFLAGS) $<

.PHONY: clean
clean:
	$(RM) $(XQUERY) $(OBJS) $(HIS) __rm_food
	$(RM) *~ __rm_food
