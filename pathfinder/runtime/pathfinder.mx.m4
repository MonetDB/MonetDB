m4_include(config.m4)
# pathfinder.mx
#
# Monet runtime support for the Pathfinder XQuery compiler
#
# (c) 2002/2003 University of Konstanz, Database Research Group
#
# $Id$
#

@f pathfinder
@a Torsten Teggy Grust
@a Maurice van Keulen
@a Henning Rode
@t Runtime Support for the Pathfinder XQuery Compiler

@m
.MODULE pathfinder;

.ITERATOR docorder (BAT[any,any] b, ptr itr) = CMDbatloop_std;
"This iterator walks the BUNs of BAT b from first to last.
 <BAT expression> @ docorder <MIL statement>
 Variables [$h,$t] are bound to head and tail of the
 current BUN in the MIL statement.
 (Added to implement XPath forward axes semantics in the
 Pathfinder XQuery compiler.)"

.ITERATOR revdocorder (BAT[any,any] b, ptr itr) = PFrevdocorder;
"This iterator walks the BUNs of BAT b from last to first.
 <BAT expression> @ revdocorder <MIL statement>
 Variables [$h,$t] are bound to head and tail of the
 current BUN in the MIL statement.
 (Added to implement XPath reverse axes semantics in the
 Pathfinder XQuery compiler.)"

m4_include(ctxpropmgmt.def)
m4_include(pair.def)
m4_include(estimation.def)
m4_include(levelsteps.def)
m4_include(staircasejoin.def)

.LOAD
m4_include(docmgmt.mil.m4)
m4_include(xmlprint.mil)
m4_include(ctxpropmgmt.mil)
m4_include(axis.mil.m4)
.END;


.END pathfinder;


@h
#ifndef PATHFINDER_H
#define PATHFINDER_H

#include <monet.h>
#include <gdk.h>

m4_include(pair.h)
m4_include(ctxpropmgmt.h)

#include "pathfinder.proto.h"

#endif

@c
#include <sys/time.h>
#include "pathfinder.h"

int
PFrevdocorder(BAT *bat, ptr *itr)
{
    Iteration it = *(Iteration *)itr;
    int i;

    BUN fst = BUNfirst (bat);
    BUN bun = BUNlast  (bat);
    int s   = BUNsize  (bat);

    while ((bun -= s) >= fst)
        if ((i = ITERATE (BUNhead (bat, bun), BUNtail (bat, bun), it)) < 0)
            return i;

    return GDK_SUCCEED;
}

m4_include(pair.c)
m4_include(ctxpropmgmt.c)
m4_include(estimation.c)
m4_include(levelsteps.c)
m4_include(staircasejoin.c)

/* vim:set shiftwidth=4 expandtab: */

