/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * MIL algebra optimization/simplification.
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */
  
/* Auxiliary routines related to the translation are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "ma_opt_impl.c"

};

node  serialize
      new_
      insert_
      seqbase
      project
      reverse
      sort
      ctrefine
      join
      leftjoin
      cross
      mirror
      kunique
      mark_grp
      mark
      count
      append
      oid
      moid
      mint
      madd

      lit_oid
      lit_int
      lit_str
      lit_bit
      lit_dbl
      ;

label Prog
      BatExpr
      AtomExpr
      Literal
      ;

Prog:        serialize (BatExpr, BatExpr, BatExpr, BatExpr,
                        BatExpr, BatExpr, BatExpr)
    { assert ($$);  /* avoid `root unused' warning */ }
    =
    { assert (_ll);  /* avoid warning `unused parameter: _ll' */ }
    ;

BatExpr:     new_;
BatExpr:     insert_ (BatExpr, AtomExpr, AtomExpr);
BatExpr:     seqbase (BatExpr, AtomExpr);
BatExpr:     project (BatExpr, AtomExpr);

BatExpr:     reverse (BatExpr);

BatExpr:     reverse (reverse (BatExpr))
    { REWRITE; }
    =
    {
        return $1.1$;
    }
    ;

BatExpr:     sort (BatExpr);
BatExpr:     ctrefine (BatExpr, BatExpr);
BatExpr:     join (BatExpr, BatExpr);
BatExpr:     leftjoin (BatExpr, BatExpr);
BatExpr:     cross (BatExpr, BatExpr);
BatExpr:     mirror (BatExpr);
BatExpr:     kunique (BatExpr);
BatExpr:     mark_grp (BatExpr, BatExpr);
BatExpr:     mark (BatExpr, AtomExpr);
BatExpr:     append (BatExpr, BatExpr);
AtomExpr:    oid (AtomExpr);
BatExpr:     moid (BatExpr);
BatExpr:     mint (BatExpr);
BatExpr:     madd (BatExpr, AtomExpr);
BatExpr:     madd (AtomExpr, BatExpr);
BatExpr:     madd (BatExpr, BatExpr);

AtomExpr:    Literal;
AtomExpr:    count (BatExpr);

Literal:     lit_oid;
Literal:     lit_int;
Literal:     lit_str;
Literal:     lit_bit;
Literal:     lit_dbl;

/* vim:set shiftwidth=4 expandtab filetype=c: */
