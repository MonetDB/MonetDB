/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * Rewrite/optimize algebra expression tree.
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
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Sabine Mayer <mayers@inf.uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */
  
/* Auxiliary routines related to the translation are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "algopt_impl.h"


};

node  lit_tbl      /* literal table */
      disjunion    /* union two relations with same schema */
      cross        /* cross product (Cartesian product) */
      eqjoin       /* equi-join */
      project      /* algebra projection and renaming operator */
      rownum       /* consecutive number generation */
      serialize    /* result serialization */
      difference   /* difference */
      select_

      num_add      /* arithmetic plus operator */
      num_subtract /* arithmetic minus operator */
      num_multiply /* arithmetic times operator */
      num_divide   /* arithmetic divide operator */

      num_gt       /* numeric greater than */
      num_eq       /* numeric equal */

      not          /* logical negation (true <--> false) */

      cast         /* cast algebra data types */

      type

      seqty1
      all
      ;


label Query
      AlgExpr
      EmptyExpr
      ;


Query:    serialize (AlgExpr, AlgExpr)
    { assert ($$);  /* avoid `root unused' warning */ };

AlgExpr:  EmptyExpr
    =
    {
        /*
         * Replace any sub-tree that we determined empty with these
         * rules by the literal empty table.
         */
        int          i;
        PFalg_att_t *atts = PFmalloc ($$->schema.count * sizeof (PFalg_att_t));

        for (i = 0; i < $$->schema.count; i++)
            atts[i] = $$->schema.items[i].name;

        return PFalg_lit_tbl_ (PFalg_attlist_ ($$->schema.count, atts),
                               0 /* no of tuples */, NULL /* tuples */);
    }
    ;

AlgExpr:  lit_tbl;
AlgExpr:  disjunion (AlgExpr, AlgExpr);
AlgExpr:  cross (AlgExpr, AlgExpr);
AlgExpr:  eqjoin (AlgExpr, AlgExpr);
AlgExpr:  project (AlgExpr);
AlgExpr:  rownum (AlgExpr);
AlgExpr:  difference (AlgExpr, AlgExpr);
AlgExpr:  select_ (AlgExpr);
AlgExpr:  num_add (AlgExpr);
AlgExpr:  num_subtract (AlgExpr);
AlgExpr:  num_multiply (AlgExpr);
AlgExpr:  num_divide (AlgExpr);
AlgExpr:  cast (AlgExpr);
AlgExpr:  not (AlgExpr);
AlgExpr:  num_gt (AlgExpr);
AlgExpr:  num_eq (AlgExpr);
AlgExpr:  type (AlgExpr);
AlgExpr:  seqty1 (AlgExpr);
AlgExpr:  all (AlgExpr);

AlgExpr:  cross (AlgExpr, lit_tbl)
    {
        /*
         * The cross product with one operand being a literal table
         * with just one tuple is particularly easy: it means just
         * adding one or more columns to the table. This rule normalizes
         * this situation and puts the one-tuple literal table to the
         * left, so we will only have to catch it in that order when
         * generating MIL code.
         *
         * (But only do it if the left operand is not a literal table
         * with exactly one tuple. Otherwise we'd end up with an
         * infinite loop.)
         */
        if (($1$->kind != aop_lit_tbl || $1$->sem.lit_tbl.count != 1)
            && $2$->sem.lit_tbl.count == 1)
            REWRITE;
        else
            ABORT;
    }
    =
    {
        return cross ($2$, $1$);
    }
    ;
AlgExpr:  cast (AlgExpr)
    {
        /*
         * If an algebra expression already has the requested
         * type, remove the cast.
         */
        int i;
        for (i = 0; i < $1$->schema.count; i++)
            if (!strcmp ($$->sem.cast.att, $1$->schema.items[i].name)) {
                if ($$->sem.cast.ty == $1$->schema.items[i].type)
                    REWRITE;
                else
                    ABORT;
            }
    }
    =
    { return $1$;};

AlgExpr:  disjunion (EmptyExpr, AlgExpr)
    { REWRITE; }
    =
    {
        /*
         * {} union R  ==>  R
         */
        return $2$;
    }
    ;
AlgExpr:  disjunion (AlgExpr, EmptyExpr)
    { REWRITE; }
    =
    {
        /*
         * R union {}  ==>  R
         */
        return $1$;
    }
    ;

EmptyExpr: lit_tbl
    {
        /* Only a literal table with no tuples is an empty expression. */
        if ($$->sem.lit_tbl.count != 0)
            ABORT;

        cost = 0;
    };

EmptyExpr: project (EmptyExpr);

EmptyExpr: cross (EmptyExpr, AlgExpr);
EmptyExpr: cross (AlgExpr, EmptyExpr);
EmptyExpr: cross (EmptyExpr, EmptyExpr);

EmptyExpr: eqjoin (EmptyExpr, AlgExpr);
EmptyExpr: eqjoin (AlgExpr, EmptyExpr);
EmptyExpr: eqjoin (EmptyExpr, EmptyExpr);

EmptyExpr: rownum (EmptyExpr);

EmptyExpr: difference (EmptyExpr, AlgExpr);
EmptyExpr: select_ (EmptyExpr);

EmptyExpr: num_add (EmptyExpr);
EmptyExpr: num_subtract (EmptyExpr);
EmptyExpr: num_multiply (EmptyExpr);
EmptyExpr: num_divide (EmptyExpr);

EmptyExpr: num_gt (EmptyExpr);
EmptyExpr: num_eq (EmptyExpr);

EmptyExpr: not (EmptyExpr);

EmptyExpr: cast (EmptyExpr);

EmptyExpr: type (EmptyExpr);
EmptyExpr: seqty1 (EmptyExpr);
EmptyExpr: all (EmptyExpr);

/* vim:set shiftwidth=4 expandtab filetype=c: */
