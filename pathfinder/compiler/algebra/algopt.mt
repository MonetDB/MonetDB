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
      doc_tbl      /* literal table */

      disjunion    /* union two relations with same schema */
      difference   /* difference */
      intersect    /* intersection */

      cross        /* cross product (Cartesian product) */
      eqjoin       /* equi-join */
      scjoin       /* staircase join */

      rownum       /* consecutive number generation */

      project      /* algebra projection and renaming operator */
      select_      /* selection of rows where column value not false */
      sum          /* partitioned sum of column */
      count_       /* partitioned row count */
      distinct     /* duplicate elimination */

      type
      cast         /* cast algebra data types */

      num_add      /* arithmetic plus operator */
      num_subtract /* arithmetic minus operator */
      num_multiply /* arithmetic times operator */
      num_divide   /* arithmetic divide operator */
      num_modulo   /* arithmetic modulo operator */
      num_neg      /* numeric negation */

      num_gt       /* numeric greater than */
      num_eq       /* numeric equal */

      and          /* logical and */
      or           /* logical or */
      not          /* logical negation (true <--> false) */

      element      /* element construction */
      attribute    /* attribute construction */
      textnode     /* text node construction */
      docnode      /* document node construction */
      comment      /* comment construction */
      processi     /* processing instruction construction */

      strconcat
      merge_adjacent

      seqty1
      all

      serialize    /* result serialization */
      ;


label Query
      AlgExpr
      EmptyExpr
      ;


Query:    serialize (AlgExpr, AlgExpr);

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
AlgExpr:  doc_tbl;

AlgExpr:  disjunion (AlgExpr, AlgExpr);
AlgExpr:  disjunion (EmptyExpr, AlgExpr)
    =
    {
        /*
         * {} union R  ==>  R
         */
        return $2$;
    }
    ;

AlgExpr:  disjunion (AlgExpr, EmptyExpr)
    =
    {
        /*
         * R union {}  ==>  R
         */
        return $1$;
    }
    ;

AlgExpr:  difference (AlgExpr, AlgExpr);
AlgExpr:  difference (AlgExpr, EmptyExpr)
    =
    {
        /*
         * R \ {}  ==>  R
         */
        return $1$;
    }
    ;

AlgExpr:  intersect (AlgExpr, AlgExpr);
AlgExpr:  cross (AlgExpr, AlgExpr);
AlgExpr:  eqjoin (AlgExpr, AlgExpr);
AlgExpr:  scjoin (AlgExpr, AlgExpr);
AlgExpr:  rownum (AlgExpr);
AlgExpr:  project (AlgExpr);
AlgExpr:  select_ (AlgExpr);
AlgExpr:  sum (AlgExpr);
AlgExpr:  count_ (AlgExpr);
AlgExpr:  distinct (AlgExpr);
AlgExpr:  type (AlgExpr);

AlgExpr:  cast (AlgExpr);
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
    { return $1$; }
    ;

AlgExpr:  num_add (AlgExpr);
AlgExpr:  num_subtract (AlgExpr);
AlgExpr:  num_multiply (AlgExpr);
AlgExpr:  num_divide (AlgExpr);
AlgExpr:  num_modulo (AlgExpr);
AlgExpr:  num_neg (AlgExpr);
AlgExpr:  num_gt (AlgExpr);
AlgExpr:  num_eq (AlgExpr);
AlgExpr:  and (AlgExpr);
AlgExpr:  or (AlgExpr);
AlgExpr:  not (AlgExpr);
AlgExpr:  element (AlgExpr, AlgExpr, AlgExpr);
AlgExpr:  attribute (AlgExpr, AlgExpr);
AlgExpr:  textnode (AlgExpr);
AlgExpr:  docnode (AlgExpr, AlgExpr);
AlgExpr:  comment (AlgExpr);
AlgExpr:  processi (AlgExpr);
AlgExpr:  strconcat (AlgExpr);
AlgExpr:  merge_adjacent (AlgExpr, AlgExpr);
AlgExpr:  seqty1 (AlgExpr);
AlgExpr:  all (AlgExpr);

EmptyExpr: lit_tbl
    {
        /* Only a literal table with no tuples is an empty expression. */
        if ($$->sem.lit_tbl.count != 0)
            ABORT;

        cost = 0;
    }
    ;

EmptyExpr:  disjunion (EmptyExpr, EmptyExpr);
EmptyExpr:  difference (EmptyExpr, AlgExpr);
EmptyExpr:  difference (EmptyExpr, EmptyExpr);
EmptyExpr:  intersect (EmptyExpr, AlgExpr);
EmptyExpr:  intersect (AlgExpr, EmptyExpr);
EmptyExpr:  intersect (EmptyExpr, EmptyExpr);
EmptyExpr:  cross (EmptyExpr, AlgExpr);
EmptyExpr:  cross (AlgExpr, EmptyExpr);
EmptyExpr:  cross (EmptyExpr, EmptyExpr);
EmptyExpr:  eqjoin (EmptyExpr, AlgExpr);
EmptyExpr:  eqjoin (AlgExpr, EmptyExpr);
EmptyExpr:  eqjoin (EmptyExpr, EmptyExpr);
EmptyExpr:  scjoin (EmptyExpr, AlgExpr);
EmptyExpr:  scjoin (AlgExpr, EmptyExpr);
EmptyExpr:  scjoin (EmptyExpr, EmptyExpr);
EmptyExpr:  rownum (EmptyExpr);
EmptyExpr:  project (EmptyExpr);
EmptyExpr:  select_ (EmptyExpr);
EmptyExpr:  sum (EmptyExpr);
EmptyExpr:  count_ (EmptyExpr);
EmptyExpr:  distinct (EmptyExpr);
EmptyExpr:  type (EmptyExpr);
EmptyExpr:  cast (EmptyExpr);
EmptyExpr:  num_add (EmptyExpr);
EmptyExpr:  num_subtract (EmptyExpr);
EmptyExpr:  num_multiply (EmptyExpr);
EmptyExpr:  num_divide (EmptyExpr);
EmptyExpr:  num_modulo (EmptyExpr);
EmptyExpr:  num_neg (EmptyExpr);
EmptyExpr:  num_gt (EmptyExpr);
EmptyExpr:  num_eq (EmptyExpr);
EmptyExpr:  and (EmptyExpr);
EmptyExpr:  or (EmptyExpr);
EmptyExpr:  not (EmptyExpr);
EmptyExpr:  element (AlgExpr, EmptyExpr, AlgExpr);
EmptyExpr:  element (AlgExpr, EmptyExpr, EmptyExpr);
EmptyExpr:  element (EmptyExpr, EmptyExpr, AlgExpr);
EmptyExpr:  element (EmptyExpr, EmptyExpr, EmptyExpr);
EmptyExpr:  attribute (EmptyExpr, AlgExpr);
EmptyExpr:  attribute (AlgExpr, EmptyExpr);
EmptyExpr:  attribute (EmptyExpr, EmptyExpr);
EmptyExpr:  textnode (EmptyExpr);
EmptyExpr:  comment (EmptyExpr);
EmptyExpr:  processi (EmptyExpr);
EmptyExpr:  strconcat (EmptyExpr);
EmptyExpr:  merge_adjacent (EmptyExpr, AlgExpr);
EmptyExpr:  merge_adjacent (AlgExpr, EmptyExpr);
EmptyExpr:  merge_adjacent (EmptyExpr, EmptyExpr);
EmptyExpr:  seqty1 (EmptyExpr);
EmptyExpr:  all (EmptyExpr);


/* vim:set shiftwidth=4 expandtab filetype=c: */
