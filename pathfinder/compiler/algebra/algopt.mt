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

      roots_
      fragment
      frag_union
      empty_frag

      serialize    /* result serialization */
      ;


label Query
      Rel
      EmptyRel
      Frag
      FragRel
      ;


Query:  serialize (Frag, Rel);

Rel:  EmptyRel
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
Rel:  lit_tbl;

Rel:  disjunion (Rel, Rel);
Rel:  disjunion (EmptyRel, Rel)
    =
    {
        /*
         * {} union R  ==>  R
         */
        return $2$;
    }
    ;

Rel:  disjunion (Rel, EmptyRel)
    =
    {
        /*
         * R union {}  ==>  R
         */
        return $1$;
    }
    ;

Rel:  difference (Rel, Rel);
Rel:  difference (Rel, EmptyRel)
    =
    {
        /*
         * R \ {}  ==>  R
         */
        return $1$;
    }
    ;

Rel:  intersect (Rel, Rel);
Rel:  cross (Rel, Rel);
Rel:  eqjoin (Rel, Rel);
Rel:  scjoin (Frag, Rel);
Rel:  rownum (Rel);
Rel:  project (Rel);
Rel:  select_ (Rel);
Rel:  sum (Rel);
Rel:  count_ (Rel);
Rel:  distinct (Rel);
Rel:  type (Rel);
Rel:  cast (Rel);
Rel:  cast (Rel)
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

Rel:  num_add (Rel);
Rel:  num_subtract (Rel);
Rel:  num_multiply (Rel);
Rel:  num_divide (Rel);
Rel:  num_modulo (Rel);
Rel:  num_neg (Rel);
Rel:  num_gt (Rel);
Rel:  num_eq (Rel);
Rel:  and (Rel);
Rel:  or (Rel);
Rel:  not (Rel);
Rel:  strconcat (Rel);
Rel:  seqty1 (Rel);
Rel:  all (Rel);


Rel:  roots_ (FragRel);


FragRel:  element (Frag, Rel, Rel);
FragRel:  element (Frag, Rel, EmptyRel);
FragRel:  attribute (Rel, Rel);
FragRel:  textnode (Rel);
FragRel:  docnode (Frag, Rel);
FragRel:  comment (Rel);
FragRel:  processi (Rel);
FragRel:  merge_adjacent (Frag, Rel);
FragRel:  doc_tbl (Rel);


Frag:  fragment (FragRel);
Frag:  frag_union (Frag, Frag);
Frag:  frag_union (empty_frag, empty_frag)
    =
    { return $1$; }
    ;
Frag:  empty_frag;


EmptyRel: lit_tbl
    {
        /* Only a literal table with no tuples is an empty expression. */
        if ($$->sem.lit_tbl.count != 0)
            ABORT;

        cost = 0;
    }
    ;

EmptyRel:  disjunion (EmptyRel, EmptyRel);
EmptyRel:  difference (EmptyRel, Rel);
EmptyRel:  difference (EmptyRel, EmptyRel);
EmptyRel:  intersect (EmptyRel, Rel);
EmptyRel:  intersect (Rel, EmptyRel);
EmptyRel:  intersect (EmptyRel, EmptyRel);
EmptyRel:  cross (EmptyRel, Rel);
EmptyRel:  cross (Rel, EmptyRel);
EmptyRel:  cross (EmptyRel, EmptyRel);
EmptyRel:  eqjoin (EmptyRel, Rel);
EmptyRel:  eqjoin (Rel, EmptyRel);
EmptyRel:  eqjoin (EmptyRel, EmptyRel);
EmptyRel:  scjoin (Frag, EmptyRel);
EmptyRel:  rownum (EmptyRel);
EmptyRel:  project (EmptyRel);
EmptyRel:  select_ (EmptyRel);
EmptyRel:  sum (EmptyRel);
EmptyRel:  count_ (EmptyRel);
EmptyRel:  distinct (EmptyRel);
EmptyRel:  type (EmptyRel);
EmptyRel:  cast (EmptyRel);
EmptyRel:  num_add (EmptyRel);
EmptyRel:  num_subtract (EmptyRel);
EmptyRel:  num_multiply (EmptyRel);
EmptyRel:  num_divide (EmptyRel);
EmptyRel:  num_modulo (EmptyRel);
EmptyRel:  num_neg (EmptyRel);
EmptyRel:  num_gt (EmptyRel);
EmptyRel:  num_eq (EmptyRel);
EmptyRel:  and (EmptyRel);
EmptyRel:  or (EmptyRel);
EmptyRel:  not (EmptyRel);
EmptyRel:  strconcat (EmptyRel);
EmptyRel:  seqty1 (EmptyRel);
EmptyRel:  all (EmptyRel);


EmptyRel:  roots_ (element (Frag, EmptyRel, Rel));
EmptyRel:  roots_ (element (Frag, EmptyRel, EmptyRel));
EmptyRel:  roots_ (attribute (EmptyRel, Rel));
EmptyRel:  roots_ (attribute (Rel, EmptyRel));
EmptyRel:  roots_ (attribute (EmptyRel, EmptyRel));
EmptyRel:  roots_ (textnode (EmptyRel));
EmptyRel:  roots_ (comment (EmptyRel));
EmptyRel:  roots_ (processi (EmptyRel));
EmptyRel:  roots_ (merge_adjacent (Frag, EmptyRel));


/* vim:set shiftwidth=4 expandtab filetype=c: */
