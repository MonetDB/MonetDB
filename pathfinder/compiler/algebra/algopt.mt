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
      num_add      /* arithmetic plus operator */
      num_subtract /* arithmetic minus operator */
      num_multiply /* arithmetic times operator */
      num_divide   /* arithmetic divide operator */
      ;


label Query
      AlgExpr
      EmptyExpr
      ;


Query:    serialize (AlgExpr)
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
AlgExpr:  num_add (AlgExpr);
AlgExpr:  num_subtract (AlgExpr);
AlgExpr:  num_multiply (AlgExpr);
AlgExpr:  num_divide (AlgExpr);

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

EmptyExpr: num_add (EmptyExpr);
EmptyExpr: num_subtract (EmptyExpr);
EmptyExpr: num_multiply (EmptyExpr);
EmptyExpr: num_divide (EmptyExpr);


/* vim:set shiftwidth=4 expandtab filetype=c: */
