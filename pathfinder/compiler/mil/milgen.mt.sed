/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * Map algebra expressions to MIL
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */
  
/* Auxiliary routines related to the translation are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "milgen_impl.h"

/*
 * FIXME
 *
 * We use the Unix `sed' tool to make `[[ e ]]' a synonym for
 * `(e)->core' (the core equivalent of e). The following sed expressions
 * will do the replacement.
 *
 * (The following lines contain the special marker that is used
 * in the build process. The build process will search the file
 * for these markers, extract the sed expressions and feed the file
 * with these expressions through sed. Write sed expressions in
 * _exactly_ this style!)
 *
 *!sed 's/FIXME/FIXME/g'
 *
 * (First line translates all `[[' into `(', second line translates all
 * `]]' into `)->core'.)
 */

};

node  lit_tbl      /* literal table */
      disjunion    /* union two relations with same schema */
      cross        /* cross product (Cartesian product) */
      project      /* algebra projection and renaming operator */
      rownum       /* consecutive number generation */
      serialize    /* result serialization */
      ;


label Query
      AlgExpr
      ;


Query:    serialize (AlgExpr)
    {   assert ($$);  /* avoid `root unused' warning */
        TOPDOWN;
    }
    =
    {
        /*
         * Set the variable `unused' to nil. Lateron, we will ``free''
         * variables by setting them to `unused'. Effectively this is
         * just setting them to nil, but makes the MIL code a bit more
         * readable.
         */
        execute (assgn (unused (), nil ()));

        tDO ($%1$);

        /* Here we would do serialization */

        /* and then we clean up */

        assert ($$);
    }
    ;

AlgExpr:  lit_tbl
    =
    {
        /*
         * Literal table construction:
         *  - Create a new BAT for each of the attributes.
         *  - The BATs get a common prefix (from new_var()), and their
         *    attribute name as a suffix (e.g. `a0001_iter'). The prefix
         *    will be stored in the algebra tree node.
         *  - All the heads are void (seqbase 0@0).
         *  - Tails will be filled with literal content (using `insert')
         */
        
        int            col, row;
        PFmil_ident_t  prefix = new_var ();   /* BAT name prefix */
        PFalg_type_t   t;

        /* only need to translate this expression if not already done so. */
        if ($$->bat_prefix)
            break;

        for (col = 0; col < $$->schema.count; col++) {
            /*
             * For each attribute and each of its types create a BAT
             * 
             * <prefix>_<att>_<type> := new (void, <type>).seqbase (0@0)
             */
            for (t = 1; t; t <<= 1)
                if (t & $$->schema.items[col].type)
                    execute (
                        assgn (
                            var (bat (prefix, $$->schema.items[col].name, t)),
                            seqbase (new (type (m_void), implty (t)),
                                     lit_oid(0))));

            /*
             * Insert the values.
             * For each row execute the insert statement
             * <prefix>_<attribute>.insert (<row>@0, <item>)
             */
            for (row = 0; row < $$->sem.lit_tbl.count; row++) {
                execute (
                    insert (
                        var (bat (prefix,
                                  $$->schema.items[col].name,
                                  $$->sem.lit_tbl.tuples[row].atoms[col].type)),
                             lit_oid (row),
                             literal ($$->sem.lit_tbl.tuples[row].atoms[col])));
            }

            /* and finally make all BATs read-only */
            for (t = 1; t; t <<= 1)
                if (t & $$->schema.items[col].type)
                    execute (
                        access (
                            var (bat (prefix, $$->schema.items[col].name, t)),
                            BAT_READ));

        }

        /* store prefix in algebra tree node */
        $$->bat_prefix = prefix;
    }
    ;

AlgExpr:  disjunion (AlgExpr, AlgExpr)
    =
    {
        int                  monoR;       /* monomorphic column in R */
        int                  i;
        PFalg_simple_type_t  t;           /* to iterate over types */
        char                *monobat;     /* name of BAT, representing mono-
                                             morphic type. This must have a
                                             void head and delivers us key oids
                                             we need. */
        PFmil_t             *shiftedS = NULL;

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();

        /* To get the max key oid in R, we require a monomorphic column of R */
        monoR = mono_col ($1$);
        if (monoR < 0)
            PFoops (OOPS_FATAL,
                    "Need at least one monomorphic column to do a UNION.");

        monobat = bat ($1$->bat_prefix,
                       $1$->schema.items[monoR].name,
                       $1$->schema.items[monoR].type);

        /* We start by copying all the BATs of R */
        for (i = 0; i < $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type) {
                    /*
                     * If both, R and S, contribute to this attribute/type,
                     * we need to make a physical copy of R's BAT. (Otherwise
                     * we would overwrite the original BAT with the inserts
                     * to come.) Of course, we also need to allow appending
                     * to the BAT in that case.
                     */
                    if (t & $2$->schema.items[i].type)
                        execute (
                            assgn (var (bat ($$->bat_prefix,
                                             $$->schema.items[i].name,
                                             t)),
                                   copy (var (bat ($1$->bat_prefix,
                                                   $1$->schema.items[i].name,
                                                   t)))),
                            access (var (bat ($$->bat_prefix,
                                              $$->schema.items[i].name,
                                              t)),
                                    BAT_APPEND));
                    else
                        execute (
                            assgn (var (bat ($$->bat_prefix,
                                             $$->schema.items[i].name,
                                             t)),
                                   var (bat ($1$->bat_prefix,
                                             $1$->schema.items[i].name,
                                             t))));
                }

        /* now ``insert'' all the BATs of S, shifting the key oids */
        for (i = 0; i < $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type) {

                    /* find out what we have to insert:
                     *  - For monomorphic columns, we can just `mark' a new
                     *    key oid for the S BAT.
                     *  - If a column is polymorphic, we need to increment
                     *    all key oids accordingly.
                     */
                    if (t == $2$->schema.items[i].type)

                        /*
                         * Column is monomorphic:
                         *
                         * s.reverse.mark(oid(int(r.reverse.max)+1)).reverse
                         *
                         */
                        shiftedS
                            = reverse (
                                mark (
                                    reverse (
                                        var (bat ($2$->bat_prefix,
                                                  $2$->schema.items[i].name,
                                                  $2$->schema.items[i].type))),
                                    cast (
                                        type (m_oid),
                                        plus (
                                            cast (
                                                type (m_int),
                                                max (
                                                    reverse (
                                                        var (monobat)))),
                                            lit_int (1)))));
                    else
                        /*
                         * Column is polymorphic:
                         *
                         * ([oid]([+]([int](s.reverse),
                         *            int (r.reverse.max) + 1))).reverse
                         *
                         */
                        shiftedS
                            = reverse (
                                mcast (
                                    type (m_oid),
                                    mplus (
                                        mcast (
                                            type (m_int),
                                            reverse (
                                                var (bat ($2$->bat_prefix,
                                                          $2$->schema.items[i]
                                                              .name,
                                                          $2$->schema.items[i]
                                                              .type)))),
                                        plus (
                                            cast (
                                                type (m_int),
                                                max (
                                                    reverse (
                                                        var (monobat)))),
                                            lit_int (1)))));

                    /*
                     * We now have the expression that should be inserted
                     * into the result BAT in shifted S.
                     * Depending on whether there actually is a BAT (from R),
                     * we either insert the shifted S, or the shifted S will
                     * form the result on its own.
                     * If we inserted into the BAT from above, we need to
                     * set it read-only afterwards.
                     */
                    if (t & $1$->schema.items[i].type)
                        execute (binsert (var (bat ($$->bat_prefix,
                                                    $$->schema.items[i].name,
                                                    t)),
                                          shiftedS),
                                 access (var (bat ($$->bat_prefix,
                                                   $$->schema.items[i].name,
                                                   t)),
                                         BAT_READ));
                    else
                        execute (assgn (var (bat ($$->bat_prefix,
                                                  $$->schema.items[i].name,
                                                  t)),
                                        shiftedS));
                }

        /* maybe we can already de-allocate our arguments */
        deallocate ($1$, $$->refctr);
        deallocate ($2$, $$->refctr);
    }
    ;

AlgExpr:  cross (AlgExpr, AlgExpr) { cost = 2; }
    =
    {
        /*
         * Cross product between two tables (R x S):
         *
         * - Grab a monomorphic attribute from R and S.
         * - Then do
         *
         * tmp := join (r_att.project (0@0), s_att.project (0@0).reverse);
         * <prefix>_att1_t1 := tmp.mark (0@0).reverse.join (r_att1_t1);
         * <prefix>_att2_t2 := tmp.mark (0@0).reverse.join (r_att2_t2);
         * ...
         * <prefix>_atti := tmp.reverse.mark (0@0).reverse.join (s_att1);
         * ...
         * tmp := nil;
         */
        int                  monoR, monoS;  /* columns with monomorphic attr. */
        PFalg_simple_type_t  t;             /* used for iteration over types */
        int                  i;

        /* only need to translate this expression if not already done so. */
        if ($$->bat_prefix)
            break;

        /* find monomorphic attributes or abort */
        monoR = mono_col ($1$);
        monoS = mono_col ($2$);
        if (monoR < 0 || monoS < 0)
            PFoops (OOPS_FATAL,
                    "need at least one monomorphic column for cross product");

        /* we can now compute the `tmp' BAT */
        execute (
            assgn (
                var ("tmp"),
                join (project (var (bat ($1$->bat_prefix,
                                         $1$->schema.items[monoR].name,
                                         $1$->schema.items[monoR].type)),
                               lit_oid (0)),
                      reverse (
                          project (var (bat ($2$->bat_prefix,
                                             $2$->schema.items[monoS].name,
                                             $2$->schema.items[monoS].type)),
                                   lit_oid (0))))),
            assgn (var ("tmp1"), reverse (mark (var ("tmp"), lit_oid (0)))),
            assgn (var ("tmp2"),
                   reverse (mark (reverse (var ("tmp")), lit_oid (0)))));

        /* we need a new prefix for the result */
        $$->bat_prefix = new_var ();

        /* now for each attribute and each type in R do the join with tmp */
        for (i = 0; i < $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type)
                    execute (
                        assgn (var (bat ($$->bat_prefix,
                                         $1$->schema.items[i].name,
                                         t)),
                               join (var ("tmp1"),
                                     var (bat ($1$->bat_prefix,
                                               $1$->schema.items[i].name,
                                               t)))));

        /* and the same for S */
        for (i = 0; i < $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type)
                    execute (
                        assgn (var (bat ($$->bat_prefix,
                                         $2$->schema.items[i].name,
                                         t)),
                               join (var ("tmp2"),
                                     var (bat ($2$->bat_prefix,
                                               $2$->schema.items[i].name,
                                               t)))));

        /* we no longer need the `tmp' variables */
        execute (assgn (var ("tmp"), unused()),
                 assgn (var ("tmp1"), unused()),
                 assgn (var ("tmp2"), unused()));

        /* maybe we can already de-allocate our arguments */
        deallocate ($1$, $$->refctr);
        deallocate ($2$, $$->refctr);
    }
    ;

AlgExpr:  cross (Query, AlgExpr)
    {
        /*
         * The cross product becomes particularly easy, if one
         * operand is just a one-row literal table.
         */
        if ($1$->sem.lit_tbl.count != 1)
            ABORT;
        cost = 1;
    }
    =
    {
        /*
         * The cross product between a one-column literal table
         * and any other table is particularly easy to evaluate:
         *
         *  R(r1, r2,...rn)  X  S(s1, s2,...sn)
         *
         *  translates into
         *
         *  <prefix>_s1 := S_s1;
         *  <prefix>_s2 := S_s2;
         *  ...
         *  <prefix>_sn := S_sn;
         *
         *  <prefix>_r1 := S_s1.project (v1);
         *  <prefix>_r2 := S_s1.project (v2);
         *  ...
         *  <prefix>_rn := S_s1.project (vn);
         *
         * All the source BATs should be marked read-only. This access
         * restriction will automatically be propagated to the new
         * result BAT, so no need to explictly set something.
         */

        int                 col;
        PFmil_ident_t       prefix = new_var ();   /* BAT name prefix */
        PFalg_simple_type_t t;
        int                 mono;

        assert ($2$->bat_prefix);

        /* Copy BAT for each attribute in S.
         */
        for (col = 0; col < $2$->schema.count; col++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[col].type)
                    execute (assgn (var (bat (prefix,
                                              $2$->schema.items[col].name,
                                              t)),
                                    var (bat ($2$->bat_prefix,
                                              $2$->schema.items[col].name,
                                              t))));

        /* we need at least one monomorphic column */
        mono = mono_col ($2$);
        if (mono < 0)
            PFoops (OOPS_FATAL,
                    "Cannot handle projection on relations with only "
                    "polymorphic attributes.");

        /* Now handle each attribute in R */
        for (col = 0; col < $1$->schema.count; col++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[col].type)
                    execute (
                        assgn (var (bat (prefix,
                                         $1$->schema.items[col].name,
                                         t)),
                                    project (
                                        var (
                                            bat($2$->bat_prefix,
                                                $2$->schema
                                                    .items[mono].name,
                                                t)),
                                            literal ($1$->sem.lit_tbl
                                                     .tuples[0].atoms[col]))));

        /* maybe we can already de-allocate our arguments */
        deallocate ($1$, $$->refctr);
        deallocate ($2$, $$->refctr);
    }
    ;

AlgExpr:  project (AlgExpr)
    =
    {
        /*
         * Projection is rather easy: For each of the ``new'' attributes
         * just do a
         *   <new_prefix>_<new_att> := <old_prefix>_<old_att>;
         */
        int i;
        PFalg_simple_type_t t;

        assert ($1$->bat_prefix);

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        for (i = 0; i < $$->sem.proj.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $$->schema.items[i].type)
                    execute (assgn (var (bat ($$->bat_prefix,
                                              $$->sem.proj.items[i].new,
                                              t)),
                                    var (bat ($1$->bat_prefix,
                                              $$->sem.proj.items[i].old,
                                              t))));

        /* maybe we can already de-allocate our argument */
        deallocate ($1$, $$->refctr);
    }
    ;
AlgExpr:  rownum (AlgExpr);


/* vim:set shiftwidth=4 expandtab filetype=c: */
