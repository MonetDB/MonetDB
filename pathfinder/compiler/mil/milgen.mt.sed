/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue { 

/*
 * Map algebra expressions to MIL
 *
 * The code in this file compiles an algebra expression tree into a
 * MIL program that computes the algebra expression result. The code
 * produces a unique prefix for each algebra expression node (almost,
 * in few cases an algebra expression need not be computed explicitly).
 * This prefix will be available in the algebra tree node in the field
 * @a bat_prefix after compilation. If print the algebra tree as AT&T
 * dot code, each node will be labeled with this prefix.
 *
 * For each prefix (i.e., each algebra tree node) BATs are produced
 * for each attribute/type combination. The BATs names are composed
 * from the prefix, following an underscore (`_'), the attribute
 * (``column'') name, another underscore, and the type of this BAT, e.g.
 *
 *  - @c a0013_iter_nat: Algebra node `a0013', attribute `iter' of
 *    type `nat' (Will be implemented as @c oid).
 *  - @c a0013_item_str: Values of type `string' that contribute to
 *    attribute `item' of the algebra node `a0013'.
 *
 * Algebra results will be computed bottom-up and stored in their
 * corresponding BATs. Each result is kept as long as the result may
 * still be used for some computation. As soon as a result has been
 * used the last time, it will be set to @c nil and thus discarded.
 * (Actually, the variable will be set to @c unused, with @c unused
 * defined to @c nil before the actual computation starts. This
 * should make reading and debugging the resulting MIL code a bit
 * easier.)
 *
 * The resulting MIL code will finally call the @c serialize function
 * from the Pathfinder runtime module.
 *
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
      eqjoin       /* equi-join */
      project      /* algebra projection and renaming operator */
      select_      /* select tuples with a certain attribute value = true */
      rownum       /* consecutive number generation */
      serialize    /* result serialization */

      num_add      /* arithmetic plus operator */
      num_subtract /* arithmetic minus operator */
      num_multiply /* arithmetic times operator */
      num_divide   /* arithmetic divide operator */

      num_gt       /* numeric greater than */
      num_eq       /* numeric equality */

      not          /* boolean negation (true <--> false) */

      cast         /* algebra cast operator */
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
        bool has_nat_part =  (aat_nat  & attr_type ($1$, "item"));
        bool has_int_part =  (aat_int  & attr_type ($1$, "item"));
        bool has_str_part =  (aat_str  & attr_type ($1$, "item"));
        bool has_node_part = (aat_node & attr_type ($1$, "item"));
        bool has_dec_part =  (aat_dec  & attr_type ($1$, "item"));
        bool has_dbl_part =  (aat_dbl  & attr_type ($1$, "item"));
        bool has_bln_part =  (aat_bln  & attr_type ($1$, "item"));

        /*
         * Set the variable `unused' to nil. Lateron, we will ``free''
         * variables by setting them to `unused'. Effectively this is
         * just setting them to nil, but makes the MIL code a bit more
         * readable.
         */
        execute (assgn (unused (), nil ()),
                 assgn (var ("tmp"), unused()),
                 assgn (var ("tmp1"), unused()),
                 assgn (var ("tmp2"), unused()));

        /* invoke compilation */
        tDO ($%1$);

        /*
         * serialize result. We might need to rewrite this slightly
         * as soon as we have a ``real'' serialization function.
         */
        execute (serialize ($1$->bat_prefix,
                            has_nat_part, has_int_part, has_str_part,
                            has_node_part, has_dec_part, has_dbl_part,
                            has_bln_part
                           ));

        /* and then we clean up */
        deallocate ($1$, $$->refctr);

        assert ($$); /* avoid `root unused' warning */
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
        /*
         * Computing the disjoint union  R U S:
         *
         * First consider R:
         *
         * - For attribute/type combinations that are in R, but not in S:
         *   ``Rename'' the BAT to the new prefix. (We won't be destructive
         *   with this BAT, so it doesn't hurt to have just a reference.)
         *
         *   <prefix>_<att>_<t> := <R>_<att>_<t>;
         *   
         * - For attribute/type combinations in R that also appear in S,
         *   we need to make a physical copy, as the following insert
         *   would have side effects otherwise:
         *
         *   <prefix>_<att>_<t> := <R>_<att>_<t>.copy;
         *   <prefix>_<att>_<t>.access (BAT_APPEND);
         *
         *   (We know we will insert in the sequel and thus make the
         *   BAT appendable right away.)
         *
         * Now we think about S:
         *
         * - First we need to shift all tuple ids by a constant value
         *   (the number of tuples in R). To get this value, we pick
         *   a monomorphic attribute from R and its maximum head value.
         *   (This is just a BAT descriptor lookup and thus for free.)
         *
         *   o For each monomorphic attribute in S, we can now simply
         *     shift the heads by re-marking them, which is expressed
         *     by the following MIL expression:
         *
         *     <S>_<att>_<t>.reverse.mark (oid(int(R.reverse.max)+1)).reverse
         *
         *     (Where R is the monomorphic attribute BAT from R.)
         *
         *   o For polymorphic attributes in S, we really have to do
         *     shifting with arithmetics:
         *
         *     ([oid]([+]([int](<S>_<att>_<t>.reverse),
         *                int(R.reverse.max) + 1))).reverse
         *
         * - The resulting expression (named `shiftedS' in the code)
         *   now either must be inserted into already what we have from R:
         *
         *   <prefix>_<att>_<t>.insert (<shiftedS>);
         *   <prefix>_<att>_<t>.access (BAT_READ);
         *
         *   or the shifted expression already forms the result for this
         *   attribute/type combination, if there was no contribution from R:
         *
         *   <prefix>_<att>_<t> := <shiftedS>;
         */

        int                  monoR;       /* monomorphic column in R */
        int                  i;
        PFalg_simple_type_t  t;           /* to iterate over types */
        char                *monobat;     /* name of BAT, representing mono-
                                             morphic type. This must have a
                                             void head and delivers us key oids
                                             we need. */
        PFmil_t             *shiftedS = NULL;

        assert ($1$->bat_prefix); assert ($2$->bat_prefix);

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
                                        add (
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
                                    madd (
                                        mcast (
                                            type (m_int),
                                            reverse (
                                                var (bat ($2$->bat_prefix,
                                                          $2$->schema.items[i]
                                                              .name,
                                                          t)))),
                                        add (
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
                    if (t & attr_type ($1$, $2$->schema.items[i].name))
                    /* if (t & $1$->schema.items[i].type) */
                        execute (binsert (var (bat ($$->bat_prefix,
                                                    $2$->schema.items[i].name,
                                                    t)),
                                          shiftedS),
                                 access (var (bat ($$->bat_prefix,
                                                   $2$->schema.items[i].name,
                                                   t)),
                                         BAT_READ));
                    else
                        execute (assgn (var (bat ($$->bat_prefix,
                                                  $2$->schema.items[i].name,
                                                  t)),
                                        shiftedS));
                }

        /* maybe we can already de-allocate our arguments */
        deallocate ($1$, $$->refctr);
        deallocate ($2$, $$->refctr);
    }
    ;

AlgExpr:  eqjoin (AlgExpr, AlgExpr)
    =
    {
        /*
         * Equi-join between R and S, over attributes r and s.
         * 
         * tmp := <R>_<r>_<t>.join (<S>_<s>_<t>.reverse);
         * tmp1 := tmp.mark (0@0).reverse;
         * tmp2 := tmp.reverse.mark (0@0).reverse;
         *
         * <prefix>_<attR>_<t> := tmp1.leftjoin (<R>_<attR>_<t>);
         *  ...
         * <prefix>_<attS>_<t> := tmp2.leftjoin (<S>_<attS>_<t>);
         *  ...
         * tmp := unused; tmp1 := unused; tmp2 := unused;
         *
         * Note that this requires the join attributes to have
         * the same monomorphic type.
         */

        PFalg_simple_type_t  t;             /* used for iteration over types */
        int                  i;

        assert ($1$->bat_prefix); assert ($2$->bat_prefix);

        /* Both attributes must have the same type
         * That type must be monomorphic. */
        assert (attr_type ($1$, $$->sem.eqjoin.att1)
                == attr_type ($2$, $$->sem.eqjoin.att2));
        assert (is_monomorphic (attr_type ($1$, $$->sem.eqjoin.att1)));

        /* only need to translate this expression if not already done so. */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();

        /* Create the tmp BATs */
        execute (
            /* tmp := <R>_<r>_<t>.join (<S>_<s>_<t>.reverse); */
            reassgn (var ("tmp"),
                     join (var (bat ($1$->bat_prefix,
                                     $$->sem.eqjoin.att1,
                                     attr_type ($1$, $$->sem.eqjoin.att1))),
                           reverse (
                               var (bat ($2$->bat_prefix,
                                         $$->sem.eqjoin.att2,
                                         attr_type($2$,
                                                   $$->sem.eqjoin.att2)))))),
            /* tmp1 := tmp.mark (0@0).reverse; */
            reassgn (var ("tmp1"),
                     reverse (mark (var ("tmp"), lit_oid (0)))),
            /* tmp2 := tmp.reverse.mark (0@0).reverse; */
            reassgn (var ("tmp2"),
                     reverse (mark (reverse (var ("tmp")), lit_oid (0)))));

        /* Now fetch all the attributes from R */
        for (i = 0; i < $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type)
                    execute (
                        assgn (var (bat ($$->bat_prefix,
                                         $1$->schema.items[i].name,
                                         t)),
                               leftjoin (var ("tmp1"),
                                         var (bat ($1$->bat_prefix,
                                                   $1$->schema.items[i].name,
                                                   t)))));

        /* and now those from S */
        for (i = 0; i < $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type)
                    execute (
                        assgn (var (bat ($$->bat_prefix,
                                         $2$->schema.items[i].name,
                                         t)),
                               leftjoin (var ("tmp2"),
                                         var (bat ($2$->bat_prefix,
                                                   $2$->schema.items[i].name,
                                                   t)))));

        /* we no longer need the tmp variables */
        execute (reassgn (var ("tmp"), unused()),
                 reassgn (var ("tmp1"), unused()),
                 reassgn (var ("tmp2"), unused()));

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
         * tmp := r_att.cross (s_att.reverse);
         * tmp1 := tmp.mark (0@0).reverse;
         * tmp2 := tmp.reverse.mark (0@0).reverse;
         * <prefix>_att1_t1 := tmp1.leftjoin (r_att1_t1);
         * <prefix>_att2_t2 := tmp1.leftjoin (r_att2_t2);
         * ...
         * <prefix>_atti_t1 := tmp2.leftjoin (s_att1_t1);
         * ...
         * tmp := nil;
         */
        int                  monoR, monoS;  /* columns with monomorphic attr. */
        PFalg_simple_type_t  t;             /* used for iteration over types */
        int                  i;

        assert ($1$->bat_prefix); assert ($2$->bat_prefix);

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
            /* tmp := r_att.cross (s_att.reverse); */
            reassgn (
                var ("tmp"),
                cross (var (bat ($1$->bat_prefix,
                                 $1$->schema.items[monoR].name,
                                 $1$->schema.items[monoR].type)),
                       reverse (var (bat ($2$->bat_prefix,
                                          $2$->schema.items[monoS].name,
                                          $2$->schema.items[monoS].type))))),
            /* tmp1 := tmp.mark (0@0).reverse; */
            reassgn (var ("tmp1"), reverse (mark (var ("tmp"), lit_oid (0)))),
            /* tmp2 := tmp.reverse.mark (0@0).reverse; */
            reassgn (var ("tmp2"),
                     reverse (mark (reverse (var ("tmp")), lit_oid (0)))));

        /* we need a new prefix for the result */
        $$->bat_prefix = new_var ();

        /* now for each attribute and each type in R do the join with tmp */
        for (i = 0; i < $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type)
                    execute (
                        /* <prefix>_att1_t1 := tmp1.leftjoin (r_att1_t1); */
                        assgn (var (bat ($$->bat_prefix,
                                         $1$->schema.items[i].name,
                                         t)),
                               leftjoin (var ("tmp1"),
                                         var (bat ($1$->bat_prefix,
                                                   $1$->schema.items[i].name,
                                                   t)))));

        /* and the same for S */
        for (i = 0; i < $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type)
                    execute (
                        /* <prefix>_att1_t1 := tmp1.leftjoin (s_att1_t1); */
                        assgn (var (bat ($$->bat_prefix,
                                         $2$->schema.items[i].name,
                                         t)),
                               leftjoin (var ("tmp2"),
                                         var (bat ($2$->bat_prefix,
                                                   $2$->schema.items[i].name,
                                                   t)))));

        /* we no longer need the `tmp' variables */
        execute (reassgn (var ("tmp"), unused()),
                 reassgn (var ("tmp1"), unused()),
                 reassgn (var ("tmp2"), unused()));

        /* maybe we can already de-allocate our arguments */
        deallocate ($1$, $$->refctr);
        deallocate ($2$, $$->refctr);
    }
    ;

AlgExpr:  cross (lit_tbl, AlgExpr)
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
         */

        int                 col;
        PFalg_simple_type_t t;
        int                 mono;

        assert ($2$->bat_prefix);

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();

        /* Copy BAT for each attribute in S.
         */
        for (col = 0; col < $2$->schema.count; col++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[col].type)
                    execute (assgn (var (bat ($$->bat_prefix,
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
                        assgn (var (bat ($$->bat_prefix,
                                         $1$->schema.items[col].name,
                                         t)),
                                    project (
                                        var (
                                            bat($2$->bat_prefix,
                                                $2$->schema
                                                    .items[mono].name,
                                                $2$->schema
                                                    .items[mono].type)),
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

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

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
AlgExpr:  rownum (AlgExpr)
    =
    {
        /*
         * Sort the argument expression, first by the first sort
         * criterion, then refine by all the others:
         *
         * tmp := <R>_<sort1>_<t>.reverse.sort.reverse;
         *
         * foreach sort specifier:
         *   tmp := tmp.CTrefine (<R>_<sorti>_<t>);
         *
         * `tmp' now gives us the order we need. Depending on the
         * presence of a partitioning attribute, we either use mark()
         * oder mark_grp().
         *
         * Without partitioning attribute:
         *   <prefix>_<att>_<t> := tmp.mark (1@0);
         *
         * With partitioning attribute:
         *   tmp := tmp.mirror.leftjoin (<R>_<part>_<t>);
         *   tmp2 := <R>_<part>_<t>.reverse.kunique.project (1@0);
         *   <prefix>_<att>_<t> := tmp.mark_grp (tmp2);
         *   tmp := unused; tmp2 := unused;
         *
         * This way, the newly constructed BAT contains all the BUNs
         * we like it to have. However, we still have to fix its order
         * to have all BATs aligned.
         *
         *   <prefix>_<att>_<t> := <prefix>_<att>_<t>.sort;
         *   <prefix>_<att>_<t>.reverse.mark (0@0).reverse; # Make head void
         *
         * All the remaining attributes are simply copied into the
         * result.
         */
        int i;
        /* PFalg_type_t t; */

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        /*
         * We make a few assumptions here:
         *  - The operand expression has already been translated.
         *  - There is at least one sort specifier.
         *    (We might need to loosen this restriction in the future.)
         *  - The attribute that we group over must be of type oid.
         */
        assert ($1$->bat_prefix);
        assert ($$->sem.rownum.sortby.count >= 1);
        assert ((!$$->sem.rownum.part)
                || attr_type ($1$, $$->sem.rownum.part) == aat_nat);

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        /*
         * Sort BAT according to sort specification, first by the
         * primary specification.
         *
         * tmp := <R>_<sort1>_<t>.reverse.sort.reverse;
         */
        execute (
            reassgn (var ("tmp"),
                   reverse (
                       sort (
                           reverse (var (bat ($1$->bat_prefix,
                                              $$->sem.rownum.sortby.atts[0],
                                              attr_type ($1$,
                                                         $$->sem.rownum
                                                            .sortby.atts[0])
                                             )))))));

        /* then refine sorting for each additional attribute given
         *
         * foreach sorti
         *   tmp := tmp.CTrefine (<R>_<sorti>_<t>);
         */
        for (i = 1; i < $$->sem.rownum.sortby.count; i++)
            execute (
                reassgn (var ("tmp"),
                    ctrefine (var ("tmp"),
                              var (bat ($1$->bat_prefix,
                                        $$->sem.rownum.sortby.atts[i],
                                        attr_type ($1$,
                                                   $$->sem.rownum
                                                       .sortby.atts[i]))))));

        /*
         * `tmp' now gives us the order we'd like to have (in its head,
         * we don't need the tail at all).
         *
         * If a partitioning attribute has been given, we fetch that
         * into the tail value and use mark_grp() to get the numbering.
         * Otherwise we just use the simple mark().
         *
         * tmp  := tmp.mirror.leftjoin (<R>_<part>_<t>);
         * tmp2 := <R>_<part>_<t>.reverse.kunique.project (1@0);
         *         (This defines the groups for the mark_grp operator.)
         * <prefix>_<att>_<t> := tmp.mark_grp (tmp2);
         * tmp := unused; tmp2 := unused;
         *
         */
        if ($$->sem.rownum.part)
            execute (
                reassgn (var ("tmp"),
                         leftjoin (
                             mirror (var ("tmp")),
                             var (bat ($1$->bat_prefix,
                                       $$->sem.rownum.part,
                                       attr_type ($1$,
                                                  $$->sem.rownum.part))))),
                reassgn (var ("tmp2"),
                         project (
                             kunique (
                                 reverse (
                                     var (
                                         bat ($1$->bat_prefix,
                                              $$->sem.rownum.part,
                                              attr_type ($1$,
                                                         $$->sem.rownum.part))))
                                     ),
                           lit_oid (1))),
                assgn (var (bat ($$->bat_prefix,
                                 $$->sem.rownum.attname,
                                 aat_nat)),
                       mark_grp (var ("tmp"), var ("tmp2"))),
                reassgn (var ("tmp"), unused ()),
                reassgn (var ("tmp2"), unused ()));
        else
            /*
             * If no partitioning attribute is given, just do a simple
             * mark (1@0).
             */
            execute (
                assgn (var (bat ($$->bat_prefix,
                                 $$->sem.rownum.attname,
                                 aat_nat)),
                       mark (var ("tmp"), lit_oid (1))),
                reassgn (var ("tmp"), unused ()));


        /*
         * Essentially the newly constructed BAT now contains exactly the
         * BUNs we like to have. Their order, however, does not match the
         * order of all the attribute BATs now. As we depend in other
         * translations on the fact that all relations are represented by
         * a set of BATs that are ordered by their tuple id (their head),
         * preferably by a void head, we need to bring all the attribute
         * BATs into the same order.
         *
         * This means there are two options:
         *
         *  - Either we pick the order of the new row numbers and re-sort
         *    all the other attributes.
         *  - Or we re-sort the new attribute BAT, which means that the
         *    result is not sorted by the constructed row numbers.
         *
         * As the latter seems less work, we decided on the second option.
         * However, if it turned out later that it is benefitial to order
         * BATs on the generated row numbers, we might better re-think that
         * decision.
         *
         * For now, we do a
         *   <prefix>_<att>_<t> := <prefix>_<att>_<t>.sort;
         *   <prefix>_<att>_<t>.reverse.mark (0@0).reverse; # Make head void
         */
        execute (
            reassgn (var (bat ($$->bat_prefix,
                               $$->sem.rownum.attname,
                               aat_nat)),
                     sort (var (bat ($$->bat_prefix,
                                     $$->sem.rownum.attname,
                                     aat_nat)))),
            reassgn (var (bat ($$->bat_prefix,
                               $$->sem.rownum.attname,
                               aat_nat)),
                     reverse (
                         mark (
                             reverse (
                                 var (bat ($$->bat_prefix,
                                           $$->sem.rownum.attname,
                                           aat_nat))),
                             lit_oid (0))))
            );

        /* copy all the remaining attributes */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  num_add (AlgExpr)
    =
    {
        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        assert ($1$->bat_prefix);

        /*
         * All the attributes in our operand are also in the result,
         * and we just copy them.
         */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /*
         * The numeric plus can now be done with help of Monet's
         * multiplex operator.
         */
        execute (assgn (var (bat ($$->bat_prefix,
                                  $$->sem.binary.res,
                                  attr_type ($$, $$->sem.binary.res))),
                        madd (var (bat ($$->bat_prefix,
                                        $$->sem.binary.att1,
                                        attr_type ($$, $$->sem.binary.att1))),
                              var (bat ($$->bat_prefix,
                                        $$->sem.binary.att2,
                                        attr_type ($$, $$->sem.binary.att2))))
                       ));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  num_subtract (AlgExpr)
    =
    {
        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        assert ($1$->bat_prefix);

        /*
         * All the attributes in our operand are also in the result,
         * and we just copy them.
         */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /*
         * The numeric minus can now be done with help of Monet's
         * multiplex operator.
         */
        execute (assgn (var (bat ($$->bat_prefix,
                                  $$->sem.binary.res,
                                  attr_type ($$, $$->sem.binary.res))),
                        msub (var (bat ($$->bat_prefix,
                                        $$->sem.binary.att1,
                                        attr_type ($$, $$->sem.binary.att1))),
                              var (bat ($$->bat_prefix,
                                        $$->sem.binary.att2,
                                        attr_type ($$, $$->sem.binary.att2))))
                       ));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  num_multiply (AlgExpr)
    =
    {
        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        assert ($1$->bat_prefix);

        /*
         * All the attributes in our operand are also in the result,
         * and we just copy them.
         */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /*
         * The numeric minus can now be done with help of Monet's
         * multiplex operator.
         */
        execute (assgn (var (bat ($$->bat_prefix,
                                  $$->sem.binary.res,
                                  attr_type ($$, $$->sem.binary.res))),
                        mmult (var (bat ($$->bat_prefix,
                                         $$->sem.binary.att1,
                                         attr_type ($$, $$->sem.binary.att1))),
                               var (bat ($$->bat_prefix,
                                         $$->sem.binary.att2,
                                         attr_type ($$, $$->sem.binary.att2))))
                       ));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  num_divide (AlgExpr)
    =
    {
        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        $$->bat_prefix = new_var ();   /* BAT name prefix */

        assert ($1$->bat_prefix);

        /*
         * All the attributes in our operand are also in the result,
         * and we just copy them.
         */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /*
         * The numeric minus can now be done with help of Monet's
         * multiplex operator.
         */
        execute (assgn (var (bat ($$->bat_prefix,
                                  $$->sem.binary.res,
                                  attr_type ($$, $$->sem.binary.res))),
                        mdiv (var (bat ($$->bat_prefix,
                                        $$->sem.binary.att1,
                                        attr_type ($$, $$->sem.binary.att1))),
                              var (bat ($$->bat_prefix,
                                        $$->sem.binary.att2,
                                        attr_type ($$, $$->sem.binary.att2))))
                       ));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  cast (AlgExpr)
    =
    {
        /*
         * Cast a column to a specified type.
         *
         * All columns that are not to be cast remain unchanged and
         * are just copied to the result.
         *
         * For the ``interesting'' column, we cast all the BATs that
         * form the attribute (we might have a polymorphic column at
         * hand), and union them. Finally we sort the result by its
         * head and make the head column a void column. (Note that
         * merging all the constituents of a polymorphic BAT should
         * yield a dense head again.)
         *
         */
        PFalg_simple_type_t t;
        int i;

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        assert ($1$->bat_prefix);

        $$->bat_prefix = new_var ();

        for (i = 0; i < $1$->schema.count; i++)
            if (strcmp ($1$->schema.items[i].name, $$->sem.cast.att)) {
                /*
                 * If this is not the attribute to cast, just copy
                 * all the BATs that form it.
                 */
                for (t = 1; t; t <<= 1)
                    if (t & $1$->schema.items[i].type)
                        execute (assgn (var (bat ($$->bat_prefix,
                                                  $1$->schema.items[i].name,
                                                  t)),
                                        var (bat ($1$->bat_prefix,
                                                  $1$->schema.items[i].name,
                                                  t))));
            }
            else {
                /*
                 * Attribute i is the attribute to cast. Union the result
                 * BAT from all the BATs that formed $1$, casting them
                 * to the requested type.
                 */
                bool empty = true;
                PFmil_t *casted = NULL;

                for (t = 1; t; t <<= 1)
                    if (t & $1$->schema.items[i].type) {

                        /*
                         * construct `[type] (<prefix>_<att>_<t>)'.
                         * put the MIL expression that represents the
                         * casted bat into the C variable `casted'.
                         */
                        casted = mcast (type (impl_types[$$->sem.cast.ty]),
                                        var (bat ($1$->bat_prefix,
                                                  $1$->schema.items[i].name,
                                                  t)));

                        /*
                         * If this is the first part of the result, just
                         * assign it to the result BAT.
                         */
                        if (empty) {
                            execute (
                                assgn (var (bat ($$->bat_prefix,
                                                 $1$->schema.items[i].name,
                                                 $$->sem.cast.ty)),
                                       casted));
                            empty = false;
                        }
                        /*
                         * Otherwise union this new part and the existing one.
                         */
                        else
                            execute (
                                reassgn (
                                    var (bat ($$->bat_prefix,
                                              $1$->schema.items[i].name,
                                              $$->sem.cast.ty)),
                                    kunion (var (bat ($$->bat_prefix,
                                                      $1$->schema.items[i].name,
                                                      $$->sem.cast.ty)),
                                            casted)));
                    }

                /*
                 * If the operand attribute was polymorphic, we need
                 * to ``fix'' the head colum to void. (It must be
                 * monomorphic after casting.
                 *
                 * <prefix>_<att>_<t>
                 *     := <prefix>_<att>_<t>.sort.reverse.mark (0@0).reverse;
                 */
                if (!is_monomorphic ($1$->schema.items[i].type))
                    execute (
                        reassgn (
                            var (bat ($$->bat_prefix,
                                      $1$->schema.items[i].name,
                                      $$->sem.cast.ty)),
                            reverse (
                                mark (
                                    reverse (
                                        sort (
                                            var (bat ($$->bat_prefix,
                                                      $1$->schema.items[i].name,
                                                      $$->sem.cast.ty)))),
                                    lit_oid (0)))));
            }
    }
    ;

AlgExpr:  num_gt (AlgExpr);
    =
    {
        /*
         * The equality operator is readily available in Monet:
         */

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        assert ($1$);
        /* types of both operants must be the same and monomorphic */
        assert (attr_type ($1$, $$->sem.binary.att1)
                == attr_type ($1$, $$->sem.binary.att2));
        assert (is_monomorphic (attr_type ($1$, $$->sem.binary.att1)));

        $$->bat_prefix = new_var ();

        /* copy all attributes from $1$ */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /* <pref>_<res>_bln := [=](<R>_<att1>_<t>, <R>_<att2>_<t>); */
        execute (
            assgn (var (bat ($$->bat_prefix,
                             $$->sem.binary.res,
                             aat_bln)),
                   mgt (var (bat ($1$->bat_prefix,
                                  $$->sem.binary.att1,
                                  attr_type ($1$, $$->sem.binary.att1))),
                        var (bat ($1$->bat_prefix,
                                  $$->sem.binary.att2,
                                  attr_type ($1$, $$->sem.binary.att1))))));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  num_eq (AlgExpr)
    =
    {
        /*
         * The equality operator is readily available in Monet:
         */

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        assert ($1$);
        /* types of both operants must be the same and monomorphic */
        assert (attr_type ($1$, $$->sem.binary.att1)
                == attr_type ($1$, $$->sem.binary.att2));
        assert (is_monomorphic (attr_type ($1$, $$->sem.binary.att1)));

        $$->bat_prefix = new_var ();

        /* copy all attributes from $1$ */
        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        /* <pref>_<res>_bln := [=](<R>_<att1>_<t>, <R>_<att2>_<t>); */
        execute (
            assgn (var (bat ($$->bat_prefix,
                             $$->sem.binary.res,
                             aat_bln)),
                   meq (var (bat ($1$->bat_prefix,
                                  $$->sem.binary.att1,
                                  attr_type ($1$, $$->sem.binary.att1))),
                        var (bat ($1$->bat_prefix,
                                  $$->sem.binary.att2,
                                  attr_type ($1$, $$->sem.binary.att1))))));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  not (AlgExpr)
    =
    {
        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        assert ($1$->bat_prefix);
        assert (attr_type ($1$, $$->sem.unary.att) == aat_bln);

        $$->bat_prefix = new_var ();

        copy_rel ($1$->bat_prefix, $$->bat_prefix, $1$->schema);

        execute (
            assgn (var (bat ($$->bat_prefix,
                             $$->sem.unary.res,
                             aat_bln)),
                   mnot (var (bat ($1$->bat_prefix,
                                   $$->sem.unary.att,
                                   aat_bln)))));

        deallocate ($1$, $$->refctr);
    }
    ;

AlgExpr:  select_ (AlgExpr)
    =
    {
        /*
         * Selection by attribute `a':
         *
         * tmp := <R>_a_bln.select (true);
         * tmp1 := tmp.mark (0@0).reverse;
         *
         * for each attribute (except `a'):
         *   <prefix>_<att>_<t> := tmp2.leftjoin (<R>_a_<t>);
         *
         * <prefix>_a_bln := tmp.reverse.mark (0@0).reverse;
         * tmp := unused; tmp1 := unused;
         */
        int i;
        PFalg_simple_type_t t;

        /* no need to do anything if we already translated that expression */
        if ($$->bat_prefix)
            break;

        assert ($1$->bat_prefix);
        assert (attr_type ($1$, $$->sem.select.att) == aat_bln);

        execute (
            /* tmp := <R>_a_bln.select (true); */
            reassgn (var ("tmp"),
                     select_ (var (bat ($1$->bat_prefix,
                                       $$->sem.select.att,
                                       aat_bln)),
                             lit_bit (true))),
            /* tmp1 := tmp.mark (0@0).reverse; */
            reassgn (var ("tmp2"),
                     reverse (mark (var ("tmp"), lit_oid (0)))));

        for (i = 0; i < $1$->schema.count; i++)
            if (strcmp ($1$->schema.items[i].name, $$->sem.select.att))
                for (t = 1; t; t <<= 1)
                    if (t & $1$->schema.items[i].type)
                        execute (
                            assgn (var (bat ($$->bat_prefix,
                                             $1$->schema.items[i].name,
                                             t)),
                                   leftjoin (
                                       var ("tmp2"),
                                       var (bat ($1$->bat_prefix,
                                                 $1$->schema.items[i].name,
                                                 t)))));
        execute (
            /* <prefix>_a_bln := tmp.reverse.mark (0@0).reverse; */
            assgn (var (bat ($$->bat_prefix, $$->sem.select.att, aat_bln)),
                   reverse (mark (reverse (var ("tmp")), lit_oid (0)))),
            /* tmp := unused; */
            reassgn (var ("tmp"), unused ()),
            /* tmp1 := unused; */
            reassgn (var ("tmp2"), unused ()));

        deallocate ($1$, $$->refctr);
    }
    ;

/* vim:set shiftwidth=4 expandtab filetype=c: */
