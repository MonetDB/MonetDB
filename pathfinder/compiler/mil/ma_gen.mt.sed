/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

prologue {

/*
 * Map algebra expressions to MIL algebra.
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
  
/*
 * (The following lines contain the special marker that is used
 * in the build process. The build process will search the file
 * for these markers, extract the sed expressions and feed the file
 * with these expressions through sed. Write sed expressions in
 * _exactly_ this style!)
 *
 *!sed 's/\[\[/(*(LOOKUP(/'
 *!sed 's/\]\]/)))/'
 *
 * (Translate `[[ a, b, c ]]' into `*(LOOKUP (a, b, c))'. This means
 * we can write [[ $$, att, type ]] to reference the MIL algebra
 * expression stored in the environment of node $$ under the key
 * att/type.)
 */

/* Auxiliary routines related to the translation are located in
 * this separate included file to facilitate automated documentation
 * via doxygen.
 */	
#include "ma_gen_impl.c"

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


Query:    serialize (AlgExpr, AlgExpr)
    {   assert ($$);  /* avoid `root unused' warning */
    }
    =
    {
        assert ($1$->env);
        assert ($2$->env);

        /*
         * For each data type that is actually in the result, set it
         * as the (corresponding) parameter of serialize(). For all
         * the others, create a new empty BAT as the parameter.
         */
        $$->ma
            = serialize (
                [[ $2$, "pos", aat_nat ]],
                [[ $2$, "item", aat_int ]]
                   ? [[ $2$, "item", aat_int ]] : new (m_void, implty(aat_int)),
                [[ $2$, "item", aat_str ]]
                   ? [[ $2$, "item", aat_str ]] : new (m_void, implty(aat_str)),
                [[ $2$, "item", aat_dec ]]
                   ? [[ $2$, "item", aat_dec ]] : new (m_void, implty(aat_dec)),
                [[ $2$, "item", aat_dbl ]]
                   ? [[ $2$, "item", aat_dbl ]] : new (m_void, implty(aat_dbl)),
                [[ $2$, "item", aat_bln ]]
                   ? [[ $2$, "item", aat_bln ]] : new (m_void, implty(aat_bln)),
                [[ $2$, "item", aat_node ]]
                   ? [[ $2$, "item", aat_node ]] : new(m_void,implty(aat_node))
                    );

        assert (_ll);  /* avoid warning `unused parameter: _ll' */
    }
    ;

AlgExpr:  lit_tbl
    =
    {
        /*
         * Literal table construction:
         *  - Create a new BAT for each of the attributes. Use a void
         *    head if applicable, i.e., if the column is monomorphic.
         *  - Then insert() all the tuples.
         */
        unsigned int          col, row;
        PFalg_simple_type_t   t;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        $$->env = new_env ();

        for (col = 0; col < (unsigned int) $$->schema.count; col++) {
            /*
             * For each attribute and each of its types create a BAT
             */
            for (t = 1; t; t <<= 1)
                if (t & $$->schema.items[col].type) {
                    if (t == $$->schema.items[col].type)
                        /* monomorphic type: new (void, <t>).seqbase (0@0) */
                        [[ $$, $$->schema.items[col].name, t ]]
                            = seqbase (new (m_void, implty (t)), lit_oid (0));
                    else
                        /* polymorphic type: new (oid, <t>) */
                        [[ $$, $$->schema.items[col].name, t ]]
                            = new (m_oid, implty (t));
                }

            /*
             * Insert the values.
             */
            for (row = 0; row < (unsigned int) $$->sem.lit_tbl.count; row++)
                [[ $$,
                   $$->schema.items[col].name,
                   $$->sem.lit_tbl.tuples[row].atoms[col].type ]]
                    = insert (
                           [[ $$,
                              $$->schema.items[col].name,
                              $$->sem.lit_tbl.tuples[row].atoms[col].type ]],
                           lit_oid (row),
                           literal ($$->sem.lit_tbl.tuples[row].atoms[col]));
        }
    }
    ;

AlgExpr:  disjunion (AlgExpr, AlgExpr)
    =
    {
        /*
         * Disjoint union  R U S:
         *
         * - Copy everything from R into the result.
         * - Now consider S:
         *    o All heads in S must be shifted by R.count. We thus search
         *      for a monomorphic column in R first to get this count value.
         *    o If the fraction provided by S is the first of that type,
         *      the shifted value of S goes directly into the result.
         *    o otherwise, we append the shifted value of S to what we
         *      already have from R.
         */
        PFalg_simple_type_t  t = 0;
        unsigned int         i;
        PFma_op_t           *m;
        PFma_op_t           *shifted;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        assert ($1$->env); assert ($2$->env);

        /* Look for a monomorphic column in left argument */
        for (i = 0, t = 0; !t && i < (unsigned int) $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t == $1$->schema.items[i].type)
                    break;

        if (!t)
            PFoops (OOPS_FATAL,
                    "Current implementation of `disjunion' requires at "
                    "least one monomorphic column in left operand.");

        /* keep this monomorphic BAT (void head) in m */
        m = [[ $1$, $1$->schema.items[i].name, t ]];

        /* Copy everything we get from $1$ */
        copy_env ($$, $1$);

        /* Now append everything from $2$ */
        for (i = 0; i < (unsigned int) $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type) {

                    if (t == $2$->schema.items[i].type)
                        /*
                         * If this attribute of $2$ is monomorphic, shifting
                         * is easy: just move the seqbase.
                         */
                        shifted
                            = seqbase (
                                [[ $2$, $2$->schema.items[i].name, t ]],
                                oid (count (m)));
                    else
                        /*
                         * Otherwise we have to do some nasty arithmetics:
                         *
                         * ([oid]([+]([int]( $2$.reverse ), m.count))).reverse
                         */
                        shifted
                            = reverse (
                                moid (
                                    madd (
                                        mint (
                                            reverse (
                                                [[ $2$,
                                                   $2$->schema.items[i].name,
                                                   t ]])),
                                        count (m))));

                    if ([[ $$, $2$->schema.items[i].name, t ]])
                        /*
                         * If we already have something for this
                         * attribute/type combination, append the other
                         * part after shifting its head values by the
                         * size of the first part.
                         */
                        [[ $$, $2$->schema.items[i].name, t ]]
                            = append ([[ $$, $2$->schema.items[i].name, t ]],
                                      shifted);
                    else
                        /*
                         * If there's nothing there yet for this
                         * attribute/type combination, just return
                         * the second part, shifting it accordingly.
                         */
                        [[ $$, $2$->schema.items[i].name, t ]]
                            = shifted;
                }
    }
    ;

AlgExpr:  eqjoin (AlgExpr, AlgExpr)
    =
    {
        /*
         * Equi-join between R and S, over attributes r and s.
         *
         * tmp := r.join (s.reverse);
         * tmp1 := tmp.mark (0@0).reverse;
         * tmp2 := tmp.reverse.mark (0@0).reverse;
         *
         * We can then form each of the result BATs:
         *
         * <R_out> := tmp1.leftjoin (R_in);
         * <S_out> := tmp2.leftjoin (S_in);
         */

        PFma_op_t           *tmp, *tmp1, *tmp2;
        unsigned int         i;
        PFalg_simple_type_t  t;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        assert (attr_type ($1$, $$->sem.eqjoin.att1)
                == attr_type ($2$, $$->sem.eqjoin.att2));
        assert (is_monomorphic (attr_type ($1$, $$->sem.eqjoin.att1)));

        $$->env = new_env ();

        /* Create the mapping BAT `tmp' */
        tmp = join ([[ $1$,
                       $$->sem.eqjoin.att1,
                       attr_type ($1$, $$->sem.eqjoin.att1) ]],
                    reverse ([[ $2$,
                                $$->sem.eqjoin.att2,
                                attr_type ($2$, $$->sem.eqjoin.att2) ]] ));

        /* and the other tmp BATs */
        tmp1 = reverse (mark (tmp, lit_oid (0)));
        tmp2 = reverse (mark (reverse (tmp), lit_oid (0)));

        /* now consider all attributes from R */
        for (i = 0; i < (unsigned int) $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type)
                    [[ $$, $1$->schema.items[i].name, t ]]
                        = leftjoin (tmp1,
                                    [[ $1$, $1$->schema.items[i].name, t ]]);

        /* and all attributes from S */
        for (i = 0; i < (unsigned int) $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type)
                    [[ $$, $2$->schema.items[i].name, t ]]
                        = leftjoin (tmp2,
                                    [[ $2$, $2$->schema.items[i].name, t ]]);

    }
    ;


AlgExpr:  cross (AlgExpr, AlgExpr)
    { cost = 2; }
    =
    {
        /*
         * Generic cross product  R x S:
         *
         * - Grab monomorphic attributes r and s from R and S.
         * - Then do
         *
         *   tmp := r.cross (s.reverse);
         *   tmp1 := tmp.mark (0@0).reverse;
         *   tmp2 := tmp.reverse.mark (0@0).reverse;
         *
         * - And for each attribute a/b in R/S
         *
         *   <a_out> := tmp1.leftjoin (a_in);
         *   <b_out> := tmp2.leftjoin (b_in);
         */

        PFma_op_t    *r, *s;
        PFma_op_t    *tmp, *tmp1, *tmp2;
        unsigned int  i;
        PFalg_simple_type_t  t;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        $$->env = new_env ();

        /* search for monomorphic attribute in $1$ */
        t = 0;
        for (i = 0; !t && i < (unsigned int) $1$->schema.count; i++)
            for (t = 1; t; i <<= 1)
                if (t == $1$->schema.items[i].type)
                    break;

        if (!t)
            PFoops (OOPS_FATAL,
                    "Cross product requires monomorphic attribute "
                    "in left operand");

        r = [[ $1$, $1$->schema.items[i].name, t ]];

        /* search for monomorphic attribute in $2$ */
        t = 0;
        for (i = 0; !t && i < (unsigned int) $2$->schema.count; i++)
            for (t = 1; t; i <<= 1)
                if (t == $2$->schema.items[i].type)
                    break;

        if (!t)
            PFoops (OOPS_FATAL,
                    "Cross product requires monomorphic attribute "
                    "in right operand");

        s = [[ $2$, $2$->schema.items[i].name, t ]];

        /* We now have monomorphic attributes r and s,
         * so we're ready for the cross product. */
        tmp = cross (r, reverse (s));
        tmp1 = reverse (mark (tmp, lit_oid (0)));
        tmp2 = reverse (mark (reverse (tmp), lit_oid (0)));

        /* And we set all the result attributes, first from R... */
        for (i = 0; i < (unsigned int) $1$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $1$->schema.items[i].type)
                    [[ $$, $1$->schema.items[i].name, t ]]
                        = leftjoin (tmp1,
                                    [[ $1$, $1$->schema.items[i].name, t ]]);

        /* then from S */
        for (i = 0; i < (unsigned int) $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $2$->schema.items[i].type)
                    [[ $$, $2$->schema.items[i].name, t ]]
                        = leftjoin (tmp2,
                                    [[ $2$, $2$->schema.items[i].name, t ]]);
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
        unsigned int         i;
        PFalg_simple_type_t  t;
        PFma_op_t           *c = NULL;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        assert ($2$->env);

        /* copy environment from $2$ to $$ */
        copy_env ($$, $2$);

        /*
         * Add new entries:
         *  - pick a monomorphic column c from the existing ones.
         *  - for each of the entries add a `c.project (val)' to the env
         */

        /* search for a monomorphic column */
        for (i = 0; i < (unsigned int) $2$->schema.count; i++)
            for (t = 1; t; t <<= 1)
                if (t == $2$->schema.items[i].type)
                    c = [[ $2$,
                           $2$->schema.items[i].name,
                           $2$->schema.items[i].type ]];

        if (!c)
            PFoops (OOPS_FATAL,
                    "Need at least one monomorphic column to translate "
                    "simple cross product");

        /* add new entries to environment */
        for (i = 0; i < (unsigned int) $1$->schema.count; i++)
            [[ $$, $1$->schema.items[i].name, $1$->schema.items[i].type ]]
                = project (c, literal ($1$->sem.lit_tbl.tuples[0].atoms[i]));
    }
    ;

AlgExpr:  project (AlgExpr)
    =
    {
        /*
         * Projection is rather easy: For each of the ``new'' attributes
         * just do a
         *   <new_att> := <old_att>;
         */
        int i;
        PFalg_simple_type_t t;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        assert ($1$->env);

        /* no need to do anything if we already translated that expression */
        if ($$->env)
            break;

        $$->env = new_env();

        for (i = 0; i < $$->sem.proj.count; i++)
            for (t = 1; t; t <<= 1)
                if (t & $$->schema.items[i].type)
                    [[ $$, $$->sem.proj.items[i].new, t ]]
                        = [[ $1$, $$->sem.proj.items[i].old, t ]];
    }
    ;

AlgExpr:  rownum (AlgExpr)
    =
    {
        PFma_op_t    *tmp, *tmp2;
        unsigned int  i;

        /* skip if we already translated that expression */
        if ($$->env)
            break;

        assert ($1$->env);

        /* copy all attributes from our operand */
        copy_env ($$, $1$);

        if ($$->sem.rownum.sortby.count >= 1) {
            /*
             * sort by first sort specification:
             *
             * tmp := <sort_crit>.reverse.sort.reverse;
             */
            tmp = reverse (
                    sort (
                        reverse ([[ $1$,
                                    $$->sem.rownum.sortby.atts[0],
                                    attr_type ($1$,
                                               $$->sem.rownum.sortby.atts[0])]]
                            )));
            /*
             * refine sorting by other sort criteria:
             *
             * tmp := tmp.CTrefine (<sort_crit>);
             */
            for (i = 1; i < (unsigned int) $$->sem.rownum.sortby.count; i++)
                tmp = ctrefine (tmp,
                                [[ $1$,
                                   $$->sem.rownum.sortby.atts[i],
                                   attr_type ($1$,
                                              $$->sem.rownum.sortby.atts[0])]]);

        }
        else
            PFoops (OOPS_FATAL,
                    "rownumber without sort specifier not implemented yet");

        if ($$->sem.rownum.part) {

            /*
             * If a partitioning attribute was given:
             *  - fetch partitioning attribute into tmp's tail
             *  - set up groups for mark_grp operator
             *  - use mark_grp to get the result
             */

            /*
             * fetch partitioning attribute into tail:
             *
             * tmp := tmp.mirror.leftjoin (<part_att>);
             */
            tmp = leftjoin (mirror (tmp),
                            [[ $1$,
                               $$->sem.rownum.part,
                               attr_type ($1$, $$->sem.rownum.part) ]]);
            /*
             * define groups for mark_grp operator:
             *
             * tmp2 := <part_att>.reverse.kunique.project (1@0);
             */
            tmp2 = project (
                    kunique (
                        reverse ([[ $1$,
                                    $$->sem.rownum.part,
                                    attr_type ($1$, $$->sem.rownum.part) ]])),
                    lit_oid (1));

            /*
             * now we can apply mark_grp:
             *
             * <new_att> := tmp.mark_grp (tmp2);
             */
            [[ $$, $$->sem.rownum.attname, aat_nat ]] = mark_grp (tmp, tmp2);
        }
        else {
            /*
             * If not partitioning attribute is given, just use
             * the mark operator:
             *
             * <new_att> := tmp.mark (1@0);
             */
            [[ $$, $$->sem.rownum.attname, aat_nat ]] = mark (tmp, lit_oid (1));
        }

        /*
         * Now make new attribute aligned with all the other BATs
         * (re-sort it).
         *
         * <new_att> := <new_att>.sort.reverse.mark (0@0).reverse;
         */
        [[ $$, $$->sem.rownum.attname, aat_nat ]]
            = reverse (
                mark (
                    reverse (sort ([[ $$, $$->sem.rownum.attname, aat_nat ]])),
                    lit_oid (0)));
    }
    ;

AlgExpr:  num_add (AlgExpr);
AlgExpr:  num_subtract (AlgExpr);
AlgExpr:  num_multiply (AlgExpr);
AlgExpr:  num_divide (AlgExpr);

AlgExpr:  cast (AlgExpr);

AlgExpr:  num_gt (AlgExpr);
AlgExpr:  num_eq (AlgExpr);
AlgExpr:  not (AlgExpr);

AlgExpr:  select_ (AlgExpr);


/* vim:set shiftwidth=4 expandtab filetype=c: */
