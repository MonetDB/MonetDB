/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Physical algebra for Pathfinder.
 *
 * For documentation on the algebra compilation process, please
 * refer to the @ref compilation documentation in algebra/algebra.c.
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include pf_config.h first */
#include "pf_config.h"
#include "pathfinder.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "physical.h"
#include "mem.h"
#include "oops.h"

#include "ordering.h"
#include "properties.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* filter out dummy error columns */
#define ERR_TYPE_MASK(t)  ((t) & ~aat_error)

/** mnemonic for a sort specification list */
#define sortby(...)     PFord_order_intro (__VA_ARGS__)

#define INIT_COST 10
#define DEFAULT_COST INIT_COST
#define AGGR_COST 50
#define JOIN_COST 100
#define SORT_COST 700

/**
 * check for a column @a a in op @a p.
 */
static bool
check_col (const PFpa_op_t *p, PFalg_col_t a)
{
    assert (p);

    for (unsigned int i = 0; i < p->schema.count; i++)
        if (a == p->schema.items[i].name)
            return true;

    return false;
}

/**
 * check for the type of column @a a in op @a p.
 */
static PFalg_simple_type_t
type_of (const PFpa_op_t *p, PFalg_col_t a)
{
    assert (p);

    for (unsigned int i = 0; i < p->schema.count; i++)
        if (a == p->schema.items[i].name)
            return p->schema.items[i].type;

    assert (0);
    return 0;
}

/**
 * Create an algebra operator (leaf) node.
 *
 * Allocates memory for an algebra operator leaf node
 * and initializes all its fields. The node will have the
 * kind @a kind.
 */
static PFpa_op_t *
leaf (enum PFpa_op_kind_t kind)
{
    unsigned int i;
    PFpa_op_t *ret = PFmalloc (sizeof (PFpa_op_t));

    ret->kind = kind;

    ret->schema.count = 0;
    ret->schema.items = NULL;
    ret->node_id = 0;
    ret->orderings = PFord_set ();
    ret->cost = 0;

    ret->env = NULL;

    ret->bit_dag   = false;
    ret->bit_reset = false;
    ret->refctr        = 0;

    for (i = 0; i < PFPA_OP_MAXCHILD; i++)
        ret->child[i] = NULL;

    return ret;
}

/**
 * Create an algebra operator node with one child.
 * Similar to #leaf(), but additionally wires one child.
 */
static PFpa_op_t *
wire1 (enum PFpa_op_kind_t kind, const PFpa_op_t *n)
{
    PFpa_op_t *ret = leaf (kind);

    assert (n);

    ret->child[0] = (PFpa_op_t *) n;

    return ret;
}

/**
 * Create an algebra operator node with two children.
 * Similar to #wire1(), but additionally wires another child.
 */
static PFpa_op_t *
wire2 (enum PFpa_op_kind_t kind, const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t *ret = wire1 (kind, n1);

    assert (n2);

    ret->child[1] = (PFpa_op_t *) n2;

    return ret;
}



/**
 * Construct algebra node that will serialize the argument when executed.
 * A serialization node will be placed on the very top of the algebra
 * expression tree. Its main use is to have an explicit match for
 * the expression root.
 */
PFpa_op_t *
PFpa_serialize (const PFpa_op_t *side_effects,
                const PFpa_op_t *alg, PFalg_col_t item)
{
    PFpa_op_t *ret = wire2 (pa_serialize, side_effects, alg);

    ret->sem.serialize.item = item;

    ret->cost = side_effects->cost + alg->cost;

    return ret;
}

/**
 * Construct an algebra node representing a literal table, given
 * an column list and a list of tuples.
 *
 * @b Orderings
 *
 * We currently derive orderings only for tables that contain
 * exactly one tuple.  These tables are trivially ordered by all
 * permutations of the table's columns.  (No need to generate
 * all @em subsets, as the permutations imply their prefixes and
 * thus each possible ordering.)
 *
 * @b Costs
 *
 * Literal tables all have a cost value of 1.
 *
 *
 * @param collist Attribute list of the literal table. (Most easily
 *                constructed using #PFalg_collist_worker() or its
 *                abbreviated macro #collist().)
 * @param count  Number of tuples that follow
 * @param tuples Tuples of this literal table, as #PFalg_tuple_t.
 *               This array must be exactly @a count items long.
 */
PFpa_op_t *
PFpa_lit_tbl (PFalg_collist_t *collist,
              unsigned int count, PFalg_tuple_t *tuples)
{
    PFpa_op_t    *ret;      /* return value we are building */
    unsigned int  i;
    unsigned int  j;

    /*
     * Empty tables make trouble during MIL generation.
     * However, after optimizing the logical tree, none of them
     * should survive anyway.
     */
    if (count == 0)
        PFoops (OOPS_FATAL,
                "cannot deal with (literal) empty tables in physical plan");

    /* instantiate the new algebra operator node */
    ret = leaf (pa_lit_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (clsize (collist) * sizeof (*(ret->schema.items)));
    for (i = 0; i < clsize (collist); i++) {
        ret->schema.items[i].name = clat (collist, i);
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = clsize (collist);

    /* pick all the given tuples from the variable argument list,
     * after allocating memory for them. */
    ret->sem.lit_tbl.count = count;
    ret->sem.lit_tbl.tuples
        = PFmalloc (count * sizeof (*(ret->sem.lit_tbl.tuples)));
    for (i = 0; i < count; i++) {

        /* copy tuple */
        ret->sem.lit_tbl.tuples[i] = tuples[i];

        /* add type of this tuple to schema */
        for (j = 0; j < tuples[i].count; j++)
            ret->schema.items[j].type |= tuples[i].atoms[j].type;

    }

    /* ---- literal table orderings ---- */
    /*
     * If a literal table contains exactly one row, it is trivially
     * sorted on all permutations of its columns. (Note that this
     * includes all sublists thereof.)
     */
    if (count == 1) {

        PFord_ordering_t ord = PFordering ();

        for (unsigned int i = 0; i < clsize (collist); i++)
            ord = PFord_refine (ord, clat (collist, i), DIR_ASC);

        /* PFord_permutations ignores the input direction and
           generates all permutations also for the directions. */
        ret->orderings = PFord_permutations (ord);
    }

    /* ---- literal table costs ---- */
    ret->cost = INIT_COST;

    return ret;
}

/**
 * Empty tables may be generated for the logical algebra.  Logical
 * optimization, however, should be able to eliminate *all* these
 * (literal) empty tables.  So none of them should occur in the
 * physical plan.
 *
 * FIXME: There's only a single exception: If a query returns the
 *        (statically) empty sequence (i.e., the query `()'), we
 *        will end up with an empty relation as the top-level
 *        expression.  We could catch this case separately, though.
 *
 * Constructor for an empty table.  Use this constructor (in
 * preference over a literal table with no tuples) to trigger
 * optimization rules concerning empty relations.
 *
 * @param schema Attribute list with annotated types (schema)
 */
PFpa_op_t *
PFpa_empty_tbl (PFalg_schema_t schema)
{
    PFpa_op_t   *ret;      /* return value we are building */

    /* instantiate the new algebra operator node */
    ret = leaf (pa_empty_tbl);

    /* set its schema */
    ret->schema.count = schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));
    /* copy schema */
    for (unsigned int i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    /* play safe: set these fields */
    ret->sem.lit_tbl.count  = 0;
    ret->sem.lit_tbl.tuples = NULL;

    PFord_ordering_t ord = PFordering ();
    for (unsigned int i = 0; i < schema.count; i++)
        ord = PFord_refine (ord, schema.items[i].name, DIR_ASC);

    /* PFord_permutations ignores the input direction and
       generates all permutations also for the directions. */
    ret->orderings = PFord_permutations (ord);

    ret->cost = INIT_COST;

    return ret;
}

/**
 * ColumnAttach: Attach a column to a table.
 *
 * In the logical algebra we express this in terms of a cross
 * product with a (one tuple) literal table.  On the physical
 * side, we can of course do this much more efficiently, e.g.,
 * with MonetDB's @c project operator.
 *
 * If you want to attach more than one column, apply ColumnAttach
 * multiple times.
 *
 * @b Orderings
 *
 * Column attachment will retain all orderings of the input
 * relation.  As the new column is a constant, we may interleave
 * the input ordering with the new column everywhere.  So with
 * the input ordering @f$ [ O_1, O_2, \dots, O_n ] @f$ and the
 * new column @f$ C @f$, we get
 * @f[
 *     { [ C, O_1, O_2, \dots, O_n ],
 *       [ O_1, C, O_2, \dots, O_n ],
 *       [ O_1, O_2, C, \dots, O_n ],
 *       \dots
 *       [ O_1, O_2, \dots, O_n, C ] }
 * @f]
 *
 * @b Costs
 *
 * Column attachment is cheap: Costs for the input relation plus 1.
 *
 * @param n       Input relation
 * @param attname Name of the new column.
 * @parma value   Value for the new column.
 */
PFpa_op_t *
PFpa_attach (const PFpa_op_t *n,
             PFalg_col_t col, PFalg_atom_t value)
{
    PFpa_op_t   *ret = wire1 (pa_attach, n);

    /* result schema is input schema plus new columns */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add new column */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = col, .type = value.type };

    ret->sem.attach.col = col;
    ret->sem.attach.value   = value;

    /* ---- ColumnAttach: orderings ---- */

    /* all the input orderings we consider */
    PFord_set_t in  = n->orderings;

    /*
     * If the input has no defined ordering, we create a new
     * set that contains one empty ordering.  Our new column
     * will then be the only result ordering
     */
    if (PFord_set_count (in) == 0)
        in = PFord_set_add (PFord_set (), PFordering ());

    /* Iterate over all the input orderings... */
    for (unsigned int i = 0; i < PFord_set_count (in); i++) {

        PFord_ordering_t current_in = PFord_set_at (in, i);
        PFord_ordering_t prefix     = PFordering ();

        /* interleave the new column everywhere possible */
        for (unsigned int j = 0; j <= PFord_count (current_in); j++) {

            PFord_ordering_t ord_asc  = PFord_refine (prefix, col, DIR_ASC);
            PFord_ordering_t ord_desc = PFord_refine (prefix, col, DIR_DESC);

            for (unsigned int k = j; k < PFord_count (current_in); k++) {
                ord_asc  = PFord_refine (
                               ord_asc,
                               PFord_order_col_at (current_in, k),
                               PFord_order_dir_at (current_in, k));
                ord_desc = PFord_refine (
                               ord_desc,
                               PFord_order_col_at (current_in, k),
                               PFord_order_dir_at (current_in, k));
            }

            PFord_set_add (ret->orderings, ord_asc);
            PFord_set_add (ret->orderings, ord_desc);

            if (j < PFord_count (current_in))
                prefix = PFord_refine (
                             prefix,
                             PFord_order_col_at (current_in, j),
                             PFord_order_dir_at (current_in, j));
        }
    }

    /* ---- ColumnAttach: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a a and @a b must not have any equally named column.
 *
 * @b Orderings
 *
 * For the input `R x S', we assume the cross product operator
 * to produce the same result as
 *
 * @verbatim
     foreach r in R
       foreach s in S
         return <r, s> .
@endverbatim
 *
 * This means that the orderings of R will be a primary ordering
 * for the result, refined by the orderings of S.
 *
 * @b Costs
 *
 * @f[
 *    cost (R \times S) = cost (R) \cdot cost (S) + cost (R) + cost (S)
 * @f]
 *
 * @bug
 *   Our current logical algebra trees do not produce cross products
 *   in the physical plan.  This code is thus not really tested.
 */
PFpa_op_t *
PFpa_cross (const PFpa_op_t *a, const PFpa_op_t *b)
{
    PFpa_op_t        *ret = wire2 (pa_cross, a, b);
    unsigned int      i;
    unsigned int      j;
    PFord_ordering_t  ord;

    assert (a); assert (b);

    /* allocate memory for the result schema */
    ret->schema.count = a->schema.count + b->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from argument 1 */
    for (i = 0; i < a->schema.count; i++)
        ret->schema.items[i] = a->schema.items[i];

    /* copy schema from argument 2, check for duplicate column names */
    for (j = 0; j < b->schema.count; j++)
        ret->schema.items[a->schema.count + j] = b->schema.items[j];

    /* ---- cross product orderings ---- */

    /* consider all orderings of a */
    for (unsigned int i = 0; i < PFord_set_count (a->orderings); i++) {

        /* refine by all the orderings in b */
        for (unsigned int j = 0; j < PFord_set_count (b->orderings); j++) {

            /* Ordering of a is the major ordering */
            ord = PFord_set_at (a->orderings, i);

            /* refine column by column with current ordering of b */
            for (unsigned int k = 0;
                 k < PFord_count (PFord_set_at (b->orderings, j));
                 k++)
                ord = PFord_refine (
                        ord,
                        PFord_order_col_at ( PFord_set_at (b->orderings, j), k),
                        PFord_order_dir_at ( PFord_set_at (b->orderings, j), k));

            /* add it to the orderings of the result */
            PFord_set_add (ret->orderings, ord);
        }
    }

    /* ---- cross product costs ---- */
    ret->cost = 4 * JOIN_COST + a->cost + b->cost;

    return ret;
}

/**
 * Cross product (Cartesian product) of two relations
 * where the second argument is only evaluated if the first argument
 * produces at least one tuple.
 */
PFpa_op_t *
PFpa_dep_cross (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    /* the operator behaves like a cross product */
    PFpa_op_t *ret = PFpa_cross (n1, n2);
    ret->kind = pa_dep_cross;
    ret->cost -= 2 * JOIN_COST;

    return ret;
}

/**
 * Border for dependent operator describing which operators
 * lie in-/outside.
 */
PFpa_op_t *
PFpa_dep_border (const PFpa_op_t *n)
{
    /* the operator behaves like a recursion border */
    PFpa_op_t *ret = PFpa_rec_border (n);
    ret->kind = pa_dep_border;

    return ret;
}

/**
 * LeftJoin: Equi-Join of two relations. Preserves the ordering
 *           of the left operand.
 */
PFpa_op_t *
PFpa_leftjoin (PFalg_col_t col1, PFalg_col_t col2,
               const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_leftjoin, n1, n2);

    /* see if we can find column col1 in n1 */
    if (check_col (n1, col1) && check_col (n2, col2)) {
        ret->sem.eqjoin.col1 = col1;
        ret->sem.eqjoin.col2 = col2;
    }
    else if (check_col (n2, col1)
             && check_col (n1, col2)) {
        ret->sem.eqjoin.col2 = col1;
        ret->sem.eqjoin.col1 = col2;
    }
    else
        PFoops (OOPS_FATAL, "problem with columns in LeftJoin");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n1 */
    for (unsigned int i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from n2 */
    for (unsigned int i = 0; i < n2->schema.count; i++)
        ret->schema.items[n1->schema.count + i] = n2->schema.items[i];

    /* ---- LeftJoin: orderings ---- */

    /*
     * We preserve any ordering of the left operand.
     *
     * We may interleave this ordering with any column marked
     * constant in the right operand.
     *
     * If the (left) join column is among the orderings in the
     * left relation, we may add the corresponding right join
     * column anywhere after the left join column.
     */

    ret->orderings = PFord_set ();

    for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++) {

        PFord_ordering_t left_ordering = PFord_set_at (n1->orderings, i);

        PFord_ordering_t ord = PFordering ();

        /*
         * Walk over left ordering, and keep an eye for the left join
         * column.
         */
        bool found = false;
        bool dir = DIR_ASC;

        for (unsigned int j = 0; j < PFord_count (left_ordering); j++) {

            if (PFord_order_col_at (left_ordering, j) == ret->sem.eqjoin.col1) {
                /* Hey, we found the left join column. */
                found = true;
                /* remember the direction */
                dir = PFord_order_dir_at (left_ordering, j);
            }

            if (found) {

                /*
                 * We already found the left join column. So we may
                 * include the right column into the result ordering.
                 * (We may include the right column anywhere after
                 * or immediately before the left column.)
                 */

                /* Insert the left join column */
                PFord_ordering_t ord2
                    = PFord_refine (ord, ret->sem.eqjoin.col2, dir);

                /* Fill up with the remaining ordering of left relation */
                for (unsigned int k = j; k < PFord_count (left_ordering); k++)
                    ord2 = PFord_refine (ord2,
                                         PFord_order_col_at (left_ordering, j),
                                         PFord_order_dir_at (left_ordering, j));

                /* and append to result */
                PFord_set_add (ret->orderings, ord2);
            }

            ord = PFord_refine (
                      ord,
                      PFord_order_col_at (left_ordering, j),
                      PFord_order_dir_at (left_ordering, j));
        }

        if (found)
            ord = PFord_refine (ord, ret->sem.eqjoin.col2, dir);

        PFord_set_add (ret->orderings, ord);

    }

    /* Kick out those many duplicates we collected */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- LeftJoin: costs ---- */
    ret->cost = 2 * JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * EqJoin: Equi-Join of two relations.
 */
PFpa_op_t *
PFpa_eqjoin (PFalg_col_t col1, PFalg_col_t col2,
             const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_eqjoin, n1, n2);

    /* see if we can find column col1 in n1 */
    if (check_col (n1, col1) && check_col (n2, col2)) {
        ret->sem.eqjoin.col1 = col1;
        ret->sem.eqjoin.col2 = col2;
    }
    else if (check_col (n2, col1)
             && check_col (n1, col2)) {
        ret->sem.eqjoin.col2 = col1;
        ret->sem.eqjoin.col1 = col2;
    }
    else
        PFoops (OOPS_FATAL, "problem with columns in EqJoin");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n1 */
    for (unsigned int i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from n2 */
    for (unsigned int i = 0; i < n2->schema.count; i++)
        ret->schema.items[n1->schema.count + i] = n2->schema.items[i];

    /* ---- EqJoin: orderings ---- */
    ret->orderings = PFord_set ();

    /* ---- EqJoin: costs ---- */
    ret->cost = 2.5 * JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * SemiJoin: Semi-Join of two relations. Preserves the ordering
 *           of the left operand.
 */
PFpa_op_t *
PFpa_semijoin (PFalg_col_t col1, PFalg_col_t col2,
               const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_semijoin, n1, n2);

    /* see if we can find column col1 in n1 */
    if (check_col (n1, col1) && check_col (n2, col2)) {
        ret->sem.eqjoin.col1 = col1;
        ret->sem.eqjoin.col2 = col2;
    }
    else
        PFoops (OOPS_FATAL, "problem with columns in SemiJoin");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n1 */
    for (unsigned int i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* ---- SemiJoin: orderings ---- */
    ret->orderings = PFord_set ();

    /* ---- SemiJoin: costs ---- */

    /* We have no alternative implmementation for this operator
       and it should be cheaper than a join plus a distinct operator
       --> dummy cost: + 10 */;
    ret->cost = JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * ThetaJoin: Theta-Join. Does not provide any ordering guarantees.
 */
PFpa_op_t *
PFpa_thetajoin (const PFpa_op_t *n1, const PFpa_op_t *n2,
                unsigned int count, PFalg_sel_t *pred)
{
    unsigned int i;
    PFpa_op_t  *ret;

    /* verify that the join columns are all available */
    for (i = 0; i < count; i++)
        if (!check_col (n1, pred[i].left) ||
            !check_col (n2, pred[i].right))
            break;

    /* did we find all columns? */
    if (i < count)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in theta-join not found",
                check_col (n2, pred[i].right)
                ? PFcol_str(pred[i].left) : PFcol_str(pred[i].right));

    ret = wire2 (pa_thetajoin, n1, n2);

    ret->sem.thetajoin.count = count;
    ret->sem.thetajoin.pred  = PFmalloc (count * sizeof (PFalg_sel_t));
    for (i = 0; i < count; i++)
        ret->sem.thetajoin.pred[i] = pred[i];

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count + n2->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n1 */
    for (unsigned int i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* copy schema from n2 */
    for (unsigned int i = 0; i < n2->schema.count; i++)
        ret->schema.items[n1->schema.count + i] = n2->schema.items[i];

    /* ---- ThetaJoin: orderings ---- */
    ret->orderings = PFord_set ();

    /* ---- ThetaJoin: costs ---- */

    /* make it a little bit more expensive than the normal equi-join
       and more expensive than the specialized variant that require
       two sorted columns up front. */
    ret->cost = 4 * JOIN_COST + 2 * SORT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * ThetaJoin: Theta-Join. Returns two columns (from n1 and n2)
 *            with duplicates eliminated. Preserves the order
 *            of the left argument.
 */
PFpa_op_t *
PFpa_unq2_thetajoin (PFalg_comp_t comp, PFalg_col_t left, PFalg_col_t right,
                     PFalg_col_t ldist, PFalg_col_t rdist,
                     const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_unq2_thetajoin, n1, n2);

    assert (check_col (n1, left) &&
            check_col (n2, right) &&
            check_col (n1, ldist) &&
            check_col (n2, rdist));

    ret->sem.unq_thetajoin.comp  = comp;
    ret->sem.unq_thetajoin.left  = left;
    ret->sem.unq_thetajoin.right = right;
    ret->sem.unq_thetajoin.ldist = ldist;
    ret->sem.unq_thetajoin.rdist = rdist;

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items = PFmalloc (2 * sizeof (*(ret->schema.items)));
    ret->schema.items[0].name = ldist;
    ret->schema.items[0].type = PFprop_type_of_ (n1->schema, ldist);
    ret->schema.items[1].name = rdist;
    ret->schema.items[1].type = PFprop_type_of_ (n2->schema, rdist);

    /* ---- ThetaJoin: orderings ---- */

    /* The result is ordered by first the left distinct column
       and then by the right distinct column. */

    ret->orderings = PFord_set ();
    PFord_set_add (ret->orderings,
                   PFord_refine (PFordering (),
                                 ldist,
                                 DIR_ASC));
    PFord_set_add (ret->orderings,
                   PFord_refine (PFord_refine (PFordering (),
                                               ldist,
                                               DIR_ASC),
                                 rdist,
                                 DIR_ASC));

    /* ---- ThetaJoin: costs ---- */
    ret->cost = 3 * JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * ThetaJoin: Theta-Join. Returns a single column with duplicates
 *            eliminated. Preserves the order of the left argument.
 */
PFpa_op_t *
PFpa_unq1_thetajoin (PFalg_comp_t comp, PFalg_col_t left, PFalg_col_t right,
                     PFalg_col_t ldist, PFalg_col_t rdist,
                     const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_unq1_thetajoin, n1, n2);

    assert (check_col (n1, left) &&
            check_col (n2, right) &&
            check_col (n1, ldist) &&
            check_col (n2, rdist));

    ret->sem.unq_thetajoin.comp  = comp;
    ret->sem.unq_thetajoin.left  = left;
    ret->sem.unq_thetajoin.right = right;
    ret->sem.unq_thetajoin.ldist = ldist;
    ret->sem.unq_thetajoin.rdist = rdist;

    /* allocate memory for the result schema */
    ret->schema.count = 1;
    ret->schema.items = PFmalloc (sizeof (*(ret->schema.items)));
    ret->schema.items[0].name = ldist;
    ret->schema.items[0].type = aat_nat;

    /* ---- ThetaJoin: orderings ---- */

    /* The result is ordered by the left distinct column */

    ret->orderings = PFord_set ();
    PFord_set_add (ret->orderings,
                   PFord_refine (PFordering (),
                                 ldist,
                                 DIR_ASC));

    /* ---- ThetaJoin: costs ---- */
    ret->cost = 3 * JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Project: Column selection and renaming.
 *
 * Note that projection does @b not eliminate duplicates. If you
 * need duplicate elimination, explictly use PFpa_distinct().
 *
 * @b Orderings
 *
 * Projection does not modify the order of incoming tuples, but
 * only throws away (or duplicates) columns.  Based on all the
 * input orderings, we pick all those prefixes that are still
 * present in the projection result (of course, we rename the
 * column accordingly).
 *
 * @b Costs
 *
 * Projection is a no-cost operator.  It just involves some
 * shuffling in the compiler, but does not require any work
 * in the resulting MIL code.  We thus simply use the costs of
 * the input relation.
 *
 * Still, it might make sense to apply costs to this operator,
 * as it may remove unneccessary column attachments that (a)
 * make our plans large and unreadable, and (b) may impact other
 * operator's work if they have to deal with lots of columns.
 */
PFpa_op_t *
PFpa_project (const PFpa_op_t *n, unsigned int count, PFalg_proj_t *proj)
{
    PFpa_op_t *ret = wire1 (pa_project, n);

    ret->sem.proj.count = count;

    ret->sem.proj.items
        = PFmalloc (ret->sem.proj.count * sizeof (*ret->sem.proj.items));

    for (unsigned int i = 0 ; i < ret->sem.proj.count; i++)
        ret->sem.proj.items[i] = proj[i];

    /* allocate space for result schema */
    ret->schema.count = count;
    ret->schema.items = PFmalloc (count * sizeof (*(ret->schema.items)));

    /* check for sanity and set result schema */
    for (unsigned int i = 0; i < ret->sem.proj.count; i++) {

        unsigned int j;

        /* lookup old name in n's schema
         * and use its type for the result schema */
        for (j = 0; j < n->schema.count; j++)
            if (ret->sem.proj.items[i].old == n->schema.items[j].name) {
                /* set name and type for this column in the result schema */
                ret->schema.items[i].name = ret->sem.proj.items[i].new;
                ret->schema.items[i].type = n->schema.items[j].type;

                break;
            }

        /* did we find the column? */
        if (j >= n->schema.count)
             PFoops (OOPS_FATAL,
                    "column `%s' referenced in projection not found",
                    PFcol_str (ret->sem.proj.items[i].old));
    }

    /* ---- Project: orderings ---- */

    /*
     * From our argument pass all those ordering prefixes that
     * are still in the schema
     */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++) {

        PFord_ordering_t ni     = PFord_set_at (n->orderings, i);
        PFord_ordering_t prefix = PFordering ();

        for (unsigned int j = 0; j < PFord_count (ni); j++) {

            unsigned int k;
            for (k = 0; k < ret->sem.proj.count; k++)
                if (ret->sem.proj.items[k].old == PFord_order_col_at (ni, j))
                    break;

            if (k < ret->sem.proj.count)
                prefix = PFord_refine (
                             prefix,
                             ret->sem.proj.items[k].new,
                             PFord_order_dir_at (ni, j));
            else
                break;
        }

        if (PFord_count (prefix) > 0)
            PFord_set_add (ret->orderings, prefix);

        /* in case an ordering starts with a key column we can do more */
        if (PFprop_key (n->prop, PFord_order_col_at (ni, 0))) {
            PFalg_col_t  order_col = PFord_order_col_at (ni, 0);
            unsigned int k;
            for (k = 0; k < count; k++)
                if (proj[k].old == order_col)
                    break;
            /* we have found an ordered key column that stays visible */
            if (k < count) {
                prefix = sortby (proj[k].new);
                /* ... so we attach the rest of the projection list
                   to the order criterion */
                for (k = 0; k < count; k++)
                    if (proj[k].old != order_col)
                        prefix = PFord_refine (prefix, proj[k].new, DIR_ASC);
                PFord_set_add (ret->orderings, prefix);
            }
        }
    }

    /* prune away duplicate orderings */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- Project: costs ---- */

    /*
     * Projection is for free, as it does not affect MIL programs.
     * Any work involved in projections is done at compile time in
     * mil/milgen.brg.
     */
    ret->cost = n->cost;

    return ret;
}

PFpa_op_t *
PFpa_slice (const PFpa_op_t *n, unsigned int low, unsigned int high)
{
    PFpa_op_t *ret = wire1 (pa_slice, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* keep the ordering we got in the `ord' argument */
    ret->sem.slice.low  = low;
    ret->sem.slice.high = high;

    /* ---- Slice: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Slice: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_select (const PFpa_op_t *n, PFalg_col_t col)
{
    PFpa_op_t *ret = wire1 (pa_select, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* keep the ordering we got in the `ord' argument */
    ret->sem.select.col = col;

    /* ---- Select: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Select: costs ---- */
    /* make it more expensive than the value select */
    ret->cost = 2 * DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_value_select (const PFpa_op_t *n, PFalg_col_t col, PFalg_atom_t value)
{
    PFpa_op_t *ret = wire1 (pa_val_select, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* keep the ordering we got in the `ord' argument */
    ret->sem.attach.col = col;
    ret->sem.attach.value   = value;

    /* ---- Select: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Select: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * AppendUnion: This variant of the Union operator is always
 * applicable, regardless of the input order.
 */
PFpa_op_t *
PFpa_append_union (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t    *ret = wire2 (pa_append_union, n1, n2);
    unsigned int  i, j;

    /* see if both operands have same number of columns */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of UNION do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each column of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two columns match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = n1->schema.items[i].name,
                          .type = ERR_TYPE_MASK (
                                      n1->schema.items[i].type
                                    | n2->schema.items[j].type) };
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of UNION do not match");
    }

    /* ---- AppendUnion: orderings ---- */

    /*
     *      e1.0 -> a(u)   e2.0 -> a(v)   u < v
     *  -------------------------------------------
     *                e1 U e2 : [a]
     */
    for (unsigned int a1 = 0; a1 < PFprop_const_count (n1->prop); a1++) {

        for (unsigned int a2 = 0; a2 < PFprop_const_count (n2->prop); a2++)
            if (PFprop_const_at (n1->prop, a1) == PFprop_const_at (n2->prop, a2)
                && PFalg_atom_comparable (PFprop_const_val_at (n1->prop, a1),
                                          PFprop_const_val_at (n2->prop, a2))) {
                bool dir;
                if (PFalg_atom_cmp (PFprop_const_val_at (n1->prop, a1),
                                    PFprop_const_val_at (n2->prop, a2)) < 0)
                    dir = DIR_ASC;
                else
                    dir = DIR_DESC;

                PFord_set_add (
                        ret->orderings,
                        PFord_refine (PFordering (),
                                      PFprop_const_at (n1->prop, a1),
                                      dir));
            }
    }

    /* prune away duplicate orderings */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- AppendUnion: costs ---- */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * MergeUnion: Merge two relations and keep some ordering.
 *
 * This operator is particularly handy for XQuery evaluation.
 * It almost directly implements XQuery's sequence construction.
 * Both input relations must be ordered according to @a ord; the
 * operator's result will have that ordering.
 *
 * @b Orderings
 *
 * Both input relations @em must follow the ordering given by
 * the @a ord argument.  The output will have (just) that
 * ordering.
 *
 * @b Costs
 *
 * The result will contain the costs for both input relations
 * plus 1.
 *
 * @bug
 *   It is quite tricky to implement a MergeUnion on MonetDB
 *   if the ordering (@a ord) is more than just a single
 *   monomorphic column.  We thus reject other orderings here.
 *
 *
 * @param n1  Left input relation.  Must be ordered by @a ord.
 * @param n2  Right input relation.  Must be ordered by @a ord.
 * @param ord This ordering describes the grouping criterion.
 */
PFpa_op_t *
PFpa_merge_union (const PFpa_op_t *n1, const PFpa_op_t *n2,
                  PFord_ordering_t ord)
{
    PFpa_op_t    *ret = wire2 (pa_merge_union, n1, n2);
    unsigned int  i, j;

    /* see if both operands have same number of columns */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of UNION do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each column of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two columns match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = n1->schema.items[i].name,
                          .type = ERR_TYPE_MASK (
                                      n1->schema.items[i].type
                                    | n2->schema.items[j].type) };
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of UNION do not match");
    }

    if (PFord_count (ord) != 1)
        PFoops (OOPS_FATAL,
                "MergeUnion for more complex orderings is not implemented "
                "(got %s)", PFord_str (ord));

    ret->sem.merge_union.ord = ord;

    /* ---- MergeUnion: orderings ---- */
#ifndef NDEBUG
    {
        bool ordered = false;
        for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++)
            if (PFord_implies (PFord_set_at (n1->orderings, i), ord)) {
                ordered = true;
                break;
            }
        if (!ordered)
            PFoops (OOPS_FATAL,
                    "input for MergeUnion does not have req. ordering ([%s])",
                    PFord_str (ord));

        ordered = false;
        for (unsigned int i = 0; i < PFord_set_count (n2->orderings); i++)
            if (PFord_implies (PFord_set_at (n2->orderings, i), ord)) {
                ordered = true;
                break;
            }
        if (!ordered)
            PFoops (OOPS_FATAL,
                    "input for MergeUnion does not have req. ordering ([%s])",
                    PFord_str (ord));
    }
#endif

    PFord_set_add (ret->orderings, ord);

    /*
     *           e1.0 -> a(u)   e2.0 -> a(v)   u < v
     *   e1:[o_1,..,oi,a,ok,..on]   e2:[o_1,..,oi,a,ok,..on]
     *  -----------------------------------------------------
     *     e1 U_{[o1,...,oi]} e2 : [o_1,..,oi,a,ok,..on]
     */
    for (unsigned int a1 = 0; a1 < PFprop_const_count (n1->prop); a1++) {

        for (unsigned int a2 = 0; a2 < PFprop_const_count (n2->prop); a2++)
            if (PFprop_const_at (n1->prop, a1) == PFprop_const_at (n2->prop, a2)
                && PFalg_atom_comparable (PFprop_const_val_at (n1->prop, a1),
                                          PFprop_const_val_at (n2->prop, a2))) {
                bool dir;
                if (PFalg_atom_cmp (PFprop_const_val_at (n1->prop, a1),
                                    PFprop_const_val_at (n2->prop, a2)) < 0)
                    dir = DIR_ASC;
                else
                    dir = DIR_DESC;

                PFord_ordering_t o
                    = PFord_refine (ord, PFprop_const_at (n1->prop, a1), dir);

                for (unsigned int o1= 0; o1 < PFord_count (n1->orderings); o1++)
                    if (PFord_implies (PFord_set_at (n1->orderings, o1), o))
                        for (unsigned int o2 = 0;
                                o2 < PFord_count (n2->orderings); o2++)
                            if (PFord_implies (PFord_set_at (n2->orderings, o2),
                                               o)) {
                                if (PFord_implies (
                                            PFord_set_at (n1->orderings, o1),
                                            PFord_set_at (n2->orderings, o2)))
                                    PFord_set_add (ret->orderings,
                                                   PFord_set_at (n2->orderings,
                                                                 o2));
                                else
                                    if (PFord_implies (
                                              PFord_set_at (n2->orderings, o2),
                                              PFord_set_at (n1->orderings, o1)))
                                        PFord_set_add (ret->orderings,
                                                PFord_set_at (n1->orderings,
                                                              o1));
                            }
            }
    }

    /* prune away duplicate orderings */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- MergeUnion: costs ---- */
    ret->cost = 2 * DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Intersect: No specialized implementation here; always applicable.
 *
 * @todo
 *   Ordering not implemented, yet.
 */
PFpa_op_t *
PFpa_intersect (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t    *ret = wire2 (pa_intersect, n1, n2);
    unsigned int  i, j;

    /* see if both operands have same number of columns */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of Intersect do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each column of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two columns match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                             .type = n1->schema.items[i].type
                                                 & n2->schema.items[j].type };
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of Intersect do not match");
    }

    /* ---- Intersect: costs ---- */
    /* FIXME: Is this a reasonable cost estimate for Intersect? */
    ret->cost = JOIN_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Difference: No specialized implementation here; always applicable.
 */
PFpa_op_t *
PFpa_difference (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t    *ret = wire2 (pa_difference, n1, n2);
    unsigned int  i, j;

    /* see if both operands have same number of columns */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of Difference do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each column of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two columns match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                             .type = n1->schema.items[i].type };
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of Difference do not match");
    }

    /* ---- Difference: costs ---- */
    /* FIXME: What is a sensible cost value for Difference? */
    ret->cost = JOIN_COST + n1->cost + n2->cost;

    return ret;
}

PFpa_op_t *
PFpa_sort_distinct (const PFpa_op_t *n, PFord_ordering_t ord)
{
    PFpa_op_t *ret = wire1 (pa_sort_distinct, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* keep the ordering we got in the `ord' argument */
    ret->sem.sort_distinct.ord = ord;

    /* ---- SortDistinct: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- SortDistinct: costs ---- */
    ret->cost = JOIN_COST + n->cost;

    return ret;
}

/**
 * StandardSort: Introduce given sort order as the only new order.
 *
 * Does neither benefit from any existing sort order, nor preserve
 * any such order.  Is thus always applicable.  A possible implementation
 * could be QuickSort.
 */
PFpa_op_t *
PFpa_std_sort (const PFpa_op_t *n, PFord_ordering_t required)
{
    PFpa_op_t *ret = wire1 (pa_std_sort, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->sem.sortby.required = required;
    ret->sem.sortby.existing = PFordering ();

    /* ---- StandardSort: ordering ---- */
    PFord_set_add (ret->orderings, required);

    /* ---- StandardSort: costs ---- */
    ret->cost = 3 * SORT_COST /* (1) */
              + PFord_count (required) * SORT_COST
              + n->cost;
    /* (1): make sorting more expensive as all path steps.
            This ensures that path steps sort internally
            which is probably more efficient. */

    for (unsigned int i = 0; i < PFord_count (required); i++)
        if (PFord_order_dir_at (required, i) == DIR_DESC)
            ret->cost += SORT_COST;

    return ret;
}

/**
 * RefineSort: Introduce new ordering, but benefit from existing
 * order.
 */
PFpa_op_t *
PFpa_refine_sort (const PFpa_op_t *n,
                  PFord_ordering_t existing, PFord_ordering_t required)
{
    PFpa_op_t *ret = wire1 (pa_refine_sort, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    assert (PFord_implies (required, existing));

    ret->sem.sortby.required = required;
    ret->sem.sortby.existing = existing;

    /* ---- StandardSort: ordering ---- */
    PFord_set_add (ret->orderings, required);

    /* ---- StandardSort: costs ---- */
    ret->cost = 3 * SORT_COST /* (1) */
              + PFord_count (required) * SORT_COST
              - PFord_count (existing) * SORT_COST
              + n->cost;
    /* (1): make sorting more expensive as all path steps.
            This ensures that path steps sort internally
            which is probably more efficient. */

    for (unsigned int i = 0; i < PFord_count (required); i++)
        if (PFord_order_dir_at (required, i) == DIR_DESC)
            ret->cost += SORT_COST;

    return ret;
}

/** Constructor for generic operator that extends the schema
    with a new column where each value is determined by the values
    of a single row (cardinality stays the same) */
PFpa_op_t *
PFpa_fun_1to1 (const PFpa_op_t *n,
               PFalg_fun_t kind,
               PFalg_col_t res,
               PFalg_collist_t *refs)
{
    PFpa_op_t          *ret;
    unsigned int        i, j, ix[clsize (refs)];
    PFalg_simple_type_t res_type = 0;

    assert (n);

    /* verify that the referenced columns in refs
       are really columns of n ... */
    for (i = 0; i < clsize (refs); i++) {
        for (j = 0; j < n->schema.count; j++)
            if (n->schema.items[j].name == clat (refs, i))
                break;
        if (j == n->schema.count)
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in generic function"
                    " operator not found", PFcol_str (clat (refs, i)));
        ix[i] = j;
    }

    /* we want to perform some more consistency checks
       that are specific to certain operators */
    switch (kind) {
        case alg_fun_num_add:
        case alg_fun_num_subtract:
        case alg_fun_num_multiply:
        case alg_fun_num_divide:
        case alg_fun_num_modulo:
            assert (clsize (refs) == 2);
            /* make sure both columns are of the same numeric type */
            assert (n->schema.items[ix[0]].type == aat_nat ||
                    n->schema.items[ix[0]].type == aat_int ||
                    n->schema.items[ix[0]].type == aat_dec ||
                    n->schema.items[ix[0]].type == aat_dbl);
            assert (n->schema.items[ix[0]].type ==
                    n->schema.items[ix[1]].type);

            res_type = n->schema.items[ix[1]].type;
            break;

        case alg_fun_fn_abs:
        case alg_fun_fn_ceiling:
        case alg_fun_fn_floor:
        case alg_fun_pf_log:
        case alg_fun_pf_sqrt:
        case alg_fun_fn_round:
            assert (clsize (refs) == 1);

            /* make sure the column is of numeric type */
            assert (n->schema.items[ix[0]].type == aat_int ||
                    n->schema.items[ix[0]].type == aat_dec ||
                    n->schema.items[ix[0]].type == aat_dbl);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_fn_substring:
            assert (clsize (refs) == 2);

            /* make sure both columns are of type str & dbl */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_dbl);

            res_type = aat_str;
            break;

        case alg_fun_fn_substring_dbl:
            assert (clsize (refs) == 3);
            /* make sure columns are of type str & dbl */
            assert (n->schema.items[ix[0]].type == aat_str);
            assert (n->schema.items[ix[1]].type == aat_dbl &&
                    n->schema.items[ix[2]].type == aat_dbl);

            res_type = aat_str;
            break;

        case alg_fun_fn_concat:
        case alg_fun_fn_substring_before:
        case alg_fun_fn_substring_after:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_string_length:
            assert (clsize (refs) == 1);
            /* make sure the column is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_int;
            break;

        case alg_fun_fn_normalize_space:
        case alg_fun_fn_upper_case:
        case alg_fun_fn_lower_case:
            assert (clsize (refs) == 1);
            /* make sure the column is of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_contains:
        case alg_fun_fn_starts_with:
        case alg_fun_fn_ends_with:
        case alg_fun_fn_matches:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_bln;
            break;
#ifdef HAVE_GEOXML
        case alg_fun_geo_wkb:
            assert (clsize(refs) == 1);
            assert (n->schema.items[ix[0]].type == aat_str );
            res_type = aat_wkb;
            break;
        case alg_fun_geo_point:
            assert (clsize (refs) == 2);
            /* make sure both attributes are of type string */
	    /*
             * assert (n->schema.items[ix[0]].type == aat_str &&
             *       n->schema.items[ix[1]].type == aat_str);
	     */
            res_type = aat_wkb;
	    break;
        case alg_fun_geo_distance:
            assert (clsize (refs) == 2);
            /* make sure both attributes are of type string */
	    /*
             * assert (n->schema.items[ix[0]].type == aat_str &&
             *       n->schema.items[ix[1]].type == aat_str);
	     */
            res_type = aat_dbl;
	    break;
        case alg_fun_geo_geometry:
            assert (clsize (refs) == 1);
            assert (n->schema.items[ix[0]].type & aat_node );
            res_type = aat_wkb;
	    break;
        case alg_fun_geo_relate:
            assert (clsize(refs) == 3);
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_wkb &&
		    n->schema.items[ix[2]].type == aat_wkb);
            res_type = aat_bln;
            break;
        case alg_fun_geo_intersection:
            assert (clsize(refs) == 2);
            assert (n->schema.items[ix[0]].type == aat_wkb &&
	            n->schema.items[ix[1]].type == aat_wkb );
            res_type = aat_wkb;
            break;
#endif

        case alg_fun_fn_matches_flag:
            assert (clsize (refs) == 3);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_fn_translate:
        case alg_fun_fn_replace:
            assert (clsize (refs) == 3);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_replace_flag:
            assert (clsize (refs) == 4);
            /* make sure all columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str &&
                    n->schema.items[ix[2]].type == aat_str &&
                    n->schema.items[ix[3]].type == aat_str);

            res_type = aat_str;
            break;

        case alg_fun_fn_name:
        case alg_fun_fn_local_name:
        case alg_fun_fn_namespace_uri:
            assert (clsize (refs) == 1);
            /* make sure column is of type node */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_str;
            break;

        case alg_fun_fn_number:
        case alg_fun_fn_number_lax:
            assert (clsize (refs) == 1);
            res_type = aat_dbl;
            break;

        case alg_fun_fn_qname:
            assert (clsize (refs) == 2);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str &&
                    n->schema.items[ix[1]].type == aat_str);

            res_type = aat_qname;
            break;

        case alg_fun_fn_doc_available:
            assert (clsize (refs) == 1);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type == aat_str);

            res_type = aat_bln;
            break;

        case alg_fun_pf_fragment:
            assert (clsize (refs) == 1);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = aat_pnode;
            break;

        case alg_fun_pf_supernode:
            assert (clsize (refs) == 1);
            /* make sure both columns are of type string */
            assert (n->schema.items[ix[0]].type & aat_node);

            res_type = n->schema.items[ix[0]].type;
            break;

        case alg_fun_pf_add_doc_str:
            assert(clsize (refs) == 3);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types*/
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_add_doc_str_int:
            assert(clsize (refs) == 4);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);
            assert(n->schema.items[ix[1]].type == aat_str);
            assert(n->schema.items[ix[2]].type == aat_str);
            assert(n->schema.items[ix[3]].type == aat_int);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types */
            res_type = aat_docmgmt | aat_path | aat_docnm | aat_colnm;
            break;

        case alg_fun_pf_del_doc:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_str);

            /* the returning type of doc management functions
             * is aat_docmgmt bitwise OR the column types */
            res_type = aat_docmgmt | aat_docnm;
            break;

        case alg_fun_pf_nid:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type == aat_pnode);

            res_type = aat_str;
            break;

        case alg_fun_pf_docname:
            assert(clsize (refs) == 1);

            /* make sure cols are of the correct type */
            assert(n->schema.items[ix[0]].type & aat_node);

            res_type = aat_str;
            break;

        case alg_fun_upd_delete:
            assert(clsize (refs) == 1);
            /* make sure that the columns is a node */
            assert(n->schema.items[ix[0]].type & aat_node);

            /* the result type is aat_update bitwise OR the type of
               the target node shifted 4 bits to the left */
            assert((n->schema.items[ix[0]].type << 4) & aat_node1);
            res_type = aat_update | (n->schema.items[ix[0]].type << 4);
            break;

        case alg_fun_fn_year_from_datetime:
        case alg_fun_fn_month_from_datetime:
        case alg_fun_fn_day_from_datetime:
        case alg_fun_fn_hours_from_datetime:
        case alg_fun_fn_minutes_from_datetime:
            assert (clsize (refs) == 1);
            /* make sure column is of type datetime */
            assert (n->schema.items[ix[0]].type == aat_dtime);

            res_type = aat_int;
            break;

        case alg_fun_fn_seconds_from_datetime:
            assert (clsize (refs) == 1);
            /* make sure column is of type datetime */
            assert (n->schema.items[ix[0]].type == aat_dtime);

            res_type = aat_dec;
            break;

        case alg_fun_fn_year_from_date:
        case alg_fun_fn_month_from_date:
        case alg_fun_fn_day_from_date:
            assert (clsize (refs) == 1);
            /* make sure column is of type date */
            assert (n->schema.items[ix[0]].type == aat_date);

            res_type = aat_int;
            break;

        case alg_fun_fn_hours_from_time:
        case alg_fun_fn_minutes_from_time:
            assert (clsize (refs) == 1);
            /* make sure column is of type time */
            assert (n->schema.items[ix[0]].type == aat_time);

            res_type = aat_int;
            break;

        case alg_fun_fn_seconds_from_time:
            assert (clsize (refs) == 1);
            /* make sure column is of type time */
            assert (n->schema.items[ix[0]].type == aat_time);

            res_type = aat_dec;
            break;

        case alg_fun_add_dur:
        case alg_fun_subtract_dur:
        case alg_fun_multiply_dur:
        case alg_fun_divide_dur:
            break;

        case alg_fun_upd_rename:
        case alg_fun_upd_insert_into_as_first:
        case alg_fun_upd_insert_into_as_last:
        case alg_fun_upd_insert_before:
        case alg_fun_upd_insert_after:
        case alg_fun_upd_replace_value_att:
        case alg_fun_upd_replace_value:
        case alg_fun_upd_replace_element:
        case alg_fun_upd_replace_node:
            assert(clsize (refs) == 2);

            /* make some assertions according to the fun signature */
            switch (kind) {
                case alg_fun_upd_rename:
                    assert(n->schema.items[ix[0]].type & aat_node);
                    assert(n->schema.items[ix[1]].type & aat_qname);
                    assert((n->schema.items[ix[0]].type << 4) & aat_node1);
                    break;
                case alg_fun_upd_insert_into_as_first:
                case alg_fun_upd_insert_into_as_last:
                case alg_fun_upd_insert_before:
                case alg_fun_upd_insert_after:
                case alg_fun_upd_replace_node:
                    assert(n->schema.items[ix[0]].type & aat_node);
                    assert(n->schema.items[ix[1]].type & aat_node);
                    assert((n->schema.items[ix[0]].type << 4) & aat_node1);
                    break;
                case alg_fun_upd_replace_value_att:
                    assert(n->schema.items[ix[0]].type & aat_anode);
                    assert(n->schema.items[ix[1]].type == aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_anode1);
                    break;
                case alg_fun_upd_replace_value:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type == aat_uA);
                    assert((n->schema.items[ix[0]].type << 4) & aat_pnode1);
                    break;
                case alg_fun_upd_replace_element:
                    assert(n->schema.items[ix[0]].type & aat_pnode);
                    assert(n->schema.items[ix[1]].type & aat_pnode);
                    assert((n->schema.items[ix[0]].type << 4) & aat_pnode1);
                    break;
                default: assert(!"should never reach here"); break;
            }

            /* the result type is aat_update bitwise OR the type of
               the target_node shifted 4 bits to the left bitwise OR
               the type of the second argument */
            res_type = aat_update | (n->schema.items[ix[0]].type << 4)
                                  |  n->schema.items[ix[1]].type;
            break;
    }

    /* create new generic function operator node */
    ret = wire1 (pa_fun_1to1, n);

    /* insert semantic values into the result */
    ret->sem.fun_1to1.kind = kind;
    ret->sem.fun_1to1.res  = res;
    ret->sem.fun_1to1.refs = PFalg_collist_copy (refs);

    /* allocate memory for the result schema (schema(n) + 'res') */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from 'n' argument */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* add the information on the 'res' column */
    ret->schema.items[ret->schema.count - 1].name = res;
    ret->schema.items[ret->schema.count - 1].type = res_type;

    /* ---- orderings ---- */
    /* keep all orderings */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Helper function for binary comparisons (with both arguments to be
 * table columns).
 */
static PFpa_op_t *
bin_comp (PFpa_op_kind_t op, const PFpa_op_t *n, PFalg_col_t res,
          PFalg_col_t col1, PFalg_col_t col2)
{
    PFpa_op_t           *ret = wire1 (op, n);
#ifndef NDEBUG
    PFalg_simple_type_t  t1 = 0;
    PFalg_simple_type_t  t2 = 0;
#endif

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++) {

        ret->schema.items[i] = n->schema.items[i];

#ifndef NDEBUG
        if (n->schema.items[i].name == col1)
            t1 = n->schema.items[i].type;
        if (n->schema.items[i].name == col2)
            t2 = n->schema.items[i].type;
#endif
    }

#ifndef NDEBUG
    assert (t1); assert (t2);

    if (t1 != t2)
        PFoops (OOPS_FATAL,
                "illegal types in arithmetic operation: %u vs. %u", t1, t2);
#endif

    /* finally add schema item for new column */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = aat_bln };

    /* store information about columns for arithmetics */
    ret->sem.binary.res = res;
    ret->sem.binary.col1 = col1;
    ret->sem.binary.col2 = col2;

    /* ---- NumAdd: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    /* 2 as we want NumAddConst to be cheaper */
    ret->cost = 2 * DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_eq (const PFpa_op_t *n, PFalg_col_t res,
         PFalg_col_t col1, PFalg_col_t col2)
{
    return bin_comp (pa_eq, n, res, col1, col2);
}

PFpa_op_t *
PFpa_gt (const PFpa_op_t *n, PFalg_col_t res,
         PFalg_col_t col1, PFalg_col_t col2)
{
    return bin_comp (pa_gt, n, res, col1, col2);
}

PFpa_op_t *
PFpa_and (const PFpa_op_t *n, PFalg_col_t res,
          PFalg_col_t col1, PFalg_col_t col2)
{
    return bin_comp (pa_bool_and, n, res, col1, col2);
}

PFpa_op_t *
PFpa_or (const PFpa_op_t *n, PFalg_col_t res,
         PFalg_col_t col1, PFalg_col_t col2)
{
    return bin_comp (pa_bool_or, n, res, col1, col2);
}


/**
 * Helper function for unary arithmetics (Boolean negation).
 */
static PFpa_op_t *
unary_arith (PFpa_op_kind_t op,
             const PFpa_op_t *n, PFalg_col_t res, PFalg_col_t col)
{
    PFpa_op_t           *ret = wire1 (op, n);
    PFalg_simple_type_t  t1 = 0;

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++) {

        ret->schema.items[i] = n->schema.items[i];

        if (n->schema.items[i].name == col)
            t1 = n->schema.items[i].type;
    }

    assert (t1);

    /* finally add schema item for new column */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = t1 };

    /* store information about columns for arithmetics */
    ret->sem.unary.res = res;
    ret->sem.unary.col = col;

    /* ---- UnaryArith: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_bool_not (const PFpa_op_t *n, PFalg_col_t res, PFalg_col_t col)
{
    return unary_arith (pa_bool_not, n, res, col);
}

/**
 * Constructor for op:to operator
 */
PFpa_op_t *
PFpa_to (const PFpa_op_t *n, PFalg_col_t res,
         PFalg_col_t col1, PFalg_col_t col2)
{
    PFpa_op_t *ret = wire1 (pa_to, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

#ifndef NDEBUG
    /* verify that columns 'col1' and 'col2' are columns of n */
    if (!check_col (n, col1))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in op:to not found",
                PFcol_str (col1));
    if (!check_col (n, col2))
        PFoops (OOPS_FATAL,
                "column `%s' referenced in op:to not found",
                PFcol_str (col2));
#endif

    /* finally add schema item for new column */
    ret->schema.items[n->schema.count].name = res;
    ret->schema.items[n->schema.count].type = aat_int;

    /* insert semantic value (input columns and result column)
       into the result */
    ret->sem.binary.res = res;
    ret->sem.binary.col1 = col1;
    ret->sem.binary.col2 = col2;

    /* ---- orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++) {
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

        /* if we have already an ordering we can also add the new
           generated column at the end. It won't break the ordering. */
        PFord_set_add (ret->orderings,
                       PFord_refine (
                           PFord_set_at (n->orderings, i),
                           res, DIR_ASC));

    }

    /* ---- costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Count: Count function operator with a loop relation to
 * correctly fill in missing values. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *
PFpa_count_ext (const PFpa_op_t *n1, const PFpa_op_t *n2,
                PFalg_col_t res, PFalg_col_t part, PFalg_col_t loop)
{
    PFpa_op_t *ret = wire2 (pa_count_ext, n1, n2);

    ret->sem.count.res  = res;
    ret->sem.count.part = part;
    ret->sem.count.loop = loop;

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

#ifndef NDEBUG
    assert (part);
    assert (check_col (n1, part));
    assert (check_col (n2, loop));
    assert (type_of (n1, part) == type_of (n2, loop));
    assert (part != res);
#endif

    ret->schema.items[0].name = part;
    ret->schema.items[0].type = type_of (n1, part);
    ret->schema.items[1].name = res;
    ret->schema.items[1].type = aat_int;

    /* ---- HashCount: orderings ---- */
    /* HashCount does not provide any orderings. */

    /* ---- HashCount: costs ---- */
    ret->cost = AGGR_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Aggr: Aggregation function operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *
PFpa_aggr (const PFpa_op_t *n, PFalg_col_t part,
           unsigned int count, PFalg_aggr_t *aggr)
{
    PFpa_op_t    *ret = wire1 (pa_aggr, n);
    unsigned int  i;

    /* set number of schema items in the result schema
     * (result columns plus partitioning column)
     */
    ret->schema.count = count + (part ? 1 : 0);

    ret->schema.items = PFmalloc (ret->schema.count *
                                  sizeof (*(ret->schema.items)));

    /* insert semantic value (aggregates) into the result */
    ret->sem.aggr.part  = part;
    ret->sem.aggr.count = count;
    ret->sem.aggr.aggr  = PFmalloc (count * sizeof (PFalg_aggr_t));

    for (i = 0; i < count; i++) {
        PFalg_col_t col = aggr[i].col;
        if (col) {
            if (!check_col (n, col))
                PFoops (OOPS_FATAL,
                        "column `%s' referenced in aggregate not found",
                        PFcol_str (col));
            ret->schema.items[i].type = type_of (n, col);
        }
        else {
            ret->schema.items[i].type = aat_int;
        }
        ret->sem.aggr.aggr[i] = aggr[i];
        ret->schema.items[i].name = aggr[i].res;
    }
    if (part) {
        if (!check_col (n, part))
            PFoops (OOPS_FATAL,
                    "column `%s' referenced in aggregate not found",
                    PFcol_str (part));
        ret->schema.items[count].name = part;
        ret->schema.items[count].type = type_of (n, part);
    }

    /* ---- Aggr: orderings ---- */
    /* Aggr does not provide any orderings. */

    /* ---- Aggr: costs ---- */
    ret->cost = AGGR_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_mark (const PFpa_op_t *n, PFalg_col_t new_col)
{
    PFpa_op_t       *ret       = wire1 (pa_mark, n);
    PFord_ordering_t res_order = sortby (new_col);

    ret->sem.mark.res  = new_col;
    ret->sem.mark.part = col_NULL;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = new_col, .type = aat_nat };

    /* ---- orderings ---- */
    PFord_set_add (ret->orderings, res_order);
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++) {
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

        /* if we have already an ordering we can also add the new
           generated column at the end. It won't break the ordering. */
        PFord_set_add (ret->orderings,
                       PFord_refine (
                           PFord_set_at (n->orderings, i),
                           new_col, DIR_ASC));
        /* if we have already an ordering we can also add the new
           generated column at the front. It won't break the ordering. */
        PFord_set_add (ret->orderings,
                       PFord_concat (
                           res_order,
                           PFord_set_at (n->orderings, i)));
    }


    /* ---- costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_rank (const PFpa_op_t *n, PFalg_col_t new_col, PFord_ordering_t ord)
{
    PFpa_op_t *ret = wire1 (pa_rank, n);

    ret->sem.rank.res = new_col;
    ret->sem.rank.ord = ord;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = new_col, .type = aat_nat };

    /* ---- orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++) {
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

        /* if we have already an ordering we can also add the new
           generated column at the end. It won't break the ordering. */
        PFord_set_add (ret->orderings,
                       PFord_refine (
                           PFord_set_at (n->orderings, i),
                           new_col, DIR_ASC));
    }

    PFord_set_add (ret->orderings, sortby (new_col));

    /* ---- costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_mark_grp (const PFpa_op_t *n, PFalg_col_t new_col, PFalg_col_t part)
{
    PFpa_op_t *ret      = wire1 (pa_mark_grp, n);
    bool       dir_asc  = false,
               dir_desc = false;

    assert (part);

    ret->sem.mark.res  = new_col;
    ret->sem.mark.part = part;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = new_col, .type = aat_nat };

    /* ---- MergeRowNum: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++) {
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

        /* if we have already an ordering we can also add the new
           generated column at the end. It won't break the ordering. */
        PFord_set_add (ret->orderings,
                       PFord_refine (
                           PFord_set_at (n->orderings, i),
                           new_col, DIR_ASC));

        /* get the direction of the partitioning column */
        if (part &&
            PFord_order_col_at (PFord_set_at (n->orderings, i), 0)
            == part) {
            if (PFord_order_dir_at (PFord_set_at (n->orderings, i), 0)
                == DIR_ASC)
                dir_asc = true;
            else
                dir_desc = true;
        }
    }

    /* of course, the new column is also a valid ordering */
    if (part) {
        if (dir_asc)
            PFord_set_add (ret->orderings, sortby (part, new_col));

        if (dir_desc)
            PFord_set_add (
                ret->orderings,
                PFord_refine (
                    PFord_refine (
                        PFordering (),
                        part,
                        DIR_DESC),
                    new_col,
                    DIR_ASC));
    }
    else
        PFord_set_add (ret->orderings, sortby (new_col));

    /* ---- MergeRowNum: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Constructor for type test on column values. The result is
 * stored in newly created column @a res.
 */
PFpa_op_t *
PFpa_type (const PFpa_op_t *n,
           PFalg_col_t col,
           PFalg_simple_type_t ty,
           PFalg_col_t res)
{
    unsigned int i;
    PFpa_op_t *ret = wire1 (pa_type, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n and change type of column  */
    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .type = aat_bln, .name = res };

    ret->sem.type.col = col;
    ret->sem.type.ty  = ty;
    ret->sem.type.res = res;

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + 1;

    return ret;
}

/**
 * Constructor for type test on column values. The result is
 * stored in newly created column @a res.
 */
PFpa_op_t *
PFpa_type_assert (const PFpa_op_t *n,
                  PFalg_col_t col,
                  PFalg_simple_type_t ty)
{
    PFpa_op_t *ret = wire1 (pa_type_assert, n);

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n and change type of column  */
    for (unsigned int i = 0; i < n->schema.count; i++)
    {
        if (col == n->schema.items[i].name)
        {
            ret->schema.items[i].name = col;
            ret->schema.items[i].type = ty;
        }
        else
            ret->schema.items[i] = n->schema.items[i];
    }

    ret->sem.type.col = col;
    ret->sem.type.ty  = ty;

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_cast (const PFpa_op_t *n, PFalg_col_t res,
           PFalg_col_t col, PFalg_simple_type_t ty)
{
    PFpa_op_t  *ret = wire1 (pa_cast, n);

    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    assert (col != res);

    ret->sem.cast.col = col;
    ret->sem.cast.ty  = ty;
    ret->sem.cast.res = res;

    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];
    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = res, .type = ty };

    /* ---- Cast: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Cast: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema.
 */
PFpa_op_t *
PFpa_llscjoin (const PFpa_op_t *ctx,
               PFalg_step_spec_t spec,
               const PFord_ordering_t in,
               const PFord_ordering_t out,
               PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_llscjoin, ctx);
    PFord_ordering_t iter_item
        = sortby (iter, item);
    PFord_ordering_t item_iter
        = sortby (item, iter);

#ifndef NDEBUG
    assert (check_col (ctx, iter));
    assert (check_col (ctx, item));
    assert (type_of (ctx, iter) == aat_nat);
#endif

    /* store semantic content in node */
    ret->sem.scjoin.spec = spec;
    ret->sem.scjoin.iter = iter;
    ret->sem.scjoin.item = item;

    /* The schema of the result part is iter|item */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter, .type = aat_nat };
    /* the result of an attribute axis is also of type attribute */
    if (spec.axis == alg_attr)
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = item, .type = aat_anode };
    else if (spec.axis == alg_anc_s)
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = item,
                                    .type = type_of (ctx, item) | aat_pnode };
    else if (spec.axis == alg_desc_s || spec.axis == alg_self)
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = item,
                                    .type = type_of (ctx, item) };
    else
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = item, .type = aat_pnode };

    /* input and output orderings */
    if (PFord_implies (in, iter_item))
        ret->sem.scjoin.in = iter_item;
    else if (PFord_implies (in, item_iter))
        ret->sem.scjoin.in = item_iter;
    else
        PFoops (OOPS_FATAL, "illegal input ordering: %s", PFord_str (in));

    if (PFord_implies (out, iter_item))
        ret->sem.scjoin.out = iter_item;
    else if (PFord_implies (out, item_iter))
        ret->sem.scjoin.out = item_iter;
    else
        PFoops (OOPS_FATAL, "illegal output ordering: %s", PFord_str (out));

    /* ---- LLSCJoin: orderings ---- */

    /* the specified output ordering */
    PFord_set_add (ret->orderings, ret->sem.scjoin.out);

    /* ---- LLSCJoin: costs ---- */

    if (spec.axis == alg_attr) {
        if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
            /* input has iter|item ordering */

            if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
                /* output has iter|item ordering */
                ret->cost = 0 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */
                ret->cost = 2 * SORT_COST;
            }

        }
        else {
            /* input has item|iter ordering */

            if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
                /* output has iter|item ordering */
                ret->cost = 1 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */

                /* should be cheapest */
                ret->cost = 3 * SORT_COST;
            }
        }
    }
    else {
        if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
            /* input has iter|item ordering */

            if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
                /* output has iter|item ordering */
                ret->cost = 3 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */
                ret->cost = 2 * SORT_COST;
            }

        }
        else {
            /* input has item|iter ordering */

            if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
                /* output has iter|item ordering */
                ret->cost = 1 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */

                /* should be cheapest */
                ret->cost = 0 * SORT_COST;
            }
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * Duplicate-Generating StaircaseJoin operator.
 */
PFpa_op_t *
PFpa_llscjoin_dup (const PFpa_op_t *ctx,
                   PFalg_step_spec_t spec,
                   bool res_item_order,
                   PFalg_col_t item_res,
                   PFalg_col_t item)
{
    PFpa_op_t       *ret        = wire1 (pa_llscjoin_dup, ctx);
    PFord_ordering_t item_order = sortby (item);

#ifndef NDEBUG
    assert (check_col (ctx, item));
#endif

    /* store semantic content in node */
    ret->sem.scjoin.spec = spec;
    ret->sem.scjoin.iter = item_res;
    ret->sem.scjoin.item = item;
    ret->sem.scjoin.in   = NULL;
    ret->sem.scjoin.out  = NULL;

    /* The schema of the result part is iter|item */
    ret->schema.count = ctx->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    for (unsigned int i = 0; i < ctx->schema.count; i++)
        ret->schema.items[i] = ctx->schema.items[i];

    /* the result of an attribute axis is also of type attribute */
    if (spec.axis == alg_attr)
        ret->schema.items[ctx->schema.count]
            = (PFalg_schm_item_t) { .name = item_res, .type = aat_anode };
    else if (spec.axis == alg_anc_s)
        ret->schema.items[ctx->schema.count]
            = (PFalg_schm_item_t) { .name = item_res,
                                    .type = type_of (ctx, item) | aat_pnode };
    else if (spec.axis == alg_desc_s || spec.axis == alg_self)
        ret->schema.items[ctx->schema.count]
            = (PFalg_schm_item_t) { .name = item_res,
                                    .type = type_of (ctx, item) };
    else
        ret->schema.items[ctx->schema.count]
            = (PFalg_schm_item_t) { .name = item_res, .type = aat_pnode };

    /* check input ordering */
    for (unsigned int i = 0; i < PFord_set_count (ctx->orderings); i++)
        if (PFord_implies (PFord_set_at (ctx->orderings, i), item_order)) {
            ret->sem.scjoin.in = item_order;
            break;
        }

    /* ---- LLSCJoin: orderings ---- */

    if (res_item_order) {
        PFord_ordering_t item_res_order = sortby (item_res);
        ret->sem.scjoin.out = item_res_order;
        /* result is in item_res_order order and
           item_res_order refined with all input orderings */
        PFord_set_add (ret->orderings, item_res_order);
        for (unsigned int i = 0; i < PFord_set_count (ctx->orderings); i++)
            PFord_set_add (ret->orderings, PFord_concat (
                                               item_res_order,
                                               PFord_set_at (ctx->orderings, i)));
        if (ret->sem.scjoin.in && 
            LEVEL_KNOWN(PFprop_level (ctx->prop, item)) &&
            (spec.axis == alg_chld || spec.axis == alg_attr))
            for (unsigned int i = 0; i < PFord_set_count (ctx->orderings); i++) {
                PFord_set_add (ret->orderings, PFord_set_at (ctx->orderings, i));
                PFord_set_add (ret->orderings, PFord_refine (
                                                   PFord_set_at (ctx->orderings, i),
                                                   item_res, DIR_ASC));
            }
    }
    else {
        /* ordering stays the same and
           all input orderings refined with the item columns  */
        for (unsigned int i = 0; i < PFord_set_count (ctx->orderings); i++) {
            PFord_set_add (ret->orderings, PFord_set_at (ctx->orderings, i));
            PFord_set_add (ret->orderings, PFord_refine (
                                               PFord_set_at (ctx->orderings, i),
                                               item_res, DIR_ASC));
        }
    }

    /* ---- LLSCJoin: costs ---- */

    if (spec.axis == alg_attr) {
        if (!ret->sem.scjoin.in) {
            /* input has input|item ordering */

            if (!ret->sem.scjoin.out) {
                /* output has input|item ordering */
                ret->cost = 0 * SORT_COST;
            }
            else {
                /* output has item|input ordering */
                ret->cost = 2 * SORT_COST;
            }

        }
        else {
            /* input has item|input ordering */

            if (!ret->sem.scjoin.out) {
                /* output has input|item ordering */
                ret->cost = 1 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */

                /* should be cheapest */
                ret->cost = 3 * SORT_COST;
            }
        }

        if (PFprop_key (ctx->prop, item)) 
            /* staircase-join with duplicates should
               be more less expensive if the output does
               not contain duplicates and can be reused */
            ret->cost += ctx->cost;
        else
            /* staircase-join with duplicates should
               be more expensive than normal staircase-join */
            ret->cost += ctx->cost + 5 * JOIN_COST;
    }
    else {
        if (!ret->sem.scjoin.in) {
            /* input has input|item ordering */

            if (!ret->sem.scjoin.out) {
                /* output has input|item ordering */
                ret->cost = 3 * SORT_COST;
            }
            else {
                /* output has item|input ordering */
                ret->cost = 1 * SORT_COST;
            }

        }
        else {
            /* input has item|input ordering */

            if (!ret->sem.scjoin.out) {
                /* output has input|item ordering */
                ret->cost = 2 * SORT_COST;
            }
            else {
                /* output has item|iter ordering */

                /* should be cheapest */
                ret->cost = 0 * SORT_COST;
            }
        }
        /* staircase-join with duplicates should
           be more expensive than normal staircase-join */
        ret->cost += ctx->cost + 5 * JOIN_COST;
    }

    return ret;
}

/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.
 */
PFpa_op_t *
PFpa_doc_tbl (const PFpa_op_t *n, PFalg_col_t res, PFalg_col_t col,
              PFalg_doc_tbl_kind_t kind)
{
    unsigned int i;
    PFpa_op_t   *ret;

    ret = wire1 (pa_doc_tbl, n);

    /* store columns to work on in semantical field */
    ret->sem.doc_tbl.res  = res;
    ret->sem.doc_tbl.col  = col;
    ret->sem.doc_tbl.kind = kind;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .name = res,
                                       .type = aat_pnode };

    /* ---- doc_tbl: orderings ---- */
    /*    ordering stays the same   */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* for single base tables the result is trivially sorted */
    if (PFprop_card (n->prop) == 1)
        PFord_set_add (ret->orderings, sortby (res));

    /* ---- doc_tbl: costs ---- */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Construct algebra node that will allow the access to the string
 * content of loaded documents nodes
 *
 * @a doc is the current document (live nodes) and @a alg is the overall
 * algebra expression.
 */
PFpa_op_t *
PFpa_doc_access (const PFpa_op_t *alg,
                 PFalg_col_t res, PFalg_col_t col, PFalg_doc_t doc_col)
{
    unsigned int i;
    PFpa_op_t *ret = wire1 (pa_doc_access, alg);

    /* allocate memory for the result schema */
    ret->schema.count = alg->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from alg and change type of column  */
    for (i = 0; i < alg->schema.count; i++)
        ret->schema.items[i] = alg->schema.items[i];

    ret->schema.items[i]
        = (struct PFalg_schm_item_t) { .type = aat_str, .name = res };

    ret->sem.doc_access.res = res;
    ret->sem.doc_access.col = col;
    ret->sem.doc_access.doc_col = doc_col;

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (alg->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (alg->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + alg->cost;

    return ret;
}

/**
 * Constructor for twig root operators.
 */
PFpa_op_t *
PFpa_twig (const PFpa_op_t *n, PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_twig, n);

    /* store columns to work on in semantical field */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* allocate memory for the result schema;
       it's the same schema as n's plus an additional result column  */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    ret->schema.items[0].name = iter;
    ret->schema.items[0].type = aat_nat;
    ret->schema.items[1].name = item;
    /* Check if the twig consists of only attributes ... */
    if (n->kind == pa_attribute ||
        (n->kind == pa_content &&
         type_of (n->child[1], n->sem.ii.item) == aat_anode))
        ret->schema.items[1].type = aat_anode;
    /* ... attributes and other nodes ... */
    else if (n->kind == pa_content &&
             type_of (n->child[1], n->sem.ii.item) & aat_anode)
        ret->schema.items[1].type = aat_node;
    /* ... or only other nodes (without attributes). */
    else
        ret->schema.items[1].type = aat_pnode;

    /* orderings */

    /* add all possible orderings */
    PFord_set_add (ret->orderings, sortby (iter));
    PFord_set_add (ret->orderings, sortby (item));
    PFord_set_add (ret->orderings, sortby (iter, item));
    PFord_set_add (ret->orderings, sortby (item, iter));

    /* costs */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Constructor for twig constructor sequence operators.
 *
 * @a fc is the next 'first' child operator and @a ns is the
 * next sibling in the constructor sequence operator list.
 */
PFpa_op_t *
PFpa_fcns (const PFpa_op_t *fc, const PFpa_op_t *ns)
{
    PFpa_op_t *ret = wire2 (pa_fcns, fc, ns);

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + fc->cost + ns->cost;

    return ret;
}

/**
 * Constructor for document node operators.
 *
 * @a scope is the current scope the document node is constructed in
 * and @a fcns is the content of the node.
 */
PFpa_op_t *
PFpa_docnode (const PFpa_op_t *scope, const PFpa_op_t *fcns, PFalg_col_t iter)
{
    PFpa_op_t *ret = wire2 (pa_docnode, scope, fcns);

    assert (check_col (scope, iter));

    /* store columns to work on in semantical field */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = col_NULL;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + scope->cost + fcns->cost;

    return ret;
}

/**
 * Constructor for element operators.
 *
 * @a tag constructs the elements' tag names, and @a fcns
 * is the content of the new elements.
 */
PFpa_op_t *
PFpa_element (const PFpa_op_t *tag, const PFpa_op_t *fcns,
              PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire2 (pa_element, tag, fcns);

    assert (check_col (tag, iter) && check_col (tag, item));

    /* store columns to work on in semantical field */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + tag->cost + fcns->cost;

    return ret;
}

/**
 * Constructor for attribute operators.
 *
 * @a cont stores the name-value relation of the attribute. @a iter, @a qn,
 * and @a val reference the iter, qname, and value input columns, respectively.
 */
PFpa_op_t *
PFpa_attribute (const PFpa_op_t *cont,
                PFalg_col_t iter, PFalg_col_t qn, PFalg_col_t val)
{
    PFpa_op_t *ret = wire1 (pa_attribute, cont);

    assert (check_col (cont, iter) &&
            check_col (cont, qn) &&
            check_col (cont, val));

    /* store columns to work on in semantical field */
    ret->sem.iter_item1_item2.iter  = iter;
    ret->sem.iter_item1_item2.item1 = qn;
    ret->sem.iter_item1_item2.item2 = val;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + cont->cost;

    return ret;
}

/**
 * Constructor for text content operators.
 *
 * @a cont is the relation storing the textnode content; @item points
 * to the respective column and @iter marks the scope in which the
 * nodes are constructed in.
 */
PFpa_op_t *
PFpa_textnode (const PFpa_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_textnode, cont);

    assert (check_col (cont, iter) && check_col (cont, item));

    /* store columns to work on in semantical field */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + cont->cost;

    return ret;
}

/**
 * Constructor for comment operators.
 *
 * @a cont is the relation storing the comment content; @item points
 * to the respective column and @iter marks the scope in which the
 * nodes are constructed in.
 */
PFpa_op_t *
PFpa_comment (const PFpa_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_comment, cont);

    assert (check_col (cont, iter) && check_col (cont, item));

    /* store columns to work on in semantical field */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + cont->cost;

    return ret;
}

/**
 * Constructor for processing instruction operators.
 *
 * @a cont stores the target-value relation of the processing-instruction.
 * @a iter, @a target, and @a val reference the iter, target, and value
 * input columns, respectively.
 */
PFpa_op_t *
PFpa_processi (const PFpa_op_t *cont,
               PFalg_col_t iter, PFalg_col_t target, PFalg_col_t val)
{
    PFpa_op_t *ret = wire1 (pa_processi, cont);

    assert (check_col (cont, iter) &&
            check_col (cont, target) &&
            check_col (cont, val));

    /* store columns to work on in semantical field */
    ret->sem.iter_item1_item2.iter  = iter;
    ret->sem.iter_item1_item2.item1 = target;
    ret->sem.iter_item1_item2.item2 = val;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + cont->cost;

    return ret;
}

/**
 * Constructor for constructor content operators (elem|doc).
 */
PFpa_op_t *
PFpa_content (const PFpa_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_content, cont);

    assert (check_col (cont, iter) && check_col (cont, item));

    /* store columns to work on in semantical field */
    ret->sem.ii.iter  = iter;
    ret->sem.ii.item  = item;

    /* Constructors have no schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* orderings */

    /* costs */
    ret->cost = DEFAULT_COST + cont->cost;

    return ret;
}

PFpa_op_t *
PFpa_slim_content (const PFpa_op_t *cont, PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = PFpa_content (cont, iter, item);
    ret->kind = pa_slim_content;
    return ret;
}

PFpa_op_t *
PFpa_merge_adjacent (const PFpa_op_t *n,
                     PFalg_col_t iter, PFalg_col_t pos, PFalg_col_t item)
{
    PFpa_op_t *ret = wire1 (pa_merge_adjacent, n);

    ret->sem.iter_pos_item.iter = iter;
    ret->sem.iter_pos_item.pos  = pos;
    ret->sem.iter_pos_item.item = item;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n and change type of column  */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* result is in iter|pos order */
    PFord_set_add (ret->orderings, sortby (iter, pos));
    /* costs */
    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Constructor for a runtime error
 */
PFpa_op_t *
PFpa_error (const PFpa_op_t *side_effects, const PFpa_op_t *n,  PFalg_col_t col)
{
    PFpa_op_t *ret = wire2 (pa_error, side_effects, n);

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n  */
    for (unsigned int i = 0; i < n->schema.count; i++) {
        ret->schema.items[i] = n->schema.items[i];
    }

    ret->sem.err.col = col;

    /* costs */
    ret->cost = DEFAULT_COST + side_effects->cost + n->cost;

    return ret;
}

/**
 * Constructor for the last item of a parameter list
 */
PFpa_op_t *
PFpa_nil (void)
{
    PFpa_op_t     *ret;

    /* create end of parameter list operator */
    ret = leaf (pa_nil);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* costs */
    ret->cost = 0;

    return ret;
}

/**
 * Constructor for a query cache
 */
PFpa_op_t *
PFpa_cache (const PFpa_op_t *side_effects, const PFpa_op_t *n,
            char *id, PFalg_col_t item)
{
    PFpa_op_t *ret = wire2 (pa_cache, side_effects, n);

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n  */
    for (unsigned int i = 0; i < n->schema.count; i++) {
        ret->schema.items[i] = n->schema.items[i];
    }

    ret->sem.cache.id = id;
    ret->sem.cache.item = item;

    /* costs */
    ret->cost = DEFAULT_COST + side_effects->cost + n->cost;

    return ret;
}

/**
 * Border for cache operator describing which operators
 * lie in-/outside.
 */
PFpa_op_t *
PFpa_cache_border (const PFpa_op_t *n)
{
    /* the operator behaves like a recursion border */
    PFpa_op_t *ret = PFpa_rec_border (n);
    ret->kind = pa_cache_border;

    return ret;
}

/**
 * Constructor for a debug operator 
 */
PFpa_op_t *
PFpa_trace (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t     *ret;

    /* create new trace node */
    ret = wire2 (pa_trace, n1, n2);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* costs */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Constructor for a debug item operator 
 */
PFpa_op_t *
PFpa_trace_items (const PFpa_op_t *n1,
                  const PFpa_op_t *n2,
                  PFalg_col_t iter,
                  PFalg_col_t item)
{
    PFpa_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that iter and item are columns of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (iter == n1->schema.items[i].name ||
            item == n1->schema.items[i].name)
            found++;

    /* did we find all columns? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "columns referenced in trace operator not found");

    /* create new trace node */
    ret = wire2 (pa_trace_items, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n1->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Constructor for a debug message operator
 */
PFpa_op_t *
PFpa_trace_msg (const PFpa_op_t *n1,
                const PFpa_op_t *n2,
                PFalg_col_t iter,
                PFalg_col_t item)
{
    PFpa_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that iter and item are columns of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (iter == n1->schema.items[i].name ||
            item == n1->schema.items[i].name)
            found++;

    /* did we find all columns? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "columns referenced in trace message operator not found");

    /* create new trace node */
    ret = wire2 (pa_trace_msg, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n1->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Constructor for debug relation map operator
 * (A set of the trace_map operators link a trace operator
 * to the correct scope.)
 */
PFpa_op_t *
PFpa_trace_map (const PFpa_op_t *n1,
                const PFpa_op_t *n2,
                PFalg_col_t      inner,
                PFalg_col_t      outer)
{
    PFpa_op_t     *ret;
    unsigned int   i, found = 0;

    assert (n1);
    assert (n2);

    /* verify that inner and outer are columns of n1 ... */
    for (i = 0; i < n1->schema.count; i++)
        if (inner == n1->schema.items[i].name ||
            outer == n1->schema.items[i].name)
            found++;

    /* did we find all columns? */
    if (found != 2)
        PFoops (OOPS_FATAL,
                "columns referenced in trace operator not found");

    /* create new trace map node */
    ret = wire2 (pa_trace_map, n1, n2);

    /* insert semantic values (column names) into the result */
    ret->sem.trace_map.inner    = inner;
    ret->sem.trace_map.outer    = outer;
    /* only initialize the trace id -- milgen.brg will do the rest */
    ret->sem.trace_map.trace_id = 0;

    /* allocate memory for the result schema (= schema(n1)) */
    ret->schema.count = n1->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n1->schema.count; i++)
        ret->schema.items[i] = n1->schema.items[i];

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n1->orderings, i));
    /* costs */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Constructor for a tail recursion operator
 */
PFpa_op_t *
PFpa_rec_fix (const PFpa_op_t *side_effects_and_paramList, const PFpa_op_t *res)
{
    PFpa_op_t     *ret;
    unsigned int   i;

    assert (side_effects_and_paramList);
    assert (res);

    /* create recursion operator */
    ret = wire2 (pa_rec_fix, side_effects_and_paramList, res);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = res->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < res->schema.count; i++)
        ret->schema.items[i] = res->schema.items[i];

    /* ordering stays the same as the result ordering */
    for (unsigned int i = 0; i < PFord_set_count (res->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (res->orderings, i));

    /* costs */
    ret->cost = DEFAULT_COST + res->cost + side_effects_and_paramList->cost;

    return ret;
}

/**
 * Constructor for side effects in a tail recursion operator
 */
PFpa_op_t *
PFpa_side_effects (const PFpa_op_t *side_effects, const PFpa_op_t *paramList)
{
    PFpa_op_t     *ret;

    assert (side_effects);
    assert (paramList);

    /* create side effects operator */
    ret = wire2 (pa_side_effects, side_effects, paramList);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* costs */
    ret->cost = DEFAULT_COST + side_effects->cost + paramList->cost;

    return ret;
}

/**
 * Constructor for a list item of a parameter list
 * related to recursion
 */
PFpa_op_t *
PFpa_rec_param (const PFpa_op_t *arguments, const PFpa_op_t *paramList)
{
    PFpa_op_t     *ret;

    assert (arguments);
    assert (paramList);

    /* create recursion parameter operator */
    ret = wire2 (pa_rec_param, arguments, paramList);

    assert (paramList->schema.count == 0);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* costs */
    ret->cost = DEFAULT_COST + arguments->cost + paramList->cost;

    return ret;
}

/**
 * Constructor for the arguments of a parameter (seed and recursion
 * will be the input relations for the base operator)
 */
PFpa_op_t *
PFpa_rec_arg (const PFpa_op_t *seed, const PFpa_op_t *recursion,
              const PFpa_op_t *base)
{
    PFpa_op_t     *ret;
    unsigned int   i, j;

    assert (seed);
    assert (recursion);
    assert (base);

    /* see if both operands have same number of columns */
    if (seed->schema.count != recursion->schema.count ||
        seed->schema.count != base->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of the arguments of recursion "
                "argument to not match");

    /* see if we find each column in all of the input relations */
    for (i = 0; i < seed->schema.count; i++) {
        for (j = 0; j < recursion->schema.count; j++)
            if (seed->schema.items[i].name == recursion->schema.items[j].name) {
                break;
            }

        if (j == recursion->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of the arguments of recursion "
                    "argument to not match");

        for (j = 0; j < base->schema.count; j++)
            if (seed->schema.items[i].name == base->schema.items[j].name) {
                break;
            }

        if (j == base->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of the arguments of recursion "
                    "argument to not match");
    }

    /* check if the orderings of the seed and the recursion
       fulfill the required ordering of the base */
    for (i = 0; i < PFord_set_count (base->orderings); i++) {
        if (!PFord_count (PFord_set_at (base->orderings, i)))
           continue; /* case is trivially fulfilled */

        for (j = 0; j < PFord_set_count (recursion->orderings); j++)
            if (PFord_implies (
                    PFord_set_at (recursion->orderings, j),
                    PFord_set_at (base->orderings, i)))
                break;

        if (j == PFord_set_count (recursion->orderings))
            PFoops (OOPS_FATAL,
                    "The ordering of the recursion arguments "
                    "does not imply the ordering of the recursion base");

        for (j = 0; j < PFord_set_count (seed->orderings); j++)
            if (PFord_implies (
                    PFord_set_at (seed->orderings, j),
                    PFord_set_at (base->orderings, i)))
                break;

        if (j == PFord_set_count (seed->orderings))
            PFoops (OOPS_FATAL,
                    "The ordering of the recursion seeds "
                    "does not imply the ordering of the recursion base");
    }

    /* create recursion operator */
    ret = wire2 (pa_rec_arg, seed, recursion);

    /*
     * insert semantic value (reference to the base
     * relation) into the result
     */
    ret->sem.rec_arg.base = (PFpa_op_t *) base;

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = seed->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < seed->schema.count; i++)
        ret->schema.items[i] = seed->schema.items[i];

    /* ordering stays the same as the base ordering */
    for (unsigned int i = 0; i < PFord_set_count (base->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (base->orderings, i));

    /* costs */
    ret->cost = DEFAULT_COST + seed->cost + recursion->cost;

    return ret;
}

/**
 * Constructor for the base relation in a recursion (-- a dummy
 * operator representing the seed relation as well as the argument
 * computed in the recursion).
 */
PFpa_op_t *
PFpa_rec_base (PFalg_schema_t schema, PFord_ordering_t ord)
{
    PFpa_op_t     *ret;
    unsigned int   i, j;

    /* check if the order criteria all appear in the schema */
    for (i = 0; i < PFord_count (ord); i++) {
        for (j = 0; j < schema.count; j++)
            if (PFord_order_col_at (ord, i) ==
                schema.items[j].name)
                break;

        if (i == schema.count)
            PFoops (OOPS_FATAL,
                    "sort column '%s' does not appear in the schema",
                    PFcol_str (PFord_order_col_at (ord, i)));
    }

    /* create base operator for the recursion */
    ret = leaf (pa_rec_base);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    /* add the ordering given to the constructor */
    PFord_set_add (ret->orderings, ord);

    /* costs */
    ret->cost = INIT_COST;

    return ret;
}

/**
 * Constructor for a border of the recursion body
 */
PFpa_op_t *
PFpa_rec_border (const PFpa_op_t *n)
{
    PFpa_op_t     *ret;
    unsigned int   i;

    assert (n);

    /* create recursion operator */
    ret = wire1 (pa_rec_border, n);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = n->schema.count;

    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* ordering stays the same as the input */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* costs */
    ret->cost = n->cost;

    return ret;
}

/**
 * Constructor for the function application
 */
PFpa_op_t *
PFpa_fun_call (const PFpa_op_t *loop, const PFpa_op_t *param_list,
               PFalg_schema_t schema, PFalg_fun_call_t kind,
               PFqname_t qname, void *ctx,
               PFalg_col_t iter, PFalg_occ_ind_t occ_ind)
{
    PFpa_op_t     *ret;
    unsigned int   i;

    assert (loop);
    assert (param_list);

    /* create new function application node */
    ret = wire2 (pa_fun_call, loop, param_list);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++)
        ret->schema.items[i] = schema.items[i];

    /* insert semantic value */
    ret->sem.fun_call.kind    = kind;
    ret->sem.fun_call.qname   = qname;
    ret->sem.fun_call.ctx     = ctx;
    ret->sem.fun_call.iter    = iter;
    ret->sem.fun_call.occ_ind = occ_ind;

    /* by default we don't know anything about the output ordering */

    /* XRPC functions return the result in iter|pos order */
    if (kind == alg_fun_call_xrpc)
        PFord_set_add (ret->orderings, sortby (schema.items[0].name,
                                               schema.items[1].name));

    /* Caching information is returned in iter|pos order */
    if (kind == alg_fun_call_cache) {
        /* if the loop relation is constant everything is sorted by pos */
        if (PFprop_const (loop->prop, iter))
            PFord_set_add (ret->orderings, sortby (schema.items[1].name));
        /* the result is ordered by iter|pos */
        PFord_set_add (ret->orderings, sortby (schema.items[0].name,
                                               schema.items[1].name));
    }

    /* costs */
    ret->cost = loop->cost + param_list->cost + DEFAULT_COST;

    return ret;
}

/**
 * Constructor for a list item of a parameter list
 * related to function application
 */
PFpa_op_t *
PFpa_fun_param (const PFpa_op_t *argument, const PFpa_op_t *param_list,
                PFalg_schema_t schema)
{
    PFpa_op_t     *ret;
    unsigned int   i;

    assert (argument);
    assert (param_list);

    /* create new function application parameter node */
    ret = wire2 (pa_fun_param, argument, param_list);

    /* allocate memory for the result schema (= schema(n)) */
    ret->schema.count = schema.count;

    ret->schema.items
        = PFmalloc (schema.count * sizeof (*(ret->schema.items)));

    for (i = 0; i < schema.count; i++) {
        ret->schema.items[i].name = schema.items[i].name;
        ret->schema.items[i].type = type_of (argument, schema.items[i].name);
    }

    /* ordering stays the same as the input */
    for (unsigned int i = 0; i < PFord_set_count (argument->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (argument->orderings, i));

    /* costs */
    ret->cost = argument->cost + param_list->cost;

    return ret;
}

/**
 * Construct concatenation of multiple strings
 *
 * @a n1 contains the sets of strings @a n2 stores the separator for each set
 */
PFpa_op_t *
PFpa_string_join (const PFpa_op_t *n1, const PFpa_op_t *n2,
                  PFalg_col_t iter, PFalg_col_t item)
{
    PFpa_op_t *ret = wire2 (pa_string_join, n1, n2);

    ret->sem.ii.iter = iter;
    ret->sem.ii.item = item;

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter, .type = aat_nat };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = item, .type = aat_str };

    /* result is in iter order */
    PFord_set_add (ret->orderings, sortby (iter));
    /* ... and automatically also in iter|item order */
    PFord_set_add (ret->orderings, sortby (iter, item));
    /* costs */
    ret->cost = DEFAULT_COST + n1->cost + n2->cost;

    return ret;
}

/**
 * Constructor for finding nodes based on id/idref
 */
PFpa_op_t *
PFpa_findnodes (const PFpa_op_t *n,
                PFalg_col_t iter,
                PFalg_col_t item,
                PFalg_col_t item_doc,
                PFalg_col_t item_res,
                bool id)
{
    PFpa_op_t *ret = wire1 (pa_findnodes, n);

    ret->sem.findnodes.id = id;
    ret->sem.findnodes.iter = iter;
    ret->sem.findnodes.item = item;
    ret->sem.findnodes.item_doc = item_doc;
    ret->sem.findnodes.item_res = item_res;

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter, .type = aat_nat };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = item_res, .type = aat_pnode };

    /* the result is ordered on iter|item */
    PFord_set_add (ret->orderings, sortby (iter, item));

    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}

/**
 * Constructor for finding nodes based on value index
 */
PFpa_op_t *
PFpa_vx_lookup (const PFpa_op_t *n,
                PFalg_col_t iter,
                PFalg_col_t item,
                PFalg_col_t item_doc,
                PFalg_col_t item_res,
                bool id,
                const char* ns1,
                const char* loc1,
                const char* ns2,
                const char* loc2)
{
    PFpa_op_t *ret = wire1 (pa_vx_lookup, n);

    ret->sem.vx_lookup.id = id;
    ret->sem.vx_lookup.iter = iter;
    ret->sem.vx_lookup.item = item;
    ret->sem.vx_lookup.item_doc = item_doc;
    ret->sem.vx_lookup.item_res = item_res;
    ret->sem.vx_lookup.ns1 = ns1;
    ret->sem.vx_lookup.loc1 = loc1;
    ret->sem.vx_lookup.ns2 = ns2;
    ret->sem.vx_lookup.loc2 = loc2;

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = iter, .type = aat_nat };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = item_res, .type = aat_pnode };

    /* the result is ordered on iter|item */
    PFord_set_add (ret->orderings, sortby (iter, item));

    ret->cost = DEFAULT_COST + n->cost;

    return ret;
}
/* vim:set shiftwidth=4 expandtab: */
