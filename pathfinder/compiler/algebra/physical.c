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
 * $Id$
 */

/* always include pathfinder.h first */
#include "pathfinder.h"

#include <assert.h>
/* FIXME: only for debugging */
#include <stdio.h>

#include "physical.h"
#include "mem.h"
#include "oops.h"

#include "ordering.h"
#include "properties.h"

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


#if 0

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
 * @param attlist Attribute list, similar to the literal table
 *                constructor PFpa_lit_tbl(), see also
 *                PFalg_attlist().
 */
PFpa_op_t *
PFpa_empty_tbl (PFalg_attlist_t attlist)
{
    PFpa_op_t   *ret;      /* return value we are building */

    /* instantiate the new algebra operator node */
    ret = leaf (pa_empty_tbl);

    /* set its schema */
    ret->schema.items
        = PFmalloc (attlist.count * sizeof (*(ret->schema.items)));
    for (unsigned int i = 0; i < attlist.count; i++) {
        ret->schema.items[i].name = attlist.atts[i];
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = attlist.count;

    /* play safe: set these fields */
    ret->sem.lit_tbl.count  = 0;
    ret->sem.lit_tbl.tuples = NULL;

    return ret;
}
#endif

/**
 * Construct an algebra node representing a literal table, given
 * an attribute list and a list of tuples.
 *
 * @b Orderings
 *
 * We currently derive orderings only for tables that contain
 * exactly one tuple.  These tables are trivially ordered by all
 * permutations of the table's attributes.  (No need to generate
 * all @em subsets, as the permutations imply their prefixes and
 * thus each possible ordering.)
 *
 * @b Costs
 *
 * Literal tables all have a cost value of 1.
 *
 *
 * @param attlist Attribute list of the literal table. (Most easily
 *                constructed using #PFalg_attlist() or its abbreviated
 *                macro #attlist().)
 * @param count  Number of tuples that follow
 * @param tuples Tuples of this literal table, as #PFalg_tuple_t.
 *               This array must be exactly @a count items long.
 */
PFpa_op_t *
PFpa_lit_tbl (PFalg_attlist_t attlist,
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
        = PFmalloc (attlist.count * sizeof (*(ret->schema.items)));
    for (i = 0; i < attlist.count; i++) {
        ret->schema.items[i].name = attlist.atts[i];
        ret->schema.items[i].type = 0;
    }
    ret->schema.count = attlist.count;

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
     * sorted on all permutations of its attributes. (Note that this
     * includes all sublists thereof.)
     */
    if (count == 1) {

        PFord_ordering_t ord = PFordering ();

        for (unsigned int i = 0; i < attlist.count; i++)
            ord = PFord_refine (ord, attlist.atts[i]);

        ret->orderings = PFord_permutations (ord);
    }

    /* ---- literal table costs ---- */
    ret->cost = 1;

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

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of UNION do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two attributes match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                             .type = n1->schema.items[i].type
                                                 | n2->schema.items[j].type };
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
                                          PFprop_const_val_at (n2->prop, a2))
                && PFalg_atom_cmp (PFprop_const_val_at (n1->prop, a1),
                                   PFprop_const_val_at (n2->prop, a2)) < 0) {

                PFord_set_add (
                        ret->orderings,
                        PFord_refine (PFordering (),
                                      PFprop_const_at (n1->prop, a1)));
            }
    }

    /* prune away duplicate orderings */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- AppendUnion: costs ---- */
    ret->cost = 1 + n1->cost + n2->cost;

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

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of UNION do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two attributes match, so include their name
                 * and type information into the result. This allows
                 * for the order of schema items in n1 and n2 to be
                 * different.
                 */
                ret->schema.items[i] =
                (struct PFalg_schm_item_t) { .name = n1->schema.items[i].name,
                                             .type = n1->schema.items[i].type
                                                 | n2->schema.items[j].type };
                break;
            }

        if (j == n2->schema.count)
            PFoops (OOPS_FATAL,
                    "Schema of two arguments of UNION do not match");
    }

    if (PFord_count (ord) != 1)
        PFoops (OOPS_FATAL,
                "MergeUnion for more complex orderings is not yet implemented "
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
                                          PFprop_const_val_at (n2->prop, a2))
                && PFalg_atom_cmp (PFprop_const_val_at (n1->prop, a1),
                                   PFprop_const_val_at (n2->prop, a2)) < 0) {

                PFord_ordering_t o
                    = PFord_refine (ord, PFprop_const_at (n1->prop, a1));

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
    ret->cost = 1 + PFord_count (ord) + n1->cost + n2->cost;

    return ret;
}

/**
 * Intersect: No specialized implementation here; always applicable.
 *
 * @todo
 *   Ordering and costs not implemented, yet.
 */
PFpa_op_t *
PFpa_intersect (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t    *ret = wire2 (pa_intersect, n1, n2);
    unsigned int  i, j;

    assert (!"PFpa_intersect(): Ordering and costs not implemented.");

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of Intersect do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two attributes match, so include their name
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

    /* see if both operands have same number of attributes */
    if (n1->schema.count != n2->schema.count)
        PFoops (OOPS_FATAL,
                "Schema of two arguments of Difference do not match");

    /* allocate memory for the result schema */
    ret->schema.count = n1->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* see if we find each attribute of n1 also in n2 */
    for (i = 0; i < n1->schema.count; i++) {
        for (j = 0; j < n2->schema.count; j++)
            if (n1->schema.items[i].name == n2->schema.items[j].name) {
                /* The two attributes match, so include their name
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
    ret->cost = 1 + n1->cost + n2->cost;

    return ret;
}

/** 
 * Cross product (Cartesian product) between two algebra expressions.
 * Arguments @a a and @a b must not have any equally named attribute.
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

    /* copy schema from argument 2, check for duplicate attribute names */
    for (j = 0; j < b->schema.count; j++)
        ret->schema.items[a->schema.count + j] = b->schema.items[j];

    /* ---- cross product orderings ---- */

    /* consider all orderings of a */
    for (unsigned int i = 0; i < PFord_set_count (a->orderings); i++) {

        /* refine by all the orderings in b */
        for (unsigned int j = 0; j < PFord_set_count (b->orderings); j++) {

            /* Ordering of a is the major ordering */
            ord = PFord_set_at (a->orderings, i);

            /* refine attribute by attribute with current ordering of b */
            for (unsigned int k = 0;
                 k < PFord_count (PFord_set_at (b->orderings, j));
                 k++)
                ord = PFord_refine (
                        ord,
                        PFord_order_at ( PFord_set_at (b->orderings, j), k));

            /* add it to the orderings of the result */
            PFord_set_add (ret->orderings, ord);
        }
    }

    /* ---- cross product costs ---- */
    ret->cost = a->cost * b->cost + a->cost + b->cost;

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
             PFalg_att_t attname, PFalg_atom_t value)
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
        = (PFalg_schm_item_t) { .name = attname, .type = value.type };

    /* FIXME: Should we copy values here? */
    ret->sem.attach.attname = attname;
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

            PFord_ordering_t ord = PFord_refine (prefix, attname);

            for (unsigned int k = j; k < PFord_count (current_in); k++)
                ord = PFord_refine (ord, PFord_order_at (current_in, k));

            PFord_set_add (ret->orderings, ord);

            if (j < PFord_count (current_in))
                prefix = PFord_refine (prefix, PFord_order_at (current_in, j));
        }
    }

    /* ---- ColumnAttach: costs ---- */
    ret->cost = 1 + n->cost;

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
    ret->cost = n->cost + 1;

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
                /* set name and type for this attribute in the result schema */
                ret->schema.items[i].name = ret->sem.proj.items[i].new;
                ret->schema.items[i].type = n->schema.items[j].type;

                break;
            }

        /* did we find the attribute? */
        if (j >= n->schema.count)
            PFoops (OOPS_FATAL,
                    "attribute `%s' referenced in projection not found",
                    PFatt_print (ret->sem.proj.items[i].old));
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
                if (ret->sem.proj.items[k].old == PFord_order_at (ni, j))
                    break;

            if (k < ret->sem.proj.count)
                prefix = PFord_refine (prefix, ret->sem.proj.items[k].new);
            else
                break;
        }

        if (PFord_count (prefix) > 0)
            PFord_set_add (ret->orderings, prefix);
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

/** Helper: Is attribute @a att contained in schema @a s? */
static bool
contains_att (PFalg_schema_t s, PFalg_att_t att)
{
    for (unsigned int i = 0; i < s.count; i++)
        if (s.items[i].name == att)
            return true;

    return false;
}

/**
 * LeftJoin: Equi-Join of two relations. Preserves the ordering
 *           of the left operand.
 */
PFpa_op_t *
PFpa_leftjoin (PFalg_att_t att1, PFalg_att_t att2,
               const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_leftjoin, n1, n2);

    /* see if we can find attribute att1 in n1 */
    if (contains_att (n1->schema, att1) && contains_att (n2->schema, att2)) {
        ret->sem.eqjoin.att1 = att1;
        ret->sem.eqjoin.att2 = att2;
    }
    else if (contains_att (n2->schema, att1)
             && contains_att (n1->schema, att2)) {
        ret->sem.eqjoin.att2 = att1;
        ret->sem.eqjoin.att1 = att2;
    }
    else
        PFoops (OOPS_FATAL, "problem with attributes in LeftJoin");

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
     * We may interleave this ordering with any attribute marked
     * constant in the right operand.
     *
     * If the (left) join attribute is among the orderings in the
     * left relation, we may add the corresponding right join
     * attribute anywhere after the left join attribute.
     */

    ret->orderings = PFord_set ();

    for (unsigned int i = 0; i < PFord_set_count (n1->orderings); i++) {

        PFord_ordering_t left_ordering = PFord_set_at (n1->orderings, i);

        PFord_ordering_t ord = PFordering ();

        /*
         * Walk over left ordering, and keep an eye for the left join
         * attribute.
         */
        bool found = false;

        for (unsigned int j = 0; j < PFord_count (left_ordering); j++) {

            if (PFord_order_at (left_ordering, j) == ret->sem.eqjoin.att1)
                /* Hey, we found the left join attribute. */
                found = true;

            if (found) {

                /*
                 * We already found the left join attribute. So we may
                 * include the right attribute into the result ordering.
                 * (We may include the right attribute anywhere after
                 * or immediately before the left attribute.)
                 */

                /* Insert the left join attribute */
                PFord_ordering_t ord2
                    = PFord_refine (ord, ret->sem.eqjoin.att2);

                /* Fill up with the remaining ordering of left relation */
                for (unsigned int k = j; k < PFord_count (left_ordering); k++)
                    ord2 = PFord_refine (ord2,
                                         PFord_order_at (left_ordering, j));

                /* and append to result */
                PFord_set_add (ret->orderings, ord2);
            }

            ord = PFord_refine (ord, PFord_order_at (left_ordering, j));
        }

        if (found)
            ord = PFord_refine (ord, ret->sem.eqjoin.att2);

        PFord_set_add (ret->orderings, ord);

    }

    /* Kick out those many duplicates we collected */
    ret->orderings = PFord_unique (ret->orderings);

    /* ---- LeftJoin: costs ---- */
    ret->cost = (n1->cost * n2->cost) + n1->cost + n2->cost;

    return ret;
}

/**
 * EqJoin: Equi-Join of two relations.
 */
PFpa_op_t *
PFpa_eqjoin (PFalg_att_t att1, PFalg_att_t att2,
             const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t  *ret = wire2 (pa_eqjoin, n1, n2);

    /* see if we can find attribute att1 in n1 */
    if (contains_att (n1->schema, att1) && contains_att (n2->schema, att2)) {
        ret->sem.eqjoin.att1 = att1;
        ret->sem.eqjoin.att2 = att2;
    }
    else if (contains_att (n2->schema, att1)
             && contains_att (n1->schema, att2)) {
        ret->sem.eqjoin.att2 = att1;
        ret->sem.eqjoin.att1 = att2;
    }
    else
        PFoops (OOPS_FATAL, "problem with attributes in EqJoin");

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
    ret->cost = (n1->cost * n2->cost) / 2 + n1->cost + n2->cost;

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
    ret->cost = (n->cost * n->cost) + n->cost;

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
    ret->cost = n->cost + n->cost;

    return ret;
}


/**
 * HashRowNumber: Introduce new row numbers.
 *
 * HashRowNumber uses a hash table to implement partitioning. Hence,
 * it does not require any specific input ordering.
 *
 * @param n        Argument relation.
 * @param new_att  Name of newly introduced attribute.
 * @param part     Partitioning attribute. @c NULL if partitioning
 *                 is not requested.
 */
PFpa_op_t *
PFpa_hash_rownum (const PFpa_op_t *n,
                  PFalg_att_t new_att,
                  PFalg_att_t part)
{
    PFpa_op_t *ret = wire1 (pa_hash_rownum, n);

    assert (!"FIXME: hash_rownum: orderings not implemented yet!");

    ret->sem.rownum.attname = new_att;
    ret->sem.rownum.part = part;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = new_att, .type = aat_nat };

    return ret;
}

PFpa_op_t *
PFpa_merge_rownum (const PFpa_op_t *n,
                   PFalg_att_t new_att,
                   PFalg_att_t part)
{
    PFpa_op_t        *ret = wire1 (pa_merge_rownum, n);
    PFord_ordering_t  ord = PFordering ();

    ret->sem.rownum.attname = new_att;
    ret->sem.rownum.part = part;

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = new_att, .type = aat_nat };

    /* ---- MergeRowNum: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* of course, the new attribute is also a valid ordering */
    if (part)
        ord = PFord_refine (ord, part);

    PFord_set_add (ret->orderings, PFord_refine (ord, new_att));

    /* ---- MergeRowNum: costs ---- */
    ret->cost = 1 + n->cost;

    return ret;
}

PFpa_op_t *
PFpa_select (const PFpa_op_t *n, PFalg_att_t att)
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
    ret->sem.select.att = att;

    /* ---- Select: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Select: costs ---- */
    ret->cost = n->cost + 1;

    return ret;
}


/**
 * HashCount: Hash-based Count operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_hash_count (const PFpa_op_t *n,
                            const PFalg_att_t res, const PFalg_att_t part)
{
    PFpa_op_t *ret = wire1 (pa_hash_count, n);

    ret->sem.count.res  = res;
    ret->sem.count.part = part;

    /* allocate memory for the result schema */
    ret->schema.count = part ? 2 : 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    if (part) {
        unsigned int i;
        for (i = 0; i < n->schema.count; i++)

            if (n->schema.items[i].name == part) {
                ret->schema.items[0] = n->schema.items[i];
                break;
            }

#ifndef NDEBUG
        if (i == n->schema.count)
            PFoops (OOPS_FATAL,
                    "HashCount: unable to find partitioning attribute `%s'",
                    PFatt_print (part));
#endif
    }

    ret->schema.items[ret->schema.count - 1]
        = (PFalg_schm_item_t) { .name = res, .type = aat_int };

    /* ---- HashCount: orderings ---- */
    /* HashCount does not provide any orderings. */

    /* ---- HashCount: costs ---- */
    ret->cost = n->cost * 3 / 2;

    return ret;
}


static PFpa_op_t *
llscj_worker (PFpa_op_kind_t axis,
              const PFpa_op_t *frag,
              const PFpa_op_t *ctx,
              const PFty_t test,
              const PFord_ordering_t in,
              const PFord_ordering_t out)
{
    PFpa_op_t *ret = wire2 (axis, frag, ctx);
    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);
    PFord_ordering_t item_iter
        = PFord_refine (PFord_refine (PFordering (), att_item), att_iter);

#ifndef NDEBUG
    unsigned short found = 0;

    for (unsigned int i = 0; i < ctx->schema.count; i++)
        if (ctx->schema.items[i].name == att_iter
            || ctx->schema.items[i].name == att_item)
            found++;

    if (found != 2)
        PFoops (OOPS_FATAL, "staircase join requires iter|item schema");
#endif

    /* The schema of the result part is iter|item */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = att_iter, .type = aat_nat };
    /* the result of an attribute axis is also of type attribute */
    if (axis == pa_llscj_attr)
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = att_item, .type = aat_anode };
    else
        ret->schema.items[1]
            = (PFalg_schm_item_t) { .name = att_item, .type = aat_pnode };

    /* store semantic content in node */
    ret->sem.scjoin.ty = test;

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

    /* ---- LLSCJchild: orderings ---- */

    /* the specified output ordering */
    PFord_set_add (ret->orderings, ret->sem.scjoin.out);

    /*
     * Costs depend on actual axis, and we do that in the
     * PFpa_llscj_XXX functions
     */

    return ret;
}

/**
 * Helper function for binary arithmetics (with both arguments to be
 * table columns).
 */
static PFpa_op_t *
bin_arith (PFpa_op_kind_t op, const PFpa_op_t *n, const PFalg_att_t res,
           const PFalg_att_t att1, const PFalg_att_t att2)
{
    PFpa_op_t           *ret = wire1 (op, n);
    PFalg_simple_type_t  t1 = 0;
    PFalg_simple_type_t  t2 = 0;

    assert (n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++) {

        ret->schema.items[i] = n->schema.items[i];

        if (n->schema.items[i].name == att1)
            t1 = n->schema.items[i].type;
        if (n->schema.items[i].name == att2)
            t2 = n->schema.items[i].type;
    }

    assert (t1); assert (t2);

    if (t1 != t2)
        PFoops (OOPS_FATAL,
                "illegal types in arithmetic operation: %u vs. %u", t1, t2);

    /* finally add schema item for new attribute */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = t1 };

    /* store information about attributes for arithmetics */
    /* FIXME: Copy strings here? */
    ret->sem.binary.res = res;
    ret->sem.binary.att1 = att1;
    ret->sem.binary.att2 = att2;

    /* ---- NumAdd: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = n->cost + 2;    /* 2 as we want NumAddConst to be cheaper */

    return ret;
}

/**
 * Helper function for binary arithmetics (where second argument is
 * an atom).
 */
static PFpa_op_t *
bin_arith_atom (PFpa_op_kind_t op, const PFpa_op_t *n, const PFalg_att_t res,
                const PFalg_att_t att1, const PFalg_atom_t att2)
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

        if (n->schema.items[i].name == att1)
            t1 = n->schema.items[i].type;
    }

    assert (t1);

    if (t1 != att2.type)
        PFoops (OOPS_FATAL,
                "illegal types in arithmetic operation: %u vs. %u",
                t1, att2.type);

    /* finally add schema item for new attribute */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = t1 };

    /* store information about attributes for arithmetics */
    /* FIXME: Copy strings here? */
    ret->sem.bin_atom.res = res;
    ret->sem.bin_atom.att1 = att1;
    ret->sem.bin_atom.att2 = att2;

    /* ---- NumAdd: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = n->cost + 1;    /* cheaper than NumAdd */

    return ret;
}

/**
 * Helper function for binary comparisons (with both arguments to be
 * table columns).
 */
static PFpa_op_t *
bin_comp (PFpa_op_kind_t op, const PFpa_op_t *n, const PFalg_att_t res,
          const PFalg_att_t att1, const PFalg_att_t att2)
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
        if (n->schema.items[i].name == att1)
            t1 = n->schema.items[i].type;
        if (n->schema.items[i].name == att2)
            t2 = n->schema.items[i].type;
#endif
    }

#ifndef NDEBUG
    assert (t1); assert (t2);

    if (t1 != t2)
        PFoops (OOPS_FATAL,
                "illegal types in arithmetic operation: %u vs. %u", t1, t2);
#endif

    /* finally add schema item for new attribute */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = aat_bln };

    /* store information about attributes for arithmetics */
    /* FIXME: Copy strings here? */
    ret->sem.binary.res = res;
    ret->sem.binary.att1 = att1;
    ret->sem.binary.att2 = att2;

    /* ---- NumAdd: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = n->cost + 2;    /* 2 as we want NumAddConst to be cheaper */

    return ret;
}

/**
 * Helper function for binary comparisons (where second argument is
 * an atom).
 */
static PFpa_op_t *
bin_comp_atom (PFpa_op_kind_t op, const PFpa_op_t *n, const PFalg_att_t res,
               const PFalg_att_t att1, const PFalg_atom_t att2)
{
    PFpa_op_t           *ret = wire1 (op, n);
#ifndef NDEBUG
    PFalg_simple_type_t  t1 = 0;
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
        if (n->schema.items[i].name == att1)
            t1 = n->schema.items[i].type;
#endif
    }

#ifndef NDEBUG
    assert (t1);

    if (t1 != att2.type)
        PFoops (OOPS_FATAL,
                "illegal types in arithmetic operation: %u vs. %u",
                t1, att2.type);
#endif

    /* finally add schema item for new attribute */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = aat_bln };

    /* store information about attributes for arithmetics */
    /* FIXME: Copy strings here? */
    ret->sem.bin_atom.res = res;
    ret->sem.bin_atom.att1 = att1;
    ret->sem.bin_atom.att2 = att2;

    /* ---- NumAdd: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = n->cost + 1;    /* cheaper than NumAdd */

    return ret;
}

PFpa_op_t *
PFpa_num_add (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_arith (pa_num_add, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_sub (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_arith (pa_num_sub, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_mult (const PFpa_op_t *n, const PFalg_att_t res,
               const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_arith (pa_num_mult, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_div (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_arith (pa_num_div, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_mod (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_arith (pa_num_mod, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_add_atom (const PFpa_op_t *n, const PFalg_att_t res,
                   const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_arith_atom (pa_num_add_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_sub_atom (const PFpa_op_t *n, const PFalg_att_t res,
                   const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_arith_atom (pa_num_sub_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_mult_atom (const PFpa_op_t *n, const PFalg_att_t res,
                    const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_arith_atom (pa_num_mult_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_div_atom (const PFpa_op_t *n, const PFalg_att_t res,
                   const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_arith_atom (pa_num_div_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_num_mod_atom (const PFpa_op_t *n, const PFalg_att_t res,
                   const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_arith_atom (pa_num_mod_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_eq (const PFpa_op_t *n, const PFalg_att_t res,
         const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_comp (pa_eq, n, res, att1, att2);
}

PFpa_op_t *
PFpa_gt (const PFpa_op_t *n, const PFalg_att_t res,
         const PFalg_att_t att1, const PFalg_att_t att2)
{
    return bin_comp (pa_gt, n, res, att1, att2);
}

PFpa_op_t *
PFpa_eq_atom (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_comp_atom (pa_eq_atom, n, res, att1, att2);
}

PFpa_op_t *
PFpa_gt_atom (const PFpa_op_t *n, const PFalg_att_t res,
              const PFalg_att_t att1, const PFalg_atom_t att2)
{
    return bin_comp_atom (pa_gt_atom, n, res, att1, att2);
}


/**
 * Helper function for unary arithmetics (numeric and Boolean negation).
 */
static PFpa_op_t *
unary_arith (PFpa_op_kind_t op,
             const PFpa_op_t *n, const PFalg_att_t res, const PFalg_att_t att)
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

        if (n->schema.items[i].name == att)
            t1 = n->schema.items[i].type;
    }

    assert (t1);

    /* finally add schema item for new attribute */
    ret->schema.items[n->schema.count]
        = (PFalg_schm_item_t) { .name = res, .type = t1 };

    /* store information about attributes for arithmetics */
    /* FIXME: Copy strings here? */
    ret->sem.unary.res = res;
    ret->sem.unary.att = att;

    /* ---- UnaryArith: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /*
     * Hmm... From the ordering contributed by the arithmetic operands
     * we could actually infer some more order information. But it's
     * probably not worth to fiddle with that...
     */

    /* ---- NumAdd: costs ---- */
    ret->cost = n->cost + 1;

    return ret;
}

PFpa_op_t *
PFpa_num_neg (const PFpa_op_t *n, const PFalg_att_t res, const PFalg_att_t att)
{
    return unary_arith (pa_num_neg, n, res, att);
}

PFpa_op_t *
PFpa_bool_not (const PFpa_op_t *n, const PFalg_att_t res, const PFalg_att_t att)
{
    return unary_arith (pa_bool_not, n, res, att);
}


PFpa_op_t *
PFpa_cast (const PFpa_op_t *n, const PFalg_att_t att, PFalg_simple_type_t ty)
{
    PFpa_op_t  *ret = wire1 (pa_cast, n);
    bool        found = false;

    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (n->schema.count * sizeof (*(ret->schema.items)));

    ret->sem.cast.att = att;
    ret->sem.cast.ty  = ty;

    for (unsigned int i = 0; i < n->schema.count; i++)
        if (n->schema.items[i].name == att) {
            ret->schema.items[i]
                = (PFalg_schm_item_t) { .name = att, .type = ty };
            found = true;
        }
        else
            ret->schema.items[i] = n->schema.items[i];

    if (!found)
        PFoops (OOPS_FATAL,
                "attribute `%s' not found in physical algebra cast",
                PFatt_print (att));

    /* ---- Cast: orderings ---- */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Cast: costs ---- */
    ret->cost = n->cost + 1;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_anc (const PFpa_op_t *frag,
                const PFpa_op_t *ctx,
                const PFty_t test,
                const PFord_ordering_t in,
                const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_anc, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_anc_self (const PFpa_op_t *frag,
                     const PFpa_op_t *ctx,
                     const PFty_t test,
                     const PFord_ordering_t in,
                     const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_anc_self, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_attr (const PFpa_op_t *frag,
                 const PFpa_op_t *ctx,
                 const PFty_t test,
                 const PFord_ordering_t in,
                 const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_attr, frag, ctx, test, in, out);

    /* there is only one implementation */
    ret->cost = 1 + ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_child (const PFpa_op_t *frag,
                  const PFpa_op_t *ctx,
                  const PFty_t test,
                  const PFord_ordering_t in,
                  const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_child, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_desc (const PFpa_op_t *frag,
                 const PFpa_op_t *ctx,
                 const PFty_t test,
                 const PFord_ordering_t in,
                 const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_desc, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_desc_self (const PFpa_op_t *frag,
                      const PFpa_op_t *ctx,
                      const PFty_t test,
                      const PFord_ordering_t in,
                      const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_desc_self,frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_foll (const PFpa_op_t *frag,
                 const PFpa_op_t *ctx,
                 const PFty_t test,
                 const PFord_ordering_t in,
                 const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_foll, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_foll_sibl (const PFpa_op_t *frag,
                      const PFpa_op_t *ctx,
                      const PFty_t test,
                      const PFord_ordering_t in,
                      const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_foll_sibl,frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_parent (const PFpa_op_t *frag,
                   const PFpa_op_t *ctx,
                   const PFty_t test,
                   const PFord_ordering_t in,
                   const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_parent, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_prec (const PFpa_op_t *frag,
                 const PFpa_op_t *ctx,
                 const PFty_t test,
                 const PFord_ordering_t in,
                 const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_prec, frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *
PFpa_llscj_prec_sibl (const PFpa_op_t *frag,
                      const PFpa_op_t *ctx,
                      const PFty_t test,
                      const PFord_ordering_t in,
                      const PFord_ordering_t out)
{
    PFpa_op_t *ret = llscj_worker (pa_llscj_prec_sibl,frag, ctx, test, in, out);

    PFord_ordering_t iter_item
        = PFord_refine (PFord_refine (PFordering (), att_iter), att_item);

    /* ---- LLSCJchild: costs ---- */

    if (PFord_implies (ret->sem.scjoin.in, iter_item)) {
        /* input has iter|item ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 3 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */
            ret->cost = 2 * ctx->cost;
        }

    }
    else {
        /* input has item|iter ordering */

        if (PFord_implies (ret->sem.scjoin.out, iter_item)) {
            /* output has iter|item ordering */
            ret->cost = 1 * ctx->cost;
        }
        else {
            /* output has item|iter ordering */

            /* should be cheapest */
            ret->cost = 0 * ctx->cost;
        }
    }

    ret->cost += ctx->cost;

    return ret;
}


/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.  Returns a (frag, result) pair.
 */
PFpa_op_t *
PFpa_doc_tbl (const PFpa_op_t *rel)
{
    PFpa_op_t         *ret;

#ifndef NDEBUG
    unsigned short found = 0;

    for (unsigned int i = 0; i < rel->schema.count; i++)
        if (rel->schema.items[i].name == att_iter
            || rel->schema.items[i].name == att_item)
            found++;

    if (found != 2)
        PFoops (OOPS_FATAL, "document access requires iter|item schema");
#endif

    ret = wire1 (pa_doc_tbl, rel);

    /* The schema of the result part is iter|item */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*ret->schema.items));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = att_iter,
                                .type = aat_nat };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = att_item,
                                .type = aat_pnode };

    /* ---- doc_tbl: orderings ---- */

    /* If the input is sorted by `iter', the output will be as well. */
    bool sorted_by_iter = false;

    for (unsigned int i = 0; i < PFord_set_count (rel->orderings); i++)
        if (PFord_implies (PFord_set_at (rel->orderings, i),
                           PFord_refine (PFordering (), att_iter))) {
            sorted_by_iter = true;
            break;
        }

    if (sorted_by_iter) {
        if (PFprop_const (rel->prop, att_item))
            PFord_set_add (ret->orderings,
                           PFord_refine (PFord_refine (PFordering (),
                                                       att_iter),
                                         att_item));
        else
            PFord_set_add (ret->orderings,
                           PFord_refine (PFordering (), att_iter));
    }

    /* ---- doc_tbl: costs ---- */
    ret->cost = 10 * rel->cost;

    return ret;
}

/**
 * Empty fragment list
 */
PFpa_op_t *
PFpa_empty_frag (void)
{
    return leaf (pa_empty_frag);
}


/**
 * Extract the expression result part from a (frag, result) pair.
 */
PFpa_op_t *
PFpa_roots (const PFpa_op_t *n)
{
    PFpa_op_t *ret = wire1 (pa_roots, n);

    /* allocate memory for the result schema */
    ret->schema.count = n->schema.count;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from n */
    for (unsigned int i = 0; i < n->schema.count; i++)
        ret->schema.items[i] = n->schema.items[i];

    /* ---- Roots: orderings ---- */

    /* `Rel' part of (Frag, Rel) pair inherits orderings of its argument */
    for (unsigned int i = 0; i < PFord_set_count (n->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (n->orderings, i));

    /* ---- Roots: costs ---- */
    ret->cost = 0;

    return ret;
}

/**
 * Extract the document fragment part from a (frag, result) pair.
 * It typically contains newly constructed nodes of some node
 * construction operator.  The document representation is dependent
 * on the back-end system.  Hence, the resulting algebra node does
 * not have a meaningful relational schema (in fact, the schema
 * component will be set to NULL).
 */
PFpa_op_t *
PFpa_fragment (const PFpa_op_t *n)
{
    PFpa_op_t *ret = wire1 (pa_fragment, n);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* ---- Fragment: costs ---- */
    ret->cost = 0;

    return ret;
}

/** Form algebraic disjoint union between two fragments. */
PFpa_op_t *
PFpa_frag_union (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t *ret = wire2 (pa_frag_union, n1, n2);

    assert (n1->schema.count == 0);
    assert (n2->schema.count == 0);

    /* allocate memory for the result schema */
    ret->schema.count = 0;
    ret->schema.items = NULL;

    /* ---- FragUnion: costs ---- */
    ret->cost = 0;

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
PFpa_doc_access (const PFpa_op_t *doc, const PFpa_op_t *alg,
                 PFalg_att_t att, PFalg_doc_t doc_col)
{
    unsigned int i;
    PFpa_op_t *ret = wire2 (pa_doc_access, doc, alg);

    /* allocate memory for the result schema */
    ret->schema.count = alg->schema.count + 1;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    /* copy schema from alg and change type of column  */
    for (i = 0; i < alg->schema.count; i++)
        ret->schema.items[i] = alg->schema.items[i];

    ret->schema.items[i] 
        = (struct PFalg_schm_item_t) { .type = aat_str, .name = att_res };

    ret->sem.doc_access.att = att;
    ret->sem.doc_access.doc_col = doc_col;

    /* ordering stays the same */
    for (unsigned int i = 0; i < PFord_set_count (alg->orderings); i++)
        PFord_set_add (ret->orderings, PFord_set_at (alg->orderings, i));
    /* costs */
    ret->cost = doc->cost + alg->cost + 1;

    return ret;
}

/**
 * Construct concatenation of multiple strings
 *
 * @a n1 contains the sets of strings @a n2 stores the separator for each set
 */
PFpa_op_t *
PFpa_string_join (const PFpa_op_t *n1, const PFpa_op_t *n2)
{
    PFpa_op_t *ret = wire2 (pa_string_join, n1, n2);

    /* allocate memory for the result schema */
    ret->schema.count = 2;
    ret->schema.items
        = PFmalloc (ret->schema.count * sizeof (*(ret->schema.items)));

    ret->schema.items[0]
        = (PFalg_schm_item_t) { .name = att_iter, .type = aat_nat };
    ret->schema.items[1]
        = (PFalg_schm_item_t) { .name = att_item, .type = aat_str };

    /* result is in iter|item order */
    PFord_set_add (ret->orderings,
                   PFord_refine (PFord_refine (PFordering (), att_iter),
                                 att_item));
    /* costs */
    ret->cost = n1->cost + n2->cost + 1;

    return ret;
}

/**
 * Construct algebra node that will serialize the argument when executed.
 * A serialization node will be placed on the very top of the algebra
 * expression tree. Its main use is to have an explicit match for
 * the expression root.
 *
 * @a doc is the current document (live nodes) and @a alg is the overall
 * algebra expression.
 */
PFpa_op_t *
PFpa_serialize (const PFpa_op_t *doc, const PFpa_op_t *alg)
{
    PFpa_op_t *ret = wire2 (pa_serialize, doc, alg);

    ret->cost = doc->cost + alg->cost;

    return ret;
}


/* vim:set shiftwidth=4 expandtab: */
