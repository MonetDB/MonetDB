/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Compile a logical algebra tree into a physical execution plan.
 *
 * @section planning Physical Plan Generation
 *
 * For any algebra subexpression, we derive all plans that appear
 * interesting so far. We consider a plan to be interesting if
 *
 *  - There is no other plan that returns the same (or better)
 *    ordering in a cheaper way.
 *
 * For each logical algebra subexpression, we thus return a
 * @em set of plans, each plan with another ordering.  At the
 * end, we pick the cheapest of all the plans we found for the
 * input query.
 *
 * This approach is a rather classical one.  However, in contrast
 * to classical relational systems, we do @em not operate on
 * algebra @em trees.  The input for our planner is a directed
 * acyclic graph (DAG).  While this is not a critical issue for
 * ``normal'' expressions (the worst case would be that we
 * unneccessarily compute a subexpression result twice), we
 * may run into trouble with expressions that contribute new
 * live node sets (document table access, node construction, etc.)
 *
 * @section live_nodes Live Node Set Handling
 *
 * In contrast to a standard relational algebra, our algebra
 * is not completely side-effect free.  Side-effects originate
 * in XQuery's node construction operators that introduce
 * side-effects in XQuery.
 *
 * In particular, we must evaluate expressions that construct
 * nodes, exactly as many times as they appear in the original
 * query.  In our logical algebra, this is reflected by the way
 * we generate the algebra DAGs:
 *
 *  - Generate an expression tree for each node constructor.
 *    In case an expression result is assigned to a variable
 *    that occurs several times, all these occurrences
 *    reference the same algebra expression tree (implemented
 *    as a C pointer).
 *    .
 *  - Two algebra expressions that construct nodes are @em never
 *    considered identical during algebra CSE.  This means that
 *    subexpressions that construct nodes are shared iff created
 *    as described in the above point, i.e., if they stem from
 *    the same XQuery subexpression.
 *
 * Node construction expressions may be referenced several times
 * in the logical tree.  They will at least be referenced by
 * a @c FRAGs and a @c ROOTS node that extract the two sub-results
 * (the newly introduced live node set and the logical query
 * result, respectively).
 *
 * If we produced multiple plans for one construction operator,
 * the different references might pick different plans, thus
 * evaluating the construction more than once.  To guarantee
 * correct consideration of side-effects, we thus produce
 * exactly one plan for each construction operator.
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include "oops.h"

#include <assert.h>
#include <stdio.h>

#include "planner.h"

/* We need PFmalloc. */
#include "mem.h"

/* We use array to handle lists of plan candidates. */
#include "array.h"

/* We compile from logical algebra into physical algebra. */
#include "logical.h"
#include "physical.h"
#include "physical_mnemonic.h"

/* We will need a notion of ``ordering''. */
#include "ordering.h"

/* short-hands */
#define L(p) ((p)->child[0])
#define R(p) ((p)->child[1])
#define LL(p) (L(L(p)))
#define LR(p) (R(L(p)))

/**
 * A ``plan'' is actually a physical algebra operator tree.
 */
typedef PFpa_op_t plan_t;

/* ensure some ordering on a given plan */
static PFplanlist_t *ensure_ordering (const plan_t *unordered,
                                      PFord_ordering_t required);
/* given a list of plans, prune all plans that are not interesting */
static PFplanlist_t *prune_plans (PFplanlist_t *planlist);
/* test if a is cheaper than b (only look at costs) */
static bool costless (const plan_t *a, const plan_t *b);
/* test if a is a better plan than b (look at orderings and costs) */
static bool better_or_equal (const plan_t *a, const plan_t *b);

/**
 * Create a new (empty) plan list.
 *
 * When searching for plans for some logical algebra subtree,
 * we start with an empty list of possible plans. We then add
 * each plan we find to this list.
 */
static PFplanlist_t *
new_planlist (void)
{
    return PFarray (sizeof (plan_t *));
}

/**
 * Add a plan to a plan list.
 *
 * We collect each ``interesting'' plan we find in a list of
 * plans that we will return as the compilation result for a
 * logical algebra subexpression.
 */
static void
add_plan (PFplanlist_t *list, const plan_t *plan)
{
    *((plan_t **) PFarray_add (list)) = (plan_t *) plan;
}

/**
 * Add a set of plans to a plan list.
 *
 * We collect each ``interesting'' plan we find in a list of
 * plans that we will return as the compilation result for a
 * logical algebra subexpression.
 */
static void
add_plans (PFplanlist_t *list, PFplanlist_t *plans)
{
    PFarray_concat (list, plans);
}




/**
 * Create physical equivalent of our `serialize' operator.
 *
 * The physical Serialize operator expects its input to be sorted
 * by iter|pos (actually, iter should be constant).
 */
static PFplanlist_t *
plan_serialize (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();

    assert (n); assert (n->kind == la_serialize);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* The serialize operator requires its input to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (n->sem.serialize.pos)));

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a Serialize operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        for (unsigned int j = 0; j < PFarray_last (L(n)->plans); j++)
            add_plan (ret,
                      serialize (
                          *(plan_t **) PFarray_at (L(n)->plans, j),
                          *(plan_t **) PFarray_at (sorted, i),
                          n->sem.serialize.item));

    return ret;
}

/**
 * Generate possible physical plans for literal table construction.
 *
 * There's actually just the one literal table constructor in
 * our physical algebra.  In some cases, however, we can avoid
 * constructing the table at all, e.g. by using the `attach'
 * operator.
 *
 * The literal table operator will try to infer some orderings
 * from the input tuples (see #PFpa_lit_tbl).
 */
static PFplanlist_t *
plan_lit_tbl (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();
    plan_t        *plan = NULL;

    PFalg_attlist_t attlist;

    attlist.count = n->schema.count;
    attlist.atts  = PFmalloc (attlist.count * sizeof (*attlist.atts));

    for (unsigned int i = 0; i < attlist.count; i++)
        attlist.atts[i] = n->schema.items[i].name;

    /*
     * There is exactly this one plan.
     */
    plan = lit_tbl (attlist, n->sem.lit_tbl.count, n->sem.lit_tbl.tuples);

    /* Add this plan to the return set of plans. */
    add_plan (ret, plan);

    return ret;
}

/**
 * Generate possible physical plans for literal table construction
 * of empty tables
 *
 * There's actually just the one literal table constructor in
 * our physical algebra.  In some cases, however, we can avoid
 * constructing the table at all, e.g. by using the `attach'
 * operator.
 *
 * The literal table operator will try to infer some orderings
 * from the input tuples (see #PFpa_lit_tbl).
 */
static PFplanlist_t *
plan_empty_tbl (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();
    plan_t        *plan = NULL;

    /*
     * There is exactly this one plan.
     */
    plan = empty_tbl (n->schema);

    /* Add this plan to the return set of plans. */
    add_plan (ret, plan);

    return ret;
}

/**
 * Generate possible physical plans for logical ColumnAttach
 * operator (direct mapping from logical to physical ColumnAttac).
 */
static PFplanlist_t *
plan_attach (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_attach);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        add_plan (ret,
                  attach (*(plan_t **) PFarray_at (L(n)->plans, r),
                          n->sem.attach.attname,
                          n->sem.attach.value));

    return ret;
}

/**
 * Worker for plan_cross(): adds plans for @a a x @a b to the
 * plan list @a ret.  In addition to the standard cross product,
 * this function also considers the case when @a a is a literal
 * table. In that case, the cross product may be implemented
 * @em much cheaper with the ColumnAttach operator.
 */
static void
cross_worker (PFplanlist_t *ret, const plan_t *a, const plan_t *b)
{
    /* add the cross product plan */
    add_plan (ret, cross (a, b));

    /* if a is a literal table, consider the ColumnAttach operator */
    if (a->kind == pa_lit_tbl && a->sem.lit_tbl.count == 1) {

        PFalg_tuple_t  t    = a->sem.lit_tbl.tuples[0];
        plan_t        *plan = (plan_t *) b;

        for (unsigned int i = 0; i < t.count; i++) {
            plan = attach (plan, a->schema.items[i].name, t.atoms[i]);
            /* assign logical properties to
               the additional physical node as well */
            plan->prop = a->prop;
        }

        add_plan (ret, plan);
    }
}

/**
 * Generate possible physical plans for cross product.
 *
 * For the logical operation R x S, this will return
 * both combinations, R x S and S x R, for the physical plan.
 *
 * If relations R and S follow the orderings O_r and O_s,
 * R x S will have the ordering O_r + O_s (see #PFpa_cross).
 */
static PFplanlist_t *
plan_cross (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_cross);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
            cross_worker (ret, *(plan_t **) PFarray_at (L(n)->plans, r),
                               *(plan_t **) PFarray_at (R(n)->plans, s));
            cross_worker (ret, *(plan_t **) PFarray_at (R(n)->plans, s),
                               *(plan_t **) PFarray_at (L(n)->plans, r));
        }

    return ret;
}

/**
 * Worker for plan_eqjoin().
 */
static void
join_worker (PFplanlist_t *ret,
             PFalg_att_t att1, PFalg_att_t att2,
             const plan_t *a, const plan_t *b)
{
    /*
     * try ``standard'' EqJoin, which does not give us any
     * ordering guarantees.
     */
    add_plan (ret, eqjoin (att1, att2, a, b));

    /*
     * LeftJoin may be more expensive, but gives us some ordering
     * guarantees.
     */
    add_plan (ret, leftjoin (att1, att2, a, b));
    add_plan (ret, leftjoin (att2, att1, b, a));

#if 0
    PFalg_att_t    att_a = NULL;
    PFalg_att_t    att_b = NULL;
    PFplanlist_t  *sorted_a;
    PFplanlist_t  *sorted_b;

    /* We can always apply the nested loops join */
    add_plan (ret, nljoin (att1, att2, a, b));

    /* test which of the two attributes is in which argument */
    for (unsigned int i = 0; i < a->schema.count; i++)
        if (a->schema.items[i].name == att1) {
            att_a = att1;
            break;
        }
        else if (a->schema.items[i].name == att2) {
            att_a = att2;
            break;
        }

    for (unsigned int i = 0; i < b->schema.count; i++)
        if (b->schema.items[i].name == att1) {
            att_b = att1;
            break;
        }
        else if (b->schema.items[i].name == att2) {
            att_b = att2;
            break;
        }

    assert (att_a && att_b);

    /* We can use MergeJoin after sorting both arguments on the
     * join attributes.
     */
    sorted_a = prune_plans (
                   ensure_ordering (a, PFord_refine (PFordering (), att_a)));
    sorted_b = prune_plans (
                   ensure_ordering (b, PFord_refine (PFordering (), att_b)));

    /* apply MergeJoin for all the resulting plans */
    for (unsigned int i = 0; i < PFarray_last (sorted_a); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_a); j++)
            add_plan (ret, merge_join (att_a, att_b,
                                       *(plan_t **) PFarray_at (sorted_a, i),
                                       *(plan_t **) PFarray_at (sorted_b, j)));
#endif
}

/**
 * Generate physical plans for equi-join.
 */
static PFplanlist_t *
plan_eqjoin (const PFla_op_t *n)
{
    PFplanlist_t  *ret       = new_planlist ();

    assert (n); assert (n->kind == la_eqjoin);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
            join_worker (ret, n->sem.eqjoin.att1, n->sem.eqjoin.att2,
                              *(plan_t **) PFarray_at (L(n)->plans, r),
                              *(plan_t **) PFarray_at (R(n)->plans, s));
            join_worker (ret, n->sem.eqjoin.att2, n->sem.eqjoin.att1,
                              *(plan_t **) PFarray_at (R(n)->plans, s),
                              *(plan_t **) PFarray_at (L(n)->plans, r));
        }

    return ret;
}

/**
 * Generate physical plans for semi-join.
 */
static PFplanlist_t *
plan_semijoin (const PFla_op_t *n)
{
    PFplanlist_t  *ret       = new_planlist ();

    assert (n); assert (n->kind == la_semijoin);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
            add_plan (ret, 
                      semijoin (n->sem.eqjoin.att1,
                                n->sem.eqjoin.att2,
                                *(plan_t **) PFarray_at (L(n)->plans, r),
                                *(plan_t **) PFarray_at (R(n)->plans, s)));
        }

    return ret;
}

/**
 * Generate physical plan for the logical `project' operator.
 *
 * There's just one physical operator for `project' that we
 * call from here.  The resulting plans will contain all
 * those orderings of @a n (or prefixes thereof), for which
 * we still have all attributes left in the projection result.
 */
static PFplanlist_t *
plan_project (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_project);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        add_plan (ret,
                  project (*(plan_t **) PFarray_at (L(n)->plans, r),
                           n->sem.proj.count, n->sem.proj.items));

    return ret;
}

/**
 * Generate physical plan for the logical `select' operator.
 *
 * There's just one physical operator for `select' that we
 * call from here.  The resulting plans will contain all
 * orderings of @a n.
 */
static PFplanlist_t *
plan_select (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_select);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        add_plan (ret,
                  select_ (*(plan_t **) PFarray_at (L(n)->plans, r),
                           n->sem.select.att));

    return ret;
}

/**
 * Helper function: Determine type of attribute @a att in schema
 * @a schema. Used by plan_disjunion().
 */
static PFalg_simple_type_t
type_of (PFalg_schema_t schema, PFalg_att_t att)
{
    for (unsigned int i = 0; i < schema.count; i++)
        if (schema.items[i].name == att)
            return schema.items[i].type;

    PFoops (OOPS_FATAL, "unable to find attribute %s in schema",
            PFatt_str (att));

    return 0;  /* pacify compilers */
}

/**
 * Generate physical plans for disjoint union.
 *
 * Available implementations are MergeUnion and AppendUnion.
 * MergeUnion is order-aware.  It expects both input relations
 * to be in some given ordering, and will propagate that ordering
 * to its output.  For two equal values in the ordering attribute,
 * MergeUnion will pick tuples from the left relation first, then
 * from the right relation.
 *
 * The alternative is AppendUnion, which simply appends two
 * relations.  AppendUnion is slightly cheaper and does not
 * require any specific input ordering.  Its output ordering,
 * however, is typically not valuable.
 *
 * @bug
 *   Due to the restrictions that MonetDB's binary table model
 *   gives us, we only support orderings on a single column in
 *   MergeUnion.
 */
static PFplanlist_t *
plan_disjunion (const PFla_op_t *n)
{
    PFplanlist_t  *ret       = new_planlist ();

    /* Consider each combination of plans in R and S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {

            plan_t *R = *(plan_t **) PFarray_at (L(n)->plans, r);
            plan_t *S = *(plan_t **) PFarray_at (R(n)->plans, s);

            /* common orderings of R and S */
            PFord_set_t common
                = PFord_unique (PFord_intersect (R->orderings, S->orderings));

            /*
             * unfortunately, we can only support MergeJoin with
             * one-column orderings in our MIL translation (and it
             * even has to be monomorphic).
             */
            PFord_set_t prefixes = PFord_set ();

            for (unsigned int i = 0; i < PFord_set_count (common); i++) {

                PFalg_att_t att = PFord_order_col_at (
                                      PFord_set_at (common, i),
                                      0);

                PFalg_simple_type_t tyR = type_of (R->schema, att);
                PFalg_simple_type_t tyS = type_of (S->schema, att);

                if (tyR == tyS
                    && (tyR == aat_nat || tyR == aat_int || tyR == aat_str
                        || tyR == aat_dec || tyR == aat_dbl || tyR == aat_uA))
                    PFord_set_add (prefixes, sortby (att));
            }

            /* kick out the duplicates */
            prefixes = PFord_unique (prefixes);

            /* and generate plans */
            for (unsigned int i = 0; i < PFord_set_count (prefixes); i++)
                add_plan (ret, merge_union (R, S, PFord_set_at (prefixes, i)));

            /* of course we can always do AppendUnion */
            add_plan (ret, append_union (R, S));

#if 0
            /* compute common prefixes of orderings in R and S */
            PFord_set_t prefixes
                = PFord_prefixes (
                        PFord_unique (
                            PFord_intersect (R->orderings, S->orderings)));

            for (unsigned int i = 0; i < PFord_set_count (prefixes); i++)
                add_plan (ret, merge_union ( R, S, PFord_set_at (prefixes, i)));
#endif
        }

    return ret;
}

/**
 * Generate physical plan for the logical 'intersect' operator.
 */
static PFplanlist_t *
plan_intersect (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    assert (n); assert (n->kind == la_intersect);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* consider each plan in L */
    for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
        /* and each plan in R */
        for (unsigned int r = 0; r < PFarray_last (R(n)->plans); r++)
            add_plan (ret,
                      intersect (
                          *(plan_t **) PFarray_at (L(n)->plans, l),
                          *(plan_t **) PFarray_at (R(n)->plans, r)));

    return ret;
}

/**
 * Generate physical plan for the logical `difference' operator.
 *
 * @todo
 *     We currently only have a Difference operator that can handle
 *     one column tables only.
 */
static PFplanlist_t *
plan_difference (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_difference);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* consider each plan in L */
    for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
        /* and each plan in R */
        for (unsigned int r = 0; r < PFarray_last (R(n)->plans); r++)
            add_plan (ret,
                      difference (
                          *(plan_t **) PFarray_at (L(n)->plans, l),
                          *(plan_t **) PFarray_at (R(n)->plans, r)));

    return ret;
}

/**
 * Create physical plan for Distinct operator (duplicate elimination).
 *
 * AFAIK, MonetDB only can do SortDistinct, i.e., sort its input, then
 * use the sort information to eliminate the duplicates.  The plans we
 * build are currently brute-force: generate all possible permutations
 * for orderings on the input relation, sort according to them, then
 * do duplicate elimination.
 */
static PFplanlist_t *
plan_distinct (const PFla_op_t *n)
{
    PFplanlist_t      *ret = new_planlist ();
    PFord_ordering_t   ord = PFordering ();
    PFord_set_t        perms;
    PFplanlist_t      *sorted;

    for (unsigned int i = 0; i < n->schema.count; i++)
        if (!PFprop_const (n->prop, n->schema.items[i].name))
            ord = PFord_refine (ord,
                                n->schema.items[i].name,
                                DIR_ASC /* will be ignored anyway */);

    /* PFord_permutations ignores the input direction and
       generates all permutations also for the directions. */
    perms = PFord_permutations (ord);

    /* consider all possible orderings (permutations) */
    for (unsigned int p = 0; p < PFord_set_count (perms); p++) {

        sorted = new_planlist ();

        /* consider all input plans and sort them */
        for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           PFord_set_at (perms, p)));

        /* throw away those that are too expensive */
        sorted = prune_plans (sorted);

        /* on each of the remaining, do a SortDistinct */
        for (unsigned int i = 0; i < PFarray_last (sorted); i++)
            add_plan (ret,
                      sort_distinct (*(plan_t **) PFarray_at (sorted, i),
                                     PFord_set_at (perms, p)));
    }

    return ret;
}

/**
 * Helper function to plan binary operators (arithmetic, comparison,
 * Boolean). If either operand column is known to be constant, plan
 * for a more specific implementation.
 */
static PFplanlist_t *
plan_binop (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    static PFpa_op_t * (*op_atom[]) (const PFpa_op_t *, const PFalg_att_t,
                                     const PFalg_att_t, const PFalg_atom_t)
        = {
              [la_num_add]      = PFpa_num_add_atom
            , [la_num_multiply] = PFpa_num_mult_atom
            , [la_num_eq]       = PFpa_eq_atom
            , [la_num_subtract] = PFpa_num_sub_atom
            , [la_num_divide]   = PFpa_num_div_atom
            , [la_num_modulo]   = PFpa_num_mod_atom
            , [la_num_gt]       = PFpa_gt_atom
            , [la_bool_and]     = PFpa_and_atom
            , [la_bool_or]      = PFpa_or_atom
        };

    static PFpa_op_t * (*op[]) (const PFpa_op_t *, const PFalg_att_t,
                                const PFalg_att_t, const PFalg_att_t)
        = {
              [la_num_add]      = PFpa_num_add
            , [la_num_multiply] = PFpa_num_mult
            , [la_num_eq]       = PFpa_eq
            , [la_num_subtract] = PFpa_num_sub
            , [la_num_divide]   = PFpa_num_div
            , [la_num_modulo]   = PFpa_num_mod
            , [la_num_gt]       = PFpa_gt
            , [la_bool_and]     = PFpa_and
            , [la_bool_or]      = PFpa_or
        };


    switch (n->kind) {

        case la_num_add:
        case la_num_multiply:
        case la_num_eq:

            assert (op_atom[n->kind]);

            /* consider NumAddConst if attribute att1 is known to be constant */
            if (PFprop_const (L(n)->prop, n->sem.binary.att1))
                for (unsigned int i = 0;
                        i < PFarray_last (L(n)->plans); i++)
                    add_plan (ret,
                        op_atom[n->kind] (
                            *(plan_t **) PFarray_at (L(n)->plans,i),
                            n->sem.binary.res,
                            n->sem.binary.att2,
                            PFprop_const_val (L(n)->prop,
                                              n->sem.binary.att1)));

            /* fall through */

        case la_num_subtract:
        case la_num_divide:
        case la_num_modulo:
        case la_num_gt:

            assert (op_atom[n->kind]);

            /* consider NumAddConst if attribute att2 is known to be constant */
            if (PFprop_const (L(n)->prop, n->sem.binary.att2))
                for (unsigned int i = 0;
                        i < PFarray_last (L(n)->plans); i++)
                    add_plan (ret,
                        op_atom[n->kind] (
                            *(plan_t **) PFarray_at (L(n)->plans,i),
                            n->sem.binary.res,
                            n->sem.binary.att1,
                            PFprop_const_val (L(n)->prop,
                                              n->sem.binary.att2)));

            /* fall through */

        default:

            assert (op[n->kind]);

            for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
                add_plan (ret,
                        op[n->kind] (
                            *(plan_t **) PFarray_at (L(n)->plans, i),
                            n->sem.binary.res,
                            n->sem.binary.att1,
                            n->sem.binary.att2));
    }

    return ret;
}

/**
 * Helper function to plan unary operators (numeric/Boolean negation)
 */
static PFplanlist_t *
plan_unary (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        switch (n->kind) {

            case la_num_neg:
                add_plan (ret,
                          num_neg (
                            *(plan_t **) PFarray_at (L(n)->plans, i),
                            n->sem.unary.res, n->sem.unary.att));
                break;

            case la_bool_not:
                add_plan (ret,
                          bool_not (
                            *(plan_t **) PFarray_at (L(n)->plans, i),
                            n->sem.unary.res, n->sem.unary.att));
                break;

            default:
                PFoops (OOPS_FATAL, "error in plan_unary");
        }

    return ret;
}

/**
 * Generate physical plan for logical aggregation operators
 * (avg, max, min, sum).
 */
static PFplanlist_t *
plan_aggr (PFpa_op_kind_t kind, const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n);
    assert (n->kind == la_avg || n->kind == la_max
            || n->kind == la_min || n->kind == la_sum);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in n */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  aggr (kind,
                        *(plan_t **) PFarray_at (L(n)->plans, i),
                        n->sem.aggr.res, n->sem.aggr.att, n->sem.aggr.part));

    return ret;
}

/**
 * Generate physical plan for the logical `Count' operator.
 *
 * Currently we only provide HashCount, which neither benefits from
 * any input ordering, nor does it guarantee any output ordering.
 *
 * FIXME:
 *   Is there a means to implement an order-aware Count operator
 *   in MonetDB?
 */
static PFplanlist_t *
plan_count (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_count);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in n */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  hash_count (
                      *(plan_t **) PFarray_at (L(n)->plans, i),
                      n->sem.aggr.res, n->sem.aggr.part));

    return ret;
}

/**
 * Generate physical plans for the logical `rownum' operator.
 *
 * The logical `rownum' operator is a rather complex operator.  In
 * our physical algebra, we make it more explicit and separate its
 * two processing steps:
 *
 *  1. Sort the input relation in a useful manner.  We can easily
 *     do this here with the help of ensure_ordering().
 *  2. On sorted input, row number generation is rather easy.  Still,
 *     we have two choices left:
 *      - `MergeRowNumber' can apply row numbers in a single scan,
 *        with no buffer requirements.  For this, we require a
 *        primary sorting on the rownum operator's partitioning
 *        argument. (We enforce that with ensure_ordering() here.)
 *      - `HashRowNumber' uses a hash table to do rownum's
 *        partitioning.  This requires only a small amount of
 *        memory, but has less requirements on the input ordering.
 *
 * We currently only consider the MergeRowNumber implementation.
 * HashRowNumber requires the input to be sorted according to the
 * rownum parameter within the partitions also given as the rownum
 * parameters.  We haven't implemented the full order framework
 * yet to describe this sort of ordering information, so we ignore
 * HashRowNumber for now.
 */
static PFplanlist_t *
plan_rownum (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();
    PFord_ordering_t ord_asc, ord_desc;

    assert (n); assert (n->kind == la_rownum);
    assert (L(n)); assert (L(n)->plans);

    /*
     * Build up the ordering that we require for MergeRowNumber
     */
    ord_asc  = PFordering ();
    ord_desc = PFordering ();

    /* the partitioning attribute must be the primary ordering */
    if (n->sem.rownum.part) {
        ord_asc  = PFord_refine (ord_asc, n->sem.rownum.part, DIR_ASC);
        ord_desc = PFord_refine (ord_desc, n->sem.rownum.part, DIR_DESC);
    }

    /* then we refine by all the attributes in the sortby parameter */
    for (unsigned int i = 0;
         i < PFord_count (n->sem.rownum.sortby);
         i++) {
        ord_asc = PFord_refine (
                      ord_asc,
                      PFord_order_col_at (n->sem.rownum.sortby, i),
                      PFord_order_dir_at (n->sem.rownum.sortby, i));
        ord_desc = PFord_refine (
                       ord_desc,
                       PFord_order_col_at (n->sem.rownum.sortby, i),
                       PFord_order_dir_at (n->sem.rownum.sortby, i));
    }

    /* ensure correct input ordering for MergeRowNumber */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++) {
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i), ord_asc));
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i), ord_desc));
    }

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a MergeRowNumber operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        for (unsigned int j = 0; j < PFarray_last (L(n)->plans); j++)
            add_plan (ret,
                      number (*(plan_t **) PFarray_at (sorted, i),
                              n->sem.rownum.attname, n->sem.rownum.part));

    return ret;
}

static PFplanlist_t *
plan_number (const PFla_op_t *n)
{
    PFplanlist_t *ret     = new_planlist ();
    plan_t        *cheapest_unordered = NULL;

    assert (n); assert (n->kind == la_number);
    assert (L(n)); assert (L(n)->plans);


    /* find the cheapest plan for our argument */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_unordered
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_unordered))
            cheapest_unordered
                = *(plan_t **) PFarray_at (L(n)->plans, i);

    add_plan (ret, number (cheapest_unordered,
                           n->sem.number.attname,
                           n->sem.number.part));

    return ret;
}

/**
 * `type' operator in the logical algebra just get a 1:1 mapping
 * into the physical type operator.
 */
static PFplanlist_t *
plan_type (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  type (*(plan_t **) PFarray_at (L(n)->plans, i),
                        n->sem.type.att, n->sem.type.ty, n->sem.type.res));

    return ret;
}

/**
 * `type_assert' operator in the logical algebra just get a 1:1 mapping
 * into the physical type operator.
 */
static PFplanlist_t *
plan_type_assert (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  type_assert (
                      *(plan_t **) PFarray_at (L(n)->plans, i),
                      n->sem.type.att, n->sem.type.ty));

    return ret;
}

/**
 * Generate physical plan for the logical `cast' operator.
 *
 * There's just one physical operator for `cast' that we
 * call from here.  The resulting plans will contain all
 * orderings of @a n.
 */
static PFplanlist_t *
plan_cast (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n); assert (n->kind == la_cast);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in R */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        add_plan (ret,
                  cast (*(plan_t **) PFarray_at (L(n)->plans, r),
                        n->sem.type.res,
                        n->sem.type.att,
                        n->sem.type.ty));

    return ret;
}

/**
 * Create physical plan for the staircase join operator (XPath location
 * steps). Uses helper functions plan_scj_XXX.
 */
static PFplanlist_t *
plan_scjoin (const PFla_op_t *n)
{
    PFplanlist_t *ret     = new_planlist ();
    PFpa_op_t* (*llscj) (const PFpa_op_t *, const PFpa_op_t *, const PFty_t,
                         const PFord_ordering_t, const PFord_ordering_t,
                         PFalg_att_t, PFalg_att_t) = NULL;

#ifndef NDEBUG
    /* ensure that input and output columns have the same name */
    assert (n->sem.scjoin.item == n->sem.scjoin.item_res);
#endif

    switch (n->sem.scjoin.axis) {
        case alg_anc:       llscj = PFpa_llscj_anc;         break;
        case alg_anc_s:     llscj = PFpa_llscj_anc_self;    break;
        case alg_attr:      llscj = PFpa_llscj_attr;        break;
        case alg_chld:      llscj = PFpa_llscj_child;       break;
        case alg_desc:      llscj = PFpa_llscj_desc;        break;
        case alg_desc_s:    llscj = PFpa_llscj_desc_self;   break;
        case alg_fol:       llscj = PFpa_llscj_foll;        break;
        case alg_fol_s:     llscj = PFpa_llscj_foll_sibl;   break;
        case alg_par:       llscj = PFpa_llscj_parent;      break;
        case alg_prec:      llscj = PFpa_llscj_prec;        break;
        case alg_prec_s:    llscj = PFpa_llscj_prec_sibl;   break;
        case alg_self:      assert (0);                     break;
    }

    /*
     * Consider loop-lifted staircase join variants.
     *
     * (For top-level XPath expressions, we'd not actually need
     * loop-lifted staircase joins, but could fall back to the
     * more efficient ``standard'' staircase join.
     */

    /*
     * Loop-lifted staircase join can handle two different orderings
     * for input and output: iter|item or item|iter. In the invocation
     * of the staircase join operators in MIL, this information is
     * encoded in a single integer value.
     */
    const PFord_ordering_t in[2]
        = { sortby (n->sem.scjoin.iter, n->sem.scjoin.item),
            sortby (n->sem.scjoin.item, n->sem.scjoin.iter) };
    const PFord_ordering_t out[2]
        = { sortby (n->sem.scjoin.iter, n->sem.scjoin.item),
            sortby (n->sem.scjoin.item, n->sem.scjoin.iter) };

    /* consider the two possible input orderings */
    for (unsigned short i = 0; i < 2; i++) {

        PFplanlist_t *ordered = new_planlist ();

        /* sort all plans according to this input ordering */
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++) {
            add_plans (ordered,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (R(n)->plans, j),
                           in[i]));
        }

        /* generate plans for each input and each output ordering */

        for (unsigned int k = 0; k < PFarray_last (ordered); k++)
            for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
                /* the evaluation of the attribute axis keeps the input order */
                if (n->sem.scjoin.axis == alg_attr)
                    add_plan (
                        ret,
                        llscj (*(plan_t **) PFarray_at (L(n)->plans, l),
                               *(plan_t **) PFarray_at (ordered, k),
                               n->sem.scjoin.ty,
                               in[i],
                               out[i],
                               n->sem.scjoin.iter,
                               n->sem.scjoin.item));
                else
                    for (unsigned short o = 0; o < 2; o++)
                        add_plan (
                            ret,
                            llscj (*(plan_t **) PFarray_at (L(n)->plans,
                                                            l),
                                   *(plan_t **) PFarray_at (ordered, k),
                                   n->sem.scjoin.ty,
                                   in[i],
                                   out[o],
                                   n->sem.scjoin.iter,
                                   n->sem.scjoin.item));
    }

    return ret;
}

/**
 * Generate physical plan for document table access.
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.  (Document table access is not
 *   precisely an operator with side-effects.  But it returns a
 *   live node set and we treat it analogously.)
 */
static PFplanlist_t *
plan_doc_tbl (const PFla_op_t *n)
{
    PFplanlist_t  *ret                = new_planlist ();
    PFplanlist_t  *ordered            = new_planlist ();
    plan_t        *cheapest_unordered = NULL;
    plan_t        *cheapest_ordered   = NULL;

#ifndef NDEBUG
    /* ensure that input and output columns have the same name */
    assert (n->sem.doc_tbl.item == n->sem.doc_tbl.item_res);
#endif

    /* find the cheapest plan for our argument */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_unordered
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_unordered))
            cheapest_unordered
                = *(plan_t **) PFarray_at (L(n)->plans, i);

    /* an ordering by `iter' is typically helpful -> sort all plans */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (n->sem.doc_tbl.iter)));

    for (unsigned int i = 0; i < PFarray_last (ordered); i++)
        if (!cheapest_ordered
            || costless (*(plan_t **) PFarray_at (ordered, i),
                         cheapest_ordered))
            cheapest_ordered = *(plan_t **) PFarray_at (ordered, i);

    /*
     * The plan `cheapest_unordered' is always the cheapest possible
     * plan.  However, an ordered plan could still be benefitial,
     * even if it is slightly more expensive on its own.  We assume
     * that the ordering makes up the cost of an unordered plan, if
     * its cost is at most 1.5 times the cost of an unordered plan.
     */
    if (cheapest_ordered->cost <= cheapest_unordered->cost * 1.5)
        add_plan (ret, doc_tbl (cheapest_ordered,
                                n->sem.doc_tbl.iter,
                                n->sem.doc_tbl.item));
    else
        add_plan (ret, doc_tbl (cheapest_unordered,
                                n->sem.doc_tbl.iter,
                                n->sem.doc_tbl.item));

    return ret;
}

/**
 * Create physical operator that allows the access to the
 * string content of the loaded documents
 */
static PFplanlist_t *
plan_doc_access (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();

    assert (n); assert (n->kind == la_doc_access);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* for each plan, generate a doc_access operator */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (L(n)->plans); j++)
            add_plan (ret,
                      doc_access (
                          *(plan_t **) PFarray_at (L(n)->plans, j),
                          *(plan_t **) PFarray_at (R(n)->plans, i),
                          n->sem.doc_access.res,
                          n->sem.doc_access.att,
                          n->sem.doc_access.doc_col));

    return ret;
}

/**
 * Generate physical plan for element constructor
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.
 */
static PFplanlist_t *
plan_element (const PFla_op_t *n)
{
    PFpa_op_t     *element;
    PFplanlist_t  *ret                = new_planlist ();
    PFplanlist_t  *ordered_qn         = new_planlist ();
    PFplanlist_t  *ordered_cont       = new_planlist ();
    plan_t        *cheapest_frag_plan = NULL;
    plan_t        *cheapest_qn_plan   = NULL;
    plan_t        *cheapest_cont_plan = NULL;
    PFalg_att_t    iter = n->sem.elem.iter_qn;
    PFalg_att_t    pos  = n->sem.elem.pos_val;
    PFalg_att_t    item = n->sem.elem.item_qn;

#ifndef NDEBUG
    /* ensure that matching columns (iter, pos, item) have the same name */
    assert (n->sem.elem.iter_qn == n->sem.elem.iter_val &&
            n->sem.elem.iter_qn == n->sem.elem.iter_res);
    assert (n->sem.elem.item_qn == n->sem.elem.item_val &&
            n->sem.elem.item_qn == n->sem.elem.item_res);
#endif

    /* find the cheapest plan for the fragments */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_frag_plan
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_frag_plan))
            cheapest_frag_plan = *(plan_t **) PFarray_at (L(n)->plans, i);

    /* find the cheapest plan for the qnames */
    for (unsigned int i = 0; i < PFarray_last (L(R(n))->plans); i++)
        add_plans (ordered_qn,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(R(n))->plans, i),
                       sortby (iter)));

    for (unsigned int i = 0; i < PFarray_last (ordered_qn); i++)
        if (!cheapest_qn_plan
            || costless (*(plan_t **) PFarray_at (ordered_qn, i),
                         cheapest_qn_plan))
            cheapest_qn_plan = *(plan_t **) PFarray_at (ordered_qn, i);

    /* find the cheapest plan for the content */
    for (unsigned int i = 0; i < PFarray_last (R(R(n))->plans); i++)
        add_plans (ordered_cont,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(R(n))->plans, i),
                       sortby (iter, pos)));

    for (unsigned int i = 0; i < PFarray_last (ordered_cont); i++)
        if (!cheapest_cont_plan
            || costless (*(plan_t **) PFarray_at (ordered_cont, i),
                         cheapest_cont_plan))
            cheapest_cont_plan = *(plan_t **) PFarray_at (ordered_cont, i);

    element = element (cheapest_frag_plan,
                       cheapest_qn_plan,
                       cheapest_cont_plan,
                       iter, pos, item);

    /* assure that element_tag also gets a property */
    R(element)->prop = PFprop ();

    add_plan (ret, element);

    return ret;
}

/**
 * Generate physical plan for attribute constructor
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.
 */
static PFplanlist_t *
plan_attribute (const PFla_op_t *n)
{
    PFplanlist_t  *ret           = new_planlist ();
    plan_t        *cheapest_plan = NULL;

    /* find the cheapest plan */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_plan
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_plan))
            cheapest_plan = *(plan_t **) PFarray_at (L(n)->plans, i);

    add_plan (ret, attribute (cheapest_plan,
                              n->sem.attr.qn,
                              n->sem.attr.val,
                              n->sem.attr.res));

    return ret;
}

/**
 * Generate physical plan for text constructor
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.
 */
static PFplanlist_t *
plan_textnode (const PFla_op_t *n)
{
    PFplanlist_t  *ret                = new_planlist ();
    plan_t        *cheapest_plan = NULL;

    /* find the cheapest plan */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_plan
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_plan))
            cheapest_plan = *(plan_t **) PFarray_at (L(n)->plans, i);

    add_plan (ret, textnode (cheapest_plan,
                             n->sem.textnode.res,
                             n->sem.textnode.item));

    return ret;
}

/**
 * `merge_adjacent' operator in the logical algebra just get a 1:1 mapping
 * into the physical merge_adjacent operator.
 */
static PFplanlist_t *
plan_merge_texts (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();
    plan_t       *cheapest_frag_plan = NULL;
    plan_t       *cheapest_sorted    = NULL;
    PFalg_att_t   iter = n->sem.merge_adjacent.iter_in;
    PFalg_att_t   pos  = n->sem.merge_adjacent.pos_in;
    PFalg_att_t   item = n->sem.merge_adjacent.item_in;

#ifndef NDEBUG
    /* ensure that matching columns (iter, pos, item) have the same name */
    assert (iter == n->sem.merge_adjacent.iter_res);
    assert (pos  == n->sem.merge_adjacent.pos_res);
    assert (item == n->sem.merge_adjacent.item_res);
#endif


    assert (n); assert (n->kind == la_merge_adjacent);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* find the cheapest plan for the fragments */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_frag_plan
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_frag_plan))
            cheapest_frag_plan = *(plan_t **) PFarray_at (L(n)->plans, i);

    /* The merge_adjacent_text_node operator requires
       its inputs to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (iter, pos)));

    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        if (!cheapest_sorted
            || costless (*(plan_t **) PFarray_at (sorted, i),
                         cheapest_sorted))
            cheapest_sorted = *(plan_t **) PFarray_at (sorted, i);

    /* generate a merge_adjacent_text_node operator for
       the single remaining plan */
    add_plan (ret,
              merge_adjacent (
                  cheapest_frag_plan,
                  cheapest_sorted,
                  iter, pos, item));
    return ret;
}

/**
 * `roots' operators in the logical algebra just get a 1:1 mapping
 * into the physical Roots operator.
 */
static PFplanlist_t *
plan_roots (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret, roots (*(plan_t **) PFarray_at (L(n)->plans, i)));

    return ret;
}

/**
 * `fragment' operators in the logical algebra just get a 1:1 mapping
 * into the physical Fragment operator.
 */
static PFplanlist_t *
plan_fragment (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  fragment (*(plan_t **) PFarray_at (L(n)->plans, i)));

    return ret;
}

/**
 * `frag_union' operators in the logical algebra just get a 1:1 mapping
 * into the physical FragUnion operator.
 */
static PFplanlist_t *
plan_frag_union (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      frag_union (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j)));

    return ret;
}

/**
 * `empty_frag' operators in the logical algebra just get a 1:1 mapping
 * into the physical EmptyFrag operator.
 */
static PFplanlist_t *
plan_empty_frag (const PFla_op_t *n __attribute__((unused)))
{
    PFplanlist_t *ret    = new_planlist ();

    (void) n; /* pacify picky compilers that do not understand
                 "__attribute__((unused))" */

    add_plan (ret, empty_frag ());

    return ret;
}

/**
 * `cond_err' operator in the logical algebra just get a 1:1 mapping
 * into the physical cond_err operator.
 */
static PFplanlist_t *
plan_cond_err (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
        add_plan (ret,
                  cond_err (
                      *(plan_t **) PFarray_at (L(n)->plans, i),
                      *(plan_t **) PFarray_at (R(n)->plans, j),
                      n->sem.err.att, n->sem.err.str));

    return ret;
}

/**
 * `concat' operator in the logical algebra just get a 1:1 mapping
 * into the physical concat operator.
 */
static PFplanlist_t *
plan_concat (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  fn_concat (*(plan_t **) PFarray_at (L(n)->plans, i),
                             n->sem.binary.res,
                             n->sem.binary.att1,
                             n->sem.binary.att2));

    return ret;
}

/**
 * `contains' operator in the logical algebra just get a 1:1 mapping
 * into the physical contains operator.
 */
static PFplanlist_t *
plan_contains (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  fn_contains (*(plan_t **) PFarray_at (L(n)->plans, i),
                               n->sem.binary.res,
                               n->sem.binary.att1,
                               n->sem.binary.att2));

    return ret;
}

/**
 * Create physical equivalent for the string_join operator
 * (concatenates sets of strings using a seperator for each set)
 *
 * The string_join operator expects its first argument (strings)
 * sorted by iter|pos and its second argument (seperators) sorted
 * by iter (as there is only one value per iteration).
 * The output is sorted by iter|item.
 */
static PFplanlist_t *
plan_string_join (const PFla_op_t *n)
{
    PFplanlist_t *ret       = new_planlist ();
    PFplanlist_t *sorted_n1 = new_planlist ();
    PFplanlist_t *sorted_n2 = new_planlist ();
    PFalg_att_t   iter = n->sem.string_join.iter;
    PFalg_att_t   pos  = n->sem.string_join.pos;
    PFalg_att_t   item = n->sem.string_join.item;

#ifndef NDEBUG
    /* ensure that matching columns (iter, pos, item) have the same name */
    assert (iter == n->sem.string_join.iter_sep &&
            iter == n->sem.string_join.iter_res);
    assert (item == n->sem.string_join.item_sep &&
            item == n->sem.string_join.item_res);
#endif

    assert (n); assert (n->kind == la_string_join);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* The string_join operator requires its inputs to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (sorted_n1,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter, pos)));

    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (sorted_n2,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (iter)));

    /* for each remaining plan, generate a string_join operator */
    for (unsigned int i = 0; i < PFarray_last (sorted_n1); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_n2); j++)
            add_plan (ret,
                      string_join (
                          *(plan_t **) PFarray_at (sorted_n1, i),
                          *(plan_t **) PFarray_at (sorted_n2, j),
                          iter, item));
    return ret;
}


/**
 * `recursion' operator(s) in the logical algebra just get a 1:1 mapping
 * into the physical recursion operator(s).
 *
 * @note
 *   For the same reason as the node constructors, we generate exactly
 *   one plan for this operator(s). More than one plan might lead to
 *   multiple recursion operators referencing a similar body. In MIL
 *   these recursions would be generated separately which would lead
 *   to different node constructions for the same constructor.
 */
static PFplanlist_t *
plan_recursion (PFla_op_t *n, PFord_ordering_t ord)
{
    PFplanlist_t *ret = new_planlist (),
                 *res,
                 *seed,
                 *recursion,
                 *base,
                 *arg,
                 *new_params,
                 *params = new_planlist ();
    plan_t       *cheapest_res_plan = NULL;
    plan_t       *cheapest_params   = NULL;
    PFla_op_t    *cur;

    assert (n->kind == la_rec_fix);

    /* get the first parameter */
    cur = L(n);
    /* start physical paramter list with the end of the list */
    add_plan (params, rec_nil ());

    /* assign logical properties to physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (params); plan++)
        (*(plan_t **) PFarray_at (params, plan))->prop = PFprop();

    /* collect the plans for all parameters
       The inputs to the recursion all have to fulfill the ordering
       specified by @a ord. */
    while (cur->kind != la_rec_nil) {
        assert (cur->kind == la_rec_param &&
                L(cur)->kind == la_rec_arg);

        seed       = new_planlist ();
        recursion  = new_planlist ();
        arg        = new_planlist ();
        new_params = new_planlist ();

        for (unsigned int i = 0; i < PFarray_last (LL(cur)->plans); i++)
            add_plans (seed,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (LL(cur)->plans, i),
                           ord));

        seed = prune_plans (seed);

        for (unsigned int i = 0; i < PFarray_last (LR(cur)->plans); i++)
            add_plans (recursion,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (LR(cur)->plans, i),
                           ord));

        recursion = prune_plans (recursion);

        base = L(cur)->sem.rec_arg.base->plans;

        /* create new arguments */
        for (unsigned int i = 0; i < PFarray_last (seed); i++)
            for (unsigned int j = 0; j < PFarray_last (recursion); j++)
                for (unsigned int k = 0; k < PFarray_last (base); k++)
                    add_plan (arg,
                              rec_arg (
                                  *(plan_t **) PFarray_at (seed, i),
                                  *(plan_t **) PFarray_at (recursion, j),
                                  *(plan_t **) PFarray_at (base, k)));

        /* assign logical properties to physical node as well */
        for (unsigned int plan = 0; plan < PFarray_last (arg); plan++)
            (*(plan_t **) PFarray_at (arg, plan))->prop = L(cur)->prop;

        /* create new paramters
           (new argument + list of paramters obtained so far) */
        for (unsigned int i = 0; i < PFarray_last (arg); i++)
            for (unsigned int j = 0; j < PFarray_last (params); j++)
                add_plan (new_params,
                          rec_param (
                              *(plan_t **) PFarray_at (arg, i),
                              *(plan_t **) PFarray_at (params, j)));

        /* assign logical properties to physical node as well */
        for (unsigned int plan = 0; plan < PFarray_last (new_params); plan++)
            (*(plan_t **) PFarray_at (new_params, plan))->prop = cur->prop;

        params = new_params;
        cur = R(cur);
    }

    /* prune all plans except one as we otherwise might end up with plans
       that might evaluate the recursion more than once */

    /* find the cheapest plan for the params */
    for (unsigned int i = 0; i < PFarray_last (params); i++)
        if (!cheapest_params
            || costless (*(plan_t **) PFarray_at (params, i),
                         cheapest_params))
            cheapest_params = *(plan_t **) PFarray_at (params, i);

    /* get the plans for the result of the recursion */
    res = R(n)->plans;

    /* find the cheapest plan for the result */
    for (unsigned int i = 0; i < PFarray_last (res); i++)
        if (!cheapest_res_plan
            || costless (*(plan_t **) PFarray_at (res, i),
                         cheapest_res_plan))
            cheapest_res_plan = *(plan_t **) PFarray_at (res, i);

    add_plan (ret, rec_fix (cheapest_params, cheapest_res_plan));

    return ret;
}

/**
 * `recursion' operator(s) in the logical algebra just get a 1:1 mapping
 * into the physical recursion operator(s).
 */
static void
plan_recursion_base (PFla_op_t *n, PFord_ordering_t ord)
{
    PFplanlist_t *plans = new_planlist ();

    add_plan (plans, rec_base (n->schema, ord));

    /* Keep the computed plans in the logical algebra node. */
    n->plans = plans;

    /* assign logical properties to physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (plans); plan++)
        (*(plan_t **) PFarray_at (plans, plan))->prop = n->prop;
}

/**
 * Worker for function clean_up_body_plans.
 *
 * return code semantics:
 *   0 -- do nothing
 *   1 -- prune plans
 *   2 -- constructor appeared - bail out
 *
 * The code is based on the precedence of the semantics.
 */
static int
clean_up_body_plans_worker (PFla_op_t *n, PFarray_t *bases)
{
    unsigned int i;
    int cur_code, code = 0 /* keep plans */;

    if (!n->plans)
        return 1 /* delete plans */;

    switch (n->kind)
    {
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_doc_tbl:
            for (i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++) {
                cur_code = clean_up_body_plans_worker (n->child[i], bases);
                /* collect the codes */
                code = code > cur_code ? code : cur_code;
            }
            if (code)
                return 2 /* constructor appeared - bail out */;
            break;
            
        /* do not delete the plans along the fragments */
        case la_frag_union:
            return 0 /* keep plans */;

        case la_rec_base:
            for (i = 0; i < PFarray_last (bases); i++)
                if (n == *(PFla_op_t **) PFarray_at (bases, i)) {
                    code = 1 /* delete plans */;
                    break;
                }
            break;

        default:
            for (i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++) {
                cur_code = clean_up_body_plans_worker (n->child[i], bases);
                /* collect the codes */
                code = code > cur_code ? code : cur_code;
            }
            break;
    }
    /* reset plan */
    if (code == 1)
        n->plans = NULL;

    return code;
}

/**
 * Delete all the plans annotated to the logical algebra nodes,
 * which can reach the base operators of the recursion.
 *
 * If one such operator is a constructor the clean up has to fail
 * (see also return value) as otherwise the recursion may refer
 * to a different constructor as the fragment information 
 * (the cheapest overall recursion plan vs. the last plan).
 */
static bool
clean_up_body_plans (PFla_op_t *n)
{
    PFarray_t *bases = PFarray (sizeof (PFla_op_t *));
    PFla_op_t *cur;
    bool code;

    assert (n->kind == la_rec_fix);

    cur = L(n);
    /* collect base operators */
    while (cur->kind != la_rec_nil) {
        assert (cur->kind == la_rec_param && L(cur)->kind == la_rec_arg);
        *(PFla_op_t **) PFarray_add (bases) = L(cur)->sem.rec_arg.base;
        cur = R(cur);
    }

    cur = L(n);
    /* clean up the plans */
    while (cur->kind != la_rec_nil) {
        code = clean_up_body_plans_worker (LR(cur), bases);
        
        /* if a constructor was detected we can stop processing now. */
        if (code == 2)
            return false; /* constructor appeared - bail out */

        assert (code);
        cur = R(cur);
    }
    code = clean_up_body_plans_worker (R(n), bases);
    assert (code);
    
    return code != 2;
}

/**
 * Return possible plans that guarantee the result from @a unordered
 * to be ordered according to @a required.
 *
 * We have three choices:
 *  - If a plan already fulfills @a required, we can just copy it
 *    to the result plan list.
 *  - If a plan partially fulfills @a required (i.e., it has a
 *    common ordering prefix with @a required), we can apply
 *    RefineSort that is much cheaper than our last-resort operator
 *    StdSort.
 *  - If we cannot benefit from any existing order, we apply
 *    StdSort.
 *
 * This function plans all those choices (if applicable).  The
 * subsequent prune_plans() call from plan_subexpression() will
 * remove inefficient plans.
 */
static PFplanlist_t *
ensure_ordering (const plan_t *unordered, PFord_ordering_t required)
{
    PFplanlist_t *ret           = new_planlist ();

    /* the best suitable ordering in `unordered' we found so far */
    PFord_ordering_t best_order = PFordering ();

    /*
     * If the output does not need to be sorted 
     * just return the unordered input.
     */
    if (PFord_implies (best_order, required)) {
        add_plan (ret, unordered);
        return ret;
    }
    
    /*
     * See if we can benefit from some existing sorting
     */
    for (unsigned int j = 0; j < PFord_set_count (unordered->orderings); j++) {

        /* the existing order we currently look at */
        PFord_ordering_t existing = PFord_set_at (unordered->orderings, j);
        PFord_ordering_t prefix   = PFordering ();

        /* collect the common prefix of required and existing ordering */
        for (unsigned int k = 0;
             k < PFord_count (required) && k < PFord_count (existing)
             && PFord_order_col_at (required, k) 
             == PFord_order_col_at (existing, k) 
             && PFord_order_dir_at (required, k)
             == PFord_order_dir_at (existing, k);
             k++)
            prefix = PFord_refine (prefix,
                                   PFord_order_col_at (existing, k),
                                   PFord_order_dir_at (existing, k));

        /*
         * If this prefix is better that what we already found
         * so far, remember it.
         */
        if (PFord_count (prefix) > PFord_count (best_order))
            best_order = prefix;

        /*
         * If this existing ordering already implies the required
         * ordering, we reached the optimum we can get and thus
         * stop searching for better prefixes.
         */
        if (PFord_implies (existing, required))
            break;
    }

    /*
     * If the input already provides the order we want,
     * just add the input to the plan list as it is.
     */
    if (PFord_implies (best_order, required))
        add_plan (ret, unordered);

    /*
     * The ``best ordering'' of `unordered' is now in `best_order'
     * (i.e., the longest common prefix of an existing ordering in
     * `unordered' and the required ordering.  We can use that to
     * apply RefineSort.
     */
    if (PFord_count (best_order) > 0)
        add_plan (ret, refine_sort (unordered, best_order, required));

    /* We can always apply StandardSort */
    add_plan (ret, std_sort (unordered, required));

    /* assign logical properties to the additional physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (ret); plan++)
        (*(plan_t **) PFarray_at (ret, plan))->prop = unordered->prop;

    return ret;
}

/**
 * Prune all those plans from a plan list that are superseded
 * by some other plan in the list.
 */
static PFplanlist_t *
prune_plans (PFplanlist_t *planlist)
{
    PFplanlist_t *ret = new_planlist ();

    /* Iterate over all plans. Add it to the return value if there's
     * not yet a better plan.
     */
    for (unsigned int i = 0; i < PFarray_last (planlist); i++) {

        /* assume there is no better plan */
        bool found_better = false;

        /* but maybe there is one already in our return list */
        for (unsigned int j = 0; j < PFarray_last (ret); j++)
            if (better_or_equal (*(plan_t **) PFarray_at (ret, j),
                                 *(plan_t **) PFarray_at (planlist, i))) {
                found_better = true;
                break;
            }

        /* if not, is there a better plan to follow in `planlist'? */
        if (!found_better)
            for (unsigned int j = i + 1; j < PFarray_last (planlist); j++)
                if (better_or_equal (*(plan_t **) PFarray_at (planlist, j),
                                     *(plan_t **) PFarray_at (planlist, i))) {
                    found_better = true;
                    break;
                }

        /*
         * If we know there is a better plan (either in the result
         * set, or lateron in the plan list), skip this plan.
         * Otherwise, add it to the result set.
         */
        if (!found_better)
            *(plan_t *) PFarray_add (ret)
                = *(plan_t *) PFarray_at (planlist, i);
    }

    return ret;
}

/**
 * Test is plan @a a is cheaper than plan @a b.
 *
 * We don't really have a suitable cost model (yet), so this is
 * actually just the integer comparison operator.
 */
static bool
costless (const plan_t *a, const plan_t *b)
{
    return (a->cost < b->cost);
}

/**
 * Test if @a a is a ``better'' plan than @a b:
 *
 *  - Plan @a a is cheaper than plan @a b.
 *  - Plan @a a provides all the orderings that @a b also provides.
 *    (Or even more orderings/more strict ones.)
 */
static bool
better_or_equal (const plan_t *a, const plan_t *b)
{
    /* If a does not cost less than b, return false. */
    if (costless (b, a))
        return false;

    /* a is only better than b if it satisfies *all* the orderings in b. */
    for (unsigned int i = 0; i < PFord_set_count (b->orderings); i++) {

        bool satisfied = false;

        for (unsigned int j = 0; j < PFord_set_count (a->orderings); j++) {
            /*
             * If the this order in a implies the current order in b,
             * we are satisfied.
             */
            if (PFord_implies (
                        PFord_set_at (a->orderings, j),
                        PFord_set_at (b->orderings, i))) {
                satisfied = true;
                break;
            }
        }

        /* If one ordering in b cannot be satisfied, a is not better than b. */
        if (! satisfied)
            return false;
    }

    return true;
}


/**
 * Compute all interesting plans that execute @a n and store them
 * in the @a plans field of @a n.
 *
 * Generates plans (through recursive invocation) bottom-up. Actual
 * planning is distributed to the @c plan_XXX function that
 * corresponds to the current node kind. Afterwards, we immediately
 * prune un-interesting plans (to reduce the search space as early
 * as possible).
 *
 * Logical algebra properties (column constantness) are propagated
 * to the corresponding physical plans for possible optimizations.
 *
 * As what we translate is actually a DAG (not a tree), the current
 * node @a n already might have been translated earlier. We thus
 * check if there is already a plan for this logical expression and
 * skip planning in that case.
 *
 * @param n subexpression to translate
 */
static void
plan_subexpression (PFla_op_t *n)
{
    PFplanlist_t *plans = NULL;

    /*
     * Skip if we already compiled this subexpression.
     * (Note that we actually compile a DAG, not a tree.)
     */
    if (n->plans)
        return;

    switch (n->kind) {
        /* process the following logical algebra nodes top-down */
        case la_element:
        case la_rec_fix:
            break;
        default:
            /* translate bottom-up (ensure that the fragment
               information is translated after the value part) */
            for (unsigned int i = PFLA_OP_MAXCHILD; i > 0; i--)
                if (n->child[i - 1])
                    plan_subexpression (n->child[i - 1]);
    }

    /* Compute possible plans. */
    switch (n->kind) {
        case la_serialize:      plans = plan_serialize (n);    break;

        case la_lit_tbl:        plans = plan_lit_tbl (n);      break;
        case la_empty_tbl:      plans = plan_empty_tbl (n);    break;
        case la_attach:         plans = plan_attach (n);       break;
        case la_cross:          plans = plan_cross (n);        break;
        case la_eqjoin:         plans = plan_eqjoin (n);       break;
        case la_semijoin:       plans = plan_semijoin (n);     break;
        case la_project:        plans = plan_project (n);      break;
        case la_select:         plans = plan_select (n);       break;
        case la_disjunion:      plans = plan_disjunion (n);    break;
        case la_intersect:      plans = plan_intersect (n);    break;
        case la_difference:     plans = plan_difference (n);   break;
        case la_distinct:       plans = plan_distinct (n);     break;

        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
                                plans = plan_binop (n);        break;

        case la_num_neg:
        case la_bool_not:
                                plans = plan_unary (n);        break;
        case la_avg:            plans = plan_aggr (pa_avg, n); break;
        case la_max:            plans = plan_aggr (pa_max, n); break;
        case la_min:            plans = plan_aggr (pa_min, n); break;
        case la_sum:            plans = plan_aggr (pa_sum, n); break;
        case la_count:          plans = plan_count (n);        break;

        case la_rownum:         plans = plan_rownum (n);       break;
        case la_number:         plans = plan_number (n);       break;
        case la_type:           plans = plan_type (n);         break;
        case la_type_assert:    plans = plan_type_assert (n);  break;
        case la_cast:           plans = plan_cast (n);         break;
     /* case la_seqty1:         */
     /* case la_all:            */

        case la_scjoin:         plans = plan_scjoin (n);       break;
        case la_doc_tbl:        plans = plan_doc_tbl (n);      break;
        case la_doc_access:     plans = plan_doc_access (n);   break;

        case la_element:
            plan_subexpression (L(n));
            assert (R(n)->kind == la_element_tag);
            plan_subexpression (L(R(n)));
            plan_subexpression (R(R(n)));
            plans = plan_element (n);
            break;
        case la_attribute:      plans = plan_attribute (n);    break;
        case la_textnode:       plans = plan_textnode (n);     break;
     /* case la_docnode:        */
     /* case la_comment:        */
     /* case la_processi:       */
        case la_merge_adjacent: plans = plan_merge_texts (n);  break;

        case la_roots:          plans = plan_roots (n);        break;
        case la_fragment:       plans = plan_fragment (n);     break;
        case la_frag_union:     plans = plan_frag_union (n);   break;
        case la_empty_frag:     plans = plan_empty_frag (n);   break;

        case la_cond_err:       plans = plan_cond_err (n);     break;

        case la_rec_fix:
        {
            PFla_op_t       *rec_arg, *cur;
            PFord_ordering_t ord;
            PFord_set_t      orderings = PFord_set ();
            PFplanlist_t    *rec_list = new_planlist ();
            plan_t          *cheapest_rec_plan = NULL;
            plans = new_planlist ();

            /* get the plans for all the seeds */
            cur = L(n);
            while (cur->kind != la_rec_nil) {
                assert (cur->kind == la_rec_param &&
                        L(cur)->kind == la_rec_arg);
                rec_arg = L(cur);
                plan_subexpression (L(rec_arg));
                cur = R(cur);
            }

            /* collect all the orderings we want to try */
            /* TODO: find the correct variable names for sortings and add them
               to the orderings */
            /*
            ord = PFordering ();
            ord = PFord_refine (PFord_refine (ord, correct_iter_name),
                                correct_item_name);
            orderings = PFord_set_add (orderings, ord);
            */
            ord = PFordering ();
            orderings = PFord_set_add (orderings, ord);

            /* generate plans for the body of the recursion
               for each ordering */
            for (unsigned int i = 0; i < PFord_set_count (orderings); i++) {
                ord = PFord_set_at (orderings, i);
                cur = L(n);

                /* create base operators with the correct ordering */
                while (cur->kind != la_rec_nil) {
                    assert (cur->kind == la_rec_param &&
                            L(cur)->kind == la_rec_arg);
                    rec_arg = L(cur);
                    plan_recursion_base (rec_arg->sem.rec_arg.base, ord);
                    cur = R(cur);
                }

                /* generate the plans for the body */
                cur = L(n);
                while (cur->kind != la_rec_nil) {
                    assert (cur->kind == la_rec_param &&
                            L(cur)->kind == la_rec_arg);
                    rec_arg = L(cur);
                    plan_subexpression (R(rec_arg));
                    cur = R(cur);
                }

                /* create plans for the result relation */
                plan_subexpression (R(n));

                /* put together all ingredients to form a physical
                   representation of the recursion (basically a 1:1
                   match) and add the generated plan to the list of
                   already collected plans */
                add_plans (rec_list, plan_recursion (n, ord));

                /* Reset all the plans in the body.
                   If a constructor appears in the body the clean up 
                   fails and we have to stop generating new plans and
                   use the current (first) plan */
                if (!clean_up_body_plans (n))
                    break;
            }

            /* For the same reason as the node constructors, we generate exactly
               one plan for this operator(s). More than one plan might lead to
               multiple recursion operators referencing a similar body. In MIL
               these recursions would be generated separately which would lead
               to different node constructions for the same constructor.  */

            /* find the cheapest plan for the recursion */
            for (unsigned int i = 0; i < PFarray_last (rec_list); i++)
                if (!cheapest_rec_plan
                    || costless (*(plan_t **) PFarray_at (rec_list, i),
                                 cheapest_rec_plan))
                    cheapest_rec_plan = *(plan_t **) PFarray_at (rec_list, i);

            /* add overall plan to the list */
            add_plan (plans, cheapest_rec_plan);
        } break;

        case la_concat:         plans = plan_concat (n);       break;
        case la_contains:       plans = plan_contains (n);     break;
        case la_string_join:    plans = plan_string_join (n);  break;

        default:
            PFoops (OOPS_FATAL,
                    "physical algebra equivalent for logical algebra "
                    "node kind %u not implemented, yet", n->kind);
    }

    assert (plans);
    assert (PFarray_last (plans) > 0);

    /* Prune un-interesting plans. */
    plans = prune_plans (plans);

    /* Keep the computed plans in the logical algebra node. */
    n->plans = plans;

    /* assign logical properties to physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (plans); plan++)
        (*(plan_t **) PFarray_at (plans, plan))->prop = n->prop;
}

/**
 * Compute physical algebra plan for logical algebra expression
 * tree rooted at @a root.
 *
 * For each sub-expression in the logical algebra tree, we compute
 * all ``interesting'' (possible) plans, ignoring those plans that
 * are superseded by a cheaper plan that guarantees the same (or
 * even stricter) ordering. From all the plans that execute
 * @a root, we pick the cheapest.
 *
 * While traversing the logical algebra tree bottom-up, we store
 * a list containing each ``interesting'' plan equivalent in
 * each logical algebra subexpression root node.
 *
 * @param root Root of the logical algebra tree to be compiled.
 * @return ``Best'' plan to execute the algebra expression rooted
 *         at @a root.
 */
PFpa_op_t *
PFplan (PFla_op_t *root)
{
    PFpa_op_t *ret = NULL;

    /* compute all interesting plans for root */
    plan_subexpression (root);

    /* Now pick the cheapest one. */
    assert (root->plans);
    assert (PFarray_last (root->plans) > 0);

    ret = *(plan_t **) PFarray_at (root->plans, 0);

    for (unsigned int i = 1; i < PFarray_last (root->plans); i++)
        if (costless (*(plan_t **) PFarray_at (root->plans, i), ret))
            ret = *(plan_t **) PFarray_at (root->plans, i);

    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
