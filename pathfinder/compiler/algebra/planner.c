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

/* always include pf_config.h first! */
#include "pf_config.h"
#include "pathfinder.h"

#include "oops.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

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

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define MAGIC_BOUNDARY 5000

/**
 * A ``plan'' is actually a physical algebra operator tree.
 */
typedef PFpa_op_t plan_t;

/* Function call kind indicator */
static PFalg_fun_call_t fun_call_kind;

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
    return PFarray (sizeof (plan_t *), 50);
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

    assert (n);
    assert (n->kind == la_serialize_seq);
    assert (L(n)->kind == la_side_effects);
    assert (LL(n)); assert (LL(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* The serialize operator requires its input to be properly sorted. */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (n->sem.ser_seq.pos)));

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a Serialize operator */
    for (unsigned int i = 0; i < PFarray_last (LL(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted); j++)
            add_plan (ret,
                      serialize (
                          *(plan_t **) PFarray_at (LL(n)->plans, i),
                          *(plan_t **) PFarray_at (sorted, j),
                          n->sem.ser_seq.item));

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
    PFplanlist_t    *ret     = new_planlist ();
    plan_t          *plan    = NULL;
    PFalg_collist_t *collist = PFalg_collist (n->schema.count);

    for (unsigned int i = 0; i < n->schema.count; i++)
        cladd (collist) = n->schema.items[i].name;

    /*
     * There is exactly this one plan.
     */
    plan = lit_tbl (collist, n->sem.lit_tbl.count, n->sem.lit_tbl.tuples);

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
                          n->sem.attach.res,
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
    bool           l_indep = true,
                   r_indep = true;

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

    /* check if the output is independent of the left side */
    for (unsigned int i = 0; i < L(n)->schema.count; i++)
        l_indep &= !PFprop_icol (n->prop, L(n)->schema.items[i].name);
    /* check if the output is independent of the right side */
    for (unsigned int i = 0; i < R(n)->schema.count; i++)
        r_indep &= !PFprop_icol (n->prop, R(n)->schema.items[i].name);
    
    /* add plans with dependent cross products */
    if (l_indep ||
        L(n)->kind == la_distinct ||
        L(n)->kind == la_rowid)
        for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
            for (unsigned int r = 0; r < PFarray_last (R(n)->plans); r++)
                add_plan (ret,
                          dep_cross (*(plan_t **) PFarray_at (L(n)->plans, l),
                                     *(plan_t **) PFarray_at (R(n)->plans, r)));

    if (r_indep ||
        R(n)->kind == la_distinct ||
        R(n)->kind == la_rowid)
        for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
            for (unsigned int r = 0; r < PFarray_last (R(n)->plans); r++)
                add_plan (ret,
                          dep_cross (*(plan_t **) PFarray_at (R(n)->plans, r),
                                     *(plan_t **) PFarray_at (L(n)->plans, l)));
    return ret;
}

/**
 * Worker for plan_eqjoin().
 */
static void
join_worker (PFplanlist_t *ret,
             PFalg_col_t col1, PFalg_col_t col2,
             const plan_t *a, const plan_t *b)
{
    /*
     * try ``standard'' EqJoin, which does not give us any
     * ordering guarantees.
     */
    add_plan (ret, eqjoin (col1, col2, a, b));

    /*
     * LeftJoin may be more expensive, but gives us some ordering
     * guarantees.
     */
    add_plan (ret, leftjoin (col2, col1, b, a));
    add_plan (ret, leftjoin (col1, col2, a, b));
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
            join_worker (ret, n->sem.eqjoin.col1, n->sem.eqjoin.col2,
                              *(plan_t **) PFarray_at (L(n)->plans, r),
                              *(plan_t **) PFarray_at (R(n)->plans, s));
        }

    /* generate a semijoin */
    if (L(n)->schema.count == 1 &&
        L(n)->kind == la_distinct) {
        PFalg_proj_t *proj = PFmalloc (n->schema.count * sizeof (PFalg_proj_t));

        /* create the above projection list */
        for (unsigned int i = 0; i < n->schema.count; i++) {
            PFalg_col_t cur = n->schema.items[i].name;
            if (cur != n->sem.eqjoin.col1)
                proj[i] = PFalg_proj (cur, cur);
            else
                proj[i] = PFalg_proj (cur, n->sem.eqjoin.col2);
        }
        
        for (unsigned int l = 0; l < PFarray_last (R(n)->plans); l++)
            for (unsigned int r = 0; r < PFarray_last (LL(n)->plans); r++)
                add_plan (ret,
                          project (
                              semijoin (
                                  n->sem.eqjoin.col2,
                                  n->sem.eqjoin.col1,
                                  *(plan_t **) PFarray_at (R(n)->plans, l),
                                  *(plan_t **) PFarray_at (LL(n)->plans, r)),
                              n->schema.count, proj));
    }
    
    /* generate a semijoin */
    if (R(n)->schema.count == 1 &&
        R(n)->kind == la_distinct) {
        PFalg_proj_t *proj = PFmalloc (n->schema.count * sizeof (PFalg_proj_t));

        /* create the above projection list */
        for (unsigned int i = 0; i < n->schema.count; i++) {
            PFalg_col_t cur = n->schema.items[i].name;
            if (cur != n->sem.eqjoin.col2)
                proj[i] = PFalg_proj (cur, cur);
            else
                proj[i] = PFalg_proj (cur, n->sem.eqjoin.col1);
        }
        
        for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
            for (unsigned int r = 0; r < PFarray_last (RL(n)->plans); r++)
                add_plan (ret,
                          project (
                              semijoin (n->sem.eqjoin.col1,
                                  n->sem.eqjoin.col2,
                                  *(plan_t **) PFarray_at (L(n)->plans, l),
                                  *(plan_t **) PFarray_at (RL(n)->plans, r)),
                              n->schema.count, proj));
    }
    
    return ret;
}

/**
 * Generate physical plans for semi-join.
 */
static PFplanlist_t *
plan_semijoin (const PFla_op_t *n)
{
    PFplanlist_t  *ret = new_planlist ();

    assert (n); assert (n->kind == la_semijoin);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
            add_plan (ret,
                      semijoin (n->sem.eqjoin.col1,
                                n->sem.eqjoin.col2,
                                *(plan_t **) PFarray_at (L(n)->plans, r),
                                *(plan_t **) PFarray_at (R(n)->plans, s)));
        }

    return ret;
}

/**
 * Generate physical plans for theta-join.
 */
static PFplanlist_t *
plan_thetajoin (const PFla_op_t *n)
{
    PFplanlist_t  *ret = new_planlist ();

    PFalg_simple_type_t cur_type;

    assert (n); assert (n->kind == la_thetajoin);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    /* combine each plan in R with each plan in S */
    for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
        for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
            add_plan (ret,
                      thetajoin (*(plan_t **) PFarray_at (L(n)->plans, r),
                                 *(plan_t **) PFarray_at (R(n)->plans, s),
                                 n->sem.thetajoin.count,
                                 n->sem.thetajoin.pred));
        }

    cur_type = PFprop_type_of (n, n->sem.thetajoin.pred[0].left);
    /* If we have only a single equi-join predicate we can also plan
       an equi-join in addition. */
    if (n->sem.thetajoin.count == 1 &&
        n->sem.thetajoin.pred[0].comp == alg_comp_eq &&
        monomorphic (cur_type) &&
        cur_type != aat_anode &&
        cur_type != aat_pnode)
        /* combine each plan in R with each plan in S */
        for (unsigned int r = 0; r < PFarray_last (L(n)->plans); r++)
            for (unsigned int s = 0; s < PFarray_last (R(n)->plans); s++) {
                join_worker (ret,
                             n->sem.thetajoin.pred[0].left,
                             n->sem.thetajoin.pred[0].right,
                             *(plan_t **) PFarray_at (L(n)->plans, r),
                             *(plan_t **) PFarray_at (R(n)->plans, s));

                /* right side is only used to check for matches */
                if ((*(plan_t **) PFarray_at (R(n)->plans, s))->schema.count == 1 &&
                    PFprop_set (n->prop)) {
                    PFalg_proj_t *proj = PFalg_proj_create (n->schema);
                    /* replace the right join column by the left one */
                    for (unsigned int i = 0; i < n->schema.count; i++)
                        if (proj[i].old == n->sem.thetajoin.pred[0].right)
                            proj[i].old = n->sem.thetajoin.pred[0].left;
                    add_plan (ret,
                              project (
                                  /* use semijoin to avoid result explosion */
                                  semijoin (n->sem.thetajoin.pred[0].left,
                                            n->sem.thetajoin.pred[0].right,
                                            *(plan_t **) PFarray_at (L(n)->plans, r),
                                            *(plan_t **) PFarray_at (R(n)->plans, s)),
                                  n->schema.count,
                                  proj));
                }
                /* left side is only used to check for matches */
                if ((*(plan_t **) PFarray_at (L(n)->plans, r))->schema.count == 1 &&
                    PFprop_set (n->prop)) {
                    PFalg_proj_t *proj = PFalg_proj_create (n->schema);
                    /* replace the left join column by the right one */
                    for (unsigned int i = 0; i < n->schema.count; i++)
                        if (proj[i].old == n->sem.thetajoin.pred[0].left)
                            proj[i].old = n->sem.thetajoin.pred[0].right;
                    add_plan (ret,
                              project (
                                  /* use semijoin to avoid result explosion */
                                  semijoin (n->sem.thetajoin.pred[0].right,
                                            n->sem.thetajoin.pred[0].left,
                                            *(plan_t **) PFarray_at (R(n)->plans, s),
                                            *(plan_t **) PFarray_at (L(n)->plans, r)),
                                  n->schema.count,
                                  proj));
                }
            }

    return ret;
}

/**
 * Generate physical plans for dependent
 * theta-join that additionally removes duplicates.
 */
static PFplanlist_t *
plan_dep_unique_thetajoin (const PFla_op_t *n)
{
    PFplanlist_t *ret     = new_planlist (),
                 *lsorted = new_planlist (),
                 *rsorted = new_planlist ();
    PFalg_proj_t  res_proj;

    PFalg_simple_type_t cur_type;

    /* check all conditions again */
    assert (n);     assert (n->kind     == la_distinct);
    assert (L(n));  assert (L(n)->kind  == la_project);
    assert (LL(n)); assert (LL(n)->kind == la_thetajoin);
    assert (L(LL(n))); assert (L(LL(n))->plans);
    assert (R(LL(n))); assert (R(LL(n))->plans);
    assert (L(n)->schema.count == 1);
    assert (LL(n)->sem.thetajoin.count == 2);
    assert (LL(n)->sem.thetajoin.pred[0].comp == alg_comp_eq);
    assert (LL(n)->sem.thetajoin.pred[1].comp != alg_comp_ne);
    assert (L(n)->sem.proj.items[0].old ==
            LL(n)->sem.thetajoin.pred[0].left ||
            L(n)->sem.proj.items[0].old ==
            LL(n)->sem.thetajoin.pred[0].right);

    /* check for nat type in the first predicate */
    if (PFprop_type_of (LL(n),
                        LL(n)->sem.thetajoin.pred[0].left) != aat_nat ||
        PFprop_type_of (LL(n),
                        LL(n)->sem.thetajoin.pred[0].right) != aat_nat)
        return ret;

    if ((cur_type = PFprop_type_of (LL(n),
                                    LL(n)->sem.thetajoin.pred[1].left)) !=
        PFprop_type_of (LL(n),
                        LL(n)->sem.thetajoin.pred[1].right) ||
        !monomorphic (cur_type) ||
        cur_type & aat_node)
        return ret;

    res_proj = PFalg_proj (L(n)->sem.proj.items[0].new,
                           LL(n)->sem.thetajoin.pred[0].left);

    /* make sure the left input is sorted by the left sort criterion */
    for (unsigned int i = 0; i < PFarray_last (L(LL(n))->plans); i++)
        add_plans (lsorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(LL(n))->plans, i),
                       sortby (LL(n)->sem.thetajoin.pred[0].left)));

    /* make sure the right input is sorted by the right sort criterion */
    for (unsigned int i = 0; i < PFarray_last (R(LL(n))->plans); i++)
        add_plans (rsorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(LL(n))->plans, i),
                       sortby (LL(n)->sem.thetajoin.pred[0].right)));

    /* combine each plan in R with each plan in S */
    for (unsigned int l = 0; l < PFarray_last (lsorted); l++)
        for (unsigned int r = 0; r < PFarray_last (rsorted); r++) {
            add_plan (ret,
                      /* add the renaming projection afterwards */
                      project (
                          unq1_tjoin (
                              /* from our compilation we know that
                                 the first predicate is the dependent
                                 one which is also used for removing
                                 duplicate tuples. */
                              LL(n)->sem.thetajoin.pred[1].comp,
                              LL(n)->sem.thetajoin.pred[1].left,
                              LL(n)->sem.thetajoin.pred[1].right,
                              LL(n)->sem.thetajoin.pred[0].left,
                              LL(n)->sem.thetajoin.pred[0].right,
                              *(plan_t **) PFarray_at (lsorted, l),
                              *(plan_t **) PFarray_at (rsorted, r)),
                          1,
                          &res_proj));
        }

    return ret;
}

/**
 * Worker that checks if a column provides nodes of a single 
 * fragment only.
 */
static bool
constant_fragment_info (PFla_op_t *n, PFalg_col_t col)
{
    /* find the origin of the column */
    PFla_op_t *op = PFprop_lineage (n->prop, col);

    if (!op)
        return false;

    /* step along the fragment information
       to check for a constant input to
       the doc_tbl operator */

    if ((op->kind == la_step ||
         op->kind == la_guide_step ||
         op->kind == la_step_join ||
         op->kind == la_guide_step_join) &&
        op->sem.step.item_res ==
        PFprop_lineage_col (n->prop, col) &&
        L(op)->kind == la_frag_union &&
        LL(op)->kind == la_empty_frag &&
        LR(op)->kind == la_fragment &&
        LRL(op)->kind == la_doc_tbl &&
        PFprop_const (LRL(op)->prop,
                      LRL(op)->sem.doc_tbl.col))
        return true;

    if (op->kind == la_doc_index_join &&
        op->sem.doc_join.item_res ==
        PFprop_lineage_col (n->prop, col) &&
        L(op)->kind == la_frag_union &&
        LL(op)->kind == la_empty_frag &&
        LR(op)->kind == la_fragment &&
        LRL(op)->kind == la_doc_tbl &&
        PFprop_const (LRL(op)->prop,
                      LRL(op)->sem.doc_tbl.col))
        return true;

    if (op->kind == la_doc_tbl &&
        PFprop_const (op->prop,
                      op->sem.doc_tbl.col))
        return true;

    return false;
}

/**
 * Generate physical plans for theta-join
 * that additionally removes duplicates.
 */
static PFplanlist_t *
plan_unique_thetajoin (const PFla_op_t *n)
{
    PFplanlist_t       *ret     = new_planlist (),
                       *lsorted = new_planlist (),
                       *rsorted = new_planlist ();
    PFalg_col_t         ldist, rdist;
    PFalg_simple_type_t ldist_ty, rdist_ty,
                        cur_type;

    /* check all conditions again */
    assert (n);     assert (n->kind     == la_distinct);
    assert (L(n));  assert (L(n)->kind  == la_project);
    assert (LL(n)); assert (LL(n)->kind == la_thetajoin);
    assert (L(LL(n))); assert (L(LL(n))->plans);
    assert (R(LL(n))); assert (R(LL(n))->plans);
    assert (L(n)->schema.count == 2);
    assert (LL(n)->sem.thetajoin.count == 1);
    assert ((PFprop_ocol (L(LL(n)), L(n)->sem.proj.items[0].old) &&
             PFprop_ocol (R(LL(n)), L(n)->sem.proj.items[1].old)) ^
            (PFprop_ocol (L(LL(n)), L(n)->sem.proj.items[1].old) &&
             PFprop_ocol (R(LL(n)), L(n)->sem.proj.items[0].old)));

    if (PFprop_ocol (L(LL(n)), L(n)->sem.proj.items[0].old)) {
        ldist = L(n)->sem.proj.items[0].old;
        rdist = L(n)->sem.proj.items[1].old;
    } else {
        ldist = L(n)->sem.proj.items[1].old;
        rdist = L(n)->sem.proj.items[0].old;
    }
    ldist_ty = PFprop_type_of (LL(n), ldist);
    rdist_ty = PFprop_type_of (LL(n), rdist);

    /* Check for either nat type or a pre node in the distinct check */
    if (ldist_ty != aat_nat &&
        (ldist_ty != aat_pnode ||
         /* We have to cope with a pre(oid) and frag(oid) column.
            This only works if we can be sure that frag(oid) is
            constant which means that the nodes have to come
            from a single document or a single collection. */
         !constant_fragment_info (LL(n), ldist)))
        return ret;
    if (rdist_ty != aat_nat &&
        (rdist_ty != aat_pnode ||
         /* We have to cope with a pre(oid) and frag(oid) column.
            This only works if we can be sure that frag(oid) is
            constant which means that the nodes have to come
            from a single document or a single collection. */
         !constant_fragment_info (LL(n), rdist)))
        return ret;

    if ((cur_type = PFprop_type_of (LL(n),
                                    LL(n)->sem.thetajoin.pred[0].left)) !=
        PFprop_type_of (LL(n),
                        LL(n)->sem.thetajoin.pred[0].right) ||
        !monomorphic (cur_type) ||
        cur_type & aat_node)
        return ret;

    /* make sure the left input is sorted by the left sort criterion */
    for (unsigned int i = 0; i < PFarray_last (L(LL(n))->plans); i++)
        add_plans (lsorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(LL(n))->plans, i),
                       sortby (ldist)));

    /* make sure the right input is sorted by the right sort criterion */
    for (unsigned int i = 0; i < PFarray_last (R(LL(n))->plans); i++)
        add_plans (rsorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(LL(n))->plans, i),
                       sortby (rdist)));

    /* combine each plan in R with each plan in S */
    for (unsigned int l = 0; l < PFarray_last (lsorted); l++)
        for (unsigned int r = 0; r < PFarray_last (rsorted); r++) {
            add_plan (ret,
                      /* add the renaming projection afterwards */
                      project (
                          unq2_tjoin (
                              /* from our compilation we know that
                                 the first predicate is the dependent
                                 one which is also used for removing
                                 duplicate tuples. */
                              LL(n)->sem.thetajoin.pred[0].comp,
                              LL(n)->sem.thetajoin.pred[0].left,
                              LL(n)->sem.thetajoin.pred[0].right,
                              ldist,
                              rdist,
                              *(plan_t **) PFarray_at (lsorted, l),
                              *(plan_t **) PFarray_at (rsorted, r)),
                          2,
                          L(n)->sem.proj.items));

            /* for an equi-join we can also plan with switched sides */
            if (LL(n)->sem.thetajoin.pred[0].comp == alg_comp_eq)
                add_plan (ret,
                          /* add the renaming projection afterwards */
                          project (
                              unq2_tjoin (
                                  /* from our compilation we know that
                                     the first predicate is the dependent
                                     one which is also used for removing
                                     duplicate tuples. */
                                  alg_comp_eq,
                                  LL(n)->sem.thetajoin.pred[0].right,
                                  LL(n)->sem.thetajoin.pred[0].left,
                                  rdist,
                                  ldist,
                                  *(plan_t **) PFarray_at (rsorted, r),
                                  *(plan_t **) PFarray_at (lsorted, l)),
                              2,
                              L(n)->sem.proj.items));
        }

    return ret;
}

/**
 * Generate physical plan for the logical `project' operator.
 *
 * There's just one physical operator for `project' that we
 * call from here.  The resulting plans will contain all
 * those orderings of @a n (or prefixes thereof), for which
 * we still have all columns left in the projection result.
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
    PFplanlist_t *ret  = new_planlist ();
    PFla_op_t    *op   = L(n);

    assert (n); assert (n->kind == la_select);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in L */
    for (unsigned int l = 0; l < PFarray_last (L(n)->plans); l++)
        add_plan (ret,
                  select_ (*(plan_t **) PFarray_at (L(n)->plans, l),
                           n->sem.select.col));

    if (op->kind == la_project &&
        L(op)->kind == la_num_eq &&
        LL(op)->kind == la_attach &&
        (L(op)->sem.binary.col1 == LL(op)->sem.attach.res ||
         L(op)->sem.binary.col2 == LL(op)->sem.attach.res)) {
        PFalg_col_t res  = LL(op)->sem.attach.res,
                    col1 = L(op)->sem.binary.col1,
                    col2 = L(op)->sem.binary.col2,
                    sel_col = n->sem.select.col;
        bool val_ref = false;

        /* normalize such that the atom is stored in col2 */
        if (res == col1) {
            col1 = col2;
            col2 = res;
        }

        for (unsigned int i = 0; i < op->sem.proj.count; i++)
            if (sel_col == op->sem.proj.items[i].new)
                sel_col = op->sem.proj.items[i].old;
            else if (op->sem.proj.items[i].old == col2)
                val_ref = true;

        /* no match -- bail out */
        if (sel_col != L(op)->sem.binary.res ||
            val_ref ||
            LL(op)->sem.attach.value.type == aat_qname)
            return ret;

        for (unsigned int l = 0; l < PFarray_last (L(LL(op))->plans); l++)
            add_plan (ret,
                      project (
                          attach (
                              val_select (
                                  *(plan_t **) PFarray_at (L(LL(op))->plans, l),
                                  col1,
                                  LL(op)->sem.attach.value),
                              L(op)->sem.binary.res,
                              PFalg_lit_bln (true)),
                          op->sem.proj.count,
                          op->sem.proj.items));
    }

    return ret;
}

/**
 * Generate physical plan for a single range selection
 * (function fn:subsequence in XQuery).
 */
static PFplanlist_t *
plan_subsequence (const PFla_op_t *n)
{
    PFalg_col_t   sel_col,
                  order_col,
                  unary_col;
    long long int low,
                  high;
    unsigned int  count   = n->schema.count;
    PFplanlist_t *ret     = new_planlist ();
    PFla_op_t    *op      = L(n);
    PFalg_proj_t *proj;

    assert (n); assert (n->kind == la_select);
    assert (L(n));

    sel_col = n->sem.select.col;

    if (PFprop_icol (n->prop, sel_col))
        return ret;

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    /* check first three operators: select-gt-attach */
    if (op->kind != la_num_gt ||
        sel_col != op->sem.binary.res ||
        L(op)->kind != la_attach ||
        L(op)->sem.attach.res != op->sem.binary.col1 ||
        PFprop_icol (op->prop, op->sem.binary.col1) ||
        PFprop_icol (op->prop, op->sem.binary.col2) ||
        L(op)->sem.attach.value.type != aat_int)
        return ret;

    order_col = op->sem.binary.col2;
    high      = L(op)->sem.attach.value.val.int_;
    op        = LL(op);

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    if (op->kind != la_select ||
        PFprop_icol (op->prop, op->sem.select.col))
        return ret;

    sel_col = op->sem.select.col;
    op      = L(op);

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    /* check for the next operators: select-not-gt-attach-cast */
    if (op->kind != la_bool_not ||
        sel_col != op->sem.unary.res ||
        PFprop_icol (op->prop, op->sem.unary.col))
        return ret;

    unary_col = op->sem.unary.col;
    op        = L(op);

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    if (op->kind != la_num_gt ||
        unary_col != op->sem.binary.res ||
        L(op)->kind != la_attach ||
        L(op)->sem.attach.res != op->sem.binary.col1 ||
        order_col != op->sem.binary.col2 ||
        PFprop_icol (op->prop, op->sem.binary.col1))
        return ret;

    low = L(op)->sem.attach.value.val.int_;
    op  = LL(op);

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    if (op->kind != la_cast ||
        op->sem.type.res != order_col ||
        PFprop_type_of (op, op->sem.type.col) != aat_nat)
        return ret;

    order_col = op->sem.type.col;
    op        = L(op);

    if (op->kind == la_project) {
        for (unsigned int i = 0; i < op->sem.proj.count; i++) {
            /* check for a non-problematic projection */
            if (op->sem.proj.items[i].new != op->sem.proj.items[i].new)
                return ret;
        }
        op = L(op);
    }

    /* and finally check for the row numbering operator
       that provides the position information */
    if (op->kind != la_rownum ||
        op->sem.sort.part ||
        op->sem.sort.res != order_col)
        return ret;

    /* Align the result schema to the expected schema.
       (Unused columns are linked to the result of the
       rownumber operator.) */
    proj = PFmalloc (count * sizeof (PFalg_proj_t));
    for (unsigned int i = 0; i < count; i++) {
        PFalg_col_t cur = n->schema.items[i].name;
        /* column used: keep the column */
        if (PFprop_icol (n->prop, cur))
            proj[i] = PFalg_proj (cur, cur);
        /* column unused: introduce a dummy column */
        else
            proj[i] = PFalg_proj (cur, op->sem.sort.res);
    }

    /* Adjust the offsets according to the slice behavior. slice() starts
       from 0 (not from 1 as rownum) and no negative offsets are allowed.
       slice() furthermore includes the upper boundary (=> high -= 2). */
    low  = (low < 1) ? 0 : low - 1;
    high = ((high < 1) ? 0 : high - 1) - 1;

    for (unsigned int l = 0; l < PFarray_last (op->plans); l++)
        add_plan (ret,
                  project (
                      slice (*(plan_t **) PFarray_at (op->plans, l), low, high),
                      count,
                      proj));
    return ret;
}

/**
 * Generate physical plan for the logical `positional select' operator.
 *
 * There's just one physical operator for `select' that we
 * call from here.  The resulting plans will contain all
 * orderings of @a n.
 */
static PFplanlist_t *
plan_pos_select (const PFla_op_t *n)
{
    PFalg_col_t      col,
                     num,
                     cast;
    PFplanlist_t    *ret           = new_planlist (),
                    *sorted        = new_planlist ();
    PFord_ordering_t ord_asc,
                     ord_desc,
                     ord_wo_part;
    bool             dir,
                     switch_order;
    unsigned int     count         = n->schema.count;
    PFalg_proj_t    *proj          = PFmalloc (count * sizeof (PFalg_proj_t));

    assert (n); assert (n->kind == la_pos_select);
    assert (L(n)); assert (L(n)->plans);

    /* Build up the ordering that we require for the positional predicate */
    ord_asc     = PFordering ();
    ord_desc    = PFordering ();
    ord_wo_part = PFordering ();

    /* the partitioning column must be the primary ordering */
    if (n->sem.pos_sel.part) {
        ord_asc  = PFord_refine (ord_asc, n->sem.pos_sel.part, DIR_ASC);
        ord_desc = PFord_refine (ord_desc, n->sem.pos_sel.part, DIR_DESC);
    }

    switch_order = n->sem.pos_sel.pos < 0;

    /* then we refine by all the columns in the sortby parameter */
    for (unsigned int i = 0;
         i < PFord_count (n->sem.pos_sel.sortby);
         i++) {
        col = PFord_order_col_at (n->sem.pos_sel.sortby, i);
        dir = PFord_order_dir_at (n->sem.pos_sel.sortby, i);
        dir = switch_order ? !dir : dir;

        ord_asc     = PFord_refine (ord_asc, col, dir);
        ord_desc    = PFord_refine (ord_desc, col, dir);
        ord_wo_part = PFord_refine (ord_wo_part, col, dir);
    }

    /* ensure correct input ordering for positional predicate */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++) {
        if (n->sem.pos_sel.part) {
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_asc));
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_desc));
        } else {
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_wo_part));
        }
    }

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* get ourselves a new column name */
    num  = PFcol_new (col_pos);
    cast = PFcol_new (col_pos);

    for (unsigned int i = 0; i < count; i++)
        proj[i] = PFalg_proj (n->schema.items[i].name,
                              n->schema.items[i].name);

    /* for each remaining plan, generate a MergeRowNumber
       and a value selection operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        add_plan (ret,
                  project (
                      val_select (
                          cast (
                              n->sem.pos_sel.part
                              ? mark_grp (*(plan_t **) PFarray_at (sorted, i),
                                          num,
                                          n->sem.pos_sel.part)
                              : mark (*(plan_t **) PFarray_at (sorted, i), num),
                              cast,
                              num,
                              aat_int),
                          cast,
                          PFalg_lit_int (
                              (switch_order ? -1 : 1) * n->sem.pos_sel.pos)),
                      count,
                      proj));

    return ret;
}

/**
 * Generate physical plans for disjoint union.
 *
 * Available implementations are MergeUnion and AppendUnion.
 * MergeUnion is order-aware.  It expects both input relations
 * to be in some given ordering, and will propagate that ordering
 * to its output.  For two equal values in the ordering column,
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
    unsigned int  r, s, i, j; 
    PFplanlist_t *ret = new_planlist ();

    /* Consider each combination of plans in R and S */
    for (r = 0; r < PFarray_last (L(n)->plans); r++)
        for (s = 0; s < PFarray_last (R(n)->plans); s++) {

            plan_t *R = *(plan_t **) PFarray_at (L(n)->plans, r);
            plan_t *S = *(plan_t **) PFarray_at (R(n)->plans, s);

            /*
             * unfortunately, we can only support MergeJoin with
             * one-column ascending orderings in our MIL translation
             * (and it even has to be monomorphic).
             *
             * To make use of this observation we do not collect all
             * matching input orderings (with PFord_intersect()) but
             * only do an intersection on the first order entry.
             * (This makes planing unions with a large number of orders
             *  a lot cheaper.)
             */
            PFord_set_t prefixes = PFord_set ();

            for (i = 0; i < PFord_set_count (R->orderings); i++) {
                PFord_ordering_t ri  = PFord_set_at (R->orderings, i);
                /* get the order prefix */
                PFalg_col_t      col = PFord_order_col_at (ri, 0);
                for (j = 0; j < PFord_set_count (S->orderings); j++) {
                    PFord_ordering_t sj = PFord_set_at (S->orderings, j);
                    
                    if (/* look for matching prefixes */
                        PFord_order_col_at (sj, 0) == col &&
                        /* make sure that merged_union is only called for
                           ascending orders */
                        PFord_order_dir_at (ri, 0) == DIR_ASC &&
                        PFord_order_dir_at (sj, 0) == DIR_ASC) {
                        PFalg_simple_type_t tyR, tyS;

                        tyR = PFprop_type_of_ (R->schema, col);
                        tyS = PFprop_type_of_ (S->schema, col);

                        if (tyR == tyS &&
                            (tyR == aat_nat ||
                             tyR == aat_int ||
                             tyR == aat_str ||
                             tyR == aat_dec ||
                             tyR == aat_dbl ||
                             tyR == aat_uA))
                            PFord_set_add (prefixes, sortby (col));
                        
                        break;
                    }
                }
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
 * Check if a column functionally depends on any other column
 * in the schema. */
static bool
dependent_col (const PFla_op_t *n, PFalg_col_t dependent)
{
    for (unsigned int i = 0; i < n->schema.count; i++) {
        /* Avoid problematic results in case of circular dependencies:
           If the column @a dependent functionally describes a column
           (before it appears as dependent column) we don't mark it 
           as dependent. */ 
        if (n->schema.items[i].name != dependent &&
            PFprop_fd (n->prop, dependent, n->schema.items[i].name))
            return false;
        if (n->schema.items[i].name != dependent &&
            PFprop_fd (n->prop, n->schema.items[i].name, dependent))
            return true;
    }
    return false;
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
        if (!PFprop_const (n->prop, n->schema.items[i].name) &&
            !dependent_col (n, n->schema.items[i].name))
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
        if (PFarray_last (ret) > MAGIC_BOUNDARY)
            break;
    }

    return ret;
}

/**
 * Generate physical plan for the logical generic function operator.
 */
static PFplanlist_t *
plan_fun_1to1 (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    assert (n); assert (n->kind == la_fun_1to1);
    assert (L(n)); assert (L(n)->plans);

    /* copy all plans */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  fun_1to1 (
                      *(plan_t **) PFarray_at (L(n)->plans, i),
                      n->sem.fun_1to1.kind,
                      n->sem.fun_1to1.res,
                      n->sem.fun_1to1.refs));

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

    static PFpa_op_t * (*op[]) (const PFpa_op_t *, const PFalg_col_t,
                                const PFalg_col_t, const PFalg_col_t)
        = {
              [la_num_eq]       = PFpa_eq
            , [la_num_gt]       = PFpa_gt
            , [la_bool_and]     = PFpa_and
            , [la_bool_or]      = PFpa_or
        };

    assert (op[n->kind]);

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                op[n->kind] (
                    *(plan_t **) PFarray_at (L(n)->plans, i),
                    n->sem.binary.res,
                    n->sem.binary.col1,
                    n->sem.binary.col2));

    return ret;
}

/**
 * Helper function to plan unary operators (Boolean negation)
 */
static PFplanlist_t *
plan_unary (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        switch (n->kind) {

            case la_bool_not:
                add_plan (ret,
                          bool_not (
                            *(plan_t **) PFarray_at (L(n)->plans, i),
                            n->sem.unary.res, n->sem.unary.col));
                break;

            default:
                PFoops (OOPS_FATAL, "error in plan_unary");
        }

    return ret;
}

/**
 * Generate physical plan for integer enumeration (op:to).
 */
static PFplanlist_t *
plan_to (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    assert (n);
    assert (n->kind == la_to);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in n */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  to (*(plan_t **) PFarray_at (L(n)->plans, i),
                      n->sem.binary.res,
                      n->sem.binary.col1,
                      n->sem.binary.col2));

    return ret;
}

/**
 * Generate physical plan for logical aggregation operators.
 */
static PFplanlist_t *
plan_aggr (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();

    assert (n);
    assert (L(n)); assert (L(n)->plans);

    /* consider each plan in n */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  aggr (*(plan_t **) PFarray_at (L(n)->plans, i),
                        n->sem.aggr.part,
                        n->sem.aggr.count,
                        n->sem.aggr.aggr));

    return ret;
}

/**
 * Generate physical plan for the logical `Count' operator
 * whose empty partitions are filled up with 0 values.
 */
static PFplanlist_t *
plan_count_ext (const PFla_op_t *n)
{
    PFplanlist_t  *ret  = new_planlist ();
    PFalg_col_t    part_col,
                   res_col,
                   new_part_col,
                   new_res_col,
                   loop_col;
    PFla_op_t     *op,
                  *count_op = NULL;
    PFalg_proj_t  *proj = PFmalloc (2 * sizeof (PFalg_proj_t));

    if (!n ||
        n->kind != la_disjunion ||
        n->schema.count != 2)
        return ret;

    op = L(n);

    if (op->kind == la_project &&
        L(op)->kind == la_aggr &&
        L(op)->sem.aggr.count == 1 &&
        L(op)->sem.aggr.aggr[0].kind == alg_aggr_count &&
        L(op)->sem.aggr.part) {
        part_col = L(op)->sem.aggr.part;
        res_col  = L(op)->sem.aggr.aggr[0].res;
        if (part_col == op->sem.proj.items[0].old &&
            res_col  == op->sem.proj.items[1].old) {
            new_part_col = op->sem.proj.items[0].new;
            new_res_col  = op->sem.proj.items[1].new;
        }
        else {
            assert (part_col == op->sem.proj.items[1].old &&
                    res_col  == op->sem.proj.items[0].old);
            new_part_col = op->sem.proj.items[1].new;
            new_res_col  = op->sem.proj.items[0].new;
        }
        count_op = L(op);
    }
    else if (op->kind == la_aggr &&
             op->sem.aggr.count == 1 &&
             op->sem.aggr.aggr[0].kind == alg_aggr_count &&
             op->sem.aggr.part) {
        part_col     = L(op)->sem.aggr.part;
        res_col      = L(op)->sem.aggr.aggr[0].res;
        new_part_col = L(op)->sem.aggr.part;
        new_res_col  = L(op)->sem.aggr.aggr[0].res;
        count_op = op;
    }
    else
        return ret;

    op = R(n);
    loop_col = new_part_col;

    if (PFprop_type_of (n, new_part_col) != aat_nat ||
        !PFprop_const (op->prop, new_res_col) ||
        PFprop_const_val (op->prop, new_res_col).type != aat_int ||
        PFprop_const_val (op->prop, new_res_col).val.int_ != 0 ||
        op->sem.proj.items[0].old == op->sem.proj.items[1].old)
        return ret;

    if (op->kind == la_project) {
        if (loop_col == op->sem.proj.items[0].new)
            loop_col = op->sem.proj.items[0].old;
        else
            loop_col = op->sem.proj.items[1].old;
        op = L(op);
    }

    if (op->kind != la_attach ||
        op->sem.attach.res == loop_col)
        return ret;

    op = L(op);

    if (op->kind != la_difference ||
        R(op)->kind != la_project ||
        RL(op)->kind != la_aggr ||
        count_op != RL(op) ||
        op->schema.count != 1 ||
        R(op)->sem.proj.items[0].old != part_col)
        return ret;

    assert (L(count_op)->plans && L(op)->plans);

    proj[0].new = new_part_col;
    proj[0].old =     part_col;
    proj[1].new = new_res_col;
    proj[1].old =     res_col;

    /* consider each plan in L */
    for (unsigned int l = 0; l < PFarray_last (L(count_op)->plans); l++)
        /* and each plan in R */
        for (unsigned int r = 0; r < PFarray_last (L(op)->plans); r++)
            add_plan (ret,
                      project (
                          ecount (
                              *(plan_t **) PFarray_at (L(count_op)->plans, l),
                              *(plan_t **) PFarray_at (L(op)->plans, r),
                              res_col,
                              part_col,
                              loop_col),
                          2,
                          proj));

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
    PFord_ordering_t ord_asc, ord_desc, ord_wo_part;

    assert (n); assert (n->kind == la_rownum);
    assert (L(n)); assert (L(n)->plans);

    /*
     * Build up the ordering that we require for MergeRowNumber
     */
    ord_asc     = PFordering ();
    ord_desc    = PFordering ();
    ord_wo_part = PFordering ();

    /* the partitioning column must be the primary ordering */
    if (n->sem.sort.part) {
        ord_asc  = PFord_refine (ord_asc, n->sem.sort.part, DIR_ASC);
        ord_desc = PFord_refine (ord_desc, n->sem.sort.part, DIR_DESC);
    }

    /* then we refine by all the columns in the sortby parameter */
    for (unsigned int i = 0;
         i < PFord_count (n->sem.sort.sortby);
         i++) {
        ord_asc     = PFord_refine (
                          ord_asc,
                          PFord_order_col_at (n->sem.sort.sortby, i),
                          PFord_order_dir_at (n->sem.sort.sortby, i));
        ord_desc    = PFord_refine (
                          ord_desc,
                          PFord_order_col_at (n->sem.sort.sortby, i),
                          PFord_order_dir_at (n->sem.sort.sortby, i));
        ord_wo_part = PFord_refine (
                          ord_wo_part,
                          PFord_order_col_at (n->sem.sort.sortby, i),
                          PFord_order_dir_at (n->sem.sort.sortby, i));
    }

    /* ensure correct input ordering for MergeRowNumber */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++) {
        if (n->sem.pos_sel.part) {
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_asc));
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_desc));
        } else {
            add_plans (sorted,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           ord_wo_part));
        }
    }

    /* throw out those plans that are too expensive */
    sorted = prune_plans (sorted);

    /* for each remaining plan, generate a MergeRowNumber operator */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        add_plan (ret,
                  n->sem.sort.part
                  ? mark_grp (*(plan_t **) PFarray_at (sorted, i),
                              n->sem.sort.res,
                              n->sem.sort.part)
                  : mark (*(plan_t **) PFarray_at (sorted, i),
                          n->sem.sort.res));

    return ret;
}

static PFplanlist_t *
plan_rank (const PFla_op_t *n)
{
    PFplanlist_t *ret    = new_planlist ();
    PFplanlist_t *sorted = new_planlist ();

    assert (n); assert (n->kind == la_rank || n->kind == la_rowrank);
    assert (L(n)); assert (L(n)->plans);
    assert (!n->sem.sort.part);

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       n->sem.sort.sortby));

    for (unsigned int i = 0; i < PFarray_last (sorted); i++)
        add_plan (ret,
                  rank (*(plan_t **) PFarray_at (sorted, i),
                        n->sem.sort.res, n->sem.sort.sortby));
    return ret;
}

static PFplanlist_t *
plan_rowid (const PFla_op_t *n)
{
    PFplanlist_t *ret     = new_planlist ();
    plan_t        *cheapest_unordered = NULL;

    assert (n); assert (n->kind == la_rowid);
    assert (L(n)); assert (L(n)->plans);


    /* find the cheapest plan for our argument */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_unordered
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_unordered))
            cheapest_unordered
                = *(plan_t **) PFarray_at (L(n)->plans, i);

    add_plan (ret, mark (cheapest_unordered, n->sem.rowid.res));

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
                        n->sem.type.col, n->sem.type.ty, n->sem.type.res));

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
                      n->sem.type.col, n->sem.type.ty));

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
                        n->sem.type.col,
                        n->sem.type.ty));

    return ret;
}

/**
 * Create physical plan for the path step operator (XPath location
 * steps).
 */
static PFplanlist_t *
plan_step (const PFla_op_t *n)
{
    PFplanlist_t *ret  = new_planlist ();
    PFalg_proj_t *proj = PFmalloc (2 * sizeof (PFalg_proj_t));

    proj[0] = PFalg_proj (n->sem.step.iter, n->sem.step.iter);
    proj[1] = PFalg_proj (n->sem.step.item_res, n->sem.step.item);

    /*
     * Loop-lifted staircase join can handle two different orderings
     * for input and output: iter|item or item|iter. In the invocation
     * of the staircase join operators in MIL, this information is
     * encoded in a single integer value.
     */
    const PFord_ordering_t in[2]
        = { sortby (n->sem.step.iter, n->sem.step.item),
            sortby (n->sem.step.item, n->sem.step.iter) };
    const PFord_ordering_t out[2]
        = { sortby (n->sem.step.iter, n->sem.step.item),
            sortby (n->sem.step.item, n->sem.step.iter) };

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
            for (unsigned short o = 0; o < 2; o++)
                add_plan (
                    ret,
                    project (
                        llscjoin (
                            *(plan_t **) PFarray_at (ordered, k),
                            n->sem.step.spec,
                            in[i],
                            out[o],
                            n->sem.step.iter,
                            n->sem.step.item),
                        2, proj));
    }

    return ret;
}

/**
 * Create physical plan for the path step join operator
 * (XPath location steps with duplicates).
 */
static PFplanlist_t *
plan_step_join (const PFla_op_t *n)
{
    PFplanlist_t *ret  = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        /* result in both original and item order */
        for (unsigned short o = 0; o < 2; o++)
            add_plan (ret,
                      llscjoin_dup (
                          *(plan_t **) PFarray_at (R(n)->plans, i),
                          n->sem.step.spec,
                          o ? true : false,
                          n->sem.step.item_res,
                          n->sem.step.item));

    return ret;
}

/**
 * Create physical plan for the path step operator
 * based on step join operator as input.
 *
 * As no iter information is available a constant iter
 * is added.
 */
static PFplanlist_t *
plan_step_join_to_step (const PFla_op_t *n)
{
    PFalg_col_t   item_out  = n->sem.proj.items[0].new,
#ifndef NDEBUG
                  item_res  = n->sem.proj.items[0].old,
#endif
                  item,
                  iter;
    PFalg_proj_t *proj_out  = PFmalloc (sizeof (PFalg_proj_t)),
                 *proj_in   = PFmalloc (sizeof (PFalg_proj_t));
    PFplanlist_t *ret       = new_planlist (),
                 *sorted    = new_planlist ();
    PFpa_op_t    *plan;
    
    /* some assertions */
    assert (n); assert (n->kind == la_project);
    assert (L(n)); assert (L(n)->kind == la_step_join);
    assert (LR(n)->plans);

    assert (n->schema.count == 1);
    assert (L(n)->kind == la_step_join);
    assert (item_res == L(n)->sem.step.item_res);
    assert (PFprop_key (L(n)->prop, L(n)->sem.step.item_res) ||
            PFprop_set (L(n)->prop));

    /* find a free iter value */
    item      = L(n)->sem.step.item;
    iter      = PFcol_new (col_iter);
    
    proj_out[0].new = item_out;
    proj_out[0].old = item;
    proj_in[0].new  = item;
    proj_in[0].old  = item;
    
    for (unsigned int i = 0; i < PFarray_last (LR(n)->plans); i++)
        add_plans (sorted,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (LR(n)->plans, i),
                       sortby (item)));

    /* create the translation for every input plan */
    for (unsigned int i = 0; i < PFarray_last (sorted); i++) {
        plan = *(plan_t **) PFarray_at (sorted, i);
        /* add plan in item|iter output order */
        add_plan (ret,
                  project (
                      llscjoin (
                          attach (
                              project (plan, 1, proj_in),
                              iter,
                              lit_nat (1)),
                          L(n)->sem.step.spec,
                          sortby (item, iter),
                          sortby (item, iter),
                          iter,
                          item),
                      1, proj_out));
    }

    return ret;
}

/**
 * Create physical plan for the path step operator
 * based on step join operator as input.
 *
 * only the iter column is needed as output.
 */
static PFplanlist_t *
plan_step_join_to_step_check (const PFla_op_t *n)
{
    PFalg_col_t   iter_out  = n->sem.proj.items[0].new,
                  iter      = n->sem.proj.items[0].old,
                  item;
    PFalg_proj_t *proj_out  = PFmalloc (sizeof (PFalg_proj_t)),
                 *proj_in   = PFmalloc (2 * sizeof (PFalg_proj_t));
    PFplanlist_t *ret       = new_planlist ();
    PFpa_op_t    *plan;
    
    /* some assertions */
    assert (n); assert (n->kind == la_project);
    assert (L(n)); assert (L(n)->kind == la_step_join);
    assert (LR(n)->plans);

    assert (n->schema.count == 1);
    assert (L(n)->kind == la_step_join);
    assert (PFprop_type_of (n, n->sem.proj.items[0].new) == aat_nat);
    assert (PFprop_key (L(n)->prop, L(n)->sem.step.item_res) ||
            PFprop_set (L(n)->prop));

    item     = L(n)->sem.step.item;
    
    proj_out[0].new = iter_out;
    proj_out[0].old = iter;
    proj_in[0].new  = iter;
    proj_in[0].old  = iter;
    proj_in[1].new  = item;
    proj_in[1].old  = item;
    
    /*
     * Loop-lifted staircase join can handle two different orderings
     * for input and output: iter|item or item|iter. In the invocation
     * of the staircase join operators in MIL, this information is
     * encoded in a single integer value.
     */
    const PFord_ordering_t in[2]
        = { sortby (iter, item), sortby (item, iter) };
    const PFord_ordering_t out[2]
        = { sortby (iter, item), sortby (item, iter) };

    /* consider the two possible input orderings */
    for (unsigned short i = 0; i < 2; i++) {

        PFplanlist_t *ordered = new_planlist ();

        /* sort all plans according to this input ordering */
        for (unsigned int j = 0; j < PFarray_last (LR(n)->plans); j++) {
            add_plans (ordered,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (LR(n)->plans, j),
                           in[i]));
        }

        /* generate plans for each input and each output ordering */

        for (unsigned int k = 0; k < PFarray_last (ordered); k++)
            for (unsigned short o = 0; o < 2; o++) {
                plan = *(plan_t **) PFarray_at (ordered, k);
                add_plan (ret,
                          project (
                              llscjoin (
                                  project (plan, 2, proj_in),
                                  L(n)->sem.step.spec,
                                  in[i],
                                  out[o],
                                  iter,
                                  item),
                              1, proj_out));
            }
    }

    return ret;
}

/**
 * Create physical plan for the path step operator
 * based on step join operator as input.
 *
 * Two-column scenario where an iter_nat|item_node relation
 * is fed in.
 */
static PFplanlist_t *
plan_step_join_to_llstep (const PFla_op_t *n)
{
    PFalg_col_t   item_out,
                  item_res,
                  item,
                  iter_out,
                  iter;
    PFalg_proj_t *proj_in   = PFmalloc (2 * sizeof (PFalg_proj_t));
    PFplanlist_t *ret       = new_planlist ();
    PFpa_op_t    *plan;
    
    /* some assertions */
    assert (n); assert (n->kind == la_project);
    assert (L(n)); assert (L(n)->kind == la_step_join);
    assert (LR(n)->plans);

    assert (n->schema.count == 2);
    assert (L(n)->kind == la_step_join);

    item_res = L(n)->sem.step.item_res;
    item     = L(n)->sem.step.item;
    
    if (n->sem.proj.items[0].old == item_res &&
        PFprop_type_of (n, n->sem.proj.items[1].new) == aat_nat) {
        item_out = n->sem.proj.items[0].new;
        iter_out = n->sem.proj.items[1].new;
        iter     = n->sem.proj.items[1].old;
    }
    else if (n->sem.proj.items[1].old == item_res &&
             PFprop_type_of (n, n->sem.proj.items[0].new) == aat_nat) {
        item_out = n->sem.proj.items[1].new;
        iter_out = n->sem.proj.items[0].new;
        iter     = n->sem.proj.items[0].old;
    }
    else
        return ret;

    if (!PFprop_key (L(n)->prop, L(n)->sem.step.item_res) &&
        !PFprop_set (L(n)->prop) &&
        !PFprop_key_right (L(n)->prop, iter))
        return ret;
        
    proj_in[0].new = iter_out;
    proj_in[0].old = iter;
    proj_in[1].new = item_out;
    proj_in[1].old = item;
    
    /*
     * Loop-lifted staircase join can handle two different orderings
     * for input and output: iter|item or item|iter. In the invocation
     * of the staircase join operators in MIL, this information is
     * encoded in a single integer value.
     */
    const PFord_ordering_t in[2]
        = { sortby (iter_out, item_out), sortby (item_out, iter_out) };
    const PFord_ordering_t out[2]
        = { sortby (iter_out, item_out), sortby (item_out, iter_out) };

    /* consider the two possible input orderings */
    for (unsigned short i = 0; i < 2; i++) {

        PFplanlist_t *ordered = new_planlist ();

        /* sort all plans according to this input ordering */
        for (unsigned int j = 0; j < PFarray_last (LR(n)->plans); j++) {
            add_plans (ordered,
                       ensure_ordering (
                           project (*(plan_t **) PFarray_at (LR(n)->plans, j),
                                    2,
                                    proj_in),
                           in[i]));
        }

        /* generate plans for each input and each output ordering */

        for (unsigned int k = 0; k < PFarray_last (ordered); k++)
            for (unsigned short o = 0; o < 2; o++) {
                plan = *(plan_t **) PFarray_at (ordered, k);
                add_plan (ret,
                          llscjoin (
                              plan,
                              L(n)->sem.step.spec,
                              in[i],
                              out[o],
                              iter_out,
                              item_out));
            }
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
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plan (ret,
                  doc_tbl (*(plan_t **) PFarray_at (L(n)->plans, i),
                           n->sem.doc_tbl.res, n->sem.doc_tbl.col,
                           n->sem.doc_tbl.kind));

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
        add_plan (ret,
                  doc_access (
                      *(plan_t **) PFarray_at (R(n)->plans, i),
                      n->sem.doc_access.res,
                      n->sem.doc_access.col,
                      n->sem.doc_access.doc_col));

    return ret;
}

/**
 * Generate physical plan for twig constructor
 *
 * @note
 *   Following the ideas in @ref live_nodes, we generate exactly
 *   one plan for this operator.
 */
static PFplanlist_t *
plan_twig (const PFla_op_t *n)
{
    PFplanlist_t  *ret           = new_planlist ();
    plan_t        *cheapest_plan = NULL;
    PFalg_col_t    iter          = n->sem.iter_item.iter;
    PFalg_col_t    item          = n->sem.iter_item.item;

    /* find the cheapest plan */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        if (!cheapest_plan
            || costless (*(plan_t **) PFarray_at (L(n)->plans, i),
                         cheapest_plan))
            cheapest_plan = *(plan_t **) PFarray_at (L(n)->plans, i);

    add_plan (ret, twig (cheapest_plan, iter, item));

    return ret;
}

/**
 * Generate physical plan for twig sequence constructor
 */
static PFplanlist_t *
plan_fcns (const PFla_op_t *n)
{
    PFplanlist_t  *ret = new_planlist ();

    /* for each plan combination , generate a constructor sequence */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      fcns (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j)));

    return ret;
}

/**
 * Generate physical plan for docnode constructor
 */
static PFplanlist_t *
plan_docnode (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.docnode.iter;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      docnode (
                          *(plan_t **) PFarray_at (ordered_in, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          iter));

    return ret;
}

/**
 * Generate physical plan for element constructor
 */
static PFplanlist_t *
plan_element (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.iter_item.iter;
    PFalg_col_t    item       = n->sem.iter_item.item;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      element (
                          *(plan_t **) PFarray_at (ordered_in, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          iter, item));

    return ret;
}

/**
 * Generate physical plan for attribute constructor
 */
static PFplanlist_t *
plan_attribute (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.iter_item1_item2.iter;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        add_plan (ret,
                  attribute (
                      *(plan_t **) PFarray_at (ordered_in, i),
                      iter,
                      n->sem.iter_item1_item2.item1,
                      n->sem.iter_item1_item2.item2));

    return ret;
}

/**
 * Generate physical plan for textnode constructor
 */
static PFplanlist_t *
plan_textnode (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.iter_item.iter;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        add_plan (ret,
                  textnode (
                      *(plan_t **) PFarray_at (ordered_in, i),
                      iter,
                      n->sem.iter_item.item));

    return ret;
}

/**
 * Generate physical plan for comment constructor
 */
static PFplanlist_t *
plan_comment (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.iter_item.iter;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        add_plan (ret,
                  comment (
                      *(plan_t **) PFarray_at (ordered_in, i),
                      iter,
                      n->sem.iter_item.item));

    return ret;
}

/**
 * Generate physical plan for processi constructor
 */
static PFplanlist_t *
plan_processi (const PFla_op_t *n)
{
    PFplanlist_t  *ret        = new_planlist ();
    PFplanlist_t  *ordered_in = new_planlist ();
    PFalg_col_t    iter       = n->sem.iter_item1_item2.iter;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter)));

    /* for each plan, generate a constructor */
    for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
        add_plan (ret,
                  processi (
                      *(plan_t **) PFarray_at (ordered_in, i),
                      iter,
                      n->sem.iter_item1_item2.item1,
                      n->sem.iter_item1_item2.item2));

    return ret;
}

/**
 * Generate physical plan for content constructor
 */
static PFplanlist_t *
plan_content (const PFla_op_t *n)
{
    PFplanlist_t *ret        = new_planlist (),
                 *ordered_in = new_planlist ();
    PFalg_col_t   iter       = n->sem.iter_pos_item.iter,
                  pos        = n->sem.iter_pos_item.pos,
                  item       = n->sem.iter_pos_item.item;

    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (ordered_in,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (iter, pos)));

    if (PFprop_node_property (R(n)->prop, item) &&
        !PFprop_node_content_queried (R(n)->prop, item))
        /* for each plan, generate a constructor */
        for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
            add_plan (ret,
                      slim_content (*(plan_t **) PFarray_at (ordered_in, i),
                                    iter, item));
    else
        /* for each plan, generate a constructor */
        for (unsigned int i = 0; i < PFarray_last (ordered_in); i++)
            add_plan (ret,
                      content (*(plan_t **) PFarray_at (ordered_in, i),
                               iter, item));

    return ret;
}

/**
 * `merge_adjacent' operator in the logical algebra just get a 1:1 mapping
 * into the physical merge_adjacent operator.
 */
static PFplanlist_t *
plan_merge_texts (const PFla_op_t *n)
{
    assert (n); assert (n->kind == la_merge_adjacent);
    assert (R(n)); assert (R(n)->plans);

    PFplanlist_t *ret      = new_planlist ();
    PFplanlist_t *sorted   = new_planlist ();
    plan_t       *cheapest_sorted    = NULL;
    PFalg_col_t   iter     = n->sem.merge_adjacent.iter_in,
                  pos      = n->sem.merge_adjacent.pos_in,
                  item     = n->sem.merge_adjacent.item_in,
                  iter_res = n->sem.merge_adjacent.iter_res,
                  pos_res  = n->sem.merge_adjacent.pos_res,
                  item_res = n->sem.merge_adjacent.item_res;
    unsigned int  count    = 3;
    PFalg_proj_t *proj     = PFmalloc (count * sizeof (PFalg_proj_t));

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

    /* ensure that the generated MIL code can cope with our position values */
    if (PFprop_type_of (n, pos) != aat_nat) {
        pos = PFcol_new (col_pos);
        cheapest_sorted = mark_grp (cheapest_sorted,
                                    pos,
                                    iter);
    }

    /* in some situations the position are identical to the item columns */
    proj[0] = PFalg_proj (iter_res, iter);
    if (pos_res == item_res) {
        count = 2;
        proj[1] = PFalg_proj (item_res, item);
    }
    else {
        proj[1] = PFalg_proj (pos_res, pos);
        proj[2] = PFalg_proj (item_res, item);
    }
                         
    /* generate a merge_adjacent_text_node operator for
       the single remaining plan */
    add_plan (ret,
              project (
                  merge_adjacent (
                      cheapest_sorted,
                      iter, pos, item),
                  count, proj));
    return ret;
}

/**
 * 'error' operator in the logical algebra just get a 1:1 mapping
 * into the physical error operator.
 */
static PFplanlist_t *
plan_error (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    /* for each plan, generate an error */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      error (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->sem.err.col));

    return ret;
}

/**
 * Check if the position and item column of a given cached query
 * are identical. In this case we are sure that item order is
 * maintained as the cached result is always sorted by pos.
 */
static bool
cache_item_ordered (const PFla_op_t *n, char *id)
{
    bool item_ordered = false;

    if (n->kind == la_cache &&
        !strcmp (id, n->sem.cache.id))
        return n->sem.cache.pos == n->sem.cache.item;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD; i++)
        if (n->child[i])
            item_ordered |= cache_item_ordered (n->child[i], id);
    
    return item_ordered;
}

/**
 * 'cache' operator in the logical algebra just get a 1:1 mapping
 * into the physical cache operator.
 */
static PFplanlist_t *
plan_cache (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();
    PFplanlist_t *sorted_n2 = new_planlist ();
    PFalg_col_t   pos  = n->sem.cache.pos;

    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++)
        add_plans (sorted_n2,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (R(n)->plans, i),
                       sortby (pos)));

    /* for each plan, generate a cache operator */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_n2); j++)
            add_plan (ret,
                      cache (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (sorted_n2, j),
                          n->sem.cache.id,
                          n->sem.cache.item));

    return ret;
}

/**
 * Constructor for a debug operator
 */
static PFplanlist_t *
plan_trace (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    /* for each plan, generate a trace operator */
    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      trace (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j)));

    return ret;
}

/**
 * Constructor for a debug item operator
 */
static PFplanlist_t *
plan_trace_items (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();
    PFplanlist_t *sorted_n1 = new_planlist ();
    PFalg_col_t   iter = n->sem.iter_pos_item.iter;
    PFalg_col_t   pos  = n->sem.iter_pos_item.pos;

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        add_plans (sorted_n1,
                   ensure_ordering (
                       *(plan_t **) PFarray_at (L(n)->plans, i),
                       sortby (iter, pos)));

    for (unsigned int i = 0; i < PFarray_last (sorted_n1); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      trace_items (
                          *(plan_t **) PFarray_at (sorted_n1, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->sem.iter_pos_item.iter,
                          n->sem.iter_pos_item.item));

    return ret;
}

/**
 * Constructor for a debug message operator
 */
static PFplanlist_t *
plan_trace_msg (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      trace_msg (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->sem.iter_item.iter,
                          n->sem.iter_item.item));

    return ret;
}

/**
 * Constructor for debug relation map operator
 * (A set of the trace_map operators link a trace operator
 * to the correct scope.)
 */
static PFplanlist_t *
plan_trace_map (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      trace_map (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->sem.trace_map.inner,
                          n->sem.trace_map.outer));

    return ret;
}

/**
 * `fun_call' operators in the logical algebra just get a 1:1 mapping
 * into the physical fun_call operator.
 */
static PFplanlist_t *
plan_fun_call (const PFla_op_t *n, const PFla_op_t *root)
{
    unsigned int i;
    PFplanlist_t *ret = new_planlist ();

    for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      fun_call (
                          *(plan_t **) PFarray_at (L(n)->plans, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->schema,
                          n->sem.fun_call.kind,
                          n->sem.fun_call.qname,
                          n->sem.fun_call.ctx,
                          n->sem.fun_call.iter,
                          n->sem.fun_call.occ_ind));

    /* check for nodes -- if that's the case we may return
       only a single plan (as constructors might be involved). */
    for (i = 0; i < n->schema.count; i++)
        if (n->schema.items[i].type & aat_node)
            break;

    if (i == n->schema.count)
        return ret;
    else {
        PFplanlist_t  *single_plan   = new_planlist ();
        plan_t        *cheapest_plan = NULL,
                      *sorted_plan;

        /* find the cheapest plan */
        for (unsigned int i = 0; i < PFarray_last (ret); i++)
            if (!cheapest_plan
                || costless (*(plan_t **) PFarray_at (ret, i),
                             cheapest_plan))
                cheapest_plan = *(plan_t **) PFarray_at (ret, i);

        add_plan (single_plan, cheapest_plan);

        if (n->sem.fun_call.kind == alg_fun_call_cache) {
            /* lookup the cache id */
            char *id;
            assert (R(n)->kind == la_fun_param &&
                    RR(n)->kind == la_nil &&
                    R(n)->schema.count == 3 &&
                    PFprop_const (R(n)->prop, R(n)->schema.items[2].name));
            id = (PFprop_const_val (R(n)->prop,
                                    R(n)->schema.items[2].name)).val.str;

            /* Add more ordering information if the input/output is guaranteed
               to be sorted on the item column. */
            if (cache_item_ordered (root, id)) {
                PFord_set_add (cheapest_plan->orderings,
                               sortby (n->schema.items[2].name));
            }
            /* Add heuristic that says that cached node sequences are
               often sorted by their preorder. This is exploited
               by explicitly assigning the sort operation no cost. */
            else {
                sorted_plan = std_sort (cheapest_plan, 
                                        sortby(n->schema.items[2].name));
                /* make the sort cost as inexpensive as possible */
                sorted_plan->cost = cheapest_plan->cost + 1;
                add_plan (single_plan, sorted_plan);
            }
        }
        
        return single_plan;
    }
}

/**
 * `fun_param' operators in the logical algebra just get a 1:1 mapping
 * into the physical fun_param operator.
 */
static PFplanlist_t *
plan_fun_param (const PFla_op_t *n)
{
    PFplanlist_t *ret = new_planlist (),
                 *left;

    if (fun_call_kind == alg_fun_call_xrpc) {
        left = new_planlist ();
        /* XRPC function call requires its inputs to be properly sorted. */
        for (unsigned int i = 0; i < PFarray_last (L(n)->plans); i++)
            add_plans (left,
                       ensure_ordering (
                           *(plan_t **) PFarray_at (L(n)->plans, i),
                           sortby (n->schema.items[0].name,
                                   n->schema.items[1].name)));
    }
    else {
        left = L(n)->plans;
    }

    for (unsigned int i = 0; i < PFarray_last (left); i++)
        for (unsigned int j = 0; j < PFarray_last (R(n)->plans); j++)
            add_plan (ret,
                      fun_param (
                          *(plan_t **) PFarray_at (left, i),
                          *(plan_t **) PFarray_at (R(n)->plans, j),
                          n->schema));

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
    assert (n); assert (n->kind == la_string_join);
    assert (L(n)); assert (L(n)->plans);
    assert (R(n)); assert (R(n)->plans);

    PFplanlist_t *ret       = new_planlist ();
    PFplanlist_t *sorted_n1 = new_planlist ();
    PFplanlist_t *sorted_n2 = new_planlist ();
    PFalg_col_t   iter      = n->sem.string_join.iter,
                  pos       = n->sem.string_join.pos,
                  item      = n->sem.string_join.item,
                  iter_res  = n->sem.string_join.iter_res,
                  item_res  = n->sem.string_join.item_res,
                  iter_sep  = n->sem.string_join.iter_sep,
                  item_sep  = n->sem.string_join.item_sep;

    PFalg_proj_t *lproj     = PFmalloc (2 * sizeof (PFalg_proj_t)),
                 *rproj     = PFmalloc (1 * sizeof (PFalg_proj_t));

    lproj[0] = PFalg_proj (iter_res, iter);
    lproj[1] = PFalg_proj (item_res, item);
    rproj[0] = PFalg_proj (iter_res, iter_sep);
    rproj[1] = PFalg_proj (item_res, item_sep);
    
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
                       sortby (iter_sep)));

    /* for each remaining plan, generate a string_join operator */
    for (unsigned int i = 0; i < PFarray_last (sorted_n1); i++)
        for (unsigned int j = 0; j < PFarray_last (sorted_n2); j++)
            add_plan (ret,
                      string_join (
                          project (
                              *(plan_t **) PFarray_at (sorted_n1, i),
                              2,
                              lproj),
                          project (
                              *(plan_t **) PFarray_at (sorted_n2, j),
                              2,
                              rproj),
                          iter_res, item_res));
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
                 *params = new_planlist (),
                 *side_effect_plans = new_planlist ();
    plan_t       *cheapest_res_plan = NULL;
    plan_t       *cheapest_params   = NULL;
    PFla_op_t    *cur;

    assert (n->kind == la_rec_fix &&
            L(n)->kind == la_side_effects);

    /* get the first parameter */
    cur = LR(n);
    /* start physical paramter list with the end of the list */
    add_plan (params, nil ());

    /* assign logical properties to physical node as well */
    for (unsigned int plan = 0; plan < PFarray_last (params); plan++)
        (*(plan_t **) PFarray_at (params, plan))->prop = PFprop();

    /* collect the plans for all parameters
       The inputs to the recursion all have to fulfill the ordering
       specified by @a ord. */
    while (cur->kind != la_nil) {
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

    for (unsigned int i = 0; i < PFarray_last (LL(n)->plans); i++)
        for (unsigned int j = 0; j < PFarray_last (params); j++)
            add_plan (side_effect_plans,
                      side_effects (
                          *(plan_t **) PFarray_at (LL(n)->plans, i),
                          *(plan_t **) PFarray_at (params, j)));

    /* prune all plans except one as we otherwise might end up with plans
       that might evaluate the recursion more than once */

    /* find the cheapest plan for the side_effects */
    for (unsigned int i = 0; i < PFarray_last (side_effect_plans); i++)
        if (!cheapest_params
            || costless (*(plan_t **) PFarray_at (side_effect_plans, i),
                         cheapest_params))
            cheapest_params = *(plan_t **) PFarray_at (side_effect_plans, i);

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
 * Generate plans for the findnode physical operator
 */
static PFplanlist_t *
plan_id_join (PFla_op_t *n)
{
    /* some assertions */
    assert (n); assert (n->kind == la_doc_index_join);
    assert (n->sem.doc_join.kind == la_dj_id ||
            n->sem.doc_join.kind == la_dj_idref);
    assert (R(n)); assert (R(n)->plans);

    PFalg_col_t   item_res  = n->sem.doc_join.item_res,
                  item      = n->sem.doc_join.item,
                  item_doc  = n->sem.doc_join.item_doc,
                  iter,
                  iter2;
    unsigned int  count     = n->schema.count + 1,
                  count_in  = 2;
    PFalg_proj_t *proj      = PFmalloc (count * sizeof (PFalg_proj_t)),
                 *proj_in   = PFmalloc (count_in * sizeof (PFalg_proj_t));
    PFplanlist_t *ret       = new_planlist ();
    PFpa_op_t    *plan,
                 *mark;

    /* create the above projection list */
    for (unsigned int i = 0; i < n->schema.count; i++)
        proj[i] = PFalg_proj (n->schema.items[i].name,
                              n->schema.items[i].name);
    /* add the result column */
    proj[n->schema.count] = PFalg_proj (item_res, item_res);

    /* get ourselves two new column name
       (for creating keys using mark and joining back) */
    iter  = PFcol_new (col_iter);
    iter2 = PFcol_new (col_iter);

    /* create the inner projection list */
    proj_in[0] = PFalg_proj (iter2, iter);
    proj_in[1] = PFalg_proj (item_res, item_res);

    /* create the translation for every input plan */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++) {
        plan = *(plan_t **) PFarray_at (R(n)->plans, i);
        mark = mark (plan, iter);

        add_plan (ret,
                  project (
                      leftjoin (
                          iter,
                          iter2,
                          mark,
                          project (
                              findnodes (mark,
                                         iter,
                                         item,
                                         item_doc,
                                         item_res,
                                         n->sem.doc_join.kind == la_dj_id),
                               count_in,
                               proj_in)),
                      count,
                      proj));
    }

    return ret;
}

/**
 * Generate plans for the value index vx_lookup physical operator
 */
static PFplanlist_t *
plan_vx_join (PFla_op_t *n)
{
    /* some assertions */
    assert (n); assert (n->kind == la_doc_index_join);
    assert (n->sem.doc_join.kind == la_dj_text ||
            n->sem.doc_join.kind == la_dj_attr);
    assert (R(n)); assert (R(n)->plans);

    PFalg_col_t   item_res  = n->sem.doc_join.item_res,
                  item      = n->sem.doc_join.item,
                  item_doc  = n->sem.doc_join.item_doc,
                  iter,
                  iter2;
    const char   *ns1       = n->sem.doc_join.ns1,
                 *loc1      = n->sem.doc_join.loc1,
                 *ns2       = n->sem.doc_join.ns2,
                 *loc2      = n->sem.doc_join.loc2;
    unsigned int  count     = n->schema.count + 1,
                  count_in  = 2;
    PFalg_proj_t *proj      = PFmalloc (count * sizeof (PFalg_proj_t)),
                 *proj_in   = PFmalloc (count_in * sizeof (PFalg_proj_t));
    PFplanlist_t *ret       = new_planlist ();
    PFpa_op_t    *plan,
                 *mark;

    /* create the above projection list */
    for (unsigned int i = 0; i < n->schema.count; i++)
        proj[i] = PFalg_proj (n->schema.items[i].name,
                              n->schema.items[i].name);
    /* add the result column */
    proj[n->schema.count] = PFalg_proj (item_res, item_res);

    /* get ourselves two new column name
       (for creating keys using mark and joining back) */
    iter  = PFcol_new (col_iter);
    iter2 = PFcol_new (col_iter);


    /* create the inner projection list */
    proj_in[0] = PFalg_proj (iter2, iter);
    proj_in[1] = PFalg_proj (item_res, item_res);

    /* create the translation for every input plan */
    for (unsigned int i = 0; i < PFarray_last (R(n)->plans); i++) {
        plan = *(plan_t **) PFarray_at (R(n)->plans, i);
        mark = mark (plan, iter);

        add_plan (ret,
                  project (
                      leftjoin (
                          iter,
                          iter2,
                          mark,
                          project (
                              vx_lookup (mark,
                                         iter,
                                         item,
                                         item_doc,
                                         item_res,
                                         n->sem.doc_join.kind == la_dj_text,
                                         ns1, loc1, ns2, loc2),
                               count_in,
                               proj_in)),
                      count,
                      proj));
    }

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
        case la_twig:
        case la_fun_call: /* a function call may contain constructors */
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
    PFarray_t *bases = PFarray (sizeof (PFla_op_t *), 3);
    PFla_op_t *cur;
    bool code;

    assert (n->kind == la_rec_fix &&
            L(n)->kind == la_side_effects);

    cur = LR(n);
    /* collect base operators */
    while (cur->kind != la_nil) {
        assert (cur->kind == la_rec_param && L(cur)->kind == la_rec_arg);
        *(PFla_op_t **) PFarray_add (bases) = L(cur)->sem.rec_arg.base;
        cur = R(cur);
    }

    cur = LR(n);
    /* clean up the plans */
    while (cur->kind != la_nil) {
        code = clean_up_body_plans_worker (LR(cur), bases);

        /* if a constructor was detected we can stop processing now. */
        if (code == 2)
            return false; /* constructor appeared - bail out */

        assert (code);
        cur = R(cur);
    }
    code = clean_up_body_plans_worker (LL(n), bases);
    /* if a constructor was detected we can stop processing now. */
    if (code == 2)
        return false; /* constructor appeared - bail out */

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
    PFplanlist_t *ret = new_planlist (),
                  /* make sure to change a copy and not the input */
                 *in  = PFarray_copy (planlist);

    /* Iterate over all plans. Add it to the return value if there's
     * not yet a better plan.
     */
    for (unsigned int i = 0; i < PFarray_last (in); i++) {
        plan_t *cur = *(plan_t **) PFarray_at (in, i);

        /* assume there is no better plan */
        bool found_better = false;

        /* but maybe there is one already in our return list */
        for (unsigned int j = 0; j < PFarray_last (ret); j++)
            if (better_or_equal (*(plan_t **) PFarray_at (ret, j), cur)) {
                found_better = true;
                break;
            }

        /* if not, is there a better plan to follow in `in'? */
        if (!found_better)
            for (unsigned int j = i + 1; j < PFarray_last (in); j++) {
                plan_t *cur_j    = *(plan_t **) PFarray_at (in, j);
                bool    modified = false;
                /* In case we find a better plan we need a replacement
                   for slot j. The while loop tries to fill in the last
                   entry (and also checks if the last entry is better as
                   the best entry found so far). */
                while (j < PFarray_last (in) &&
                       better_or_equal (cur_j, cur)) {
                    cur = cur_j;
                    cur_j = *(plan_t **) PFarray_top (in);
                    PFarray_del (in);
                    modified = true;
                }
                /* fill in the replacement plan for slot j */
                if (modified && j < PFarray_last (in))
                    *(plan_t **) PFarray_at (in, j) = cur_j;
            }

        /*
         * If we know there is a better plan (either in the result
         * set, or lateron in the plan list), skip this plan.
         * Otherwise, add it to the result set.
         */
        if (!found_better)
            *(plan_t **) PFarray_add (ret) = cur;
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
 * @param root the root operator
 */
static void
plan_subexpression (PFla_op_t *n, PFla_op_t *root)
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
        case la_rec_fix:
        case la_fun_call:
            break;
        default:
            /* translate bottom-up (ensure that the fragment
               information is translated after the value part) */
            for (unsigned int i = PFLA_OP_MAXCHILD; i > 0; i--)
                if (n->child[i - 1])
                    plan_subexpression (n->child[i - 1], root);
    }

    /* Compute possible plans. */
    switch (n->kind) {
        case la_serialize_seq:  plans = plan_serialize (n);    break;
        case la_side_effects:
            /* dummy plans (they are not used anyway) */
            plans = new_planlist (); break;

        case la_lit_tbl:        plans = plan_lit_tbl (n);      break;
        case la_empty_tbl:      plans = plan_empty_tbl (n);    break;
        case la_attach:         plans = plan_attach (n);       break;
        case la_cross:          plans = plan_cross (n);        break;
        case la_eqjoin:         plans = plan_eqjoin (n);       break;
        case la_semijoin:       plans = plan_semijoin (n);     break;
        case la_thetajoin:      plans = plan_thetajoin (n);    break;
        case la_project:
            plans = plan_project (n);

            /* in addition try to implement the combination
               of thetajoin and distinct more efficiently */
            if (L(n)->kind == la_thetajoin &&
                PFprop_set (n->prop)) {
                /* check for dependent thetajoins where the
                   distinct column is also a join column */
                if (n->schema.count == 1 &&
                    L(n)->sem.thetajoin.count == 2 &&
                    L(n)->sem.thetajoin.pred[0].comp == alg_comp_eq &&
                    L(n)->sem.thetajoin.pred[1].comp != alg_comp_ne &&
                    (n->sem.proj.items[0].old ==
                     L(n)->sem.thetajoin.pred[0].left ||
                     n->sem.proj.items[0].old ==
                     L(n)->sem.thetajoin.pred[0].right)) {
                    add_plans (plans, plan_dep_unique_thetajoin (
                                          PFla_distinct (n)));
                /* check for independent thetajoins where the
                   two distinct column come from both the left
                   and the right children of the thetajoin */
                } else if (n->schema.count == 2 &&
                    L(n)->sem.thetajoin.count == 1 &&
                    L(n)->sem.thetajoin.pred[0].comp != alg_comp_ne &&
                    (PFprop_ocol (L(L(n)), n->sem.proj.items[0].old) &&
                     PFprop_ocol (R(L(n)), n->sem.proj.items[1].old)) ^
                    (PFprop_ocol (L(L(n)), n->sem.proj.items[1].old) &&
                     PFprop_ocol (R(L(n)), n->sem.proj.items[0].old))) {
                    add_plans (plans, plan_unique_thetajoin (
                                          PFla_distinct (n)));
                }
            }

            if (n->schema.count == 1 &&
                L(n)->kind == la_step_join &&
                n->sem.proj.items[0].old == L(n)->sem.step.item_res &&
                (PFprop_key (L(n)->prop, L(n)->sem.step.item_res) ||
                 PFprop_set (L(n)->prop))) {
                add_plans (plans, plan_step_join_to_step (n));
            }

            if (n->schema.count == 1 &&
                L(n)->kind == la_step_join &&
                PFprop_type_of (n, n->sem.proj.items[0].new) == aat_nat &&
                (PFprop_key (L(n)->prop, L(n)->sem.step.item_res) ||
                 PFprop_set (L(n)->prop))) {
                add_plans (plans, plan_step_join_to_step_check (n));
            }

            if (n->schema.count == 2 &&
                L(n)->kind == la_step_join) 
                add_plans (plans, plan_step_join_to_llstep (n));
            break;

        case la_select:
            plans = plan_select (n);
            add_plans (plans, plan_subsequence (n));
            break;
        case la_pos_select:     plans = plan_pos_select (n);   break;
        case la_disjunion:
            plans = plan_disjunion (n);
            add_plans (plans, plan_count_ext (n));
            break;
        case la_intersect:      plans = plan_intersect (n);    break;
        case la_difference:     plans = plan_difference (n);   break;
        case la_distinct:
            plans = plan_distinct (n);

            /* in addition try to implement the combination
               of thetajoin and distinct more efficiently */
            if (L(n)->kind == la_project &&
                LL(n)->kind == la_thetajoin) {
                /* check for dependent thetajoins where the
                   distinct column is also a join column */
                if (n->schema.count == 1 &&
                    LL(n)->sem.thetajoin.count == 2 &&
                    LL(n)->sem.thetajoin.pred[0].comp == alg_comp_eq &&
                    LL(n)->sem.thetajoin.pred[1].comp != alg_comp_ne &&
                    (L(n)->sem.proj.items[0].old ==
                     LL(n)->sem.thetajoin.pred[0].left ||
                     L(n)->sem.proj.items[0].old ==
                     LL(n)->sem.thetajoin.pred[0].right)) {
                    add_plans (plans, plan_dep_unique_thetajoin (n));
                /* check for independent thetajoins where the
                   two distinct column come from both the left
                   and the right children of the thetajoin */
                } else if (n->schema.count == 2 &&
                    LL(n)->sem.thetajoin.count == 1 &&
                    (PFprop_ocol (L(LL(n)), L(n)->sem.proj.items[0].old) &&
                     PFprop_ocol (R(LL(n)), L(n)->sem.proj.items[1].old)) ^
                    (PFprop_ocol (L(LL(n)), L(n)->sem.proj.items[1].old) &&
                     PFprop_ocol (R(LL(n)), L(n)->sem.proj.items[0].old))) {
                    add_plans (plans, plan_unique_thetajoin (n));
                }
            }

            if (n->schema.count == 2 &&
                L(n)->kind == la_project &&
                LL(n)->kind == la_step_join &&
                /* check for an iter column ... */
                (PFprop_type_of (n, n->schema.items[0].name) == aat_nat ||
                 PFprop_type_of (n, n->schema.items[1].name) == aat_nat) &&
                /* ... and the correct item column */
                (L(n)->sem.proj.items[0].old == LL(n)->sem.step.item_res ||
                 L(n)->sem.proj.items[1].old == LL(n)->sem.step.item_res)) {
                /* tests above are sufficient as the step_join result
                   can never be of type nat */
                add_plans (plans, plan_step_join_to_llstep (L(n)));
            }
            if (n->schema.count == 1 &&
                L(n)->kind == la_project &&
                LL(n)->kind == la_step_join &&
                L(n)->sem.proj.items[0].old == LL(n)->sem.step.item_res &&
                (PFprop_key (LL(n)->prop, LL(n)->sem.step.item_res) ||
                 PFprop_set (LL(n)->prop))) {
                add_plans (plans, plan_step_join_to_step (L(n)));
            }

            break;

        case la_fun_1to1:       plans = plan_fun_1to1 (n);     break;
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
                                plans = plan_binop (n);           break;

        case la_bool_not:
                                plans = plan_unary (n);           break;
        case la_to:             plans = plan_to (n);              break;
        case la_aggr:           plans = plan_aggr (n);            break;

        case la_rownum:         plans = plan_rownum (n);          break;
        case la_rowrank:
            PFinfo (OOPS_WARNING,
                    "The column generated by the rowrank operator"
                    " will not start with value 1");
            /* fall through */
        case la_rank:           plans = plan_rank (n);            break;
        case la_rowid:          plans = plan_rowid (n);           break;
        case la_type:           plans = plan_type (n);            break;
        case la_type_assert:    plans = plan_type_assert (n);     break;
        case la_cast:           plans = plan_cast (n);            break;

        case la_step:           plans = plan_step (n);            break;
        case la_step_join:      plans = plan_step_join (n);       break;
        case la_doc_tbl:        plans = plan_doc_tbl (n);         break;

        /* case doc_index_join */
        case la_doc_index_join:
            /* need to first check the doc_join->kind to decide which
             * physical operator to construct                         */
            if (n->sem.doc_join.kind == la_dj_id ||
                n->sem.doc_join.kind == la_dj_idref) {
                plans = plan_id_join (n);
            } else if (n->sem.doc_join.kind == la_dj_text ||
                       n->sem.doc_join.kind == la_dj_attr) {
                plans = plan_vx_join (n);
            }                                                     break;
        case la_doc_access:     plans = plan_doc_access (n);      break;

        case la_twig:           plans = plan_twig (n);            break;
        case la_fcns:           plans = plan_fcns (n);            break;
        case la_docnode:        plans = plan_docnode (n);         break;
        case la_element:        plans = plan_element (n);         break;
        case la_attribute:      plans = plan_attribute (n);       break;
        case la_textnode:       plans = plan_textnode (n);        break;
        case la_comment:        plans = plan_comment (n);         break;
        case la_processi:       plans = plan_processi (n);        break;
        case la_content:        plans = plan_content (n);         break;

        case la_merge_adjacent: plans = plan_merge_texts (n);     break;
        /* copy the plans from the children */
        case la_roots:          plans = L(n)->plans;              break;
        /* dummy plans (they are not used anyway) */
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
                                plans = new_planlist ();          break;

        case la_error:          plans = plan_error (n);           break;

        case la_nil:
            plans = new_planlist ();
            add_plan (plans, nil ());
            break;

        case la_cache:          plans = plan_cache (n);           break;
        case la_trace:          plans = plan_trace (n);           break;
        case la_trace_items:    plans = plan_trace_items (n);     break;
        case la_trace_msg:      plans = plan_trace_msg (n);       break;
        case la_trace_map:      plans = plan_trace_map (n);       break;

        case la_rec_fix:
        {
            PFla_op_t       *rec_arg, *cur;
            PFord_ordering_t ord;
            PFord_set_t      orderings = PFord_set ();
            PFplanlist_t    *rec_list = new_planlist ();
            plan_t          *cheapest_rec_plan = NULL;
            plans = new_planlist ();

            /* get the plans for all the seeds */
            assert (L(n)->kind == la_side_effects);
            cur = LR(n);
            while (cur->kind != la_nil) {
                assert (cur->kind == la_rec_param &&
                        L(cur)->kind == la_rec_arg);
                rec_arg = L(cur);
                plan_subexpression (L(rec_arg), root);
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
                cur = LR(n);

                /* create base operators with the correct ordering */
                while (cur->kind != la_nil) {
                    assert (cur->kind == la_rec_param &&
                            L(cur)->kind == la_rec_arg);
                    rec_arg = L(cur);
                    plan_recursion_base (rec_arg->sem.rec_arg.base, ord);
                    cur = R(cur);
                }

                /* generate the plans for the body */
                cur = LR(n);
                while (cur->kind != la_nil) {
                    assert (cur->kind == la_rec_param &&
                            L(cur)->kind == la_rec_arg);
                    rec_arg = L(cur);
                    plan_subexpression (R(rec_arg), root);
                    cur = R(cur);
                }

                /* create plans for the side effects */
                plan_subexpression (LL(n), root);

                /* create plans for the result relation */
                plan_subexpression (R(n), root);

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

        case la_fun_call:
        {   /* TOPDOWN */
            PFalg_fun_call_t old_fun_call_kind = fun_call_kind;
            fun_call_kind = n->sem.fun_call.kind;

            plan_subexpression (L(n), root);
            plan_subexpression (R(n), root);
            plans = plan_fun_call (n, root);

            fun_call_kind = old_fun_call_kind;
        } break;

        case la_fun_param:      plans = plan_fun_param (n);       break;
        /* copy the plans from the rest of the list */
        case la_fun_frag_param: plans = R(n)->plans;              break;

        case la_string_join:    plans = plan_string_join (n);     break;

        default:
            PFoops (OOPS_FATAL,
                    "physical algebra equivalent for logical algebra "
                    "node kind %u not implemented, yet", n->kind);
    }

    assert (plans);
#ifndef NDEBUG
    switch (n->kind) {
        case la_side_effects:
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
            break;
        default:
            assert (PFarray_last (plans) > 0);
            break;
    }
#endif

    /* Introduce some more orders (on iter columns of length 1 or 2)
       in the hope to share more sort operations. */
    switch (n->kind) {
        case la_side_effects:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_merge_adjacent:
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_nil:
        case la_cache:
        case la_trace:
        case la_trace_msg:
        case la_trace_map:
        case la_fun_call:
            break;

        default:
            /* add check to avoid a segfault if there is no plan */
            if (PFarray_last (plans) > 0)
        {
            PFord_ordering_t ord = PFordering ();
            PFord_set_t orderings = PFord_set ();
            plan_t *p = (*(plan_t **) PFarray_at (plans, 0));
            unsigned int plan_count = PFarray_last (plans);
            unsigned int i, j, plan;
            PFalg_col_t col;

            for (i = 0; i < p->schema.count; i++) {
                col = p->schema.items[i].name;
                /* check for an iter-like column (to avoid generating
                   a large amount of plans) */
                if (PFcol_new_fixed (col, 1) == PFcol_new_fixed (col_iter, 1))
                    ord = PFord_refine (ord, p->schema.items[i].name, DIR_ASC);
            }

            /* collect all possible orderings of length 1 and 2 */
            for (i = 0; i < PFord_count (ord); i++) {
                PFord_ordering_t  ordering = PFordering ();

                ordering = PFord_refine (ordering,
                                         PFord_order_col_at (ord, i),
                                         DIR_ASC);
                PFord_set_add (orderings, ordering);

                for (j = 0; j < PFord_count (ord); j++)
                    if (j != i)
                        PFord_set_add (orderings,
                                       PFord_refine (
                                           ordering,
                                           PFord_order_col_at (ord, j),
                                           DIR_ASC));
            }

            /* add all plans satisfying the generated ordering
               to the list of plans */
            for (plan = 0; plan < plan_count; plan++) {
                p = (*(plan_t **) PFarray_at (plans, plan));
                /* Make sure that we only add potentially better plans
                   if we have only a small number of input orders.
                   (Choose 1000 as otherwise prune_plans() will have
                    to compare more than 1.000.000 orderings for each
                    plan combination.)
                    Furthermore ensure that we do not increase the plan space
                    if we already have a large number of possible plans. */
                if (PFord_set_count (p->orderings) < 1000 &&
                    PFarray_last (plans) < MAGIC_BOUNDARY)
                    for (i = 0; i < PFord_set_count (orderings); i++) {
                        ord = PFord_set_at (orderings, i);
                        add_plans (plans, ensure_ordering (p, ord));
                    }
            }
        }
    }

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

    assert (root->kind == la_serialize_seq);

    /* compute all interesting plans for root */
    plan_subexpression (root, root);

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
