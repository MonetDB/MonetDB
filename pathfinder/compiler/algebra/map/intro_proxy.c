/**
 * @file
 *
 * Introduce proxy operators.
 *
 * Optimizations based on the proxies rely on some properties of
 * the proxy which are only implicitly available in the current
 * version.
 *
 * If the semantics of the compilation changes this might also
 * break the optimizations based on the proxy. For more information
 * please refer to the proxy handling in compiler/algebra/opt/opt_mvd.c.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "la_proxy.h"
#include "properties.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"
#include "alg_dag.h"
#include "algopt.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** ... and so on */
#define LL(p) (L(L(p)))
#define RL(p) (L(R(p)))

#define SEEN(p)  ((p)->bit_dag)
#define pfIN(p)  ((p)->bit_in)
#define pfOUT(p) ((p)->bit_out)

/**
 * worker for remove_semijoin_operators() that
 * replaces semijoin operators by equi-joins and
 * a distinct.
 */
static void
remove_semijoin_worker (PFla_op_t *p)
{
    assert (p);

    /* nothing to do if we already visited that node */
    if (SEEN(p))
        return;

    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        remove_semijoin_worker (p->child[i]);

    if (p->kind == la_semijoin) {
        /* we need to additional projections:
           top    to remove the right join argument from the result list
           right  to avoid name conflicts with the left input columns
                  and to ensure that distinct is applied only on the join
                  column */
        PFalg_proj_t *top   = PFmalloc (p->schema.count *
                                        sizeof (PFalg_proj_t)),
                     *right = PFmalloc (sizeof (PFalg_proj_t));
        PFalg_att_t   used_cols = 0,
                      new_col = p->sem.eqjoin.att2,
                      cur_col;

        /* fill the 'top' projection and collect all used columns
           of the left input relation (equals the semijoin output
           schema) */
        for (unsigned int i = 0; i < p->schema.count; i++) {
            cur_col = p->schema.items[i].name;

            top[i] = PFalg_proj (cur_col, cur_col);
            used_cols |= cur_col;
        }
        /* rename the join argument in case a name conflict occurs */
        if (used_cols & new_col)
            new_col = PFalg_ori_name (
                          PFalg_unq_name (new_col, 0),
                          ~used_cols);

        /* project away all columns except for the join column */
        right[0] = PFalg_proj (new_col, p->sem.eqjoin.att2);

        /* replace the semijoin */
        *p = *PFla_project_ (
                  PFla_eqjoin (L(p),
                               PFla_distinct (
                                   PFla_project_ (R(p), 1, right)),
                               p->sem.eqjoin.att1,
                               new_col),
                  p->schema.count,
                  top);
    }

    SEEN(p) = true;
}

/**
 * This function removes all semijoin operators
 * as the proxy introduction phases cannot cope
 * with them.
 */
static void
remove_semijoin_operators (PFla_op_t *root)
{
    assert (root->kind == la_serialize_seq ||
            root->kind == la_serialize_rel);

    remove_semijoin_worker (root);
    PFla_dag_reset (root);
}

/**
 *
 * conflict resolving functions that are used by the
 * eqjoin - rowid proxy (kind=1) and the
 * semijoin - rowid - cross proxy
 *
 */

/**
 * replace_reference replaces the references to the @a old node by
 * the reference to the @a new one. As a side effect @a ref_list
 * is modified.
 */
static void
replace_reference (PFarray_t *ref_list, PFla_op_t *old, PFla_op_t *new)
{
    for (unsigned int i = 0; i < PFarray_last (ref_list); i++)
        if (*(PFla_op_t **) PFarray_at (ref_list, i) == old) {
            *(PFla_op_t **) PFarray_at (ref_list, i) = new;
        }
}

/**
 * join_resolve_conflict_worker tries to cope with the
 * remaining conflicting nodes. It accepts operators of
 * whose cardinality is the same as the one of the equi-join
 * and whose columns are all still visible at the entry
 * operator.
 *
 * Currently these operators are projections and distincts
 * with a distance to the equi-join of at most 2.
 *
 * @ret contains the information whether the conflict list
 *      is still consistent.
 */
static bool
join_resolve_conflict_worker (PFla_op_t *p,
                              PFla_op_t *lp,
                              PFla_op_t *rp,
                              PFalg_att_t latt,
                              PFalg_att_t ratt,
                              PFarray_t *conflict_list,
                              PFarray_t *exit_refs)
{
    PFla_op_t *node;
    bool remove, rp_ref = false, rlp_ref = false;
    unsigned int i = 0, last = PFarray_last (conflict_list);
    unsigned int ori_last = last;

    while (i < last) {
        remove = false;
        node = *(PFla_op_t **) PFarray_at (conflict_list, i);

        /* rewrite only patterns with project and distinct */
        if (node->kind != la_project &&
            node->kind != la_distinct)
            return ori_last == last;

        /* look into the children of the join operator */
        if (rp == node)                  { rp_ref  = true; remove = true; }
        else if (L(rp) && L(rp) == node) { rlp_ref = true; remove = true; }

        if (remove) {
            *(PFla_op_t **) PFarray_at (conflict_list, i)
                = *(PFla_op_t **) PFarray_top (conflict_list);
            PFarray_del (conflict_list);
            last--;
        }
        else
            i++;
    }

    if (rp_ref && !rlp_ref &&
        rp->kind == la_distinct) {
        unsigned int count = rp->schema.count;
        PFalg_proj_t *proj_list = PFmalloc (count * sizeof (PFalg_proj_t));

        for (i = 0; i < rp->schema.count; i++)
            proj_list[i] = PFalg_proj (rp->schema.items[i].name,
                                       rp->schema.items[i].name);

        *p  = *PFla_eqjoin (lp, PFla_distinct (L(rp)), latt, ratt);

        /* make sure the exit reference is adjusted
           before the node is overwritten */
        replace_reference (exit_refs, rp, R(p));

        *rp = *PFla_project_ (p, count, proj_list);
        return true;
    }
    else if (rp_ref && !rlp_ref &&
             rp->kind == la_project) {
        unsigned int count = rp->schema.count;
        /* create new projection lists */
        PFalg_proj_t *proj_list1 = PFmalloc (count *
                                             sizeof (*(proj_list1)));
        PFalg_proj_t *proj_list2 = PFmalloc (count *
                                             sizeof (*(proj_list2)));

        for (i = 0; i < rp->schema.count; i++) {
            proj_list1[i] = rp->sem.proj.items[i];
            proj_list2[i] = PFalg_proj (rp->sem.proj.items[i].new,
                                        rp->sem.proj.items[i].new);
        }

        *p  = *PFla_eqjoin (lp,
                            PFla_project_ (L(rp), count, proj_list1),
                            latt,
                            ratt);

        /* make sure the exit reference is adjusted
           before the node is overwritten */
        replace_reference (exit_refs, rp, R(p));

        *rp = *PFla_project_ (p, count, proj_list2);
        return true;
    }
    else if (rlp_ref &&
             rp->kind == la_project &&
             L(rp)->kind == la_distinct &&
             rp->schema.count == 1 &&
             L(rp)->schema.count == 1) {
        PFla_op_t *rlp = L(rp);
        /* create new projection lists */
        PFalg_proj_t *proj_list  = PFmalloc (sizeof (PFalg_proj_t));
        PFalg_proj_t *proj_listP = PFmalloc (sizeof (PFalg_proj_t));
        PFalg_proj_t *proj_listD = PFmalloc (sizeof (PFalg_proj_t));

        proj_list [0] = rp->sem.proj.items[0];
        proj_listP[0] = PFalg_proj (rp->sem.proj.items[0].new,
                                    rp->sem.proj.items[0].new);
        proj_listD[0] = PFalg_proj (rp->sem.proj.items[0].old,
                                    rp->sem.proj.items[0].new);

        *p  = *PFla_eqjoin (lp, PFla_project_ (
                                    PFla_distinct (L(L(rp))),
                                    1, proj_list),
                            latt, ratt);

        /* make sure the exit references are adjusted
           before the node is overwritten */
        replace_reference (exit_refs, rlp, RL(p));
        replace_reference (exit_refs, rp, R(p));

        *rlp = *PFla_project_ (p, 1, proj_listD);
        *rp = *PFla_project_ (p, 1, proj_listP);
        return true;
    }
    /* There still migh be conflicts which can't be solved -- if the
       conflict list has changed without any rewrite we have to report an
       inconsistent state. */
    else
        return ori_last == last;
}

#define CONFLICT 0
#define PROXY_ONLY 1
#define MULTIPLE 2

/**
 * join_resolve_conflicts discards entries in the conflict list
 * that refer to the proxy entry or proxy exit operator. If there
 * are remaining conflicting entries we try to solve them in
 * a separate worker.
 *
 * @ret the return code can be one of the following three values:
 *      CONFLICT (0) meaning we could not solve all conflicts
 *      PROXY_ONLY (1) meaning that the proxy exit operator was
 *                     only referenced from inside the proxy.
 *      MULTIPLE (2) meaning that the proxy exit operator was
 *                   also referenced from 'out'side the proxy.
 */
static int
join_resolve_conflicts (PFla_op_t *proxy_entry,
                        PFla_op_t *proxy_exit,
                        PFarray_t *conflict_list,
                        PFarray_t *exit_refs)
{
    PFla_op_t *node, *p;
    int leaf_ref = PROXY_ONLY;
    unsigned int i = 0, last = PFarray_last (conflict_list);
    bool consistent;

    assert (proxy_entry);
    assert (proxy_exit);
    assert (conflict_list);

    /* remove entry and exit references */
    while (i < last) {
        node = *(PFla_op_t **) PFarray_at (conflict_list, i);
        if (proxy_exit == node) {
            leaf_ref = MULTIPLE;
            *(PFla_op_t **) PFarray_at (conflict_list, i)
                = *(PFla_op_t **) PFarray_top (conflict_list);
            PFarray_del (conflict_list);
            last--;
        } else if (proxy_entry == node) {
            *(PFla_op_t **) PFarray_at (conflict_list, i)
                = *(PFla_op_t **) PFarray_top (conflict_list);
            PFarray_del (conflict_list);
            last--;
        } else
            i++;
    }

    if (!last)
        return leaf_ref;

    p = proxy_entry;

    /* This check is more restrictive as required... */
    if (!PFprop_key_left (p->prop, p->sem.eqjoin.att1) ||
        !PFprop_key_right (p->prop, p->sem.eqjoin.att2))
        return CONFLICT;

    /* the subdomain relationship ensures that we only look at the child
       of the equi-join whose cardinality is unchanged after the join. */

    /* Check all domains before calling the worker as
       it might rewrite the DAG which results in missing
       domain information in the newly constructed nodes. */
    if (PFprop_subdom (p->prop,
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2),
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1)) &&
        PFprop_subdom (p->prop,
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1),
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2))) {
        consistent = join_resolve_conflict_worker (p, L(p), R(p),
                                                   p->sem.eqjoin.att1,
                                                   p->sem.eqjoin.att2,
                                                   conflict_list,
                                                   exit_refs);
        if (! consistent) return CONFLICT;
        consistent = join_resolve_conflict_worker (p, R(p), L(p),
                                                   p->sem.eqjoin.att2,
                                                   p->sem.eqjoin.att1,
                                                   conflict_list,
                                                   exit_refs);
        if (! consistent) return CONFLICT;
    }
    else if (PFprop_subdom (p->prop,
                            PFprop_dom_right (p->prop,
                                              p->sem.eqjoin.att2),
                            PFprop_dom_left (p->prop,
                                             p->sem.eqjoin.att1))) {
        consistent = join_resolve_conflict_worker (p, L(p), R(p),
                                                   p->sem.eqjoin.att1,
                                                   p->sem.eqjoin.att2,
                                                   conflict_list,
                                                   exit_refs);
        if (! consistent) return CONFLICT;
    }
    else if (PFprop_subdom (p->prop,
                            PFprop_dom_left (p->prop,
                                             p->sem.eqjoin.att1),
                            PFprop_dom_right (p->prop,
                                              p->sem.eqjoin.att2))) {
        consistent = join_resolve_conflict_worker (p, R(p), L(p),
                                                   p->sem.eqjoin.att2,
                                                   p->sem.eqjoin.att1,
                                                   conflict_list,
                                                   exit_refs);
        if (! consistent) return CONFLICT;
    }

    return PFarray_last (conflict_list)? CONFLICT : leaf_ref;
}




/**
 *
 * Functions specific to the semijoin - rowid - cross proxy
 * rewrite.
 *
 */

#define check_base(p) (((p)->kind == la_project &&       \
                        L((p))->kind == la_cross) ||     \
                       ((p)->kind == la_project &&       \
                        L((p))->kind == la_thetajoin) || \
                       ((p)->kind == la_cross) ||        \
                       ((p)->kind == la_thetajoin))
/**
 * semijoin_entry checks the complete pattern
 * -- see function modify_semijoin_proxy() for
 *    more information.
 */
static bool
semijoin_entry (PFla_op_t *p)
{
    PFla_op_t *lp, *rp;

    if (p->kind != la_eqjoin)
        return false;

    lp = L(p);
    rp = R(p);

    if (lp->kind == la_project)
        lp = L(lp);

    if (rp->kind == la_project)
        rp = L(rp);

    return ((lp->kind == la_distinct &&
             rp->kind == la_rowid &&
             check_base(L(rp)) &&
             L(p)->schema.count == 1 &&
             lp->schema.count == 1 &&
             PFprop_subdom (p->prop,
                            PFprop_dom_left (p->prop,
                                             p->sem.eqjoin.att1),
                            PFprop_dom_right (p->prop,
                                              p->sem.eqjoin.att2)) &&
             PFprop_subdom (p->prop,
                            PFprop_dom_right (p->prop,
                                              p->sem.eqjoin.att2),
                            PFprop_dom (rp->prop,
                                        rp->sem.rowid.res)))
            ||
            (rp->kind == la_distinct &&
             lp->kind == la_rowid &&
             check_base(L(lp)) &&
             R(p)->schema.count == 1 &&
             rp->schema.count == 1 &&
             PFprop_subdom (p->prop,
                            PFprop_dom_right (p->prop,
                                              p->sem.eqjoin.att2),
                            PFprop_dom_left (p->prop,
                                             p->sem.eqjoin.att1)) &&
             PFprop_subdom (p->prop,
                            PFprop_dom_left (p->prop,
                                             p->sem.eqjoin.att1),
                            PFprop_dom (lp->prop,
                                        lp->sem.rowid.res))));
}

/**
 * semijoin_exit looks for the correct rowid operator.
 */
static bool
semijoin_exit (PFla_op_t *p, PFla_op_t *entry)
{
    PFla_op_t *lp, *rp;

    if (p->kind != la_rowid || !check_base (L(p)))
        return false;

    lp = L(entry);
    rp = R(entry);

    if (lp->kind == la_project)
        lp = L(lp);

    if (rp->kind == la_project)
        rp = L(rp);

    return lp == p || rp == p;
}

#define OP_NOJOIN 0
#define OP_JOIN   1
#define OP_UNDEF  2

/**
 * Worker for function only_eqjoin_refs that traverses the DAG
 * and checks the references to the exit,
 */
static int
only_eqjoin_refs_worker (PFla_op_t *p, PFla_op_t *exit,
                         PFla_op_t *entry, int join)
{
    int cur_op, res_op;

    assert (p);
    assert (exit);

    if (p == exit)
        /* return what operators were used above p */
        return join;
    else if (SEEN(p))
        /* look at each node only once */
        return OP_UNDEF;
    else
        SEEN(p) = true;

    /* prepare the operator code for the recursive descent */
    if (p->kind == la_project)
        cur_op = join;
    /* mark equi-joins that operate on proxy entry and the proxy
       base as valid candidates for rewiring. */
    else if (p->kind == la_eqjoin &&
        ((PFprop_subdom (p->prop,
                         PFprop_dom_left (p->prop,
                                          p->sem.eqjoin.att1),
                         PFprop_dom (entry->prop,
                                     entry->sem.eqjoin.att1)) &&
          PFprop_subdom (p->prop,
                         PFprop_dom_right (p->prop,
                                           p->sem.eqjoin.att2),
                         PFprop_dom (exit->prop,
                                     exit->sem.rowid.res)))
         ||
         (PFprop_subdom (p->prop,
                         PFprop_dom_right (p->prop,
                                           p->sem.eqjoin.att2),
                         PFprop_dom (entry->prop,
                                     entry->sem.eqjoin.att1)) &&
          PFprop_subdom (p->prop,
                         PFprop_dom_left (p->prop,
                                          p->sem.eqjoin.att1),
                         PFprop_dom (exit->prop,
                                     exit->sem.rowid.res)))))
        cur_op = OP_JOIN;
    else
        cur_op = OP_NOJOIN;

    res_op = OP_UNDEF;
    /* traverse children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++) {
        join = only_eqjoin_refs_worker (p->child[i], exit, entry, cur_op);
        /* Bail out if we found a mismatch */
        if (join == OP_NOJOIN)
            return OP_NOJOIN;
        /* - otherwise collect the result and check the other branch. */
        else if (join == OP_JOIN)
            res_op = join;
    }

    return res_op;
}

/**
 * Check whether the references to the proxy exit are all
 * equi-join (possibly followed by projections).
 */
static bool
only_eqjoin_refs (PFla_op_t *root,
                  PFla_op_t *proxy_entry,
                  PFla_op_t *proxy_exit)
{
    int join_ops_only;

    /* Make sure that the proxy pattern is not visited,
       thus pruning all internal references to the exit node. */
    SEEN(proxy_entry) = true;
    SEEN(proxy_exit) = true;

    /* Traverse the DAG and check whether the 'external' references to
       the exit node are 'valid' joins (and projection) only. */
    join_ops_only = only_eqjoin_refs_worker (root, proxy_exit,
                                             proxy_entry, OP_NOJOIN);
    PFla_dag_reset (root);

    return join_ops_only != OP_NOJOIN;
}

/**
 * We rewrite a DAG fragment of the form shown in (1) and (3) into the
 * DAGs shown in (3) and (4), respectively. Operators in parenthesis are
 * optional ones.
 *
 * The basic pattern (in (1)) consists of an equi-join (or a semi-join)
 * which has a rowid and a distinct argument. The pattern can be seen
 * as some kind of semijoin.  The distinct operator works on a single
 * column that is inferred from the column generated by the rowid operator.
 * Underneath the rowid operator a cross product is located.
 * The pattern is rewritten in such a way that the rowid operator # ends
 * up at the top of the DAG fragment. As input for the distinct operator we
 * use a combined key of two new rowid operators that reside in the cross
 * product arguments. An intermediate rowid + join operator pair maps the
 * columns of #" and #"' such that the join pushdown optimization phase
 * handles there correct integration in sub-DAG 1. The two equi-joins above
 * map the columns of the cross product arguments to the pattern top and thus
 * replace the equi-join of (1). The rowid operator # and a final projection
 * that adjusts the required schema completes the new DAG.
 * The optional projections in (1) pi_(left) and pi_(right) are both
 * integrated into the new final projection pi_(entry) in (2)
 *
 *
 *                                                     |
 *                                                     |
 *                                                 pi_(entry)
 *                                                     |
 *                                                     #_____
 *                   |                                       \
 *                   |                           ____________|X|
 *                  |X|                         /               \
 *                 /   \                      |X|____            |
 *                / (pi_(right))              /      \           |
 *               |      |                     |   distinct       |
 *               |   distinct                 |       |          |
 *          (pi_(left)) |                     |  pi_(#",#"')     |
 *               |     / \                    |       |          |
 *               |    / 1 \               ___/      _|X|_        |
 *               |   /_____\             /         /     \       |
 *               |      |                | pi_(#',#",#"') |      |
 *               |      |                \___      |     / \     |
 *               |      |                    \     |    / 1 \    |
 *               \___   |                     |    |   /_____\   |
 *                   \ /                      |    |      |      |
 *                    #                       |    |   pi_(exit) |
 *                    |                       |    |      |      |
 *                    X                       |    \___ #'       |
 *                  _/ \_                     |         |        |
 *                 /     \                    |         X        |
 *                /2\    /3\                  \_____   / \   ___/
 *               /___\  /___\                       \ /   \ /
 *                                                   #"    #"'
 *                                                   |      |
 *                                                  /2\    /3\
 *                                                 /___\  /___\
 *
 *
 *                  ( 1 )                             ( 2 )
 *
 * If the rowid operator # in (1) is referenced by another operator
 * outside the proxy pattern we have to ensure the pattern in (3). The
 * basic idea is to allow only references to # that don't require the
 * exact cardinality but only use it for mapping the columns of sub-DAGs
 * 2 and 3. (As a mapping equi-join was the only reference we observed
 * the check is restricted to the pattern in (3).) In addition to the
 * mapping of (1) -> (2) the reference to # in (3) is replaced by a
 * projection pi_(exit) that is connected to the new rowid operator #
 * at the top of the new DAG.
 *
 *
 *                                 (|X|)--(_)*---     |
 *                                   |           \    |
 *                                 (pi)*          pi_(entry)
 *                                   |                |
 *         (|X|)                  (pi_(exit))---------#_____
 *         /    \                                           \
 *        |      \                              ____________|X|
 *        |      (_)*                          /               \
 *        |        \                         |X|____            |
 *        |         \   |                    /      \           |
 *       (pi)*       \ /                     |   distinct       |
 *        |          |X|                     |       |          |
 *        |         /   \                    |  pi_(#",#"')     |
 *        |        / (pi_(right))            |       |          |
 *        |       |      |               ___/      _|X|_        |
 *        |       |   distinct          /         /     \       |
 *        |  (pi_(left)) |              | pi_(#',#",#"') |      |
 *        |       |     / \             \___      |     / \     |
 *        |       |    / 1 \                \     |    / 1 \    |
 *        |       |   /_____\                |    |   /_____\   |
 *        |       |      |                   |    |      |      |
 *        |       |      |                   |    |   pi_(exit) |
 *        |       |      |                   |    |      |      |
 *        |       \___   |                   |    \___ #'       |
 *         \          \ /                    |         |        |
 *          -----------#                     |         X        |
 *                     |                     \_____   / \   ___/
 *                     X                           \ /   \ /
 *                   _/ \_                          #"    #"'
 *                  /     \                         |      |
 *                 /2\    /3\                      /2\    /3\
 *                /___\  /___\                    /___\  /___\
 *
 *
 *                  ( 3 )                            ( 4 )
 */
static bool
modify_semijoin_proxy (PFla_op_t *root,
                       PFla_op_t *proxy_entry,
                       PFla_op_t *proxy_exit,
                       PFarray_t *conflict_list,
                       PFarray_t *exit_refs,
                       PFarray_t *checked_nodes)
{
    int leaf_ref;
    PFalg_att_t num_col, num_col1, num_col2, join_att2,
                num_col_alias, num_col_alias1, num_col_alias2,
                used_cols = 0;
    unsigned int i, j;
    PFalg_proj_t *exit_proj, *proxy_proj, *dist_proj, *left_proj,
                 *opt_base_proj = NULL;
    PFla_op_t *lp, *rp, *lproject = NULL, *rproject = NULL,
              *num1, *num2, *rowid, *new_rowid, *lexit, *op;

    /* do not introduce proxy if some conflicts remain */
    if (!(leaf_ref = join_resolve_conflicts (proxy_entry,
                                             proxy_exit,
                                             conflict_list,
                                             /* as we might also rewrite nodes
                                                referencing the exit we have
                                                to update the exit_refs list
                                                as well */
                                             exit_refs)))
        return false;

    /* If the proxy exit is referenced from outside the proxy as well
       and the references are NOT equi-joins we can not remove the
       rowid operator at the proxy exit and thus don't push up the
       cross product. Thus we would not benefit from a rewrite
       and give up. */
    if (leaf_ref == MULTIPLE &&
        ! only_eqjoin_refs (root, proxy_entry, proxy_exit))
        return false;

    /* check for the remaining part of the pattern */
    lp = L(proxy_entry);
    rp = R(proxy_entry);
    /* look for an project operator in the left equi-join branch */
    if (lp->kind == la_project) {
        lproject = lp;
        lp = L(lp);
    }
    /* look for an project operator in the right equi-join branch */
    if (rp->kind == la_project) {
        rproject = rp;
        rp = L(rp);
    }

    /* normalize the pattern such that the distinct
       operator always resides in the 'virtual' right side (rp). */
    if (lp->kind == la_distinct) {
        rp = lp; /* we do not need the old 'rp' reference anymore */
        lproject = rproject; /* we do not need the old 'rproject' anymore */
    }

    assert (rp->kind == la_distinct);

    /* the name of the single distinct column */
    join_att2 = rp->schema.items[0].name;
    rp = L(rp);

    /* we have now checked the additional requirements (conflicts
       resolved or rewritable) and als have ensured that:
       - rp references the distinct operator
       - lproject references the project above the proxy_exit (if any)

       Thus we can begin the translation ...
     */

    /* name of the key column */
    num_col = proxy_exit->sem.rowid.res;

    exit_proj  = PFmalloc (proxy_exit->schema.count * sizeof (PFalg_proj_t));
    proxy_proj = PFmalloc (proxy_entry->schema.count * sizeof (PFalg_proj_t));
    dist_proj  = PFmalloc (2 * sizeof (PFalg_proj_t));
    left_proj  = PFmalloc (3 * sizeof (PFalg_proj_t));

    /* Collect all the used column names and create the projection list
       that hides the new rowid columns inside the cross product for
       all operators referencing the proxy exit. */
    for (i = 0; i < proxy_exit->schema.count; i++) {
        used_cols = used_cols | proxy_exit->schema.items[i].name;
        exit_proj[i] = PFalg_proj (
                           proxy_exit->schema.items[i].name,
                           proxy_exit->schema.items[i].name);
    }

    lexit = L(proxy_exit);
    if (lexit->kind == la_project) {
        for (i = 0; i < L(lexit)->schema.count; i++)
            used_cols = used_cols | L(lexit)->schema.items[i].name;
        opt_base_proj = PFmalloc ((lexit->schema.count + 2) *
                                  sizeof (PFalg_proj_t));
        for (i = 0; i < lexit->sem.proj.count; i++)
            opt_base_proj[i+2] = lexit->sem.proj.items[i];
        lexit = L(lexit);
    }

    /* We mark the right join column used.
       (It possibly has a different name as we discarded
        renaming projections (rproject)) */
    used_cols = used_cols | join_att2;

    /* Create 4 new column names for the two additional rowid operators
       and their backmapping equi-joins */
    num_col1 = PFalg_ori_name (PFalg_unq_name (num_col, 0), ~used_cols);
    used_cols = used_cols | num_col1;
    num_col2 = PFalg_ori_name (PFalg_unq_name (num_col, 0), ~used_cols);
    used_cols = used_cols | num_col2;
    num_col_alias1 = PFalg_ori_name (PFalg_unq_name (num_col, 0), ~used_cols);
    used_cols = used_cols | num_col_alias1;
    num_col_alias2 = PFalg_ori_name (PFalg_unq_name (num_col, 0), ~used_cols);
    used_cols = used_cols | num_col_alias2;

    /* Create a new alias for the mapping rowid operator if its
       name conflicts with the column name of the sub-DAG */
    if (join_att2 == num_col) {
        num_col_alias = PFalg_ori_name (
                            PFalg_unq_name (num_col, 0),
                            ~used_cols);
        used_cols = used_cols | num_col_alias;
    } else
        num_col_alias = num_col;

    /* Create the projection list for the input of the equi-join that
       maps the two new column names num_col1 and num_col2. */
    left_proj[0] = PFalg_proj (num_col_alias, num_col);
    left_proj[1] = PFalg_proj (num_col1, num_col1);
    left_proj[2] = PFalg_proj (num_col2, num_col2);
    /* Create the projection list for the input of the modified
       distinct operator (two instead of one column) */
    dist_proj[0] = PFalg_proj (num_col_alias1, num_col1);
    dist_proj[1] = PFalg_proj (num_col_alias2, num_col2);

    /* Create the projection list that maps the names to the ones
       generated by the current proxy entry operator. It handles the
       possible projections (lproject and rproject) by looking up
       the correct previous names (case 1 and 2). */
    for (i = 0; i < proxy_entry->schema.count; i++) {
        PFalg_att_t entry_col = proxy_entry->schema.items[i].name;
        /* Be aware that this also creates the correct projection if
           the proxy entry operator is not an equi-join but a semi-join:
           then proxy_entry->sem.eqjoin.att2 will never occur in the schema. */
        if (entry_col == proxy_entry->sem.eqjoin.att1 ||
            entry_col == proxy_entry->sem.eqjoin.att2)
            proxy_proj[i] = PFalg_proj (entry_col, num_col);
        else if (lproject) {
            for (j = 0; j < lproject->sem.proj.count; j++)
                if (entry_col == lproject->sem.proj.items[j].new) {
                    proxy_proj[i] =
                        PFalg_proj (entry_col,
                                    lproject->sem.proj.items[j].old);
                    break;
                }
            assert (j != lproject->sem.proj.count);
        } else
            proxy_proj[i] = PFalg_proj (entry_col, entry_col);
    }

    /* build the modified DAG */
    num1 = PFla_rowid (L(lexit), num_col1);
    num2 = PFla_rowid (R(lexit), num_col2);

    if (lexit->kind == la_cross)
        op = PFla_cross (num1, num2);
    else if (lexit->kind == la_thetajoin)
        op = PFla_thetajoin (
                 num1,
                 num2,
                 lexit->sem.thetajoin.count,
                 lexit->sem.thetajoin.pred);
    else
        return false;

    if (opt_base_proj) {
        unsigned int count;
        PFalg_proj_t *proj;

        opt_base_proj[0] = PFalg_proj (num_col1, num_col1);
        opt_base_proj[1] = PFalg_proj (num_col2, num_col2);
        op = PFla_project_ (
                 op,
                 L(proxy_exit)->schema.count + 2,
                 opt_base_proj);

        count = 0;
        proj = PFmalloc (op->schema.count * sizeof (PFalg_proj_t));
        for (i = 0; i < op->schema.count; i++)
            if (PFprop_ocol (num1, op->sem.proj.items[i].old))
                proj[count++] = op->sem.proj.items[i];

        num1 = PFla_project_ (num1, count, proj);

        count = 0;
        proj = PFmalloc (op->schema.count * sizeof (PFalg_proj_t));
        for (i = 0; i < op->schema.count; i++)
            if (PFprop_ocol (num2, op->sem.proj.items[i].old))
                proj[count++] = op->sem.proj.items[i];

        num2 = PFla_project_ (num2, count, proj);

    }

    rowid = PFla_rowid (op, num_col);

    new_rowid = PFla_rowid (
                     PFla_eqjoin (
                         num2,
                         PFla_eqjoin (
                             num1,
                             PFla_distinct (
                                 PFla_project_ (
                                     PFla_eqjoin (
                                         PFla_project_ (
                                             rowid,
                                             3, left_proj),
                                         rp,
                                         num_col_alias,
                                         join_att2),
                                     2, dist_proj)),
                             num_col1,
                             num_col_alias1),
                         num_col2,
                         num_col_alias2),
                     num_col);

    *proxy_entry = *PFla_project_ (
                        new_rowid,
                        proxy_entry->schema.count,
                        proxy_proj);

    if (leaf_ref == MULTIPLE) {
        /* Split up the references to the proxy exit: The modified
           proxy exit (operator: exit_op) is input to the references
           inside the proxy. The references from outside the proxy
           are linked to the new entry of the proxy as they discard
           the tuples pruned inside anyway. */
        /* hide the two new columns num_col1 and num_col2 by
           introducing the following projection */
        PFla_op_t *exit_op = PFla_project_ (rowid,
                                            proxy_exit->schema.count,
                                            exit_proj);

        for (i = 0; i < PFarray_last (exit_refs); i++) {
            if (L(*(PFla_op_t **) PFarray_at (exit_refs, i)) == proxy_exit)
                L(*(PFla_op_t **) PFarray_at (exit_refs, i)) = exit_op;
            if (R(*(PFla_op_t **) PFarray_at (exit_refs, i)) == proxy_exit)
                R(*(PFla_op_t **) PFarray_at (exit_refs, i)) = exit_op;
        }

        /* hide the two new columns num_col1 and num_col2 by
           introducing the following projection */
        *proxy_exit = *PFla_project_ (new_rowid,
                                      proxy_exit->schema.count,
                                      exit_proj);
    } else
        /* hide the two new columns num_col1 and num_col2 by
           introducing the following projection */
        *proxy_exit = *PFla_project_ (
                           rowid,
                           proxy_exit->schema.count,
                           exit_proj);

    /* add the new equi-joins to the list of already checked nodes */
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(new_rowid);
    *(PFla_op_t **) PFarray_add (checked_nodes) = R(L(new_rowid));
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(L(R(R(L(new_rowid)))));

    return true;
}




/**
 *
 * Functions specific to joins that are unnested
 * (independent expressions on the left and the right side).
 *
 */

/**
 * proxy_nest_entry checks if a node is a equi-join on domains
 * that only have a common super domain but the domains are
 * no subdomain of each other.
 */
static bool
proxy_nest_entry (PFla_op_t *p)
{
    if (p->kind != la_eqjoin)
        return false;

    if (PFprop_key_left (p->prop, p->sem.eqjoin.att1) &&
        PFprop_subdom (p->prop,
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2),
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1)))
        return false;

    if (PFprop_key_right (p->prop, p->sem.eqjoin.att2) &&
        PFprop_subdom (p->prop,
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1),
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2)))
        return false;

    return true;
}

/**
 * proxy_nest_exit looks for the rowid operator that generated
 * the joins columns.
 */
static bool
proxy_nest_exit (PFla_op_t *p, PFla_op_t *entry)
{
    PFla_op_t *cur;
    dom_t att1_dom, att2_dom, super_dom;

    if (p->kind != la_rowid)
        return false;

    /* only unnest if the rewrite seems promising -- if a cross product
       lies in wait for further optimization */
    cur = p;
    while (cur->kind == la_project ||
           cur->kind == la_rowid)
        cur = L(cur);
    if (cur->kind != la_cross)
        return false;

    /* look up the newly generated domain and the join domains */
    super_dom = PFprop_dom (p->prop, p->sem.rowid.res);
    att1_dom = PFprop_dom_left (entry->prop, entry->sem.eqjoin.att1);
    att2_dom = PFprop_dom_right (entry->prop, entry->sem.eqjoin.att2);

    /* compare the equi-join and rowid domains on subdomain relationship */
    return PFprop_subdom (p->prop, att1_dom, super_dom) &&
           PFprop_subdom (p->prop, att2_dom, super_dom);
}

/**
 * The following 3 declarations and functions are used to ensure
 * in function nest_proxy() that the two branches do not reference
 * each other.
 */
/* forward declaration */
static PFla_op_t *
find_proxy_exit (PFla_op_t *p,
                 PFla_op_t *entry,
                 PFla_op_t *exit,
                 PFarray_t *exit_refs,
                 bool (* check_exit) (PFla_op_t *, PFla_op_t *));
/* function used as function pointer argument */
static bool
proxy_nest_helper_exit (PFla_op_t *p, PFla_op_t *exit)
{
    return p == exit;
}
/* forward declaration */
static void
find_conflicts (PFla_op_t *p, PFarray_t *conflict_list);

/**
 * We rewrite a DAG of the form shown in (1) into an equivalent one
 * (shown in (2)). This allows to directly following proxy detection
 * to generate separate proxies for the two join branches.
 *
 * This rewrite is applied if both left and right branch are referenced
 * only via the eqjoin operator (no references between left and right
 * are allowed either).
 *
 * We replace the left branch by a projection (pi_(lower_left)) that
 * maps all columns available in the original rowid operator # through
 * the new join into the new right branch of the upper join. Another
 * projection (pi_(upper_right)) retrieves the original names except for
 * the rowid column -- the column generated by the new upper rowid
 * operator is used here -- and replaces the references to the original
 * rowid operator # in the original left branch.
 *
 * The projection pi_(upper_left) transports the results of the original
 * right branch to the top of the plan where the projection pi_(top)
 * adjusts the schema to the required format.
 *
 *                      |                             |
 *                     |X|                            pi_(top)
 *                   __/ \__                          |
 *                  /       \                        |X|
 *                 /         ^                    ___/ \___
 *                /\        / \                  /         \
 *               /  \      /   \                 |         /\
 *              /left\    /right\                |        /  \
 *             /______\  /_______\               |       /left\
 *                |          |                   |      /______\
 *                \____  ____/                   |          |
 *                     \/                        |     pi_(upper_right)
 *                     #                         |          |
 *                     |                   pi_(upper_left)  |
 *              (     ... *                      \___   ____/
 *                     |                             \ /
 *                     X                              #
 *                    / \                             |
 *                  ... ...    )                     |X|
 *                                                ___/ \___
 *                                               /         \
 *                   ( 1 )                 pi_(lower_left)  ^
 *                                               |         / \
 *                                               |        /   \
 *  (*) ... represents an arbitrary number       |       /right\
 *      of the following operators:              |      /_______\
 *      - project                                |          |
 *      - attach                                 \___   ____/
 *                                                   \ /
 *                                                    #
 *                                                    |
 *                                             (     ... *
 *                                                    |
 *                                                    X
 *                                                   / \
 *                                                 ... ...    )
 *
 *
 *                                                  ( 2 )
 */
static bool
nest_proxy (PFla_op_t *root,
            PFla_op_t *proxy_entry,
            PFla_op_t *proxy_exit,
            PFarray_t *conflict_list,
            PFarray_t *exit_refs,
            PFarray_t *checked_nodes)
{
    PFla_op_t    *node,
                 *new_num,
                 *pi_upper_right,
                 *res;
    PFalg_att_t   new_num_col,
                  used_cols,
                  cur_col,
                  new_col;
    unsigned int  i,
                  last;
    PFalg_proj_t *top_proj,
                 *upper_left_proj,
                 *upper_right_proj,
                 *lower_left_proj;
    PFarray_t    *left_exit_refs;

    (void) root;

    /* remove entry and exit references */
    last = PFarray_last (conflict_list);
    while (last) {
        node = *(PFla_op_t **) PFarray_top (conflict_list);
        if (proxy_exit == node || proxy_entry == node) {
            PFarray_del (conflict_list);
            last--;
        } else
            return false;
    }

    /* In the following we assure that no node of the
       left branch references a node of the right branch
       and vice versa.
       A side-effect is the list of exit references
       of the left branch. It will be later used to replace
       the rowid operator. (We thus do not need the
       list of exit references for both branches.) */
    left_exit_refs = PFarray (sizeof (PFla_op_t *));
    (void) exit_refs;

    /* mark nodes in the left branch as INside and
       collect the references to the exit node */
    find_proxy_exit (L(proxy_entry),
                     proxy_exit,
                     proxy_exit,
                     left_exit_refs,
                     proxy_nest_helper_exit);
    /* mark nodes in the right branch as OUTside and
       collect the conflicts */
    find_conflicts (R(proxy_entry), conflict_list);

    PFla_in_out_reset (proxy_entry);
    PFla_dag_reset (proxy_entry);

    /* remove exit references --
       if some other nodes are referenced
       on both sides then bail out */
    last = PFarray_last (conflict_list);
    while (last) {
        node = *(PFla_op_t **) PFarray_top (conflict_list);
        if (proxy_exit == node) {
            PFarray_del (conflict_list);
            last--;
        } else
            return false;
    }

    /* prepare the projection lists */
    top_proj         = PFmalloc (proxy_entry->schema.count *
                                sizeof (PFalg_proj_t));
    upper_left_proj  = PFmalloc ((R(proxy_entry)->schema.count + 1) *
                                 sizeof (PFalg_proj_t));
    upper_right_proj = PFmalloc (proxy_exit->schema.count *
                                 sizeof (PFalg_proj_t));
    lower_left_proj  = PFmalloc (proxy_exit->schema.count *
                                 sizeof (PFalg_proj_t));

    /* Generate the upper left projection and create a list
       of used variables. This list is then used to avoid
       naming conflicts at the lower join and the above rowid
       operator. */
    used_cols = proxy_entry->sem.eqjoin.att1;

    for (i = 0; i < L(proxy_entry)->schema.count; i++)
        used_cols = used_cols | L(proxy_entry)->schema.items[i].name;

    for (i = 0; i < R(proxy_entry)->schema.count; i++) {
        used_cols = used_cols | R(proxy_entry)->schema.items[i].name;
        upper_left_proj[i] = PFalg_proj (
                                 R(proxy_entry)->schema.items[i].name,
                                 R(proxy_entry)->schema.items[i].name);
    }

    /* Generate a new column name that will hold the values of the
       new upper rowid operator... */
    new_num_col = PFalg_ori_name (
                      PFalg_unq_name (proxy_exit->sem.rowid.res, 0),
                      ~used_cols);
    used_cols = used_cols | new_num_col;

    /* add the column to the projection list of the left branch
       of the upper join thus adjusting the cardinality of
       the right output */
    upper_left_proj[R(proxy_entry)->schema.count] = PFalg_proj (new_num_col,
                                                                new_num_col);

    /* generate the top projection. It prunes the rowid columns
       of the upper join and adds a new column holding the content
       of the original rowid column. */
    for (i = 0; i < proxy_entry->schema.count; i++)
        if (proxy_entry->sem.eqjoin.att1 !=
            proxy_entry->schema.items[i].name)
            top_proj[i] = PFalg_proj (
                              proxy_entry->schema.items[i].name,
                              proxy_entry->schema.items[i].name);
        else
            top_proj[i] = PFalg_proj (
                              proxy_entry->sem.eqjoin.att1,
                              proxy_entry->sem.eqjoin.att2);

    /* generate the two projection lists that map the input to
       the originally left (now upper) branch without name
       conflicts */
    for (i = 0; i < proxy_exit->schema.count; i++) {
        cur_col = proxy_exit->schema.items[i].name;

        if (cur_col != proxy_exit->sem.rowid.res) {
            new_col = PFalg_ori_name (PFalg_unq_name (cur_col, 0), ~used_cols);
            used_cols = used_cols | new_col;

            lower_left_proj[i] = PFalg_proj (new_col, cur_col);
            upper_right_proj[i] = PFalg_proj (cur_col, new_col);
        }
        else {
            lower_left_proj[i] = PFalg_proj (proxy_entry->sem.eqjoin.att1,
                                             cur_col);
            upper_right_proj[i] = PFalg_proj (cur_col, new_num_col);
        }
    }

    /* create the new lower join containing only
       the originally right branch */
    new_num = PFla_rowid (
                  PFla_eqjoin (
                      PFla_project_ (
                          proxy_exit,
                          proxy_exit->schema.count,
                          lower_left_proj),
                      R(proxy_entry),
                      proxy_entry->sem.eqjoin.att1,
                      proxy_entry->sem.eqjoin.att2),
                  new_num_col);

    /* create the projection that builds the input
       to the originally left branch */
    pi_upper_right = PFla_project_ (
                         new_num,
                         proxy_exit->schema.count,
                         upper_right_proj);

    /* bind all references inside the left branch to the
       mapping projection pi_upper_right */
    for (i = 0; i < PFarray_last (left_exit_refs); i++) {
        node = *(PFla_op_t **) PFarray_at (left_exit_refs, i);
        if (L(node) == proxy_exit)
            L(node) = pi_upper_right;
        if (R(node) == proxy_exit)
            R(node) = pi_upper_right;
    }

    /* Create the upper join containing the originally left branch
       and the results of the originally right branch ... */
    res = PFla_project_ (
              PFla_eqjoin (
                  PFla_project_ (
                      new_num,
                      R(proxy_entry)->schema.count + 1,
                      upper_left_proj),
                  L(proxy_entry),
                  new_num_col,
                  proxy_entry->sem.eqjoin.att1),
              proxy_entry->schema.count,
              top_proj);

    /* ... and replace the old proxy entry operator with the two
       nested joins. */
    *proxy_entry = *res;

    /* make sure the new created joins are not checked in the next
       traversal */
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(res);
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(new_num);

    return true;
}




/**
 *
 * Functions specific to the generation of duplicate generating
 * XPath location step.
 */

/**
 * Nothing needed up front.
 */
static void
step_join_prepare (PFla_op_t *root)
{
    (void) root;
}

/**
 * step_join_entry detects an expression
 * of the following form:
 *
 *        |X|
 *      __/ \__
 *     /       \
 *    (pi)      |
 *    |         |
 *   step     (pi)
 *    |         |
 *    (pi)      |
 *     \__   __/
 *        \ /
 *         #
 */
static bool
step_join_entry (PFla_op_t *p)
{
    PFalg_att_t join_att;
    PFla_op_t  *cur,
               *rowid  = NULL,
               *step   = NULL;

    if (p->kind != la_eqjoin)
        return false;

    /* check left side */
    cur = L(p);
    join_att = p->sem.eqjoin.att1;
    /* cope with projection */
    if (cur->kind == la_project) {
        for (unsigned int i = 0; i < cur->sem.proj.count; i++)
            if (cur->sem.proj.items[i].new == join_att) {
                join_att = cur->sem.proj.items[i].old;
                break;
            }
        cur = L(cur);
    }

    if (cur->kind == la_rowid &&
        cur->sem.rowid.res == join_att)
        rowid = cur;
    else if ((cur->kind == la_step || cur->kind == la_guide_step) &&
        cur->sem.step.iter == join_att) {
        cur = R(cur);

        /* cope with projection */
        if (cur->kind == la_project) {
            for (unsigned int i = 0; i < cur->sem.proj.count; i++)
                if (cur->sem.proj.items[i].new == join_att) {
                    join_att = cur->sem.proj.items[i].old;
                    break;
                }
            cur = L(cur);
        }

        if (cur->kind == la_rowid &&
            cur->sem.rowid.res == join_att)
            step = cur;
    } else
        return false;

    /* check right side */
    cur = R(p);
    join_att = p->sem.eqjoin.att2;
    /* cope with projection */
    if (cur->kind == la_project) {
        for (unsigned int i = 0; i < cur->sem.proj.count; i++)
            if (cur->sem.proj.items[i].new == join_att) {
                join_att = cur->sem.proj.items[i].old;
                break;
            }
        cur = L(cur);
    }

    if (cur->kind == la_rowid &&
        cur->sem.rowid.res == join_att &&
        !rowid)
        rowid = cur;
    else if ((cur->kind == la_step || cur->kind == la_guide_step) &&
        cur->sem.step.iter == join_att &&
        !step) {
        cur = R(cur);

        /* cope with projection */
        if (cur->kind == la_project) {
            for (unsigned int i = 0; i < cur->sem.proj.count; i++)
                if (cur->sem.proj.items[i].new == join_att) {
                    join_att = cur->sem.proj.items[i].old;
                    break;
                }
            cur = L(cur);
        }

        if (cur->kind == la_rowid &&
            cur->sem.rowid.res == join_att)
            step = cur;
    } else
        return false;

    return step && step == rowid;
}

/**
 * step_join_exit just tests whether the previously detected
 * rowid operator matches the current one.
 */
static bool
step_join_exit (PFla_op_t *p, PFla_op_t *entry)
{
    PFla_op_t *cur;

    if (p->kind != la_rowid)
        return false;

    cur = L(entry);
    while (cur && cur->kind != la_rowid) {
        switch (cur->kind) {
            case la_project:
                cur = L(cur);
                break;

            case la_step:
            case la_guide_step:
                cur = R(cur);
                break;

            default:
                cur = NULL;
        }
    }
    return cur == p;
}

/**
 * intro_step_join replaces the pattern
 * detected above
 *
 *        |X|
 *      __/ \__
 *     /       \
 *    (pi)      |
 *    |         |
 *   step     (pi)
 *    |         |
 *    (pi)      |
 *     \__   __/
 *        \ /
 *         #
 *
 * (if there are no outside references) into
 * a duplicate aware step operator step_join.
 * A projection on top of the new operator ensures
 * the correct mapping of the columns.
 */
static bool
intro_step_join (PFla_op_t *root,
                  PFla_op_t *proxy_entry,
                  PFla_op_t *proxy_exit,
                  PFarray_t *conflict_list,
                  PFarray_t *exit_refs,
                  PFarray_t *checked_nodes)
{
    PFla_op_t    *step,
                 *proj = NULL,
                 *cur;
    PFalg_proj_t *proj_list;
    PFalg_att_t   join_att,
                  item_res,
                  item = 0,
                  item_proj = 0,
                  used_cols = 0;
    unsigned int  last,
                  i,
                  count = 0;

    (void) root;
    (void) exit_refs;
    (void) checked_nodes;

    /* remove entry and exit references */
    last = PFarray_last (conflict_list);
    while (last) {
        cur = *(PFla_op_t **) PFarray_top (conflict_list);
        if (proxy_exit == cur || proxy_entry == cur) {
            PFarray_del (conflict_list);
            last--;
        } else
            return false;
    }

    /* detect in which branch of the eqjoin operator
       the step resides and prepare the transformation */
    if (L(proxy_entry)->kind == la_step ||
        L(proxy_entry)->kind == la_guide_step) {
        join_att = proxy_entry->sem.eqjoin.att2;
        step     = L(proxy_entry);
        cur      = R(proxy_entry);
    } else if (L(proxy_entry)->kind == la_project &&
               (LL(proxy_entry)->kind == la_step ||
                LL(proxy_entry)->kind == la_guide_step)) {
        join_att = proxy_entry->sem.eqjoin.att2;
        proj     =  L(proxy_entry);
        step     = LL(proxy_entry);
        cur      =  R(proxy_entry);
    } else if (R(proxy_entry)->kind == la_step ||
               R(proxy_entry)->kind == la_guide_step) {
        join_att = proxy_entry->sem.eqjoin.att1;
        step     = R(proxy_entry);
        cur      = L(proxy_entry);
    } else if (R(proxy_entry)->kind == la_project &&
               (RL(proxy_entry)->kind == la_step ||
                RL(proxy_entry)->kind == la_guide_step)) {
        join_att = proxy_entry->sem.eqjoin.att1;
        proj     =  R(proxy_entry);
        step     = RL(proxy_entry);
        cur      =  L(proxy_entry);
    } else
        PFoops (OOPS_FATAL, "Proxy pattern does not match");


    /* prepare projection list for the mapping projection */
    proj_list = PFmalloc (proxy_entry->schema.count * sizeof (PFalg_proj_t));

    /* Fill the name pairs of the projection list with the tuples streaming
       through the non-step branch of the eqjoin (as well as the one for
       the second join argument). */
    if (cur->kind == la_project) {
        for (i = 0; i < cur->sem.proj.count; i++) {
            if (cur->sem.proj.items[i].new == join_att) {
                proj_list[count++] = PFalg_proj (proxy_entry->sem.eqjoin.att1,
                                                 cur->sem.proj.items[i].old);
                proj_list[count++] = PFalg_proj (proxy_entry->sem.eqjoin.att2,
                                                 cur->sem.proj.items[i].old);
            } else
                proj_list[count++] = cur->sem.proj.items[i];
        }
    } else {
        for (i = 0; i < cur->schema.count; i++) {
            if (cur->schema.items[i].name == join_att) {
                proj_list[count++] = PFalg_proj (proxy_entry->sem.eqjoin.att1,
                                                 cur->schema.items[i].name);
                proj_list[count++] = PFalg_proj (proxy_entry->sem.eqjoin.att2,
                                                 cur->schema.items[i].name);
            } else
                proj_list[count++] = PFalg_proj (cur->schema.items[i].name,
                                                 cur->schema.items[i].name);
        }
    }
    /* Collect the names of the inputs to ensure that the step_join
       creates a new column name. */
    for (i = 0; i < proxy_exit->schema.count; i++)
        used_cols = used_cols | proxy_exit->schema.items[i].name;

    /* Get the column name providing the context
       nodes of the path step */
    if (R(step)->kind == la_project) {
        for (i = 0; i < R(step)->sem.proj.count; i++)
            if (step->sem.step.item ==
                R(step)->sem.proj.items[i].new) {
                item = R(step)->sem.proj.items[i].old;
                break;
            }

        assert (item);
    } else
        item = step->sem.step.item;

    /* Ensure that the context node column is not the same
       as the resulting column of the new path step */
    used_cols = used_cols | item;

    /* Create a new column name for the result of the new path step. */
    item_res = PFalg_ori_name (PFalg_unq_name (att_item, 0), ~used_cols);

    /* Get the column of the resulting item column of the step */
    if (proj) {
        assert (proj->sem.proj.count <= 2);

        for (i = 0; i < proj->sem.proj.count; i++)
            if (step->sem.step.item ==
                proj->sem.proj.items[i].old) {
                item_proj = proj->sem.proj.items[i].new;
                break;
            }
    } else
        item_proj = step->sem.step.item;

    /* If the column generated by the path step is in the overall
       result, then add it to the projection list. */
    if (item_proj) {
        proj_list[count++] = PFalg_proj (item_proj, item_res);
    }

    /* Replace the detected pattern by the new duplicate generating step
       and a name mapping projection on top of it. */
    if (step->kind == la_step)
        *proxy_entry = *PFla_project_ (
                            PFla_step_join (
                                L(step),
                                proxy_exit,
                                step->sem.step.axis,
                                step->sem.step.ty,
                                step->sem.step.level,
                                item,
                                item_res),
                            proxy_entry->schema.count,
                            proj_list);
    else
        *proxy_entry = *PFla_project_ (
                            PFla_guide_step_join (
                                L(step),
                                proxy_exit,
                                step->sem.step.axis,
                                step->sem.step.ty,
                                step->sem.step.guide_count,
                                step->sem.step.guides,
                                step->sem.step.level,
                                item,
                                item_res),
                            proxy_entry->schema.count,
                            proj_list);
    return true;
}





/**
 *
 * Functions specific to the kind=1 proxy generation.
 * (eqjoin - rowid delimited)
 *
 */

/**
 * join_prepare infers all properties that are required to find the
 * correct proxy nodes.
 */
static void
join_prepare (PFla_op_t *root)
{
    PFprop_infer_key (root);
    /* key property inference already requires
       the domain property inference for native
       types. Thus we can skip it:
    PFprop_infer_nat_dom (root);
    */
}

/**
 * join_entry checks if a node is a key equi-join on subdomains.
 */
static bool
join_entry (PFla_op_t *p)
{
    if (p->kind != la_eqjoin)
        return false;

    if (PFprop_key_left (p->prop, p->sem.eqjoin.att1) &&
        PFprop_subdom (p->prop,
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2),
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1)))
        return true;

    if (PFprop_key_right (p->prop, p->sem.eqjoin.att2) &&
        PFprop_subdom (p->prop,
                       PFprop_dom_left (p->prop,
                                        p->sem.eqjoin.att1),
                       PFprop_dom_right (p->prop,
                                         p->sem.eqjoin.att2)))
        return true;

    return false;
}

/**
 * join_exit looks for the rowid or rownum operator that generated
 * the joins columns.
 */
static bool
join_exit (PFla_op_t *p, PFla_op_t *entry)
{
    dom_t entry_dom, dom;

    if (p->kind != la_rownum && p->kind != la_rowid)
        return false;

    /* only allow key columns and look up the newly generated domain */
    if (p->kind == la_rownum) {
        if (p->sem.sort.part) return false;
        dom = PFprop_dom (p->prop, p->sem.sort.res);
    } else {
        dom = PFprop_dom (p->prop, p->sem.rowid.res);
    }

    /* look up the super domain of the two join attributes */
    if (PFprop_key_right (entry->prop, entry->sem.eqjoin.att2) &&
        PFprop_subdom (entry->prop,
                       PFprop_dom_left (entry->prop,
                                        entry->sem.eqjoin.att1),
                       PFprop_dom_right (entry->prop,
                                         entry->sem.eqjoin.att2)))
        entry_dom = PFprop_dom_right (entry->prop, entry->sem.eqjoin.att2);
    else
        entry_dom = PFprop_dom_left (entry->prop, entry->sem.eqjoin.att1);

    /* compare the equi-join and rownum/rowid domains
       on equality */
    return PFprop_subdom (p->prop, dom, entry_dom) &&
           PFprop_subdom (p->prop, entry_dom, dom);
}

/**
 * We rewrite a DAG fragment of the form shown in (1). To avoid
 * rewriting large parts of the sub-DAG t1 we build a set of operators
 * around it (see Figure (2)).
 *
 * The rowid operator at the base of the fragment stays
 * outside of the proxy pattern to correctly cope with references from
 * outside of the new proxy. All references are linked to the new
 * projection pi_(exit). A new rowid operator (#_(new_num_col))
 * introduces a new numbering that is only used for mapping inside the
 * proxy. (It flows trough t1 as the projection pi_(exit) replaces
 * the rowid column num_col with the new one.) The projection pi_(left)
 * maps the 'old' num_col with a new equi-join and the projection pi_(entry)
 * maps all columns to names of the base. The rational behind this decision
 * is to make the proxy an 'operator' that only generates new columns (and
 * possibly removes some), but does not rename a column. The projection
 * pi_(proxy) removes the intermediate new rowid columns (new_num_col
 * and its join alias). The proxy operator stores a reference to its base
 * in its semantical field (see dotted lines in (2)) and a projection
 * above replaces the old proxy entry and maps the column to its original
 * name.
 * Note that the projections pi_(base) and pi_(entry) both remove duplicate
 * columns that an unambiguous mapping between column names is possible.
 * (Otherwise the proxy might name the same columns differently at
 * the proxy entry and the proxy exit.)
 *
 *
 *             |                            |
 *            |X|                           pi_(above)
 *            / \                           |
 *           /   \                        proxy (kind=1)
 *          /   / \             _ _ _ _ __/ |  \_ _ __
 *          |  / t1\           /(sem.base1) pi_(proxy)\(sem.ref)
 *          | /_____\          |            |          |
 *          |    |                         |X|
 *          \   /              |          /   \        |
 *           \ /                         /  pi_(entry)
 *            #_(num_col)      |        |      |       |
 *                                      |     |X|
 *                             |        |     / \      |
 *                                      |    /  |
 *                             |   pi_(left)/  / \     |
 *                                      |  /  / t1\
 *                             |        |  | /_____\   |
 *                                      |  \__ / _ _ _/
 *                             |        |     \|/
 *                                      |   pi_(exit)
 *                             |        |      |
 *                                      \___   |
 *                             |            \ /
 *                             \ _ _ _       #_(new_num_col)
 *                                    \     /
 *                                  proxy_base
 *                                       |
 *                                      pi_(base)
 *                                       |
 *                                       #_(num_col)
 *
 *            ( 1 )                    ( 2 )
 *
 */
static bool
generate_join_proxy (PFla_op_t *root,
                     PFla_op_t *proxy_entry,
                     PFla_op_t *proxy_exit,
                     PFarray_t *conflict_list,
                     PFarray_t *exit_refs,
                     PFarray_t *checked_nodes)
{
    PFalg_att_t num_col, new_num_col, num_col_alias, new_num_col_alias,
                icols = 0, used_cols = 0;
    unsigned int i, j, k, count, dist_count, base_count;
    PFalg_attlist_t req_cols, new_cols,
                    trace_ori, trace_mod;
    PFla_op_t *num_op, *exit_op, *entry_op, *proxy_op, *base_op;

    /* over estimate the projection size */
    PFalg_proj_t *base_proj  = PFmalloc (proxy_exit->schema.count *
                                         sizeof (PFalg_proj_t));
    PFalg_proj_t *exit_proj  = PFmalloc (proxy_exit->schema.count *
                                         sizeof (PFalg_proj_t));
    PFalg_proj_t *above_proj = PFmalloc (proxy_entry->schema.count *
                                         sizeof (PFalg_proj_t));
    PFalg_proj_t *left_proj  = PFmalloc (2 * sizeof (PFalg_proj_t));
    /* over estimate the projection size */
    PFalg_proj_t *proxy_proj = PFmalloc ((proxy_entry->schema.count - 1) *
                                         sizeof (PFalg_proj_t));
    /* over estimate the projection size */
    PFalg_proj_t *entry_proj = PFmalloc ((proxy_entry->schema.count - 1) *
                                         sizeof (PFalg_proj_t));

    /* skip proxy generation if some conflicts cannot be resolved */
    if (!join_resolve_conflicts (proxy_entry,
                                 proxy_exit,
                                 conflict_list,
                                 /* as we might also rewrite nodes
                                    referencing the exit we have
                                    to update the exit_refs list
                                    as well */
                                 exit_refs))
        return false;

    /* Discard proxies with rownum operators as these would probably never
       benefit from the rewrites based on proxies. In addition checking the
       usage of the rownum column requires quite some work. */
    if (proxy_exit->kind == la_rownum)
        return false;

    /* prepare the list of attributes that are input to the proxy operator */
    trace_ori.count = proxy_entry->schema.count;
    trace_ori.atts = PFmalloc (proxy_entry->schema.count *
                               sizeof (PFalg_attlist_t));
    for (i = 0; i < proxy_entry->schema.count; i++)
        trace_ori.atts[i] = proxy_entry->schema.items[i].name;

    /* assign unique names to track the names inside the proxy body */
    trace_mod = PFprop_trace_names (proxy_entry, proxy_exit, trace_ori);

    /* assign unique names to find identical columns with different names */
    PFprop_infer_unq_names (root);

    /* short-hand for the key column name */
    num_col = proxy_exit->sem.rowid.res;

    /* Skip the first entry of the exit_proj projection list
       (-- it will be filled in after collecting all the used column
        names and generating a new free one). */
    count = 1;
    base_count = 0;

    /* collect the list of differing column names at the base
       of the proxy, all the used column names and create a projection
       list that reconstructs the schema of the proxy base except for
       the rowid column. */
    for (i = 0; i < proxy_exit->schema.count; i++) {
        PFalg_att_t exit_col = proxy_exit->schema.items[i].name;
        used_cols = used_cols | exit_col;

        for (j = 0; j < base_count; j++)
            /* use unique names to get rid of duplicate columns */
            if (PFprop_unq_name (proxy_exit->prop, base_proj[j].new) ==
                PFprop_unq_name (proxy_exit->prop, exit_col)) {
                exit_proj[count++] = PFalg_proj (exit_col, base_proj[j].new);
                break;
            }

        if (j == base_count) {
            base_proj[base_count++] = PFalg_proj (exit_col, exit_col);
            /* num_col only occurs once --
               that's why we only have to cope at this place */
            if (num_col != exit_col)
                exit_proj[count++] = PFalg_proj (exit_col, exit_col);
        }

    }
    assert (proxy_exit->schema.count == count);

    /* Generate a new column name that will hold the values of the
       new nested rowid operator... */
    new_num_col = PFalg_ori_name (
                      PFalg_unq_name (num_col, 0),
                      ~used_cols);
    used_cols = used_cols | new_num_col;

    /* ... and replace the rowid column by the column generated
       by the new nested rowid operator */
    exit_proj[0] = PFalg_proj (num_col, new_num_col);

    /* In addition create two names for mapping the old as well
       as the new rowid operators... */
    new_num_col_alias = PFalg_ori_name (
                            PFalg_unq_name (num_col, 0),
                            ~used_cols);
    used_cols = used_cols | new_num_col_alias;
    num_col_alias = PFalg_ori_name (
                        PFalg_unq_name (num_col, 0),
                        ~used_cols);
    used_cols = used_cols | num_col_alias;

    /* ... and add them to the mapping projection list. */
    left_proj[0] = PFalg_proj (new_num_col_alias, new_num_col);
    left_proj[1] = PFalg_proj (num_col_alias, num_col);

    /* store the new columns (an upper limit is the whole relation) */
    new_cols.count = 0;
    new_cols.atts = PFmalloc (proxy_entry->schema.count *
                              sizeof (PFalg_attlist_t));

    /* reset counters for the projection lists */
    count = 0;
    dist_count = 0;

    /* Add the second join argument to the projection that replaces
       proxy entry join. This will ensure that there are no dangling
       references. */
    above_proj[count++] = PFalg_proj (proxy_entry->sem.eqjoin.att2,
                                      num_col);

    /* map the input to the output names.
       For new columns create new 'free' names */
    for (i = 0; i < proxy_entry->schema.count; i++) {
        PFalg_att_t entry_col = proxy_entry->schema.items[i].name;
        PFalg_att_t base_col = att_NULL;

        /* discard the second join argument in the projection lists.
           (It is already added to the outermost projection -- above_proj --
            to keep the plan consistent.) */
        if (entry_col == proxy_entry->sem.eqjoin.att2)
            continue;

        /* look up the original name of the entry column */
        for (j = 0; j < trace_ori.count; j++)
            if (entry_col == trace_ori.atts[j])
                base_col = trace_mod.atts[j];

        /* check for duplicate column names */
        for (k = 0; k < dist_count; k++)
            /* use unique names to get rid of duplicate columns */
            if (PFprop_unq_name (proxy_entry->prop,
                                 entry_proj[k].old) ==
                PFprop_unq_name (proxy_entry->prop, entry_col)) {
                /* Create a projection list that maps the output
                   columns of the proxy operator such that the
                   plan stays consistent. */
                above_proj[count++] = PFalg_proj (entry_col,
                                                  entry_proj[k].new);
                break;
            }

        /* discard duplicate columns (thus keep a distinct list of columns */
        if (k == dist_count) {
            /* For each entry column we try to find the respective exit
               column by comparing the unique names. */
            for (j = 0; j < base_count; j++) {
                PFalg_att_t exit_col = base_proj[j].new;
                /* check whether this is an unchanged input column */
                if (base_col && base_col == exit_col) {
                    /* map output name to the same name as the input column */
                    entry_proj[dist_count] = PFalg_proj (exit_col, entry_col);
                    /* keep input names and replace the 'new_num_col'
                       (disguised as 'num_col') by the real 'num_col' */
                    proxy_proj[dist_count] = exit_col == num_col
                                             ? PFalg_proj (exit_col,
                                                           num_col_alias)
                                             : PFalg_proj (exit_col,
                                                           exit_col);
                    dist_count++;
                    /* Create a projection list that maps the output
                       columns of the proxy operator such that the
                       plan stays consistent. */
                    above_proj[count++] = PFalg_proj (entry_col, exit_col);
                    break;
                }
            }
            /* create names for the new columns and prepare icols list */
            if (j == base_count) {
                /* create new column name */
                PFalg_att_t new_exit_col = PFalg_ori_name (
                                               PFalg_unq_name (entry_col, 0),
                                               ~used_cols);
                used_cols = used_cols | new_exit_col;
                /* add column to the list of new columns */
                new_cols.atts[new_cols.count++] = new_exit_col;
                /* map columns similar to the above mapping (for already
                   existing columns) */
                entry_proj[dist_count] = PFalg_proj (new_exit_col, entry_col);
                proxy_proj[dist_count] = PFalg_proj (new_exit_col, new_exit_col);
                dist_count++;
                above_proj[count++] = PFalg_proj (entry_col, new_exit_col);

                /* collect all the columns that were generated
                   in the proxy body */
                icols = icols | entry_col;
            }
        }
    }
    assert (count == proxy_entry->schema.count);

    /* We are certain that this join is only a mapping join
       (see join_entry ()). Any input column (at the proxy base)
       that is propagated along the left *and* the right side
       of the equi-join and which is not modified contains
       the same values and can be pruned from the entry_proj
       projection list. Here we can prune one of its occurrences
       in the above renaming projection list. */
    for (i = 0; i < dist_count; i++)
        for (j = i+1; j < dist_count; j++)
            if (entry_proj[i].new == entry_proj[j].new) {
                /* copy the last projection pair to the
                   current column that appears twice */
                dist_count--;
                entry_proj[i] = entry_proj[dist_count];
                proxy_proj[i] = proxy_proj[dist_count];
                i--;
                break;
            }

    /* Create a proxy base with a projection that removes duplicate
       columns on top of the proxy exit. (If this rowid operator
       is not referenced above it will be removed by a following icol
       optimization phase.) */
    base_op = PFla_proxy_base (
                  PFla_project_ (proxy_exit,
                                 base_count,
                                 base_proj));
    /* Create a new rowid operator which will replace the old key column
       and will be used for the mapping of the old key column as well. */
    num_op = PFla_rowid (base_op, new_num_col);
    /* Create a projection that replaces the old by the new rowid column. */
    exit_op = PFla_project_ (num_op, proxy_exit->schema.count, exit_proj);

    /* Link the projection to all operators inside the proxy body which
       reference the proxy exit. */
    for (i = 0; i < PFarray_last (exit_refs); i++) {
        if (L(*(PFla_op_t **) PFarray_at (exit_refs, i)) == proxy_exit)
            L(*(PFla_op_t **) PFarray_at (exit_refs, i)) = exit_op;
        if (R(*(PFla_op_t **) PFarray_at (exit_refs, i)) == proxy_exit)
            R(*(PFla_op_t **) PFarray_at (exit_refs, i)) = exit_op;
    }

    /* Start icols property inference with the collected 'new' columns
       ON THE MODIFIED DAG */
    PFprop_infer_icol_specific (proxy_entry, icols);

    /* All the columns that are required at the exit point are required columns
       for the proxy */
    req_cols.atts = PFmalloc (PFprop_icols_count (base_op->prop) *
                              sizeof (PFalg_attlist_t));
    count = 0;
    /* copy all 'required' columns */
    for (i = 0; i < base_op->schema.count; i++)
        if (num_col != base_op->schema.items[i].name &&
            PFprop_icol (base_op->prop, base_op->schema.items[i].name))
            req_cols.atts[count++] = base_op->schema.items[i].name;
    /* adjust the count */
    req_cols.count = count;

    /* We are certain that this join is only a mapping join
       (see join_entry ()). Any input column (at the proxy base)
       that is propagated along the left *and* the right side
       of the equi-join and which is not modified contains
       the same values and can be pruned from the entry_proj
       projection list. Here we can prune one of its occurrences
       in the above renaming projection list. */
    for (i = 0; i < dist_count; i++)
        for (j = i+1; j < dist_count; j++)
            if (entry_proj[i].new == entry_proj[j].new) {
                /* copy the last projection pair to the
                   current column that appears twice */
                dist_count--;
                entry_proj[i] = entry_proj[dist_count];
                proxy_proj[i] = proxy_proj[dist_count];
                i--;
                break;
            }

    /* Rebuild the proxy entry operator and extend it with a projection
       that maps the resulting column names to the names of the proxy
       base. In addition map the old key column to the resulting relation
       and project away the new 'intermediate' key columns. */
    entry_op = PFla_project_ (
                   PFla_eqjoin (
                       PFla_project_ (num_op, 2, left_proj),
                       PFla_project_ (PFla_eqjoin (
                                          L(proxy_entry), R(proxy_entry),
                                          proxy_entry->sem.eqjoin.att1,
                                          proxy_entry->sem.eqjoin.att2),
                                      dist_count,
                                      entry_proj),
                       new_num_col_alias,
                       /* 'num_col' only works because the entry projection
                          renames all columns to the column at the proxy
                          exit (based on the unmodified DAG). Thus it
                          names the join column back to the old rowid
                          operator as it didn't know the new name during
                          the inference. */
                       num_col),
                   dist_count,
                   proxy_proj);

    /* Generate the proxy operator ... */
    /* Note that the kind = 1 is arbitrarly chosen and may be anything else
       as long as it is aligned with the kind used in algebra/opt/opt_mvd.c */
    proxy_op = PFla_proxy (entry_op, 1, exit_op, base_op, new_cols, req_cols);

    /* ... and replace the old proxy entry operator with a projection that
       references the proxy operator and maps back the column names to
       the original ones generated by the proxy entry. */
    *proxy_entry = *PFla_project_ (proxy_op,
                                   proxy_entry->schema.count,
                                   above_proj);

    /* make sure the new created joins are not checked in the next
       traversal */
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(entry_op);
    *(PFla_op_t **) PFarray_add (checked_nodes) = L(R(L(entry_op)));

    return true;
}



#if 0
/**
 *
 * Functions specific to the nested proxies rewrite.
 *
 */


/**
 * proxy_unnest_resolve_conflicts discards all detected
 * proxies that reference nodes which reference nodes
 * inside the proxy.
 */
static bool
proxy_unnest_resolve_conflicts (PFla_op_t *proxy_entry,
                                PFarray_t *conflict_list)
{
    PFla_op_t *node;
    unsigned int last = PFarray_last (conflict_list);

    assert (proxy_entry);
    assert (conflict_list);

    /* remove entry and exit references */
    while (last) {
        node = *(PFla_op_t **) PFarray_top (conflict_list);
        if (proxy_entry == node) {
            PFarray_del (conflict_list);
            last--;
        } else
            return false;
    }

    return true;
}

/**
 * The nested proxy pattern requires no preparation.
 */
static void
proxy_unnest_prepare (PFla_op_t *root)
{
    (void) root;
}

/**
 * Look for proxies of kind=1 as the starting point of the
 * new proxy pattern.
 */
static bool
proxy_unnest_entry (PFla_op_t *p)
{
    return p->kind == la_proxy && p->sem.proxy.kind == 1;
}

/**
 * Look for a proxy of kind=1 that is connected with the
 * entry proxy via a special set of operators.
 */
static bool
proxy_unnest_exit (PFla_op_t *proxy, PFla_op_t *entry)
{
    PFla_op_t *p;

    if (proxy->kind != la_proxy || proxy->sem.proxy.kind != 1)
        return false;

    /* FIXME: temporarily disable rewrite if a cross product
       is placed underneath. The two proxies might be completely
       independent. */
    p = L(proxy->sem.proxy.base1);

    while (p->kind == la_project ||
           p->kind == la_rowid ||
           p->kind == la_proxy_base)
        p = L(p);

    if (p->kind == la_cross)
        return false;

    p = L(entry->sem.proxy.base1);
    while (p->kind != la_proxy) {
        switch (p->kind) {
            case la_attach:
            case la_project:
            case la_fun_1to1:
            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
            case la_bool_not:
            case la_cast:
                /* get rid of dummy nodes */
                if (L(p)->kind == la_dummy) L(p) = LL(p);
                p = L(p);
                break;

            case la_doc_access:
                /* get rid of dummy nodes */
                if (R(p)->kind == la_dummy) R(p) = RL(p);
                p = R(p);
                break;

            default:
                return false;
        }
    }
    return proxy == p;
}

/**
 * collect_mappings_worker modifies the
 * two column name lists based on the column names
 * in the first three arguments:
 * - res is set to null in req_col_names
 * - res is removed from new_col_names
 * - att1 and att2 are added to new_col_names if
 *   they are not already present.
 */
static void
collect_mappings_worker (PFalg_att_t res,
                         PFalg_attlist_t refs,
                         unsigned int req_count,
                         PFalg_proj_t *req_col_names,
                         PFarray_t *new_col_names)
{
    PFalg_att_t col;
    unsigned int i, j;
    bool att_present;

    /* reset name mapping of the result column
       in the list of required columns */
    for (i = 0; i < req_count; i++)
        if (res == req_col_names[i].old)
            req_col_names[i].old = att_NULL;

    /* Remove result column */
    for (i = 0; i < PFarray_last (new_col_names); i++) {
        col = *(PFalg_att_t *) PFarray_at (new_col_names, i);
        if (col == res) {
            *(PFalg_att_t *) PFarray_at (new_col_names, i)
                = *(PFalg_att_t *) PFarray_top (new_col_names);
            PFarray_del (new_col_names);
            i--;
        }
    }

    /* Add columns in refs if they are not already an available column. */
    for (i = 0; i < refs.count; i++) {
        att_present = false;
        for (j = 0; j < PFarray_last (new_col_names); j++) {
            col = *(PFalg_att_t *) PFarray_at (new_col_names, j);
            if (col == refs.atts[i]) {
                att_present = true;
                break;
            }
        }
        if (!att_present)
            *(PFalg_att_t *) PFarray_add (new_col_names) = refs.atts[i];
    }
}

/**
 * collect_mappings traverses a list of operators and keeps
 * track of the column names for a given list of required output
 * columns. In addition it collects the names of all columns that
 * are required for processing the operators on their way.
 */
static void
collect_mappings (PFla_op_t *p,
                  unsigned int req_count,
                  PFalg_proj_t *req_col_names,
                  PFarray_t *new_col_names)
{
    while (p->kind != la_proxy) {
        switch (p->kind) {
            case la_attach:
                collect_mappings_worker (p->sem.attach.res,
                                         PFalg_attlist_ (0, NULL),
                                         req_count,
                                         req_col_names,
                                         new_col_names);
                break;

            case la_project:
            {
                PFalg_att_t col;
                unsigned int i, j;

                for (i = 0; i < req_count; i++) {
                    col = req_col_names[i].old;
                    for (j = 0; j < p->sem.proj.count; j++)
                        if (col == p->sem.proj.items[j].new) {
                            req_col_names[i].old = p->sem.proj.items[j].old;
                            break;
                        }
                }
                for (i = 0; i < PFarray_last (new_col_names); i++) {
                    col = *(PFalg_att_t *) PFarray_at (new_col_names, i);
                    for (j = 0; j < p->sem.proj.count; j++)
                        if (col == p->sem.proj.items[j].new) {
                            *(PFalg_att_t *) PFarray_at (new_col_names, i) =
                                p->sem.proj.items[j].old;
                            break;
                        }
                }
            }   break;

            case la_fun_1to1:
                collect_mappings_worker (p->sem.fun_1to1.res,
                                         p->sem.fun_1to1.refs,
                                         req_count,
                                         req_col_names,
                                         new_col_names);
                break;

            case la_num_eq:
            case la_num_gt:
            case la_bool_and:
            case la_bool_or:
                collect_mappings_worker (p->sem.binary.res,
                                         PFalg_attlist (
                                             p->sem.binary.att1,
                                             p->sem.binary.att2),
                                         req_count,
                                         req_col_names,
                                         new_col_names);
                break;

            case la_bool_not:
                collect_mappings_worker (p->sem.unary.res,
                                         PFalg_attlist (p->sem.unary.att),
                                         req_count,
                                         req_col_names,
                                         new_col_names);
                break;

            case la_cast:
                collect_mappings_worker (p->sem.type.res,
                                         PFalg_attlist (p->sem.type.att),
                                         req_count,
                                         req_col_names,
                                         new_col_names);
                break;

            case la_doc_access:
                collect_mappings_worker (p->sem.doc_access.res,
                                         PFalg_attlist (p->sem.doc_access.att),
                                         req_count,
                                         req_col_names,
                                         new_col_names);

                p = R(p);
                continue;
                break;

            default:
                PFoops (OOPS_FATAL,
                        "Can't match the operator of kind %i in proxy"
                        "generation.", p->kind);
                break;
        }
        p = L(p);
    }

}

/**
 * We rewrite a DAG of the form shown in (1) into an equivalent one
 * (shown in (2)). Thus we avoid that the upper proxy DAG is evaluated
 * in dependence of the lower proxy. Intermediate results become much
 * smaller.
 * Some conditions have to be fulfilled to allow such a rewrite. The
 * upper proxy (proxy1) may only work on columns that are already
 * available at the bottom of the lower proxy (proxy2). This is ensured
 * by comparing the required columns of proxy1 with the new columns of
 * proxy2 and checking that the operators between the proxies do not
 * generate any of the required columns. Furthermore there may not be
 * any references to this pattern from outside it and the operators
 * between the proxies have to work on tuples only (discarding any
 * iter information).
 * If the conditions are all fulfilled we can rewrite our pattern from
 * (1) to (2):
 * - We remove the rowid operators of both proxies and replace them
 *   by a common rowid operator (plus additional projections to map
 *   the column names correctly).
 *   All columns referenced in proxy1 that are not required are linked
 *   to a dummy column to avoid dangling references -- these columns
 *   will be pruned by following icols optimization phase.
 * - The result of both proxies is joined based on the values generated
 *   in the common rowid operator. Again projections ensure the correct
 *   column name mapping.
 *   As the optional operators between the original first and original
 *   second proxy (...) may have some spare columns (the required columns
 *   of proxy1) we misuse one of them to propagate the join column. This
 *   is the reason for the projection pi_(top2) -- it maps the join column
 *   to an unused 'required' column.
 * - As a side effect the operators marking the patterns as proxies are
 *   removed.
 *
 *            |
 *          proxy1 (kind=1)
 *            |
 *            pi_(proxy)                              pi_(top)
 *            |                                         |
 *           |X|                                       |X|
 *          /   \                          ____________/ \___________
 *         /  pi_(entry)                  /                          \
 *        |      |                        |                          |
 *        |     |X|                      pi_(top1)                  pi_(mid)
 *    pi_(left) / \                       |                          |
 *        |    / t1\                      |                         ... (*)
 *        |   /_____\                     |                          |
 *        |      |                        |                         pi_(top2)
 *        |      |                        |                          |
 *        |   pi_(exit)                  |X|                        |X|
 *        |      |                      /   \                      /   \
 *        \___   |                     /  pi_(entry)              /  pi_(entry)
 *            \ /                     |      |                   |      |
 *             #_(new_num_col)        |     |X|                  |     |X|
 *             |                  pi_(left) / \              pi_(left) / \
 *         proxy1_base                |    / t1\                 |    / t2\
 *             |                      |   /_____\                |   /_____\
 *             |                      |      |                   |      |
 *            ... (*)                 |      |                   |      |
 *             |                      |   pi_(exit)              |   pi_(exit)
 *             |                      |      |                   |      |
 *           proxy2 (kind=1)          \___   |                   \___   |
 *             |                          \ /                        \ /
 *             pi_(proxy)                 pi_(base1)              pi_(base2)
 *             |                           |                          |
 *            |X|                          \___________   ____________/
 *           /   \                                     \ /
 *          /  pi_(entry)                               #_(num_col1)
 *         |      |                                     |
 *         |     |X|
 *     pi_(left) / \                                  ( 2 )
 *         |    / t2\
 *         |   /_____\             (*) ... represents an arbitrary number
 *         |      |                    of the following operators:
 *         |      |                    - project
 *         |   pi_(exit)               - attach
 *         |      |                    - +, -, *, \, %
 *         \___   |                    - eq, gt, not, and, or
 *             \ /                     - cast
 *              #_(new_num_col)        - doc_access
 *              |
 *          proxy2_base
 *              |
 *
 *            ( 1 )
 */
static bool
unnest_proxy (PFla_op_t *root,
              PFla_op_t *proxy1,
              PFla_op_t *proxy2,
              PFarray_t *conflict_list,
              PFarray_t *exit_refs,
              PFarray_t *checked_nodes)
{
    /* additional references to the nodes of the pattern */
    PFla_op_t *mid_proxy, *proxy1_base, *proxy2_base;
    /* temporary nodes */
    PFla_op_t *p;
    /* newly constructed operators */
    PFla_op_t *num_op, *proxy1_num, *proxy2_num;

    /* record the name usage/mapping of the in-between proxy */
    PFalg_proj_t *req_col_names;
    PFarray_t    *new_col_names;

    PFalg_att_t cur_col,
                map_col_old, map_col_new,
                num_col1, num_col2;
    PFalg_att_t icols, used_cols;

    unsigned int i, j,
                 req_count, top1_proj_count;

    /* projection lists required for the to-be-rewritten DAG */
    PFalg_proj_t *top_proj,
                 *top1_proj, *base1_proj,
                 *mid2_proj,
                 *top2_proj, *base2_proj;

    assert (root);
    assert (proxy1);
    assert (proxy2);
    assert (conflict_list);
    assert (exit_refs);
    assert (checked_nodes);
#ifdef NDEBUG
    /* StM: otherwise compilers (correctly) complain about these being
       unuset in case assertions are switched off */
    (void) root;
    (void) exit_refs;
    (void) checked_nodes;
#endif

    /* Skip proxy generation if operators other than proxy1
       are referenced from outside the pattern. */
    if (!proxy_unnest_resolve_conflicts (proxy1,
                                         conflict_list))
        return false;

    /* Ensure that the 'upper' proxy is not completely
       independent. (Otherwise the mvd optimization will
       rewrite it.) */
    if (!proxy1->sem.proxy.req_cols.count)
        return false;

    /* collect all required columns of the entry proxy */
    icols = 0;
    for (i = 0; i < proxy1->sem.proxy.req_cols.count; i++)
        icols = icols | proxy1->sem.proxy.req_cols.atts[i];

    /* Start icols property inference with the collected 'required' columns */
    PFprop_infer_icol_specific (proxy1->sem.proxy.base1, icols);

    /* skip proxy rewrite if the upper proxy requires
       columns generated by the base proxy */
    for (i = 0; i < proxy2->schema.count; i++) {
        cur_col = proxy2->schema.items[i].name;
        if (PFprop_icol (proxy2->prop, cur_col))
            for (j = 0; j < proxy2->sem.proxy.new_cols.count; j++)
                if (cur_col == proxy2->sem.proxy.new_cols.atts[j])
                    return false;
    }

    /**
     * collect the remaining relevant operators:
     *  - the base of the 'upper' proxy (proxy1)
     *  - the base of the 'lower' proxy (proxy2)
     *  - the beginning of the intermediate proxy (mid_proxy)
     */
    proxy1_base = proxy1->sem.proxy.base1;
    proxy2_base = proxy2->sem.proxy.base1;
    mid_proxy = L(proxy1_base);

    p = mid_proxy;
    while (p != proxy2) {
        if (p->kind == la_doc_access)
            p = R(p);
        else
            p = L(p);
    }

    /* generate an initial list of required attribute names */
    req_count     = proxy1->sem.proxy.req_cols.count;
    req_col_names = PFmalloc (req_count *
                              sizeof (PFalg_proj_t));
    for (unsigned int i = 0; i < req_count; i++) {
        cur_col = proxy1->sem.proxy.req_cols.atts[i];
        req_col_names[i] = PFalg_proj (cur_col, cur_col);
    }
    new_col_names = PFarray (sizeof (PFalg_att_t));

    /* In req_col_names we store the mappings of the required column
       names (collect_mappings updates the columns names as side effect)
       and in new_col_names we collect (as side effect as well) the list
       of columns that is required by the in-between proxy. */
    collect_mappings (mid_proxy,
                      req_count,
                      req_col_names,
                      new_col_names);

    /* Ensure that the intermediate proxy does not generate
       the columns required by the upper proxy. */
    for (i = 0; i < req_count; i++)
        if (!req_col_names[i].old)
            return false;

    /* dummy initialization */
    map_col_new = map_col_old = att_NULL;

    /* Ensure that at least one of the required columns is not used
       in the in-between proxy, such we can misuse it to transport
       the key values for the connecting join. */
    for (i = 0; i < req_count; i++) {
        cur_col = req_col_names[i].old;
        for (j = 0; j < PFarray_last (new_col_names); j++)
            if (cur_col == *(PFalg_att_t *) PFarray_at (new_col_names, j))
                break;
        if (j == PFarray_last (new_col_names)) {
            map_col_new = req_col_names[i].new;
            map_col_old = req_col_names[i].old;
            break;
        }
    }
    /* There is no 'free' column that can be used
       to transport the key information. */
    if (i == req_count) return false;



    /* All conditions have been checked -- start rewriting. */



    /* Get the rowid operators of the two proxies */
    proxy1_num = L(L(L(L(proxy1))));
    proxy2_num = L(L(L(L(proxy2))));

    assert (proxy1_num->kind == la_rowid &&
            proxy2_num->kind == la_rowid);

    top_proj   = PFmalloc (proxy1->schema.count *
                           sizeof (PFalg_proj_t));
    /* #new columns + join column + 1 required column is
       an upper bound for the size of the top1 projection */
    top1_proj  = PFmalloc ((proxy1->sem.proxy.new_cols.count + 2) *
                           sizeof (PFalg_proj_t));
    base1_proj = PFmalloc (proxy1_num->schema.count *
                           sizeof (PFalg_proj_t));
    mid2_proj  = PFmalloc (mid_proxy->schema.count *
                           sizeof (PFalg_proj_t));
    top2_proj  = PFmalloc (proxy2->schema.count *
                           sizeof (PFalg_proj_t));
    base2_proj = PFmalloc (proxy2_num->schema.count *
                           sizeof (PFalg_proj_t));

    /* collect all the used column names*/
    used_cols = 0;
    for (i = 0; i < proxy1->sem.proxy.new_cols.count; i++)
        used_cols = used_cols | proxy1->sem.proxy.new_cols.atts[i];
    for (i = 0; i < proxy1_base->schema.count; i++)
        used_cols = used_cols | proxy1_base->schema.items[i].name;
    for (i = 0; i < proxy2_base->schema.count; i++)
        used_cols = used_cols | proxy2_base->schema.items[i].name;

    /* Generate two new column names from the list
       of 'remaining' column names. */
    num_col1 = PFalg_ori_name (PFalg_unq_name (att_iter, 0), ~used_cols);
    used_cols = used_cols | num_col1;
    num_col2 = PFalg_ori_name (PFalg_unq_name (att_iter, 0), ~used_cols);
    /* used_cols = used_cols | num_col2; */

    /* create overall rowid operator */
    num_op = PFla_rowid (L(proxy2_base), num_col1);

    /* create projection that replaces the rowid operator of the
       'upper' proxy pattern */
    assert (L(proxy1_num) = proxy1_base);
    for (i = 0; i < proxy1_base->schema.count; i++) {
        cur_col = proxy1_base->schema.items[i].name;

        for (j = 0; j < req_count; j++)
            if (cur_col == req_col_names[j].new) {
                base1_proj[i] = req_col_names[j];
                break;
            }
        if (j == req_count)
            base1_proj[i] = PFalg_proj (cur_col, num_col1);
    }
    base1_proj[i] = PFalg_proj (proxy1_num->sem.rowid.res, num_col1);

    /* replace the rowid operator */
    *proxy1_num = *PFla_project_ (num_op,
                                  proxy1_num->schema.count,
                                  base1_proj);


    /* create projection that replaces the rowid operator of the
       'lower' proxy pattern */
    assert (L(proxy2_num) = proxy2_base);
    for (i = 0; i < proxy2_base->schema.count; i++) {
        cur_col = proxy2_base->schema.items[i].name;
        base2_proj[i] = PFalg_proj (cur_col, cur_col);
    }
    base2_proj[i] = PFalg_proj (proxy2_num->sem.rowid.res, num_col1);

    /* replace the rowid operator */
    *proxy2_num = *PFla_project_ (num_op,
                                  proxy2_num->schema.count,
                                  base2_proj);


    /* prepare projection located at the top of proxy2 (pi_(top2)) */
    for (i = 0; i < L(proxy2)->sem.proj.count; i++) {
        cur_col = L(proxy2)->sem.proj.items[i].new;
        if (cur_col == map_col_old)
            top2_proj[i] = PFalg_proj (map_col_old,
                                       L(L(proxy2))->sem.eqjoin.att1);
        else
            top2_proj[i] = L(proxy2)->sem.proj.items[i];
    }

    /* replace the projection */
    *proxy2 = *PFla_project_ (L(L(proxy2)),
                              proxy2->schema.count,
                              top2_proj);


    /* prepare projection located at the top of in-between
       operators (pi_(mid)) */
    for (i = 0; i < mid_proxy->schema.count; i++) {
        cur_col = mid_proxy->schema.items[i].name;
        if (map_col_new == cur_col)
            mid2_proj[i] = PFalg_proj (num_col2, cur_col);
        else
            mid2_proj[i] = PFalg_proj (cur_col, cur_col);
    }

    /* prepare projection located at the top of proxy1 (pi_(top1)) */
    top1_proj_count = 0;
    for (i = 0; i < L(proxy1)->sem.proj.count; i++) {
        cur_col = L(proxy1)->sem.proj.items[i].new;
        if (map_col_new == cur_col)
            top1_proj[top1_proj_count++] = L(proxy1)->sem.proj.items[i];
        else
            for (j = 0; j < proxy1->sem.proxy.new_cols.count; j++)
                if (proxy1->sem.proxy.new_cols.atts[j] == cur_col) {
                    top1_proj[top1_proj_count++] = L(proxy1)->sem.proj.items[i];
                    break;
                }
    }
    top1_proj[top1_proj_count++] = PFalg_proj (num_col1,
                                     L(L(proxy1))->sem.eqjoin.att1);

    /* prepare the projection replacing the root of the pattern */
    for (i = 0; i < proxy1->schema.count; i++) {
        cur_col = proxy1->schema.items[i].name;
        top_proj[i] = PFalg_proj (cur_col, cur_col);
    }

    /* Connect the proxies via an equi-join. (Projections map
       the column names to avoid naming conflicts.) */
    *proxy1 = *PFla_project_ (
                   PFla_eqjoin (
                       PFla_project_ (
                           L(L(proxy1)),
                           top1_proj_count,
                           top1_proj),
                       PFla_project_ (
                           mid_proxy,
                           mid_proxy->schema.count,
                           mid2_proj),
                       num_col1,
                       num_col2),
                   proxy1->schema.count,
                   top_proj);

    return true;
}
#endif




/**
 *
 * Functions that drive the proxy recognition and transformation
 * process. Different kinds of proxies can be plugged in by
 * specifying an entry and an exit criterion, some pre conditions
 * (e.g., property inference), and a handler that transforms the
 * discovered pattern.
 *
 */

/* helper function that checks if a node appears in a list */
static bool
node_in_list (PFla_op_t *p, PFarray_t *node_list)
{
    for (unsigned int i = 0; i < PFarray_last (node_list); i++)
        if (*(PFla_op_t **) PFarray_at (node_list, i) == p)
            return true;
    return false;
}

/**
 * find_proxy_entry traverses the DAG bottom up and returns the
 * first node that does not appear in the @a checked_nodes list
 * and fulfills the condition of @a check_entry.
 */
static PFla_op_t *
find_proxy_entry (PFla_op_t *p, PFarray_t *checked_nodes,
                  bool (* check_entry) (PFla_op_t *))
{
    PFla_op_t *node;

    assert (p);
    assert (checked_nodes);

    /* look at each node only once */
    if (SEEN(p))
        return NULL;
    else
        SEEN(p) = true;

    /* traverse children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++) {
        node = find_proxy_entry (p->child[i], checked_nodes, check_entry);
        if (node) return node;
    }

    /* If node does not appear in the list of already checked nodes
       check the entry condition. */
    if (check_entry (p) &&
        !node_in_list (p, checked_nodes))
        return p;

    return NULL;
}

/**
 * find_proxy_exit traverses the DAG top down and returns the
 * node that fulfills the condition of @a check_exit for the complete
 * sub-DAG. @a exit makes the 'first' exit node visible for the remaining
 * sub-DAG to avoid conflicting exit references and to collect
 * the referencing nodes (@a exit_refs).
 *
 * In addition we mark all nodes during the traversal with the 'in' bit.
 * This marks all operators of the proxy and prepares the generation of
 * the conflict list (which contains operators that are 'in'side as well
 * as 'out'side of the proxy.
 */
static PFla_op_t *
find_proxy_exit (PFla_op_t *p,
                 PFla_op_t *entry,
                 PFla_op_t *exit,
                 PFarray_t *exit_refs,
                 bool (* check_exit) (PFla_op_t *, PFla_op_t *))
{
    PFla_op_t *node;

    assert (p);

    /* check the exit criterion */
    if (check_exit (p, entry)) {
        SEEN(p) = true;
        pfIN(p) = true;
        return p;
    }
    /* Avoid following the fragment information (and marking it as
       inside the proxy). We might end up somewhere in the DAG.
       (we rely on the fact that frag_union is the first fragment
        node for each operator that consumes fragment information. */
    else if (p->kind == la_frag_union)
        return NULL;
    /* look at each node only once */
    else if (SEEN(p))
        return NULL;

    /* Mark nodes as seen and 'in'side the proxy. */
    SEEN(p) = true;
    pfIN(p) = true;

    /* traverse children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++) {
        node = find_proxy_exit (p->child[i], entry, exit, exit_refs, check_exit);
        /* store the first exit node we find */
        if (node && !exit)
            exit = node;
        /* check if the new exit node and the first one match */
        else if (node && exit && node != exit)
            PFoops (OOPS_FATAL,
                    "Cannot cope with multiple different exit nodes "
                    "in proxy recognition phase!");

        /* collect all nodes that reference the exit node */
        if (exit && exit == p->child[i])
            *(PFla_op_t **) PFarray_add (exit_refs) = p;
    }

    return exit;
}

/**
 * find_conflicts traverses the DAG starting from the root
 * and marks all nodes as 'out'. If a node is 'in'side the
 * proxy ('in' bits were generated by the previous call of
 * 'find_proxy_exit') as well as 'out'side it is appended
 * to the conflict list and the processing is stopped at
 * this point. This ensures that only operator on the border
 * of the proxy are reported.
 */
static void
find_conflicts (PFla_op_t *p, PFarray_t *conflict_list)
{
    assert (p);
    assert (conflict_list);

    pfOUT(p) = true;

    if (pfOUT(p) && pfIN(p)) {
        *(PFla_op_t **) PFarray_add (conflict_list) = p;
        SEEN(p) = true; /* this stops the traversal in the next line */
    }

    /* look at each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    if (p->kind == la_rec_arg &&
        pfIN(p->sem.rec_arg.base))
        *(PFla_op_t **) PFarray_add (conflict_list) = p->sem.rec_arg.base;

    /* traverse children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        find_conflicts (p->child[i], conflict_list);
}

/**
 * Worker for PFintro_proxies. For each kind of proxy this worker looks
 * up all possible proxy nodes and generates them whenever possible.
 */
static bool
intro_proxy_kind (PFla_op_t *root,
                  void (* prepare_traversal) (PFla_op_t *),
                  bool (* entry_criterion) (PFla_op_t *),
                  bool (* exit_criterion) (PFla_op_t *, PFla_op_t *),
                  bool (* generate_proxy) (PFla_op_t *, PFla_op_t *,
                                           PFla_op_t *, PFarray_t *,
                                           PFarray_t *, PFarray_t *),
                  PFarray_t *checked_nodes)
{
    PFla_op_t *proxy_entry, *proxy_exit;
    PFarray_t *exit_refs     = PFarray (sizeof (PFla_op_t *));
    PFarray_t *conflict_list = PFarray (sizeof (PFla_op_t *));

    bool found_proxy = true;
    bool rewrote_proxy = false;

    while (found_proxy) {
        found_proxy = false;

        /* Infer properties first */
        prepare_traversal (root);

        /* Traverse DAG and look up a new proxy entry based on the entry
           criterion. The checked_nodes lists prohibits that one operator
           is matched multiple times */
        proxy_entry = find_proxy_entry (root, checked_nodes, entry_criterion);
        PFla_dag_reset (root);

        if (!proxy_entry)
            continue;
        else
            found_proxy = true;

        /* add new proxy node to the list of checked proxy nodes */
        *(PFla_op_t **) PFarray_add (checked_nodes) = proxy_entry;

        PFarray_last (exit_refs) = 0;
        /* Traverse DAG starting from the proxy entry and try to find
           a corresponding proxy exit based on the exit_criterion.
           In addition all the operators that reference the proxy exit
           are collected in the exit_refs list. */
        proxy_exit = find_proxy_exit (proxy_entry,
                                      proxy_entry,
                                      NULL,
                                      exit_refs,
                                      exit_criterion);

        if (!proxy_exit) {
            /* clean up and continue */
            PFla_in_out_reset (root);
            PFla_dag_reset (root);
            continue;
        }

        PFarray_last (conflict_list) = 0;
        /* Traverse DAG starting from the root and collect all
           the operators in the proxy that are also referenced
           by 'out'side operators. */
        find_conflicts (root, conflict_list);

        PFla_in_out_reset (root);
        PFla_dag_reset (root);

        /* generate a new proxy operator using all the information
           gathered. */
        rewrote_proxy = generate_proxy (root,
                                        proxy_entry,
                                        proxy_exit,
                                        conflict_list,
                                        exit_refs,
                                        checked_nodes)
                        || rewrote_proxy;
    }

    return rewrote_proxy;
}

/**
 * Introduce proxy operators.
 */
PFla_op_t *
PFintro_proxies (PFla_op_t *root)
{
    PFarray_t *checked_nodes = PFarray (sizeof (PFla_op_t *));

    /* remove all semijoin operators as most of our
       proxies cannot cope with semijoins and thus
       would recognize less proxies. */
    remove_semijoin_operators (root);

    /* find proxies and rewrite them in one go.
       They are based on semi-join - rowid/rownum pairs */
    if (intro_proxy_kind (root,
                          join_prepare,
                          semijoin_entry,
                          semijoin_exit,
                          modify_semijoin_proxy,
                          checked_nodes))
        return root;

    /* As we match the same nodes (equi-joins) again we need to reset
       the list of checked nodes */
    PFarray_last (checked_nodes) = 0;

    /* rewrite joins that are unnested (independent expressions
       on the left and the right side) into a nested variant
       such that the following proxy introduction and rewrites
       using these proxies may benefit from it. */
    intro_proxy_kind (root,
                      join_prepare,
                      proxy_nest_entry,
                      proxy_nest_exit,
                      nest_proxy,
                      checked_nodes);

    /* As we match the same nodes (equi-joins) again we need to reset
       the list of checked nodes */
    PFarray_last (checked_nodes) = 0;

    /* rewrite joins that contain only a single XPath location
       step into a new step_join operator. */
    intro_proxy_kind (root,
                      step_join_prepare,
                      step_join_entry,
                      step_join_exit,
                      intro_step_join,
                      checked_nodes);

    /* As we match the same nodes (equi-joins) again we need to reset
       the list of checked nodes */
    PFarray_last (checked_nodes) = 0;

    /* generate proxies consisting of equi-join - rowid/rownum pairs */
    if (!intro_proxy_kind (root,
                           join_prepare,
                           join_entry,
                           join_exit,
                           generate_join_proxy,
                           checked_nodes))
        return root;

    /* As we match different nodes the current list is of no importance */
    PFarray_last (checked_nodes) = 0;

    /* We require the superfluous rowid operators to be pruned. */
    PFalgopt_icol (root);

#if 0
    /* We don't need the following rewrite anymore. All observed cases
       are already handled by the thetajoin optimization. */

    /* rewrite (directly) following proxies that are independent
       of each other into an DAG that might evaluates both proxies
       in parallel */
    intro_proxy_kind (root,
                      proxy_unnest_prepare,
                      proxy_unnest_entry,
                      proxy_unnest_exit,
                      unnest_proxy,
                      checked_nodes);
#endif

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
