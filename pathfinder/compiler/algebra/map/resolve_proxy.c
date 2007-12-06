/**
 * @file
 *
 * Resolve proxy operators.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include pathfinder.h first! */
#include "pathfinder.h"
#include <assert.h>
#include <stdio.h>

#include "la_proxy.h"
#include "alg_dag.h"
#include "properties.h"
#include "mem.h"

#define SEEN(p) ((p)->bit_dag)

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

static void
resolve_proxies (PFla_op_t *p)
{
    unsigned int i;
    assert (p);

    /* look at each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* traverse children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        resolve_proxies (p->child[i]);

    /* throw away additional proxy nodes */
    if (p->kind == la_proxy || p->kind == la_proxy_base)
        *p = *PFla_dummy (p->child[0]);

    /* Replace the duplicate generating path step operators
       by the more explicit variant:

             pi
              |
             |X|
             / \
            /   |
          step  |
            |   |
           pi   |
            \   |
             \ /
              #
    */
    if (p->kind == la_step_join || p->kind == la_guide_step_join) {
        PFla_op_t    *rowid, *step;
        PFalg_att_t   used_cols = 0,
                      join_att1,
                      join_att2,
                      cur;
        PFalg_proj_t *top_proj   = PFmalloc (p->schema.count *
                                             sizeof (PFalg_proj_t));

        /* Collect the used columns and generate a projection list
           that discards the join columns. */
        for (i = 0; i < p->schema.count; i++) {
            cur = p->schema.items[i].name;
            used_cols = used_cols | cur;
            top_proj[i] = PFalg_proj (cur, cur);
        }

        /* Generate two new column names (used for the join attributes). */
        join_att1 = PFalg_ori_name (PFalg_unq_name (att_iter, 0), ~used_cols);
        used_cols = used_cols | join_att1;
        join_att2 = PFalg_ori_name (PFalg_unq_name (att_iter, 0), ~used_cols);

        /* Generate a new rowid operator. */
        rowid = PFla_rowid (R(p), join_att2);

        /* Generate the pattern sketched above. The projection
           underneath the path step operator renames the
           join columns as well as the resulting item column. */
        if (p->kind == la_step_join)
            step = PFla_step (
                       L(p),
                       PFla_project (
                           rowid,
                           PFalg_proj (join_att1, join_att2),
                           PFalg_proj (p->sem.step.item_res,
                                       p->sem.step.item)),
                       p->sem.step.axis,
                       p->sem.step.ty,
                       p->sem.step.level,
                       join_att1,
                       p->sem.step.item_res,
                       p->sem.step.item_res);
        else
            step = PFla_guide_step (
                       L(p),
                       PFla_project (
                           rowid,
                           PFalg_proj (join_att1, join_att2),
                           PFalg_proj (p->sem.step.item_res,
                                       p->sem.step.item)),
                       p->sem.step.axis,
                       p->sem.step.ty,
                       p->sem.step.guide_count,
                       p->sem.step.guides,
                       p->sem.step.level,
                       join_att1,
                       p->sem.step.item_res,
                       p->sem.step.item_res);

        *p = *PFla_project_ (
                  PFla_eqjoin (
                      step,
                      rowid,
                      join_att1,
                      join_att2),
                  p->schema.count,
                  top_proj);
    }
}

/**
 * Resolve proxy operators.
 */
PFla_op_t *
PFresolve_proxies (PFla_op_t *root)
{
    resolve_proxies (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
