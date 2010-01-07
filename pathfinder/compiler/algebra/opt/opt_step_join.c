/**
 * @file
 *
 * Push-Down Step-Join operators to detect and remove superfluous steps.
 *
 * 2009/05/14 (JR):
 * In this phase we mix the order of path steps. While we are guaranteed 
 * to avoid endless loops (as we push-down each step-join only once) we
 * may change the input path step evaluation order to a better or worse
 * evaluation order.
 *
 * E.g., the path sequence /a[b/c]/d/e may be evaluated in one of the
 * following orders: a,b,c,d,e;  a,d,e,b,c;  a,b,d,c,e; ...
 *
 * Currently we do not care about the order as we assume that future
 * runtime query optimization decides for the best order.
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
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_step_join */
static void
opt_step_join (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply set optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_step_join (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_project:
            /* merge adjacent projection operators */
            if (L(p)->kind == la_project)
                *p = *PFla_project_ (LL(p),
                                     p->schema.count,
                                     PFalg_proj_merge (
                                         p->sem.proj.items,
                                         p->sem.proj.count,
                                         L(p)->sem.proj.items,
                                         L(p)->sem.proj.count));
            break;

        case la_step_join:
        {
            PFla_op_t *op = p;
            while (op->kind == la_step_join) {
                /* push projection operator above the step-join
                   to get it out of the way */
                if (R(op)->kind == la_project) {
                    PFalg_col_t col_in  = col_NULL,
                                col_out = PFcol_new (op->sem.step.item_res);
                    PFalg_proj_t *proj = PFmalloc (op->schema.count *
                                                   sizeof (PFalg_proj_t));

                    for (unsigned int i = 0; i < R(op)->sem.proj.count; i++) {
                        proj[i] = R(op)->sem.proj.items[i];
                        /* get the 'old' item column name */
                        if (proj[i].new == op->sem.step.item)
                            col_in = proj[i].old;
                    }
                    proj[R(op)->sem.proj.count] = PFalg_proj (
                                                      op->sem.step.item_res,
                                                      col_out);

                    *op = *PFla_project_ (
                               PFla_step_join (L(op),
                                               RL(op),
                                               op->sem.step.spec,
                                               op->sem.step.level,
                                               col_in,
                                               col_out),
                               op->schema.count, proj);
                    op = L(op);
                }
                /* In case we have identical step-joins we can merge them.
                   We however need to ensure that the cardinality stays the
                   same. This is achieved by introducing a key column before
                   and a join on the key column afterwards---we duplicate the
                   cardinality of the step-join. */
                else if (R(op)->kind == la_step_join &&
                         op->sem.step.spec.axis == R(op)->sem.step.spec.axis &&
                         op->sem.step.spec.kind == R(op)->sem.step.spec.kind &&
                         !PFqname_eq (op->sem.step.spec.qname,
                                      R(op)->sem.step.spec.qname) &&
                         op->sem.step.item == R(op)->sem.step.item) {
                    PFalg_col_t col1 = PFcol_new (col_iter),
                                col2 = PFcol_new (col_iter);
                    PFla_op_t *step = PFla_step_join (L(op),
                                                      PFla_rowid (RR(op), col1),
                                                      op->sem.step.spec,
                                                      op->sem.step.level,
                                                      op->sem.step.item,
                                                      op->sem.step.item_res);
                    *op = *PFla_project_ (
                               PFla_eqjoin (
                                   step,
                                   PFla_project (
                                       step,
                                       PFalg_proj (col2, col1),
                                       PFalg_proj (R(op)->sem.step.item_res,
                                                   op->sem.step.item_res)),
                                   col1,
                                   col2),
                               op->schema.count,
                               PFalg_proj_create (op->schema));

                    /* we reached our goal and are done with rewriting */
                }
                /* in case we found a step-join that does not provide
                   the input to the current step we push down the upper
                   step-join */
                else if (R(op)->kind == la_step_join &&
                         op->sem.step.item != R(op)->sem.step.item_res &&
                         op->sem.step.item != R(op)->sem.step.item) {
                    PFla_op_t *step = PFla_step_join (L(op),
                                                      RR(op),
                                                      op->sem.step.spec,
                                                      op->sem.step.level,
                                                      op->sem.step.item,
                                                      op->sem.step.item_res);
                    *op = *PFla_step_join (L(op),
                                           step,
                                           R(op)->sem.step.spec,
                                           R(op)->sem.step.level,
                                           R(op)->sem.step.item,
                                           R(op)->sem.step.item_res);
                    op = R(op);
                }
                /* in case an equi-join sits in the way (e.g., introduced
                   by the above rewrite) we push down the step-join */
                else if (R(op)->kind == la_eqjoin) {
                    /* decide into which side of the equi-join we push down
                       the step-join */
                    if (PFprop_ocol (RL(op), op->sem.step.item)) {
                        PFla_op_t *step = PFla_step_join (L(op),
                                                          RL(op),
                                                          op->sem.step.spec,
                                                          op->sem.step.level,
                                                          op->sem.step.item,
                                                          op->sem.step.item_res);
                        *op = *PFla_eqjoin (step,
                                       RR(op),
                                       R(op)->sem.eqjoin.col1, 
                                       R(op)->sem.eqjoin.col2);
                        op = L(op);
                    }
                    else {
                        PFla_op_t *step = PFla_step_join (L(op),
                                                          RR(op),
                                                          op->sem.step.spec,
                                                          op->sem.step.level,
                                                          op->sem.step.item,
                                                          op->sem.step.item_res);
                        *op = *PFla_eqjoin (RL(op),
                                       step,
                                       R(op)->sem.eqjoin.col1, 
                                       R(op)->sem.eqjoin.col2);
                        op = R(op);
                    }
                }
                else
                    break;
            }
        }   break;

        default:
            break;
    }
}

/**
 * Invoke step join push down.
 */
PFla_op_t *
PFalgopt_step_join (PFla_op_t *root)
{
    opt_step_join (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
