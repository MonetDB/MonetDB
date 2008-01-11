/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the set property.
 * (This requires no burg pattern matching as we
 *  apply optimizations in a peep-hole style on
 *  single nodes only.)
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

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** and so on ... */
#define LL(p) (L(L(p)))
#define LR(p) (R(L(p)))
#define RL(p) (L(R(p)))
#define RR(p) (R(R(p)))

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_set */
static void
opt_set (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply set optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_set (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_eqjoin:
            /* Rewrite step into duplicate generating step
               (underneath join operators). This hopefully allows
               the above join to be pushed down in a later phase. */
            if (PFprop_set (p->prop) &&
                L(p)->kind == la_step &&
                PFprop_set (L(p)->prop)) {
                PFalg_att_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (att_item, 0),
                               ~(L(p)->sem.step.item |
                                 L(p)->sem.step.iter));
                *L(p) = *PFla_project (
                             PFla_step_join (
                                 LL(p),
                                 LR(p),
                                 L(p)->sem.step.axis,
                                 L(p)->sem.step.ty,
                                 L(p)->sem.step.level,
                                 L(p)->sem.step.item,
                                 item_res),
                             PFalg_proj (L(p)->sem.step.iter,
                                         L(p)->sem.step.iter),
                             PFalg_proj (L(p)->sem.step.item,
                                         item_res));
                break;
            }
            if (PFprop_set (p->prop) &&
                R(p)->kind == la_step &&
                PFprop_set (R(p)->prop)) {
                PFalg_att_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (att_item, 0),
                               ~(R(p)->sem.step.item |
                                 R(p)->sem.step.iter));
                *R(p) = *PFla_project (
                             PFla_step_join (
                                 RL(p),
                                 RR(p),
                                 R(p)->sem.step.axis,
                                 R(p)->sem.step.ty,
                                 R(p)->sem.step.level,
                                 R(p)->sem.step.item,
                                 item_res),
                             PFalg_proj (R(p)->sem.step.iter,
                                         R(p)->sem.step.iter),
                             PFalg_proj (R(p)->sem.step.item,
                                         item_res));
                break;
            }
            break;

        case la_distinct:
            if (PFprop_set (p->prop))
                *p = *PFla_dummy (L(p));
            break;

        case la_step:
            if (PFprop_set (p->prop) &&
                PFprop_icol (p->prop, p->sem.step.item) &&
                !PFprop_icol (p->prop, p->sem.step.iter))
                *p = *PFla_step (
                          L(p),
                          PFla_attach (
                              PFla_project (
                                  R(p),
                                  PFalg_proj (
                                      p->sem.step.item,
                                      p->sem.step.item)),
                              p->sem.step.iter,
                              PFalg_lit_nat (1)),
                          p->sem.step.axis,
                          p->sem.step.ty,
                          p->sem.step.level,
                          p->sem.step.iter,
                          p->sem.step.item,
                          p->sem.step.item);
            break;

        case la_step_join:
            if (PFprop_set (p->prop) &&
                PFprop_icols_count (p->prop) == 2 &&
                PFprop_icol (p->prop, p->sem.step.item_res)) {
                PFalg_att_t iter = 0,
                            item = p->sem.step.item_res;

                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (PFprop_icol (p->prop, p->schema.items[i].name) &&
                        p->schema.items[i].name != item &&
                        p->schema.items[i].type == aat_nat) {
                        iter = p->schema.items[i].name;
                        break;
                    }

                if (!iter)
                    break;

                *p = *PFla_step (
                          L(p),
                          PFla_project (
                              R(p),
                              PFalg_proj (p->sem.step.item_res,
                                          p->sem.step.item),
                              PFalg_proj (iter, iter)),
                          p->sem.step.axis,
                          p->sem.step.ty,
                          p->sem.step.level,
                          iter,
                          p->sem.step.item_res,
                          p->sem.step.item_res);
            }
            break;

        case la_guide_step:
            if (PFprop_set (p->prop) &&
                PFprop_icol (p->prop, p->sem.step.item) &&
                !PFprop_icol (p->prop, p->sem.step.iter))
                *p = *PFla_guide_step (
                          L(p),
                          PFla_attach (
                              PFla_project (
                                  R(p),
                                  PFalg_proj (
                                      p->sem.step.item,
                                      p->sem.step.item)),
                              p->sem.step.iter,
                              PFalg_lit_nat (1)),
                          p->sem.step.axis,
                          p->sem.step.ty,
                          p->sem.step.guide_count,
                          p->sem.step.guides,
                          p->sem.step.level,
                          p->sem.step.iter,
                          p->sem.step.item,
                          p->sem.step.item);
            break;

        default:
            break;
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_set (PFla_op_t *root)
{
    /* Infer set properties first */
    PFprop_infer_set (root);
    PFprop_infer_icol (root);

    /* Optimize algebra tree */
    opt_set (root);
    PFla_dag_reset (root);
    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
