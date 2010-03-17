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
        case la_cross:
            /* We can throw away an argument of a cross product
               if it does not need to produce duplicates and if the columns
               of the (other) child argument are not referenced. */
            if (PFprop_set (p->prop)) {
                PFalg_col_t new_col = PFcol_new (col_iter);
                bool referenced;

                /* check for references of the left side */
                referenced = false;
                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    if (PFprop_icol (p->prop, L(p)->schema.items[i].name)) {
                        referenced = true;
                        break;
                    }

                /* check we do not apply the rewrite twice */
                if (!referenced && L(p)->schema.count == 1 &&
                    PFprop_const (p->prop, L(p)->schema.items[0].name)) {
                    /* in case the distinct got lost add it once more */
                    if (L(p)->kind != la_distinct &&
                        (L(p)->kind != la_project ||
                         LL(p)->kind != la_distinct))
                        L(p) = PFla_distinct (L(p));

                    break;
                }
                /* replace the cross product by a projection */
                else if (!referenced) {
                    PFalg_proj_t *proj = PFmalloc (L(p)->schema.count *
                                                   sizeof (PFalg_proj_t));
                    for (unsigned int i = 0; i < L(p)->schema.count; i++)
                        proj[i] = PFalg_proj (L(p)->schema.items[i].name,
                                              new_col);
                    L(p) = PFla_project_ (
                               PFla_distinct (
                                   PFla_project (
                                       PFla_attach (
                                           L(p),
                                           new_col,
                                           PFalg_lit_nat(42)),
                                       PFalg_proj (new_col, new_col))),
                               L(p)->schema.count,
                               proj);
                    break;
                }
                /* check for references of the right side */
                referenced = false;
                for (unsigned int i = 0; i < R(p)->schema.count; i++)
                    if (PFprop_icol (p->prop, R(p)->schema.items[i].name)) {
                        referenced = true;
                        break;
                    }

                /* check we do not apply the rewrite twice */
                if (!referenced && R(p)->schema.count == 1 &&
                    PFprop_const (p->prop, R(p)->schema.items[0].name)) {
                    /* in case the distinct got lost add it once more */
                    if (R(p)->kind != la_distinct &&
                        (R(p)->kind != la_project ||
                         RL(p)->kind != la_distinct))
                        R(p) = PFla_distinct (R(p));

                    break;
                }
                /* replace the cross product by a projection */
                else if (!referenced) {
                    PFalg_proj_t *proj = PFmalloc (R(p)->schema.count *
                                                   sizeof (PFalg_proj_t));
                    for (unsigned int i = 0; i < R(p)->schema.count; i++)
                        proj[i] = PFalg_proj (R(p)->schema.items[i].name,
                                              new_col);
                    R(p) = PFla_project_ (
                               PFla_distinct (
                                   PFla_project (
                                       PFla_attach (
                                           R(p),
                                           new_col,
                                           PFalg_lit_nat(42)),
                                       PFalg_proj (new_col, new_col))),
                               R(p)->schema.count,
                               proj);
                    break;
                }
            }
            break;

#if 0 /* steps not used anymore */
        case la_eqjoin:
            /* Rewrite step into duplicate generating step
               (underneath join operators). This hopefully allows
               the above join to be pushed down in a later phase. */
            if (PFprop_set (p->prop) &&
                L(p)->kind == la_step &&
                PFprop_set (L(p)->prop)) {
                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(L(p)->sem.step.item |
                                 L(p)->sem.step.iter));
                *L(p) = *PFla_project (
                             PFla_step_join (
                                 LL(p),
                                 LR(p),
                                 L(p)->sem.step.spec,
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
                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(R(p)->sem.step.item |
                                 R(p)->sem.step.iter));
                *R(p) = *PFla_project (
                             PFla_step_join (
                                 RL(p),
                                 RR(p),
                                 R(p)->sem.step.spec,
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
#endif

        case la_distinct:
            if (PFprop_set (p->prop))
                *p = *PFla_dummy (L(p));
            break;

#if 0 /* steps not used anymore */
        case la_step:
            if (PFprop_set (p->prop) &&
                PFprop_icol (p->prop, p->sem.step.item) &&
                PFprop_not_icol (p->prop, p->sem.step.iter))
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
                          p->sem.step.spec,
                          p->sem.step.level,
                          p->sem.step.iter,
                          p->sem.step.item,
                          p->sem.step.item);
            break;
#endif

#if 0 /* disable step_join -> step rewrite */
        case la_step_join:
            if (PFprop_set (p->prop) &&
                PFprop_icols_count (p->prop) == 2 &&
                PFprop_icol (p->prop, p->sem.step.item_res)) {
                PFalg_col_t iter = 0,
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
                          p->sem.step.spec,
                          p->sem.step.level,
                          iter,
                          p->sem.step.item_res,
                          p->sem.step.item_res);
            }
            break;
#endif

#if 0 /* steps not used anymore */
        case la_guide_step:
            if (PFprop_set (p->prop) &&
                PFprop_icol (p->prop, p->sem.step.item) &&
                PFprop_not_icol (p->prop, p->sem.step.iter))
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
                          p->sem.step.spec,
                          p->sem.step.guide_count,
                          p->sem.step.guides,
                          p->sem.step.level,
                          p->sem.step.iter,
                          p->sem.step.item,
                          p->sem.step.item);
            break;
#endif

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
    PFprop_infer_set_extended (root);
    PFprop_infer_icol (root);
    PFprop_infer_const (root);

    /* Optimize algebra tree */
    opt_set (root);
    PFla_dag_reset (root);
    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
/* vim:set foldmarker=#if,#endif foldmethod=marker foldopen-=search: */
