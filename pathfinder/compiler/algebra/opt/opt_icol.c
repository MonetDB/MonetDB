/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the icols property.
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

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* worker for PFalgopt_icol */
static void
opt_icol (PFla_op_t *p)
{
    bool bottom_up = true;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    switch (p->kind)
    {
        case la_rec_arg:
            bottom_up = false;
            break;

        default:
            break;
    }

    if (bottom_up)
        /* apply icol optimization for children */
        for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
            opt_icol (p->child[i]);

    /* action code */
    switch (p->kind) {
        case la_lit_tbl:
        {
            unsigned int count = PFprop_icols_count (p->prop);

            /* prune columns that are not required as long as
               at least one column remains. */
            if (count && count < p->schema.count) {
                /* create new list of columns */
                PFalg_collist_t *collist = PFalg_collist (count);

                /* create list of tuples each containing a list of atoms */
                PFalg_tuple_t *tuples = PFmalloc (p->sem.lit_tbl.count *
                                                  sizeof (*(tuples)));;
                for (unsigned int i = 0; i < p->sem.lit_tbl.count; i++)
                    tuples[i].atoms = PFmalloc (count *
                                                sizeof (*(tuples[i].atoms)));
                count = 0;

                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (PFprop_icol (p->prop, p->schema.items[i].name)) {
                        /* retain matching values in literal table */
                        cladd (collist) = p->schema.items[i].name;
                        for (unsigned int j = 0; j < p->sem.lit_tbl.count; j++)
                            tuples[j].atoms[count] =
                                    p->sem.lit_tbl.tuples[j].atoms[i];
                        count++;
                    }

                for (unsigned int i = 0; i < p->sem.lit_tbl.count; i++)
                    tuples[i].count = count;

                *p = *PFla_lit_tbl_ (collist, p->sem.lit_tbl.count, tuples);
                SEEN(p) = true;
            } else if (!count && p->schema.count > 1) {
                /* prune everything except one column */

                /* create new list of columns */
                PFalg_collist_t *collist = PFalg_collist (1);

                /* create list of tuples each containing a list of atoms */
                PFalg_tuple_t *tuples = PFmalloc (p->sem.lit_tbl.count *
                                                  sizeof (*(tuples)));;
                for (unsigned int i = 0; i < p->sem.lit_tbl.count; i++)
                    tuples[i].atoms = PFmalloc (1 *
                                                sizeof (*(tuples[i].atoms)));

                /* retain matching values in literal table */
                cladd (collist) = p->schema.items[0].name;
                for (unsigned int j = 0; j < p->sem.lit_tbl.count; j++) {
                    tuples[j].atoms[0] = PFalg_lit_nat (42);
                    tuples[j].count = 1;
                }

                *p = *PFla_lit_tbl_ (collist, p->sem.lit_tbl.count, tuples);
                SEEN(p) = true;
            }
        } break;

        case la_empty_tbl:
        {
            unsigned int count = PFprop_icols_count (p->prop);

            /* prune columns that are not required as long as
               at least one column remains. */
            if (count && count < p->schema.count) {
                /* create new schema */
                PFalg_schema_t schema;
                schema.count = count;
                schema.items = PFmalloc (schema.count *
                                         sizeof (*(schema.items)));
                count = 0;
                /* throw out all columns that are not in the icols list */
                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (PFprop_icol (p->prop, p->schema.items[i].name)) {
                        schema.items[count++] = p->schema.items[i];
                    }

                *p = *PFla_empty_tbl_ (schema);
                SEEN(p) = true;

            } else if (!count && p->schema.count > 1) {
                /* prune everything except one column */
                PFalg_schema_t schema;
                schema.count = 1;
                schema.items = PFmalloc (schema.count *
                                         sizeof (*(schema.items)));
                /* just use first column */
                schema.items[0] = p->schema.items[0];

                *p = *PFla_empty_tbl_ (schema);
                SEEN(p) = true;
            }
        } break;

        case la_attach:
            /* prune attach if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.attach.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_project:
        {   /* Because the icols columns are intersected with the
               ocol columns we can replace the current projection
               list with the icols columns. */
            unsigned int count = PFprop_icols_count (p->prop);
            if (count < p->schema.count) {
                PFalg_proj_t *proj;

                /* ensure that at least one column remains! */
                count = count?count:1;
                proj = PFmalloc (count * sizeof (PFalg_proj_t));

                count = 0;
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    if (PFprop_icol (p->prop, p->sem.proj.items[j].new))
                        proj[count++] = p->sem.proj.items[j];

                /* Ensure that at least one column remains!
                   Because the projection list may reference
                   only columns that are discarded because of
                   the icols property, a new projection
                   mapping arbitrary columns is generated */
                if (!count)
                    proj[count++] = PFalg_proj (p->sem.proj.items[0].new,
                                                L(p)->schema.items[0].name);

                *p = *PFla_project_ (L(p), count, proj);

                SEEN(p) = true;
                break;
            }

        }   break;

        case la_disjunion:
            /* prune unnecessary columns before the union */
            if (PFprop_icols_count (p->prop) < p->schema.count) {
                /* introduce a projection for the left and right
                   union argument */
                if (PFprop_icols_count (p->prop)) {
                    PFla_op_t *ret;
                    PFalg_collist_t *icols =
                                     PFprop_icols_to_collist (p->prop);
                    PFalg_proj_t *cols = PFmalloc (clsize (icols) *
                                                   sizeof (PFalg_proj_t));

                    for (unsigned int i = 0; i < clsize (icols); i++)
                        cols[i] = PFalg_proj (clat (icols, i), clat (icols, i));

                    ret = PFla_project_ (L(p), clsize (icols), cols);
                    L(p) = ret;

                    ret = PFla_project_ (R(p), clsize (icols), cols);
                    R(p) = ret;

                    break;
                }
                /* use the left and right icols information
                   for generating the projection list (one item) */
                else {
                    PFla_op_t *ret;
                    PFalg_proj_t *cols = PFmalloc (1 * sizeof (PFalg_proj_t));

                    for (unsigned int i = 0; i < p->schema.count; i++)
                        if (PFprop_icol_left (p->prop,
                                              p->schema.items[i].name)) {
                            cols[0] = PFalg_proj (p->schema.items[i].name,
                                                  p->schema.items[i].name);

                            ret = PFla_project_ (L(p), 1, cols);
                            L(p) = ret;

                            ret = PFla_project_ (R(p), 1, cols);
                            R(p) = ret;

                            break;
                        }
                    break;
                }
            }
            break;

        case la_fun_1to1:
            /* prune generic function operator if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.fun_1to1.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            /* prune binary operation if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.binary.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_bool_not:
            /* prune unary operation if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.unary.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_aggr:
        {
            unsigned int  count = 0;
            PFalg_aggr_t *aggr = PFmalloc (p->sem.aggr.count *
                                           sizeof (PFalg_aggr_t));

            for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                if (PFprop_icol (p->prop, p->sem.aggr.aggr[i].res))
                    aggr[count++] = p->sem.aggr.aggr[i];

            /* replace aggregate function if result column is not required */
            if (!count) {
                /* as an aggregate is required we either
                   (a) evaluate a distinct on the partition (if present) or
                   (b) create a one tuple literal table with a bogus value
                       (as it is never referenced) */
                if (p->sem.aggr.part)
                    *p = *distinct (project (L(p), proj (p->sem.aggr.part,
                                                         p->sem.aggr.part)));
                else
                    *p = *lit_tbl (collist (p->sem.aggr.aggr[0].res),
                                   tuple (lit_nat (42)));
            }
            else if (count < p->sem.aggr.count) {
                p->sem.aggr.count = count;
                p->sem.aggr.aggr  = aggr;
            }
            SEEN(p) = true;
        }   break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
            /* prune rownum, rowrank, or rank if the result
               column is not required */
            if (PFprop_not_icol (p->prop, p->sem.sort.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_rowid:
            /* prune rowid if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.rowid.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_type:
            /* prune type if result column is not required */
        case la_cast:
            /* prune cast if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.type.res)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_type_assert:
            /* prune type assertion if restricted column is not
               used afterwards */
            if (PFprop_not_icol (p->prop, p->sem.type.col)) {
                *p = *PFla_dummy (L(p));
                break;
            }
            break;

        case la_step:
            break;

        case la_doc_access:
            /* prune doc_access if result column is not required */
            if (PFprop_not_icol (p->prop, p->sem.doc_access.res)) {
                *p = *PFla_dummy (R(p));
                break;
            }
            break;

        case la_roots:
            /* prune twig if result column is not required */
            if (L(p)->kind == la_twig &&
                PFprop_not_icol (p->prop, L(p)->sem.iter_item.item))
                switch (LL(p)->kind) {

                    case la_docnode:
                        *p = *PFla_project (
                                  LLL(p),
                                  PFalg_proj (
                                      L(p)->sem.iter_item.iter,
                                      LL(p)->sem.docnode.iter));
                        break;

                    case la_element:
                    case la_comment:
                        *p = *PFla_project (
                                  LLL(p),
                                  PFalg_proj (
                                      L(p)->sem.iter_item.iter,
                                      LL(p)->sem.iter_item.iter));
                        break;

                    case la_attribute:
                    case la_processi:
                        *p = *PFla_project (
                                  LLL(p),
                                  PFalg_proj (
                                      L(p)->sem.iter_item.iter,
                                      LL(p)->sem.iter_item1_item2.iter));
                        break;

                    case la_content:
                        *p = *PFla_project (
                                  LLR(p),
                                  PFalg_proj (
                                      L(p)->sem.iter_item.iter,
                                      LL(p)->sem.iter_pos_item.iter));
                        break;

                    case la_textnode:
                        /* As a textnode based on an empty string
                           has to result in an empty sequence we
                           are not allowed to throw away the textnode
                           constructor. */
                        break;

                    default:
                        assert(0);
                }
            else if (L(p)->kind == la_doc_tbl &&
                     PFprop_not_icol (L(p)->prop, L(p)->sem.doc_tbl.res)) {
                *p = *PFla_dummy (LL(p));
                break;
            }
            break;

        case la_frag_union:
            if (L(p)->kind == la_fragment &&
                LL(p)->kind == la_twig &&
                LLL(p)->kind != la_textnode && /* retain textnodes */
                PFprop_not_icol (LL(p)->prop, LL(p)->sem.iter_item.item))
                *p = *PFla_dummy (R(p));
            else if (R(p)->kind == la_fragment &&
                RL(p)->kind == la_twig &&
                RLL(p)->kind != la_textnode && /* retain textnodes */
                PFprop_not_icol (RL(p)->prop, RL(p)->sem.iter_item.item))
                *p = *PFla_dummy (L(p));
            else if (L(p)->kind == la_fragment &&
                LL(p)->kind == la_doc_tbl &&
                PFprop_not_icol (LL(p)->prop, LL(p)->sem.doc_tbl.res))
                *p = *PFla_dummy (R(p));
            else if (R(p)->kind == la_fragment &&
                RL(p)->kind == la_doc_tbl &&
                PFprop_not_icol (RL(p)->prop, RL(p)->sem.doc_tbl.res))
                *p = *PFla_dummy (L(p));
            break;

        case la_rec_arg:
            /* optimize the seeds */
            opt_icol (L(p));

            /* If some columns have been thrown away in the seeds
               we have to make sure that the base relations know
               about this fact. */
            if (L(p)->schema.count != p->schema.count) {
                PFalg_schema_t schema;

                schema.items = PFmalloc (L(p)->schema.count *
                                         sizeof (PFalg_schema_t));
                schema.count = L(p)->schema.count;

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    schema.items[i] = L(p)->schema.items[i];

                p->sem.rec_arg.base->schema = schema;
            }

            /* optimize the recursion body */
            opt_icol (R(p));

            /* If the both inputs to the recursion (seed and recursive call)
               contain different columns the schema of the recursive call has
               to be adjusted. A projection based on the schema of the seeds
               does the job.
               We can assume that both inputs to the rec_arg operator already
               use the same column names -- the initial translation and the
               name mappings both maintain this naming. */
            if (L(p)->schema.count != R(p)->schema.count) {
                PFalg_proj_t *proj;

                proj = PFmalloc (L(p)->schema.count * sizeof (PFalg_proj_t));

                for (unsigned int i = 0; i < L(p)->schema.count; i++)
                    proj[i] = PFalg_proj (L(p)->schema.items[i].name,
                                          L(p)->schema.items[i].name);

                R(p) = PFla_project_ (R(p), L(p)->schema.count, proj);
            }
            break;

        default:
            break;
    }

    /* ensure that we have the correct schema */
    PFprop_update_ocol (p);
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_icol (PFla_op_t *root)
{
    /* Infer icol properties first */
    PFprop_infer_icol (root);

    /* Optimize algebra tree */
    opt_icol (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
