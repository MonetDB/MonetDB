/**
 * @file
 *
 * This file contains optimizations that make the detection
 * of join graphs much easier. The idea is to use less but more generic
 * distinct operators (that e.g., remove duplicates of multiple nested
 * path steps in one go instead of a single distinct after each step).
 * Furthermore we try to replace all step operators by step_join operators
 * that can handle more input columns (than just iter|item) which should
 * ultimatively lead to less mapping joins and blocking rowid operators.
 *
 * Because the optimizations introduced here might hurt the efficiency
 * of the generated MIL code we used a separate optimization phase that
 * only the SQL generation (which can handle join graphs) calls by default.
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

/* Bottom-up worker for PFalgopt_join_graph */
static void
opt_join_graph (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply join-graph specific optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_join_graph (p->child[i]);

    /* action code */
    switch (p->kind) {
        /* Remove unnecessary distinct operators */
        case la_distinct:
            if (PFprop_ckey (L(p)->prop, p->schema))
                *p = *PFla_dummy (L(p));
            break;

        /* Use a rank operator with multiple sort criteria as the place
           where a new distinct operator based on the composite key
           property is introduced. (Rank operators with multiple sort
           criteria are only introduced during back-mapping -- after
           a for-loop body has been processed. Nested for-loops that
           return path steps thus will always benefit from a new distinct
           operator as nesting and the correct duplicates are treated
           by the new distinct operator.) */
        case la_rank:
            if (PFord_count (p->sem.sort.sortby) > 1 &&
                !PFprop_set (p->prop) && /* ignore nested rank operators */
                L(p)->kind != la_distinct) /* introduce distinct only once */ {
                PFalg_col_t col;
                PFalg_schema_t schema;
                schema.count = 0;
                schema.items = PFmalloc ((p->schema.count - 1) *
                                         sizeof (PFalg_schema_t));
                /* Create a new schema without result column... */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    col = p->schema.items[i].name;
                    if (col != p->sem.sort.res)
                        schema.items[schema.count++].name = col;
                }
                /* ... and use it to check if it contains a composite key. */
                if (PFprop_ckey (p->prop, schema) > 1)
                    L(p) = PFla_distinct (L(p));
            }
            break;

        /* Replace rowid generated columns by already existing
           key columns.
           !!!This means that iter columns with type aat_nat
              are replaced by columns of arbitrary type!!! */
        case la_rowid:
        {
            PFalg_col_t key = col_NULL;

            for (unsigned int i = 0; i < p->schema.count; i++)
                if (p->sem.rowid.res != p->schema.items[i].name &&
                    PFprop_key (p->prop, p->schema.items[i].name))
                    key = p->schema.items[i].name;

            if (key) {
                PFalg_proj_t *proj_list;

                /* create projection list */
                proj_list = PFmalloc (p->schema.count *
                                      sizeof (*(proj_list)));

                for (unsigned int i = 0; i < p->schema.count; i++)
                    if (p->sem.rowid.res != p->schema.items[i].name)
                        proj_list[i] = PFalg_proj (p->schema.items[i].name,
                                                   p->schema.items[i].name);
                    else
                        proj_list[i] = PFalg_proj (p->sem.rowid.res, key);

                *p = *PFla_project_ (L(p), p->schema.count, proj_list);
                break;
            }
            /* Get rid of a rowid operator that is only used to maintain
               the correct cardinality. In case other columns provide
               a compound key we replace the rowid operator by a rank
               operator consuming the compound key. */
            else if (PFprop_req_unique_col (p->prop, p->sem.rowid.res)) {
                PFalg_collist_t *collist;
                unsigned int     count = PFprop_ckeys_count (p->prop),
                                 i,
                                 j;
                for (i = 0; i < count; i++) {
                    collist = PFprop_ckey_at (p->prop, i);
                    /* make sure the result is not part of the compound key
                       and all columns of the new key are already used
                       elsewhere. */
                    for (j = 0; j < clsize (collist); j++)
                        if (clat (collist, j) == p->sem.rowid.res ||
                            !PFprop_icol (p->prop, clat (collist, j)))
                            break;
                    if (j == clsize (collist)) {
                        PFord_ordering_t sortby = PFordering ();
                        for (j = 0; j < clsize (collist); j++)
                            sortby = PFord_refine (sortby,
                                                   clat (collist, j),
                                                   DIR_ASC);
                        *p = *PFla_rank (L(p), p->sem.rowid.res, sortby);
                        break;
                    }
                }
            }
        }   break;

        case la_doc_access:
            /* replace the atomize (string-value) operator by
               a more explicit variant (descendant-or-self::text() step_join +
               doc_access(text) + string_join) the SQL Code generator can cope
               with */
            if (p->sem.doc_access.doc_col == doc_atomize) {
                PFalg_col_t       join_col1 = PFcol_new (col_iter),
                                  join_col2 = PFcol_new (col_iter),
                                  step_col  = PFcol_new (col_item),
                                  in_col    = p->sem.doc_access.col,
                                  res_col   = p->sem.doc_access.res;
                PFla_op_t        *rowid,
                                 *node_scj,
                                 *nodes;
                PFalg_step_spec_t desc_text_spec;
                PFalg_proj_t     *proj = PFmalloc (p->schema.count *
                                                   sizeof (PFalg_proj_t));

                rowid = rowid (R(p), join_col1);

                /* retrieve all descendant textnodes (`/descendant-or-self::text()') */
                desc_text_spec.axis = alg_desc_s;
                desc_text_spec.kind = node_kind_text;
                /* missing QName */
                desc_text_spec.qname = PFqname (PFns_wild, NULL);
                node_scj = PFla_step_join_simple (
                               L(p),
                               project (rowid,
                                        proj (join_col2, join_col1),
                                        proj (in_col, in_col)),
                               desc_text_spec,
                               in_col, step_col);

                /* concatenate all texts within an iteration using
                   the empty string as delimiter */
                nodes = fn_string_join (
                            project (
                                doc_access (
                                    L(p),
                                    node_scj,
                                    res_col, step_col, doc_text),
                                proj (join_col2, join_col2),
                                proj (step_col, step_col),
                                proj (res_col, res_col)),
                            project (
                                attach (rowid, res_col, lit_str ("")),
                                proj (join_col2, join_col1),
                                proj (res_col, res_col)),
                            join_col2, step_col, res_col,
                            join_col2, res_col,
                            join_col2, res_col);

                for (unsigned int i = 0; i < p->schema.count; i++)
                    proj[i] = PFalg_proj (p->schema.items[i].name,
                                          p->schema.items[i].name);

                /* align result with the other columns
                   (and prune additional columns) */
                *p = *PFla_project_ (eqjoin (
                                         rowid,
                                         nodes,
                                         join_col1,
                                         join_col2),
                                     p->schema.count,
                                     proj);
            }
            break;

#if 0
        /* Replace all step operators by step_join operators
           to allow a following optimization phase to get rid
           of unnecessary eqjoin and rowid operators */
        case la_step:
            if ((PFprop_key_right (p->prop, p->sem.step.item) &&
                 (p->sem.step.spec.axis == alg_attr ||
                  p->sem.step.spec.axis == alg_chld ||
                  p->sem.step.spec.axis == alg_self)) ||
                (PFprop_key_right (p->prop, p->sem.step.item) &&
                 LEVEL_KNOWN(PFprop_level_right (p->prop, p->sem.step.item)) &&
                 (p->sem.step.spec.axis == alg_desc ||
                  p->sem.step.spec.axis == alg_desc_s))) {

                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));

                *p = *PFla_project (
                             PFla_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.spec,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;

        /* Replace all guide_step operators by guide_step_join
           operators to allow a following optimization phase
           to get rid of unnecessary eqjoin and rowid operators */
        case la_guide_step:
            if (((PFprop_key_right (p->prop, p->sem.step.item) ||
                  PFprop_ckey (R(p)->prop, p->schema)) &&
                 (p->sem.step.spec.axis == alg_attr ||
                  p->sem.step.spec.axis == alg_chld ||
                  p->sem.step.spec.axis == alg_self)) ||
                ((PFprop_key_right (p->prop, p->sem.step.item) ||
                  PFprop_ckey (R(p)->prop, p->schema)) &&
                 LEVEL_KNOWN(PFprop_level_right (p->prop, p->sem.step.item)) &&
                 (p->sem.step.spec.axis == alg_desc ||
                  p->sem.step.spec.axis == alg_desc_s))) {

                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));

                *p = *PFla_project (
                             PFla_guide_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.spec,
                                 p->sem.step.guide_count,
                                 p->sem.step.guides,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;
#endif

        /* throw away proxy nodes and thus avoid calling option '-o {'
           which would remove all step_joins as well. */
        case la_proxy:
        case la_proxy_base:
            *p = *PFla_dummy (L(p));
            break;

        default:
            break;
    }
}

/* Top-down property worker for PFalgopt_join_graph */
static void
opt_set (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply join-graph specific optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_set (p->child[i]);

    /* action code */
    switch (p->kind) {
#if 0
        /* Replace semijoin operators by normal equi-joins. (A following
           optimization phase might then try to remove the equi-joins.) */
        case la_semijoin:
            if (PFprop_set (p->prop)) {
                /* we need to additional projections:
                   top    to remove the right join argument from the result list
                   right  to avoid name conflicts with the left input columns
                          and to ensure that distinct is applied
                          only on the join column */
                PFalg_proj_t *top   = PFmalloc (p->schema.count *
                                                sizeof (PFalg_proj_t));
                PFalg_col_t   new_col = PFalg_new_name (p->sem.eqjoin.col2),
                              cur_col;

                /* fill the 'top' projection and collect all used columns
                   of the left input relation (equals the semijoin output
                   schema) */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    cur_col = p->schema.items[i].name;

                    top[i] = PFalg_proj (cur_col, cur_col);
                }

                /* replace the semijoin */
                *p = *PFla_project_ (
                          PFla_eqjoin (L(p),
                                       /* project away all columns except
                                          for the join column */
                                       PFla_project (
                                           R(p),
                                           PFalg_proj (
                                               new_col,
                                               p->sem.eqjoin.col2)),
                                       p->sem.eqjoin.col1,
                                       new_col),
                          p->schema.count,
                          top);
            }
            break;
#endif

        /* Remove unnecessary distinct operators and narrow the schema
           of a distinct operator if some columns are not required
           while the remaining columns still provide a composite key. */
        case la_distinct:
            if (PFprop_set (p->prop))
                *p = *PFla_dummy (L(p));
            break;

#if 0
        /* Replace all step operators by step_join operators
           to allow a following optimization phase to get rid
           of unnecessary eqjoin and rowid operators */
        case la_step:
            if (PFprop_set (p->prop)) {

                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));

                *p = *PFla_project (
                             PFla_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.spec,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;

        /* Replace all guide_step operators by guide_step_join
           operators to allow a following optimization phase
           to get rid of unnecessary eqjoin and rowid operators */
        case la_guide_step:
            if (PFprop_set (p->prop)) {

                PFalg_col_t item_res;
                item_res = PFalg_ori_name (
                               PFalg_unq_name (col_item),
                               ~(p->sem.step.item |
                                 p->sem.step.iter));

                *p = *PFla_project (
                             PFla_guide_step_join (
                                 L(p),
                                 R(p),
                                 p->sem.step.spec,
                                 p->sem.step.guide_count,
                                 p->sem.step.guides,
                                 p->sem.step.level,
                                 p->sem.step.item,
                                 item_res),
                             PFalg_proj (p->sem.step.iter,
                                         p->sem.step.iter),
                             PFalg_proj (p->sem.step.item_res,
                                         item_res));
                break;
            }
            break;
#endif

        default:
            break;
    }
}

/* Bottom-up rewrite that introduces additional distinct operators on top
   of rank and equi-join operators if their schema contains a key. */
static void
opt_distinct (PFla_op_t *p)
{
    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply join-graph specific optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_distinct (p->child[i]);

    /* rewrite children */
    if (L(p) &&
        (L(p)->kind == la_rank ||
         L(p)->kind == la_eqjoin) &&
        !PFprop_set (L(p)->prop) &&
        (PFprop_keys_count (L(p)->prop) || 
         PFprop_ckey (L(p)->prop, L(p)->schema)))
        L(p) = PFla_distinct (L(p));

    if (R(p) &&
        (R(p)->kind == la_rank ||
         R(p)->kind == la_eqjoin) &&
        !PFprop_set (R(p)->prop) &&
        (PFprop_keys_count (R(p)->prop) || 
         PFprop_ckey (R(p)->prop, R(p)->schema)))
        R(p) = PFla_distinct (R(p));
}


/**
 * Invoke algebra optimization.
 *
 * NOTE: make sure that top-down and bottom-up properties (especially
 *       property key and set) are not mixed as this might result in
 *       inconsistencies (as e.g., two distinct operators are removed
 *       while one still has to remain).
 */
PFla_op_t *
PFalgopt_join_graph (PFla_op_t *root, PFguide_list_t *guide_list)
{
    /* PHASE 1: Seed distinct operators on top of the query plan
                if possible to remove all other distinct operators. */

    /* Infer all key information we can get */
    PFprop_infer_composite_key (root);
    PFprop_infer_key_and_fd (root);
    /* Infer set property to avoid the excessive 
       introduction of distinct operators */
    PFprop_infer_set (root);

    /* introduce additional distinct operators on top of the plan */
    opt_distinct (root);
    PFla_dag_reset (root);

    /* PHASE 2: optimize based on top-down set property */

    /* Infer set property once more */
    PFprop_infer_set (root);
    /* and evaluate all optimization on the top-down
       set property separately */
    opt_set (root);
    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property
       to remove inconsistencies introduced by changing the types
       of unreferenced columns (rule distinct). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);

    /* PHASE 3: optimize based on bottom-up properties */

    /* Infer key, icols, set, and composite key
       properties first */
    PFprop_infer_reqval (root);
    PFprop_infer_composite_key (root);
    PFprop_infer_icol (root);
    PFprop_infer_set (root);
    PFprop_infer_guide (root, guide_list);
    /* as a prerequisite infer the guides */
    PFprop_infer_key_with_guide (root);
    /* level is already inferred by key */
    /* PFprop_infer_level (root); */

    /* Optimize algebra tree */
    opt_join_graph (root);
    PFla_dag_reset (root);

    /* In addition optimize the resulting DAG using the icols property
       to remove inconsistencies introduced by changing the types
       of unreferenced columns (rule distinct). The icols optimization
       will ensure that these columns are 'really' never used. */
    root = PFalgopt_icol (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */ 
/* vim:set foldmarker=#if,#endif foldmethod=marker foldopen-=search: */
