/**
 * @file
 *
 * Map relational algebra expression DAG with unique column names
 * into an equivalent one with bit-encoded column names.
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

#include "map_names.h"
#include "properties.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(p) ((p)->bit_dag)

/* lookup subtree with original column names */
#define O(p) (lookup (map, (p)))
/* shortcut for function PFprop_ori_name */
#define ONAME(p,col) (PFprop_ori_name ((p)->prop,(col)))

/* helper macros for renaming projection */
#define LEFT 0
#define RIGHT 1
#define C_UNAME(p,col,s) ((s) ? PFprop_unq_name_right ((p)->prop,(col)) \
                              : PFprop_unq_name_left  ((p)->prop,(col)))

struct ori_unq_map {
    PFla_op_t *ori;
    PFla_op_t *unq;
};
typedef struct ori_unq_map ori_unq_map;

/* worker for macro 'O(p)': based on an original subtree
   looks up the corresponding subtree with original column names */
static PFla_op_t *
lookup (PFarray_t *map, PFla_op_t *unq)
{
    for (unsigned int i = 0; i < PFarray_last (map); i++)
        if (((ori_unq_map *) PFarray_at (map, i))->unq == unq)
            return ((ori_unq_map *) PFarray_at (map, i))->ori;

    assert (!"could not look up node");

    return NULL;
}

/**
 * Add a projection above the @a side child of operator @a p
 * whenever some columns needs to be renamed or if the column
 * @a free_col is bound without projection.
 * (see also macros using the function below)
 */
static PFla_op_t *
add_renaming_projection (PFla_op_t *p,
                         unsigned int side,
                         PFalg_col_t free_col,
                         PFarray_t *map)
{
    PFalg_col_t ori_new, ori_old, unq, ori_free;
    PFla_op_t *c = O(p->child[side]);
    PFalg_proj_t *projlist = PFmalloc (c->schema.count *
                                       sizeof (PFalg_proj_t));
    bool renamed = false;
    unsigned int count = 0;

    ori_free = free_col ? ONAME(p, free_col) : col_NULL;

    for (unsigned int i = 0; i < c->schema.count; i++) {
        ori_old = c->schema.items[i].name;

        /* Enforce projection if column free_col
           is not free without projection */
        if (ori_old == ori_free)
            renamed = true;

        /* lookup unique name for column @a ori_old */
        unq = C_UNAME (p, ori_old, side);

        /* column ori_old is not referenced by operator @a p
           and thus does not appear in the projection */
        if (!unq) continue;

        /* lookup corresponding new name for column @a ori_old */
        ori_new = ONAME(p, unq);

        /* don't allow missing matches */
        assert (ori_new);

        projlist[count++] = proj (ori_new, ori_old);
        renamed = renamed || (ori_new != ori_old);
    }

    if (renamed)
        return PFla_project_ (c, count, projlist);
    else
        return c;
}
/* shortcut for simplified function add_renaming_projection */
#define PROJ(s,p) add_renaming_projection ((p),(s), col_NULL, map)
/* 'SECure PROJection': shortcut for function add_renaming_projection
   that ensures that column 'a; does not appear in the 's'.th child
   of 'p' */
#define SEC_PROJ(s,p,a) add_renaming_projection ((p),(s), (a), map)

/* worker for unary operators */
static PFla_op_t *
unary_op (PFla_op_t *(*OP) (const PFla_op_t *,
                            PFalg_col_t, PFalg_col_t),
          PFla_op_t *p,
          PFarray_t *map)
{
    return OP (SEC_PROJ(LEFT, p, p->sem.unary.res),
               ONAME(p, p->sem.unary.res),
               ONAME(p, p->sem.unary.col));
}

/* worker for binary operators */
static PFla_op_t *
binary_op (PFla_op_t *(*OP) (const PFla_op_t *, PFalg_col_t,
                             PFalg_col_t, PFalg_col_t),
           PFla_op_t *p,
           PFarray_t *map)
{
    return OP (SEC_PROJ(LEFT, p, p->sem.binary.res),
               ONAME(p, p->sem.binary.res),
               ONAME(p, p->sem.binary.col1),
               ONAME(p, p->sem.binary.col2));
}

/* worker for PFmap_ori_names */
static void do_map_ori_names (PFla_op_t *p, PFarray_t *map);

static void
map_ori_names (PFla_op_t *p, PFarray_t *map)
{
    assert (p);

    PFrecursion_fence ();

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply name mapping for children bottom up */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        map_ori_names (p->child[i], map);

    do_map_ori_names (p, map);
}

static void
do_map_ori_names (PFla_op_t *p, PFarray_t *map)
{
    PFla_op_t *res = NULL;

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
            res = serialize_seq (O(L(p)),
                                 PROJ(RIGHT, p),
                                 ONAME(p, p->sem.ser_seq.pos),
                                 ONAME(p, p->sem.ser_seq.item));
            break;

        case la_serialize_rel:
        {
            PFalg_collist_t *items = PFalg_collist_copy (p->sem.ser_rel.items);
            for (unsigned int i = 0; i < clsize (items); i++)
                clat (items, i) = ONAME(p, clat (items, i));

            res = serialize_rel (O(L(p)),
                                 PROJ(RIGHT, p),
                                 ONAME(p, p->sem.ser_rel.iter),
                                 ONAME(p, p->sem.ser_rel.pos),
                                 items);
        }   break;

        case la_side_effects:
            res = PFla_side_effects (O(L(p)), O(R(p)));
            break;


        case la_lit_tbl:
        {
            PFalg_collist_t *collist = PFalg_collist (p->schema.count);

            for (unsigned int i = 0; i < p->schema.count; i++)
                cladd (collist) = ONAME(p, p->schema.items[i].name);

            res = PFla_lit_tbl_ (collist,
                                 p->sem.lit_tbl.count,
                                 p->sem.lit_tbl.tuples);
        }   break;

        case la_empty_tbl:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items = PFmalloc (schema.count *
                                     sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = ONAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = PFla_empty_tbl_ (schema);
        }   break;

        case la_ref_tbl:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items = PFmalloc (schema.count *
                                     sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = ONAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = PFla_ref_tbl_ (p->sem.ref_tbl.name,
                                 schema,
                                 p->sem.ref_tbl.tcols,
                                 p->sem.ref_tbl.keys);
        }   break;

        case la_attach:
            res = attach (SEC_PROJ(LEFT, p, p->sem.attach.res),
                          ONAME(p, p->sem.attach.res),
                          p->sem.attach.value);
            break;

        case la_cross:
            res = cross (PROJ(LEFT, p), PROJ(RIGHT, p));
            break;

        case la_eqjoin:
            res = eqjoin (PROJ(LEFT, p),
                          PROJ(RIGHT, p),
                          ONAME (p, p->sem.eqjoin.col1),
                          ONAME (p, p->sem.eqjoin.col2));
            break;

        case la_internal_op:
            /* interpret this operator as internal join */
            if (p->sem.eqjoin_opt.kind == la_eqjoin) {
            /* We need to turn the implicit projections
               into explicit ones to use the la_eqjoin
               operator again. */
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
                PFarray_t    *lproj  = p->sem.eqjoin_opt.lproj,
                             *rproj  = p->sem.eqjoin_opt.rproj;
                PFalg_proj_t *projlist,
                             *lprojlist,
                             *rprojlist;
                PFla_op_t    *left   = O(L(p)),
                             *right  = O(R(p));
                unsigned int  lcount = 0,
                              rcount = 0,
                              i;
                PFalg_col_t   col1_new,
                              col2_new,
                              col2_old,
                              col2_unq,
                              used_cols = 0,
                              ori,
                              unq_new,
                              unq_old,
                              ori_new,
                              ori_old;
                
                /* look up the join column names */
                col1_new = ONAME(p, proj_at (lproj, 0).new);
                col2_unq = proj_at (rproj, 0).old;
                col2_old = PFprop_ori_name_right (p->prop, col2_unq);

                projlist  = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
                lprojlist = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
                rprojlist = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
                
                /* create the projection list for the left operand */
                for (i = 0; i < PFarray_last (lproj); i++) {
                    unq_new = proj_at (lproj, i).new;
                    unq_old = proj_at (lproj, i).old;
                    ori_new = ONAME (p, unq_new);
                    ori_old = ONAME (L(p), unq_old);
                    lprojlist[lcount++] = proj (ori_new, ori_old);
                }

                /* Create a new unused column name for the right join argument
                   that is projected away again. This way name conflicts are
                   avoided. */
                for (i = 0; i < p->schema.count; i++)
                    used_cols |= ONAME(p, p->schema.items[i].name);
                col2_new = PFcol_ori_name (col2_unq, ~used_cols);
                rprojlist[rcount++] = proj (col2_new, col2_old);

                /* create the projection list for the right operand */
                for (i = 1; i < PFarray_last (rproj); i++) {
                    unq_new = proj_at (rproj, i).new;
                    unq_old = proj_at (rproj, i).old;
                    ori_new = ONAME (p, unq_new);
                    ori_old = ONAME (R(p), unq_old);
                    rprojlist[rcount++] = proj (ori_new, ori_old);
                }

                res = eqjoin (PFla_project_ (left, lcount, lprojlist),
                              PFla_project_ (right, rcount, rprojlist),
                              col1_new, col2_new);

                /* As some operators rely on the schema of its operands
                   we introduce a projection that removes the second join
                   column thus maintaining the schema of the duplicate
                   aware eqjoin operator. */
                for (unsigned int i = 0; i < p->schema.count; i++) {
                    ori = ONAME(p, p->schema.items[i].name);
                    projlist[i] = proj (ori, ori);
                }
                res = PFla_project_ (res, p->schema.count, projlist);
            }
            else
                PFoops (OOPS_FATAL,
                        "internal optimization operator is not allowed here");
            break;

        case la_semijoin:
            res = semijoin (PROJ(LEFT, p), O(R(p)),
                            ONAME (p, p->sem.eqjoin.col1),
                            PFprop_ori_name_right (p->prop,
                                                   p->sem.eqjoin.col2));
            break;

        case la_thetajoin:
        {
            PFalg_sel_t *pred = PFmalloc (p->sem.thetajoin.count *
                                          sizeof (PFalg_sel_t));

            for (unsigned int i = 0; i < p->sem.thetajoin.count; i++)
                pred[i] = PFalg_sel (p->sem.thetajoin.pred[i].comp,
                                     ONAME (p, p->sem.thetajoin.pred[i].left),
                                     ONAME (p, p->sem.thetajoin.pred[i].right));

            res = thetajoin (PROJ(LEFT, p), PROJ(RIGHT, p),
                             p->sem.thetajoin.count, pred);
        }   break;

        case la_project:
        {
            PFla_op_t *left;
            PFalg_proj_t *projlist = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
            PFalg_col_t new, old;
            unsigned int count = 0;
            bool renamed = false;

            left = O(L(p));

            for (unsigned int i = 0; i < left->schema.count; i++) {
                old = left->schema.items[i].name;
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    /* we may get multiple hits */
                    if (old == PFprop_ori_name_left (
                                   p->prop,
                                   p->sem.proj.items[j].old)) {
                        new = ONAME(p, p->sem.proj.items[j].new);
                        projlist[count++] = proj (new, old);
                        renamed = renamed || (new != old);
                    }
            }

            /* if the projection does not prune a column
               we may skip the projection operator */
            if (count == left->schema.count && !renamed)
                res = left;
            else
                res = PFla_project_ (left, count, projlist);
        }   break;

        case la_select:
            res = select_ (PROJ(LEFT, p), ONAME(p, p->sem.select.col));
            break;

        case la_pos_select:
        {
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.pos_sel.sortby);
                 i++)
                sortby = PFord_refine (
                             sortby,
                             ONAME(p, PFord_order_col_at (
                                          p->sem.pos_sel.sortby, i)),
                             PFord_order_dir_at (
                                 p->sem.pos_sel.sortby, i));

            res = pos_select (PROJ(LEFT, p),
                              p->sem.pos_sel.pos,
                              sortby,
                              ONAME(p, p->sem.pos_sel.part));
        }   break;

        case la_disjunion:
            res = disjunion (PROJ(LEFT, p), PROJ(RIGHT, p));
            break;

        case la_intersect:
            res = intersect (PROJ(LEFT, p), PROJ(RIGHT, p));
            break;

        case la_difference:
            res = difference (PROJ(LEFT, p), PROJ(RIGHT, p));
            break;

        case la_distinct:
            res = distinct (PROJ(LEFT, p));
            break;

        case la_fun_1to1:
        {
            PFalg_collist_t *refs = PFalg_collist_copy (p->sem.fun_1to1.refs);
            for (unsigned int i = 0; i < clsize (refs); i++)
                clat (refs, i) = ONAME(p, clat (refs, i));

            res = fun_1to1 (SEC_PROJ(LEFT, p, p->sem.fun_1to1.res),
                            p->sem.fun_1to1.kind,
                            ONAME(p, p->sem.fun_1to1.res),
                            refs);
        }   break;

        case la_num_eq:
            res = binary_op (PFla_eq, p, map);
            break;
        case la_num_gt:
            res = binary_op (PFla_gt, p, map);
            break;
        case la_bool_and:
            res = binary_op (PFla_and, p, map);
            break;
        case la_bool_or:
            res = binary_op (PFla_or, p, map);
            break;
        case la_bool_not:
            res = unary_op (PFla_not, p, map);
            break;

        case la_to:
            res = binary_op (PFla_to, p, map);
            break;
            
        case la_aggr:
        {
            PFla_op_t *left;
            PFalg_aggr_t *aggr = PFmalloc (p->sem.aggr.count *
                                           sizeof (PFalg_aggr_t));
            PFalg_col_t  col;
            unsigned int count = 0;

            left = O(L(p));

            for (unsigned int i = 0; i < left->schema.count; i++) {
                col = left->schema.items[i].name;
                for (unsigned int j = 0; j < p->sem.aggr.count; j++)
                    /* we may get multiple hits */
                    if (col == PFprop_ori_name_left (
                                   p->prop,
                                   p->sem.aggr.aggr[j].col)) {
                        aggr[count++] = PFalg_aggr (
                                            p->sem.aggr.aggr[j].kind,
                                            ONAME(p, p->sem.aggr.aggr[j].res),
                                            col);
                    }
            }
            for (unsigned int j = 0; j < p->sem.aggr.count; j++)
                if (p->sem.aggr.aggr[j].kind == alg_aggr_count)
                    aggr[count++] = PFalg_aggr (
                                        p->sem.aggr.aggr[j].kind,
                                        ONAME(p, p->sem.aggr.aggr[j].res),
                                        col_NULL);

            assert (count == p->sem.aggr.count);

            res = aggr (left,
                        p->sem.aggr.part?ONAME(p, p->sem.aggr.part):col_NULL,
                        p->sem.aggr.count,
                        aggr);
        }   break;

        case la_rownum:
        case la_rowrank:
        case la_rank:
        {
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.sort.sortby);
                 i++)
                sortby = PFord_refine (
                             sortby,
                             ONAME(p, PFord_order_col_at (
                                          p->sem.sort.sortby, i)),
                             PFord_order_dir_at (
                                 p->sem.sort.sortby, i));

            if (p->kind == la_rownum)
                res = rownum (SEC_PROJ(LEFT, p, p->sem.sort.res),
                              ONAME(p, p->sem.sort.res),
                              sortby,
                              ONAME(p, p->sem.sort.part));
            else if (p->kind == la_rowrank)
                res = rowrank (SEC_PROJ(LEFT, p, p->sem.sort.res),
                               ONAME(p, p->sem.sort.res),
                               sortby);
            else if (p->kind == la_rank)
                res = rank (SEC_PROJ(LEFT, p, p->sem.sort.res),
                            ONAME(p, p->sem.sort.res),
                            sortby);
        }   break;

        case la_rowid:
            res = rowid (SEC_PROJ(LEFT, p, p->sem.rowid.res),
                         ONAME(p, p->sem.rowid.res));
            break;

        case la_type:
            res = type (SEC_PROJ(LEFT, p, p->sem.type.res),
                        ONAME(p, p->sem.type.res),
                        ONAME(p, p->sem.type.col),
                        p->sem.type.ty);
            break;

        case la_type_assert:
            res = type_assert_pos (PROJ(LEFT, p),
                                   ONAME(p, p->sem.type.col),
                                   p->sem.type.ty);
            break;

        case la_cast:
            res = cast (SEC_PROJ(LEFT, p, p->sem.type.res),
                        ONAME(p, p->sem.type.res),
                        ONAME(p, p->sem.type.col),
                        p->sem.type.ty);
            break;

        case la_step:
            /* In case columns iter and item_in are identical columns
               item_in and item_out cannot refer to the same original
               name. Then we need to ensure that we correct the naming
               upfront (as the physical translation relies on the fact
               that item_in and item_out are identical columns). */
            if (ONAME(p, p->sem.step.item) !=
                ONAME(p, p->sem.step.item_res)) {
                PFalg_col_t iter, item_in, item_out;
                iter     = ONAME(p, p->sem.step.iter);
                item_in  = ONAME(p, p->sem.step.item);
                item_out = ONAME(p, p->sem.step.item_res);

                assert (item_in == iter);

                res = step (O(L(p)),
                            project (PROJ(RIGHT, p),
                                     proj (iter, iter),
                                     proj (item_out, item_in)),
                            p->sem.step.spec,
                            p->sem.step.level,
                            iter,
                            item_out,
                            item_out);
            } else
                res = step (O(L(p)), PROJ(RIGHT, p),
                            p->sem.step.spec,
                            p->sem.step.level,
                            ONAME(p, p->sem.step.iter),
                            ONAME(p, p->sem.step.item),
                            ONAME(p, p->sem.step.item_res));
            break;

        case la_step_join:
            res = step_join (O(L(p)),
                             SEC_PROJ(RIGHT, p, p->sem.step.item_res),
                             p->sem.step.spec,
                             p->sem.step.level,
                             ONAME(p, p->sem.step.item),
                             ONAME(p, p->sem.step.item_res));
            break;

        case la_guide_step:
            /* In case columns iter and item_in are identical columns
               item_in and item_out cannot refer to the same original
               name. Then we need to ensure that we correct the naming
               upfront (as the physical translation relies on the fact
               that item_in and item_out are identical columns). */
            if (ONAME(p, p->sem.step.item) !=
                ONAME(p, p->sem.step.item_res)) {
                PFalg_col_t iter, item_in, item_out;
                iter     = ONAME(p, p->sem.step.iter);
                item_in  = ONAME(p, p->sem.step.item);
                item_out = ONAME(p, p->sem.step.item_res);

                assert (item_in == iter);

                res = guide_step (O(L(p)),
                                  project (PROJ(RIGHT, p),
                                           proj (iter, iter),
                                           proj (item_out, item_in)),
                                  p->sem.step.spec,
                                  p->sem.step.guide_count,
                                  p->sem.step.guides,
                                  p->sem.step.level,
                                  iter,
                                  item_out,
                                  item_out);
            } else
                res = guide_step (O(L(p)), PROJ(RIGHT, p),
                                  p->sem.step.spec,
                                  p->sem.step.guide_count,
                                  p->sem.step.guides,
                                  p->sem.step.level,
                                  ONAME(p, p->sem.step.iter),
                                  ONAME(p, p->sem.step.item),
                                  ONAME(p, p->sem.step.item_res));
            break;

        case la_guide_step_join:
            res = guide_step_join (O(L(p)),
                                   SEC_PROJ(RIGHT, p, p->sem.step.item_res),
                                   p->sem.step.spec,
                                   p->sem.step.guide_count,
                                   p->sem.step.guides,
                                   p->sem.step.level,
                                   ONAME(p, p->sem.step.item),
                                   ONAME(p, p->sem.step.item_res));
            break;

        case la_doc_index_join:
            res = doc_index_join (O(L(p)),
                                  SEC_PROJ(RIGHT, p, p->sem.doc_join.item_res),
                                  p->sem.doc_join.kind,
                                  ONAME(p, p->sem.doc_join.item),
                                  ONAME(p, p->sem.doc_join.item_res),
                                  ONAME(p, p->sem.doc_join.item_doc),
                                  p->sem.doc_join.ns1,
                                  p->sem.doc_join.loc1,
                                  p->sem.doc_join.ns2,
                                  p->sem.doc_join.loc2);
            break;

        case la_doc_tbl:
            res = doc_tbl (SEC_PROJ(LEFT, p, p->sem.doc_tbl.res),
                           ONAME(p, p->sem.doc_tbl.res),
                           ONAME(p, p->sem.doc_tbl.col),
                           p->sem.doc_tbl.kind);
            break;

        case la_doc_access:
            res = doc_access (O(L(p)),
                              SEC_PROJ(RIGHT, p, p->sem.doc_access.res),
                              ONAME(p, p->sem.doc_access.res),
                              ONAME(p, p->sem.doc_access.col),
                              p->sem.doc_access.doc_col);
            break;

        case la_twig:
            res = twig (O(L(p)),
                        ONAME(p, p->sem.iter_item.iter),
                        ONAME(p, p->sem.iter_item.item));
            break;

        case la_fcns:
            res = fcns (O(L(p)), O(R(p)));
            break;

        case la_docnode:
            res = docnode (PROJ(LEFT, p), O(R(p)),
                           ONAME (p, p->sem.docnode.iter));
            break;

        case la_element:
            res = element (
                      PROJ(LEFT, p), O(R(p)),
                      ONAME (p, p->sem.iter_item.iter),
                      ONAME (p, p->sem.iter_item.item));
            break;

        case la_attribute:
            res = attribute (PROJ(LEFT, p),
                             ONAME(p, p->sem.iter_item1_item2.iter),
                             ONAME(p, p->sem.iter_item1_item2.item1),
                             ONAME(p, p->sem.iter_item1_item2.item2));
            break;

        case la_textnode:
            res = textnode (PROJ(LEFT, p),
                            ONAME (p, p->sem.iter_item.iter),
                            ONAME (p, p->sem.iter_item.item));
            break;

        case la_comment:
            res = comment (PROJ(LEFT, p),
                           ONAME (p, p->sem.iter_item.iter),
                           ONAME (p, p->sem.iter_item.item));
            break;

        case la_processi:
            res = processi (PROJ(LEFT, p),
                            ONAME(p, p->sem.iter_item1_item2.iter),
                            ONAME(p, p->sem.iter_item1_item2.item1),
                            ONAME(p, p->sem.iter_item1_item2.item2));
            break;

        case la_content:
            res = content (O(L(p)), PROJ(RIGHT, p),
                           ONAME(p, p->sem.iter_pos_item.iter),
                           ONAME(p, p->sem.iter_pos_item.pos),
                           ONAME(p, p->sem.iter_pos_item.item));
            break;

        case la_merge_adjacent:
            /* In case columns pos and item_in are identical columns
               item_in and item_out cannot refer to the same original
               name. Then we need to ensure that we correct the naming
               upfront (as the physical translation relies on the fact
               that item_in and item_out are identical columns). */
            if (ONAME(p, p->sem.merge_adjacent.item_in) !=
                ONAME(p, p->sem.merge_adjacent.item_res)) {
                PFalg_col_t iter, pos, item_in, item_out;
                iter     = ONAME(p, p->sem.merge_adjacent.iter_in);
                pos      = ONAME(p, p->sem.merge_adjacent.pos_in);
                item_in  = ONAME(p, p->sem.merge_adjacent.item_in);
                item_out = ONAME(p, p->sem.merge_adjacent.item_res);

                assert (iter == ONAME(p, p->sem.merge_adjacent.iter_res));
                assert (pos  == ONAME(p, p->sem.merge_adjacent.pos_res));
                assert (item_in == pos);

                res = merge_adjacent (
                          O(L(p)),
                          project (PROJ(RIGHT, p),
                                   proj (iter, iter),
                                   proj (pos, pos),
                                   proj (item_out, item_in)),
                          iter,
                          pos,
                          item_out,
                          iter,
                          pos,
                          item_out);
            }
            else
                res = merge_adjacent (
                          O(L(p)), PROJ(RIGHT, p),
                          ONAME(p, p->sem.merge_adjacent.iter_in),
                          ONAME(p, p->sem.merge_adjacent.pos_in),
                          ONAME(p, p->sem.merge_adjacent.item_in),
                          ONAME(p, p->sem.merge_adjacent.iter_res),
                          ONAME(p, p->sem.merge_adjacent.pos_res),
                          ONAME(p, p->sem.merge_adjacent.item_res));
            break;

        case la_roots:
            res = roots (O(L(p)));
            break;

        case la_fragment:
            res = fragment (O(L(p)));
            break;

        case la_frag_extract:
            res = frag_extract (O(L(p)), p->sem.col_ref.pos);
            break;

        case la_frag_union:
            res = PFla_frag_union (O(L(p)), O(R(p)));
            break;

        case la_empty_frag:
            res = empty_frag ();
            break;

        case la_error:
            res = PFla_error (O(L(p)), O(R(p)),
                              PFprop_ori_name_right (p->prop, p->sem.err.col));
            break;

        case la_nil:
            res = nil ();
            break;

        case la_cache:
            res = PFla_cache (O(L(p)), O(R(p)),
                              p->sem.cache.id,
                              PFprop_ori_name_right (
                                  p->prop,
                                  p->sem.cache.pos),
                              PFprop_ori_name_right (
                                  p->prop,
                                  p->sem.cache.item));
            break;

        case la_trace:
            res = trace (O(L(p)), O(R(p)));
            break;

        case la_trace_items:
            res = trace_items (
                      PROJ(LEFT, p),
                      O(R(p)),
                      ONAME(p, p->sem.iter_pos_item.iter),
                      ONAME(p, p->sem.iter_pos_item.pos),
                      ONAME(p, p->sem.iter_pos_item.item));
            break;

        case la_trace_msg:
            res = trace_msg (
                      PROJ(LEFT, p),
                      O(R(p)),
                      ONAME(p, p->sem.iter_item.iter),
                      ONAME(p, p->sem.iter_item.item));
            break;

        case la_trace_map:
            res = trace_map (
                      PROJ(LEFT, p),
                      O(R(p)),
                      ONAME(p, p->sem.trace_map.inner),
                      ONAME(p, p->sem.trace_map.outer));
            break;

        case la_rec_fix:
            res = rec_fix (O(L(p)),
                           PROJ(RIGHT, p));
            break;

        case la_rec_param:
        {
            PFla_op_t *arg = NULL;

            /* In case the recursion argument is not referenced anymore
               we can safely throw it away. */
            for (unsigned int i = 0; i < PFarray_last (map); i++)
                if (((ori_unq_map *) PFarray_at (map, i))->unq == L(p)) {
                    arg = ((ori_unq_map *) PFarray_at (map, i))->ori;
                    break;
                }

            if (arg)
                res = rec_param (arg, O(R(p)));
            else
                res = O(R(p));
        }   break;

        case la_rec_arg:
        /* The both inputs (seed and recursion) may not use the same
           column names (as the base). Thus we live with inconsistent
           original names and additionally add a renaming projection
           (Schema L -> Schema Base and Schema R -> Schema Base)
           if necessary. */
        {
            PFla_op_t *base = NULL;
            PFalg_col_t unq, base_ori, seed_ori, rec_ori;
            unsigned int count = p->schema.count;
            bool seed_rename = false, rec_rename = false;
            PFalg_proj_t *seed_proj = PFmalloc (count *
                                                sizeof (PFalg_proj_t));
            PFalg_proj_t *rec_proj = PFmalloc (count *
                                               sizeof (PFalg_proj_t));

            assert (count == p->sem.rec_arg.base->schema.count &&
                    count == L(p)->schema.count &&
                    count == R(p)->schema.count);

            for (unsigned int i = 0; i < count; i++) {
                unq      = p->sem.rec_arg.base->schema.items[i].name;
                base_ori = ONAME(p->sem.rec_arg.base, unq);
                seed_ori = ONAME(L(p), unq);
                rec_ori  = ONAME(R(p), unq);

                seed_proj[i] = proj (base_ori, seed_ori);
                rec_proj[i]  = proj (base_ori, rec_ori);

                seed_rename = seed_rename || (base_ori != seed_ori);
                rec_rename  = rec_rename  || (base_ori != rec_ori);
            }

            /* In case the recursion base is not referenced anymore
               we do not need to include the recursion argument. */
            for (unsigned int i = 0; i < PFarray_last (map); i++)
                if (((ori_unq_map *) PFarray_at (map, i))->unq
                    == p->sem.rec_arg.base) {
                    base = ((ori_unq_map *) PFarray_at (map, i))->ori;
                    break;
                }

            if (base)
                res = rec_arg (seed_rename
                                   ? PFla_project_ (O(L(p)), count, seed_proj)
                                   : O(L(p)),
                               rec_rename
                                   ? PFla_project_ (O(R(p)), count, rec_proj)
                                   : O(R(p)),
                               base);
            else
                return;
        }   break;

        case la_rec_base:
        /* combine the new original names with the already
           existing types (see also 'case la_empty_tbl') */
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items = PFmalloc (schema.count *
                                     sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = ONAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = rec_base (schema);
        } break;

        case la_fun_call:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items = PFmalloc (schema.count *
                                     sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = ONAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = fun_call (O(L(p)), O(R(p)),
                            schema,
                            p->sem.fun_call.kind,
                            p->sem.fun_call.qname,
                            p->sem.fun_call.ctx,
                            ONAME(L(p), p->sem.fun_call.iter),
                            p->sem.fun_call.occ_ind);
        }   break;

        case la_fun_param:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items = PFmalloc (schema.count *
                                     sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = ONAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = fun_param (PROJ(LEFT, p), O(R(p)), schema);
        }   break;

        case la_fun_frag_param:
            res = fun_frag_param (O(L(p)), O(R(p)), p->sem.col_ref.pos);
            break;

        case la_proxy:
        case la_proxy_base:
            PFoops (OOPS_FATAL,
                    "PROXY EXPANSION MISSING");
            break;

        case la_string_join:
            res = fn_string_join (
                      PROJ(LEFT, p), PROJ(RIGHT, p),
                      ONAME(p, p->sem.string_join.iter),
                      ONAME(p, p->sem.string_join.pos),
                      ONAME(p, p->sem.string_join.item),
                      ONAME(p, p->sem.string_join.iter_sep),
                      ONAME(p, p->sem.string_join.item_sep),
                      ONAME(p, p->sem.string_join.iter_res),
                      ONAME(p, p->sem.string_join.item_res));
            break;

        case la_dummy:
            res = O(L(p));
            break;
    }

    assert(res);

    /* Add pair (p, res) to the environment map
       to allow lookup of already generated subplans. */
    *(ori_unq_map *) PFarray_add (map) =
        (ori_unq_map) { .ori = res, .unq = p};
}

/**
 * Invoke name mapping.
 */
PFla_op_t *
PFmap_ori_names (PFla_op_t *root)
{
    PFarray_t *map = PFarray (sizeof (ori_unq_map), 300);

    /* infer original bit-encoded names */
    PFprop_infer_ori_names (root);

    /* generate equivalent algebra DAG */
    map_ori_names (root, map);

    /* return algebra DAG with original bit-encoded names */
    return O(root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
