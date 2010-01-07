/**
 * @file
 *
 * Map relational algebra expression DAG with
 * bit-encoded column names into an equivalent
 * one with unique column names.
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

/* lookup subtree with unique column names */
#define U(p) (lookup (map, (p)))
/* shortcut for function PFprop_unq_name */
#define UNAME(p,col) (PFprop_unq_name ((p)->prop,(col)))

struct ori_unq_map {
    PFla_op_t *ori;
    PFla_op_t *unq;
};
typedef struct ori_unq_map ori_unq_map;

/* worker for macro 'U(p)': based on an original subtree
   looks up the corresponding subtree with unique column names */
static PFla_op_t *
lookup (PFarray_t *map, PFla_op_t *ori)
{
    for (unsigned int i = 0; i < PFarray_last (map); i++)
        if (((ori_unq_map *) PFarray_at (map, i))->ori == ori)
            return ((ori_unq_map *) PFarray_at (map, i))->unq;

    assert (!"could not look up node");

    return NULL;
}

/* worker for unary operators */
static PFla_op_t *
unary_op (PFla_op_t *(*OP) (const PFla_op_t *,
                            PFalg_col_t, PFalg_col_t),
          PFla_op_t *p,
          PFarray_t *map)
{
    return OP (U(L(p)),
               UNAME(p, p->sem.unary.res),
               UNAME(p, p->sem.unary.col));
}

/* worker for binary operators */
static PFla_op_t *
binary_op (PFla_op_t *(*OP) (const PFla_op_t *, PFalg_col_t,
                             PFalg_col_t, PFalg_col_t),
           PFla_op_t *p,
           PFarray_t *map)
{
    return OP (U(L(p)),
               UNAME(p, p->sem.binary.res),
               UNAME(p, p->sem.binary.col1),
               UNAME(p, p->sem.binary.col2));
}

/* worker for PFmap_unq_names */
static void
map_unq_names (PFla_op_t *p, PFarray_t *map)
{
    PFla_op_t *res = NULL;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply name mapping for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        map_unq_names (p->child[i], map);

    /* action code */
    switch (p->kind) {
        case la_serialize_seq:
            res = serialize_seq (U(L(p)), U(R(p)),
                                 UNAME(p, p->sem.ser_seq.pos),
                                 UNAME(p, p->sem.ser_seq.item));
            break;

        case la_serialize_rel:
        {
            PFalg_collist_t *items = PFalg_collist_copy (p->sem.ser_rel.items);
            for (unsigned int i = 0; i < clsize (items); i++)
                clat (items, i) = UNAME(p, clat (items, i));

            res = serialize_rel (U(L(p)), U(R(p)),
                                 UNAME(p, p->sem.ser_rel.iter),
                                 UNAME(p, p->sem.ser_rel.pos),
                                 items);
        }   break;

        case la_side_effects:
            res = PFla_side_effects (U(L(p)), U(R(p)));
            break;

        case la_lit_tbl:
        {
            PFalg_collist_t *collist = PFalg_collist (p->schema.count);

            for (unsigned int i = 0; i < p->schema.count; i++)
                cladd (collist) = UNAME(p, p->schema.items[i].name);

            res = PFla_lit_tbl_ (collist,
                                 p->sem.lit_tbl.count,
                                 p->sem.lit_tbl.tuples);
        }   break;

        case la_empty_tbl:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = PFla_empty_tbl_ (schema);
        }   break;

        case la_ref_tbl:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };


            res = PFla_ref_tbl_ (p->sem.ref_tbl.name,
                                 schema,
                                 p->sem.ref_tbl.tcols,
                                 p->sem.ref_tbl.keys);
        }   break;

        case la_attach:
            res = attach (U(L(p)),
                          UNAME(p, p->sem.attach.res),
                          p->sem.attach.value);
            break;

        case la_cross:
        case la_thetajoin:
        {
            /* Add a projection for each operand to ensure
               that all columns are renamed. */
            PFla_op_t *left, *right;
            PFalg_proj_t *projlist1, *projlist2;
            PFalg_col_t ori, unq;
            unsigned int count1, count2;

            left = U(L(p));
            right = U(R(p));

            projlist1 = PFmalloc (left->schema.count *
                                  sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (right->schema.count *
                                  sizeof (PFalg_proj_t));

            count1 = 0;
            count2 = 0;

            for (unsigned int i = 0; i < left->schema.count; i++) {
                unq = left->schema.items[i].name;
                ori = PFprop_ori_name_left (p->prop, unq);
                assert (ori);

                projlist1[count1++] =
                    proj (UNAME(p, ori),
                          unq);
            }

            for (unsigned int i = 0; i < right->schema.count; i++) {
                unq = right->schema.items[i].name;
                ori = PFprop_ori_name_right (p->prop, unq);
                assert (ori);

                projlist2[count2++] =
                    proj (UNAME(p, ori),
                          unq);
            }

            left  = PFla_project_ (left, count1, projlist1);
            right = PFla_project_ (right, count2, projlist2);

            if (p->kind == la_cross)
                res = cross (left, right);
            else { /* p->kind == la_thetajoin */
                PFalg_sel_t *pred = PFmalloc (p->sem.thetajoin.count *
                                              sizeof (PFalg_sel_t));

                for (unsigned int i = 0; i < p->sem.thetajoin.count; i++)
                    pred[i] = PFalg_sel (
                                  p->sem.thetajoin.pred[i].comp,
                                  UNAME (p, p->sem.thetajoin.pred[i].left),
                                  UNAME (p, p->sem.thetajoin.pred[i].right));

                res = thetajoin (left, right,
                                 p->sem.thetajoin.count, pred);
            }
        }   break;


        case la_eqjoin:
        {
            /* Prepare a projection list for both operands
               to ensure the correct names of all columns. */
            PFla_op_t    *left  = U(L(p)),
                         *right = U(R(p));
            PFalg_proj_t *projlist,
                         *projlist1,
                         *projlist2;
            PFalg_col_t   ori,
                          unq,
                          col1  = UNAME (p, p->sem.eqjoin.col1),
                          col2  = UNAME (p, p->sem.eqjoin.col2),
                          new_unq;
            unsigned int  count;

            projlist  = PFmalloc (p->schema.count * sizeof (PFalg_proj_t));
            projlist1 = PFmalloc (left->schema.count * sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (right->schema.count * sizeof (PFalg_proj_t));

            assert (col1 == col2);
            /* Replace col2 by a new column name to ensure that
               the join columns use a different column name. */
            col2 = PFcol_new (col2);
            
            for (unsigned int i = 0; i < left->schema.count; i++) {
                unq = left->schema.items[i].name;
                ori = PFprop_ori_name_left (p->prop, unq);
                assert (ori);
                projlist1[i] = proj (UNAME(p, ori), unq);
            }

            for (unsigned int i = 0; i < right->schema.count; i++) {
                unq     = right->schema.items[i].name;
                ori     = PFprop_ori_name_right (p->prop, unq);
                assert (ori);
                new_unq = UNAME(p, ori);
                if (new_unq == col1)
                    projlist2[i] = proj (col2, unq);
                else
                    projlist2[i] = proj (new_unq, unq);
            }

            res = eqjoin (PFla_project_ (left, left->schema.count, projlist1),
                          PFla_project_ (right, right->schema.count, projlist2),
                          col1,
                          col2);

            /* Make sure that the new column name col2 is not visible.
               (Otherwise some mapping might fail due to the missing
                properties collected for that column.) */
            count = 0;
            for (unsigned int i = 0; i < res->schema.count; i++) {
                unq = res->schema.items[i].name;
                if (unq != col2)
                    projlist[count++] = proj (unq, unq);
            }
            res = PFla_project_ (res, count, projlist);
        }   break;

        case la_semijoin:
            /* Transform semi-join operations into equi-joins
               as these semi-joins might be superfluous as well. */
            if (PFprop_set (p->prop)) {
                PFalg_col_t col1_unq,
                            col2_unq;

                /* we have to make sure that only the columns from
                   the left side are visible */
                PFalg_proj_t *projlist;
                PFla_op_t    *left = U(L(p));

                projlist  = PFmalloc (left->schema.count *
                                      sizeof (PFalg_proj_t));
                for (unsigned int i = 0; i < left->schema.count; i++)
                    projlist[i] = proj (left->schema.items[i].name,
                                        left->schema.items[i].name);
                
                col1_unq = PFprop_unq_name_left (p->prop, p->sem.eqjoin.col1);
                col2_unq = PFprop_unq_name_right (p->prop, p->sem.eqjoin.col2);
                
                res = PFla_project_ (
                          PFla_eqjoin (left, U(R(p)), col1_unq, col2_unq),
                          left->schema.count,
                          projlist);
            } else {
                res = semijoin (U(L(p)), U(R(p)),
                                UNAME (p, p->sem.eqjoin.col1),
                                PFprop_unq_name_right (p->prop,
                                                       p->sem.eqjoin.col2));
            }
            break;

        case la_project:
        {
            PFla_op_t *left;
            PFalg_proj_t *projlist = PFmalloc (p->schema.count *
                                               sizeof (PFalg_proj_t));
            PFalg_col_t unq;
            unsigned int count = 0;

            left = U(L(p));

            /* we ensure that a column split like proj_(item2:item1, item1)
               is avoided in the unique name encoding by keeping the column
               only once (proj_(item23)) */
            for (unsigned int i = 0; i < left->schema.count; i++)
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    if ((unq = UNAME(p, p->sem.proj.items[j].new)) ==
                        left->schema.items[i].name) {
                        projlist[count++] = proj (unq, unq);
                        break;
                    }

            /* if the projection does not prune a column
               we may skip the projection operator */
            if (count == left->schema.count)
                res = left;
            else
                res = PFla_project_ (left, count, projlist);

        }   break;

        case la_select:
            res = select_ (U(L(p)), UNAME(p, p->sem.select.col));
            break;

        case la_pos_select:
        {
            PFord_ordering_t sortby = PFordering ();

            for (unsigned int i = 0;
                 i < PFord_count (p->sem.pos_sel.sortby);
                 i++)
                sortby = PFord_refine (
                             sortby,
                             UNAME(p, PFord_order_col_at (
                                          p->sem.pos_sel.sortby, i)),
                             PFord_order_dir_at (
                                 p->sem.pos_sel.sortby, i));

            res = pos_select (U(L(p)),
                              p->sem.pos_sel.pos,
                              sortby,
                              UNAME(p, p->sem.pos_sel.part));
        }   break;

        case la_disjunion:
        case la_intersect:
        case la_difference:
        {
            /* Add a projection for each operand to ensure
               that all columns are present and with the correct
               name (names matching in pairs again). */
            PFla_op_t *left, *right;
            PFalg_proj_t *projlist1, *projlist2;
            PFalg_col_t ori, unq;

            projlist1 = PFmalloc (p->schema.count *
                                  sizeof (PFalg_proj_t));
            projlist2 = PFmalloc (p->schema.count *
                                  sizeof (PFalg_proj_t));

            for (unsigned int i = 0; i < p->schema.count; i++) {
                ori = p->schema.items[i].name;
                unq = UNAME(p, ori);
                projlist1[i] = proj (unq,
                                     PFprop_unq_name_left (p->prop, ori));
                projlist2[i] = proj (unq,
                                     PFprop_unq_name_right (p->prop, ori));
            }

            left  = PFla_project_ (U(L(p)), p->schema.count, projlist1);
            right = PFla_project_ (U(R(p)), p->schema.count, projlist2);

            if (p->kind == la_disjunion)
                res = disjunion (left, right);
            else if (p->kind == la_intersect)
                res = intersect (left, right);
            else if (p->kind == la_difference)
                res = difference (left, right);
        }   break;

        case la_distinct:
            res = distinct (U(L(p)));
            break;

        case la_fun_1to1:
        {
            PFalg_collist_t *refs = PFalg_collist_copy (p->sem.fun_1to1.refs);
            for (unsigned int i = 0; i < clsize (refs); i++)
                clat (refs, i) = UNAME(p, clat (refs, i));

            res = fun_1to1 (U(L(p)),
                            p->sem.fun_1to1.kind,
                            UNAME(p, p->sem.fun_1to1.res),
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
            PFalg_aggr_t *aggr = PFmalloc (p->sem.aggr.count *
                                           sizeof (PFalg_aggr_t));

            for (unsigned int i = 0; i < p->sem.aggr.count; i++)
                aggr[i] = PFalg_aggr (p->sem.aggr.aggr[i].kind,
                                      UNAME(p, p->sem.aggr.aggr[i].res),
                                      p->sem.aggr.aggr[i].col
                                      ? UNAME(L(p), p->sem.aggr.aggr[i].col)
                                      : col_NULL);

            res = aggr (U(L(p)),
                        p->sem.aggr.part?UNAME(p, p->sem.aggr.part):col_NULL,
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
                             UNAME(p, PFord_order_col_at (
                                          p->sem.sort.sortby, i)),
                             PFord_order_dir_at (
                                 p->sem.sort.sortby, i));

            if (p->kind == la_rownum)
                res = rownum (U(L(p)),
                              UNAME(p, p->sem.sort.res),
                              sortby,
                              UNAME(p, p->sem.sort.part));
            else if (p->kind == la_rowrank)
                res = rowrank (U(L(p)),
                               UNAME(p, p->sem.sort.res),
                               sortby);
            else if (p->kind == la_rank)
                res = rank (U(L(p)),
                            UNAME(p, p->sem.sort.res),
                            sortby);
        }   break;

        case la_rowid:
            res = rowid (U(L(p)), UNAME(p, p->sem.rowid.res));
            break;

        case la_type:
            res = type (U(L(p)),
                        UNAME(p, p->sem.type.res),
                        UNAME(p, p->sem.type.col),
                        p->sem.type.ty);
            break;

        case la_type_assert:
            res = type_assert_pos (U(L(p)),
                                   UNAME(p, p->sem.type.col),
                                   p->sem.type.ty);
            break;

        case la_cast:
            res = cast (U(L(p)),
                        UNAME(p, p->sem.type.res),
                        UNAME(p, p->sem.type.col),
                        p->sem.type.ty);
            break;

        case la_step:
            res = step (U(L(p)), U(R(p)),
                        p->sem.step.spec,
                        p->sem.step.level,
                        UNAME(p, p->sem.step.iter),
                        /* unique name of input column item is
                           stored in the child operator only */
                        UNAME(R(p), p->sem.step.item),
                        UNAME(p, p->sem.step.item_res));
            break;

        case la_step_join:
            res = step_join (U(L(p)), U(R(p)),
                             p->sem.step.spec,
                             p->sem.step.level,
                             UNAME(p, p->sem.step.item),
                             UNAME(p, p->sem.step.item_res));
            break;

        case la_guide_step:
            res = guide_step (U(L(p)), U(R(p)),
                              p->sem.step.spec,
                              p->sem.step.guide_count,
                              p->sem.step.guides,
                              p->sem.step.level,
                              UNAME(p, p->sem.step.iter),
                              /* unique name of input column item is
                                 stored in the child operator only */
                              UNAME(R(p), p->sem.step.item),
                              UNAME(p, p->sem.step.item_res));
            break;

        case la_guide_step_join:
            res = guide_step_join (U(L(p)), U(R(p)),
                                   p->sem.step.spec,
                                   p->sem.step.guide_count,
                                   p->sem.step.guides,
                                   p->sem.step.level,
                                   UNAME(p, p->sem.step.item),
                                   UNAME(p, p->sem.step.item_res));
            break;

        case la_doc_index_join:
            res = doc_index_join (U(L(p)), U(R(p)),
                                  p->sem.doc_join.kind,
                                  UNAME(p, p->sem.doc_join.item),
                                  UNAME(p, p->sem.doc_join.item_res),
                                  UNAME(p, p->sem.doc_join.item_doc),
                                  p->sem.doc_join.ns1,
                                  p->sem.doc_join.loc1,
                                  p->sem.doc_join.ns2,
                                  p->sem.doc_join.loc2);
            break;

        case la_doc_tbl:
            res = doc_tbl (U(L(p)),
                           UNAME(p, p->sem.doc_tbl.res),
                           UNAME(p, p->sem.doc_tbl.col),
                           p->sem.doc_tbl.kind);
            break;

        case la_doc_access:
            res = doc_access (U(L(p)), U(R(p)),
                              UNAME(p, p->sem.doc_access.res),
                              UNAME(p, p->sem.doc_access.col),
                              p->sem.doc_access.doc_col);
            break;

        case la_twig:
            res = twig (U(L(p)),
                        UNAME(p, p->sem.iter_item.iter),
                        UNAME(p, p->sem.iter_item.item));
            break;

        case la_fcns:
            res = fcns (U(L(p)), U(R(p)));
            break;

        case la_docnode:
        res = docnode (U(L(p)), U(R(p)),
                       UNAME (L(p), p->sem.docnode.iter));
        break;

        case la_element:
            res = element (
                      U(L(p)), U(R(p)),
                      UNAME (L(p), p->sem.iter_item.iter),
                      UNAME (L(p), p->sem.iter_item.item));
            break;

        case la_attribute:
            res = attribute (U(L(p)),
                             UNAME(L(p), p->sem.iter_item1_item2.iter),
                             UNAME(L(p), p->sem.iter_item1_item2.item1),
                             UNAME(L(p), p->sem.iter_item1_item2.item2));
            break;

        case la_textnode:
            res = textnode (U(L(p)),
                            UNAME (L(p), p->sem.iter_item.iter),
                            UNAME (L(p), p->sem.iter_item.item));
            break;

        case la_comment:
            res = comment (U(L(p)),
                           UNAME (L(p), p->sem.iter_item.iter),
                           UNAME (L(p), p->sem.iter_item.item));
            break;

        case la_processi:
            res = processi (U(L(p)),
                            UNAME(L(p), p->sem.iter_item1_item2.iter),
                            UNAME(L(p), p->sem.iter_item1_item2.item1),
                            UNAME(L(p), p->sem.iter_item1_item2.item2));
            break;

        case la_content:
            res = content (U(L(p)), U(R(p)),
                           UNAME(R(p), p->sem.iter_pos_item.iter),
                           UNAME(R(p), p->sem.iter_pos_item.pos),
                           UNAME(R(p), p->sem.iter_pos_item.item));
            break;

        case la_merge_adjacent:
            res = merge_adjacent (
                      U(L(p)), U(R(p)),
                      /* unique name of input columns iter_in, pos_in,
                         and item_in are stored in the child operator only */
                      UNAME(R(p), p->sem.merge_adjacent.iter_in),
                      UNAME(R(p), p->sem.merge_adjacent.pos_in),
                      UNAME(R(p), p->sem.merge_adjacent.item_in),
                      UNAME(p, p->sem.merge_adjacent.iter_res),
                      UNAME(p, p->sem.merge_adjacent.pos_res),
                      UNAME(p, p->sem.merge_adjacent.item_res));
            break;

        case la_roots:
            res = roots (U(L(p)));
            break;

        case la_fragment:
            res = fragment (U(L(p)));
            break;

        case la_frag_extract:
            res = frag_extract (U(L(p)), p->sem.col_ref.pos);
            break;

        case la_frag_union:
            res = PFla_frag_union (U(L(p)), U(R(p)));
            break;

        case la_empty_frag:
            res = empty_frag ();
            break;

        case la_error:
            res = PFla_error (U(L(p)), U(R(p)),
                              UNAME(R(p), p->sem.err.col));
            break;

        case la_cache:
            res = PFla_cache (U(L(p)), U(R(p)),
                              p->sem.cache.id,
                              UNAME(R(p), p->sem.cache.pos),
                              UNAME(R(p), p->sem.cache.item));
            break;

        case la_trace:
            res = trace (U(L(p)), U(R(p)));
            break;

        case la_trace_items:
            res = trace_items (
                      U(L(p)),
                      U(R(p)),
                      UNAME(p, p->sem.iter_pos_item.iter),
                      UNAME(p, p->sem.iter_pos_item.pos),
                      UNAME(p, p->sem.iter_pos_item.item));
            break;

        case la_trace_msg:
            res = trace_msg (
                      U(L(p)),
                      U(R(p)),
                      UNAME(p, p->sem.iter_item.iter),
                      UNAME(p, p->sem.iter_item.item));
            break;

        case la_trace_map:
            res = trace_map (
                      U(L(p)),
                      U(R(p)),
                      UNAME(p, p->sem.trace_map.inner),
                      UNAME(p, p->sem.trace_map.outer));
            break;

        case la_nil:
            res = nil ();
            break;

        case la_rec_fix:
            res = rec_fix (U(L(p)), U(R(p)));
            break;

        case la_rec_param:
        {
            PFla_op_t *arg = NULL;

            /* In case the recursion argument is not referenced anymore
               we can safely throw it away. */
            for (unsigned int i = 0; i < PFarray_last (map); i++)
                if (((ori_unq_map *) PFarray_at (map, i))->ori == L(p)) {
                    arg = ((ori_unq_map *) PFarray_at (map, i))->unq;
                    break;
                }

            if (arg)
                res = rec_param (arg, U(R(p)));
            else
                res = U(R(p));
        }   break;

        case la_rec_arg:
        /* The results of the left (seed) and the right (recursion) argument
           have different result schemas. Thus we introduce mapping
           projections that transform the seed and the recursion schema into
           the base schema. */
        {
            PFla_op_t *base = NULL;
            PFalg_proj_t *seed_projlist = PFmalloc (p->schema.count *
                                                    sizeof (PFalg_proj_t));
            PFalg_proj_t *rec_projlist = PFmalloc (p->schema.count *
                                                   sizeof (PFalg_proj_t));

            for (unsigned int i = 0; i < p->schema.count; i++) {
                seed_projlist[i] = proj (UNAME(p, p->schema.items[i].name),
                                         UNAME(L(p), p->schema.items[i].name));
                rec_projlist[i] = proj (UNAME(p, p->schema.items[i].name),
                                        UNAME(R(p), p->schema.items[i].name));
            }

            /* In case the recursion base is not referenced anymore
               we do not need to include the recursion argument. */
            for (unsigned int i = 0; i < PFarray_last (map); i++)
                if (((ori_unq_map *) PFarray_at (map, i))->ori
                    == p->sem.rec_arg.base) {
                    base = ((ori_unq_map *) PFarray_at (map, i))->unq;
                    break;
                }

            if (base)
                res = rec_arg (PFla_project_ (U(L(p)), 
                                              p->schema.count,
                                              seed_projlist),
                               PFla_project_ (U(R(p)),
                                              p->schema.count,
                                              rec_projlist),
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
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = rec_base (schema);
        } break;

        case la_fun_call:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = fun_call (U(L(p)), U(R(p)),
                            schema,
                            p->sem.fun_call.kind,
                            p->sem.fun_call.qname,
                            p->sem.fun_call.ctx,
                            UNAME(L(p), p->sem.fun_call.iter),
                            p->sem.fun_call.occ_ind);
        }   break;

        case la_fun_param:
        {
            PFalg_schema_t schema;
            schema.count = p->schema.count;
            schema.items  = PFmalloc (schema.count *
                                      sizeof (PFalg_schema_t));

            for (unsigned int i = 0; i < p->schema.count; i++)
                schema.items[i] =
                    (struct PFalg_schm_item_t)
                        { .name = UNAME(p, p->schema.items[i].name),
                          .type = p->schema.items[i].type };

            res = fun_param (U(L(p)), U(R(p)), schema);
        }   break;

        case la_fun_frag_param:
            res = fun_frag_param (U(L(p)), U(R(p)), p->sem.col_ref.pos);
            break;

        case la_proxy:
        case la_proxy_base:
            PFoops (OOPS_FATAL,
                    "PROXY EXPANSION MISSING");
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

        case la_string_join:
            res = fn_string_join (
                      U(L(p)), U(R(p)),
                      /* unique name of input columns iter, pos, item,
                         iter_seq, and item_sep are stored in the child
                         operators only */
                      UNAME(L(p), p->sem.string_join.iter),
                      UNAME(L(p), p->sem.string_join.pos),
                      UNAME(L(p), p->sem.string_join.item),
                      UNAME(R(p), p->sem.string_join.iter_sep),
                      UNAME(R(p), p->sem.string_join.item_sep),
                      UNAME(p, p->sem.string_join.iter_res),
                      UNAME(p, p->sem.string_join.item_res));
            break;

        case la_dummy:
            res = U(L(p));
            break;
    }

    assert(res);

    /* Add pair (p, res) to the environment map
       to allow lookup of already generated subplans. */
    *(ori_unq_map *) PFarray_add (map) =
        (ori_unq_map) { .ori = p, .unq = res};
}

/**
 * Invoke name mapping.
 */
PFla_op_t *
PFmap_unq_names (PFla_op_t *root)
{
    PFarray_t *map = PFarray (sizeof (ori_unq_map), 300);

    /* reset the column name counter */
    PFalg_init ();

    /* infer unique names */
    PFprop_infer_unq_names (root);
    /* infer the set property */
    PFprop_infer_set (root);

    /* generate equivalent algebra DAG */
    map_unq_names (root, map);

    /* return algebra DAG with unique names */
    return U(root);
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
