/**
 * @file
 *
 * Inference of composite key property of logical algebra expressions.
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

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"
#include "qname.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps left */
#define LL(p) (L(L(p)))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))

/**
 * worker for PFprop_ckey;
 * Test if @a attlist is in the list of key columns in array @a keys
 * and return the size of the key.
 */
static unsigned int
key_worker (PFarray_t *keys, PFalg_att_t attlist)
{
    if (!keys) return 0;

    for (unsigned int i = 0; i < PFarray_last (keys); i++)
        if ((attlist & *(PFalg_att_t *) PFarray_at (keys, i)) ==
            *(PFalg_att_t *) PFarray_at (keys, i)) {
            PFalg_att_t cols = *(PFalg_att_t *) PFarray_at (keys, i);
            unsigned int counter = 0;

            while (cols) {
                counter += cols & 1;
                cols >>= 1;
            }
            return counter;
        }

    return 0;
}

/**
 * Test if a schema @a schema contains a composite key and return
 * the size of the composite key (0 if no matching key was found).
 */
unsigned int
PFprop_ckey (const PFprop_t *prop, PFalg_schema_t schema)
{
    PFalg_att_t cols = 0;
    for (unsigned int i = 0; i < schema.count; i++)
        cols |= schema.items[i].name;

    return key_worker (prop->ckeys, cols);
}

/*
 * count number of composite keys (in a property container @a prop)
 */
unsigned int
PFprop_ckeys_count (const PFprop_t *prop)
{
    if (!prop->ckeys)
        return 0;
    else
        return PFarray_last (prop->ckeys);
}

/**
 * Return attributes that build a composite key (at position @a i)
 * as an PFalg_attlist_t.
 */
PFalg_attlist_t
PFprop_ckey_at (const PFprop_t *prop, unsigned int i)
{
    PFalg_attlist_t new_list;
    PFalg_att_t cols = *(PFalg_att_t *) PFarray_at (prop->ckeys, i);
    PFalg_att_t ori_cols = cols;
    unsigned int counter = 0, bit_shift = 1;

    /* collect all columns */
    while (cols) {
        counter += cols & 1;
        cols >>= 1;
    }

    new_list.count = counter;
    new_list.atts = PFmalloc (new_list.count * sizeof (*(new_list.atts)));

    counter = 0;
    cols = ori_cols;

    /* unfold cols into a list of attributes */
    while (cols) {
        new_list.atts[counter] = ori_cols & bit_shift;
        bit_shift <<= 1;

        counter += cols & 1;
        cols >>= 1;
    }
    return new_list;
}

/**
 * Extends the composite key list @a with a single
 * attribute list @a cols if @a cols is not in the list.
 */
static void
union_ (PFarray_t *a, PFalg_att_t cols)
{
    assert (a);

    if (!key_worker (a, cols))
        *(PFalg_att_t *) PFarray_add (a) = cols;
}

/**
 * Extends composite key list @a with all compound
 * keys of the composite key list @a b that are not
 * in @a a/
 */
static void
union_list (PFarray_t *a, PFarray_t *b)
{
    PFalg_att_t cur;

    assert (a && b);

    for (unsigned int i = 0; i < PFarray_last (b); i++) {
        cur = *(PFalg_att_t *) PFarray_at (b, i);
        if (!key_worker (a, cur))
            *(PFalg_att_t *) PFarray_add (a) = cur;
    }
}

static void
copy (PFarray_t *base, PFarray_t *content)
{
    for (unsigned int i = 0; i < PFarray_last (content); i++)
        *(PFalg_att_t *) PFarray_add (base) =
            *(PFalg_att_t *) PFarray_at (content, i);
}

/**
 * Infer composite key property of a given node @a n;
 * worker for prop_infer().
 */
static void
infer_ckey (PFla_op_t *n)
{
    switch (n->kind) {
        case la_serialize_seq:
            /* just copy composite keys from right child */
            copy (n->prop->ckeys, R(n)->prop->ckeys);
            break;

        case la_serialize_rel:
            /* just copy composite keys from the child */
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            break;

        case la_lit_tbl:
            /* all columns are key */
            if (n->sem.lit_tbl.count == 1)
                for (unsigned int i = 0; i < n->schema.count; i++)
                    union_ (n->prop->ckeys, n->schema.items[i].name);
            else
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    unsigned int j = 0;
                    unsigned int k = 0;

                    /* skip all columns where the comparison
                       might be unstable in respect to
                       differing implementations */
                    if (n->schema.items[i].type != aat_nat &&
                        n->schema.items[i].type != aat_int)
                        continue;

                    /* compare each tuple with all others */
                    while (j < n->sem.lit_tbl.count) {
                        if (!PFalg_atom_cmp (n->sem.lit_tbl.tuples[j].atoms[i],
                                             n->sem.lit_tbl.tuples[k].atoms[i]))
                            break;
                        else if (k == n->sem.lit_tbl.count)
                            { k = 0; j++; }
                        else
                            k++;
                    }
                    /* all values are unique thus also key */
                    if (j == n->sem.lit_tbl.count)
                        union_ (n->prop->ckeys, n->schema.items[i].name);

                }
            break;

        case la_empty_tbl:
        case la_disjunion: /* disjoint domains could help here */
        case la_fcns:
        case la_docnode:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_content:
        case la_merge_adjacent:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            /* no composite keys */
            break;

        case la_attach:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.attach.res);
            break;

        case la_cross:
        {
            PFalg_att_t l_list, r_list;
            unsigned int i, j;
            /* combine all keys of the left argument
               with all keys of the right argument */
            for (i = 0; i < PFarray_last (L(n)->prop->ckeys); i++)
                for (j = 0; j < PFarray_last (R(n)->prop->ckeys); j++) {
                    l_list = *(PFalg_att_t *) PFarray_at (L(n)->prop->ckeys, i);
                    r_list = *(PFalg_att_t *) PFarray_at (R(n)->prop->ckeys, j);
                    union_ (n->prop->ckeys, l_list | r_list);
                }

            for (i = 0; i < PFarray_last (L(n)->prop->keys); i++)
                for (j = 0; j < PFarray_last (R(n)->prop->keys); j++) {
                    l_list = *(PFalg_att_t *) PFarray_at (L(n)->prop->keys, i);
                    r_list = *(PFalg_att_t *) PFarray_at (R(n)->prop->keys, j);
                    union_ (n->prop->ckeys, l_list | r_list);
                }
        }   break;

        case la_eqjoin:
        case la_eqjoin_unq:
        {
            PFalg_att_t l_list, r_list;
            unsigned int i, j;

            /* only a key-join retains all key properties */
            if (PFprop_key (L(n)->prop, n->sem.eqjoin.att1) &&
                PFprop_key (R(n)->prop, n->sem.eqjoin.att2)) {
                PFarray_t *left, *right;

                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, L(n)->prop->ckeys);
                union_list (n->prop->ckeys, R(n)->prop->ckeys);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (x.keys - x.join_att + y.keys) */
                left  = L(n)->prop->ckeys;
                right = R(n)->prop->ckeys;
                for (i = 0; i < PFarray_last (left); i++)
                    for (j = 0; j < PFarray_last (right); j++) {
                        l_list = *(PFalg_att_t *) PFarray_at (left, i);
                        r_list = *(PFalg_att_t *) PFarray_at (right, j);
                        if (l_list & n->sem.eqjoin.att1)
                            union_ (n->prop->ckeys,
                                    (l_list & (~n->sem.eqjoin.att1)) | r_list);
                        if (r_list & n->sem.eqjoin.att2)
                            union_ (n->prop->ckeys,
                                    (r_list & (~n->sem.eqjoin.att2)) | l_list);
                    }
            }
            else if (PFprop_key (L(n)->prop, n->sem.eqjoin.att1)) {
                PFarray_t *left, *right;

                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, R(n)->prop->ckeys);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (r.keys - r.join_att + l.keys) */
                left  = L(n)->prop->ckeys;
                right = R(n)->prop->ckeys;
                for (i = 0; i < PFarray_last (left); i++)
                    for (j = 0; j < PFarray_last (right); j++) {
                        l_list = *(PFalg_att_t *) PFarray_at (left, i);
                        r_list = *(PFalg_att_t *) PFarray_at (right, j);
                        if (r_list & n->sem.eqjoin.att2)
                            union_ (n->prop->ckeys,
                                    (r_list & (~n->sem.eqjoin.att2)) | l_list);
                    }
            } else if (PFprop_key (R(n)->prop, n->sem.eqjoin.att2)) {
                PFarray_t *left, *right;

                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, L(n)->prop->ckeys);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (l.keys - l.join_att + r.keys) */
                left  = L(n)->prop->ckeys;
                right = R(n)->prop->ckeys;
                for (i = 0; i < PFarray_last (left); i++)
                    for (j = 0; j < PFarray_last (right); j++) {
                        l_list = *(PFalg_att_t *) PFarray_at (left, i);
                        r_list = *(PFalg_att_t *) PFarray_at (right, j);
                        if (l_list & n->sem.eqjoin.att1)
                            union_ (n->prop->ckeys,
                                    (l_list & (~n->sem.eqjoin.att1)) | r_list);
                    }
            } else {
                PFarray_t *left, *right;

                /* combine all keys of the left argument
                   with all keys of the right argument */
                left  = L(n)->prop->ckeys;
                right = R(n)->prop->ckeys;
                for (i = 0; i < PFarray_last (L(n)->prop->ckeys); i++)
                    for (j = 0; j < PFarray_last (R(n)->prop->ckeys); j++) {
                        l_list = *(PFalg_att_t *) PFarray_at (left, i);
                        r_list = *(PFalg_att_t *) PFarray_at (right, j);
                        union_ (n->prop->ckeys, l_list | r_list);
                    }
            }

            for (i = 0; i < PFarray_last (L(n)->prop->keys); i++)
                for (j = 0; j < PFarray_last (R(n)->prop->keys); j++) {
                    l_list = *(PFalg_att_t *) PFarray_at (L(n)->prop->keys, i);
                    r_list = *(PFalg_att_t *) PFarray_at (R(n)->prop->keys, j);
                    union_ (n->prop->ckeys, l_list | r_list);
                }
        }   break;

        case la_semijoin:
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            break;

        case la_thetajoin:
        {
            bool key_left = false,
                 key_right = false;

            for (unsigned int i = 0; i < n->sem.thetajoin.count; i++)
                if (n->sem.thetajoin.pred[i].comp == alg_comp_eq) {
                    key_left = key_left ||
                               PFprop_key (L(n)->prop,
                                           n->sem.thetajoin.pred[i].left);
                    key_right = key_right ||
                                PFprop_key (L(n)->prop,
                                            n->sem.thetajoin.pred[i].right);
                }

            /* only a key-join retains all key properties */
            if (key_left && key_right) {
                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, L(n)->prop->ckeys);
                union_list (n->prop->ckeys, R(n)->prop->ckeys);
            }
            else if (key_left) {
                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, R(n)->prop->ckeys);
            } else if (key_right) {
                /* copy composite keys from the non-key join argument */
                copy (n->prop->ckeys, L(n)->prop->ckeys);
            }

            PFalg_att_t l_list, r_list;
            unsigned int i, j;
            /* combine all keys of the left argument
               with all keys of the right argument */
            for (i = 0; i < PFarray_last (L(n)->prop->ckeys); i++)
                for (j = 0; j < PFarray_last (R(n)->prop->ckeys); j++) {
                    l_list = *(PFalg_att_t *) PFarray_at (L(n)->prop->ckeys, i);
                    r_list = *(PFalg_att_t *) PFarray_at (R(n)->prop->ckeys, j);
                    union_ (n->prop->ckeys, l_list | r_list);
                }

            for (i = 0; i < PFarray_last (L(n)->prop->keys); i++)
                for (j = 0; j < PFarray_last (R(n)->prop->keys); j++) {
                    l_list = *(PFalg_att_t *) PFarray_at (L(n)->prop->keys, i);
                    r_list = *(PFalg_att_t *) PFarray_at (R(n)->prop->keys, j);
                    union_ (n->prop->ckeys, l_list | r_list);
                }
        }   break;

        case la_project:
        {
            PFarray_t *ckeys = L(n)->prop->ckeys;
            PFalg_att_t ckey;
            PFalg_att_t cols = 0;
            bool rename = false;
            unsigned int i, j;

            /* Collect all 'old' projection column names. */
            for (i = 0; i < n->sem.proj.count; i++) {
                if (n->sem.proj.items[i].new != n->sem.proj.items[i].old)
                    rename = true;
                cols |= n->sem.proj.items[i].old;
            }
            /* Copy all composite keys that are still valid
               after projecting away some columns. */
            for (i = 0; i < PFarray_last (ckeys); i++) {
                ckey = *(PFalg_att_t *) PFarray_at (ckeys, i);
                if ((cols & ckey) == ckey)
                    union_ (n->prop->ckeys, ckey);
            }

            /* If the projection did not rename any column we are done ... */
            if (!rename) break;

            /* ... Otherwise we need to fix the column names. */
            ckeys = n->prop->ckeys;
            for (i = 0; i < PFarray_last (ckeys); i++) {
                ckey = *(PFalg_att_t *) PFarray_at (ckeys, i);
                cols = 0;

                for (j = 0; j < n->sem.proj.count; j++)
                    if (ckey & n->sem.proj.items[j].old)
                        cols |= n->sem.proj.items[j].new;
                *(PFalg_att_t *) PFarray_at (ckeys, i) = cols;
            }
        }   break;

        case la_select:
        case la_pos_select:
        case la_difference:
        case la_type_assert:
        case la_roots:
        case la_cond_err:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            break;

        case la_intersect:
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            union_list (n->prop->ckeys, R(n)->prop->ckeys);
            break;

        case la_distinct:
            if (PFprop_ckey (L(n)->prop, n->schema))
                /* composite key columns stay the same */
                copy (n->prop->ckeys, L(n)->prop->ckeys);
            else {
                /* we have a new composite key */
                PFalg_att_t cols = 0;
                for (unsigned int i = 0; i < n->schema.count; i++)
                    cols |= n->schema.items[i].name;
                union_ (n->prop->ckeys, cols);
            }
            break;

        case la_fun_1to1:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.fun_1to1.res);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.binary.res);
            break;

        case la_bool_not:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.unary.res);
            break;

        case la_to:
            if (n->sem.to.part)
                union_ (n->prop->ckeys, n->sem.to.part | n->sem.to.res);
            union_ (n->prop->ckeys, n->sem.to.res);
            break;

        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (n->sem.aggr.part)
                union_ (n->prop->ckeys, n->sem.aggr.part);
            else
                union_ (n->prop->ckeys, n->sem.aggr.res);
            break;

        case la_rownum:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1 || !n->sem.rownum.part)
                union_ (n->prop->ckeys, n->sem.rownum.res);
            break;

        case la_rank:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            union_ (n->prop->ckeys, n->sem.rank.res);
            break;

        case la_number:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            union_ (n->prop->ckeys, n->sem.number.res);
            break;

        case la_type:
        case la_cast:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, L(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.type.res);
            break;

        case la_step:
        case la_guide_step:
            if (n->sem.step.axis == alg_chld &&
                PFprop_key (R(n)->prop, n->sem.step.iter) &&
                PFprop_key (R(n)->prop, n->sem.step.item))
                union_ (n->prop->ckeys, n->sem.step.item_res);

            if (PFprop_const (n->prop, n->sem.step.iter))
                union_ (n->prop->ckeys, n->sem.step.item_res);
            else
                union_ (n->prop->ckeys,
                        n->sem.step.iter | n->sem.step.item_res);
            break;

        case la_step_join:
        case la_guide_step_join:
        {
            PFarray_t *right = R(n)->prop->ckeys;
            PFalg_att_t ckey;
            for (unsigned int i = 0; i < PFarray_last (right); i++) {
                ckey = *(PFalg_att_t *) PFarray_at (right, i);
                union_ (n->prop->ckeys,
                        ckey | n->sem.step.item_res);
            }

            if (PFprop_card (R(n)->prop) == 1)
                union_ (n->prop->ckeys, n->sem.step.item_res);

        }   break;

        case la_doc_index_join:
        {
            PFarray_t *right = R(n)->prop->ckeys;
            PFalg_att_t ckey;
            for (unsigned int i = 0; i < PFarray_last (right); i++) {
                ckey = *(PFalg_att_t *) PFarray_at (right, i);
                union_ (n->prop->ckeys,
                        ckey | n->sem.doc_join.item_res);
            }

            if (PFprop_card (R(n)->prop) == 1)
                union_ (n->prop->ckeys, n->sem.doc_join.item_res);

        }   break;

        case la_doc_tbl:
            if (PFprop_card (n->prop) == 1) {
                /* If the cardinality is equal to one
                   the result is key itself. */
                union_ (n->prop->ckeys, n->sem.doc_tbl.iter);
                union_ (n->prop->ckeys, n->sem.doc_tbl.item_res);
            } else
                /* Otherwise at least column iter is key. */
                union_ (n->prop->ckeys, n->sem.doc_tbl.iter);
            break;

        case la_doc_access:
            /* composite key columns are propagated */
            copy (n->prop->ckeys, R(n)->prop->ckeys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->ckeys, n->sem.doc_access.res);
            break;

        case la_twig:
            /* a constructor builds unique new nodes */
            union_ (n->prop->ckeys, n->sem.iter_item.item);
            switch (L(n)->kind) {
                case la_docnode:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.docnode.iter))
                        union_ (n->prop->ckeys, n->sem.iter_item.iter);
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.iter_item.iter))
                        union_ (n->prop->ckeys, n->sem.iter_item.iter);
                    break;

                case la_attribute:
                case la_processi:
                    if (PFprop_key (LL(n)->prop,
                                    L(n)->sem.iter_item1_item2.iter))
                        union_ (n->prop->ckeys, n->sem.iter_item.iter);
                    break;

                case la_content:
                    if (PFprop_key (LR(n)->prop,
                                    L(n)->sem.iter_pos_item.iter))
                        union_ (n->prop->ckeys, n->sem.iter_item.iter);
                    break;

                default:
                    break;
            }
            break;

        case la_nil:
        case la_trace:
        case la_trace_msg:
        case la_trace_map:
            /* delete keys to avoid rewrites */
            break;

        case la_rec_fix:
            /* get the keys of the overall result */
            copy (n->prop->ckeys, R(n)->prop->ckeys);
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            copy (n->prop->ckeys, R(n)->prop->ckeys);
            break;

        case la_rec_base:
            /* infer no properties of the seed */
            break;

        case la_proxy:
        case la_proxy_base:
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            break;

        case la_string_join:
            /* Every iteration yields exactly one
               tuple. Iterations iter are thus key. */
            union_ (n->prop->ckeys, n->sem.string_join.iter_res);
            break;

        case la_cross_mvd:
            PFoops (OOPS_FATAL,
                    "clone column aware cross product operator is "
                    "only allowed inside mvd optimization!");

        case la_dummy:
            copy (n->prop->ckeys, L(n)->prop->ckeys);
            break;
    }
}

/* worker for PFprop_infer_composite_key */
static void
prop_infer (PFla_op_t *n)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i]);

    n->bit_dag = true;

    /* reset composite key information
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->ckeys)
        PFarray_last (n->prop->ckeys) = 0;
    else
        n->prop->ckeys   = PFarray (sizeof (PFalg_att_t));

    /* infer information on composite key columns */
    infer_ckey (n);
}

/**
 * Infer composite key property for a DAG rooted in root
 */
void
PFprop_infer_composite_key (PFla_op_t *root) {
    /* infer the 'simple' key property */
    PFprop_infer_key (root);

    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
