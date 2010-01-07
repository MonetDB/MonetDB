/**
 * @file
 *
 * Inference of key property of logical algebra expressions.
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

#include "properties.h"
#include "alg_dag.h"
#include "oops.h"
#include "mem.h"
#include "qname.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/**
 * worker for PFprop_key;
 * Test if @a col is in the list of key columns in array @a keys
 */
static bool
key_worker (PFalg_collist_t *keys, PFalg_col_t col)
{
    if (!keys) return false;

    for (unsigned int i = 0; i < clsize (keys); i++)
        if (col == clat (keys, i))
            return true;

    return false;
}

/**
 * Test if @a col is in the list of key columns in container @a prop
 */
bool
PFprop_key (const PFprop_t *prop, PFalg_col_t col)
{
    if (!prop) return false;

    return key_worker (prop->keys, col);
}

/**
 * Test if @a col is in the list of key columns of the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_key_left (const PFprop_t *prop, PFalg_col_t col)
{
    if (!prop) return false;

    return key_worker (prop->l_keys, col);
}

/**
 * Test if @a col is in the list of key columns of the right child
 * (information is stored in property container @a prop)
 */
bool
PFprop_key_right (const PFprop_t *prop, PFalg_col_t col)
{
    if (!prop) return false;

    return key_worker (prop->r_keys, col);
}

/*
 * count number of key columns
 */
unsigned int
PFprop_keys_count (const PFprop_t *prop)
{
    if (!prop->keys)
        return 0;
    else
        return clsize (prop->keys);
}

/**
 * Return key columns in an collist.
 */
PFalg_collist_t *
PFprop_keys_to_collist (const PFprop_t *prop)
{
    if (!prop->keys)
        return PFalg_collist (0);
    else
        return PFalg_collist_copy (prop->keys);
}

/**
 * Extends key list @a with column @a b
 * if @a b is not in the list.
 */
static void
union_ (PFalg_collist_t *a, PFalg_col_t b)
{
    assert (a);

    if (!key_worker (a, b))
        cladd (a) = b;
}

/**
 * Extends key list @a with all the items
 * of the key list @a b that are not in @a a/
 */
static void
union_list (PFalg_collist_t *a, PFalg_collist_t *b)
{
    PFalg_col_t cur;

    assert (a && b);

    for (unsigned int i = 0; i < clsize (b); i++) {
        cur = clat (b, i);
        if (!key_worker (a, cur))
            cladd (a) = cur;
    }
}

static void
copy (PFalg_collist_t *base, PFalg_collist_t *content)
{
    for (unsigned int i = 0; i < clsize (content); i++)
        cladd (base) = clat (content, i);
}

/**
 * For a list of guides find the biggest occurrence indicator.
 */
static unsigned int
find_guide_max (unsigned int count, PFguide_tree_t **guides)
{
    unsigned int max;

    assert (count);
    max = guides[0]->max;
    for (unsigned int i = 1; i < count; i++)
        max = max > guides[i]->max ? max : guides[i]->max;

    return max;
}

/**
 * For a single guide recursively find the biggest occurrence indicator.
 */
static unsigned int
find_guide_max_rec_worker (PFguide_tree_t *guide,
                           unsigned int ctx_count,
                           PFguide_tree_t **ctx_guides)
{
    unsigned int max;
    PFguide_tree_t *parent = guide->parent;

    assert (parent);
    for (unsigned int i = 0; i < ctx_count; i++)
        if (ctx_guides[i] == parent)
            return guide->max;

    max = find_guide_max_rec_worker (parent, ctx_count, ctx_guides);
    return max > guide->max ? max : guide->max;
}

/**
 * For a list of guides recursively find the biggest occurrence indicator.
 */
static unsigned int
find_guide_max_rec (unsigned int count, PFguide_tree_t **guides,
                    unsigned int ctx_count, PFguide_tree_t **ctx_guides)
{
    unsigned int max, overall_max;

    assert (count);
    overall_max = find_guide_max_rec_worker (guides[0], ctx_count, ctx_guides);
    for (unsigned int i = 1; i < count; i++) {
        max = find_guide_max_rec_worker (guides[i], ctx_count, ctx_guides);
        overall_max = overall_max > max ? overall_max : max;
    }

    return overall_max;
}

/**
 * Infer key property of a given node @a n; worker for prop_infer().
 */
static void
infer_key (PFla_op_t *n, bool with_guide_info, bool with_fd_info)
{
    /* copy key properties of children into current node */
    if (L(n)) copy (n->prop->l_keys, L(n)->prop->keys);
    if (R(n)) copy (n->prop->r_keys, R(n)->prop->keys);

    switch (n->kind) {
        case la_serialize_seq:
        case la_serialize_rel:
            /* just copy keys from right child */
            copy (n->prop->keys, R(n)->prop->keys);
            break;

        case la_side_effects:
        case la_empty_tbl:
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
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            /* no keys */
            break;

        case la_lit_tbl:
            /* all columns are key */
            if (n->sem.lit_tbl.count == 1)
                for (unsigned int i = 0; i < n->schema.count; i++)
                    union_ (n->prop->keys, n->schema.items[i].name);
            else
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    unsigned int j = 0;
                    unsigned int k = j+1;

                    /* skip all columns where the comparison
                       might be unstable in respect to
                       differing implementations */
                    if (n->schema.items[i].type != aat_nat &&
                        n->schema.items[i].type != aat_int)
                        continue;

                    /* compare each tuple with all others */
                    while (j < n->sem.lit_tbl.count) {
                        if (k == n->sem.lit_tbl.count)
                            { j++; k = j+1; }
                        else if (!PFalg_atom_cmp (n->sem.lit_tbl.tuples[j].atoms[i],
                                                  n->sem.lit_tbl.tuples[k].atoms[i]))
                            break;
                        else
                            k++;
                    }
                    /* all values are unique thus also key */
                    if (j == n->sem.lit_tbl.count)
                        union_ (n->prop->keys, n->schema.items[i].name);

                }
            break;

        case la_ref_tbl:
            for (unsigned int key = 0;
                     key < PFarray_last (n->sem.ref_tbl.keys);
                     key++)
            {
                PFarray_t* keyPositions = *((PFarray_t**) PFarray_at (n->sem.ref_tbl.keys, key));
                unsigned int keyCount = PFarray_last (keyPositions);

                /* Make sure we don't process a component key here. */
                if(keyCount == 1 ) {
                    int keyPos =  *(int*) PFarray_at (keyPositions, 0);
    
                    assert(keyPos >= 0);
    
                    PFalg_schm_item_t schemaItem = n->schema.items[keyPos];
                    PFalg_col_t key = schemaItem.name;
    
                    cladd (n->prop->keys) = key;
                }
            }    
            break;

        case la_disjunion:
            /*
             * If
             *  (a) a column a is key in both arguments and
             *  (b) the domains of a in the two arguments are disjoint
             * a will be key in the result as well.
             *
             * (We need domain information for this, though.)
             */
            if (L(n)->prop->domains && R(n)->prop->domains
                && n->prop->disjdoms) {

                for (unsigned int i = 0; i < clsize (L(n)->prop->keys); i++) {

                    PFalg_col_t key_col = clat (L(n)->prop->keys, i);

                    if (key_worker (R(n)->prop->keys, key_col)
                        && PFprop_disjdom (n->prop,
                                           PFprop_dom (L(n)->prop, key_col),
                                           PFprop_dom (R(n)->prop, key_col)))
                        union_ (n->prop->keys, key_col);
                }
            }
            break;

        case la_attach:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.attach.res);
            break;

        case la_cross:
            /* keys of one side can be retained
               if the other side has a cardinality of 1.
               Otherwise no information about keys
               is available */
            if (PFprop_card (L(n)->prop) == 1)
                copy (n->prop->keys, R(n)->prop->keys);
            else if (PFprop_card (R(n)->prop) == 1)
                copy (n->prop->keys, L(n)->prop->keys);
            break;

        case la_eqjoin:
            /* only a key-join retains all key properties */
            if (PFprop_key (L(n)->prop, n->sem.eqjoin.col1) &&
                PFprop_key (R(n)->prop, n->sem.eqjoin.col2)) {
                copy (n->prop->keys, L(n)->prop->keys);
                union_list (n->prop->keys, R(n)->prop->keys);
            }
            else if (PFprop_key (L(n)->prop, n->sem.eqjoin.col1))
                copy (n->prop->keys, R(n)->prop->keys);
            else if (PFprop_key (R(n)->prop, n->sem.eqjoin.col2))
                copy (n->prop->keys, L(n)->prop->keys);
            break;

        case la_internal_op:
            /* interpret this operator as internal join */
            if (n->sem.eqjoin_opt.kind == la_eqjoin) {
                /* do the same as for normal joins and
                   correctly update the columns names */
#define proj_at(l,i) (*(PFalg_proj_t *) PFarray_at ((l),(i)))
                PFarray_t  *lproj = n->sem.eqjoin_opt.lproj,
                           *rproj = n->sem.eqjoin_opt.rproj;
                PFalg_col_t col1  = proj_at(lproj, 0).old,
                            col2  = proj_at(rproj, 0).old,
                            res   = proj_at(lproj, 0).new;
                
                /* only a key-join retains all key properties */
                if (PFprop_key (L(n)->prop, col1) &&
                    PFprop_key (R(n)->prop, col2)) {
                    union_ (n->prop->keys, res);
                    for (unsigned int i = 1; i < PFarray_last (lproj); i++)
                        if (PFprop_key (L(n)->prop, proj_at(lproj, i).old))
                            union_ (n->prop->keys, proj_at(lproj, i).new);
                    for (unsigned int i = 1; i < PFarray_last (rproj); i++)
                        if (PFprop_key (R(n)->prop, proj_at(rproj, i).old))
                            union_ (n->prop->keys, proj_at(rproj, i).new);
                }
                else if (PFprop_key (L(n)->prop, col1)) {
                    for (unsigned int i = 1; i < PFarray_last (rproj); i++)
                        if (PFprop_key (R(n)->prop, proj_at(rproj, i).old))
                            union_ (n->prop->keys, proj_at(rproj, i).new);
                }
                else if (PFprop_key (R(n)->prop, col2)) {
                    for (unsigned int i = 1; i < PFarray_last (lproj); i++)
                        if (PFprop_key (L(n)->prop, proj_at(lproj, i).old))
                            union_ (n->prop->keys, proj_at(lproj, i).new);
                }
            }
            else
                PFoops (OOPS_FATAL,
                        "internal optimization operator is not allowed here");
            break;
            
        case la_semijoin:
            copy (n->prop->keys, L(n)->prop->keys);
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
                                PFprop_key (R(n)->prop,
                                            n->sem.thetajoin.pred[i].right);
                }

            /* only a key-join retains all key properties */
            if (key_left && key_right) {
                copy (n->prop->keys, L(n)->prop->keys);
                union_list (n->prop->keys, R(n)->prop->keys);
            }
            else if (key_left)
                copy (n->prop->keys, R(n)->prop->keys);
            else if (key_right)
                copy (n->prop->keys, L(n)->prop->keys);
        }   break;

        case la_project:
            /* rename keys columns from old to new */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (key_worker (L(n)->prop->keys, n->sem.proj.items[i].old))
                    union_ (n->prop->keys, n->sem.proj.items[i].new);
            break;

        case la_select:
        case la_difference:
        case la_type_assert:
        case la_roots:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);
            break;

        case la_pos_select:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* propagate the partition column as there can
               be only one matching position for every partition */
            if (n->sem.pos_sel.part)
                union_ (n->prop->keys, n->sem.pos_sel.part);
            break;

        case la_intersect:
        {
            bool key_left = false,
                 key_right = false;

            for (unsigned int i = 0; i < n->schema.count; i++) {
                key_left = key_left ||
                           key_worker (L(n)->prop->keys,
                                       n->schema.items[i].name);
                key_right = key_right ||
                            key_worker (R(n)->prop->keys,
                                        n->schema.items[i].name);
            }

            /* only a key-join retains all key properties */
            if (key_left && key_right) {
                copy (n->prop->keys, L(n)->prop->keys);
                union_list (n->prop->keys, R(n)->prop->keys);
            }
            else if (key_left)
                copy (n->prop->keys, R(n)->prop->keys);
            else if (key_right)
                copy (n->prop->keys, L(n)->prop->keys);
        }   break;

        case la_distinct:
            if (n->schema.count == 1)
                /* if distinct works on a single column,
                   this column is key afterwards. */
                union_ (n->prop->keys, n->schema.items[0].name);
            else
                /* key columns stay the same */
                copy (n->prop->keys, L(n)->prop->keys);

            if (with_fd_info)
                /* check if all columns can be described by a single
                   column */
                for (unsigned int i = 0; i < n->schema.count; i++) {
                    bool match = true;
                    for (unsigned int j = 0; j < n->schema.count; j++)
                        if (i != j) {
                            match &= PFprop_fd (n->prop,
                                                n->schema.items[i].name,
                                                n->schema.items[j].name);
                        }
                    if (match)
                        union_ (n->prop->keys, n->schema.items[i].name);
                }
            break;

        case la_fun_1to1:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.fun_1to1.res);
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.binary.res);
            break;

        case la_bool_not:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.unary.res);
            break;

        case la_to:
            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (L(n)->prop) == 1)
                union_ (n->prop->keys, n->sem.binary.res);
            break;

        case la_aggr:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (n->sem.aggr.part)
                union_ (n->prop->keys, n->sem.aggr.part);
            else
                for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                    union_ (n->prop->keys, n->sem.aggr.aggr[i].res);
            break;

        case la_rownum:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1 || !n->sem.sort.part)
                union_ (n->prop->keys, n->sem.sort.res);
            break;

        case la_rowrank:
        case la_rank:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* propagate the result column as key
               if one of the sort criteria is already a key */
            for (unsigned int i = 0; i < PFord_count (n->sem.sort.sortby); i++)
                if (key_worker (L(n)->prop->keys,
                                PFord_order_col_at (n->sem.sort.sortby, i))) {
                    union_ (n->prop->keys, n->sem.sort.res);
                    break;
                }
            break;

        case la_rowid:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);
            union_ (n->prop->keys, n->sem.rowid.res);
            break;

        case la_type:
        case la_cast:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.type.res);
            break;

        case la_guide_step:
            /* copy the iter key if it exists and the cardinality
               does not change */
            if (PFprop_key (R(n)->prop, n->sem.step.iter) &&
                (n->sem.step.spec.axis == alg_chld ||
                 n->sem.step.spec.axis == alg_attr ||
                 n->sem.step.spec.axis == alg_self) &&
                find_guide_max (n->sem.step.guide_count,
                                n->sem.step.guides) <= 1)
                union_ (n->prop->keys, n->sem.step.iter);
        case la_step:
            /* if the input is key the output will be as well
               (non-recursive downward step) */
            if ((n->sem.step.spec.axis == alg_chld ||
                 n->sem.step.spec.axis == alg_attr ||
                 n->sem.step.spec.axis == alg_self) &&
                PFprop_key (R(n)->prop, n->sem.step.item))
                union_ (n->prop->keys, n->sem.step.item_res);

            /* if attribute step is only a 'filter' (at most a single
               attribute for each context node) we can copy all keys */
            if (n->sem.step.spec.axis == alg_attr &&
                n->sem.step.spec.kind == node_kind_attr &&
                ! (PFQNAME_NS_WILDCARD (n->sem.step.spec.qname)
                   || PFQNAME_LOC_WILDCARD (n->sem.step.spec.qname)) &&
                PFprop_key (R(n)->prop, n->sem.step.iter))
                union_ (n->prop->keys, n->sem.step.iter);

            /* the self axis can be only a filter */
            if (n->sem.step.spec.axis == alg_self &&
                PFprop_key (R(n)->prop, n->sem.step.iter))
                union_ (n->prop->keys, n->sem.step.iter);
            break;

        case la_guide_step_join:
            /* copy existing keys if the cardinality does not change */
            if ((n->sem.step.spec.axis == alg_chld ||
                 n->sem.step.spec.axis == alg_attr ||
                 n->sem.step.spec.axis == alg_self) &&
                find_guide_max (n->sem.step.guide_count,
                                n->sem.step.guides) <= 1)
                copy (n->prop->keys, R(n)->prop->keys);
            if (with_guide_info &&
                LEVEL_KNOWN(PFprop_level_right (n->prop, n->sem.step.item)) &&
                (n->sem.step.spec.axis == alg_desc ||
                 n->sem.step.spec.axis == alg_desc_s) &&
                PFprop_guide (R(n)->prop, n->sem.step.item) &&
                find_guide_max_rec (
                    n->sem.step.guide_count,
                    n->sem.step.guides,
                    PFprop_guide_count (R(n)->prop, n->sem.step.item),
                    PFprop_guide_elements (R(n)->prop, n->sem.step.item)) <= 1)
                copy (n->prop->keys, R(n)->prop->keys);
        case la_step_join:
            if ((key_worker (R(n)->prop->keys, n->sem.step.item) &&
                 (n->sem.step.spec.axis == alg_attr ||
                  n->sem.step.spec.axis == alg_chld ||
                  n->sem.step.spec.axis == alg_self)) ||
                (LEVEL_KNOWN(PFprop_level_right (n->prop, n->sem.step.item)) &&
                 key_worker (R(n)->prop->keys, n->sem.step.item) &&
                 (n->sem.step.spec.axis == alg_desc ||
                  n->sem.step.spec.axis == alg_desc_s))) {
                union_ (n->prop->keys, n->sem.step.item_res);
            }

            /* if attribute step is only a 'filter' (at most a single
               attribute for each context node) we can copy all keys */
            if (n->sem.step.spec.axis == alg_attr &&
                n->sem.step.spec.kind == node_kind_attr &&
                ! (PFQNAME_NS_WILDCARD (n->sem.step.spec.qname)
                   || PFQNAME_LOC_WILDCARD (n->sem.step.spec.qname)))
                copy (n->prop->keys, R(n)->prop->keys);

            /* the self axis can be only a filter */
            if (n->sem.step.spec.axis == alg_self)
                copy (n->prop->keys, R(n)->prop->keys);

            if (PFprop_card (R(n)->prop) == 1)
                union_ (n->prop->keys, n->sem.step.item_res);
            break;

        case la_doc_index_join:
            if (PFprop_card (R(n)->prop) == 1)
                union_ (n->prop->keys, n->sem.step.item_res);
            break;

        case la_doc_tbl:
            /* key columns are propagated */
            copy (n->prop->keys, L(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.doc_tbl.res);
            break;

        case la_doc_access:
            /* key columns are propagated */
            copy (n->prop->keys, R(n)->prop->keys);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (n->prop->keys, n->sem.doc_access.res);
            break;

        case la_twig:
            /* a constructor builds unique new nodes */
            union_ (n->prop->keys, n->sem.iter_item.item);
            switch (L(n)->kind) {
                case la_docnode:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.docnode.iter))
                        union_ (n->prop->keys, n->sem.iter_item.iter);
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.iter_item.iter))
                        union_ (n->prop->keys, n->sem.iter_item.iter);
                    break;

                case la_attribute:
                case la_processi:
                    if (PFprop_key (LL(n)->prop,
                                    L(n)->sem.iter_item1_item2.iter))
                        union_ (n->prop->keys, n->sem.iter_item.iter);
                    break;

                case la_content:
                    if (PFprop_key (LR(n)->prop,
                                    L(n)->sem.iter_pos_item.iter))
                        union_ (n->prop->keys, n->sem.iter_item.iter);
                    break;

                default:
                    break;
            }
            break;

        case la_error:
        case la_cache:
            copy (n->prop->keys, R(n)->prop->keys);
            break;

        case la_nil:
        case la_trace:
            break;

        case la_rec_fix:
            /* get the keys of the overall result */
            copy (n->prop->keys, R(n)->prop->keys);
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            copy (n->prop->keys, R(n)->prop->keys);
            break;

        case la_rec_base:
            /* infer no properties of the seed */
            break;

        case la_fun_call:
            if (n->sem.fun_call.occ_ind == alg_occ_exactly_one &&
                n->sem.fun_call.kind == alg_fun_call_xrpc &&
                key_worker (L(n)->prop->keys, n->sem.fun_call.iter))
                union_ (n->prop->keys, n->schema.items[0].name);
            break;

        case la_fun_param:
            copy (n->prop->keys, L(n)->prop->keys);
            break;

        case la_proxy:
        case la_proxy_base:
            copy (n->prop->keys, L(n)->prop->keys);
            break;

        case la_string_join:
            /* Every iteration yields exactly one
               tuple. Iterations iter are thus key. */
            union_ (n->prop->keys, n->sem.string_join.iter_res);
            break;

        case la_dummy:
            copy (n->prop->keys, L(n)->prop->keys);
            break;
    }
}

/* worker for PFprop_infer_key */
static void
prop_infer (PFla_op_t *n, bool with_guide_info, bool with_fd_info)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i], with_guide_info, with_fd_info);

    n->bit_dag = true;

    /* reset key information
       (reuse already existing lists if already available
        as this increases the performance of the compiler a lot) */
    if (n->prop->keys)
        clsize (n->prop->keys) = 0;
    else
        n->prop->keys = PFalg_collist (10);

    if (L(n)) {
        if (n->prop->l_keys)
            clsize (n->prop->l_keys) = 0;
        else
            n->prop->l_keys = PFalg_collist (10);
    }

    if (R(n)) {
        if (n->prop->r_keys)
            clsize (n->prop->r_keys) = 0;
        else
            n->prop->r_keys = PFalg_collist (10);
    }

    /* infer information on key columns */
    infer_key (n, with_guide_info, with_fd_info);
}

/**
 * Worker for key property inference.
 */
static void
prop_infer_key (PFla_op_t *root, bool guides, bool fds)
{
    /* infer cardinalities to discover more key columns */
    PFprop_infer_card (root);
    /* use the cheaper domain inference that only infers
       domains for columns of the native type */
    PFprop_infer_nat_dom (root);
    PFprop_infer_level (root);

    prop_infer (root, guides, fds);
    PFla_dag_reset (root);
}

/**
 * Infer key property for a DAG rooted in root
 */
void
PFprop_infer_key (PFla_op_t *root)
{
    prop_infer_key (root, false, false);
}

/**
 * Infer the key properties assuming that guides have been already inferred.
 */
void
PFprop_infer_key_with_guide (PFla_op_t *root)
{
    prop_infer_key (root, true, false);
}

/**
 * Infer key and functional dependency properties for a DAG rooted in root.
 */
void
PFprop_infer_key_and_fd (PFla_op_t *root)
{
    PFprop_infer_functional_dependencies (root);
    prop_infer_key (root, false, true);
}

/* vim:set shiftwidth=4 expandtab: */
