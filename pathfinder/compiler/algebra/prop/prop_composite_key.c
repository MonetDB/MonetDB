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

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/* mnemonic column list accessors */
#include "alg_cl_mnemonic.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** abbreviation for column list combination */
#define clconcat(cl1,cl2) PFalg_collist_concat (PFalg_collist_copy ((cl1)), \
                                                (cl2))
/** abbreviation for 'column list' list accessors */
#define llat(cl,i)        *(PFalg_collist_t **) PFarray_at ((cl), (i))
#define lladd(cl)         *(PFalg_collist_t **) PFarray_add ((cl))
#define llsize(cl)        PFarray_last ((cl))

/** abbreviation for the column list lists */
#define CKEYS  n->prop->ckeys
#define LCKEYS L(n)->prop->ckeys
#define RCKEYS R(n)->prop->ckeys

/**
 * Check if a column @a b appears in list @a a.
 */
static bool
in (PFalg_collist_t *a, PFalg_col_t b)
{
    if (!a) return false;

    for (unsigned int i = 0; i < clsize (a); i++)
        if (b == clat (a, i))
            return true;

    return false;
}

/**
 * Check if all column in a list of key columns @a ckey
 * also appears in column list @a collist.
 */
static bool
ckey_in (PFalg_collist_t *ckey, PFalg_collist_t *collist)
{
    bool found = false;
    for (unsigned int i = 0; i < clsize (ckey); i++) {
        found = in (collist, clat (ckey, i));
        if (found == false)
            break;
    }
    return found;
}

/**
 * worker for PFprop_ckey;
 * Test if @a collist matches any compossite key in @a ckeys
 * and return the size of the key.
 */
static unsigned int
ckey_worker (PFarray_t *ckeys, PFalg_collist_t *collist)
{
    if (!ckeys)
        return 0;

    for (unsigned int i = 0; i < llsize (ckeys); i++) {
        PFalg_collist_t *ckey = llat (ckeys, i);
        if (ckey_in (ckey, collist))
            return clsize (ckey);
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
    PFalg_collist_t *collist = PFalg_collist (schema.count);
    for (unsigned int i = 0; i < schema.count; i++)
        cladd (collist) = schema.items[i].name;

    return ckey_worker (prop->ckeys, collist);
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
        return llsize (prop->ckeys);
}

/**
 * Return columns that build a composite key (at position @a i)
 * as an PFalg_collist_t.
 */
PFalg_collist_t *
PFprop_ckey_at (const PFprop_t *prop, unsigned int i)
{
    return PFalg_collist_copy (llat (prop->ckeys, i));
}

/**
 * Remove column @a b from column list @a a.
 */
static PFalg_collist_t *
remove_col (PFalg_collist_t *a, PFalg_col_t b)
{
    assert (a);

    for (unsigned int i = 0; i < clsize (a); i++)
        if (b == clat (a, i)) {
            clat (a, i) = cltop (a);
            clsize (a) -= 1;
            break;
        }
    return a;
}

/**
 * Extend the composite key list @a with a single
 * column list @a collist if @a collist is not in the list.
 */
static void
union_ (PFarray_t *a, PFalg_collist_t *collist)
{
    assert (a);

    if (!ckey_worker (a, collist))
        lladd (a) = collist;
}

/**
 * Extend composite key list @a with all compound
 * keys of the composite key list @a b that are not
 * in @a a.
 */
static void
union_list (PFarray_t *a, PFarray_t *b)
{
    PFalg_collist_t *collist;

    assert (a && b);

    for (unsigned int i = 0; i < llsize (b); i++) {
        collist = llat (b, i);
        if (!ckey_worker (a, collist))
            lladd (a) = collist;
    }
}

/**
 * Create a copy of a composite key list.
 */
static void
copy (PFarray_t *base, PFarray_t *content)
{
    for (unsigned int i = 0; i < llsize (content); i++)
        lladd (base) = llat (content, i);
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
        case la_serialize_rel:
            /* just copy composite keys from right child */
            copy (CKEYS, RCKEYS);
            break;

        case la_side_effects:
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
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_fun_frag_param:
            /* no composite keys */
            break;

        case la_lit_tbl:
            /* all columns are key */
            if (n->sem.lit_tbl.count == 1)
                for (unsigned int i = 0; i < n->schema.count; i++)
                    union_ (CKEYS, collist (n->schema.items[i].name));
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
                        union_ (CKEYS, collist (n->schema.items[i].name));
                }
            break;

        case la_ref_tbl:
            for (unsigned int key = 0;
                     key < PFarray_last (n->sem.ref_tbl.keys);
                     key++)
                {
                    PFarray_t* keyPositions = *((PFarray_t**) PFarray_at (n->sem.ref_tbl.keys, key));
                    unsigned int keyCount = PFarray_last (keyPositions);
                    /* Make sure we really have a component key here. */
                    if(keyCount > 1 ) {

                        PFalg_collist_t *collist = PFalg_collist (keyCount);
    
                        for (unsigned int i = 0;
                            i < PFarray_last (keyPositions);
                            i++)
                        {
                            int keyPos = *((int*) PFarray_at (keyPositions, i));
                            assert(keyPos >= 0);
                            PFalg_schm_item_t schemaItem = n->schema.items[keyPos];
                            PFalg_col_t keyName = schemaItem.name;
                            PFalg_collist_add (collist) = keyName;
        
                        }
    
                        union_ (CKEYS, collist);
                    }
                }    
            break;

        case la_attach:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.attach.res));
            break;

        case la_cross:
        {
            unsigned int i, j;
            /* combine all keys of the left argument
               with all keys of the right argument */
            for (i = 0; i < llsize (LCKEYS); i++)
                for (j = 0; j < llsize (RCKEYS); j++) {
                    union_ (CKEYS,
                            clconcat (llat (LCKEYS, i),
                                      llat (RCKEYS, j)));
                }

            for (i = 0; i < clsize (L(n)->prop->keys); i++)
                for (j = 0; j < clsize (R(n)->prop->keys); j++) {
                    union_ (CKEYS,
                            collist (clat (L(n)->prop->keys, i),
                                     clat (R(n)->prop->keys, j)));
                }
        }   break;

        case la_eqjoin:
        {
            PFalg_collist_t *l_list,
                            *r_list;
            unsigned int    i,
                            j;

            /* only a key-join retains all key properties */
            if (PFprop_key (L(n)->prop, n->sem.eqjoin.col1) &&
                PFprop_key (R(n)->prop, n->sem.eqjoin.col2)) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, LCKEYS);
                union_list (CKEYS, RCKEYS);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (x.keys - x.join_col + y.keys) */
                for (i = 0; i < llsize (LCKEYS); i++)
                    for (j = 0; j < llsize (RCKEYS); j++) {
                        l_list = llat (LCKEYS, i);
                        r_list = llat (RCKEYS, j);
                        if (in (l_list, n->sem.eqjoin.col1))
                            union_ (CKEYS,
                                    remove_col (clconcat (l_list, r_list),
                                                n->sem.eqjoin.col1));
                        if (in (r_list, n->sem.eqjoin.col2))
                            union_ (CKEYS,
                                    remove_col (clconcat (l_list, r_list),
                                                n->sem.eqjoin.col2));
                    }
            }
            else if (PFprop_key (L(n)->prop, n->sem.eqjoin.col1)) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, RCKEYS);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (r.keys - r.join_col + l.keys) */
                for (i = 0; i < llsize (LCKEYS); i++)
                    for (j = 0; j < llsize (RCKEYS); j++) {
                        l_list = llat (LCKEYS, i);
                        r_list = llat (RCKEYS, j);
                        if (in (r_list, n->sem.eqjoin.col2))
                            union_ (CKEYS,
                                    remove_col (clconcat (l_list, r_list),
                                                n->sem.eqjoin.col2));
                    }
            } else if (PFprop_key (R(n)->prop, n->sem.eqjoin.col2)) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, LCKEYS);

                /* combine all composite keys from the non-key join argument
                   without the join argument with the keys from the key join
                   argument: (l.keys - l.join_col + r.keys) */
                for (i = 0; i < llsize (LCKEYS); i++)
                    for (j = 0; j < llsize (RCKEYS); j++) {
                        l_list = llat (LCKEYS, i);
                        r_list = llat (RCKEYS, j);
                        if (in (l_list, n->sem.eqjoin.col1))
                            union_ (CKEYS,
                                    remove_col (clconcat (l_list, r_list),
                                                n->sem.eqjoin.col1));
                    }
            } else {
                /* combine all keys of the left argument
                   with all keys of the right argument */
                for (i = 0; i < llsize (LCKEYS); i++)
                    for (j = 0; j < llsize (RCKEYS); j++) {
                        union_ (CKEYS,
                                clconcat (llat (LCKEYS, i),
                                          llat (RCKEYS, j)));
                        /* if both join arguments together form a
                           composite key we can also drop one column */
                        if (in (llat (LCKEYS, i), n->sem.eqjoin.col1) &&
                            in (llat (RCKEYS, i), n->sem.eqjoin.col2)) {
                            union_ (CKEYS,
                                    remove_col (clconcat (llat (LCKEYS, i),
                                                          llat (RCKEYS, j)),
                                                n->sem.eqjoin.col1));
                            union_ (CKEYS,
                                    remove_col (clconcat (llat (LCKEYS, i),
                                                          llat (RCKEYS, j)),
                                                n->sem.eqjoin.col2));
                        }
                    }
            }

            for (i = 0; i < clsize (L(n)->prop->keys); i++)
                for (j = 0; j < clsize (R(n)->prop->keys); j++) {
                    union_ (CKEYS,
                            collist (clat (L(n)->prop->keys, i),
                                     clat (R(n)->prop->keys, j)));
                }
        }   break;
        
        case la_semijoin:
            copy (CKEYS, LCKEYS);
            break;

        case la_thetajoin:
        {
            unsigned int i, j;
            bool key_left = false,
                 key_right = false;
            PFalg_collist_t *lcollist = PFalg_collist (L(n)->schema.count),
                            *rcollist = PFalg_collist (R(n)->schema.count);

            for (i = 0; i < n->sem.thetajoin.count; i++)
                if (n->sem.thetajoin.pred[i].comp == alg_comp_eq) {
                    key_left = key_left ||
                               PFprop_key (L(n)->prop,
                                           n->sem.thetajoin.pred[i].left);
                    key_right = key_right ||
                                PFprop_key (R(n)->prop,
                                            n->sem.thetajoin.pred[i].right);
                    /* collect all eq predicates */
                    cladd (lcollist) = n->sem.thetajoin.pred[i].left;
                    cladd (rcollist) = n->sem.thetajoin.pred[i].right;
                }

            /* find composite keys in the input */
            key_left  |= ckey_worker (LCKEYS, lcollist);
            key_right |= ckey_worker (RCKEYS, rcollist);

            /* only a key-join retains all key properties */
            if (key_left && key_right) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, LCKEYS);
                union_list (CKEYS, RCKEYS);
            }
            else if (key_left) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, RCKEYS);
            } else if (key_right) {
                /* copy composite keys from the non-key join argument */
                copy (CKEYS, LCKEYS);
            }

            /* combine all keys of the left argument
               with all keys of the right argument */
            for (i = 0; i < llsize (LCKEYS); i++)
                for (j = 0; j < llsize (RCKEYS); j++) {
                    union_ (CKEYS,
                            clconcat (llat (LCKEYS, i),
                                      llat (RCKEYS, j)));
                }

            for (i = 0; i < clsize (L(n)->prop->keys); i++)
                for (j = 0; j < clsize (R(n)->prop->keys); j++) {
                    union_ (CKEYS,
                            collist (clat (L(n)->prop->keys, i),
                                     clat (R(n)->prop->keys, j)));
                }
        }   break;

        case la_project:
        {
            PFarray_t *ckeys = LCKEYS;
            PFalg_collist_t *ckey,
                            *collist = PFalg_collist (n->schema.count);
            bool rename = false;
            unsigned int i, j, k;

            /* Collect all 'old' projection column names. */
            for (i = 0; i < n->sem.proj.count; i++) {
                if (n->sem.proj.items[i].new != n->sem.proj.items[i].old)
                    rename = true;
                cladd (collist) = n->sem.proj.items[i].old;
            }
            /* Copy all composite keys that are still valid
               after projecting away some columns. */
            for (i = 0; i < llsize (ckeys); i++) {
                ckey = llat (ckeys, i);
                if (ckey_in (ckey, collist))
                    union_ (CKEYS, ckey);
            }

            /* If the projection did not rename any column we are done ... */
            if (!rename) break;

            /* ... Otherwise we need to fix the column names. */
            ckeys = CKEYS;
            for (i = 0; i < llsize (ckeys); i++) {
                ckey = PFalg_collist_copy (llat (ckeys, i));

                for (j = 0; j < clsize (ckey); j++)
                    for (k = 0; k < n->sem.proj.count; k++)
                        if (clat (ckey, j) == n->sem.proj.items[k].old) {
                            clat (ckey, j) = n->sem.proj.items[k].new;
                            break;
                        }
                llat (ckeys, i) = ckey;
            }
        }   break;

        case la_select:
        case la_pos_select:
        case la_difference:
        case la_type_assert:
        case la_roots:
        case la_trace_items:
        case la_trace_msg:
        case la_trace_map:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);
            break;

        case la_intersect:
        {
            bool key_left = false,
                 key_right = false;

            for (unsigned int i = 0; i < n->schema.count; i++) {
                key_left = key_left ||
                           PFprop_key (L(n)->prop, 
                                       n->schema.items[i].name);
                key_right = key_right ||
                            PFprop_key (R(n)->prop, 
                                        n->schema.items[i].name);
            }

            /* only a key-join retains all key properties */
            if (key_left && key_right) {
                copy (CKEYS, LCKEYS);
                union_list (CKEYS, RCKEYS);
            }
            else if (key_left)
                copy (CKEYS, RCKEYS);
            else if (key_right)
                copy (CKEYS, LCKEYS);
        }   break;

        case la_distinct:
            if (PFprop_ckey (L(n)->prop, n->schema))
                /* composite key columns stay the same */
                copy (CKEYS, LCKEYS);
            else {
                /* we have a new composite key */
                PFalg_collist_t *collist = PFalg_collist (n->schema.count);
                for (unsigned int i = 0; i < n->schema.count; i++)
                    cladd (collist) = n->schema.items[i].name;
                union_ (CKEYS, collist);
            }
            break;

        case la_fun_1to1:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.fun_1to1.res));
            break;

        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.binary.res));
            break;

        case la_bool_not:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.unary.res));
            break;

        case la_to:
        {
            PFarray_t       *ckeys = LCKEYS;
            PFalg_collist_t *ckey;
            unsigned int     i;

            /* extend all keys with the result column */
            for (i = 0; i < llsize (ckeys); i++) {
                ckey = PFalg_collist_copy (llat (ckeys, i));
                cladd (ckey) = n->sem.binary.res;
                union_ (CKEYS, ckey);
            }
        }   break;

        case la_aggr:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (n->sem.aggr.part)
                union_ (CKEYS, collist (n->sem.aggr.part));
            else
                for (unsigned int i = 0; i < n->sem.aggr.count; i++)
                    union_ (CKEYS, collist (n->sem.aggr.aggr[i].res));
            break;

        case la_rownum:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1 || !n->sem.sort.part)
                union_ (CKEYS, collist (n->sem.sort.res));
            if (n->sem.sort.part)
                union_ (CKEYS, collist (n->sem.sort.res, n->sem.sort.part));
            break;

        case la_rowrank:
        case la_rank:
        {
            PFarray_t       *ckeys   = LCKEYS;
            unsigned int     i,
                             count   = PFord_count (n->sem.sort.sortby);
            PFalg_collist_t *collist = PFalg_collist (count);
            PFalg_col_t      col;

            copy (CKEYS, ckeys);

            for (i = 0; i < count; i++) {
                col = PFord_order_col_at (n->sem.sort.sortby, i);
                /* check normal keys ... */
                if (PFprop_key (L(n)->prop, col))
                    break;
                cladd (collist) = col;
            }
            if (i < count) {
                union_ (CKEYS, collist (n->sem.sort.res));
                break;
            }
            /* ... and composed keys */
            for (i = 0; i < llsize (ckeys); i++)
                if (ckey_in (llat (ckeys, i), collist)) {
                    union_ (CKEYS, collist (n->sem.sort.res));
                    break;
                }
        }   break;

        case la_rowid:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);
            union_ (CKEYS, collist (n->sem.rowid.res));
            break;

        case la_type:
        case la_cast:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* if the cardinality is equal to one
               the result is a key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.type.res));
            break;

        case la_step:
        case la_guide_step:
            if (n->sem.step.spec.axis == alg_chld &&
                PFprop_key (R(n)->prop, n->sem.step.iter) &&
                PFprop_key (R(n)->prop, n->sem.step.item))
                union_ (CKEYS, collist (n->sem.step.item_res));

            if (PFprop_const (n->prop, n->sem.step.iter))
                union_ (CKEYS, collist (n->sem.step.item_res));
            else
                union_ (CKEYS, collist (n->sem.step.iter,
                                        n->sem.step.item_res));
            break;

        case la_step_join:
        case la_guide_step_join:
        {
            PFarray_t       *right = RCKEYS;
            PFalg_collist_t *ckey,
                            *new_ckey;
            for (unsigned int i = 0; i < llsize (right); i++) {
                ckey = llat (right, i);

                if ((in (ckey, n->sem.step.item) &&
                     (n->sem.step.spec.axis == alg_attr ||
                      n->sem.step.spec.axis == alg_chld ||
                      n->sem.step.spec.axis == alg_self)) ||
                    (LEVEL_KNOWN(PFprop_level_right (n->prop,
                                                     n->sem.step.item)) &&
                     in (ckey, n->sem.step.item) &&
                     (n->sem.step.spec.axis == alg_desc ||
                      n->sem.step.spec.axis == alg_desc_s))) {
                    new_ckey = remove_col (PFalg_collist_copy (ckey),
                                           n->sem.step.item);
                    cladd (new_ckey) = n->sem.step.item_res;
                    union_ (CKEYS, new_ckey); 
                }
                
                new_ckey = PFalg_collist_copy (ckey);
                cladd (new_ckey) = n->sem.step.item_res;
                union_ (CKEYS, new_ckey); 
            }

            /* if attribute step is only a 'filter' (at most a single
               attribute for each context node) we can copy all keys */
            if (n->sem.step.spec.axis == alg_attr &&
                n->sem.step.spec.kind == node_kind_attr &&
                ! (PFQNAME_NS_WILDCARD (n->sem.step.spec.qname)
                   || PFQNAME_LOC_WILDCARD (n->sem.step.spec.qname)))
                copy (CKEYS, right);

            /* the self axis can be only a filter */
            if (n->sem.step.spec.axis == alg_self)
                copy (CKEYS, right);

            if (PFprop_card (R(n)->prop) == 1)
                union_ (CKEYS, collist (n->sem.step.item_res));

        }   break;

        case la_doc_index_join:
        {
            PFarray_t       *right = RCKEYS;
            PFalg_collist_t *ckey,
                            *new_ckey;
            for (unsigned int i = 0; i < llsize (right); i++) {
                ckey = llat (right, i);
                new_ckey = PFalg_collist_copy (ckey);
                cladd (new_ckey) = n->sem.doc_join.item_res;
                union_ (CKEYS, new_ckey); 
            }

            if (PFprop_card (R(n)->prop) == 1)
                union_ (CKEYS, collist (n->sem.doc_join.item_res));

        }   break;

        case la_doc_tbl:
            /* composite key columns are propagated */
            copy (CKEYS, LCKEYS);

            /* If the cardinality is equal to one
               the result is key itself. */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.doc_tbl.res));
            break;

        case la_doc_access:
            /* composite key columns are propagated */
            copy (CKEYS, RCKEYS);

            /* if the cardinality is equal to one
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                union_ (CKEYS, collist (n->sem.doc_access.res));
            break;

        case la_twig:
            /* a constructor builds unique new nodes */
            union_ (CKEYS, collist (n->sem.iter_item.item));
            switch (L(n)->kind) {
                case la_docnode:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.docnode.iter))
                        union_ (CKEYS, collist (n->sem.iter_item.iter));
                    break;

                case la_element:
                case la_textnode:
                case la_comment:
                    if (PFprop_key (LL(n)->prop, L(n)->sem.iter_item.iter))
                        union_ (CKEYS, collist (n->sem.iter_item.iter));
                    break;

                case la_attribute:
                case la_processi:
                    if (PFprop_key (LL(n)->prop,
                                    L(n)->sem.iter_item1_item2.iter))
                        union_ (CKEYS, collist (n->sem.iter_item.iter));
                    break;

                case la_content:
                    if (PFprop_key (LR(n)->prop,
                                    L(n)->sem.iter_pos_item.iter))
                        union_ (CKEYS, collist (n->sem.iter_item.iter));
                    break;

                default:
                    break;
            }
            break;

        case la_error:
        case la_cache:
            copy (CKEYS, RCKEYS);
            break;

        case la_nil:
        case la_trace:
            break;

        case la_rec_fix:
            /* get the keys of the overall result */
            copy (CKEYS, RCKEYS);
            break;

        case la_rec_param:
            /* recursion parameters do not have properties */
            break;

        case la_rec_arg:
            copy (CKEYS, RCKEYS);
            break;

        case la_rec_base:
            /* infer no properties of the seed */
            break;

        case la_fun_call:
            if (n->sem.fun_call.occ_ind == alg_occ_exactly_one &&
                n->sem.fun_call.kind == alg_fun_call_xrpc &&
                ckey_worker (LCKEYS, collist (n->sem.fun_call.iter)))
                union_ (CKEYS, collist (n->schema.items[0].name));
            break;

        case la_fun_param:
            copy (CKEYS, LCKEYS);
            break;

        case la_proxy:
        case la_proxy_base:
            copy (CKEYS, LCKEYS);
            break;

        case la_string_join:
            /* Every iteration yields exactly one
               tuple. Iterations iter are thus key. */
            union_ (CKEYS, collist (n->sem.string_join.iter_res));
            break;

        case la_internal_op:
            PFoops (OOPS_FATAL,
                    "internal optimization operator is not allowed here");

        case la_dummy:
            copy (CKEYS, LCKEYS);
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
    if (CKEYS)
        llsize (CKEYS) = 0;
    else
        /* prepare the property for 10 composite keys */
        CKEYS = PFarray (sizeof (PFalg_collist_t *), 10);

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

