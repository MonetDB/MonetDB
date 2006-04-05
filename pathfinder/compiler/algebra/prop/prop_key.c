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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2006 University of Konstanz.  All Rights Reserved.
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

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])

/**
 * Test if @a attr is in the list of key columns in container @a prop
 */
bool
PFprop_key (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->keys & attr;
}

/**
 * Test if @a attr is in the list of key columns of the left child
 * (information is stored in property container @a prop)
 */
bool
PFprop_key_left (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->l_keys & attr;
}

/**
 * Test if @a attr is in the list of key columns of the right child
 * (information is stored in property container @a prop)
 */
bool
PFprop_key_right (const PFprop_t *prop, PFalg_att_t attr)
{
    return prop->r_keys & attr;
}

/**
 * worker for PFprop_keys_count and PFprop_keys_to_attlist
 */
static unsigned int
keys_count (const PFprop_t *prop)
{
    PFalg_att_t keys = prop->keys;
    unsigned int counter = 0;

    while (keys) {
        counter += keys & 1;
        keys >>= 1;
    }
    return counter;
}

/*
 * count number of keys attributes
 */
unsigned int
PFprop_keys_count (const PFprop_t *prop)
{
    return keys_count (prop);
}

/**
 * Return keys attributes in an attlist.
 */
PFalg_attlist_t
PFprop_keys_to_attlist (const PFprop_t *prop)
{
    PFalg_attlist_t new_list;
    PFalg_att_t keys = prop->keys;
    unsigned int counter = 0, bit_shift = 1;

    new_list.count = keys_count (prop);
    new_list.atts = PFmalloc (new_list.count * sizeof (*(new_list.atts)));

    /* unfold keys into a list of attributes */
    while (keys && counter < new_list.count) {
        new_list.atts[counter] = prop->keys & bit_shift;
        bit_shift <<= 1;

        counter += keys & 1;
        keys >>= 1;
    }
    return new_list;
}

/**
 * Returns union of two keys lists
 */
static PFalg_att_t
union_ (PFalg_att_t a, PFalg_att_t b)
{
    return a | b;
}

/**
 * Returns difference of two keys lists
 */
static PFalg_att_t
diff (PFalg_att_t a, PFalg_att_t b)
{
    return a & (~b);
}

/**
 * Infer key property of a given node @a n; worker for prop_infer().
 */
static void
infer_key (PFla_op_t *n)
{
    if (L(n)) n->prop->l_keys = L(n)->prop->keys;
    if (R(n)) n->prop->r_keys = R(n)->prop->keys;

    switch (n->kind) {
        case la_serialize:
            /* just copy keys from left child */
            n->prop->keys = R(n)->prop->keys;
            break;

        case la_lit_tbl:
            n->prop->keys = 0;
            /* all columns are key */
            if (n->sem.lit_tbl.count == 1)
                for (unsigned int i = 0; i < n->schema.count; i++)
                    n->prop->keys = union_ (n->prop->keys,
                                            n->schema.items[i].name);
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
                        n->prop->keys = union_ (n->prop->keys,
                                                n->schema.items[i].name);
                    
                }
            break;
                                            
        case la_empty_tbl:
        case la_disjunion:
        case la_element_tag:
        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
        case la_docnode:
        case la_comment:
        case la_processi:
        case la_merge_adjacent:
            /* no keys */
            n->prop->keys = 0;
            break;

        case la_attach:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;
            
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.attach.attname);
            break;
                
        case la_cross:
            /* keys of one side can be retained
               if the other side has a cardinality of 1.
               Otherwise no information about keys
               is available */
            if (PFprop_card (L(n)->prop) == 1)
                n->prop->keys = R(n)->prop->keys;
            else if (PFprop_card (R(n)->prop) == 1)
                n->prop->keys = L(n)->prop->keys;
            else
                n->prop->keys = 0;
            break;
            
        case la_eqjoin:
            /* only a key-join retains all key properties */
            if (PFprop_key (L(n)->prop, n->sem.eqjoin.att1) &&
                PFprop_key (R(n)->prop, n->sem.eqjoin.att2))
                n->prop->keys = union_ (L(n)->prop->keys,
                                        R(n)->prop->keys);
            else if (PFprop_key (L(n)->prop, n->sem.eqjoin.att1))
                n->prop->keys = R(n)->prop->keys;
            else if (PFprop_key (R(n)->prop, n->sem.eqjoin.att2))
                n->prop->keys = L(n)->prop->keys;
            else
                n->prop->keys = 0;
            break;
            
        case la_project:
            n->prop->keys = 0;
            /* rename keys columns from old to new */
            for (unsigned int i = 0; i < n->sem.proj.count; i++)
                if (L(n)->prop->keys & n->sem.proj.items[i].old)
                    n->prop->keys = union_ (n->prop->keys,
                                            n->sem.proj.items[i].new);
            break;
            
        case la_select:
        case la_difference:
        case la_type_assert:
        case la_roots:
        case la_cond_err:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;
            break;

        case la_intersect:
            n->prop->keys = union_ (L(n)->prop->keys,
                                    R(n)->prop->keys);
            break;
            
        case la_distinct:
            if (n->schema.count == 1)
                /* if distinct works on a single column,
                   this column is key afterwards. */
                n->prop->keys = n->schema.items[0].name;
            else
                /* key columns stay the same */
                n->prop->keys = L(n)->prop->keys;
            break;
            
        case la_num_add:
        case la_num_subtract:
        case la_num_multiply:
        case la_num_divide:
        case la_num_modulo:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_concat:
        case la_contains:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.binary.res);
            break;
            
        case la_num_neg:
        case la_bool_not:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.unary.res);
            break;
            
        case la_avg:
	case la_max:
	case la_min:
        case la_sum:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (PFprop_key (L(n)->prop, n->sem.aggr.part))
                n->prop->keys = n->sem.aggr.part;
            else
                n->prop->keys = n->sem.aggr.res;
            break;
            
        case la_count:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (PFprop_key (L(n)->prop, n->sem.count.part))
                n->prop->keys = n->sem.count.part;
            else if (!n->sem.count.part)
                n->prop->keys = n->sem.count.res;
            else
                n->prop->keys = 0;
            break;
            
        case la_rownum:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1 || !n->sem.rownum.part)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.rownum.attname);
            break;
            
        case la_number:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1 || !n->sem.number.part)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.number.attname);
            break;
            
        case la_type:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.type.res);
            break;
            
        case la_cast:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.cast.res);
            break;
            
        case la_seqty1:
        case la_all:
            /* either the partition is key or if not
               present the aggregated result as it
               contains only one tuple */
            if (PFprop_key (L(n)->prop, n->sem.blngroup.part))
                n->prop->keys = n->sem.blngroup.part;
            else
                n->prop->keys = n->sem.blngroup.res;
            break;
            
        case la_scjoin:
            if (PFprop_const (n->prop, att_iter))
                n->prop->keys = att_item;
            else
                n->prop->keys = 0;
            break;

        case la_doc_tbl:
            if (PFprop_card (n->prop) == 1)
                /* If the cardinality is equal to one 
                   the result is key itself. */
                n->prop->keys = union_ (att_iter, att_item);
            else
                /* Otherwise at least column iter is key. */
                n->prop->keys = att_iter;
            break;
            
        case la_doc_access:
            /* key columns are propagated */
            n->prop->keys = L(n)->prop->keys;

            /* if the cardinality is equal to one 
               the result is key itself */
            if (PFprop_card (n->prop) == 1)
                n->prop->keys = union_ (n->prop->keys,
                                        n->sem.doc_access.res);
            break;
            
        case la_element:
            /* Element construction builds exactly
               one element in each iteration. */
            n->prop->keys = union_ (att_iter, att_item);
            break;
            
        case la_attribute:
            n->prop->keys = union_ (L(n)->prop->keys,
                                    n->sem.attr.res);
            break;

        case la_textnode:
            n->prop->keys = union_ (L(n)->prop->keys,
                                    n->sem.textnode.res);
            break;

        case la_string_join:
            /* Every iteration yields exactly one
               tuple. Iterations iter are thus key. */
            n->prop->keys = att_iter;
            break;
    }
}

/* worker for PFprop_infer_key */
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

    /* infer information on key columns */
    infer_key (n);
}

/**
 * Infer key property for a DAG rooted in root
 */
void
PFprop_infer_key (PFla_op_t *root) {
    /* infer cardinalities and constant column to
       discover more key columns */
    PFprop_infer_card (root);
    PFprop_infer_const (root);

    prop_infer (root);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
