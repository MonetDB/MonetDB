/**
 * @file
 *
 * Inference of unique column names in logical algebra expressions.
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
#include <stdio.h>

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
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))

/**
 * Return unique name of attribute @a attr stored
 * in property container @a prop.
 */
PFalg_att_t
PFprop_unq_name (const PFprop_t *prop, PFalg_att_t attr)
{
    assert (prop);
    assert (prop->name_pairs);
    
    for (unsigned int i = 0; i < PFarray_last (prop->name_pairs); i++)
        if (attr == ((name_pair_t *) PFarray_at (prop->name_pairs, i))->ori)
            return ((name_pair_t *) PFarray_at (prop->name_pairs, i))->unq;

    return 0;
}

/**
 * Return original name of unique attribute @a attr stored
 * in property container @a prop.
 */
PFalg_att_t
PFprop_ori_name (const PFprop_t *prop, PFalg_att_t attr)
{
    assert (prop);
    assert (prop->name_pairs);
    
    for (unsigned int i = 0; i < PFarray_last (prop->name_pairs); i++)
        if (attr == ((name_pair_t *) PFarray_at (prop->name_pairs, i))->unq)
            return ((name_pair_t *) PFarray_at (prop->name_pairs, i))->ori;

    return 0;
}

/**
 * Returns the textual representation of an unique attribute @a attr.
 */
char *
PFunq_att_str (PFalg_att_t attr)
{
    assert (attr < 10000);
    size_t len = sizeof ("0000");
    char *res = PFmalloc (len);
    snprintf (res, len, "%u", attr);

    return res;
}

/**
 * Add a new original name/unique name pair to the list of name pairs
 * (stored in property container @a prop).
 */
static void
add_name_pair (PFprop_t *prop, PFalg_att_t ori, unsigned int unq)
{
    assert (prop);
    assert (prop->name_pairs);
    
    *(name_pair_t *) PFarray_add (prop->name_pairs)
        = (name_pair_t) { .ori = ori, .unq = (PFalg_att_t) unq };
}

/**
 * Add all original name/unique name pairs of the node @a child to
 * the list of name pairs (stored in property container @a prop).
 */
static void
bulk_add_name_pairs (PFprop_t *prop, PFla_op_t *child)
{
    assert (prop);
    assert (child);
    assert (child->prop);

    for (unsigned int i = 0; i < child->schema.count; i++) {
        add_name_pair (prop, 
                       child->schema.items[i].name,
                       PFprop_unq_name (child->prop,
                                        child->schema.items[i].name));
    }
}

/**
 * Infer unique name properties; worker for prop_infer().
 */
static unsigned int
infer_unq_names (PFla_op_t *n, unsigned int id)
{
    switch (n->kind) {
        case la_serialize:
            bulk_add_name_pairs (n->prop, R(n));
            break;

        case la_lit_tbl:
        case la_empty_tbl:
            /* create new unique names for all attributes */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_name_pair (n->prop, n->schema.items[i].name, id++);
            break;
            
        case la_attach:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.attach.attname, id++);
            break;

        case la_cross:
        case la_eqjoin:
            bulk_add_name_pairs (n->prop, L(n));
            bulk_add_name_pairs (n->prop, R(n));
            break;

        case la_project:
            /* bind all existing unique names to the possibly new names */
            for (unsigned int i = 0; i < n->schema.count; i++)
                add_name_pair (n->prop, 
                               n->sem.proj.items[i].new, 
                               PFprop_unq_name (L(n)->prop,
                                                n->sem.proj.items[i].old));
            break;

        case la_select:
            bulk_add_name_pairs (n->prop, L(n));
            break;
            
        case la_disjunion:
            /* create new unique attribute names if the unique 
               names of the children do not match */
            for (unsigned int i = 0; i < L(n)->schema.count; i++) {
                unsigned int j;
                PFalg_att_t unq1, unq2;

                for (j = 0; j < R(n)->schema.count; j++)
                    if (L(n)->schema.items[i].name ==
                        R(n)->schema.items[j].name) {
                        unq1 = PFprop_unq_name (L(n)->prop,
                                                L(n)->schema.items[i].name);
                        unq2 = PFprop_unq_name (R(n)->prop,
                                                R(n)->schema.items[j].name);
                            
                        if (unq1 == unq2)
                            add_name_pair (n->prop,
                                           L(n)->schema.items[i].name,
                                           unq1);
                        else
                            add_name_pair (n->prop, 
                                           L(n)->schema.items[i].name,
                                           id++);
                        break;
                    }
                if (j == R(n)->schema.count)
                    PFoops (OOPS_FATAL,
                            "can't find matching columns in "
                            "unique name property inference.");
            }
            break;
            
        case la_intersect:
        case la_difference:
        case la_distinct:
            /* create new unique names for all existing attributes */
            for (unsigned int i = 0; i < L(n)->schema.count; i++)
                add_name_pair (n->prop, L(n)->schema.items[i].name, id++);
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
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.binary.res, id++);
            break;

        case la_num_neg:
        case la_bool_not:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.unary.res, id++);
            break;

        case la_avg:
	case la_max:
	case la_min:
        case la_sum:
            add_name_pair (n->prop, n->sem.aggr.res, id++);
            if (n->sem.aggr.part)
                add_name_pair (n->prop, n->sem.aggr.part, id++);
            break;

        case la_count:
            add_name_pair (n->prop, n->sem.count.res, id++);
            if (n->sem.count.part)
                add_name_pair (n->prop, n->sem.count.part, id++);
            break;

        case la_rownum:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.rownum.attname, id++);
            break;

        case la_number:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.number.attname, id++);
            break;

        case la_type:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.type.res, id++);
            break;

        case la_type_assert:
            bulk_add_name_pairs (n->prop, L(n));
            break;

        case la_cast:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.cast.res, id++);
            break;

        case la_seqty1:
        case la_all:
            add_name_pair (n->prop, n->sem.blngroup.res, id++);
            if (n->sem.blngroup.part)
                add_name_pair (n->prop, n->sem.blngroup.part, id++);
            break;

        case la_scjoin:
            add_name_pair (n->prop, att_iter, id++);
            add_name_pair (n->prop, att_item, id++);
            break;
            
        case la_doc_tbl:
            add_name_pair (n->prop,
                           att_iter,
                           PFprop_unq_name (L(n)->prop, att_iter));
            add_name_pair (n->prop, att_item, id++);
            break;
            
        case la_doc_access:
            bulk_add_name_pairs (n->prop, R(n));
            add_name_pair (n->prop, n->sem.doc_access.res, id++);
            break;

        case la_element:
            add_name_pair (n->prop,
                           att_iter,
                           PFprop_unq_name (R(n)->prop, att_iter));
            add_name_pair (n->prop, att_item, id++);
            break;
        
        case la_element_tag:
            add_name_pair (n->prop,
                           att_iter,
                           PFprop_unq_name (L(n)->prop, att_iter));
            add_name_pair (n->prop, att_item, id++);
            break;
            
        case la_attribute:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.attr.res, id++);
            break;

        case la_textnode:
            bulk_add_name_pairs (n->prop, L(n));
            add_name_pair (n->prop, n->sem.textnode.res, id++);
            break;

        case la_docnode:
        case la_comment:
        case la_processi:
            break;
            
        case la_merge_adjacent:
            add_name_pair (n->prop, att_iter, id++);
            add_name_pair (n->prop, att_pos, id++);
            add_name_pair (n->prop, att_item, id++);
            break;
            
        case la_roots:
            bulk_add_name_pairs (n->prop, L(n));
            break;

        case la_fragment:
        case la_frag_union:
        case la_empty_frag:
            break;
            
        case la_cond_err:
            bulk_add_name_pairs (n->prop, L(n));
            break;

        case la_string_join:
            add_name_pair (n->prop, att_iter, id++);
            add_name_pair (n->prop, att_item, id++);
            break;

        case la_cross_dup:
            PFoops (OOPS_FATAL,
                    "duplicate aware cross product operator is "
                    "only allowed inside mvd optimization!");
    }
    return id;
}

/* worker for PFprop_infer_unq_names */
static unsigned int
prop_infer (PFla_op_t *n, unsigned int cur_col_id)
{
    assert (n);

    /* nothing to do if we already visited that node */
    if (n->bit_dag)
        return cur_col_id;

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        cur_col_id = prop_infer (n->child[i], cur_col_id);

    n->bit_dag = true;

    /* reset the unique name information */
    n->prop->name_pairs = PFarray (sizeof (name_pair_t));
    
    /* infer unique name columns */
    cur_col_id = infer_unq_names (n, cur_col_id);

    return cur_col_id;
}

/**
 * Infer unique names for a DAG rooted in root
 */
void
PFprop_infer_unq_names (PFla_op_t *root)
{
    /* avoid using the bits of iter 1, item 2,
       and pos 4 and thus start with 8 */
    prop_infer (root, 8);
    PFla_dag_reset (root);
}

/* vim:set shiftwidth=4 expandtab: */
