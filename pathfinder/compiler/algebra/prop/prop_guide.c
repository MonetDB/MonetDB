/**
 * @file
 *
 * Inference of guide nodes of logical algebra expressions.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "mem.h"
#include "properties.h"
#include "alg_dag.h"

/* Easily access subtree-parts */
#include "child_mnemonic.h"

#define SEEN(n) ((n)->bit_dag)
/* prop of n */
#define PROP(n) ((n)->prop)
/* guide_mapping_list of n */
#define MAPPING_LIST(n) ((n)->prop->guide_mapping_list)

#define GUIDE_AT(n,i)     (*((PFguide_tree_t **) PFarray_at ((n), (i))))
#define GUIDE_ADD(n)      (*((PFguide_tree_t **) PFarray_add ((n))))
#define GUIDE_IDX_AT(n,i) (*((int *) PFarray_at ((n), (i))))
#define GUIDE_IDX_ADD(n)  (*((int *) PFarray_add ((n))))
#define GUIDE_MAP_AT(n,i) (*((PFguide_mapping_t **) PFarray_at ((n), (i))))
#define GUIDE_MAP_ADD(n)  (*((PFguide_mapping_t **) PFarray_add ((n))))

/* Maps a list of guides to a column */
typedef struct PFguide_mapping_t PFguide_mapping_t;

struct PFguide_mapping_t {
    PFalg_att_t   column;        /* name of the column */
    int           first_guide;   /* first index for the sorted guide nodes */
    /* NOTE: keep guide_order and guide_list aligned */
    PFarray_t    *guide_order;   /* indexes for the sorted guide nodes */
    PFarray_t    *guide_list;    /* list of guide nodes */
};

/* ++++++++++++++++++++++++++++++++++++ */

/**
 * @brief Create a new guide mapping for column @a column.
 */
static PFguide_mapping_t *
new_guide_mapping (PFalg_att_t column)
{
    PFguide_mapping_t *new_mapping;

    /* Create new PFguide_mapping_t */
    new_mapping = (PFguide_mapping_t *) PFmalloc (sizeof (PFguide_mapping_t));

    /* initialize new PF_guide_mapping element */
    new_mapping->column      = column;
    new_mapping->first_guide = -1;
    new_mapping->guide_order = PFarray (sizeof (int), 20);
    new_mapping->guide_list  = PFarray (sizeof (PFguide_tree_t *), 20);

    return new_mapping;
}

/**
 * @brief Copy the guide mapping @a mapping.
 */
static PFguide_mapping_t *
copy_guide_mapping (PFguide_mapping_t *mapping)
{
    PFguide_mapping_t *new_mapping;

    if (mapping == NULL) return NULL;

    /* Create new PFguide_mapping_t */
    new_mapping = (PFguide_mapping_t *) PFmalloc (sizeof (PFguide_mapping_t));

    /* initialize new PF_guide_mapping element */
    new_mapping->column      = mapping->column;
    new_mapping->first_guide = mapping->first_guide;
    new_mapping->guide_order = PFarray_copy (mapping->guide_order);
    new_mapping->guide_list  = PFarray_copy (mapping->guide_list);

    return new_mapping;
}

/**
 * @brief Add a guide mapping @a mapping to the guide mapping
 *        list @a map_list. (Initialize the guide mapping list
 *        if @a mapping is the first entry.)
 */
static PFarray_t *
add_guide_mapping (PFarray_t *map_list,
                   PFguide_mapping_t *guide_mapping)
{
    if (map_list == NULL)
        map_list = PFarray (sizeof (PFguide_mapping_t *), 5);
    
    GUIDE_MAP_ADD (map_list) = guide_mapping;

    return map_list;
}

/**
 * @brief Get the corresponding guide mapping from the
 *        guide mapping list @a map_list where the columns
 *        names (@a column) are equal.
 */
static PFguide_mapping_t *
get_guide_mapping (PFarray_t *map_list, PFalg_att_t column)
{
    PFguide_mapping_t *mapping;

    if (map_list == NULL) return NULL;

    /* search matching mapping of column*/
    for (unsigned int i = 0; i < PFarray_last (map_list); i++) {
        /* get element from PFarray_t */
        mapping = GUIDE_MAP_AT(map_list, i);

        if (column == mapping->column) return mapping;
    }

    return NULL;
}

/**
 * @brief Create a deep copy of the guide mapping list @a map_list.
 */
static PFarray_t *
deep_copy_guide_mapping_list (PFarray_t *map_list)
{
    PFarray_t *new_map_list;
    
    if (map_list == NULL) return NULL;

    /* new mapping list that will be the copy of the map_list list
       (leave some initial array space for additional mappings) */
    new_map_list = PFarray (sizeof (PFguide_mapping_t *),
                                    PFarray_last (map_list)+3);

    /* make a copy of all mapping elements of the map_list */
    for (unsigned int i = 0; i < PFarray_last (map_list); i++)
        GUIDE_MAP_ADD(new_map_list) = copy_guide_mapping (
                                          GUIDE_MAP_AT(map_list, i));
    return new_map_list;
}

/**
 * @brief Add a guide node to a given guide mapping @a mapping.
 *
 * add_guide() applies duplicate elimination and uses an additional
 * structure (@a guide_order) to keep the guides ordered and thus
 * the duplicate elimination cheaper.
 *
 * We always try to insert new guide nodes in reverse pre-order 
 * of their guide id to keep the number of lookups for the duplicate
 * elimination traversal of the list small.
 */
static void
add_guide (PFguide_mapping_t *mapping, PFguide_tree_t *guide)
{
    unsigned int guide_id;
    int          index,
                 prev_index = -1,
                 new_index;
   
    assert (mapping);
    if (guide == NULL)
        return;
    
    guide_id = guide->guide;
    index    = mapping->first_guide;

    /* this is the first entry */
    if (index < 0) {
        GUIDE_ADD(mapping->guide_list)      = guide;
        GUIDE_IDX_ADD(mapping->guide_order) = index;
        mapping->first_guide = 0;
        return;
    }
    /* the new entry is smaller than all others */
    else if (GUIDE_AT(mapping->guide_list,index)->guide > guide_id) {
        new_index = PFarray_last (mapping->guide_list);
        GUIDE_ADD(mapping->guide_list)      = guide;
        /* link to the next entry */
        GUIDE_IDX_ADD(mapping->guide_order) = index;
        /* adjust the first entry and link to the new one */
        mapping->first_guide = new_index;
        return;
    }
    
    while (index >= 0) {
        assert (index < (int) PFarray_last (mapping->guide_order));

        if (GUIDE_AT(mapping->guide_list,index) == guide)
            /* duplicate found */
            return;
        /* traverse list until no entry has a smaller guide id */
        else if (GUIDE_AT(mapping->guide_list,index)->guide <= guide_id) {
            prev_index = index;
            index      = GUIDE_IDX_AT(mapping->guide_order,index);
        }
        /* the next entry has a bigger guide id */
        else
            /* here is the place to insert */
            break;
    }

    /* insert the guide at the correct place (possibly at the end) */
    new_index = PFarray_last (mapping->guide_list);
    GUIDE_ADD(mapping->guide_list)      = guide;
    /* link to the next entry */
    GUIDE_IDX_ADD(mapping->guide_order) = index;
    /* adjust the previous entry and link to the new one */
    GUIDE_IDX_AT(mapping->guide_order,prev_index) = new_index;
}

/**
 * @brief Copy the guide mappings with respect
 *        to the projection operator semantics.
 */
static void
copy_project (PFla_op_t *n)
{
    PFguide_mapping_t *mapping      = NULL;
    PFarray_t         *map_list     = MAPPING_LIST(L(n)),
                      *new_map_list = PFarray (sizeof (PFguide_mapping_t *),
                                               n->sem.proj.count);
    PFalg_att_t        new,
                       old;

    if (map_list == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* iterate over all columns */
    for (unsigned int i = 0; i < n->sem.proj.count; i++) {
        /* get new and old column name */
        new = n->sem.proj.items[i].new;
        old = n->sem.proj.items[i].old;

        /* get guide mapping from list */
        mapping = get_guide_mapping (map_list, old);

        if (mapping == NULL)
            continue;

        /* create a copy */
        mapping = copy_guide_mapping (mapping);

        /* set new column name */
        mapping->column = new;

        /* assign guide mapping to the list */
        GUIDE_MAP_ADD(new_map_list) = mapping;
    }

    /* only keep the guide mapping list
       if we have a mapping */
    if (PFarray_last (new_map_list))
        MAPPING_LIST(n) = new_map_list;
    else
        MAPPING_LIST(n) = NULL;
}

/**
 * @brief Combine the guide mappings with respect
 *        to the intersection operator semantics.
 */
static void
copy_intersect (PFla_op_t  *n)
{
    /* guide mappings to compare */
    PFguide_mapping_t *left_mapping,
                      *right_mapping,
                      *new_mapping;
    /* guides to compare */
    PFguide_tree_t    *left_guide,
                      *right_guide;
    unsigned int       left_count,
                       right_count;

    /* mapping list of left or right child is NULL */
    if (MAPPING_LIST(L(n)) == NULL ||
        MAPPING_LIST(R(n)) == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* calculate intersection between left and right elements */
    for (unsigned int i = 0; i < PFarray_last (MAPPING_LIST(L(n))); i++) {
        /* get left guide_mapping */
        left_mapping = GUIDE_MAP_AT(MAPPING_LIST(L(n)), i);

        assert (left_mapping);

        right_mapping = get_guide_mapping (MAPPING_LIST(R(n)),
                                           left_mapping->column);

        /* we cannot say anything about the guides of this column */
        if (right_mapping == NULL)
            continue;

        /* Create new mapping because left and right mappings are not NULL,
           so at least will be returned a guide mapping with no guide nodes */
        new_mapping = new_guide_mapping (left_mapping->column);
        /* add the new mapping to the guide mapping list */
        MAPPING_LIST(n) = add_guide_mapping (MAPPING_LIST(n), new_mapping);

        left_count  = PFarray_last (left_mapping->guide_list);
        right_count = PFarray_last (right_mapping->guide_list);
        
        /* compute intersection */
        for (unsigned int j = 0; j < left_count; j++) {
            left_guide = GUIDE_AT(left_mapping->guide_list, j);
            for (unsigned int k = 0; k < right_count; k++) {
                right_guide = GUIDE_AT(right_mapping->guide_list, k);
                if (left_guide == right_guide) {
                    add_guide (new_mapping, left_guide);
                    break;
                }
            }
        }
    }
}

/**
 * @brief Combine the guide mappings with respect
 *        to the union operator semantics.
 */
static void
copy_disunion (PFla_op_t  *n)
{
    /* guide mappings to compare */
    PFguide_mapping_t *left_mapping,
                      *right_mapping,
                      *new_mapping;
    unsigned int       left_count,
                       right_count;

    /* mapping list of left or right child is NULL */
    if (MAPPING_LIST(L(n)) == NULL ||
        MAPPING_LIST(R(n)) == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* calculate union between left and right elements */
    for (unsigned int i = 0; i < PFarray_last (MAPPING_LIST(L(n))); i++) {
        /* get left guide_mapping */
        left_mapping = GUIDE_MAP_AT(MAPPING_LIST(L(n)), i);

        assert (left_mapping);

        right_mapping = get_guide_mapping (MAPPING_LIST(R(n)),
                                           left_mapping->column);

        /* we cannot say anything about the guides of this column */
        if (right_mapping == NULL)
            continue;

        left_count  = PFarray_last (left_mapping->guide_list);
        right_count = PFarray_last (right_mapping->guide_list);
        
        /* Create new mapping because left and right mappings are not NULL,
           so at least will be returned a guide mapping with no guide nodes */
        if (left_count == 0)
            new_mapping = copy_guide_mapping (right_mapping);
        else if (right_count == 0)
            new_mapping = copy_guide_mapping (left_mapping);
        else {
            new_mapping = new_guide_mapping (left_mapping->column);

            for (unsigned int j = 0; j < left_count; j++)
                add_guide (new_mapping, GUIDE_AT(left_mapping->guide_list, j));
            for (unsigned int k = 0; k < right_count; k++)
                add_guide (new_mapping, GUIDE_AT(right_mapping->guide_list, k));
        }

        /* add the new mapping to the guide mapping list */
        MAPPING_LIST(n) = add_guide_mapping (MAPPING_LIST(n), new_mapping);
    }
}

/**
 * @brief Apply kind and name test for path steps.
 *
 * @param return_attr ensures that attribute nodes are allowed/discarded,
 *                    if true/false, respectively.
 */
static bool
node_test_ (PFguide_tree_t *n, PFalg_step_spec_t spec, bool return_attr)
{
    assert(n);

    switch (spec.kind) {
        case node_kind_node:
            /* depending on whether attribute nodes may
               be returned or not we discard attribute nodes */
            return return_attr || n->kind != attr;

        case node_kind_doc:
            return n->kind == doc;
            
        case node_kind_elem:
            if (n->kind != elem)
                return false;
            /* cope with wildcards correctly */
            if (!PFQNAME_NS_WILDCARD(spec.qname) &&
                strcmp (PFqname_uri (spec.qname),
                        PFqname_uri (n->name)) != 0)
                return false;
            return PFQNAME_LOC_WILDCARD(spec.qname) ||
                   strcmp (PFqname_loc (spec.qname),
                           PFqname_loc (n->name)) == 0;
            
        case node_kind_attr:
            /* an attribute kind test for axis that cannot
               return attributes is always evaluated to false */
            if (!return_attr)
                return false;
            if (n->kind != attr)
                return false;
            /* cope with wildcards correctly */
            if (!PFQNAME_NS_WILDCARD(spec.qname) &&
                strcmp (PFqname_uri (spec.qname),
                        PFqname_uri (n->name)) != 0)
                return false;
            return PFQNAME_LOC_WILDCARD(spec.qname) ||
                   strcmp (PFqname_loc (spec.qname),
                           PFqname_loc (n->name)) == 0;
            
        case node_kind_text:
            return n->kind == text;
            
        case node_kind_comm:
            return n->kind == comm;
            
        case node_kind_pi:
            if (n->kind != pi)
                return false;
            /* cope with wildcards correctly */
            return PFQNAME_LOC_WILDCARD(spec.qname) ||
                   strcmp (PFqname_loc (spec.qname),
                           PFqname_loc (n->name)) == 0;
    }
    return false;
}
#define node_test(n,s)      node_test_((n),(s),false)
#define node_test_attr(n,s) node_test_((n),(s),true)

/**
 * @brief Collect all ancestor nodes in the guide
 *        that fulfill the node test.
 */
static void
getAncestorFromGuide (PFguide_tree_t *n,
                      PFalg_step_spec_t spec,
                      PFguide_mapping_t *mapping)
{
    PFguide_tree_t *guide = n->parent;

    /* compute ancestors */
    while (guide != NULL) {
        if (node_test (guide, spec))
            add_guide (mapping, guide);
        guide = guide->parent;
    }
}

/**
 * @brief Recursively collect all descendant nodes in the guide
 *        that fulfill the node test.
 */
static void
getDescendantFromGuide (PFguide_tree_t *n,
                        PFalg_step_spec_t spec,
                        PFguide_mapping_t *mapping)
{
    PFarray_t       *children = n->child_list; /* all children of n */
    PFguide_tree_t  *child;                    /* one child element */

    if (children == NULL) return;

    /* collect the children in reverse document order */
    for (unsigned int i = PFarray_last (children); i > 0; i--) {
        child = GUIDE_AT(children, i-1);
        assert (child);
        /* recursively get all descendants */
        getDescendantFromGuide (child, spec, mapping);

        /* add node if the node test succeeds */
        if (node_test (child, spec))
            add_guide (mapping, child);
    }
}

/**
 * @brief Collect a new guide mapping
 *        that represents the result of a path step.
 */
static void
copy_step (PFla_op_t *n)
{
    PFguide_mapping_t *mapping      = NULL, /* guide mapping for column */
                      *new_mapping;
    PFalg_axis_t       axis         = n->sem.step.spec.axis;
    PFalg_att_t        item         = n->sem.step.item,     /* input col */
                       item_res     = n->sem.step.item_res; /* output col */
    PFarray_t         *map_list     = MAPPING_LIST(R(n)),   /* input mapping */
                      *guides       = NULL, /* list of guide nodes */
                      *children     = NULL; /* children of guide nodes */
    unsigned int       guide_count;
    PFguide_tree_t    *guide        = NULL,
                      *child        = NULL;

    /* do not add a guide for the horizontal axes: following,
       preceding, following-sibling, and preceding-sibling */
    if (axis == alg_fol ||
        axis == alg_fol_s ||
        axis == alg_prec ||
        axis == alg_prec_s)
        return;

    /* no guides -> do nothing */
    if (map_list == NULL)
        return;

    /* get the guide mapping for input column */
    mapping = get_guide_mapping (map_list, item);
    if (mapping == NULL)
        return;

    /* we are now sure that we have input guides and
       thus also produce a new guide mapping */
    new_mapping = new_guide_mapping (item_res);
    /* add the new mapping to the guide mapping list */
    MAPPING_LIST(n) = add_guide_mapping (MAPPING_LIST(n), new_mapping);

    /* get array of guide nodes */
    guides = mapping->guide_list;
    if (guides == NULL)
        return; /* return an empty mapping */
    
    /* lookup the number of guides */ 
    guide_count = PFarray_last (guides);
    
    if (guide_count == 0)
        return; /* return an empty mapping */

    /* if axis is NOT the attribute axis or a *self axis,
       but the kind is attribute the guides will be empty */
    if (axis != alg_attr &&
        axis != alg_self && axis != alg_anc_s && axis != alg_desc_s &&
        n->sem.step.spec.kind == node_kind_attr)
        return; /* return an empty mapping */
    
    switch (axis) {
        /* attribute axis */
        case (alg_attr):
        /* child axis */
        case (alg_chld):
            /* child step for all guide nodes */
            for (unsigned int i = 0; i < guide_count; i++) {
                guide    = GUIDE_AT(guides, i);
                children = guide->child_list;

                if (children == NULL)
                    continue;
                
                /* apply node test for all children (and collect children
                   in reverse document order) */
                for (unsigned int j = PFarray_last (children); j > 0; j--) {
                    child = GUIDE_AT(children, j-1);
                    if (node_test_ (child, n->sem.step.spec, axis == alg_attr))
                        add_guide (new_mapping, child);
                }
            }
            break;

        /* self axis */
        case (alg_self):
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                
                /* apply node test for all guides */
                if (node_test_attr (guide, n->sem.step.spec))
                    add_guide (new_mapping, guide);
            }
            break;

        /* ancestor-or-self axis */
        case (alg_anc_s):
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                
                /* apply node test for all guides */
                if (node_test_attr (guide, n->sem.step.spec))
                    add_guide (new_mapping, guide);
            }
            /* NO BREAK! */
        /* ancestor axis */
        case (alg_anc):
            /* find all ancestor */
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                getAncestorFromGuide (guide, n->sem.step.spec, new_mapping);
            }
            break;
            
        /* parent axis */
        case (alg_par):
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                
                if (guide->parent == NULL)
                    break;
                
                if (node_test (guide->parent, n->sem.step.spec))
                    add_guide (new_mapping, guide->parent);
            }
            break;
            
        /* descendant-or-self axis */
        case (alg_desc_s):
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                
                /* apply node test for all guides */
                if (node_test_attr (guide, n->sem.step.spec))
                    add_guide (new_mapping, guide);
            }
            /* NO BREAK! */
        /* descendant axis */
        case (alg_desc):
            for (unsigned int i = 0; i < guide_count; i++) {
                guide = GUIDE_AT(guides, i);
                getDescendantFromGuide (guide, n->sem.step.spec, new_mapping);
            }
            break;
            
        default:
            break;
    }
}

/**
 * @brief Return the already assigned guide mapping
 *        that represents the result of a path step.
 */
static void
copy_guide_step (PFla_op_t *n)
{
    PFguide_mapping_t *mapping = new_guide_mapping (n->sem.step.item_res);
    MAPPING_LIST(n) = add_guide_mapping (MAPPING_LIST(n), mapping);

    /* copy all guides in sem.step.guides to the guide mapping */
    for (unsigned int i = n->sem.step.guide_count; i > 0; i--) {
        add_guide (mapping, n->sem.step.guides[i-1]);
    }
}

/**
 * @brief Assign the first guide to the result column
 *        of the fn:doc() algebra equivalent.
 */
static void
copy_doc_tbl (PFla_op_t *n, PFguide_tree_t *guide)
{
    /* Test if the document name is constant */
    if (PFprop_const_left (n->prop, n->sem.doc_tbl.att) == true) {
        /* value of the document name */
        PFalg_atom_t a = PFprop_const_val_left (PROP(n), n->sem.doc_tbl.att);
        if (a.type == aat_str &&
            /* Is guide filename equal to query filename */
            guide->kind == doc &&
            strcmp (PFqname_loc (guide->name), a.val.str) == 0) {
            PFguide_mapping_t *mapping;
            /* create a new guide mapping */
            mapping = new_guide_mapping (n->sem.doc_tbl.res);
            /* add the guide to the mapping */
            add_guide (mapping, guide);
            /* add the mapping to the guide mapping list */
            MAPPING_LIST(n) = add_guide_mapping (MAPPING_LIST(n), mapping);
        }
    }
}

/* Infer guide property; worker for prop_infer(). */
static void
infer_guide (PFla_op_t *n, PFguide_tree_t *guide)
{
    assert (n);
    assert (n->prop);
    switch (n->kind) {
        /* copy R */
        case la_serialize_seq:
        case la_doc_index_join:
        case la_doc_access:
            /* Deep copy of right children guide_mapping_list */
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(R(n)));
            break;

        /* copy L */
        case la_serialize_rel:
        case la_attach:
        case la_semijoin:
        case la_select:
        case la_pos_select:
        case la_difference:
        case la_distinct:
        case la_fun_1to1:
        case la_num_eq:
        case la_num_gt:
        case la_bool_and:
        case la_bool_or:
        case la_bool_not:
        case la_to:
        case la_rownum:
        case la_rowrank:
        case la_rank:
        case la_rowid:
        case la_type:
        case la_type_assert:
        case la_cast:
        case la_roots:
        case la_error:
        case la_cond_err:
        case la_trace:
        case la_trace_msg:
        case la_trace_map:
        case la_proxy:
        case la_proxy_base:
        case la_dummy:
            /* Deep copy of left children guide_mapping_list */
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(L(n)));
            break;

        /* do nothing */
        case la_lit_tbl:
        case la_empty_tbl:
        case la_ref_tbl:
        case la_avg:
        case la_max:
        case la_min:
        case la_sum:
        case la_count:
        case la_seqty1:
        case la_all:
        case la_twig:
        case la_fcns:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_comment:
        case la_processi:
        case la_docnode:
        case la_content:
        case la_merge_adjacent:
        case la_fragment:
        case la_frag_extract:
        case la_frag_union:
        case la_empty_frag:
        case la_rec_fix:
        case la_rec_param:
        case la_rec_arg:
        case la_rec_base:
        case la_fun_call:
        case la_fun_param:
        case la_fun_frag_param:
        case la_cross_mvd:
        case la_eqjoin_unq:
        case la_string_join:
        case la_nil:
            break;

        /* copy L+R*/
        case la_cross:
        case la_eqjoin:
        case la_thetajoin:
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(L(n)));
            if (MAPPING_LIST(R(n))) {
                if (MAPPING_LIST(n))
                    MAPPING_LIST(n) = PFarray_concat (MAPPING_LIST(n),
                                                      MAPPING_LIST(R(n)));
                else
                    MAPPING_LIST(n) = deep_copy_guide_mapping_list (
                                          MAPPING_LIST(R(n)));
            }
            break;

        /* project */
        case la_project:
            if (MAPPING_LIST(L(n)) == NULL)
                MAPPING_LIST(n) = NULL;
            else
                copy_project (n);
            break;

        /* union */
        case la_disjunion:
            copy_disunion (n);
            break;

        /* intersect */
        case la_intersect:
            copy_intersect (n);
            break;

        /* step */
        case la_step:
            copy_step (n);
            break;

        /* step_join */
        case la_step_join:
            /* Deep copy of right children guide_mapping_list */
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(R(n)));
            
            copy_step (n);
            break;

        case la_guide_step:
            copy_guide_step (n);
            break;
            
        case la_guide_step_join:
            /* Deep copy of right children guide_mapping_list */
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(R(n)));
            
            copy_guide_step (n);
            break;

        /* create guide nodes */
        case la_doc_tbl:
            /* Deep copy of left children guide_mapping_list */
            MAPPING_LIST(n) = deep_copy_guide_mapping_list (MAPPING_LIST(L(n)));
            
            copy_doc_tbl (n, guide);
            break;
    }
}

/* worker for PFprop_infer_guide */
static void
prop_infer (PFla_op_t *n, PFguide_tree_t *guide)
{
    /* no guide tree -> do nothing */
    if (guide == NULL)
        return;

    assert(n);
    assert(PROP(n));

    if (SEEN(n))
        return;

    /* calculate guide nodes for all operators */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer (n->child[i], guide);

    SEEN(n) = true;

    /* Initialize guide nodes */
    if (MAPPING_LIST(n))
        PFarray_last (MAPPING_LIST(n)) = 0;
    else
        MAPPING_LIST(n) = NULL;

    infer_guide (n, guide);

    return;
}

/**
 * @brief Infer guide property for a DAG rooted in root.
 */
void
PFprop_infer_guide (PFla_op_t *root, PFguide_tree_t *guide)
{
    if (guide == NULL)
        return;

    /* infer constant columns
       to lookup constant document names */
    PFprop_infer_const (root);

    prop_infer (root, guide);

    PFla_dag_reset (root);
}


/**
 * @brief Return if the column @a column has a guide mapping.
 */
bool
PFprop_guide (PFprop_t *prop, PFalg_att_t column)
{
    assert(prop);

    if (prop->guide_mapping_list == NULL) return false;

    return (get_guide_mapping (prop->guide_mapping_list, column) != NULL);
}

/**
 * @brief Return how many guide nodes are assigned to the guide
 *        mapping of column @a column.
 */
unsigned int
PFprop_guide_count (PFprop_t *prop, PFalg_att_t column)
{
    PFguide_mapping_t *mapping;

    assert(prop);

    if (prop->guide_mapping_list == NULL) return 0;

    /* get guide_mapping to column */
    mapping = get_guide_mapping (prop->guide_mapping_list, column);
    
    /* get guide_mapping */
    if (mapping == NULL) return 0;

    return PFarray_last (mapping->guide_list);
}

/**
 * @brief Return the guides of the column @a colum
 *        as an array of PFguide_tree_t pointers.
 */
PFguide_tree_t **
PFprop_guide_elements (PFprop_t *prop, PFalg_att_t column)
{
    PFguide_mapping_t *mapping;
    PFguide_tree_t   **ret      = NULL;
    unsigned int       count, i;
    int                index;

    assert(prop);

    if (prop->guide_mapping_list == NULL) return NULL;

    /* get guide_mapping to column */
    mapping = get_guide_mapping (prop->guide_mapping_list, column);
    
    /* get guide_mapping */
    if (mapping == NULL) return NULL;

    count = PFarray_last (mapping->guide_list);

    /* allocate memory and copy guide nodes */
    ret = (PFguide_tree_t **) PFmalloc (count * sizeof (PFguide_tree_t *));

    index = mapping->first_guide;
    i     = 0;

    /* return the guides in guide id order */
    while (index >= 0) {
        assert (index < (int) PFarray_last (mapping->guide_order));
        ret[i++] = GUIDE_AT(mapping->guide_list, index);
        index    = GUIDE_IDX_AT(mapping->guide_order, index);
    }

    return ret;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
