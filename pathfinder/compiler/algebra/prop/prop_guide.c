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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */


#include "pathfinder.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "properties.h"
#include "alg_dag.h"
#include "subtyping.h"

#define SEEN(n) ((n)->bit_dag)
/* left and right child*/
#define L(n) ((n)->child[0])
#define R(n) ((n)->child[1])
/* prop of n */
#define PROP(n) ((n)->prop)
/* guide_mapping_list of n */
#define MAPPING_LIST(n) ((n)->prop->guide_mapping_list)

/* Dummy namespace for the guide */
PFns_t PFns_guide =
    { .prefix = "",
      .uri    = "" };

/* get the corresponding PFguide_mapping_t element from @a guide_mapping_list
 * where the columns are equal */
static PFguide_mapping_t *get_guide_mapping(PFarray_t *guide_mapping_list,
        PFalg_att_t column);

/* Copy @a guide_mapping element */
static PFguide_mapping_t *copy_guide_mapping(
        PFguide_mapping_t *guide_mapping);

/* Deep copy of @a guide_mapping_list */
static PFarray_t *deep_copy_guide_mapping_list(
        PFarray_t *guide_mapping_list);

/* the elements of @a list2 will be copied in @a list1  */
static PFarray_t *merge_guide_mapping_list(PFarray_t *list1, PFarray_t *list2);

/* Create a PFty_t from a guide element */
static PFty_t  create_PFty_t(PFguide_tree_t *n);

/* Returns all ancestor of @a n */
static PFarray_t *getAncestorFromGuide(PFguide_tree_t *n, PFty_t type);

/* Get recursive all descendant elements */
static PFarray_t *getDescendantFromGuideRec(PFguide_tree_t *n, PFty_t type,
    PFarray_t *descendant);

/* Returns all descendant of @a n if they are a suptype of @a type */
static PFarray_t *getDescendantFromGuide(PFguide_tree_t *n, PFty_t type);

/* copy guide nodes from left child guide_mapping_list to @a n */
static void copyL(PFla_op_t *n);

/* copy guide nodes from right child guide_mapping_list to @a n */
static void copyR(PFla_op_t *n);

/* copy guide nodes from left and right child guide_mapping_list to @a n */
static void copyLR(PFla_op_t *n);

/* add a PFguide_mapping_t to a @guide_mapping_list  if the @a column
 * do not exist, otherwise it will be added only @a guide in the
 * corresponding PFguide_mapping_t in @guide_mapping_list */
static PFarray_t* add_guide(PFarray_t *guide_mapping_list, 
        PFguide_tree_t  *guide, PFalg_att_t column);

/* initialize the guide property in @a n */
static void copy_doc_tbl(PFla_op_t *n, PFguide_tree_t *guide);

/* Copy guides for the project operator */
static void copy_project(PFla_op_t *n);

/* Remove duplicate guides from array */
static void remove_duplicate(PFla_op_t *n);

/* Copy guides for the step operator*/
static void copy_step(PFla_op_t *n);

/* copy all guides from @a n->sem.step.guides to @a n->prop->guide_list */
static void copy_guide_step(PFla_op_t *n);

/* Copy guides for the step operator*/
static void copy_dup_step(PFla_op_t *n);

/* Union between left and right child guide nodes */
static void copy_disunion(PFla_op_t  *n);

/* Intersect between left and right child guide nodes */
static void copy_intersect(PFla_op_t  *n);

/* Infer domain properties; worker for prop_infer(). */
static void infer_guide(PFla_op_t *n, PFguide_tree_t *guide);


/* ++++++++++++++++++++++++++++++++++++ */

/* get the corresponding PFguide_mapping_t element from @a guide_mapping_list
 * where the columns are equal */
static PFguide_mapping_t *
get_guide_mapping(PFarray_t *guide_mapping_list, PFalg_att_t column)
{
    if(guide_mapping_list == NULL)
        return NULL;

    PFguide_mapping_t *guide_mapping = NULL;

    /* serch matching guide_mapping of column*/
    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        /* get element from PFarray_t */
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                guide_mapping_list, i));

        if(column == guide_mapping->column) {
            return guide_mapping;
        }
    }

    return NULL;
}

/* Copy @a guide_mapping element */
static PFguide_mapping_t*
copy_guide_mapping(PFguide_mapping_t *guide_mapping)
{
    if(guide_mapping == NULL)
        return NULL;

    /* PFguide_tree_t elements to be copied */
    PFguide_tree_t *guide_tree_element = NULL;
    /* Create new PFguide_mapping_t */
    PFguide_mapping_t *new_guide_mapping = (PFguide_mapping_t*) 
            PFmalloc(sizeof(PFguide_mapping_t));

    /* initialize new PF_guide_mapping element */
    *new_guide_mapping = (PFguide_mapping_t) {
        .column = guide_mapping->column,
        .guide_list = PFarray(sizeof(PFguide_tree_t**)),
    };

    /* insert all PFguide_tree_t** elements in the new list */
    for(unsigned int i = 0; i < PFarray_last(guide_mapping->guide_list);i++) {
        guide_tree_element = *((PFguide_tree_t**)
                PFarray_at(guide_mapping->guide_list, i));

        *((PFguide_tree_t**) PFarray_add(new_guide_mapping->guide_list)) = 
                guide_tree_element;
    }

    return new_guide_mapping;
}

/* Deep copy of @a guide_mapping_list */
static PFarray_t *
deep_copy_guide_mapping_list(PFarray_t *guide_mapping_list)
{
    if(guide_mapping_list == NULL)
        return NULL;

    /* new list that will be the copy of guide_mapping list */
    PFarray_t *new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
    /* PFguide_mapping_t elements to copy */
    PFguide_mapping_t *guide_mapping_element = NULL;

    /* make a copy of all PFguide_mapping_t elements of the 
     * guide_mapping_list */
    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        guide_mapping_element = *((PFguide_mapping_t**)
                (PFarray_at(guide_mapping_list, i)));

        *((PFguide_mapping_t**)PFarray_add(new_guide_mapping_list)) = 
                copy_guide_mapping(guide_mapping_element); 
    }

    return new_guide_mapping_list;
}

/* the elements of @a list1 and @a list2 will be merged without duple elimination  */
static PFarray_t *merge_guide_mapping_list(PFarray_t *list1, PFarray_t *list2)
{
    if(list1 == NULL) {
        return list2;
    }

    if(list2 == NULL)
        return list1;

    PFguide_mapping_t *guide_mapping = NULL;

    /* copy the elements */
    for(unsigned int i = 0; i < PFarray_last(list2); i++) {
        guide_mapping = *((PFguide_mapping_t**)PFarray_at(list2, i));

        *((PFguide_mapping_t**) PFarray_add(list1)) = guide_mapping;
    }

    return list1;
}

/* Create a PFty_t type from a guide node */
static PFty_t
create_PFty_t(PFguide_tree_t *n)
{

    assert(n);

    PFty_t  ret;
    switch(n->kind) {
        case(elem):
            ret = PFty_elem(PFqname(PFns_guide, n->tag_name),
                PFty_xs_anyType());
            break;
        case(attr):
            ret = PFty_attr(PFqname(PFns_guide, n->tag_name),
                PFty_star(PFty_atomic()));
            break;
        case(text):
            ret = PFty_text();
            break;
        case(comm):
            ret = PFty_comm();
            break;
        case(pi):
            ret = PFty_pi(NULL);
            break;
        case(doc):
            ret = PFty_doc(PFty_xs_anyNode());
    }
    return ret;
}

/* return the ancestors of @a n */
static PFarray_t *
getAncestorFromGuide(PFguide_tree_t *n, PFty_t type)
{
    assert(n);

    PFarray_t  *parents = PFarray(sizeof(PFguide_tree_t**));
    PFguide_tree_t  *node = n->parent;

    /* compute ancestors */
    while(node != NULL) {
         if(PFty_subtype(create_PFty_t(node), type)) {
            *(PFguide_tree_t**) PFarray_add(parents) = node;
        }
        node = node->parent;
    }

    return parents;
}

/* Get recursive all descendant elements that are subtypes
 * of @type */
static PFarray_t *
getDescendantFromGuideRec(PFguide_tree_t *n, PFty_t type,
    PFarray_t *descendant)
{
    if(n == NULL)
        return NULL;

    PFarray_t  *childs = n->child_list; /* all childs of n */
    PFguide_tree_t  *element = NULL;    /* one child element */
    if(childs == NULL)
        return NULL;

    /* loop over all child element of @a n */
    for(unsigned int i = 0; i < PFarray_last(childs); i++) {
        element = *((PFguide_tree_t**) PFarray_at(childs, i));
        /* get recursive all descendant element */
        getDescendantFromGuideRec(element, type, descendant); 
        /* add element if it is a subtype of @a type*/
        if(PFty_subtype(create_PFty_t(element),type)) {
            *((PFguide_tree_t**) PFarray_add(descendant)) = element;
        }
    }

    return descendant;
}

/* Returns all descendant of @a n if they are a suptype of @a type */
static PFarray_t *
getDescendantFromGuide(PFguide_tree_t *n, PFty_t type)
{
    PFarray_t  *descendant =  PFarray(sizeof(PFguide_tree_t**));

    PFarray_t *ret =  getDescendantFromGuideRec(n, type, descendant);
    return ret;
}

/* copy guide nodes from left and right child guide_mapping_list to @a n */
static void
copyL(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));
    assert(L(n));
    assert(PROP(L(n)));

    /* Deep copy of left childs guide_mapping_list */
    MAPPING_LIST(n) = deep_copy_guide_mapping_list(MAPPING_LIST(L(n)));

    return;
}

/* copy guide nodes from right child guide_mapping_list to @a n */
static void
copyR(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));
    assert(R(n));
    assert(PROP(R(n)));

    /* Deep copy of right childs guide_mapping_list */
    MAPPING_LIST(n) = deep_copy_guide_mapping_list(MAPPING_LIST(R(n)));

    return;
}

/* copy guide nodes from left and right child guide_mapping_list to @a n */
static void copyLR(PFla_op_t *n)
{
    PFarray_t *left_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(L(n)));

    PFarray_t *right_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(R(n)));

    /* merge the two child lists */
    left_child_guide_mapping_list = merge_guide_mapping_list(left_child_guide_mapping_list,
            right_child_guide_mapping_list);

    MAPPING_LIST(n) = left_child_guide_mapping_list;

    return;
}

/* add a PFguide_mapping_t to a @guide_mapping_list if the @a column
 * do not exist, otherwise it will be added only @a guide in the
 * corresponding PFguide_mapping_t in @guide_mapping_list */
static PFarray_t*
add_guide(PFarray_t *guide_mapping_list, PFguide_tree_t  *guide,
        PFalg_att_t column)
{
    PFguide_mapping_t *guide_mapping = NULL;

    /* Create new array of PFguide_mapping_t*/
    if(guide_mapping_list == NULL)
        guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));

    /* look up if the column just exist */
    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        /* get element from PFarray_t */
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                guide_mapping_list, i));

        if(guide_mapping == NULL)
            continue;

        /* if guide_mapping exist add the guide */
        if(column == guide_mapping->column) {
            if(guide != NULL)
                *((PFguide_tree_t**) PFarray_add(
                        guide_mapping->guide_list)) = guide;

            return guide_mapping_list;
        }
    }

    /* Create new PFguide_mapping_t */
    guide_mapping = (PFguide_mapping_t*)PFmalloc(sizeof(PFguide_mapping_t));
    *guide_mapping = (PFguide_mapping_t) {
        .column = column,
        .guide_list = PFarray(sizeof(PFguide_tree_t**)),
    };

    /* insert values in PFguide_mapping_t */
    if(guide != NULL)
        *(PFguide_tree_t**) PFarray_add(guide_mapping->guide_list) = guide;

    /* insert PFguide_mapping_t in PFarray_t */
    *(PFguide_mapping_t**) PFarray_add(guide_mapping_list) = guide_mapping;

    return guide_mapping_list;
}



/* initialize the guide property in @a n */
static void
copy_doc_tbl(PFla_op_t *n, PFguide_tree_t *guide)
{
    assert(n);
    assert(PROP(n));

    /* Test if the document name is constant */
    if(PFprop_const_left (n->prop, n->sem.doc_tbl.item) == true) {
        /* value of the document name */
        PFalg_atom_t a = PFprop_const_val_left (PROP(n),
            n->sem.doc_tbl.item);
        if(a.type == aat_str) {
            /* Is guide filename equal to qurey filename */
            if(strcmp(guide->tag_name,a.val.str) == 0) {
                /* add the guide to the guide_list */
                MAPPING_LIST(n) = add_guide(MAPPING_LIST(n), guide, n->sem.doc_tbl.item);
            } else {
                MAPPING_LIST(n) = NULL;
            }
        }
    }
}

/* copy guide nodes for project operator*/
static void
copy_project(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));
    assert(L(n));
    assert(PROP(L(n)));

    PFarray_t *guide_mapping_list = MAPPING_LIST(L(n)),
            *new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
    PFguide_mapping_t *guide_mapping = NULL;
    PFalg_att_t new_column, old_column;

    if(guide_mapping_list == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* iterate over all columns */
    for (unsigned int i = 0; i < n->sem.proj.count; i++) {
        /* get new and old column name */
        new_column = n->sem.proj.items[i].new;
        old_column = n->sem.proj.items[i].old;

        /* get guide mapping from list */
        guide_mapping = get_guide_mapping(guide_mapping_list, old_column);

        if(guide_mapping == NULL)
            continue;

        guide_mapping = copy_guide_mapping(guide_mapping);

        /* set new column name */
        guide_mapping->column = new_column;

        /* assign guide mapping to the list */
        *((PFguide_mapping_t**) PFarray_add(new_guide_mapping_list)) =
                guide_mapping;
    }

    MAPPING_LIST(n) = new_guide_mapping_list;

    return;
}

/* Remove duplicate guides from array */
static void
remove_duplicate(PFla_op_t *n)
{

    assert(n);
    assert(n->prop);

    PFarray_t *guide_mapping_list = n->prop->guide_mapping_list;
    PFguide_mapping_t  *guide_mapping = NULL;
    PFarray_t *guide_list = NULL;
    /* array without duplicate elements*/
    PFarray_t  *new_guide_list = PFarray(sizeof(PFguide_tree_t**));
    PFguide_tree_t  *element = NULL, *element2 = NULL;
    bool add_element_bool = true; /* if true add the guide */

    /* no elements -> do nothing */
    if(guide_mapping_list == NULL)
        return;

    /* remove duplicate element from all guide mappings */
    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                guide_mapping_list, i));

        if(guide_mapping == NULL)
            continue;

        guide_list = guide_mapping->guide_list;
        if(guide_list == NULL)
            continue;

        /* remove the duplicate guide nodes */
        for(unsigned int j = 0; j < PFarray_last(guide_list); j++) {
            element = *((PFguide_tree_t**) PFarray_at(guide_list, j));
            add_element_bool = true;
            for(unsigned int k = 0; k < PFarray_last(new_guide_list); k++) {
                element2 = *((PFguide_tree_t**) PFarray_at(new_guide_list,k));
                if(element->guide == element2->guide) {
                    add_element_bool = false;
                    break;
                }
            }
            if(add_element_bool)
                *(PFguide_tree_t**) PFarray_add(new_guide_list) = element;
        }
        guide_mapping->guide_list = new_guide_list;
    }
    return;
}


/* Stair case join / pathstep */
static void
copy_step(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));
    assert(R(n));
    assert(PROP(R(n)));

    PFalg_axis_t  axis = n->sem.step.axis; /* axis step */
    PFarray_t *guide_mapping_list = MAPPING_LIST(R(n));

    PFalg_att_t column_in = n->sem.step.item,  /* input column */
            column_out = n->sem.step.item_res; /* output column */

    PFguide_mapping_t *guide_mapping = NULL;    /* guide mapping for column */
    PFarray_t *guide_list = NULL;
    PFguide_tree_t  *element = NULL, *element2 = NULL, *child_element = NULL;
    PFarray_t       *new_guide_mapping_list = NULL;   /* return array */
    PFarray_t       *childs = NULL; /* childs of guide nodes */
    PFarray_t       *help_array = NULL;

    /* delete guide for axis following/preceding
        following-sibling/preceding-sibling */
    if(axis == alg_fol || axis == alg_fol_s ||
            axis == alg_prec || axis == alg_prec_s) {

        MAPPING_LIST(n) = NULL;
        return;
    }

    /* no guides -> do nothing */
    if(guide_mapping_list == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* get guide_mapping for input column */
    guide_mapping = get_guide_mapping(guide_mapping_list, column_in);
    if(guide_mapping == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }

    /* get array of guide nodes */
    guide_list = guide_mapping->guide_list;
    if(guide_list == NULL) {
        MAPPING_LIST(n) = PFarray(sizeof(PFguide_mapping_t**));;
        return;
    }

    /* if axis is NOT the attribute axis, but the ty is ty_attr
     * the guides will be emty */
    if(axis != alg_attr) {
        if(n->sem.step.ty.type == ty_attr) {
            new_guide_mapping_list = add_guide(new_guide_mapping_list, 
                    child_element, column_out);          
            MAPPING_LIST(n) = new_guide_mapping_list;
            return;
        }
    } 
    switch(axis) {
        case(alg_chld):
        /* child axis */
            /* child step for all guide nodes */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                childs = element->child_list;
                if(childs == NULL)
                    continue;
                /* subtype check for all childs */
                for(unsigned int j = 0; j < PFarray_last(childs); j++) {
                    child_element= *((PFguide_tree_t**) PFarray_at(childs,j));
                    if(PFty_subtype(
                            create_PFty_t(child_element),
                            n->sem.step.ty)) {
                        /* add child */
                        new_guide_mapping_list = add_guide(new_guide_mapping_list, child_element, column_out);
                    }
                }
            }
            break;

        case(alg_self):
        /* self axis */

            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                /* subtype check for all childs */
                if(PFty_subtype(create_PFty_t(element),
                        n->sem.step.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(new_guide_mapping_list, element, column_out);
                }
            }
            break;

        case(alg_anc_s):
        /* ancestor-or-self axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                /* subtype check for all childs */
                if(PFty_subtype(create_PFty_t(element),
                        n->sem.step.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(new_guide_mapping_list, element, column_out);
                }
            }
            /* NO BREAK! */
        case(alg_anc):
        /* ancestor axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                /* find all ancestor */
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                help_array = getAncestorFromGuide(element, n->sem.step.ty);
                if(help_array == NULL)
                    continue;
                /* add ancestor to return array */
                for(unsigned int k = 0; k < PFarray_last(help_array); k++) {
                    element2= *((PFguide_tree_t**) PFarray_at(help_array, k));
                    if(PFty_subtype(create_PFty_t(element2),
                            n->sem.step.ty)) {
                        new_guide_mapping_list = add_guide(new_guide_mapping_list, element2, column_out);
                    }
                }
            }
            break;
      case(alg_par):
      /* parent axis */
           for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                if(element->parent == NULL)
                    break;
                if(PFty_subtype(create_PFty_t(element->parent),
                            n->sem.step.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(new_guide_mapping_list, element->parent,
                            column_out);
                }
            }
            break;
        case(alg_desc_s):
        /* descendant-or-self axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                if(PFty_subtype(create_PFty_t(element),
                        n->sem.step.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(new_guide_mapping_list, element, column_out);
                }
            }
            /* NO BREAK! */
        case(alg_desc):
        /* descendant axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                help_array = getDescendantFromGuide(element,n->sem.step.ty);
                if(help_array == NULL)
                    continue;
                /* add descendant to return array */
                for(unsigned int k = 0; k < PFarray_last(help_array); k++) {
                    element2= *((PFguide_tree_t**) PFarray_at(help_array, k));
                    if(PFty_promotable(create_PFty_t(element2),
                            n->sem.step.ty)) {
                        new_guide_mapping_list = add_guide(new_guide_mapping_list, element2, column_out);
                    }
                }
            }
            break;
        case(alg_attr):
        /* attribute axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                childs = element->child_list;
                if(childs == NULL)
                    continue;  
                /* subtype check for all childs */
                for(unsigned int j = 0; j < PFarray_last(childs); j++) {
                    child_element= *((PFguide_tree_t**) PFarray_at(childs,j));
                    if(PFty_subtype(
                            create_PFty_t(child_element),
                            n->sem.step.ty)) {
                        /* add guide nodes to return array */
                        new_guide_mapping_list = add_guide(
                        new_guide_mapping_list, child_element, column_out);
                    }
                }
            }
            break;
        default:
            break;
    }

    MAPPING_LIST(n) = new_guide_mapping_list;
    return;
}

/* copy all guides from @a n->sem.step.guides to @a n->prop->guide_list */
static void
copy_guide_step(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));

    PFarray_t *new_guide_mapping_list = NULL;

    /* copy all guides in sem.step.guides to prop->guide_list */
    for(unsigned int i = 0; i < n->sem.step.guide_count; i++) {
        new_guide_mapping_list = add_guide(new_guide_mapping_list, 
                n->sem.step.guides[i], n->sem.step.item_res);
    }
    
    MAPPING_LIST(n) = new_guide_mapping_list;
    return;
}



/* Intersect between left and right child guide nodes */
static void
copy_intersect(PFla_op_t  *n)
{
    assert(n);
    assert(PROP(n));
    assert(L(n));
    assert(PROP(L(n)));
    assert(R(n));
    assert(PROP(R(n)));

    /* guide mapping list  of left child */
    PFarray_t *left_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(L(n)));
    /* guide mapping list of right child */
    PFarray_t *right_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(R(n)));
    /* intersection of left and right elements */
    PFarray_t *new_guide_mapping_list = NULL;
    /* guide mappings to compare */
    PFguide_mapping_t *left_guide_mapping = NULL, 
            *right_guide_mapping = NULL;
    /* guide lists to compare */
    PFguide_tree_t *left_guide = NULL, *right_guide = NULL;

    /* mapping list of left child is NULL */
    if(left_child_guide_mapping_list == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
     }

    /* mapping list of right child is NULL */
    if(right_child_guide_mapping_list == NULL) {
        MAPPING_LIST(n) = NULL;
        return;
    }
    
    /* calculate intersect between left and right elements */
    for(unsigned int i = 0; i < PFarray_last(left_child_guide_mapping_list);
            i++) {
        /* get left guide_mapping */
        left_guide_mapping = *((PFguide_mapping_t**)PFarray_at(
                left_child_guide_mapping_list, i));

        if(left_guide_mapping == NULL)
            continue;

        right_guide_mapping = get_guide_mapping(
                right_child_guide_mapping_list, left_guide_mapping->column);

        if(right_guide_mapping == NULL)
            continue;

        /* create new guide_mapping because left and right mappings
         * are not NULL, so at least will be returned a guide mapping
         * with no guide nodes */
        new_guide_mapping_list = add_guide(new_guide_mapping_list, NULL, left_guide_mapping->column);

        /* compute intersect */
        for(unsigned int i = 0; i < PFarray_last(
                left_guide_mapping->guide_list); i++) {
            left_guide = *((PFguide_tree_t**) PFarray_at(
                    left_guide_mapping->guide_list, i));

            if(left_guide != NULL) {
                for(unsigned int j = 0; j < PFarray_last(
                        right_guide_mapping->guide_list); j++) {
                    right_guide = *((PFguide_tree_t**) PFarray_at(
                            right_guide_mapping->guide_list, j));

                    if(right_guide != NULL) {
                        if(left_guide->guide == right_guide->guide)
                            new_guide_mapping_list = add_guide(new_guide_mapping_list, left_guide, 
                                    left_guide_mapping->column);
                    }

                }
            }
        }
    }

    MAPPING_LIST(n) = new_guide_mapping_list;
    return; 
}

/* Copy guides for the dup_step operator*/
static void copy_dup_step(PFla_op_t *n)
{
    assert(n);
    assert(PROP(n));
    assert(R(n));
    assert(PROP(R(n)));

    /* Deep copy of right childs guide_mapping_list */
    PFarray_t *right_guide_mapping_list = 
            deep_copy_guide_mapping_list(MAPPING_LIST(R(n)));

    /* step on n */
    copy_step(n);

    /* concat the results */
    MAPPING_LIST(n) = merge_guide_mapping_list(MAPPING_LIST(n), 
            right_guide_mapping_list);

    return;
}



/* Union between left and right child guide nodes */
static void
copy_disunion(PFla_op_t  *n)
{
    assert(n);
    assert(PROP(n));
    assert(L(n));
    assert(PROP(L(n)));
    assert(R(n));
    assert(PROP(R(n)));

    /* guide mapping list  of left child */
    PFarray_t *left_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(L(n)));
    /* guide mapping list of right child */
    PFarray_t *right_child_guide_mapping_list = deep_copy_guide_mapping_list(
            MAPPING_LIST(R(n)));
    /* intersection of left and right elements */
    PFarray_t *new_guide_mapping_list = NULL;
    /* guide mappings to compare */
    PFguide_mapping_t *left_guide_mapping = NULL,
            *right_guide_mapping = NULL;
    /* guide lists to compare */
    PFguide_tree_t *left_guide = NULL, *right_guide = NULL;
    /* is the guide node in the list */
    bool guide_node_in_list = false;

    /* mapping list of left child is NULL */
    if(left_child_guide_mapping_list == NULL) {
        MAPPING_LIST(n) = right_child_guide_mapping_list;
        return;
    }

    /* mapping list of right child is NULL */
    if(right_child_guide_mapping_list == NULL) {
        MAPPING_LIST(n) = left_child_guide_mapping_list;
        return;
    }

    /* calculate union between left and right elements */
    for(unsigned int i = 0; i < PFarray_last(left_child_guide_mapping_list);
            i++) {
        /* get left guide_mapping */
        left_guide_mapping = *((PFguide_mapping_t**)PFarray_at(
                left_child_guide_mapping_list, i));

        if(left_guide_mapping == NULL)
            continue;

        /* get corresponding right guide mapping to left guide mapping */
        right_guide_mapping = get_guide_mapping(
                right_child_guide_mapping_list, left_guide_mapping->column);

        if(right_guide_mapping == NULL)
            continue;

        /* create new guide_mapping because left and right mappings
         * are not NULL, so at least will be returned a guide mapping
         * with no guide nodes */
        new_guide_mapping_list = add_guide(new_guide_mapping_list, NULL,
                left_guide_mapping->column);

        /* add guide_nodes from the left child  */
        for(unsigned int j = 0; j < PFarray_last(
                left_guide_mapping->guide_list); j++) {
            left_guide = *((PFguide_tree_t**) PFarray_at(
                    left_guide_mapping->guide_list, j));

            new_guide_mapping_list = add_guide(new_guide_mapping_list, 
                    left_guide, left_guide_mapping->column);
        }

        /* add all lacking elements from right child */
        for(unsigned int k = 0; k < PFarray_last(
                right_guide_mapping->guide_list); k++) {
            right_guide = *((PFguide_tree_t**) PFarray_at(
                    right_guide_mapping->guide_list, k));
            if(right_guide != NULL) {
                guide_node_in_list = false;
                for(unsigned int l = 0; l < PFarray_last(
                        left_guide_mapping->guide_list); l++) {
                    left_guide = *((PFguide_tree_t**) PFarray_at(
                            left_guide_mapping->guide_list, l));
                    if(left_guide != NULL) {
                        if(left_guide->guide == right_guide->guide)
                            guide_node_in_list = true;

                    }
                }
                /* insert element in list */
                if(guide_node_in_list == false)                  
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, right_guide,
                            left_guide_mapping->column);
            }
        }
    }
    MAPPING_LIST(n) = new_guide_mapping_list;
    return;
}






/* Infer domain properties; worker for prop_infer(). */
static void
infer_guide(PFla_op_t *n, PFguide_tree_t *guide)
{
    assert(n);
    assert(n->prop);
    switch (n->kind) {
        /* copyR */ 
        case la_serialize:
        case la_doc_access: 
            copyR(n);
            break;

        /* do nothing */
        case la_lit_tbl: 
        case la_empty_tbl: 
        case la_to:
        case la_avg: 
        case la_max: 
        case la_min: 
        case la_sum: 
        case la_count: 
        case la_seqty1: 
        case la_all: 
        case la_id:
        case la_idref:
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
        case la_frag_union: 
        case la_empty_frag: 
        case la_rec_fix: 
        case la_rec_param: 
        case la_rec_arg: 
        case la_rec_base: 
        case la_cross_mvd: 
        case la_eqjoin_unq: 
        case la_string_join: 
        case la_nil: 
            break;

        /* copyL */
        case la_attach: 
        case la_semijoin: 
        case la_select: 
        case la_difference: 
        case la_distinct: 
        case la_fun_1to1: 
        case la_num_eq: 
        case la_num_gt: 
        case la_bool_and: 
        case la_bool_or: 
        case la_bool_not: 
        case la_rownum: 
        case la_rank:
        case la_number: 
        case la_type: 
        case la_type_assert: 
        case la_cast: 
        case la_roots: 
        case la_cond_err: 
        case la_trace: 
        case la_trace_msg: 
        case la_trace_map: 
        case la_proxy: 
        case la_proxy_base: 
        case la_dummy: 
            copyL(n);
            break;

        /* copyL+R*/
        case la_cross: 
        case la_eqjoin: 
        case la_thetajoin: 
            copyLR(n); 
            break;

        /* project */
        case la_project:
            copy_project(n);
            break;

        /* union */
        case la_disjunion: 
            copy_disunion(n);
            break;

        /* intersect */
        case la_intersect: 
            copy_intersect(n);

            remove_duplicate(n);
            break;
        
        /* step */
        case la_step: 
            copy_step(n);
            remove_duplicate(n);
            break;

        /* dup_step */
        case la_dup_step: 
            copy_dup_step(n);
            break;

        case la_guide_step:
            copy_guide_step(n);
            break;

        /* create guide nodes */
        case la_doc_tbl: 
            copy_doc_tbl(n, guide);
            break;
    }
    return;
}

/* worker for PFprop_infer_guide */
static void
prop_infer(PFla_op_t *n, PFguide_tree_t *guide)
{
    /* no guide tree -> do nothing */
    if (guide == NULL)
        return;

    assert(n);
    assert(PROP(n));

    if(SEEN(n))
        return;

    /* Set guide nodes to NULL  */
    MAPPING_LIST(n) = NULL;

    /* calculate guide nodes for all operators */    
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && n->child[i]; i++)
        prop_infer(n->child[i], guide);

    SEEN(n) = true;

    infer_guide(n, guide);

    return;
}


/**
 * Infer guide property for a DAG rooted in root
 */
void
PFprop_infer_guide(PFla_op_t *root, PFguide_tree_t *guide)
{
    /* infer constant columns
       to lookup constant document names */
    PFprop_infer_const (root);

    prop_infer(root, guide);

    PFla_dag_reset(root);
}


/* Return if the property has guide nodes  */
bool 
PFprop_guide(PFprop_t *prop, PFalg_att_t column)
{
    assert(prop);
    
    if(prop->guide_mapping_list == NULL)
        return false;

    /* get guide_mapping to column */
    PFguide_mapping_t *guide_mapping = get_guide_mapping(
        prop->guide_mapping_list, column);

    /* when guide_mapping exist, exist guide nodes */
    if(guide_mapping == NULL)
        return false;
    else
        return true;
}
   
/* Return how many guide nodes are in the property @a prop for @a column */
unsigned int 
PFprop_guide_count(PFprop_t *prop, PFalg_att_t column)
{
    assert(prop);
    
    if(prop->guide_mapping_list == NULL)
        return 0;

    /* get guide_mapping to column */
    PFguide_mapping_t *guide_mapping = get_guide_mapping(
            prop->guide_mapping_list, column);
    PFarray_t *guide_list = NULL;

    /* get guide_mapping */
    if(guide_mapping == NULL)
        return 0;

    /*get list of guide nodes */
    guide_list = guide_mapping->guide_list;

    if(guide_list == NULL)
        return 0;

    return PFarray_last(guide_list);
}

/* Return an array of pointers of PFguide_tree_t of  guide nodes in the 
 * property @a prop for @a column */
PFguide_tree_t** 
PFprop_guide_elements(PFprop_t *prop, PFalg_att_t column)
{
    assert(prop);

    if(prop->guide_mapping_list == NULL)
        return NULL;

    /* get guide_mapping to column */
    PFguide_mapping_t *guide_mapping = get_guide_mapping(
            prop->guide_mapping_list, column);
    PFarray_t *guide_list = NULL;
    PFguide_tree_t **ret = NULL;
    unsigned int count = 0;

    /* get guide_mapping */
    if(guide_mapping == NULL)
        return NULL;

    /*get list of guide nodes */
    guide_list = guide_mapping->guide_list;

    if(guide_list == NULL)
        return NULL;

    count = PFarray_last(guide_list);   
    /* allocate memory and copy guide nodes */
    ret = (PFguide_tree_t**)PFmalloc(count * sizeof(PFguide_tree_t*));

    for(unsigned int i = 0; i < count; i++) {
        ret[i] = *((PFguide_tree_t**)PFarray_at(guide_list, i));
    }

    return ret;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */ 


