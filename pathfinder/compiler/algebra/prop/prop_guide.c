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
#include <stdio.h>
#include <string.h>

#include "properties.h"
#include "load_stats.h"
#include "logical.h"
#include "alg_dag.h"
#include "bitset.h"
#include "pfstrings.h"
#include "subtyping.h"

#define SEEN(n) ((n)->bit_dag)



/* Dummy namespace for the guide */
PFns_t PFns_guide = 
    { .prefix = "",
      .uri    = "" };  

/* extract the filename from the path */
char *get_filename(char * path_and_filename);

/* add a PFguide_mapping_t to a @guide_mapping_list  if the @a column 
 * do not exist, otherwise it will be added only @a guide in the 
 * corresponding PFguide_mapping_t in @guide_mapping_list */
PFarray_t *add_guide(PFarray_t *guide_mapping_list, 
        PFguide_tree_t  *guide, char *column);

/* add the @a guide_mapping to the @a guide_mapping_list, if the 
 * column already exist the guide nodes will be added to the right 
 * guide mapping in @a guide_mapping_list */
PFarray_t *add_guide_mapping(PFarray_t *guide_mapping_list,
        PFguide_mapping_t *guide_mapping);

/* remove the PFguide_mapping_t from the @a guide_mapping_list where 
 * the column is equal */
PFarray_t *remove_column(PFarray_t *guide_mapping_list, char* column);

/* get the corresponding PFguide_mapping_t element from @a guide_mapping_list
 * where the columns are equal */
PFguide_mapping_t *get_guide_mapping(PFarray_t *guide_mapping_list,
        char* column);

/* initialize the guide property in @a n */
void copy_doc_tbl(PFla_op_t *n, PFguide_tree_t *guide);

/* Intersect between left and right child guide nodes */
void union_guides(PFla_op_t  *n);

/* Differnce between left and right child guide nodes */
void difference(PFla_op_t  *n);

/* Create a PFty_t from a guide element */
PFty_t  create_PFty_t(PFguide_tree_t *n);

/* Returns all ancestor of @a n */
PFarray_t *getAncestorFromGuide(PFguide_tree_t *n, PFty_t type);

/* Get recursive all descendant elements */
PFarray_t *getDescendantFromGuideRec(PFguide_tree_t *n, PFty_t type,
    PFarray_t *descendant);

/* Returns all descendant of @a n if they are a suptype of @a type */
PFarray_t *getDescendantFromGuide(PFguide_tree_t *n, PFty_t type);

/* Remove duplicate guides from array */
void remove_duplicate(PFla_op_t *n);

/* Copy the guides from the right child to the parent */
void copyR(PFla_op_t *n); 

/* Copy the guide_mapping_list from the left child to the parent */
void copyL(PFla_op_t *n);

/* Copy guides for the scjoin operator*/
void copy_scjoin(PFla_op_t *n);

/* Copy guides for the project operator */
void copy_project(PFla_op_t *n);

/* Infer domain properties; worker for prop_infer(). */
void infer_guide(PFla_op_t *n, PFguide_tree_t *guide);

/* worker for PFprop_infer_guide */
void prop_infer(PFla_op_t *n, PFguide_tree_t *guide);


/* Extract the filename from @a path_and_filename 
 * example: '/home/user/filename.xml' will return 'filename.xml' 
 * example: 'filename.xml' will return 'filename.xml' */
char *
get_filename(char * path_and_filename) 
{
    char  *pos = NULL, * filename = NULL;

    /* find last position of '/' */
    pos = strrchr(path_and_filename, (int)'/') + 1;

    /* copy the filename */
    if(pos != NULL) {
        filename = (char*)PFmalloc(sizeof(char)*(strlen(pos)+1));
        strncpy(filename, pos, strlen(pos));
    } else {
        filename=(char*)PFmalloc(sizeof(char)*(strlen(path_and_filename)+1));
        strncpy(filename, path_and_filename, strlen(path_and_filename));
    }

    return filename;
}

/* add a PFguide_mapping_t to a @guide_mapping_list  if the @a column 
 * do not exist, otherwise it will be added only @a guide in the 
 * corresponding PFguide_mapping_t in @guide_mapping_list */
PFarray_t *
add_guide(PFarray_t *guide_mapping_list,PFguide_tree_t  *guide, char *column)
{
    if(guide == NULL || column == NULL)
        return NULL;

    PFguide_mapping_t *guide_mapping = NULL;
    PFarray_t *new_guide_mapping_list = NULL;

    /* Create new array of PFguide_mapping_t*/
    if(guide_mapping_list == NULL) {
        new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
    } else {
        new_guide_mapping_list = PFarray_copy(guide_mapping_list);
    }

    /* look up if the column just exist */
    for(unsigned int i = 0; i < PFarray_last(new_guide_mapping_list); i++) {
        /* get element from PFarray_t */
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                new_guide_mapping_list, i));

        if(guide_mapping == NULL)
            break;

        /* compare */
        if(strcmp(column, guide_mapping->column) == 0) {
            *(PFguide_tree_t**) PFarray_add(guide_mapping->guide_list)=guide;
            return new_guide_mapping_list;
        }
    }
    /* Create new PFguide_mapping_t */
    guide_mapping = (PFguide_mapping_t*)PFmalloc(sizeof(PFguide_mapping_t**));
    *guide_mapping = (PFguide_mapping_t) {
        .column = (char*) PFmalloc(strlen(column)+1),
        .guide_list = PFarray(sizeof(PFguide_tree_t**)),
    };

    /* insert values in PFguide_mapping_t */
    strncpy(guide_mapping->column, column, strlen(column));
    *(PFguide_tree_t**) PFarray_add(guide_mapping->guide_list) = guide;
   
    /* insert PFguide_mapping_t in PFarray_t */
    *(PFguide_mapping_t**) PFarray_add(new_guide_mapping_list)=guide_mapping;
    
    return PFarray_copy(new_guide_mapping_list); 
}

/* add the @a guide_mapping to the @a guide_mapping_list, if the
 * column already exist the guide nodes will be added to the right
 * guide mapping in @a guide_mapping_list */
PFarray_t *
add_guide_mapping(PFarray_t *guide_mapping_list,
        PFguide_mapping_t *guide_mapping)
{
    PFarray_t *new_guide_mapping_list = NULL;

    if(guide_mapping_list != NULL) 
        new_guide_mapping_list = PFarray_copy(guide_mapping_list);

    if(guide_mapping == NULL)
        return new_guide_mapping_list;

    char *column = guide_mapping->column; /* column name */
    PFarray_t *guide_list = guide_mapping->guide_list; /*elements to insert*/
    PFguide_tree_t *element = NULL; 

    if(guide_list == NULL)
        return guide_mapping_list;

    for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
        element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
        /* insert element in list */
        new_guide_mapping_list = add_guide(new_guide_mapping_list,
                element, column);
    }
    return new_guide_mapping_list;
}

/* remove the PFguide_mapping_t from the @a guide_mapping_list where 
 * the column is equal */
PFarray_t *
remove_column(PFarray_t *guide_mapping_list, char* column)
{
    assert(guide_mapping_list);
    assert(column);

    PFarray_t *new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
    PFguide_mapping_t *guide_mapping = NULL;
    
    /* lookup if the column just exist */
    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        /* get element from PFarray_t */
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                guide_mapping_list, i));

        /* if column is not equal, add the PFguide_mapping_t */
        if(strcmp(column, guide_mapping->column) != 0) {
            *(PFguide_mapping_t**) PFarray_add(new_guide_mapping_list) = 
                    guide_mapping;
        }
    }    

    return new_guide_mapping_list;
}

/* get the corresponding PFguide_mapping_t element from @a guide_mapping_list
 * where the columns are equal */
PFguide_mapping_t *
get_guide_mapping(PFarray_t *guide_mapping_list, char* column)
{
    assert(guide_mapping_list);
    assert(column);

    PFguide_mapping_t *guide_mapping = NULL;

    for(unsigned int i = 0; i < PFarray_last(guide_mapping_list); i++) {
        /* get element from PFarray_t */
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                guide_mapping_list, i));

        if(strcmp(column, guide_mapping->column) == 0) {
            return guide_mapping;
        }
    }

    return NULL;
}

/* initialize the guide property in @a n */
void
copy_doc_tbl(PFla_op_t *n, PFguide_tree_t *guide)
{
    assert(n);
    assert(n->prop);

    /* Test if the document name is constant */ 
    if(PFprop_const (n->prop, n->sem.doc_tbl.item) == true) {
        /* value of the document name */  
        PFalg_atom_t a = PFprop_const_val (n->prop, 
            n->sem.doc_tbl.item);

        if(a.type == aat_str) {
            /* Is guide filename equal to qurey filename */
            if(strcmp(guide->tag_name,get_filename(a.val.str)) == 0) {

                /* add the guide to the guide_list */
                n->prop->guide_mapping_list = add_guide(
                        n->prop->guide_mapping_list, guide, 
                        PFatt_str (n->sem.doc_tbl.item));

    	    } else {
                n->prop->guide_mapping_list = NULL;
            }
        }
    }    
}


/* Intersect between left and right child guide nodes */
void
union_guides(PFla_op_t  *n)
{
    assert(n);
    assert(n->prop);
    assert(n->child[0]);
    assert(n->child[1]);
    assert(n->child[0]->prop);
    assert(n->child[1]->prop);
    
    PFarray_t  *left_guide_mapping_list = PFarray_copy( 
            n->child[0]->prop->guide_mapping_list); /* left child */
    PFarray_t  *right_guide_mapping_list = PFarray_copy(
            n->child[1]->prop->guide_mapping_list); /* right chid */
    PFarray_t  *new_guide_mapping_list = NULL; /* the union of left 
                                                  and right child */
    PFguide_mapping_t *guide_mapping = NULL;

    /* if one child is NULL, return the other */
    if(left_guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = PFarray_copy(right_guide_mapping_list);
        return;
    }
    if(right_guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = PFarray_copy(left_guide_mapping_list);
        return;
    }

    /* copy the elements from left child */
    for(unsigned int i = 0; i < PFarray_last(left_guide_mapping_list); i++) {
         guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                left_guide_mapping_list, i));

        new_guide_mapping_list = add_guide_mapping(new_guide_mapping_list,
                guide_mapping);
    }

    /* Get union of left and right child guide nodes */
    for(unsigned int i = 0; i < PFarray_last(right_guide_mapping_list); i++) {
        guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                right_guide_mapping_list, i));

        /* union of right and left child */
        new_guide_mapping_list = add_guide_mapping(new_guide_mapping_list,
                guide_mapping);
    }

    n->prop->guide_mapping_list = new_guide_mapping_list;
    
    /* remove the duplicate elements */
    remove_duplicate(n);
    return;   
}

/* Compute the difference between left and right child. 
 * Will return elements from left child without the 
 * the elements in right child */
void
difference(PFla_op_t  *n)
{
    assert(n);
    assert(n->child[0]);
    assert(n->child[1]);
    assert(n->child[0]->prop);
    assert(n->child[1]->prop);

    PFarray_t  *left_guide_mapping_list = 
            n->child[0]->prop->guide_mapping_list; /* left child */
    PFarray_t  *right_guide_mapping_list = 
            n->child[1]->prop->guide_mapping_list; /* right child */
    /* difference between left and right */
    PFarray_t *new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
    PFguide_mapping_t *left_guide_mapping = NULL, *right_guide_mapping = NULL,
            *new_guide_mapping = NULL;
    PFarray_t *left_guide_list = NULL, *right_guide_list = NULL,
            *new_guide_list = PFarray(sizeof(PFguide_tree_t**));
    PFguide_tree_t  *element = NULL, *element2 = NULL;
    bool add_element_bool = true;

    /* left child is NULL -> difference is NULL */
    if(left_guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = NULL;
        return;
    }
    /* right child is NULL -> difference is left child */
    if(right_guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = left_guide_mapping_list;
        return;
    }

    /* compute the difference of left and right child */
    for(unsigned int i = 0; i < PFarray_last(left_guide_mapping_list); i++) {
        left_guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                left_guide_mapping_list, i));

        if(left_guide_mapping == NULL)
            break;

        for(unsigned int j = 0; j < PFarray_last(right_guide_mapping_list); 
                j++) {
            right_guide_mapping = *((PFguide_mapping_t**) PFarray_at(
                    right_guide_mapping_list, j));

            if(right_guide_mapping == NULL)
                break;

            /* columns are not equal -> do nothing*/
            if(strcmp(left_guide_mapping->column, 
                    right_guide_mapping->column) != 0)
                break;

            left_guide_list = left_guide_mapping->guide_list;
            right_guide_list = right_guide_mapping->guide_list;
    
            /* Get the difference of left and right child guide nodes */
            for(unsigned int k = 0; k < PFarray_last(left_guide_list); k++) {
                element = *((PFguide_tree_t**) PFarray_at(
                        left_guide_list, k));
                add_element_bool = true;
                for(unsigned int l = 0; l < PFarray_last(
                        right_guide_list); l++) {
                    element2 = *((PFguide_tree_t**) PFarray_at(
                            right_guide_list, l));
                    /* if guide already exist -> do not add guide node */
                    if(element->guide == element2->guide) {
                        add_element_bool = false;
                        break;
                    }
                }
                if(add_element_bool) 
                    *(PFguide_tree_t**) PFarray_add(new_guide_list) = element;
            }
            /* difference was build */
            new_guide_mapping = PFmalloc(sizeof(PFguide_mapping_t));
            *new_guide_mapping = (PFguide_mapping_t) {
                .column = PFmalloc(sizeof(left_guide_mapping->column) + 1),
                .guide_list = PFmalloc(sizeof(PFguide_tree_t*)),            
            };
            /* insert values in guide_mapping */
            strncpy(new_guide_mapping->column, left_guide_mapping->column,
                    sizeof(left_guide_mapping->column));
            new_guide_mapping->guide_list = new_guide_list;

            /* add guide mapping to the guide mapping list */
            *((PFguide_mapping_t**) PFarray_add(new_guide_mapping_list)) = 
                    new_guide_mapping;
        }
    }

    n->prop->guide_mapping_list = new_guide_mapping_list;
    return;
}

/* Create a PFty_t type from a guide node */
PFty_t 
create_PFty_t(PFguide_tree_t *n)
{
    PFty_t  ret;
    switch(n->kind) {
        case(elem):
            ret = PFty_elem(PFqname(PFns_guide, n->tag_name), 
                PFty_xs_anyType());
            break;
        case(attr):
            ret = PFty_elem(PFqname(PFns_guide, n->tag_name),
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
PFarray_t *
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
PFarray_t *
getDescendantFromGuideRec(PFguide_tree_t *n, PFty_t type, 
    PFarray_t *descendant)
{
    if(n == NULL)
        return descendant;

    PFarray_t  *childs = n->child_list; /* all childs of n */
    PFguide_tree_t  *element;           /* one child element */

    if(childs == NULL)
        return descendant;

    /* loop over all child element of @a n */
    for(unsigned int i = 0; i < PFarray_last(childs); i++) {
        element = *((PFguide_tree_t**) PFarray_at(childs, i));
        /* get recursive all descendant element */
        PFarray_concat(descendant,
            getDescendantFromGuideRec(element, type, descendant));

        /* add element if it is a subtype of @a type*/
        if(PFty_subtype(create_PFty_t(element), type)) {
            *(PFguide_tree_t**) PFarray_add(descendant) = element;
        }
    }

    return descendant;
}

/* Returns all descendant of @a n if they are a suptype of @a type */
PFarray_t *
getDescendantFromGuide(PFguide_tree_t *n, PFty_t type)
{
    PFarray_t  *descendant =  PFarray(sizeof(PFguide_tree_t**));

    return getDescendantFromGuideRec(n, type, descendant);
}

/* Remove duplicate guides from array */
void
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
            break;

        guide_list = guide_mapping->guide_list;
        if(guide_list == NULL) 
            break;

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

/* Copy the guide_mapping_list from the right child to the parent */
void 
copyR(PFla_op_t *n)
{
    assert(n);
    assert(n->child[1]);
    assert(n->child[1]->prop);

    PFarray_t *child_guide_mapping_list = 
            n->child[1]->prop->guide_mapping_list;

    /* copy guide_mapping_list from the right child */
    if(child_guide_mapping_list != NULL)
        n->prop->guide_mapping_list = PFarray_copy(child_guide_mapping_list);

    return;
}

/* Copy the guide_mapping_list from the left child to the parent */
void 
copyL(PFla_op_t *n)
{
    assert(n);
    assert(n->child[0]);
    assert(n->child[0]->prop);

    PFarray_t *child_guide_mapping_list = 
            n->child[0]->prop->guide_mapping_list;

    /* copy guide_mapping_list from the left child */
    if(child_guide_mapping_list != NULL)
        n->prop->guide_mapping_list = PFarray_copy(child_guide_mapping_list);

    return;
}

/* Stair case join / pathstep */
void
copy_scjoin(PFla_op_t *n)
{
    assert(n);
    assert(n->prop);
    assert(n->child[1]);
    assert(n->child[1]->prop);

    PFalg_axis_t     axis = n->sem.scjoin.axis; /* axis step */
    PFarray_t *guide_mapping_list = n->child[1]->prop->guide_mapping_list;

    char *column_in = PFatt_str(n->sem.scjoin.item); /* input column */
    char *column_out = PFatt_str(n->sem.scjoin.item_res); /* output column */

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
        
        n->prop->guide_mapping_list = NULL;
        return;
    }
    
    /* no guides -> do nothing */
    if(guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = NULL;
        return;
    }
    /* get guide_mapping for input column */
    guide_mapping = get_guide_mapping(guide_mapping_list, column_in);
    if(guide_mapping == NULL) {
        n->prop->guide_mapping_list = NULL;
        return;
    }
    
    /* get array of guide nodes */
    guide_list = guide_mapping->guide_list;
    if(guide_list == NULL) {
        n->prop->guide_mapping_list = NULL;
        return;
    }

    switch(axis) {
        case(alg_chld):
        /* child axis */
            new_guide_mapping_list = NULL;
            /* child step for all guide nodes */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i)); 
                childs = element->child_list;
                /* subtype check for all childs */
                for(unsigned int j = 0; j < PFarray_last(childs); j++) {
                    child_element= *((PFguide_tree_t**) PFarray_at(childs,j));
                    if(PFty_subtype(
                            create_PFty_t(child_element),
                            n->sem.scjoin.ty)) {
                        /* add child */
                        new_guide_mapping_list = add_guide(
                                new_guide_mapping_list, child_element, 
                                column_out);
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
                        n->sem.scjoin.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element,
                            column_out);
                }
            }
            break;

        case(alg_anc_s):    
        /* ancestor-or-self axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                /* subtype check for all childs */
                if(PFty_subtype(create_PFty_t(element),
                        n->sem.scjoin.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element,
                            column_out);
                }
            }
            /* NO BREAK! */
        case(alg_anc):
        /* ancestor axis */
            for(unsigned int i = 0; i < 1/*PFarray_last(guide_list)*/; i++) {
                /* find all ancestor */
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                help_array = getAncestorFromGuide(element, n->sem.scjoin.ty);
                /* add ancestor to return array */
                for(unsigned int j = 0; j < PFarray_last(help_array); j++) {
                    element2= *((PFguide_tree_t**) PFarray_at(help_array, j));
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element2,
                            column_out);
                }
            }
            break;
      case(alg_par):
      /* parent axis */
           for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                if(PFty_subtype(create_PFty_t(element->parent),
                            n->sem.scjoin.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element->parent,
                            column_out);
                }
            }
            break;      
        case(alg_desc_s):
        /* descendant-or-self axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                if(PFty_subtype(create_PFty_t(element),
                        n->sem.scjoin.ty)) {
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element,
                            column_out);
                }
            }
            /* NO BREAK! */
        case(alg_desc):
        /* descendant axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                help_array = getDescendantFromGuide(element,n->sem.scjoin.ty);
                /* add descendant to return array */
                for(unsigned int j = 0; j < PFarray_last(help_array); j++) {
                    element2= *((PFguide_tree_t**) PFarray_at(help_array, j));
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element2,
                            column_out);
                }
            }            
            break; 
        case(alg_attr):
        /* attribute axis */
            for(unsigned int i = 0; i < PFarray_last(guide_list); i++) {
                element = *((PFguide_tree_t**) PFarray_at(guide_list, i));
                if(PFty_subtype(create_PFty_t(element), n->sem.scjoin.ty))
                    /* add guide nodes to return array */
                    new_guide_mapping_list = add_guide(
                            new_guide_mapping_list, element,
                            column_out);
            } 
            break;
        default:
            break;
    }

    n->prop->guide_mapping_list = new_guide_mapping_list;
    new_guide_mapping_list = NULL;
    return;
}

/* copy guide nodes for project operator*/
void 
copy_project(PFla_op_t *n)
{
    assert(n);
    assert(n->prop);
    assert(n->child[0]);
    assert(n->child[0]->prop);

    PFarray_t *guide_mapping_list = n->child[0]->prop->guide_mapping_list,
            *new_guide_mapping_list = NULL;
    PFguide_mapping_t *guide_mapping = NULL;
    char *new_column = NULL, *old_column = NULL;

    if(guide_mapping_list == NULL) {
        n->prop->guide_mapping_list = NULL;
        return;
    }

    /* iterate over all columns */
    for (unsigned int i = 0; i < n->sem.proj.count; i++) {
        /* get new and old column name */
        new_column = PFatt_str (n->sem.proj.items[i].new);
        old_column = PFatt_str (n->sem.proj.items[i].old);

        /* get guide mapping from list */
        guide_mapping = get_guide_mapping(guide_mapping_list, old_column);

        if(guide_mapping == NULL)
            break;

        /* set new column name */
        guide_mapping->column = new_column;

        /* assign guide mapping to the list */
        if(new_guide_mapping_list == NULL)
            new_guide_mapping_list = PFarray(sizeof(PFguide_mapping_t**));
        
        *((PFguide_mapping_t**) PFarray_add(new_guide_mapping_list)) = 
                guide_mapping;
    }

    n->prop->guide_mapping_list = new_guide_mapping_list;

    return;
}




/* Infer domain properties; worker for prop_infer(). */
void
infer_guide(PFla_op_t *n, PFguide_tree_t *guide)
{
    assert(n);
    assert(n->prop);

    switch (n->kind) {
        /* get guide nodes from the left and right child */
        case la_cross:    
        case la_eqjoin:
        case la_thetajoin: 
        case la_disjunion:   
        case la_intersect:
            union_guides(n);
            break;

        /* copyL */
        case la_dummy:     
        case la_semijoin:    
        case la_select:   
        case la_distinct:
        case la_attach:  
        case la_fun_1to1:
        case la_num_eq:  
        case la_num_gt:    
        case la_bool_and:   
        case la_bool_or:  
        case la_bool_not:   
        case la_rownum:     
        case la_number:    
        case la_roots:
        case la_type:    
        case la_type_assert:
        case la_cast:
        case la_element:
        case la_attribute:
        case la_textnode:
        case la_docnode:  
        case la_comment:        
        case la_processi:   
        case la_merge_adjacent:
        case la_cond_err:     
        case la_trace:       
        case la_trace_msg:    
        case la_trace_map:    
            copyL(n);
	    break;
        case la_project: 
            copy_project(n);
            break;       
        case la_difference:
            difference(n);
            break; 
        case la_scjoin:    
            copy_scjoin(n);
            remove_duplicate(n);
            break;
        case la_doc_tbl:
            copy_doc_tbl(n, guide);
    	    break;
    	case la_doc_access:
            copyR(n); 
    	    break;
        
        /* do nothing */
        case la_serialize:
        case la_fragment:   
        case la_frag_union: 
        case la_nil:         
        case la_rec_fix:      
        case la_rec_param:    
        case la_rec_arg:      
        case la_rec_base:    
        case la_empty_tbl:    
        case la_proxy:        
        case la_proxy_base:   
        case la_cross_mvd:   
        case la_eqjoin_unq:   
        case la_string_join:  
        case la_avg:  
        case la_max:        
        case la_min:        
        case la_sum:       
        case la_count:      
        case la_seqty1:     
        case la_all:
        case la_dup_scjoin:
        case la_empty_frag: 
        case la_lit_tbl:
        default:
            break;
    }
    return;
}


/* worker for PFprop_infer_guide */ 
void
prop_infer(PFla_op_t *n, PFguide_tree_t *guide)
{
    /* no guide tree -> do nothing */
    if (guide == NULL)
        return;

    assert(n);

    if(SEEN(n))
        return;

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
    prop_infer(root, guide);

    PFla_dag_reset(root);
}

 
