/**
 * @file
 *
 * Properties of logical algebra expressions.
 *
 * We consider some properties that can be derived on the logical
 * level of our algebra, like key properties, or the information
 * that a column contains only constant values.  These properties
 * may still be helpful for physical optimization; we will thus
 * propagate any logical property to the physical tree as well.
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

#ifndef PROPERTIES_H
#define PROPERTIES_H

/* required in logical.h */
typedef struct PFprop_t PFprop_t;

#include "algebra.h"
#include "logical.h"

struct PFprop_t {
    PFarray_t   *constants;  /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFalg_att_t  icols;      /**< List of attributes required by the
                                  parent operators. */
    PFalg_att_t  keys;       /**< List of attributes that have
                                  unique values. */
    unsigned int card;       /**< Exact number of tuples in intermediate
                                  result. (0 means we don't know) */
    PFarray_t   *domains;    /**< List of attributes along with their
                                  corresponding domain identifier. */
    PFarray_t   *dom_rel;    /**< List of domain pairs that store 
                                  the relationship between different domains */

    /* to allow peep-hole optimizations we also store property
       information of the children (left child 'l_', right child 'r_' */
    PFarray_t  *l_constants; /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFarray_t  *r_constants; /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFalg_att_t l_icols;     /**< List of attributes required by the
                                  parent operators. */
    PFalg_att_t r_icols;     /**< List of attributes required by the
                                  parent operators. */
    PFalg_att_t l_keys;      /**< List of attributes that have
                                  unique values. */
    PFalg_att_t r_keys;      /**< List of attributes that have
                                  unique values. */
    PFarray_t  *child_domains; /**< List of attributes along with their
                                  corresponding domain identifier. */
};

/* constant item */
struct const_t {
    PFalg_att_t  attr;
    PFalg_atom_t value;
};
typedef struct const_t const_t;

/* domain item */
struct dom_t {
    PFalg_att_t  attr;
    unsigned int dom;
};
typedef struct dom_t dom_t;

/* domain-subdomain relationship item */
struct dom_rel_t {
    unsigned int dom;
    unsigned int subdom;
};
typedef struct dom_rel_t dom_rel_t;

/**
 * Create new property container.
 */
PFprop_t *PFprop (void);

/**
 * Infer all properties of the current tree
 * rooted in root whose flag is set.
 */
void PFprop_infer (bool const_, bool ocols, bool icols,
                   bool key, bool card, bool dom, PFla_op_t *root);

/**
 * Create new property fields for a DAG rooted in @a root
 */
void PFprop_create_prop (PFla_op_t *root);

/**
 * Infer property for a DAG rooted in @a root
 * (The implementation is located in the
 *  corresponding prop/prop_*.c file)
 */
void PFprop_infer_const (PFla_op_t *root);
void PFprop_infer_icol (PFla_op_t *root);
void PFprop_infer_ocol (PFla_op_t *root);
void PFprop_infer_key (PFla_op_t *root);
void PFprop_infer_card (PFla_op_t *root);
void PFprop_infer_dom (PFla_op_t *root);

/* ---------------------- constant property accessors ---------------------- */

/**
 * Test if @a attr is marked constant in container @a prop.
 */
bool PFprop_const (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_const_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_const_right (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Lookup value of @a attr in property container @a prop.  Attribute
 * @a attr must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t PFprop_const_val (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Lookup value of @a attr in the list of constants of the left
 * child. (Information resides in property container @a prop.)
 * Attribute @a attr must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t PFprop_const_val_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Lookup value of @a attr in the list of constants of the right
 * child. (Information resides in property container @a prop.)
 * Attribute @a attr must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t PFprop_const_val_right (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return number of attributes marked const.
 */
unsigned int PFprop_const_count (const PFprop_t *prop);

/**
 * Return name of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_att_t PFprop_const_at (const PFprop_t *prop, unsigned int i);

/**
 * Return value of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_atom_t PFprop_const_val_at (const PFprop_t *prop, unsigned int i);

/* ------------------------ icol property accessors ------------------------ */

/**
 * Test if @a attr is in the list of icol columns in container @a prop
 */
bool PFprop_icol (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of icol columns of the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_icol_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of icol columns of the right child
 * (information is stored in property container @a prop)
 */
bool PFprop_icol_right (const PFprop_t *prop, PFalg_att_t attr);

/*
 * count number of icols attributes
 */
unsigned int PFprop_icols_count (const PFprop_t *prop);

/**
 * Return icols attributes as an attlist.
 */
PFalg_attlist_t PFprop_icols_to_attlist (const PFprop_t *prop);

/* ------------------------ ocol property accessors ------------------------ */

/**
 * Test if @a attr is in the list of ocol columns of node @a n
 */
bool PFprop_ocol (const PFla_op_t *n, PFalg_att_t attr);

/**
 * Infer ocol property for a single node based on 
 * the schemas of its children
 */
void PFprop_update_ocol (PFla_op_t *n);

/* ------------------------- key property accessors ------------------------ */

/**
 * Test if @a attr is in the list of key columns in container @a prop
 */
bool PFprop_key (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of key columns of the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_key_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of key columns of the right child
 * (information is stored in property container @a prop)
 */
bool PFprop_key_right (const PFprop_t *prop, PFalg_att_t attr);

/*
 * count number of keys attributes
 */
unsigned int PFprop_keys_count (const PFprop_t *prop);

/**
 * Return keys attributes as an attlist.
 */
PFalg_attlist_t PFprop_keys_to_attlist (const PFprop_t *prop);

/* --------------------- cardinality propery accessors --------------------- */

/**
 * Return cardinality stored in property container @a prop.
 */
unsigned int PFprop_card (const PFprop_t *prop);

/* ----------------------- domain property accessors ----------------------- */

/**
 * Return domain of attribute @a attr stored in property container @a prop.
 */
unsigned int PFprop_dom (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if domain of attribute @a attr1 is a subdomain of the domain of 
 * attribute @a attr2 (in container @a prop)
 */
bool PFprop_subdom (const PFprop_t *prop, 
                    PFalg_att_t attr1,
                    PFalg_att_t attr2);

/**
 * Write domain-subdomain relationships of property container @a prop
 * to character array @a f.
 */
void PFprop_write_dom_rel (PFarray_t *f, const PFprop_t *prop);

#endif  /* PROPERTIES_H */

/* vim:set shiftwidth=4 expandtab: */
