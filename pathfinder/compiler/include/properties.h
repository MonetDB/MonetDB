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

#ifndef PROPERTIES_H
#define PROPERTIES_H

/* required in logical.h */
typedef struct PFprop_t PFprop_t;

#include "algebra.h"
#include "logical.h"

/* required values list */
struct req_bool_val_t {
    PFalg_att_t  name;
    PFalg_att_t  val;
};
typedef struct req_bool_val_t req_bool_val_t;

struct PFprop_t {
    unsigned int card;       /**< Exact number of tuples in intermediate
                                  result. (0 means we don't know) */
    PFarray_t   *constants;  /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFarray_t   *domains;    /**< List of attributes along with their
                                  corresponding domain identifier. */
    PFarray_t   *subdoms;    /**< Subdomain relationships (parent/child
                                  relationships between domains) */
    PFarray_t   *disjdoms;   /**< Disjoint domains (list with pairs of
                                  disjoint domains) */
    bool         set;        /**< boolean flag that indicates whether the
                                  cardinality of an operator is important */
    PFalg_att_t  icols;      /**< List of attributes required by the
                                  parent operators. */
    PFarray_t   *keys;       /**< List of attributes that have
                                  unique values. */
    req_bool_val_t req_bool_vals; /**< List of attributes with their
                                       corresponding required values. */
    PFalg_att_t  req_order_cols;  /**< List of columns whose values need
                                       to maintain the order. */
    PFalg_att_t  req_bijective_cols;  /**< List of columns whose values have
                                       to fulfill any bijective function. */
    PFalg_att_t  req_multi_col_cols; /**< List of columns that can be
                                       split up into multiple columns. */
    PFalg_att_t  req_value_cols;  /**< List of columns whose values are
                                       important for the query evaluation. */
    PFarray_t   *req_node_vals;   /**< List of columns and their associated
                                       node usage information. */
    PFarray_t   *name_pairs; /**< List of attributes with their corresponding
                                  unique names. */
    PFarray_t   *level_mapping; /**< List of attributes annotated with
                                     level information. */
    PFarray_t   *guide_mapping_list; /**< List of guide mappings that contain
                                          a pair of column and list of guide
                                          nodes for the operator */
    PFarray_t   *ckeys;      /**< List of composite lists of attributes that
                                  build a key for a relation. */

    /* to allow peep-hole optimizations we also store property
       information of the children (left child 'l_', right child 'r_' */
    PFarray_t  *l_constants; /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFarray_t  *r_constants; /**< List of attributes marked constant,
                                  along with their corresponding values. */
    PFarray_t  *l_domains;   /**< List of attributes along with their
                                  corresponding domain identifier. */
    PFarray_t  *r_domains;   /**< List of attributes along with their
                                  corresponding domain identifier. */
    PFalg_att_t l_icols;     /**< List of attributes required by the
                                  parent operators. */
    PFalg_att_t r_icols;     /**< List of attributes required by the
                                  parent operators. */
    PFarray_t  *l_keys;      /**< List of attributes that have
                                  unique values. */
    PFarray_t  *r_keys;      /**< List of attributes that have
                                  unique values. */
    PFarray_t  *l_name_pairs; /**< List of unique attributes with their
                                   corresponding new unique names. */
    PFarray_t  *r_name_pairs; /**< List of unique attributes with their
                                   corresponding new unique names. */
    PFarray_t  *l_level_mapping; /**< List of attributes annotated with
                                      level information. */
    PFarray_t  *r_level_mapping; /**< List of attributes annotated with
                                      level information. */
};

/* constant item */
struct const_t {
    PFalg_att_t  attr;
    PFalg_atom_t value;
};
typedef struct const_t const_t;

typedef unsigned int dom_t;

/* domain item */
struct dom_pair_t {
    PFalg_att_t  attr;
    dom_t dom;
};
typedef struct dom_pair_t dom_pair_t;

/* domain-subdomain relationship item */
struct subdom_t {
    dom_t dom;
    dom_t subdom;
};
typedef struct subdom_t subdom_t;

/** pair of disjoint domains */
struct disjdom_t {
    dom_t  dom1;
    dom_t  dom2;
};
typedef struct disjdom_t disjdom_t;

/* unique name item */
struct name_pair_t {
    PFalg_att_t ori;
    PFalg_att_t unq;
};
typedef struct name_pair_t name_pair_t;

/* level information */
struct level_t {
    PFalg_att_t attr;
    int         level;
};
typedef struct level_t level_t;

/**
 * Create new property container.
 */
PFprop_t *PFprop (void);

/**
 * Infer all properties of the current tree
 * rooted in root whose flag is set.
 */
void PFprop_infer (bool card, bool const_, bool set, 
                   bool dom, bool icols, bool ckey,
                   bool key, bool ocols, bool req_node,
                   bool reqval, bool level, bool refctr,
                   bool guides, bool ori_names, bool unq_names,
                   PFla_op_t *root, PFguide_tree_t *guide);

/**
 * Reset the property of an operator.
 */
void PFprop_reset (PFla_op_t *root, void (*reset_fun) (PFla_op_t *));

/**
 * Create new property fields for a DAG rooted in @a root
 */
void PFprop_create_prop (PFla_op_t *root);

/**
 * Infer property for a DAG rooted in @a root
 * (The implementation is located in the
 *  corresponding prop/prop_*.c file)
 */
void PFprop_infer_card (PFla_op_t *root);
void PFprop_infer_composite_key (PFla_op_t *root);
void PFprop_infer_const (PFla_op_t *root);
void PFprop_infer_set (PFla_op_t *root);
void PFprop_infer_dom (PFla_op_t *root);
void PFprop_infer_nat_dom (PFla_op_t *root);
void PFprop_infer_icol (PFla_op_t *root);
void PFprop_infer_key (PFla_op_t *root);
void PFprop_infer_ocol (PFla_op_t *root);
void PFprop_infer_req_node (PFla_op_t *root);
void PFprop_infer_reqval (PFla_op_t *root);
void PFprop_infer_unq_names (PFla_op_t *root);
void PFprop_infer_ori_names (PFla_op_t *root);
void PFprop_infer_level (PFla_op_t *root);
void PFprop_infer_refctr (PFla_op_t *root);
void PFprop_infer_guide (PFla_op_t *root, PFguide_tree_t *guide);

bool PFprop_check_rec_delta (PFla_op_t *root);

/* --------------------- cardinality propery accessors --------------------- */

/**
 * Return cardinality stored in property container @a prop.
 */
unsigned int PFprop_card (const PFprop_t *prop);

/* -------------------- composite key property accessors ------------------- */

/**
 * Test if all columns in a schema @a schema together build a composite key
 */
unsigned int PFprop_ckey (const PFprop_t *prop, PFalg_schema_t schema);

/*
 * count number of composite keys
 */
unsigned int PFprop_ckeys_count (const PFprop_t *prop);

/**
 * Return attributes that build a composite key (at position @a i) as an attlist
 */
PFalg_attlist_t PFprop_ckey_at (const PFprop_t *prop, unsigned int i);

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

/* ---------------------- set property accessors ---------------------- */

/**
 * Test if container @a prop allows the cardinality to be changed
 */
bool PFprop_set (const PFprop_t *prop);

/* ----------------------- domain property accessors ----------------------- */

/**
 * Return domain of attribute @a attr stored in property container @a prop.
 */
dom_t PFprop_dom (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return domain of attribute @a attr in the domains of the
 * left child node (stored in property container @a prop)
 */
dom_t PFprop_dom_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return domain of attribute @a attr in the domains of the
 * right child nod (stored in property container @a prop)
 */
dom_t PFprop_dom_right (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if domain @a subdom is a subdomain of the domain @a dom
 * (using the domain relationship list in property container @a prop).
 */
bool PFprop_subdom (const PFprop_t *prop, dom_t subdom, dom_t dom); 

/**
 * Test if domains @a a and @a b are disjoint
 * (using the disjdoms field in PFprop_t)
 */
bool PFprop_disjdom (const PFprop_t *prop, dom_t a, dom_t b);

/**
 * Writes domain represented by @a domain to character array @a f.
 */
void PFprop_write_domain (PFarray_t *f, dom_t domain);

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in AT&T dot notation to character array @a f.
 */
void PFprop_write_dom_rel_dot (PFarray_t *f, const PFprop_t *prop);

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in XML notation to character array @a f.
 */
void PFprop_write_dom_rel_xml (PFarray_t *f, const PFprop_t *prop);

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

/**
 * Infer icols property for a DAG rooted in @a root starting
 * with the icols collected in @a icols.
 */
void PFprop_infer_icol_specific (PFla_op_t *root, PFalg_att_t icols);

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

/**
 * Infer the key properties assuming that guides have been already inferred.
 */
void PFprop_infer_key_with_guide (PFla_op_t *root);

/* ------------------------ ocol property accessors ------------------------ */

/**
 * Test if @a attr is in the list of ocol columns of node @a n
 */
bool PFprop_ocol (const PFla_op_t *n, PFalg_att_t attr);

/**
 * Return the type of @a attr in the list of ocol columns
 */
PFalg_simple_type_t PFprop_type_of (const PFla_op_t *n, PFalg_att_t attr);

/**
 * Infer ocol property for a single node based on 
 * the schemas of its children
 */
void PFprop_update_ocol (PFla_op_t *n);

/* -------------------- required value property accessors ------------------ */

/**
 * Test if @a attr is in the list of required value columns
 * in container @a prop
 */
bool PFprop_req_bool_val (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Looking up required value of column @a attr
 * in container @a prop
 */
bool PFprop_req_bool_val_val (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of order columns
 * in container @a prop
 */
bool PFprop_req_order_col (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of bijective columns
 * in container @a prop
 */
bool PFprop_req_bijective_col (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of multi-col columns
 * in container @a prop
 */
bool PFprop_req_multi_col_col (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if @a attr is in the list of value columns
 * in container @a prop
 */
bool PFprop_req_value_col (const PFprop_t *prop, PFalg_att_t attr);

/* -------------------- required node property accessors ------------------- */

/**
 * @brief Test if column @a attr is linked to any node properties.
 */
bool PFprop_node_property (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if the subtree of column @a attr is queried.
 */
bool PFprop_node_content_queried (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Test if the nodes of column @a attr are serialized.
 */
bool PFprop_node_serialize (const PFprop_t *prop, PFalg_att_t attr);

/* --------------------- unique names property accessors ------------------- */

/**
 * Return unique name of attribute @a attr stored
 * in property container @a prop.
 */
PFalg_att_t PFprop_unq_name (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return unique name of attribute @a attr stored
 * in the left name mapping field of property container @a prop.
 */
PFalg_att_t PFprop_unq_name_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return unique name of attribute @a attr stored
 * in the right name mapping field of property container @a prop.
 */
PFalg_att_t PFprop_unq_name_right (const PFprop_t *prop, PFalg_att_t attr);

/* -------------------- original names property accessors ------------------ */

/**
 * Return original name of unique attribute @a attr stored
 * in property container @a prop.
 */
PFalg_att_t PFprop_ori_name (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return original name of attribute @a attr stored
 * in the left name mapping field of property container @a prop.
 */
PFalg_att_t PFprop_ori_name_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return original name of attribute @a attr stored
 * in the right name mapping field of property container @a prop.
 */
PFalg_att_t PFprop_ori_name_right (const PFprop_t *prop, PFalg_att_t attr);

/* ------------------------ level property accessors ----------------------- */

/**
 * Return the level of nodes stored in column @a attr.
 */
int PFprop_level (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return the level of nodes stored in column @a attr
 * in the left level mapping filed of property container @a prop.
 */
int PFprop_level_left (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return the level of nodes stored in column @a attr
 * in the right level mapping filed of property container @a prop.
 */
int PFprop_level_right (const PFprop_t *prop, PFalg_att_t attr);

/* ------------------ reference counter property accessors ----------------- */

/**
 * Return the number of consuming parent operators.
 */
#define PFprop_refctr(n) ((n)->refctr)

/* --------------------- name tracing property accessors ------------------- */

/**
 * Start from node @a start and keep track of column name changes of the
 * attributes in @a list. At the node @a goal return the updated column name
 * list.
 */
PFalg_attlist_t PFprop_trace_names (PFla_op_t *start,
                                    PFla_op_t *goal,
                                    PFalg_attlist_t list);


/* ---------------------- guide property accessors ------------------------- */
/* Return if the property @a prop has guide nodes for @a column  */
bool PFprop_guide(PFprop_t *prop, PFalg_att_t column);
/* Return how many guide nodes are in the property @a prop for @a column */
unsigned int PFprop_guide_count(PFprop_t *prop, PFalg_att_t column);
/* Return an array of pointers of PFguide_tree_t of  guide nodes in the 
 * property @a prop for @a column */
PFguide_tree_t** PFprop_guide_elements(PFprop_t *prop, PFalg_att_t column);

#endif  /* PROPERTIES_H */


/* vim:set shiftwidth=4 expandtab: */
