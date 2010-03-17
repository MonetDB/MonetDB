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

#ifndef PROPERTIES_H
#define PROPERTIES_H

/* required in logical.h */
typedef struct PFprop_t PFprop_t;

#include "algebra.h"
#include "logical.h"

struct PFprop_t {
    unsigned int     card;       /**< Exact number of tuples in intermediate
                                      result. (0 means we don't know) */
    PFarray_t       *constants;  /**< List of columns marked constant,
                                      along with their corresponding values. */
    PFarray_t       *domains;    /**< List of columns along with their
                                      corresponding domain identifier. */
    PFarray_t       *subdoms;    /**< Subdomain relationships (parent/child
                                      relationships between domains) */
    PFarray_t       *disjdoms;   /**< Disjoint domains (list with pairs of
                                      disjoint domains) */
    PFarray_t       *lineage;    /**< List of columns along with the operator
                                      they were created at. */
    bool             set;        /**< boolean flag that indicates whether the
                                      cardinality of an operator is important */
    PFalg_col_t      set_col;    /**< extended set information to overcome
                                      some limitations of the simple set
                                      property inference */
    PFalg_collist_t *icols;      /**< List of columns required by the
                                      parent operators. */
    PFalg_collist_t *keys;       /**< List of columns that have
                                      unique values. */
    PFarray_t       *fds;        /**< List of functional dependencies. */
    PFarray_t       *reqvals;    /**< List of columns their associated
                                      usage information. */
    PFarray_t       *req_node_vals;   /**< List of columns and their associated
                                           node usage information. */
    PFarray_t       *name_pairs; /**< List of columns with their corresponding
                                      unique names. */
    PFalg_col_t      free_cols;  /**< List of bit-encoded columns that are not
                                      in use. */  
    PFalg_col_t      iter_cols;  /**< List of bit-encoded iter columns
                                      (prop_rec_delta.c) */
    PFalg_col_t      pos_cols;   /**< List of bit-encoded pos columns
                                      (prop_rec_delta.c) */
    PFarray_t       *level_mapping; /**< List of columns annotated with
                                         level information. */
    PFarray_t       *guide_mapping_list; /**< List of guide mappings that contain
                                              a pair of column and list of guide
                                              nodes for the operator */
    PFarray_t       *ckeys;      /**< List of composite lists of columns that
                                      build a key for a relation. */
    PFarray_t       *name_origin; /**< List of original names of columns */

    /* to allow peep-hole optimizations we also store property
       information of the children (left child 'l_', right child 'r_' */
    PFarray_t       *l_constants; /**< List of columns marked constant,
                                       along with their corresponding values. */
    PFarray_t       *r_constants; /**< List of columns marked constant,
                                       along with their corresponding values. */
    PFarray_t       *l_domains;   /**< List of columns along with their
                                       corresponding domain identifier. */
    PFarray_t       *r_domains;   /**< List of columns along with their
                                       corresponding domain identifier. */
    PFalg_collist_t *l_icols;     /**< List of columns required by the
                                       parent operators. */
    PFalg_collist_t *r_icols;     /**< List of columns required by the
                                       parent operators. */
    PFalg_collist_t *l_keys;      /**< List of columns that have
                                       unique values. */
    PFalg_collist_t *r_keys;      /**< List of columns that have
                                       unique values. */
    PFarray_t       *l_name_pairs; /**< List of unique columns with their
                                        corresponding new unique names. */
    PFarray_t       *r_name_pairs; /**< List of unique columns with their
                                        corresponding new unique names. */
    PFarray_t       *l_level_mapping; /**< List of columns annotated with
                                           level information. */
    PFarray_t       *r_level_mapping; /**< List of columns annotated with
                                           level information. */
};

/* constant item */
struct const_t {
    PFalg_col_t  col;
    PFalg_atom_t value;
};
typedef struct const_t const_t;

/* domain information */
struct dom_t {
    unsigned int  id;
    struct dom_t *super_dom;
    PFarray_t    *union_doms;
};
typedef struct dom_t dom_t;

/* domain item */
struct dom_pair_t {
    PFalg_col_t  col;
    dom_t       *dom;
};
typedef struct dom_pair_t dom_pair_t;

/* domain-subdomain relationship item */
struct subdom_t {
    dom_t *dom;
    dom_t *subdom;
};
typedef struct subdom_t subdom_t;

/** pair of disjoint domains */
struct disjdom_t {
    dom_t *dom1;
    dom_t *dom2;
};
typedef struct disjdom_t disjdom_t;

/** lineage mapping */
struct lineage_t {
    PFalg_col_t col;
    PFla_op_t  *op;
    PFalg_col_t ori_col;
};
typedef struct lineage_t lineage_t;

/* functional dependencies */
struct fd_t {
    PFalg_col_t col1; /**< key/describing column */
    PFalg_col_t col2; /**< dependent column */ 
};
typedef struct fd_t fd_t;

/* unique name item */
struct name_pair_t {
    PFalg_col_t ori;
    PFalg_col_t unq;
};
typedef struct name_pair_t name_pair_t;

/* level information */
struct level_t {
    PFalg_col_t col;
    int         level;
};
typedef struct level_t level_t;

/* name origin item */
struct name_origin_t {
    PFalg_col_t  col;
    char *       name;
};
typedef struct name_origin_t name_origin_t;

/**
 * Create new property container.
 */
PFprop_t *PFprop (void);

/**
 * Infer all properties of the current tree
 * rooted in root whose flag is set.
 */
void PFprop_infer (bool card, bool const_, bool set, 
                   bool dom, bool lineage, bool icols, bool ckey,
                   bool key, bool fd, bool ocols, bool req_node,
                   bool reqval, bool level, bool refctr,
                   bool guides, bool ori_names, bool unq_names,
                   bool name_origin,
                   PFla_op_t *root, PFguide_list_t *guide_list);

/**
 * Reset the property of an operator.
 */
void PFprop_reset (PFla_op_t *root, void (*reset_fun) (PFla_op_t *));
/**
 * Make the reset function also usable for other contexts.
 */
#define PFla_map_fun(r,f) PFprop_reset ((r),(f))

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
void PFprop_infer_set_extended (PFla_op_t *root);
void PFprop_infer_dom (PFla_op_t *root);
void PFprop_infer_nat_dom (PFla_op_t *root);
void PFprop_infer_lineage (PFla_op_t *root);
void PFprop_infer_icol (PFla_op_t *root);
void PFprop_infer_key (PFla_op_t *root);
void PFprop_infer_functional_dependencies (PFla_op_t *root);
void PFprop_infer_ocol (PFla_op_t *root);
void PFprop_infer_req_node (PFla_op_t *root);
void PFprop_infer_reqval (PFla_op_t *root);
void PFprop_infer_unq_names (PFla_op_t *root);
void PFprop_infer_ori_names (PFla_op_t *root);
void PFprop_infer_name_origin (PFla_op_t *root);
void PFprop_infer_level (PFla_op_t *root);
void PFprop_infer_refctr (PFla_op_t *root);
void PFprop_infer_guide (PFla_op_t *root, PFguide_list_t *guides);

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
 * Return columns that build a composite key (at position @a i) as an collist
 */
PFalg_collist_t * PFprop_ckey_at (const PFprop_t *prop, unsigned int i);

/* ---------------------- constant property accessors ---------------------- */

/**
 * Test if @a col is marked constant in container @a prop.
 */
bool PFprop_const (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_const_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is marked constant in the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_const_right (const PFprop_t *prop, PFalg_col_t col);

/**
 * Lookup value of @a col in property container @a prop.  Attribute
 * @a col must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t PFprop_const_val (const PFprop_t *prop, PFalg_col_t col);

/**
 * Lookup value of @a col in the list of constants of the left
 * child. (Information resides in property container @a prop.)
 * Attribute @a col must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t PFprop_const_val_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Lookup value of @a col in the list of constants of the right
 * child. (Information resides in property container @a prop.)
 * Attribute @a col must be marked constant, otherwise
 * the function will fail.
 */
PFalg_atom_t PFprop_const_val_right (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return number of columns marked const.
 */
unsigned int PFprop_const_count (const PFprop_t *prop);

/**
 * Return name of constant column number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_col_t PFprop_const_at (const PFprop_t *prop, unsigned int i);

/**
 * Return value of constant column number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_atom_t PFprop_const_val_at (const PFprop_t *prop, unsigned int i);

/* ---------------------- set property accessors ---------------------- */

/**
 * Test if container @a prop allows the cardinality to be changed
 */
bool PFprop_set (const PFprop_t *prop);

/**
 * Test if container @a prop allows the cardinality to be changed
 */
bool PFprop_set (const PFprop_t *prop);

/* ----------------------- domain property accessors ----------------------- */

/**
 * Return domain of column @a col stored in property container @a prop.
 */
dom_t * PFprop_dom (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return domain of column @a col in the domains of the
 * left child node (stored in property container @a prop)
 */
dom_t * PFprop_dom_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return domain of column @a col in the domains of the
 * right child nod (stored in property container @a prop)
 */
dom_t * PFprop_dom_right (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if domain @a subdom is a subdomain of the domain @a dom
 * (using the domain relationship list in property container @a prop).
 */
bool PFprop_subdom (const PFprop_t *prop, dom_t *subdom, dom_t *dom); 

/**
 * Test if domains @a a and @a b are disjoint
 * (using the disjdoms field in PFprop_t)
 */
bool PFprop_disjdom (const PFprop_t *prop, dom_t *a, dom_t *b);

/**
 * Writes domain represented by @a domain to character array @a f.
 */
void PFprop_write_domain (PFarray_t *f, dom_t *domain);

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in AT&T dot notation to character array @a f.
 */
void PFprop_write_dom_rel_dot (PFarray_t *f, const PFprop_t *prop, int id);

/**
 * Write domain-subdomain relationships of property container @a prop
 * to in XML notation to character array @a f.
 */
void PFprop_write_dom_rel_xml (PFarray_t *f, const PFprop_t *prop);

/* ---------------------- lineage property accessors ----------------------- */

/**
 * Look up the lineage of column @a col in the property container @a prop.
 */
PFla_op_t * PFprop_lineage (const PFprop_t *prop, PFalg_col_t col);

/**
 * Look up the original column name of column @a col
 * in the property container @a prop.
 */
PFalg_col_t PFprop_lineage_col (const PFprop_t *prop, PFalg_col_t col);

/* ------------------------ icol property accessors ------------------------ */

/**
 * Test if @a col is in the list of icol columns in container @a prop
 */
bool PFprop_icol (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is *not* in the list of icol columns in container @a prop
 */
bool PFprop_not_icol (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of icol columns of the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_icol_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of icol columns of the right child
 * (information is stored in property container @a prop)
 */
bool PFprop_icol_right (const PFprop_t *prop, PFalg_col_t col);

/*
 * count number of icols columns
 */
unsigned int PFprop_icols_count (const PFprop_t *prop);

/**
 * Return icols columns as an collist.
 */
PFalg_collist_t * PFprop_icols_to_collist (const PFprop_t *prop);

/**
 * Infer icols property for a DAG rooted in @a root starting
 * with the icols collected in @a icols.
 */
void PFprop_infer_icol_specific (PFla_op_t *root, PFalg_collist_t *icols);

/* ------------------------- key property accessors ------------------------ */

/**
 * Test if @a col is in the list of key columns in container @a prop
 */
bool PFprop_key (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of key columns of the left child
 * (information is stored in property container @a prop)
 */
bool PFprop_key_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of key columns of the right child
 * (information is stored in property container @a prop)
 */
bool PFprop_key_right (const PFprop_t *prop, PFalg_col_t col);

/*
 * count number of keys columns
 */
unsigned int PFprop_keys_count (const PFprop_t *prop);

/**
 * Return keys columns as an collist.
 */
PFalg_collist_t * PFprop_keys_to_collist (const PFprop_t *prop);

/**
 * Infer the key properties assuming that guides have been already inferred.
 */
void PFprop_infer_key_with_guide (PFla_op_t *root);

/**
 * Infer key and functional dependency properties for a DAG rooted in root.
 */
void PFprop_infer_key_and_fd (PFla_op_t *root);

/* ---------------- functional dependency property accessor ---------------- */

/**
 * Test if a column @a dependent functionally depends on column @a describing.
 * in the list of functional dependencies in container @a prop
 */
bool PFprop_fd (const PFprop_t *prop,
                PFalg_col_t describing, PFalg_col_t dependent);

/* ------------------------ ocol property accessors ------------------------ */

/**
 * Test if @a col is in the list of ocol columns of node @a n
 */
bool PFprop_ocol (const PFla_op_t *n, PFalg_col_t col);

/**
 * Determine type of column @a col in schema @a schema. 
 */
PFalg_simple_type_t PFprop_type_of_ (PFalg_schema_t schema, PFalg_col_t col);

/**
 * Return the type of @a col in the list of ocol columns
 */
PFalg_simple_type_t PFprop_type_of (const PFla_op_t *n, PFalg_col_t col);

/**
 * Infer ocol property for a single node based on 
 * the schemas of its children
 */
void PFprop_update_ocol (PFla_op_t *n);

/* -------------------- required value property accessors ------------------ */

/**
 * Test if @a col is in the list of required value columns
 * in container @a prop
 */
bool PFprop_req_bool_val (const PFprop_t *prop, PFalg_col_t col);

/**
 * Looking up required value of column @a col
 * in container @a prop
 */
bool PFprop_req_bool_val_val (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of order columns
 * in container @a prop
 */
bool PFprop_req_order_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of bijective columns
 * in container @a prop
 */
bool PFprop_req_bijective_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of rank columns
 * in container @a prop
 */
bool PFprop_req_rank_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of multi-col columns
 * in container @a prop
 */
bool PFprop_req_multi_col_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of filter columns
 * in container @a prop
 */
bool PFprop_req_filter_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of link columns
 * (columns only used in the iter column of operator serialize_rel)
 * in container @a prop
 */
bool PFprop_req_link_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col is in the list of value columns
 * in container @a prop
 */
bool PFprop_req_value_col (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if @a col may be represented by something that maintains
 * the same duplicates.
 */
bool PFprop_req_unique_col (const PFprop_t *prop, PFalg_col_t col);

/* -------------------- required node property accessors ------------------- */

/**
 * @brief Test if column @a col is linked to any node properties.
 */
bool PFprop_node_property (const PFprop_t *prop, PFalg_col_t col);

/**
 * @brief Test if the node ids of column @a col are required.
 */
bool PFprop_node_id_required (const PFprop_t *prop, PFalg_col_t col);

/**
 * @brief Test if the node order of column @a col are required.
 */
bool PFprop_node_order_required (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if the subtree of column @a col is queried.
 */
bool PFprop_node_content_queried (const PFprop_t *prop, PFalg_col_t col);

/**
 * Test if the nodes of column @a col are serialized.
 */
bool PFprop_node_serialize (const PFprop_t *prop, PFalg_col_t col);

/* --------------------- unique names property accessors ------------------- */

/**
 * Return unique name of column @a col stored
 * in property container @a prop.
 */
PFalg_col_t PFprop_unq_name (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return unique name of column @a col stored
 * in the left name mapping field of property container @a prop.
 */
PFalg_col_t PFprop_unq_name_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return unique name of column @a col stored
 * in the right name mapping field of property container @a prop.
 */
PFalg_col_t PFprop_unq_name_right (const PFprop_t *prop, PFalg_col_t col);

/* -------------------- original names property accessors ------------------ */

/**
 * Return original name of unique column @a col stored
 * in property container @a prop.
 */
PFalg_col_t PFprop_ori_name (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return original name of column @a col stored
 * in the left name mapping field of property container @a prop.
 */
PFalg_col_t PFprop_ori_name_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return original name of column @a col stored
 * in the right name mapping field of property container @a prop.
 */
PFalg_col_t PFprop_ori_name_right (const PFprop_t *prop, PFalg_col_t col);

/* ------------------------ level property accessors ----------------------- */

/**
 * Return the level of nodes stored in column @a col.
 */
int PFprop_level (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return the level of nodes stored in column @a col
 * in the left level mapping filed of property container @a prop.
 */
int PFprop_level_left (const PFprop_t *prop, PFalg_col_t col);

/**
 * Return the level of nodes stored in column @a col
 * in the right level mapping filed of property container @a prop.
 */
int PFprop_level_right (const PFprop_t *prop, PFalg_col_t col);

/* define the unknown level to be smaller than the collection level */
#define UNKNOWN_LEVEL -2
#define LEVEL_KNOWN(l) (l > UNKNOWN_LEVEL)

/* --------------------- name origin property accessors -------------------- */

/**
 * Return the original name of column @a col (in container @a prop).
 */
char * PFprop_name_origin (const PFprop_t *prop, PFalg_col_t col);

/* ------------------ reference counter property accessors ----------------- */

/**
 * Return the number of consuming parent operators.
 */
#define PFprop_refctr(n) ((n)->refctr)

/* --------------------- name tracing property accessors ------------------- */

/**
 * Start from node @a start and keep track of column name changes of the
 * columns in @a list. At the node @a goal return the updated column name
 * list.
 */
PFalg_collist_t * PFprop_trace_names (PFla_op_t *start,
                                      PFla_op_t *goal,
                                      PFalg_collist_t *list);


/* ---------------------- guide property accessors ------------------------- */

/* Return if the property @a prop has guide nodes for @a column  */
bool PFprop_guide (PFprop_t *prop, PFalg_col_t column);

/* Return how many guide nodes are in the property @a prop for @a column */
unsigned int PFprop_guide_count (PFprop_t *prop, PFalg_col_t column);

/* Return an array of pointers of PFguide_tree_t of  guide nodes in the 
 * property @a prop for @a column */
PFguide_tree_t** PFprop_guide_elements (PFprop_t *prop, PFalg_col_t column);

#endif  /* PROPERTIES_H */


/* vim:set shiftwidth=4 expandtab: */
