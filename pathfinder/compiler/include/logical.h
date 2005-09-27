/**
 * @file
 *
 * Declarations specific to logical algebra.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 * $Id$
 */

#ifndef LOGICAL_H
#define LOGICAL_H

#include <stdbool.h>

#include "array.h"

#include "variable.h"
#include "algebra.h"
#include "physical.h"

/** algebra operator node */
typedef struct PFla_op_t PFla_op_t;

#include "properties.h"

/* .............. algebra operators (operators on relations) .............. */

/** algebra operator kinds */
enum PFla_op_kind_t {
      la_lit_tbl        =  1 /**< literal table */
    , la_empty_tbl      =  2 /**< empty literal table */
    , la_disjunion      =  3 /**< union two relations with same schema */
    , la_intersect      =  4 /**< intersect two relations with same schema */
    , la_difference     =  5 /**< difference of two relations w/ same schema */
    , la_cross          =  6 /**< cross product (Cartesian product) */
    , la_eqjoin         =  7 /**< equi-join */
    , la_scjoin         =  8 /**< staircase join */
    , la_select         =  9 /**< selection of rows where column value != 0 */
    , la_type           = 10 /**< selection of rows where a column is of a
                                  certain type */
    , la_cast           = 11 /**< type cast of an attribute */
    , la_project        = 12 /**< algebra projection and renaming operator */
    , la_rownum         = 13 /**< consecutive number generation */
    , la_serialize      = 14 /**< serialize algebra expression
                                  (Placed on the very top of the tree.) */
    , la_num_add        = 15 /**< arithmetic plus operator */
    , la_num_subtract   = 16 /**< arithmetic minus operator */
    , la_num_multiply   = 17 /**< arithmetic times operator */
    , la_num_divide     = 18 /**< arithmetic divide operator */
    , la_num_modulo     = 19 /**< arithmetic modulo operator */
    , la_num_eq         = 20 /**< numeric equal operator */
    , la_num_gt         = 21 /**< numeric greater-than operator */
    , la_num_neg        = 22 /**< numeric negation operator */
    , la_bool_and       = 23 /**< boolean AND operator */
    , la_bool_or        = 24 /**< boolean OR operator */
    , la_bool_not       = 25 /**< boolean NOT operator */
    , la_sum            = 26 /**< operator for (partitioned) sum of a column */
    , la_count          = 27 /**< (partitioned) row counting operator */
    , la_distinct       = 28 /**< duplicate elimination operator */
    , la_element        = 29 /**< element-constructing operator */
    , la_element_tag    = 30 /**< part of the element-constructing operator;
                                  connecting element tag and content;
                                  due to Burg we use two "wire2" operators
                                  now instead of one "wire3 operator "*/
    , la_attribute      = 31 /**< attribute-constructing operator */
    , la_textnode       = 32 /**< text node-constructing operator */
    , la_docnode        = 33 /**< document node-constructing operator */
    , la_comment        = 34 /**< comment-constructing operator */
    , la_processi       = 35 /**< processing instruction-constr. operator */
    , la_concat         = 36 /**< req. for fs:item-sequence-to-node-sequence
                                  builtin function */
    , la_merge_adjacent = 37 /**< operator for pf:merge-adjacent-text-nodes
                                  builtin function */
    , la_doc_access     = 38
    , la_string_join    = 39
    , la_seqty1         = 40 /**< test for exactly one type occurrence in one
                                  iteration (Pathfinder extension) */
    , la_all            = 41 /**< test if all items in an iteration are true */
    , la_roots          = 42 /**< algebraic repres. of the roots of newly
                                  created xml nodes (e.g. element());
                                  schema: iter | pos | item */
    /* all operators below represent xml node fragments with no schema */
    , la_fragment       = 43 /**< representation of a node fragment */
    , la_frag_union     = 44 /**< special node type used to form an algebraic
                                  union of fragments */
    , la_empty_frag     = 45 /**< representation of an empty fragment */
    , la_doc_tbl        = 46 /**< document relation (is also a fragment) */

    , la_dummy               /**< dummy node during compilation */
};
/** algebra operator kinds */
typedef enum PFla_op_kind_t PFla_op_kind_t;

/** semantic content in algebra operators */
union PFla_op_sem_t {

    /* semantic content for literal table constr. */
    struct {
        unsigned int    count;    /**< number of tuples */
        PFalg_tuple_t  *tuples;   /**< array holding the tuples */
    } lit_tbl;                    /**< semantic content for literal table
                                       constructor */

    /* semantic content for projection operator */
    struct {
        unsigned int    count;    /**< length of projection list */
        PFalg_proj_t   *items;    /**< projection list */
    } proj;

    /* semantic content for rownum operator */
    struct {
        PFalg_att_t     attname;  /**< name of generated (integer) attribute */
        PFalg_attlist_t sortby;   /**< sort crit. (list of attribute names */
        PFalg_att_t     part;     /**< optional partitioning attribute,
                                       otherwise NULL */
    } rownum;

    /* semantic content for equi-join operator */
    struct {
        PFalg_att_t     att1;     /**< name of attribute from "left" rel */
        PFalg_att_t     att2;     /**< name of attribute from "right" rel */
    } eqjoin;

    struct {
        PFalg_axis_t    axis;
        PFty_t          ty;
    } scjoin;

    /* semantic content for selection operator */
    struct {
        PFalg_att_t     att;     /**< name of selected attribute */
    } select;

    /* semantic content for type test operator */
    struct {
        PFalg_att_t     att;     /**< name of type-tested attribute */
        PFalg_simple_type_t ty;  /**< comparison type */
        PFalg_att_t     res;     /**< column to store result of type test */
    } type;

    /* semantic content for type cast operator */
    struct {
        PFalg_att_t     att;     /**< name of casted attribute */
        PFalg_simple_type_t ty;  /**< algebra type to cast to */
    } cast;

    /* semantic content for binary (arithmetic and boolean) operators */
    struct {
        PFalg_att_t     att1;     /**< first operand */
        PFalg_att_t     att2;     /**< second operand */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } binary;

    /* semantic content for unary operators */
    struct {
        PFalg_att_t     att;      /**< operand */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } unary;

    /* semantic content for operators for (partitioned) sum of a column */
    struct {
        PFalg_att_t     att;      /**< attribute to be summed up */
        PFalg_att_t     part;     /**< partitioning attribute */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } sum;

    /* semantic content for (partitioned) row counting operator */
    struct {
        PFalg_att_t     part;     /**< partitioning attribute */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } count;

    /* boolean grouping functions (seqty1, all,...) */
    struct {
        PFalg_att_t     res;      /**< result attribute */
        PFalg_att_t     att;      /**< value attribute */
        PFalg_att_t     part;     /**< partitioning attribute */
    } blngroup;

    /* semantic content for dummy built-in function operator */
    struct {
        PFarray_t      *args;     /**< arguments of a built_in function */
    } builtin;

    /* reference columns for document access */
    struct {
        PFalg_att_t     att;      /**< name of the reference attribute */
        PFalg_doc_t     doc_col;  /**< referenced column in the document */
    } doc_access;
};
/** semantic content in algebra operators */
typedef union PFla_op_sem_t PFla_op_sem_t;


/** maximum number of children of a #PFla_op_t node */
#define PFLA_OP_MAXCHILD 2

/** algebra operator node */
struct PFla_op_t {
    PFla_op_kind_t     kind;       /**< operator kind */
    PFla_op_sem_t      sem;        /**< semantic content for this operator */
    PFalg_schema_t     schema;     /**< result schema */
    PFarray_t         *env;        /**< environment to store the corresponding
                                        MIL algebra expression trees (for MIL
                                        algebra generation only) */
    short              state_label;/**< Burg puts its state information here. */

    PFplanlist_t      *plans;      /**< Physical algebra plans that implement
                                        this logical algebra subexpression. */
    PFprop_t          *prop;       /**< Properties derived for this expression*/

    struct PFla_op_t  *child[PFLA_OP_MAXCHILD];

    int                node_id;    /**< specifies the id of this operator
                                        node; required exclusively to
                                        create dot output. */
};

/**
 * For a correct treatment of xml node fragments, we work with sets of
 * fragments and algebra expressions of fragments. This type represents
 * a set of xml fragments. Algebra expressions of xml fragments are
 * represented by PFla_op_t structs.
 */
typedef PFarray_t PFla_set_t;

struct PFla_pair_t {
    struct PFla_op_t  *rel;      /**< algebra representation of query */
    PFla_set_t        *frag;     /**< set of currently live-node fragments */
};
typedef struct PFla_pair_t PFla_pair_t;



/* ***************** Constructors ******************* */


/**
 * Construct algebra node representing a literal table (actually just
 * a wrapper for #PFla_lit_tbl_()).
 *
 * Call with the table's schema (as #PFalg_attlist_t) and the tuples
 * for that table (as type #PFalg_tuple_t).
 *
 * Functions with a variable number of arguments need a mechanism to
 * detect the end of the argument list. We therefore wrap the actual
 * worker #PFla_lit_tbl_() into this macro. The macro detects the
 * number of arguments passed (using arithmetics with sizeof()) and
 * prepends a count information to an array created from the input.
 */
#define PFla_lit_tbl(a,...)                                    \
    PFla_lit_tbl_ ((a),                                        \
                   (sizeof ((PFalg_tuple_t[]) { __VA_ARGS__ }) \
                       / sizeof (PFalg_tuple_t)),              \
                   (PFalg_tuple_t []) { __VA_ARGS__ } )
PFla_op_t *PFla_lit_tbl_ (PFalg_attlist_t a,
                          unsigned int count, PFalg_tuple_t *tpls);


/**
 * Empty table constructor.  Use this instead of an empty table
 * without any tuples to facilitate optimization.
 */
PFla_op_t *PFla_empty_tbl (PFalg_attlist_t a);


/**
 * Cross product (Cartesian product) of two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_cross (const PFla_op_t *n1, const PFla_op_t *n2);


/**
 * Equi-join between two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_eqjoin (const PFla_op_t *n1, const PFla_op_t *n2,
                         PFalg_att_t att1, PFalg_att_t att2);

/**
 * Dummy node to pass various stuff around during algebra compilation.
 */
PFla_op_t * PFla_dummy (void);

/**
 * Staircase join between two relations. Each such join corresponds
 * to the evaluation of an XPath location step.
 */
PFla_op_t * PFla_scjoin (const PFla_op_t *doc, const PFla_op_t *n,
                         const PFla_op_t *dummy);

/**
 * Disjoint union of two relations.
 * Both argument must have the same schema.
 */
PFla_op_t * PFla_disjunion (const PFla_op_t *, const PFla_op_t *);


/**
 * Intersection between two relations.
 * Both argument must have the same schema.
 */
PFla_op_t * PFla_intersect (const PFla_op_t *, const PFla_op_t *);


/**
 * Difference of two relations.
 * Both argument must have the same schema.
 */
PFla_op_t * PFla_difference (const PFla_op_t *, const PFla_op_t *);

/**
 * Construct projection operator
 * (actually just a wrapper for #PFla_project_()).
 *
 * #PFla_project_() needs an information about the length of
 * the array argument passed. This macro computes this length
 * by arithmetics involving the sizeof operator and prepends
 * that information to the invocation of #PFla_project_().
 *
 * You may thus call this macro with the projection argument
 * @a n (pointer to #PFla_op_t) and an arbitrary number of
 * projection attributes (as #PFalg_proj_t). (You may want to
 * construct the latter using #PFalg_proj().)
 *
 * If you include the file algebra_mnemonic.h, this macro will
 * be available under the abbreviated name #project(). The
 * projection list items can then be constructed using #proj().
 */
#define PFla_project(n,...)                                        \
    PFla_project_ ((n),                                            \
                   (sizeof ((PFalg_proj_t[]) { __VA_ARGS__ })      \
                       / sizeof (PFalg_proj_t)),                   \
                   (PFalg_proj_t[]) { __VA_ARGS__ } )
PFla_op_t *PFla_project_ (const PFla_op_t *n,
                          unsigned int count, PFalg_proj_t *p);


PFla_op_t * PFla_rownum (const PFla_op_t *n, PFalg_att_t a,
                         PFalg_attlist_t s, PFalg_att_t p);


/** Constructor for selection of not-0 column values. */
PFla_op_t * PFla_select (const PFla_op_t *n, PFalg_att_t att);

/**
 * Constructor for type test of column values. The result is
 * stored in newly created column.
 */
PFla_op_t * PFla_type (const PFla_op_t *n, PFalg_att_t res,
                       PFalg_att_t att, PFalg_simple_type_t ty);

/**
 * Constructor for the type cast of a column.
 */
PFla_op_t * PFla_cast (const PFla_op_t *n, PFalg_att_t att,
                       PFalg_simple_type_t ty);

/** Constructor for arithmetic addition operators. */
PFla_op_t * PFla_add (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic subtraction operators. */
PFla_op_t * PFla_subtract (const PFla_op_t *n, PFalg_att_t res,
                           PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic multiplication operators. */
PFla_op_t * PFla_multiply (const PFla_op_t *n, PFalg_att_t res,
                           PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic division operators. */
PFla_op_t * PFla_divide (const PFla_op_t *n, PFalg_att_t res,
                         PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic modulo operators. */
PFla_op_t * PFla_modulo (const PFla_op_t *n, PFalg_att_t res,
                         PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric equal operators. */
PFla_op_t * PFla_eq (const PFla_op_t *n, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric greater-than operators. */
PFla_op_t * PFla_gt (const PFla_op_t *n, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric negation operators. */
PFla_op_t * PFla_neg (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att);

/** Constructor for boolean AND operators. */
PFla_op_t * PFla_and (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean OR operators. */
PFla_op_t * PFla_or (const PFla_op_t *n, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean NOT operators. */
PFla_op_t * PFla_not (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t att);

/** Constructor for operators forming (partitioned) sum of a column. */
PFla_op_t * PFla_sum (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att, PFalg_att_t part);

/** Constructor for (partitioned) row counting operators. */
PFla_op_t * PFla_count (const PFla_op_t *n, PFalg_att_t res,
                        PFalg_att_t part);

/** Constructor for sequence type matching operator for `1' occurrence */
PFla_op_t * PFla_seqty1 (const PFla_op_t *n,
                         PFalg_att_t res, PFalg_att_t att, PFalg_att_t part);

/**
 * Constructor for `all' test.
 * (Do all tuples in partition @a part carry the value true in
 * attribute @a item ?)
 */
PFla_op_t * PFla_all (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att, PFalg_att_t part);

/** Constructor for duplicate elimination operator. */
PFla_op_t * PFla_distinct (const PFla_op_t *n);


/*********** node construction functionality *************/

/*
 * Algebra operators instantiated by the following functions
 * create new XML tree nodes.  Their algebraic result is a
 * (document fragment, expression result) pair.  The two components
 * of such a pair may be derived with the operators `fragment'
 * (see #PFla_fragment()) that extracts the document fragment
 * part, and `roots' (see #PFla_roots()) for the expression
 * result.
 *
 * Some operators (including most of the operators below) need
 * access to the `live node set' and thus have a `fragment'
 * type argument.  These operators don't want duplicates in the
 * `live node set', which would be introduced very quickly by
 * typical user queries.
 *
 * (Consider, e.g., the XQuery expression `(/foo, /bar)', where
 * both operands of the sequence construction carry the same
 * set of live nodes.  The union between these two sets would
 * produce lots of duplicate nodes in the live node set.)
 *
 * However, such duplicates originate from the exact same XQuery
 * operator and are thus already identical algebra expressions.
 * Typically, they will be represented in this C code by the
 * exact same pointer to the algebra expression root.  (The above
 * query is such an example.)
 *
 * Duplicates can thus already be removed at compile time.  We
 * do this by collecting document fragments in a _set_ of
 * fragments.  (This set is held duplicate free, based on C
 * pointer identity.)  Whenever we need the current live node
 * set as the input to an algebra operator, we turn this set
 * into an algebraic `fragment union' in the algebra expression
 * tree.
 */

/** Constructor for element operators. */
PFla_op_t * PFla_element (const PFla_op_t *doc, const PFla_op_t *tags,
                          const PFla_op_t *cont);

/** Constructor for attribute operators. */
PFla_op_t * PFla_attribute (const PFla_op_t *tags, const PFla_op_t *cont);

/** Constructor for text node operators. */
PFla_op_t * PFla_textnode (const PFla_op_t *cont);

/** Constructor for document node operators. */
PFla_op_t * PFla_docnode (const PFla_op_t *doc, const PFla_op_t *cont);

/** Constructor for comment operators. */
PFla_op_t * PFla_comment (const PFla_op_t *cont);

/** Constructor for processing instruction operators. */
PFla_op_t * PFla_processi (const PFla_op_t *cont);

/**
 * Constructor required for fs:item-sequence-to-node-sequence()
 * functionality
 */
PFla_op_t * PFla_strconcat (const PFla_op_t *n);

/** Constructor for pf:merge-adjacent-text-nodes() functionality */
PFla_op_t * PFla_pf_merge_adjacent_text_nodes (const PFla_op_t *doc,
                                               const PFla_op_t *n);

PFla_op_t * PFla_doc_access (const PFla_op_t *doc,
                             const PFla_op_t *n,
                             PFalg_att_t col,
                             PFalg_doc_t doc_col);

PFla_op_t * PFla_string_join (const PFla_op_t *text,
                              const PFla_op_t *sep);

/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.  Returns a (frag, result) pair.
 */
PFla_op_t * PFla_doc_tbl (const PFla_op_t *rel);



/**************** document fragment related stuff ******************/

/**
 * Extract the expression result part from a (frag, result) pair.
 * The result of this algebra operator is a relation with schema
 * iter | pos | item.
 */
PFla_op_t * PFla_roots (const PFla_op_t *n);

/**
 * Extract the document fragment part from a (frag, result) pair.
 * It typically contains newly constructed nodes of some node
 * construction operator.  The document representation is dependent
 * on the back-end system.  Hence, the resulting algebra node does
 * not have a meaningful relational schema (in fact, the schema
 * component will be set to NULL).
 */
PFla_op_t * PFla_fragment (const PFla_op_t *n);



/****************** fragment set handling *******************/

/*
 * These functions implement a _set_ of document fragments. This
 * set is held duplicate free (in terms of C pointer identity),
 * and may be turned into an algebra expression tree (with
 * `frag_union' operators) with the function PFla_set_to_alg().
 */

/**
 * Create a new, empty set of fragments.
 */
PFla_set_t *PFla_empty_set (void);

/**
 * Create a new set of fragments, holding one fragment.
 */
PFla_set_t *PFla_set (const PFla_op_t *n);

/**
 * Form a set-oriented union between two sets of fragments.
 * Eliminate duplicate fragments.
 */
PFla_set_t *PFla_set_union (PFla_set_t *frag1, PFla_set_t *frag2);

/**
 * Convert a set of fragments into an algebra expression. The fragments
 * are unified by a special, fragment-specific union operator. It creates
 * a binary tree in which the bottom-most leaf is always represented by an
 * empty fragment.
 */
PFla_op_t *PFla_set_to_la (PFla_set_t *frags);


/** Form algebraic disjoint union between two fragments. */
PFla_op_t * PFla_frag_union (const PFla_op_t *n1, const PFla_op_t *n2);

/** Constructor for an empty fragment */
PFla_op_t * PFla_empty_frag (void);


/****************************************************************/


/**
 * A `serialize' node will be placed on the very top of the algebra
 * expression tree.
 */
PFla_op_t * PFla_serialize (const PFla_op_t *doc, const PFla_op_t *alg);


#endif  /* LOGICAL_H */

/* vim:set shiftwidth=4 expandtab: */
