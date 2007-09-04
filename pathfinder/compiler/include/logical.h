/**
 * @file
 *
 * Declarations specific to logical algebra.
 *
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

#ifndef LOGICAL_H
#define LOGICAL_H

#include "array.h"

#include "variable.h"
#include "algebra.h"
#include "physical.h"
#include "sql.h"
#include "ordering.h"
#include "mem.h"
#include "load_stats.h"

/** algebra operator node */
typedef struct PFla_op_t PFla_op_t;

#include "properties.h"

/* .............. algebra operators (operators on relations) .............. */

/** algebra operator kinds */
enum PFla_op_kind_t {
      la_serialize       =  1 /**< serialize algebra expression
                                   (Placed on the very top of the tree.) */
    , la_lit_tbl         =  2 /**< literal table */
    , la_empty_tbl       =  3 /**< empty literal table */
    , la_attach          =  4 /**< attach constant column */
    , la_cross           =  5 /**< cross product (Cartesian product) */
    , la_eqjoin          =  6 /**< equi-join */
    , la_semijoin        =  7 /**< semi-join */
    , la_thetajoin       =  8 /**< theta-join (with possibly multiple
                                   predicates) */
    , la_project         =  9 /**< algebra projection and renaming operator */
    , la_select          = 10 /**< selection of rows where column value != 0 */
    , la_disjunion       = 11 /**< union two relations with same schema */
    , la_intersect       = 12 /**< intersect two relations with same schema */
    , la_difference      = 13 /**< difference of two relations w/ same schema */
    , la_distinct        = 14 /**< duplicate elimination operator */
    , la_fun_1to1        = 20 /**< generic operator that extends the schema with
                                   a new column where each value is determined 
                                   by the values of a single row (cardinality 
                                   stays the same) */
    , la_num_eq          = 25 /**< numeric equal operator */
    , la_num_gt          = 26 /**< numeric greater-than operator */
    , la_bool_and        = 28 /**< boolean AND operator */
    , la_bool_or         = 29 /**< boolean OR operator */
    , la_bool_not        = 30 /**< boolean NOT operator */
    , la_to              = 31 /**< op:to operator */
    , la_avg             = 32 /**< operator for (partitioned) avg of a column */
    , la_max             = 33 /**< operator for (partitioned) max of a column */
    , la_min             = 34 /**< operator for (partitioned) min of a column */
    , la_sum             = 35 /**< operator for (partitioned) sum of a column */
    , la_count           = 36 /**< (partitioned) row counting operator */
    , la_rownum          = 37 /**< consecutive number generation */
    , la_rank            = 38 /**< arbitrary but ordered number generation */
    , la_number          = 39 /**< arbitrary, unordered number generation */
    , la_type            = 40 /**< selection of rows where a column is of a
                                   certain type */
    , la_type_assert     = 41 /**< restricts the type of a relation */
    , la_cast            = 42 /**< type cast of an attribute */
    , la_seqty1          = 43 /**< test for exactly one type occurrence in one
                                   iteration (Pathfinder extension) */
    , la_all             = 44 /**< test if all items in an iteration are true */
    , la_step            = 49 /**< XPath location step */
    , la_step_join       = 50 /**< duplicate generating path step */
    , la_guide_step      = 51 /**< XPath location step 
                                   (with guide information) */
    , la_guide_step_join = 52 /**< duplicate generating path step 
                                   (with guide information) */
    , la_id              = 53 /**< Operator representing a fn:id node lookup */
    , la_idref           = 54 /**< Operator representing a fn:idref node 
                                   lookup */
    , la_doc_tbl         = 55 /**< document relation (is also a fragment) */
    , la_doc_access      = 56 /**< document access necessary 
                                   for pf:string-value */
    , la_twig            = 60 /**< twig root operator */
    , la_fcns            = 61 /**< twig constructor sequence */
    , la_docnode         = 62 /**< document node-constructing operator */
    , la_element         = 63 /**< element-constructing operator */
    , la_attribute       = 64 /**< attribute-constructing operator */
    , la_textnode        = 65 /**< text node-constructing operator */
    , la_comment         = 66 /**< comment-constructing operator */
    , la_processi        = 67 /**< processing instruction-constr. operator */
    , la_content         = 68 /**< constructor content operator (elem|doc) */
    , la_merge_adjacent  = 69 /**< operator for pf:merge-adjacent-text-nodes
                                   builtin function */
    , la_roots           = 70 /**< algebraic repres. of the roots of newly
                                   created xml nodes (e.g. element());
                                   schema: iter | pos | item */
    /* all operators below represent xml node fragments with no schema */
    , la_fragment        = 71 /**< representation of a node fragment */
    , la_frag_union      = 72 /**< special node type used to form an algebraic
                                   union of fragments */
    , la_empty_frag      = 73 /**< representation of an empty fragment */
                             
    , la_cond_err        = 80 /**< facility to trigger runtime errors */
    , la_nil             = 81 /**< end of the list of parameters */
    , la_trace           = 82 /**< debug operator */
    , la_trace_msg       = 83 /**< debug operator */
    , la_trace_map       = 84 /**< debug relation map operator */
    , la_rec_fix         = 85 /**< operator representing a tail recursion */
    , la_rec_param       = 86 /**< list of parameters of the recursion */
    , la_rec_arg         = 87 /**< reference to the arguments of a parameter
                                   in the recursion */
    , la_rec_base        = 88 /**< base of the DAG describing the recursion */
                         
    , la_proxy           = 96 /**< proxy operator that represents a group
                                   of operators */
    , la_proxy_base      = 97 /**< completes the content of the proxy 
                                   (a virtual base table) */
    , la_cross_mvd       = 98 /**< clone column aware cross product */
    , la_eqjoin_unq      = 99 /**< clone column aware equi-join */
    /* builtin support for XQuery functions */
    , la_string_join     =102 /**< fn:string-join */
                         
    , la_dummy           =120 /**< dummy operator that does nothing */
};
/** algebra operator kinds */
typedef enum PFla_op_kind_t PFla_op_kind_t;

/** semantic content in algebra operators */
union PFla_op_sem_t {

    /* semantic content for serialize operator */
    struct {
        PFalg_att_t     pos;      /**< name of attribute pos */
        PFalg_att_t     item;     /**< name of attribute item */
    } serialize;

    /* semantic content for literal table constr. */
    struct {
        unsigned int    count;    /**< number of tuples */
        PFalg_tuple_t  *tuples;   /**< array holding the tuples */
    } lit_tbl;                    /**< semantic content for literal table
                                       constructor */

    struct {
        PFalg_att_t     res;      /**< names of new attribute */
        PFalg_atom_t    value;    /**< value for the new attribute */
    } attach;                     /**< semantic content for column attachment
                                       operator (ColumnAttach) */

    /* semantic content for equi-join operator */
    struct {
        PFalg_att_t     att1;     /**< name of attribute from "left" rel */
        PFalg_att_t     att2;     /**< name of attribute from "right" rel */
    } eqjoin;

    /* semantic content for clone column aware equi-join operator */
    struct {
        PFalg_att_t     att1;     /**< name of attribute from "left" rel */
        PFalg_att_t     att2;     /**< name of attribute from "right" rel */
        PFalg_att_t     res;      /**< name of result attribute */
    } eqjoin_unq;

    /* semantic content for theta-join operator */
    struct {
        unsigned int    count;    /**< length of predicate list */
        PFalg_sel_t    *pred;     /**< predicate list */
    } thetajoin;

    /* semantic content for theta-join operator 
       (used during thetajoin optimization) */
    struct {
        PFarray_t      *pred;     /**< internal list of predicates
                                       (an extended variant of the
                                        normal semantic content) */
    } thetajoin_opt;

    /* semantic content for projection operator */
    struct {
        unsigned int    count;    /**< length of projection list */
        PFalg_proj_t   *items;    /**< projection list */
    } proj;

    /* semantic content for selection operator */
    struct {
        PFalg_att_t     att;     /**< name of selected attribute */
    } select;

    /* semantic content for generic (row based) function operator */
    struct {
        PFalg_fun_t         kind;  /**< kind of the function */
        PFalg_att_t         res;   /**< attribute to hold the result */
        PFalg_attlist_t     refs;  /**< list of attributes required 
                                        to compute attribute res */
    } fun_1to1;

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

    /* semantic content for op:to operator */
    struct {
        PFalg_att_t     att1;     /**< first operand */
        PFalg_att_t     att2;     /**< second operand */
        PFalg_att_t     part;     /**< partitioning attribute */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } to;

    /*
     * semantic content for operators applying a 
     * (partitioned) aggregation function (count, sum, min, max and avg)
     * on a column
     * or a boolean grouping function (seqty1, all,...)
     */
    struct {
        PFalg_att_t     att;   /**< attribute to be used for the agg. func. */
                                  /* Note that 'att' is ignored by la_count */
        PFalg_att_t     part;  /**< partitioning attribute */
        PFalg_att_t     res;   /**< attribute to hold the result */
    } aggr;

    /* semantic content for rownum operator */
    struct {
        PFalg_att_t     res;      /**< name of generated (integer) attribute */
        PFord_ordering_t sortby;  /**< sort crit. (list of attribute names
                                       and direction) */
        PFalg_att_t     part;     /**< optional partitioning attribute,
                                       otherwise NULL */
    } rownum;

    /* semantic content for rank operator */
    struct {
        PFalg_att_t     res;      /**< name of generated (integer) attribute */
        PFord_ordering_t sortby;  /**< sort crit. (list of attribute names
                                       and direction) */
    } rank;

    /* semantic content for number operator */
    struct {
        PFalg_att_t     res;      /**< name of generated (integer) attribute */
    } number;

    /* semantic content for type test, cast, and type_assert operator */
    struct {
        PFalg_att_t     att;     /**< name of type-tested, casted or type
                                      asserted attribute */
        PFalg_simple_type_t ty;  /**< comparison, cast, and restriction type */
        PFalg_att_t     res;     /**< column to store result of type test 
                                      or cast */
                             /* Note that 'res' is ignored by la_type_assert */
    } type;

    /* store the semantic information for path steps (with guide information) */
    struct {
        PFalg_axis_t     axis;
        PFty_t           ty;
        unsigned int     guide_count;
        PFguide_tree_t **guides;
        int              level;
        PFalg_att_t      iter;     /**< column to look up the iterations */
        PFalg_att_t      item;     /**< column to look up the context nodes */
        PFalg_att_t      item_res; /**< column to store the resulting nodes */
    } step;

    /* store the semantic information for fn:id/fn:idref */
    struct {
        PFalg_att_t     iter;     /**< column to look up the iterations */
        PFalg_att_t     item;     /**< column to look up the context nodes */
        PFalg_att_t     item_res; /**< column to store the resulting nodes */
        PFalg_att_t     item_doc; /**< column to store the fragment info */
    } id;
    
    /* store the column names necessary for document lookup */
    struct {
        PFalg_att_t     iter;     /**< iter column to retain */
        PFalg_att_t     item;     /**< column that contains the references */
        PFalg_att_t     item_res; /**< column to store the document nodes */
    } doc_tbl;

    /* store the column names necessary for document access */
    struct {
        PFalg_att_t     res;      /**< result attribute */
        PFalg_att_t     att;      /**< name of the reference attribute */
        PFalg_doc_t     doc_col;  /**< referenced column in the document */
    } doc_access;

    /* store the column names necessary for a twig root operator */
    /* store the column names necessary for an element constructor */
    /* store the column names necessary for a text constructor */
    /* store the column names necessary for a comment constructor */
    /* semantic content for debug message operator */
    struct {
        PFalg_att_t     iter;     /**< iter column */
        PFalg_att_t     item;     /**< item column */
    } iter_item;

    /* store the column names necessary for a constructor content operator */
    /* semantic content for debug operator */
    struct {
        PFalg_att_t     iter;      /**< name of iter column */
        PFalg_att_t     pos;       /**< name of pos column */
        PFalg_att_t     item;      /**< name of item column */
    } iter_pos_item;

    /* store the column names necessary for an attribute constructor */
    /* store the column names necessary for a pi constructor */
    struct {
        PFalg_att_t     iter;     /**< name of the iter column */
        PFalg_att_t     item1;    /**< name of the first item column */
        PFalg_att_t     item2;    /**< name of the second item column */
    } iter_item1_item2;

    /* store the column names necessary for a document constructor */
    struct {
        PFalg_att_t     iter;     /**< iter column of the doc relation */
    } docnode;

    /* store the column names necessary for a merge_adjacent operator */
    struct {
        PFalg_att_t     iter_in;  /**< iter column of input relation */
        PFalg_att_t     pos_in;   /**< pos column of input relation */
        PFalg_att_t     item_in;  /**< item column of input relation */
        PFalg_att_t     iter_res; /**< iter column of result relation */
        PFalg_att_t     pos_res;  /**< pos column of result relation */
        PFalg_att_t     item_res; /**< item column of result relation */
    } merge_adjacent;

    /* semantic content for conditional error */
    struct {
        PFalg_att_t     att;     /**< name of the boolean attribute */
        char *          str;     /**< error message */
    } err;

    /* semantic content for debug relation map operator */
    struct {
        PFalg_att_t      inner;    /**< name of the inner column */
        PFalg_att_t      outer;    /**< name of the outer column */
    } trace_map;

    /* semantic content for an argument of a recursion parameter */
    struct {
        PFla_op_t      *base;    /**< reference to the base relation
                                      of the recursion */
    } rec_arg;

    /* semantic content for proxy nodes */
    struct {
        unsigned int    kind;      /**< proxy kind */
        PFla_op_t      *ref;       /**< reference a certain operator in the
                                        proxy body */
        PFla_op_t      *base1;     /**< the leafs first child */
        PFla_op_t      *base2;     /**< the leafs second child */
        PFalg_attlist_t req_cols;  /**< list of columns required
                                        to evaluate proxy */
        PFalg_attlist_t new_cols;  /**< list of new generated columns */
    } proxy;

    /* store the column names necessary for a string_join operator */
    struct {
        PFalg_att_t     iter;     /**< iter column of string relation */
        PFalg_att_t     pos;      /**< pos column of string relation */
        PFalg_att_t     item;     /**< item column of string relation */
        PFalg_att_t     iter_sep; /**< iter column of separator relation */
        PFalg_att_t     item_sep; /**< item column of separator relation */
        PFalg_att_t     iter_res; /**< iter column of result relation */
        PFalg_att_t     item_res; /**< item column of result relation */
    } string_join;
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

    unsigned short     state_label;/**< Burg puts its state information here. */
    short              child_state_label[PFLA_OP_MAXCHILD];
                                   /**< control information for Burg pattern
                                        matching */

    unsigned           bit_reset:1;/**< used to reset the dag bit
                                             in a DAG traversal */
    unsigned           bit_dag:1;  /**< enables DAG traversal */
    unsigned           bit_in:1;   /**< indicates that node is part
                                        of a proxy node */
    unsigned           bit_out:1;  /**< indicates that node is not part
                                        of a proxy node */
    unsigned int       refctr;     /**< indicates the incoming edges of
                                        each node */

    PFplanlist_t      *plans;      /**< Physical algebra plans that implement
                                        this logical algebra subexpression. */
    PFsql_alg_ann_t   *sql_ann;    /**< SQL annotations used during SQL code
                                        generation. */
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
 * An operator that duplicates a given node and uses the
 * the nodes in @a left and @a right as its children.
 */
PFla_op_t * PFla_op_duplicate (PFla_op_t *n, PFla_op_t *left,
                               PFla_op_t *right);

/**
 * An operator that duplicates a given node.
 */
#define PFla_op_clone(a) PFla_op_duplicate((a),(a)->child[0],(a)->child[1])

/**
 * A dummy operator that is generated whenever some rewrite 
 * throws away an operator (e.g., '*p = *L(p);') and the replacement
 * is an already existing node that may not be split into multiple 
 * operators (e.g. a number operator).
 */
PFla_op_t * PFla_dummy (PFla_op_t *n);

/**
 * A `serialize' node will be placed on the very top of the algebra
 * expression tree.
 */
PFla_op_t * PFla_serialize (const PFla_op_t *doc, const PFla_op_t *alg,
                            PFalg_att_t pos, PFalg_att_t item);

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
PFla_op_t *PFla_empty_tbl_ (PFalg_schema_t s);

PFla_op_t *PFla_attach (const PFla_op_t *n,
                        PFalg_att_t res, PFalg_atom_t value);

/**
 * Cross product (Cartesian product) of two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_cross (const PFla_op_t *n1, const PFla_op_t *n2);

/**
 * Cross product (Cartesian product) of two relations.
 * Duplicate attribute names allowed.
 */
PFla_op_t * PFla_cross_clone (const PFla_op_t *n1, const PFla_op_t *n2);


/**
 * Equi-join between two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_eqjoin (const PFla_op_t *n1, const PFla_op_t *n2,
                         PFalg_att_t att1, PFalg_att_t att2);

/**
 * Semi-join between two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_semijoin (const PFla_op_t *n1, const PFla_op_t *n2,
                           PFalg_att_t att1, PFalg_att_t att2);

/**
 * Theta-join between two relations.
 * No duplicate attribute names allowed.
 */
PFla_op_t * PFla_thetajoin (const PFla_op_t *n1, const PFla_op_t *n2,
                            unsigned int count, PFalg_sel_t *sellist);

/**
 * Theta-join between two relations.
 * No duplicate attribute names allowed.
 * Special internal variant used during thetajoin optimization.
 */
PFla_op_t * PFla_thetajoin_opt_internal (const PFla_op_t *n1,
                                         const PFla_op_t *n2,
                                         PFarray_t *data);

/**
 * Equi-join between two relations.
 * Duplicate attribute names allowed.
 */
PFla_op_t * PFla_eqjoin_clone (const PFla_op_t *n1, const PFla_op_t *n2,
                               PFalg_att_t att1, PFalg_att_t att2,
                               PFalg_att_t res);

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

/** Constructor for selection of not-0 column values. */
PFla_op_t * PFla_select (const PFla_op_t *n, PFalg_att_t att);

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

/** Constructor for duplicate elimination operator. */
PFla_op_t * PFla_distinct (const PFla_op_t *n);

/** Constructor for generic operator that extends the schema 
    with a new column where each value is determined by the values
    of a single row (cardinality stays the same) */
PFla_op_t * PFla_fun_1to1 (const PFla_op_t *n, PFalg_fun_t kind,
                           PFalg_att_t res, PFalg_attlist_t refs);

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

/** Constructor for boolean AND operators. */
PFla_op_t * PFla_and (const PFla_op_t *n, PFalg_att_t res,
                      PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean OR operators. */
PFla_op_t * PFla_or (const PFla_op_t *n, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean NOT operators. */
PFla_op_t * PFla_not (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t att);

/** Constructor for op:to operator. */
PFla_op_t * PFla_to (const PFla_op_t *n, 
                     PFalg_att_t res,
                     PFalg_att_t att1,
                     PFalg_att_t att2,
                     PFalg_att_t part);

/** 
 * Constructor for operators forming the application of a 
 * (partitioned) aggregation function (sum, min, max and avg) on a column.
 */
PFla_op_t * PFla_aggr (PFla_op_kind_t kind, const PFla_op_t *n,
		       PFalg_att_t res, PFalg_att_t att, PFalg_att_t part);

/** Constructor for (partitioned) row counting operators. */
PFla_op_t * PFla_count (const PFla_op_t *n, PFalg_att_t res,
                        PFalg_att_t part);

/** Constructor for the row numbering operator. */
PFla_op_t * PFla_rownum (const PFla_op_t *n, PFalg_att_t a,
                         PFord_ordering_t s, PFalg_att_t p);

/** Constructor for the ranking operator. */
PFla_op_t * PFla_rank (const PFla_op_t *n, PFalg_att_t a,
                       PFord_ordering_t s);

/** Constructor for the numbering operator. */
PFla_op_t * PFla_number (const PFla_op_t *n, PFalg_att_t a);

/**
 * Constructor for type test of column values. The result is
 * stored in newly created column.
 */
PFla_op_t * PFla_type (const PFla_op_t *n, PFalg_att_t res,
                       PFalg_att_t att, PFalg_simple_type_t ty);

/**
 * Constructor for type assertion check. The result is the
 * input relation n where the type of attribute att is replaced
 * by ty
 */
PFla_op_t * PFla_type_assert (const PFla_op_t *n, PFalg_att_t att,
                              PFalg_simple_type_t ty, bool pos);

/**
 * Constructor for the type cast of a column.
 */
PFla_op_t * PFla_cast (const PFla_op_t *n, PFalg_att_t res, PFalg_att_t att,
                       PFalg_simple_type_t ty);

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

/**
 * Constructor for XPath step evaluation.
 */
PFla_op_t * PFla_step_simple (const PFla_op_t *doc, const PFla_op_t *n,
                              PFalg_axis_t axis, PFty_t seqty,
                              PFalg_att_t iter, PFalg_att_t item,
                              PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation.
 */
PFla_op_t * PFla_step (const PFla_op_t *doc, const PFla_op_t *n,
                       PFalg_axis_t axis, PFty_t seqty, int level,
                       PFalg_att_t iter, PFalg_att_t item,
                       PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (without duplicate removal).
 */
PFla_op_t * PFla_step_join_simple (const PFla_op_t *doc, const PFla_op_t *n,
                                   PFalg_axis_t axis, PFty_t seqty,
                                   PFalg_att_t item,
                                   PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (without duplicate removal).
 */
PFla_op_t * PFla_step_join (const PFla_op_t *doc, const PFla_op_t *n,
                            PFalg_axis_t axis, PFty_t seqty, int level,
                            PFalg_att_t item,
                            PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (with guide information).
 */
PFla_op_t * PFla_guide_step_simple (const PFla_op_t *doc, const PFla_op_t *n,
                                    PFalg_axis_t axis, PFty_t seqty,
                                    unsigned int guide_count,
                                    PFguide_tree_t **guides,
                                    PFalg_att_t iter, PFalg_att_t item,
                                    PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (with guide information).
 */
PFla_op_t * PFla_guide_step (const PFla_op_t *doc, const PFla_op_t *n,
                             PFalg_axis_t axis, PFty_t seqty,
                             unsigned int guide_count,
                             PFguide_tree_t **guides, int level,
                             PFalg_att_t iter, PFalg_att_t item,
                             PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (without duplicate removal and
 * with guide information).
 */
PFla_op_t * PFla_guide_step_join_simple (const PFla_op_t *doc,
                                         const PFla_op_t *n,
                                         PFalg_axis_t axis, PFty_t seqty,
                                         unsigned int guide_count,
                                         PFguide_tree_t **guides,
                                         PFalg_att_t item,
                                         PFalg_att_t item_res);

/**
 * Constructor for XPath step evaluation (without duplicate removal and
 * with guide information).
 */
PFla_op_t * PFla_guide_step_join (const PFla_op_t *doc, const PFla_op_t *n,
                                  PFalg_axis_t axis, PFty_t seqty,
                                  unsigned int guide_count,
                                  PFguide_tree_t **guides, int level,
                                  PFalg_att_t item,
                                  PFalg_att_t item_res);

/**
 * Constructor for fn:id evaluation.
 */
PFla_op_t * PFla_id (const PFla_op_t *doc, const PFla_op_t *n,
                     PFalg_att_t iter, PFalg_att_t item,
                     PFalg_att_t item_res, PFalg_att_t item_doc);

/**
 * Constructor for fn:idref evaluation.
 */
PFla_op_t * PFla_idref (const PFla_op_t *doc, const PFla_op_t *n,
                        PFalg_att_t iter, PFalg_att_t item,
                        PFalg_att_t item_res, PFalg_att_t item_doc);

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

/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.  Returns a (frag, result) pair.
 */
PFla_op_t * PFla_doc_tbl (const PFla_op_t *rel, 
                          PFalg_att_t iter, PFalg_att_t item,
                          PFalg_att_t item_res);

/** Constructor for string access of loaded documents */
PFla_op_t * PFla_doc_access (const PFla_op_t *doc,
                             const PFla_op_t *n,
                             PFalg_att_t res,
                             PFalg_att_t col,
                             PFalg_doc_t doc_col);

/** Constructor for twig root operators. */
PFla_op_t * PFla_twig (const PFla_op_t *twig,
                       PFalg_att_t iter,
                       PFalg_att_t item);

/** Constructor for twig constructor sequence operators. */
PFla_op_t * PFla_fcns (const PFla_op_t *fc,
                       const PFla_op_t *ns);
                          
/** Constructor for document node operators. */
PFla_op_t * PFla_docnode (const PFla_op_t *scope,
                          const PFla_op_t *fcns,
                          PFalg_att_t iter);

/** Constructor for element operators. */
PFla_op_t * PFla_element (const PFla_op_t *tags,
                          const PFla_op_t *fcns,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/** Constructor for attribute operators. */
PFla_op_t * PFla_attribute (const PFla_op_t *cont,
                            PFalg_att_t iter,
                            PFalg_att_t qn,
                            PFalg_att_t val);

/** Constructor for text node operators. */
PFla_op_t * PFla_textnode (const PFla_op_t *cont, 
                           PFalg_att_t iter,
                           PFalg_att_t item);

/** Constructor for comment operators. */
PFla_op_t * PFla_comment (const PFla_op_t *cont,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/** Constructor for processing instruction operators. */
PFla_op_t * PFla_processi (const PFla_op_t *cont,
                           PFalg_att_t iter,
                           PFalg_att_t target,
                           PFalg_att_t val);

/** Constructor for constructor content operators (elem|doc). */
PFla_op_t * PFla_content (const PFla_op_t *doc,
                          const PFla_op_t *cont,
                          PFalg_att_t iter,
                          PFalg_att_t pos,
                          PFalg_att_t item);

/** Constructor for pf:merge-adjacent-text-nodes() functionality */
PFla_op_t * PFla_pf_merge_adjacent_text_nodes (const PFla_op_t *doc,
                                               const PFla_op_t *cont,
                                               PFalg_att_t iter_in,
                                               PFalg_att_t pos_in,
                                               PFalg_att_t item_in,
                                               PFalg_att_t iter_res,
                                               PFalg_att_t pos_res,
                                               PFalg_att_t item_res);

/**
 * Constructor required for fs:item-sequence-to-node-sequence()
 * functionality
 */
PFla_op_t * PFla_pos_merge_str (const PFla_op_t *n);

/**************** document fragment related stuff ******************/

/**
 * Extract the expression result part from a (frag, result) pair.
 * The result of this algebra operator is a relation with schema
 * iter | item or iter | pos | item (merge_adjacent).
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
 * Constructor for conditional error
 */
PFla_op_t * PFla_cond_err (const PFla_op_t *n, const PFla_op_t *err,
                           PFalg_att_t att, char *err_string);

/**
 * Constructor for the last item of a parameter list
 */
PFla_op_t *PFla_nil (void);

/**
 * Constructor for debug operator
 */
PFla_op_t * PFla_trace (const PFla_op_t *n1,
                        const PFla_op_t *n2,
                        PFalg_att_t iter,
                        PFalg_att_t pos,
                        PFalg_att_t item);

/**
 * Constructor for debug message operator
 */
PFla_op_t * PFla_trace_msg (const PFla_op_t *n1,
                            const PFla_op_t *n2,
                            PFalg_att_t iter,
                            PFalg_att_t item);

/**
 * Constructor for debug relation map operator
 * (A set of the trace_map operators link a trace operator
 * to the correct scope.)
 */
PFla_op_t * PFla_trace_map (const PFla_op_t *n1,
                            const PFla_op_t *n2,
                            PFalg_att_t      inner,
                            PFalg_att_t      outer);

/**
 * Constructor for a tail recursion operator
 */
PFla_op_t *PFla_rec_fix (const PFla_op_t *paramList,
                         const PFla_op_t *res);

/**
 * Constructor for a list item of a parameter list
 * related to recursion
 */
PFla_op_t *PFla_rec_param (const PFla_op_t *arguments,
                           const PFla_op_t *paramList);

/**
 * Constructor for the arguments of a parameter (seed and recursion
 * will be the input relations for the base operator)
 */
PFla_op_t *PFla_rec_arg (const PFla_op_t *seed,
                         const PFla_op_t *recursion,
                         const PFla_op_t *base);

/**
 * Constructor for the base relation in a recursion (-- a dummy
 * operator representing the seed relation as well as the argument
 * computed in the recursion).
 */
PFla_op_t *PFla_rec_base (PFalg_schema_t schema);

/****************************************************************/

/**
 * Constructor for a proxy operator with a single child
 */
PFla_op_t *PFla_proxy (const PFla_op_t *n, unsigned int kind,
                       PFla_op_t *ref, PFla_op_t *base,
                       PFalg_attlist_t new_cols, PFalg_attlist_t req_cols);

/**
 * Constructor for a proxy operator with a two children
 */
PFla_op_t *PFla_proxy2 (const PFla_op_t *n, unsigned int kind,
                       PFla_op_t *ref, PFla_op_t *base1, PFla_op_t *base2,
                       PFalg_attlist_t new_cols, PFalg_attlist_t req_cols);

/**
 * Constructor for a proxy base operator
 */
PFla_op_t *PFla_proxy_base (const PFla_op_t *n);

/****************** built-in functions **************************/

/**
 * Constructor for builtin function fn:string-join
 */
PFla_op_t * PFla_fn_string_join (
    const PFla_op_t *text, const PFla_op_t *sep,
    PFalg_att_t iter, PFalg_att_t pos, PFalg_att_t item,
    PFalg_att_t iter_sep, PFalg_att_t item_sep,
    PFalg_att_t iter_res, PFalg_att_t item_res);

#endif  /* LOGICAL_H */

/* vim:set shiftwidth=4 expandtab: */
