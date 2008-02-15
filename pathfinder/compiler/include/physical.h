/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations specific to physical algebra.
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef PHYSICAL_H
#define PHYSICAL_H

#include "variable.h"
#include "algebra.h"

#include "ordering.h"


/** algebra operator node */
typedef struct PFpa_op_t PFpa_op_t;

/* .............. algebra operators (operators on relations) .............. */

/** algebra operator kinds */
enum PFpa_op_kind_t {
      pa_serialize      =   1
    , pa_lit_tbl        =   2 /**< literal table */
    , pa_empty_tbl      =   3 /**< empty literal table */
    , pa_attach         =   4 /**< ColumnAttach */
    , pa_cross          =  10 /**< Cross */
    , pa_leftjoin       =  11 /**< LeftJoin */
    , pa_eqjoin         =  14 /**< Generic join implementation */
    , pa_semijoin       =  15 /**< Semijoin implementation */
    , pa_thetajoin      =  16 /**< Thetajoin implementation */
    , pa_unq2_thetajoin =  17 /**< Thetajoin implementation */
    , pa_unq1_thetajoin =  18 /**< Thetajoin implementation */
    , pa_project        =  19 /**< Project */
    , pa_select         =  20 /**< Select: filter true rows in given att */
    , pa_val_select     =  21 /**< Select: filter rows by value in given att */
    , pa_append_union   =  23 /**< AppendUnion */
    , pa_merge_union    =  24 /**< MergeUnion */
    , pa_intersect      =  25 /**< Intersect */
    , pa_difference     =  26 /**< Difference */
    , pa_sort_distinct  =  27 /**< SortDistinct */
    , pa_std_sort       =  28 /**< StdSort */
    , pa_refine_sort    =  29 /**< RefineSort */
    , pa_fun_1to1       =  30 /**< generic operator that extends the schema with
                                   a new column where each value is determined 
                                   by the values of a single row (cardinality 
                                   stays the same) */
    , pa_eq             =  40 /**< Numeric or String Equality */
    , pa_gt             =  42 /**< Numeric or String GreaterThan */
    , pa_bool_not       =  45 /**< Boolean negation */
    , pa_bool_and       =  46 /**< Boolean and */
    , pa_bool_or        =  47 /**< Boolean or */
    , pa_to             =  50 /**< op:to operator */
    , pa_count_ext      =  54 /**< Count operator with loop backup
                                   for empty values*/
    , pa_count          =  55 /**< Count operator */
    , pa_avg            =  56 /**< Avg operator */
    , pa_max            =  57 /**< Max operator */
    , pa_min            =  58 /**< Min operator */    
    , pa_sum            =  59 /**< Sum operator */
    , pa_mark           =  60 /**< consecutive numbering operator 
                                   (starting from 1) */
    , pa_rank           =  61 /**< ranking operator */
    , pa_mark_grp       =  62 /**< grouped consecutive numbering operator
                                   (starting from 1) */
    , pa_type           =  63 /**< selection of rows where a column is of a
                                   certain type */
    , pa_type_assert    =  64 /**< restriction of the type of a given column */
    , pa_cast           =  65 /**< cast a table to a given type */
    , pa_llscjoin       = 100 /**< Loop-Lifted StaircaseJoin */
    , pa_doc_tbl        = 120 /**< Access to persistent document relation */
    , pa_doc_access     = 121 /**< Access to string content of loaded docs */
    , pa_twig           = 122 /**< twig root operator */
    , pa_fcns           = 123 /**< twig constructor sequence */
    , pa_docnode        = 124 /**< document node-constructing operator */
    , pa_element        = 125 /**< element-constructing operator */
    , pa_attribute      = 126 /**< attribute-constructing operator */
    , pa_textnode       = 127 /**< text node-constructing operator */
    , pa_comment        = 128 /**< comment-constructing operator */
    , pa_processi       = 129 /**< processing instruction-constr. operator */
    , pa_content        = 130 /**< constructor content operator (elem|doc) */
    , pa_slim_content   = 131 /**< shallow constructor content operator */
    , pa_merge_adjacent = 132
    , pa_error          = 139 /**< error operator */
    , pa_cond_err       = 140 /**< conditional error operator */
    , pa_nil            = 141 /**< end of the list of parameters */
    , pa_trace          = 142 /**< debug operator */
    , pa_trace_msg      = 143 /**< debug message operator */
    , pa_trace_map      = 144 /**< debug relation map operator */
    , pa_rec_fix        = 145 /**< operator representing a tail recursion */
    , pa_rec_param      = 146 /**< list of parameters of the recursion */
    , pa_rec_arg        = 147 /**< reference to the arguments of a parameter
                                  in the recursion */
    , pa_rec_base       = 148 /**< base of the DAG describing the recursion */
    , pa_rec_border     = 149 /**< border of the DAG describing the recursion */
    , pa_fun_call       = 150 /**< function application */
    , pa_fun_param      = 151 /**< function application parameter */
    , pa_string_join    = 160 /**< Concatenation of multiple strings */
};
/** algebra operator kinds */
typedef enum PFpa_op_kind_t PFpa_op_kind_t;

/** semantic content in algebra operators */
union PFpa_op_sem_t {

    /* semantic content for serialize operator */
    struct {
        PFalg_att_t     item;     /**< name of attribute item */
    } serialize;
    
    /* semantic content for literal table constr. */
    struct {
        unsigned int    count;    /**< number of tuples */
        PFalg_tuple_t  *tuples;   /**< array holding the tuples */
    } lit_tbl;                    /**< semantic content for literal table
                                       constructor */

    struct {
        PFalg_att_t     attname;  /**< names of new attribute */
        PFalg_atom_t    value;    /**< value for the new attribute */
    } attach;                     /**< semantic content for column attachment
                                       operator (ColumnAttach) */

    /* semantic content for equi-join operator */
    struct {
        PFalg_att_t     att1;     /**< name of attribute from "left" rel */
        PFalg_att_t     att2;     /**< name of attribute from "right" rel */
    } eqjoin;

    /* semantic content for theta-join operator */
    struct {
        unsigned int    count;    /**< length of predicate list */
        PFalg_sel_t    *pred;     /**< predicate list */
    } thetajoin;

    /* semantic content for theta-join operator */
    struct {
        PFalg_comp_t    comp;     /**< comparison */
        PFalg_att_t     left;     /**< name of attribute from "left" rel */
        PFalg_att_t     right;    /**< name of attribute from "right" rel */
        PFalg_att_t     ldist;    /**< name of distinct attribute
                                       from "left" rel */
        PFalg_att_t     rdist;    /**< name of distinct attribute
                                       from "right" rel */
    } unq_thetajoin;

    /* semantic content for projection operator */
    struct {
        unsigned int    count;    /**< length of projection list */
        PFalg_proj_t   *items;    /**< projection list */
    } proj;

    /** semantic content for selection operator */
    struct {
        PFalg_att_t     att;     /**< name of selected attribute */
    } select;

    struct {
        PFord_ordering_t ord;     /**< ``grouping'' parameter for
                                       MergeUnion */
    } merge_union;

    /** semantic content for SortDistinct operator */
    struct {
        PFord_ordering_t ord;    /**< ordering to consider for duplicate
                                      elimination */
    } sort_distinct;

    /** semantic content for sort operators */
    struct {
        PFord_ordering_t required;
        PFord_ordering_t existing;
    } sortby;

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

    /**
     * semantic content for unary (numeric or Boolean) operators
     * (e.g. Boolean negation)
     */
    struct {
        PFalg_att_t     att;      /**< argument attribute */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } unary;

    /** semantic content for Count operators */
    struct {
        PFalg_att_t         res;  /**< Name of result attribute */
        PFalg_att_t         part; /**< Partitioning attribute */
        PFalg_att_t         loop; /**< loop attribute */
    } count;

    /*
     * semantic content for operators applying a 
     * (partitioned) aggregation function (sum, min, max and avg) on a column
     */
    struct {
        PFalg_att_t     att;  /**< attribute to be used for the agg. func. */
        PFalg_att_t     part; /**< partitioning attribute */
        PFalg_att_t     res;  /**< attribute to hold the result */
    } aggr;

    /* semantic content for mark operators */
    struct {
        PFalg_att_t     res;      /**< name of generated (integer) attribute */
        PFalg_att_t     part;     /**< optional partitioning attribute,
                                       otherwise NULL */
    } mark;

    /** semantic content for rank operator */
    struct {
        PFalg_att_t      res;     /**< name of generated (integer) attribute */
        PFord_ordering_t ord;    /**< ordering to consider for duplicate
                                      elimination */
    } rank;

    /* semantic content for type test operator */
    struct {
        PFalg_att_t     att;     /**< name of type-tested attribute */
        PFalg_simple_type_t ty;  /**< comparison type */
        PFalg_att_t     res;     /**< column to store result of type test */
    } type;

    /* semantic content for type_assert operator */
    struct {
        PFalg_att_t     att;     /**< name of the asserted attribute */
        PFalg_simple_type_t ty;  /**< restricted type */
    } type_a;

    /** semantic content for Cast operator */
    struct {
        PFalg_att_t         att; /**< attribute to cast */
        PFalg_simple_type_t ty;  /**< target type */
        PFalg_att_t         res; /**< column to store result of the cast */
    } cast;

    /** semantic content for staircase join operator */
    struct {
        PFalg_axis_t     axis;
        PFty_t           ty;      /**< sequence type that describes the
                                       node test */
        PFord_ordering_t in;      /**< input ordering */
        PFord_ordering_t out;     /**< output ordering */
        PFalg_att_t      iter;    /**< iter column */
        PFalg_att_t      item;    /**< item column */
    } scjoin;

    /* store the column names necessary for a twig root operator */
    /* store the column names necessary for a document constructor */
    /* store the column names necessary for an element constructor */
    /* store the column names necessary for a text constructor */
    /* store the column names necessary for a comment constructor */
    /* store the column names necessary for a constructor content operator */
    /* semantic content for debug operator */
    /* semantic content for debug message operator */
    struct {
        PFalg_att_t     iter;     /**< iter column */
        PFalg_att_t     item;     /**< item column */
    } ii;

    /* store the column names necessary for document access */
    struct {
        PFalg_att_t     res;      /**< attribute to hold the result */
        PFalg_att_t     att;      /**< name of the reference attribute */
        PFalg_doc_t     doc_col;  /**< referenced column in the document */
    } doc_access;

    /* store the column names necessary for an attribute constructor */
    /* store the column names necessary for a pi constructor */
    struct {
        PFalg_att_t     iter;     /**< name of the iter column */
        PFalg_att_t     item1;    /**< name of the first item column */
        PFalg_att_t     item2;    /**< name of the second item column */
    } iter_item1_item2;

    /* semantic content for error and conditional error */
    struct {
        PFalg_att_t     att;      /**< error:      column of error message
                                       cond_error: name of the bool column */
        char *          str;      /**< error message (only used by cond_err) */
    } err;
    
    /* semantic content for debug relation map operator */
    struct {
        PFalg_att_t      inner;    /**< name of the inner column */
        PFalg_att_t      outer;    /**< name of the outer column */
        unsigned int     trace_id; /**< the trace identifier 
                                        (used in milgen.brg) */
    } trace_map;

    /* semantic content for an argument of a recursion parameter */
    struct {
        PFpa_op_t      *base;    /**< reference to the base relation
                                      of the recursion */
    } rec_arg;

    struct {
        PFalg_fun_call_t kind;    /**< kind of function call */
        PFqname_t       qname;    /**< function name */
        void           *ctx;      /**< reference to the context node
                                       representing the function call */
        PFalg_att_t     iter;     /**< the loop relation */
        PFalg_occ_ind_t occ_ind;  /**< occurrence indicator for the
                                       iter column of the result
                                       (used for optimizations) */
    } fun_call;
};
/** semantic content in physical algebra operators */
typedef union PFpa_op_sem_t PFpa_op_sem_t;

/**
 * A ``plan list'' is an array of plans.
 */
typedef PFarray_t PFplanlist_t;


/** maximum number of children of a #PFpa_op_t node */
#define PFPA_OP_MAXCHILD 2

/** algebra operator node */
struct PFpa_op_t {
    PFpa_op_kind_t     kind;       /**< operator kind */
    PFpa_op_sem_t      sem;        /**< semantic content for this operator */
    PFalg_schema_t     schema;     /**< result schema */

    PFord_set_t        orderings;
    unsigned long      cost;       /**< costs estimated for this subexpress. */
    struct PFprop_t   *prop;

    PFarray_t         *env;        /**< environment to store the corresponding
                                        MIL variable bindings (see milgen.brg)
                                        */
    short              state_label;/**< Burg puts its state information here. */

    unsigned           bit_reset:1;/**< used to reset the dag bit
                                             in a DAG traversal */
    unsigned           bit_dag:1;  /**< enables DAG traversal */
    unsigned           bit_in:1;   /**< indicates that node is part
                                        of a recursion body */
    
    struct PFpa_op_t  *child[PFPA_OP_MAXCHILD];
    unsigned int       refctr;
    int                node_id;    /**< specifies the id of this operator
                                        node; required exclusively to
                                        create dot output. */
};



/* ***************** Constructors ******************* */

/**
 * A `serialize' node will be placed on the very top of the algebra
 * expression tree.
 */
PFpa_op_t * PFpa_serialize (const PFpa_op_t *alg, PFalg_att_t item);

/****************************************************************/

PFpa_op_t *PFpa_lit_tbl (PFalg_attlist_t attlist,
                         unsigned int count, PFalg_tuple_t *tuples);

/**
 * Empty table constructor.  Use this instead of an empty table
 * without any tuples to facilitate optimization.
 */
PFpa_op_t *PFpa_empty_tbl (PFalg_schema_t schema);

PFpa_op_t *PFpa_attach (const PFpa_op_t *n,
                        PFalg_att_t attname, PFalg_atom_t value);

/**
 * Cross product (Cartesian product) of two relations.
 *
 * Cross product is defined as the result of
 *  
 *@verbatim
    foreach $a in a
      foreach $b in b
        return ($a, $b) .
@endverbatim
 *
 * That is, the left operand is in the *outer* loop.
 */
PFpa_op_t * PFpa_cross (const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * LeftJoin: Equi-Join on two relations, preserving the ordering
 *           of the left operand.
 */
PFpa_op_t *
PFpa_leftjoin (PFalg_att_t att1, PFalg_att_t att2,
               const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * EqJoin: Equi-Join. Does not provide any ordering guarantees.
 */
PFpa_op_t *
PFpa_eqjoin (PFalg_att_t att1, PFalg_att_t att2,
             const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * SemiJoin: Semi-Join. Preserves the order of the left argument.
 */
PFpa_op_t *
PFpa_semijoin (PFalg_att_t att1, PFalg_att_t att2,
               const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * ThetaJoin: Theta-Join. Does not provide any ordering guarantees.
 */
PFpa_op_t *
PFpa_thetajoin (const PFpa_op_t *n1, const PFpa_op_t *n2,
                unsigned int count, PFalg_sel_t *pred);

/**
 * ThetaJoin: Theta-Join. Returns a two columns (from n1 and n2)
 *            with duplicates eliminated. Preserves the order 
 *            of the left argument.
 */
PFpa_op_t *
PFpa_unq2_thetajoin (PFalg_comp_t comp, PFalg_att_t left, PFalg_att_t right,
                     PFalg_att_t ldist, PFalg_att_t rdist,
                     const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * ThetaJoin: Theta-Join. Returns a single column with duplicates
 *            eliminated. Preserves the order of the left argument.
 */
PFpa_op_t *
PFpa_unq1_thetajoin (PFalg_comp_t comp, PFalg_att_t left, PFalg_att_t right,
                     PFalg_att_t ldist, PFalg_att_t rdist,
                     const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * Project.
 *
 * Note that projection does @b not eliminate duplicates. If you
 * need duplicate elimination, explictly use a Distinct operator.
 */
PFpa_op_t *PFpa_project (const PFpa_op_t *n, unsigned int count,
                         PFalg_proj_t *proj);

/**
 * Select: Filter rows by Boolean value in attribute @a att
 */
PFpa_op_t *PFpa_select (const PFpa_op_t *n, PFalg_att_t att);

/**
 * Select: Filter rows by given value @a value in attribute @a att
 */
PFpa_op_t *PFpa_value_select (const PFpa_op_t *n,
                              PFalg_att_t att, PFalg_atom_t value);

/**
 * Construct AppendUnion operator node.
 *
 * AppendUnion simply appends relation @a b to @a a. It does not
 * require any specific order. The output has an order that is
 * typically not really useful.
 */
PFpa_op_t *PFpa_append_union (const PFpa_op_t *, const PFpa_op_t *);

PFpa_op_t *PFpa_merge_union (const PFpa_op_t *, const PFpa_op_t *,
                             PFord_ordering_t);

/**
 * Intersect: No specialized implementations here; always applicable.
 */
PFpa_op_t *PFpa_intersect (const PFpa_op_t *, const PFpa_op_t *);

/**
 * Difference: No specialized implementations here; always applicable.
 */
PFpa_op_t *PFpa_difference (const PFpa_op_t *, const PFpa_op_t *);

/**
 * SortDistinct: Eliminate duplicate tuples.
 *
 * Requires the input to be fully sorted.  Specify this ordering
 * in the second argument (should actually be redundant, but we
 * keep it anyway).
 */
PFpa_op_t *PFpa_sort_distinct (const PFpa_op_t *, PFord_ordering_t);

/**
 * StandardSort: Introduce given sort order as the only new order.
 *
 * Does neither benefit from any existing sort order, nor preserve
 * any such order.  Is thus always applicable.  A possible implementation
 * could be QuickSort.
 */
PFpa_op_t *PFpa_std_sort (const PFpa_op_t *, PFord_ordering_t);

/**
 * RefineSort: Introduce new ordering, but benefit from existing ordering.
 */
PFpa_op_t *PFpa_refine_sort (const PFpa_op_t *,
                             PFord_ordering_t, PFord_ordering_t);

/****************************************************************/

/** Constructor for generic operator that extends the schema 
    with a new column where each value is determined by the values
    of a single row (cardinality stays the same) */
PFpa_op_t * PFpa_fun_1to1 (const PFpa_op_t *n, PFalg_fun_t kind,
                           PFalg_att_t res, PFalg_attlist_t refs);

/**
 * Comparison operator eq.
 */
PFpa_op_t *PFpa_eq (const PFpa_op_t *, PFalg_att_t res,
                    PFalg_att_t att1, PFalg_att_t att2);

/**
 * Comparison operator gt.
 */
PFpa_op_t *PFpa_gt (const PFpa_op_t *, PFalg_att_t res,
                    PFalg_att_t att1, PFalg_att_t att2);

/**
 * Boolean negation
 */
PFpa_op_t *PFpa_bool_not (const PFpa_op_t *,
                          PFalg_att_t res, PFalg_att_t att);

/**
 * Boolean and.
 */
PFpa_op_t *PFpa_and (const PFpa_op_t *, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/**
 * Boolean or.
 */
PFpa_op_t *PFpa_or (const PFpa_op_t *, PFalg_att_t res,
                    PFalg_att_t att1, PFalg_att_t att2);

/**
 * Constructor for op:to operator
 */
PFpa_op_t * PFpa_to (const PFpa_op_t *n, PFalg_att_t res,
                     PFalg_att_t att1, PFalg_att_t att2);

/**
 * Count: Count function operator with a loop relation to
 * correctly fill in missing values. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_count_ext (const PFpa_op_t *, const PFpa_op_t *,
                           PFalg_att_t, PFalg_att_t, PFalg_att_t);

/**
 * Count: Count function operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_count (const PFpa_op_t *,
                       PFalg_att_t, PFalg_att_t);

/**
 * Aggr: Aggregation function operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_aggr (PFpa_op_kind_t kind, const PFpa_op_t *n, PFalg_att_t res, 
		     PFalg_att_t att, PFalg_att_t part);

/****************************************************************/

PFpa_op_t *PFpa_mark (const PFpa_op_t *n, PFalg_att_t new_att);
PFpa_op_t *PFpa_rank (const PFpa_op_t *n, PFalg_att_t new_att,
                      PFord_ordering_t ord);
PFpa_op_t *PFpa_mark_grp (const PFpa_op_t *n,
                          PFalg_att_t new_att,
                          PFalg_att_t part);

/**
 * Type operator
 */
PFpa_op_t *PFpa_type (const PFpa_op_t *,
                      PFalg_att_t,
                      PFalg_simple_type_t, PFalg_att_t);

/**
 * Constructor for type assertion check. The result is the
 * input relation n where the type of attribute att is replaced
 * by ty
 */
PFpa_op_t * PFpa_type_assert (const PFpa_op_t *n, PFalg_att_t att,
                              PFalg_simple_type_t ty);

/**
 * Cast operator
 */
PFpa_op_t *PFpa_cast (const PFpa_op_t *,
                      PFalg_att_t, PFalg_att_t, 
                      PFalg_simple_type_t);

/****************************************************************/

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema
 */
PFpa_op_t *PFpa_llscjoin (const PFpa_op_t *ctx,
                          PFalg_axis_t axis,
                          const PFty_t test,
                          const PFord_ordering_t in,
                          const PFord_ordering_t out,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/**
 * Access to persistently stored document table.
 */
PFpa_op_t * PFpa_doc_tbl (const PFpa_op_t *,
                          PFalg_att_t res,
                          PFalg_att_t att);

/**
 * Access to the string content of loaded documents
 */
PFpa_op_t * PFpa_doc_access (const PFpa_op_t *alg,
                             PFalg_att_t res,
                             PFalg_att_t att,
                             PFalg_doc_t doc_col);

/** Constructor for twig root operators. */
PFpa_op_t * PFpa_twig (const PFpa_op_t *twig,
                       PFalg_att_t iter,
                       PFalg_att_t item);

/** Constructor for twig constructor sequence operators. */
PFpa_op_t * PFpa_fcns (const PFpa_op_t *fc,
                       const PFpa_op_t *ns);
                          
/** Constructor for document node operators. */
PFpa_op_t * PFpa_docnode (const PFpa_op_t *scope,
                          const PFpa_op_t *fcns,
                          PFalg_att_t iter);

/** Constructor for element operators. */
PFpa_op_t * PFpa_element (const PFpa_op_t *tags,
                          const PFpa_op_t *fcns,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/** Constructor for attribute operators. */
PFpa_op_t * PFpa_attribute (const PFpa_op_t *cont,
                            PFalg_att_t iter,
                            PFalg_att_t qn,
                            PFalg_att_t val);

/** Constructor for text node operators. */
PFpa_op_t * PFpa_textnode (const PFpa_op_t *cont, 
                           PFalg_att_t iter,
                           PFalg_att_t item);

/** Constructor for comment operators. */
PFpa_op_t * PFpa_comment (const PFpa_op_t *cont,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/** Constructor for processing instruction operators. */
PFpa_op_t * PFpa_processi (const PFpa_op_t *cont,
                           PFalg_att_t iter,
                           PFalg_att_t target,
                           PFalg_att_t val);

/** Constructor for constructor content operators (elem|doc). */
PFpa_op_t * PFpa_content (const PFpa_op_t *cont,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/** Constructor for shallow constructor content operators (elem|doc). */
PFpa_op_t * PFpa_slim_content (const PFpa_op_t *cont,
                               PFalg_att_t iter,
                               PFalg_att_t item);

/** Constructor for pf:merge-adjacent-text-nodes() functionality */
PFpa_op_t * PFpa_merge_adjacent (const PFpa_op_t *n,
                                 PFalg_att_t,
                                 PFalg_att_t,
                                 PFalg_att_t);

/**
 * Constructor for a runtime error message
 */
PFpa_op_t * PFpa_error (const PFpa_op_t *n,  PFalg_att_t att,
                        PFalg_simple_type_t att_ty);

/**
 * Constructor for conditional error
 */
PFpa_op_t * PFpa_cond_err (const PFpa_op_t *n, const PFpa_op_t *err,
                           PFalg_att_t att, char *err_string);

/****************************************************************/

/**
 * Constructor for the last item of a parameter list
 * related to recursion
 */
PFpa_op_t *PFpa_nil (void);

/**
 * Constructor for debug operator
 */
PFpa_op_t * PFpa_trace (const PFpa_op_t *n1,
                        const PFpa_op_t *n2,
                        PFalg_att_t iter,
                        PFalg_att_t item);

/**
 * Constructor for debug message operator
 */
PFpa_op_t * PFpa_trace_msg (const PFpa_op_t *n1,
                           const PFpa_op_t *n2,
                           PFalg_att_t iter,
                           PFalg_att_t item);

/**
 * Constructor for debug relation map operator
 * (A set of the trace_map operators link a trace operator
 * to the correct scope.)
 */
PFpa_op_t * PFpa_trace_map (const PFpa_op_t *n1,
                            const PFpa_op_t *n2,
                            PFalg_att_t      inner,
                            PFalg_att_t      outer);

/**
 * Constructor for a tail recursion operator
 */
PFpa_op_t *PFpa_rec_fix (const PFpa_op_t *paramList,
                         const PFpa_op_t *res);

/**
 * Constructor for a list item of a parameter list
 * related to recursion
 */
PFpa_op_t *PFpa_rec_param (const PFpa_op_t *arguments,
                           const PFpa_op_t *paramList);

/**
 * Constructor for the arguments of a parameter (seed and recursion
 * will be the input relations for the base operator)
 */
PFpa_op_t *PFpa_rec_arg (const PFpa_op_t *seed,
                         const PFpa_op_t *recursion,
                         const PFpa_op_t *base);

/**
 * Constructor for the base relation in a recursion (-- a dummy
 * operator representing the seed relation as well as the argument
 * computed in the recursion).
 */
PFpa_op_t *PFpa_rec_base (PFalg_schema_t schema, PFord_ordering_t ord);

/**
 * Constructor for the border relation in a recursion (-- a dummy
 * operator representing the border of a recursion body).
 */
PFpa_op_t *PFpa_rec_border (const PFpa_op_t *n);

/**
 * Constructor for the function application
 */
PFpa_op_t *PFpa_fun_call (const PFpa_op_t *loop,
                          const PFpa_op_t *param_list,
                          PFalg_schema_t schema,
                          PFalg_fun_call_t kind,
                          PFqname_t qname,
                          void *ctx,
                          PFalg_att_t iter,
                          PFalg_occ_ind_t occ_ind);

/**
 * Constructor for a list item of a parameter list
 * related to function application
 */
PFpa_op_t *PFpa_fun_param (const PFpa_op_t *argument,
                           const PFpa_op_t *param_list,
                           PFalg_schema_t schema);
                                   
/****************************************************************/
/* operators introduced by built-in functions */

/**
 * Concatenation of multiple strings (using seperators)
 */
PFpa_op_t * PFpa_string_join (const PFpa_op_t *n1, 
                              const PFpa_op_t *n2,
                              PFalg_att_t,
                              PFalg_att_t);


#endif  /* PHYSICAL_H */

/* vim:set shiftwidth=4 expandtab: */
