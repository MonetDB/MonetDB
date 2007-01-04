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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
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
#if 0                        
    , pa_nljoin         =  12 /**< NestedLoopJoin */
    , pa_merge_join     =  13 /**< MergeJoin */
#endif                       
    , pa_eqjoin         =  14 /**< Generic join implementation */
    , pa_semijoin       =  15 /**< Semijoin implementation */
    , pa_project        =  16 /**< Project */
    , pa_select         =  17 /**< Select: filter rows by value in given att */
    , pa_append_union   =  20 /**< AppendUnion */
    , pa_merge_union    =  21 /**< MergeUnion */
    , pa_intersect      =  22 /**< Intersect */
    , pa_difference     =  23 /**< Difference */
    , pa_sort_distinct  =  24 /**< SortDistinct */
    , pa_std_sort       =  25 /**< StdSort */
    , pa_refine_sort    =  26 /**< RefineSort */
    , pa_fun_1to1       =  30 /**< generic operator that extends the schema with
                                   a new column where each value is determined 
                                   by the values of a single row (cardinality 
                                   stays the same) */
    , pa_eq             =  40 /**< Numeric or String Equality */
    , pa_eq_atom        =  41 /**< Numeric or String Equality */
    , pa_gt             =  42 /**< Numeric or String GreaterThan */
    , pa_gt_atom        =  43 /**< Numeric or String GreaterThan */
    , pa_bool_not       =  45 /**< Boolean negation */
    , pa_bool_and       =  46 /**< Boolean and */
    , pa_bool_or        =  47 /**< Boolean or */
    , pa_bool_and_atom  =  48 /**< Boolean and, where one arg is an atom */
    , pa_bool_or_atom   =  49 /**< Boolean or, where one arg is an atom */
    , pa_hash_count     =  55 /**< Hash-based count operator */
    , pa_avg            =  56 /**< Avg operator */
    , pa_max            =  57 /**< Max operator */
    , pa_min            =  58 /**< Min operator */    
    , pa_sum            =  59 /**< Sum operator */
    , pa_number         =  60 /**< Numbering operator */
    , pa_type           =  63 /**< selection of rows where a column is of a
                                   certain type */
    , pa_type_assert    =  64 /**< restriction of the type of a given column */
    , pa_cast           =  65 /**< cast a table to a given type */
    , pa_llscj_anc      = 100 /**< Loop-Lifted StaircaseJoin Ancestor */
    , pa_llscj_anc_self = 101 /**< Loop-Lifted StaircaseJoin AncestorOrSelf */
    , pa_llscj_attr     = 102 /**< Loop-Lifted StaircaseJoin AncestorOrSelf */
    , pa_llscj_child    = 103 /**< Loop-Lifted StaircaseJoin Child */
    , pa_llscj_desc     = 104 /**< Loop-Lifted StaircaseJoin Descendant */
    , pa_llscj_desc_self= 105 /**< Loop-Lifted StaircaseJoin DescendantOrSelf */
    , pa_llscj_foll     = 106 /**< Loop-Lifted StaircaseJoin Following */
    , pa_llscj_foll_sibl= 107 /**< Loop-Lifted StaircaseJoin FollowingSibling */
    , pa_llscj_parent   = 108 /**< Loop-Lifted StaircaseJoin Parent */
    , pa_llscj_prec     = 109 /**< Loop-Lifted StaircaseJoin Preceding */
    , pa_llscj_prec_sibl= 110 /**< Loop-Lifted StaircaseJoin PrecedingSibling */
    , pa_doc_tbl        = 120 /**< Access to persistent document relation */
    , pa_doc_access     = 121 /**< Access to string content of loaded docs */
    , pa_element        = 122 /**< element-constructing operator */
    , pa_element_tag    = 123 /**< part of the element-constructing operator;
                                  connecting element tag and content;
                                  due to Burg we use two "wire2" operators
                                  now instead of one "wire3 operator "*/
    , pa_attribute      = 124 /**< attribute-constructing operator */
    , pa_textnode       = 125 /**< text node-constructing operator */
    , pa_docnode        = 126 /**< document node-constructing operator */
    , pa_comment        = 127 /**< comment-constructing operator */
    , pa_processi       = 128 /**< processing instruction-constr. operator */
    , pa_merge_adjacent = 129
    , pa_roots          = 130
    , pa_fragment       = 131
    , pa_frag_union     = 132
    , pa_empty_frag     = 133
    , pa_cond_err       = 140 /**< conditional error operator */
    , pa_rec_fix        = 141 /**< operator representing a tail recursion */
    , pa_rec_param      = 142 /**< list of parameters of the recursion */
    , pa_rec_nil        = 143 /**< end of the list of parameters of the 
                                  recursion */
    , pa_rec_arg        = 144 /**< reference to the arguments of a parameter
                                  in the recursion */
    , pa_rec_base       = 145 /**< base of the DAG describing the recursion */
    , pa_rec_border     = 146 /**< border of the DAG describing the recursion */
    , pa_string_join    = 150 /**< Concatenation of multiple strings */
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

    /* semantic content for binary (arithmetic and boolean) operators
     * where the second argument is an atom (if we know that an
     * attribute will be constant) */
    struct {
        PFalg_att_t     att1;     /**< first operand */
        PFalg_atom_t    att2;     /**< second operand */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } bin_atom;

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

    /* semantic content for number operator */
    struct {
        PFalg_att_t     attname;  /**< name of generated (integer) attribute */
        PFalg_att_t     part;     /**< optional partitioning attribute,
                                       otherwise NULL */
    } number;

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
        PFty_t           ty;      /**< sequence type that describes the
                                       node test */
        PFord_ordering_t in;      /**< input ordering */
        PFord_ordering_t out;     /**< output ordering */
        PFalg_att_t      iter;    /**< iter column */
        PFalg_att_t      item;    /**< item column */
    } scjoin;

    /* reference iter and item columns */
    struct {
        PFalg_att_t     iter;     /**< iter column */
        PFalg_att_t     item;     /**< item column */
    } ii;

    /* reference columns for document access */
    struct {
        PFalg_att_t     res;      /**< attribute to hold the result */
        PFalg_att_t     att;      /**< name of the reference attribute */
        PFalg_doc_t     doc_col;  /**< referenced column in the document */
    } doc_access;

    /* reference columns of attribute constructor */
    struct {
        PFalg_att_t     qn;       /**< name of the qname item column */
        PFalg_att_t     val;      /**< name of the value item column */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } attr;

    /* reference columns of text constructor */
    struct {
        PFalg_att_t     item;     /**< name of the item column */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } textnode;

    /* semantic content for conditional error */
    struct {
        PFalg_att_t     att;     /**< name of the boolean attribute */
        char *          str;     /**< error message */
    } err;
    
    /* semantic content for an argument of a recursion parameter */
    struct {
        PFpa_op_t      *base;    /**< reference to the base relation
                                      of the recursion */
    } rec_arg;

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
PFpa_op_t * PFpa_serialize (const PFpa_op_t *doc, const PFpa_op_t *alg,
                            PFalg_att_t item);

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
@verbatim
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
 * Comparison operator eq, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_eq_atom (const PFpa_op_t *, PFalg_att_t res,
                         PFalg_att_t att1, PFalg_atom_t att2);

/**
 * Comparison operator gt, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_gt_atom (const PFpa_op_t *, PFalg_att_t res,
                         PFalg_att_t att1, PFalg_atom_t att2);

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
 * Boolean and, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_and_atom (const PFpa_op_t *, PFalg_att_t res,
                          PFalg_att_t att1, PFalg_atom_t att2);

/**
 * Boolean or, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_or_atom (const PFpa_op_t *, PFalg_att_t res,
                         PFalg_att_t att1, PFalg_atom_t att2);

/**
 * HashCount: Hash-based Count operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_hash_count (const PFpa_op_t *,
                            PFalg_att_t, PFalg_att_t);

/**
 * Aggr: Aggregation function operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_aggr (PFpa_op_kind_t kind, const PFpa_op_t *n, PFalg_att_t res, 
		     PFalg_att_t att, PFalg_att_t part);

/****************************************************************/

PFpa_op_t *PFpa_number (const PFpa_op_t *n,
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
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *PFpa_llscj_anc (const PFpa_op_t *frag,
                           const PFpa_op_t *ctx,
                           const PFty_t test,
                           const PFord_ordering_t in,
                           const PFord_ordering_t out,
                           PFalg_att_t iter,
                           PFalg_att_t item);
PFpa_op_t *PFpa_llscj_anc_self (const PFpa_op_t *frag,
                                const PFpa_op_t *ctx,
                                const PFty_t test,
                                const PFord_ordering_t in,
                                const PFord_ordering_t out,
                                PFalg_att_t iter,
                                PFalg_att_t item);
PFpa_op_t *PFpa_llscj_attr (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out,
                            PFalg_att_t iter,
                            PFalg_att_t item);
PFpa_op_t *PFpa_llscj_child (const PFpa_op_t *frag,
                             const PFpa_op_t *ctx,
                             const PFty_t test,
                             const PFord_ordering_t in,
                             const PFord_ordering_t out,
                             PFalg_att_t iter,
                             PFalg_att_t item);
PFpa_op_t *PFpa_llscj_desc (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out,
                            PFalg_att_t iter,
                            PFalg_att_t item);
PFpa_op_t *PFpa_llscj_desc_self (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out,
                                 PFalg_att_t iter,
                                 PFalg_att_t item);
PFpa_op_t *PFpa_llscj_foll (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out,
                            PFalg_att_t iter,
                            PFalg_att_t item);
PFpa_op_t *PFpa_llscj_foll_sibl (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out,
                                 PFalg_att_t iter,
                                 PFalg_att_t item);
PFpa_op_t *PFpa_llscj_parent (const PFpa_op_t *frag,
                              const PFpa_op_t *ctx,
                              const PFty_t test,
                              const PFord_ordering_t in,
                              const PFord_ordering_t out,
                              PFalg_att_t iter,
                              PFalg_att_t item);
PFpa_op_t *PFpa_llscj_prec (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out,
                            PFalg_att_t iter,
                            PFalg_att_t item);
PFpa_op_t *PFpa_llscj_prec_sibl (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out,
                                 PFalg_att_t iter,
                                 PFalg_att_t item);

/**
 * Access to persistently stored document table.
 *
 * Requires an iter | item schema as its input.
 */
PFpa_op_t * PFpa_doc_tbl (const PFpa_op_t *,
                          PFalg_att_t iter,
                          PFalg_att_t item);

/**
 * Access to the string content of loaded documents
 */
PFpa_op_t * PFpa_doc_access (const PFpa_op_t *doc, 
                             const PFpa_op_t *alg,
                             PFalg_att_t res,
                             PFalg_att_t att,
                             PFalg_doc_t doc_col);

/**
 * element constructor
 *
 * Requires an iter | item schema as its qname input
 * and a an iter | pos | item schema as its content input.
 */
PFpa_op_t * PFpa_element (const PFpa_op_t *, 
                          const PFpa_op_t *,
                          const PFpa_op_t *,
                          PFalg_att_t,
                          PFalg_att_t,
                          PFalg_att_t);

/**
 * Attribute constructor
 */
PFpa_op_t * PFpa_attribute (const PFpa_op_t *,
                            PFalg_att_t,
                            PFalg_att_t,
                            PFalg_att_t);

/**
 * Text constructor
 */
PFpa_op_t * PFpa_textnode (const PFpa_op_t *,
                           PFalg_att_t, PFalg_att_t);

PFpa_op_t * PFpa_merge_adjacent (const PFpa_op_t *fragment,
                                 const PFpa_op_t *n,
                                 PFalg_att_t,
                                 PFalg_att_t,
                                 PFalg_att_t);

/**
 * Extract result part from a (frag, result) pair.
 */
PFpa_op_t *PFpa_roots (const PFpa_op_t *n);

/**
 * Extract fragment part from a (frag, result) pair.
 */
PFpa_op_t *PFpa_fragment (const PFpa_op_t *n);

/**
 * Form disjoint union between two fragments.
 */
PFpa_op_t *PFpa_frag_union (const PFpa_op_t *n1, const PFpa_op_t *n2);

/**
 * Empty fragment list
 */
PFpa_op_t *PFpa_empty_frag (void);

/**
 * Constructor for conditional error
 */
PFpa_op_t * PFpa_cond_err (const PFpa_op_t *n, const PFpa_op_t *err,
                           PFalg_att_t att, char *err_string);

/****************************************************************/

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
 * Constructor for the last item of a parameter list
 * related to recursion
 */
PFpa_op_t *PFpa_rec_nil (void);

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
