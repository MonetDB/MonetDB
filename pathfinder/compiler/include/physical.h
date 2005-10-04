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

#ifndef PHYSICAL_H
#define PHYSICAL_H

#include <stdbool.h>

#include "variable.h"
#include "algebra.h"
#include "mil.h"

#include "ordering.h"


/* .............. algebra operators (operators on relations) .............. */

/** algebra operator kinds */
enum PFpa_op_kind_t {
      pa_lit_tbl        =   1 /**< literal table */
    , pa_empty_tbl      =   2 /**< empty literal table */
    , pa_append_union   =   3 /**< AppendUnion */
    , pa_merge_union    =   4 /**< MergeUnion */
    , pa_intersect      =   5 /**< Intersect */
    , pa_difference     =   6 /**< Difference */
    , pa_cross          =   7 /**< Cross */
    , pa_attach         =   8 /**< ColumnAttach */
    , pa_project        =   9 /**< Project */
    , pa_leftjoin       =  10 /**< LeftJoin */
    , pa_eqjoin         =  11 /**< Generic join implementation */
#if 0
    , pa_nljoin         =  10 /**< NestedLoopJoin */
    , pa_merge_join     =  11 /**< MergeJoin */
#endif
    , pa_sort_distinct  =  12 /**< SortDistinct */
    , pa_std_sort       =  13 /**< StdSort */
    , pa_refine_sort    =  14 /**< RefineSort */
    , pa_merge_rownum   =  15 /**< MergeRowNumber */
    , pa_hash_rownum    =  16 /**< HashRowNumber */
    , pa_num_add        =  17 /**< Arithmetic + */
    , pa_num_add_atom   =  18 /**< Arithmetic +, where one arg is an atom */
    , pa_num_sub        =  19 /**< Arithmetic - */
    , pa_num_sub_atom   =  20 /**< Arithmetic -, where one arg is an atom */
    , pa_num_mult       =  21 /**< Arithmetic * */
    , pa_num_mult_atom  =  22 /**< Arithmetic *, where one arg is an atom */
    , pa_num_div        =  23 /**< Arithmetic / */
    , pa_num_div_atom   =  24 /**< Arithmetic /, where one arg is an atom */
    , pa_num_mod        =  25 /**< Arithmetic mod */
    , pa_num_mod_atom   =  26 /**< Arithmetic mod, where one arg is an atom */
    , pa_eq             =  27 /**< Numeric or String Equality */
    , pa_eq_atom        =  28 /**< Numeric or String Equality */
    , pa_gt             =  29 /**< Numeric or String GreaterThan */
    , pa_gt_atom        =  30 /**< Numeric or String GreaterThan */
    , pa_num_neg        =  31 /**< Numeric negation */
    , pa_bool_not       =  32 /**< Boolean negation */
    , pa_bool_and       =  33 /**< Boolean and */
    , pa_bool_or        =  34 /**< Boolean or */
    , pa_cast           =  35 /**< cast a table to a given type */
    , pa_select         =  36 /**< Select: filter rows by value in given att */
    , pa_hash_count     =  37 /**< Hash-based count operator */
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
    , pa_doc_tbl        = 111 /**< Access to persistent document relation */
    , pa_doc_access     = 112 /**< Access to string content of loaded docs */
    , pa_string_join    = 113 /**< Concatenation of multiple strings */
    , pa_serialize      = 114
    , pa_roots          = 115
    , pa_fragment       = 116
    , pa_frag_union     = 117
    , pa_empty_frag     = 118
};
/** algebra operator kinds */
typedef enum PFpa_op_kind_t PFpa_op_kind_t;

/** semantic content in algebra operators */
union PFpa_op_sem_t {

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

    /* semantic content for projection operator */
    struct {
        unsigned int    count;    /**< length of projection list */
        PFalg_proj_t   *items;    /**< projection list */
    } proj;

    /** semantic content for sort operators */
    struct {
        PFord_ordering_t required;
        PFord_ordering_t existing;
    } sortby;

    /* semantic content for rownum operator */
    struct {
        PFalg_att_t     attname;  /**< name of generated (integer) attribute */
        PFalg_att_t     part;     /**< optional partitioning attribute,
                                       otherwise NULL */
    } rownum;

    struct {
        PFord_ordering_t ord;     /**< ``grouping'' parameter for
                                       MergeUnion */
    } merge_union;

    /* semantic content for equi-join operator */
    struct {
        PFalg_att_t     att1;     /**< name of attribute from "left" rel */
        PFalg_att_t     att2;     /**< name of attribute from "right" rel */
    } eqjoin;

    /** semantic content for staircase join operator */
    struct {
        PFty_t           ty;      /**< sequence type that describes the
                                       node test */
        PFord_ordering_t in;      /**< input ordering */
        PFord_ordering_t out;     /**< output ordering */
    } scjoin;

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
     * (e.g. numeric/Boolean negation)
     */
    struct {
        PFalg_att_t     att;      /**< argument attribute */
        PFalg_att_t     res;      /**< attribute to hold the result */
    } unary;

    /** semantic content for selection operator */
    struct {
        PFalg_att_t     att;     /**< name of selected attribute */
    } select;

    /** semantic content for Cast operator */
    struct {
        PFalg_att_t         att; /**< attribute to cast */
        PFalg_simple_type_t ty;  /**< target type */
    } cast;

    /** semantic content for Count operators */
    struct {
        PFalg_att_t         res;  /**< Name of result attribute */
        PFalg_att_t         part; /**< Partitioning attribute */
    } count;

    /** semantic content for SortDistinct operator */
    struct {
        PFord_ordering_t ord;    /**< ordering to consider for duplicate
                                      elimination */
    } sort_distinct;

    /* reference columns for document access */
    struct {
        PFalg_att_t     att;      /**< name of the reference attribute */
        PFalg_doc_t     doc_col;  /**< referenced column in the document */
    } doc_access;
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
    struct PFpa_op_t  *child[PFPA_OP_MAXCHILD];
    unsigned int       refctr;
    unsigned int       usectr;
    int                node_id;    /**< specifies the id of this operator
                                        node; required exclusively to
                                        create dot output. */
};
/** algebra operator node */
typedef struct PFpa_op_t PFpa_op_t;



/* ***************** Constructors ******************* */

PFpa_op_t *PFpa_lit_tbl (PFalg_attlist_t a,
                         unsigned int count, PFalg_tuple_t *tpls);


/**
 * Empty table constructor.  Use this instead of an empty table
 * without any tuples to facilitate optimization.
 */
PFpa_op_t *PFpa_empty_tbl (PFalg_attlist_t a);

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

PFpa_op_t *PFpa_attach (const PFpa_op_t *n,
                        PFalg_att_t attname, PFalg_atom_t value);


/**
 * SortDistinct: Eliminate duplicate tuples.
 *
 * Requires the input to be fully sorted.  Specify this ordering
 * in the second argument (should actually be redundant, but we
 * keep it anyway).
 */
PFpa_op_t *PFpa_sort_distinct (const PFpa_op_t *, PFord_ordering_t);


/**
 * Project.
 *
 * Note that projection does @b not eliminate duplicates. If you
 * need duplicate elimination, explictly use a Distinct operator.
 */
PFpa_op_t *PFpa_project (const PFpa_op_t *n, unsigned int count,
                         PFalg_proj_t *proj);


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

/**
 * HashRowNumber: Introduce new row numbers.
 *
 * HashRowNumber uses a hash table to implement partitioning. Hence,
 * it does not require any specific input ordering.
 *
 * @param n        Argument relation.
 * @param new_att  Name of newly introduced attribute.
 * @param part     Partitioning attribute. @c NULL if partitioning
 *                 is not requested.
 */
PFpa_op_t *PFpa_hash_rownum (const PFpa_op_t *n,
                             PFalg_att_t new_att,
                             PFalg_att_t part);

PFpa_op_t *PFpa_merge_rownum (const PFpa_op_t *n,
                              PFalg_att_t new_att,
                              PFalg_att_t part);

/**
 * StaircaseJoin operator.
 *
 * Input must have iter|item schema, and be sorted on iter.
 */
PFpa_op_t *PFpa_llscj_anc (const PFpa_op_t *frag,
                           const PFpa_op_t *ctx,
                           const PFty_t test,
                           const PFord_ordering_t in,
                           const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_anc_self (const PFpa_op_t *frag,
                                const PFpa_op_t *ctx,
                                const PFty_t test,
                                const PFord_ordering_t in,
                                const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_attr (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_child (const PFpa_op_t *frag,
                             const PFpa_op_t *ctx,
                             const PFty_t test,
                             const PFord_ordering_t in,
                             const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_desc (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_desc_self (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_foll (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_foll_sibl (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_parent (const PFpa_op_t *frag,
                              const PFpa_op_t *ctx,
                              const PFty_t test,
                              const PFord_ordering_t in,
                              const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_prec (const PFpa_op_t *frag,
                            const PFpa_op_t *ctx,
                            const PFty_t test,
                            const PFord_ordering_t in,
                            const PFord_ordering_t out);
PFpa_op_t *PFpa_llscj_prec_sibl (const PFpa_op_t *frag,
                                 const PFpa_op_t *ctx,
                                 const PFty_t test,
                                 const PFord_ordering_t in,
                                 const PFord_ordering_t out);

/**
 * Arithmetic operator +.
 *
 * This generic variant expects both operands to be available as
 * columns in the argument relation. If know one of the operands
 * to be actually a constant, we may prefer PFpa_num_add_const()
 * and avoid materialization of the constant attribute.
 */
PFpa_op_t *PFpa_num_add (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Arithmetic operator -. See PFpa_num_add() for details.
 */
PFpa_op_t *PFpa_num_sub (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Arithmetic operator *. See PFpa_num_add() for details.
 */
PFpa_op_t *PFpa_num_mult (const PFpa_op_t *, const PFalg_att_t res,
                          const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Arithmetic operator /. See PFpa_num_add() for details.
 */
PFpa_op_t *PFpa_num_div (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Arithmetic operator mod. See PFpa_num_add() for details.
 */
PFpa_op_t *PFpa_num_mod (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Arithmetic operator +.
 *
 * This variant expects one operands to be an atomic value (which
 * is helpful if we know one attribute/argument to be constant).
 */
PFpa_op_t *PFpa_num_add_atom (const PFpa_op_t *, const PFalg_att_t res,
                              const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Arithmetic operator -. See PFpa_num_add_atom() for details.
 */
PFpa_op_t *PFpa_num_sub_atom (const PFpa_op_t *, const PFalg_att_t res,
                              const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Arithmetic operator *. See PFpa_num_add_atom() for details.
 */
PFpa_op_t *PFpa_num_mult_atom (const PFpa_op_t *, const PFalg_att_t res,
                               const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Arithmetic operator /. See PFpa_num_add_atom() for details.
 */
PFpa_op_t *PFpa_num_div_atom (const PFpa_op_t *, const PFalg_att_t res,
                              const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Arithmetic operator mod. See PFpa_num_add_atom() for details.
 */
PFpa_op_t *PFpa_num_mod_atom (const PFpa_op_t *, const PFalg_att_t res,
                              const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Comparison operator eq.
 */
PFpa_op_t *PFpa_eq (const PFpa_op_t *, const PFalg_att_t res,
                    const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Comparison operator gt.
 */
PFpa_op_t *PFpa_gt (const PFpa_op_t *, const PFalg_att_t res,
                    const PFalg_att_t att1, const PFalg_att_t att2);

/**
 * Comparison operator eq, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_eq_atom (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Comparison operator gt, where one column is an atom (constant).
 */
PFpa_op_t *PFpa_gt_atom (const PFpa_op_t *, const PFalg_att_t res,
                         const PFalg_att_t att1, const PFalg_atom_t att2);

/**
 * Numeric negation
 */
PFpa_op_t *PFpa_num_neg (const PFpa_op_t *,
                         const PFalg_att_t res, const PFalg_att_t att);

/**
 * Boolean negation
 */
PFpa_op_t *PFpa_bool_not (const PFpa_op_t *,
                          const PFalg_att_t res, const PFalg_att_t att);

/**
 * Cast operator
 */
PFpa_op_t *PFpa_cast (const PFpa_op_t *,
                      const PFalg_att_t, PFalg_simple_type_t);

/**
 * Select: Filter rows by Boolean value in attribute @a att
 */
PFpa_op_t *PFpa_select (const PFpa_op_t *, const PFalg_att_t);

/**
 * HashCount: Hash-based Count operator. Does neither benefit from
 * any existing ordering, nor does it provide/preserve any input
 * ordering.
 */
PFpa_op_t *PFpa_hash_count (const PFpa_op_t *,
                            const PFalg_att_t, const PFalg_att_t);

/**
 * Access to persistently stored document table.
 *
 * Requires an iter | item schema as its input.
 */
PFpa_op_t * PFpa_doc_tbl (const PFpa_op_t *);

/**
 * Empty fragment list
 */
PFpa_op_t *PFpa_empty_frag (void);

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


/****************************************************************/
/* operators introduced by built-in functions */

/**
 * Access to the string content of loaded documents
 */
PFpa_op_t * PFpa_doc_access (const PFpa_op_t *doc, 
                             const PFpa_op_t *alg,
                             PFalg_att_t att,
                             PFalg_doc_t doc_col);

/**
 * Concatenation of multiple strings (using seperators)
 */
PFpa_op_t * PFpa_string_join (const PFpa_op_t *n1, 
                              const PFpa_op_t *n2);

/****************************************************************/


/**
 * A `serialize' node will be placed on the very top of the algebra
 * expression tree.
 */
PFpa_op_t * PFpa_serialize (const PFpa_op_t *doc, const PFpa_op_t *alg);


#endif  /* PHYSICAL_H */

/* vim:set shiftwidth=4 expandtab: */
