/**
 * @file
 *
 * Declarations for algebra tree
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
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *          Sabine Mayer <sbnmayer@inf.uni-konstanz.de>
 *
 * $Id$
 */

#ifndef ALGEBRA_H
#define ALGEBRA_H

#include "variable.h"
#include "stdbool.h"

/* ............... atomic values ............... */

/**
 * Simple atomic types that our algebra knows.
 * Actual attribute types can be any combination of types from here
 * (polymorphism). We represent these polymorphic algebra types with
 * help of a bit-vector. Each of the bits corresponds to one of the
 * enumeration types here.
 */
enum PFalg_simple_type_t {
      aat_nat   = 0x01  /**< algebra simple atomic type natural number */
    , aat_int   = 0x02  /**< algebra simple atomic type integer */
    , aat_str   = 0x04  /**< algebra simple atomic type string  */
    , aat_node  = 0x08  /**< algebra simple atomic type node */
    , aat_dec   = 0x10  /**< algebra simple atomic type decimal */
    , aat_dbl   = 0x20  /**< algebra simple atomic type double  */
    , aat_bln   = 0x40  /**< algebra simple atomic type boolean  */
    , aat_qname = 0x80  /**< algebra simple atomic type QName  */
};
/** Simple atomic types in our algebra */
typedef enum PFalg_simple_type_t PFalg_simple_type_t;

typedef unsigned int nat;

/**
 * The types we use for attributes are combinations of the simple
 * types, represented as a bit-vector. For now, 16 bits suffice by
 * far, but we may extend this vector if desired.
 */
typedef unsigned short PFalg_type_t;


/** atomic algebra values */
union PFalg_atom_val_t {
    nat        nat;     /**< value for natural number atoms (#aat_nat) */
    int        int_;    /**< value for integer atoms (#aat_int) */
    char      *str;     /**< value for string atoms (#aat_str)  */
    int        node;    /**< value for node atoms (#aat_node) */
    float      dec;     /**< value for decimal atoms (#aat_dec) */
    double     dbl;     /**< value for double atoms (#aat_dbl) */
    bool       bln;     /**< value for boolean atoms (#aat_bln) */
    PFqname_t  qname;
};
/** algebra values */
typedef union PFalg_atom_val_t PFalg_atom_val_t;

/** typed atomic value representation in our algebra */
struct PFalg_atom_t {
    PFalg_simple_type_t type;  /**< type of this atom */
    PFalg_atom_val_t    val;   /**< value */
};
/** typed atomic value representation in our algebra */
typedef struct PFalg_atom_t PFalg_atom_t;


/* ............... tuples of atomic values ............... */

/** a tuple is an array of atoms, with length specified in `count' */
struct PFalg_tuple_t {
    int           count;   /**< number of atoms in this tuple */
    PFalg_atom_t *atoms;   /**< array containing the atoms */
};

typedef struct PFalg_tuple_t PFalg_tuple_t;


/* ................ algebra attribute lists ................ */

/** An attribute (name) is represented by a C string */
typedef char * PFalg_att_t;

/** A list of attributes (actually: attribute names) */
struct PFalg_attlist_t {
    int          count;    /**< number of items in this list */
    PFalg_att_t *atts;     /**< array that holds the actual list items */
};
typedef struct PFalg_attlist_t PFalg_attlist_t;


/* ............. algebra schema specification .............. */

/** An algebra schema item is a (name, type) pair */
struct PFalg_schm_item_t {
    PFalg_att_t     name;
    PFalg_type_t    type;
};

/** A schema is then a list of schema items */
struct PFalg_schema_t {
    int                        count;  /**< number of items in the list */
    struct PFalg_schm_item_t  *items;  /**< array holding the schema items */
};
typedef struct PFalg_schema_t PFalg_schema_t;


/** item in a projection list, an (new-name,old-name) pair */
struct PFalg_proj_t {
    PFalg_att_t new;   /**< new attribute name to assign */
    PFalg_att_t old;   /**< old attribute name */
};
typedef struct PFalg_proj_t PFalg_proj_t;



/* ....... staircase join specs (semantic infos of scj operators) ....... */

/** location steps */
enum PFalg_axis_t {
      aop_anc          /**< ancestor axis */
    , aop_anc_s        /**< ancestor-or-self axis */
    , aop_attr         /**< attribute axis */
    , aop_chld         /**< child axis */
    , aop_desc         /**< descendant axis */
    , aop_desc_s       /**< descendant-or-self axis */
    , aop_fol          /**< following axis */
    , aop_fol_s        /**< following-sibling axis */
    , aop_par          /**< parent axis */
    , aop_prec         /**< preceding axis */
    , aop_prec_s       /**< preceding-sibling axis */
    , aop_self         /**< self axis */
};
/** location steps */
typedef enum PFalg_axis_t PFalg_axis_t;

/** kind tests */
enum PFalg_test_t {
      aop_name         /**< name test */
    , aop_node         /**< node test */
    , aop_comm         /**< comment nodes */
    , aop_text         /**< text nodes */
    , aop_pi           /**< processing instructions */
    , aop_pi_tar       /**< processing instructions with target specified */
    , aop_doc          /**< document node */
    , aop_elem         /**< element nodes */
    , aop_at_tst       /**< attribute test TODO: difference to axis? */
};
/** location steps */
typedef enum PFalg_test_t PFalg_test_t;

enum PFalg_node_kind_t {
      node_kind_elem   /**< elements */
    , node_kind_attr   /**< attributes */
    , node_kind_text   /**< text nodes */
    , node_kind_pi     /**< processing instructions */
    , node_kind_comm   /**< comments */
    , node_kind_doc    /**< document nodes */
    , node_kind_node   /**< any XML tree node */
};
typedef enum PFalg_node_kind_t PFalg_node_kind_t;



/* .............. algebra operators (operators on relations) .............. */

/** algebra operator kinds */
enum PFalg_op_kind_t {
      aop_lit_tbl        =  1 /**< literal table */
    , aop_empty_tbl      =  2 /**< empty literal table */
    , aop_disjunion      =  3 /**< union two relations with same schema */
    , aop_intersect      =  4 /**< intersect two relations with same schema */
    , aop_difference     =  5 /**< difference of two relations w/ same schema */
    , aop_cross          =  6 /**< cross product (Cartesian product) */
    , aop_eqjoin         =  7 /**< equi-join */
    , aop_scjoin         =  8 /**< staircase join */
    , aop_select         =  9 /**< selection of rows where column value != 0 */
    , aop_type           = 10 /**< selection of rows where a column is of a
			           certain type */
    , aop_cast           = 11 /**< type cast of an attribute */
    , aop_project        = 12 /**< algebra projection and renaming operator */
    , aop_rownum         = 13 /**< consecutive number generation */
    , aop_serialize      = 14 /**< serialize algebra expression
                                   (Placed on the very top of the tree.) */
    , aop_num_add        = 15 /**< arithmetic plus operator */
    , aop_num_subtract   = 16 /**< arithmetic minus operator */
    , aop_num_multiply   = 17 /**< arithmetic times operator */
    , aop_num_divide     = 18 /**< arithmetic divide operator */
    , aop_num_modulo     = 19 /**< arithmetic modulo operator */
    , aop_num_eq         = 20 /**< numeric equal operator */
    , aop_num_gt         = 21 /**< numeric greater-than operator */
    , aop_num_neg        = 22 /**< numeric negation operator */
    , aop_bool_and       = 23 /**< boolean AND operator */
    , aop_bool_or        = 24 /**< boolean OR operator */
    , aop_bool_not       = 25 /**< boolean NOT operator */
    , aop_sum            = 26 /**< operator for (partitioned) sum of a column */
    , aop_count          = 27 /**< (partitioned) row counting operator */
    , aop_distinct       = 28 /**< duplicate elimination operator */
    , aop_element        = 29 /**< element-constructing operator */
    , aop_element_tag    = 30 /**< part of the element-constructing operator;
			           connecting element tag and content;
			           due to Burg we use two "wire2" operators
			           now instead of one "wire3 operator "*/
    , aop_attribute      = 31 /**< attribute-constructing operator */
    , aop_textnode       = 32 /**< text node-constructing operator */
    , aop_docnode        = 33 /**< document node-constructing operator */
    , aop_comment        = 34 /**< comment-constructing operator */
    , aop_processi       = 35 /**< processing instruction-constr. operator */
    , aop_concat         = 36 /**< req. for fs:item-sequence-to-node-sequence
                                   builtin function */
    , aop_merge_adjacent = 37 /**< operator for pf:merge-adjacent-text-nodes
                                   builtin function */
    , aop_seqty1         = 38 /**< test for exactly one type occurrence in one
                                   iteration (Pathfinder extension) */
    , aop_all            = 39 /**< test if all items in an iteration are true */
    , aop_roots          = 40 /**< algebraic repres. of the roots of newly
			           created xml nodes (e.g. element());
			           schema: iter | pos | item */
    /* all operators below represent xml node fragments with no schema */
    , aop_fragment       = 41 /**< representation of a node fragment */
    , aop_frag_union     = 42 /**< special node type used to form an algebraic
			        union of fragments */
    , aop_empty_frag     = 43 /**< representation of an empty fragment */
    , aop_doc_tbl        = 44 /**< document relation (is also a fragment) */

    , aop_dummy               /**< dummy node during compilation */
};
/** algebra operator kinds */
typedef enum PFalg_op_kind_t PFalg_op_kind_t;

/** semantic content in algebra operators */
union PFalg_op_sem_t {

    /* semantic content for literal table constr. */
    struct {
        int             count;    /**< number of tuples */
        PFalg_tuple_t  *tuples;   /**< array holding the tuples */
    } lit_tbl;                    /**< semantic content for literal table
                                       constructor */

    /* semantic content for projection operator */
    struct {
        int             count;    /**< length of projection list */
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

    /* semantic content for staircase join operator */
    struct {
        PFalg_axis_t      axis;      /**< represented axis */
        PFalg_node_kind_t kind;      /**< node kind to test for */
        /* FIXME: delete */
        PFalg_test_t      test;      /**< represented kind test @deprecated */
	union {
	    char         *target;    /**< target specified in pi's */
	    PFqname_t     qname;     /**< for name tests */
	} str;
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
	PFarray_t      *args;     /**< arguments of a buit_in function */
    } builtin;
};
/** semantic content in algebra operators */
typedef union PFalg_op_sem_t PFalg_op_sem_t;


/** maximum number of children of a #PFalg_op_t node */
#define PFALG_OP_MAXCHILD 2

/** algebra operator node */
struct PFalg_op_t {
    PFalg_op_kind_t    kind;       /**< operator kind */
    PFalg_op_sem_t     sem;        /**< semantic content for this operator */
    PFalg_schema_t     schema;     /**< result schema */
    PFarray_t         *env;        /**< environment to store the corresponding
                                        MIL algebra expression trees (for MIL
                                        algebra generation only) */
    short              state_label;/**< Burg puts its state information here. */
    struct PFma_op_t  *ma;         /**< MIL algebra generation will store its
                                        overall result here. */
    struct PFalg_op_t *child[PFALG_OP_MAXCHILD];
    int                node_id;    /**< specifies the id of this operator
				        node; required exclusively to
					create dot output. */
};
/** algebra operator node */
typedef struct PFalg_op_t PFalg_op_t;

/**
 * For a correct treatment of xml node fragments, we work with sets of
 * fragments and algebra expressions of fragments. This type represents
 * a set of xml fragments. Algebra expressions of xml fragments are
 * represented by PFalg_op_t structs.
 */
typedef PFarray_t PFalg_set_t;

struct PFalg_pair_t {
    struct PFalg_op_t *rel;      /* algebra representation of query */
    PFalg_set_t *frag;           /* set of currently live-node fragments */
};
typedef struct PFalg_pair_t PFalg_pair_t;



/* ***************** Constructors ******************* */

/** construct literal natural number (atom) */
PFalg_atom_t PFalg_lit_nat (nat value);

/** construct literal integer (atom) */
PFalg_atom_t PFalg_lit_int (int value);

/** construct literal string (atom) */
PFalg_atom_t PFalg_lit_str (char *value);

/** construct literal float (atom) */
PFalg_atom_t PFalg_lit_dec (float value);

/** construct literal double (atom) */
PFalg_atom_t PFalg_lit_dbl (double value);

/** construct literal boolean (atom) */
PFalg_atom_t PFalg_lit_bln (bool value);

/** construct literal QName (atom) */
PFalg_atom_t PFalg_lit_qname (PFqname_t value);


/**
 * Construct a literal table tuple, a list of atoms.
 * (actually just a wrapper for #PFalg_tuple_()).
 *
 * Functions with a variable number of arguments need a mechanism to
 * detect the end of the argument list. We therefore wrap the actual
 * worker #PFalg_tuple_() into this macro. The macro detects the
 * number of arguments passed (using arithmetics with sizeof()) and
 * prepends a count information to the actual argument list.
 */
#define PFalg_tuple(...)                                       \
    PFalg_tuple_ ((sizeof ((PFalg_atom_t[]) { __VA_ARGS__ })   \
                      / sizeof (PFalg_atom_t)),                \
                  (PFalg_atom_t[]) { __VA_ARGS__ } )

/** Worker to construct a literal table tuple */
PFalg_tuple_t PFalg_tuple_ (int count, PFalg_atom_t *atoms);

/**
 * Construct an attribute list (list of attribute names only).
 */
#define PFalg_attlist(...)                                     \
    PFalg_attlist_ ((sizeof ((PFalg_att_t[]) { __VA_ARGS__ })   \
                       / sizeof (PFalg_att_t)),                \
                   (PFalg_att_t[]) { __VA_ARGS__ })
PFalg_attlist_t PFalg_attlist_ (int count, PFalg_att_t *atts);


/**
 * Construct algebra node representing a literal table (actually just
 * a wrapper for #PFalg_lit_tbl_()).
 *
 * Call with the table's schema (as #PFalg_attlist_t) and the tuples
 * for that table (as type #PFalg_tuple_t).
 *
 * Functions with a variable number of arguments need a mechanism to
 * detect the end of the argument list. We therefore wrap the actual
 * worker #PFalg_lit_tbl_() into this macro. The macro detects the
 * number of arguments passed (using arithmetics with sizeof()) and
 * prepends a count information to an array created from the input.
 */
#define PFalg_lit_tbl(a,...)                                    \
    PFalg_lit_tbl_ ((a),                                        \
                    (sizeof ((PFalg_tuple_t[]) { __VA_ARGS__ }) \
                        / sizeof (PFalg_tuple_t)),              \
                    (PFalg_tuple_t []) { __VA_ARGS__ } )
PFalg_op_t *PFalg_lit_tbl_ (PFalg_attlist_t a, int count, PFalg_tuple_t *tpls);


/**
 * Empty table constructor.  Use this instead of an empty table
 * without any tuples to facilitate optimization.
 */
PFalg_op_t *PFalg_empty_tbl (PFalg_attlist_t a);


/**
 * Cross product (Cartesian product) of two relations.
 * No duplicate attribute names allowed.
 */
PFalg_op_t * PFalg_cross (PFalg_op_t *n1, PFalg_op_t *n2);


/**
 * Equi-join between two relations.
 * No duplicate attribute names allowed.
 */
PFalg_op_t * PFalg_eqjoin (PFalg_op_t *n1, PFalg_op_t *n2,
			   PFalg_att_t att1, PFalg_att_t att2);

/**
 * Dummy node to pass various stuff around during algebra compilation.
 */
PFalg_op_t * PFalg_dummy (void);

/**
 * Staircase join between two relations. Each such join corresponds
 * to the evaluation of an XPath location step.
 */
PFalg_op_t * PFalg_scjoin (PFalg_op_t *doc, PFalg_op_t *n,
			   PFalg_op_t *dummy);

/**
 * Disjoint union of two relations.
 * Both argument must have the same schema.
 */
PFalg_op_t * PFalg_disjunion (PFalg_op_t *, PFalg_op_t *);


/**
 * Intersection between two relations.
 * Both argument must have the same schema.
 */
PFalg_op_t * PFalg_intersect (PFalg_op_t *, PFalg_op_t *);


/**
 * Difference of two relations.
 * Both argument must have the same schema.
 */
PFalg_op_t * PFalg_difference (PFalg_op_t *, PFalg_op_t *);

/** Constructor for projection list item */
PFalg_proj_t PFalg_proj (PFalg_att_t new, PFalg_att_t old);

/**
 * Construct projection list (#PFalg_proj_list_t)
 * (actually just a wrapper for #PFalg_project_()).
 *
 * #PFalg_project_() needs an information about the length of
 * the array argument passed. This macro computes this length
 * by arithmetics involving the sizeof operator and prepends
 * that information to the invocation of #PFalg_project_().
 *
 * You may thus call this macro with the projection argument
 * @a n (pointer to #PFalg_op_t) and an arbitrary number of
 * projection attributes (as #PFalg_proj_t). (You may want to
 * construct the latter using #PFalg_proj().)
 *
 * If you include the file algebra_mnemonic.h, this macro will
 * be available under the abbreviated name #project(). The
 * projection list items can then be constructed using #proj().
 */
#define PFalg_project(n,...)                                        \
    PFalg_project_ ((n),                                            \
                    (sizeof ((PFalg_proj_t[]) { __VA_ARGS__ })      \
                        / sizeof (PFalg_proj_t)),                   \
                    (PFalg_proj_t[]) { __VA_ARGS__ } )
PFalg_op_t *PFalg_project_ (PFalg_op_t *n, int count, PFalg_proj_t *p);


PFalg_op_t * PFalg_rownum (PFalg_op_t *n, PFalg_att_t a,
                           PFalg_attlist_t s, PFalg_att_t p);


/** Constructor for selection of not-0 column values. */
PFalg_op_t * PFalg_select (PFalg_op_t *n, PFalg_att_t att);

/**
 * Constructor for type test of column values. The result is
 * stored in newly created column.
 */
PFalg_op_t * PFalg_type (PFalg_op_t *n, PFalg_att_t res,
			 PFalg_att_t att, PFalg_simple_type_t ty);

/**
 * Constructor for the type cast of a column.
 */
PFalg_op_t * PFalg_cast (PFalg_op_t *n, PFalg_att_t att,
                         PFalg_simple_type_t ty);

/** Constructor for arithmetic addition operators. */
PFalg_op_t * PFalg_add (PFalg_op_t *n, PFalg_att_t res,
                        PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic subtraction operators. */
PFalg_op_t * PFalg_subtract (PFalg_op_t *n, PFalg_att_t res,
                             PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic multiplication operators. */
PFalg_op_t * PFalg_multiply (PFalg_op_t *n, PFalg_att_t res,
                             PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic division operators. */
PFalg_op_t * PFalg_divide (PFalg_op_t *n, PFalg_att_t res,
                           PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for arithmetic modulo operators. */
PFalg_op_t * PFalg_modulo (PFalg_op_t *n, PFalg_att_t res,
                           PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric equal operators. */
PFalg_op_t * PFalg_eq (PFalg_op_t *n, PFalg_att_t res,
                       PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric greater-than operators. */
PFalg_op_t * PFalg_gt (PFalg_op_t *n, PFalg_att_t res,
                       PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for numeric negation operators. */
PFalg_op_t * PFalg_neg (PFalg_op_t *n, PFalg_att_t res,
			PFalg_att_t att);

/** Constructor for boolean AND operators. */
PFalg_op_t * PFalg_and (PFalg_op_t *n, PFalg_att_t res,
			PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean OR operators. */
PFalg_op_t * PFalg_or (PFalg_op_t *n, PFalg_att_t res,
		       PFalg_att_t att1, PFalg_att_t att2);

/** Constructor for boolean NOT operators. */
PFalg_op_t * PFalg_not (PFalg_op_t *n, PFalg_att_t res,
			PFalg_att_t att);

/** Constructor for operators forming (partitioned) sum of a column. */
PFalg_op_t * PFalg_sum (PFalg_op_t *n, PFalg_att_t res,
			PFalg_att_t att, PFalg_att_t part);

/** Constructor for (partitioned) row counting operators. */
PFalg_op_t * PFalg_count (PFalg_op_t *n, PFalg_att_t res,
			  PFalg_att_t part);

/** Constructor for sequence type matching operator for `1' occurrence */
PFalg_op_t * PFalg_seqty1 (PFalg_op_t *n,
                           PFalg_att_t res, PFalg_att_t att, PFalg_att_t part);

/** Cast nat to int. */
PFalg_op_t * PFalg_cast_item (PFalg_op_t *o);

/**
 * Constructor for `all' test.
 * (Do all tuples in partition @a part carry the value true in
 * attribute @a item ?)
 */
PFalg_op_t * PFalg_all (PFalg_op_t *n, PFalg_att_t res,
                        PFalg_att_t att, PFalg_att_t part);

/** Constructor for duplicate elimination operators. */
PFalg_op_t * PFalg_distinct (PFalg_op_t *n);


/*********** node construction functionality *************/

/*
 * Algebra operators instantiated by the following functions
 * create new XML tree nodes.  Their algebraic result is a
 * (document fragment, expression result) pair.  The two components
 * of such a pair may be derived with the operators `fragment'
 * (see #PFalg_fragment()) that extracts the document fragment
 * part, and `roots' (see #PFalg_roots()) for the expression
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
PFalg_op_t * PFalg_element (PFalg_op_t *doc, PFalg_op_t *tags,
                            PFalg_op_t *cont);

/** Constructor for attribute operators. */
PFalg_op_t * PFalg_attribute (PFalg_op_t *tags, PFalg_op_t *cont);

/** Constructor for text node operators. */
PFalg_op_t * PFalg_textnode (PFalg_op_t *cont);

/** Constructor for document node operators. */
PFalg_op_t * PFalg_docnode (PFalg_op_t *doc, PFalg_op_t *cont);

/** Constructor for comment operators. */
PFalg_op_t * PFalg_comment (PFalg_op_t *cont);

/** Constructor for processing instruction operators. */
PFalg_op_t * PFalg_processi (PFalg_op_t *cont);

/**
 * Constructor required for fs:item-sequence-to-node-sequence()
 * functionality
 */
PFalg_op_t * PFalg_strconcat (PFalg_op_t *n);

/** Constructor for pf:merge-adjacent-text-nodes() functionality */
PFalg_op_t * PFalg_pf_merge_adjacent_text_nodes (PFalg_op_t *doc,
						 PFalg_op_t *n);


/**
 * Access to (persistently stored) XML documents, the fn:doc()
 * function.  Returns a (frag, result) pair.
 */
PFalg_op_t * PFalg_doc_tbl (PFalg_op_t *rel);



/**************** document fragment related stuff ******************/

/**
 * Extract the expression result part from a (frag, result) pair.
 * The result of this algebra operator is a relation with schema
 * iter | pos | item.
 */
PFalg_op_t * PFalg_roots (PFalg_op_t *n);

/**
 * Extract the document fragment part from a (frag, result) pair.
 * It typically contains newly constructed nodes of some node
 * construction operator.  The document representation is dependent
 * on the back-end system.  Hence, the resulting algebra node does
 * not have a meaningful relational schema (in fact, the schema
 * component will be set to NULL).
 */
PFalg_op_t * PFalg_fragment (PFalg_op_t *n);



/****************** fragment set handling *******************/

/*
 * These functions implement a _set_ of document fragments. This
 * set is held duplicate free (in terms of C pointer identity),
 * and may be turned into an algebra expression tree (with
 * `frag_union' operators) with the function PFalg_set_to_alg().
 */

/**
 * Create a new, empty set of fragments.
 */
PFalg_set_t *PFalg_empty_set (void);

/**
 * Create a new set of fragments, holding one fragment.
 */
PFalg_set_t *PFalg_set (PFalg_op_t *n);

/**
 * Form a set-oriented union between two sets of fragments.
 * Eliminate duplicate fragments.
 */
PFalg_set_t *PFalg_set_union (PFalg_set_t *frag1, PFalg_set_t *frag2);

/**
 * Convert a set of fragments into an algebra expression. The fragments
 * are unified by a special, fragment-specific union operator. It creates
 * a binary tree in which the bottom-most leaf is always represented by an
 * empty fragment.
 */
PFalg_op_t *PFalg_set_to_alg (PFalg_set_t *frags);


/** Form algebraic disjoint union between two fragments. */
PFalg_op_t * PFalg_frag_union (PFalg_op_t *n1, PFalg_op_t *n2);

/** Constructor for an empty fragment */
PFalg_op_t * PFalg_empty_frag (void);


/****************************************************************/


/**
 * A `serialize' node will be placed on the very top of the algebra
 * expression tree.
 */
PFalg_op_t * PFalg_serialize (PFalg_op_t *doc, PFalg_op_t *alg);


#endif  /* ALGEBRA_H */

/* vim:set shiftwidth=4 expandtab: */
