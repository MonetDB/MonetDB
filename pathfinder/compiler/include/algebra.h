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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef ALGEBRA_H
#define ALGEBRA_H

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
    nat   nat;     /**< value for natural number atoms (#aat_nat) */
    int   int_;    /**< value for integer atoms (#aat_int) */
    char *str;     /**< value for string atoms (#aat_str)  */
    int   node;    /**< value for node atoms (#aat_node) */
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



/* ............... algebra operators (operators on relations) ............... */

/** algebra operator kinds */
enum PFalg_op_kind_t {
      aop_lit_tbl       /**< literal table */
    , aop_disjunion     /**< union two relations with same schema */
    , aop_cross         /**< cross product (Cartesian product) */
    , aop_project       /**< algebra projection and renaming operator */
    , aop_rownum        /**< consecutive number generation */

    , aop_serialize     /**< serialize algebra expression
                             (Placed on the very top of the tree.) */
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
    int                refctr;     /**< number of references to this node. */
    int                usectr;     /**< How often has the result already been
                                        used? (If this reaches @a refctr, we
                                        may destroy the variable.) */
    char              *bat_prefix; /**< prefix that all BATs will carry that
                                        represent this relation. The attribute
                                        name is appended after a `_' for the
                                        full BAT name (there's one BAT for
                                        each attribute). */
    struct PFalg_op_t *child[PFALG_OP_MAXCHILD];
};
/** algebra operator node */
typedef struct PFalg_op_t PFalg_op_t;



/* ***************** Constructors ******************* */

/** construct literal natural number (atom) */
PFalg_atom_t PFalg_lit_nat (nat value);

/** construct literal integer (atom) */
PFalg_atom_t PFalg_lit_int (int value);

/** construct literal string (atom) */
PFalg_atom_t PFalg_lit_str (char *value);

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
 * Cross product (Cartesian product) of two relations.
 * No duplicate attribute names allowed.
 */
PFalg_op_t * PFalg_cross (PFalg_op_t *n1, PFalg_op_t *n2);

/**
 * Disjoint union of two relations.
 * Both argument must have the same schema.
 */
PFalg_op_t * PFalg_disjunion (PFalg_op_t *, PFalg_op_t *);

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

PFalg_op_t * PFalg_serialize (PFalg_op_t *);

/**
 * Core to algebra tree compilation
 */
/* PFalg_op_t * PFalg (PFcnode_t *c); */

#endif  /* ALGEBRA_H */

/* vim:set shiftwidth=4 expandtab: */
