/**
 * @file
 *
 * Declarations regarding the transformation from logical algebra to SQL.
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

#ifndef SQL_H
#define SQL_H

#include "algebra.h"

/*..................... type definitions ......................*/

enum PFsql_special_t {
      sql_col_pre
    , sql_col_level
    , sql_col_size
    , sql_col_kind
    , sql_col_prop
    , sql_col_tag
    , sql_col_nameid
    , sql_col_value
    , sql_col_name
    , sql_col_twig_pre
    , sql_col_iter
    , sql_col_pos
    , sql_col_guide
    , sql_col_max
};
typedef enum PFsql_special_t PFsql_special_t;

/* Reserve identifiers. */
#define PF_SQL_TABLE_SYSDUMMY1  1
#define PF_SQL_TABLE_FRAG       2
#define PF_SQL_TABLE_RESULT     3
#define PF_SQL_TABLE_DOC        4
#define PF_SQL_RES_TABLE_COUNT (PF_SQL_TABLE_DOC + 1)

#define PF_SQL_ALIAS_UNBOUND    0
#define PF_SQL_RES_ALIAS_COUNT (PF_SQL_ALIAS_UNBOUND + 1)

#define PF_SQL_COLUMN_SPECIAL    0
#define PF_SQL_RES_COLUMN_COUNT (PF_SQL_COLUMN_SPECIAL + 1)

/**
 * SQL identifier for table names.
 */
typedef unsigned int PFsql_tident_t;
/**
 * SQL identifier for table aliases (correlation names).
 */
typedef unsigned int PFsql_aident_t;
/**
 * SQL identifier for column names.
 */
struct PFsql_col_t {
    PFsql_special_t     spec;
    PFalg_att_t         att;
    PFalg_simple_type_t ty;
    unsigned int        id;
};
typedef struct PFsql_col_t PFsql_col_t;

/*............... SQL operators (relational operators) ............*/

/**
 * SQL operator kinds.
 * Each SQL tree node has a type, specified
 * in this enum.
 */
enum PFsql_kind_t {
      sql_root              /* The root of the SQL operator tree:
                               it combines the schema information 
                               with the query operators */

    , sql_ser_info          /* an item of a sequence of schema information
                               (used by serialization) */
    , sql_ser_comment       /* a comment within the schema information */
    , sql_ser_mapping       /* a mapping between relation and column names
                               and the hard-coded names the serialization
                               expects (required by serialization) */
    , sql_ser_doc           /* identifier for the schema of document nodes
                               (required by serialization) */
    , sql_ser_res           /* identifier for the schema of the result sequence
                               (required by serialization) */
    , sql_ser_type          /* type */

    , sql_tbl_def           /* a table name definition */
    , sql_schema_tbl_name   /* a database table reference (schema+tablename) */
    , sql_tbl_name          /* a table name reference */
    , sql_alias             /* SQL alias (a table reference) */
    , sql_column_list       /* an item of a list of column names */
    , sql_column_name       /* SQL column name (a column reference) */
    , sql_star              /* a SQL wildcard '*' */

    , sql_with              /* WITH operator 
                               (second child of the sql_root operator) */
    , sql_cmmn_tbl_expr     /* common table expression */
    , sql_comment           /* comment */
    , sql_bind              /* bind a common table expression */
    , sql_nil               /* end of list operator */

    , sql_select            /* SELECT expression */
    , sql_select_list       /* an item of a list of selection criteria
                               (first child of a SELECT expression) */
    , sql_column_assign     /* AS binding operator: binds an expression
                               to a column name */
    , sql_from_list         /* an item of a list of FROM list criteria
                               (second child of a SELECT expression) */
    , sql_alias_bind        /* AS binding operator: binds a table to an alias
                               (left child of sql_from_list operator) */
    , sql_on                /* a join expression
                               (left child of sql_from_list operator) */
    , sql_right_outer_join  /* RIGHT OUTER JOIN clause */
    , sql_left_outer_join   /* LEFT OUTER JOIN clause */
    , sql_inner_join        /* INNER JOIN clause */

    , sql_union             /* UNION ALL expression */
    , sql_diff              /* EXCEPT ALL expression */

    , sql_lit_int           /* literal integer */
    , sql_lit_lng           /* literal 64 bit integer */
    , sql_lit_dec           /* literal decimal */
    , sql_lit_str           /* literal string */
    , sql_lit_null          /* NULL value */

    , sql_add               /* add expression */
    , sql_sub               /* subtract expression */
    , sql_mul               /* multiplication expression */
    , sql_div               /* divide expression */

    , sql_floor             /* floor expression */
    , sql_ceil              /* ceil expression */
    , sql_modulo            /* modulo expression */
    , sql_abs               /* abs expression */

    , sql_concat            /* string concatenation */

    , sql_is                /* IS predicate */
    , sql_is_not            /* IS NOT predicate */
    , sql_eq                /* = comparison */
    , sql_gt                /* > comparison */
    , sql_gteq              /* >= comparison */
    , sql_between           /* range predicate */
    , sql_like              /* like comparison */
    , sql_in                /* in comparison */
    , sql_stmt_list          /* an item of a list of statments 
                               (second argument of a sql_in operator) */
    , sql_list_list         /* list of lists */
    , sql_not               /* negation */
    , sql_and               /* AND expression */
    , sql_or                /* OR expression */

    , sql_count             /* COUNT () aggregate */
    , sql_max               /* MAX () aggregate */
    , sql_sum               /* SUM () aggregate */
    , sql_min               /* MIN () aggregate */
    , sql_avg               /* AVG () aggregate */

    , sql_over              /* OVER expression */
    , sql_rownumber         /* ROWNUMBER () function
                               (first argument of a sql_over operator) */
    , sql_wnd_clause        /* a window clause
                               (second argument of a sql_over operator) */
    , sql_order_by          /* ORDER BY clause
                               (first argument of a sql_wnd_clause operator) */
    , sql_sortkey           /* an item of a list of order criteria */
    , sql_partition         /* PARTITION BY clause
                               (second argument of a sql_wnd_clause operator) */

    , sql_cast              /* CAST expression */
    , sql_type              /* a SQL type */

    , sql_coalesce          /* COALESCE () function */

    , sql_case              /* case operator */
    , sql_when              /* WHEN .. THEN .. clause */
    , sql_else              /* ELSE .. clause */
    , sql_values            /* Table Functions */
};
/* SQL operator kinds. */
typedef enum PFsql_kind_t PFsql_kind_t;

enum ser_report_t {
       ser_no      = 0      /*< no serialization possible
                                just by expanding the schema.*/
    ,  ser_tc      = 1      /*< serialization by expanding schema is
                                possible but we have to take care, if
                                ancestors are willing to let us do this. */
    ,  ser_yes     = 2      /*< serialization is possible, there are no
                                constructors involved. */
};
typedef enum ser_report_t ser_report_t;

/**
 * Semantic content in SQL operators.
 */
union PFsql_sem_t {

    struct {
        char *str;           /**< Comment. */
    } comment;
 
    struct {
        char *str;             /**< Schema name */
    } schema;
 
    struct {
        PFsql_tident_t name;    /**< Table name. */
    } tbl;
 
    struct {
        bool distinct;         /**< Boolean indicating if elimination
                                    of equal tuples is required. */
    } select;
 
    struct {
        PFsql_aident_t alias;   /**< Alias name. */
        PFsql_col_t   *name;    /**< Column name. */
    } column;
 
    struct {
        PFsql_aident_t name;    /**< Alias name. */
    } alias;
 
    /* semantic content for literal values */
    struct {
        union {
            int i;               /**< Integer value. */
            long long int l;     /**< Long integer value. */
            char *s;             /**< String value. */
            bool b;              /**< Boolean value. */
            double dec;          /**< Decimal value. */
        } val;
    } atom;
    
    struct {
        bool dir_asc;        /**< Boolean indicating if the sort criterion
                                  has to be ordered ascending or descending. */
    } sortkey;
 
    struct {
        PFalg_simple_type_t t; /**< semantic information for type */
    } type;
};
typedef union PFsql_sem_t PFsql_sem_t;

/** Each node has at most for childs */
#define PFSQL_OP_MAXCHILD  5

/**
 * SQL operator node.
 * Represents a single SQL tree node in the
 * SQL tree.
 */
struct PFsql_t {
    PFsql_kind_t    kind;                     /**< Operator kind. */
    PFsql_sem_t     sem;                      /**< Semantic content. */
    struct PFsql_t *child[PFSQL_OP_MAXCHILD]; /**< The operators children. */
};
typedef struct PFsql_t PFsql_t;

/**
 * Annotations to logical algebra tree nodes. Used during
 * SQL code generation.
 */
struct PFsql_alg_ann_t {
    unsigned    bound:1;       /**< indicates if the operator has been bound */

    PFarray_t   *colmap;       /**< Mapping table that maps (logical)
                                    attribute/type  pairs to their
                                    SQL expression or in case of
                                    a binding to their column names.
                                    (select list) */
    PFarray_t   *frommap;      /**< table--alias mappings */
    PFarray_t   *wheremap;     /**< contains references to the boolean
                                    in colmap */
    
    PFsql_t     *fragment;     /**< a fragment reference */
    PFsql_t     *content_size; /**< a reference to a content-size binding */

    unsigned int twig_pre;     /**< local pre value for constructions
                                    with the twig-constructor */
    int          twig_size;    /**< local size value for constructions
                                    with the twig-constructor */
    unsigned int twig_level;   /**< local level value for constructions
                                    with the twig-constructor */
    ser_report_t ser_report;       /**< serialization report */
};
typedef struct PFsql_alg_ann_t PFsql_alg_ann_t;

/* .......... General .......... */

#define PFsql_generic_list(fun,...)                                         \
            fun (sizeof ((PFsql_t *[]) {__VA_ARGS__}) / sizeof (PFsql_t *), \
                 (const PFsql_t *[]) {__VA_ARGS__})

/**
 * A sequence of `common table expression'.
 */
PFsql_t* PFsql_common_table_expr(const PFsql_t *old, const PFsql_t *new);

/**
 * A sequence of columns.
 */
#define PFsql_column_list(...) \
            PFsql_generic_list (PFsql_column_list_, __VA_ARGS__)
/**
 * A sequence of select_list-expressions.
 */
#define PFsql_select_list(...) \
            PFsql_generic_list (PFsql_select_list_, __VA_ARGS__)
/**
 * A sequence of from_list-expressions.
 */
#define PFsql_from_list(...) \
            PFsql_generic_list (PFsql_from_list_, __VA_ARGS__)
/**
 * A sequence of where_list-expressions.
 */
#define PFsql_where_list(...) \
            PFsql_generic_list (PFsql_where_list_, __VA_ARGS__)
/**
 * A sequence of in_list-expressions.
 */
#define PFsql_stmt_list(...) \
            PFsql_generic_list (PFsql_stmt_list_, __VA_ARGS__)

/**
 * A sequence of lists
 */
#define PFsql_list_list(...) \
            PFsql_generic_list (PFsql_list_list, __VA_ARGS__)

/**
 * A sequence of case-expressions.
 */
#define PFsql_case(...) \
            PFsql_generic_list (PFsql_case_, __VA_ARGS__)

/* .......... Constructors .......... */

/**
 * The root of the SQL operator tree:
 * it combines the schema information with the query operators.
 */
PFsql_t * PFsql_root (const PFsql_t *ser_info, const PFsql_t *query);

/* .......... Serialization information .......... */

/**
 * An item of of a sequence of schema information,
 * used by the serialization.
 */
PFsql_t * PFsql_serialization_info_item (const PFsql_t *info,
                                         const PFsql_t *list);

PFsql_t * PFsql_serialization_type (const PFsql_t *type,
                                    const PFsql_t *qtype);

/**
 * Some specific schema information used by the serializer.
 * We communicate to the serializer the document and 
 * result relation.
 * Further we communicate some of the special attributes,
 * the serializer needs. 
 */
PFsql_t * PFsql_serialization_name_mapping (PFsql_t *schema, PFsql_t *column);
PFsql_t * PFsql_serialization_document (void);
PFsql_t * PFsql_serialization_result (void);
/**
 * Construct a comment in the serialization information.
 */
PFsql_t * PFsql_serialization_comment (char *cmmnt);

/* .......... Tables .......... */

/**
 * Construct a SQL tree node representing a definition of a relation.
 */
PFsql_t * PFsql_table_def (PFsql_tident_t name, PFsql_t *columnlist);
/**
 * Collate a schema and a table_name, to identify a table in a database.
 */
PFsql_t * PFsql_schema_table_name (const char *schema,
                                   const PFsql_t *table_name);
/**
 * Construct a SQL tree node representing a reference to a relation.
 */
PFsql_t * PFsql_table_name (PFsql_tident_t name);
/**
 * Construct a SQL tree node representing a SQL `correlation name'.
 */
PFsql_t * PFsql_alias (PFsql_aident_t name);

/* .......... Columns .......... */

/**
 * A sequence of columns.
 *
 * @note
 *   Normally you should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_column_list (or its mnemonic variant
 *   #column_list instead. It will automatically calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 *
 * @param   count Number of columns in the array that follows.
 * @param   list  Array of exactly @a count column nodes.
 * @return        A chain of column nodes.
 */
PFsql_t * PFsql_column_list_ (unsigned int count, const PFsql_t **columns);
/**
 * Create an SQL tree node representing a SQL column name.
 */
PFsql_t * PFsql_column_name (PFsql_aident_t alias, PFsql_col_t *name);
/**
 * Create a SQL tree node representing the SQL wildcard `*'.
 */
PFsql_t * PFsql_star (void);

/* .......... Top Level Query Constructs .......... */

/**
 * Create a SQL tree node representing the SQL `WITH' operator.
 */
PFsql_t * PFsql_with (const PFsql_t *a, const PFsql_t *fs);
/**
 * Create a SQL tree representing a list of SQL
 * `common table expressions'.
 */
PFsql_t * PFsql_common_table_expr_ (int count, const PFsql_t **stmts);
/**
 * Create a SQL tree node representing a comment.
 *
 * @param   fmt   Format string.
 * @return        A comment formatted comment
 *                                string.
 */
PFsql_t * PFsql_comment (const char *fmt, ...);
/**
 * Bind a `common table expression' to a
 * name, so it can be referenced within
 * the whole statement.
 */
PFsql_t * PFsql_bind (PFsql_t *table_name, PFsql_t *query);
/**
 * Create a SQL tree node representing a nil
 * object for all kind of lists.
 */
PFsql_t * PFsql_nil (void);

/* .......... Select .......... */

/**
 * Create a SQL tree node representing the SQL `SELECT' operator.
 *
 * @note
 *   Represents the SELECT ALL statement in SQL.
 */
PFsql_t * PFsql_select (bool distinct,
                        const PFsql_t *selectlist,
                        const PFsql_t *fromlist,
                        const PFsql_t *wherelist,
                        const PFsql_t *orderbylist,
                        const PFsql_t *groupbylist);
/**
 * A sequence of select_list-expressions.
 *
 * @note
 *   Normally you should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_select_list (or its mnemonic variant
 *   #select_list instead. It will automatically calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 *
 * @param   count Number of expressions in the array that follows.
 * @param   list  Array of exactly @a count expression nodes.
 * @return        A chain of expression nodes.
 */
PFsql_t * PFsql_select_list_ (unsigned int count, const PFsql_t ** list);
/**
 * Create an SQL tree node representing the SQL
 * `AS' to bind SQL statements to attribute names.
 */
PFsql_t * PFsql_column_assign (const PFsql_t *expr, const PFsql_t *column_name);
/**
 * A sequence of from_list-expressions.
 *
 * @note
 *   Normally you should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_from_list (or its mnemonic variant
 *   #from_list instead. It will automatically calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 *
 * @param   count Number of expressions in the array that follows.
 * @param   list  Array of exactly @a count expression nodes.
 * @return        A chain of expression nodes.
 */
PFsql_t * PFsql_from_list_ (unsigned int count, const PFsql_t **list);
/**
 * Adds an item to the front of an existing from_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t * PFsql_add_from (const PFsql_t *list, const PFsql_t *item);
/**
 * Bind a table to a correlation name (alias).
 */
PFsql_t * PFsql_alias_bind (const PFsql_t *expr, const PFsql_t *alias);
/**
 * Create the part of the join clause
 * where you can define the join predicate.
 */
PFsql_t * PFsql_on (PFsql_t *join, PFsql_t *expr);
/**
 * Join two relations with a `Right Outer Join'.
 */
PFsql_t * PFsql_right_outer_join (PFsql_t *tblref1, PFsql_t *tblref2);

/**
 * Join two relations with `Left Outer Join'.
 */
PFsql_t * PFsql_left_outer_join (PFsql_t *tblref1, PFsql_t *tblref2);

/**
 * Join two relations with `Inner Join'.
 */
PFsql_t * PFsql_inner_join (PFsql_t *tblref1, PFsql_t *tblref2);

/**
 * A sequence of where_list-expressions.
 *
 * @note
 *   Normally you should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_where_list (or its mnemonic variant
 *   #where_list instead. It will automatically calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 *
 e @param   count Number of expressions in the array that follows.
 * @param   list  Array of exactly @a count expression nodes.
 * @return        A chain of expression nodes.
 */
PFsql_t * PFsql_where_list_ (unsigned int count, const PFsql_t **list);

/* .......... Union .......... */

/**
 * Create a SQL tree node representing the SQL
 * `UNION ALL' operator.
 */
PFsql_t * PFsql_union (const PFsql_t *a, const PFsql_t *b);

/* .......... Difference .......... */

/**
 * Create a SQL tree node representing the SQL
 * `EXCEPT ALL' operator.
 */
PFsql_t * PFsql_difference (const PFsql_t *a, const PFsql_t *b);

/* .......... Literal construction .......... */

/**
 * Create a SQL tree node representing a literal integer.
 *
 * @param   i     The integer value to represent in SQL.
 */
PFsql_t * PFsql_lit_int (int i);
/**
 * Create a SQL tree node representing a literl 64bit integer.
 *
 * @param   l  The integer value to represent in SQL.
 */
PFsql_t * PFsql_lit_lng (long long int l);
/**
 * Create a SQL tree node representing a literal string.
 *
 * @param   s  The string value to represent in SQL.
 */
PFsql_t * PFsql_lit_str (const char *s);
/**
 * Create a SQL tree node representing a literal decimal value.
 *
 * @param dec The decimal value to represent in SQL.
 */
PFsql_t * PFsql_lit_dec (float dec);
/**
 * Create a SQL tree node representing a NULL.
 */
PFsql_t * PFsql_null (void);

/* .......... Arithmetic Operators .......... */

/**
 * Create a SQL tree node representing an arithmetic add
 * operator.
 *
 * @param   a  First addend.
 * @param   b  Second addend.
 */
PFsql_t * PFsql_add (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing an arithmetic
 * subtract operator.
 *
 * @param   a  The minuend.
 * @param   b  The subtrahend.
 */
PFsql_t * PFsql_sub (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing an arithmetic
 * multiplication operator.
 *
 * @param   a  The multiplicand.
 * @param   b  The multiplier.
 */
PFsql_t * PFsql_mul (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing an arithmetic
 * division operator.
 *
 * @param   a  The divident.
 * @param   b  The quotient.
 */
PFsql_t * PFsql_div (const PFsql_t *a, const PFsql_t *b);

/* ........... Integer Functions ................ */

/**
 * Create a SQL tree node representing the SQL
 * floor operator.
 */
PFsql_t * PFsql_floor (const PFsql_t *a);

/**
 * Create a SQL tree node representing the SQL
 * ceil operator.
 */
PFsql_t * PFsql_ceil (const PFsql_t *a);

/**
 * Create a SQL tree node representing the SQL
 * modulo operator.
 */
PFsql_t * PFsql_modulo (const PFsql_t *a, const PFsql_t *b);

/**
 * Create a SQL tree node representing the SQL
 * abs operator.
 */
PFsql_t * PFsql_abs (const PFsql_t *a);

/* .......... String Functions .......... */

/**
 * Create a SQL tree node representing the SQL
 * concat operator.
 */
PFsql_t * PFsql_concat (const PFsql_t *a, const PFsql_t *b);

/* .......... Table Functions ........... */
PFsql_t *PFsql_values (const PFsql_t *a);
PFsql_t *PFsql_list_list_ (unsigned int count, const PFsql_t **list);

/* .......... Boolean Operators .......... */

/**
 * Create a SQL tree node representing a boolean
 * `IS' operator.
 */
PFsql_t *
PFsql_is (const PFsql_t *a, const PFsql_t *b);

/**
 * Create a SQL tree node representing a boolean
 * `IS' operator.
 */
PFsql_t *
PFsql_is_not (const PFsql_t *a, const PFsql_t *b);

/**
 * Create a SQL tree node representing a boolean
 * (comparison) equality operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t * PFsql_eq (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing a boolean
 * (comparison) greater-than operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t * PFsql_gt (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing a boolean
 * (comparison) greater-than-or_equal operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t * PFsql_gteq (const PFsql_t *a, const PFsql_t *b);

/*
 * Create a SQL tree node representing a 
 * `between' predicate.
 */
PFsql_t * PFsql_between(const PFsql_t *clmn, const PFsql_t *a, const PFsql_t *b);

/**
 * Create a tree node representing the SQL
 * `like' statement to compare a string
 * with a certain pattern.
 */
PFsql_t * PFsql_like (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing the in operator
 */
PFsql_t * PFsql_in (const PFsql_t *column, const PFsql_t *list);
PFsql_t * PFsql_stmt_list_ (unsigned int count, const PFsql_t **list); 

/**
 * Create a SQL tree node representing a boolean
 * negation operator.
 *
 * @param   a
 */
PFsql_t * PFsql_not (const PFsql_t * a);
/**
 * Create a SQL tree node representing a boolean
 * `and' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t * PFsql_and (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing a boolean
 * `or' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t * PFsql_or (const PFsql_t *a, const PFsql_t *b);

/* .......... Aggregate Functions .......... */

/**
 * Construct a SQL tree node representing
 * the aggregate function `COUNT'.
 */
PFsql_t * PFsql_count (const PFsql_t *column);
/**
 * Construct a SQL tree node representing
 * the aggregate function `MAX'.
 */
PFsql_t * PFsql_max (const PFsql_t *column);
/**
 * Construct a SQL tree node representing
 * the aggregate function `MIN'.
 */
PFsql_t * PFsql_min (const PFsql_t *column);
/**
 * Construct a SQL tree node representing
 * the aggregate function `AVG'.
 */
PFsql_t * PFsql_avg (const PFsql_t *column);
/**
 * Construct a SQL tree node representing
 * the aggregate function `SUM'.
 */
PFsql_t * PFsql_sum (const PFsql_t * column);

/* .......... OLAP Functionality .......... */

/**
 * Create a SQL tree node representing the SQL `OVER' keyword.
 */
PFsql_t * PFsql_over (const PFsql_t *a, const PFsql_t *b);
/**
 * Create a SQL tree node representing SQL `ROWNUMBER()' function.
 */
PFsql_t * PFsql_rownumber (void);
/**
 * The whole clause, consisting of sortkey- and
 * partition expressions is called window_clause.
 */
PFsql_t * PFsql_window_clause (const PFsql_t *part_clause,
                               const PFsql_t *order_clause);
/**
 * Create a SQL tree node representing the SQL `ORDER BY' clause.
 */
PFsql_t * PFsql_order_by (const PFsql_t *sortkey_list);
/**
 * The sortkey expressions are used in OLAP-functions
 * to provide an ordering among the tuples in a relation.
 */
PFsql_t * PFsql_sortkey (PFsql_t *sortkey, bool dir_asc, PFsql_t *sort_list);
/**
 * We mainly use this to express the partition keyword
 * used by some OLAP-functions.
 */
PFsql_t * PFsql_partition (const PFsql_t *partition_list);

/* .......... Remaining Operators .......... */

/**
 * Construct a SQL type. SQL supports several types
 * like integer or decimals. This is probably most
 * needed when you cast one attribut to another type.
 *
 * @param   t  The type.
 */
PFsql_t * PFsql_type (PFalg_simple_type_t t);
/**
 * Construct a SQL tree representing a SQL
 * `CAST' operator.
 *
 * @param   expr  Expression.
 * @param   t     Type.
 */
PFsql_t * PFsql_cast (const PFsql_t *expr, const PFsql_t *t);
/**
 * Create a tree node representing the SQL
 * `COALESCE' function.
 */
PFsql_t * PFsql_coalesce (PFsql_t *expr1, PFsql_t *expr2);
/**
 * Construct a SQL tree node representing a SQL
 * `case' statement.
 */
PFsql_t * PFsql_case_ (unsigned int count, const PFsql_t **list);
/**
 * Construct a SQL tree node representing a
 * branch within a case-statement.
 */
PFsql_t * PFsql_when (PFsql_t *boolexpr, PFsql_t *expr);
/**
 * Construct a SQL tree node representing an
 * else-branch within a case-statement.
 */
PFsql_t * PFsql_else (PFsql_t *expr);
/**
 * Duplicate a given SQL tree.
 */
PFsql_t * PFsql_op_duplicate (PFsql_t *expr);

/* .......... Printing Functions .......... */

/**
 * Convert the table name @a name into a string.
 *
 * @param name The identifier to convert.
 */
char * PFsql_table_str (PFsql_tident_t name);
/**
 * Convert the alias name @a name to a string.
 *
 * @param name The identifier to convert.
 */
char * PFsql_alias_name_str (PFsql_aident_t name);
/**
 * Convert the @a ident to a string.
 *
 * @param ident The identifier to convert.
 *
 * @note
 *      Pathfinder uses only a very restricted set of
 *  predefined column_names. Because of polymorphic
 *  types our names consists of the name
 *  as it would used by the algebra, an underscore,
 *  and the shortform of the type.
 *  There are also some special names like
 *  kind or size.
 */
char * PFsql_column_name_str (PFsql_col_t *name);
/**
 * Convert the @a type to a string.
 *
 * @param type The type to convert.
 */
char * PFsql_simple_type_str (PFalg_simple_type_t type);

#endif /* __SQL_H__ */

/* vim:set shiftwidth=4 expandtab: */
