/**
 * @file
 *
 * Declarations regarding the transformation from logical algebra to sql. 
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
#define ATT_BITS 5
#define TYPE_BITS 4
#define SPEC_BITS 3
#define ASCII_A   97

/* special bits for column names */
#define sql_col_pre     0x00000002
#define sql_col_level   0x00000004
#define sql_col_size    0x00000008
#define sql_col_kind    0x00000010
#define sql_col_prop    0x00000020
#define sql_col_tag     0x00000040
typedef unsigned int PFsql_special_t;

/* Reserve identifiers. */
#define PF_SQL_TABLE_SYSDUMMY1  1
#define PF_SQL_TABLE_FRAG       2
#define PF_SQL_TABLE_RESULT     3
#define PF_SQL_TABLE_DOC        4
#define PF_SQL_TABLE_TIME_PRINT 23
#define PF_SQL_RES_TABLE_COUNT (PF_SQL_TABLE_TIME_PRINT + 1)

#define PF_SQL_CORRELATION_UNBOUNDED 0
#define PF_SQL_CORRELATION_TIME_PRINT 1
#define PF_SQL_RES_CORRELATION_COUNT (PF_SQL_CORRELATION_TIME_PRINT + 1)

#define PF_SQL_COLUMN_TIME_PRINT 23
#define PF_SQL_RES_COLUMN_COUNT (PF_SQL_COLUMN_TIME_PRINT + 1)

/**
 * Type to count the tuples in a table.
 */
typedef unsigned int PFsql_tuple_count_t;
/**
 * Type to count the atoms in a tuple.
 */
typedef unsigned int PFsql_atom_count_t;
/**
 * Type to count the schema items.
 */
typedef unsigned int PFsql_schema_item_count_t;
/**
 * Type to count attributes.
 */
typedef unsigned int PFsql_att_count_t;
/**
 * Type to count types in an itemset.
 */
typedef unsigned int PFsql_type_count_t;
/**
 * SQL identifiers are unsigned ints.
 */
typedef unsigned int PFsql_ident_t;
/**
 * SQL correlation name.
 */
typedef unsigned int PFsql_correlation_name_t;

/*............... SQL operators (relational operators) ............*/

/**
 * SQL operator kinds.
 * Each SQL tree node has a type, specified
 * in this enum.
 */
enum PFsql_kind_t {
   sql_tmp_tbl          =  1,
   sql_comment          =  2,
   sql_cmmn_tbl_expr    =  3,
   sql_tbl_name         =  4,
   sql_declare          =  5,
   sql_bind             =  6,
   sql_attbind          =  7,
   sql_list_terminator  =  8,
   sql_bln_expr         =  9,
   sql_expr             = 10,
   sql_frm_list         = 11,
   sql_lit_int          = 12,
   sql_lit_lng          = 13,
   sql_lit_str          = 14,
   sql_lit_bln          = 15,
   sql_add              = 16,
   sql_sub              = 17,
   sql_mul              = 18,
   sql_div              = 19,
   sql_gt               = 20,
   sql_eq               = 21,
   sql_not              = 22,
   sql_and              = 23,
   sql_or               = 24,
   sql_with             = 25,
   sql_union            = 26,
   sql_select           = 27,
   sql_slct_list        = 28,
   sql_clmn_assign      = 29,
   sql_clmn_name        = 30,
   sql_over             = 31,
   sql_rownumber        = 32,
   sql_ordr_by          = 33,
   sql_table_ref        = 34,
   sql_dot              = 35,
   sql_alias            = 36,
   sql_crrltn_name      = 37,
   sql_clmn_list        = 38,
   sql_lit_null         = 39,
   sql_whr_list         = 40,
   sql_star             = 41,
   sql_tb_name          = 42,
   sql_schm             = 43,
   sql_cst              = 44,
   sql_type             = 45,
   sql_prttn            = 46,
   sql_prt_expr         = 47,
   sql_srtky_expr       = 48,
   sql_wnd_clause       = 49,
   sql_diff             = 50,
   sql_gteq             = 51,
   sql_count            = 52,
   sql_schm_inf         = 53,
   sql_schm_doc         = 54,
   sql_schm_res         = 55,
   sql_schm_cmmnt       = 56,
   sql_schm_expr        = 57,
   sql_seq              = 58,
   sql_max              = 59,
   sql_on               = 60,
   sql_innr_join        = 61,
   sql_outr_join        = 62,
   sql_rght_outr_join   = 62,
   sql_sum              = 63,
   sql_lit_dec          = 64,
   sql_clsc             = 65,
   sql_expr_list        = 66
};
typedef enum PFsql_kind_t PFsql_kind_t;

/**
 * Semantic content in SQL operators.
 */
union PFsql_sem_t {
    struct {
        PFalg_simple_type_t t;
    } type;
    struct {
        PFsql_ident_t ident;
        struct PFsql_t    *clmn_list;
    } tablename;
    struct {
        char *str;
    } schema;
    struct {
        PFsql_ident_t ident;
    } correlation;
    struct {
        PFsql_correlation_name_t ident;
    } column;
    struct {
        bool distinct;                  /**< No equal tuples in result
                                             relation */
        struct PFsql_t   *select_list;  /**< Select list. */
        struct PFsql_t   *from_list;    /**< Select from list. */
        struct PFsql_t   *where_list;   /**< Select where list. */
        struct PFsql_t   *grpby_list;   /**< Select group by list. */
    } select;
    struct {
         bool distinct;
    } count;
    char             *comment;         /**< String containing comment. */

    struct {
        bool null;
        union {
            int              i;                /**< Integer containing
                                                    literal. */   
            long long int    l;                /**< Long integer
                                                    containing literal. */
            char             *s;               /**< String containing
                                                    literal string. */
            bool             b;                /**< Boolean containing
                                                    literal boolean. */
            double           dec;
      } val;
    } atom;
    char             *table;           /**< string identifying
                                            the table */
};
typedef union PFsql_sem_t PFsql_sem_t; 

/** Each node has at most for childs */
#define PFSQL_OP_MAXCHILD  3 

/**
 * SQL operator node.
 * Represents a single SQL tree node in the
 * SQL tree.
 */
struct PFsql_t {
    PFsql_kind_t     kind;          /**< Operator kind. */
    PFsql_sem_t      sem;           /**< Semantic content for 
                                            this operator. */
    PFsql_correlation_name_t    crrlname;  /**< Each expression is bound to a
                                                table identified by its
                                                correlation name */
    struct PFsql_t   *child[PFSQL_OP_MAXCHILD]; /**< Childs of this
                                                        operator. */
};
typedef struct PFsql_t PFsql_t;

/**
 * Annotations to logical algebra tree nodes. Used during 
 * SQL code generation.
 */
struct PFsql_alg_ann_t {
    PFsql_t       *tabname;      /**< Table name that this subexpression
                                      has been bound to. NULL otherwise. */
    PFsql_t       *sfw;          /**< SQL code that implements this
                                      subexpression. Should always be an
                                      SFW clause. */
    PFsql_t       *fragment;     /**< table containing a fragment */
    PFarray_t   *colmap;     /**< Mapping table that maps (logical)
                                      attribute/type  pairs to their
                                      SQL expression or in case of 
                                      a binding to their column names. */
    PFarray_t   *wheremap;  /**< contains references to the boolean
                                 in colmap */
};
typedef struct PFsql_alg_ann_t PFsql_alg_ann_t;

/*.................... General ...................*/
/**
 * Decorate an SQL expression with a correlation.
 */
PFsql_t* PFsql_correlation_decorator(PFsql_t *op,
                        PFsql_correlation_name_t crrl);

/*.................... Constructors .....................*/

/**
 * Construct an SQL leaf.
 */
PFsql_t* PFsql_op_leaf(PFsql_kind_t kind);

#define PFsql_common_table_expr(...) \
    PFsql_common_table_expr_ (sizeof((PFsql_t *[]) {__VA_ARGS__} ) / \
                sizeof (PFsql_t*), (const PFsql_t *[]) \
                {__VA_ARGS__})
PFsql_t* PFsql_common_table_expr_(int count, const PFsql_t **stmts);

PFsql_t* PFsql_star(void);

PFsql_t* PFsql_select(const PFsql_t *selectlist, const PFsql_t *fromlist,
               const PFsql_t *wherelist, const PFsql_t *grpbylist);

PFsql_t* PFsql_select_distinct(const PFsql_t *selectlist,
        const PFsql_t *fromlist, const PFsql_t *wherelist, PFsql_t *grpbylist);

/**
 * Construct a SQL `union' operator.
 */
PFsql_t* PFsql_union(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a SQL `difference' operator.
 */
PFsql_t* PFsql_difference(const PFsql_t *a, const PFsql_t *b);

/*............ Common Operators ............*/

PFsql_t* PFsql_with( const PFsql_t *a );

/**
 * Construct a comment.
 */
PFsql_t* PFsql_comment(const char *fmt, ...);

/**
 * Construct a SQL type.
 */
PFsql_t* PFsql_type(PFalg_simple_type_t t);

/**
 * Construct a SQL tree representing a SQL
 * `CAST' operator.
 */
PFsql_t* PFsql_cast(const PFsql_t *expr, const PFsql_t *t);

PFsql_t* PFsql_seq(const PFsql_t *schm_inf, const PFsql_t *cmtblexpr);

/*............ Aggregat Functions ............*/

PFsql_t* PFsql_coalesce(PFsql_t *expr);

PFsql_t* PFsql_count(bool dist, const PFsql_t *expr);

PFsql_t* PFsql_max(const PFsql_t *clmn);

PFsql_t* PFsql_sum(const PFsql_t *clmn);

/**
 * Construct a SQL `rownumber' operator.
 */
PFsql_t* PFsql_rownumber(void);

/**
 * Construct a SQL `over' operator.
 */
PFsql_t* PFsql_over(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a SQL `partition' clause.
 */
PFsql_t* PFsql_partition(const PFsql_t *a);

#define PFsql_part_expressions(...) \
   PFsql_part_expressions_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
               sizeof(PFsql_t*), (const PFsql_t *[]) \
               {__VA_ARGS__})
PFsql_t* PFsql_part_expressions_(unsigned int count, const PFsql_t **list);

PFsql_t* PFsql_part_expressions_empty(void);

PFsql_t* PFsql_part_expressions_add(const PFsql_t *list, const PFsql_t *item);

PFsql_t* PFsql_order_by(const PFsql_t *a);

PFsql_t* PFsql_sortkey_expressions_empty(void);

PFsql_t* PFsql_sortkey_expressions_add(const PFsql_t *list,
        const PFsql_t *item);

PFsql_t* PFsql_window_clause(const PFsql_t *partcls, const PFsql_t *ordercls);

/*................. Tables ..................*/

#define PFsql_sysdummy1()     PFsql_table_name(PF_SQL_TABLE_SYSDUMMY1, NULL)
#define PFsql_fragrelation()  PFsql_table_name(PF_SQL_TABLE_FRAG, NULL)
#define PFsql_result(clmnlist)  PFsql_table_name(PF_SQL_TABLE_RESULT, clmnlist)
#define PFsql_document(clmnlist) PFsql_table_name(PF_SQL_TABLE_DOC, clmnlist)
#define PFsql_table_name_default(name)    PFsql_table_name(name, NULL)
PFsql_t* PFsql_table_name(PFsql_ident_t name, PFsql_t *clmnlist);

PFsql_t* PFsql_schema(const char *name);

PFsql_t* PFsql_bind(PFsql_t *a, PFsql_t *b);

PFsql_t* PFsql_tab_name(const PFsql_t *schema, const PFsql_t *table_name);

PFsql_t* PFsql_alias( const PFsql_t *a, const PFsql_t *b);

PFsql_t* PFsql_correlation_name( PFsql_correlation_name_t name );

/*................. Columns ...................*/

PFsql_t* PFsql_column_name(PFsql_ident_t ident);

PFsql_t* PFsql_column_assign(const PFsql_t *a, const PFsql_t *b);

/*................. String conversion ....................*/

/**
 * Convert the @a name to a string.
 */
char* PFsql_table_str(PFsql_ident_t name);

/**
 * Convert the @a ident to a string.
 */
char* PFsql_column_name_str(PFsql_ident_t ident);

/**
 * Convert the @a type to a string.
 */
char* PFsql_simple_type_str(PFalg_simple_type_t type);

/**
 * Convert the @a name to a string.
 */
char* PFsql_correlation_name_str(PFsql_correlation_name_t name);

/*.................. Literal construction .................... */

/**
 * Construct a NULL.
 */
PFsql_t* PFsql_null(void);

/**
 * Construct a literal integer.
 */
PFsql_t* PFsql_lit_int(int i);

/**
 * Construct a literal 64bit integer.
 */
PFsql_t* PFsql_lit_lng(long long int lng);

/**
 * Construct a literal string.
 */
PFsql_t* PFsql_lit_str(const char* s);

/**
 * Construct a literal boolean value.
 */
PFsql_t* PFsql_lit_bln(bool b);

/**
 * Construct a literal decimal value.
 */
PFsql_t* PFsql_lit_dec(float dec);

/*............. List construction ............*/

/**
 * Create a SQL tree node representing a list terminator.
 */
PFsql_t* PFsql_list_terminator(void);

/**
 * A sequence of select_list-expressions.
 */
#define PFsql_select_list(...) \
    PFsql_select_list_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
            sizeof(PFsql_t*), (const PFsql_t *[]) \
            {__VA_ARGS__})
PFsql_t* PFsql_select_list_(unsigned int count, const PFsql_t **list);

/**
 * A sequence of sortkey-expressions.
 */
#define PFsql_sortkey_expressions(...) \
   PFsql_sortkey_expressions_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
            sizeof(PFsql_t*), (const PFsql_t *[]) \
            {__VA_ARGS__})
PFsql_t* PFsql_sortkey_expressions_(unsigned int count, const PFsql_t **list);

/**
 * Create an empty select_list.
 */
PFsql_t* PFsql_select_list_empty(void);

/**
 * Adds an item to the front of an existing select_list.
 */
PFsql_t* PFsql_select_list_add(const PFsql_t *list, const PFsql_t *item);

/**
 * A sequence of from_list-expressions.
 */
#define PFsql_from_list(...) \
    PFsql_from_list_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
            sizeof(PFsql_t*), (const PFsql_t *[]) \
            {__VA_ARGS__})
PFsql_t* PFsql_from_list_(unsigned int count, const PFsql_t **list);

#define PFsql_expression_list(...) \
PFsql_expression_list_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
        sizeof(PFsql_t*), (const PFsql_t *[]) \
        {__VA_ARGS__})
PFsql_t* PFsql_expression_list_(unsigned int count, const PFsql_t **list);

/**
 * Create an empty from_list.
 */
PFsql_t* PFsql_from_list_empty(void);

/**
 * Adds an item to the front of an existing from_list.
 */
PFsql_t* PFsql_from_list_add(const PFsql_t *list, const PFsql_t *item);

/**
 * A sequence of where_list-expressions.
 */
#define PFsql_where_list(...) \
    PFsql_where_list_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
            sizeof(PFsql_t*), (const PFsql_t *[]) \
            {__VA_ARGS__})
PFsql_t* PFsql_where_list_(unsigned int count,
        const PFsql_t **list);

/**
 * Create an empty where_list.
 */
PFsql_t* PFsql_where_list_empty(void);

/**
 * Adds an item to the front of an existing where_list.
 */
PFsql_t* PFsql_where_list_add(const PFsql_t *list,
        const PFsql_t *item);

/**
 * A sequence of columns.
 */
#define PFsql_column_list(...) \
   PFsql_column_list_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
                  sizeof(PFsql_t*), (const PFsql_t *[]) \
                  {__VA_ARGS__})
PFsql_t* PFsql_column_list_(unsigned int count, const PFsql_t **clmn);

/*..............Schema Information ............*/

#define PFsql_schema_information(...) \
   PFsql_schema_information_(sizeof((PFsql_t *[]) {__VA_ARGS__}) / \
                  sizeof(PFsql_t*), (const PFsql_t *[]) \
                  {__VA_ARGS__})
PFsql_t* PFsql_schema_information_(unsigned int count, const PFsql_t **list);

PFsql_t* PFsql_schema_expression(PFsql_t *schm, PFsql_t *clmn);

PFsql_t* PFsql_schema_document(void);

PFsql_t* PFsql_schema_result(void);

PFsql_t* PFsql_schema_comment(char *cmmnt);

/*................ Join ................*/

PFsql_t* PFsql_on(PFsql_t *join, PFsql_t *expr);

PFsql_t* PFsql_inner_join(PFsql_t *tblref1, PFsql_t *tblref2);

PFsql_t* PFsql_outer_join(PFsql_t *tblref1, PFsql_t *tblref2);

PFsql_t* PFsql_right_outer_join(PFsql_t *tblref1, PFsql_t *tblref2);

/*................ Aritmethic ..............*/

/**
 * Construct an arithmetic addition operator.
 */
PFsql_t* PFsql_add(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct an arithmetic subtraction operator.
 */
PFsql_t* PFsql_sub(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct an arithmetic multiplication operator.
 */
PFsql_t* PFsql_mul(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct an arithmetic division operator.
 */
PFsql_t* PFsql_div(const PFsql_t *a, const PFsql_t *b);

/*.............. Boolean operator constructors ............ */

/**
 * Construct a (comparison) greater-than operator.
 */
PFsql_t* PFsql_gt(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a (comparison) great-than-or-equal operator.
 */
PFsql_t* PFsql_gteq(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a (comparison) equality operator.
 */
PFsql_t* PFsql_eq(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a boolean `not' operator.
 */
PFsql_t* PFsql_not(const PFsql_t *a);

/**
 * Construct a boolean `and' operator.
 */
PFsql_t* PFsql_and(const PFsql_t *a, const PFsql_t *b);

/**
 * Construct a boolean `or' operator.
 */
PFsql_t* PFsql_or(const PFsql_t *a, const PFsql_t *b);

#endif /* __SQL_H__ */

/* vim:set shiftwidth=4 expandtab: */
