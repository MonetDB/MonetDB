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
#define ATT_BITS        5
#define TYPE_BITS       4
#define SPEC_BITS       4
#define ASCII_A         97

/* special bits for column names */
#define sql_col_pre     0x00000002
#define sql_col_level   0x00000004
#define sql_col_size    0x00000008
#define sql_col_kind    0x00000010
#define sql_col_prop    0x00000020
#define sql_col_tag     0x00000040
#define sql_col_nameid  0x00000080
#define sql_col_value   0x00000100
#define sql_col_name    0x00000200
#define sql_col_apre    0x00000400
typedef unsigned int PFsql_special_t;

/* Reserve identifiers. */
#define PF_SQL_TABLE_SYSDUMMY1          1
#define PF_SQL_TABLE_FRAG               2
#define PF_SQL_TABLE_RESULT             3
#define PF_SQL_TABLE_DOC                4
#define PF_SQL_TABLE_TIME_PRINT         23
#define PF_SQL_RES_TABLE_COUNT (PF_SQL_TABLE_TIME_PRINT + 1)

#define PF_SQL_CORRELATION_UNBOUNDED    0
#define PF_SQL_CORRELATION_TIME_PRINT   1
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
enum PFsql_kind_t
{
  sql_tmp_tbl = 1,
  sql_comment = 2,
  sql_cmmn_tbl_expr = 3,
  sql_tbl_name = 4,
  sql_declare = 5,
  sql_bind = 6,
  sql_attbind = 7,
  sql_list_terminator = 8,
  sql_bln_expr = 9,
  sql_expr = 10,
  sql_frm_list = 11,
  sql_lit_int = 12,
  sql_lit_lng = 13,
  sql_lit_str = 14,
  sql_lit_bln = 15,
  sql_add = 16,
  sql_sub = 17,
  sql_mul = 18,
  sql_div = 19,
  sql_gt = 20,
  sql_eq = 21,
  sql_not = 22,
  sql_and = 23,
  sql_or = 24,
  sql_with = 25,
  sql_union = 26,
  sql_select = 27,
  sql_slct_list = 28,
  sql_clmn_assign = 29,
  sql_clmn_name = 30,
  sql_over = 31,
  sql_rownumber = 32,
  sql_ordr_by = 33,
  sql_table_ref = 34,
  sql_dot = 35,
  sql_alias = 36,
  sql_crrltn_name = 37,
  sql_clmn_list = 38,
  sql_lit_null = 39,
  sql_whr_list = 40,
  sql_star = 41,
  sql_tb_name = 42,
  sql_schm = 43,
  sql_cst = 44,
  sql_type = 45,
  sql_prttn = 46,
  sql_prt_expr = 47,
  sql_srtky_expr = 48,
  sql_wnd_clause = 49,
  sql_diff = 50,
  sql_gteq = 51,
  sql_count = 52,
  sql_schm_inf = 53,
  sql_schm_doc = 54,
  sql_schm_res = 55,
  sql_schm_cmmnt = 56,
  sql_schm_expr = 57,
  sql_seq = 58,
  sql_max = 59,
  sql_on = 60,
  sql_innr_join = 61,
  sql_outr_join = 62,
  sql_rght_outr_join = 62,
  sql_sum = 63,
  sql_lit_dec = 64,
  sql_clsc = 65,
  sql_expr_list = 66,
  sql_order = 67,
  sql_asc = 68,
  sql_desc = 69,
  sql_like = 70,
  sql_min = 71,
  sql_avg = 72,
  sql_case = 73,
  sql_when = 74,
  sql_else = 75
};
typedef enum PFsql_kind_t PFsql_kind_t;

/**
 * Semantic content in SQL operators.
 */
union PFsql_sem_t
{
  struct
  {
    PFalg_simple_type_t t;			/**< semantic information for type */
  } type;
  struct
  {
    PFsql_ident_t ident;			/**< id of the table name */
  } tablename;
  struct
  {
    char *str;					/**< schema name */
  } schema;
  struct
  {
    PFsql_ident_t ident;			/**< id of the correlation */
  } correlation;
  struct
  {
    PFsql_correlation_name_t ident;		/**< id of the column */
  } column;
  struct
  {
    bool distinct;				/**< elimination of equal tuples */
  } select;
  struct
  {
    bool distinct;				/**< count only distinct tuples */
  } count;
  char *comment;				/**< String containing comment. */

  struct
  {
    bool null;
    union
    {
      int i;					/**< Integer containing
                                                     literal. */
      long long int l;				/**< Long integer containing
                                                     literal. */
      char *s;					/**< String containing
                                                     literal string. */
      bool b;					/**< Boolean containing
                                                     literal boolean. */
      double dec;
    } val;
  } atom;
  char *table;					/**< string identifying
                                                     the table */
};
typedef union PFsql_sem_t PFsql_sem_t;

/** Each node has at most for childs */
#define PFSQL_OP_MAXCHILD  4

/**
 * SQL operator node.
 * Represents a single SQL tree node in the
 * SQL tree.
 */
struct PFsql_t
{
  PFsql_kind_t kind;				/**< Operator kind. */
  PFsql_sem_t sem;				/**< Semantic content for
                                                    this operator. */
  PFsql_correlation_name_t crrlname;		/**< Each expression is bound to a
                                                    table identified by its
                                                    correlation name */
  bool frag;					/**< mark to identify this node as
                                                     as a reference to the persistent
                                                     document. */
  struct PFsql_t *child[PFSQL_OP_MAXCHILD];	/**< Childs of this
                                                            operator. */
};
typedef struct PFsql_t PFsql_t;

/**
 * Annotations to logical algebra tree nodes. Used during
 * SQL code generation.
 */
struct PFsql_alg_ann_t
{
  PFsql_t *tabname;		    /**< Table name that this subexpression
                                         has been bound to. NULL otherwise. */
  PFsql_t *sfw;			    /**< SQL code that implements this
                                         subexpression. Should always be an
                                         SFW clause. */
  PFsql_t *fragment;		    /**< table containing a fragment */
  PFarray_t *colmap;		    /**< Mapping table that maps (logical)
                                         attribute/type  pairs to their
                                         SQL expression or in case of
                                         a binding to their column names. */
  PFarray_t *wheremap;		    /**< contains references to the boolean
                                         in colmap */
};
typedef struct PFsql_alg_ann_t PFsql_alg_ann_t;

/*.................... General ...................*/

/**
 * Decorate an SQL expression with a correlationname.
 * Each Operator can be annotated with a correlation name
 * to collate it to a relation.
 *
 * @param  op   SQL operator or expression.
 * @param  crrl The identifier of the correlationname.
 * @return	   A SQL node annotaded with a correlation
 *			   name.
 */
PFsql_t *PFsql_correlation_decorator (PFsql_t * op,
				      PFsql_correlation_name_t crrl);

/**
 * Mark a relation as fragment. This mark is used when
 * we bundle the Pathstep-Operators to identify one
 * relation as the table that contains the persistent
 * XML-document.
 *
 * @param  op	SQL table name.
 * @param  frag Boolean value to mark a table either as
 *				fragment or not.
 * @return 		A table name, with mark @a frag.
 */
PFsql_t *PFsql_table_decorator (PFsql_t * op, bool frag);

/*.................... Wiring Functions .....................*/

/**
 * Construct a SQL tree with a given node kind and one child.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n  Child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first child set to @a n, and all remaining
 *             children set to @c NULL.
 * @note
 *   Allocates memory and initialize fields of
 *   an sql operator. The node has the kind of @a kind.
 */
PFsql_t *PFsql_op_leaf (PFsql_kind_t kind);

/*.................... Constructors ........................*/

/**
 * A sequence of select_list-expressions.
 */
#define PFsql_select_list(...) \
    PFsql_select_list_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
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
PFsql_t *PFsql_select_list_ (unsigned int count, const PFsql_t ** list);

/**
 * Create an empty select_list.
 */
PFsql_t *PFsql_select_list_empty (void);

/**
 * Adds an item to the front of an existing
 * select_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t *PFsql_select_list_add (const PFsql_t * list, const PFsql_t * item);

/**
 * A sequence of from_list-expressions.
 */
#define PFsql_from_list(...) \
    PFsql_from_list_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
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
PFsql_t *PFsql_from_list_ (unsigned int count, const PFsql_t ** list);

/**
 * Create an empty from_list.
 */
PFsql_t *PFsql_from_list_empty (void);

/**
 * Adds an item to the front of an existing
 * from_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t *PFsql_from_list_add (const PFsql_t * list, const PFsql_t * item);

/**
 * A sequence of where_list-expressions.
 */
#define PFsql_where_list(...) \
    PFsql_where_list_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
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
 * @param   count Number of expressions in the array that follows.
 * @param   list  Array of exactly @a count expression nodes.
 * @return        A chain of expression nodes.
 */
PFsql_t *PFsql_where_list_ (unsigned int count, const PFsql_t ** list);

/**
 * Create an empty where_list.
 */
PFsql_t *PFsql_where_list_empty (void);

/**
 * Adds an item to the front of an existing
 * where_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t *PFsql_where_list_add (const PFsql_t * list, const PFsql_t * item);

/**
 * A sequence of columns.
 */
#define PFsql_column_list(...) \
    PFsql_column_list_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
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
PFsql_t *PFsql_column_list_ (unsigned int count, const PFsql_t ** clmn);

/**
 * Create a SQL tree node representing a list terminator.
 */
PFsql_t *PFsql_list_terminator (void);

/**
 * Construct a SQL tree node representing a SQL
 * `case' statement.
 */
#define PFsql_case(...) \
    PFsql_case_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
/**
 * Construct a SQL tree node representing a SQL
 * `case' statement.
 *
 * @note
 *   You normally should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_case (or its mnemonic variant
 *   #case instead). It will automaticall calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 */
PFsql_t *PFsql_case_ (unsigned int count, const PFsql_t ** list);

/**
 * Construct a SQL tree node representing a
 * branch within a case-statement.
 */
PFsql_t *PFsql_when (PFsql_t * boolexpr, PFsql_t * expr);

/**
 * Construct a SQL tree node representing an
 * else-branch within a case-statement.
 */
PFsql_t *PFsql_else (PFsql_t * expr);

/**
 * Create a SQL tree node representing a SQL
 * `common table expression'.
 */
#define PFsql_common_table_expr(...) \
    PFsql_common_table_expr_ (sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof (PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
/**
 * Create a SQL tree node representing a SQL
 * `common table expression'.
 *
 * @note
 *   You normally should not need to invoke this function directly.
 *   Use the wrapper macro #PFsql_common_table_expr (or its mnemonic variant
 *   #common_table_expr instead). It will automaticall calculate @a count
 *   for you, so you will only have to pass a list of arguments to
 *   that (variable argument list) macro.
 */
PFsql_t *PFsql_common_table_expr_ (int count, const PFsql_t ** stmts);

/**
 * Create a SQL tree node representing the SQL
 * wildcard `*'
 */
PFsql_t *PFsql_star (void);

/**
 * Create a SQL tree node representing the SQL
 * `select' operator.
 *
 * @note
 *   Represents the SELECT ALL statement in SQL.
 */
PFsql_t *PFsql_select (const PFsql_t * selectlist, const PFsql_t * fromlist,
		       const PFsql_t * wherelist, const PFsql_t * grpbylist);

/**
 * Create a SQL tree node representing the SQL
 * `select' operator. The purpose of this
 * operator is to remove all equal tuples.
 */
PFsql_t *PFsql_select_distinct (const PFsql_t * selectlist,
				const PFsql_t * fromlist,
				const PFsql_t * wherelist,
				PFsql_t * grpbylist);

/**
 * Construct a SQL `union' operator.
 */
PFsql_t *PFsql_union (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a SQL `difference' operator.
 */
PFsql_t *PFsql_difference (const PFsql_t * a, const PFsql_t * b);

/**
 * Create a SQL tree node representing the SQL
 * `order by' clause.
 */
PFsql_t *PFsql_order_by (const PFsql_t * a);

/**
 * Used to order some attribute in an
 * ascending manner.
 */
PFsql_t *PFsql_asc (void);

/**
 * Used to order some attribute in a
 * descending manner.
 */
PFsql_t *PFsql_desc (void);

PFsql_t *PFsql_order (PFsql_t * a, PFsql_t * sort);


/**
 * Create a SQL tree node representing the SQL
 * `with' operator.
 */
PFsql_t *PFsql_with (const PFsql_t * a);

/**
 * Create a SQL tree node representing a comment.
 *
 * @param   fmt   Format string.
 * @return  	  A comment formatted comment
 *				  string.
 */
PFsql_t *PFsql_comment (const char *fmt, ...);

/**
 * Construct a SQL type. SQL supports several types
 * like integer or decimals. This is probably most
 * needed when you cast one attribut to another type.
 *
 * @param   t  The type.
 */
PFsql_t *PFsql_type (PFalg_simple_type_t t);

/**
 * Construct a SQL tree representing a SQL
 * `CAST' operator.
 *
 * @param   expr  Expression.
 * @param   t     Type.
 */
PFsql_t *PFsql_cast (const PFsql_t * expr, const PFsql_t * t);


/**
 * This is used conflate the schema information with the
 * final query.
 */
PFsql_t *PFsql_seq (const PFsql_t * schm_inf, const PFsql_t * cmtblexpr);

/**
 * Create a tree node representing the SQL
 * `coalesce' function.
 */
PFsql_t *PFsql_coalesce (PFsql_t * expr1, PFsql_t * expr2);

/**
 * Create a tree node representing the SQL
 * `like' statement to compare a string
 * with a certain pattern.
 */
PFsql_t *PFsql_like (const PFsql_t * a, const PFsql_t * b);

/*..............OLAP Functionality .............*/

/**
 * Create a SQL tree node representing SQL
 * `rownumber()' function.
 */
PFsql_t *PFsql_rownumber (void);

/**
 * Create a SQL tree node representing the SQL
 * `over' keyword.
 */
PFsql_t *PFsql_over (const PFsql_t * a, const PFsql_t * b);

/**
 * We mainly use this to express the partition keyword
 * used by some OLAP-functions.
 */
PFsql_t *PFsql_partition (const PFsql_t * a);

/**
 * Construct a list of partition-expressions used by some
 * OLAP-functions.
 */
#define PFsql_part_expressions(...) \
    PFsql_part_expressions_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
/**
 * Construct a list of partition-expressions used by some
 * OLAP-functions.
 * 
 * @note
 *   Normally you should not use this function directly, you should
 *   use #PFsql_part_expression (or its mnemonic variant #part_expression)
 *   instead - so you have only have to pass a list of expressions,
 *   and you don't have to calculate count yourself.
 */
PFsql_t *PFsql_part_expressions_ (unsigned int count, const PFsql_t ** list);


/**
 * A sequence of sortkey-expressions.
 */
#define PFsql_sortkey_expressions(...) \
    PFsql_sortkey_expressions_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
/**
 * The sortkey expressions are used in OLAP-functions
 * to provide an ordering among the tuples in a relation.
 *
 * @note
 *   Normally you should not use this function directly, you should
 *   use #PFsql_sortkey_expressions (or its mnemonic variant #sortkey_expressions)
 *   instead - so you have only have to pass a list of expressions,
 *   and you don't have to calculate count yourself.
 */
PFsql_t *PFsql_sortkey_expressions_ (unsigned int count,
				     const PFsql_t ** list);

PFsql_t *PFsql_sortkey_expressions_empty (void);


PFsql_t *PFsql_sortkey_expressions_add (const PFsql_t * list,
					const PFsql_t * item);

/**
 * The whole clause, consisting of sortkey- and
 * partition expressions is called window_clause.
 */
PFsql_t *PFsql_window_clause (const PFsql_t * partcls,
			      const PFsql_t * ordercls);

/*............... Aggregat Function .................*/

/**
 * Construct a SQL tree node representing
 * the aggregat function `count'.
 */
PFsql_t *PFsql_count (bool dist, const PFsql_t * expr);

/**
 * Construct a SQL tree node representing
 * the aggregat function `max'.
 */
PFsql_t *PFsql_max (const PFsql_t * clmn);

/**
 * Construct a SQL tree node representing
 * the aggregat function `min'.
 */
PFsql_t *PFsql_min (const PFsql_t * clmn);

/**
 * Construct a SQL tree node representing
 * the aggregat function `avg'.
 */
PFsql_t *PFsql_avg (const PFsql_t * clmn);

/**
 * Construct a SQL tree node representing
 * the aggregat function `sum'.
 */
PFsql_t *PFsql_sum (const PFsql_t * clmn);

/*................. Tables ..................*/

/**
 * Macro to reference the IBM DB2 sysdummy1 table
 */
#define PFsql_sysdummy1()     PFsql_table_name(PF_SQL_TABLE_SYSDUMMY1, NULL)

/**
 * Macro to reference the persistent document tables.
 */
#define PFsql_fragrelation()  PFsql_table_name(PF_SQL_TABLE_FRAG, NULL)

/**
 * Macro to reference the result table.
 */
#define PFsql_result(clmnlist)  PFsql_table_name(PF_SQL_TABLE_RESULT, clmnlist)

/**
 * Macro to reference the document table.
 */
#define PFsql_document(clmnlist) PFsql_table_name(PF_SQL_TABLE_DOC, clmnlist)

/**
 * Macro to construct a table without a columnlist.
 */
#define PFsql_table_name_default(name)    PFsql_table_name(name, NULL)

/**
 * Construct a SQL tree node representing a
 * reference to a relation.
 */
PFsql_t *PFsql_table_name (PFsql_ident_t name, PFsql_t * clmnlist);

/**
 * In some databases (for instance db2) each
 * table is laying in a schema. We use this
 * constructor to create such schema.
 */
PFsql_t *PFsql_schema (const char *name);

/**
 * Bind a `common table expression' to a
 * name, so it can be referenced within
 * the whole statement.
 */
PFsql_t *PFsql_bind (PFsql_t * a, PFsql_t * b);

/**
 * Collate a table_name and a schema, to identifying/reference
 * to a table in a database.
 */
PFsql_t *PFsql_tab_name (const PFsql_t * schema, const PFsql_t * table_name);

/**
 * Bind a table to a correlation name.
 */
PFsql_t *PFsql_alias (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a SQL tree node representing a SQL
 * `correlation name'.
 */
PFsql_t *PFsql_correlation_name (PFsql_correlation_name_t name);

/*................. Columns ...................*/

/**
 * Create an SQL tree node representing a SQL
 * column name.
 * We use an identifier to express our table
 * name. During the generation phase, transform
 * this identifier to a string.
 */
PFsql_t *PFsql_column_name (PFsql_ident_t ident);


/**
 * Create an SQL tree node representing the SQL
 * `AS' to bind SQL statements to attribute names.
 */
PFsql_t *PFsql_column_assign (const PFsql_t * a, const PFsql_t * b);

/*................. String conversion ....................*/

/**
 * Convert the @a name to a string.
 *
 * @param name The identifier to convert.
 */
char *PFsql_table_str (PFsql_ident_t name);

/**
 * Convert the @a ident to a string.
 */
char *PFsql_column_name_str (PFsql_ident_t ident);

/**
 * Convert the @a type to a string.
 */
char *PFsql_simple_type_str (PFalg_simple_type_t type);

/**
 * Convert the @a name to a string.
 */
char *PFsql_correlation_name_str (PFsql_correlation_name_t name);

/*.................. Literal construction .................... */

/**
 * Construct a NULL.
 */
PFsql_t *PFsql_null (void);

/**
 * Construct a literal integer.
 */
PFsql_t *PFsql_lit_int (int i);

/**
 * Construct a literal 64bit integer.
 */
PFsql_t *PFsql_lit_lng (long long int lng);

/**
 * Construct a literal string.
 */
PFsql_t *PFsql_lit_str (const char *s);

/**
 * Construct a literal boolean value.
 */
PFsql_t *PFsql_lit_bln (bool b);

/**
 * Construct a literal decimal value.
 */
PFsql_t *PFsql_lit_dec (float dec);

/*..............Schema Information ............*/

/**
 * A sequence of schema informations, used by the serializer.
 */
#define PFsql_schema_information(...) \
    PFsql_schema_information_(sizeof((PFsql_t *[]) {__VA_ARGS__} \
    ) / \
    sizeof(PFsql_t*), (const PFsql_t *[]) \
    {__VA_ARGS__} \
    )
/**
 * A sequence of schema informations, used by the serializer.
 *
 * @note
 *   Normally you should not use this function directly, you should
 *   use #PFsql_sortkey_expressions (or its mnemonic variant #sortkey_expressions)
 *   instead - so you have only have to pass a list of expressions,
 *   and you don't have to calculate count yourself.
 *   
 */
PFsql_t *PFsql_schema_information_ (unsigned int count,
				    const PFsql_t ** list);

/**
 * Some specific schema information used by the serializer.
 * We communicate to the serializer the document and 
 * result relation.
 * Further we communicate some of the special attributes,
 * the serializer needs. 
 */
PFsql_t *PFsql_schema_expression (PFsql_t * schm, PFsql_t * clmn);

PFsql_t *PFsql_schema_document (void);

PFsql_t *PFsql_schema_result (void);

/**
 * Construct a comment in the schema information.
 */
PFsql_t *PFsql_schema_comment (char *cmmnt);

/*................ Join ................*/

/**
 * Create the part of the join clause
 * where you can define the join predicate.
 */
PFsql_t *PFsql_on (PFsql_t * join, PFsql_t * expr);

/**
 * Join two relations with an `Inner Join'.
 */
PFsql_t *PFsql_inner_join (PFsql_t * tblref1, PFsql_t * tblref2);

/**
 * Join two relations with an `Outer Join'.
 */
PFsql_t *PFsql_outer_join (PFsql_t * tblref1, PFsql_t * tblref2);

/**
 * Join two relations with a `Right Outer Join'.
 */
PFsql_t *PFsql_right_outer_join (PFsql_t * tblref1, PFsql_t * tblref2);

/*................ Aritmethic ..............*/

/**
 * Construct an arithmetic addition operator.
 */
PFsql_t *PFsql_add (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct an arithmetic subtraction operator.
 */
PFsql_t *PFsql_sub (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct an arithmetic multiplication operator.
 */
PFsql_t *PFsql_mul (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct an arithmetic division operator.
 */
PFsql_t *PFsql_div (const PFsql_t * a, const PFsql_t * b);


/*.............. Boolean operator constructors ............ */

/**
 * Construct a (comparison) greater-than operator.
 */
PFsql_t *PFsql_gt (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a (comparison) great-than-or-equal operator.
 */
PFsql_t *PFsql_gteq (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a (comparison) equality operator.
 */
PFsql_t *PFsql_eq (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a boolean `not' operator.
 */
PFsql_t *PFsql_not (const PFsql_t * a);

/**
 * Construct a boolean `and' operator.
 */
PFsql_t *PFsql_and (const PFsql_t * a, const PFsql_t * b);

/**
 * Construct a boolean `or' operator.
 */
PFsql_t *PFsql_or (const PFsql_t * a, const PFsql_t * b);
#endif /* __SQL_H__ */

/* vim:set shiftwidth=4 expandtab: */
