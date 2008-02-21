/**
 * @file
 *
 * SQL tree node constructor and SQL specific printing functions.
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

#include "pathfinder.h"
#include "sql.h"
#include "sql_mnemonic.h"
#include "algebra.h"
#include "mem.h"
#include "oops.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

/* .......... General .......... */

/**
 * Create a SQL leaf node operator
 *
 * @param  kind  Kind of the operator node.
 * @return       A sql operator with kind
 *
 * @note
 *    Allocates memory and initializes fields of a
 *    SQL operator. The node has the kind @a kind.
 */
static PFsql_t *
leaf (PFsql_kind_t kind)
{
    /* node we want to return */
    PFsql_t *ret = (PFsql_t *) PFmalloc (sizeof (PFsql_t));
    
    ret->kind = kind;
    
    /* initialize children */
    for (unsigned int i = 0; i < PFSQL_OP_MAXCHILD; i++)
      ret->child[i] = NULL;
    
    return ret;
}

/**
 * Construct a SQL tree of the given kind and a single child.
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
static PFsql_t *
wire1 (PFsql_kind_t k, const PFsql_t * n)
{
    PFsql_t *ret  = leaf (k);
    ret->child[0] = (PFsql_t *) n;
    return ret;
}

/**
 * Construct a SQL tree of the given node kind and two children.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n1 First child node to attach to the new node.
 * @param   n2 Second child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first children set to @a n1 and @a n2,
 *             and all remaining children set to @c NULL.
 */
static PFsql_t *
wire2 (PFsql_kind_t k, const PFsql_t * n1, const PFsql_t * n2)
{
    PFsql_t *ret  = wire1 (k, n1);
    ret->child[1] = (PFsql_t *) n2;
    return ret;
}

/**
 * Construct a SQL tree of the given node kind and three children.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n1 First child node to attach to the new node.
 * @param   n2 Second child node to attach to the new node.
 * @param   n3 Third child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first three children set to @a n1, @a n2, and
 *             @a n3 and all remaining children set to @c NULL.
 */
static PFsql_t *
wire3 (PFsql_kind_t k,
       const PFsql_t * n1, const PFsql_t * n2, const PFsql_t * n3)
{
    PFsql_t *ret  = wire2 (k, n1, n2);
    ret->child[2] = (PFsql_t *) n3;
    return ret;
}

/**
 * Construct a SQL tree of the given node kind and two children.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n1 First child node to attach to the new node.
 * @param   n2 Second child node to attach to the new node.
 * @param   n3 Third child node to attach to the new node.
 * @param   n4 Fourth child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first four children set to @a n1, @a n2,
 *             @a n3, and @a n4 and all remaining children
 *             set to @c NULL.
 */
static PFsql_t *
wire5 (PFsql_kind_t k,
       const PFsql_t * n1, const PFsql_t * n2,
       const PFsql_t * n3, const PFsql_t * n4,
       const PFsql_t * n5)
{
    PFsql_t *ret  = wire3 (k, n1, n2, n3);
    ret->child[3] = (PFsql_t *) n4;
    ret->child[4] = (PFsql_t *) n5;
    return ret;
}

static PFsql_t *
sql_list (PFsql_kind_t kind, unsigned int count, const PFsql_t **nodes)
{
    PFsql_t *list = nil ();

    /* keep the first item at the start of the list */
    for (int i = count - 1; i >= 0; i--) {
        if (nodes[i])
            list = wire2 (kind, nodes[i], list);
    }
    return list;
}



/* .......... Constructors .......... */

/**
 * The root of the SQL operator tree:
 * it combines the schema information with the query operators.
 */
PFsql_t *
PFsql_root (const PFsql_t * schm_inf, const PFsql_t * query)
{
    return wire2 (sql_root, schm_inf, query);
}



/* .......... Serialization information .......... */

/**
 * An item of of a sequence of schema information,
 * used by the serialization.
 */
PFsql_t *
PFsql_serialization_info_item (const PFsql_t *info, const PFsql_t *list)
{
    return wire2 (sql_ser_info, info, list);
}

/**
 * Some specific schema information used by the serializer.
 * We communicate the special attributes, the serializer needs. 
 */
PFsql_t *
PFsql_serialization_name_mapping (const PFsql_t *column, const PFsql_t *name)
{
    return wire2 (sql_ser_mapping, column, name);
}

PFsql_t *
PFsql_serialization_type (const PFsql_t *type, const PFsql_t *qtype)
{
    return wire2 (sql_ser_type, type, qtype);
}

/**
 * Construct a comment in the serialization information.
 */
PFsql_t *
PFsql_serialization_comment (char *cmmnt)
{
    PFsql_t *ret = leaf (sql_ser_comment);
    ret->sem.comment.str = cmmnt;
    return ret;
}



/* .......... Tables .......... */

/**
 * Construct a SQL tree node representing a definition of a relation.
 */
PFsql_t *
PFsql_table_def (PFsql_tident_t name, PFsql_t *columnlist)
{
    PFsql_t *ret = wire1 (sql_tbl_def, columnlist);
    ret->sem.tbl.name = name;
    return ret;
}

/**
 * Construct a SQL tree node representing a definition of an alias name
 * with columnlist.
 */
PFsql_t * PFsql_alias_def (PFsql_aident_t name, PFsql_t *columnlist)
{
    PFsql_t *ret = wire1 (sql_alias_def, columnlist);
    ret->sem.alias.name = name;
    return ret;
}

/**
 * Collate a schema and a table_name, to identify a table in a database.
 */
PFsql_t *
PFsql_schema_table_name (const char *schema, const PFsql_t *table_name)
{
    PFsql_t *ret = wire1 (sql_schema_tbl_name, table_name);
    ret->sem.schema.str = (char *) schema;
    return ret;
}

/**
 * Construct a SQL tree node representing a reference to a relation.
 */
PFsql_t *
PFsql_table_name (PFsql_tident_t name)
{
    PFsql_t *ret = leaf (sql_tbl_name);
    ret->sem.tbl.name = name;
    return ret;
}

/**
 * Construct a SQL tree node representing a reference to a 
 * column of an external relation. 
 */
PFsql_t *
PFsql_ref_column_name (PFsql_aident_t alias, char* name)
{
    PFsql_t *ret = leaf (sql_ref_column_name);
    ret->sem.column.alias = alias;
    ret->sem.ref_column_name.name = PFstrdup(name);
    return ret;
}

/**
 * Construct a SQL tree node representing a reference to an 
 * external relation. 
 */
PFsql_t *
PFsql_ref_table_name (char* name)
{
    PFsql_t *ret = leaf (sql_ref_tbl_name);
    ret->sem.ref_tbl.name = PFstrdup(name);
    return ret;
}

/**
 * Construct a SQL tree node representing a SQL `correlation name'.
 */
PFsql_t *
PFsql_alias (PFsql_aident_t name)
{
  PFsql_t *ret = leaf (sql_alias);
    ret->sem.alias.name = name;
    return ret;
}

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
PFsql_t *
PFsql_column_list_ (unsigned int count, const PFsql_t **columns)
{
    return sql_list (sql_column_list, count, columns);
}

/**
 * Create an SQL tree node representing a SQL column name.
 */
PFsql_t *
PFsql_column_name (PFsql_aident_t alias, PFsql_col_t *name)
{
    PFsql_t *ret = leaf (sql_column_name);
    ret->sem.column.alias = alias;
    ret->sem.column.name = name;

    return ret;
}

/**
 * Create a SQL tree node representing the SQL wildcard `*'.
 */
PFsql_t *
PFsql_star (void)
{
    return leaf (sql_star);
}



/* .......... Top Level Query Constructs .......... */

/**
 * Create a SQL tree node representing the SQL `WITH' operator.
 */
PFsql_t *
PFsql_with (const PFsql_t *a, const PFsql_t *fs)
{
    return wire2 (sql_with, a, fs);
}

/**
 * Create a SQL tree representing a list of SQL
 * `common table expressions'.
 */
PFsql_t *
PFsql_common_table_expr (const PFsql_t *old, const PFsql_t *new)
{
    return wire2 (sql_cmmn_tbl_expr, old, new); 
}

/**
 * Create a SQL tree node representing a comment.
 *
 * @param   fmt   Format string.
 * @return        A comment formatted comment
 *                                string.
 */
PFsql_t *
PFsql_comment (const char *fmt, ...)
{
    /* create a new comment tree node */
    PFsql_t *ret = leaf (sql_comment);
 
    PFarray_t *a = PFarray (sizeof (char));
 
    /* create the formatted string */
    va_list args;
 
    va_start (args, fmt);
    PFarray_vprintf (a, fmt, args);
    va_end (args);
 
    ret->sem.comment.str = PFarray_at (a, 0);
 
    return ret;
}

/**
 * Bind a `common table expression' to a
 * name, so it can be referenced within
 * the whole statement.
 */
PFsql_t *
PFsql_bind (PFsql_t *table_name, PFsql_t *query)
{
    assert (table_name->kind == sql_tbl_def);
    assert (query);
    return wire2 (sql_bind, table_name, query);
}

/**
 * Create a SQL tree node representing a nil
 * object for all kind of lists.
 */
PFsql_t *
PFsql_nil (void)
{
    return leaf (sql_nil);
}



/* .......... Select .......... */

/**
 * Create a SQL tree node representing the SQL `SELECT' operator.
 *
 * @note
 *   Represents the SELECT ALL statement in SQL.
 */
PFsql_t *
PFsql_select (bool distinct,
              const PFsql_t *selectlist,
              const PFsql_t *fromlist,
              const PFsql_t *wherelist,
              const PFsql_t *orderbylist,
              const PFsql_t *groupbylist)
{
    PFsql_t *ret = wire5 (sql_select,
                          selectlist, fromlist, wherelist,
                          orderbylist, groupbylist);
    ret->sem.select.distinct = distinct;
    return ret;
}

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
PFsql_t *
PFsql_select_list_ (unsigned int count, const PFsql_t ** list)
{
    return sql_list (sql_select_list, count, list);
}

/**
 * Create an SQL tree node representing the SQL
 * `AS' to bind SQL statements to attribute names.
 */
PFsql_t *
PFsql_column_assign (const PFsql_t *expr, const PFsql_t *column_name)
{
    return wire2 (sql_column_assign, expr, column_name);
}

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
PFsql_t *
PFsql_from_list_ (unsigned int count, const PFsql_t **list)
{
    return sql_list (sql_from_list, count, list);
}

/**
 * Adds an item to the front of an existing from_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t *
PFsql_add_from (const PFsql_t *list, const PFsql_t *item)
{
    return wire2 (sql_from_list, item, list);
}

/**
 * Bind a table to a correlation name (alias).
 */
PFsql_t *
PFsql_alias_bind (const PFsql_t *expr, const PFsql_t *alias)
{
    return wire2 (sql_alias_bind, expr, alias);
}

/**
 * Create the part of the join clause
 * where you can define the join predicate.
 */
PFsql_t *
PFsql_on (PFsql_t *join, PFsql_t *expr)
{
    return wire2 (sql_on, join, expr);
}

/**
 * Join two relations with a `Left Outer Join'.
 */
PFsql_t *
PFsql_left_outer_join (PFsql_t *tblref1, PFsql_t *tblref2)
{
    return wire2 (sql_left_outer_join, tblref1, tblref2);
}

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
PFsql_t *
PFsql_where_list_ (unsigned int count, const PFsql_t **list)
{
    return sql_list (sql_and, count, list);
}



/* .......... Set operators .......... */

/**
 * Create a SQL tree node representing the SQL
 * `UNION ALL' operator.
 */
PFsql_t *
PFsql_union (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_union, a, b);
}

/**
 * Create a SQL tree node representing the SQL
 * `EXCEPT ALL' operator.
 */
PFsql_t *
PFsql_difference (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_diff, a, b);
}

/**
 * Create a SQL tree node representing the SQL
 * `INTERSECT ALL' operator.
 */
PFsql_t *
PFsql_intersect (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_intersect, a, b);
}

/* .......... Literal construction .......... */

/**
 * Create a SQL tree node representing a literal integer.
 *
 * @param   i     The integer value to represent in SQL.
 */
PFsql_t *
PFsql_lit_int (int i)
{
    PFsql_t *ret = leaf (sql_lit_int);
    ret->sem.atom.val.i = i;
    return ret;
}

/**
 * Create a SQL tree node representing a literl 64bit integer.
 *
 * @param   l  The integer value to represent in SQL.
 */
PFsql_t *
PFsql_lit_lng (long long int l)
{
    PFsql_t *ret = leaf (sql_lit_lng);
    ret->sem.atom.val.l = l;
    return ret;
}

/**
 * Create a SQL tree node representing a literal string.
 *
 * @param   s  The string value to represent in SQL.
 */
PFsql_t *
PFsql_lit_str (const char *s)
{
    PFsql_t *ret = leaf (sql_lit_str);
    
    /* check if string is defined */
    assert (s);
    
    ret->sem.atom.val.s = (char *) s;
    return ret;
}

/**
 * Create a SQL tree node representing a literal decimal value.
 *
 * @param dec The decimal value to represent in SQL.
 */
PFsql_t *
PFsql_lit_dec (float dec)
{
    PFsql_t *ret = leaf (sql_lit_dec);
    ret->sem.atom.val.dec = dec;
    return ret;
}

/**
 * Create a SQL tree node representing a NULL.
 */
PFsql_t *
PFsql_null (void)
{
    return leaf (sql_lit_null);
}



/* .......... Arithmetic Operators .......... */

/**
 * Create a SQL tree node representing an arithmetic add
 * operator.
 *
 * @param   a  First addend.
 * @param   b  Second addend.
 */
PFsql_t *
PFsql_add (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_add, a, b);
}

/**
 * Create a SQL tree node representing an arithmetic
 * subtract operator.
 *
 * @param   a  The minuend.
 * @param   b  The subtrahend.
 */
PFsql_t *
PFsql_sub (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_sub, a, b);
}


/**
 * Create a SQL tree node representing an arithmetic
 * multiplication operator.
 *
 * @param   a  The multiplicand.
 * @param   b  The multiplier.
 */
PFsql_t *
PFsql_mul (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_mul, a, b);
}


/**
 * Create a SQL tree node representing an arithmetic
 * division operator.
 *
 * @param   a  The divident.
 * @param   b  The quotient.
 */
PFsql_t *
PFsql_div (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_div, a, b);
}

/* ............ Integer Functions ............. */

/**
 * Create a SQL tree node representing the SQL
 * floor operator.
 */
PFsql_t * PFsql_floor (const PFsql_t *a)
{
    return wire1 (sql_floor, a);
}

/**
 * Create a SQL tree node representing the SQL
 * ceil operator.
 */
PFsql_t * PFsql_ceil (const PFsql_t *a)
{
    return wire1 (sql_ceil, a);
}

/**
 * Create a SQL tree node representing the SQL
 * modulo operator.
 */
PFsql_t * PFsql_modulo (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_modulo, a, b);
}

/**
 * Create a SQL tree node representing the SQL
 * abs operator.
 */
PFsql_t * PFsql_abs (const PFsql_t *a)
{
    return wire1 (sql_abs, a);
}

/* .......... String Functions .......... */

/**
 * Create a SQL tree node representing the SQL
 * concat operator.
 */
PFsql_t * PFsql_concat (const PFsql_t *a, const PFsql_t *b)
{
     return wire2 (sql_concat, a, b);
}

/* ......... Table Functions ........... */

PFsql_t *
PFsql_values (const PFsql_t *list)
{
    return wire1 (sql_values, list);
}

PFsql_t *
PFsql_list_list_ (unsigned int count, const PFsql_t **list)
{
    return sql_list (sql_list_list,  count, list);
}


/* .......... Boolean Operators .......... */

/**
 * Create a SQL tree node representing a boolean
 * `IS' operator.
 */
PFsql_t *
PFsql_is (const PFsql_t *a, const PFsql_t *b)
{
    PFsql_t *ret = wire2 (sql_is, a, b);
    return ret; 
}

/**
 * Create a SQL tree node representing a boolean
 * `IS NOT' operator.
 */
PFsql_t *
PFsql_is_not (const PFsql_t *a, const PFsql_t *b)
{
    PFsql_t *ret = wire2 (sql_is_not, a, b);
    return ret; 
}

/**
 * Create a SQL tree node representing a boolean
 * (comparison) equality operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t *
PFsql_eq (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_eq, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * (comparison) greater-than operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t *
PFsql_gt (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_gt, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * (comparison) greater-than-or_equal operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t *
PFsql_gteq (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_gteq, a, b);
}

/*
 * Create a SQL tree node representing a 
 * `between' predicate.
 */
PFsql_t * PFsql_between(const PFsql_t *clmn, const PFsql_t *a, const PFsql_t *b)
{
    assert (clmn);
    assert (a);
    assert (b);
    return wire3 (sql_between, clmn, a, b); 
}

/**
 * Create a tree node representing the SQL
 * `like' statement to compare a string
 * with a certain pattern.
 */
PFsql_t *
PFsql_like (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_like, a, b);
}

/**
 * Create a SQL tree node representing the in operator
 */
PFsql_t *
PFsql_in (const PFsql_t *column, const PFsql_t *list)
{
    return wire2 (sql_in, column, list);
}

PFsql_t *
PFsql_stmt_list_ (unsigned int count, const PFsql_t **list) 
{
    return sql_list (sql_stmt_list, count, list);
}

/**
 * Create a SQL tree node representing a boolean
 * negation operator.
 *
 * @param   a
 */
PFsql_t *
PFsql_not (const PFsql_t * a)
{
    return wire1 (sql_not, a);
}

/**
 * Create a SQL tree node representing a boolean
 * `and' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t *
PFsql_and (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_and, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * `or' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t *
PFsql_or (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_or, a, b);
}



/* .......... Aggregate Functions .......... */

/**
 * Construct a SQL tree node representing
 * the aggregate function `COUNT'.
 */
PFsql_t *
PFsql_count (const PFsql_t *column)
{
    return wire1 (sql_count, column);
}

/**
 * Construct a SQL tree node representing
 * the aggregate function `MAX'.
 */
PFsql_t *
PFsql_max (const PFsql_t *column)
{
    return wire1 (sql_max, column);
}

/**
 * Construct a SQL tree node representing
 * the aggregate function `MIN'.
 */
PFsql_t *
PFsql_min (const PFsql_t *column)
{
    return wire1 (sql_min, column);
}

/**
 * Construct a SQL tree node representing
 * the aggregate function `AVG'.
 */
PFsql_t *
PFsql_avg (const PFsql_t *column)
{
    return wire1 (sql_avg, column);
}

/**
 * Construct a SQL tree node representing
 * the aggregate function `SUM'.
 */
PFsql_t *
PFsql_sum (const PFsql_t * column)
{
    return wire1 (sql_sum, column);
}

/* ........... String Functions ........... */

/**
 * Construct a SQL tree  node representing
 * the SQL length functions for strings
 */
PFsql_t *
PFsql_str_length (const PFsql_t *a)
{
    return wire1 (sql_str_length, a);
}

/**
 * Construct a SQL tree  node representing
 * the SQL length functions for strings
 */
PFsql_t *
PFsql_str_upper (const PFsql_t *a)
{
    return wire1 (sql_str_upper, a);
}

/**
 * Construct a SQL tree  node representing
 * the SQL length functions for strings
 */
PFsql_t *
PFsql_str_lower (const PFsql_t *a)
{
    return wire1 (sql_str_lower, a);
}


/* .......... OLAP Functionality .......... */

/**
 * Create a SQL tree node representing the SQL `OVER' keyword.
 */
PFsql_t *
PFsql_over (const PFsql_t *a, const PFsql_t *b)
{
    return wire2 (sql_over, a, b);
}

/**
 * Create a SQL tree node representing SQL `ROW_NUMBER()' function.
 */
PFsql_t *
PFsql_row_number (void)
{
    return leaf (sql_row_number);
}

/**
 * Create a SQL tree node representing SQL `DENSE_RANK()' function.
 */
PFsql_t *
PFsql_dense_rank (void)
{
    return leaf (sql_dense_rank);
}

/**
 * The whole clause, consisting of sortkey- and
 * partition expressions is called window_clause.
 */
PFsql_t *
PFsql_window_clause (const PFsql_t *part_clause, const PFsql_t *order_clause)
{
    return wire2 (sql_wnd_clause, part_clause, order_clause);
}

/**
 * Create a SQL tree node representing the SQL `ORDER BY' clause.
 */
PFsql_t *
PFsql_order_by (const PFsql_t *sortkey_list)
{
    return wire1 (sql_order_by, sortkey_list);
}

/**
 * The sortkey expressions are used in OLAP-functions
 * to provide an ordering among the tuples in a relation.
 */
PFsql_t *
PFsql_sortkey_list_ (unsigned int count, const PFsql_t **list)
{
    return sql_list (sql_sortkey_list, count, list);
}

/**
 * Sort key item
 */
PFsql_t *
PFsql_sortkey_item (PFsql_t *expr, bool dir)
{
    PFsql_t *ret = wire1 (sql_sortkey_item, expr);
    ret->sem.sortkey.dir_asc = dir;
    return ret;
}

/**
 * We mainly use this to express the partition keyword
 * used by some OLAP-functions.
 */
PFsql_t *
PFsql_partition (const PFsql_t *partition_list)
{
    return wire1 (sql_partition, partition_list);
}



/* .......... Remaining Operators .......... */

/**
 * Construct a SQL type. SQL supports several types
 * like integer or decimals. This is probably most
 * needed when you cast one attribut to another type.
 *
 * @param   t  The type.
 */
PFsql_t *
PFsql_type (PFalg_simple_type_t t)
{
    PFsql_t *ret = leaf (sql_type);
    ret->sem.type.t = t;
    return ret;
}

/**
 * Construct a SQL tree representing a SQL
 * `CAST' operator.
 *
 * @param   expr  Expression.
 * @param   t     Type.
 */
PFsql_t *
PFsql_cast (const PFsql_t *expr, const PFsql_t *t)
{
    return wire2 (sql_cast, expr, t);
}

/**
 * Create a tree node representing the SQL
 * `COALESCE' function.
 */
PFsql_t *
PFsql_coalesce (PFsql_t *expr1, PFsql_t *expr2)
{
    return wire2 (sql_coalesce, expr1, expr2);
}

/**
 * Construct a SQL tree node representing a SQL
 * `case' statement.
 */
PFsql_t *
PFsql_case_ (unsigned int count, const PFsql_t **list)
{
    return sql_list (sql_case, count, list);
}

/**
 * Construct a SQL tree node representing a
 * branch within a case-statement.
 */
PFsql_t *
PFsql_when (PFsql_t *boolexpr, PFsql_t *expr)
{
    return wire2 (sql_when, boolexpr, expr);
}

/**
 * Construct a SQL tree node representing an
 * else-branch within a case-statement.
 */
PFsql_t *
PFsql_else (PFsql_t *expr)
{
    return wire1 (sql_else, expr);
}

/**
 * Create a DB2 selectivity hint.
 */
PFsql_t *
PFsql_selectivity (PFsql_t *pred, PFsql_t *sel)
{
    return wire2 (sql_db2_selectivity, pred, sel);
}

/**
 * Duplicate a given SQL tree.
 */
PFsql_t *
PFsql_op_duplicate (PFsql_t *expr)
{
    if (!expr) return NULL;

    switch (expr->kind) {
        case sql_ref_tbl_name:
            return ref_table_name (PFstrdup (expr->sem.ref_tbl.name)); 

        case sql_ref_column_name:
            return ref_column_name (expr->sem.ref_column_name.alias,
                                    PFstrdup (expr->sem.ref_column_name.name)); 
        case sql_ser_comment:
            return ser_comment (expr->sem.comment.str);
            
        case sql_tbl_def:
            return table_def (expr->sem.tbl.name,
                              duplicate(expr->child[0]));
            
        case sql_schema_tbl_name:
            return schema_table_name (expr->sem.schema.str,
                                      duplicate(expr->child[0]));
            
        case sql_tbl_name:
            return table_name (expr->sem.tbl.name);
            
        case sql_alias:
            return alias (expr->sem.alias.name);
            
        case sql_column_name:
            return ext_column_name (expr->sem.column.alias,
                                    expr->sem.column.name);
            
        case sql_comment:
            return comment (expr->sem.comment.str);
            
        case sql_select:
            return PFsql_select (expr->sem.select.distinct,
                                 duplicate(expr->child[0]),
                                 duplicate(expr->child[1]),
                                 duplicate(expr->child[2]),
                                 duplicate(expr->child[3]),
                                 duplicate(expr->child[4]));
            
        case sql_lit_int:
        case sql_lit_lng:
        case sql_lit_dec:
        case sql_lit_str:
        {
            PFsql_t *ret = leaf (expr->kind);
            ret->sem.atom = expr->sem.atom;
            return ret;
        }
        
        case sql_sortkey_item:
            return sortkey_item (duplicate (expr->child[0]),
                                 expr->sem.sortkey.dir_asc);

        case sql_type:
            return type (expr->sem.type.t);
            
        case sql_between:
            return wire3 (expr->kind,
                          duplicate(expr->child[0]),
                          duplicate(expr->child[1]),
                          duplicate(expr->child[2]));

        default:
            /* translate SQL node constructors that
               have at most two children */
            return wire2 (expr->kind,
                          duplicate(expr->child[0]),
                          duplicate(expr->child[1]));
    }
}



/* .......... Printing Functions .......... */

/**
 * Convert the table name @a name into a string.
 *
 * @param name The identifier to convert.
 */
char *
PFsql_table_str (PFsql_tident_t name)
{
    switch (name) {
        case PF_SQL_TABLE_SYSDUMMY1: return "sysdummy1";
        case PF_SQL_TABLE_FRAG:      return "xmldoc";
        case PF_SQL_TABLE_RESULT:    return "result";
        case PF_SQL_TABLE_DOC:       return "document";
                                     
        default:
        {
            size_t len = sizeof ("t0000");
            char  *res = (char *) PFmalloc (len);
            /* to express table names we use
             * the following format:
             *    t[0-9][0-9][0-9][0-9]
             */
            assert (name >= PF_SQL_RES_TABLE_COUNT);
            name -= PF_SQL_RES_TABLE_COUNT;
            assert (name < 10000);

            snprintf (res, len, "t%04u", name);
            return res;
        }
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Convert the alias name @a name to a string.
 *
 * @param name The identifier to convert.
 */
char *
PFsql_alias_name_str (PFsql_aident_t name)
{
    switch (name) {
        case PF_SQL_ALIAS_UNBOUND:
            PFoops (OOPS_FATAL, "alias name is unbound");

        default:
        {
            size_t len = sizeof ("a0000");
            char  *res = (char *) PFmalloc (len);
            /* to express alias names we use
             * the following format:
             *    a[0-9][0-9][0-9][0-9]
             */
            assert (name >= PF_SQL_RES_ALIAS_COUNT);
            name -= PF_SQL_RES_ALIAS_COUNT;
            assert (name < 10000);

            snprintf (res, len, "a%04u", name);
            return res;
        }
    }
    return NULL; /* satisfy picky compilers */
}

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
char *
PFsql_column_name_str (PFsql_col_t *name)
{
    char  *res    = NULL;
 
    if (name->id == PF_SQL_COLUMN_SPECIAL)
        switch (name->spec) {
            case sql_col_pre:        return "pre";
            case sql_col_level:      return "level";
            case sql_col_size:       return "size";
            case sql_col_kind:       return "kind";
            case sql_col_prop:       return "prop";
            case sql_col_nameid:     return "nameid";
            case sql_col_value:      return "value";
            case sql_col_name:       return "name";
            case sql_col_ns_uri:     return "uri";  
            case sql_col_twig_pre:   return "twig_pre";
            case sql_col_iter:       return "iter";
            case sql_col_pos:        return "pos";
            case sql_col_guide:      return "guide";
            case sql_col_max:        return "max";
            case sql_col_dist:
                assert (name->ty < 100);
                res = (char *) PFmalloc (7 * sizeof (char));
                snprintf (res, 7, "dist%02u", name->ty);
                return res;
        }
    else {
        char  *attstr = PFatt_str (name->att);
        char  *tystr  = PFalg_simple_type_str (name->ty);
        size_t len    = strlen (attstr) + strlen (tystr) + 2;
     
        res = (char *) PFmalloc (len * sizeof (char));
        snprintf (res, len, "%s_%s", attstr, tystr);
    }
    return res;
}

/**
 * Convert the @a type to a string.
 *
 * @param type The type to convert.
 */
char *
PFsql_simple_type_str (PFalg_simple_type_t type)
{
    switch (type) {
        case aat_pre:
        case aat_nat:
        case aat_int:
        /* we even translate boolean as INTEGER */
        case aat_bln:
            return "INTEGER";
        case aat_uA:
        case aat_str:
            return "VARCHAR(100)";
        case aat_charseq:
            return "CHAR(100)";
        case aat_dbl:
        case aat_dec:
            return "DECIMAL(20,10)";
        case aat_qname_loc:
        case aat_qname_uri:
            return "VARCHAR(100)";
        default:
            PFoops (OOPS_FATAL, "unknown type '0x%X'", type);
    }
    return NULL; /* satisfy picky compiler */
}

/* vim:set shiftwidth=4 expandtab: */
