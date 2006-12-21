/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Functions related to sql tree construction.
 */

/*
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
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
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

/*............... Constructors ..............*/ 

/**
 * Construct a SQL tree with given node kind and one child.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n  Child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first child set to @a n, and all remaining
 *             children set to @c NULL.
 */
static PFsql_t* 
wire1(PFsql_kind_t k, const PFsql_t *n)
{
    PFsql_t *ret  =  PFsql_op_leaf( k );

    ret->child[0] = (PFsql_t*)n;
    
    return ret;
}

/**
 * Construct a SQL tree node with given node kind and two children.
 *
 * @param   k  Kind of the newly constructed node.
 * @param   n1 First child node to attach to the new node.
 * @param   n2 Second child node to attach to the new node.
 * @return     A SQL tree node type, with kind set to @a k,
 *             the first children set to @a n1 and @a n2,
 *             and all remaining children set to @c NULL.
 */
static PFsql_t*
wire2(PFsql_kind_t k, const PFsql_t *n1, const PFsql_t *n2)
{
    PFsql_t *ret  =  wire1( k, n1);

    ret->child[1] = (PFsql_t*)n2;
    
    return ret;
}

/**
 * Create an sql operator (leaf) node.
 *
 * @param   kind  Kind of the operator node.
 * @return        A sql operator with kind
 *                set to @a kind.
 *
 * @note
 *    Allocates memory and initializes fields of an 
 *    sql operator. The node has the kind @a kind.
 */
PFsql_t*
PFsql_op_leaf (PFsql_kind_t kind)
{
    /* node we want to return */
    PFsql_t *ret  =  (PFsql_t*)PFmalloc(
            sizeof( PFsql_t ) );
    ret->kind        =  kind;
   
    /* initialize childs */
    for( unsigned int i = 0; i < PFSQL_OP_MAXCHILD; i++)
        ret->child[i] = NULL; 

    return ret;
}

PFsql_t*
PFsql_common_table_expr_ (int count, const PFsql_t **stmts)
{
    assert( count > 0 );

    if( count == 1)
        return (PFsql_t*) stmts[0];
    else
        return wire2(sql_cmmn_tbl_expr, stmts[0],
                PFsql_common_table_expr_ (count - 1, stmts + 1));
    return NULL; /* satisfy picky compilers */
}

PFsql_t*
PFsql_star(void)
{
   return leaf(sql_star);
}

/**
 * Create a SQL tree node representing the SQL
 * `select' operator.
 *
 * @param   selectlist  Attribute list.
 * @param   fromlist    From list.
 */
PFsql_t*
PFsql_select(const PFsql_t *selectlist, const PFsql_t *fromlist,
        const PFsql_t *wherelist)
{
    PFsql_t *ret = leaf(sql_select);

    /* distinct flag is false */
    ret->sem.select.distinct = false;
    /* initialize select lists  */
    ret->sem.select.select_list  = (PFsql_t*)selectlist;
    ret->sem.select.from_list    = (PFsql_t*)fromlist;
    ret->sem.select.where_list   = (PFsql_t*)wherelist;
    ret->sem.select.grpby_list   = NULL;

    return ret;
}

/**
 * Create a SQL tree node representing the SQL
 * `select' operator. There are no equal tuples in the
 * relation.
 *
 * @param   selectlist  Attribute list.
 * @param   fromlist    From list.
 */
PFsql_t*
PFsql_select_distinct(const PFsql_t *selectlist,
        const PFsql_t *fromlist, const PFsql_t *wherelist)
{
    PFsql_t *ret = select( selectlist, fromlist, wherelist );
    ret->sem.select.distinct = true;
    return ret;
}

/**
 * Create a SQL tree node representing the SQL
 * `union' operator.
 */
PFsql_t*
PFsql_union(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_union, a, b);
}

/*........... Common Operators ..........*/

/**
 * Create a SQL tree node representing the SQL
 * `with' operator.
 */
PFsql_t*
PFsql_with(const PFsql_t *a, const PFsql_t *b)
{
    return wire2( sql_with, a, b );
}

/**
 * Create a SQL tree node representing a comment.
 *
 * @param   fmt   Format string.
 */
PFsql_t*
PFsql_comment(const char *fmt, ...)
{
    PFsql_t *ret = leaf(sql_comment);
    PFarray_t *a = PFarray(sizeof(char));

    va_list args;

    va_start(args, fmt);
    PFarray_vprintf(a, fmt, args);
    va_end(args);

    ret->sem.comment = PFarray_at(a, 0);

    return ret;
}

/**
 * Construct a SQL type.
 *
 * @param   t  The type. 
 */
PFsql_t*
PFsql_type(PFalg_simple_type_t t)
{
    PFsql_t *ret = leaf(sql_type);
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
PFsql_t*
PFsql_cast(const PFsql_t *expr, const PFsql_t *t)
{
    return wire2(sql_cst, expr, t);
}

/*............ Aggregat Functions ...........*/

/**
 * Create a SQL tree node representing SQL
 * `rownumber()' aggregat function.
 */
PFsql_t*
PFsql_rownumber(void)
{
    return leaf(sql_rownumber);
}

/**
 * Create a SQL tree node representing a SQL
 * `over' operator.
 *
 * @param   a  Right statement of over operator.
 * @param   b  Left statement of over operator.
 */
PFsql_t*
PFsql_over(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_over, a, b);
}

PFsql_t*
PFsql_partition(const PFsql_t *a)
{
    return wire1(sql_prttn, a);
}

PFsql_t*
PFsql_part_expressions_(unsigned int count, const PFsql_t **list)
{
    assert( count > 0 );

    if( count == 1 )
    {
        return wire2(sql_prt_expr, list[0], terminator());
    }
    else
    {
        return wire2(sql_prt_expr, list[0],
                PFsql_part_expressions_(count - 1, list + 1));
    }
    return NULL;
}

PFsql_t*
PFsql_part_expressions_empty(void)
{
    return leaf(sql_list_terminator);
}

PFsql_t*
PFsql_part_expression_add(const PFsql_t *list, const PFsql_t *item)
{
    return wire2(sql_prt_expr, item, list);
}

PFsql_t*
PFsql_order_by(const PFsql_t *a)
{
    return wire1(sql_ordr_by, a);
}

PFsql_t*
PFsql_sortkey_expressions_empty()
{
    return terminator();
}

PFsql_t*
PFsql_sortkey_expressions_add(const PFsql_t *list, const PFsql_t *item)
{
    return wire2(sql_srtky_expr, item, list);
}

PFsql_t*
PFsql_window_clause(const PFsql_t *partcls, const PFsql_t *ordercls)
{
    return wire2(sql_wnd_clause, partcls, ordercls);
}

/*............ Tables ...........*/

/**
 * Construct a SQL tree node representing a 
 * reference to a relation.
 */
PFsql_t*
PFsql_table_name(PFsql_ident_t name, PFsql_t *clmnlist)
{
    PFsql_t *ret = leaf(sql_tbl_name);
    ret->sem.tablename.ident = name;
    ret->sem.tablename.clmn_list = clmnlist;
    return ret;
}

PFsql_t*
PFsql_schema(const char *schm)
{
    PFsql_t *ret = leaf(sql_schm);
    ret->sem.schema.str = (char*)schm;
    return ret;
}

PFsql_t*
PFsql_bind(PFsql_t *a, PFsql_t *b)
{
    assert( a );
    assert( b );
    assert( a->kind == sql_tbl_name );
    return wire2( sql_bind, a, b );
}

PFsql_t*
PFsql_tab_name(const PFsql_t *schema, const PFsql_t *table_name)
{
    return wire2(sql_tb_name, schema, table_name);
}

PFsql_t*
PFsql_alias(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_alias, a, b);
}

PFsql_t*
PFsql_correlation_name(PFsql_ident_t name)
{
    PFsql_t *ret = leaf(sql_crrltn_name);
    ret->sem.correlation.ident = name;
    return ret;
}

/*............ Columns .............*/

/**
 * Create an SQL tree node representing a SQL
 * column name.
 *
 * @param   att   The name of the attribute.
 * @param   ty    The type of the attribute.
 */
PFsql_t*
PFsql_column_name(PFsql_ident_t ident)
{
    PFsql_t *ret;
    ret = leaf(sql_clmn_name);
    ret->sem.column.ident = ident;

    return ret;
}

/**
 * Create an SQL tree node representing the SQL
 * `AS' to bind SQL statements to attribute names.
 *
 * @param   a  SQL statement or expression.
 * @param   b  Attribute reference.
 */
PFsql_t*
PFsql_column_assign(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_clmn_assign, a, b);
}

/*............ String conversion ..........*/

/**
 * Convert the @a name to a string.
 *
 * @param name The identifier to convert.
 */
char*
PFsql_table_str(PFsql_ident_t name)
{
    switch( name ) {
        case PF_SQL_TABLE_SYSDUMMY1:
        {
            return "sysdummy1";
        } break;
        case PF_SQL_TABLE_FRAG:
        {
            return "test";
        } break;
        default:
        {
            assert( name >= PF_SQL_RES_TABLE_COUNT);
            name -= PF_SQL_RES_TABLE_COUNT;
            assert( name < 10000 );
            size_t len = sizeof("a0000");
            char *res = (char*)PFmalloc( len );
            snprintf( res, len, "a%04u", name);
            return res;
        }
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Convert the @a ident to a string.
 *
 * @param ident The identifier to convert.
 */
char*
PFsql_column_name_str(PFsql_ident_t ident)
{
    char *attstr = NULL;
    char *tystr = NULL;
    char *res = NULL;
    size_t len = 0;

    PFalg_att_t att = ((0x0000001F & ident) <= 0)? 0x00000000:
        (0x00000001 << (((0x0000001F) & ident)-1));
    PFalg_simple_type_t ty = (0x00000001 << (((0x000001E0) & ident) >>
                ATT_BITS));

    attstr = PFatt_str(att);
    tystr  = PFsimple_type_str(ty);

    len = strlen(attstr);
    len += strlen(tystr);

    res = (char*)PFmalloc(len * sizeof(char));
    snprintf(res, len+2, "%s_%s", attstr, tystr);

    return res;
}

/**
 * Convert the @a type to a string.
 *
 * @param type The type to convert.
 */
char*
PFsql_simple_type_str(PFalg_simple_type_t type)
{
    switch( type ) {
        case aat_nat:
        case aat_int:   return "INTEGER";
        case aat_str:    return "VARCHAR(100)";
        default:              return "unknown";
    }
    return "unknown";
}

/**
 * Convert the @a name to a string.
 *
 * @param name The identifier to convert.
 */
char*
PFsql_loc_var_str(PFsql_ident_t name)
{
   switch( name ) {
       default:
       {
           assert( name <= 26 );
           char *res = (char*)PFmalloc( 2 * sizeof( char ) );
           snprintf(res, 2, "%c", (char)name + ASCII_A );
           return res; 
       }
   }
}

/*............ Literal construction ............*/

/**
 * Create a SQL tree node representing a NULL.
 */
PFsql_t*
PFsql_null(void)
{
    PFsql_t *ret = leaf( sql_lit_null );
    ret->sem.atom.null = true;
    return ret;
}

/**
 * Create a SQL tree node representing a literal integer.
 *
 * @param   i     The integer value to represent in SQL.
 */
PFsql_t*
PFsql_lit_int(int i)
{
    PFsql_t *ret = leaf( sql_lit_int );
    ret->sem.atom.val.i = i;
    return ret;
}

/**
 * Create a SQL tree node representing a literl 64bit integer.
 *
 * @param   l  The integer value to represent in SQL.
 */
PFsql_t*
PFsql_lit_lng(long long int l)
{
    PFsql_t *ret = leaf( sql_lit_lng );
    ret->sem.atom.val.l = l;
    return ret;
}

/**
 * Create a SQL tree node representing a literal string.
 *
 * @param   s  The string value to represent in SQL.
 */
PFsql_t*
PFsql_lit_str(const char *s)
{
    PFsql_t *ret = leaf( sql_lit_str );

    /* check if string is defined */
    assert( s );

    ret->sem.atom.val.s = (char *)s;
    return ret;
}

/**
 * Create a SQL tree node representing a literal boolean.
 *
 * @param   b  The boolean value to represent in SQL.
 */
PFsql_t*
PFsql_lit_bln(bool b)
{
    PFsql_t *ret = leaf(sql_lit_bln);
    
    ret->sem.atom.val.b = b;
    return ret;
}

/*........... List construction .........*/

/**
 * Create a SQL tree node representing a terminator
 * object for all kind of lists. 
 */
PFsql_t*
PFsql_list_terminator(void)
{
    return leaf(sql_list_terminator);
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
PFsql_t*
PFsql_select_list_(unsigned int count, const PFsql_t **list)
{
    assert( count > 0 );

    if( count == 1 )
    {
        return wire2(sql_slct_list, list[0], terminator());
    }
    else
    {
        return wire2(sql_slct_list, list[0],
                PFsql_select_list_(count - 1, list + 1));
    }
    return NULL;
}

/**
 * Create an empty select_list.
 */
PFsql_t*
PFsql_select_list_empty(void)
{
   return terminator();
}

/**
 * Adds an item to the front of an existing 
 * select_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t*
PFsql_select_list_add(const PFsql_t *list, const PFsql_t *item)
{
      return wire2(sql_slct_list, item, list);
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
PFsql_t*
PFsql_from_list_(unsigned int count, const PFsql_t **list)
{
    assert( count > 0 );

    if( count == 1)
    {
        return wire2(sql_frm_list, list[0], terminator());
    }
    else
    {
        return wire2(sql_frm_list, list[0],
                PFsql_from_list_(count - 1, list + 1));
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Create an empty from_list.
 */
PFsql_t*
PFsql_from_list_empty(void)
{
    return terminator();
}

/**
 * Adds an item to the front of an existing 
 * from_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t*
PFsql_from_list_add(const PFsql_t *list, const PFsql_t *item)
{
    return wire2(sql_frm_list, item, list);
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
 * @param   count Number of expressions in the array that follows.
 * @param   list  Array of exactly @a count expression nodes.
 * @return        A chain of expression nodes.
 */
PFsql_t*
PFsql_where_list_(unsigned int count, const PFsql_t **list)
{
    assert( count > 0 );

    if( count == 1 )
    {
        return wire2(sql_whr_list, list[0], terminator());
    }
    else
    {
        return wire2(sql_whr_list, list[0],
                PFsql_where_list_(count - 1, list + 1));
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Create an empty where_list.
 */
PFsql_t*
PFsql_where_list_empty(void)
{
    return terminator();
}

/**
 * Adds an item to the front of an existing 
 * where_list.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t*
PFsql_where_list_add(const PFsql_t *list, const PFsql_t *item)
{
    return and( item, list);
}

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
PFsql_t*
PFsql_column_list_(unsigned int count, const PFsql_t **list)
{
    assert( count > 0 );

    if( count == 1 )
    {
        return wire2(sql_clmn_list, list[0], terminator());
    }
    else
    {
        return wire2(sql_clmn_list, list[0],
                PFsql_column_list_(count - 1, list + 1));
    }
    return NULL; /* satisfy picky compilers */
}

/**
 * Create an empty column_list.
 */
PFsql_t*
PFsql_column_list_empty(void)
{
    return terminator();
}

/**
 * Adds an item to the front of an existing 
 * columnlist.
 *
 * @param   list  The list.
 * @param   item  The item to append to the front
 *                of the @a list list.
 */
PFsql_t*
PFsql_column_list_add(PFsql_t *list, PFsql_t *item)
{
    return wire2(sql_clmn_list, item, list);
}

/*........... Arithmetic .............*/

/**
 * Create a SQL tree node representing an arithmetic add
 * operator.
 *
 * @param   a  First addend.
 * @param   b  Second addend.
 */
PFsql_t*
PFsql_add(const PFsql_t *a, const PFsql_t *b)
{
    return wire2( sql_add, a, b);
}

/**
 * Create a SQL tree node representing an arithmetic
 * subtract operator.
 *
 * @param   a  The minuend.
 * @param   b  The subtrahend.
 */
PFsql_t*
PFsql_sub(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_sub, a, b);
}

/**
 * Create a SQL tree node representing an arithmetic
 * multiplication operator.
 *
 * @param   a  The multiplicand.
 * @param   b  The multiplier.
 */
PFsql_t*
PFsql_mul(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_mul, a, b);
}

/**
 * Create a SQL tree node representing an arithmetic
 * division operator.
 *
 * @param   a  The divident.
 * @param   b  The quotient.
 */
PFsql_t*
PFsql_div(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_div, a, b);
}

/*............ Boolean operator constructors ...........*/

/**
 * Create a SQL tree node representing a boolean
 * (comparison) greater-than operator.
 *
 * @param   a  
 * @param   b
 */
PFsql_t*
PFsql_gt(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_gt, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * (comparison) equality operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t*
PFsql_eq(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_eq, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * negation operator.
 *
 * @param   a
 */
PFsql_t*
PFsql_not(const PFsql_t *a)
{
    return wire1(sql_not, a);
}

/**
 * Create a SQL tree node representing a boolean
 * `and' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t*
PFsql_and(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_and, a, b);
}

/**
 * Create a SQL tree node representing a boolean
 * `or' operator.
 *
 * @param   a
 * @param   b
 */
PFsql_t*
PFsql_or(const PFsql_t *a, const PFsql_t *b)
{
    return wire2(sql_or, a, b);
}

