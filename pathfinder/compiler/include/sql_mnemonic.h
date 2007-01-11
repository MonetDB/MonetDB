/**
 * @file
 *
 * Mnemonic abbreviations for sql constructors.
 * (Generic sql constructors)
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

#ifndef __SQL_MNEMONIC_H__
#define __SQL_MNEMONIC_H__

/*............ SQL operator construction .............*/

/** Operator leaf construction. */
#define leaf( k )                   PFsql_op_leaf( k )
/** Construct sequence of SQL statements. */
#define common_table_expr( ... )    PFsql_common_table_expr( __VA_ARGS__ )
#define star()                      PFsql_star()
/** SQL select construction. */
#define select(sl, fl, wl)              PFsql_select(sl, fl, wl)
/** SQL select distinct construction */
#define select_distinct(sl, fl, wl)     PFsql_select_distinct(sl, fl, wl)
#define disjunion( a, b )           PFsql_union( a , b )
#define difference( a, b )          PFsql_difference( a, b )

/*............ Common Operators ...........*/

#define with( a, b )                   PFsql_with( a, b )
/** Comment construction. */
#define comment( ... )              PFsql_comment( __VA_ARGS__ )
#define type( t )                   PFsql_type( t )
#define cast( a, b )                PFsql_cast( a, b )

/*............ SQL aggregat functions ............*/ 

#define count(b, e)                    PFsql_count(b, e)
/* SQL rownumber aggrfun construction. */
#define rownumber( )                   PFsql_rownumber( )
#define over( a, b )                   PFsql_over( a, b )
#define partition( a )                 PFsql_partition( a )
#define part_expressions( ... )        PFsql_part_expressions( __VA_ARGS__ )
#define part_expressions_empty()       PFsql_part_expressions_empty()
#define part_expressions_add(l, i)     PFsql_part_expressions_add(l, i)
#define order_by( a )               PFsql_order_by( a )
#define sortkey_expressions_empty()    PFsql_sortkey_expressions_empty()
#define sortkey_expressions_add(l, i)  PFsql_sortkey_expressions_add(l, i)
#define window_clause(p, o)            PFsql_window_clause(p, o)

/*........... Tables .............*/

#define SYSIBM                      "sysibm"
/* SQL table construction. */
#define sysdummy1()                 PFsql_sysdummy1()
#define fragrelation()              PFsql_fragrelation()
#define result()                    PFsql_result()
#define table_name_default( n )     PFsql_table_name_default( n )
#define table_name( n, clmnlist )   PFsql_table_name( n, clmnlist )
#define schema( s )                 PFsql_schema( s )
/* SQL bind construction. */
#define bind( a, b )                PFsql_bind( a, b )
#define tab_name( a, b )            PFsql_tab_name( a, b )
#define alias( a, b )               PFsql_alias( a, b )
#define correlation_name( i )       PFsql_correlation_name( i )
#define crrl_deco( op, crrl )       PFsql_correlation_decorator( op, crrl )
#define CRRL_UNBOUNDED              PF_SQL_CORRELATION_UNBOUNDED

/*........... Columns .............*/

/* SQL attribute variable construction. */
#define column_name( i )          PFsql_column_name( i )
#define column_assign( a, b )       PFsql_column_assign( a, b )

/*........... literal constructors ............*/ 

/** Literal null construction */
#define null( )               PFsql_null( )
/** Literal int construction. */
#define lit_int( i )          PFsql_lit_int( i )
/** Literal long construction. */
#define lit_lng( l )          PFsql_lit_lng( l )
/** Literal string construction. */
#define lit_str( s )          PFsql_lit_str( s )
/** Literal boolean construction. */
#define lit_bln( b )          PFsql_lit_bln( b )

/* ............ List construction ..............*/

/** Construct a list terminator. */
#define terminator( )                  PFsql_list_terminator()
/** A sequence of select_list-expressions. */
#define select_list( ... )             PFsql_select_list( __VA_ARGS__ )
/** Construct empty attribute list. */
#define select_list_empty( )           PFsql_select_list_empty( )
/** Add an item to an existing attribute list. */
#define select_list_add( list, item )  PFsql_select_list_add( list, item )
/** Construct a from_list-expression. */
#define from_list( ... )               PFsql_from_list( __VA_ARGS__ )
/** Construct empty fromlist. */
#define from_list_empty( )             PFsql_from_list_empty( )
/** Add an item to an existing fromlist. */
#define from_list_add( list, item )    PFsql_from_list_add( list, item )
/** A sequence of columns */
#define column_list( ... )             PFsql_column_list( ... )
/** Construct empty argument list */
#define column_list_empty( )           PFsql_column_list_empty( )
/** Add an item to an existing arglist. */
#define column_list_add( list, item )  PFsql_column_list_add( list, item )

#define where_list_empty( )            PFsql_where_list_empty( )
#define where_list_add( l, i )         PFsql_where_list_add( l, i )

/*.......... Arithmetic operator constructors ........*/

/** Construct arithmetic `add' operator. */
#define add( a, b )                 PFsql_add( a, b )  
/** Construct arithmetic `subtraction' operator. */
#define sub( a, b )                 PFsql_sub( a, b )
/** Construct arithmetic `multiplicaton' operator. */
#define mul( a, b )                 PFsql_mul( a, b )
/** Construct arithmetic `division' operator. */
#define div( a, b )                 PFsql_div( a, b )

/*......... Boolean operator constructors .......*/

/** Construct boolean `greater-than' operator. */
#define gt( a, b )                  PFsql_gt( a, b )
/** Construct boolean `greater-than-or-equal' operator. */
#define gteq( a, b )                  PFsql_gteq( a, b )
/** Construct boolean `equal' operator.  */
#define eq( a, b )                  PFsql_eq( a, b )
/** Construct boolean `not' operator. */
#define not( a, b )                 PFsql_not( a, b )
/** Construct a boolean `and' operator. */
#define and( a, b )                 PFsql_and( a, b )
/** Construct a boolean `or' operator. */
#define or( a, b )                  PFsql_or( a, b )

#endif /* __SQL_MNEMONIC_H__ */
