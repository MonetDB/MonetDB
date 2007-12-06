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

#define root(s,q)                    PFsql_root((s),(q))

/* .......... Serialization information .......... */
#define ser_info_item(i,l)           PFsql_serialization_info_item((i),(l))
#define ser_map(c,n)                 PFsql_serialization_name_mapping((c),(n))
#define ser_comment(str)             PFsql_serialization_comment((str))
#define ser_type(t,q)                PFsql_serialization_type((t),(q))

/* .......... Tables .......... */
#define schema_table_name(s,t)       PFsql_schema_table_name(s,t)
#define table_name(n)                PFsql_table_name(n)
#define ref_table_name(n)            PFsql_ref_table_name(n)
#define table_def(n,clmnlist)        PFsql_table_def(n,clmnlist)
#define alias(i)                     PFsql_alias(i)

/* .......... Columns .......... */
#define column_list(...)             PFsql_column_list(__VA_ARGS__)
#define column_name(i)               PFsql_column_name(PF_SQL_ALIAS_UNBOUND,i)
#define ref_column_name(a,n)         PFsql_ref_column_name(PF_SQL_ALIAS_UNBOUND,n)
#define ext_column_name(a,i)         PFsql_column_name(a,i)
#define star()                       PFsql_star()

/* .......... Top Level Query Constructs .......... */
#define with(a,f)                    PFsql_with(a,f)
#define common_table_expr(o,n)       PFsql_common_table_expr(o,n)
#define comment(...)                 PFsql_comment(__VA_ARGS__)
#define bind(t,e)                    PFsql_bind(t,e)
#define nil()                        PFsql_nil()

/* .......... Select .......... */
#define select_distinct(sl,fl,wl)       PFsql_select(true,sl,fl,wl,NULL,NULL)
#define select(sl,fl,wl)                PFsql_select(false,sl,fl,wl,NULL,NULL)
#define select_list(...)                PFsql_select_list(__VA_ARGS__)
#define column_assign(e,c)              PFsql_column_assign(e,c)
#define from_list(...)                  PFsql_from_list(__VA_ARGS__)
#define add_from(list,item)             PFsql_add_from(list,item)
#define alias_bind(t,a)                 PFsql_alias_bind((t),(a))
#define on(jn,ex)                       PFsql_on(jn, ex)
#define left_outer_join(tbl1, tbl2)     PFsql_left_outer_join(tbl1, tbl2)
#define where_list(...)                 PFsql_where_list(__VA_ARGS__)

/* .......... Union .......... */
#define union_(a,b)                  PFsql_union(a,b)

/* .......... Difference .......... */
#define diff(a,b)                    PFsql_difference(a,b)

/* .......... Literal construction .......... */
#define lit_int(i)                   PFsql_lit_int(i)
#define lit_lng(l)                   PFsql_lit_lng(l)
#define lit_str(s)                   PFsql_lit_str(s)
#define lit_dec(d)                   PFsql_lit_dec(d)
#define null()                       PFsql_null()
                                     
/* .......... Arithmetic Operators .......... */
#define add(a,b)                     PFsql_add(a,b)
#define sub(a,b)                     PFsql_sub(a,b)
#define mul(a,b)                     PFsql_mul(a,b)
#define div(a,b)                     PFsql_div(a,b)

/* .......... Integer Functions ............. */
#define floor(a)                     PFsql_floor(a)
#define ceil(a)                      PFsql_ceil(a)
#define modulo(a,b)                  PFsql_modulo(a,b)
#define abs(a)                       PFsql_abs(a)

/* .......... String Functions ........... */
#define concat(a,b)                  PFsql_concat(a,b)

/* .......... Table Functions ............ */
#define values(a)                    PFsql_values(a)
#define list_list(...)               PFsql_list_list(__VA_ARGS__)
                                     
/* .......... Boolean Operators .......... */
#define is(a, b)                     PFsql_is(a, b)
#define is_not(a, b)                 PFsql_is_not(a, b)
#define eq(a,b)                      PFsql_eq(a,b)
#define gt(a,b)                      PFsql_gt(a,b)
#define gteq(a,b)                    PFsql_gteq(a,b)
#define between(c,a,b)               PFsql_between(c,a,b)
#define like(a,b)                    PFsql_like(a,b)
#define in(a,b)                      PFsql_in(a,b)
#define stmt_list(...)               PFsql_stmt_list(__VA_ARGS__)
#define not_(a)                      PFsql_not(a)
#define and_(a,b)                    PFsql_and(a,b)
#define or_(a,b)                     PFsql_or(a,b)
                                     
/* .......... Aggregate Functions .......... */
#define count(c)                     PFsql_count(c)
#define max(c)                       PFsql_max(c)
#define min(c)                       PFsql_min(c)
#define avg(c)                       PFsql_avg(c)
#define sum(c)                       PFsql_sum(c)
                                     
/* .......... OLAP Functionality .......... */
#define over(a,b)                    PFsql_over(a,b)
#define row_number()                 PFsql_row_number()
#define dense_rank()                 PFsql_dense_rank()
#define window_clause(p,o)           PFsql_window_clause(p,o)
#define order_by(a)                  PFsql_order_by(a)

#define sortkey_list(...)            PFsql_sortkey_list(__VA_ARGS__)
#define sortkey_item(i,d)            PFsql_sortkey_item(i,d)
#define partition(cl)                PFsql_partition(cl)
                                     
/* .......... Remaining Operators .......... */
#define type(t)                      PFsql_type(t)
#define cast(e,t)                    PFsql_cast(e,t)
#define coalesce(e1,e2)              PFsql_coalesce(e1,e2)
#define case_(...)                   PFsql_case(__VA_ARGS__)
#define when(be,e)                   PFsql_when(be,e)
#define else_(e)                     PFsql_else(e)
#define selectivity(pred,sel)        PFsql_selectivity(pred,sel)

#define duplicate(e)                 PFsql_op_duplicate(e)

#endif /* __SQL_MNEMONIC_H__ */
