/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Debugging: dump SQL language tree in
 * human readable format 
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
 * $Id
 */

#include "pathfinder.h"

#include <stdlib.h>
#include <string.h>
#include "oops.h"

#include "prettysql.h"

#include "mem.h"
/* PFsql_t */
#include "sql.h"
/* PFty_str */
#include "types.h"
/* PFesc_string */
#include "pfstrings.h"

#include "prettyp.h"
#include <assert.h>

static char *ID[] = {
      [sql_tbl_name] = "table_name",
      [sql_add]      = "+",
      [sql_sub]      = "-",
      [sql_mul]      = "*",
      [sql_div]      = "/"
};

FILE *out = NULL;

/* forward declarations for the left site of grammar rules */
static void print_statements(PFsql_t*);
static void print_statement(PFsql_t*);
static void print_schema_name(PFsql_t*);
static void print_variable(PFsql_t*);
static void print_select_list(PFsql_t*);
static void print_clm_list(PFsql_t*);
static void print_aggrfunction(PFsql_t*);
static void print_common_table_expressions(PFsql_t *);
static void print_common_table_expression(PFsql_t*);
static void print_sort_key_expressions(PFsql_t*);
static void print_window_clause(PFsql_t*);
static void print_table_name(PFsql_t*);
static void print_subselect(PFsql_t*);
static void print_from_list(PFsql_t*);
static void print_tablereference(PFsql_t*);
static void print_fullselect(PFsql_t*);
static void print_expr(PFsql_t*);
static void print_correlation(PFsql_t*);
static void print_column_name(PFsql_t*);
static void print_schema_information(PFsql_t*);
static void print_schema_table(PFsql_t*);
static void print_schema_relcol(PFsql_t*);
static void print_join(PFsql_t*);

static void
print_sequence(PFsql_t *n)
{
   assert( n );
   assert( n->kind == sql_seq );

   print_schema_information(n->child[0]);
   PFprettyprintf("\n");
   print_statements(n->child[1]);
}   

/**
 * Convert the @a ident to a string.
 *
 * @param ident The identifier to convert.
 */
static char*
sql_column_name_str(PFsql_ident_t ident)
{
    char *attstr = NULL;
    char *tystr = NULL;
    char *res = NULL;
    size_t len = 0;

    /* check if special bits are set */
    if( (ident >> (ATT_BITS + TYPE_BITS)) & 0x00000007 ) {
        switch( (0x00000001) << ((ident >> (ATT_BITS + TYPE_BITS))
             & 0x00000007) ) {
            case sql_col_pre:    return "pre";
            case sql_col_level:  return "level";
            case sql_col_size: return "size";
            case sql_col_kind: return "kind";
            case sql_col_prop: return "prop";
            case sql_col_tag: return "tag";
            default: PFoops( OOPS_FATAL, 
                             "unknown special flag set");
        }
    }
    PFalg_att_t att = ((0x0000001F & ident) <= 0)?  0x00000000:
            (0x00000001 << (((0x0000001F) & ident)-1));
    PFalg_simple_type_t ty = (0x00000001 << (((0x000001E0) & ident)
                >> ATT_BITS));

    attstr = PFatt_str(att);
    tystr  = PFalg_simple_type_str(ty);

    len = strlen(attstr);
    len += strlen(tystr);

    res = (char*)PFmalloc(len * sizeof(char));
    snprintf(res, len+2, "%s-%s", attstr, tystr);

    return res;
}

static void
print_schema_information( PFsql_t *n )
{
    assert( n );

    switch( n->kind ) {
        case sql_schm_inf:
            print_schema_information( n->child[0] );
            fprintf(out, "\n");
            print_schema_information( n->child[1] );
            break;
        case sql_schm_cmmnt:
            fprintf(out, "-- !! ");
            fprintf(out, "%s", n->sem.comment );
            fprintf(out, " !!");
            break;
        case sql_schm_expr:
            fprintf(out, "-- ");
            print_schema_table( n->child[0] );
            print_schema_relcol( n->child[1] );
            break;
        default:
            PFoops( OOPS_FATAL,
                    "SQL generation doesn't support this kind "
                    "of schema information (%u)", n->kind );
    }
}

static void 
print_schema_relcol( PFsql_t *n )
{
    assert( n );
    
    fprintf(out, "-");
    switch( n->kind ) {
        case sql_clmn_name:
            fprintf(out, "%s", sql_column_name_str( n->sem.column.ident ));
            fprintf(out, ": ");
            fprintf(out, "%s", PFsql_column_name_str( n->sem.column.ident ));
            break;
        case sql_tbl_name:
            fprintf(out, "relation: ");
            fprintf(out, "%s", PFsql_table_str( n->sem.tablename.ident ));
            break;
        default:
            PFoops( OOPS_FATAL,
                    "In schema information is neither a "
                    "column nor a relation (%u)", n->kind);
    }
}

static void
print_schema_table( PFsql_t *n )
{
    assert( n );

    switch( n->kind ) {
        case sql_schm_doc:
            fprintf(out, "document");
            break;
        case sql_schm_res:
            fprintf(out, "result");
            break;
        default:
            PFoops( OOPS_FATAL, 
                    "SQL generation doesn't support this schema tables");
    }
}

/**
 * Print SQL statements.
 *
 * @param n SQL tree node.
 */
static void
print_statements(PFsql_t *n)
{
    switch( n->kind ) {
        case sql_with:
        {
            PFprettyprintf("WITH ");
            print_common_table_expressions( n->child[0] );
        } break;
       default:
        {
            /* TODO print string of node kind in the warning */
            print_statement( n );
            PFprettyprintf("\n");
        } break;
    }
}

static void
print_common_table_expressions(PFsql_t *n)
{
   assert( n );
   switch( n->kind ) {
       case sql_cmmn_tbl_expr:
       {
           print_common_table_expressions( n->child[0] );
           PFprettyprintf(", ");
           print_common_table_expression( n->child[1] );
       } break;
       default:
       {
           PFprettyprintf("%c", START_BLOCK);
           print_common_table_expression( n );
           PFprettyprintf("%c", END_BLOCK);
       } break;
   }
}

static void
print_common_table_expression(PFsql_t *n)
{
    assert( n );
    assert( n->kind == sql_bind );

    print_table_name( n->child[0] );
    PFprettyprintf(" AS (%c", START_BLOCK);
    print_fullselect( n->child[1] );
    PFprettyprintf("%c)", END_BLOCK);
}

static void
print_table_name(PFsql_t *n)
{
    assert( n );
    assert( n->kind == sql_tbl_name );

    PFprettyprintf("%s", PFsql_table_str( n->sem.tablename.ident ));
    /* the column-list has not to be specified in every case */ 
    if( n->child[0]) {
        PFprettyprintf("(");
        print_clm_list(n->child[0]);
        PFprettyprintf(")");
    }
}

static void
print_subselect(PFsql_t *n)
{
    assert( n );
    switch( n->kind ) {
        case sql_select:
        {
            PFprettyprintf("SELECT ");
            PFprettyprintf("%s%c", ( n->sem.select.distinct )?
                    "DISTINCT ":"", START_BLOCK);
            print_select_list( n->child[0] );

            PFprettyprintf("%c FROM %c", END_BLOCK, START_BLOCK);
            print_from_list( n->child[1] );
            PFprettyprintf("%c", END_BLOCK);
            /* where list is optional, we leave the where list
               to zero when its not specified */
            if( n->child[2]) {
                PFprettyprintf(" WHERE %c", START_BLOCK);
                print_expr( n->child[2]);
                PFprettyprintf("%c", END_BLOCK);
            }
            if( n->child[3]) {
                PFprettyprintf(" GROUP BY %c", START_BLOCK);
                print_clm_list( n->child[3] );
                PFprettyprintf("%c", END_BLOCK);
            }
        } break;
        default:
        {
            PFoops( OOPS_FATAL,
                    "Fullselect doesnt support this kind of node (%u).",
                    n->kind);
        } break;
    }
}

static void
print_from_list(PFsql_t *n)
{
    assert( n );

    switch( n->kind ) {
        case sql_frm_list:
            PFprettyprintf("%c", START_BLOCK);
            print_from_list( n->child[0] );
            PFprettyprintf("%c, %c", START_BLOCK, END_BLOCK);
            print_from_list( n->child[1] );
            PFprettyprintf("%c", END_BLOCK);
            break;
        case sql_alias:
        case sql_tb_name:
        case sql_tbl_name:
        case sql_select:
            print_tablereference( n );
            break;
        case sql_on:
            print_join( n );
            break;
        default:
            PFoops( OOPS_FATAL,
                    "This kind (%u) of statement is not supported by "
                    "fromlist", n->kind);
            break;
    }
}

static void
print_tablereference(PFsql_t* n)
{
    assert( n );

    switch( n->kind ) {
        case sql_alias:
        {
          PFprettyprintf("%c", START_BLOCK);
          print_tablereference( n->child[0] );
          PFprettyprintf("%c AS %c", END_BLOCK, START_BLOCK);
          print_correlation( n->child[1]);
          PFprettyprintf("%c", END_BLOCK);
        } break;
        case sql_tbl_name:
        {
            print_table_name( n );
        } break;
        case sql_tb_name:
        {
            print_schema_name( n->child[0] );
            PFprettyprintf(".");
            print_table_name( n->child[1] );
        } break;
        default:
        {
            PFprettyprintf("(");
            print_fullselect( n );
            PFprettyprintf(")");
        } break;
    }
}

static void
print_correlation( PFsql_t *n)
{
   assert( n );
   assert( n->kind == sql_crrltn_name );

   PFprettyprintf("%s",
       PFsql_correlation_name_str( n->sem.correlation.ident ));
}

static void
print_fullselect(PFsql_t *n)
{
    assert( n );
    switch( n->kind ) {
       case sql_union:
        {
            PFprettyprintf("(");
            print_fullselect(n->child[0]);
            PFprettyprintf(") UNION ALL (");
            print_fullselect(n->child[1]);
            PFprettyprintf(")");
        } break;
        case sql_diff:
        {
            PFprettyprintf("(");
            print_fullselect(n->child[0]);
            PFprettyprintf(") EXCEPT ALL (");
            print_fullselect(n->child[1]);
            PFprettyprintf(")");
        } break;

        default:
        {
            print_subselect( n );
        } break;
    }
}

static void
print_expr(PFsql_t *n)
{
    assert( n );

    switch( n->kind )
    {
        case sql_not:
         PFprettyprintf("NOT (");
         print_expr( n->child[0] );
         PFprettyprintf(")");
         break;
        case sql_max:
        {
            PFprettyprintf("MAX (");
            print_expr( n->child[0] );
            PFprettyprintf(")");
        } break;
        case sql_and:
        {
           PFprettyprintf("(");
           print_expr( n->child[0] );
           PFprettyprintf(" AND ");
           print_expr( n->child[1] );
           PFprettyprintf(")");
        } break;
        case sql_eq:
        {
          PFprettyprintf("(");
          print_expr( n->child[0] );
          PFprettyprintf(" = ");
          print_expr( n->child[1] );
          PFprettyprintf(")");
        } break;
        case sql_gt:
        {
          PFprettyprintf("(");
          print_expr( n->child[0] );
          PFprettyprintf(" > ");
          print_expr( n->child[1] );
          PFprettyprintf(")");
        } break;
        case sql_gteq:
        {
          PFprettyprintf("(");
          print_expr( n->child[0] );
          PFprettyprintf(" >= ");
          print_expr( n->child[1] );
          PFprettyprintf(")");
        } break;
        case sql_clmn_name:
        {
            if( n->crrlname != PF_SQL_CORRELATION_UNBOUNDED ) {
                PFprettyprintf("%s",
                    PFsql_correlation_name_str(n->crrlname));
                PFprettyprintf(".");
            }
            PFprettyprintf("%s",
                    PFsql_column_name_str(n->sem.column.ident));
        } break;
        case sql_lit_int:
        {
            PFprettyprintf("%i", n->sem.atom.val.i);
        } break;
        case sql_lit_str:
        {
            PFprettyprintf("'%s'", n->sem.atom.val.s);
        } break;
        case sql_cst:
            PFprettyprintf("CAST(");
            print_statement(n->child[0]);
            PFprettyprintf(" AS ");
            print_statement(n->child[1]);
            PFprettyprintf(")");
        break;
        /* expression : '(' expression '+' expression ')' */
        case sql_add:
        case sql_sub:
        case sql_mul:
        case sql_div:
        {
            PFprettyprintf("(");
            print_statement(n->child[0]);
            PFprettyprintf(" %s ", ID[n->kind]);
            print_statement(n->child[1]);
            PFprettyprintf(")");
        } break;
	case sql_like:
	{
	    PFprettyprintf("(");
	    print_statement(n->child[0]);
	    PFprettyprintf(" LIKE  '%%");
	    /* write the string without beginning and 
	       trailing ' */
	    assert (n->child[1]->kind == sql_lit_str);
	    PFprettyprintf("%s", n->child[1]->sem.atom.val.s); 
	    PFprettyprintf("\%%')");
	} break;
        default:
        {
            PFoops( OOPS_FATAL, "expression screwed up (%u)",
                    n->kind );
        } break;
    }
}
    
static void
print_select_list(PFsql_t *n)
{
    assert( n);
    
    switch( n->kind ) {
        case sql_slct_list:
            PFprettyprintf("%c", START_BLOCK);
            print_select_list( n->child[0] );
            PFprettyprintf(",%c %c", END_BLOCK, START_BLOCK);
            print_select_list( n->child[1] );
            PFprettyprintf("%c", END_BLOCK);
            break;
        default:
            print_statement(n);
            break;
    }
}

static void
print_part_expression(PFsql_t *n)
{
    assert( n );
    
    if( !((n->kind == sql_prt_expr) ||
            (n->kind == sql_list_terminator)) )
    {
        PFoops( OOPS_FATAL,
                "Pathfinder failed to print partition list" );
    }
   
    if( (n->kind == sql_list_terminator) &&
            (n->child[0] == NULL) && (n->child[1] == NULL) )
        return;

    print_statement(n->child[0]);
    if(!(n->child[1]->kind == sql_list_terminator ))
        PFprettyprintf(",");
    PFprettyprintf(" ");
    print_part_expression(n->child[1]);
}

static void
print_join( PFsql_t *n )
{
    assert( n );

    switch( n->kind ) {
        case sql_on:
            print_join( n->child[0] );
            PFprettyprintf(" ON ");
            print_expr( n->child[1] );
            break;
        case sql_innr_join:
            print_join( n->child[0] );
            PFprettyprintf(" INNER JOIN ");
            print_join( n->child[1] );
            break;
        case sql_outr_join:
            print_join( n->child[0] );
            PFprettyprintf(" RIGHT OUTER JOIN ");
            print_join( n->child[1] ); 
            break;
        case sql_alias:
        case sql_tb_name:
        case sql_tbl_name:
        case sql_select:
            print_tablereference( n );
            break;
        default:
            PFoops( OOPS_FATAL,
                    "SQL genereation doesn't support this kind "
                    "of join attribute (%u)", n->kind);
    }
}

static void
print_sort_key_expressions(PFsql_t *n)
{
    assert( n);
    
    switch( n->kind ) {
        case sql_srtky_expr:
            print_sort_key_expressions( n->child[0] );
            PFprettyprintf(", ");
            print_sort_key_expressions( n->child[1] );
            break;
        default:
            print_statement( n );
            break;
    }
}

static void
print_window_clause(PFsql_t* n)
{
    assert( n );
    assert( n->kind == sql_wnd_clause );

    if( n->child[0] ) {
        print_statement(n->child[0]);
    }
    if( n->child[1] ) {
        print_statement(n->child[1]);
    }
}

static void
print_clm_list(PFsql_t *n)
{
    assert( n );

    switch( n->kind ) {
        case sql_clmn_list:
            print_clm_list( n->child[0] );
            PFprettyprintf(", ");
            print_clm_list( n->child[1] );
            break;
        default:
            print_column_name( n );
    }
}

static void
print_column_name( PFsql_t *n )
{
    assert( n );
    assert( n->kind == sql_clmn_name );

    if( n->crrlname != PF_SQL_CORRELATION_UNBOUNDED ) {
        PFprettyprintf("%s", 
            PFsql_correlation_name_str(n->crrlname));
        PFprettyprintf(".");
    }
    PFprettyprintf("%s",
        PFsql_column_name_str(n->sem.column.ident));

}

static void
print_statement(PFsql_t *n)
{
    switch( n->kind ) {
	case sql_asc:
	    PFprettyprintf("ASC");
	    break;
   	case sql_desc:
	    PFprettyprintf("DESC");
	    break;
	case sql_order:
	    print_statement (n->child[0]);
	    PFprettyprintf(" ");
	    print_statement (n->child[1]);
	    break;
        case sql_sum:
            PFprettyprintf("SUM(");
            print_statement( n->child[0] );
            PFprettyprintf(")");
            break;
        case sql_max:
        {
            PFprettyprintf("MAX (");
            print_statement( n->child[0] );
            PFprettyprintf(")");
        } break;
        case sql_count:
        {
            PFprettyprintf("COUNT (");
            PFprettyprintf("%s", (n->sem.count.distinct)?"DISTINCT ":"");
            print_statement( n->child[0] );
            PFprettyprintf(")"); 
        } break;
        case sql_eq:
            PFprettyprintf("(");
            print_statement( n->child[0] );
            PFprettyprintf(" = ");
            print_statement( n->child[1] );
            PFprettyprintf(")");
            break;
        case sql_prttn:
            PFprettyprintf("PARTITION BY ");
            print_part_expression(n->child[0]);
            break;
        case sql_ordr_by:
            PFprettyprintf("ORDER BY ");
            print_sort_key_expressions(n->child[0]);
            break;
        case sql_cst:
            PFprettyprintf("CAST(");
            print_statement(n->child[0]);
            PFprettyprintf(" AS ");
            print_statement(n->child[1]);
            PFprettyprintf(")");
            break;
        case sql_type:
            PFprettyprintf("%s", PFsql_simple_type_str(n->sem.type.t));
            break;
        case sql_tb_name:
            print_schema_name( n->child[0] );
            PFprettyprintf(".");
            print_table_name( n->child[1] );
            break;
        case sql_star:
            PFprettyprintf("*");
            break;
        case sql_union:
            {
                PFprettyprintf("(");
                print_statement( n->child[0] );
                PFprettyprintf(") UNION ALL (");
                print_statement( n->child[1] );
                PFprettyprintf(")");
            } break;
        case sql_diff:
            {
                PFprettyprintf("(");
                print_statement( n->child[0] );
                PFprettyprintf(") EXCEPT ALL (");
                print_statement( n->child[1] );
                PFprettyprintf(")");
            } break;
        case sql_alias:
            {
                if(n->child[0]->kind != sql_tbl_name ) {
                    PFprettyprintf("(");
                }
                print_statement( n->child[0] );
                if(n->child[0]->kind != sql_tbl_name ){
                    PFprettyprintf(")");
                } 
                PFprettyprintf(" ");
            } break;
        case sql_dot:
        {
            PFprettyprintf(".");
            print_statement( n->child[1] );
        } break;
        case sql_over:
        {   
           print_aggrfunction(n->child[0]);
           PFprettyprintf(" OVER (%c", START_BLOCK );
           print_window_clause(n->child[1]);
           PFprettyprintf("%c)", END_BLOCK);
        } break;
        case sql_bind:
        {
            print_variable( n->child[0] );
            PFprettyprintf(" AS (");
            print_statement( n->child[1] );
            PFprettyprintf(")");
        } break;
        case sql_clmn_name:
        {
            if( n->crrlname != PF_SQL_CORRELATION_UNBOUNDED ) {
                PFprettyprintf("%s", 
                    PFsql_correlation_name_str(n->crrlname));
                PFprettyprintf(".");
            }
            PFprettyprintf("%s",
                    PFsql_column_name_str(n->sem.column.ident));
        } break;
        case sql_tbl_name:
        {
            print_variable( n );
        } break;
        case sql_clmn_assign:
        {
            print_statement( n->child[0]);
            PFprettyprintf(" AS ");
            print_statement( n->child[1]);
        } break;
        case sql_lit_dec:
        {
            PFprettyprintf("%g", n->sem.atom.val.dec);
        } break;
        case sql_lit_int:
        {
            PFprettyprintf("%i", n->sem.atom.val.i);
        } break;
        case sql_lit_null:
        {
            PFprettyprintf("NULL");
        } break;
        case sql_lit_str:
        {
            PFprettyprintf("'%s'", n->sem.atom.val.s);
        } break;
        case sql_table_ref:
        {
            PFprettyprintf("%s", n->sem.table );
        } break;
       // case sql_select:
       // {
       //     PFprettyprintf("SELECT ");
       //     if( n->sem.select.distinct ) PFprettyprintf("DISTINCT ");
       //     print_select_list(n->child[0]);
       //     if(n->child[1])
       //     {
       //         PFprettyprintf(" FROM ");
       //     }
       // } break;
        /* expression : '(' expression '+' expression ')' */
        case sql_add:
        case sql_sub:
        case sql_mul:
        case sql_div:
        {
            PFprettyprintf("(");
            print_statement(n->child[0]);
            PFprettyprintf(" %s ", ID[n->kind]);
            print_statement(n->child[1]);
            PFprettyprintf(")");
        } break;
	case sql_like:
	{
	    PFprettyprintf("(");
	    print_statement(n->child[0]);
	    PFprettyprintf(" LIKE  '%%");
	    /* write the string without beginning and 
	       trailing ' */
	    assert (n->child[1]->kind == sql_lit_str);
	    PFprettyprintf("%s", n->child[1]->sem.atom.val.s); 
	    PFprettyprintf("\%%')");
	} break;
        case sql_gt:
        {
          PFprettyprintf("(");
          print_expr( n->child[0] );
          PFprettyprintf(" > ");
          print_expr( n->child[1] );
          PFprettyprintf(")");
        } break;
        case sql_gteq:
        {
          PFprettyprintf("(");
          print_expr( n->child[0] );
          PFprettyprintf(" >= ");
          print_expr( n->child[1] );
          PFprettyprintf(")");
        } break;

        default:
        {
            PFoops( OOPS_FATAL,
                    "Pathfinder doesn't support this kind "
                    "of SQL tree node (%i)", n->kind); 
        } break;
    }
}

static void
print_schema_name(PFsql_t *n)
{
    assert( n );
    assert( n->kind == sql_schm );

    PFprettyprintf("%s", n->sem.schema.str);
}

static void
print_aggrfunction(PFsql_t *n)
{
    switch( n->kind ) {
        case sql_rownumber:
        {
            PFprettyprintf("%cROWNUMBER()%c",
                START_BLOCK, END_BLOCK);
        } break;
        default:
        {
            PFoops( OOPS_FATAL,
                    "Pathfinder doesn't support this kind "
                    "of aggregat function");
        } break;
    }
}

static void
print_expression(PFsql_t *n)
{
    assert( n );
    PFprettyprintf("%c", START_BLOCK);
    switch( n->kind ) {
        case sql_expr_list:
        {
            print_expression(n->child[0]);
            PFprettyprintf(", ");
            print_expression(n->child[1]);
        } break;
        case sql_tbl_name:
        {
            print_variable (n);
        } break;
        case sql_sum:
        {
            PFprettyprintf("(");
            print_statement(n->child[0]);
            PFprettyprintf(" %s ", ID[n->kind]);
            print_statement(n->child[1]);
            PFprettyprintf(")");
        } break;
        default:
        {
            /* TODO insert NDEBUG */
            PFinfo( OOPS_NOTICE, 
                        "node: %s", ID[n->kind]);
            PFoops( OOPS_FATAL,
                    "Illegal SQL tree. SQL printer screwed up "
                    "(kind: %u).",
                    n->kind );
        } break;
    }
    PFprettyprintf("%c", END_BLOCK);
}

static void
print_variable( PFsql_t *n )
{
    assert( n );
    assert( n->kind == sql_tbl_name );

    PFprettyprintf("%s", PFsql_table_str( n->sem.tablename.ident ));
    if( n->child[0]) {
        PFprettyprintf("(");
        print_clm_list(n->child[0]);
        PFprettyprintf(")");
    }
}
/**
 * Recursively walk the SQL tree @a n and prettyprint
 * the query it represents.
 *
 * @param n SQL tree to prettyprint
 */
static void sql_pretty(PFsql_t *n)
{
    bool comma;

    if (!n)
        return;

    comma = true;

    print_sequence(n);
}

/**
 * Dump SQL tree @a t in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param t root of SQL tree
 */
void
PFsql_pretty(FILE *f, PFsql_t *t)
{
    out = f;
    PFprettyprintf ("%c", START_BLOCK);
    sql_pretty (t);
    PFprettyprintf ("%c", END_BLOCK);

    (void) PFprettyp (f);

    fputc ('\n', f);
}

/* vim:set shiftwidth=4 expandtab: */
