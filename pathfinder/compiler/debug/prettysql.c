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
 * $Id$
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

/*
 * Easily access subtree parts.
 */
/** starting from p, make a left step */
#define L(p)      ((p)->child[0])
/** starting from p, make a right step */
#define R(p)      ((p)->child[1])

/* shortcut for pretty printing */
#define pretty_dump(f,i) PFprettyp_extended ((f), 70, (i))

/**
 * Break the current line and indent the next line 
 * by @a ind characters.
 *
 * @param f file to print to
 * @param ind indentation level
 */
static void
indent (FILE *f, int ind)
{
    fputc ('\n', f);

    while (ind-- > 0)
        fputc (' ', f);
}

static char *ID[] = {
      [sql_add]      = "+",
      [sql_sub]      = "-",
      [sql_mul]      = "*",
      [sql_div]      = "/",
      [sql_not]      = "NOT",
      [sql_count]    = "COUNT",
      [sql_max]      = "MAX",
      [sql_sum]      = "SUM",
      [sql_and]      = "AND",
      [sql_eq]       = "=",
      [sql_gt]       = ">",
      [sql_gteq]     = ">="
};

/* forward declarations for the left site of grammar rules */
static void print_statement(PFsql_t*);
static void print_expr(PFsql_t*);
static void print_fullselect(FILE*,PFsql_t*,int);

static void 
print_schema_relcol (FILE *f, PFsql_t *n)
{
    assert (n);
    
    fprintf (f, "-");
    switch (n->kind) {
        case sql_clmn_name:
            fprintf (f, "%s", PFsql_column_name_str (n->sem.column.ident));
            fprintf (f, ": ");
            fprintf (f, "%s", PFsql_column_name_str (n->sem.column.ident));
            break;
            
        case sql_tbl_name:
            fprintf (f, "relation: ");
            fprintf (f, "%s", PFsql_table_str (n->sem.tablename.ident));
            break;

        default:
            PFoops (OOPS_FATAL,
                    "In schema information is neither a "
                    "column nor a relation (%u)", n->kind);
    }
}

static void
print_schema_table (FILE *f, PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_schm_doc:
            fprintf (f, "document");
            break;

        case sql_schm_res:
            fprintf (f, "result");
            break;

        default:
            PFoops (OOPS_FATAL, 
                    "SQL generation doesn't support this schema tables");
    }
}

static void
print_schema_information (FILE *f, PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_schm_inf:
            print_schema_information (f, L(n));
            fprintf (f, "\n");
            print_schema_information (f, R(n));
            break;

        case sql_schm_cmmnt:
            fprintf (f, "-- !! ");
            fprintf (f, "%s", n->sem.comment);
            fprintf (f, " !!");
            break;

        case sql_schm_expr:
            fprintf (f, "-- ");
            print_schema_table (f, L(n));
            print_schema_relcol (f, R(n));
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL generation doesn't support this kind "
                    "of schema information (%u)", n->kind );
    }
}

static void
print_clm_list(PFsql_t *n)
{
    assert (n);

    switch( n->kind ) {
        case sql_clmn_list:
            print_clm_list (L(n));
            PFprettyprintf (", ");
            print_clm_list (R(n));
            break;
        case sql_clmn_name:
            if (n->crrlname != PF_SQL_CORRELATION_UNBOUNDED)
                PFprettyprintf(
                    "%s.", 
                    PFsql_correlation_name_str (n->crrlname));
            
            PFprettyprintf(
                "%s",
                PFsql_column_name_str (n->sem.column.ident));
            break;

        default:
            assert (0);
    }
}

static void
print_table_name (PFsql_t *n)
{
    assert (n);
    assert (n->kind == sql_tbl_name);

    PFprettyprintf ("%s", PFsql_table_str(n->sem.tablename.ident));
    /* the column-list has not to be specified in every case */ 
    if (L(n)) {
        PFprettyprintf ("(");
        print_clm_list (L(n));
        PFprettyprintf (")");
    }
}

static void
print_schema_name (PFsql_t *n)
{
    assert (n);
    assert (n->kind == sql_schm);

    PFprettyprintf ("%s", n->sem.schema.str);
}

static void
print_expr(PFsql_t *n)
{
    assert (n);

    switch( n->kind )
    {
        case sql_not:
        case sql_max:
            PFprettyprintf("%s (", ID[n->kind]);
            print_expr (L(n));
            PFprettyprintf(")");
            break;
            
        case sql_and:
        case sql_eq:
        case sql_gt:
        case sql_gteq:
        case sql_add:
        case sql_sub:
        case sql_mul:
        case sql_div:
            /* expression : '(' expression 'OP' expression ')' */
            PFprettyprintf("(");
            print_expr (L(n));
            PFprettyprintf(" %s ", ID[n->kind]);
            print_expr (R(n));
            PFprettyprintf(")");
            break;
        case sql_clmn_name:
            if (n->crrlname != PF_SQL_CORRELATION_UNBOUNDED) {
                PFprettyprintf("%s.",
                    PFsql_correlation_name_str (n->crrlname));
            }
            PFprettyprintf("%s",
                    PFsql_column_name_str (n->sem.column.ident));
            break;

        case sql_lit_int:
            PFprettyprintf("%i", n->sem.atom.val.i);
            break;

        case sql_lit_str:
            PFprettyprintf("'%s'", n->sem.atom.val.s);
            break;

        case sql_lit_dec:
            PFprettyprintf("%g", n->sem.atom.val.dec);
            break;

        case sql_cst:
            PFprettyprintf ("CAST (");
            print_statement (L(n));
            PFprettyprintf (" AS ");
            print_statement (R(n));
            PFprettyprintf (")");
            break;

	case sql_like:
	    PFprettyprintf ("(");
	    print_statement (L(n));
	    PFprettyprintf (" LIKE  '%%");
	    /* write the string without beginning and 
	       trailing ' */
	    assert (R(n)->kind == sql_lit_str);
	    PFprettyprintf ("%s", R(n)->sem.atom.val.s); 
	    PFprettyprintf ("%%')");
            break;

        default:
            PFoops( OOPS_FATAL, "expression screwed up (%u)",
                    n->kind );
    }
}
    
static void
print_select_list(PFsql_t *n)
{
    assert( n);
    
    switch( n->kind ) {
        case sql_slct_list:
            PFprettyprintf("%c", START_BLOCK);
            print_select_list( L(n) );
            PFprettyprintf(",%c %c", END_BLOCK, START_BLOCK);
            print_select_list( R(n) );
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
            (L(n) == NULL) && (R(n) == NULL) )
        return;

    print_statement(L(n));
    if(!(R(n)->kind == sql_list_terminator ))
        PFprettyprintf(",");
    PFprettyprintf(" ");
    print_part_expression(R(n));
}

static void
print_sort_key_expressions(PFsql_t *n)
{
    assert( n);
    
    switch( n->kind ) {
        case sql_srtky_expr:
            print_sort_key_expressions( L(n) );
            PFprettyprintf(", ");
            print_sort_key_expressions( R(n) );
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

    if( L(n) ) {
        print_statement(L(n));
    }
    if( R(n) ) {
        print_statement(R(n));
    }
}

static void print_case (PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_case:
            print_case(n->child[0]);
            print_case(n->child[1]);
            break;
        case sql_when:
            PFprettyprintf("WHEN ");
            print_expr (n->child[0]);
            PFprettyprintf(" THEN ");
            print_statement (n->child[1]);
            break;
        case sql_else:
            PFprettyprintf(" ELSE ");
            print_statement (n->child[0]);
            break;
        default:
            PFoops (OOPS_FATAL, "Sqlgen: Pathfinder doesn't support this"
                    " kind of node (%i) in case statement",
                    n->kind);
    }
}

static void
print_statement(PFsql_t *n)
{
    switch (n->kind) {
        case sql_case:
            PFprettyprintf("CASE ");
            print_case (n);
            PFprettyprintf(" END");
            break;
        case sql_asc:
            PFprettyprintf ("ASC");
            break;

        case sql_desc:
            PFprettyprintf ("DESC");
            break;

        case sql_order:
            print_statement (L(n));
            PFprettyprintf (" ");
            print_statement (R(n));
            break;

        case sql_sum:
        case sql_max:
        case sql_count:
            PFprettyprintf("%s (", ID[n->kind]);
            print_statement( L(n) );
            PFprettyprintf(")");
            break;

        case sql_eq:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (" = ");
            print_statement (R(n));
            PFprettyprintf (")");
            break;

        case sql_prttn:
            PFprettyprintf ("PARTITION BY ");
            print_part_expression(L(n));
            break;

        case sql_ordr_by:
            PFprettyprintf ("ORDER BY ");
            print_sort_key_expressions (L(n));
            break;

        case sql_cst:
            PFprettyprintf ("CAST(");
            print_statement (L(n));
            PFprettyprintf (" AS ");
            print_statement (R(n));
            PFprettyprintf (")");
            break;

        case sql_type:
            PFprettyprintf ("%s", PFsql_simple_type_str(n->sem.type.t));
            break;

        case sql_tb_name:
            print_schema_name (L(n));
            PFprettyprintf (".");
            print_table_name (R(n));
            break;

        case sql_star:
            PFprettyprintf("*");
            break;

        case sql_union:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (") UNION ALL (");
            print_statement (R(n));
            PFprettyprintf (")");
            break;

        case sql_diff:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (") EXCEPT ALL (");
            print_statement (R(n));
            PFprettyprintf (")");
            break;

        case sql_alias:
            if (L(n)->kind != sql_tbl_name ) {
                PFprettyprintf ("(");
            }
            print_statement (L(n));
            if (L(n)->kind != sql_tbl_name ){
                PFprettyprintf (")");
            } 
            PFprettyprintf(" ");
            break;

        case sql_dot:
            PFprettyprintf (".");
            print_statement (R(n));
            break;

        case sql_over:
            {   
                PFprettyprintf(" ROWNUMBER () OVER (%c", START_BLOCK );
                print_window_clause(R(n));
                PFprettyprintf("%c)", END_BLOCK);
            } break;
        case sql_bind:
            {
                print_table_name( L(n) );
                PFprettyprintf(" AS (");
                print_statement( R(n) );
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
                print_table_name( n );
            } break;
        case sql_clmn_assign:
            {
                print_statement( L(n));
                PFprettyprintf(" AS ");
                print_statement( R(n));
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
            //     print_select_list(L(n));
            //     if(R(n))
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
                print_statement(L(n));
                PFprettyprintf(" %s ", ID[n->kind]);
                print_statement(R(n));
                PFprettyprintf(")");
            } break;
        case sql_like:
            {
                PFprettyprintf("(");
                print_statement(L(n));
                PFprettyprintf(" LIKE  '%%");
                /* write the string without beginning and 
                   trailing ' */
                assert (R(n)->kind == sql_lit_str);
                PFprettyprintf("%s", R(n)->sem.atom.val.s); 
                PFprettyprintf("%%')");
            } break;
        case sql_gt:
            {
                PFprettyprintf("(");
                print_expr (L(n));
                PFprettyprintf(" > ");
                print_expr (R(n));
                PFprettyprintf(")");
            } break;
        case sql_gteq:
            {
                PFprettyprintf("(");
                print_expr (L(n));
                PFprettyprintf(" >= ");
                print_expr (R(n));
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
print_tablereference (FILE *f, PFsql_t* n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_alias:
            assert (R(n)->kind == sql_crrltn_name);
            
            print_tablereference (f, L(n), i);
            fprintf (f, " AS %s",
                     PFsql_correlation_name_str (
                         R(n)->sem.correlation.ident));
            break;

        case sql_tbl_name:
            /* prettyprint table name */
            PFprettyprintf("%c", START_BLOCK);
            print_table_name( n );
            PFprettyprintf("%c", END_BLOCK);
            pretty_dump (f, i);
            break;

        case sql_tb_name:
            /* prettyprint table name */
            PFprettyprintf("%c", START_BLOCK);
            print_schema_name( L(n) );
            PFprettyprintf(".");
            print_table_name( R(n) );
            PFprettyprintf("%c", END_BLOCK);
            pretty_dump (f, i);
            break;

        case sql_select:
        case sql_union:
        case sql_diff:
            /* print nested selection */
            print_fullselect(f, n , i);
            break;

        default:
            assert (0);
    }
}

static void
print_join (FILE *f, PFsql_t *n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_on:
            print_join (f, L(n), i);
            indent (f, i-3);
            fprintf (f, "ON ");

            /* prettyprint expression */
            PFprettyprintf ("%c", START_BLOCK);
            print_expr (R(n));
            PFprettyprintf ("%c", END_BLOCK);
            pretty_dump (f, i);
            break;
            
        case sql_innr_join:
            print_join (f, L(n), i);
            indent (f, i-6);
            fprintf (f, "INNER JOIN ");
            print_join (f, R(n), i-6+11);
            break;
            
        case sql_outr_join:
            print_join (f, L(n), i);
            indent (f, i-6);
            fprintf (f, "RIGHT OUTER JOIN ");
            print_join (f, R(n), i-6+17);
            break;

        case sql_alias:
        case sql_tb_name:
        case sql_tbl_name:
        case sql_select:
            print_tablereference (f, n, i);
            break;

        default:
            PFoops( OOPS_FATAL,
                    "SQL generation doesn't support this kind "
                    "of join attribute (%u)", n->kind);
    }
}

static void
print_from_list (FILE *f, PFsql_t *n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_frm_list:
            print_from_list (f, L(n), i);
            fprintf (f, ",");
            indent (f, i);
            print_from_list (f, R(n), i);
            break;
            
        case sql_alias:
        case sql_tb_name:
        case sql_tbl_name:
        case sql_select:
            print_tablereference (f, n, i);
            break;
            
        case sql_on:
            print_join (f, n, i);
            break;
            
        default:
            PFoops (OOPS_FATAL,
                    "This kind (%u) of statement is not supported by "
                    "fromlist", n->kind);
            break;
    }
}

static void
print_fullselect (FILE *f, PFsql_t *n, int i)
{
    assert (n);
    
    fputc ('(', f);
    i += 1;

    switch (n->kind) {
       case sql_union:
           print_fullselect (f, L(n), i);
           indent (f, i);
           fprintf (f, "UNION ALL");
           indent (f, i);
           print_fullselect (f, R(n), i);
           break;
            
        case sql_diff:
            print_fullselect (f, L(n), i);
            indent (f, i);
            fprintf (f, "EXCEPT ALL");
            indent (f, i);
            print_fullselect (f, R(n), i);
            break;

        case sql_select:
            /* SELECT */
            fprintf (f, "SELECT%s ", n->sem.select.distinct ? " DISTINCT" : "");
            
            /* prettyprint selection list */
            PFprettyprintf ("%c", START_BLOCK);
            print_select_list (L(n));
            PFprettyprintf ("%c", END_BLOCK);
            pretty_dump (f, i+7);

            /* FROM */
            indent (f, i);
            fprintf (f, "  FROM ");
            print_from_list (f, R(n), i+7);

            /* WHERE (optional) */
            if (n->child[2]) {
                indent (f, i);
                fprintf (f, " WHERE ");

                /* prettyprint where list */
                PFprettyprintf ("%c", START_BLOCK);
                print_expr (n->child[2]);
                PFprettyprintf ("%c", END_BLOCK);
                pretty_dump (f, i+7);
            }

            /* GROUP BY (optional) */
            if (n->child[3]) {
                indent (f, i);
                fprintf (f, " GROUP BY ");

                /* prettyprint group by list */
                PFprettyprintf ("%c", START_BLOCK);
                print_clm_list (n->child[3]);
                PFprettyprintf ("%c", END_BLOCK);
                pretty_dump (f, i+7);
            }
            break;

        default:
            PFoops (OOPS_FATAL,
                    "Fullselect doesnt support this kind of node (%u).",
                    n->kind);
            break;
    }

    fputc (')', f);
}

static void
print_common_table_expression (FILE *f, PFsql_t *n)
{
    assert (n);
    assert (n->kind == sql_bind);

    /* prettyprint the binding name */
    PFprettyprintf ("%c", START_BLOCK);
    print_table_name( L(n) );
    PFprettyprintf ("%c", END_BLOCK);
    pretty_dump (f, 2);
    
    fprintf (f, " AS");
    indent (f, 2);
    print_fullselect (f, R(n), 2);
}

static void
print_comment(FILE *f, PFsql_t *n)
{
    fprintf(f, "--%s", n->sem.comment);
}

/**
 * print the list of bindings. For each binding 
 * a new pretty printing phase is started.
 */
static void
print_binding (FILE* f, PFsql_t *n)
{
    PFsql_t *next_binding;

    /* collect all bindings */
    if (n->kind == sql_cmmn_tbl_expr) {
        print_binding (f, L(n));
        next_binding = R(n);
        fprintf(f, ", \n");
    }
    else
        next_binding = n;
        
    //assert (next_binding->kind == sql_bind);
    
    /* print a binding */
    switch (next_binding->kind) {
        case sql_comment:
        {
            print_comment(f, next_binding);
        } break;
        default:
            print_common_table_expression (f, next_binding);
            break;
    }

//    fputc ('\n', f);
}

/**
 * Dump SQL tree @a n in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param n root of SQL tree
 */
void
PFsql_pretty(FILE *f, PFsql_t *n)
{
    /* make sure that we have the sql root in our hands */
    assert (n);
    assert (n->kind == sql_seq);
    
    /* first print all schema information without pretty
       printing */
    print_schema_information (f, L(n));

    /* ... and then generate for each binding a new
       pretty printing phase */
    assert (R(n)->kind == sql_with);
    fprintf(f, "\nWITH\n");
    print_binding (f, L(R(n)));
}

/* vim:set shiftwidth=4 expandtab: */
