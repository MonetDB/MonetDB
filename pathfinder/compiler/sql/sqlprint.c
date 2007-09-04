/**
 * @file
 * 
 * dump SQL language tree in human readable format 
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
#include <assert.h>

#include "sqlprint.h"
#include "oops.h"
#include "prettyp.h"

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
      
      [sql_count]    = "COUNT",
      [sql_max]      = "MAX",
      [sql_sum]      = "SUM",
      [sql_min]      = "MIN",
      [sql_avg]      = "AVG",
      
      [sql_and]      = "AND",
      [sql_or]       = "OR", 
};

/* forward declarations */
static void print_statement (PFsql_t *);
static void print_fullselect (FILE *, PFsql_t *, int);

static void 
print_schema_relcol (FILE *f, PFsql_t *n)
{
    assert (n);
    
    switch (n->kind) {
        case sql_column_name:
            fprintf (f, "%s", PFsql_column_name_str (n->sem.column.name));
            fprintf (f, ": ");
            fprintf (f, "%s", PFsql_column_name_str (n->sem.column.name));
            break;
            
        case sql_tbl_def:
        case sql_tbl_name:
            fprintf (f, "relation: ");
            fprintf (f, "%s", PFsql_table_str (n->sem.tbl.name));
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: schema relation; "
                    "Got: %u)", n->kind);
    }
}

static void
print_schema_table (FILE *f, PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_ser_doc:
            fprintf (f, "document");
            break;

        case sql_ser_res:
            fprintf (f, "result");
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: schema table; "
                    "Got: %u)", n->kind);
    }
}

static void
print_schema_information (FILE *f, PFsql_t *n)
{
    assert (n);

    /* stop at the end of the list */
    if (n->kind == sql_nil) return;

    assert (n->kind == sql_ser_info);

    /* translate left child */
    switch (L(n)->kind) {
        case sql_ser_comment:
            fprintf (f, "-- !! %s !!", L(n)->sem.comment.str);
            break;

        case sql_ser_mapping:
            fprintf (f, "-- ");
            print_schema_table (f, L(L(n)));
            fprintf (f, "-");
            print_schema_relcol (f, R(L(n)));
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: schema information; "
                    "Got: %u)", n->kind);
    }

    /* and then the rest of the list */
    fprintf (f, "\n");
    print_schema_information (f, R(n));
}

static void
print_column_name (PFsql_t *n)
{
    assert (n->kind == sql_column_name);
    
    if (n->sem.column.alias != PF_SQL_ALIAS_UNBOUND)
        PFprettyprintf (
            "%s.", 
            PFsql_alias_name_str (n->sem.column.alias));
    
    PFprettyprintf (
        "%s",
        PFsql_column_name_str (n->sem.column.name));
}

static void
print_column_name_list (PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_column_list:
            print_column_name_list (L(n));

            if (R(n)->kind != sql_nil) {
                PFprettyprintf (", ");
                print_column_name_list (R(n));
            }
            break;
            
        case sql_column_name:
            print_column_name (n);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: column name list; "
                    "Got: %u)", n->kind);
    }
}

static void
print_table_def (PFsql_t *n)
{
    assert (n);
    assert (n->kind == sql_tbl_def);

    PFprettyprintf ("%s (", PFsql_table_str (n->sem.tbl.name));
    print_column_name_list (L(n));
    PFprettyprintf (")");
}

static void
print_literal (PFsql_t * n)
{
    assert (n);

    switch (n->kind) {
        case sql_lit_int:
            PFprettyprintf ("%i", n->sem.atom.val.i);
            break;

        case sql_lit_str:
            PFprettyprintf ("'%s'", n->sem.atom.val.s);
            break;

        case sql_lit_dec:
            PFprettyprintf ("%g", n->sem.atom.val.dec);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: literal; "
                    "Got: %u)", n->kind);
    }
}

static void
print_literal_list (PFsql_t * n)
{
    assert (n);

    switch (n->kind) {
        case sql_lit_list:
            print_literal_list (L(n));

            if (R(n)->kind != sql_nil) {
                PFprettyprintf (", ");
                print_literal_list (R(n));
            }
            break;
            
        case sql_lit_int:
        case sql_lit_dec:
        case sql_lit_str:
            print_literal (n);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: literal list; "
                    "Got: %u)", n->kind);
    }
}

static void
print_condition (PFsql_t *n)
{
    assert (n);

    switch (n->kind)
    {
        case sql_not:
            PFprettyprintf ("NOT (");
            print_condition (L(n));
            PFprettyprintf (")");
            break;
            
        case sql_and:
        case sql_or:
            /* expression : '(' expression 'OP' expression ')' */
            PFprettyprintf ("(");
            print_condition (L(n));
            PFprettyprintf (" %s ", ID[n->kind]);
            print_condition (R(n));
            PFprettyprintf (")");
            break;
            
        case sql_eq:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (" = ");
            print_statement (R(n));
            PFprettyprintf (")");
            break;
            
        case sql_gt:
            /* switch arguments */
            PFprettyprintf ("(");
            print_statement (R(n));
            PFprettyprintf (" < ");
            print_statement (L(n));
            PFprettyprintf (")");
            break;

        case sql_gteq:
            /* switch arguments */
            PFprettyprintf ("(");
            print_statement (R(n));
            PFprettyprintf (" <= ");
            print_statement (L(n));
            PFprettyprintf (")");
            break;
            
    	case sql_like:
    	    if (R(n)->kind != sql_lit_str)
                PFoops (OOPS_FATAL, "LIKE only works with constant strings");
            
            PFprettyprintf ("(");
    	    print_statement (L(n));
    	    /* write the string without beginning and trailing ' */
    	    PFprettyprintf (" LIKE  '%%%s%%')", R(n)->sem.atom.val.s);
            break;
    
        case sql_in:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (" IN (");
            print_literal_list (R(n));
            PFprettyprintf ("))");
            break; 
            
        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: condition; "
                    "Got: %u)", n->kind);
    }
}
    
static void
print_sort_key_expressions (PFsql_t *n)
{
    assert( n);
    
    switch (n->kind) {
        case sql_sortkey:
            print_sort_key_expressions (L(n));
            if (n->sem.sortkey.dir_asc)
                PFprettyprintf (" ASC");
            else
                PFprettyprintf (" DESC");

            if (R(n)->kind != sql_nil) {
                PFprettyprintf (", ");
                print_sort_key_expressions (R(n));
            }
            break;

        default:
            print_statement (n);
            break;
    }
}

static void
print_window_clause (PFsql_t *n)
{
    assert (n);
    assert (n->kind == sql_wnd_clause);

    if (L(n)) {
        assert (L(n)->kind == sql_partition);

        PFprettyprintf ("PARTITION BY ");
        /* prettyprint partition by list */
        PFprettyprintf ("%c", START_BLOCK);
        print_column_name_list (L(L(n)));
        PFprettyprintf ("%c%s", END_BLOCK, R(n) ? " " : "");
    }
    if (R(n)) {
        assert (R(n)->kind == sql_order_by);
        PFprettyprintf ("ORDER BY ");
        print_sort_key_expressions (L(R(n)));
    }
}

static void
print_case (PFsql_t *n)
{
    assert (n);

    switch (n->kind) {
        case sql_case:
            print_case (L(n));
            print_case (R(n));
            break;
            
        case sql_when:
            PFprettyprintf ("WHEN ");
            print_condition (L(n));
            PFprettyprintf (" THEN ");
            print_statement (R(n));
            break;
            
        case sql_else:
            PFprettyprintf (" ELSE ");
            print_statement (L(n));
            break;
            
        case sql_nil:
            break;
            
        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: case statement; "
                    "Got: %u)", n->kind);
    }
}

static void
print_statement (PFsql_t *n)
{
    switch (n->kind) {
        case sql_coalesce:
            PFprettyprintf ("COALESCE (");
            print_statement (L(n));
            PFprettyprintf (", ");
            print_statement (R(n));
            PFprettyprintf(")");
            break;
            
        case sql_case:
            PFprettyprintf ("CASE ");
            print_case (n);
            PFprettyprintf (" END");
            break;

        case sql_sum:
        case sql_min:
        case sql_avg:
        case sql_max:
        case sql_count:
            PFprettyprintf("%s (", ID[n->kind]);
            print_statement (L(n));
            PFprettyprintf(")");
            break;

        case sql_cast:
            assert (R(n)->kind == sql_type);

            PFprettyprintf ("CAST(");
            print_statement (L(n));
            PFprettyprintf (" AS %s)",
                            PFsql_simple_type_str (R(n)->sem.type.t));
            break;

        case sql_schema_tbl_name:
            assert (L(n)->kind == sql_tbl_name);

            PFprettyprintf ("%s.%s",
                            n->sem.schema.str,
                            PFsql_table_str (L(n)->sem.tbl.name));
            break;

        case sql_star:
            PFprettyprintf ("*");
            break;

        case sql_over:
            assert (L(n)->kind == sql_rownumber);
            
            PFprettyprintf ("ROW_NUMBER () OVER (%c", START_BLOCK );
            print_window_clause (R(n));
            PFprettyprintf ("%c)", END_BLOCK);
            break;
    
        case sql_column_name:
            print_column_name (n);
            break;

        case sql_lit_int:
        case sql_lit_dec:
        case sql_lit_str:
            print_literal (n);
            break;

        case sql_lit_null:
            PFprettyprintf ("NULL");
            break;
            
        case sql_add:
        case sql_sub:
        case sql_mul:
        case sql_div:
            PFprettyprintf ("(");
            print_statement (L(n));
            PFprettyprintf (" %s ", ID[n->kind]);
            print_statement (R(n));
            PFprettyprintf (")");
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: statement; "
                    "Got: %u)", n->kind);
    }
}

static void
print_select_list (PFsql_t *n)
{
    assert (n);
    
    switch (n->kind) {
        case sql_select_list:
            PFprettyprintf("%c", START_BLOCK);
            print_select_list (L(n));
            
            if (R(n)->kind != sql_nil) {
                PFprettyprintf (",%c %c", END_BLOCK, START_BLOCK);
                print_select_list (R(n));
            }
            PFprettyprintf ("%c", END_BLOCK);
            break;

        case sql_column_assign:
            print_statement (L(n));
            PFprettyprintf (" AS ");
            print_column_name (R(n));
            break;

        default:
            print_statement (n);
            break;
    }
}

static void
print_conjunctive_list (FILE *f, PFsql_t *n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_and:
            if (R(n)->kind != sql_nil) {
                print_conjunctive_list (f, R(n), i);
                indent (f, i-4);
                fprintf (f, "AND ");
            }
            print_conjunctive_list (f, L(n), i);
            break;

        default:
            /* prettyprint a single expression */
            PFprettyprintf ("%c", START_BLOCK);
            print_condition (n);
            PFprettyprintf ("%c", END_BLOCK);
            pretty_dump (f, i);
            break;
    }
}

static void
print_tablereference (FILE *f, PFsql_t* n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_alias_bind:
            assert (R(n)->kind == sql_alias);
            
            if (L(n)->kind == sql_select || L(n)->kind == sql_union)
                /* print nested selection */
                print_fullselect (f, L(n), i);
            else
                print_tablereference (f, L(n), i);
            fprintf (f, " AS %s", PFsql_alias_name_str (R(n)->sem.alias.name));
            break;

        case sql_tbl_name:
            /* prettyprint table name */
            fprintf (f, "%s", PFsql_table_str (n->sem.tbl.name));
            break;

        case sql_schema_tbl_name:
            assert (L(n)->kind == sql_tbl_name);

            fprintf (f, "%s.%s", n->sem.schema.str,
                     PFsql_table_str(L(n)->sem.tbl.name));
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: table reference; "
                    "Got: %u)", n->kind);
    }
}

static void
print_join (FILE *f, PFsql_t *n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_outer_join:
            print_tablereference (f, L(n), i);
            indent (f, i-6);
            fprintf (f, "RIGHT OUTER JOIN ");
            print_tablereference (f, R(n), i-6+17);
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: join clause; "
                    "Got: %u)", n->kind);
    }
}

static void
print_from_list (FILE *f, PFsql_t *n, int i)
{
    assert (n);

    switch (n->kind) {
        case sql_from_list:
            if (R(n)->kind != sql_nil) {
                print_from_list (f, R(n), i);
                fprintf (f, ",");
                indent (f, i);
            }
            print_from_list (f, L(n), i);
            break;

        case sql_alias_bind:
        case sql_schema_tbl_name:
        case sql_tbl_name:
            print_tablereference (f, n, i);
            break;

        case sql_on:
            print_join (f, L(n), i);
            indent (f, i-3);
            fprintf (f, "ON ");
            print_conjunctive_list (f, R(n), i);
            break;
            
        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: from list; "
                    "Got: %u)", n->kind);
    }
}

static void
print_fullselect (FILE *f, PFsql_t *n, int i)
{
    assert (n);
    
    fputc ('(', f);
    i += 1;

    switch (n->kind) {
        case sql_select:
            /* SELECT */
            fprintf (f, "SELECT %s", n->sem.select.distinct ? "DISTINCT " : "");
            
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
                print_conjunctive_list (f, n->child[2], i+7);
            }

            /* GROUP BY (optional) */
            if (n->child[3]) {
                indent (f, i);
                fprintf (f, " GROUP BY ");

                /* prettyprint group by list */
                PFprettyprintf ("%c", START_BLOCK);
                print_column_name_list (n->child[3]);
                PFprettyprintf ("%c", END_BLOCK);
                pretty_dump (f, i+7);
            }
            break;

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

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: fullselect; "
                    "Got: %u)", n->kind);
    }

    fputc (')', f);
}

/**
 * print the list of bindings. For each binding 
 * a new pretty printing phase is started.
 */
static void
print_binding (FILE* f, PFsql_t *n)
{
    /* keep a stack to avoid printing 
       a comma after the last binding */
    static unsigned int nest = 0;

    /* collect all bindings */
    switch (n->kind) {
        case sql_cmmn_tbl_expr:
            nest++; /* increase nesting */
            print_binding (f, L(n));
            print_binding (f, R(n));
            nest--; /* decrease nesting */
            break;

        case sql_bind:
            /* prettyprint the binding name */
            PFprettyprintf ("%c", START_BLOCK);
            print_table_def (L(n));
            PFprettyprintf ("%c", END_BLOCK);
            pretty_dump (f, 2);
            
            fprintf (f, " AS");
            indent (f, 2);
            print_fullselect (f, R(n), 2);

            /* avoid printing a ',' after the last binding */
            if (nest > 2)
                fprintf (f, ",\n");
            fputc ('\n', f);
            break;

        case sql_comment:
            fprintf(f, "-- %s\n", n->sem.comment.str);
            break;

        case sql_nil:
            break;

        default:
            PFoops (OOPS_FATAL,
                    "SQL grammar conflict. (Expected: binding; "
                    "Got: %u)", n->kind);
    }
}

/**
 * Dump SQL tree @a n in pretty-printed form
 * into file @a f.
 *
 * @param f file to dump into
 * @param n root of SQL tree
 */
void
PFsql_print (FILE *f, PFsql_t *n)
{
    /* make sure that we have the sql root in our hands */
    assert (n);
    assert (n->kind == sql_root);
    
    /* first print all schema information */
    print_schema_information (f, L(n));

    /* ... and then print the query */
    assert (R(n)->kind == sql_with);
    fprintf(f, "\nWITH\n");
    print_binding (f, L(R(n)));
}

/* vim:set shiftwidth=4 expandtab: */
