/**
 * @file
 *
 * Serialize and print SQL tree.
 * 
 * I this file is definitely under construction ;)
 *
 * @verbatim

 statements ::=   'WITH' commontable_expressions fullselect
 
 commontable_expressions ::=  commontable-expressions ','
                           commontable-expression

 commontable-expression  ::=  table-name'('column_list') AS ('
                           fullselect')'

 fullselect ::=   '('fullselect') UNION ('fullselect')'
              |   '('fullselect') EXCEPT ('fullselect')'
              |   subselect

 subselect  ::=   'SELECT' select-list 'FROM' from-list ['WHERE' where-list]

 from-list  ::=   table-reference from-list 

 table-reference  ::=   

 * $Id$
 */

/* always include pathfinder.h first! */ 
#include "pathfinder.h" 

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "mem.h"
#include "sql.h"
#include "sqlprint.h"


#include "pfstrings.h"
#include "oops.h"

static char *ID[] = {
      [sql_tbl_name] = "table_name",
      [sql_add]      = "+",
      [sql_sub]      = "-",
      [sql_mul]      = "*",
      [sql_div]      = "/"
};

/** The string we print to */
static PFarray_t *out = NULL;

/* Wrapper to print stuff */
static void sqlprintf(char*, ...)
    __attribute__ ((format (printf, 1, 2)));

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


/**
   * Convert the @a ident to a string.
    *
     * @param ident The identifier to convert.
      */
char*
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
print_sequence(PFsql_t *n)
{
   assert( n );
   assert( n->kind == sql_seq );

   print_schema_information(n->child[0]);
   sqlprintf("\n");
   print_statements(n->child[1]);
}

static void
print_schema_information( PFsql_t *n )
{
    assert( n );

    switch( n->kind ) {
        case sql_schm_inf:
            print_schema_information( n->child[0] );
            sqlprintf("\n");
            print_schema_information( n->child[1] );
            break;
        case sql_schm_cmmnt:
            sqlprintf("-- !! ");
            sqlprintf("%s", n->sem.comment );
            sqlprintf(" !!");
            break;
        case sql_schm_expr:
            sqlprintf("-- ");
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
    
    sqlprintf("-");
    switch( n->kind ) {
        case sql_clmn_name:
            sqlprintf("%s", sql_column_name_str( n->sem.column.ident ));
            sqlprintf(": ");
            sqlprintf("%s", PFsql_column_name_str( n->sem.column.ident ));
            break;
        case sql_tbl_name:
            sqlprintf("relation: ");
            sqlprintf("%s", PFsql_table_str( n->sem.tablename.ident ));
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
            sqlprintf("document");
            break;
        case sql_schm_res:
            sqlprintf("result");
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
            sqlprintf("WITH \n");
            print_common_table_expressions( n->child[0] );
            sqlprintf(" \n");
        } break;
       default:
        {
            /* TODO print string of node kind in the warning */
            print_statement( n );
            sqlprintf("\n");
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
           sqlprintf(", \n");
           print_common_table_expression( n->child[1] );
       } break;
       default:
       {
           print_common_table_expression( n );
       } break;
   }
}

static void
print_common_table_expression(PFsql_t *n)
{
    assert( n );
    assert( n->kind == sql_bind );

    print_table_name( n->child[0] );
    sqlprintf(" AS (");
    print_fullselect( n->child[1] );
    sqlprintf(")");
}

static void
print_table_name(PFsql_t *n)
{
    assert( n );
    assert( n->kind == sql_tbl_name );

    sqlprintf("%s", PFsql_table_str( n->sem.tablename.ident ));
    /* the column-list has not to be specified in every case */ 
    if( n->sem.tablename.clmn_list ) {
        sqlprintf("(");
        print_clm_list(n->sem.tablename.clmn_list);
        sqlprintf(")");
    }
}

static void
print_subselect(PFsql_t *n)
{
    assert( n );
    switch( n->kind ) {
        case sql_select:
        {
            sqlprintf("SELECT ");
            sqlprintf("%s", ( n->sem.select.distinct )?
                    "DISTINCT ":"");
            print_select_list( n->sem.select.select_list );
            sqlprintf(" FROM ");
            print_from_list( n->sem.select.from_list );
            /* where list is optional, we leave the where list
               to zero when its not specified */
            if( n->sem.select.where_list ) {
                sqlprintf(" WHERE ");
                print_expr( n->sem.select.where_list );
            }
            if( n->sem.select.grpby_list ) {
                sqlprintf(" GROUP BY ");
                print_clm_list( n->sem.select.grpby_list );
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
    
    if( !((n->kind == sql_frm_list) ||
            (n->kind == sql_list_terminator)) )
    {
        PFoops( OOPS_FATAL,
                "Pathfinder failed to print from list (%u)", n->kind );
    }
   
    if( (n->kind == sql_list_terminator) &&
            (n->child[0] == NULL) && (n->child[1] == NULL) )
        return;

    print_tablereference(n->child[0]);
    if(!(n->child[1]->kind == sql_list_terminator ))
        sqlprintf(", ");
    print_from_list(n->child[1]);
}

static void
print_tablereference(PFsql_t* n)
{
    assert( n );

    switch( n->kind ) {
        case sql_select:
        case sql_union:
        {
            sqlprintf("TABLE (");
            print_fullselect( n );
            sqlprintf(")");
        } break;
        case sql_alias:
        {
          print_tablereference( n->child[0] );
          sqlprintf(" ");
          print_correlation( n->child[1]);
        } break;
        case sql_tbl_name:
        {
            print_table_name( n );
        } break;
        case sql_tb_name:
        {
            print_schema_name( n->child[0] );
            sqlprintf(".");
            print_table_name( n->child[1] );
        } break;
        default:
        {
            PFoops( OOPS_FATAL, "Tablereference not supported"
                    " (%u)", n->kind);
        } break;
    }
}

static void
print_correlation( PFsql_t *n)
{
   assert( n );
   assert( n->kind == sql_crrltn_name );

   sqlprintf("%s",
       PFsql_correlation_name_str( n->sem.correlation.ident ));
}

static void
print_fullselect(PFsql_t *n)
{
    assert( n );
    switch( n->kind ) {
       case sql_union:
        {
            sqlprintf("(");
            print_fullselect(n->child[0]);
            sqlprintf(") UNION (");
            print_fullselect(n->child[1]);
            sqlprintf(")");
        } break;
        case sql_diff:
        {
            sqlprintf("(");
            print_fullselect(n->child[0]);
            sqlprintf(") EXCEPT (");
            print_fullselect(n->child[1]);
            sqlprintf(")");
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

    if( n->kind == sql_list_terminator )
        return;
    if( (n->child[1]) && (n->child[1]->kind == sql_list_terminator) ) {
        print_expr( n->child[0]);
        return;
    }
    switch( n->kind )
    {
        case sql_and:
        {
           sqlprintf("(");
           print_expr( n->child[0] );
           sqlprintf(" AND ");
           print_expr( n->child[1] );
           sqlprintf(")");
        } break;
        case sql_eq:
        {
          sqlprintf("(");
          print_expr( n->child[0] );
          sqlprintf(" = ");
          print_expr( n->child[1] );
          sqlprintf(")");
        } break;
        case sql_add:
        {
          sqlprintf("(");
          print_expr( n->child[0] );
          sqlprintf(" + ");
          print_expr( n->child[1] );
          sqlprintf(")");
        } break;
        case sql_sub:
        {
          sqlprintf("(");
          print_expr( n->child[0] );
          sqlprintf(" - ");
          print_expr( n->child[1] );
          sqlprintf(")");

        } break;
        case sql_gt:
        {
          sqlprintf("(");
          print_expr( n->child[0] );
          sqlprintf(" > ");
          print_expr( n->child[1] );
          sqlprintf(")");
        } break;
        case sql_gteq:
        {
          sqlprintf("(");
          print_expr( n->child[0] );
          sqlprintf(" >= ");
          print_expr( n->child[1] );
          sqlprintf(")");
        } break;
        case sql_clmn_name:
        {
            if( n->crrlname != PF_SQL_CORRELATION_UNBOUNDED ) {
                sqlprintf("%s",
                    PFsql_correlation_name_str(n->crrlname));
                sqlprintf(".");
            }
            sqlprintf("%s",
                    PFsql_column_name_str(n->sem.column.ident));
        } break;
        case sql_lit_int:
        {
            sqlprintf("%i", n->sem.atom.val.i);
        } break;
        case sql_lit_str:
        {
            sqlprintf("'%s'", n->sem.atom.val.s);
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
    
    if( !((n->kind == sql_slct_list) ||
            (n->kind == sql_list_terminator)) )
    {
        PFoops( OOPS_FATAL,
                "Pathfinder failed to print attribute list" );
    }
   
    if( (n->kind == sql_list_terminator) &&
            (n->child[0] == NULL) && (n->child[1] == NULL) )
        return;

    print_statement(n->child[0]);
    if(!(n->child[1]->kind == sql_list_terminator ))
        sqlprintf(", ");
    print_select_list(n->child[1]);
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
        sqlprintf(",");
    sqlprintf(" ");
    print_part_expression(n->child[1]);
}

static void
print_sort_key_expressions(PFsql_t *n)
{
    assert( n);
    
    if( !((n->kind == sql_srtky_expr) ||
            (n->kind == sql_list_terminator)) )
    {
        PFoops( OOPS_FATAL,
                "Pathfinder failed to print attribute list" );
    }
   
    if( (n->kind == sql_list_terminator) &&
            (n->child[0] == NULL) && (n->child[1] == NULL) )
        return;

    print_statement(n->child[0]);
    if(!(n->child[1]->kind == sql_list_terminator ))
        sqlprintf(", ");
    print_sort_key_expressions(n->child[1]);
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
            sqlprintf(", ");
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
        sqlprintf("%s", 
            PFsql_correlation_name_str(n->crrlname));
        sqlprintf(".");
    }
    sqlprintf("%s",
        PFsql_column_name_str(n->sem.column.ident));

}

static void
print_statement(PFsql_t *n)
{
    switch( n->kind ) {
        case sql_count:
        {
            sqlprintf("COUNT (");
            sqlprintf("%s", (n->sem.count.distinct)?"DISTINCT ":"");
            print_statement( n->child[0] );
            sqlprintf(")"); 
        } break;
        case sql_eq:
            sqlprintf("(");
            print_statement( n->child[0] );
            sqlprintf(" = ");
            print_statement( n->child[1] );
            sqlprintf(")");
            break;
        case sql_prttn:
            sqlprintf("PARTITION BY ");
            print_part_expression(n->child[0]);
            break;
        case sql_ordr_by:
            sqlprintf("ORDER BY ");
            print_sort_key_expressions(n->child[0]);
            break;
        case sql_cst:
            sqlprintf("CAST(");
            print_statement(n->child[0]);
            sqlprintf(" AS ");
            print_statement(n->child[1]);
            sqlprintf(")");
            break;
        case sql_type:
            sqlprintf("%s", PFsql_simple_type_str(n->sem.type.t));
            break;
        case sql_tb_name:
            print_schema_name( n->child[0] );
            sqlprintf(".");
            print_table_name( n->child[1] );
            break;
        case sql_star:
            sqlprintf("*");
            break;
        case sql_union:
            {
                sqlprintf("(");
                print_statement( n->child[0] );
                sqlprintf(") UNION (");
                print_statement( n->child[1] );
                sqlprintf(")");
            } break;
        case sql_diff:
            {
                sqlprintf("(");
                print_statement( n->child[0] );
                sqlprintf(") EXCEPT (");
                print_statement( n->child[1] );
                sqlprintf(")");
            } break;
        case sql_alias:
            {
                if(n->child[0]->kind != sql_tbl_name ) {
                    sqlprintf("(");
                }
                print_statement( n->child[0] );
                if(n->child[0]->kind != sql_tbl_name ){
                    sqlprintf(")");
                } 
                sqlprintf(" ");
            } break;
        case sql_dot:
        {
            sqlprintf(".");
            print_statement( n->child[1] );
        } break;
        case sql_over:
        {   
           print_aggrfunction(n->child[0]);
           sqlprintf(" OVER (");
           print_window_clause(n->child[1]);
           sqlprintf(")");
        } break;
        case sql_bind:
        {
            print_variable( n->child[0] );
            sqlprintf(" AS (");
            print_statement( n->child[1] );
            sqlprintf(")");
        } break;
        case sql_clmn_name:
        {
            if( n->crrlname != PF_SQL_CORRELATION_UNBOUNDED ) {
                sqlprintf("%s", 
                    PFsql_correlation_name_str(n->crrlname));
                sqlprintf(".");
            }
            sqlprintf("%s",
                    PFsql_column_name_str(n->sem.column.ident));
        } break;
        case sql_tbl_name:
        {
            print_variable( n );
        } break;
        case sql_clmn_assign:
        {
            print_statement( n->child[0]);
            sqlprintf(" AS ");
            print_statement( n->child[1]);
        } break;
        case sql_lit_int:
        {
            sqlprintf("%i", n->sem.atom.val.i);
        } break;
        case sql_lit_null:
        {
            sqlprintf("NULL");
        } break;
        case sql_lit_str:
        {
            sqlprintf("'%s'", n->sem.atom.val.s);
        } break;
        case sql_table_ref:
        {
            sqlprintf("%s", n->sem.table );
        } break;
        case sql_select:
        {
            sqlprintf("SELECT ");
            if( n->sem.select.distinct ) sqlprintf("DISTINCT ");
            print_select_list(n->sem.select.select_list);
            if(n->sem.select.from_list)
            {
                sqlprintf(" FROM ");
            }
        } break;
        /* expression : '(' expression '+' expression ')' */
        case sql_add:
        case sql_sub:
        case sql_mul:
        case sql_div:
        {
            sqlprintf("(");
            print_statement(n->child[0]);
            sqlprintf(" %s ", ID[n->kind]);
            print_statement(n->child[1]);
            sqlprintf(")");
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

    sqlprintf("%s", n->sem.schema.str);
}

static void
print_aggrfunction(PFsql_t *n)
{
    switch( n->kind ) {
        case sql_rownumber:
        {
            sqlprintf("ROWNUMBER()");
        } break;
        default:
        {
            PFoops( OOPS_FATAL,
                    "Pathfinder doesn't support this kind "
                    "of aggregat function");
        } break;
    }
}

#if 0
        gcc: defined but never used

static void
print_expression(PFsql_t *n)
{
    assert( n );
    switch( n->kind ) {
        /* expression : Variable */
        case sql_crrltn_name:
        {
            print_loc_variable( n );
        } break;
        case sql_tbl_name:
        {
            print_variable( n );
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
}
#endif

static void
print_variable( PFsql_t *n )
{
    assert( n );
    assert( n->kind == sql_tbl_name );

    sqlprintf("%s", PFsql_table_str( n->sem.tablename.ident ));
    if( n->sem.tablename.clmn_list ) {
        sqlprintf("(");
        print_clm_list(n->sem.tablename.clmn_list);
        sqlprintf(")");
    }
}

#if 0
       gcc: defined but never used

static void
print_loc_variable( PFsql_t *n )
{
    assert( n );
    assert( n->kind == sql_crrltn_name );
    
    sqlprintf("%s", PFsql_loc_var_str( n->sem.correlation.ident ));
}
#endif

static void
sqlprintf(char *fmt, ...)
{
    va_list args;

    assert ( out );

    /* print string */
    va_start( args, fmt );

    if ( PFarray_vprintf(out, fmt, args) == -1 )
        PFoops(OOPS_FATAL,
                "unable to print SQL output");
    
    va_end( args );
}

PFarray_t *
PFsql_serialize(PFsql_t* s)
{
    out = PFarray (sizeof(char));

    /* `statements' is the top rule of the grammar */
    print_sequence( s );

    return out;
}

void
PFsqlprint(FILE *stream, PFarray_t *sqlprog)
{
    char c;
    unsigned int pos;
    unsigned int spaces = 0;
    unsigned int indent = 0;

    for( pos = 0; (c = *((char*)PFarray_at(sqlprog, pos))) != '\0';
            pos++) {
        switch (c) {
            case '\n':
                {
                    fputc('\n', stream);
                    spaces = indent;
                } break;
            case ' ':
                {
                    spaces++;
                    break;
                } break;
            case '}':
                {
                    spaces = spaces > INDENT_WIDTH ?
                              spaces - INDENT_WIDTH : 0;
                    indent -= 2 * INDENT_WIDTH;
                }
            case '{':
                {
                    indent += INDENT_WIDTH;
                }
            default:
                {
                while (spaces > 0) {
                    spaces--;
                    fputc(' ', stream);
                }
                fputc(c, stream);
                } break;
        }
    }
}

