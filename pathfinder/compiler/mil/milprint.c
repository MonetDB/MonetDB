/**
 * @file
 *
 * Serialize MIL tree.
 *
 * Serialization is done with the help of a simplified MIL grammar:
 *
 * @verbatim
 
   statements       : statements statements                    <m_seq>
                    | statement ';'                            <otherwise>

   statement        : Variable ':=' expression                 <m_assgn>
                    | expr '.insert (' expr ',' expr ')'       <m_insert>
                    | expr '.insert (' expr ')'                <m_binsert>
                    | expr '.access (' restriction ')'         <m_access>
                    | <nothing>                                <m_nop>

   expression       : Variable                                 <m_var>
                    | literal                                  <m_lit_*, m_nil>
                    | 'new (' Type ',' Type ')'                <m_new>
                    | expression '.seqbase (' expression ')'   <m_seqbase>
                    | expression '.project (' expression ')'   <m_project>
                    | expression '.mark (' expression ')'      <m_mark>
                    | expression '.join (' expression ')'      <m_join>
                    | expression '.reverse'                    <m_reverse>
                    | expression '.copy'                       <m_copy>
                    | expression '.max'                        <m_max>
                    | Type '(' expression ')'                  <m_cast>
                    | '[' Type '](' expression ')'             <m_mcast>
                    | expression '+' expression                <m_plus>
                    | '[+](' expression ',' expression ')'     <m_mplus>

   literal          : IntegerLiteral                           <m_lit_int>
                    | StringLiteral                            <m_lit_str>
                    | OidLiteral                               <m_lit_oid>
                    | 'nil'                                    <m_nil>
@endverbatim
 *
 * Grammar rules are reflected by @c print_* functions in this file.
 * Depending on the current MIL tree node kind (see enum values
 * in brackets above), the corresponding sub-rule is chosen (i.e.
 * the corresponding sub-routine is called).
 *
 *
 * Copyright Notice:
 * -----------------
 *
 *  The contents of this file are subject to the MonetDB Public
 *  License Version 1.0 (the "License"); you may not use this file
 *  except in compliance with the License. You may obtain a copy of
 *  the License at http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 *
 *  Software distributed under the License is distributed on an "AS
 *  IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  rights and limitations under the License.
 *
 *  The Original Code is the ``Pathfinder'' system. The Initial
 *  Developer of the Original Code is the Database & Information
 *  Systems Group at the University of Konstanz, Germany. Portions
 *  created by U Konstanz are Copyright (C) 2000-2004 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */
#include <stdio.h>
#include <assert.h>

#include "pathfinder.h"
#include "milprint.h"

#include "oops.h"
#include "pfstrings.h"

/** The string we print to */
static PFarray_t *out = NULL;

/** Wrapper to print stuff */
static void milprintf (char *, ...)
    __attribute__ ((format (printf, 1, 2)));

/* forward declarations for left sides of grammar rules */
static void print_statements (PFmil_t *);
static void print_statement (PFmil_t *);
static void print_variable (PFmil_t *);
static void print_expression (PFmil_t *);
static void print_literal (PFmil_t *);
static void print_type (PFmil_t *);

#ifdef NDEBUG
#define debug_output
#else
/**
 * In our debug versions we want to have meaningful error messages when
 * generating MIL output failed. So we print the MIL script as far as
 * we already generated it.
 */
#define debug_output \
  PFinfo (OOPS_FATAL, "I encountered problems while generating MIL output."); \
  PFinfo (OOPS_FATAL, "This is possibly due to an illegal MIL tree, "         \
                      "not conforming to the grammar in milprint.c");         \
  PFinfo (OOPS_FATAL, "This is how far I was able to generate the script:");  \
  fprintf (stderr, "%s", (char *) out->base);
#endif

/**
 * Implementation of the grammar rules for `statements'.
 *
 * @param n MIL tree node
 */
static void
print_statements (PFmil_t * n)
{
    switch (n->kind) {

        /* statements : statements statements */
        case m_seq:
            print_statements (n->child[0]);
            print_statements (n->child[1]);
            break;

        /* statements : statement ';' */
        default:
            print_statement (n);
            milprintf (";\n");
            break;
    }
}

/**
 * Implementation of the grammar rules for `statement'.
 *
 * @param n MIL tree node
 */
static void
print_statement (PFmil_t * n)
{
    switch (n->kind) {

        /* statement : variable ':=' expression */
        case m_assgn:
            print_variable (n->child[0]);
            milprintf (" := ");
            print_expression (n->child[1]);
            break;

        case m_insert:
            print_expression (n->child[0]);
            milprintf (".insert (");
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
            break;

        case m_binsert:
            print_expression (n->child[0]);
            milprintf (".insert (");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        case m_access:
            print_expression (n->child[0]);
            switch (n->sem.access) {
                case BAT_READ:   milprintf (".access (BAT_READ)"); break;
                case BAT_APPEND: milprintf (".access (BAT_APPEND)"); break;
                case BAT_WRITE:  milprintf (".access (BAT_WRITE)"); break;
            }
            break;

        /* `nop' nodes (`no operation') may be produced during compilation */
        case m_nop:
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}

/**
 * Implementation of the grammar rules for `expression'.
 *
 * @param n MIL tree node
 */
static void
print_expression (PFmil_t * n)
{
    switch (n->kind) {

        /* expression : Variable */
        case m_var:
            print_variable (n);
            break;

        /* expression : 'new (' Type ',' Type ')' */
        case m_new:
            milprintf ("new (");
            print_type (n->child[0]);
            milprintf (", ");
            print_type (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.seqbase (' expression ')' */
        case m_seqbase:
            print_expression (n->child[0]);
            milprintf (".seqbase (");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.project (' expression ')' */
        case m_project:
            print_expression (n->child[0]);
            milprintf (".project (");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.mark (' expression ')' */
        case m_mark:
            print_expression (n->child[0]);
            milprintf (".mark (");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.join (' expression ')' */
        case m_join:
            print_expression (n->child[0]);
            milprintf (".join (");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.reverse' */
        case m_reverse:
            print_expression (n->child[0]);
            milprintf (".reverse");
            break;

        /* expression : expression '.copy' */
        case m_copy:
            print_expression (n->child[0]);
            milprintf (".copy");
            break;

        /* expression : expression '.max' */
        case m_max:
            print_expression (n->child[0]);
            milprintf (".max");
            break;

        /* expression : Type '(' expression ')' */
        case m_cast:
            print_type (n->child[0]);
            milprintf ("(");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : '[' Type '](' expression ')' */
        case m_mcast:
            milprintf ("[");
            print_type (n->child[0]);
            milprintf ("](");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '+' expression */
        case m_plus:
            milprintf ("(");
            print_expression (n->child[0]);
            milprintf ("+");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : '[+](' expression ',' expression ')' */
        case m_mplus:
            milprintf ("[+](");
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : literal */
        case m_lit_int:
        case m_lit_str:
        case m_lit_oid:
        case m_nil:
            print_literal (n);
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}


/**
 * Create MIL script output for variables.
 *
 * @param n MIL tree node of type #m_var.
 */
static void
print_variable (PFmil_t * n)
{
    assert (n->kind == m_var);

    milprintf ("%s", n->sem.ident);
}

/**
 * Implementation of the grammar rules for `literal'.
 *
 * @param n MIL tree node
 */
static void
print_literal (PFmil_t * n)
{
    switch (n->kind) {

        /* literal : IntegerLiteral */
        case m_lit_int:
            milprintf ("%i", n->sem.i);
            break;

        /* literal : StringLiteral */
        case m_lit_str:
            milprintf ("\"%s\"", PFesc_string (n->sem.s));
            break;

        /* literal : OidLiteral */
        case m_lit_oid:
            milprintf ("%u@0", n->sem.o);
            break;

        /* literal : 'nil' */
        case m_nil:
            milprintf ("nil");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree, literal expected. "
                                "MIL printer skrewed up.");
    }
}

static void
print_type (PFmil_t *n)
{
    char *types[] = {
          [m_oid]    "oid"
        , [m_void]   "void"
        , [m_int]    "int"
        , [m_str]    "str"
    };

    if (n->kind != m_type) {
        debug_output;     /* Print MIL code so far when in debug mode. */
        PFoops (OOPS_FATAL, "Illegal MIL tree, type expected. "
                            "MIL printer skrewed up.");
    }

    milprintf (types[n->sem.t]);
}

/**
 * output a single chunk of MIL code to the output character
 * array @a out. Uses @c printf style syntax.
 * @param fmt printf style format string, followed by an arbitrary
 *            number of arguments, according to format string
 */
static void
milprintf (char * fmt, ...)
{
    va_list args;

    assert (out);

    /* print string */
    va_start (args, fmt);

    if (PFarray_vprintf (out, fmt, args) == -1) 
        PFoops (OOPS_FATAL, "unable to print MIL output");

    va_end (args);
}

/**
 * Serialize the internal representation of a MIL program into a
 * string representation that can serve as an input to Monet.
 * 
 * @param m   The MIL tree to print
 * @return Dynamic (character) array holding the generated MIL script.
 */
PFarray_t *
PFmil_serialize (PFmil_t * m)
{
    out = PFarray (sizeof (char));

    /* `statements' is the top rule of our grammar */
    print_statements (m);

    return out;
}

/**
 * Print the generated MIL script in @a milprg to the output stream
 * @a stream, while indenting it nicely.
 *
 * Most characters of the MIL script will be output 1:1 (using fputc).
 * If we encounter a newline, we add spaces according to our current
 * indentation level. If we see curly braces, we increase or decrease
 * the indentation level. Spaces are not printed immediately, but
 * `buffered' (we only increment the counter @c spaces for that).
 * If we see an opening curly brace, we can `redo' some of these
 * spaces to make the opening curly brace be indented less than the
 * block it surrounds.
 *
 * @param stream The output stream to print to (usually @c stdout)
 * @param milprg The dynamic (character) array holding the MIL script.
 */
void
PFmilprint (FILE *stream, PFarray_t * milprg)
{
    char         c;              /* the current character  */
    unsigned int pos;            /* current position in input array */
    unsigned int spaces = 0;     /* spaces accumulated in our buffer */
    unsigned int indent = 0;     /* current indentation level */

    for (pos = 0; (c = *((char *) PFarray_at (milprg, pos))) != '\0'; pos++) {

        switch (c) {

            case '\n':                     /* print newline and spaces       */
                fputc ('\n', stream);      /* according to indentation level */
                spaces = indent;
                break;

            case ' ':                      /* buffer spaces                  */
                spaces++;
                break;

            case '}':                      /* `undo' some spaces when we see */
                                           /* an opening curly brace         */
                spaces = spaces > INDENT_WIDTH ? spaces - INDENT_WIDTH : 0;
                indent -= 2 * INDENT_WIDTH;
                /* Double indentation, as we will reduce indentation when
                 * we fall through next. */

            case '{':
                indent += INDENT_WIDTH;
                /* fall through */

            default:
                while (spaces > 0) {
                    spaces--;
                    fputc (' ', stream);
                }
                fputc (c, stream);
                break;
        }
    }
}

/* vim:set shiftwidth=4 expandtab: */
