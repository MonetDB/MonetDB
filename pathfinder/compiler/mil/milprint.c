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

   statement        : 'var' Variable ':=' expression           <m_assgn>
                    | Variable ':=' expression                 <m_reassgn>
                    | <nothing>                                <m_nop>
                    | 'serialize (...)'                        <m_serialize>

   expression       : Variable                                 <m_var>
                    | literal                                  <m_lit_*, m_nil>
                    | 'new (' Type ',' Type ')'                <m_new>
                    | expression '.seqbase (' expression ')'   <m_seqbase>
                    | expression '.select (' expression ')'    <m_select>
                    | expression '.project (' expression ')'   <m_project>
                    | expression '.mark (' expression ')'      <m_mark>
                    | expression '.mark_grp (' expression ')'  <m_mark_grp>
                    | expression '.cross (' expression ')'     <m_cross>
                    | expression '.join (' expression ')'      <m_join>
                    | expression '.leftjoin (' expression ')'  <m_leftjoin>
                    | expression '.kunion (' expression ')'    <m_kunion>
                    | expression '.CTrefine (' expression ')'  <m_ctrefine>
                    | expression '.insert (' expression ')'    <m_binsert>
                    | expression '.kunique ()'                 <m_kunique>
                    | expression '.reverse ()'                 <m_reverse>
                    | expression '.mirror ()'                  <m_mirror>
                    | expression '.copy ()'                    <m_copy>
                    | expression '.sort ()'                    <m_sort>
                    | expression '.max ()'                     <m_max>
                    | expression '.count ()'                   <m_count>
                    | expression '.access (' restriction ')'   <m_access>
                    | expr '.insert (' expr ',' expr ')'       <m_insert>
                    | Type '(' expression ')'                  <m_cast>
                    | '[' Type '](' expression ')'             <m_mcast>
                    | '+(' expression ',' expression ')'       <m_add>
                    | '[+](' expression ',' expression ')'     <m_madd>
                    | '[-](' expression ',' expression ')'     <m_msub>
                    | '[*](' expression ',' expression ')'     <m_mmult>
                    | '[/](' expression ',' expression ')'     <m_mdiv>
                    | '[>](' expression ',' expression ')'     <m_mgt>
                    | '[=](' expression ',' expression ')'     <m_meq>
                    | '[not](' expression ')'                  <m_mnot>

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

static char *ID[] = {

      [m_new]      = "new"
    , [m_seqbase]  = "seqbase"
    , [m_order]    = "order"
    , [m_select]   = "select"
    , [m_insert]   = "insert"
    , [m_binsert]  = "insert"
    , [m_project]  = "project"
    , [m_mark]     = "mark"
    , [m_mark_grp] = "mark_grp"
    , [m_access]   = "access"
    , [m_cross]    = "cross"
    , [m_join]     = "join"
    , [m_leftjoin] = "leftjoin"
    , [m_reverse]  = "reverse"
    , [m_mirror]   = "mirror"
    , [m_copy]     = "copy"
    , [m_kunique]  = "kunique"
    , [m_kunion]   = "kunion"

    , [m_sort]     = "sort"
    , [m_ctrefine] = "CTrefine"

    , [m_add]      = "+"
    , [m_madd]     = "[+]"
    , [m_msub]     = "[-]"
    , [m_mmult]    = "[*]"
    , [m_mdiv]     = "[/]"
    , [m_mgt]      = "[>]"
    , [m_meq]      = "[=]"
    , [m_mnot]     = "[not]"

    , [m_max]      = "max"
    , [m_count]    = "count"

};

/** The string we print to */
static PFarray_t *out = NULL;

/* Wrapper to print stuff */
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

        /* statement : 'var' variable ':=' expression
         *           | variable ':=' expression           */
        case m_assgn:
            milprintf ("var ");
            /* fall through */
        case m_reassgn:
            print_variable (n->child[0]);
            milprintf (" := ");
            print_expression (n->child[1]);
            break;

        case m_order:
            print_expression (n->child[0]);
            milprintf (".%s", ID[n->kind]);
            break;

        /* `nop' nodes (`no operation') may be produced during compilation */
        case m_nop:
            break;

        case m_serialize:
            milprintf ("serialize (");
            for (unsigned int i = 0; i < 7; i++) {
                if (i)
                    milprintf (", ");
                print_expression (n->child[i]);
            }
            milprintf (")");
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
        /* expression : expression '.select (' expression ')' */
        case m_select:
        /* expression : expression '.project (' expression ')' */
        case m_project:
        /* expression : expression '.mark (' expression ')' */
        case m_mark:
        /* expression : expression '.mark_grp (' expression ')' */
        case m_mark_grp:
        /* expression : expression '.cross (' expression ')' */
        case m_cross:
        /* expression : expression '.join (' expression ')' */
        case m_join:
        /* expression : expression '.leftjoin (' expression ')' */
        case m_leftjoin:
        /* expression : expression '.CTrefine (' expression ')' */
        case m_ctrefine:
        /* expression : expression '.insert (' expression ')' */
        case m_binsert:
        /* expression : expression '.kunion (' expression ')' */
        case m_kunion:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : expression '.reverse' */
        case m_reverse:
        /* expression : expression '.mirror' */
        case m_mirror:
        /* expression : expression '.kunique' */
        case m_kunique:
        /* expression : expression '.copy' */
        case m_copy:
        /* expression : expression '.sort' */
        case m_sort:
        /* expression : expression '.max' */
        case m_max:
        /* expression : expression '.count' */
        case m_count:
            print_expression (n->child[0]);
            milprintf (".%s ()", ID[n->kind]);
            break;

        case m_access:
            print_expression (n->child[0]);
            switch (n->sem.access) {
                case BAT_READ:   milprintf (".access (BAT_READ)"); break;
                case BAT_APPEND: milprintf (".access (BAT_APPEND)"); break;
                case BAT_WRITE:  milprintf (".access (BAT_WRITE)"); break;
            }
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

        /* expression : '+(' expression ',' expression ')' */
        case m_add:
        /* expression : '[+](' expression ',' expression ')' */
        case m_madd:
        /* expression : '[-](' expression ',' expression ')' */
        case m_msub:
        /* expression : '[*](' expression ',' expression ')' */
        case m_mmult:
        /* expression : '[/](' expression ',' expression ')' */
        case m_mdiv:
        /* expression : '[>](' expression ',' expression ')' */
        case m_mgt:
        /* expression : '[=](' expression ',' expression ')' */
        case m_meq:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (", ");
            print_expression (n->child[1]);
            milprintf (")");
            break;

        /* expression : '[not](' expression ')' */
        case m_mnot:
            milprintf ("%s(", ID[n->kind]);
            print_expression (n->child[0]);
            milprintf (")");
            break;

        /* expression : literal */
        case m_lit_int:
        case m_lit_str:
        case m_lit_oid:
        case m_lit_dbl:
        case m_lit_bit:
        case m_nil:
            print_literal (n);
            break;

        case m_insert:
            print_expression (n->child[0]);
            milprintf (".%s (", ID[n->kind]);
            print_expression (n->child[1]);
            milprintf (", ");
            print_expression (n->child[2]);
            milprintf (")");
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

        /* literal : DblLiteral */
        case m_lit_dbl:
            milprintf ("dbl(%g)", n->sem.d);
            break;

        /* literal : BitLiteral */
        case m_lit_bit:
            milprintf (n->sem.b ? "true" : "false");
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
          [m_oid]   = "oid"
        , [m_void]  = "void"
        , [m_int]   = "int"
        , [m_str]   = "str"
        , [m_dbl]   = "dbl"
        , [m_bit]   = "bit"
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
