/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Convert the internal representation of a MIL programm into a
 * string.
 *
 * MIL printing is done following a very simple grammar for MIL:
 *
 * @verbatim
 
   statements       : statements statements                    <m_comm_seq>
                    | 'if' condition '{' statements '}'        <m_ifthenelse>
                         [ 'else {' statements '}' ]
                    | Variable '@batloop'                      <m_batloop>
                    | statement ';'                            <otherwise>

   statement        : Variable ':=' expression                 <m_assgn>
                    | 'print (' expression ')'                 <m_print>
                    | Variable '.insert (' expression ')'      <m_insert>
                    | 'error (' expression ')'                 <m_error>

   expression       : Variable                                 <m_var>
                    | literal                                  <m_lit_*, m_nil>
                    | 'new (' type ',' type ')'                <m_new>
                    | expression '.seqbase (' expression ')'   <m_seqbase>
                    | expression '.fetch (' expression ')'     <m_fetch>
                    | '$t'                                     <m_tail>
                    | Type '(' expression ')'                  <m_cast>
                    | '[' Type '] (' expression ')'            <m_fcast>
                    | 'not (' expression ')'                   <m_not>
                    | 'isnil (' expression ')'                 <m_isnil>
                    | expression '.insert (' expression ')'    <m_insert>
                    | expression '+' expression                <m_plus>
                    | expression '=' expression                <m_equals>
                    | expression '||' expresssion              <m_or>
                    | 'count (' expression ')'                 <m_count>
                    | 'error (' expression ')'                 <m_error>
                    | 'ifthenelse (' expression ','
                                     expression ','
                                     expression ')'            <m_ifthenelse_>
                    | FunctionName '(' argument_list ')'       <m_apply>

   condition        : '(' expression ')'

   argument_list    : <empty>                                  <m_nil>
                    | expression remaining_args                <m_args>

   remaining_args   : <empty>                                  <m_nil>
                    | ',' expression remaining_args            <m_args>

   literal          : IntegerLiteral                           <m_lit_int>
                    | BooleanLiteral                           <m_lit_bit>
                    | StringLiteral                            <m_lit_str>
                    | DoubleLiteral                            <m_lit_dbl>
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

#include "pathfinder.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include "milprint.h"

#include "array.h"
#include "pfstrings.h"
#include "oops.h"

/* inserted for PFmalloc in temporary MILprint */
#include "mem.h"

/**
 * Denotes an illegal value for a PFmty_simpl_t. This is used in
 * conjunction with milprintf() to correctly start the first color
 * change and terminate the last one.
 */
#define ILLEGAL_TY 99

/** The string we print to */
static PFarray_t *out = NULL;

/* forward declarations for left sides of grammar rules */
static void milprintf (PFmty_t, char *, ...)
    __attribute__ ((format (printf, 2, 3)));
static void print_statements (PFmnode_t *);
static void print_statement (PFmnode_t *);
static void print_variable (PFmnode_t *);
static void print_expression (PFmnode_t *);
static void print_condition (PFmnode_t *);
static void print_argument_list (PFmnode_t *);
static void print_remaining_args (PFmnode_t *);
static void print_literal (PFmnode_t *);
static void print_type (PFmnode_t *);

/** map types to their type names */
static char * mty2str[] = {
      [mty_oid]   "oid"
    , [mty_void]  "void"
    , [mty_bit]   "bit"
    , [mty_int]   "int"
    , [mty_dbl]   "dbl"
    , [mty_str]   "str"
    , [mty_node]  "oid"
    , [mty_item]  "oid"
};

static char *cast[8][8] = {
/* oid   void  bit         int         str         dbl        node  item   */
 { NULL, NULL, NULL,       NULL,       NULL,       NULL,      NULL, NULL }
,{ NULL, NULL, NULL,       NULL,       NULL,       NULL,      NULL, NULL }
,{ NULL, NULL, "",         "bit_int",  "bit_str",  "bit_dbl", NULL, "bit_item" }
,{ NULL, NULL, "int_bit",  "",         "int_str",  "int_dbl", NULL, "int_item" }
,{ NULL, NULL, "str_bit",  "str_int",  "",         "str_dbl", NULL, "str_item" }
,{ NULL, NULL, "dbl_bit",  "dbl_int",  "dbl_str",  "",        NULL, "dbl_item" }
,{ NULL, NULL, NULL,       NULL,       NULL,       NULL,      "",   "node_item"}
,{ NULL, NULL, "item_bit", "item_int", "item_str", "item_dbl","item_node", ""  }
};

/** Strings to send to turn on a certain color, depending on output type */
static char* start_color[3][8] = {
    [output_xterm] {
                [mty_oid]     "\033[30m"    /* black */
              , [mty_void]    "\033[30m"    /* black */
              , [mty_bit]     "\033[33m"    /* yellow */
              , [mty_int]     "\033[34m"    /* blue */
              , [mty_str]     "\033[32m"    /* green */
              , [mty_dbl]     "\033[36m"    /* cyan */
              , [mty_node]    "\033[31m"    /* red */
              , [mty_item]    "\033[35m"    /* pink */
    }
  , [output_html] {
                [mty_oid]     "<font color='black'>"      /* black */
              , [mty_void]    "<font color='black'>"      /* black */
              , [mty_bit]     "<font color='yellow'>"     /* yellow */
              , [mty_int]     "<font color='blue'>"       /* blue */
              , [mty_str]     "<font color='lime'>"       /* green */
              , [mty_dbl]     "<font color='aqua'>"       /* cyan */
              , [mty_node]    "<font color='red'>"        /* red */
              , [mty_item]    "<font color='fuchsia'>"    /* pink */
    }
  , [output_monet] {
                [mty_oid]     ""   /* black */
              , [mty_void]    ""   /* black */
              , [mty_bit]     ""   /* yellow */
              , [mty_int]     ""   /* blue */
              , [mty_str]     ""   /* green */
              , [mty_dbl]     ""   /* cyan */
              , [mty_node]    ""   /* red */
              , [mty_item]    ""   /* pink */
    }
};

/** Strings to send to turn off a certain color, depending on output type */
static char* end_color[3][8] = {
    [output_xterm] {
                [mty_oid]     ""    /* black */
              , [mty_void]    ""    /* black */
              , [mty_bit]     ""    /* yellow */
              , [mty_int]     ""    /* blue */
              , [mty_str]     ""    /* green */
              , [mty_dbl]     ""    /* cyan */
              , [mty_node]    ""    /* red */
              , [mty_item]    ""    /* pink */
    }
  , [output_html] {
                [mty_oid]     "</font>"   /* black */
              , [mty_void]    "</font>"   /* black */
              , [mty_bit]     "</font>"   /* yellow */
              , [mty_int]     "</font>"   /* blue */
              , [mty_str]     "</font>"   /* green */
              , [mty_dbl]     "</font>"   /* cyan */
              , [mty_node]    "</font>"   /* red */
              , [mty_item]    "</font>"   /* pink */
    }
  , [output_monet] {
                [mty_oid]     ""   /* black */
              , [mty_void]    ""   /* black */
              , [mty_bit]     ""   /* yellow */
              , [mty_int]     ""   /* blue */
              , [mty_str]     ""   /* green */
              , [mty_dbl]     ""   /* cyan */
              , [mty_node]    ""   /* red */
              , [mty_item]    ""   /* pink */
    }
};

/** Strings to send to turn on printing in bold, depending on output type */
static char* start_boldness[3][2] = {
    [output_xterm] {
                [mty_simple]     ""
              , [mty_sequence]   "\033[1m"
    }
  , [output_html] {
                [mty_simple]     ""
              , [mty_sequence]   "<b>"
    }
  , [output_monet] {
                [mty_simple]     ""
              , [mty_sequence]   ""
    }
};

/** Strings to send to turn off printing in bold, depending on output type */
static char* end_boldness[3][2] = {
    [output_xterm] {
                [mty_simple]     ""
              , [mty_sequence]   "\033[m\017"
    }
  , [output_html] {
                [mty_simple]     ""
              , [mty_sequence]   "</b>"
    }
  , [output_monet] {
                [mty_simple]     ""
              , [mty_sequence]   ""
    }
};


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
  PFinfo (OOPS_FATAL, "This is how far I was able to generate the script:");\
  fprintf (stderr, "%s", (char *) out->base);
#endif


/**
 * Implementation of the grammar rules for `statements'.
 *
 * @param n MIL tree node
 */
static void
print_statements (PFmnode_t * n)
{
    switch (n->kind) {

        /* statements : 'if' condition statements 'else' statements */
        case m_ifthenelse:
            milprintf (n->mty, "if ");
            print_condition (n->child[0]);
            milprintf (n->mty, " {\n");
            print_statements (n->child[1]);
            if (n->child[2]->kind != m_nil) {
                milprintf (n->mty, "} else {\n");
                print_statements (n->child[2]);
            }
            milprintf (n->mty, "}\n");
            break;

        /* statements : statements statements */
        case m_comm_seq:
            print_statements (n->child[0]);
            print_statements (n->child[1]);
            break;

        /* statements : variable '@batloop {' statements '}' */
        case m_batloop:
            print_variable (n->child[0]);
            milprintf (n->mty, "@batloop {\n");
            print_statements (n->child[1]);
            milprintf (n->mty, "}\n");
            break;

        /* statements : statement ';' */
        default:
            print_statement (n);
            milprintf (n->mty, ";\n");
            break;
    }
}

/**
 * Implementation of the grammar rules for `statement'.
 *
 * @param n MIL tree node
 */
static void
print_statement (PFmnode_t * n)
{
    switch (n->kind) {

        /* statement : variable ':=' expression */
        case m_assgn:
            print_variable (n->child[0]);
            milprintf (n->mty, " := ");
            print_expression (n->child[1]);
            break;

        /* statement : 'print (' expression ')' */
        case m_print:
            milprintf (n->mty, "print (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* statement : variable '.insert (' expression ')' */
        case m_insert:
            print_variable (n->child[0]);
            milprintf (n->mty, ".insert (");
            print_expression (n->child[1]);
            milprintf (n->mty, ")");
            break;

        /* statement : 'error (' expression ')' */
        case m_error:
            milprintf (n->mty, "error (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}

/**
 * Create MIL script output for variables.
 *
 * To make variable names really unique, we use the local part of
 * the variable's name and append its memory address. (Actually, the
 * memory address itself is unique, but scripts are easier to debug
 * if we can find out the original variable name.)
 *
 * @param n MIL tree node
 */
static void
print_variable (PFmnode_t * n)
{
    if (n->kind != m_var) {
        debug_output;     /* Print MIL code so far when in debug mode. */
        PFoops (OOPS_FATAL, "Illegal MIL tree. Expected a variable "
                            "and got something different.");
    }

    milprintf (n->mty, "%s_%x", n->sem.var->qname.loc,
                                (size_t) n->sem.var);
}

/**
 * Implementation of the grammar rules for `statement'.
 *
 * @param n MIL tree node
 */
static void
print_expression (PFmnode_t * n)
{
    switch (n->kind) {

        /* expression : Variable */
        case m_var:
            print_variable (n);
            break;

        /* expression : literal */
        case m_lit_int:
        case m_lit_bit:
        case m_lit_str:
        case m_lit_dbl:
        case m_lit_oid:
        case m_nil:
            print_literal (n);
            break;

        /* expression : 'new (' type ',' type ')' */
        case m_new:
            milprintf (n->mty, "new (");
            print_type (n->child[0]);
            milprintf (n->mty, ", ");
            print_type (n->child[1]);
            milprintf (n->mty, ")");
            break;

        /* expression : expression '.seqbase (' expression ')' */
        case m_seqbase:
            print_expression (n->child[0]);
            milprintf (n->mty, ".seqbase (");
            print_expression (n->child[1]);
            milprintf (n->mty, ")");
            break;

        /* expression : expression '.fetch (' expression ')' */
        case m_fetch:
            print_expression (n->child[0]);
            milprintf (n->mty, ".fetch (");
            print_expression (n->child[1]);
            milprintf (n->mty, ")");
            break;

        /* expression : '$t' */
        case m_tail:
            milprintf (n->mty, "$t");
            break;

        /* expression : type '(' expression ')' */
        case m_cast:
            /* milprintf (n->mty, "%s (", mty2str[n->mty.ty]); */
            milprintf (n->mty, "%s (", cast[n->child[0]->mty.ty][n->mty.ty]);
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : '[' type '] (' expression ')' */
        case m_fcast:
            /* milprintf (n->mty, "[%s] (", mty2str[n->mty.ty]); */
            milprintf (n->mty, "[%s] (", cast[n->child[0]->mty.ty][n->mty.ty]);
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : 'not (' expression ')' */
        case m_not:
            milprintf (n->mty, "not (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : 'isnil (' expression ')' */
        case m_isnil:
            milprintf (n->mty, "isnil (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : expression '.insert (' expression ')' */
        case m_insert:
            print_expression (n->child[0]);
            milprintf (n->mty, ".insert (");
            print_expression (n->child[1]);
            milprintf (n->mty, ")");
            break;

        /* expression : expression '+' expression */
        case m_plus:
            print_expression (n->child[0]);
            milprintf (n->mty, " + ");
            print_expression (n->child[1]);
            break;

        /* expression : expression '=' expression */
        case m_equals:
            print_expression (n->child[0]);
            milprintf (n->mty, " = ");
            print_expression (n->child[1]);
            break;

        /* expression : expression '||' expression */
        case m_or:
            print_expression (n->child[0]);
            milprintf (n->mty, " || ");
            print_expression (n->child[1]);
            break;

        /* expression : 'count (' expression ')' */
        case m_count:
            milprintf (n->mty, "count (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : 'error (' expression ')' */
        case m_error:
            milprintf (n->mty, "error (");
            print_expression (n->child[0]);
            milprintf (n->mty, ")");
            break;

        /* expression : 'ifthenelse (' expression ','
         *                             expression ','
         *                             expression ')' */
        case m_ifthenelse_:
            milprintf (n->mty, "ifthenelse (");
            print_expression (n->child[0]);
            milprintf (n->mty, ", ");
            print_expression (n->child[1]);
            milprintf (n->mty, ", ");
            print_expression (n->child[2]);
            milprintf (n->mty, ")");
            break;

        /* expression : FunctionName '(' argument_list ')' */
        case m_apply:
            milprintf (n->mty, "%s (", PFqname_str (n->sem.fun->qname));
            print_argument_list (n->child[0]);
            milprintf (n->mty, ")");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}

/**
 * Implementation of the grammar rules for `condition'.
 *
 * @param n MIL tree node
 */
static void
print_condition (PFmnode_t * n)
{
    /* condition : '(' expression ')' */
    milprintf (n->mty, "(");
    print_expression (n);
    milprintf (n->mty, ")");
}

/**
 * Implementation of the grammar rules for `argument_list'
 *
 * @param n MIL tree node
 */
static void
print_argument_list (PFmnode_t * n)
{
    switch (n->kind) {

        case m_nil:
            break;

        case m_args:
            print_expression (n->child[0]);
            print_remaining_args (n->child[1]);
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}

/**
 * Implementation of grammar rules for `remaining_args'
 *
 * @param n MIL tree node
 */
static void
print_remaining_args (PFmnode_t * n)
{
    switch (n->kind) {

        case m_nil:
            break;

        case m_args:
            milprintf (n->mty, ", ");
            print_expression (n->child[0]);
            print_remaining_args (n->child[1]);
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree. MIL printer skrewed up.");
    }
}

/**
 * Implementation of the grammar rules for `literal'.
 *
 * @param n MIL tree node
 */
static void
print_literal (PFmnode_t * n)
{
    switch (n->kind) {

        /* literal : IntegerLiteral */
        case m_lit_int:
            milprintf (n->mty, "%i", n->sem.num);
            break;

        /* literal : BooleanLiteral */
        case m_lit_bit:
            milprintf (n->mty, n->sem.tru ? "true" : "false");
            break;

        /* literal : StringLiteral */
        case m_lit_str:
            milprintf (n->mty, "\"%s\"", PFesc_string (n->sem.str));
            break;

        /* literal : DoubleLiteral */
        case m_lit_dbl:
            milprintf (n->mty, "%g", n->sem.dbl);
            break;

        /* literal : OidLiteral */
        case m_lit_oid:
            milprintf (n->mty, "%i@0", n->sem.o);
            break;

        /* literal : 'nil' */
        case m_nil:
            milprintf (n->mty, "nil");
            break;

        default:
            debug_output;     /* Print MIL code so far when in debug mode. */
            PFoops (OOPS_FATAL, "Illegal MIL tree, literal expected. "
                                "MIL printer skrewed up.");
    }
}

/**
 * Create MIL script output for MIL types (usually in new() statements).
 *
 * @param n MIL tree node
 */
static void
print_type (PFmnode_t * n)
{
    if (n->kind != m_type) {
        debug_output;     /* Print MIL code so far when in debug mode. */
        PFoops (OOPS_FATAL, "Illegal MIL tree, MIL type specifier expected. "
                            "MIL printer skrewed up.");
    }

    milprintf (n->mty, "%s", mty2str[n->sem.mty.ty]);
}




/**
 * output a single chunk of MIL code to the output character
 * array @a out. Uses @c printf style syntax.
 * @param fmt printf style format string, followed by an arbitrary
 *            number of arguments, according to format string
 */
static void
milprintf (PFmty_t mty, char * fmt, ...)
{
    va_list args;
    static PFmty_t current_mty = { .ty = ILLEGAL_TY, .quant = 0 };

    assert (out);

    /* set color accordingly */
    if (!(current_mty.ty == mty.ty && current_mty.quant == mty.quant)) {
        if (current_mty.ty != ILLEGAL_TY) {
            if (PFarray_printf (out,
                        "%s%s",
                        end_boldness[PFstate.output_type][current_mty.quant],
                        end_color[PFstate.output_type][current_mty.ty]) == -1)
                PFoops (OOPS_FATAL, "unable to print MIL output");
        }
        if (mty.ty != ILLEGAL_TY) {
            if (PFarray_printf (out,
                        "%s%s",
                        start_color[PFstate.output_type][mty.ty],
                        start_boldness[PFstate.output_type][mty.quant]) == -1)
                PFoops (OOPS_FATAL, "unable to print MIL output");
        }
        current_mty = mty;
    }

    /* print actual string */
    va_start (args, fmt);

    if (PFarray_vprintf (out, fmt, args) == -1) 
        PFoops (OOPS_FATAL, "unable to print MIL output");

    va_end (args);
}


/**
 * Convert the internal representation of a MIL program into a
 * string representation that can serve as an input to Monet.
 * 
 * @param m   The MIL tree to print
 * @return Dynamic (character) array holding the generated MIL script.
 */
PFarray_t *
PFmil_gen (PFmnode_t * m)
{
    out = PFarray (sizeof (char));

    /* `statements' is the top rule of our grammar */
    print_statements (m);

    /* correctly reset all color changes */
    milprintf ((PFmty_t) { .ty = ILLEGAL_TY, .quant = 0}, "\n");

    /*
     * on Xterms we actually do not reset any color, but just set
     * new ones. Although this is certainly a hack, this command
     * resets the terminal color to black.
     */
    if (PFstate.output_type == output_xterm)
        PFarray_printf (out, "\033[30m");

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

/* ============================================================================ */
/* ====================                                        ================ */
/* ====================  added MIL Hack ouput (iter|pos|item)  ================ */
/* ====================                                        ================ */
/* ============================================================================ */

    /* saves the actual level (corresponding to the for nodes) */
    static unsigned int act_level = 0;
    /* 'counter' is the number of saved intermediate results */
    static unsigned int counter = 0;

/* different kind types */
#define NODE 'n'
#define ATTR 'a'
#define QNAME 'q'
#define BOOL 'b'
#define INT 'i'
#define DBL 'd'
#define DEC 'e'
#define STR 's'

    static void
    translate2MIL (PFcnode_t *c);

    static void
    init (void)
    {
        printf("# init ()\n");
        /* pathfinder functions (scj, doc handling) are made visible */
        /* modules already loaded by the Mserver */
        printf("# module(\"pathfinder\");\n");
        printf("# module(\"aggrX3\");\n");
        printf("# module(\"xtables\");\n");
        printf("# module(\"malalgebra\");\n");

        /* for debugging purposes "foo.xml" is loaded */
        printf("doc_to_working_set(\"foo.xml\");\n");
        printf("var TattrID_attr := Tattr_own.mirror;\n");
        printf("TattrID_attr.access(BAT_APPEND).seqbase(0@0);\n");
        printf("var TattrID_pre := Tattr_own.copy;\n");
        printf("TattrID_pre.access(BAT_APPEND).seqbase(0@0);\n");

        /* the first loop is initialized */
        printf("var loop000 := bat(void,oid).seqbase(0@0);\n");
        printf("loop000.insert(0@0, 1@0);\n");
        /* variable environment vars */
        printf("var vu_fid;\n");
        printf("var vu_vid;\n");
        printf("var inner000 := loop000;\n");
        printf("var outer000 := loop000;\n");
        printf("var v_vid000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_iter000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_pos000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_item000 := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        printf("var v_kind000 := bat(void,chr).access(BAT_APPEND).seqbase(0@0);\n");

        /* value containers */
        printf("var str_values := bat(void,str).seqbase(0@0).access(BAT_WRITE);\n");
        printf("str_values.reverse.key(true);\n");
        printf("var int_values := bat(void,int).seqbase(0@0).access(BAT_WRITE);\n");
        printf("int_values.reverse.key(true);\n");
        printf("var dbl_values := bat(void,dbl).seqbase(0@0).access(BAT_WRITE);\n");
        printf("dbl_values.reverse.key(true);\n");
        printf("var dec_values := bat(void,dbl).seqbase(0@0).access(BAT_WRITE);\n");
        printf("dec_values.reverse.key(true);\n");

        /* variable binding for loop-lifting of the empty sequence */
        printf("var empty_bat := bat(void,oid).seqbase(0@0);\n");
        printf("var empty_kind_bat := bat(void,chr).seqbase(0@0);\n");

        /* variables for (intermediate) results */
        printf("var iter;\n");
        printf("var pos;\n");
        printf("var item;\n");
        printf("var kind;\n");
        
        /* variable for empty scj */
        printf("var empty_res_bat := bat(oid,oid);\n");

        /* boolean mapping */
        printf("var bool_map := bat(bit,oid).insert(false,0@0).insert(true,1@0);\n");
        printf("var bool_not := bat(oid,oid).insert(0@0,1@0).insert(1@0,0@0);\n");

    }

    /**
     * the variables iter, pos, item, kind are used to
     * create an human readable output (iter|pos|item),
     * by converting the underlying value of item|kind
     * into a string
     */
    static void
    print_output (void)
    {
        printf("{ # print_output ()\n");
        /* the values of the different kinds are combined
           by inserting the converted bats into 'output_item' */
        printf("var output_item := bat(oid, str);\n");

        /* gets string values for string kind */ 
        printf("var temp1_str := kind.ord_select('%c');\n", STR);
        printf("temp1_str := temp1_str.mirror.leftfetchjoin(item);\n");
        printf("temp1_str := temp1_str.leftfetchjoin(str_values);\n");
        printf("output_item.insert(temp1_str);\n");
        printf("temp1_str := nil;\n");

        printf("var temp1_node := kind.ord_select('%c');\n", NODE);
        printf("var oid_pre := temp1_node.mirror.leftfetchjoin(item);\n");
        printf("temp1_node := [str](oid_pre);\n");
        printf("temp1_node := temp1_node.[+](\" (node) name: \");\n");
        printf("temp1_node := temp1_node.[+](oid_pre.leftfetchjoin(Tpre_prop).leftfetchjoin(Tprop_loc));\n");
        printf("temp1_node := temp1_node.[+](\"; size: \");\n");
        printf("temp1_node := temp1_node.[+](oid_pre.leftfetchjoin(Tpre_size));\n");
        printf("temp1_node := temp1_node.[+](\"; level: \");\n");
        printf("temp1_node := temp1_node.[+]([int](oid_pre.leftfetchjoin(Tpre_level)));\n");
        printf("oid_pre := nil;\n");
        printf("output_item.insert(temp1_node);\n");
        printf("temp1_node := nil;\n");

        printf("var temp1_attr := kind.ord_select('%c');\n", ATTR);
        printf("var oid_attrID := temp1_attr.mirror.leftfetchjoin(item);\n");
        printf("temp1_attr := [str](oid_attrID);\n");
        printf("temp1_attr := temp1_attr.[+](\" (attr) owned by: \");\n");
        printf("temp1_attr := temp1_attr.[+](oid_attrID.leftfetchjoin(TattrID_pre));\n");
        printf("temp1_attr := temp1_attr.[+](\"; \");\n");
        printf("var oid_attr := oid_attrID.leftfetchjoin(TattrID_attr);\n");
        printf("oid_attrID := nil;\n");
        printf("temp1_attr := temp1_attr.[+](oid_attr.leftfetchjoin(Tattr_loc));\n");
        printf("temp1_attr := temp1_attr.[+](\"='\");\n");
        printf("temp1_attr := temp1_attr.[+](oid_attr.leftfetchjoin(Tattr_val));\n");
        printf("temp1_attr := temp1_attr.[+](\"'\");\n");
        printf("oid_attr := nil;\n");
        printf("output_item.insert(temp1_attr);\n");
        printf("temp1_attr := nil;\n");

        printf("var temp1_qn := kind.ord_select('%c');\n", QNAME);
        printf("var oid_qnID := temp1_qn.mirror.leftfetchjoin(item);\n");
        printf("temp1_qn := [str](oid_qnID);\n");
        printf("temp1_qn := temp1_qn.[+](\" (qname) '\");\n");
        printf("temp1_qn := temp1_qn.[+](oid_qnID.leftfetchjoin(Tprop_ns));\n");
        printf("temp1_qn := temp1_qn.[+](\":\");\n");
        printf("temp1_qn := temp1_qn.[+](oid_qnID.leftfetchjoin(Tprop_loc));\n");
        printf("temp1_qn := temp1_qn.[+](\"'\");\n");
        printf("oid_qnID := nil;\n");
        printf("output_item.insert(temp1_qn);\n");
        printf("temp1_qn := nil;\n");

        printf("var bool_strings := bat(oid,str).insert(0@0,\"false\").insert(1@0,\"true\");\n");
        printf("var temp1_bool := kind.ord_select('%c');\n", BOOL);
        printf("temp1_bool := temp1_bool.mirror.leftfetchjoin(item);\n");
        printf("temp1_bool := temp1_bool.leftfetchjoin(bool_strings);\n");
        printf("bool_strings := nil;\n");
        printf("output_item.insert(temp1_bool);\n");
        printf("temp1_bool := nil;\n");

        printf("var temp1_int := kind.ord_select('%c');\n", INT);
        printf("temp1_int := temp1_int.mirror.leftfetchjoin(item);\n");
        printf("temp1_int := temp1_int.leftfetchjoin(int_values);\n");
        printf("temp1_int := [str](temp1_int);\n");
        printf("output_item.insert(temp1_int);\n");
        printf("temp1__int := nil;\n");

        printf("var temp1_dbl := kind.ord_select('%c');\n", DBL);
        printf("temp1_dbl := temp1_dbl.mirror.leftfetchjoin(item);\n");
        printf("temp1_dbl := temp1_dbl.leftfetchjoin(dbl_values);\n");
        printf("temp1_dbl := [str](temp1_dbl);\n");
        printf("output_item.insert(temp1_dbl);\n");
        printf("temp1_dbl := nil;\n");

        printf("var temp1_dec := kind.ord_select('%c');\n", DEC);
        printf("temp1_dec := temp1_dec.mirror.leftfetchjoin(item);\n");
        printf("temp1_dec := temp1_dec.leftfetchjoin(dec_values);\n");
        printf("temp1_dec := [str](temp1_dec);\n");
        printf("output_item.insert(temp1_dec);\n");
        printf("temp1_dec := nil;\n");

        /*
        printf("print (iter, pos, item, kind);\n");
        printf("print (output_item);\n");
        */
        printf("print(\"result\");\n");
        printf("print (iter, pos, output_item);\n");
        printf("output_item := nil;\n");

        printf("print(\"attributes\");\n");
        printf("if (TattrID_attr.count < 100) {\n");
        printf("        TattrID_attr.leftfetchjoin(Tattr_loc).print(TattrID_pre, TattrID_attr);\n");
        printf("} else {\n");
        printf("        TattrID_attr.count.print;\n");
        printf("}\n");
        printf("print(\"working set\");\n");
        printf("if (Tpre_size.count < 100) {\n");
        printf("        Tpre_prop.leftfetchjoin(Tprop_loc).print(Tpre_level.[int], Tpre_size);\n");
        printf("} else {\n");
        printf("        Tpre_size.count.print;\n");
        printf("}\n");
        printf("} # end of print_output ()\n");
    }

    static void
    translateEmpty (void)
    {
        printf("# translateEmpty ()\n");
        printf("iter := empty_bat;\n");
        printf("pos := empty_bat;\n");
        printf("item := empty_bat;\n");
        printf("kind := empty_kind_bat;\n");
    }

    static void
    cleanUpLevel (void)
    {
        printf("# cleanUpLevel ()\n");
        printf("inner%03u := nil;\n", act_level);
        printf("outer%03u := nil;\n", act_level);
        printf("loop%03u := nil;\n", act_level);

        printf("v_vid%03u := nil;\n", act_level);
        printf("v_iter%03u := nil;\n", act_level);
        printf("v_pos%03u := nil;\n", act_level);
        printf("v_item%03u := nil;\n", act_level);
        printf("v_kind%03u := nil;\n", act_level);
    }
                                                                                                                                                        
    /**
     * lookup a variable in the variable environment
     */
    static void
    translateVar (PFcnode_t *c)
    {
        printf("{ # translateVar (c)\n");
        printf("var vid := v_vid%03u.ord_uselect(%i@0);\n", act_level, c->sem.var->vid);
        printf("vid := vid.mark(0@0).reverse;\n");
        printf("iter := vid.leftfetchjoin(v_iter%03u);\n", act_level);
        printf("pos := vid.leftfetchjoin(v_pos%03u);\n", act_level);
        printf("item := vid.leftfetchjoin(v_item%03u);\n", act_level);
        printf("kind := vid.leftfetchjoin(v_kind%03u);\n", act_level);
        printf("vid := nil;\n");
        printf("} # end of translateVar (c)\n");
    }

    /**
     * saves a intermediate result
     */
    static int
    saveResult (void)
    {
        counter++;
        printf("{ # saveResult () : int\n");

        printf("iter%03u := iter;\n", counter);
        printf("pos%03u := pos;\n", counter);
        printf("item%03u := item;\n", counter);
        printf("kind%03u := kind;\n", counter);
        printf("iter := nil;\n");
        printf("pos := nil;\n");
        printf("item := nil;\n");
        printf("kind := nil;\n");

        printf("# saveResult () : int\n");
        return counter;
    }
    /**
     * gives back a intermediate result
     */
    static void
    deleteResult (void)
    {
        printf("# deleteResult ()\n");

        printf("iter%03u := nil;\n", counter);
        printf("pos%03u := nil;\n", counter);
        printf("item%03u := nil;\n", counter);
        printf("kind%03u := nil;\n", counter);

        printf("} # deleteResult ()\n");
        counter--;
    }

    static void
    translateSeq (int i)
    {
        printf("if (iter.count = 0) {\n");
        printf("        iter := iter%03u;\n", i);
        printf("        pos := pos%03u;\n", i);
        printf("        item := item%03u;\n", i);
        printf("        kind := kind%03u;\n", i);
        printf("} else if (iter%03u.count != 0)\n",i);
 //       /* translation follows exactly the description in the XQuery for SQL Hosts paper */
        printf("{ # translateSeq (counter)\n");
        printf("var merged_result := merged_union (iter%03u, iter, item%03u, item, kind%03u, kind);\n", i, i, i, i);
 //       printf("merged_result.print;\n");
 //       printf("print (merged_result.fetch(0), merged_result.fetch(1), merged_result.fetch(2), merged_result.fetch(3));\n");
        printf("iter := merged_result.fetch(0);\n");
        printf("item := merged_result.fetch(1);\n");
        printf("kind := merged_result.fetch(2);\n");
        printf("merged_result := nil;\n");
        printf("pos := iter.mark_grp(iter.reverse.project(1@0));\n");

 //       /* bind the second argument of the sequence to xxx2 and the first argument to xxx1 */
 //       printf("var iter2 := iter; var pos2 := pos; var item2 := item; var kind2 := kind;\n");
 //       printf("var iter1 := iter%03u; var pos1 := pos%03u; var item1 := item%03u; var kind1 := kind%03u;\n", i, i, i, i);
 //
 //       /* add the ord column to the tables and save in temp1 the end of the first table */
 //       printf("var ord1 := iter1.project(1@0); var ord2 := iter2.project(2@0); var temp1 := count(iter1); temp1 := oid(temp1);\n");
 //
 //       /* insertion of the second table in the first table (for all 5 columns) */
 //       printf("iter1 := iter1.reverse.mark(0@0).reverse; iter2 := iter2.reverse.mark(0@0).reverse;");
 //       printf("iter2 := iter2.seqbase(temp1); iter1.access(BAT_APPEND); iter1.insert(iter2); iter1.access(BAT_READ);\n");
 //
 //       printf("ord1 := ord1.reverse.mark(0@0).reverse; ord2 := ord2.reverse.mark(0@0).reverse;");
 //       printf("ord2 := ord2.seqbase(temp1); ord1.access(BAT_APPEND); ord1.insert(ord2); ord1.access(BAT_READ);\n");
 //
 //       printf("pos1 := pos1.reverse.mark(0@0).reverse; pos2 := pos2.reverse.mark(0@0).reverse;");
 //       printf("pos2 := pos2.seqbase(temp1); pos1.access(BAT_APPEND); pos1.insert(pos2); pos1.access(BAT_READ);\n");
 //
 //       printf("item1 := item1.reverse.mark(0@0).reverse; item2 := item2.reverse.mark(0@0).reverse;");
 //       printf("item2 := item2.seqbase(temp1); item1.access(BAT_APPEND); item1.insert(item2); item1.access(BAT_READ);\n");
 //
 //       printf("kind1 := kind1.reverse.mark(0@0).reverse; kind2 := kind2.reverse.mark(0@0).reverse;");
 //       printf("kind2 := kind2.seqbase(temp1); kind1.access(BAT_APPEND); kind1.insert(kind2); kind1.access(BAT_READ);\n");
 //
 //       /* create a sorting (sort by iter, ord, pos) for the table */
 //       printf("temp1 := iter1.reverse; temp1 := temp1.sort; temp1 := temp1.reverse;");
 //       printf("temp1 := temp1.CTrefine(ord1); temp1 := temp1.CTrefine(pos1);");
 //       printf("temp1 := temp1.mark(0@0); temp1 := temp1.reverse;\n");

 //       /* map sorting to every column */
 //       printf("iter := temp1.join(iter1);");
 //       printf("pos := temp1.mark(1@0);");
 //       printf("item := temp1.join(item1);");
 //       printf("kind := temp1.join(kind1);\n");
 //
 //       /* clean up the temporary variables */
 //       printf("iter1 := nil; pos1 := nil; item1 := nil; ord1 := nil; kind1 := nil;");
 //       printf("iter2 := nil; pos2 := nil; item2 := nil; ord2 := nil; kind2 := nil;");
 //       printf("temp1 := nil;\n");

        printf("} # end of translateSeq (counter)\n");
    }

    /**
     * create the variables for the next for-scope
     */
    static void
    project (void)
    {
        printf("# project ()\n");
        printf("var outer%03u := iter;\n", act_level);
        printf("iter := iter.mark(1@0);\n");
        printf("var inner%03u := iter;\n", act_level);
        printf("pos := iter.project(1@0);\n");
        printf("var loop%03u := inner%03u;\n", act_level, act_level);

        printf("var v_vid%03u;\n", act_level);
        printf("var v_iter%03u;\n", act_level);
        printf("var v_pos%03u;\n", act_level);
        printf("var v_item%03u;\n", act_level);
        printf("var v_kind%03u;\n", act_level);
    }

    /**
     * find the variables which are used in a deeper nesting 
     */
    static void
    getExpanded (int fid)
    {
        printf("{ # getExpanded (fid)\n");
        printf("var vu_nil := vu_fid.ord_uselect(%i@0);\n",fid);
        printf("var vid_vu := vu_vid.reverse;\n");
        printf("var oid_nil := vid_vu.leftjoin(vu_nil);\n");
        printf("vid_vu := nil;\n");
        printf("expOid := v_vid%03u.leftjoin(oid_nil);\n", act_level - 1);
        printf("oid_nil := nil;\n");
        printf("expOid := expOid.mirror;\n");
        printf("} # end of getExpanded (fid)\n");
    }

    /**
     * create the expanded (inner_outer |X| iter) version of
     * the mapping
     */
    static void
    expand (void)
    {
        printf("{ # expand ()\n");

        printf("var expOid_iter := expOid.leftfetchjoin(v_iter%03u);\n", act_level-1); 
                                               /* -1 is important */
        printf("var iter_expOid := expOid_iter.reverse;\n");
        printf("expOid_iter := nil;\n");
        printf("var oidMap_expOid := outer%03u.leftjoin(iter_expOid);\n", act_level);
        printf("iter_expOid := nil;\n");
        printf("var expOid_oidMap := oidMap_expOid.reverse;\n");
        printf("oidMap_expOid := nil;\n");
        printf("expOid_iter := expOid_oidMap.leftfetchjoin(inner%03u);\n", act_level);
        printf("expOid_oidMap := nil;\n");
        printf("v_iter%03u := expOid_iter;\n", act_level);
        printf("oidNew_expOid := expOid_iter.mark(0@0).reverse;\n");
        printf("expOid_iter := nil;\n");

        printf("} # end of expand ()\n");
    }

    /**
     * map the columns to the next scope
     */
    static void
    join (void)
    {
        printf("# join ()\n");
        printf("v_iter%03u := v_iter%03u.reverse.mark(0@0).reverse;\n", act_level, act_level);

        printf("var new_v_iter := v_iter%03u;\n", act_level);
        printf("v_iter%03u := bat(void,oid,count(new_v_iter)*2);\n", act_level);
        printf("v_iter%03u.seqbase(0@0);\n", act_level);
        printf("v_iter%03u.access(BAT_APPEND);\n", act_level);
        printf("v_iter%03u.insert(new_v_iter);\n", act_level);
        printf("new_v_iter := nil;\n");

        printf("var new_v_vid := oidNew_expOid.leftjoin(v_vid%03u);\n", act_level - 1);
        printf("v_vid%03u := bat(void,oid,count(new_v_vid)*2);\n", act_level);
        printf("v_vid%03u.seqbase(0@0);\n", act_level);
        printf("v_vid%03u.access(BAT_APPEND);\n", act_level);
        printf("v_vid%03u.insert(new_v_vid);\n", act_level);
        printf("new_v_vid := nil;\n");

        printf("var new_v_pos := oidNew_expOid.leftjoin(v_pos%03u);\n", act_level - 1);
        printf("v_pos%03u := bat(void,oid,count(new_v_pos)*2);\n", act_level);
        printf("v_pos%03u.seqbase(0@0);\n", act_level);
        printf("v_pos%03u.access(BAT_APPEND);\n", act_level);
        printf("v_pos%03u.insert(new_v_pos);\n", act_level);
        printf("new_v_pos := nil;\n");

        printf("var new_v_item := oidNew_expOid.leftjoin(v_item%03u);\n", act_level - 1);
        printf("v_item%03u := bat(void,oid,count(new_v_item)*2);\n", act_level);
        printf("v_item%03u.seqbase(0@0);\n", act_level);
        printf("v_item%03u.access(BAT_APPEND);\n", act_level);
        printf("v_item%03u.insert(new_v_item);\n", act_level);
        printf("new_v_item := nil;\n");

        printf("var new_v_kind := oidNew_expOid.leftjoin(v_kind%03u);\n", act_level - 1);
        printf("v_kind%03u := bat(void,chr,count(new_v_kind)*2);\n", act_level);
        printf("v_kind%03u.seqbase(0@0);\n", act_level);
        printf("v_kind%03u.access(BAT_APPEND);\n", act_level);
        printf("v_kind%03u.insert(new_v_kind);\n", act_level);
        printf("new_v_kind := nil;\n");

        /* should work now with leftjoin !?!
        printf("# sort inside join ()\n");
        printf("var temp1 := v_iter%03u.reverse;\n",act_level);
        printf("temp1 := temp1.sort;\n");
        printf("temp1 := temp1.reverse;\n");
        printf("temp1 := temp1.CTrefine(v_pos%03u);\n",act_level);
        printf("temp1 := temp1.mark(0@0);\n");
        printf("temp1 := temp1.reverse;\n");
        printf("v_vid%03u := temp1.join(v_vid%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_iter%03u := temp1.join(v_iter%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_pos%03u := temp1.join(v_pos%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_item%03u := temp1.join(v_item%03u).access(BAT_APPEND);\n",act_level,act_level);
        printf("v_kind%03u := temp1.join(v_kind%03u).access(BAT_APPEND);\n",act_level,act_level);
        */
        /*
        printf("print (\"testoutput in join() expanded to level %i\");\n",act_level);
        printf("print (v_vid%03u, v_iter%03u, v_pos%03u, v_kind%03u);\n",act_level,act_level,act_level,act_level);
        */
    }

    /**
     * map the result back to the next outer scope
     */
    static void
    mapBack (void)
    {
        printf("{ # mapBack ()\n");
        /* the iters are mapped back to the next outer scope */
        printf("var iter_oidMap := inner%03u.reverse;\n", act_level);
        printf("var oid_oidMap := iter.leftfetchjoin(iter_oidMap);\n");
        printf("iter_oidMap := nil;\n");
        printf("iter := oid_oidMap.leftfetchjoin(outer%03u);\n", act_level);
        printf("oid_oidMap := nil;\n");
        /* FIXME: instead of mark the partitioned mark should be used */
        /* printf("pos := pos.mark(1@0);\n"); */
        printf("pos := iter.mark_grp(iter.reverse.project(1@0));\n");
        printf("item := item;\n");
        printf("kind := kind;\n");
        printf("} # end of mapBack ()\n");
    }

    /**
      * if join is pruned the variables for the next scope have to be
      * initialized without content instead
      */
    static void
    createNewVarTable (void)
    {
        printf("# createNewVarTable ()\n");
        printf("v_iter%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_vid%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_pos%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_item%03u := bat(void,oid).seqbase(0@0).access(BAT_APPEND);\n", act_level);
        printf("v_kind%03u := bat(void,chr).seqbase(0@0).access(BAT_APPEND);\n", act_level);
    }

    /**
     * appends the information of a variable to the 
     * corresponding column of the variable environment
     */
    static void
    append (char *name, int level)
    {
        printf("{ # append (%s, level)\n", name);
        printf("var seqb := oid(v_%s%03u.count);\n",name, level);
        printf("var temp_%s := %s.reverse.mark(seqb).reverse;\n", name, name);
        printf("seqb := nil;\n");
        printf("v_%s%03u.insert(temp_%s);\n", name, level, name);
        printf("temp_%s := nil;\n", name);
        printf("} # append (%s, level)\n", name);
    }

    /**
     * inserts a variable binding into the variable environment
     * of the actual level (scope)
     */
    static void
    insertVar (int vid)
    {
        printf("{ # insertVar (vid)\n");
        printf("var vid := iter.project(%i@0);\n", vid);

        append ("vid", act_level);
        append ("iter", act_level);
        append ("pos", act_level);
        append ("item", act_level);
        append ("kind", act_level);
        printf("vid := nil;\n");

        /*
        printf("print (\"testoutput in insertVar(%i@0) expanded to level %i\");\n", vid, act_level);
        printf("print (v_vid%03u, v_iter%03u, v_pos%03u, v_kind%03u);\n",act_level,act_level,act_level,act_level);
        */
        printf("} # insertVar (vid)\n");
    }

    /**
     * loop-lifting of a Constant
     * - before calling a variable 'itemID' with an oid
     *   has to be bound
     * @param kind the kind of the item
     */
    static void
    translateConst (char kind)
    {
        printf("# translateConst (kind)\n");
        printf("iter := loop%03u;\n",act_level);
        printf("iter := iter.reverse.mark(0@0).reverse;\n");
        printf("pos := iter.project(1@0);\n");
        printf("item := iter.project(itemID);\n");
        printf("kind := iter.project('%c');\n", kind);
    }

    /**
     * iterative loop-lifted staircasejoin
     */
    static void
    loop_liftedSCJ (char *axis, char *kind, char *ns, char *loc)
    {
        /* iter|pos|item input contains only nodes (kind=NODE) */
        printf("# loop_liftedSCJ (axis, kind, ns, loc)\n");

        if (!strcmp (axis, "attribute"))
        {
                printf("{ # attribute axis\n");
                /* get all unique iter|item combinations */
                printf("var iter_item := iter.reverse.leftfetchjoin(item).unique;\n");
                /* if unique destroys the order a sort is needed */
                /* printf("iter_item := iter_item.sort;\n"); */

                printf("var iter_oid := iter_item.mark(0@0);\n");
                printf("var oid_item := iter_item.reverse.mark(0@0).reverse;\n");
                printf("iter_item := nil;\n");

                printf("var temp1 := TattrID_pre.reverse;\n");
                printf("var oid_attrID := oid_item.leftjoin(temp1);\n");
                printf("temp1 := nil;\n");

                /* kind test could be necessary if qnames are saved together
                   (see tagname test for other axis) */
                if (ns)
                {
                        printf("temp1 := oid_attrID.leftfetchjoin(TattrID_attr);\n");
                        printf("temp1 := temp1.leftfetchjoin(Tattr_ns);\n");
                        printf("temp1 := temp1.[=](\"%s\").ord_select(true);\n", ns);
                        printf("temp1 := temp1.mirror;\n");
                        printf("oid_attrID := temp1.leftjoin(oid_attrID);\n");
                        printf("temp1 := nil;\n");
                }
                if (loc)
                {
                        printf("temp1 := oid_attrID.leftfetchjoin(TattrID_attr);\n");
                        printf("temp1 := temp1.leftfetchjoin(Tattr_loc);\n");
                        printf("temp1 := temp1.[=](\"%s\").ord_select(true);\n", loc);
                        printf("temp1 := temp1.mirror;\n");
                        printf("oid_attrID := temp1.leftjoin(oid_attrID);\n");
                        printf("temp1 := nil;\n");
                }

                printf("iter_item := iter_oid.leftjoin(oid_attrID);\n");
                printf("res_scj := iter_item;\n");
                printf("iter_item := nil;\n");
                printf("temp1 := nil;\n");

                printf("} # end of attribute axis\n");
        }
        else
        {
                /* FIXME: in case iter is not sorted do it to be sure ?!? */
                /* FIXME: this should be resolved by pf:distinct-doc-order */
                /*
                printf("var sorting := iter.reverse.sort.reverse.mark(0@0).reverse;\n");
                printf("iter := sorting.join(iter);\n");
                printf("item := sorting.join(item);\n");
                printf("sorting := nil;\n");
                */
                if (kind)
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_kind_test_joined(iter,item,%s);\n", kind);
                }
                else if (loc)
                {
                        printf("var propID := Tprop_loc;\n");
                        printf("propID := propID.ord_uselect(\"%s\");\n", loc);
                        if (ns)
                        {
                        printf("propID := propID.mirror;\n");
                        printf("propID := propID.leftfetchjoin(Tprop_ns);\n");
                        printf("propID := propID.ord_uselect(\"%s\");\n", ns);
                        }
                        printf("if (propID.count != 0)\n");
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_tagname_test_joined(iter,item,propID);\n");
                        
                        printf("propID := nil;\n");
                }
                else if (ns)
                {
                        printf("var propID := Tprop_ns;\n");
                        printf("propID := propID.ord_uselect(\"%s\");\n", ns);
                        printf("if (propID.count != 0)\n");
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step", axis);
                        printf("_with_tagname_test_joined(iter,item,propID);\n");
                        
                        printf("propID := nil;\n");
                }
                else
                {
                        printf("res_scj := ");
                        printf("loop_lifted_%s_step_joined(iter,item);\n",axis);
                }

        //      /* creates the two output tables, which are joined the result of the scj */
        //      printf("pruned_input := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        //      printf("ctx_dn := bat(void,oid).access(BAT_APPEND).seqbase(0@0);\n");
        //                                                                                                                                              
        //      /* whis works only if tagnames are saved unique */
        //      /* tagname is tested before the step:
        //         - if name is not found, path is will not be evaluated
        //         - else: tagnames are already looked up and only a 
        //           node comparison (join) is needed as refinement step */
        //      printf("var propID := Tprop_loc;\n");
        //      if (loc)
        //      {
        //              printf("propID := propID.uselect(\"%s\");\n", loc);
        //      }
        //      if (ns)
        //      {
        //              printf("propID := propID.mirror;\n");
        //              printf("propID := propID.join(Tprop_ns);\n");
        //              printf("propID := propID.uselect(\"%s\");\n", ns);
        //      }
        //      if (loc || ns)
        //      {
        //              printf("if (propID.count != 0) { # node axis\n");
        //      }
        //      else
        //              printf("{ # node axis\n");
        //
        //      printf("var offset := 0;\n");
        //
        //      /* FIXME: in case iter is not sorted do it to be sure ?!? */
        //      printf("var sorting := iter.reverse.sort.reverse.mark(0@0).reverse;\n");
        //      printf("iter := sorting.join(iter);\n");
        //      printf("item := sorting.join(item);\n");
        //      printf("sorting := nil;\n");
        //
        //      /* create a bat containing the number of iter rows, with the
        //         iter as head value and the number of items (this iter has) in the tail */
        //      printf("var uniqueIter_count := {count}(iter.reverse, iter.tunique);\n");
        //      printf("uniqueIter_count := uniqueIter_count.[-](1);\n");
        //
        //      printf("var temp1_scj;\n");
        //      printf("var seqb;\n");
        //
        //      /* iterate over all iters */
        //      printf("uniqueIter_count@batloop ()\n");
        //      printf("{\n");
        //              /* get blocks of ctx-nodes per iter and sort them by pre value*/
        //              printf("temp1_scj := item.slice(offset, offset + int($t)).reverse.sort;\n");
        //              /* execute the SCJ */
        //              printf("temp1_scj := temp1_scj.%s;\n", axis);
        //
        //              /* add kind and tagname test inline */
        //              if (kind)
        //              {
        //                      printf("temp1_scj := temp1_scj.mirror.join(Tpre_kind);\n");
        //                      printf("temp1_scj := temp1_scj.[=](%s).select(true);\n", kind);
        //              }
        //              if (ns || loc)
        //              {
        //                      printf("temp1_scj := temp1_scj.mirror.join(Tpre_prop);\n");
        //                      printf("temp1_scj := temp1_scj.join(propID);\n");
        //                      printf("temp1_scj := temp1_scj.mirror.join(Tpre_kind);\n");
        //                      printf("temp1_scj := temp1_scj.[=](ELEMENT).select(true);\n");
        //              }
        //
        //              printf("seqb := count (ctx_dn);\n");
        //              printf("seqb := oid (seqb);\n");
        //              printf("temp1_scj := temp1_scj.mark(seqb).reverse;\n");
        //              printf("ctx_dn.insert(temp1_scj);\n");
        //
        //              printf("temp1_scj := temp1_scj.project($h);\n");
        //              printf("pruned_input.insert(temp1_scj);\n");
        //
        //              printf("offset := offset + $t + 1;\n");
        //      printf("}\n");
        //
        //      printf("propID := nil;\n");
        //      printf("offset := nil;\n");
        //      printf("temp1_scj := nil;\n");
        //      printf("seqb := nil;\n");
        //      printf("uniqueIter_count := nil;\n");
        //      printf("iter := nil;\n");
        //      printf("item := nil;\n");
        //
        //      printf("} # end of node axis\n");
        //
        //      printf("ctx_dn.access(BAT_READ);\n");
        //      printf("pruned_input.access(BAT_READ);\n");
        }
    }


    static void
    translateLocsteps (PFcnode_t *c)
    {
        char *ns, *loc;
        char *axis = (char *) PFmalloc (sizeof ("descendant_or_self"));

        printf("{ # translateLocsteps (c)\n");

        /* variable for the (iterative) scj */
        printf("var res_scj := empty_res_bat;");

        /* make this path step only for nodes */
        printf("var sel_ls := kind.ord_uselect('%c');\n", NODE);
        printf("sel_ls := sel_ls.mark(0@0).reverse;\n");
        printf("item := sel_ls.leftfetchjoin(item);\n");
        printf("iter := sel_ls.leftfetchjoin(iter);\n");
        printf("sel_ls := nil;\n");

        switch (c->kind)
        {
                case c_ancestor:
                    axis = "ancestor";
                    break;
                case c_ancestor_or_self:
                    axis = "ancestor_or_self";
                    break;
                case c_attribute:
                    axis = "attribute";
                    break;
                case c_child:
                    axis = "child";
                    break;
                case c_descendant:
                    axis = "descendant";
                    break;
                case c_descendant_or_self:
                    axis = "descendant_or_self";
                    break;
                case c_following:
                    axis = "following";
                    break;
                case c_following_sibling:
                    axis = "following_sibling";
                    break;
                case c_parent:
                    axis = "parent";
                    break;
                case c_preceding:
                    axis = "preceding";
                    break;
                case c_preceding_sibling:
                    axis = "preceding_sibling";
                    break;
                case c_self:
                    axis = "attribute";
                    break;
                default:
                    PFoops (OOPS_FATAL, "illegal XPath axis in MIL-translation");
        }

        switch (c->child[0]->kind)
        {
                case c_namet:
                    ns = c->child[0]->sem.qname.ns.uri;
                    loc = c->child[0]->sem.qname.loc;

                    /* translate wildcard '*' as 0 and missing ns as "" */
                    if (!ns)
                        ns = "";
                    else if (!strcmp (ns,"*"))
                        ns = 0;

                    /* translate wildcard '*' as 0 */
                    if (loc && (!strcmp(loc,"*")))
                        loc = 0;

                    loop_liftedSCJ (axis, 0, ns, loc); 
                    break;
                case c_kind_node:
                    loop_liftedSCJ (axis, 0, 0, 0);
                    break;
                case c_kind_comment:
                    loop_liftedSCJ (axis, "COMMENT", 0, 0);
                    break;
                case c_kind_text:
                    loop_liftedSCJ (axis, "TEXT", 0, 0);
                    break;
                case c_kind_pi:
                    loop_liftedSCJ (axis, "PI", 0, 0);
                    break;
                case c_kind_doc:
                    loop_liftedSCJ (axis, "DOCUMENT", 0, 0);
                    break;
                case c_kind_elem:
                    loop_liftedSCJ (axis, "ELEMENT", 0, 0);
                    break;
                case c_kind_attr:
                    loop_liftedSCJ (axis, "ATTRIBUTE", 0, 0);
                    break;
                default:
                    PFoops (OOPS_FATAL, "illegal node test in MIL-translation");
                    break;
        }

        /* res_scj = iter|item bat */
        printf("iter := res_scj.mark(0@0).reverse;\n");
        printf("pos := iter.mark_grp(iter.reverse.project(1@0));\n");
        printf("item := res_scj.reverse.mark(0@0).reverse;\n");
        if (!strcmp (axis, "attribute"))
                printf("kind := res_scj.mark(0@0).reverse.project('%c');\n", ATTR);
        else
                printf("kind := res_scj.mark(0@0).reverse.project('%c');\n", NODE);

        printf("res_scj := nil;\n");
        printf("} # end of translateLocsteps (c)\n");
    }

    /**
     * changes item and inserts if needed
     * the int values to 'int_values'
     */
    static void
    createEnumeration (void)
    {
        printf("{ # createEnumeration ()\n");
        /* the head of item has to be void with the seqbase 0@0 */
        printf("var ints_cE := item.mirror.[int];\n");
        /* the original version is replaced (see beneath) */
        printf("int_values.insert(ints_cE);\n");
        /* FIXME: it's not 100% sure, that order is not changed and so
                 mark could have a negative effect and switch values */
        printf("int_values := int_values.reverse.mark(0@0).reverse;\n");
        printf("int_values.seqbase(0@0).access(BAT_WRITE);\n");
        printf("int_values.reverse.key(true);\n");
        /* compares the integers and gives back the ones not in
           the int_values bat */
        /*
        printf("var new_ints := ints_cE.reverse.kdiff(int_values.reverse);\n");
        printf("seqb := oid(int_values.count);\n");
        printf("new_ints := new_ints.mark(seqb).reverse;\n");
        printf("seqb := nil;\n");
        */
        /* add the new integers to the int_values bat */
        /*
        printf("int_values.insert(new_ints);\n");
        printf("new_ints := nil;\n");
        */
        /* get the oids for the integers */
        printf("item := ints_cE.leftjoin(int_values.reverse);\n");
        printf("ints_cE := nil;\n");
        /* change kind information to int */
        printf("kind := kind.project('%c');\n", INT);
        printf("} # end of createEnumeration ()\n");
    }

    /**
     * loop-lifted conversion of the element name from a string
     * or a text node to a qname (can't handle namespaces) 
     */
    static void
    getQName (void)
    {
        printf("{ # getQName ()\n");

        /* FIXME: is it possible to have more than one kind */
        /* test if the name consists only of QNames */
        printf("var qnames := kind.ord_uselect('%c');\n", QNAME);
        printf("var all_s2q := kind.count;\n");
        printf("var qn_s2q := qnames.count;\n");


        printf("if (all_s2q != qn_s2q) {\n");

        printf("        var nodes := kind.ord_uselect('%c');\n", NODE);
        printf("        nodes := nodes.mirror.leftfetchjoin(item);\n");
        printf("        nodes := nodes.leftfetchjoin(Tpre_kind);\n");
        printf("        var textNodes := nodes.ord_uselect(TEXT);\n");
        printf("        var tN_s2q := textNodes.count;\n");
        printf("        if (nodes.count != tN_s2q)\n");
        printf("           ERROR (\"no nodes except textnodes allowed in Qname conversion\");\n");
        printf("        nodes := nil;\n");

        printf("        var strings_s2q := kind.ord_uselect('%c');\n", STR);
        printf("        var added := strings_s2q.count + qn_s2q + tN_s2q;\n");
        printf("        if (all_s2q != added) ");
        printf(                 "ERROR (\"only strings, text nodes and qnames can be");
        printf(                 " used as element names\");\n");
        printf("        all_s2q := nil;\n");
        printf("        tN_s2q := nil;\n");
        printf("        added := nil;\n");
        printf("        qn_s2q := nil;\n");
        /* end of kind testing */

        printf("        textNodes := textNodes.mirror.leftfetchjoin(item);\n");
        printf("        textNodes := textNodes.leftfetchjoin(Tpre_prop);\n");
        printf("        textNodes := textNodes.leftfetchjoin(Tprop_text);\n");

        printf("        var oid_oid := strings_s2q.mark(0@0).reverse;\n");
        printf("        strings_s2q := nil;\n");
        /* get all the unique strings */
        printf("        var oid_item := oid_oid.leftfetchjoin(item);\n");
        /* FIXME: is the copy next line really needed? */
        printf("        var oid_item_backup := oid_item.copy;\n");
        printf("        oid_item := oid_item.tunique.mark(0@0).reverse;\n");
        printf("        var oid_str := oid_item.leftfetchjoin(str_values);\n");
        printf("        oid_item := nil;\n");

        /* string name is only translated into local name, because
           no URIs for the namespace are available */
        printf("        var prop_name := Tprop_ns.ord_uselect(\"\");\n");

        printf("        prop_name := Tprop_loc.semijoin(prop_name);\n");

        /* find all strings which are not in the Tprop_loc bat */
        printf("        oid_str := oid_str.union(textNodes.tunique.reverse);\n");
        printf("        var str_oid := oid_str.reverse.kdiff(prop_name.reverse);\n");
        printf("        oid_str := nil;\n");
        printf("        prop_name := nil;\n");
        printf("        oid_str := str_oid.mark(oid(Tprop_loc.count)).reverse;\n");
        printf("        str_oid := nil;\n");
        /* add the strings as local part of the qname into the working set */
        printf("        Tprop_loc.insert(oid_str);\n");
        printf("        oid_str := oid_str.project(\"\");\n");
        printf("        Tprop_ns.insert(oid_str);\n");
        printf("        oid_str := nil;\n");

        /* get all the possible matching names from the updated working set */
        printf("        prop_name := Tprop_ns.ord_uselect(\"\");\n");
        printf("        prop_name := Tprop_loc.semijoin(prop_name);\n");

        printf("        oid_str := oid_oid.leftfetchjoin(oid_item_backup);\n");
        printf("        oid_oid := nil;\n");
        printf("        oid_str := oid_str.leftfetchjoin(str_values);\n");
        printf("        oid_item_backup := nil;\n");
        printf("        oid_str := merged_union(textNodes.reverse, oid_str.reverse).reverse;\n");
        /* get property ids for each string */
        printf("        var oid_prop := oid_str.leftjoin(prop_name.reverse);\n");
        printf("        oid_str := nil;\n");
        printf("        prop_name := nil;\n");
        /* oid_prop now contains the items with property ids
           which were before strings */
        /* FIXME: see fixme above: if only one kind is possible 
           this is overhead */
        printf("        if (qnames.count = 0)\n");
        printf("                item := oid_prop;\n");
        printf("        else {\n");
        /* qnames and newly generated qnames are merged (first 2 parameters are
           the oids for the sorting) */
        printf("                item := merged_union "
                                        "(qnames.reverse,"
                                        " oid_prop.reverse).reverse;\n");
        printf("                item := item.reverse.mark(0@0).reverse;\n");
        printf("                ");
        printf("        }\n");
        printf("        oid_prop := nil;\n");
        printf("        qnames := nil;\n");
        printf("}\n");

        printf("} # end of getQName ()\n");
    }

    /**
     * loop-lifted element construction 
     * @param i the counter of the actual saved result (elem name)
     */
    static void
    loop_liftedElemConstr (int i)
    {
        printf("{ # loop_liftedElemConstr (counter)\n"
               "var root_level;\n"
               "var root_size;\n"
               "var root_kind;\n"
               "var root_prop;\n");

        /* attr */ printf("var preNew_preOld;\n");
        /* attr */ printf("var attrIDs := kind.ord_uselect('%c');\n", ATTR);
        /* attr */ printf("attrIDs := attrIDs.mirror;\n");
        /* attr */ printf("attrIDs := attrIDs.leftfetchjoin(item);\n");
        /* attr */ printf("attrIDs := attrIDs.reverse;\n");
        /* attr */ printf("attrIDs := attrIDs.leftfetchjoin(iter);\n");
        /* attr */ printf("attrIDs := attrIDs.reverse;\n");

        /* in a first version only nodes are handled */
        printf("var nodes := kind.ord_uselect('%c');\n", NODE);
        /* if no nodes are found we jump right to the end and only
           have to execute the stuff for the root construction */
        printf("if (nodes.count != 0) {\n");
        
        printf("var oid_oid := nodes.mark(0@0).reverse;\n"
               "nodes := nil;\n"
               "var node_items := oid_oid.leftfetchjoin(item);\n"
               /* set iter to a distinct list and therefore don't
                  prune any node */
               "var iter_input := oid_oid.mirror;\n");

        /* get all subtree copies */
        printf("var res_scj := "
               "loop_lifted_descendant_or_self_step_unjoined"
               "(iter_input,node_items);\n");

        printf("iter_input := nil;\n");
        /* variables for the result of the scj */
        printf("var pruned_input := res_scj.fetch(0);\n"
               "var ctx_dn := res_scj.fetch(1);\n"
               "res_scj := nil;\n"
        /* res_ec is the iter|dn table resulting from the scj */
               "var res_ec := pruned_input.reverse.leftjoin(ctx_dn);\n");

        /* change the level of the subtree copies */
        /* get the level of the content root nodes */
        /* - unique is needed, if pruned_input has more than once an ctx value
           - join with iter between pruned_input and item is not needed, because
           in this case pruned_input has the void column as iter value */
        printf("nodes := pruned_input.leftfetchjoin(node_items).unique;\n"
               "var contentRoot_level := nodes.leftfetchjoin(Tpre_level);\n"
               "nodes := nil;\n"
               "var content_level := ctx_dn.leftfetchjoin(Tpre_level);\n"
               "content_level := content_level.[-](contentRoot_level);\n"
               "contentRoot_level := nil;\n"
               "content_level := content_level.[+](chr(1));\n"
        /* join is made after the multiplex, because the level has to be change only
           once for each dn-node. With the join the multiplex is automatically
           expanded */
               "content_level := pruned_input.reverse.leftjoin(content_level);\n");

        /* create subtree copies for the other bats */
        printf("var content_size := res_ec.leftfetchjoin(Tpre_size);\n"
               "var content_prop := res_ec.leftfetchjoin(Tpre_prop);\n"
               "var content_kind := res_ec.leftfetchjoin(Tpre_kind);\n");

        /* attr */ /* content_pre is needed for attribute subtree copies */
        /* attr */ printf("var content_pre := res_ec;\n");
        /* attr */ printf("content_pre := content_pre.reverse.mark(0@0).reverse;\n");
        /* attr */ printf("res_ec := nil;\n");

        /* create content_iter as sorting argument for the merged union */
        printf("var content_void := content_level.mark(0@0).reverse;\n"
               "var content_iter := content_void.leftfetchjoin(oid_oid).leftfetchjoin(iter);\n"
               "content_void := nil;");

        printf("content_level := content_level.reverse.mark(0@0).reverse;\n"
               "content_size := content_size.reverse.mark(0@0).reverse;\n"
               "content_prop := content_prop.reverse.mark(0@0).reverse;\n"
               "content_kind := content_kind.reverse.mark(0@0).reverse;\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"content\");\n");
        printf("print(content_iter, content_size, [int](content_level), ");
        printf("[int](content_kind), content_prop, content_pre);\n");
        */

        /* get the maximum level of the new constructed nodes
           and set the maximum of the working set */
        printf("{\n");
        printf("var height := content_level.max + 1;\n");
        printf("Theight := max(Theight, height);\n");
        printf("height := nil;\n");
        printf("}\n");

        /* calculate the sizes for the root nodes */
        printf("var contentRoot_size := node_items.leftfetchjoin(Tpre_size).[+](1);\n"
        /* following line does the same like the line above
               "contentRoot_size := {count}(pruned_input.reverse.leftjoin(ctx_dn),node_items);\n" */
               "var size_oid := contentRoot_size.reverse;\n"
               "contentRoot_size := nil;\n"
               "size_oid := size_oid.leftfetchjoin(oid_oid);\n"
               "oid_oid := nil;\n"
               "var size_iter := size_oid.leftfetchjoin(iter);\n"
               "size_oid := nil;\n"
               "var iter_size := size_iter.reverse;\n"
               "size_iter := nil;\n"
        /* sums up all the sizes into an size for each iter */
        /* every element must have a name, but elements don't need content.
           Therefore the second argument of the grouped sum has to be from the
           names result */
               "iter_size := {sum}(iter_size, iter%03u.tunique);\n", i);

        printf("root_level := iter_size.project(chr(0));\n"
               "root_size := iter_size;\n"
               "root_kind := iter_size.project(ELEMENT);\n"
               "root_prop := iter%03u.reverse.leftfetchjoin(item%03u);\n", i, i);

        printf("root_level := root_level.reverse.mark(0@0).reverse;\n"
               "root_size := root_size.reverse.mark(0@0).reverse;\n"
               "root_kind := root_kind.reverse.mark(0@0).reverse;\n"
               "root_prop := root_prop.reverse.mark(0@0).reverse;\n"
               "var root_iter := iter_size.mark(0@0).reverse;\n"
               "iter_size := nil;\n");

        /* attr */ /* root_pre is a dummy needed for merge union with content_pre */
        /* attr */ printf("var root_pre := root_iter.project(nil);\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"root\");\n");
        printf("print(root_iter, root_size, [int](root_level), [int](root_kind), root_prop);\n");
        */

        /* merge union root and nodes */
        printf("{\n");
        printf("var merged_result := merged_union ("
               "root_iter, content_iter, root_size, content_size, "
               "root_level, content_level, root_kind, content_kind, "
               "root_prop, content_prop, root_pre, content_pre);\n");
        printf("root_iter := nil;\n"
               "content_iter := nil;\n"
               "root_size := merged_result.fetch(1);\n"
               "content_size := nil;\n"
               "root_level := merged_result.fetch(2);\n"
               "content_level := nil;\n"
               "root_kind := merged_result.fetch(3);\n"
               "content_kind := nil;\n"
               "root_prop := merged_result.fetch(4);\n"
               "content_prop := nil;\n"
               "root_pre := merged_result.fetch(5);\n"
               "content_pre := nil;\n"
               "merged_result := nil;\n");
        /* printing output for debugging purposes */
        /* 
        printf("merged_result.print;\n");
        printf("print(\"merged (root & content)\");\n"
               "print(root_size, [int](root_level), [int](root_kind), root_prop);\n");
        */
        printf("}\n");
       
        
        /* attr */ /* preNew_preOld has in the tail old pre
           values merged with nil values */
        /* attr */ printf("preNew_preOld := root_pre;\n");
        /* attr */ printf("root_pre := nil;\n");

        printf("} else { # if (nodes.count != 0) ...\n");

        printf("root_level := item%03u.project(chr(0));\n", i);
        printf("root_size := item%03u.project(0);\n", i);
        printf("root_kind := item%03u.project(ELEMENT);\n", i);
        printf("root_prop := item%03u;\n", i);

        /* attr */ printf("preNew_preOld := item%03u;\n", i);
        /* attr */ printf("preNew_preOld := preNew_preOld.reverse.mark(0@0).reverse;\n");
        /* attr */ printf("preNew_preOld := preNew_preOld.project(nil);\n");

        printf("root_level := root_level.reverse.mark(0@0).reverse;\n"
               "root_size := root_size.reverse.mark(0@0).reverse;\n"
               "root_kind := root_kind.reverse.mark(0@0).reverse;\n"
               "root_prop := root_prop.reverse.mark(0@0).reverse;\n");

        printf("} # end of else in 'if (nodes.count != 0)'\n");

        /* set the offset for the new created trees */
        printf("{\n"
               "var seqb := count(Tpre_size);\n"
               "seqb := oid(seqb);\n"
               "root_level.seqbase(seqb);\n"
               "root_size.seqbase(seqb);\n"
               "root_kind.seqbase(seqb);\n"
               "root_prop.seqbase(seqb);\n"
               /* get the new pre values */
        /* attr */ "preNew_preOld.seqbase(seqb);\n"
               "}\n");
        /* insert the new trees into the working set */
        printf("Tpre_level.insert(root_level);\n"
               "Tpre_size.insert(root_size);\n"
               "Tpre_kind.insert(root_kind);\n"
               "Tpre_prop.insert(root_prop);\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"actual working set\");\n");
        printf("print(Tpre_size, [int](Tpre_level), [int](Tpre_kind), Tpre_prop);\n");
        */

        /* save the new roots for creation of the intermediate result */
        printf("var roots := root_level.ord_uselect(chr(0));\n");
        printf("roots := roots.mark(0@0).reverse;\n");

        /* resetting the temporary variables */
        printf("root_level := nil;\n"
               "root_size := nil;\n"
               "root_prop := nil;\n"
               "root_kind := nil;\n");

        /* adding the new constructed roots to the Tdoc_pre table, that a
           following (preceding) step can check the fragment boundaries */
        printf("{ # adding new fragments to the Tdoc_pre and Tdoc_name table\n"
               "var seqb := Tdoc_pre.count;\n"
               "seqb := oid(seqb);\n"
               "var new_pres := roots.reverse.mark(seqb).reverse;\n"
               "seqb := nil;\n"
               "Tdoc_pre.insert(new_pres);\n"
               /* Tdoc_name is also needed because, they should be aligned for
                  doc_to_working_set(str) */
               "Tdoc_name.insert(new_pres.project(str(nil)));\n"
               "new_pres := nil;\n"
               "}\n");

        /* return the root elements in iter|pos|item|kind representation */
        /* should contain for each iter exactly 1 root element
           unless there is a thinking error */
        printf("iter := iter%03u;\n", i);
        printf("pos := roots.mark(0@0);\n");
        printf("item := roots;\n");
        printf("kind := roots.project('%c');\n", NODE);

        /* attr translation */
        /* actual there are two variables bound:
           preNew_preOld - for mapping new attributes to new pre values
           attrIDs - the content root attributes
        */
        /* 1. step: add subtree copies of attributes */
        printf("{ # create attribute subtree copies\n");
        printf("var preOld_attrIDOld := TattrID_pre.reverse;\n");
        printf("var preNew_attrIDOld := preNew_preOld.leftjoin(preOld_attrIDOld);\n");
        printf("preNew_preOld := nil;\n");
        printf("preOld_attrIDOld := nil;\n");
        printf("var preNew_attr := preNew_attrIDOld.leftfetchjoin(TattrID_attr);\n");
        printf("preNew_attrIDOld := nil;\n");
        printf("var seqb := oid(TattrID_pre.count);\n");
        printf("var attrIDNew_preNew := preNew_attr.mark(seqb).reverse;\n");
        printf("var attrIDNew_attr := preNew_attr.reverse.mark(seqb).reverse;\n");
        printf("seqb := nil;\n");
        printf("preNew_attr := nil;\n");
        printf("TattrID_pre.insert(attrIDNew_preNew);\n");
        printf("attrIDNew_preNew := nil;\n");
        printf("TattrID_attr.insert(attrIDNew_attr);\n");
        printf("attrIDNew_attr := nil;\n");
        printf("} # end of create attribute subtree copies\n");
        /* 2. step: add attribute bindings of new root nodes */
        printf("{ # create attribute root entries\n");
        printf("var seqb := oid(TattrID_pre.count);\n");
        /* use iter & attrIDs + iter & newPre to create new attributes with 
           their corresponding pre values */
        printf("var iter_attr := attrIDs.leftfetchjoin(TattrID_attr);\n");
        printf("attrIDs := nil;\n");
        /* test uniqueness */
        printf("if (iter_attr.count != iter_attr.unique.count)\n"
               "{\n"
               "   if (item%03u.count > 0)\n"
               "      ERROR (\"attributes are not unique in element"
               " construction of '%%s' within each iter\",\n"
               "             item%03u.leftfetchjoin(Tprop_loc).fetch(0));\n"
               "   else\n"
               "     ERROR (\"attributes are not unique in element"
               " construction within each iter\");\n"
               "}\n", i, i);
        printf("var iter_oid := iter%03u.reverse;\n", i);
        printf("var attr_oid := iter_attr.reverse.leftjoin(iter_oid);\n");
        printf("iter_oid := nil;\n");
        printf("iter_attr := nil;\n");
        printf("var attr_pre := attr_oid.leftfetchjoin(roots);\n");
        printf("roots := nil;\n");
        /* add them to TattrID_attr and TattrID_pre */
        printf("var seqb := oid(TattrID_pre.count);\n");
        printf("var attrIDNew_pre := attr_pre.reverse.mark(seqb).reverse;\n");
        printf("var attrIDNew_attr := attr_pre.mark(seqb).reverse;\n");
        printf("seqb := nil;\n");
        printf("pre_attr := nil;\n");
        printf("TattrID_pre.insert(attrIDNew_pre);\n");
        printf("attrIDNew_pre := nil;\n");
        printf("TattrID_attr.insert(attrIDNew_attr);\n");
        printf("attrIDNew_attr := nil;\n");
        printf("} # end of create attribute root entries\n");

        /* printing output for debugging purposes */
        /*
        printf("print(\"Theight\"); Theight.print;\n");
        printf("print(\"Tdoc_pre\"); Tdoc_pre.print;\n");
        printf("print(\"Tdoc_name\"); Tdoc_name.print;\n");
        */

        printf("} # end of loop_liftedElemConstr (counter)\n");
    }
   

    /**
     * prints to stdout MIL-expressions for the c_apply core node
     * @param fnQname the name of the function
     * @args the head of the argument list
     */
    static void
    translateFunction (PFqname_t fnQname, PFcnode_t *args)
    {
        if (!PFqname_eq(fnQname,PFqname (PFns_fn,"doc")))
        {
                translate2MIL (args->child[0]);
                /* FIXME: doesn't work with actual attribute representation */
                /* FIXME: only works with strings - no error handling */
                printf("{ # translate fn:doc (string?) as document?\n");
                printf("var docs := item.tunique.reverse;\n");
                printf("docs := docs.leftfetchjoin(str_values);\n");
                printf("docs := docs.reverse.kdiff(Tdoc_name.reverse).reverse;\n");
                printf("docs@batloop () {\n");
                printf("doc_to_working_set($t);\n");
                printf("}\n");
                printf("docs := nil;\n");
                printf("item := item.leftfetchjoin(str_values);\n");
                printf("item := item.leftjoin(Tdoc_name.reverse);\n");
                printf("item := item.leftfetchjoin(Tdoc_pre);\n");
                printf("item := item.reverse.mark(0@0).reverse;\n");
                printf("kind := kind.project('%c');\n", NODE);
                printf("} # end of translate fn:doc (string?) as document?\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_pf,"distinct-doc-order")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate pf:distinct-doc-order (node*) as node*\n");
                printf("kind := kind.ord_uselect('%c');\n", NODE);
                printf("item := kind.mirror.leftfetchjoin(item);\n");
                printf("var item_iter := item.reverse.leftfetchjoin(iter);\n");
                printf("item_iter := item_iter.unique;\n");
                printf("iter := item_iter.reverse.mark(0@0).reverse;\n");
                printf("item := item_iter.mark(0@0).reverse;\n");
                printf("item_iter := nil;\n");
                printf("var sorting := iter.reverse.sort.reverse;\n");
                printf("sorting := sorting.CTrefine(item);");
                printf("sorting := sorting.mark(0@0).reverse;\n");
                printf("iter := sorting.leftfetchjoin(iter);\n");
                printf("pos := iter.mark(1@0);\n");
                printf("item := sorting.leftfetchjoin(item);\n");
                printf("kind := iter.project('%c');\n", NODE);
                printf("sorting := nil;\n");
                printf("} # end of translate pf:distinct-doc-order (node*) as node*\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"count")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate fn:count (item*) as integer\n");
                /* counts for all iters the number of items */
                /* uses the actual loop, to collect the iters, which are translated 
                   into empty sequences */
                printf("var iter_count := {count}(iter.reverse,loop%03u.reverse);\n", act_level);
                printf("iter := iter_count.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("int_values.insert(iter_count);\n");
                /* FIXME: it's not 100% sure, that order is not changed and so
                          mark could have a negative effect and switch values */
                printf("int_values := int_values.reverse.mark(0@0).reverse;\n");
                printf("int_values.seqbase(0@0).access(BAT_WRITE);\n");
                printf("int_values.reverse.key(true);\n");
                /* get the oids for the integers */
                printf("item := iter_count.leftjoin(int_values.reverse);\n");
                printf("item := item.reverse.mark(0@0).reverse;\n"); 
                printf("kind := iter.project('%c');\n", INT);
                printf("iter_count := nil;\n");
                printf("} # end of translate fn:count (item*) as integer\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"empty")))
        {
                translate2MIL (args->child[0]);
                printf("{ # translate fn:empty (item*) as boolean\n");
                printf("var iter_count := {count}(iter.reverse,loop%03u.reverse);\n", act_level);
                printf("var iter_bool := iter_count.[=](0);\n");
                printf("iter_count := nil;\n");
                printf("iter_bool := iter_bool.leftjoin(bool_map);\n");
                printf("iter := iter_bool.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("item := iter_bool.reverse.mark(0@0).reverse;\n");
                printf("kind := iter.project('%c');\n", BOOL);
                printf("iter_bool := nil;\n");
                printf("} # end of translate fn:empty (item*) as boolean\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"not")))
        {
                translate2MIL (args->child[0]);
                printf("# translate fn:not (boolean) as boolean\n");
                printf("item := item.leftfetchjoin(bool_not);\n");
        }
        else if (!PFqname_eq(fnQname,PFqname (PFns_fn,"boolean")))
        {
                translate2MIL (args->child[0]);
                
                printf("{ # translate fn:boolean (item*) as boolean\n");
                printf("iter := iter.reverse;\n");
                printf("var iter_count := {count}(iter,loop%03u.reverse);\n", act_level);
                printf("var test := iter_count.[=](1).ord_uselect(true);\n");
                printf("var trues := iter_count.[!=](0);\n");
                printf("trues.access(BAT_WRITE);\n");
                printf("iter_count := nil;\n");
                printf("item := iter.leftfetchjoin(item);\n");
                printf("kind := iter.leftfetchjoin(kind);\n");
                printf("test := test.mirror;\n");
                printf("test := test.leftjoin(kind);\n");
                printf("var str_test := test.ord_uselect('%c');\n", STR);
                printf("var int_test := test.ord_uselect('%c');\n", INT);
                printf("var dbl_test := test.ord_uselect('%c');\n", DBL);
                printf("var dec_test := test.ord_uselect('%c');\n", DEC);
                printf("var bool_test := test.ord_uselect('%c');\n", BOOL);
                printf("test := nil;\n");
                printf("str_test := str_test.mirror;\n");
                printf("int_test := int_test.mirror;\n");
                printf("dbl_test := dbl_test.mirror;\n");
                printf("dec_test := dec_test.mirror;\n");
                printf("bool_test := bool_test.mirror;\n");
                printf("str_test := str_test.leftjoin(item);\n");
                printf("int_test := int_test.leftjoin(item);\n");
                printf("dec_test := dec_test.leftjoin(item);\n");
                printf("dbl_test := dbl_test.leftjoin(item);\n");
                printf("bool_test := bool_test.leftjoin(item);\n");
                printf("str_test := str_test.leftfetchjoin(str_values);\n");
                printf("int_test := int_test.leftfetchjoin(int_values);\n");
                printf("dec_test := dec_test.leftfetchjoin(dec_values);\n");
                printf("dbl_test := dbl_test.leftfetchjoin(dbl_values);\n");
                printf("bool_test := bool_test.ord_uselect(0@0);\n");
                printf("str_test := str_test.ord_uselect(\"\");\n");
                printf("int_test := int_test.ord_uselect(0);\n");
                printf("dec_test := dec_test.ord_uselect(dbl(0));\n");
                printf("dbl_test := dbl_test.ord_uselect(dbl(0));\n");
                printf("str_test := str_test.project(false);\n");
                printf("int_test := int_test.project(false);\n");
                printf("dec_test := dec_test.project(false);\n");
                printf("dbl_test := dbl_test.project(false);\n");
                printf("bool_test := bool_test.project(false);\n");
                printf("trues.replace(str_test);\n");
                printf("str_test := nil;\n");
                printf("trues.replace(int_test);\n");
                printf("int_test := nil;\n");
                printf("trues.replace(dec_test);\n");
                printf("dec_test := nil;\n");
                printf("trues.replace(dbl_test);\n");
                printf("dbl_test := nil;\n");
                printf("trues.replace(bool_test);\n");
                printf("bool_test := nil;\n");
                
                printf("trues := trues.leftjoin(bool_map);\n");
                printf("iter := trues.mark(0@0).reverse;\n");
                printf("pos := iter.project(1@0);\n");
                printf("item := trues.reverse.mark(0@0).reverse;\n");
                printf("kind := iter.project('%c');\n", BOOL);
                printf("trues := nil;\n");
                printf("} # end of translate fn:boolean (item*) as boolean\n");
                
        }
        else translateEmpty ();
    }

    /**
     * loop-lifted attribute construction
     * @param i the counter of the actual saved result (attr name)
     */
    static void
    loop_liftedAttrConstr (int i)
    {
        PFlog("first short version of attribute constructor: can only handle one string or integer per iter");
        printf("{ # loop_liftedAttrConstr (int i)\n");
        printf("var temp_int := kind.ord_uselect('%c');\n", INT);
        printf("temp_int := temp_int.mirror;\n");
        printf("temp_int := temp_int.leftfetchjoin(item);\n");
        printf("temp_int := temp_int.leftfetchjoin(int_values);\n");
        printf("temp_int := temp_int.[str];\n");
        printf("var temp_str := kind.ord_uselect('%c');\n", STR);
        printf("temp_str := temp_str.mirror;\n");
        printf("temp_str := temp_str.leftfetchjoin(item);\n");
        printf("temp_str := temp_str.leftfetchjoin(str_values);\n");
        printf("var temp_node := kind.ord_uselect('%c');\n", NODE);
        printf("temp_node := temp_node.mirror;\n");
        /* FIXME: small test */
        printf("var test := {count}(iter.reverse,iter.tunique);\n"
               "if (test.count != test.sum) ERROR (\"more than 1 argument in attribute constructor\");\n");

        printf("temp_node := temp_node.leftfetchjoin(item);\n");
        printf("temp_node := temp_node.leftfetchjoin(Tpre_kind);\n");
        printf("temp_node := temp_node.ord_uselect(TEXT);\n");
        printf("temp_node := temp_node.mirror;\n");
        printf("temp_node := temp_node.leftfetchjoin(item);\n");
        printf("temp_node := temp_node.leftfetchjoin(Tpre_prop);\n");
        printf("temp_node := temp_node.leftfetchjoin(Tprop_text);\n");
        printf("item := merged_union (temp_str.reverse, temp_int.reverse);\n");
        printf("item := merged_union (item, temp_node.reverse).reverse;\n");

        printf("var seqb := int(Tattr_loc.reverse.max) + 1;\n");
        printf("seqb := oid(seqb);\n");
        printf("item := item.reverse.mark(0@0).reverse;\n");
        printf("item.seqbase(seqb);\n");
        printf("item%03u := item%03u.leftfetchjoin(Tprop_loc);\n", i, i);
        printf("item%03u.seqbase(seqb);\n");
        printf("Tattr_loc.insert(item%03u);\n", i);
        printf("Tattr_ns.insert(item.project(\"\"));\n");
        printf("Tattr_val.insert(item);\n");
        printf("item := item.mirror;\n");
        printf("seqb := oid(TattrID_attr.count);\n");
        printf("item.seqbase(seqb);\n");
        printf("TattrID_attr.insert(item);\n");
        printf("TattrID_pre.insert(item.mark(nil));\n");

        printf("item := item.mirror;\n");
        printf("item.seqbase(0@0);\n");
        printf("kind := kind.project('%c');\n", ATTR);
        printf("} # end of loop_liftedAttrConstr (int i)\n");
    }

    static void
    loop_liftedTextConstr (void)
    {
        /* expects exactly one string for each iter */
        printf("if (iter.tunique.count != kind.uselect('%c').count) ", STR);
        printf("ERROR (\"Text Constructor awaits exactly one string for each iter\");\n");

        printf("{ # adding new strings to text node content and create new nodes\n"
               "var oid_str := item.leftfetchjoin(str_values);\n"
               "var seqb := oid(Tprop_text.count);\n"
               "var oid_oid := oid_str.mark(seqb);\n"
               "oid_str.seqbase(seqb);\n"
               "seqb := nil;\n"
               "Tprop_text.insert(oid_str);\n"
               "oid_str := nil;\n"
               "seqb := oid(Tpre_kind.count);\n"
               "var newPre_prop := oid_oid.reverse.mark(seqb).reverse;\n"
               "Tpre_prop.insert(newPre_prop);\n"
               "Tpre_size.insert(newPre_prop.project(0));\n"
               "Tpre_level.insert(newPre_prop.project(chr(0)));\n"
               "Tpre_kind.insert(newPre_prop.project(TEXT));\n"
               "newPre_prop := nil;\n"
               "item := item.mark(seqb);\n"
               "seqb := nil;\n"
               "kind := kind.project('%c');\n"
               "}\n", NODE);

        /* adding the new constructed roots to the Tdoc_pre table, that a
           following (preceding) step can check the fragment boundaries */
        printf("{ # adding new fragments to the Tdoc_pre and Tdoc_name table\n"
               "var seqb := Tdoc_pre.count;\n"
               "seqb := oid(seqb);\n"
               "var new_pres := item.reverse.mark(seqb).reverse;\n"
               "seqb := nil;\n"
               "Tdoc_pre.insert(new_pres);\n"
               /* Tdoc_name is also needed because, they should be aligned for
                  doc_to_working_set(str) */
               "Tdoc_name.insert(new_pres.project(str(nil)));\n"
               "new_pres := nil;\n"
               "Theight := max(Theight, 1);\n"
               "}\n");
    }

    /*
     * translates either then or else block of an if-then-else
     *
     * to avoid more than one expansion of the subtree for each 
     * branch three branches (PHASES) are added in MIL. They
     * avoid the expansion of the variable environment and of the 
     * subtree if the if-clause produces either only true or only
     * false values. If then- or else-clause is empty (c_empty)
     * the function will only be called for the other.
     *
     *  '-' = not      |   skip  |  c_empty 
     *        executed | 0  1  2 | then  else
     *  PHASE 1 (then) |    -  - |  -
     *  PHASE 2 (then) |       - |  -
     *  PHASE 3 (then) |    -  - |  -
     *  PHASE 1 (else) |    -  - |        -
     *  PHASE 2 (else) |    -    |        -
     *  PHASE 3 (else) |    -  - |        -
     *
     * @param c the then/else expression
     * @param then is the boolean saving if the branch (then/else)
     * @param bool_res the number, where the result of the if-clause
     *        is saved 
     */
    static void
    translateIfThen (PFcnode_t *c, int then, int bool_res)
    {
        act_level++;
        printf("{ # translateIfThen\n");

        /* initial setting of new 'scope' */
        printf("var loop%03u := loop%03u;\n", act_level, act_level-1);
        printf("var inner%03u := inner%03u;\n", act_level, act_level-1);
        printf("var outer%03u := outer%03u;\n", act_level, act_level-1);
        printf("var v_vid%03u := v_vid%03u;\n", act_level, act_level-1);
        printf("var v_iter%03u := v_iter%03u;\n", act_level, act_level-1);
        printf("var v_pos%03u := v_pos%03u;\n", act_level, act_level-1);
        printf("var v_item%03u := v_item%03u;\n", act_level, act_level-1);
        printf("var v_kind%03u := v_kind%03u;\n", act_level, act_level-1);

        /* 1. PHASE: create all mapping stuff to next 'scope' */
        printf("if (skip = 0)\n{\n");
        /* output for debugging
        printf("\"PHASE 1 of %s-clause active\".print;\n",then?"then":"else");
        */

        /* get the right set of sequences, which have to be processed */
        if (!then)
                printf("selected := item%03u.ord_uselect(0@0);\n", bool_res);

        printf("iter := selected.mirror.join(iter%03u);\n", bool_res);
        printf("iter := iter.reverse.mark(0@0).reverse;\n");
        printf("outer%03u := iter;\n", act_level);
        printf("iter := iter.mark(1@0);\n");
        printf("inner%03u := iter;\n", act_level);
        printf("loop%03u := inner%03u;\n", act_level, act_level);
        printf("iter := nil;\n");

        /* - in a first version no variables are pruned
             at an if-then-else node 
           - if-then-else is executed more or less like a for loop */
        printf("var expOid := v_iter%03u.mirror;\n", act_level);
        //getExpanded (c->sem.num);
        //printf("if (expOid.count != 0) {\n");
                printf("var oidNew_expOid;\n");
                expand ();
                join ();
        //printf("} else {\n");
        //        createNewVarTable ();
        //printf("} # end if\n");
        printf("expOid := nil;\n");

        printf("}\n");

        /* 2. PHASE: execute then/else expression if there are 
           true/false values in the boolean expression */
        if (then)
                printf("if (skip != 1)\n{\n");
        else
                printf("if (skip != 2)\n{\n");
        /* output for debugging
        printf("\"PHASE 2 of %s-clause active\".print;\n",then?"then":"else");
        */

        translate2MIL (c);
        printf("}\n");
        printf("else\n{\n");
        translateEmpty ();
        printf("}\n");

        /* 3. PHASE: create all mapping stuff from to actual 'scope' */
        printf("if (skip = 0)\n{\n");
        /* output for debugging
        printf("\"PHASE 3 of %s-clause active\".print;\n",then?"then":"else");
        */
                mapBack ();
        printf("}\n");

        cleanUpLevel ();
        printf("} # end of translateIfThen\n");
        act_level--;
    }

    /**
     * prints to stdout MIL-expressions, for the following
     * core nodes:
     * c_var, c_seq, c_for, c_let
     * c_lit_str, c_lit_dec, c_lit_dbl, c_lit_int
     * c_empty, c_true, c_false
     * c_locsteps, (axis: c_ancestor, ...)
     * (tests: c_namet, c_kind_node, ...)
     * c_ifthenelse,
     * (constructors: c_elem, ...)
     *
     * the following list is not supported so far:
     * c_nil
     * c_apply, c_arg,
     * c_typesw, c_cases, c_case, c_seqtype, c_seqcast
     * c_error, c_root, c_int_eq
     */
    static void
    translate2MIL (PFcnode_t *c)
    {
        char *ns, *loc;
        int inter_res, bool_res;

        assert(c);
        switch (c->kind)
        {
                case c_var:
                        translateVar(c);
                        break;
                case c_seq:
                        if ((c->child[0]->kind == c_empty)
                            && (c->child[1]->kind != c_empty))
                                translateEmpty ();
                        else if (c->child[0]->kind == c_empty)
                                translate2MIL (c->child[1]);
                        else if (c->child[1]->kind == c_empty)
                                translate2MIL (c->child[0]);
                        else
                        {
                                translate2MIL (c->child[0]);
                                inter_res = saveResult ();

                                translate2MIL (c->child[1]);

                                translateSeq (inter_res);
                                deleteResult ();
                        }
                        break;
                case c_let:
                        translate2MIL (c->child[1]);
                        if (c->child[0]->sem.var->used)
                                insertVar (c->child[0]->sem.var->vid);

                        translate2MIL (c->child[2]);
                        break;
                case c_for:
                        translate2MIL (c->child[2]);
                        /* not allowed to overwrite iter,pos,item */

                        act_level++;
                        printf("{\n");
                        project ();

                        printf("var expOid;\n");
                        getExpanded (c->sem.num);
                        printf("if (expOid.count != 0) {\n");
                                printf("var oidNew_expOid;\n");
                                expand ();
                                join ();
                        printf("} else {\n");
                                createNewVarTable ();
                        printf("} # end if\n");
                        printf("expOid := nil;\n");

                        if (c->child[0]->sem.var->used)
                                insertVar (c->child[0]->sem.var->vid);
                        if ((c->child[1]->kind == c_var)
                            && (c->child[1]->sem.var->used))
                        {
                                /* changes item and kind and inserts if needed
                                   new int values to 'int_values' bat */
                                createEnumeration ();
                                insertVar (c->child[1]->sem.var->vid);
                        }
                        /* end of not allowed to overwrite iter,pos,item */

                        translate2MIL (c->child[3]);
                        
                        mapBack ();
                        cleanUpLevel ();
                        printf("}\n");
                        act_level--;
                        break;
                case c_ifthenelse:
                        translate2MIL (c->child[0]);
                        bool_res = saveResult ();
                        printf("{\n");
                        /* idea:
                        select trues
                        if (trues = count) or (trues = 0)
                             only give back one of the results
                        else
                             do the whole stuff
                        */
                        printf("var selected := item%03u.ord_uselect(1@0);\n", bool_res);
                        printf("var skip := 0;\n");
                        printf("if (selected.count = item%03u.count) "
                               "skip := 2;\n", bool_res);
                        printf("else if (selected.count = 0) "
                               "skip := 1;\n");
                        /* if at compile time one argument is already known to
                           be empty don't do the other */
                        if (c->child[2]->kind == c_empty)
                        {
                                translateIfThen (c->child[1], 1, bool_res);
                        }
                        else if (c->child[1]->kind == c_empty)
                        {
                                translateIfThen (c->child[2], 0, bool_res);
                        }
                        else
                        {
                                translateIfThen (c->child[1], 1, bool_res);
                                inter_res = saveResult ();
                                translateIfThen (c->child[2], 0, bool_res);
                                translateSeq (inter_res);
                                deleteResult ();
                        }
                        printf("}\n");
                        deleteResult ();
                        break;
                case c_locsteps:
                        translate2MIL (c->child[1]);
                        translateLocsteps (c->child[0]);
                        break;
                case c_elem:
                        translate2MIL (c->child[0]);

                        if (c->child[0]->kind != c_tag)
                            getQName ();

                        inter_res = saveResult ();

                        translate2MIL (c->child[1]);

                        loop_liftedElemConstr (inter_res);
                        deleteResult ();
                        break;
                case c_attr:
                        translate2MIL (c->child[0]);

                        if (c->child[0]->kind != c_tag)
                            getQName ();

                        inter_res = saveResult ();

                        translate2MIL (c->child[1]);

                        loop_liftedAttrConstr (inter_res);
                        deleteResult ();
                        break;
                case c_tag:
                        ns = c->sem.qname.ns.uri;
                        loc = c->sem.qname.loc;

                        /* translate missing ns as "" */
                        if (!ns)
                            ns = "";

                        printf("{\n");
                        printf("var propID := Tprop_ns.select(\"%s\");\n", ns);
                        printf("var temp2 := Tprop_loc.select(\"%s\");\n", loc);
                        printf("propID := propID.semijoin(temp2);\n");
                        printf("temp2 := nil;\n");
                        printf("var itemID;\n");

                        printf("if (propID.count = 0) {\n");
                        /* perhaps max is better to get boundary then count 
                           (if seqbase is not 0@0 or if head is not void) */
                        printf("         itemID := oid(Tprop_loc.count);\n");
                        printf("         Tprop_ns.insert (itemID,\"%s\");\n", ns);
                        printf("         Tprop_loc.insert (itemID,\"%s\");\n", loc);
                        printf("} else ");
                        printf(         "itemID := propID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst (QNAME);
                        printf("propID := nil;\n");
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_text:
                        translate2MIL (c->child[0]);
                        loop_liftedTextConstr ();
                        break;
                case c_lit_str:
                        /* the value of the string is looked up in the 
                           str_values table. If it already exists the oid
                           is given back else it is inserted and a new
                           oid is created */
                        /* old version
                        printf("{\n");
                        printf("var itemID := str_values.ord_uselect(\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("if (itemID.count = 0) {\n");
                        printf("         itemID := oid(str_values.count);\n");
                        printf("         str_values.insert (nil,\"%s\");\n",
                                        PFesc_string (c->sem.str));
                        printf("} else ");
                        printf(         "itemID := itemID.reverse.fetch(0);\n");
                        */
                        printf("{\n");
                        printf("str_values.insert (nil,\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("var itemID := str_values.ord_uselect(\"%s\");\n",
                               PFesc_string (c->sem.str));
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(STR);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_int:
                        printf("{\n");
                        printf("int_values.insert (nil,%u);\n",
                               c->sem.num);
                        printf("var itemID := int_values.ord_uselect(%u);\n",
                               c->sem.num);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(INT);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_dec:
                        printf("{\n");
                        printf("dec_values.insert (nil,dbl(%g));\n",
                               c->sem.dec);
                        printf("var itemID := dec_values.ord_uselect(dbl(%g));\n",
                               c->sem.dec);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(DEC);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_lit_dbl:
                        printf("{\n");
                        printf("dbl_values.insert (nil,dbl(%g));\n",
                               c->sem.dbl);
                        printf("var itemID := dbl_values.ord_uselect(dbl(%g));\n",
                               c->sem.dbl);
                        printf("itemID := itemID.reverse.fetch(0);\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(DBL);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_true:
                        printf("{\n");
                        printf("var itemID := 1@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(BOOL);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_false:
                        printf("{\n");
                        printf("var itemID := 0@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(BOOL);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_root:
                        /* builtin function, which is only translated for 
                           debugging purposes (with "foo.xml" - see init) */
                        printf("{\n");
                        printf("var itemID := 0@0;\n");
                        /* translateConst needs a bound variable itemID */
                        translateConst(NODE);
                        printf("itemID := nil;\n");
                        printf("}\n");
                        break;
                case c_empty:
                        translateEmpty ();
                        break;
                case c_seqcast:
                        /* seqcast just ignores the cast */
                        PFlog("cast to '%s' ignored",PFty_str (c->child[0]->sem.type));
                        translate2MIL (c->child[1]);
                        break;
                case c_apply:
                        translateFunction (c->sem.fun->qname, c->child[0]);
                        break;
                case c_nil:
                default: 
                        PFoops (OOPS_WARNING, "not supported feature is translated");
                        break;
        }
    }

    /* the fid increasing for every for node */ 
    static int fid = 0;
    /* the actual fid saves the level (the right fid) */
    static int act_fid = 0;
    /* the vid increasing for every new variable binding */
    static int vid = 0;

    /**
      * the pairs of vid and fid are inserted in the var_usage bat
      *
      * @param var the pointer to the variable and its vid
      * @param fid the id of the for expression
      */
    static void
    add_level (PFvar_t *var, int fid)
    {
        printf("var_usage.insert(%i@0,%i@0);\n", var->vid, fid); 
    }

    /**
      * for a variable usage all fids between the definition of the 
      * variable and its usage are added to the var_usage bat
      *
      * @param c the variable core node
      * @param way the path of the for ids (active for-expression)
      */
    static void
    update_expansion (PFcnode_t *c,  PFarray_t *way)
    {
        int m;
        PFvar_t *var;

        assert(c->sem.var);
        var = c->sem.var;

        for (m = PFarray_last (way) - 1; m >= 0 
             && *(int *) PFarray_at (way, m) > var->base; m--)
        {
            add_level (var, *(int *) PFarray_at (way, m));
        }
    }

    /**
      * for each variable a vid (variable id) and
      * for each for expression a fid (for id) is added;
      * for each variable usage the needed fids are added to a 
      * bat variable_usage
      *
      * @param c a core tree node
      * @param way an array containing the path of the for ids
      *        (active for-expression)
      */
    static void
    append_lev (PFcnode_t *c,  PFarray_t *way)
    {
        unsigned int i;

        if (c->kind == c_var) 
        {
           /* inserts fid|vid combinations into var_usage bat */
           update_expansion (c, way);
           /* the field used is for pruning the MIL code and
              avoid translation of variables which are later
              not used */
           c->sem.var->used = 1;
        }

        /* only in for and let variables can be bound */
        else if (c->kind == c_for)
        {
           if (c->child[2])
               append_lev (c->child[2], way);
           
           fid++;
           c->sem.num = fid;
           *(int *) PFarray_add (way) = fid;
           act_fid = fid;

           c->child[0]->sem.var->base = act_fid;
           c->child[0]->sem.var->vid = vid;
           c->child[0]->sem.var->used = 0;
           vid++;

           if (c->child[1]->kind == c_var)
           {
                c->child[1]->sem.var->base = act_fid;
                c->child[1]->sem.var->vid = vid;
                c->child[1]->sem.var->used = 0;
                vid++;
           }

           if (c->child[3])
               append_lev (c->child[3], way);
           
           act_fid = *(int *) PFarray_at (way, PFarray_last (way) - 1);
           PFarray_del (way);
        }

        else if (c->kind == c_let)
        {
           if (c->child[1])
               append_lev (c->child[1], way);

           c->child[0]->sem.var->base = act_fid;
           c->child[0]->sem.var->vid = vid;
           c->child[0]->sem.var->used = 0;
           vid++;

           if (c->child[2])
               append_lev (c->child[2], way);
        }

        else 
        {
           for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
              append_lev (c->child[i], way);
        } 
    }

    /**
      * first MIL generation from Pathfinder Core
      *
      * first to each `for' and `var' node additional
      * information is appended. With this information
      * the core tree is translated into MIL.
      *
      * @param c the root of the core tree
      */
    void
    PFprintMILtemp (PFcnode_t *c)
    {
        PFarray_t *way;

        way = PFarray (sizeof (int));
        assert (way);
        
        /* some bats and module get initialized, variables get bound */
        init ();

        /* append_lev appends information to the core nodes and
           creates a var_usage table, which is later split in vu_fid
           and vu_vid */
        printf("{\n"
               "var_usage := bat(oid,oid);\n");/* [vid, fid] */
        append_lev (c, way);
        /* the contents of var_usage will be sorted by fid and
           then refined (sorted) by vid */
        printf("var_usage := var_usage.unique.reverse;\n"
               "var_usage.access(BAT_READ);\n"
               "vu_fid := var_usage.mark(1000@0).reverse;\n"
               "vu_vid := var_usage.reverse.mark(1000@0).reverse;\n"
               "var_usage := nil;\n"
               "var sorting := vu_fid.reverse.sort.reverse;\n"
               "sorting := sorting.CTrefine(vu_vid);\n"
               "sorting := sorting.mark(1000@0).reverse;\n"
               "vu_vid := sorting.leftfetchjoin(vu_vid);\n"
               "vu_fid := sorting.leftfetchjoin(vu_fid);\n"
               "sorting := nil;\n"
               "}\n");

        /* recursive translation of the core tree */
        translate2MIL (c);

        /* print result in iter|pos|item representation */
        print_output ();
        printf("clear_working_set();\n");

        if (counter) PFoops (OOPS_FATAL, 
                             "wrong number of saveResult() and \
deleteResult() calls in milprint.c");
    }
/* vim:set shiftwidth=4 expandtab: */
