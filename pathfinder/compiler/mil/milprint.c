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

    /* generates a number for the row where a value can be found 
       for a lookup in values */
    static unsigned int valuecount = 0;
    /* saves the actual level (corresponding to the for nodes) */
    static unsigned int act_level = 0;
    /* start 'seq_level' from a level where no name conflict can occur */
    static unsigned int seq_level = 10;

    static void
    init (void)
    {
        printf("# init ()\n");
        printf("values := bat(void,str);\n");
        printf("loop000 := bat(void,oid);\n");
        printf("loop000.insert(nil, 1@0);\n");
        printf("loop000 := loop000.reverse.mark(0@0).reverse;\n");
        printf("v_vid000 := bat(void,oid);\n");
        printf("v_iter000 := bat(void,oid);\n");
        printf("v_pos000 := bat(void,oid);\n");
        printf("v_item000 := bat(void,oid);\n");
        printf("empty_bat := bat(void,oid);\n");
    }

    static void
    translateEmpty (void)
    {
        printf("# translateEmpty ()\n");
        printf("iter := empty_bat;\n");
        printf("pos := empty_bat;\n");
        printf("item := empty_bat;\n");
    }

    static void
    translateVar (PFcnode_t *c)
    {
        printf("# translateVar (c)\n");
        printf("vid := v_vid%03u.uselect(%i@0);\n", act_level, c->sem.var->vid);
        printf("vid := vid.mark(0@0);\n");
        printf("vid := vid.reverse;\n");
        printf("iter := vid.join(v_iter%03u);\n", act_level);
        printf("pos := vid.join(v_pos%03u);\n", act_level);
        printf("item := vid.join(v_item%03u);\n", act_level);
        printf("vid := nil;\n");
    }

    static void
    saveSeqResult (int i)
    {
        printf("# saveSeqResult (seq_level)\n");
        printf("iter%03u := iter;\n", i);
        printf("pos%03u := pos;\n", i);
        printf("item%03u := item;\n", i);
    }

    static void
    translateSeq (int i)
    {
        printf("# translateSeq (seq_level)\n");
printf("iter2 := iter;pos2 := pos;item2 := item;iter1 := iter%03u;pos1 := pos%03u;item1 := item%03u;\
ord1 := iter1.project(1@0);ord2 := iter2.project(2@0);temp := count(iter1);temp := oid(temp);\
iter1 := iter1.reverse;iter1 := iter1.mark(0@0);iter1 := iter1.reverse;iter2 := iter2.reverse;iter2 := iter2.mark(0@0);iter2 := iter2.reverse;iter2 := iter2.seqbase(temp);iter1.access(BAT_APPEND);iter1.insert(iter2);iter1.access(BAT_READ);\
ord1 := ord1.reverse;ord1 := ord1.mark(0@0);ord1 := ord1.reverse;ord2 := ord2.reverse;ord2 := ord2.mark(0@0);ord2 := ord2.reverse;ord2 := ord2.seqbase(temp);ord1.access(BAT_APPEND);ord1.insert(ord2);ord1.access(BAT_READ);\
pos1 := pos1.reverse;pos1 := pos1.mark(0@0);pos1 := pos1.reverse;pos2 := pos2.reverse;pos2 := pos2.mark(0@0);pos2 := pos2.reverse;pos2 := pos2.seqbase(temp);pos1.access(BAT_APPEND);pos1.insert(pos2);pos1.access(BAT_READ);\
item1 := item1.reverse;item1 := item1.mark(0@0);item1 := item1.reverse;item2 := item2.reverse;item2 := item2.mark(0@0);item2 := item2.reverse;item2 := item2.seqbase(temp);item1.access(BAT_APPEND);item1.insert(item2);item1.access(BAT_READ);\
temp := iter1.reverse;temp := temp.sort;temp := temp.reverse;temp := temp.CTrefine(ord1);temp := temp.CTrefine(pos1);temp := temp.mark(0@0);temp := temp.reverse;\
iter := temp.join(iter1);pos := temp.mark(1@0);item := temp.join(item1);\
iter1 := nil;pos1 := nil;item1 := nil;ord1 := nil;iter2 := nil;pos2 := nil;item2 := nil;ord2 := nil;iter%03u := nil;pos%03u := nil;item%03u := nil;temp := nil;\n", i, i, i, i, i, i);

    }

    static void
    deleteTemp (void)
    {
        printf("# deleteTemp ()\n");
        printf("temp := nil;\n");
        printf("temp2 := nil;\n");
        printf("temp3 := nil;\n");
    }

    static void
    project (void)
    {
        printf("# project ()\n");
        printf("outer%03u := iter;\n", act_level);
        printf("iter := iter.mark(1@0);\n");
        printf("inner%03u := iter;\n", act_level);
        printf("pos := iter.project(1@0);\n");
        printf("loop%03u := inner%03u;\n", act_level, act_level);
    }

    static void
    getExpanded (int fid)
    {
        printf("# getExpanded (fid)\n");
        printf("temp := vu_fid.uselect(%i@0);\n",fid);
        printf("temp := temp.mark(10@0);\n");
        printf("temp2 := vu_vid.reverse;\n");
        printf("temp := temp2.join(temp);\n");
        printf("temp := v_vid%03u.join(temp);\n", act_level - 1);
        printf("temp := temp.mirror;\n");
        printf("temp2 := nil;\n");
    }

    static void
    expand (void)
    {
        printf("# expand ()\n");
        printf("temp2 := temp.join(v_iter%03u);\n", act_level-1); 
                                               /* -1 is important */
        printf("temp2 := temp2.reverse;\n");
        printf("temp2 := outer%03u.join(temp2);\n", act_level);
        printf("temp2 := temp2.reverse;\n");
        printf("v_iter%03u := temp2.join(inner%03u);\n", act_level, act_level);
        printf("temp2 := v_iter%03u.mark(0@0);\n", act_level);
        printf("temp2 := temp2.reverse;\n");
    }

    static void
    join (void)
    {
        printf("# join ()\n");
        printf("v_iter%03u := v_iter%03u.reverse;\n", act_level, act_level);
        printf("v_iter%03u := v_iter%03u.mark(0@0);\n", act_level, act_level);
        printf("v_iter%03u := v_iter%03u.reverse;\n", act_level, act_level);
        printf("v_vid%03u := temp2.join(temp);\n", act_level);
        printf("v_vid%03u := v_vid%03u.join(v_vid%03u);\n",
               act_level, act_level, act_level - 1);
        printf("v_pos%03u := temp2.join(temp);\n", act_level);
        printf("v_pos%03u := v_pos%03u.join(v_pos%03u);\n",
               act_level, act_level, act_level - 1);
        printf("v_item%03u := temp2.join(temp);\n", act_level);
        printf("v_item%03u := v_item%03u.join(v_item%03u);\n",
               act_level, act_level, act_level - 1);
    }

    static void
    append (char *name, int level)
    {
        printf("# append (%s, level)\n", name);
        printf("temp := %s.reverse;\n", name);
        printf("temp := temp.mark(0@0);\n");
        printf("temp := temp.reverse;\n");
        printf("temp := temp.seqbase(temp2);\n");
        printf("v_%s%03u := v_%s%03u.access(BAT_APPEND);\n",name, level, name, level);
        printf("v_%s%03u.insert(temp);\n",name, level);
        printf("v_%s%03u.access(BAT_READ);\n",name, level);
        printf("temp := nil;\n");
    }

    static void
    mapBack (void)
    {
        printf("# mapBack ()\n");
        printf("temp := iter.mirror;\n");
        printf("temp3 := inner%03u.reverse;\n", act_level);
        printf("temp2 := iter.join(temp3);\n");
        printf("iter := temp2.join(outer%03u);\n", act_level);
        printf("pos := pos.mark(1@0);\n");
        printf("item := item;\n");
        deleteTemp ();
    }

    static void
    insertVar (int vid_)
    {
        char *vid, *iter, *pos, *item;
        vid = PFmalloc (sizeof("vid"));
        snprintf (vid, sizeof("vid"), "vid");
        iter = PFmalloc (sizeof("iter"));
        snprintf (iter, sizeof("iter"), "iter");
        pos = PFmalloc (sizeof("pos"));
        snprintf (pos, sizeof("pos"), "pos");
        item = PFmalloc (sizeof("item"));
        snprintf (item, sizeof("item"), "item");

        printf("# insertVar (vid)\n");
        printf("vid := iter.project(%i@0);\n", vid_);
        printf("temp2 := count(vid);\n");
        printf("temp2 := oid(temp2);\n");

        append (vid, act_level);
        append (iter, act_level);
        append (pos, act_level);
        append (item, act_level);
        printf("temp2 := nil;\n");
        printf("vid := nil;\n");
    }

    static void
    translateConst (int valueId)
    {
        printf("# translateConst (valueId)\n");
        printf("iter := loop%03u;\n",act_level);
        printf("iter := iter.reverse;\n");
        printf("iter := iter.mark(0@0);\n");
        printf("iter := iter.reverse;\n");
        printf("pos := iter.project(1@0);\n");
        printf("item := iter.project(%i@0);\n", valueId);
    }

    static void
    translate2MIL (PFcnode_t *c)
    {
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
                                saveSeqResult (seq_level);
                                seq_level++;
                                                                                                                                                         
                                translate2MIL (c->child[1]);
                                seq_level--;
                                translateSeq (seq_level);
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
                        project ();
                        getExpanded (c->sem.num);
                        expand ();
                        join ();
                        deleteTemp ();
                        if (c->child[0]->sem.var->used)
                                insertVar (c->child[0]->sem.var->vid);
                        if ((c->child[1]->kind == c_var)
                            && (c->child[1]->sem.var->used))
                                insertVar (c->child[1]->sem.var->vid);
                        /* end of not allowed to overwrite iter,pos,item */

                        translate2MIL (c->child[3]);
                        
                        mapBack ();
                        act_level--;
                        break;
                case c_lit_str:
                        printf("values.insert(%i@0,\"%s\");\n",
                               valuecount,
                               PFesc_string (c->sem.str));
                        translateConst(valuecount);
                        valuecount++;
                        break;
                case c_lit_int:
                        printf("values.insert(%i@0,\"%u\");\n",
                               valuecount,
                               c->sem.num);
                        translateConst(valuecount);
                        valuecount++;
                        break;
                case c_lit_dec:
                        printf("values.insert(%i@0,\"%.5g\");\n",
                               valuecount,
                               c->sem.dec);
                        translateConst(valuecount);
                        valuecount++;
                        break;
                case c_lit_dbl:
                        printf("values.insert(%i@0,\"%.5g\");\n",
                               valuecount,
                               c->sem.dbl);
                        translateConst(valuecount);
                        valuecount++;
                        break;
                case c_empty:
                        translateEmpty ();
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
        printf("var_usage.insert(%i@0,%i@0)\n;", var->vid, fid); 
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
        
        /* append_lev appends information to the core nodes and
           creates a var_usage table, which is later split in vu_fid
           and vu_vid */
        printf("var_usage := bat(oid,oid);\n");
        append_lev (c, way);
        printf("var_usage := var_usage.unique.reverse.sort;\n");
        printf("vu_fid := var_usage.mark(1000@0).reverse;\n");
        printf("vu_vid := var_usage.reverse.mark(1000@0).reverse;\n");

        /* some bats are initialized and some variables get bound */
        init ();
        /* recursive translation of the core tree */
        translate2MIL (c);

        /* print result in iter|pos|item representation */
        printf("temp := values.reverse.mark(0@0).reverse;\n");
        printf("temp := item.join(temp);\n");
        printf("print (iter, pos, temp);\n");
    }
/* vim:set shiftwidth=4 expandtab: */
