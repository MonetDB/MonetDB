/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Entry point to Pathfinder compiler.
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

/**
 * @mainpage Pathfinder XQuery Compiler Project
 *
 * @section procedure Compilation Procedure
 *
 * @subsection scanning Lexical Scanning
 *
 * Lexical scanning of the input query is done with a
 * <a href="http://www.gnu.org/software/flex/">flex</a> generated
 * scanner. The implementation in parser/XQuery.l follows the appendix of
 * the <a href="http://www.w3.org/TR/2002/WD-xquery-20020430">April 30</a>
 * working draft of the <a href="http://www.w3.org">W3 Consortium</a>.
 * The scanner dissects the input query into lexical tokens that serve
 * as an input for the @ref parsing "Parser".
 *
 * @subsection parsing Parsing
 *
 * In the parsing phase, the input query is analyzed according to the
 * XQuery grammar rules. The parser is implemented with 
 * <a href="http://www.gnu.org/software/bison/bison.html">bison</a> (see
 * file parser/XQuery.y). The implementation closely follows the grammar
 * described in the
 * <a href="http://www.w3.org/TR/2002/WD-xquery-20020430">April 30</a>
 * working draft of the <a href="http://www.w3.org">W3 Consortium</a>.
 *
 * The output of the parser is an abstract syntax tree (aka ``parse tree'')
 * that represents the input query in memory in a form that is very close
 * to the input syntax. Declarations of the abstract syntax tree and
 * helper functions to access and modify the tree are to be found in
 * parser/abssyn.h.
 *
 * @subsection abssyn_norm Normalization of the Abstract Syntax Tree
 *
 * Semantically equivalent queries can result in different abstract
 * syntax trees, e.g., when parentheses are introduced, etc. In a
 * normalization phase that follows query parsing, we get rid of these
 * ambiguities and make sure that semantically equivalent queries result
 * in the same parse tree.
 *
 * <b>An example for a normalization rule:</b>
 *
 * Using parentheses, nested @c exprseq nodes can be created in the
 * abstract syntax tree. They are semantically equivalent to right deep
 * chains of @c exprseq nodes. Thus,
 @verbatim
 exprseq (exprseq (a, b), exprseq (c, nil))
 @endverbatim
 * is rewritten to
 @verbatim
 exprseq (a, exprseq (b, exprseq (c, nil)))
 @endverbatim
 *
 * Abstract syntax normalization is done with a twig based pattern matcher.
 * Twig is a generic utility to create tree manipulation programs. Based
 * on a tree grammar, it generates a tree walker that searches the abstract
 * syntax tree for tree patterns specified by the programmer. Action code
 * (written in C) can be invoked on encountering certain tree patterns.
 * If more than one tree pattern match a part of the tree, @e cost
 * definitions help choosing the right action code to invoke.
 *
 * The twig input file for the abstract syntax tree normalization is
 * normalize.mt, located in the semantics subdirectory. After processing
 * it with @c twig, the pattern matcher is generated as C code. To allow
 * for documentation of the contained C functions with doxygen, most of
 * the C code is implemented in semantics/norm_impl.c. The twig generated
 * code includes this file. More information on twig, as well as the
 * sources can be found at http://www.inf.uni-konstanz.de/~teubner/twig/
 *
 * @subsection varscoping Variable Scope Checking
 *
 * The parsing phase is followed by a tree traversal that analyzes
 * variable declaration and usages. Scoping rules are verified and
 * to each variable a unique memory block is assigned that contains all
 * required information about this variable. Multiple references to the
 * same variable result in references to the same memory block, following
 * the XQuery scoping rules.
 *
 * Variable scoping is implemented in semantics/varscope.c. In
 * pathfinder/variable.c access and helper functions around variables are
 * implemented.
 *
 * @subsection ns Namespace Resolution
 *
 * Another tree traversal ensures that the given query uses XML
 * Namespaces (NS) correctly.  In a qualified name (QName) @c ns:loc,
 * it is mandatory that the NS prefix @c ns is in scope, according to
 * the W3C XQuery and W3C XML Namespace rules.
 *
 * For each such successful NS test, if the NS prefix @c ns has been
 * mapped to the URI @c uri, this phase attaches @c uri to the QName
 * in question.  After this phase has completed, a QName either uses
 * no NS at all or carries the complete @c ns @c |-> @c uri mapping
 * with it.
 *
 * All XQuery NS-relevant prolog declarations, NS scoping rules, and
 * NS declaration attributes of the form @c xmlns="uri" and @c
 * xmlns:ns="uri" (such NS decl attributes are removed after they have
 * been processed) are understood.
 *
 * NS resolution is implemented in semantics/ns.c.  Include
 * semantics/ns.h if you need access to NS specific data structures
 * and operations only, include semantics/nsres.c if you need to
 * access the actual NS resolution function (this should be needed in
 * main.c, only).
 *
 * @subsection fun_check Checking for Correct Function Usage
 *
 * Similar to checking the variable scoping rules, correct usage of
 * functions is checked in a next phase. Valid functions are either
 * built-in functions or user-defined functions (defined in the query
 * prolog with ``<code>define function</code>''). Function usage is
 * checked in PFfun_check() (file semantics/functions.c). We maintain
 * an environment of visible functions (#PFfun_env).  This environment
 * holds function descriptors (#PFfun_t). Before traversing the whole
 * abstract syntax tree, this array is filled with (a) built-in
 * functions and (b) user-defined functions.
 *
 * The built-in XQuery Functions & Operators (XQuery F&O) are registered
 * with the function environment in semantics/xquery_fo.c.
 *
 * User-defined functions are discovered by a tree-walk through the
 * subtree below the <code>query prolog</code> node. The function
 * descriptor contains information like the name of the function, as
 * well as its parameter and return types. When all functions are
 * registered in the array, the whole abstract syntax tree is
 * traversed, searching for #p_fun_ref nodes. For each of these, the
 * function is looked up in the array. If it is found, the semantic
 * information in the current node is replaced by a pointer to the
 * #PFfun_t struct containing the information for the function to call
 * here. Otherwise, an error is reported.
 *
 * @subsection fs XQuery Formal Semantics (Compilation to Core Language)
 *
 * The abstract syntax tree used in the previous processing steps very
 * closely represents the surface syntax of XQuery. For internal
 * query optimization and evaluation steps, however, we use another,
 * more suitable query representation, called <em>core</em>. Although
 * very similar to the XML Query surface language, core is missing some
 * ``syntactic sugar'' and makes many implicit semantics (i.e., implicit
 * casting rules) more explicit.
 *
 * Our core language is very close to the proposal by the W3C, described
 * in the XQuery Formal Semantics document. Core is the data structure
 * on which the subsequent type inference and type checking phases will
 * operate on. Internally, core is described by a tree structure
 * consisting of #PFcnode_t nodes.
 *
 * @subsection core_simplification Core Simplification and Normalization
 *
 * The Formal Semantics compilation sometimes produces unnecessarily
 * complex Core code. The simplification phase in @c simplify.mt
 * simplifies some of these cases and normalizes the Core language tree.
 *
 * @subsection type_inference Type Inference and Check
 *
 * Type inference walks the core expression, inferring a type for each
 * node.  Types are attached to the core tree nodes to facilitate type
 * checking.
 * 
 * PFty_check() decorates the core tree with type annotations but
 * lets the tree alone otherwise (almost).
 *
 * @subsection coreopt Core Language Optimization
 *
 * The Core language tree is now labeled with static type information,
 * allowing for various optimizations and rewrites of the tree. This
 * is done in @c coreopt.mt.
 *
 * <b>Example:</b>
 *
 * @verbatim
     typeswitch $v
       case node return e1
       default   return e2
@endverbatim
 *
 * If we can statically decide that the type of <code>$v</code> is
 * @em always a subtype of @c node (e.g. if it is the result of an
 * XPath expression), we may replace the whole expression by expression
 * @c e1. Conversely, we can replace the expression by @c e2, if we
 * know that the type of <code>$v</code> can @em never be a subtype
 * of @c node (the static type of <code>$v</code> and the type @c node
 * are @em disjoint).
 *
 * @subsection core2alg Compiling Core to a Relational Algebra
 *
 * Pathfinder compiles XQuery expressions for execution on a relational
 * backend (MonetDB). The heart of the compiler is thus the compilation
 * from XQuery Core to a language over relational data, i.e., a purely
 * relational algebra. This compilation is done in @c core2alg.mt.sed.
 *
 * The compilation follows the ideas described in the paper presented
 * at the TDM'04 workshop (``Relational Algebra: Mother Tongue---XQuery:
 * Fluent'', <a href='http://www.inf.uni-konstanz.de/~teubner/publications/algebra-mapping.pdf'>http://www.inf.uni-konstanz.de/~teubner/publications/algebra-mapping.pdf</a>).
 * The result of this compilation phase is an algebra expression (in
 * an internal tree representation, as described by @c algebra.c). The
 * algebra is an almost standard relational algebra, with some operations
 * made very explicit (e.g., no higher order functions). Few extensions,
 * such as the staircase join operation, are added for tree-specific
 * operations.
 *
 * @subsection algebra_cse Algebra Common Subexpression Elimination (CSE)
 * 
 * The algebra code generated in the previous step contains @em many
 * redundancies. To avoid re-computation of the same expression, the
 * code in algebra_cse.c rewrites the algebra expression @em tree into
 * an equivalent @em graph (DAG), with identical subexpressions only
 * appearing once.
 *
 * @subsection algopt Algebra Optimization
 *
 * The generated algebra code is another, maybe the most important,
 * hook for optimizations. The (twig based) code in @c algopt.mt does
 * some of them (e.g., removal of statically empty expressions).
 *
 * @subsection milgen MIL Code Generation
 *
 * The (optimized) algebra expression tree is compiled to our target
 * language, MIL, in @c milgen.mt.sed. The compilation produces another
 * in-memory data structure that will be serialized afterwards (see below).
 *
 * The MIL code is produced bottom-up, with a twig based compiler. The
 * resulting MIL code computes the result of each sub-expression
 * (i.e., each node in the algebra DAG) and stores it in a set of MIL
 * variables (MonetDB only knows 2-column tables, we need a set of BATs
 * to store a whole relation). The name of these variables is determined
 * with help of a @em prefix that is generated for each sub-expression,
 * followed by suffixes for attribute name and type. The generated code
 * will keep each sub-expression result as long in the corresponding
 * variable, as it may still be used by other operations (i.e., if there
 * still are ingoing edges in the DAG that have not yet been used). As
 * soon as a variable is no longer needed, we set it to @c nil, which
 * allows MonetDB to dispose the variable's old content.
 *
 * @subsection milprint Printing the MIL Code as the Compiler Output
 *
 * The in-memory representation of the generated MIL code is serialized
 * to a string in milprint.c. The serialization follows a simple grammar
 * on the in-memory MIL tree that at the same time checks the tree for
 * correctness (will hopefully help to detect bugs in the generation
 * early).
 *
 * @section commandline Command Line Switches
 *
 * @subsection treeprinting Tree Printing
 *
 * Tree data structures are used in several situations within this project.
 * To make these in-memory structures visible for debugging purposes, we
 * use the help of the tree drawing tool ``dot'' from
 * <a href="http://www.att.com">AT&T's</a>
 * <a href="http://www.research.att.com/sw/tools/graphviz/">graphviz</a>
 * package. The tree structure is printed in a syntax understandable by
 * dot. By piping Pathfinder's output to dot, the tree can be represented
 * as PostScript or whatever. ``dot'' output is selected by the command
 * line option <code>-D</code>. (In the example, `<code>-p</code>'
 * requests to print the <code>p</code>arse tree in dot notation.)
 @verbatim
 $ echo '/foo/bar' | ./pf -Dps6 | dot -Tps -o foo.ps
 @endverbatim
 * See the dot manpage for more information about dot.
 *
 * @subsection prettyprinting Prettyprinting
 *
 * Output in human readable form is done with the help of a prettyprinting
 * algorithm. The output should be suitable to be viewed on a 80 character
 * wide ASCII terminal. Prettyprinted output can be selected with the
 * command line option <code>-P</code>:
 @verbatim
 $ echo '/foo/bar' | ./pf -Pps6
 @endverbatim
 *
 * @subsection what_to_print Data Structures that may be Printed
 *
 * The command line switches <code>-p</code>, <code>-c</code>, and
 * <code>-a</code> request printing of parse tree (aka. abstract
 * syntax tree), Core language tree, or algebra expression tree,
 * respectively.
 *
 * @subsection stopping Stopping Processing at a Certain Point
 *
 * For debugging it may be interesting to stop the compiler at a
 * certain point of processing. The compiler will stop at that point
 * and will print out tree structures as requested by other switches.
 *
 * You find all the stop points listed by issuing the command line
 * help (option <code>-h</code>). The following example stops processing
 * after mapping the abstract syntax tree to Core and prints the
 * resulting Core tree:
 * @verbatim
 $ echo '/foo/bar' | ./pf -Pcs9
@endverbatim
 *
 * @subsection types Static typing for the core language
 *
 * Pathfinder performs static type checking for the generated XQuery
 * core programs.  Passing <code>-t</code> to Pathfinder lets the
 * compiler print type annotations (in braces <code>{ }</code>) along
 * with core program output.
 *
 *
 * @section credits Credits
 *
 * This project has been initiated by the
 * <a href="http://www.inf.uni-konstanz.de/dbis/">Database and Information
 * Systems Group</a> at <a href="http://www.uni-konstanz.de/">University
 * of Konstanz, Germany</a>. It is lead by Torsten Grust
 * (torsten.grust@uni-konstanz.de) and Jens Teubner
 * (jens.teubner@uni-konstanz.de). Several students from U Konstanz have
 * been involved in this project, namely
 * 
 *  - Gerlinde Adam
 *  - Natalia Fibich
 *  - Guenther Hagleitner
 *  - Stefan Hohenadel
 *  - Boris Koepf
 *  - Sabine Mayer
 *  - Steffen Mecke
 *  - Martin Oberhofer
 *  - Henning Rode
 *  - Rainer Scharpf
 *  - Peter Uhl
 *  - Lucian Vacariuc
 *  - Michael Wachter
 *
 * For our work on XML and XQuery we had great support from Maurice van
 * Keulen (keulen@cs.utwente.nl) from the
 * <a href="http://www.cs.utwente.nl/eng/">University of Twente, The
 * Netherlands</a>. Since 2003, Pathfinder is a joint effort also with
 * the <a href='monetdb.cwi.nl'>MonetDB</a> project, the main-memory
 * database system developed by the database group at
 * <a href='http://www.cwi.nl'>CWI Amsterdam</a>.
 */

#include "pathfinder.h"

#include <stdlib.h>
#ifdef HAVE_LIBGEN_H
#include <libgen.h>
#endif
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>

#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
#include <getopt.h>

/**
 * long (GNU-style) command line options and their abbreviated
 * one-character variants.  Keep this list SORTED in ascending
 * order of one-character option names.
 */
static struct option long_options[] = {
    { "print-att-dot",                 0, NULL, 'D' },
    { "haskell",                       0, NULL, 'H' },
    { "print-mil_summer",              0, NULL, 'M' },
    { "optimize",                      0, NULL, 'O' },
    { "print-human-readable",          0, NULL, 'P' },
    { "timing",                        0, NULL, 'T' },
    { "print-algebra",                 0, NULL, 'a' },
    { "print-core-tree",               0, NULL, 'c' },
    { "help",                          0, NULL, 'h' },
    { "print-parse-tree",              0, NULL, 'p' },
    { "quiet",                         0, NULL, 'q' },
    { "stop-after",                    1, NULL, 's' },
    { "typing",                        0, NULL, 't' },
    { NULL,                            0, NULL, 0 }
};

/**
 * character buffer large enough to hold longest
 * command line option plus some extra formatting space
 */
static char opt_buf[sizeof ("print-human-readable") + 8];

static int 
cmp_opt (const void *o1, const void *o2) 
{
    return ((struct option *)o1)->val - ((struct option *)o2)->val;
}

/**
 * map a one-character command line option to its equivalent
 * long form
 */
static const char 
*long_option (char *buf, char *t, char o) 
{
    struct option key = { 0, 0, 0, o };
    struct option *l;

    if ((l = (struct option *) bsearch (&key, long_options,
                                        sizeof (long_options) / 
                                            sizeof (struct option) - 1,
                                        sizeof (struct option),
                                        cmp_opt)))
        return snprintf (buf, sizeof (opt_buf), t, l->name), buf;
    else
        return "";
}

#else

/* no long option names w/o GNU getopt */
#include <unistd.h>

#define long_option(buf,t,o) ""
#define opt_buf 0

#endif

#include "parser.h"       /* parsing XQuery syntax */
#include "abssyn.h"
#include "varscope.h"     /* variable scoping */
#include "normalize.h"    /* parse tree normalization */
#include "ns.h"           /* namespaces */
#include "nsres.h"        /* namespace resolution */
#include "xquery_fo.h"    /* built-in XQuery F&O */
#include "func_chk.h"     /* function resolution */
#include "prettyp.h"
#include "abssynprint.h"
#include "coreprint.h"
#include "algdebug.h"
#include "timer.h"
#include "fs.h"           /* core mapping (formal semantics) */
#include "types.h"        /* type system */
#include "import.h"       /* XML Schema import */
#include "simplify.h"     /* core simplification */
#include "typecheck.h"    /* type inference and check */
#include "algebra.h"      /* algebra tree */
#include "core2alg.h"     /* Compile Core to Relational Algebra */
#include "algopt.h"
#include "milgen.h"       /* MIL tree generation */
#include "milprint.h"     /* create string representation of MIL tree */
#include "milprint_summer.h" /* create MILcode directly from the Core tree */
#include "oops.h"
#include "mem.h"
#include "algebra_cse.h"
#include "coreopt.h"
#include "hsk_parser.h"

/* GC_max_retries, GC_gc_no */
#include "gc.h"

static char *phases[] = {
    [ 1]    "right after input parsing",
    [ 2]    "after parse/abstract syntax tree has been normalized",
    [ 3]    "after namespaces have been checked and resolved",
    [ 4]    "after variable scoping has been checked",
    [ 5]    "after XQuery built-in functions have been loaded",
    [ 6]    "after valid function usage has been checked",
    [ 7]    "after XML Schema predefined types have been loaded",
    [ 8]    "after XML Schema document has been imported (if any)",
    [ 9]    "after the abstract syntax tree has been mapped to Core",
    [10]    "after the Core tree has been simplified/normalized",
    [11]    "after type inference and checking",
    [12]    "after XQuery Core optimization",
    [13]    "after the Core tree has been translated to the internal algebra",
    [14]    "after the algebra tree has been rewritten/optimized",
    [15]    "after the common subexpression elimination on the algebra tree",
    [16]    "after the algebra has been translated to MIL",
    [17]    "after the MIL program has been serialized"
};

#define STOP_POINT(a) \
    if ((a) == PFstate.stop_after) \
        goto bailout;

/** global state of the compiler */
PFstate_t PFstate = {
    quiet               : false,
    timing              : false,
    print_dot           : false,
    print_pretty        : false,
    stop_after          : 0,
    print_types         : false,
    optimize            : false,
    print_parse_tree    : false,
    print_core_tree     : false,
    print_algebra_tree  : false,
    parse_hsk           : false
};

/**
 * @c basename(argv[0]) is stored here later. The basename() call may
 * modify its argument (according to the man page). To avoid modifying
 * @c argv[0] we make a copy first and store it here.
 */
static char *progname = 0;

/**
 * @c dirname(argv[0]) is stored here later. The dirname() call may
 * modify its argument (according to the manpage). To avoid modifying
 * @c argv[0] we make a copy first and store it here.
 */
/*static char *pathname = 0;*/ /* StM: unused! */

/**
 * Print abstract syntax tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_abssyn (PFpnode_t * proot)
{
    if (PFstate.print_dot)
        PFabssyn_dot (stdout, proot);

    if (PFstate.print_pretty)
        PFabssyn_pretty (stdout, proot);
}


/**
 * Print core tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_core (PFcnode_t * croot)
{
    if (PFstate.print_dot)
        PFcore_dot (stdout, croot);

    if (PFstate.print_pretty)
        PFcore_pretty (stdout, croot);
}

/**
 * Print algebra tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_algebra (PFalg_op_t * aroot)
{
    if (PFstate.print_dot)
        PFalg_dot (stdout, aroot);

    if (PFstate.print_pretty)
        PFalg_pretty (stdout, aroot);
}

#if 0
/**
 * Print MIL tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_mil (PFmnode_t * mroot)
{
    if (PFstate.print_dot)
        PFmil_dot (stdout, mroot);

    if (PFstate.print_pretty)
        PFmil_pretty (stdout, mroot);
}
#endif


static PFcnode_t * unfold_lets (PFcnode_t *c);

/**
 * Entry point to the Pathfinder compiler,
 * parses the command line (switches), then invokes the pipeline
 * of query processing steps (starting with the parser).
 */
int
main (int argc, char *argv[])
{
    PFpnode_t  *proot  = 0;
    PFcnode_t  *croot  = 0;
    PFalg_op_t *aroot  = 0;
    PFmil_t    *mroot  = 0;
    PFarray_t  *mil_program = 0;
    unsigned int i;

    /* fd of query file (if present) */
    int query = 0;

    /* elapsed time for compiler phase */
    long tm;

    /*
     * Determine basename(argv[0]) and dirname(argv[0]) on *copies*
     * of argv[0] as both functions may modify their arguments.
     */
#ifdef HAVE_BASENAME
    progname = basename (PFstrdup (argv[0]));
#else
    progname = PFstrdup (argv[0]);
#endif
    /*pathname = dirname (PFstrdup (argv[0]));*/ /* StM: unused! */

    /* getopt-based command line parsing */
    while (true) {
        int c;

#if HAVE_GETOPT_H && HAVE_GETOPT_LONG
        int option_index = 0;
        opterr = 1;
        c = getopt_long (argc, argv, "DHMOPTachpqrs:t", 
                         long_options, &option_index);
#else
        c = getopt (argc, argv, "DHMOPTachpqrs:t");
#endif

        if (c == -1)
            break;            /* end of command line */
        
        switch (c) {
        case 'h':
            printf ("Pathfinder XQuery Compiler ($Revision$, $Date$)\n");
            printf ("(c) University of Konstanz, DBIS group\n\n");
            printf ("Usage: %s [OPTION] [FILE]\n\n", argv[0]);            
            printf ("  Reads from standard input if FILE is omitted.\n\n");
            printf ("  -h%s: print this help message\n",
                    long_option (opt_buf, ", --%s", 'h'));
            printf ("  -q%s: do not print informational messages to log file\n",
                    long_option (opt_buf, ", --%s", 'q'));
            printf ("  -M%s: print MIL code (summer version) and stop\n",
                    long_option (opt_buf, ", --%s", 'M'));
            printf ("  -P%s: print internal tree structure human-readable\n",
                    long_option (opt_buf, ", --%s", 'P'));
            printf ("  -D%s: print internal tree structure in AT&T dot notation\n",
                    long_option (opt_buf, ", --%s", 'D'));
            printf ("  -T%s: print elapsed times for compiler phases\n",
                    long_option (opt_buf, ", --%s", 'T'));
            printf ("  -O%s: enable (expensive) optimizations\n",
                    long_option (opt_buf, ", --%s", 'O'));
            printf ("  -t%s: print static types (in {...}) for core\n",
                    long_option (opt_buf, ", --%s", 't'));
            printf ("  -s%s: stop processing after certain phase:\n",
                    long_option (opt_buf, ", --%s", 's'));

            for (i = 1; i < (sizeof (phases) / sizeof (char *)); i++)
                printf ("        %2u  %s\n", i, phases[i]);

            printf ("  -p%s: print internal parse tree\n",
                    long_option (opt_buf, ", --%s", 'p'));
            printf ("  -c%s: print internal Core language\n",
                    long_option (opt_buf, ", --%s", 'c'));
            printf ("  -a%s: stop after algebra tree generation\n",
                    long_option (opt_buf, ", --%s", 'a'));
            printf ("\n");
            printf ("Enjoy.\n");
            exit (0);
          
        case 'q':
            PFstate.quiet = true;
            break;

        case 'p':
            PFstate.print_parse_tree = true;
            break;

        case 'c':
            PFstate.print_core_tree = true;
            break;

        case 'a':
            PFstate.print_algebra_tree = true;
            break;

        case 's':
            PFstate.stop_after = atoi (optarg);
            if (PFstate.stop_after >= (sizeof (phases) / sizeof (*phases)))
                PFoops (OOPS_CMDLINEARGS,
                        "unrecognized stop point. Try `%s -h'", progname);
            break;

        case 'D':
            PFstate.print_dot = true;
            break;

        case 'H':
            PFstate.parse_hsk = true;
            break;

        case 'M':
            PFstate.summer_branch = true;
            break;

        case 'P':
            PFstate.print_pretty = true;
            break;

        case 'T':
            PFstate.timing = true;
            break;

        case 'O':
            PFstate.optimize = true;
            break;

        case 't':
            PFstate.print_types = true;
            break;

        default:
            (void) PFoops (OOPS_CMDLINEARGS, "try `%s -h'", progname);
            goto failure;
        }           /* end of switch */
    }           /* end of while */

    /* connect stdin to query file (if present) */
    if (optind < argc) {
        if ((query = open (argv[optind], O_RDONLY)) < 0) {
            PFoops (OOPS_FATAL, 
                    "cannot read query from file `%s': %s",
                    argv[optind],
                    strerror (errno));
            goto failure;
        }

        close (fileno (stdin));

        if (dup2 (query, fileno (stdin)) < 0) {
            PFoops (OOPS_FATAL,
                    "cannot dup query file: %s",
                    strerror (errno));
            goto failure;
        }
    }

    /* setup for garbage collector 
     */
  
    /* how often will retry GC before we report out of memory and give up? */
    GC_max_retries = 2;

    /* Parsing of Haskell XQuery to Algebra output */
    if (PFstate.parse_hsk)
    {
        aroot = PFhsk_parse ();
        goto subexelim;
    }

    /* compiler chain below 
     */
    tm = PFtimer_start ();
  
    /* Invoke parser on stdin (or whatever stdin has been dup'ed to) 
     */
    PFparse (&proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("parsing:\t\t %s", PFtimer_str (tm));

    STOP_POINT(1);
    
    tm = PFtimer_start ();

    /* Abstract syntax tree normalization 
     */
    proot = PFnormalize_abssyn (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("normalization:\t %s", PFtimer_str (tm));

    STOP_POINT(2);
    
    tm = PFtimer_start ();

    /* Resolve NS usage */
    PFns_resolve (proot);

    STOP_POINT(3);
    
    /* Check variable scoping and replace QNames by PFvar_t pointers */
    PFvarscope (proot);
  
    STOP_POINT(4);

    /* Load built-in XQuery F&O into function environment */
    PFfun_xquery_fo ();

    STOP_POINT(5);

    /* Resolve function usage 
     */
    PFfun_check (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("semantical analysis:\t %s", PFtimer_str (tm));

    STOP_POINT(6);

    tm = PFtimer_start ();

    /* Load XML Schema/XQuery predefined types into the type environment */
    PFty_predefined ();
    
    STOP_POINT(7);

    /* XML Schema import */
    PFschema_import (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("XML Schema import:\t %s", PFtimer_str (tm));

    STOP_POINT(8);

    tm = PFtimer_start ();

    /* XQuery core mapping
     */
    croot = PFfs (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("core mapping:\t\t %s", PFtimer_str (tm));

    STOP_POINT(9);

    /* Core simplification */
    tm = PFtimer_start ();

    croot = PFsimplify (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("core simplification:\t %s", PFtimer_str (tm));

    STOP_POINT(10);

    /* Type inference and check */
    tm = PFtimer_start ();
  
    croot = PFty_check (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("type checking:\t %s", PFtimer_str (tm));

    STOP_POINT(11);


    /* Core tree optimization */
    tm = PFtimer_start ();
  
    croot = PFcoreopt (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("core tree optimization:\t %s", PFtimer_str (tm));

    STOP_POINT(12);


    /*
     * FIXME:
     * This is the place where we should do some optimization stuff
     * on our core tree. This optimization should end in an unfolding
     * of all unneccessary `let' clauses; we need that for efficient
     * MIL code generation.
     * As we don't have nifty optimizations yet, we do unfolding right
     * here. Lateron, we put unfolding into (maybe twig based)
     * optimization code.
     */

#if 0
    /* ***** begin of temporary unfolding code ***** */

    tm = PFtimer_start ();

    croot = unfold_lets (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("let unfolding:\t %s", PFtimer_str (tm));

#ifdef DEBUG_UNFOLDING
    print_core (croot);
#endif

    /* ***** end of temporary unfolding code ***** */
#endif

    /*
     * generate temporary MIL Code (summer branch version)
     */

    if (PFstate.summer_branch) {
        tm = PFtimer_start ();
        PFprintMILtemp (stdout, croot);
        tm = PFtimer_stop (tm);

        if (PFstate.timing)
            PFlog ("MIL code output:\t %s", PFtimer_str (tm));
        goto bailout;
    }

    /*
     * map core to algebra tree
     */
    tm = PFtimer_start ();

    aroot = PFcore2alg (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("Algebra tree generation:\t %s", PFtimer_str (tm));

    STOP_POINT(13);

    /* Rewrite/optimize algebra tree */
    tm = PFtimer_start ();

    aroot = PFalgopt (aroot);

    tm = PFtimer_stop (tm);

    if (PFstate.timing)
        PFlog ("Algebra tree rewrite/optimization:\t %s", PFtimer_str (tm));

    STOP_POINT(14);

    tm = PFtimer_start ();

    /* 
     * common subexpression elimination in the algebra tree
     */
subexelim:
    tm = PFtimer_start ();

    aroot = PFcse_eliminate (aroot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("Common subexpression elimination in algebra tree:\t %s",
               PFtimer_str (tm));

    STOP_POINT(15);

    /* Map core to MIL */
    mroot = PFmilgen (aroot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("MIL code generation:\t %s", PFtimer_str (tm));

    STOP_POINT(16);

    /* Render MIL program in Monet MIL syntax 
     */
    if (!(mil_program = PFmil_serialize (mroot)))
        goto failure;

    STOP_POINT(17);

    /* Print MIL program to stdout */
    if (mil_program)
        PFmilprint (stdout, mil_program);

 bailout:
    /* Finally report on # of GCs run */
    if (PFstate.timing)
        PFlog ("#garbage collections:\t %d", (int) GC_gc_no);

    /* print abstract syntax tree if requested */
    if (PFstate.print_parse_tree) {
        if (proot) {
            if (PFstate.print_pretty) {
                printf ("Parse tree %s:\n", phases[PFstate.stop_after]);
                PFabssyn_pretty (stdout, proot);
            }
            if (PFstate.print_dot)
                PFabssyn_dot (stdout, proot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "parse tree not available at this point of compilation");
    }

    /* print core tree if requested */
    if (PFstate.print_core_tree) {
        if (croot) {
            if (PFstate.print_pretty) {
                printf ("Core tree %s:\n", phases[PFstate.stop_after]);
                PFcore_pretty (stdout, croot);
            }
            if (PFstate.print_dot)
                PFcore_dot (stdout, croot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "core tree not available at this point of compilation");
    }

    /* print algebra tree if requested */
    if (PFstate.print_algebra_tree) {
        if (aroot) {
            if (PFstate.print_pretty) {
                printf ("Algebra tree %s:\n", phases[PFstate.stop_after]);
                PFalg_pretty (stdout, aroot);
            }
            if (PFstate.print_dot)
                PFalg_dot (stdout, aroot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "core tree not available at this point of compilation");
    }

    exit (EXIT_SUCCESS);

 failure:
    exit (EXIT_FAILURE);
}

/**
 * Walk core tree @a e and replace occurrences of variable @a v
 * by core tree @a a (i.e., compute e[a/v]).
 *
 * @note Only copies the single node @a a for each occurence of
 *       @a v. Only use this function if you
 *       - either call it only with atoms @a a,
 *       - or copy @a a at most once (i.e. @a v occurs at most
 *         once in @a e).
 *       Otherwise we might come into deep trouble as children
 *       of @a e would have more than one parent afterwards...
 *
 * @param v variable to replace
 * @param a core tree to insert for @a v
 * @param e core tree to walk over
 * @return modified core tree
 */
static void
replace_var (PFvar_t *v, PFcnode_t *a, PFcnode_t *e)
{
  unsigned short int i;

  assert (v && a && e);

  if (e->kind == c_var && e->sem.var == v)
      *e = *a;
  else
      for (i = 0; (i < PFCNODE_MAXCHILD) && e->child[i]; i++)
          replace_var (v, a, e->child[i]);
}

/**
 * Worker for #unfoldable
 *
 * Walks recursively through the core tree fragment @a e and
 * tests if all occurences of @a v could be unfolded. The walk
 * is stopped immediately if anything violates the unfoldable
 * conditions.
 *
 * The parameter @a num_occur is considered as a static variable
 * for this tree walk, it is incremented with every occurence
 * of @a v. Obviously it should have an initial value of 0 when
 * called from outside (see #unfoldable).
 *
 * Parameter @a e1_is_atomic gives the information if we can
 * safely unfold although we counted more than one occurence
 * of @a v.
 *
 * @param v            the variable that should be unfolded
 * @param e            root node of current core sub-tree
 * @param num_occur    total occurences of this variable found so far;
 *                     should be 0 when called from outside.
 * @param e1_is_atomic Is the replacement for @a v an atom? In that
 *                     case @a v can also be replaced if there are
 *                     multiple occurences.
 */
static bool
unfoldable_ (PFvar_t *v, PFcnode_t *e, int *num_occur, bool e1_is_atomic)
{
    unsigned int i;

    /* if we found an occurence of this variable, count it */
    if ((e->kind == c_var) && (e->sem.var == v))
        (*num_occur)++;

    /*
     * if the bound expression was not an atom, abort if we counted
     * more than one occurence.
     */
    if ((*num_occur > 1) && !e1_is_atomic)
        return false;

    /* visit all our children and test for them */
    for (i = 0; i < PFCNODE_MAXCHILD && e->child[i]; i++)
        if (!unfoldable_ (v, e->child[i], num_occur, e1_is_atomic))
            return false;

    /*
     * We may not unfold if $v occurs in special places
     */
    switch (e->kind) {

        /*
         * Do not unfold $v in the `in' clause of FLWOR expressions
         */
        case c_for:      
            assert (e->child[2]->kind == c_var);
            if (e->child[2]->sem.var != v)
                return true;
            break;

        /*
         * Only unfold $v in an `if-then-else' condition
         * if e1 is an atom.
         */
        case c_ifthenelse:
            if (e1_is_atomic
                || (e->child[0]->kind != c_var)
                || (e->child[0]->sem.var != v))
                return true;
            break;

        /*
         * We can safely unfold variables here
         */
        case c_var:
        case c_lit_str:
        case c_lit_int:
        case c_lit_dec:
        case c_lit_dbl:
        case c_nil:
        case c_empty:
        case c_let:
        case c_seq:
            return true;

        default:
            break;
    }

    return false;
}

/**
 * Test if the expression
 *
 * @verbatim
     let $v := e1 return e2
@endverbatim
 * 
 * can be unfolded to
 *
 * @verbatim
     e2[e1/$v]
@endverbatim
 *
 * This is only possible, if some conditions are met (in order):
 *
 *  - $v must not occur as the `in' part of a FLWOR expression.
 *    (MIL code generation would fail otherwise, as iteration does
 *    not happen over a BAT variable.)
 *
 *  - If $v is an atom, we can safely unfold it.
 *
 *  - If $v occurs more than once in e2, do not unfold for performance
 *    reasons. (We don't want to re-evaluate e2 more than once.)
 *    This restriction is also implied by the implementation of
 *    replace_var().
 *
 *  - Note that we do not consider conditional expressions separately.
 *    This implies lazy evaluation as in the W3C drafts. (Particularly,
 *    errors within a twig of an if-then-else clause are only raised,
 *    if that twig is actually chosen by the if-then-else condition.)
 */
static bool
unfoldable (PFvar_t *v, PFcnode_t *e1, PFcnode_t *e2)
{
    bool e1_is_atomic;
    int  num_occur;

    /* see if e1 is atomic */
    switch (e1->kind) {
        case c_var:
        case c_lit_str:
        case c_lit_int:
        case c_lit_dec:
        case c_lit_dbl:
            e1_is_atomic = true;
            break;

        default:
            e1_is_atomic = false;
            break;
    }

    num_occur = 0;

    return unfoldable_ (v, e2, &num_occur, e1_is_atomic);
}

static PFcnode_t *
unfold_lets (PFcnode_t *c)
{
    unsigned int i;

    if (c->kind == c_let) {
        if (unfoldable (c->child[0]->sem.var, c->child[1], c->child[2])) {
            replace_var (c->child[0]->sem.var, c->child[1], c->child[2]);
            return unfold_lets (c->child[2]);
        }
    }

    for (i = 0; i < PFCNODE_MAXCHILD && c->child[i]; i++)
        c->child[i] = unfold_lets (c->child[i]);

    return c;
}

/* vim:set shiftwidth=4 expandtab: */
