/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Entry point to Pathfinder compiler.
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
 * For each such successful NS test, if the NS prefix @ns has been
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
 * @subsection type_inference Type Inference and Check
 *
 * Type inference walks the core expression, inferring a type for each
 * node.  Types are attached to the core tree nodes to facilitate type
 * checking.
 * 
 * PFty_check() decorates the core tree with type annotations but
 * lets the tree alone otherwise.
 * 
 * @subsection core2mil Mapping Core to MIL (Monet Interface Language)
 *
 * The final output of our compiler will be a MIL program, MIL being the
 * interface language understood by the Monet database system. Our
 * MIL program will depend on a Monet module "pathfinder" that provides
 * additional types and functionality (e.g. staircase join) to execute
 * the query.
 *
 * PFcore2mil() maps the core tree into a tree structure describing the
 * MIL program (with #PFmnode_t nodes). This program can than be converted
 * to a string representation with PFmilprint() and sent to the Monet
 * database system. A description of this mapping scheme is given in
 * a separate section of this documentation.
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
 * line option <code>-D</code>. (In the example, `<code>-p</code>' makes
 * Pathfinder stop after parsing and print out the parse tree.)
 @verbatim
 $ echo '/foo/bar' | bin/Pathfinder -Dp | dot -Tps -o foo.ps
 @endverbatim
 * See the dot manpage for more information about dot.
 *
 * @subsection prettyprinting Prettyprinting
 *
 * Output in human readable form is done with the help of a prettyprinting
 * algorithm. The output should be suitable to be viewed on a 80 character
 * wide ASCII terminal. For some data structures a colored output might
 * be available. Escape sequences are generated that allow for colored
 * output on xterm windows. Note that this might become ugly on other
 * terminal types. Prettyprinted output can be selected with the command
 * line option <code>-P</code>:
 @verbatim
 $ echo '/foo/bar' | bin/Pathfinder -Pp
 @endverbatim
 *
 * @subsection timing Timing Output
 *
 * If you're interested in performance of the compiler, the command line
 * option <code>-T</code> might be your choice. For several compilation
 * phases, timing statistics are printed to stdout.
 *
 * @subsection abstract_syntax Abstract Syntax
 *
 * The abstract syntax tree (parse tree, generated by the @ref parsing "Parser")
 * can be printed out in either dot notation (use command line switch @c -D)
 * or in human readable form (use command line switch @c -P). You make
 * Pathfinder stop immediately after parsing with the command line option
 * <code>-p</code>.
 *
 * @subsection normalization Normalization
 *
 * After parsing, the abstract syntax tree is normalized (see above).
 * To stop Pathfinder immediately after doing this, use the command line
 * option <code>-n</code>.
 *
 * @subsection semantic_analysis Semantic Analysis
 *
 * The normalization phase is followed by a semantic analysis of the
 * abstract syntax tree. This includes variable scoping, function ``scoping'',
 * and namespace resolution. To stop Pathfinder immediately after doing
 * this, use the command line option <code>-s</code>.
 *
 *
 * @subsection types Static typing for the core language
 *
 * Pathfinder performs static type checking for the generated XQuery
 * core programs.  Passing <code>-t</code> to Pathfinder lets the
 * compiler print type annotations (in braces <code>{ }</code>) along
 * with core program output.
 *
 * @section testing A Word on Testing
 *
 * @subsection simple_tests Simple Tests for Single Queries
 *
 * By either printing out tree structures with the @ref prettyprinting
 * "Prettyprinting" option of the Pathfinder binary or generating dot
 * output and piping it to the dot utility (see @ref treeprinting),
 * single queries can be tested on the command line.
 *
 * @subsection postscript_batch Generating Trees as PostScript Files in Batch
 *
 * If you want to run many queries to test your changes to the code,
 * you can specify them in a file and use the @c query-batch script
 * to generate the PostScript file. Put your queries into an ascii
 * file and seperate your queries with a '--' on a single line.
 * @c query-batch will generate a PostScript file, that contains
 * all queries and the generated tree structures. Use a PostScript
 * viewer or your printer to check the result.
 *
 * To generate PostScript output of the Abstract Syntax Tree this way
 * call @c query-batch like this:
 @verbatim
 $ test/query-batch -a -p -b bin/Pathfinder your_input_file.xq
 $ gv your_input_file.xq.ps &
 @endverbatim
 *
 * If the query file you generated might be useful for the future, use
 * .xq as a file extension and drop the file into the @c test subdirectory.
 * Also see the next chapters for automated testing.
 *
 * @subsection diff_batch Automated Checking Using Reference Files
 *
 * The @c query-batch script can also compare the output of the binary
 * against a given reference. This reference file should have the file
 * name of the input file plus the extension '.abssyn' for abstract
 * syntax tree. It contains the @b human @b readable (prettyprinted)
 * representation of the abstract syntax tree, where the output of
 * different queries is separated by '--' again.
 *
 * To start this 'diff' test, invoke @c query-batch like this:
 @verbatim
 $ test/query-batch -a -d -b bin/Pathfinder your_input_file.xq
 @endverbatim
 *
 * You don't have to write your reference files by hand. Use the '-r'
 * option of @c query-batch to let it create a reference file for you.
 @verbatim
 $ test/query-batch -a -r -b bin/Pathfinder your_input_file.xq
 $ ls your_input_file.xq*
 your_input_file.xq
 your_input_file.xq.ref
 @endverbatim
 *
 * @note @c query-batch does not generate a '.abssyn' file to not overwrite
 *   an existing file. To use your new reference file, rename it
 *   accordingly.
 *
 * @warning Check the reference file or the PostScript output of your
 *   queries thoroughly before creating your reference file. Simply
 *   generating the '.ref' file and copying it to '.abssyn' makes the
 *   whole test system useless!
 *
 * @subsection fast_test The Fast Test Method
 *
 * If you run @c make with the @c test target
 @verbatim
 $ make test
 @endverbatim
 * all @c *.xq files in the @c test subdirectory will automatically be
 * tested against the reference files in the same directory. The output
 * is printed to @c stdout.
 *
 * <b>
 * Always run this test before checking in any changes! Never check in
 * anything that does not pass @c make @c test !
 * </b>
 *
 * If tests fail, there can be two reasons:
 *  - The code is buggy. @b Solution: fix it.
 *  - Your modifications might have changed the data structures in our
 *    compiler and/or its output. @b Solution: thoroughly check
 *    if everything is ok and modify the @e reference files.
 *
 * If no test queries exist that address the problem you have just been
 * working on, create new queries. Group them in a sensible way in one
 * or more files and drop them into the @c test directory. Don't forget
 * to also create the reference file(s) and add both to the CVS repository.
 *
 * @section credits Credits
 *
 * This is a project of the
 * <a href="http://www.inf.uni-konstanz.de/dbis/">Database and Information
 * Systems Group</a> at <a href="http://www.uni-konstanz.de/">University
 * of Konstanz, Germany</a>. The project is lead by Torsten Grust
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
 * Netherlands</a>.
 */

#include <stdlib.h>
#include <libgen.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>

#include "pf_config.h"

#if HAVE_GETOPT_H
#include <getopt.h>

/**
 * long (GNU-style) command line options and their abbreviated
 * one-character variants.  Keep this list SORTED in ascending
 * order of one-character option names.
 */
static struct option long_options[] = {
    { "print-att-dot",               0, NULL, 'D' },
    { "optimize",                    0, NULL, 'O' },
    { "print-human-readable",        0, NULL, 'P' },
    { "timing",                      0, NULL, 'T' },
    { "stop-after-core-compilation", 0, NULL, 'c' },
    { "daemon",                      0, NULL, 'd' },
    { "help",                        0, NULL, 'h' },
    { "log",                         0, NULL, 'l' },
    { "stop-after-mil-generation",   0, NULL, 'm' },
    { "stop-after-normalizing",      0, NULL, 'n' },
    { "output-type",                 0, NULL, 'o' },
    { "stop-after-parsing",          0, NULL, 'p' },
    { "quiet",                       0, NULL, 'q' },
    { "stop-after-semantics",        0, NULL, 's' },
    { "typing",                      0, NULL, 't' },
    { NULL,                          0, NULL, 0 }
};

/**
 * character buffer large enough to hold longest
 * command line option plus some extra formatting space
 */
static char opt_buf[sizeof ("stop-after-core-compilation") + 8];

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

#include "pathfinder.h"
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
#include "mildebug.h"
#include "daemon.h"
#include "timer.h"
#include "fs.h"           /* core mapping (formal semantics) */
#include "types.h"        /* type system */
#include "import.h"       /* XML Schema import */
#include "simplify.h"     /* core simplification */
#include "typecheck.h"    /* type inference and check */
#include "core2mil.h"     /* mapping core --> MIL */
#include "milprint.h"     /* create string representation of MIL tree */

#include "subtyping.h"

/* GC_max_retries, GC_gc_no */
#include "gc.h"

/** global state of the compiler */
PFstate_t PFstate = {
    quiet               : false,
    daemon              : false,
    timing              : false,
    print_dot           : false,
    print_pretty        : false,
    stop_after          : phas_all,
    print_types         : false,
    output_type         : output_monet,
    optimize            : false
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
static char *pathname = 0;

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


static PFcnode_t * unfold_lets (PFcnode_t *c);

/**
 * Entry point to the Pathfinder compiler,
 * parses the command line (switches), then invokes the pipeline
 * of query processing steps (starting with the parser).
 */
int
main (int argc, char *argv[])
{
    PFpnode_t *proot  = 0;
    PFcnode_t *croot  = 0;
    PFmnode_t *mroot  = 0;
    PFarray_t *mil_program = 0;
    PFrc_t rc;

    FILE *logf;
    char *logfn = 0;

    /* elapsed time for compiler phase */
    long tm;

    /*
     * Determine basename(argv[0]) and dirname(argv[0]) on *copies*
     * of argv[0] as both functions may modify their arguments.
     */
    progname = basename (PFstrdup (argv[0]));
    pathname = dirname (PFstrdup (argv[0]));

    /* getopt-based command line parsing */
    while (true) {
        int c;

#if HAVE_GETOPT_H
        int option_index = 0;
        opterr = 1;
        c = getopt_long (argc, argv, "DOPTcd:hl:mno:pqst", 
                         long_options, &option_index);
#else
        c = getopt (argc, argv, "DOPTcd:hl:mno:pqst");
#endif

        if (c == -1)
            break;            /* end of command line */
        
        switch (c) {
        case 'h':
            printf ("Pathfinder Compiler, revision $Revision$ ($Date$)\n");
            printf ("(c) University of Konstanz, DBIS group\n\n");
            printf ("Usage: %s [OPTION]...\n\n", argv[0]);            
            printf ("  -h%s: print this help message\n",
                    long_option (opt_buf, ", --%s", 'h'));
            printf ("  -q%s: do not print informational messages to log file\n",
                    long_option (opt_buf, ", --%s", 'q'));
            printf ("  -l file%s: specify log file name (default stderr)\n",
                    long_option (opt_buf, ", --%s file", 'l'));
            printf ("  -d port%s: act as dæmon listening on specified TCP port\n",
                    long_option (opt_buf, ", --%s port", 'd'));
            printf ("  -o monet|xterm|html%s: specify output markup\n"
                    "     (default: monet)\n",
                    long_option (opt_buf, ", --%s monet|xterm|html", 'o'));
            printf ("  -P%s: print internal tree structure human-readable\n",
                    long_option (opt_buf, ", --%s", 'P'));
            printf ("  -D%s: print internal tree structure in AT&T dot notation\n",
                    long_option (opt_buf, ", --%s", 'D'));
            printf ("  -T%s: print elapsed times for compiler phases\n",
                    long_option (opt_buf, ", --%s", 'T'));
            printf ("  -O%s: enable (expensive) optimizations\n",
                    long_option (opt_buf, ", --%s", 'O'));
            printf ("  -t%s: print static types (in {...}) for core (implies -P)\n",
                    long_option (opt_buf, ", --%s", 't'));
            printf ("  -p%s: stop processing after parsing\n",
                    long_option (opt_buf, ", --%s", 'p'));
            printf ("  -s%s: stop after semantical analysis\n",
                    long_option (opt_buf, ", --%s", 's'));
            printf ("  -n%s: stop after core tree normalization\n",
                    long_option (opt_buf, ", --%s", 'n'));
            printf ("  -c%s: stop after core language generation\n",
                    long_option (opt_buf, ", --%s", 'c'));
            printf ("  -m%s: stop after MIL code generation\n",
                    long_option (opt_buf, ", --%s", 'm'));
            printf ("\n");
            printf ("Enjoy.\n");
            exit (0);
          
        case 'q':
            PFstate.quiet = true;
            break;

        case 'd':
            PFstate.daemon = true;

            if ((rc = PFdaemonize (atoi (optarg)))) {
                PFoops (rc, 
                        "dæmon cannot listen on port `%s': %s",
                        optarg,
                        strerror (errno));
                goto bailout;
            }
            break;
        
        case 'l': 
            /* yank leading blanks, tabs in command line */
            logfn = PFstrdup (strtok (optarg, " \t"));
            break;

        case 'o': 
            if (!strcmp (optarg, "monet"))
                PFstate.output_type = output_monet;
            else if (!strcmp (optarg, "xterm"))
                PFstate.output_type = output_xterm;
            else if (!strcmp (optarg, "html"))
                PFstate.output_type = output_html;
            else
                PFoops (OOPS_CMDLINEARGS, "illegal output type `%s'", optarg);
            break;

        case 'p':
            if (PFstate.stop_after > phas_parse)
                PFstate.stop_after = phas_parse;
            break;

        case 'n':
            if (PFstate.stop_after > phas_simpl)
                PFstate.stop_after = phas_simpl;
            break;

        case 's':
            if (PFstate.stop_after > phas_semantics)
                PFstate.stop_after = phas_semantics;
            break;

        case 'c':
            if (PFstate.stop_after > phas_fs)
                PFstate.stop_after = phas_fs;
            break;

        case 'm':
            if (PFstate.stop_after > phas_mil)
                PFstate.stop_after = phas_mil;
            break;

        case 'D':
            PFstate.print_dot = true;
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
            PFstate.print_pretty = true;
            break;

        default:
            (void) PFoops (OOPS_CMDLINEARGS, "try `%s -h'", progname);
            goto bailout;
        }           /* end of switch */
    }           /* end of while */
        
    /* reroute stderr to log file 
     * if this has been requested (via `-l') 
     */
    if (logfn) {
        if ((logf = fopen (logfn, "a")) == NULL) {
            PFoops (OOPS_FATAL,
                    "cannot append to log file `%s': %s",
                    logfn,
                    strerror (errno));
            goto bailout;
        }
    
        close (fileno (stderr));
    
        if (dup2 (fileno (logf), fileno (stderr)) < 0) {
            PFoops (OOPS_FATAL,
                    "failed to reroute log messages: %s",
                    strerror (errno));
            goto bailout;
        }
    }

    /* setup for garbage collector 
     */
  
    /* how often will retry GC before we report out of memory and give up? */
    GC_max_retries = 2;

    /* compiler chain below 
     */
    tm = PFtimer_start ();
  
    /* Invoke parser on stdin (or whatever stdin has been dup'ed to) 
     */
    if (PFparse (&proot))
        goto bailout;

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("parsing:\t\t %s", PFtimer_str (tm));

    if (PFstate.stop_after == phas_parse) {
        print_abssyn (proot);
        goto bailout;
    }
    
    tm = PFtimer_start ();

    /* Abstract syntax tree normalization 
     */
    proot = PFnormalize_abssyn (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("normalization:\t %s", PFtimer_str (tm));

    tm = PFtimer_start ();

    /* Resolve NS usage 
     */
    PFns_resolve (proot);

    /* Check variable scoping and replace QNames by PFvar_t pointers 
     */
    if (PFvarscope (proot))
        goto bailout;
  
    /* Load built-in XQuery F&O into function environment 
     */
    PFfun_xquery_fo ();

    /* Resolve function usage 
     */
    PFfun_check (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("semantical analysis:\t %s", PFtimer_str (tm));

    if (PFstate.stop_after == phas_semantics) {
        print_abssyn (proot);
        goto bailout;
    }

    tm = PFtimer_start ();

    /* Load XML Schema built-in types into type enviroment 
     */
    PFty_xs_builtins ();

    /* XML Schema import 
     */
    PFschema_import (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("XML Schema import:\t %s", PFtimer_str (tm));

    tm = PFtimer_start ();

    /* XQuery core mapping
     */
    croot = PFfs (proot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("core mapping:\t\t %s", PFtimer_str (tm));

    if (PFstate.stop_after == phas_fs) {
        print_core (croot);
        goto bailout;
    }

    /* Core simplification
     */
    tm = PFtimer_start ();

    croot = PFsimplify (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("core simplification:\t %s", PFtimer_str (tm));

    if (PFstate.stop_after == phas_simpl) {
        print_core (croot);
        goto bailout;
    }

    /* Type inference and check
     */
    tm = PFtimer_start ();
  
    croot = PFty_check (croot);

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("type checking:\t %s", PFtimer_str (tm));

    if (PFstate.print_types)
        print_core (croot);


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



    tm = PFtimer_start ();

    /* Map core to MIL 
     */
    if (PFcore2mil (croot, &mroot))
        goto bailout;

    tm = PFtimer_stop (tm);
    if (PFstate.timing)
        PFlog ("MIL code generation:\t %s", PFtimer_str (tm));

    if (PFstate.stop_after == phas_mil) {
        print_mil (mroot);
        goto bailout;
    }

    /* Render MIL program in Monet MIL syntax 
     */
    if (!(mil_program = PFmil_gen (mroot)))
        goto bailout;

    /* Print MIL program to stdout
     */
    if (mil_program)
        /* fprintf (stdout, "%s", mil_program); */
        PFmilprint (stdout, mil_program);

 bailout:
    /* Finally report on # of GCs run */
    if (PFstate.timing)
        PFlog ("#garbage collections:\t %d", (int) GC_gc_no);

    exit (EXIT_SUCCESS);
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
