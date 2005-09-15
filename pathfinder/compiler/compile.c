/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler driver for the Pathfinder compiler
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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#if HAVE_SIGNAL_H
#include <signal.h>
#endif

#include "compile.h"
#include "compile_interface.h"
#include "parser.h"       /* parsing XQuery syntax */
#include "scanner.h"
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
#include "logdebug.h"
#include "timer.h"
#include "fs.h"           /* core mapping (formal semantics) */
#include "types.h"        /* type system */
#include "import.h"       /* XML Schema import */
#include "simplify.h"     /* core simplification */
#include "coreopt.h"
#include "typecheck.h"    /* type inference and check */
#include "core2alg.h"     /* Compile Core to Relational Algebra */
#include "algopt.h"
#include "algebra_cse.h"
#include "planner.h"
#include "physdebug.h"
#include "milgen.h"       /* MIL command tree generation */
#include "milprint.h"     /* create string representation of MIL tree */
#include "milprint_summer.h" /* create MILcode directly from the Core tree */
#include "oops.h"
#include "mem.h"
#include "coreopt.h"

static char *phases[] = {
    [ 1]  = "right after input parsing",
    [ 2]  = "after parse/abstract syntax tree has been normalized",
    [ 3]  = "after namespaces have been checked and resolved",
    [ 4]  = "after variable scoping has been checked",
    [ 5]  = "after XQuery built-in functions have been loaded",
    [ 6]  = "after valid function usage has been checked",
    [ 7]  = "after XML Schema predefined types have been loaded",
    [ 8]  = "after XML Schema document has been imported (if any)",
    [ 9]  = "after the abstract syntax tree has been mapped to Core",
    [10]  = "after the Core tree has been simplified/normalized",
    [11]  = "after type inference and checking",
    [12]  = "after XQuery Core optimization",
    [13]  = "after the Core tree has been translated to the logical algebra",
    [14]  = "after the logical algebra tree has been rewritten/optimized",
    [15]  = "after the CSE on the logical algebra tree",
    [16]  = "after compiling logical into the physical algebra",
    [17]  = "after compiling the physical algebra into MIL code",
    [18]  = "after the MIL program has been serialized"
};

/* pretty ugly to have such a global, could not entirely remove it yet JF */
/** global state of the compiler */
PFstate_t PFstate = {
    .debug               = 1,
    .timing              = false,
    .print_dot           = false,
    .print_pretty        = false,
    .stop_after          = 0,
    .print_types         = false,
    .optimize            = 1,
    .print_parse_tree    = false,
    .print_core_tree     = false,
    .print_la_tree       = false,
    .print_pa_tree       = false,
    .summer_branch       = true,

    .format              = NULL,

    .genType             = "xml"
};

jmp_buf PFexitPoint;

PFquery_t PFquery = {
    .version            = "1.0",
    .encoding           = NULL,
    .ordering           = true,  /* implementation defined: ordered */
    .empty_order        = undefined,
    .inherit_ns         = false, /* implementation def'd: inherit-ns: no */
};

/** Compilation stage we've last been in. */
unsigned int last_stage = 0;

#define STOP_POINT(a) \
    last_stage = (a); \
    if ((a) == status->stop_after) \
        goto bailout;

void segfault_handler (int sig);

/**
 * helper function: read input file into a buffer 
 */
static char* 
pf_read(FILE *pfin) {
    size_t off = 0, len = 2048, n;
    char* buf = (char*) malloc(len);
    while((n = fread(buf+off, 1, len-off-1, pfin)) > 0) {
	off += n;
        if (off >= len - 1) {
            len *= 2;
            buf = (char*) realloc(buf, len);
        }
    }
    buf[off] = 0;
    return buf;
}
   

/**
 * Compiler driver of the Pathfinder compiler,
 * It invokes the pipeline of query processing steps
 * (starting with the parser).
 */
int
pf_compile (FILE *pfin, FILE *pfout, PFstate_t *status)
{
    PFpnode_t  *proot  = NULL;
    PFcnode_t  *croot  = NULL;
    PFla_op_t  *laroot  = NULL;
    PFpa_op_t  *paroot = NULL;
    PFmil_t    *mroot  = NULL;
    PFarray_t  *mil_program = NULL;
    char       *xquery = NULL;

    /* elapsed time for compiler phase */
    long tm, tm_first;

    lexical_init();

#if HAVE_SIGNAL_H
    /* setup sementation fault signal handler */
    signal (SIGSEGV, segfault_handler);
#endif

#if 0
    /* Parsing of Haskell XQuery to Algebra output */
    if (status->parse_hsk)
    {
        laroot = PFhsk_parse ();
        goto subexelim;
    }
#endif

    /* compiler chain below 
     */
  
    /* Invoke parser on stdin (or whatever stdin has been dup'ed to)
     */
    xquery = pf_read(pfin);
    tm_first = tm = PFtimer_start ();
    PFparse (xquery, &proot);
    tm = PFtimer_stop (tm);
    free(xquery);

    if (status->timing)
        PFlog ("parsing:\t\t %s", PFtimer_str (tm));

    STOP_POINT(1);
    
    tm = PFtimer_start ();

    /* Abstract syntax tree normalization 
     */
    proot = PFnormalize_abssyn (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
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
    if (status->timing)
        PFlog ("semantical analysis:\t %s", PFtimer_str (tm));

    STOP_POINT(6);

    tm = PFtimer_start ();

    /* Load XML Schema/XQuery predefined types into the type environment */
    PFty_predefined ();
    
    STOP_POINT(7);

    /* XML Schema import */
    PFschema_import (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("XML Schema import:\t %s", PFtimer_str (tm));

    STOP_POINT(8);

    tm = PFtimer_start ();

    /* XQuery core mapping
     */
    croot = PFfs (proot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core mapping:\t\t %s", PFtimer_str (tm));

    STOP_POINT(9);

    /* Core simplification */
    tm = PFtimer_start ();

    croot = PFsimplify (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core simplification:\t %s", PFtimer_str (tm));

    STOP_POINT(10);

    /* Type inference and check */
    tm = PFtimer_start ();
  
    croot = PFty_check (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("type checking:\t %s", PFtimer_str (tm));

    STOP_POINT(11);


    /* Core tree optimization */
    tm = PFtimer_start ();
  
    croot = PFcoreopt (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("core tree optimization:\t %s", PFtimer_str (tm));

    STOP_POINT(12);

    /*
     * generate temporary MIL Code (summer branch version)
     */

    if (status->summer_branch) {
        char *prologue = NULL, *query = NULL, *epilogue = NULL;
        tm = PFtimer_start ();
        if (PFprintMILtemp (croot, status, tm_first, &prologue, &query, &epilogue))
            goto failure;
        fputs(prologue, pfout);
        fputs(query, pfout);
        /* epilogue is not necessary for standalone scripts */ 
        tm = PFtimer_stop (tm);
        if (status->timing)
            PFlog ("MIL code output:\t %s", PFtimer_str (tm));
        goto bailout;
    }

    /*
     * map core to algebra tree
     */
    tm = PFtimer_start ();

    laroot = PFcore2alg (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("logical algebra tree generation:\t %s", PFtimer_str (tm));

    STOP_POINT(13);

    /* Rewrite/optimize algebra tree */
    tm = PFtimer_start ();

    laroot = PFalgopt (laroot);

    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("logical algebra tree rewrite/optimization:\t %s",
               PFtimer_str (tm));

    STOP_POINT(14);

    tm = PFtimer_start ();

    /* 
     * common subexpression elimination in the algebra tree
     */
subexelim:
    tm = PFtimer_start ();

    laroot = PFla_cse (laroot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("common subexpression elimination in logical algebra tree:\t %s",
               PFtimer_str (tm));

    STOP_POINT(15);

    /* Compile algebra into physical algebra */
    tm = PFtimer_start ();
    paroot = PFplan (laroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("compilation to physical algebra:\t %s", PFtimer_str (tm));

    STOP_POINT(16);

    /* Map physical algebra to MIL */
    tm = PFtimer_start ();
    mroot = PFmilgen (paroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("MIL code generation:\t %s", PFtimer_str (tm));

    STOP_POINT(17);

    /* Render MIL program in Monet MIL syntax 
     */
    if (!(mil_program = PFmil_serialize (mroot)))
        goto failure;

    STOP_POINT(18);

    /* Print MIL program to pfout */
    if (mil_program)
        PFmilprint (pfout, mil_program);

 bailout:
    /* print abstract syntax tree if requested */
    if (status->print_parse_tree) {
        if (proot) {
            if (status->print_pretty) {
                if (status->debug != 0)
                    printf ("Parse tree %s:\n", phases[status->stop_after]);
                PFabssyn_pretty (pfout, proot);
            }
            if (status->print_dot)
                PFabssyn_dot (pfout, proot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "parse tree not available at this point of compilation");
    }

    /* print core tree if requested */
    if (status->print_core_tree) {
        if (croot) {
            if (status->print_pretty) {
                if (status->debug != 0)
                    printf ("Core tree %s:\n", phases[status->stop_after]);
                PFcore_pretty (pfout, croot);
            }
            if (status->print_dot)
                PFcore_dot (pfout, croot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "core tree not available at this point of compilation");
    }

    /* print algebra tree if requested */
    if (status->print_la_tree) {
        if (laroot) {
            if (status->print_pretty) {
                if (status->debug != 0)
                    printf ("Algebra tree %s:\n", phases[status->stop_after]);
                /* PFalg_pretty (pfout, aroot); */
            }
            if (status->print_dot)
                PFla_dot (pfout, laroot);
                /* PFalg_dot (pfout, laroot); */
        }
        else
            PFinfo (OOPS_NOTICE,
                    "logical algebra tree not available "
                    "at this point of compilation");
    }

    /* print MIL algebra tree if requested */
    if (status->print_pa_tree) {
        if (paroot) {
            if (status->print_pretty) {
                PFinfo (OOPS_WARNING,
                        "Cannot prettyprint physical algebra tree. Sorry.");
            }
            if (status->print_dot)
                PFpa_dot (pfout, paroot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "Physical algebra tree not available at this "
                    "point of compilation");
    }

    return ( 1 ); /* EXIT_SUCCES */

 failure:
    return ( -1 ); /* EXIT_FAILURE */
}

/**
 * Compiler driver of the Pathfinder compiler interface for usage
 * by the Monet Runtime environment. 
 * 
 * mode is used to indicate "sax", "xml" or "none" output.
 *
 * MonetDB actually would like pathfinder to 
 * - be thread-safe (now there are global vars all over the place) 
 * - use string input/output rather than files.
 *
 * This interface fixes the second issue. For the moment, the MonetDB
 * Runtime environment uses a lock to stay stable under concurrent requests. 
 */
char*
pf_compile_MonetDB (char *xquery, char* mode, char** prologue, char** query, char** epilogue)
{
	PFpnode_t  *proot  = NULL;
	PFcnode_t  *croot  = NULL;
        long tm = PFtimer_start ();
        int res;

        *prologue = NULL;
        *query = NULL;
        *epilogue = NULL;

        pf_alloc = pa_create();
        lexical_init();

        PFstate.invocation = invoke_monetdb;
        PFstate.summer_branch = true;

        if (strncmp(mode,"timing",6) == 0 ) {
                PFstate.timing = 1;
                mode += 7;
        } else {
                PFstate.timing = 0;
        }
        if (strncmp(mode,"debug",5) == 0 ) {
                PFstate.debug = 1;
                mode += 6;
        }
        PFstate.genType = mode;
        if (setjmp(PFexitPoint) != 0 ) {
                return PFerrbuf;
        }
	/* repeat pf_compile, which we can't reuse as we don't want to deal with files here */
        PFparse (xquery, &proot);
        proot = PFnormalize_abssyn (proot);
        PFns_resolve (proot);
        PFvarscope (proot);
        PFfun_xquery_fo ();
        PFfun_check (proot);
        PFty_predefined ();
        PFschema_import (proot);
        croot = PFfs (proot);
        croot = PFsimplify (croot);
        croot = PFty_check (croot);
    	croot = PFcoreopt (croot);
        res = PFprintMILtemp (croot, &PFstate, tm, prologue, query, epilogue);
        pa_destroy(pf_alloc);
        return res ? PFerrbuf : NULL;
}

#if HAVE_SIGNAL_H
/**
 * This handler is called whenever we get a SIGSEGV.
 *
 * It will print out some informative message and then terminate the program.
 */
RETSIGTYPE
segfault_handler (int sig)
{
    fprintf (stderr, "!ERROR: Segmentation fault.\n");
    fprintf (stderr,
             "The Pathfinder compiler experienced an internal problem.\n");
    fprintf (stderr,
             "You may want to report this problem to the Pathfinder \n");
    fprintf (stderr,
             "development team (pathfinder@inf.uni-konstanz.de).\n\n");
    fprintf (stderr,
             "When reporting problems, please attach your XQuery input,\n");
    fprintf (stderr,
             "as well as the following information:\n");
    fprintf (stderr, "  Invocation: ");

    switch (PFstate.invocation) {
        case invoke_cmdline: fprintf (stderr, "command line\n");
                             break;
        case invoke_monetdb: fprintf (stderr, "MonetDB\n");
                             break;
        default:             fprintf (stderr, "unknown\n");
                             break;
    }

    fprintf (stderr, "  Compilation stage: %u\n\n", last_stage);

    fprintf (stderr, "We apologize for the inconvenience...\n\n");

    signal (sig, SIG_DFL);
    raise (sig);
}
#endif

/* vim:set shiftwidth=4 expandtab: */
