/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Compiler driver for the Pathfinder compiler
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
 *  created by U Konstanz are Copyright (C) 2000-2005 University
 *  of Konstanz. All Rights Reserved.
 *
 *  Contributors:
 *          Torsten Grust <torsten.grust@uni-konstanz.de>
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *          Jan Flokstra <flokstra@cs.utwente.nl>
 *
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
#include "ma_debug.h"
#include "timer.h"
#include "fs.h"           /* core mapping (formal semantics) */
#include "types.h"        /* type system */
#include "import.h"       /* XML Schema import */
#include "simplify.h"     /* core simplification */
#include "typecheck.h"    /* type inference and check */
#include "algebra.h"      /* algebra tree */
#include "core2alg.h"     /* Compile Core to Relational Algebra */
#include "algopt.h"
#include "algebra_cse.h"
#include "ma_gen.h"       /* MIL algebra generation */
#include "ma_opt.h"
#include "ma_cse.h"
#include "milgen.h"       /* MIL command tree generation */
#include "milprint.h"     /* create string representation of MIL tree */
#include "milprint_summer.h" /* create MILcode directly from the Core tree */
#include "oops.h"
#include "mem.h"
#include "coreopt.h"
#include "hsk_parser.h"

/* GC_max_retries, GC_gc_no */
#include "gc.h"

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
    [13]  = "after the Core tree has been translated to the internal algebra",
    [14]  = "after the algebra tree has been rewritten/optimized",
    [15]  = "after the common subexpression elimination on the algebra tree",
    [16]  = "after the algebra has been translated to MIL algebra",
    [17]  = "after MIL algebra optimization/simplification",
    [18]  = "after MIL algebra common subexpression elimination",
    [19]  = "after the MIL algebra has been compiled into MIL commands",
    [20]  = "after the MIL program has been serialized"
};

/* pretty ugly to have such a global, could not entirely remove it yet JF */
/** global state of the compiler */
PFstate_t PFstate = {
    .quiet               = false,
    .timing              = false,
    .print_dot           = false,
    .print_pretty        = false,
    .stop_after          = 0,
    .print_types         = false,
    .optimize            = 1,
    .print_parse_tree    = false,
    .print_core_tree     = false,
    .print_algebra_tree  = false,
    .print_ma_tree       = false,
    .parse_hsk           = false,
    .summer_branch       = true,
    .genType             = PF_GEN_XML
};

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
    size_t off = 0, len = 65536;
    char* buf = (char*) PFmalloc(len);
    while(fread(buf+off, len-off, 1, pfin) == len-off) {
	off = len; len *= 2;
	buf = (char*) PFrealloc(len, buf);
    }
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
    PFalg_op_t *aroot  = NULL;
    PFma_op_t  *maroot = NULL;
    PFmil_t    *mroot  = NULL;
    PFarray_t  *mil_program = NULL;

    /* elapsed time for compiler phase */
    long tm;

#if HAVE_SIGNAL_H
    /* setup sementation fault signal handler */
    signal (SIGSEGV, segfault_handler);
#endif

    /* setup for garbage collector 
     */
  
    /* how often will retry GC before we report out of memory and give up? */
    GC_max_retries = 2;

    /* Parsing of Haskell XQuery to Algebra output */
    if (status->parse_hsk)
    {
        aroot = PFhsk_parse ();
        goto subexelim;
    }

    /* compiler chain below 
     */
    tm = PFtimer_start ();
  
    /* Invoke parser on stdin (or whatever stdin has been dup'ed to)
     */
    PFparse (pf_read(pfin), &proot);

    tm = PFtimer_stop (tm);
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
        tm = PFtimer_start ();
        fputs(PFprintMILtemp (croot, status), pfout);
        tm = PFtimer_stop (tm);

        if (status->timing)
            PFlog ("MIL code output:\t %s", PFtimer_str (tm));
        goto bailout;
    }

    /*
     * map core to algebra tree
     */
    tm = PFtimer_start ();

    aroot = PFcore2alg (croot);

    tm = PFtimer_stop (tm);
    if (status->timing)
        PFlog ("Algebra tree generation:\t %s", PFtimer_str (tm));

    STOP_POINT(13);

    /* Rewrite/optimize algebra tree */
    tm = PFtimer_start ();

    aroot = PFalgopt (aroot);

    tm = PFtimer_stop (tm);

    if (status->timing)
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
    if (status->timing)
        PFlog ("Common subexpression elimination in algebra tree:\t %s",
               PFtimer_str (tm));

    STOP_POINT(15);

    /* Compile algebra into two-column MIL algebra */
    tm = PFtimer_start ();
    maroot = PFma_gen (aroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("Compilation to MIL algebra:\t %s", PFtimer_str (tm));

    STOP_POINT(16);

    /* MIL algebra common subexpression elimination */
    tm = PFtimer_start ();
    maroot = PFma_opt (maroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("MIL algebra optimization:\t %s", PFtimer_str (tm));

    STOP_POINT(17);

    /* MIL algebra common subexpression elimination */
    tm = PFtimer_start ();
    maroot = PFma_cse (maroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("MIL algebra CSE:\t %s", PFtimer_str (tm));

    STOP_POINT(18);

    /* Map core to MIL */
    tm = PFtimer_start ();
    mroot = PFmilgen (maroot);
    tm = PFtimer_stop (tm);

    if (status->timing)
        PFlog ("MIL code generation:\t %s", PFtimer_str (tm));

    STOP_POINT(19);

    /* Render MIL program in Monet MIL syntax 
     */
    if (!(mil_program = PFmil_serialize (mroot)))
        goto failure;

    STOP_POINT(20);

    /* Print MIL program to pfout */
    if (mil_program)
        PFmilprint (pfout, mil_program);

 bailout:
    /* Finally report on # of GCs run */
    if (status->timing)
        PFlog ("#garbage collections:\t %d", (int) GC_gc_no);

    /* print abstract syntax tree if requested */
    if (status->print_parse_tree) {
        if (proot) {
            if (status->print_pretty) {
                if (!status->quiet)
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
                if (!status->quiet)
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
    if (status->print_algebra_tree) {
        if (aroot) {
            if (status->print_pretty) {
                if (!status->quiet)
                    printf ("Algebra tree %s:\n", phases[status->stop_after]);
                PFalg_pretty (pfout, aroot);
            }
            if (status->print_dot)
                PFalg_dot (pfout, aroot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "core tree not available at this point of compilation");
    }

    /* print MIL algebra tree if requested */
    if (status->print_ma_tree) {
        if (maroot) {
            if (status->print_pretty) {
                PFinfo (OOPS_WARNING,
                        "Cannot prettyprint MIL algebra tree. Sorry.");
            }
            if (status->print_dot)
                PFma_dot (pfout, maroot);
        }
        else
            PFinfo (OOPS_NOTICE,
                    "MIL algebra tree not available at this "
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
pf_compile_MonetDB (char *xquery, char* mode)
{
	PFpnode_t  *proot  = NULL;
	PFcnode_t  *croot  = NULL;

        PFstate.invocation = invoke_monetdb;
        PFstate.summer_branch = true;
        PFstate.genType = PF_GEN_XML;

        if ( strcmp(mode,"dm") == 0 ) {
                PFstate.genType = PF_GEN_DM;
        } else if ( strcmp(mode,"sax") == 0 ) {
                PFstate.genType = PF_GEN_SAX;
        } else if ( strcmp(mode,"none") == 0 ) {
                PFstate.genType = PF_GEN_NONE;
        } else if ( strcmp(mode,"org") == 0 ) {
                PFstate.genType = PF_GEN_ORG;
        } else if ( strcmp(mode,"xml") ) {
                fprintf(stderr,"pf_compile_interface: unkown output mode \"%s\", using \"xml\".\n",mode);
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
        return PFprintMILtemp (croot, &PFstate);
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
