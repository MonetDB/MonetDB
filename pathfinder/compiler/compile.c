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
 *  created by U Konstanz are Copyright (C) 2000-2004 University
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
    [16]    "after the algebra has been translated to MIL algebra",
    [17]    "after MIL algebra optimization/simplification",
    [18]    "after MIL algebra common subexpression elimination",
    [19]    "after the MIL algebra has been compiled into MIL commands",
    [20]    "after the MIL program has been serialized"
};

/* pretty ugly to have such a global, could not entirely remove it yet JF */
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
    print_ma_tree       : false,
    parse_hsk           : false,
    genType             : PF_GEN_ORG
};

#if 0
/**
 * Print abstract syntax tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_abssyn (FILE* pfout, PFstate_t* status, PFpnode_t * proot)
{
    if (status->print_dot)
        PFabssyn_dot ( pfout, proot);

    if (status->print_pretty)
        PFabssyn_pretty (pfout, proot);
}

/**
 * Print core tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_core (FILE* pfout, PFstate_t* status, PFcnode_t * croot)
{
    if (status->print_dot)
        PFcore_dot (pfout, croot);

    if (status->print_pretty)
        PFcore_pretty (pfout, croot);
}

/**
 * Print algebra tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
static void
print_algebra (FILE* pfout, PFstate_t* status, PFalg_op_t * aroot)
{
    if (status->print_dot)
        PFalg_dot (pfout, aroot);

    if (status->print_pretty)
        PFalg_pretty (pfout, aroot);
}

/**
 * Print MIL tree in dot notation or prettyprinted,
 * depending on command line switches.
 */
/*
static void
print_mil (FILE* pfout, PFstate_t* status, PFmnode_t * mroot)
{
    if (status->print_dot)
        PFmil_dot (pfout, mroot);

    if (status->print_pretty)
        PFmil_pretty (pfout, mroot);
}
*/
#endif

static PFcnode_t * unfold_lets (PFcnode_t *c);

#define STOP_POINT(a) \
    if ((a) == status->stop_after) \
        goto bailout;

/**
 * Linking test funn, remove if succeed
 */
int
pf_ping() {
        return 42;
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
    PFparse (pfin, &proot);

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
    if (status->timing)
        PFlog ("let unfolding:\t %s", PFtimer_str (tm));

#ifdef DEBUG_UNFOLDING
    print_core (pfout,status,croot);
#endif

    /* ***** end of temporary unfolding code ***** */
#endif

    /*
     * generate temporary MIL Code (summer branch version)
     */

    if (status->summer_branch) {
        tm = PFtimer_start ();
        PFprintMILtemp (pfout, croot, status);
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
 * by the Monet Runtime environment. In and out are the same. But the 
 * mode is different and is used to indicate "sax" or "xml" output.
 */
int
pf_compile_interface (FILE *pfin, FILE *pfout, char* mode)
{
        PFstate_t* status = &PFstate; /* incomplete */
        status->summer_branch = true;
        if ( strcmp(mode,"xml") == 0 ) {
                status->genType = PF_GEN_XML;
        } else if ( strcmp(mode,"sax") == 0 ) {
                status->genType = PF_GEN_SAX;
        } else {
                /* incomplete */
                fprintf(stderr,"pf_compile_interface: unkown output mode \"%s\", using \"xml\".\n",mode);
                status->genType = PF_GEN_XML;
        }
        int res = pf_compile(pfin,pfout,status);
        return res;
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
