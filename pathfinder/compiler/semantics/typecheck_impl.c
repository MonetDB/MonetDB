/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

/** 
 * @file		
 *	
 * Type inference (static semantics) and type checking for XQuery
 * core.  Auxiliary routines.  The type checking process itself is
 * twig-based.
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

#include <assert.h>

#include "pathfinder.h"
#include "typecheck.h"

#include "subtyping.h"
/* PFcnode_t */
#include "core.h"
/** twig-generated node type identifiers */
#include "typecheck.symbols.h"

/** twig: type of tree node */
#define TWIG_NODE PFcnode_t

/** twig: max number of children under a core tree node */
#define TWIG_MAXCHILD PFCNODE_MAXCHILD

/** twig: mapping core tree node types to twig node identifiers */
static int TWIG_ID[] = {
   [c_var]                var_       /**< variable */
 , [c_lit_str]            lit_str    /**< string literal */
 , [c_lit_int]            lit_int    /**< integer literal */
 , [c_lit_dec]            lit_dec    /**< decimal literal */
 , [c_lit_dbl]            lit_dbl    /**< double literal */
 , [c_nil]                nil        /**< nil */

 , [c_seq]                seq        /**< sequence construction */

 , [c_let]                let        /**< let binding */
 , [c_for]                for_       /**< for binding */

 , [c_apply]              apply      /**< function application */
 , [c_arg]                arg        /**< function argument (list) */

 , [c_ifthenelse]         ifthenelse /**< if-then-else conditional */

 , [c_typesw]             typeswitch /**< typeswitch */
 , [c_cases]              cases      /**< case rules (list) */
 , [c_case]               case_      /**< case rule */
 , [c_seqtype]            seqtype    /**< SequenceType */
 , [c_seqcast]            seqcast    /**< cast along <: */
 , [c_proof]              proof      /**< type checker only: prove <: rel. */

 , [c_locsteps]           locsteps
  
 , [c_ancestor]           ancestor
 , [c_ancestor_or_self]   ancestor_or_self
 , [c_attribute]          attribute
 , [c_child]              child_
 , [c_descendant]         descendant
 , [c_descendant_or_self] descendant_or_self
 , [c_following]          following
 , [c_following_sibling]  following_sibling
 , [c_parent]             parent_
 , [c_preceding]          preceding
 , [c_preceding_sibling]  preceding_sibling
 , [c_self]               self

 , [c_namet]              namet
 , [c_kind_node]          kind_node
 , [c_kind_comment]       kind_comment
 , [c_kind_text]          kind_text
 , [c_kind_pi]            kind_pi
 , [c_kind_doc]           kind_doc
 , [c_kind_elem]          kind_elem
 , [c_kind_attr]          kind_attr

 , [c_true]               true_      /**< built-in function `fn:true ()' */
 , [c_false]              false_     /**< built-in function `fn:false ()' */
 , [c_empty]              empty_     /**< empty sequence */

 , [c_root]               root_      /**< document root node */
};

/** twig: setup twig */
#include "twig.h"

#undef var_       
#undef lit_str    
#undef lit_int    
#undef lit_dec    
#undef lit_dbl    
#undef nil        
#undef seq        
#undef let        
#undef for_       
#undef apply      
#undef arg        
#undef ifthenelse 
#undef typeswitch 
#undef cases      
#undef case_      
#undef seqtype    
#undef upcast    
#undef downcast    
#undef locsteps
#undef ancestor
#undef ancestor_or_self
#undef attribute
#undef child_
#undef descendant
#undef descendant_or_self
#undef following
#undef following_sibling
#undef parent_
#undef preceding
#undef preceding_sibling
#undef self
#undef namet
#undef kind_node
#undef kind_comment
#undef kind_text
#undef kind_pi
#undef kind_doc
#undef kind_elem
#undef kind_attr
#undef true_ 
#undef false_
#undef empty_
#undef root_

/** mnemonic XQuery Core constructors */
#include "core_mnemonic.h"

/** 
 * Top (last) entry contains a pointer to the list of expected
 * paramater types for the currently type checked function.
 */
static PFarray_t *par_ty;

/**
 * Resolve function overloading.  In the list of functions of
 * the same name @a qn, find the first (most specific) to match the actual
 * argument types @a args (matching is based on <:).
 *
 * @attention NB. W3C XQuery FS 5.1.4 defines argument type matching
 * based on <: and `can be promoted to'.  The latter is ignored here.
 * We will have to find out if we can get away with <: only.
 *
 * @attention NB. This relies on the list of functions for name @a qn
 * to be sorted: the most specific instance comes first (see
 * semantics/xquery_fo.c)
 *
 * @param qn name of (overloaded) function
 * @param args right-deep core tree of function arguments 
 *             arg (e1, arg (e2, ..., arg (en, nil)...))
 */
static PFfun_t *
overload (PFqname_t qn, PFcnode_t *args)
{
    PFarray_t *fns;
    PFarray_t *args_str;
    PFcnode_t *arg;
    PFfun_t *fn;
    unsigned int i, a;
    bool match;
    char semi;

    assert (args && (args->kind == c_nil || args->kind == c_arg));

    fns = PFenv_lookup (PFfun_env, qn);
    assert (fns);

    for (i = 0; i < PFarray_last (fns); i++) {
        fn    = *(PFfun_t **) PFarray_at (fns, i);
        arg   = args;

        /* are all actual argument types in <: relationship with
         * expected formal parameter types?
         */
        for (a = 0, match = true; a < fn->arity; a++) {
            match = match && PFty_subtype (arg->child[0]->type,
                                           (fn->par_ty)[a]);
            if (!match)
                break;

            arg = arg->child[1];
            assert (arg);
        }

        /* yes, return this function (its the most specific match) */
        if (match)
            return fn;
    } 

    /* construct (error) message listing the actual argument types:
     * ": t1; t2; ...; tn" (NB: n >= 1 is guaranteed here)
     */
    args_str = PFarray (sizeof (char));

    semi = ':';

    do {
        PFarray_printf (args_str, "%c %s", 
                        semi,
                        PFty_str (args->child[0]->type));
        semi = ';';
        args = args->child[1];
    } while (args->kind != c_nil);

    PFoops (OOPS_TYPECHECK, 
            "no variant of function %s accepts the given argument type(s)%s",
            PFqname_str (qn),
            (char *) PFarray_at (args_str, 0));
}


/**
 * Apply specific typing rules for standard XQuery F&O functions
 * (see W3C XQuery, 7.2)
 *
 * @param fn function reference
 * @param args right-deep core tree of function arguments 
 * @return return type of @a fn when applied to arguments @a args
 */
static PFty_t
specific (PFfun_t *fn, PFcnode_t *args)
{
    assert (fn);
    assert (args && (args->kind == c_nil || args->kind == c_arg));

    return fn->ret_ty;
}


/**
 * Invoke type inference and type checking for core tree @a r
 *
 * @param r root of core tree
 * @return typed core tree
 */
PFcnode_t *
PFty_check (PFcnode_t *r)
{
    PFcnode_t *core;

    /* intialize stack of expected parameter types */
    par_ty = PFarray (sizeof (PFty_t *));

    /* invoke twig: the tree is traversed and type annotations are
     * attached (the type checkers removes `proofs' and may add
     * `seqcast's, so return the modified core tree);
     */
    core = rewrite (r, 0);

    /* sanity: no left over expected parameter type lists on the stack */
    assert (PFarray_empty (par_ty));

    return core;
}


/* vim:set shiftwidth=4 expandtab filetype=c: */
