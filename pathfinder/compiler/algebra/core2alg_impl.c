/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

/**
 * @file
 *
 * Auxiliary routines to compile XQuery Core into Relational Algebra.
 * The mapping process itself is twig-based.
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include <assert.h>

#include "core2alg.h"

/** twig-generated node type identifiers */
#include "core2alg.symbols.h"

#include "array.h"

/** twig: type of tree node */
#define TWIG_NODE PFcnode_t

/** twig: number of children under a Core tree node */
#define TWIG_MAXCHILD PFCNODE_MAXCHILD

static int TWIG_ID[] = {
      [c_var]          var_       /**< variable */
    , [c_lit_str]      lit_str    /**< string literal */
    , [c_lit_int]      lit_int    /**< integer literal */
    , [c_lit_dec]      lit_dec    /**< decimal literal */
    , [c_lit_dbl]      lit_dbl    /**< double literal */
    , [c_nil]          nil        /**< end-of-sequence marker */

    , [c_seq]          seq        /**< sequence construction */

    , [c_let]          let        /**< let binding */
    , [c_for]          for_       /**< for binding */

    , [c_locsteps]     locsteps   /**< path of location steps only */

    , [c_apply]        apply      /**< function application */
    , [c_arg]          arg        /**< function argument (list) */

    , [c_typesw]       typesw     /**< typeswitch clause */
    , [c_cases]        cases      /**< case concatenation for typeswitch */
    , [c_case]         case_      /**< single case for typeswitch */
    , [c_seqtype]      seqtype    /**< a SequenceType */
    , [c_seqcast]      seqcast    /**< cast along <: */
    , [c_proof]        proof      /**< type checker only: prove <: rel.ship */

    , [c_ifthenelse]   ifthenelse /**< if-then-else conditional */

    , [c_ancestor]            ancestor
    , [c_ancestor_or_self]    ancestor_or_self
    , [c_attribute]           attribute
    , [c_child]               child_
    , [c_descendant]          descendant
    , [c_descendant_or_self]  descendant_or_self
    , [c_following]           following
    , [c_following_sibling]   following_sibling
    , [c_parent]              parent_
    , [c_preceding]           preceding
    , [c_preceding_sibling]   preceding_sibling
    , [c_self]                self

    , [c_namet]               namet

    , [c_kind_node]           kind_node
    , [c_kind_comment]        kind_comment
    , [c_kind_text]           kind_text
    , [c_kind_pi]             kind_pi
    , [c_kind_doc]            kind_doc
    , [c_kind_elem]           kind_elem
    , [c_kind_attr]           kind_attr

    , [c_elem]                elem
    , [c_attr]                attr 
    , [c_text]                text
    , [c_doc]                 doc 
    , [c_comment]             comment
    , [c_pi]                  pi  
    , [c_tag]                 tag

    , [c_true]         true_      /**< Built-in function 'true' */
    , [c_false]        false_     /**< Built-in function 'false' */
    , [c_error]        error      /**< Built-in function 'error' */
    , [c_root]         root_      /**< Built-in function 'root' */
    , [c_empty]        empty_     /**< Built-in function 'empty' */
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
#undef locsteps   
#undef apply      
#undef arg        
#undef typesw     
#undef cases      
#undef case_      
#undef seqtype    
#undef seqcast     
#undef proof
#undef ifthenelse 
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
#undef instof 
#undef elem
#undef attr 
#undef text
#undef doc 
#undef comment
#undef pi  
#undef tag
#undef true_  
#undef false_ 
#undef error  
#undef root_  
#undef empty_ 

#include "algebra_mnemonic.h"
#include "subtyping.h"


/* Constructor for environment entry */
static PFalg_env_t enventry (PFvar_t *var, PFalg_op_t *op);


/**
 * Collect info on kind tests within an XPath expression.
 */
static PFalg_op_t *nameTest (PFqname_t  qname);
static PFalg_op_t *nodeTest (void);
static PFalg_op_t *commTest (void);
static PFalg_op_t *textTest (void);
static PFalg_op_t *piTest (void);
static PFalg_op_t *pitarTest (char *target);
static PFalg_op_t *docTest (void);
static PFalg_op_t *elemTest (void);
static PFalg_op_t *attrTest (void);

/**
 * Collect info on location steps within an XPath expression.
 */
static PFalg_op_t *anc (PFalg_op_t *n);
static PFalg_op_t *anc_self (PFalg_op_t *n);
static PFalg_op_t *attr (PFalg_op_t *n);
static PFalg_op_t *child (PFalg_op_t *n);
static PFalg_op_t *desc (PFalg_op_t *n);
static PFalg_op_t *desc_self (PFalg_op_t *n);
static PFalg_op_t *fol (PFalg_op_t *n);
static PFalg_op_t *fol_sibl (PFalg_op_t *n);
static PFalg_op_t *par (PFalg_op_t *n);
static PFalg_op_t *prec (PFalg_op_t *n);
static PFalg_op_t *prec_sibl (PFalg_op_t *n);
static PFalg_op_t *self (PFalg_op_t *n);

/* Concatenate the parameters of built-in functions. */
static PFalg_op_t * args (PFalg_op_t *arg, PFalg_op_t *args);

/** Create the tail of an argument list. */
static PFalg_op_t * args_tail(void);


static PFarray_t  *env = NULL;
static PFalg_op_t *loop = NULL;
static PFalg_op_t *delta __attribute__((unused)) = NULL;


PFalg_op_t *
PFcore2alg (PFcnode_t *c)
{
    PFalg_op_t *ret = NULL;

    assert (c);

    /* yet empty environment */
    env = PFarray (sizeof (PFalg_env_t));

    /* loop is initially a table with just one tuple */
    loop = lit_tbl (attlist ("iter"), tuple (lit_nat (1)));

    /*
     * We don't use our construction macro here, as some compilers
     * (e.g., icc) don't like empty __VA_ARGS__ macros.
     *
     delta = lit_tbl (attlist ("pre", "size", "level", "kind", "prop", "frag"));
     */
    delta = PFalg_lit_tbl_ (
            attlist ("pre", "size", "level", "kind", "prop", "frag"),
            0, (PFalg_tuple_t *) NULL);

    ret = rewrite (c, 0)->alg;

    if (!ret)
        PFoops (OOPS_FATAL, "Translation to Relational Algebra failed.");

    return serialize (ret);

    /*
    return serialize (lit_tbl (attlist ("pos", "item"),
                               tuple (lit_int (1), lit_int (42)),
                               tuple (lit_int (1), lit_str ("foo")),
                               tuple (lit_int (2), lit_int (43))));

    */
    /*
    return serialize (project (lit_tbl (attlist ("pos", "item"),
                                        tuple (lit_int (1), lit_int (42)),
                                        tuple (lit_int (2), lit_int (43))),
                               proj ("pos1", "pos"),
                               proj ("item1", "item")));
    */

    /*
    return
        serialize (
            cross (lit_tbl (attlist ("pos", "item"),
                            tuple (lit_int (1), lit_str ("foo")),
                            tuple (lit_int (2), lit_int (42))
                            ),
                   project (lit_tbl (attlist ("pos", "item"),
                                     tuple (lit_int (1), lit_str ("foo")),
                                     tuple (lit_int (2), lit_int (43))
                                     ),
                            proj ("pos1", "pos"), proj ("item1", "item"))));
    */

    /*
    return serialize (
            cross (
                project (
                    cross (lit_tbl (attlist ("pos"),
                                    tuple (lit_int (0)),
                                    tuple (lit_int (1))),
                           project (lit_tbl (attlist ("item"),
                                             tuple (lit_int (0))),
                                    proj ("item1", "item"))),
                    proj ("pos1", "pos")),
                cross (lit_tbl (attlist ("pos"),
                                tuple (lit_int (0)),
                                tuple (lit_int (1))),
                       project (lit_tbl (attlist ("item"),
                                         tuple (lit_int (0))),
                                proj ("item1", "item"))))
            );
    */

    /*
    return serialize (
              cross (lit_tbl (schema (att ("pos", aat_int),
                                      att ("item", aat_int)),
                              tuple (lit_int (1), lit_int (2))),
                     project (lit_tbl (schema (att ("pos", aat_int),
                                               att ("item", aat_int)),
                                       tuple (lit_int (1), lit_int (2))),
                              proj ("pos1", "pos"),
                              proj ("item1", "item"))
                     ));
    */
    /*
    return serialize (
              rownum (lit_tbl (schema (att ("pos", aat_int),
                                       att ("item", aat_int)),
                               tuple (lit_int (1), lit_int (2))),
                      "row", sortby("pos"), "item"));
                      */

}


/**
 * Construct a new entry to be inserted into the variable environment.
 * Called whenever a new variable is declared.
 */
static PFalg_env_t
enventry (PFvar_t *var, PFalg_op_t *op)
{
    return (PFalg_env_t) { .var = var, .op = op };
}


/*
 * Required for XPath processing, i.e. staircase join evaluation.
 * Dummy operator that collects the kind test information during
 * the bottom-up traversal of the core tree. The collected semantic
 * information will later be incorporated into the "real" staircase
 * join node.
 */
static PFalg_op_t *nameTest (PFqname_t qname)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_name;
    ret->sem.scjoin.str.qname = qname;

    return ret;
}

static PFalg_op_t *nodeTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_node;

    return ret;
}

static PFalg_op_t *commTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_comm;

    return ret;
}

static PFalg_op_t *textTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_text;

    return ret;
}

static PFalg_op_t *piTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_pi;

    return ret;
}

static PFalg_op_t *pitarTest (char *target)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_pi_tar;
    ret->sem.scjoin.str.target = target;

    return ret;
}

static PFalg_op_t *docTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_doc;

    return ret;
}

static PFalg_op_t *elemTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_elem;

    return ret;
}

static PFalg_op_t *attrTest (void)
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));

    ret->sem.scjoin.test = aop_at_tst;

    return ret;
}


/*
 * Required for XPath processing, i.e. staircase join evaluation.
 * Dummy operator that collects the location step information
 * during the bottom-up traversal of the core tree. The collected
 * semantic information will later be incorporated into the "real"
 * staircase join node.
 */
static PFalg_op_t *anc (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_anc;

    return n;
}

static PFalg_op_t *anc_self (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_anc_s;

    return n;
}

static PFalg_op_t *attr (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_attr;

    return n;
}

static PFalg_op_t *child (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_chld;

    return n;
}

static PFalg_op_t *desc (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_desc;

    return n;
}

static PFalg_op_t *desc_self (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_desc_s;

    return n;
}

static PFalg_op_t *fol (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_fol;

    return n;
}

static PFalg_op_t *fol_sibl (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_fol_s;

    return n;
}

static PFalg_op_t *par (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_par;

    return n;
}

static PFalg_op_t *prec (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_prec;

    return n;
}

static PFalg_op_t *prec_sibl (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_prec_s;

    return n;
}

static PFalg_op_t *self (PFalg_op_t *n)
{
    n->sem.scjoin.axis = aop_self;

    return n;
}


/**
 * Concatenate the parameters of built-in functions. @a args
 * contains the arguments seen so far, @a arg is the new
 * argument to be concatenated to the beginning of the list
 * (in order to maintain correct order of the incoming arguments,
 * because they arrive at this function from last to first).
 */
static PFalg_op_t * args (PFalg_op_t *arg, PFalg_op_t *args)
{
    PFarray_t *a = PFarray (sizeof (PFalg_op_t *));
    *((PFalg_op_t **) PFarray_add (a)) = arg;

    /* check if 'arg' is very first argument to arrive here */
    if (args->sem.builtin.args == NULL)
        args->sem.builtin.args = a;
    else
        args->sem.builtin.args = PFarray_concat (a,
                                 args->sem.builtin.args);

    return args;
}

/**
 * Create the tail of an argument list.
 */
static PFalg_op_t * args_tail()
{
    PFalg_op_t *ret = PFmalloc (sizeof (PFalg_op_t));
    ret->sem.builtin.args = NULL;

    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
