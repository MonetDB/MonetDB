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

#include "algebra.h"
#include "algebra_mnemonic.h"
#include "subtyping.h"


/* Constructor for environment entry */
static PFalg_env_t enventry (PFvar_t *var,
                             PFalg_op_t *result, PFalg_op_t *doc);


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
static struct PFalg_pair_t args (struct PFalg_pair_t arg,
                                 struct PFalg_pair_t args);

/** Create the tail of an argument list. */
static struct PFalg_pair_t args_tail(void);


static PFarray_t  *env = NULL;
static PFalg_op_t *loop = NULL;
static PFalg_op_t *delta __attribute__((unused)) = NULL;
static PFalg_op_t *empty_doc __attribute__((unused)) = NULL;

/**
 * Given an XQuery type @a ty, an algebra expression @a e, and the
 * loop relation @a loop, return an algebra expression that returns
 * the relation with schema (iter, subty), so that for each iter value
 * in @a loop there exists one tuple, with the subty attribute set to
 * true, if @a e has a subtype of @a ty, and false otherwise.
 */
static PFalg_op_t *
type_test (PFty_t ty, PFalg_pair_t e, PFalg_op_t *loop)
{
    PFalg_op_t *itemty;

    /*
     * Collect algebra expression with schema (iter,pos,itemty)
     * so that itemty is true for any item that is a subtype of
     * ty, and false otherwise.
     *
     * The surface language only allows QNames for predefined
     * types, or node kind tests. Fortunately, only few atomic
     * types are predefined: xs:integer, xs:decimal, xs:double,
     * xs:boolean, xs:string. For all of them we have an algebra
     * correspondance.
     * 
     * We first consider the case that ty is the empty sequence.
     * (This cannot be entered on the surface language. But it
     * may be introduced during core generation/optimization.
     * And we want to avoid nasty bugs here, when that case would
     * be caught in the following cases.)
     *
     *        /                        subty \
     *       | dist (proj_iter (e)) X ------- |       (non-empty iters)
     *        \                        false /
     *                           U
     *    /                                 subty \
     *   | (loop \ dist (proj_iter (e))) X ------- |  (empty iters)
     *    \                                 false /
     *
     */
    if (PFty_subtype (ty, PFty_empty ()))
        return
            disjunion (
                cross (
                    distinct (project (e.result, proj ("iter", "iter"))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (false)))),
                cross (
                    difference (
                        loop,
                        distinct (project (e.result, proj ("iter", "iter")))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (true)))));
    /*
     * To test, e.g., for integer values, use
     *
     *   proj_iter,pos,itemty (type_itemty:item/int (e))
     *
     */
    else if (PFty_subtype (ty, PFty_star (PFty_xs_integer ())))
        itemty = project (type (e.result, "itemty", "item", aat_int),
                          proj ("iter", "iter"),
                          proj ("pos", "pos"),
                          proj ("itemty", "itemty"));
    else if (PFty_subtype (ty, PFty_star (PFty_xs_decimal ())))
        /* xs:integer is a subtype of xs:decimal.
         * Test for both types. The `type' operator merely adds a boolean
         * column. We OR them after testing for for both types.
         */
        itemty =
            project (
                or (type (type (e.result, "isint", "item", aat_int),
                          "isdec", "item", aat_dec),
                    "itemty", "isint", "isdec"),
                proj ("iter", "iter"),
                proj ("pos", "pos"),
                proj ("itemty", "itemty"));
    else if (PFty_subtype (ty, PFty_star (PFty_xs_double ())))
        itemty = project (type (e.result, "itemty", "item", aat_dbl),
                          proj ("iter", "iter"),
                          proj ("pos", "pos"),
                          proj ("itemty", "itemty"));
    else if (PFty_subtype (ty, PFty_star (PFty_xs_boolean ())))
        itemty = project (type (e.result, "itemty", "item", aat_bln),
                          proj ("iter", "iter"),
                          proj ("pos", "pos"),
                          proj ("itemty", "itemty"));
    else if (PFty_subtype (ty, PFty_star (PFty_xs_string ())))
        itemty = project (type (e.result, "itemty", "item", aat_str),
                          proj ("iter", "iter"),
                          proj ("pos", "pos"),
                          proj ("itemty", "itemty"));
    else
        PFoops (OOPS_FATAL,
                "Sorry, I cannot translate the test for type `%s'",
                PFty_str (ty));


    /*
     * Second part is the test for the occurence indicator.
     */

    /*
     * Ocurrence indicator `1' (exactly one item).
     *
     * seqty1_subty:item/iter (proj_iter,item:itemty (itemty))
     *                    U
     *  /                              subty\
     * | (loop \ proj_iter (itemty)) X ----- |
     *  \                              false/
     *
     * (First part considers all iterations with length of at
     * least one: The itemty expression contains true/false values
     * as determined above. The seqty1 operator sets true for all
     * those `iter' groups, where there is exactly one tuple with
     * value `true', and false otherwise. The second part of the
     * union considers all the empty sequences. They do not match
     * the occurrence indicator and are thus set to false.)
     */
    if (PFty_subtype (ty, PFty_item ()))
        return
            disjunion (
                seqty1 (project (itemty,
                                 proj ("iter", "iter"),
                                 proj ("item", "itemty")),
                        "subty", "item", "iter"),
                cross (
                    difference (
                        loop,
                        project (itemty, proj ("iter", "iter"))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (false)))));

    /*
     * Ocurrence indicator `?' (zero or one item).
     *
     * seqty1_subty:item/iter (proj_iter,item:itemty (itemty))
     *                    U
     *  /                              subty\
     * | (loop \ proj_iter (itemty)) X ----- |
     *  \                              true /
     *
     * In contrast to `1', we return true for all empty sequences.
     */
    if (PFty_subtype (ty, PFty_opt (PFty_item ())))
        return
            disjunion (
                seqty1 (project (itemty,
                                 proj ("iter", "iter"),
                                 proj ("item", "itemty")),
                        "subty", "item", "iter"),
                cross (
                    difference (
                        loop,
                        project (itemty, proj ("iter", "iter"))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (true)))));

    /*
     * Ocurrence indicator `+' (one or more items).
     *
     * all_subty:item/iter (proj_iter,item:itemty (itemty))
     *                    U
     *  /                              subty \
     * | (loop \ proj_iter (itemty)) X -----  |
     *  \                              false /
     *
     * Groupwise test if all tuples in itemty carry a `true'.
     * This makes all iterations true that contain only items
     * that satisfy the type test, and false all those that
     * contain at least one item that does not satisfy the
     * type test. We are left with considering the empty sequences
     * that do not qualify for the name test. We return false for
     * them.
     */
    if (PFty_subtype (ty, PFty_plus (PFty_item ())))
        return
            disjunion (
                all (project (itemty,
                              proj ("iter", "iter"), proj ("item", "itemty")),
                     "subty", "item", "iter"),
                cross (
                    difference (
                        loop,
                        project (itemty, proj ("iter", "iter"))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (false)))));

    /*
     * Ocurrence indicator `*' (zero or more items).
     *
     * all_subty:item/iter (proj_iter,item:itemty (itemty))
     *                    U
     *  /                              subty\
     * | (loop \ proj_iter (itemty)) X ----  |
     *  \                              true /
     *
     * Almost the same as `+', but return true for empty sequences.
     */
    if (PFty_subtype (ty, PFty_star (PFty_item ())))
        return
            disjunion (
                all (project (itemty,
                              proj ("iter", "iter"), proj ("item", "itemty")),
                     "subty", "item", "iter"),
                cross (
                    difference (
                        loop,
                        project (itemty, proj ("iter", "iter"))),
                    lit_tbl (attlist ("subty"), tuple (lit_bln (true)))));

    /*
     * We should never reach this point.
     */
    PFoops (OOPS_FATAL, "Error in type_test().");
}

PFalg_op_t *
PFcore2alg (PFcnode_t *c)
{
    PFcnode_t *ret = NULL;

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

    empty_doc = PFalg_lit_tbl_ (
                attlist ("pre", "size", "level", "kind", "prop", "frag"),
                0, (PFalg_tuple_t *) NULL);

    ret =  rewrite (c, 0);

    if (!ret)
        PFoops (OOPS_FATAL, "Translation to Relational Algebra failed.");

    return serialize (ret->alg.doc, ret->alg.result);

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

#if 0
/**
 * Implementation type that we use for a given XQuery type.
 * Returns the algebra type that matches a given XQuery type
 * best.
 *
 * Possible uses for this function:
 *  - As a helper to compile the XQuery @c cast operator. We pick the
 *    implementation type that best matches the requested type.
 *  - Cast types before invoking a built-in operation (see the
 *    `apply' translation in core2alg.mt.sed).
 *
 * CURRENTLY UNUSED.
 */
static PFalg_simple_type_t
implty (PFty_t ty)
{
    if (PFty_subtype (ty, PFty_star (PFty_xs_integer ())))
        return aat_int;
    else if (PFty_subtype (ty, PFty_star (PFty_xs_decimal ())))
        return aat_dec;
    else if (PFty_subtype (ty, PFty_star (PFty_xs_double ())))
        return aat_dbl;
    else if (PFty_subtype (ty, PFty_star (PFty_xs_boolean ())))
        return aat_bln;
    else if (PFty_subtype (ty, PFty_star (PFty_xs_string ())))
        return aat_str;
    else {
        PFinfo (OOPS_WARNING,
                "Don't know what the implementation type is of `%s'.",
                PFty_str (ty));
        PFinfo (OOPS_WARNING,
                "I will simply try use the string type.");
        PFinfo (OOPS_WARNING,
                "If you know it, feel free to fix implty() in core2alg_impl.c");
        return aat_str;
    }
}
#endif

/**
 * Construct a new entry to be inserted into the variable environment.
 * Called whenever a new variable is declared.
 */
static PFalg_env_t
enventry (PFvar_t *var, PFalg_op_t *result, PFalg_op_t *doc)
{
    return (PFalg_env_t) { .var = var, .result = result, .doc = doc };
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
    ret->sem.scjoin.str.target = PFstrdup (target);

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
static struct PFalg_pair_t args (struct PFalg_pair_t arg,
                                 struct PFalg_pair_t args)
{
    PFarray_t *a = PFarray (sizeof (struct  PFalg_pair_t));
    *((struct PFalg_pair_t *) PFarray_add (a)) = arg;

    /* check if 'arg' is very first argument to arrive here */
    if (args.result->sem.builtin.args == NULL)
        args.result->sem.builtin.args = a;
    else
        args.result->sem.builtin.args = PFarray_concat (a,
                                 args.result->sem.builtin.args);

    return args;
}

/**
 * Create the tail of an argument list.
 */
static struct PFalg_pair_t args_tail()
{
    struct  PFalg_pair_t ret;
    
    ret.result = PFmalloc (sizeof (PFalg_op_t));
    ret.doc = empty_doc;

    ret.result->sem.builtin.args = NULL;

    return ret;
}

/* vim:set shiftwidth=4 expandtab: */
