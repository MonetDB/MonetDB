/* -*- mode:C; c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*-*/

/**
 * @file
 *
 * Constructors for the XQuery Core language.  
 *
 * Users of these constructors may want to use abbreviations for the
 * real constructor names, e.g.,
 *
 *   #define let PFcore_let
 *
 * (see include/core_cons.h which does these `#define's for you).
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

/*
 * NOTE (Revision Information):
 *
 * Changes in the Core2MIL_Summer2004 branch have been merged into
 * this file on July 15, 2004. I have tagged this file in the
 * Core2MIL_Summer2004 branch with `merged-into-main-15-07-2004'.
 *
 * For later merges from the Core2MIL_Summer2004, please only merge
 * the changes since this tag.
 *
 * Jens
 */

#include "pathfinder.h"

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdarg.h>

/* PFcore_t */
#include "core.h"

/* PFvar_t */
#include "variable.h"
/* PFarray_t */
#include "array.h"
/* PFpnode_t */
#include "abssyn.h"
#include "mem.h"
#include "oops.h"
/* needed for function conversion */
#include "subtyping.h"

/** 
 * Allocates a new core tree leaf and initializes its kind.
 *
 * @param  kind type of new leaf
 * @return new core tree leaf
 */
PFcnode_t * 
PFcore_leaf (PFctype_t kind)
{
    PFcnode_t *core;
    int c;

    core = (PFcnode_t *) PFmalloc (sizeof (PFcnode_t));
  
    for (c = 0; c < PFCNODE_MAXCHILD; c++)
        core->child[c] = 0;

    core->kind = kind;

    /* static type initialized to none */
    core->type = PFty_none ();
    core->alg = (struct PFalg_pair_t) { .rel = NULL, .frag = NULL};
  
    return core;
}

/**
 * Allocates and initializes a new core tree node @a n with given @a
 * kind, then wires single child node @a n1.
 *
 * @param  kind type of new node
 * @param  n1   pointer to child
 * @return new parse tree node
 */
PFcnode_t *
PFcore_wire1 (PFctype_t kind, PFcnode_t *n1) 
{
    PFcnode_t *core;

    assert (n1);

    core = PFcore_leaf (kind);
    core->child[0] = n1;

    return core;
}

/**
 * Allocates and initializes a new core tree node @a n with given @a
 * kind, then wires two child nodes @a n1 and @a n2.
 *
 * @param  kind type of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @return new parse tree node
 */
PFcnode_t *
PFcore_wire2 (PFctype_t kind, PFcnode_t *n1, PFcnode_t *n2) 
{
    PFcnode_t *core;
    
    assert (n1 && n2);

    core = PFcore_wire1 (kind, n1);
    core->child[1] = n2;

    return core;
} 

/**
 * Allocates and initializes a new core tree node @a n with given @a
 * kind, then wires three child nodes @a n1, @a n2, and @a n3.
 *
 * @param  kind type of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @param  n3   pointer to third child
 * @return new parse tree node
 */
PFcnode_t *
PFcore_wire3 (PFctype_t kind, PFcnode_t *n1, PFcnode_t *n2, PFcnode_t *n3) 
{
    PFcnode_t *core;

    assert (n1 && n2 && n3);

    core = PFcore_wire2 (kind, n1, n2);
    core->child[2] = n3;

    return core;
} 

/**
 * Allocates and initializes a new core tree node @a n with given @a
 * kind, then wires four child nodes @a n1, @a n2, @a n3 and @a n4.
 *
 * @param  kind type of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @param  n3   pointer to third child
 * @param  n4   pointer to fourth child
 * @return new parse tree node
 */
PFcnode_t *
PFcore_wire4 (PFctype_t kind,
         PFcnode_t *n1, PFcnode_t *n2, PFcnode_t *n3, PFcnode_t *n4) 
{
    PFcnode_t *core;

    assert (n1 && n2 && n3 && n4);

    core = PFcore_wire3 (kind, n1, n2, n3);
    core->child[3] = n4;

    return core;
} 

/** 
 * Create a core @c nil node to terminate lists or denote optionsl subtrees.
 */
PFcnode_t *
PFcore_nil (void)
{
    return PFcore_leaf (c_nil);
}


/**
 * maximum size of variable name
 */
#define VNAME_DOT "dot"
#define VNAME_MAX sizeof (VNAME_DOT "_000")

/**
 * Generate a new (unique) variable. This unique name is created by
 * incrementing a number that is appended to the variable name.  A
 * prefix may be given that may not be longer than 3 characters. If no
 * prefix is given (by passing 0), the default `v' is used. The
 * namespace for the newly created variable is the internal Pathfinder
 * namespace `pf:...' (PFns_pf).
 *
 * @param prefix The prefix to use. The variable name will be
 *   "[prefix]_[num]", where [prefix] is the prefix and [num] is
 *   a three digit, zero-padded integer number
 * @return the new variable
 */
PFvar_t *
PFcore_new_var (char *prefix)
{
    static unsigned int vars = 0;
    char                vname[VNAME_MAX];
    int                 l;

    /* prefix may not exceed 3 characters */
    assert (!prefix || strlen (prefix) < sizeof (VNAME_DOT));

    /* construct new var name */
    if (prefix)
        l = snprintf (vname, VNAME_MAX, "%3s_%03u", prefix, vars);
    else
        l = snprintf (vname, VNAME_MAX, "v_%03u", vars);
    
    /* warn if we needed to truncate the variable name
     * (this does not affect the correct core mapping but may
     * confuse the user)
     */
    if ((PFstate.print_dot || PFstate.print_pretty) && 
        (size_t) l >= VNAME_MAX)
        PFinfo (OOPS_NOTICE, "truncated variable name `%s' in core query",
                vname);

    /* ensure uniqueness */
    vars++;

    /* assign internal Pathfinder namespace `#pf:...' */
    return PFnew_var (PFqname (PFns_pf, PFstrdup (vname)));
}

/**
 * Create a new core tree node representing a variable usage
 * @param v a pointer to a #PFvar_t struct, uniquely identifying
 *   a variable
 * @return a new core tree node
 */
PFcnode_t *
PFcore_var (PFvar_t *v)
{
    PFcnode_t *core;
  
    assert (v);

    core = PFcore_leaf (c_var);
    core->sem.var = v;

    return core;
}

/**
 * Create a new core tree node representing an integer literal
 *
 * @param i the integer to represent
 * @return a new core tree node
 */
PFcnode_t *
PFcore_num (int i)
{
    PFcnode_t *core;
  
    core = PFcore_leaf (c_lit_int);
    core->sem.num = i;

    return core;
}

/**
 * Create a new core tree node representing a decimal literal
 * @param d the decimal to represent
 * @return A new core tree node
 */
PFcnode_t *
PFcore_dec (double d)
{
    PFcnode_t *core;

    core = PFcore_leaf (c_lit_dec);
    core->sem.dec = d;

    return core;
}

/**
 * Create a new core tree node representing a double literal
 * @param d the double to represent
 * @return a new core tree node
 */
PFcnode_t *
PFcore_dbl (double d)
{
    PFcnode_t *core;
  
    core = PFcore_leaf (c_lit_dbl);
    core->sem.dbl = d;

    return core;
}

/**
 * Create a new core tree node representing a string literal
 *
 * @param s the string to represent
 * @return a new core tree node
 */
PFcnode_t *
PFcore_str (char *s)
{
    PFcnode_t *core;
  
    assert (s);

    core = PFcore_leaf (c_lit_str);
    core->sem.str = PFstrdup (s);

    return core;
}

/**
 * Create a core tree node to represent an XQuery type, e.g., in
 * typeswitch or cast expressions.
 *
 * @param t an XQuery type
 * @return a core tree node representing the type.
 */
PFcnode_t *
PFcore_seqtype (PFty_t t)
{
    PFcnode_t *core;

    core = PFcore_leaf (c_seqtype);
    core->sem.type = t;

    return core;
}

/**
 * Create a core tree node to representing a cast along the <:
 * relationship:
 *
 *   seqast (t, e)
 *
 * NB: the static or dynamic semantics need to make sure that
 *     e->type <: t or t <: e->type
 *
 * @param t SequenceType
 * @param e expression to cast
 * @return a core tree node representing the cast
 */
PFcnode_t *
PFcore_seqcast (PFcnode_t *t, PFcnode_t *e)
{
    assert (t && (t->kind == c_seqtype || t->kind == c_stattype));

    return PFcore_wire2 (c_seqcast, t, e);
}

/**
 * Create a core tree node representing a subtyping proof
 * e1 <: t (if successful return e2 else yield static type error):
 *
 *   proof (e1, t, e2)
 *
 * @param e1 expression to test
 * @param t  SequenceType to test against
 * @param e2 guarded expression
 * @return   a core tree node representing the proof
 */
PFcnode_t *
PFcore_proof (PFcnode_t *e1, PFcnode_t *t, PFcnode_t *e2)
{
    assert (e1 && e2);
    assert (t && t->kind == c_seqtype);

    return PFcore_wire3 (c_proof, e1, t, e2);
}

/**
 * Create a core tree node representing the @b static type of
 * a given expression @a e. Such a node may be used instead of
 * a @c seqtype node. As soon as (during typechecking) @a e's
 * type is known, the node will be replaced by an equivalent
 * @c seqtype node.
 *
 * @param e expression to cast
 * @return a core tree node representing the type
 */
PFcnode_t *
PFcore_stattype (PFcnode_t *e)
{
    assert (e);

    return PFcore_wire1 (c_stattype, e);
}

/**
 * Create a core tree node representing a typeswitch:
 *
 *   typeswitch (e1)
 *      case ... \
 *      ...      |- e2
 *      case ... /
 *      default e3
 *
 * @param e1 the atom be type-"switched"
 * @param e2 the list of "cases"
 * @param e3 the expression for the default case
 * @return core representation of typeswitch 
 */
PFcnode_t *
PFcore_typeswitch (PFcnode_t *e1, PFcnode_t *e2, PFcnode_t *e3)
{
    assert (e1 && e2 && e3);

    /* first argument must be a variable,
     * second argument must be a cases list 
     */    
    assert (IS_ATOM (e1));
    assert (e2->kind == c_cases);

    return PFcore_wire3 (c_typesw, e1, e2, e3);
}

/**
 * Create a core tree node representing a case branch in a list
 * typeswitch cases.
 *
 * @param t the type to match
 * @param e the expression to return
 * @return a @c c_case node with children @c t and @a e.
 */
PFcnode_t *
PFcore_case (PFcnode_t *t, PFcnode_t *e)
{
    assert (e);
    /* first argument must be a type node */
    assert (t && t->kind == c_seqtype);
    
    return PFcore_wire2 (c_case, t, e);
}

/**
 * Create a core tree node representing typeswitch cases.
 *
 * @param e1 case branch
 * @param e2 further cases (or nil)
 * @return a @c c_cases node with children @a e1 and @a e2.
 */
PFcnode_t *
PFcore_cases (PFcnode_t *e1, PFcnode_t *e2)
{
    assert (e1 && e2);

    /* first argument must be a case node, second must be cases or nil */
    assert (e1->kind == c_case);
    assert (e2->kind == c_cases || e2->kind == c_nil);

    return PFcore_wire2 (c_cases, e1, e2);
}

/**
 * Create a core tree node representing an if-then-else clause:
 *
 *    if p then e1 else e2
 *
 * @param p the if-clause
 * @param e1 the then-clause
 * @param e2 the else-clause
 * @return core representation of if-then-else clause
 */
PFcnode_t *
PFcore_ifthenelse (PFcnode_t *p, PFcnode_t *e1, PFcnode_t *e2)
{
    assert (p && e1 && e2);

    /* first argument must be an atom */
    assert (IS_ATOM (p));

    return PFcore_wire3 (c_ifthenelse, p, e1, e2);
}

/**
 * Create a core tree node representing a for binding:
 *
 *     for v at p in e1 return e2
 *
 * @param v  variable to be bound
 * @param p  positional variable
 * @param e1 expression that is bound to @a v (@em must be a variable)
 * @param e2 return part of the expression
 * @return core representation of for binding
 */
PFcnode_t *
PFcore_for (PFcnode_t *v, PFcnode_t *p, PFcnode_t *e1, PFcnode_t *e2)
{
    assert (v && p && e1 && e2);

    /* first and second arguments must be variables, third an atom */
    assert (v->kind == c_var);
    /* the positional variable is optional */
    assert (p->kind == c_nil || p->kind == c_var);
    assert (IS_ATOM (e1));

    return PFcore_wire4 (c_for, v, p, e1, e2);
}

/**
 * Create a core tree node representing a let binding:
 *
 *    let v := e1 return e2
 *
 * @param v   variable to bind
 * @param e1  expression that is bound to @a var
 * @param e2  return part of the expression
 * @return core representation of let binding
 */
PFcnode_t *
PFcore_let (PFcnode_t *v, PFcnode_t *e1, PFcnode_t *e2)
{
    assert (v);

    /* first argument must be a variable */
    assert (v->kind == c_var);

    return PFcore_wire3 (c_let, v, e1, e2);
}

/**
 * Create a new core tree node representing a sequence construction
 * (i.e., the `,' operator in XQuery):
 *
 *     e1, e2
 *
 * @param e1 left side of the comma
 * @param e2 right side of the comma
 * @return core representation of sequence construction
 */
PFcnode_t *
PFcore_seq (PFcnode_t *e1, PFcnode_t *e2)
{
    /* e1 and e2 must be atoms */
    assert (IS_ATOM (e1));
    assert (IS_ATOM (e2));

    return PFcore_wire2 (c_seq, e1, e2);
}

/**
 * Creates a core tree representation of the empty sequence.
 *
 * @return core node representing the empty sequence
 */
PFcnode_t *
PFcore_empty (void)
{
    return PFcore_leaf (c_empty);
}

/** 
 * Create a new core tree node that represents 
 * the builtin function `fn:true ()'.
 */
PFcnode_t *
PFcore_true (void)
{
    return PFcore_leaf (c_true);
}

/** 
 * Create a new core tree node that represents 
 * the builin function `fn:false ()'.
 */
PFcnode_t *
PFcore_false (void)
{
    return PFcore_leaf (c_false);
}

/** 
 * FIXME: should this be replaced by a function call `fn:root ($.)'?
 *
 * Creates the core tree node for the XPath step `/' (root).
 * 
 * @return core representation of `/' step.
 */
PFcnode_t *
PFcore_root (void)
{
    return PFcore_leaf (c_root);
}

/**
 * Create a core tree node representing a path of location steps.
 *
 * @param l location step
 * @param ls prior location steps in path
 * @return core tree node representation path of location steps
 */
PFcnode_t *
PFcore_locsteps (PFcnode_t *l, PFcnode_t *ls)
{
    assert (l && ls);

    return PFcore_wire2 (c_locsteps, l, ls);
}

/**
 * Map an XPath step to its corresponding core tree node.
 * 
 * @param paxis    a value from the #PFpaxis_t enum in the abstract syntax tree
 * @param nodetest a node to wire as a child below the new core node.
 *                 XPath axes can either have a kind test or a name test
 *                 as a child.
 *
 * @note In the abstract syntax tree, all axes are represented by
 *   the same abstract syntax tree node type. In core, however, we use
 *   different node kinds for each axis. This (hopefully)
 *   makes pattern matching and optimization with Twig easier.
 */
PFcnode_t *
PFcore_step (PFpaxis_t paxis, PFcnode_t *nodetest)
{
    PFctype_t kind = 0;

    switch (paxis) {
    case p_ancestor:           
        kind = c_ancestor; 
        break;
    case p_ancestor_or_self:   
        kind = c_ancestor_or_self; 
        break;
    case p_attribute:          
        kind = c_attribute; 
        break;
    case p_child:              
        kind = c_child; 
        break;
    case p_descendant:         
        kind = c_descendant; 
        break;
    case p_descendant_or_self: 
        kind = c_descendant_or_self; 
        break;
    case p_following:          
        kind = c_following; 
        break;
    case p_following_sibling:  
        kind = c_following_sibling; 
        break;
    case p_parent:             
        kind = c_parent; 
        break;
    case p_preceding:          
        kind = c_preceding; 
        break;
    case p_preceding_sibling:  
        kind = c_preceding_sibling; 
        break;
    case p_self:               
        kind = c_self; 
        break;
    default: 
        PFoops (OOPS_FATAL, "illegal XPath axis (%d)", paxis);
    }

    return PFcore_wire1 (kind, nodetest);
}

/**
 * Map a kind test in an XPath step to its corresponding core tree node.
 * 
 * @param pkind a value from the #PFpkind_t enum in the abstract syntax tree
 * @param n     a node to wire as a child below the new core node.
 *              Node test nodes usually have a @c nil node. The processing
 *              instruction node test, however, may have a literal string
 *              (the target specification) as a child.
 *
 * @note In the abstract syntax tree, all kind tests are represented by
 *   the same abstract syntax tree node type. In core, however, we use
 *   different node kinds for each node test variant. This (hopefully)
 *   makes pattern matching and optimization with Twig easier.
 */
PFcnode_t *
PFcore_kindt (PFpkind_t pkind, PFcnode_t *n)
{
    PFctype_t kind = 0;

    assert (n);

    switch (pkind) {
    case p_kind_node:    
        kind = c_kind_node;    
        break;
    case p_kind_comment: 
        kind = c_kind_comment; 
        break;
    case p_kind_text:    
        kind = c_kind_text;    
        break;
    case p_kind_pi:      
        kind = c_kind_pi;      
        break;
    case p_kind_doc:     
        kind = c_kind_doc;     
        break;
    case p_kind_elem:    
        kind = c_kind_elem;    
        break;
    case p_kind_attr:    
        kind = c_kind_attr;    
        break;
    default: 
        PFoops (OOPS_FATAL, "illegal kind test (%d)", pkind);
    }

    return PFcore_wire1 (kind, n);
}

/**
 * Create a new core tree node representing a name test.
 *
 * @param qn the QName to test
 * @return new core tree node
 */
PFcnode_t *
PFcore_namet (PFqname_t qn)
{
    PFcnode_t *core;
  
    core = PFcore_leaf (c_namet);
    core->sem.qname = qn;

    return core;
}

/**
 * Create a new core tree node representing element constructor.
 *
 * @param e1 the tag oder expression containing the name of the element.
 * @param e2 the content of the element.
 * @return the core representation of the element constructor
 */
PFcnode_t *
PFcore_constr_elem (PFcnode_t *e1, PFcnode_t *e2)
{
    assert (e1 && e2);

    return PFcore_wire2 (c_elem, e1, e2);
}

/**
 * Create a new core tree node representing attribute constructor.
 *
 * @param e1 the tag oder expression containing the name of the attribute.
 * @param e2 the content of the element.
 * @return the core representation of the attribute constructor
 */
PFcnode_t *
PFcore_constr_attr (PFcnode_t *e1, PFcnode_t *e2)
{
    assert (e1 && e2);

    return PFcore_wire2 (c_attr, e1, e2);
}

/**
 * Create a new core tree node representing a text, doc, comment or
 * processing-instruction constructor.
 *
 * @param pkind the type of the constructor
 * @param e the content of the constructor.
 * @return the core representation of the constructor
 */
PFcnode_t *
PFcore_constr (PFptype_t pkind, PFcnode_t *e)
{
    PFctype_t kind = 0;

    assert (e);

    switch (pkind) {
    case p_text:    
        kind = c_text;    
        break;
    case p_comment: 
        kind = c_comment; 
        break;
    case p_pi:    
        kind = c_pi;    
        break;
    case p_doc:      
        kind = c_doc;      
        break;
    default: 
        PFoops (OOPS_FATAL, "illegal constructor node (%d)", pkind);
    }

    return PFcore_wire1 (kind, e);
}

/**
 * Create a new core tree node representing the tagname of an element 
 * or attribute constructor.
 * 
 * @param qn the tagname of an element or attribute constructor
 * @return the core representation of an element or attribute tagname
 */
PFcnode_t *
PFcore_tag (PFqname_t qn)
{
    PFcnode_t *core;
  
    core = PFcore_leaf (c_tag);
    core->sem.qname = qn;

    return core;
}

/**
 * Resolve a reference to a function name using Pathfinder's function
 * environment.  If the functions is undefined, raise a fatal error.
 *
 * @param qn QName of function
 * @return pointer to function descriptor
 */
PFfun_t *
PFcore_function (PFqname_t qn)
{
    PFarray_t *fn;

    /* perform lookup in function environment */
    fn = PFenv_lookup (PFfun_env, qn);

    if (fn) 
        return *((PFfun_t **) PFarray_at (fn, 0));

    PFoops (OOPS_FATAL, "function `%s' not defined",
            PFqname_str (qn));

    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/**
 * Count the number of actual arguments for the function call in core
 * tree node @a c.
 *
 * @param c The current @c c_arg node; when called from outside,
 *   this is the topmost @c c_arg node below the function
 *   call. (Can also be a @c c_nil node if no parameters are
 *   specified or the bottom is reached during recursion.)
 * @return number of actual arguments
 */
static unsigned int
actual_args (PFcnode_t *c)
{
    switch (c->kind) {
        case c_nil:
            return 0;
                                                                                                                                                             
        case c_arg:
            return 1 + actual_args (c->child[1]);
                                                                                                                                                             
        default:
            PFoops (OOPS_FATAL,
                    "illegal node kind in function"
                    " application (expecting nil/arg)");
    }
                                                                                                                                                             
    /* just to pacify picky compilers; never reached due to "exit" in PFoops */
    return 0;
}

/**
 * Add core nodes, which converts arguments with the type
 * node or untypedAtomic of a function application to their
 * expected type
 *
 * @param c the function whose arguments are converted
 * @return the updated function application
 */
static PFcnode_t *
apply_function_conversion (PFcnode_t *c)
{
    unsigned int i = 0;
    PFty_t expected;
    PFvar_t *v1;
    PFfun_t *fun;
    PFcnode_t *args, *result;

    assert (c);
    fun = c->sem.fun;
    args = c->child[0];
    result = c;

    while (args->kind == c_arg && i < fun->arity)
    {
        /* the function conversion has only to be applied for
           arguments which are a subtype of atomic and perhaps
           have a quantifying type */
        expected = PFty_prime((fun->par_ty)[i]);

        if (PFty_subtype (expected, PFty_atomic ()))
        {
            /* special case: numeric type is translated as xs:double */
            if (PFty_eq (expected, PFty_numeric ()))
                expected = PFty_double ();

            /* 1. step: apply atomization */
            /* 2. step: cast untypedAtomic to expected type */
            v1 = PFcore_new_var (0);
            result = PFcore_let (PFcore_var (v1),
                                 PFcore_fs_convert_op_by_type 
                                     (PFcore_fn_data (args->child[0]),
                                      expected),
                                 result);
            args->child[0] = PFcore_var (v1);

            /* 3. step: apply promotion */
            /* FIXME: how does this work? */
        }
        args = args->child[1];
        i++;
    }
    return result;
}


/**
 * Test if a xquery function is called, where the
 * conversion rules are already applied
 * - comparisons and calculations have this conversion
 *   already automatically
 * - typed-value is applied inside of fn:data and therefore
 *   would cause an infinite loop
 *
 * @param fn function descriptor
 * @return an integer containing the boolean value
 */
static int
already_converted (PFfun_t *fn)
{
    PFqname_t qn = fn->qname;
            /* avoid recursive call inside fn:data */
    return (!PFqname_eq (qn, PFqname (PFns_pf, "typed-value")) ||
            /* comparisons */
            !PFqname_eq (qn, PFqname (PFns_op, "eq")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "ne")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "le")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "lt")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "ge")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "gt")) ||
            /* calculations */
            !PFqname_eq (qn, PFqname (PFns_op, "plus")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "minus")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "times")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "div")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "idiv")) ||
            !PFqname_eq (qn, PFqname (PFns_op, "mod")));
}

/**
 * Create a core tree node representing a function application:
 *
 *     fn (e)
 *
 * if at a later point an argument is added, then the number of 
 * function arguments will not fit anymore and probably 
 * the function conversion will convert to the wrong types
 *
 * @param fn function descriptor
 * @param e  representation of argument list (right-deep tree of param nodes)
 * @return core representation of function application
 */
PFcnode_t *
PFcore_apply (PFfun_t *fn, PFcnode_t *e)
{
    PFcnode_t *core;
    PFarray_t *funs;
    PFfun_t *fun;
    unsigned int arity, i;

    assert (fn && e);
    arity = actual_args (e);
    fun = fn;
    funs = PFenv_lookup (PFfun_env, fun->qname);
    /* get the first entry */
    fun = *((PFfun_t **) PFarray_at (funs, 0));

    /* get the function where the number of arguments fit */
    for (i = 1; i < PFarray_last (funs); i++) {
        /* be sure that the least specific one (last)
           with the same arity is chosen */
        if (arity < fun->arity)
            break;
        fun = *((PFfun_t **) PFarray_at (funs, i));
    }
                                                                                                                                                             
    /* see if number of actual argument matches function declaration */
    if (arity != fun->arity)
        PFoops (OOPS_APPLYERROR,
                "wrong number of arguments for function `%s' "
                "(expected %u, got %u)",
                PFqname_str (fun->qname), fun->arity, arity);

    core = PFcore_wire1 (c_apply, e);
    core->sem.fun = fun;

    /* add 'conversion rule' specific code for functions whose most 
       non-specific type is subtype of 'atomic+' 
       (and where the conversion rules are not already appliedif necessary)
       - e.g., to allow doc(<name>foo.xml</name> if signature is doc(string?) 
    */
    if (!already_converted (fun))
        core = apply_function_conversion (core);
  
    return core;
}


/**
 * Create a core tree node representing a function argument (list).
 *
 * @param e function argument
 * @param es rest of function argument list
 * @return core representation of function argument (list)
 */
PFcnode_t *
PFcore_arg (PFcnode_t *e, PFcnode_t *es)
{
    assert (e && es);
 
    return PFcore_wire2 (c_arg, e, es);
}


/**
 * Generate a variadic function application node.  You may pass any
 * number of core trees (including 0) as arguments to the function.
 * The last argument needs to be followed by 0.  
 *
 * NB. Do not call this function directly, but call macro #APPLY which
 *     supplies the trailing 0 argument for you.
 * 
 * @param fn pointer to function descriptor
 * @param ... arguments to @a fn
 * @return core tree representing the function application
 */
PFcnode_t *
PFcore_apply_ (PFfun_t *fn, ...)
{
    va_list arglist;
    PFarray_t *args;
    PFcnode_t *a;
    PFvar_t *v;
    unsigned int i;
    PFcnode_t *core, *fnargs;
    PFcnode_t *apply;

    assert (fn);

    args = PFarray (sizeof (PFcnode_t *));

    va_start (arglist, fn);
    while ((a = va_arg (arglist, PFcnode_t *)))
        *((PFcnode_t **) PFarray_add (args)) = a;
    va_end (arglist);

    /* bind dummy node to apply */
    apply  = PFcore_nil ();
    core   = apply;
    fnargs = PFcore_nil ();

    i = PFarray_last (args);

    while (i--) {
        v = PFcore_new_var (0);

        core = PFcore_let (PFcore_var (v),
                           *((PFcnode_t **) PFarray_at (args, i)),
                           core);
        fnargs = PFcore_arg (PFcore_var (v),
                              fnargs);
    }

    /* bind apply instead of dummy node */
    *apply = *(PFcore_apply (fn, fnargs));
    return core;
}


/**
 * Represent the formal semantics function fs:convert-operand
 * as an equivalent core expression. If @a e is untyped and the
 * type @a t is @b not @c untypedAtomic then @a e is casted
 * to the type @a t. If @a e is untyped and the type @a t @b is
 * @c untypedAtomic then @a e is casted to <code>xs:string</code>.
 * Otherwise @a e is returned as is.
 *
 * In contrast to the W3C function fs:convert-operand this function
 * expects a @b type as its second argument. If you want an
 * @b expression as the second argument (as in the specs), use
 * PFcore_fs_convert_op_by_expr() below.
 *
 * @param e The core expression on which the typeswitch is called
 * @param t The type to which n is casted if it is untyped.
 * @return A core expression with the expanded function
 */
PFcnode_t *
PFcore_fs_convert_op_by_type (PFcnode_t *e, PFty_t t)
{
    PFty_t target;
    PFvar_t *v1 = PFcore_new_var (0);
    PFvar_t *v2 = PFcore_new_var (0);

    assert (e);

    /*
     * `untypedAtomic' as the second argument means a cast to
     * `string'. Otherwise, we cast to the type given as the
     * second argument.
     */
    if (PFty_subtype (t, PFty_xdt_untypedAtomic()))
        target = PFty_xs_string ();
    else
        target = t;

    /*
     * let $v1 := [[ e ]] return
     *   for $v2 in $v2 return
     *     typeswitch ($v2)
     *       case xdt:untypedAtomic return cast $v2 to <target type>
     *       default return $v2
     *
     * where <target type> has been determined above to xs:string
     * if t is xdt:untypedAtomic or to t otherwise.
     */
    return PFcore_let 
              (PFcore_var (v1), e,
               PFcore_for
                   (PFcore_var (v2), PFcore_nil (), PFcore_var (v1),
                    PFcore_typeswitch 
                       (PFcore_var (v2),
                        PFcore_cases 
                            (PFcore_case 
                                 (PFcore_seqtype (PFty_xdt_untypedAtomic ()),
                       /*return*/ PFcore_seqcast (PFcore_seqtype (target),
                                                  PFcore_var (v2))
                                  ),
                             PFcore_nil ()
                             ),
                        PFcore_var (v2))
                   )
               );
}

/**
 * Represent the formal semantics function fs:convert-operand
 * as an equivalent core expression. If both, @a e1 and @a e2
 * have type @c untypedAtomic then @a e1 is casted to type string.
 * If @a e1 has type @c untypedAtomic (but not @a e2) then @a e1
 * is casted to the type of @a e2. Otherwise @a e1 is returned as
 * is.
 *
 * If you already know the type of @a e2 while writing your
 * code, please use PFcore_fs_convert_op_by_type() above, as
 * it makes your code more readable.
 *
 * @param e1 The core expression on which the typeswitch is called
 * @param e2 An expression whose type defines the target type to
 *           cast to.
 * @return A core expression with the expanded function
 *
 * @see http://www.w3.org/TR/xquery-semantics/#sec_convert_operand
 */
PFcnode_t *
PFcore_fs_convert_op_by_expr (PFcnode_t *e1, PFcnode_t *e2)
{
    PFvar_t *v1 = PFcore_new_var (0);
    PFvar_t *v2 = PFcore_new_var (0);
    PFvar_t *v3 = PFcore_new_var (0);

    /*
     * let $v1 := [[ e1 ]] return
     *   let $v2 := [[ e2 ]] return
     *     for $v3 in $v1 return
     *       typeswitch ($v3)                  // $v3 is from e1
     *         case xdt:untypedAtomic return
     *           typeswitch ($v2)              // $v2 is e2
     *             case xdt:untypedAtomic return cast $v3 as xs:string
     *             default return cast $v3 as <type of $v2>
     *         default return $v3
     *
     * <type of $v2> is inserted into the core tree by means of a
     * stattype node (the static type of $v2). stattype nodes are
     * replaced by seqtype nodes (actual types) as soon as the
     * type information is available during type checking.
     */
    return PFcore_let 
              (PFcore_var (v1), e1,
               PFcore_let
                  (PFcore_var (v2), e2,
                   PFcore_for
                      (PFcore_var (v3), PFcore_nil (), PFcore_var (v1),
                       PFcore_typeswitch
                          (PFcore_var (v3),
                           PFcore_cases
                               (PFcore_case
                                   (PFcore_seqtype (PFty_xdt_untypedAtomic ()),
                                    PFcore_typeswitch
                                       (PFcore_var (v2),
                                        PFcore_cases
                                           (PFcore_case
                                               (PFcore_seqtype
                                                   (PFty_xdt_untypedAtomic ()),
                                                PFcore_seqcast
                                                   (PFcore_seqtype
                                                       (PFty_xs_string ()),
                                                    PFcore_var (v3))),
                                           PFcore_nil ()),
                                           PFcore_seqcast
                                              (PFcore_stattype (PFcore_var(v2)),
                                               PFcore_var (v3)))),
                                PFcore_nil ()),
                           PFcore_var (v3)))));
}

/**
 * Helper function that expands the function fn:data into
 * a core expression
 * @param n The core expression on which fn:data is called
 * @return A core expression with the expanded fn:data function
 */
PFcnode_t *
PFcore_fn_data (PFcnode_t *n)
{
    PFvar_t *v1 = PFcore_new_var (0);
    PFvar_t *v2 = PFcore_new_var (0);
    PFvar_t *v3 = PFcore_new_var (0);
    PFvar_t *v4 = PFcore_new_var (0);
    PFvar_t *v5 = PFcore_new_var (0);
    PFvar_t *v6 = PFcore_new_var (0);

    PFfun_t *op_sv = PFcore_function (PFqname (PFns_pf, "string-value"));
    PFcnode_t *typed_value = PFcore_let (PFcore_var (v6),
                                         APPLY (op_sv, PFcore_var (v5)),
                                         PFcore_seqcast
                                             (PFcore_seqtype
                                                  (PFty_untypedAtomic ()),
                                              PFcore_var (v6))
                                        );

    assert (n);

    return PFcore_let
               (PFcore_var (v1), n,
                PFcore_for 
                    (PFcore_var (v2), PFcore_nil (), PFcore_var (v1),
                     PFcore_let 
                         (PFcore_var (v3), PFcore_var (v2), 
                          PFcore_typeswitch 
                              (PFcore_var (v3),
                               PFcore_cases 
                                   (PFcore_case 
                                        (PFcore_seqtype (PFty_atomic ()),
                  /*return */            PFcore_var (v3)),
                                    PFcore_nil ()),
                  /*default*/  PFcore_let 
                                   (PFcore_var (v4), PFcore_var (v3),
                                    PFcore_typeswitch
                                        (PFcore_var (v4),
                                         PFcore_cases
                                             (PFcore_case 
                                                  (PFcore_seqtype (PFty_node ()),
                  /*return */                      PFcore_let (PFcore_var (v5),
                               /* FIXME: seqcast needed     */ PFcore_seqcast 
                               /* because of input-arg-type */     (PFcore_seqtype 
                               /* of function string-value  */       (PFty_node ()),
                                                                    PFcore_var (v4)),
                                                               typed_value
                                                              )
                                                  ),
                                                  PFcore_nil ()
                                             ),
                  /*default*/            PFcore_empty ()
                                        )
                                   )
                              )
                         )
                    )
               );

}

/**
 * Helper function that expands the quantified `some' expression into
 * a core expression. 
 * @param v The variable the existential qualifier is bound to
 * @param qExpr The quantified expression
 * @param expr The expression which is tested in the qualified expression
 * @return A core expression with the expanded quantified `some' expression
 */
PFcnode_t *
PFcore_some (PFcnode_t *v, PFcnode_t *expr, PFcnode_t *qExpr)
{
    PFvar_t *v1 = PFcore_new_var (0);
    PFvar_t *v2 = PFcore_new_var (0);
    PFvar_t *v3 = PFcore_new_var (0);
    PFvar_t *v4 = PFcore_new_var (0);
    PFvar_t *v5 = PFcore_new_var (0);
    PFfun_t *fn_not   = PFcore_function (PFqname (PFns_fn, "not"));
    PFfun_t *fn_empty = PFcore_function (PFqname (PFns_fn, "empty"));
                                                                                                                                                          
    return PFcore_let 
              (PFcore_var (v1), expr,
               PFcore_let 
                  (PFcore_var (v2),
                   PFcore_for (v,
                               PFcore_nil (),
                               PFcore_var (v1),
                               PFcore_let (PFcore_var (v3),
                                           PFcore_ebv (qExpr),
                                           PFcore_ifthenelse 
                                              (PFcore_var (v3),
                                               PFcore_num (1),
                                               PFcore_empty ()))),
                   PFcore_let 
                      (PFcore_var (v4),
                       APPLY (fn_empty, PFcore_var (v2)),
                       PFcore_let 
                          (PFcore_var (v5),
                           APPLY (fn_not, PFcore_var (v4)),
                           PFcore_var (v5)))));
}

/**
 * Helper function that creates a core expression that evaluates to
 * the effective boolean value of @a n. The effective boolean value
 * is defined in
 * <a href="http://www.w3.org/TR/2002/WD-xquery-20021115/#id-ebv">section
 * 2.4.3.2 of the Nov 2002 W3C draft</a>:
 * - If the expression is the empty sequence, its effective boolean
 *   value is @c false.
 * - If the expression is a single atomic value of type
 *   <code>xs:boolean</code>, this value is the effective boolean value.
 * - If the expression is a numeric value that is not zero, its effective
 *   boolean value is @c false.
 * - If the expression is a single atomic value of type
 *   <code>xs:double</code> or <code>xs:float</code> and value @c NaN,
 *   its effective boolean value is @c false.
 * - In any other case, the result is @c true.
 *
 * Originally, calculation of the effective boolean value had to be
 * done in the core language. Since the Nov 15 draft, it has moved to
 * a built-in function <code>fn:boolean</code>, defined in
 * <a href='http://www.w3.org/TR/2002/WD-xquery-operators-20021115/#func-boolean'>section
 * 14.2.1 of the 'Functions & Operators Draft'</a>.
 *
 * @param n The core expression whose effective boolean value is to be
 *   determined. If this is a core variable, this variable is directly
 *   used. Otherwise, an additional @c let
 *   clause binds a new variable and then re-applies @a ebv ().
 * @return A core expression representing the effective boolean value
 *   of @a n. This expression always evaluates to a boolean value
 *   or the error value. 
 */
PFcnode_t *
PFcore_ebv (PFcnode_t *n)
{
    PFvar_t *v;

    assert (n);

    if (n->kind == c_var)
        /* if the core expression already is a variable, we re-use it and
         * determine the effective boolean value.
         */
        return APPLY (PFcore_function (PFqname (PFns_fn, "boolean")), n);
    else {
        /* if n is not a variable, wrap the evaluation into a 'let' clause
         * and call this function again.
         */
        v = PFcore_new_var (0);
        
        return PFcore_let (PFcore_var (v), n, 
			   PFcore_ebv (PFcore_var (v)));
    }
}

/**
 * implementation for functions error() and error_loc()
 */
static PFcnode_t *
error_impl (const char * msg, va_list msgs)
{
    va_list tmp;
    char c;
    char *buf;
    unsigned int size;
    PFcnode_t *core;

    va_copy(tmp, msgs);
    size = vsnprintf (&c, 1, msg, tmp) + 1;
    buf = (char *) PFmalloc (size);
    vsnprintf (buf, size, msg, msgs);

    core = PFcore_leaf (c_error);
    core->sem.str = buf;

    return core;
}

/**
 * Creates a core tree node that represents the error-call in
 * XQuery core. A message can be passed as a parameter that will
 * be output to the user.
 *
 * @param msg @c printf style format string for the error message
 */
__attribute__ ((unused))
PFcnode_t *
PFcore_error (const char * msg, ...)
{
    va_list args;
    PFcnode_t *core;

    va_start (args, msg);
    core = error_impl (msg, args);
    va_end (args);

    return core;
}

/**
 * Creates a core tree node that represents the error-call in
 * XQuery core. A message can be passed as a parameter that will
 * be output to the user. This function is similar to error(),
 * but will additionally print the location @a loc in the error
 * message.
 *
 * @param loc location where the error occured
 * @param msg @c printf style format string for the error message
 */
PFcnode_t *
PFcore_error_loc (PFloc_t loc, const char * msg, ...)
{
    va_list      args;
    PFcnode_t *  core;
    unsigned int msglen = strlen (msg);
    char         buf[msglen + sizeof ("error at 000,000-999,999: ")];

    snprintf (buf, msglen + sizeof ("error at 000,000-999,999: "),
              "error at %-.3u,%-.3u-%-.3u,%-.3u: %s",
              loc.first_row, loc.first_col, loc.last_row, loc.last_col, msg);

    va_start (args, msg);
    core = error_impl (buf, args);
    va_end (args);

    return core;
}

/* vim:set shiftwidth=4 expandtab: */
