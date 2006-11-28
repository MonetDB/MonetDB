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
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
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
    core->alg = (struct PFla_pair_t) { .rel = NULL, .frag = NULL};
  
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
PFcore_wire1 (PFctype_t kind, const PFcnode_t *n1) 
{
    PFcnode_t *core;

    assert (n1);

    core = PFcore_leaf (kind);
    core->child[0] = (PFcnode_t *) n1;

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
PFcore_wire2 (PFctype_t kind, const PFcnode_t *n1, const PFcnode_t *n2) 
{
    PFcnode_t *core;
    
    assert (n1 && n2);

    core = PFcore_wire1 (kind, n1);
    core->child[1] = (PFcnode_t *) n2;

    return core;
} 


/** 
 * Create a core @c nil node to terminate lists or denote optional subtrees.
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
#define VNAME_MAX sizeof (VNAME_DOT "_0000")

/**
 * Generate a new (unique) variable. This unique name is created by
 * incrementing a number that is appended to the variable name.  A
 * prefix may be given that may not be longer than 4 characters. If no
 * prefix is given (by passing 0), the default `v' is used. The
 * namespace for the newly created variable is the internal Pathfinder
 * namespace `pf:...' (PFns_pf).
 *
 * @param prefix The prefix to use. The variable name will be
 *   "[prefix]_[num]", where [prefix] is the prefix and [num] is
 *   a three digit, zero-padded integer number
 * @return the new variable
 */
unsigned int core_vars; /* global for core_new_var (TODO REMOVE) */


PFvar_t *
PFcore_new_var (char *prefix)
{
    char                vname[VNAME_MAX];
    int                 l;

    /* prefix may not exceed 4 characters */
    assert (!prefix || strlen (prefix) < sizeof (VNAME_DOT));

    /* construct new var name */
    if (prefix)
        l = snprintf (vname, VNAME_MAX, "%3s_%04u", prefix, core_vars);
    else
        l = snprintf (vname, VNAME_MAX, "v_%04u", core_vars);
    
    /* warn if we needed to truncate the variable name
     * (this does not affect the correct core mapping but may
     * confuse the user)
     */
    if ((PFstate.print_dot || PFstate.print_pretty) && 
        ((size_t) l >= VNAME_MAX || l < 0))
        PFinfo (OOPS_NOTICE, "truncated variable name `%s' in core query",
                vname);

    /* ensure uniqueness */
    core_vars++;

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
PFcore_num (long long int i)
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
 * Separate function declarations from the query body.
 */
PFcnode_t *
PFcore_main (const PFcnode_t *a, const PFcnode_t *b)
{
    return PFcore_wire2 (c_main, a, b);
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
 * Create a `seqcast' Core tree node.  `seqcast' nodes make
 * changes in the @b static type along the <: relationship
 * explicit.  They do not have any semantics during query
 * evaluation, but are only introduced to ensure correct
 * static type inference/checking.
 *
 * `seqcast' is only allowed along the <: relationship, and
 * static type semantics need to make sure that the "cast"
 * would always succeed. I.e., static semantics need to make
 * sure that e.type <: t holds when applying seqcast (t, e).
 *
 * Example applications:
 *
 * - @verbatim
       typeswitch ($x)
         case $y as xs:integer return $y + 1
         default return "not an integer"
@endverbatim
 *   .
 *   If the dynamic type of @a x is @c xs:integer, we assign
 *   @a x to @a y.  (There's no need to cast anything, as we
 *   know that @a x already has type @c xs:integer.)  However,
 *   for correct type inference, we need to somehow signal that
 *   the @b static type of @a y is @c xs:integer, while for
 *   @a x we may have some much more complex type.  (Note that
 *   the arithmetics in the @c return part will most probably
 *   fail if we use the static type of @a x.)  In XQuery Core,
 *   we will thus implement this typeswitch as
 *   @verbatim
       typeswitch ($x)
         case xs:integer return
           let $y := seqcast (xs:integer, $x) return $y + 1
         default return "not an integer"
@endverbatim
 *   (The `seqcast' makes the change in the static type explicit.)
 *   .
 * - @verbatim
       let $x as xs:decimal := 42 return $x
@endverbatim
 *   .
 *   XQuery semantics (Sec. 4.8.3 of the June 2005 FS draft) requires
 *   that @a x is considered to have the @b static type @c xs:decimal
 *   in the @c return clause.  The @b dynamic type of @a x, however,
 *   is not affected by the @c as clause and remains @c xs:integer.
 *   Additional semantics of the @c as clause requires that static
 *   typing checks whether the assigned value (42) is actually a
 *   subtype of the specified type (@c xs:decimal).
 *
 * If you want to cast values at @b runtime, have a look at
 * PFcore_cast()!
 *
 * @param t SequenceType
 * @param e expression to "cast". Static semantics must ensure
 *          that e.type <: t always holds during query evaluation.
 * @return a core tree node representing the seqcast
 */
PFcnode_t *
PFcore_seqcast (const PFcnode_t *t, const PFcnode_t *e)
{
    assert (t && (t->kind == c_seqtype || t->kind == c_stattype));

    return PFcore_wire2 (c_seqcast, t, e);
}

/**
 * Create a Core tree node that represents an XQuery cast:
 *
 * @verbatim
        e cast as t
@endverbatim
 *
 * Note that this is different from the `seqcast' nodes we
 * introduce in our Core tree.  `cast' implements the actual
 * cast, which may fail at runtime depending on dynamic values
 * (e.g., <code>"foo" cast as xs:integer</code>).
 *
 * @param t SequenceType
 * @param e expression to cast
 * @return a core tree node representing the cast
 */
PFcnode_t *
PFcore_cast (const PFcnode_t *t, const PFcnode_t *e)
{
    assert (t && (t->kind == c_seqtype || t->kind == c_stattype));

    return PFcore_wire2 (c_cast, t, e);
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
PFcore_proof (const PFcnode_t *cond, const PFcnode_t *expr)
{
    assert (cond); assert (cond->kind == c_subty); assert (expr);

    return PFcore_wire2 (c_proof, cond, expr);
}

/**
 * Subtype condition for a proof expression.
 */
PFcnode_t *
PFcore_subty (const PFcnode_t *expr, const PFcnode_t *type)
{
    assert (expr); assert (type);

    return PFcore_wire2 (c_subty, expr, type);
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
PFcore_stattype (const PFcnode_t *e)
{
    assert (e);

    return PFcore_wire1 (c_stattype, e);
}

/**
 * Create a core tree node representing a typeswitch:
 *
 *   typeswitch (e1)
 *      case t   return e2
 *      default  return e3
 *
 *             typeswitch
 *            /          \
 *           e1          cases
 *                      /     \
 *                  case       default
 *                 /    \         |
 *                t     e2        e3
 *
 * @param e1 the atom be type-"switched"
 * @param e2 the list of "cases"
 * @param e3 the expression for the default case
 * @return core representation of typeswitch 
 */
PFcnode_t *
PFcore_typeswitch (const PFcnode_t *e1, const PFcnode_t *e2)
{
    assert (e1); assert (e2);

    /* first argument must be a variable,
     * second argument must be a cases list 
     */    
    assert (IS_ATOM (e1));
    assert (e2->kind == c_cases);

    return PFcore_wire2 (c_typesw, e1, e2);
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
PFcore_case (const PFcnode_t *t, const PFcnode_t *e)
{
    assert (e);
    /* first argument must be a type node */
    assert (t); assert (t->kind == c_seqtype);
    
    return PFcore_wire2 (c_case, t, e);
}

/**
 * Create a core tree node representing typeswitch cases.
 *
 * @param e1 case branch
 * @param e2 default branch
 * @return a @c c_cases node with children @a e1 and @a e2.
 */
PFcnode_t *
PFcore_cases (const PFcnode_t *e1, const PFcnode_t *e2)
{
    assert (e1); assert (e2);

    /* first argument must be a case node, second must be cases or nil */
    assert (e1->kind == c_case);
    assert (e2->kind == c_default);

    return PFcore_wire2 (c_cases, e1, e2);
}

/**
 * Create a core tree node representing a typeswitch default clause
 *
 * @param e1 expression to return in the default case
 */
PFcnode_t *
PFcore_default (const PFcnode_t *e)
{
    return PFcore_wire1 (c_default, e);
}

/**
 * Create a core tree node representing an if-then-else clause:
 *
 *    if p then e1 else e2
 *
 * @param cond  condition to test
 * @param ret   a then_else node describing the then- and else-branches
 *
 * @return core representation of if-then-else clause
 */
PFcnode_t *
PFcore_if (const PFcnode_t *cond, const PFcnode_t *ret)
{
    assert (cond); assert (ret); assert (ret->kind == c_then_else);

    /* first argument must be an atom */
    assert (IS_ATOM (cond));

    return PFcore_wire2 (c_if, cond, ret);
}

PFcnode_t *
PFcore_then_else (const PFcnode_t *t, const PFcnode_t *e)
{
    assert (t); assert (e);

    return PFcore_wire2 (c_then_else, t, e);
}


/**
 * Create a core tree node representing a flwor expression
 *
 * @param bind  list of for loops and let bindings
 * @param ret   where - order by - return clause of the bindings
 * @return core representation of flwor expression
 */
PFcnode_t *
PFcore_flwr (const PFcnode_t *bind, const PFcnode_t *ret)
{
    assert (bind); assert (ret);

    return PFcore_wire2 (c_flwr, bind, ret);
}


/**
 * Create a core tree node representing a for binding:
 *
 *     for $v at $p in e1 return e2
 *
 *@verbatim
                            for
                           /   \
                   forbinds     e2
                  /       \
              vars        e1
             /    \
            $v    $p

@endverbatim
 *
 * @param bind  variables to be bound (as a forbind node)
 * @param ret   return part of the expression
 * @return core representation of for binding
 */
PFcnode_t *
PFcore_for (const PFcnode_t *bind, const PFcnode_t *ret)
{
    assert (bind); assert (bind->kind == c_forbind); assert (ret);

    return PFcore_wire2 (c_for, bind, ret);
}

/**
 * Bind variables @a vars to expression @a expr in a for clause
 *
 * @param vars  bound variables (binding and positional variable)
 *              as a forvars node
 * @param expr  the expression to bind to
 */
PFcnode_t *
PFcore_forbind (const PFcnode_t *vars, const PFcnode_t *expr)
{
    assert (vars); assert (vars->kind == c_forvars); assert (expr);

    /* binding expression must be an atom */
    assert (IS_ATOM (expr));

    return PFcore_wire2 (c_forbind, vars, expr);
}

/**
 * Variables bound in a for clause
 *
 * @param bindvar  variable bound in the for clause
 * @param pos      positional variable
 */
PFcnode_t *
PFcore_forvars (const PFcnode_t *bindvar, const PFcnode_t *pos)
{
    assert (bindvar); assert (bindvar->kind == c_var);
    assert (pos); assert (pos->kind == c_var || pos->kind == c_nil);

    return PFcore_wire2 (c_forvars, bindvar, pos);
}

/**
 * Create a core tree node representing a let binding:
 *
 *    let v := e1 return e2
 *
 * @param binding  variable and expression to bind
 * @param expr     return part of the expression
 * @return core representation of let binding
 */
PFcnode_t *
PFcore_let (const PFcnode_t *binding, const PFcnode_t *expr)
{
    assert (binding);

    /* first argument must be a variable binding */
    assert (binding->kind == c_letbind);

    return PFcore_wire2 (c_let, binding, expr);
}

/**
 * Create the binding part of a let binding.
 *
 * @param var   variable that is bound
 * @param expr  the expression the variable should be bound to
 * @return core representation of binding
 */
PFcnode_t *
PFcore_letbind (const PFcnode_t *var, const PFcnode_t *expr)
{
    assert (var); assert (var->kind == c_var); assert (expr);

    return PFcore_wire2 (c_letbind, var, expr);
}

/**
 * Create a core `orderby' node
 *
 * @param stable true if we want `stable order by'.
 * @param crits  order criterions
 * @param expr   expression to order
 */
PFcnode_t *
PFcore_orderby (bool stable, const PFcnode_t *crits, const PFcnode_t *expr)
{
    PFcnode_t *ret;

    assert (crits); assert (crits->kind == c_orderspecs); assert (expr);

    ret = PFcore_wire2 (c_orderby, crits, expr);

    ret->sem.tru = stable;

    return ret;
}

/**
 * Create a core order criterion (to assemble into a list)
 *
 * @param crit   order criterion
 * @param specs  other order specs
 */
PFcnode_t *
PFcore_orderspecs (PFsort_t mode, const PFcnode_t *crit, const PFcnode_t *specs)
{
    PFcnode_t *ret;

    assert (crit); assert (specs);
    assert (specs->kind == c_orderspecs || specs->kind == c_nil);

    ret = PFcore_wire2 (c_orderspecs, crit, specs);

    ret->sem.mode = mode;

    return ret;
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
PFcore_seq (const PFcnode_t *e1, const PFcnode_t *e2)
{
    return PFcore_wire2 (c_seq, e1, e2);
}

PFcnode_t *
PFcore_ordered (const PFcnode_t *e)
{
    return PFcore_wire1 (c_ordered, e);
}

PFcnode_t *
PFcore_unordered (const PFcnode_t *e)
{
    return PFcore_wire1 (c_unordered, e);
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
 * Create a core tree node representing a path of location steps.
 *
 * @param l location step
 * @param ls prior location steps in path
 * @return core tree node representation path of location steps
 */
PFcnode_t *
PFcore_locsteps (const PFcnode_t *l, const PFcnode_t *ls)
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
PFcore_step (PFpaxis_t paxis, const PFcnode_t *nodetest)
{
    PFctype_t kind = (PFctype_t)0;

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
/* [STANDOFF] */
    case p_select_narrow:
        kind = c_select_narrow;
        break;
    case p_select_wide:
        kind = c_select_wide;
        break;
    case p_reject_narrow:
        kind = c_reject_narrow;
        break;
    case p_reject_wide:
        kind = c_reject_wide;
        break;
/* [/STANDOFF] */
    default: 
        PFoops (OOPS_FATAL, "illegal XPath axis (%d)", paxis);
    }

    return PFcore_wire1 (kind, nodetest);
}

/**
 * Create a new core tree node representing element constructor.
 *
 * @param e1 the tag oder expression containing the name of the element.
 * @param e2 the content of the element.
 * @return the core representation of the element constructor
 */
PFcnode_t *
PFcore_constr_elem (const PFcnode_t *e1, const PFcnode_t *e2)
{
    assert (e1 && e2);

    return PFcore_wire2 (c_elem, e1, e2);
}

/**
 * Create a new core tree node representing attribute constructor.
 *
 * @param e1 the tag oder expression containing the name of the attribute.
 * @param e2 the content of the attribute.
 * @return the core representation of the attribute constructor
 */
PFcnode_t *
PFcore_constr_attr (const PFcnode_t *e1, const PFcnode_t *e2)
{
    assert (e1 && e2);

    return PFcore_wire2 (c_attr, e1, e2);
}

/**
 * Construct Core tree node representing a processing-instruction constructor.
 *
 * @param e1 the string oder expression containing the pi target.
 * @param e2 the content of the processing.
 * @return the core representation of the constructor
 */
PFcnode_t *
PFcore_constr_pi (const PFcnode_t *e1, const PFcnode_t *e2)
{
    assert (e1 && e2);

    return PFcore_wire2 (c_pi, e1, e2);
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
PFcore_constr (PFptype_t pkind, const PFcnode_t *e)
{
    PFctype_t kind = (PFctype_t)0;

    assert (e);

    switch (pkind) {
    case p_text:    
        kind = c_text;    
        break;
    case p_comment: 
        kind = c_comment; 
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
 * Create a new core tree node that represents a
 *
 * @verbatim
      with $var seeded by expr recurse expr
@endverbatim
 *
 * expression (this is a Pathfinder extension).  The first argument
 * must be a variable name, the second argument a @c seed node that
 * pairs the seed and the return expression.
 *
 * @param var          A core expression that represents the variable
 *                     used for recursion; must be of type @c c_var.
 * @param seed_recurse A core expression standing for the `seed' and
 *                     `recurse' parts of the expression.  Must be of
 *                     type @c c_seed.
 */
PFcnode_t *
PFcore_recursion (const PFcnode_t *var, const PFcnode_t *seed_recurse)
{
    assert (var->kind == c_var);
    assert (seed_recurse->kind == c_seed);

    return PFcore_wire2 (c_recursion, var, seed_recurse);
}

/**
 * Create a new core tree node that represents the
 *
 * @verbatim
      seeded by expr recurse expr
@endverbatim
 *
 * part of a recursive expression (this is a Pathfinder extension).
 */
PFcnode_t *
PFcore_seed (const PFcnode_t *seed, const PFcnode_t *recurse)
{
    return PFcore_wire2 (c_seed, seed, recurse);
}


/**
 * Pathfinder extension: XRPC calls.
 *
 * @param uri Core expression that identifies the URI where to
 *            execute the function call
 * @param fun Core expression; function call that shall be executed
 *            at @a uri
 */
PFcnode_t *
PFcore_xrpc (const PFcnode_t *uri, const PFcnode_t *fun)
{
    return PFcore_wire2 (c_xrpc, uri, fun);
}

/**
 * Function declaration in the list of declarations
 * (see #PFcore_fun_decls).
 */
PFcnode_t *
PFcore_fun_decl (PFfun_t *fun, const PFcnode_t *args, const PFcnode_t *body)
{
    PFcnode_t *ret;

    ret = PFcore_wire2 (c_fun_decl, args, body);
    ret->sem.apply.fun = fun;

    return ret;
}

/**
 * Right-deep list of function declarations.
 */
PFcnode_t *
PFcore_fun_decls (const PFcnode_t *fun, const PFcnode_t *funs)
{
    return PFcore_wire2 (c_fun_decls, fun, funs);
}

/**
 * Right-deep list of function parameters.
 */
PFcnode_t *
PFcore_params (const PFcnode_t *param, const PFcnode_t *params)
{
    assert (param); assert (param->kind == c_param);
    assert (params); assert (params->kind == c_params || params->kind == c_nil);

    return PFcore_wire2 (c_params, param, params);
}

/**
 * function parameter.
 */
PFcnode_t *
PFcore_param (const PFcnode_t *type, const PFcnode_t *var)
{
    assert (type); assert (type->kind == c_seqtype);
    assert (var); assert (var->kind == c_var);

    return PFcore_wire2 (c_param, type, var);
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
actual_args (const PFcnode_t *c)
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

#if 0
/**
 * Convert function argument or return value according to
 * to the function conversion rules in W3C XQuery 3.1.5.
 *
 * -- If the expected type is an atomic type,
 * 
 *    (1) Apply atomization (i.e., fn:data)
 *    (2) Cast each xdt:untypedAtomic item to the expected
 *        atomic type. For built-in functions with expected
 *        type `numeric', cast them to xs:double.
 *    (3) Apply numeric type promotion, if possible.
 *
 * -- Leave other arguments as they are. (FIXME: correct?)
 *
 * We use fn:data() for Step 1, fs:convert-operand() for Step 2.
 * FIXME: Step 3?
 *
 * In XQuery Core:
 *
 * -- The expected type t is an atomic type:
 *
 *    let $v1 := arg return
 *      for $v2 in $v1 return
 *        let $v3 := fn:data ($v2) return
 *          convert-operand ($v3, prime(expected))
 *
 */
PFcnode_t *
PFcore_function_conversion (const PFcnode_t *e, PFty_t expected)
{
    PFty_t exp_prime;

    PFcnode_t *ret;
    PFvar_t   *v1 = new_var (NULL);
    PFvar_t   *v2 = new_var (NULL);
    PFvar_t   *v3 = new_var (NULL);

    assert (e);

    exp_prime = PFty_prime (expected);

    if (PFty_subtype (exp_prime, PFty_atomic ())) {

        /*
         * Built-in functions allow the expected type to be `numeric'.
         * In that case, we shall convert to xs:double.
         */
        if (PFty_eq (exp_prime, PFty_numeric ()))
            exp_prime = PFty_double ();

        /* Steps 1 and 2:
         *
         * let $v1 := e return
         *   for $v2 in $v1 return
         *     let $v3 := fn:data ($v2) return
         *       convert-op ($v3, exp_prime)
         */
        ret = PFcore_let (
                PFcore_letbind (var (v1), e),
                PFcore_for (
                  PFcore_forbind (
                    PFcore_forvars (var (v2), var (v1)),
                    PFcore_let (
                      PFcore_letbind (var (v3), fn_data (var (v2))),
                      PFcore_fs_convert_op_by_type (var (v3), exp_prime)))));
    }
    else
        /* do nothing if expected is not atomic */
        ret = e;

    return ret;
}
#endif

#if 0
/**
 * Add core nodes, which converts arguments with the type
 * node or untypedAtomic of a function application to their
 * expected type
 *
 * @param c the function whose arguments are converted
 * @return the updated function application
 */
static PFcnode_t *
apply_function_conversion (const PFcnode_t *c)
{
    unsigned int i = 0;
    PFty_t expected;
    PFvar_t *v1;
    PFfun_t *fun;
    PFcnode_t *args, *result;

    assert (c);
    fun = c->sem.apply.fun;
    assert (fun->sig_count == 1);
    args = c->child[0];
    result = (PFcnode_t *) c;

    while (args->kind == c_arg && i < fun->arity)
    {
        /* the function conversion has only to be applied for
           arguments which are a subtype of atomic and perhaps
           have a quantifying type */
        expected = PFty_prime((fun->sigs[0].par_ty)[i]);

        if (PFty_subtype (expected, PFty_atomic ()))
        {
            /* special case: numeric type is translated as xs:double */
            if (PFty_eq (expected, PFty_numeric ()))
                expected = PFty_double ();

            /* 1. step: apply atomization */
            /* 2. step: cast untypedAtomic to expected type */
            v1 = PFcore_new_var (0);
            result = PFcore_let (
                         PFcore_letbind (PFcore_var (v1),
                                         PFcore_fs_convert_op_by_type 
                                             (PFcore_fn_data (args->child[0]),
                                              expected)),
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
#endif

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
PFcore_apply (PFapply_t *fn, const PFcnode_t *e)
{
    PFcnode_t *core;
    PFarray_t *funs;
    PFfun_t *fun, *fun_prev;
    unsigned int arity, i;

    assert (fn && fn->fun && e);
    arity = actual_args (e);
    if (fn->rpc_uri != NULL) arity--;
    fun = fn->fun;
    funs = PFenv_lookup (PFfun_env, fun->qname);

    /*
     * There's exactly one function defined in XQuery that allows
     * an arbitrary number of arguments: fn:concat().
     * We allow that here (and when we compile to Core). The
     * simplification phase (simplify.brg) will normalized all
     * such calls of fn:concat() to subsequent calls with only
     * two arguments.
     */
    if (PFqname_eq (fun->qname, PFqname (PFns_fn, "concat")) == 0) {
        core = PFcore_wire1 (c_apply, e);
        core->sem.apply = *fn;
        return core;
    }

    /* get the function where the number of arguments fit */
    for (i = 0; i < PFarray_last (funs); i++) {
        fun_prev = fun;
        fun = *((PFfun_t **) PFarray_at (funs, i));
        /* be sure that the least specific one (last)
           with the same arity is chosen */
        if (arity < fun->arity)
        {
            fun = fun_prev;
            break;
        }
    }
                                                                                                                                                             
    /* see if number of actual argument matches function declaration */
    if (arity != fun->arity)
        PFoops (OOPS_APPLYERROR,
                "wrong number of arguments for function `%s' "
                "(expected %u, got %u)",
                PFqname_str (fun->qname), fun->arity, arity);

    core = PFcore_wire1 (c_apply, e);
    core->sem.apply.fun = fun;
    core->sem.apply.rpc_uri = fn->rpc_uri;

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
PFcore_arg (const PFcnode_t *e, const PFcnode_t *es)
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
PFcore_apply_ (PFapply_t *fn, ...)
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

        core = PFcore_flwr (
                   PFcore_let (
                       PFcore_letbind (PFcore_var (v),
                                       *((PFcnode_t **) PFarray_at (args, i))),
                       PFcore_nil ()),
                   core);
        fnargs = PFcore_arg (PFcore_var (v), fnargs);
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
PFcore_fs_convert_op_by_type (const PFcnode_t *e, PFty_t t)
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
    return PFcore_flwr (
               PFcore_let (PFcore_letbind (PFcore_var (v1), e), PFcore_nil ()),
               PFcore_flwr (
                   PFcore_for (PFcore_forbind (PFcore_forvars (PFcore_var (v2),
                                                               PFcore_nil ()),
                                               PFcore_var (v1)), 
                               PFcore_nil ()),
                   PFcore_typeswitch (
                       PFcore_var (v2),
                       PFcore_cases (
                           PFcore_case (
                               PFcore_seqtype (PFty_xdt_untypedAtomic ()),
                    /*return*/ PFcore_cast (PFcore_seqtype (target),
                                            PFcore_var (v2))),
                           PFcore_default (PFcore_var (v2))))
                   )
               );
}

/**
 * Helper function for PFcore_fs_convert_op_by_expr()
 * Creates the following expression subtree.
 * typeswitch ($var1)
 *     case type return $var2 cast as type
 *     default default_
 */
static PFcnode_t *
add_conversion_case (const PFcnode_t *default_, PFvar_t *var1, PFvar_t *var2, 
                     PFty_t type) 
{
    return 
        PFcore_typeswitch (
            PFcore_var (var1),
            PFcore_cases (
                PFcore_case (
                    PFcore_seqtype (type),
                    PFcore_cast (
                        PFcore_seqtype (type),
                        PFcore_var (var2))),
                PFcore_default (default_)));
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
PFcore_fs_convert_op_by_expr (const PFcnode_t *e1, const PFcnode_t *e2)
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
     *             case xs:string return cast $v3 as xs:string
     *             case xs:boolean return cast $v3 as xs:boolean
     *             case xs:integer return cast $v3 as xs:integer
     *             case xs:double return cast $v3 as xs:double
     *             case xs:decimal return cast $v3 as xs:decimal
     *             default return $v3 // should not happen
     *         default return $v3
     */
    PFcnode_t *type_conv = 
        add_conversion_case (
            add_conversion_case (
                add_conversion_case (
                    add_conversion_case (
                        add_conversion_case (
                            PFcore_var (v3),
                            v2, v3, PFty_xs_decimal ()),
                        v2, v3, PFty_xs_double ()),
                    v2, v3, PFty_xs_integer ()),
                v2, v3, PFty_xs_boolean ()),
            v2, v3, PFty_xs_string ());

    return PFcore_flwr (
               PFcore_let (PFcore_letbind (PFcore_var (v1), e1),
                           PFcore_nil ()),
               PFcore_flwr (
                   PFcore_let (PFcore_letbind (PFcore_var (v2), e2),
                               PFcore_nil ()),
                   PFcore_flwr (
                       PFcore_for (
                           PFcore_forbind (PFcore_forvars (PFcore_var (v3),
                                                           PFcore_nil ()),
                                           PFcore_var (v1)),
                           PFcore_nil ()),
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
                                                PFcore_cast
                                                   (PFcore_seqtype
                                                       (PFty_xs_string ()),
                                                    PFcore_var (v3))),
                                            PFcore_default (type_conv)))),
                                PFcore_default (PFcore_var (v3)))))));
}

/**
 * Helper function that expands the function fn:data into
 * a core expression
 * @param n The core expression on which fn:data is called
 * @return A core expression with the expanded fn:data function
 */
PFcnode_t *
PFcore_fn_data (const PFcnode_t *n)
{
    PFvar_t *v = PFcore_new_var (NULL);
    PFfun_t *fn_data = PFcore_function (PFqname (PFns_fn, "data"));

    return PFcore_flwr (
               PFcore_let (PFcore_letbind (PFcore_var (v), n), PFcore_nil ()),
               APPLY (fn_data, PFcore_var (v)));

#if 0
    PFvar_t *v1 = PFcore_new_var (NULL);
    PFvar_t *v2 = PFcore_new_var (NULL);
    PFvar_t *v3 = PFcore_new_var (NULL);
    PFvar_t *v4 = PFcore_new_var (NULL);

    PFfun_t *op_sv = PFcore_function (PFqname (PFns_pf, "string-value"));
    PFcnode_t *typed_value
        = PFcore_let (PFcore_letbind (PFcore_var (v4),
                                      APPLY (op_sv, PFcore_var (v3))),
                      PFcore_seqcast (PFcore_seqtype (PFty_untypedAtomic ()),
                                      PFcore_var (v4)));

    assert (n);

    /*
     * let $v1 := n return
     *   for $v2 in $v1 return
     *     typeswitch ($v2)
     *       case atomic return $v2
     *       default return
     *         typeswitch ($v2)
     *           case node return let $v3 := seqcast (node, $v2)
     *                              return typed-value
     *           default return ()
     */
    return PFcore_let (
            PFcore_letbind (PFcore_var (v1), n),
            PFcore_for (
                PFcore_forbind (
                    PFcore_forvars (PFcore_var (v2),
                                    PFcore_nil ()),
                    PFcore_var (v1)),
                PFcore_typeswitch (
                    PFcore_var (v2),
                    PFcore_cases (
                        PFcore_case (
                            PFcore_seqtype (PFty_atomic ()),
                            PFcore_var (v2)),
                        PFcore_default (
                            PFcore_typeswitch (
                                PFcore_var (v2),
                                PFcore_cases (
                                    PFcore_case (
                                        PFcore_seqtype (PFty_node ()),
                                        PFcore_let (
                                            PFcore_letbind (
                                                PFcore_var (v3),
                                                PFcore_seqcast (
                                                    PFcore_seqtype(PFty_node()),
                                                    PFcore_var (v2))),
                                            typed_value)),
                                    PFcore_default (PFcore_empty ()))))))));
#endif
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
PFcore_some (const PFcnode_t *v, const PFcnode_t *expr, const PFcnode_t *qExpr)
{
    PFvar_t *v1 = PFcore_new_var (0);
    PFvar_t *v2 = PFcore_new_var (0);
    PFvar_t *v3 = PFcore_new_var (0);
    PFvar_t *v4 = PFcore_new_var (0);
    PFvar_t *v5 = PFcore_new_var (0);
    PFfun_t *fn_not   = PFcore_function (PFqname (PFns_fn, "not"));
    PFfun_t *fn_empty = PFcore_function (PFqname (PFns_fn, "empty"));
                                                                                                                                                          
    return PFcore_flwr (
               PFcore_let (PFcore_letbind (PFcore_var (v1), expr), 
                           PFcore_nil ()),
               PFcore_flwr (
                   PFcore_let (
                       PFcore_letbind (PFcore_var (v2),
                                       PFcore_flwr (
                                           PFcore_for (
                                               PFcore_forbind (
                                                   PFcore_forvars (
                                                       v,
                                                       PFcore_nil ()),
                                                   PFcore_var (v1)),
                                               PFcore_nil ()),
                                           PFcore_flwr (
                                               PFcore_let (
                                                   PFcore_letbind (
                                                       PFcore_var (v3),
                                                       PFcore_ebv (qExpr)),
                                                   PFcore_nil ()),
                                               PFcore_if 
                                                  (PFcore_var (v3),
                                                   PFcore_then_else (
                                                       PFcore_num (1),
                                                       PFcore_empty ()))))),
                       PFcore_nil ()),
                   PFcore_flwr (
                       PFcore_let (
                           PFcore_letbind (PFcore_var (v4),
                                           APPLY (fn_empty, PFcore_var (v2))),
                           PFcore_nil ()),
                       PFcore_flwr (
                           PFcore_let (
                               PFcore_letbind (PFcore_var (v5),
                                               APPLY (fn_not, PFcore_var (v4))),
                               PFcore_nil ()),
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
PFcore_ebv (const PFcnode_t *n)
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
        
        return PFcore_flwr (PFcore_let (PFcore_letbind (PFcore_var (v), n), 
                                        PFcore_nil ()),
                            PFcore_ebv (PFcore_var (v)));
    }
}

/* vim:set shiftwidth=4 expandtab: */
