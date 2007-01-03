/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Check variable scoping in abstract syntax tree.
 *
 * The abstract syntax tree is traversed recursively. With the help of
 * a stack-type environment, variable scoping is checked. On any occurence of
 *
 *  - a @c flwr, @c some or @c every node, or a function declaration
 *    node, the current variable environment is saved and restored 
 *    after the node has been processed.  The variables are bound only within 
 *    the rightmost subtree of the node.
 *
 *  - a @c typeswitch node, the leftmost child is processed (which may
 *    not use any variable that wasn't seen so far). If the typeswitch
 *    contains an @c as clause, the variable is pushed onto the stack
 *    (which also creates a new PFvar_t struct). The @c cases and
 *    @c default subtrees are processed with the new variable in scope.
 *    The variable is popped off the stack before returning from the
 *    typeswitch expression (if there is an @c as clause).
 *
 *  - a variable binding (can be either a @c let node or a @c bind node
 *    that is used for @c for, @c some and @c every), A new variable is
 *    created with the help of PFnew_var. The pointer to the correspondig
 *    @c struct is pushed onto the variabel environment stack.
 *    If we find a variable of the same name on the stack (i.e., in
 *    scope), issue a warning about variable reuse.  Note that
 *    a @c bind node may bring into scope up to two variables 
 *    (positional variables in @c for).  
 *
 *  - a variable usage, the stack is scanned top down for the first
 *    occurence of a variable with the same name. If the variable is
 *    found, this is the correct in-scope variable. If the search is not
 *    successful, scoping rules have been violated by the user. In this
 *    case we push an error message onto the stack and set a
 *    @c scoping_failed flag to stop processing the query after this
 *    phase. The scope checking, however, is continued to possibly find
 *    more scoping rule violations and report them to the user.
 *    A typical rule violation is variable reuse.
 *
 *  - a library module declaration, we back up the current variable
 *    environment and scope the declaration body with an initially
 *    empty environment.  When we come back from scoping the body,
 *    only variables exported by the module are on the stack, and we
 *    add them to the "outer" stack that we restore as well.
 *
 * Within this scoping phase, for all variables that occur in the user
 * query, the pointer to a QName (see #PFpnode_t, #PFsem_t) is replaced
 * by a pointer to a PFvar_t struct. This struct again contains the
 * QName of the variable and uniquely identifies the variable. Later
 * occurencies of the same variable point to the same PFvar_t struct.
 * To indicate successful scoping, the type of abstract syntax tree node
 * is lifted from p_varref (unscoped) to p_var (scoped).
 *
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#include "pathfinder.h"

#include <assert.h>

#include "varscope.h"

#include "variable.h"
/* PFscope_t */
#include "scope.h"
#include "oops.h"

/** Create variable environment used for scoping */
static PFscope_t *var_env;

static PFvar_t *find_var (PFqname_t);

/**
 * If a violation of scoping rules is found during processing, this flag
 * is set to true. We still keep on processing to possibly find more rule
 * violations and report them all to the user.
 *
 * global (TODO: remove)
 */
static bool scoping_failed = false;

static void scope_var_decls (PFpnode_t *n);

/**
 * Push a new variable onto the variable environment stack.
 *
 * @param n variable to push onto the stack. 
 * @return Status code as described in pathfinder/oops.c
 */
static void
push (PFpnode_t *n)
{
    PFvar_t *var;
    PFqname_t *varname;

    /* create new variable */
    assert (n && n->kind == p_varref);

    varname = &(n->sem.qname);
    assert (varname);

    if (! (var = PFnew_var (*varname)))
        PFoops (OOPS_OUTOFMEM, 
                "allocation of new variable failed");

    /* If we find a variable of the same name on the stack (i.e., in scope),
     * issue a warning (the user might mistake an XQuery clause like 
     * `let $x := $x + 1' for an imperative variable update). 
     */
    if (PFscope_lookup (var_env, *varname))
        PFinfo_loc (OOPS_WARN_VARREUSE, n->loc, "$%s", PFqname_str (*varname));

    /* register variable in environment */
    PFscope_into (var_env, *varname, var);

    var->type = PFty_none ();

    /* initialize fields of varref node n */
    n->sem.var = var;
    n->kind = p_var;
}

/**
 * Traverse the stack top-down and return the PFvar_t pointer that
 * corresponds to the given variable name using the scoping rules.
 * @param varname The variable name to look for
 * @return Pointer to the corresponding PFvar_t struct or @c NULL
 *   if the variable was not found.
 */
static PFvar_t *
find_var (PFqname_t varname)
{
    return ((PFvar_t *) PFscope_lookup (var_env, varname));
}

/**
 * Traverse through abstract syntax tree and do actual variable scope
 * checking work. For some node types, action code is executed as
 * described above.
 *
 * @param n The current abstract syntax tree node
 */
static void
scope (PFpnode_t *n)
{
    unsigned int child;     /* Iterate over children */
    PFvar_t     *var;

    assert (n);

    switch (n->kind) {
    case p_varref:  /* Variable usage */
        /*
         * Traverse stack topdown and find the first variable whose name
         * matches the current variable. If we find it, we store the
         * information in the current parse tree node. If not (NULL is
         * returned), scoping rules have been violated. Query processing
         * has to stop after this scoping phase. We do, however, not abort
         * processing here, but keep on looking for more rule violations
         * to report them all to the user.
         */
        var = find_var (n->sem.qname);
        
        if (!var) {
            PFinfo_loc (OOPS_UNKNOWNVAR, n->loc,
                        "$%s", PFqname_str (n->sem.qname));
            scoping_failed = true;
        }
        
        n->sem.var = var;
        n->kind = p_var;
        
        break;
        
    case p_flwr:     
        /*                       flwr
         *                      / | | \
         *                 binds  o p  e
         *
         * (1) save current variable environment 
         * (2) process variable bindings 
         * (3) process o `order by', p `where', and e `return' clauses
         * (4) restore variable environment
         */
        
        /* (1) */
        PFscope_open (var_env);
        
        /* (2) */
        scope (n->child[0]);
        
        /* (3) */
        scope (n->child[1]);
        
        /* (4) */
        PFscope_close (var_env);
        
        break;
        
    case p_some:      
    case p_every:     
        /*                       some/every
         *                         /   \
         *                     binds    e
         *
         * (1) save current variable environment 
         * (2) process variable bindings 
         * (3) process quantifier body e `satifies' clause
         * (4) restore variable environment
         */
        
        /* (1) */
        PFscope_open (var_env);
        
        /* (2) */
        scope (n->child[0]);
        
        /* (3) */
        scope (n->child[1]);
        
        /* (4) */
        PFscope_close (var_env);
        
        break;
        
    case p_fun_decl:  
        /*                      fun_decl
         *                     /        \
         *                 fun_sig       e
         *                /       \
         *             params      t
         *
         *
         * (1) save current variable environment 
         * (2) process function parameters
         * (3) process function body e
         * (4) restore variable environment
         */
        
        /* (1) */
        PFscope_open (var_env);
        
        /* (2) */
        scope (n->child[0]);
        
        /* (3) */
        scope (n->child[1]);
        
        /* (4) */
        PFscope_close (var_env);
        
        break;
        
    case p_bind:     
        /*                 bind
         *                /    \
         *             vars     e        for $v as t at $i in e
         *            /    \             some $v as t satisfies e
         *     var_type     i
         *      /    \
         *     v      t
         *
         * (i may be nil: no positional vars for some/every)
         *
         * (1) process e (v, i not yet visible in e)
         * (2) bring v into scope
         * (3) bring i into scope (if present)
         */
      
        /*
         * Raise an error if positional variable and bound
         * variable have the same name, e.g.
         *
         *   for $x at $x in e return e'
         */
        if (n->child[0]->child[1]->kind == p_varref) {
            if (!PFqname_eq (n->child[0]->child[0]->child[0]->sem.qname,
                             n->child[0]->child[1]->sem.qname)) {
                PFoops_loc (OOPS_VARREDEFINED, n->loc,
                            "it is illegal to use the same name for "
                            "positional and binding variable (`for $%s "
                            "at $%s ...')",
                            PFqname_str (n->child[0]->child[0]->child[0]
                                                              ->sem.qname),
                            PFqname_str (n->child[0]->child[1]->sem.qname));
            }
        }
      
        /* (1) */
        scope (n->child[1]);

        /* (2) */
        assert (n->child[0] && n->child[0]->child[0]
                && n->child[0]->child[0]->child[0]
                && n->child[0]->child[0]->child[0]->kind == p_varref);

        push (n->child[0]->child[0]->child[0]);
        
        /* (3) */
        assert (n->child[0] && n->child[0]->child[1]);

        if (n->child[0]->child[1]->kind == p_varref)
            push (n->child[0]->child[1]);

        break;

    case p_let:  
        /*                 let
         *                /   \
         *         var_type    e       let $v as t := e 
         *          /    \
         *         v      t
         *
         * (1) process e (v not yet visible in e)
         * (2) bring v into scope
         */
      
        /* (1) */
        scope (n->child[1]);
      
        /* (2) */
        assert (n->child[0] && n->child[0]->child[0]
                && n->child[0]->child[0]->kind == p_varref);

        push (n->child[0]->child[0]);

        break;

    case p_param:    /* function parameter */
        /* Abstract syntax tree layout:
         *
         *                param                
         *                 / \        declare function ... (..., t v, ...)
         *                t   v
         */
        assert (n->child[1] && (n->child[1]->kind == p_varref));

        push (n->child[1]);

        break;

    case p_case:          /* branch of a typeswitch expression */
        /* Abstract syntax tree layout:
         *
         *                 case
         *                /    \
         *           var_type   e            case $v as t return e
         *          /       \
         *         v         t
         *
         */
      
        /* occurrence of a branch variable is optional */
        assert (n->child[0] && n->child[0]->child[0]);

        /* visibility of branch variable is branch-local only */
        PFscope_open (var_env);

        if (n->child[0]->child[0]->kind == p_varref)
            push (n->child[0]->child[0]);

        /* visit the case branch e itself */
        assert (n->child[1]);
        
        scope (n->child[1]);

        PFscope_close (var_env);

        break;

    case p_default:    /* default clause of a typeswitch expression */
        /*
         * Abstract syntax tree layout:
         *
         *              default
         *               /   \         default $v return e
         *              v     e
         *
         * (variable $v is optional)
         */
        PFscope_open (var_env);

        assert (n->child[0]);
        if (n->child[0]->kind == p_varref)
            push (n->child[0]);

        scope (n->child[1]);

        PFscope_close (var_env);

        break;

    case p_var_decl:    /* variable declaration in prolog */
        /* already done, so skip on */
        break;

    case p_main_mod:
        /* scope global variable declarations first */
        scope_var_decls (n->child[0]);

        /* then everything else */
        scope (n->child[0]);
        scope (n->child[1]);
        break;

    case p_lib_mod:
        /*
         * stop when we encounter a library module; already did that
         */
        break;

        /*
         * Recursion expression (a Pathfinder extension)
         *
         * with $v [as t] seeded by e1 return e2
         *
         *               recursion
         *              /         \
         *         var_type       seed
         *          /    \        /  \
         *         v      t      e1  e2
         *
         * (1) scope-check e1
         * (2) create a new scope
         * (3) add variable v to it
         * (4) scope-check e2
         * (5) close the scope
         */
    case p_recursion:

        assert (n->child[0]->kind == p_var_type);
        assert (n->child[1]->kind == p_seed);

        /* (1) */
        scope (n->child[1]->child[0]);
        /* (2) */
        PFscope_open (var_env);
        /* (3) */
        push (n->child[0]->child[0]);
        /* (4) */
        scope (n->child[1]->child[1]);
        /* (5) */
        PFscope_close (var_env);
        break;

    default:
        /*
         * For all other cases just traverse the whole tree recursively.
         */
        for (child = 0; (child < PFPNODE_MAXCHILD) && n->child[child]; child++)
            scope (n->child[child]);

        break;
    }

}

/**
 * Variable declarations in the query prolog must be scoped before anything
 * else. Functions declared later in the query may depend on them.
 */
static void
scope_var_decls (PFpnode_t *n)
{
    unsigned int child;     /* Iterate over children */

    assert (n);

    switch (n->kind) {

        case p_main_mod:
        case p_lib_mod:
            /* stop traversing */
            break;

        case p_var_decl:
            /*
             * Abstract syntax tree layout:
             *
             *           var_decl
             *          /        \
             *     var_type       e       declare variable $v as t := e
             *     /      \
             *    v        t
             *
             * (1) bring variable into scope
             * (2) scope-check the initialization code
             *     (may not use any variables that are declared later)
             */
            assert (n->child[0] && n->child[0]->child[0]
                    && n->child[0]->child[0]->kind == p_varref);

            /*
             * It is illegal to re-define globally declared variables
             */
            if (PFscope_lookup (var_env, n->child[0]->child[0]->sem.qname))
                PFoops_loc (OOPS_VARREDEFINED, n->child[0]->child[0]->loc,
                            "redefinition of variable $%s",
                            PFqname_str (n->child[0]->child[0]->sem.qname));

            push (n->child[0]->child[0]);

            scope (n->child[1]);

            break;

        default:
            /*
             * For all other cases just traverse the whole tree recursively.
             */
            for (child = 0;
                 child < PFPNODE_MAXCHILD && n->child[child];
                 child++)
                scope_var_decls (n->child[child]);

            break;

    }
}

static void
scope_lib_mod (PFpnode_t *n)
{
    switch (n->kind) {

        case p_lib_mod:
        {  
            /* back up variable environment */
            PFscope_t *old_var_env = var_env;

            /* and library module with an empty environment */
            var_env = PFscope ();

            /* scope variable declarations first */
            scope_var_decls (n->child[1]);

            /* then the rest (function declarations) */
            scope (n->child[1]);

            /*
             * When we're back variables exported by the module are
             * available on the stack. We add them to the surrounding
             * environment.
             *
             * `false' means we do not allow variable overriding, and
             * PFscope_append() will bail out if a variable in var_env
             * already exists in old_var_env.
             *
             * This essentially implements that we do not allow the
             * same variable to be declared in different library
             * modules. We cannot do that check here, because we
             * cannot look into the (hash based) environments. So we
             * have to hand that problem over to PFscope_append().
             */
            PFscope_append (old_var_env, var_env, false);

            /* and set the surrounding stack as the current one */
            var_env = old_var_env;

        } break;

        default:
            /*
             * For all other cases just traverse the whole tree recursively.
             */
            for (int child = 0;
                 child < PFPNODE_MAXCHILD && n->child[child];
                 child++)
                scope_lib_mod (n->child[child]);

            break;

    }
}

/**
 * Check variable scoping for the parse tree.
 *
 * Scoping rules are checked recursively. In all nodes that reference
 * variables, the qname in the semantic content of the node is replaced
 * by a pointer to a PFvar_t struct (var). If scoping rules are violated,
 * a message is pushed onto the error stack for each violation and the
 * function returns an error code. An error can also be returned due to
 * other errors, like out of memory errors.
 * @param root Pointer to the parse tree root node
 */
void
PFvarscope (PFpnode_t * root)
{
    var_env = PFscope ();

    /* initialize global */
    scoping_failed = false;

    switch (root->kind) {

        case p_lib_mod:
            /* scope modules that the input module has imported */
            scope_lib_mod (root->child[1]);

            /* look out for `declare variable' statements in the
             * module definition
             */
            scope_var_decls (root->child[1]);

            /* scope the input module */
            scope (root->child[1]);

            break;

        case p_main_mod:
            /* scope modules that the input module has imported */
            scope_lib_mod (root->child[0]);

            /* handle `declare variable's in the prolog */
            scope_var_decls (root->child[0]);

            /* scope our own prolog */
            scope (root->child[0]);

            /* scope the query body */
            scope (root->child[1]);

            break;

        default:
            PFoops (OOPS_FATAL, "illegal parse tree in PFvarscope()");
            break;
    }

    if (scoping_failed)
        PFoops (OOPS_UNKNOWNVAR,
                "erroneous variable references reported above");
}


/* vim:set shiftwidth=4 expandtab: */
