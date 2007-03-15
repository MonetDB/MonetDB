/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
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

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <stdio.h>
#include <assert.h>

#include "bitset.h"

#include "mil.h"

#include "oops.h"

/**
 * The MIL dead code elimination removes unnecessary operators and in
 * addition has some sanity checks if debug mode is enabled.
 * These sanity checks introduce additional recursive calls whenever
 * all variables in its body are marked 'used' (and thus the recursive
 * descent is avoided). They traverse the MIL tree without transformation
 * (change = false) and rely in some situations on some MIL coding 
 * convention (ensured only by a later phase). Otherwise they might hit
 * an assignment which would break the DCE.
 *
 * The convention is that only the operators seq, catch (body arg), 
 * if (then & else arg), and while (body arg) may contain assignments.
 */

static PFmil_t *
mil_dce_worker (PFmil_t *root, PFbitset_t *used_vars, PFbitset_t *dirty_vars,
                bool change);

/** Adds recursivly all variables to the set of the used variables. */
static void add_vars_to_used (PFmil_t *root, PFbitset_t *used_vars) 
{
    if (root->kind == m_var) 
        PFbitset_set (used_vars, root->sem.ident, true);
    else {
        for (unsigned int i = 0; i < MIL_MAXCHILD 
                 && root->child[i] != NULL; i++)
            add_vars_to_used(root->child[i], used_vars);
    }
}

/**
 * Reduce unused subexpressions of root to a sequence of statements 
 * with side effects.
 */
static PFmil_t *
reduce_expressions (PFmil_t *root,
                    PFbitset_t *used_vars,
                    PFbitset_t *dirty_vars,
                    bool change) 
{
    /* new node, replacing this expression node. */
    PFmil_t *new_op = PFmil_nop();

    for (unsigned int i = 0; i < MIL_MAXCHILD && root->child[i] != NULL; i++) {

        PFmil_t *param_op
            = mil_dce_worker (root->child[i], used_vars, dirty_vars, change);

        /* concat statements, if multiple parameters have side effects. */
        if (param_op->kind != m_nop)
            new_op = PFmil_seq (new_op, param_op);
    }

    return change?new_op:root;
}

/** 
 * Returns true if the subtree duplicates a variable.
 * e.g. append(append(x, 1), 2) duplicates the variable x.
 */
static bool
var_duplication (PFmil_t *root, PFmil_ident_t *var)
{
    switch (root->kind) {

        case m_var:
            *var = root->sem.ident;
            return true;

        case m_assert_order:
        case m_chk_order:
        case m_order:
        case m_key:
        case m_seqbase:
        case m_insert:
        case m_bappend:
        case m_access:
        case m_col_name:
        case m_reverse:
            /* these functions return the first argument */
            return var_duplication (root->child[0], var);

        default:
            return false;
    }
}

static PFmil_t *
mil_dce_worker (PFmil_t *root, PFbitset_t *used_vars, PFbitset_t *dirty_vars,
                bool change) 
{

#ifdef NDEBUG
    /* Only used for debugging */
    (void) dirty_vars;
#endif

    switch (root->kind) {

        case m_seq:
            {
                /* descend in reverse pre-order, collect the used variables
                 * from the end to beginning of the program. */
                PFmil_t *new_child;

                for (int i = MIL_MAXCHILD; --i >= 0;)
                    if (root->child[i] != NULL) {
                        new_child = mil_dce_worker (root->child[i],
                                                    used_vars,
                                                    dirty_vars,
                                                    change);
                        if (change) root->child[i] = new_child;
                    }
                return root;
            }

        case m_assgn:
            {
                PFmil_t  *lvalue = root->child[0];
                PFmil_t  *rvalue = root->child[1];
                bool      used   = PFbitset_get (used_vars, lvalue->sem.ident); 

                assert(lvalue->kind == m_var);

                /* The variable is unused before the assignment. */
                PFbitset_set (used_vars, lvalue->sem.ident, false);

                /* recognize x := unused; assignment */
                if (rvalue->kind == m_var 
                        && rvalue->sem.ident == PF_MIL_VAR_UNUSED) {
                    return root;
                }

#ifndef NDEBUG
                /*
                 * Situations like
                 *
                 *  x := y;
                 *  ...
                 *  foo(y);
                 *
                 * where foo() has a side-effect on y usually indicate
                 * a bug in milgen.brg.  When translating an algebraic
                 * operator, we do not want it to destructively modify
                 * any of its arguments (which would lead to situations
                 * like the one above).  As a sanity-check, test for
                 * situations like these.
                 */
                PFmil_ident_t var;

                if (var_duplication (rvalue, &var) ) {
                    /* Assignment is of the form x := y. Neither x nor y 
                       can be dirty */
                    if (PFbitset_get (dirty_vars, var) ||
                        PFbitset_get (dirty_vars, lvalue->sem.ident))
                        PFoops (OOPS_FATAL,
                                "illegal combination of copy-by-reference "
                                "and side-effects");
                } 
                
                /* variable on the left side is clean */
                PFbitset_set (dirty_vars, lvalue->sem.ident, false);
#endif

                if (!used) 
                    /*
                     * assignment on unused variable, remove assignment,
                     * but invoke elimination recursively on the r-value
                     * (which could have side effects!)
                     */
                    return mil_dce_worker (rvalue,
                                           used_vars,
                                           dirty_vars,
                                           change);
#ifndef NDEBUG
                else
                    /* update dirty vars */
                    mil_dce_worker (rvalue, used_vars, dirty_vars, false);
#endif

                /*
                 * assignment on used variable, mark all on the right
                 * side as used.
                 */
                add_vars_to_used (rvalue, used_vars);

                return root;
            }
            break;

        case m_catch:
            {
                PFmil_t  *new_child;
                PFmil_t  *var = root->child[0];
                PFmil_t  *body = root->child[1];

                assert(var->kind == m_var);

                /* The variable is set to true. */
                PFbitset_set (used_vars, var->sem.ident, true);

#ifndef NDEBUG
                /* variable on the left side is clean */
                PFbitset_set (dirty_vars, var->sem.ident, false);
#endif

                /* try to find dead code in the body */ 
                new_child = mil_dce_worker (body,
                                            used_vars,
                                            dirty_vars,
                                            change);
                if (change) root->child[1] = new_child;

                return root;
            }
            break;

        case m_if:
            {
                PFmil_t *new_child;

                /* all variables of the condition are used. */
                add_vars_to_used (root->child[0], used_vars);

#ifndef NDEBUG
                /* update dirty vars */
                mil_dce_worker (root->child[0], used_vars, dirty_vars, false);
#endif

                /*
                 * to find the used variables of a if-then-else block, we must 
                 * find them for the if and for the else part and join the sets.
                 */
                PFbitset_t *else_used_vars = PFbitset_copy (used_vars);
                PFbitset_t *else_dirty_vars = NULL;

#ifndef NDEBUG
                else_dirty_vars = PFbitset_copy (dirty_vars);
#endif

                /* invoke dce on if part */
                new_child = mil_dce_worker (root->child[1], 
                                            used_vars,
                                            dirty_vars,
                                            change);
                if (change) root->child[1] = new_child;

                /* invoke dce on else part */
                new_child = mil_dce_worker (root->child[2], 
                                            else_used_vars, 
                                            else_dirty_vars,
                                            change);
                if (change) root->child[2] = new_child;

                PFbitset_or (used_vars, else_used_vars);

#ifndef NDEBUG
                PFbitset_or (dirty_vars, else_dirty_vars);
#endif

                /*
                 * further optimization: check if if-block and else-block are 
                 * empty and remove if clause.
                 * (I think this is not needed because the if clause is only
                 * used rarely).
                 */
                
                /* the variables of the condition are required (add them again
                   in case the then and else branch did throw them away) */
                add_vars_to_used (root->child[0], used_vars);

                return root;
            }
            break;

        case m_while:
            {
                PFmil_t *new_child;

                /* all variables of the condition are used. */
                add_vars_to_used (root->child[0], used_vars);

#ifndef NDEBUG
                /* update dirty vars */
                mil_dce_worker (root->child[0], used_vars, dirty_vars, false);
#endif

                /* ONLY collect the variables required for an iteration */
                mil_dce_worker (root->child[1], used_vars, dirty_vars, false);

                /* the variables of the condition are required
                   for the next iteration */
                add_vars_to_used (root->child[0], used_vars);

                /* invoke dce on the loop body (used_vars now contains the
                   variables in the condition as well as the ones required
                   for another pass through the body */
                new_child = mil_dce_worker (root->child[1], 
                                            used_vars,
                                            dirty_vars,
                                            change);
                if (change) root->child[1] = new_child;
                
                /* the variables of the condition are required for the first
                   check of the condition */
                add_vars_to_used (root->child[0], used_vars);

                return root;
            }
            break;

        case m_module:
        case m_comment:
        case m_declare:
            /*
             * we must preserve all variable declarations, because the 
             * unused assignments are not removed.
             */
            return root;

        case m_assert_order:
        case m_chk_order:
        case m_order:
        case m_key:
        case m_seqbase:
        case m_insert:
        case m_binsert:
        case m_bappend:
        case m_access:
        case m_col_name:
            {
                PFmil_ident_t var;

                if (var_duplication (root->child[0], &var)) {

#ifndef NDEBUG
                    /*
                     * variable is used as first parameter of a expression
                     * with side effects, mark it as dirty.
                     *
                     * Only mark variable dirty if we also apply a change.
                     * Otherwise we might have not enough information to
                     * conclude correctly that something is dirty. (E.g., in
                     * 'while (b) { a := new(..); a.append(..); b := a; }'
                     * b is only used in the next iteration and thus not
                     * used in the first traversal. In consequence a is not
                     * marked used which leads in 'case m_assgn' to code
                     * that does not clean up the dirty flag correctly.)
                     */
                    if (change)
                        PFbitset_set (dirty_vars, var, true);
#endif

                    if (PFbitset_get (used_vars, var)) {

                        /* statement is e.g. append(a, b) and a is used. */
                        add_vars_to_used (root, used_vars);

#ifndef NDEBUG
                        /* update dirty vars */
                        for (unsigned int i = 0; i < MIL_MAXCHILD 
                                && root->child[i] != NULL; i++)
                            mil_dce_worker (root->child[i],
                                            used_vars, 
                                            dirty_vars,
                                            false);
#endif

                        return root;
                    }
                    else
                        /*
                         * statement is e.g. append(a, b) and a is not used, 
                         * but b could have side effects.
                         */
                        return reduce_expressions (root,
                                                   used_vars,
                                                   dirty_vars,
                                                   change);
                }
                else
                    /*
                     * statement is e.g. append(a, b) and a is not used,
                     * but b could have side effects.
                     */
                    return reduce_expressions (root, 
                                               used_vars,
                                               dirty_vars,
                                               change);
            }
            break;

        case m_serialize:
        case m_trace:
        case m_destroy_ws:
        case m_error:
        case m_print:

#ifndef NDEBUG
            /* update dirty vars */
            for (unsigned int i = 0; i < MIL_MAXCHILD 
                    && root->child[i] != NULL; i++)
                mil_dce_worker (root->child[i], used_vars, dirty_vars, false);
#endif
            /* all variables in the parameter list are used. */
            add_vars_to_used (root, used_vars);
            return root;

        default:
            /*
             * side effect free expression, descend recursively
             * to find side effects in child nodes
             */
            return reduce_expressions (root, used_vars, dirty_vars, change);
    }
}

/**
 * Peform a dead MIL code elimination.
 * It reads the code from the end to the beginning of the program and
 * collects the used variables, starting from the serialize, error and
 * print statements. 
 * Assignments on unused variables are removed, expressions are only
 * kept if they have side effects (e.g. v.append(x)).
 * Problems:
 * If two variables point to the same BAT, an expression with side 
 * effects can be deleted, because the variable is recognized as unused.
 * Example:
 * var x := y;
 * x.append (1);
 * output (y);
 * To solve this problem, some heuristics are implemented in
 * the DEBUG version. They assure that the progam does not contain
 * statements like var_a := var_b if var_a or var_b is used in a
 * function with side effects.
 */
PFmil_t *
PFmil_dce (PFmil_t *root)
{

    PFbitset_t *used_vars = PFbitset ();
    PFbitset_t *dirty_vars = PFbitset ();

    /* variable unused is always used because it helps MonetDB to cleanup */
    PFbitset_set (used_vars, PF_MIL_VAR_UNUSED, true);

    PFmil_t *res = mil_dce_worker (root, used_vars, dirty_vars, true);

    return res;
}

/* vim:set shiftwidth=4 expandtab: */
