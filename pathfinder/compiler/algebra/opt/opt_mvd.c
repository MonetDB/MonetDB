/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on multi valued dependencies. (Cross
 * products and attach operators are moved up
 * along the DAG structure.)
 *
 * We add a new cross operator that can cope with identical
 * columns (la_cross_mvd). This allows us to push down operators
 * into both operands of the cross product whenever we do not
 * know which operand really requires the operators.
 * A cleaning phase then replaces the clone column aware cross products
 * by normal ones and adds additional project operators if name
 * conflicts would arise.
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
#include <assert.h>
#include <stdio.h>

#include "algopt.h"
#include "properties.h"
#include "alg_dag.h"
#include "mem.h"          /* PFmalloc() */
#include "oops.h"

/* apply cse before rewriting */
#include "algebra_cse.h"
/* mnemonic algebra constructors */
#include "logical_mnemonic.h"

/*
 * Easily access subtree-parts.
 */
/** starting from p, make a step left */
#define L(p) ((p)->child[0])
/** starting from p, make a step right */
#define R(p) ((p)->child[1])
/** starting from p, make two steps left */
#define LL(p) L(L(p))
/** starting from p, make a step left, then a step right */
#define LR(p) R(L(p))
/** starting from p, make a step right, then a step left */
#define RL(p) L(R(p))
/** starting from p, make two steps right */
#define RR(p) R(R(p))
/** and so on ... */
#define LLL(p) L(L(L(p)))
#define LRL(p) L(R(L(p)))
#define RLL(p) L(L(R(p)))
#define RRL(p) L(R(R(p)))

#define SEEN(p) ((p)->bit_dag)

/**
 * Use cross product implementation that copes
 * with 'c'loned 'a'ttribute 'n'ames.
 */
#define cross_can(a,b) PFla_cross_clone ((a),(b))

/* Keep track of the ineffective cross product - cross product
   rewrites (cross_changes) and stop if max_cross_changes is reached. */
static unsigned int cross_changes;
static unsigned int max_cross_changes;

static bool
is_cross (PFla_op_t *p)
{
    return (p->kind == la_cross || p->kind == la_cross_mvd);
}

/* check if @a att appears in the schema of operator @a p */
static bool
att_present (PFla_op_t *p, PFalg_att_t att)
{
    for (unsigned int i = 0; i < p->schema.count; i++)
        if (p->schema.items[i].name == att)
            return true;

    return false;
}

/* worker for binary operators */
static bool
modify_binary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_att_t,
                                      PFalg_att_t,
                                      PFalg_att_t))
{
    bool modified = false;

    if (is_cross (L(p))) {
        bool switch_left = att_present (LL(p), p->sem.binary.att1) &&
                           att_present (LL(p), p->sem.binary.att2);
        bool switch_right = att_present (LR(p), p->sem.binary.att1) &&
                            att_present (LR(p), p->sem.binary.att2);
                           
        if (switch_left && switch_right) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.binary.res,
                                  p->sem.binary.att1,
                                  p->sem.binary.att2),
                              op (LR(p),
                                  p->sem.binary.res,
                                  p->sem.binary.att1,
                                  p->sem.binary.att2)));
            modified = true;
        }
        else if (switch_left) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.binary.res,
                                  p->sem.binary.att1,
                                  p->sem.binary.att2),
                              LR(p)));
            modified = true;
        }
        else if (switch_right) {
            *p = *(cross_can (LL(p),
                              op (LR(p),
                                  p->sem.binary.res,
                                  p->sem.binary.att1,
                                  p->sem.binary.att2)));
            modified = true;
        }
    }
    return modified;
}

/* worker for unary operators */
static bool
modify_unary_op (PFla_op_t *p,
                  PFla_op_t * (* op) (const PFla_op_t *,
                                      PFalg_att_t,
                                      PFalg_att_t))
{
    bool modified = false;

    if (is_cross (L(p))) {
        bool switch_left = att_present (LL(p), p->sem.unary.att);
        bool switch_right = att_present (LR(p), p->sem.unary.att);
                           
        if (switch_left && switch_right) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.unary.res,
                                  p->sem.unary.att),
                              op (LR(p),
                                  p->sem.unary.res,
                                  p->sem.unary.att)));
            modified = true;
        }
        else if (switch_left) {
            *p = *(cross_can (op (LL(p),
                                  p->sem.unary.res,
                                  p->sem.unary.att),
                              LR(p)));
            modified = true;
        }
        else if (switch_right) {
            *p = *(cross_can (LL(p),
                              op (LR(p),
                                  p->sem.unary.res,
                                  p->sem.unary.att)));
            modified = true;
        }
    }
    return modified;
}

/* worker for aggregate operators */
static bool
modify_aggr (PFla_op_t *p,
             PFla_op_kind_t kind)
{
    bool modified = false;

    /* An expression that contains the partitioning attribute is
       independent of the aggregate. The translation thus moves the
       expression above the aggregate operator and removes its partitioning
       column. */ 
    if (is_cross (L(p)) &&
        p->sem.aggr.part) {
        if (att_present (LL(p), p->sem.aggr.part) &&
            !att_present (LL(p), p->sem.aggr.att)) {
            *p = *(cross_can (
                      LL(p),
                      aggr (kind, 
                            LR(p),
                            p->sem.aggr.res,
                            p->sem.aggr.att,
                            att_NULL)));
            modified = true;
        }
        /* if not present check the right operand */
        else if (att_present (LR(p), p->sem.aggr.part) &&
                 !att_present (LR(p), p->sem.aggr.att)) {
            *p = *(cross_can (
                      LR(p),
                      aggr (kind, 
                            LL(p),
                            p->sem.aggr.res,
                            p->sem.aggr.att,
                            att_NULL)));
            modified = true;
        }
    }
    return modified;
}

/* check if the semantical information 
   of two attributes matches */
static bool
project_identical (PFla_op_t *a, PFla_op_t *b)
{
    if (a->sem.proj.count != b->sem.proj.count)
        return false;

    for (unsigned int i = 0; i < a->sem.proj.count; i++)
        if (a->sem.proj.items[i].new != b->sem.proj.items[i].new
            || a->sem.proj.items[i].old != b->sem.proj.items[i].old)
            return false;

    return true;
}

/**
 * worker for PFalgopt_mvd
 *
 * opt_mvd looks up an independent expression and
 * tries to move it up the DAG as much as possible.
 */
static bool
opt_mvd (PFla_op_t *p)
{
    bool modified = false;
    bool cross_cross = false;

    assert (p);

    /* rewrite each node only once */
    if (SEEN(p))
        return false;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        modified = opt_mvd (p->child[i]) || modified ;

    /** 
     * In the following action code we try to propagate cross
     * products up the DAG as far as possible. To avoid possible 
     * 'deadlocks' we discard all independent expressions that
     * are constant thus reducing the number of rivalling cross
     * product operators.
     *
     * Exceptions are the attach and the union operator where
     * the nested attach operator introduces new independencies.
     * (For intersect and difference we assume that constant
     *  optimization already removed the attach operators.)
     */

    /* look if there is an occurrence of two nested cross products */
    if (!is_cross (p) &&
        ((L(p) &&
          is_cross (L(p)) &&
          (is_cross (LL(p)) || is_cross (LR(p))))
         ||
         (R(p) &&
          is_cross (R(p)) &&
          (is_cross (RL(p)) || is_cross (RR(p))))))
        cross_cross = true;

    /* action code */
    switch (p->kind) {

    case la_serialize:
        break;

    case la_lit_tbl:
    case la_empty_tbl:
        /* nothing to do -- everything is constant */
        break;

    case la_attach:
        /* Replace two consecutive attach operators by a cross
           product of the input of the nested attach operator and
           a base table. The idea is to move possibly independent
           expression up the DAG. */
        if (L(p)->kind == la_attach) {
            *p = *(cross_can (
                      LL(p),
                      attach (
                          lit_tbl (attlist (L(p)->sem.attach.res),
                                   tuple (L(p)->sem.attach.value)),
                          p->sem.attach.res,
                          p->sem.attach.value)));
            modified = true;
        }
        else if (is_cross (L(p))) {
            /* push attach into both cross product operands */
            *p = *(cross_can (
                       attach (LL(p),
                               p->sem.attach.res,
                               p->sem.attach.value),
                       attach (LR(p),
                               p->sem.attach.res,
                               p->sem.attach.value)));
            modified = true;
        }
        break;

    case la_cross:
    case la_cross_mvd:
        /* If there are two nested cross products make sure that
           each operand has the chance to be propagated up the DAG:

                 x            x              x          x
                / \          / \            / \        / \
               1   x    =>  2   x   and    x   3  =>  1   x   
                  / \          / \        / \            / \
                 2   3        3   1      1   2          2   3

           The only problem is that this might cause an endless
           loop if the above operator introduces a dependency.
           Therefore we introduce a check at the end of this function
           that sets the 'modified' bit to false if multiple times
           two adjacent nested cross products did not cause a rewrite.
          */

        if (is_cross (L(p))) {
            *p = *(cross_can (LL(p),
                              cross_can (LR(p),
                                         R(p))));
            modified = true;
        }
        else if (is_cross (R(p))) {
            *p = *(cross_can (RL(p),
                              cross_can (RR(p),
                                         L(p))));
            modified = true;
        }
        break;

    case la_eqjoin_unq:
        break;

    case la_eqjoin:
        /* Move the independent expression (the one without
           join attribute) up the DAG. */
        if (is_cross (L(p))) {
            if (att_present (LL(p), p->sem.eqjoin.att1))
                *p = *(cross_can (LR(p), eqjoin (LL(p), R(p), 
                                                 p->sem.eqjoin.att1,
                                                 p->sem.eqjoin.att2)));
            else
                *p = *(cross_can (LL(p), eqjoin (LR(p), R(p),
                                                 p->sem.eqjoin.att1,
                                                 p->sem.eqjoin.att2)));

            modified = true;
            break;

        }
        if (is_cross (R(p))) {
            if (att_present (RL(p), p->sem.eqjoin.att2))
                *p = *(cross_can (RR(p), eqjoin (L(p), RL(p),
                                                 p->sem.eqjoin.att1,
                                                 p->sem.eqjoin.att2)));
            else
                *p = *(cross_can (RL(p), eqjoin (L(p), RR(p),
                                                 p->sem.eqjoin.att1,
                                                 p->sem.eqjoin.att2)));

            modified = true;
            break;
        }
        break;

    case la_semijoin:
        /* Move the independent expression (the one without
           join attribute) up the DAG. */
        if (is_cross (L(p))) {
            if (att_present (LL(p), p->sem.eqjoin.att1))
                *p = *(cross_can (LR(p), semijoin (LL(p), R(p), 
                                                   p->sem.eqjoin.att1,
                                                   p->sem.eqjoin.att2)));
            else
                *p = *(cross_can (LL(p), semijoin (LR(p), R(p),
                                                   p->sem.eqjoin.att1,
                                                   p->sem.eqjoin.att2)));

            modified = true;
            break;

        }
        break;
        
    case la_thetajoin:
        /* Move the independent expression (the one without any
           join attributes) up the DAG. */
        if (is_cross (L(p))) {
            bool all_left = true,
                 all_right = true;
            
            for (unsigned int i = 0; i < p->sem.thetajoin.count; i++) {
                all_left  = all_left &&
                            att_present (LL(p), p->sem.thetajoin.pred[i].left);
                all_right = all_right &&
                            att_present (LR(p), p->sem.thetajoin.pred[i].left);
            }

            if (all_left)
                *p = *(cross_can (LR(p), thetajoin (LL(p), R(p), 
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else if (all_right)
                *p = *(cross_can (LL(p), thetajoin (LR(p), R(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else
                /* do not rewrite */
                break;

            modified = true;
            break;
        }
        if (is_cross (R(p))) {
            bool all_left = true,
                 all_right = true;
            
            for (unsigned int i = 0; i < p->sem.thetajoin.count; i++) {
                all_left  = all_left &&
                            att_present (RL(p), p->sem.thetajoin.pred[i].right);
                all_right = all_right &&
                            att_present (RR(p), p->sem.thetajoin.pred[i].right);
            }

            if (all_left)
                *p = *(cross_can (RR(p), thetajoin (L(p), RL(p), 
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else if (all_right)
                *p = *(cross_can (RL(p), thetajoin (L(p), RR(p),
                                                    p->sem.thetajoin.count,
                                                    p->sem.thetajoin.pred)));
            else
                /* do not rewrite */
                break;

            modified = true;
            break;
        }
        break;
        
    case la_project:
        /* Split project operator and push it beyond the cross product. */
        if (is_cross (L(p))) {
            PFalg_proj_t *proj_list1, *proj_list2;
            unsigned int count1 = 0, count2 = 0;

            /* create first projection list */
            proj_list1 = PFmalloc (p->schema.count *
                                   sizeof (*(proj_list1)));

            for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    if (LL(p)->schema.items[i].name 
                        == p->sem.proj.items[j].old) {
                        proj_list1[count1++] = p->sem.proj.items[j];
                    }

            /* create second projection list */
            proj_list2 = PFmalloc (p->schema.count *
                                   sizeof (*(proj_list1)));

            for (unsigned int i = 0; i < LR(p)->schema.count; i++)
                for (unsigned int j = 0; j < p->sem.proj.count; j++)
                    if (LR(p)->schema.items[i].name 
                        == p->sem.proj.items[j].old) {
                        proj_list2[count2++] = p->sem.proj.items[j];
                    }

            /* Ensure that both arguments add at least one column to
               the result. */
            if (count1 && count2) {
                *p = *(cross_can (
                          PFla_project_ (LL(p), count1, proj_list1),
                          PFla_project_ (LR(p), count2, proj_list2)));
                modified = true;
            }
        }
        break;

    case la_select:
        if (is_cross (L(p))) {
            if (att_present (LL(p), p->sem.select.att))
                *p = *(cross_can (LR(p), select_ (LL(p),
                                                  p->sem.select.att)));
            else
                *p = *(cross_can (LL(p), select_ (LR(p),
                                                  p->sem.select.att)));

            modified = true;
        }
        break;

    case la_pos_select:
        /* An expression that does not contain any sorting column
           required by the positional select operator, but contains
           the partitioning attribute is independent of the positional
           select. The translation thus moves the expression above 
           the positional selection and removes its partitioning column. */ 
        if (is_cross (L(p)) &&
            p->sem.pos_sel.part) {
            bool part, sortby;

            /* first check the dependencies of the left cross product input */
            part = false;
            sortby = false;
            for (unsigned int i = 0; i < LL(p)->schema.count; i++) {
                if (LL(p)->schema.items[i].name == p->sem.pos_sel.part)
                    part = true;
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.pos_sel.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.pos_sel.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LL(p),
                          pos_select (
                              LR(p),
                              p->sem.pos_sel.pos,
                              p->sem.pos_sel.sortby,
                              att_NULL)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            part = false;
            sortby = false;
            for (unsigned int i = 0; i < LR(p)->schema.count; i++) {
                if (LR(p)->schema.items[i].name == p->sem.pos_sel.part)
                    part = true;
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.pos_sel.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.pos_sel.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LR(p),
                          pos_select (
                              LL(p),
                              p->sem.pos_sel.pos,
                              p->sem.pos_sel.sortby,
                              att_NULL)));
                modified = true;
                break;
            }
        }
        break;

    case la_disjunion:
        /* If the children of the union operator are independent
           expressions (both attach operator or cross product) which
           reference the same subexpression, we move this expression
           above the union. */
        if (L(p)->kind == la_attach && 
            R(p)->kind == la_attach &&
            LL(p) == RL(p)) {
            *p = *(cross_can (
                      LL(p),
                      disjunion (
                          lit_tbl (attlist (L(p)->sem.attach.res),
                                   tuple (L(p)->sem.attach.value)),
                          lit_tbl (attlist (R(p)->sem.attach.res),
                                   tuple (R(p)->sem.attach.value)))));
            modified = true;
            break;
        }
        else if (L(p)->kind == la_attach &&
                 is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (
                           LL(p),
                           disjunion (
                               lit_tbl (attlist (L(p)->sem.attach.res),
                                        tuple (L(p)->sem.attach.value)),
                               RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (
                           LL(p),
                           disjunion (
                               lit_tbl (attlist (L(p)->sem.attach.res),
                                        tuple (L(p)->sem.attach.value)),
                               RL(p))));
                modified = true;
                break;
            }
        }
        else if (is_cross (L(p)) &&
                 R(p)->kind == la_attach) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (
                           RL(p),
                           disjunion (
                               lit_tbl (attlist (R(p)->sem.attach.res),
                                        tuple (R(p)->sem.attach.value)),
                               LR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (
                           RL(p),
                           disjunion (
                               lit_tbl (attlist (R(p)->sem.attach.res),
                                        tuple (R(p)->sem.attach.value)),
                               LL(p))));
                modified = true;
                break;
            }
        }
        else if (is_cross (L(p)) &&
                 is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RL(p))));
                modified = true;
                break;
            }
            else if (LL(p)->kind == la_project &&
                     RL(p)->kind == la_project &&
                     LLL(p) == RLL(p) &&
                     project_identical (LL(p), RL(p))) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p)->kind == la_project &&
                     RL(p)->kind == la_project &&
                     LRL(p) == RLL(p) &&
                     project_identical (LR(p), RL(p))) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p)->kind == la_project &&
                     RR(p)->kind == la_project &&
                     LLL(p) == RRL(p) &&
                     project_identical (LL(p), RR(p))) {
                *p = *(cross_can (LL(p), disjunion (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p)->kind == la_project &&
                     RR(p)->kind == la_project &&
                     LRL(p) == RRL(p) &&
                     project_identical (LR(p), RR(p))) {
                *p = *(cross_can (LR(p), disjunion (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_intersect:
        /* If the children of the intersect operator are independent
           expressions (cross product) which reference the same
           subexpression, we move this expression above the intersect. */
        if (is_cross (L(p)) &&
            is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), intersect (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), intersect (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), intersect (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), intersect (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_difference:
        /* If the children of the difference operator are independent
           expressions (cross product) which reference the same
           subexpression, we move this expression above the difference. */
        if (is_cross (L(p)) &&
            is_cross (R(p))) {
            if (LL(p) == RL(p)) {
                *p = *(cross_can (LL(p), difference (LR(p), RR(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RL(p)) {
                *p = *(cross_can (LR(p), difference (LL(p), RR(p))));
                modified = true;
                break;
            }
            else if (LL(p) == RR(p)) {
                *p = *(cross_can (LL(p), difference (LR(p), RL(p))));
                modified = true;
                break;
            }
            else if (LR(p) == RR(p)) {
                *p = *(cross_can (LR(p), difference (LL(p), RL(p))));
                modified = true;
                break;
            }
        }
        break;

    case la_distinct:
        /* Push distinct into both cross product operands. */
        if (is_cross (L(p))) {
            *p = *(cross_can (distinct (LL(p)), distinct (LR(p))));
            modified = true;
        }
        break;
        
    case la_fun_1to1:
        if (is_cross (L(p))) {
            bool switch_left = true;
            bool switch_right = true;

            for (unsigned int i = 0; i < p->sem.fun_1to1.refs.count; i++) {
                switch_left  = switch_left &&
                               att_present (LL(p),
                                            p->sem.fun_1to1.refs.atts[i]);
                switch_right = switch_right &&
                               att_present (LR(p),
                                            p->sem.fun_1to1.refs.atts[i]);
            }
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (fun_1to1 (LL(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs),
                                  fun_1to1 (LR(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (fun_1to1 (LL(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  fun_1to1 (LR(p),
                                            p->sem.fun_1to1.kind,
                                            p->sem.fun_1to1.res,
                                            p->sem.fun_1to1.refs)));
                modified = true;
            }
        }
        break;

    case la_num_eq:
        modified = modify_binary_op (p, PFla_eq);
        break;
    case la_num_gt:
        modified = modify_binary_op (p, PFla_gt);
        break;
    case la_bool_and:
        modified = modify_binary_op (p, PFla_and);
        break;
    case la_bool_or:
        modified = modify_binary_op (p, PFla_or);
        break;
    case la_bool_not:
        modified = modify_unary_op (p, PFla_not);
        break;
        
    case la_to:
        if (is_cross (L(p))) {
            if (att_present (LL(p), p->sem.to.att1) &&
                att_present (LL(p), p->sem.to.att2)) {
                *p = *(cross_can (
                           LR(p),
                           project (to (attach (LL(p),
                                                p->sem.to.part,
                                                lit_nat(1)),
                                        p->sem.to.res,
                                        p->sem.to.att1,
                                        p->sem.to.att2,
                                        p->sem.to.part),
                                    proj (p->sem.to.res,
                                          p->sem.to.res))));
                modified = true;
            } else if (att_present (LR(p), p->sem.to.att1) &&
                       att_present (LR(p), p->sem.to.att2)) {
                *p = *(cross_can (
                           LL(p),
                           project (to (attach (LR(p),
                                                p->sem.to.part,
                                                lit_nat(1)),
                                        p->sem.to.res,
                                        p->sem.to.att1,
                                        p->sem.to.att2,
                                        p->sem.to.part),
                                    proj (p->sem.to.res,
                                          p->sem.to.res))));
                modified = true;
            }
        }
        break;
        
    case la_avg:
        modified = modify_aggr (p, la_avg);
        break;
    case la_max:
        modified = modify_aggr (p, la_max);
        break;
    case la_min:
        modified = modify_aggr (p, la_min);
        break;
    case la_sum:
        modified = modify_aggr (p, la_sum);
        break;
    case la_count:
        /* An expression that contains the partitioning attribute is
           independent of the aggregate. The translation thus moves the
           expression above the aggregate operator and removes its partitioning
           column. */ 
        if (is_cross (L(p)) &&
            p->sem.aggr.part) {
            if (att_present (LL(p), p->sem.aggr.part)) {
                *p = *(cross_can (
                          LL(p),
                          count (LR(p),
                                 p->sem.aggr.res,
                                 att_NULL)));
                modified = true;
            }
            /* if not present it has to be in the right operand */
            else {
                *p = *(cross_can (
                          LR(p),
                          count (LL(p),
                                 p->sem.aggr.res,
                                 att_NULL)));
                modified = true;
            }
        }
        break;

    case la_rownum:
        /* An expression that does not contain any sorting column
           required by the rownum operator, but contains the partitioning
           attribute is independent of the rownum. The translation thus
           moves the expression above the rownum and removes its partitioning
           column. */ 
        if (is_cross (L(p)) &&
            p->sem.rownum.part) {
            bool part, sortby;

            /* first check the dependencies of the left cross product input */
            part = false;
            sortby = false;
            for (unsigned int i = 0; i < LL(p)->schema.count; i++) {
                if (LL(p)->schema.items[i].name == p->sem.rownum.part)
                    part = true;
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.rownum.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.rownum.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LL(p),
                          rownum (
                              LR(p),
                              p->sem.rownum.res,
                              p->sem.rownum.sortby,
                              att_NULL)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            part = false;
            sortby = false;
            for (unsigned int i = 0; i < LR(p)->schema.count; i++) {
                if (LR(p)->schema.items[i].name == p->sem.rownum.part)
                    part = true;
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.rownum.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.rownum.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            }
            if (part && !sortby) {
                *p = *(cross_can (
                          LR(p),
                          rownum (
                              LL(p),
                              p->sem.rownum.res,
                              p->sem.rownum.sortby,
                              att_NULL)));
                modified = true;
                break;
            }
        }
        break;

    case la_rank:
        /* An expression that does not contain any sorting column
           required by the rank operator, is independent of the rank. 
           The translation thus moves the expression above the rank. */
        if (is_cross (L(p))) {
            bool sortby;

            /* first check the dependencies of the left cross product input */
            sortby = false;
            for (unsigned int i = 0; i < LL(p)->schema.count; i++)
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.rank.sortby);
                     j++)
                    if (LL(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.rank.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            if (!sortby) {
                *p = *(cross_can (
                          LL(p),
                          rank (
                              LR(p),
                              p->sem.rank.res,
                              p->sem.rank.sortby)));
                modified = true;
                break;
            }

            /* then check the dependencies of the right cross product input */
            sortby = false;
            for (unsigned int i = 0; i < LR(p)->schema.count; i++)
                for (unsigned int j = 0;
                     j < PFord_count (p->sem.rownum.sortby);
                     j++)
                    if (LR(p)->schema.items[i].name 
                        == PFord_order_col_at (
                               p->sem.rownum.sortby,
                               j)) {
                        sortby = true;
                        break;
                    }
            if (!sortby) {
                *p = *(cross_can (
                          LR(p),
                          rank (
                              LL(p),
                              p->sem.rank.res,
                              p->sem.rank.sortby)));
                modified = true;
                break;
            }
        }
        break;

    case la_number:
        break;

    case la_type:
        if (is_cross (L(p))) {
            bool switch_left = att_present (LL(p), p->sem.type.att);
            bool switch_right = att_present (LR(p), p->sem.type.att);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (type (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty),
                                  type (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (type (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  type (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty)));
                modified = true;
            }
        }
        break;
        
    case la_type_assert:
        if (is_cross (L(p))) {
            bool switch_left = att_present (LL(p), p->sem.type.att);
            bool switch_right = att_present (LR(p), p->sem.type.att);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (type_assert_pos (
                                      LL(p),
                                      p->sem.type.att,
                                      p->sem.type.ty),
                                  type_assert_pos (
                                      LR(p),
                                      p->sem.type.att,
                                      p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (type_assert_pos (
                                      LL(p),
                                      p->sem.type.att,
                                      p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  type_assert_pos (
                                      LR(p),
                                      p->sem.type.att,
                                      p->sem.type.ty)));
                modified = true;
            }
        }
        break;
        
    case la_cast:
        if (is_cross (L(p))) {
            bool switch_left = att_present (LL(p), p->sem.type.att);
            bool switch_right = att_present (LR(p), p->sem.type.att);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (cast (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty),
                                  cast (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (cast (LL(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty),
                                  LR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (LL(p),
                                  cast (LR(p),
                                        p->sem.type.res,
                                        p->sem.type.att,
                                        p->sem.type.ty)));
                modified = true;
            }
        }
        break;
        
    case la_seqty1:
        if (is_cross (L(p)) &&
            p->sem.aggr.part) {
            if (att_present (LL(p), p->sem.aggr.part) &&
                !att_present (LL(p), p->sem.aggr.att)) {
                *p = *(cross_can (
                          LL(p),
                          seqty1 (
                              LR(p),
                              p->sem.aggr.res,
                              p->sem.aggr.att,
                              att_NULL)));
                modified = true;
            }
            /* if not present check the right operand */
            else if (att_present (LR(p), p->sem.aggr.part) &&
                     !att_present (LR(p), p->sem.aggr.att)) {
                *p = *(cross_can (
                          LR(p),
                          seqty1 (
                              LL(p),
                              p->sem.aggr.res,
                              p->sem.aggr.att,
                              att_NULL)));
                modified = true;
            }
        }
        break;

    case la_all:
        if (is_cross (L(p)) &&
            p->sem.aggr.part) {
            if (att_present (LL(p), p->sem.aggr.part) &&
                !att_present (LL(p), p->sem.aggr.att)) {
                *p = *(cross_can (
                          LL(p),
                          all (
                              LR(p),
                              p->sem.aggr.res,
                              p->sem.aggr.att,
                              att_NULL)));
                modified = true;
            }
            /* if not present check the right operand */
            else if (att_present (LR(p), p->sem.aggr.part) &&
                     !att_present (LR(p), p->sem.aggr.att)) {
                *p = *(cross_can (
                          LR(p),
                          all (
                              LL(p),
                              p->sem.aggr.res,
                              p->sem.aggr.att,
                              att_NULL)));
                modified = true;
            }
        }
        break;
        
    case la_step:
        if (is_cross (R(p))) {
            if (att_present (RL(p), p->sem.step.item)) {
                *p = *(cross_can (
                           RR(p),
                           project (step (L(p),
                                            attach (RL(p),
                                                    p->sem.step.iter,
                                                    lit_nat(1)),
                                            p->sem.step.axis,
                                            p->sem.step.ty,
                                            p->sem.step.level,
                                            p->sem.step.iter,
                                            p->sem.step.item,
                                            p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            } else {
                *p = *(cross_can (
                           RL(p),
                           project (step (L(p),
                                            attach (RR(p),
                                                    p->sem.step.iter,
                                                    lit_nat(1)),
                                            p->sem.step.axis,
                                            p->sem.step.ty,
                                            p->sem.step.level,
                                            p->sem.step.iter,
                                            p->sem.step.item,
                                            p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            }
            modified = true;
        }
        break;
        
    case la_step_join:
        if (is_cross (R(p))) {
            bool switch_left = att_present (RL(p), p->sem.step.item);
            bool switch_right = att_present (RR(p), p->sem.step.item);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (step_join (
                                        L(p),
                                        RL(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  step_join (
                                        L(p), 
                                        RR(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (step_join (
                                        L(p), RL(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  step_join (
                                        L(p),
                                        RR(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
        }
        break;
        
    case la_guide_step:
        if (is_cross (R(p))) {
            if (att_present (RL(p), p->sem.step.item)) {
                *p = *(cross_can (
                           RR(p),
                           project (guide_step (
                                        L(p),
                                        attach (RL(p),
                                                p->sem.step.iter,
                                                lit_nat(1)),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.iter,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            } else {
                *p = *(cross_can (
                           RL(p),
                           project (guide_step (
                                        L(p),
                                        attach (RR(p),
                                                p->sem.step.iter,
                                                lit_nat(1)),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.iter,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                    proj (p->sem.step.item_res,
                                          p->sem.step.item_res))));
            }
            modified = true;
        }
        break;
        
    case la_guide_step_join:
        if (is_cross (R(p))) {
            bool switch_left = att_present (RL(p), p->sem.step.item);
            bool switch_right = att_present (RR(p), p->sem.step.item);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (guide_step_join (
                                        L(p),
                                        RL(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  guide_step_join (
                                        L(p), 
                                        RR(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (guide_step_join (
                                        L(p), RL(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  guide_step_join (
                                        L(p),
                                        RR(p),
                                        p->sem.step.axis,
                                        p->sem.step.ty,
                                        p->sem.step.guide_count,
                                        p->sem.step.guides,
                                        p->sem.step.level,
                                        p->sem.step.item,
                                        p->sem.step.item_res)));
                modified = true;
            }
        }
        break;
        
    case la_id:
        if (is_cross (R(p))) {
            if (att_present (RL(p), p->sem.id.item) &&
                att_present (RL(p), p->sem.id.item_doc)) {
                *p = *(cross_can (
                           RR(p),
                           project (id (L(p),
                                        attach (RL(p),
                                                p->sem.id.iter,
                                                lit_nat(1)),
                                        p->sem.id.iter,
                                        p->sem.id.item,
                                        p->sem.id.item_res,
                                        p->sem.id.item_doc),
                                    proj (p->sem.id.item_res,
                                          p->sem.id.item_res))));
                modified = true;
            } else if (att_present (RR(p), p->sem.id.item) &&
                       att_present (RR(p), p->sem.id.item_doc)) {
                *p = *(cross_can (
                           RL(p),
                           project (id (L(p),
                                        attach (RR(p),
                                                p->sem.id.iter,
                                                lit_nat(1)),
                                        p->sem.id.iter,
                                        p->sem.id.item,
                                        p->sem.id.item_res,
                                        p->sem.id.item_doc),
                                    proj (p->sem.id.item_res,
                                          p->sem.id.item_res))));
                modified = true;
            }
        }
        break;
        
    case la_idref:
        if (is_cross (R(p))) {
            if (att_present (RL(p), p->sem.id.item) &&
                att_present (RL(p), p->sem.id.item_doc)) {
                *p = *(cross_can (
                           RR(p),
                           project (idref (
                                        L(p),
                                        attach (RL(p),
                                                p->sem.id.iter,
                                                lit_nat(1)),
                                        p->sem.id.iter,
                                        p->sem.id.item,
                                        p->sem.id.item_res,
                                        p->sem.id.item_doc),
                                    proj (p->sem.id.item_res,
                                          p->sem.id.item_res))));
                modified = true;
            } else if (att_present (RR(p), p->sem.id.item) &&
                       att_present (RR(p), p->sem.id.item_doc)) {
                *p = *(cross_can (
                           RL(p),
                           project (idref (
                                        L(p),
                                        attach (RR(p),
                                                p->sem.id.iter,
                                                lit_nat(1)),
                                        p->sem.id.iter,
                                        p->sem.id.item,
                                        p->sem.id.item_res,
                                        p->sem.id.item_doc),
                                    proj (p->sem.id.item_res,
                                          p->sem.id.item_res))));
                modified = true;
            }
        }
        break;
        
    case la_doc_tbl:
        /* should not appear as roots already
           translates the doc_tbl operator. */
        break;
        
    case la_doc_access:
        if (is_cross (R(p))) {
            bool switch_left = att_present (RL(p), p->sem.doc_access.att);
            bool switch_right = att_present (RR(p), p->sem.doc_access.att);
                               
            if (switch_left && switch_right) {
                *p = *(cross_can (doc_access (L(p), RL(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.att,
                                        p->sem.doc_access.doc_col),
                                  doc_access (L(p), RR(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.att,
                                        p->sem.doc_access.doc_col)));
                modified = true;
            }
            else if (switch_left) {
                *p = *(cross_can (doc_access (L(p), RL(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.att,
                                        p->sem.doc_access.doc_col),
                                  RR(p)));
                modified = true;
            }
            else if (switch_right) {
                *p = *(cross_can (RL(p),
                                  doc_access (L(p), RR(p),
                                        p->sem.doc_access.res,
                                        p->sem.doc_access.att,
                                        p->sem.doc_access.doc_col)));
                modified = true;
            }
        }
        break;

    case la_twig:
    case la_fcns:
    case la_docnode:
    case la_element:
    case la_attribute:
    case la_textnode:
    case la_comment:
    case la_processi:
    case la_content:
    case la_merge_adjacent:
        /* constructors introduce like the unpartitioned 
           number or rownum operators a dependency. */
        break;
        
    case la_roots:
        /* modify the only pattern starting in roots
           that is no constructor: roots-doc_tbl */
        if (L(p)->kind == la_doc_tbl) {
            if (is_cross (LL(p))) {
                if (att_present (LL(L(p)), L(p)->sem.doc_tbl.item)) {
                    /* save iter relation */
                    PFla_op_t *iter_rel = LR(L(p));
                    /* overwrite doc_tbl node to update
                       both roots and frag operators */
                    *(L(p)) = *(doc_tbl (attach (LL(L(p)),
                                                 L(p)->sem.doc_tbl.iter,
                                                 lit_nat (1)),
                                         L(p)->sem.doc_tbl.iter,
                                         L(p)->sem.doc_tbl.item,
                                         L(p)->sem.doc_tbl.item_res));
                    /* push roots + doc_tbl through the cross product */
                    *p = *(cross_can (
                               iter_rel,
                               project (roots (L(p)),
                                        proj (L(p)->sem.doc_tbl.item_res,
                                              L(p)->sem.doc_tbl.item_res))));
                }
                else {
                    /* save iter relation */
                    PFla_op_t *iter_rel = LL(L(p));
                    /* overwrite doc_tbl node to update
                       both roots and frag operators */
                    *(L(p)) = *(doc_tbl (attach (LR(L(p)),
                                                 L(p)->sem.doc_tbl.iter,
                                                 lit_nat (1)),
                                         L(p)->sem.doc_tbl.iter,
                                         L(p)->sem.doc_tbl.item,
                                         L(p)->sem.doc_tbl.item_res));
                    /* push roots + doc_tbl through the cross product */
                    *p = *(cross_can (
                               iter_rel,
                               project (roots (L(p)),
                                        proj (L(p)->sem.doc_tbl.item_res,
                                              L(p)->sem.doc_tbl.item_res))));
                }
                modified = true;
            }
            else if (LL(p)->kind == la_attach && 
                     LL(p)->sem.attach.res == L(p)->sem.doc_tbl.item) {
                /* save iter relation */
                PFla_op_t *iter_rel = LL(L(p));
                
                /* create base table and overwrite doc_tbl 
                   node to update both roots and frag operators */
                *(L(p)) = *(doc_tbl (lit_tbl (
                                         attlist (L(p)->sem.doc_tbl.iter,
                                                  L(p)->sem.doc_tbl.item),
                                         tuple (lit_nat (1),
                                                LL(p)->sem.attach.value)),
                                     L(p)->sem.doc_tbl.iter,
                                     L(p)->sem.doc_tbl.item,
                                     L(p)->sem.doc_tbl.item_res));

                /* Apply the cross product with the possibly
                   huge iter relation. */
                *p = *(cross_can (
                          iter_rel,
                          project (
                              roots (L(p)),
                              proj (L(p)->sem.doc_tbl.item_res,
                                    L(p)->sem.doc_tbl.item_res))));
                modified = true;
            }
        }
        break;
        
    case la_fragment:
        break;
    case la_frag_union:
        break;
    case la_empty_frag:
        break;
        
    case la_cond_err:
        if (is_cross (L(p))) {
            *p = *(cross_can (cond_err (LL(p), R(p), 
                                        p->sem.err.att,
                                        p->sem.err.str),
                              LR(p)));
            modified = true;
        }
        break;
        
    case la_nil:
    case la_trace:
    case la_trace_msg:
    case la_trace_map:
        /* we may not modify the cardinality */
    case la_rec_fix:
    case la_rec_param:
    case la_rec_arg:
    case la_rec_base:
        /* do not rewrite anything that has to do with recursion */
        break;
                
    case la_proxy:
        /**
         * ATTENTION: The proxies (especially the kind=1 version) are 
         * generated in such a way, that the following rewrite does not
         * introduce inconsistencies. 
         * The following rewrite would break if the proxy contains operators
         * that are themselves not allowed to be rewritten by the multi-value
         * dependency optimization phase. We may not transform expressions that
         * rely on the cardinality of their inputs.
         *
         * In the current situation these operators are:
         * - aggregates (sum and count)
         * - rownum and number operators
         * - constructors (element, attribute, and textnode)
         * - fn:string-join
         *
         * By exploiting the semantics of our generated plans (at the creation
         * date of this rule) we ensure that none of the problematic operators
         * appear in the plans. 
         *
         *               !!! BEWARE THIS IS NOT CHECKED !!!
         *
         * (A future redefinition of the translation scheme might break it.)
         *
         * Here are the reasons why it currently works without problems:
         * - aggregates (sum and count):
         *    The aggregates are only a problem if used without partitioning
         *    or if the partition criterion is not inferred from the number
         *    operator at the proxy exit. As an aggregate always uses the
         *    current scope (ensured during generation) and all scopes are
         *    properly nested, we have a functional dependency between the
         *    partitioning attribute of an aggregate inside this proxy 
         *    (which is also equivalent to a scope) and the proxy exit number
         *    operator.
         *
         * - rownum and number operators:
         *    With a similar explanation as the one for aggregates we can be
         *    sure that every rownum and number operator inside the proxy is
         *    not used outside the proxy pattern. The generation process
         *    ensures that these are not used outside the scope or if used
         *    outside are partitioned by the number of the number operator at
         *    theproxy exit.
         *
         * - constructors (element, attribute, and textnode):
         *    Constructors are never allowed inside the proxy of kind=1.
         *    This is ensured by the fragment information that introduces
         *    conflicting references at the constructors -- they are linked
         *    from outside (via la_fragment) and inside (via la_roots) the
         *    proxy -- which cannot be resolved. This leads to the abortion
         *    of the proxy generation whenever a constructor appears inside
         *    the proxy.
         *
         * - fn:string-join
         *    The same scope arguments as for the aggregates apply.
         *
         */

        /**
         * We are looking for a proxy (of kind 1) followed by a cross product.
         * Thus we first ensure that the proxy still has the correct shape
         * (see Figure (1)) and then check whether the proxy is independent
         * of tree t1. If that's the case we rewrite the DAG in Figure (1)
         * into the one of Figure (2). 
         *
         *                proxy (kind=1)                 X
         *      __________/ |  \___                     / \
         *     /(sem.base1) pi_1   \ (sem.ref)        t1   proxy (kind=1)                     
         *     |            |       |            __________/ |  \___
         *     |           |X|      |           /(sem.base1) pi_1'  \ (sem.ref)
         *     |          /   \     |           |            |       |
         *     |         |     pi_2 |           |           |X|      |
         *     |         |     |    |           |          /   \     |
         *     |         |    |X|   |           |         |     pi_2 |
         *     |        pi_3  / \   |           |         |     |    |
         *     |         |   /___\  |           |         |    |X|   |
         *     |         |     | __/            |        pi_3' / \   |
         *     |         |     |/               |         |   /___\  |
         *     |         |     pi_4             |         |     | __/ 
         *     |         |     |                |         |     |/    
         *     |          \   /                 |         |     pi_4' 
         *     |           \ /                  |         |     |     
         *     \______      #                   |          \   /      
         *            \    /                    |           \ /       
         *          proxy_base                  \______      #        
         *              |                              \    /         
         *              X                            proxy_base       
         *             / \                               |            
         *           t1   t2                             t2              
         *                                                            
         *            ( 1 )                             ( 2 )
         *                                                            
         *
         * The changes happen at the 3 projections: pi_1, pi_3, and pi_4.
         * - All columns of t1 in pi_1 are projected out (resulting in pi_1').
         * - All columns of t1 in pi_3 and pi_4 are replaced by a dummy
         *   column (the first column of t2) thus resulting in the modified
         *   projections pi_3' and pi_4', respectively. We don't throw out
         *   the columns of t1 in the proxy as this would require a bigger
         *   rewrite, which is done eventually by the following icols
         *   optimization.
         */
        if (is_cross (L(p->sem.proxy.base1)) &&
            p->sem.proxy.kind == 1 &&
            /* check consistency */
            L(p)->kind == la_project &&
            LL(p)->kind == la_eqjoin &&
            L(LL(p))->kind == la_project &&
            R(LL(p))->kind == la_project &&
            RL(LL(p))->kind == la_eqjoin &&
            LL(LL(p))->kind == la_number &&
            L(LL(LL(p))) == p->sem.proxy.base1 &&
            p->sem.proxy.ref->kind == la_project &&
            L(p->sem.proxy.ref) == LL(LL(p))) {

            PFla_op_t *cross = L(p->sem.proxy.base1);
            PFla_op_t *lcross, *rcross;
            PFla_op_t *ref = p->sem.proxy.ref;
            unsigned int i, j, count = 0;
            bool rewrite = false;

            /* first check the dependencies of the left cross product input */
            for (i = 0; i < L(cross)->schema.count; i++) 
                for (j = 0; j < p->sem.proxy.req_cols.count; j++)
                    if (L(cross)->schema.items[i].name 
                        == p->sem.proxy.req_cols.atts[j]) {
                        count++;
                        break;
                    }
            if (p->sem.proxy.req_cols.count == count) {
                lcross = L(cross);
                rcross = R(cross);
                rewrite = true;
            }
            else {
                count = 0;
                /* then check the dependencies of the right cross product
                   input */
                for (i = 0; i < R(cross)->schema.count; i++)
                    for (j = 0; j < p->sem.proxy.req_cols.count; j++)
                        if (R(cross)->schema.items[i].name 
                            == p->sem.proxy.req_cols.atts[j]) {
                            count++;
                            break;
                        }
                if (p->sem.proxy.req_cols.count == count) {
                    lcross = R(cross);
                    rcross = L(cross);
                    rewrite = true;
                }
            }

            if (rewrite) {
                PFalg_att_t dummy_col = lcross->schema.items[0].name;
                /* pi_1' */
                PFalg_proj_t *proj_proxy = PFmalloc (L(p)->schema.count *
                                                     sizeof (PFalg_proj_t));
                /* pi_3' */
                PFalg_proj_t *proj_left = PFmalloc (L(LL(p))->schema.count *
                                                    sizeof (PFalg_proj_t));
                /* pi_4' */
                PFalg_proj_t *proj_exit = PFmalloc (ref->schema.count *
                                                    sizeof (PFalg_proj_t));

                /* prune the columns of the right argument 
                   of the Cartesian product */
                count = 0;
                for (i = 0; i < L(p)->sem.proj.count; i++) {
                    for (j = 0; j < rcross->schema.count; j++) 
                        if (L(p)->sem.proj.items[i].new == 
                            rcross->schema.items[j].name)
                            break;
                    if (j == rcross->schema.count)
                        proj_proxy[count++] = L(p)->sem.proj.items[i];
                }
                
                /* replace the columns of the right argument
                   of the Cartesian product by a dummy column
                   of the left argument */
                for (i = 0; i < L(LL(p))->sem.proj.count; i++) {
                    for (j = 0; j < rcross->schema.count; j++) 
                        if (L(LL(p))->sem.proj.items[i].old == 
                            rcross->schema.items[j].name)
                            break;
                    if (j == rcross->schema.count)
                        proj_left[i] = L(LL(p))->sem.proj.items[i];
                    else
                        proj_left[i] = PFalg_proj (
                                           L(LL(p))->sem.proj.items[i].new,
                                           dummy_col);
                }
                /* replace the columns of the right argument
                   of the Cartesian product by a dummy column
                   of the left argument */
                for (i = 0; i < ref->sem.proj.count; i++) {
                    for (j = 0; j < rcross->schema.count; j++) 
                        if (ref->sem.proj.items[i].old == 
                            rcross->schema.items[j].name)
                            break;
                    if (j == rcross->schema.count)
                        proj_exit[i] = ref->sem.proj.items[i];
                    else
                        proj_exit[i] = PFalg_proj (
                                           ref->sem.proj.items[i].new,
                                           dummy_col);
                }

                PFla_op_t *new_number = PFla_number (
                                            PFla_proxy_base (lcross),
                                            L(ref)->sem.number.res);

                *ref = *PFla_project_ (new_number, 
                                       ref->schema.count,
                                       proj_exit);

                *p = *(cross_can (
                          rcross,
                          PFla_proxy (
                              PFla_project_ (
                                  PFla_eqjoin (
                                      PFla_project_ (
                                          new_number,
                                          L(LL(p))->schema.count,
                                          proj_left),
                                      R(LL(p)),
                                      LL(p)->sem.eqjoin.att1,
                                      LL(p)->sem.eqjoin.att2),
                                  count, proj_proxy),
                              1,
                              ref,
                              L(new_number),
                              p->sem.proxy.new_cols,
                              p->sem.proxy.req_cols)));

                modified = true;
                break;
            }
        }
        break;

    case la_proxy_base:
    case la_string_join:
    case la_dummy:
        break;
    }

    /* update cross_changes counter if
       cross product couldn't be propagated up the tree */
    if (cross_cross && !is_cross (p)) {
        cross_changes++;
        modified = true;
    } else if (cross_cross)
        cross_changes = 0;
        
    /* discard the subtree with directly following cross products */
    if (cross_changes >= max_cross_changes)
        modified = false;

    return modified;
}

/**
 * clean_up_cross replaces all clone column aware cross
 * product operators by normal cross products
 * and introduces additional project operators
 * whenever there are duplicate columns.
 */
static void
clean_up_cross (PFla_op_t *p)
{
    unsigned int i;

    /* rewrite each node only once */
    if (SEEN(p))
        return;
    else
        SEEN(p) = true;

    /* apply complex optimization for children */
    for (i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        clean_up_cross (p->child[i]);

    if (p->kind == la_cross_mvd) {
        PFalg_proj_t *proj_list;
        unsigned int j;
        unsigned int count = 0;
        unsigned int dup_count = 0;
        
        /* create projection list */
        proj_list = PFmalloc ((L(p)->schema.count)
                              * sizeof (*(proj_list)));
        
        /* create projection list */
        for (i = 0; i < L(p)->schema.count; i++) {
            for (j = 0; j < R(p)->schema.count; j++)
                if (L(p)->schema.items[i].name == R(p)->schema.items[j].name) {
                    dup_count++;
                    break;
                }
                    
            if (j == R(p)->schema.count)
                proj_list[count++] = proj (L(p)->schema.items[i].name,
                                           L(p)->schema.items[i].name);
        }

        /* ensure that we do not generate empty projection lists */
        if (!count) {
            /* we can throw away the cross product if no column 
               is required and the cross product does not change
               the cardinality */
            if (PFprop_card (L(p)->prop) == 1)
                *p = *dummy (R(p));
            else {
                PFalg_proj_t *proj_list2;
                proj_list2 = PFmalloc ((R(p)->schema.count - 1)
                                       * sizeof (*(proj_list)));
                count = 0;
                /* split up the columns such that they do not conflict
                   anymore */
                assert (R(p)->schema.count > 1);
                /* throw out the first column of the left child
                   from the right child */
                for (j = 0; j < R(p)->schema.count; j++)
                    if (L(p)->schema.items[0].name !=
                        R(p)->schema.items[j].name) {
                        proj_list2[count++] = 
                            proj (R(p)->schema.items[j].name,
                                  R(p)->schema.items[j].name);
                    }
                /* keep only the first column of the left child */
                proj_list[0] = proj (L(p)->schema.items[0].name,
                                     L(p)->schema.items[0].name);

                /* apply project operator on both childs */
                *p = *(cross (PFla_project_ (L(p), 1, proj_list),
                              PFla_project_ (R(p), count, proj_list2)));
            }
        }
        else {
            if (dup_count)
                *p = *(cross (PFla_project_ (L(p), count, proj_list), R(p)));
            else
                *p = *(cross (L(p), R(p)));
        }
    }
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_mvd (PFla_op_t *root, unsigned int noneffective_tries)
{
    /* remove all common subexpressions to
       detect more patterns */ 
    PFla_cse (root);

    /* Keep track of the ineffective 
       cross product - cross product 
       rewrites and stop if 
       max_cross_changes is reached. */
    cross_changes = 0;
    max_cross_changes = noneffective_tries;

    /* Optimize algebra tree */
    while (opt_mvd (root))
        PFla_dag_reset (root);

    PFla_dag_reset (root);

    /* Infer cardinality property first */
    PFprop_infer_card (root);

    /* replace clone column aware cross
       products by normal ones */
    clean_up_cross (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
