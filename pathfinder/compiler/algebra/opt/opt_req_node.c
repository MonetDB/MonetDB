/**
 * @file
 *
 * Optimize relational algebra expression DAG
 * based on the required node properties.
 * (This requires no burg pattern matching as we
 *  apply optimizations in a peep-hole style on
 *  single nodes only.)
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
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

/* Easily access subtree-parts */
#include "child_mnemonic.h"

/** mnemonic algebra constructors */
#include "logical_mnemonic.h"

/**
 * @brief Check if all input (item) values of a twig constructor are constant.
 */
static bool
twig_constant (PFla_op_t *p)
{
    switch (p->kind) {
        case la_fcns:
            return twig_constant (L(p)) && twig_constant (R(p));
        case la_docnode:
            return twig_constant (R(p));
        case la_element:
            return PFprop_const_left (p->prop, p->sem.iter_item.item) &&
                   twig_constant (R(p));
        case la_textnode:
        case la_comment:
            return PFprop_const_left (p->prop, p->sem.iter_item.item);
        case la_attribute:
        case la_processi:
            return PFprop_const_left (p->prop, p->sem.iter_item1_item2.item1) &&
                   PFprop_const_left (p->prop, p->sem.iter_item1_item2.item2);
        case la_content:
            return false;
        case la_nil:
            return true;
        default:
            PFoops (OOPS_FATAL, "traversal reached non-constructor node");
    }
    return false;
}

/**
 * @brief Check if the loop relation (here the iter columns
 *        of the twig constructor) is constant.
 */
static bool
loop_constant (PFla_op_t *p)
{
    switch (p->kind) {
        case la_docnode:
            return PFprop_const_left (p->prop, p->sem.docnode.iter);
        case la_element:
        case la_textnode:
        case la_comment:
            return PFprop_const_left (p->prop, p->sem.iter_item.iter);
        case la_attribute:
        case la_processi:
            return PFprop_const_left (p->prop, p->sem.iter_item1_item2.iter);
        case la_content:
            return PFprop_const_right (p->prop, p->sem.iter_pos_item.iter);
        default:
            PFoops (OOPS_FATAL, "traversal reached non-constructor node");
    }
    return true;
}

/**
 * @brief Look up the original loop relation and build a new one
 *        (with output column @a iter).
 */
static PFla_op_t *
get_loop_relation (PFla_op_t *p, PFalg_att_t iter)
{
    switch (p->kind) {
        case la_docnode:
            return project (L(p), proj (iter, p->sem.docnode.iter));
        case la_element:
        case la_textnode:
        case la_comment:
            return project (L(p), proj (iter, p->sem.iter_item.iter));
        case la_attribute:
        case la_processi:
            return project (L(p), proj (iter, p->sem.iter_item1_item2.iter));
        case la_content:
            return project (R(p), proj (iter, p->sem.iter_pos_item.iter));
        default:
            PFoops (OOPS_FATAL, "traversal reached non-constructor node");
    }
    return NULL;
}

/**
 * @brief Copy a constant fragment using a different loop relation (@a loop).
 */
static PFla_op_t *
build_constant_frag (PFla_op_t *p, PFla_op_t *loop)
{
    switch (p->kind) {
        case la_fcns:
            return fcns (build_constant_frag (L(p), loop),
                         build_constant_frag (R(p), loop));
        case la_docnode:
            return docnode (loop,
                            build_constant_frag (R(p), loop), att_iter);
        case la_element:
            return element (attach (loop,
                                    att_item,
                                    PFprop_const_val_left (
                                        p->prop,
                                        p->sem.iter_item.item)),
                            build_constant_frag (R(p), loop),
                            att_iter, att_item);
        case la_textnode:
            return textnode (attach (loop,
                                     att_item,
                                     PFprop_const_val_left (
                                         p->prop,
                                         p->sem.iter_item.item)),
                             att_iter, att_item);
        case la_comment:
            return comment (attach (loop,
                                    att_item,
                                    PFprop_const_val_left (
                                        p->prop,
                                        p->sem.iter_item.item)),
                            att_iter, att_item);
        case la_attribute:
            return attribute (attach (
                                  attach (loop,
                                          att_item,
                                          PFprop_const_val_left (
                                              p->prop,
                                              p->sem.iter_item1_item2.item1)),
                                  att_item1,
                                  PFprop_const_val_left (
                                      p->prop,
                                      p->sem.iter_item1_item2.item2)),
                              att_iter, att_item, att_item1);
        case la_processi:
            return processi (attach (
                                 attach (loop,
                                         att_item,
                                         PFprop_const_val_left (
                                             p->prop,
                                             p->sem.iter_item1_item2.item1)),
                                 att_item1,
                                 PFprop_const_val_left (
                                     p->prop,
                                     p->sem.iter_item1_item2.item2)),
                             att_iter, att_item, att_item1);
        case la_content:
            PFoops (OOPS_FATAL, "constructor not constant");
        case la_nil:
            return nil ();
        default:
            PFoops (OOPS_FATAL, "traversal reached non-constructor node");
    }
    return NULL;
}

/* worker for PFalgopt_req_node */
static void
opt_req_node (PFla_op_t *p)
{
    assert (p);

    /* nothing to do if we already visited that node */
    if (p->bit_dag)
        return;

    /* If a merge-adjacent-text-node node is not queried we don't mind
       if more textnodes then required exist and throw the operator away. */
    if (p->kind == la_roots &&
        L(p)->kind == la_merge_adjacent &&
        PFprop_node_property (L(p)->prop,
                              L(p)->sem.merge_adjacent.item_res) &&
        !PFprop_node_content_queried (L(p)->prop,
                                      L(p)->sem.merge_adjacent.item_res)
        /* we don't check for self steps or constructors as this kind
           of operator is only introduced directly underneath node
           constructors */) {
        *p = *project (LR(p),
                       proj (L(p)->sem.merge_adjacent.iter_res,
                             L(p)->sem.merge_adjacent.iter_in),
                       proj (L(p)->sem.merge_adjacent.pos_res,
                             L(p)->sem.merge_adjacent.pos_in),
                       proj (L(p)->sem.merge_adjacent.item_res,
                             L(p)->sem.merge_adjacent.item_in));
    }
    /* We also have to get rid of the fragment information
       -- as otherwise the SQL translation might stumble. */
    else if (p->kind == la_frag_union &&
             R(p)->kind == la_fragment &&
             RL(p)->kind == la_merge_adjacent &&
             PFprop_node_property (RL(p)->prop,
                                   RL(p)->sem.merge_adjacent.item_res) &&
             !PFprop_node_content_queried (RL(p)->prop,
                                           RL(p)->sem.merge_adjacent.item_res))
        *p = *L(p);

    /* Make sure that the above two patterns consistently remove the
       merge-adjacent-text-node operator. (Due to the top-down rewriting
       this check is never called before the previous ones.) */
    else if (p->kind == la_merge_adjacent &&
             PFprop_node_property (p->prop,
                                   p->sem.merge_adjacent.item_res) &&
             !PFprop_node_content_queried (p->prop,
                                           p->sem.merge_adjacent.item_res))
        PFoops (OOPS_FATAL, "rewrite for merge-adjacent-text-node operator"
                            " is missing");

    /* detect constant twigs whose document order and id are not needed
       and make them independent from the loop relation (if they are not
       already in the outer-most scope */
    else if (p->kind == la_roots &&
             L(p)->kind == la_twig &&
             PFprop_node_property (L(p)->prop,
                                   L(p)->sem.iter_item.item) &&
             !PFprop_node_id_required (L(p)->prop,
                                       L(p)->sem.iter_item.item) &&
             !PFprop_node_order_required (L(p)->prop,
                                          L(p)->sem.iter_item.item) &&
             !loop_constant (LL(p)) &&
             twig_constant (LL(p))) {
        /* Save the old item column name as well as the old loop relation
           before overwriting the references. */
        PFalg_att_t item = L(p)->sem.iter_item.item;
        PFla_op_t  *loop = get_loop_relation (LL(p), L(p)->sem.iter_item.iter);

        /* relink the fragment operator */
        *L(p) = *twig (build_constant_frag (
                           LL(p),
                           lit_tbl (attlist (att_iter), tuple (lit_nat (1)))),
                       att_iter, att_item);

        *p    = *cross (project (roots (L(p)), proj (item, att_item)),
                        loop);
    }

    /* infer properties for children */
    for (unsigned int i = 0; i < PFLA_OP_MAXCHILD && p->child[i]; i++)
        opt_req_node (p->child[i]);

    /* mark node as visited */
    p->bit_dag = true;
}

/**
 * Invoke algebra optimization.
 */
PFla_op_t *
PFalgopt_req_node (PFla_op_t *root)
{
    /* Infer req node properties first */
    PFprop_infer_req_node (root);
    /* infer constants to detect constant fragments */
    PFprop_infer_const (root);

    /* Optimize algebra tree */
    opt_req_node (root);
    PFla_dag_reset (root);

    return root;
}

/* vim:set shiftwidth=4 expandtab filetype=c: */
