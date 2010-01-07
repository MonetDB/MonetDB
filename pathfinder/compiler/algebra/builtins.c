/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Creation of algebra translations for XQuery built-in functions.
 * The representation of such functions was extended by a function
 * pointer that references the functions defined in this file.
 *
 *  - The order of different functions reflects the order
 *    in the XQuery 1.0 and XPath 2.0 Functions and Operators
 *    recommendation (see http://www.w3.org/TR/xpath-functions/).
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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

/* always include "pathfinder.h", first! */
#include "pf_config.h"
#include "pathfinder.h"
#include "oops.h"

#include <assert.h>
#include <string.h>
#include <stdio.h>

#include "mem.h"
#include "builtins.h"

#include "logical.h"
#include "logical_mnemonic.h"

/**
 * Assign correct row numbers in case we need the real values.
 */
static PFla_op_t *
adjust_positions (const PFla_op_t *n)
{
    return project (rownum (n, col_pos1, sortby (col_pos), col_iter),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos1),
                    proj (col_item, col_item));
}

/* ----------------------------------------- */
/* Helper functions to build a decision tree */
/* ----------------------------------------- */

/**
 * Returns a iter/pos/items schema where the items have the type
 * @a ty
 */
static PFla_op_t *
sel_type (const PFla_op_t *n, PFalg_simple_type_t ty)
{
    return project (
               type_assert_pos (
                    select_ (
                           type (n,
                                 col_res,
                                 col_item, ty),
                           col_res),
                    col_item, ty),
               proj (col_iter, col_iter),
               proj (col_pos, col_pos),
               proj (col_item, col_item));
}

/**
 * Constructs a typeswitch subtree based on the
 * algebra type of the item column.
 *
 * @param n The relation used as input for the typeswitch.
 * @param count The number of types in the types array
 * @param types The types to distinguish
 * @param cnst  A callback function, called for each type encounterd
 *              in the relation
 * @param params Optional parameters, passed to the callback function.
 */
static PFla_op_t *
typeswitch (PFla_op_t *n,
            unsigned int count,
            const PFalg_simple_type_t *types,
            PFla_op_t *(*cnst) (PFla_op_t *n, PFalg_simple_type_t, void *),
            void *params)
{
    PFla_op_t *res = NULL;
    PFalg_simple_type_t item_types = 0;
    bool found = false;

    for (unsigned int i = 0; i < n->schema.count; i++) {
        if (n->schema.items[i].name == col_item) {
            found = true;
            item_types = n->schema.items[i].type;
            break;
        }
    }
    if (!found)
        PFoops (OOPS_FATAL,
                "column `%s' referenced in typeswitch not found",
                PFcol_str (col_item));

    /* Iterate over the list of types and fire the callback function
       on the tuples that match that type (selection is done with
       sel_type ()).  */
    for (unsigned int i = 0; i < count; i++) {
        PFalg_simple_type_t ty = item_types & types[i];
        if (ty == item_types) {
            res = cnst (n, types[i], params);
            break;
        }
        else if (ty != 0) {
            PFla_op_t *tsw = cnst (sel_type (n, ty), types[i], params);
            if (res != NULL)
                /* collect all the resulting tuples */
                res = disjunion (res, tsw);
            else
                res = tsw;
        }
    }
    return res;
}

struct typeswitch2_params {
    PFla_op_t *n;
    PFalg_simple_type_t t;
    int count;
    const PFalg_simple_type_t *types;
    void *params;
    PFla_op_t *(*cnst) (PFla_op_t *n1, PFla_op_t *n2,
                        PFalg_simple_type_t, PFalg_simple_type_t, void *);
};

static PFla_op_t *
typeswitch2_callback_snd_arg (PFla_op_t *n2,
                              PFalg_simple_type_t t2, void *params) {
    struct typeswitch2_params *p = (struct typeswitch2_params *) params;

    return p->cnst (p->n,
                    n2,
                    p->t,
                    t2,
                    p->params);
}

static PFla_op_t *
typeswitch2_callback_fst_arg (PFla_op_t *n1,
                              PFalg_simple_type_t t1, void *params) {
    struct typeswitch2_params *p = (struct typeswitch2_params *) params;

    return typeswitch (p->n,
                       p->count,
                       p->types,
                       typeswitch2_callback_snd_arg,
                       (struct typeswitch2_params[])
                         {{ .n = n1,
                            .t = t1,
                            .params = p->params,
                            .cnst = p->cnst }});
}

/**
 * Constructs a typeswitch subtree based on the
 * algebra type of the item column.
 *
 * First a typeswitch for each input type of the first relation is
 * introduced. The callback function of the typeswitch function
 * then again creates a typeswitch tree for each type in the second
 * relation.
 *
 * Variant with two input relations.
 * @param n1 The first relation used as input for the typeswitch.
 * @param n2 The second relation used as input for the typeswitch.
 * @param count The number of types in the types array
 * @param types The types to distinguish
 * @param cnst  A callback function, called for each type combination
 *              encounterd in the relations
 * @param params Optional parameters, passed to the callback function.
 */
static PFla_op_t *
typeswitch2 (PFla_op_t *n1,
             PFla_op_t *n2,
             unsigned int count,
             const PFalg_simple_type_t *types,
             PFla_op_t *(*cnst) (PFla_op_t *n1,
                                 PFla_op_t *n2,
                                 PFalg_simple_type_t,
                                 PFalg_simple_type_t,
                                 void *),
             void *params)
{
    return typeswitch (n1,
                       count,
                       types,
                       typeswitch2_callback_fst_arg,
                       (struct typeswitch2_params[])
                         {{ .n = n2,
                            .count = count,
                            .types = types,
                            .params = params,
                            .cnst = cnst }});
}

/* ---------------------------------------------- */
/* Helper Function to Translate a Binary Function */
/* ---------------------------------------------- */

/**
 * Worker function to construct algebra implementation of binary
 * functions (e.g., arithmetic functions: op:numeric-add(),
 * op:numeric-subtract(); comparison functions: op:eq(); boolean
 * functions: op:and();...).
 *
 * env,loop,delta: e1 => q1,delta1   env,loop,delta1: e2 => q2,delta2
 * ------------------------------------------------------------------
 *                 env,loop,delta: (e1 + e2) =>
 * (proj_iter,pos,item:res(
 *   OP_res<item,item1>(
 *     cast_item,t (q1)
 *     |X| (iter,iter1)
 *     (proj_iter1:iter,item1:item (cast_item,t (q2)))))
 *  ,
 *  delta2
 * )
 *
 * (where OP is the algebra operator that implements the function,
 * and t is the implementation type that is used)
 *
 * @param t    Algebra type that should be used for this operator.
 *             All arguments will be cast to that type before invoking
 *             the actual algebra operator. This makes these
 *             operations always monomorphic (and thus reasonably
 *             efficient).
 * @param OP   Algebra tree node constructor for the algebra operator
 *             that shall be used for this operation. (Pick any of the
 *             construction functions in algebra.c.)
 * @param n1   The first input relation q1
 * @param n2   The second input relation q1
 * @return     The logical algebra node that represents the result of
 *             the binary operator.
 */
static PFla_op_t *
bin_op (PFalg_simple_type_t t,
        PFla_op_t *(*OP) (const PFla_op_t *, PFalg_col_t,
                          PFalg_col_t, PFalg_col_t),
        const PFla_op_t *n1,
        const PFla_op_t *n2)
{
    return project (OP (eqjoin (
                            project (cast (n1,
                                           col_cast, col_item, t),
                                     proj (col_iter, col_iter),
                                     proj (col_pos, col_pos),
                                     proj (col_item, col_cast)),
                            project (cast (n2,
                                           col_cast, col_item, t),
                                     proj (col_iter1, col_iter),
                                     proj (col_item1, col_cast)),
                            col_iter,
                            col_iter1),
                        col_res, col_item, col_item1),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos),
                    proj (col_item, col_res));
}

/* ---------------------------------------------- */
/* Helper Function to Translate an Unary Function */
/* ---------------------------------------------- */

/**
 * Worker function to construct algebra implementation of unary
 * functions (e.g., fn:not()).
 *
 * env,loop,delta: e1 => q1,delta1
 * ------------------------------------------------------------------
 *                 env,loop,delta: e1 =>
 * (proj_iter,pos,item:res(OP_res<item>(cast_item,t (q1)))
 *  ,
 *  delta2
 * )
 *
 * (where OP is the algebra operator that implements the function,
 * and t is the implementation type that is used)
 *
 * @param t    Algebra type that should be used for this operator.
 *             All arguments will be cast to that type before invoking
 *             the actual algebra operator. This makes these
 *             operations always monomorphic (and thus reasonably
 *             efficient).
 * @param OP   Algebra tree node constructor for the algebra operator
 *             that shall be used for this operation. (Pick any of the
 *             construction functions in algebra.c.)
 * @param arg  The input <frag,relation> pair
 * @return     The logical algebra node that represents the result of
 *             the unary operator.
 */
static struct PFla_pair_t
un_op (PFalg_simple_type_t t,
       PFla_op_t *(*OP) (const PFla_op_t *,
                         PFalg_col_t,
                         PFalg_col_t),
       struct PFla_pair_t arg)
{
    return (struct PFla_pair_t) {
        .rel = project (OP (project (
                                cast (arg.rel,
                                      col_cast,
                                      col_item,
                                      t),
                                proj (col_iter, col_iter),
                                proj (col_pos, col_pos),
                                proj (col_item, col_cast)),
                            col_res,
                            col_item),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/* ----------------------------------------------- */
/* Helper Function to Translate Simple Aggregates  */
/* ----------------------------------------------- */

/**
 * Construct a simple aggregate.
 */
static PFla_op_t *
simple_aggr (const PFla_op_t *n,
             PFalg_col_t part,
             PFalg_aggr_kind_t kind,
             PFalg_col_t res,
             PFalg_col_t col)
{
    PFalg_aggr_t aggr = PFalg_aggr (kind, res, col);
    return PFla_aggr (n, part, 1, &aggr);
}

/* ------------ */
/* 2. ACCESSORS */
/* ------------ */

/* ----------------- */
/* 2.1. fn:node-name */
/* ----------------- */

/* The fn:node-name function returns an expanded for 
 * node kinds that can have names.
 * Among them obviously elements and attributes. 
 */

static struct PFla_pair_t
fn_bui_node_name (struct PFla_pair_t
                    (*nodes)
                    (const PFla_op_t * loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     PFla_pair_t *args),
                  const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    PFla_op_t *qnames = project (
                            doc_access (
                                PFla_set_to_la (args[0].frag),
                                nodes (loop, ordering, side_effects, args).rel,
                                col_res, col_item, doc_qname),
                            proj (col_iter, col_iter),
                            proj (col_pos, col_pos),
                            proj (col_item, col_res));

    return (struct PFla_pair_t) {
                    .rel = qnames,
                    .frag = PFla_empty_set ()
                };
}

static struct PFla_pair_t
fn_bui_node_name_attr_ (const PFla_op_t* loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;
    return args[0];
}

static struct PFla_pair_t
fn_bui_node_name_elem_ (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;
    return args[0];
}

/* filter attributes only */
static struct PFla_pair_t
fn_bui_node_name_attr_filter (const PFla_op_t* loop,
                              bool ordering,
                              PFla_op_t **side_effects,
                              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;
    
    PFla_op_t *attributes = NULL;
    PFalg_step_spec_t self_attr_spec;
    self_attr_spec.axis = alg_self;
    self_attr_spec.kind = node_kind_attr;
    
    /* just find every attribute  */
    self_attr_spec.qname = PFqname (PFns_wild, NULL); 
    
    attributes = attach (
                     project (
                         PFla_step_join_simple (
                             PFla_set_to_la (args[0].frag),
                             project (args[0].rel,
                                      proj (col_iter, col_iter),
                                      proj (col_item, col_item)),
                             self_attr_spec,
                             col_item, col_res),
                         proj (col_iter, col_iter),
                         proj (col_item, col_res)),
                     col_pos, lit_int(1));

    return (struct PFla_pair_t) {
                    .rel = attributes,
                    .frag = args[0].frag
                };
}

/* filter elements only */
static struct PFla_pair_t
fn_bui_node_name_element_filter (const PFla_op_t* loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;
    
    PFla_op_t *elements = NULL;
    PFalg_step_spec_t self_elem_spec;
    self_elem_spec.axis = alg_self;
    self_elem_spec.kind = node_kind_elem; 
    
    /* just find every element */
    self_elem_spec.qname = PFqname (PFns_wild, NULL);
    
    elements = attach (
                   project (
                       PFla_step_join_simple (
                           PFla_set_to_la (args[0].frag),
                           project (args[0].rel,
                                    proj (col_iter, col_iter),
                                    proj (col_item, col_item)),
                           self_elem_spec,
                           col_item, col_res),
                       proj (col_iter, col_iter),
                       proj (col_item, col_res)),
                   col_pos, lit_int(1));
                                
    return (struct PFla_pair_t) {
                    .rel = elements,
                    .frag = args[0].frag
                };
}

static struct PFla_pair_t
fn_bui_node_name_node_ (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    PFla_op_t *union_ = disjunion (
                            fn_bui_node_name_attr_filter (loop,
                                                          ordering,
                                                          side_effects,
                                                          args).rel,
                            fn_bui_node_name_element_filter (loop,
                                                             ordering,
                                                             side_effects,
                                                             args).rel);
                                                    
    return (struct PFla_pair_t) {
                .rel  = union_,
                .frag = args[0].frag
            };
}

/* node-name for attributes */
struct PFla_pair_t
PFfn_bui_node_name_attr (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    return fn_bui_node_name (fn_bui_node_name_attr_,
                             loop, ordering, side_effects, args);
}

/* node-name for elements */
struct PFla_pair_t 
PFfn_bui_node_name_elem (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    return fn_bui_node_name (fn_bui_node_name_elem_,
                             loop, ordering, side_effects, args);
}

/* node-name for general nodes */
struct PFla_pair_t
PFfn_bui_node_name_node (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    return fn_bui_node_name (fn_bui_node_name_node_,
                             loop, ordering, side_effects, args);
}



/* -------------- */
/* 2.3. fn:string */
/* -------------- */

/**
 * The fn:string function casts all values to string
 * It uses fn:data() for atomizing nodes.
 */
static struct PFla_pair_t
fn_string (struct PFla_pair_t (*data)
             (const PFla_op_t * loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args),
           const PFla_op_t *loop,
           bool ordering,
           PFla_op_t **side_effects,
           struct PFla_pair_t *args)
{
    /* as long as fn:data uses #pf:string-value()
       we can use it safely to convert the nodes */
    PFla_op_t *strings = project (
                             cast (
                                 data (
                                     loop,
                                     ordering,
                                     side_effects,
                                     args).rel,
                                 col_cast,
                                 col_item,
                                 aat_str),
                             proj (col_iter, col_iter),
                             proj (col_pos, col_pos),
                             proj (col_item, col_cast));

    /* add the empty strings for the missing iterations */
    PFla_op_t *res = disjunion (
                         strings,
                         attach (
                             attach (
                                 difference (
                                     loop,
                                     project (
                                         strings,
                                         proj (col_iter, col_iter))),
                                 col_pos, lit_nat (1)),
                             col_item, lit_str ("")));

    return (struct PFla_pair_t) { .rel  = res, .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_attr (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_attr, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_text (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_text, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_pi (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_pi, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_comm (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_comm, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_elem (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_elem, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_elem_attr (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_elem_attr,
                      loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data, loop, ordering, side_effects, args);
}

/* ------------ */
/* 2.4. fn:data */
/* ------------ */

/**
 * Build operator tree for built-in function 'fn:data'.
 * It uses #pf:string-value() for atomizing nodes.
 */
static struct PFla_pair_t
fn_data (struct PFla_pair_t (*str_val)
             (const PFla_op_t *loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args),
         PFalg_simple_type_t node_type,
         const PFla_op_t *loop,
         bool ordering,
         PFla_op_t **side_effects,
         struct PFla_pair_t *args)
{
    (void) loop; (void) side_effects;

    /*
     * carry out specific type test on type
     */
    PFla_op_t *type = type (args[0].rel, col_res, col_item, node_type);

    /* select those rows that have type "node" */
    PFla_op_t *nodes = project (
                                type_assert_pos (
                                                 select_ (type, col_res),
                                                 col_item, node_type),
                                proj (col_iter, col_iter),
                                proj (col_pos, col_pos),
                                proj (col_item, col_item));

    /* select the remaining rows */
    PFla_op_t *atomics = project (type_assert_neg (select_ (not (type,
                                                                 col_res1,
                                                                 col_res),
                                                            col_res1),
                                                   col_item, node_type),
                                  proj (col_iter, col_iter),
                                  proj (col_pos, col_pos),
                                  proj (col_item, col_item));

    /* renumber */
    PFla_op_t *q = rowid (nodes, col_inner);

    PFla_op_t *map = project (q,
                              proj (col_outer, col_iter),
                              proj (col_inner, col_inner),
                              proj (col_pos1, col_pos));

    struct  PFla_pair_t str_args = {
        .rel = attach (
                       project (
                                q,
                                proj (col_iter, col_inner),
                                proj (col_item, col_item)),
                       col_pos, lit_nat(1)),
        .frag = args[0].frag };

    PFla_op_t *res = project(
                         eqjoin(
                             cast(str_val (project (q, proj (col_iter,
                                                             col_inner)),
                                           ordering,
                                           side_effects,
                                           &str_args).rel,
                                  col_cast,
                                  col_item,
                                  aat_uA),
                             map,
                             col_iter,
                             col_inner),
                         proj(col_iter, col_outer),
                         proj(col_pos, col_pos1),
                         proj(col_item, col_cast));

    return (struct  PFla_pair_t) {
        .rel  = disjunion (atomics, res),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_attr (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value_attr, aat_anode,
                    loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_text (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value_text, aat_pnode,
                    loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_pi (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value_pi, aat_pnode,
                    loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_comm (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value_comm, aat_pnode,
                    loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_elem (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value_elem, aat_pnode,
                    loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_elem_attr (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_elem_attr,
                   aat_node, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args)
{
    return fn_data (PFbui_pf_string_value, aat_node,
                    loop, ordering, side_effects, args);
}

/* --------------------- */
/* 3. THE ERROR FUNCTION */
/* --------------------- */

/**
 * Build up operator tree for built-in function 'fn:error()'.
 */
struct PFla_pair_t
PFbui_fn_error_empty (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering; (void) args;

    *side_effects = error (
                        *side_effects,
                        attach (
                            loop,
                            col_item,
                            lit_str ("http://www.w3.org/2005/xqt-errors#FOER0000")),
                        col_item);

    return (struct PFla_pair_t) {
        .rel  = PFla_empty_tbl_ (ipi_schema(0)),
        .frag = PFla_empty_set ()};
}

/**
 * Build up operator tree for built-in function 'fn:error(string)'.
 */
struct PFla_pair_t
PFbui_fn_error (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    *side_effects = error (
                        *side_effects,
                        args[0].rel,
                        col_item);

    return (struct PFla_pair_t) {
        .rel  = PFla_empty_tbl_ (ipi_schema(0)),
        .frag = PFla_empty_set ()};
}

/**
 * Build up operator tree for built-in function 'fn:error(string?, string)'.
 */
struct PFla_pair_t
PFbui_fn_error_str (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering;

    *side_effects = error (
                        *side_effects,
                        project (
                            fun_1to1 (
                                eqjoin (
                                    disjunion (
                                        args[0].rel,
                                        attach (
                                            attach (
                                                difference (
                                                    loop,
                                                    project (
                                                        args[0].rel,
                                                        proj (col_iter,
                                                              col_iter))),
                                                col_pos, lit_nat (1)),
                                            col_item, lit_str (""))),
                                        project (args[1].rel,
                                                 proj (col_iter1, col_iter),
                                                 proj (col_item1, col_item)),
                                        col_iter,
                                        col_iter1),
                                alg_fun_fn_concat,
                                col_res,
                                collist(col_item, col_item1)),
                            proj (col_iter, col_iter),
                            proj (col_pos, col_pos),
                            proj (col_item, col_res)),
                        col_item);

    return (struct PFla_pair_t) {
        .rel  = PFla_empty_tbl_ (ipi_schema(0)),
        .frag = PFla_empty_set ()};
}

/* -------------------------------------- */
/* 6. FUNCTIONS AND OPERATORS ON NUMERICS */
/* -------------------------------------- */
/* -------------------------------- */
/* 6.2. Operators on Numeric Values */
/* -------------------------------- */

/**
 * Callback function for the typeswitch in bin_arith.
 */
static PFla_op_t *
bin_arith_callback (PFla_op_t *n1,
                    PFla_op_t *n2,
                    PFalg_simple_type_t t1,
                    PFalg_simple_type_t t2,
                    void *params)
{
    /* get the correct type */
    PFalg_simple_type_t t;
    if (t1 == t2)
        t = t1;
    else if (t1 == aat_dec || t2 == aat_dec)
        t = aat_dec;
    else if (t1 == aat_dbl || t2 == aat_dbl)
        t = aat_dbl;
    else {
        PFoops (OOPS_FATAL,
                "invalid type combination in binary arithmetic function");
        return NULL; /* never reached. */
    }

    return bin_op (t,
                   (PFla_op_t *(*) (const PFla_op_t *,
                                    PFalg_col_t,
                                    PFalg_col_t,
                                    PFalg_col_t)) params,
                   n1,
                   n2);
}

/**
 * Helper function for binary arithmetics with a typeswitch.
 * For every possible combination of input types, bin_op is
 * called with the appropriate parameters.
 * @see bin_op()
 */
static struct PFla_pair_t
bin_arith (PFla_op_t *(*OP) (const PFla_op_t *, PFalg_col_t,
                             PFalg_col_t, PFalg_col_t),
           const PFla_op_t *loop,
           bool ordering,
           PFla_op_t **side_effects,
           struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = typeswitch2 (args[0].rel,
                                  args[1].rel,
                                  3,
                                  (PFalg_simple_type_t [3])
                                      { aat_int, aat_dbl, aat_dec },
                                  bin_arith_callback,
                                  (void*) OP);
    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-add ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_add (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return bin_arith (PFla_add, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:numeric-subtract ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    return bin_arith (PFla_subtract, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:numeric-multiply ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    return bin_arith (PFla_multiply, loop, ordering, side_effects, args);
}

/**
 * Special typeswitch-callback function for divison.
 * If both types are integer, they are cast to decimal.
 */
static PFla_op_t *
divide_callback (PFla_op_t *n1,
                 PFla_op_t *n2,
                 PFalg_simple_type_t t1,
                 PFalg_simple_type_t t2,
                 void *params)
{
    (void) params;

    PFalg_simple_type_t t;
    if (t1 == t2 && t1 == aat_int)
        t = aat_dec;
    else if (t1 == aat_dec || t2 == aat_dec)
        t = aat_dec;
    else if (t1 == aat_dbl || t2 == aat_dbl)
        t = aat_dbl;
    else {
        PFoops (OOPS_FATAL,
                "invalid type combination in binary arithmetic function");
        return NULL; /* never reached. */
    }
    return bin_op (t,
                   PFla_divide,
                   n1,
                   n2);
}

/**
 * Algebra implementation for op:numeric-divide ()
 * @see bin_arith()
 *
 * NB: According to the XQuery specifications, the division of two
 * integers returns a decimal number, i.e. we let the two operands be
 * promoted to decimal (see special helper function above).
 */
struct PFla_pair_t
PFbui_op_numeric_divide (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = typeswitch2 (args[0].rel,
                                  args[1].rel,
                                  3,
                                  (PFalg_simple_type_t [3])
                                    { aat_int, aat_dbl, aat_dec, },
                                  divide_callback,
                                  NULL);
    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-integer-divide ()
 * @see bin_arith()
 *
 * NB: ($a idiv $b) <=> ($a div $b) cast as xs:integer
 */
struct PFla_pair_t
PFbui_op_numeric_idivide (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    return (struct PFla_pair_t) {
        .rel = project (
                   cast (bin_arith (
                             PFla_divide,
                             loop,
                             ordering,
                             side_effects,
                             args).rel,
                         col_cast, col_item, aat_int),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_cast)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-mod ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    return bin_arith (PFla_modulo, loop, ordering, side_effects, args);
}

/* ------------------------------------------- */
/* 6.3. Comparison Operators on Numeric Values */
/* ------------------------------------------- */

/**
 * Helper function for binary comparison operators.
 * @see bin_op()
 */
static struct PFla_pair_t
bin_comp (PFalg_simple_type_t t,
          PFla_op_t *(*OP) (const PFla_op_t *, PFalg_col_t,
                            PFalg_col_t, PFalg_col_t),
          const PFla_op_t *loop,
          bool ordering,
          PFla_op_t **side_effects,
          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = bin_op (t,
                             OP,
                             args[0].rel,
                             args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:eq(integer?,integer?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_eq, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:eq(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_eq, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:eq(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_eq, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:eq(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_eq, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:eq(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_eq, loop, ordering, side_effects, args);
}


/**
 * Algebra implementation for op:ne(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_int, PFla_eq,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:ne(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec, PFla_eq,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:ne(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl, PFla_eq,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:ne(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln, PFla_eq,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:ne(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln,  PFla_not,
                  bin_comp (aat_str, PFla_eq,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:lt(integer?,integer?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_gt,
                     loop,
                     ordering,
                     side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_gt,
                     loop,
                     ordering,
                     side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_gt,
                     loop,
                     ordering,
                     side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_gt,
                     loop,
                     ordering,
                     side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_gt,
                     loop,
                     ordering,
                     side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:le(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_int, PFla_gt,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:le(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec, PFla_gt,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:le(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl, PFla_gt,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:le(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln, PFla_gt,
                            loop, ordering, side_effects, args));
}

/**
 * Algebra implementation for op:le(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_str, PFla_gt,
                            loop, ordering, side_effects, args));
}
/**
 * Algebra implementation for op:gt(integer?,integer?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_gt, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:gt(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_gt, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:gt(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_gt, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:gt(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_gt, loop, ordering, side_effects, args);
}

/**
 * Algebra implementation for op:gt(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_gt, loop, ordering, side_effects, args);
}


/**
 * Algebra implementation for op:ge(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_int (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_int, PFla_gt,
                            loop,
                            ordering,
                            side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));
}

/**
 * Algebra implementation for op:ge(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_dec (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec, PFla_gt,
                            loop,
                            ordering,
                            side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));
}

/**
 * Algebra implementation for op:ge(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_dbl (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl, PFla_gt,
                            loop,
                            ordering,
                            side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));
}

/**
 * Algebra implementation for op:ge(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln, PFla_gt,
                            loop,
                            ordering,
                            side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));
}

/**
 * Algebra implementation for op:ge(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_str (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_str, PFla_gt,
                            loop,
                            ordering,
                            side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));
}

/* -------------------------------- */
/* 6.4. Functions on Numeric Values */
/* -------------------------------- */

static PFla_pair_t
numeric_fun_op (PFalg_simple_type_t t,
                PFalg_fun_t kind,
                const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            cast (args[0].rel,
                                  col_cast,
                                  col_item,
                                  t),
                            kind,
                            col_res,
                            collist (col_cast)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

struct PFla_pair_t
PFbui_fn_abs_int (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_abs,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_abs_dec (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_abs,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_abs_dbl (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_abs,
                           loop, ordering, side_effects, args);
}


struct PFla_pair_t
PFbui_fn_ceiling_int (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_ceiling,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_ceiling_dec (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_ceiling,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_ceiling_dbl (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_ceiling,
                           loop, ordering, side_effects, args);
}


struct PFla_pair_t
PFbui_fn_floor_int (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_floor,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_floor_dec (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_floor,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_floor_dbl (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_floor,
                           loop, ordering, side_effects, args);
}


struct PFla_pair_t
PFbui_fn_round_int (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_round,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_round_dec (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_round,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_fn_round_dbl (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_round,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_sqrt_int (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_pf_sqrt,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_sqrt_dec (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_pf_sqrt,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_sqrt_dbl (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_pf_sqrt,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_log_int (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_pf_log,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_log_dec (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_pf_log,
                           loop, ordering, side_effects, args);
}

struct PFla_pair_t
PFbui_pf_log_dbl (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_pf_log,
                           loop, ordering, side_effects, args);
}

/* ----------------------- */
/* 7. FUNCTIONS ON STRINGS */
/* ----------------------- */
/* ------------------------------- */
/* 7.4. Functions on String Values */
/* ------------------------------- */

/**
 * Algebra implementation for
 * <code>fn:concat (anyAtomicType?, anyAtomicType?, ...)</code>
 * The fn:concat function is wrapped in the generic function operator
 */
struct PFla_pair_t
PFbui_fn_concat (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_fn_concat,
                       col_res,
                       collist(col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * The fn:string_join function is also available as primitive
 */
struct PFla_pair_t
PFbui_fn_string_join (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = attach (
                    fn_string_join (args[0].rel,
                                    project (
                                        args[1].rel,
                                        proj (col_iter, col_iter),
                                        proj (col_item, col_item)),
                                    col_iter, col_pos, col_item,
                                    col_iter, col_item,
                                    col_iter, col_item),
                    col_pos, lit_nat (1)),
        .frag = args[0].frag };
}

/**
 * Algebra implementation for <code>fn:substring (xs:string?, xs:double)</code>
 */
struct PFla_pair_t
PFbui_fn_substring (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (cast (args[1].rel,
                                          col_cast,
                                          col_item,
                                          aat_dbl),
                                    proj (col_iter1, col_iter),
                                    proj (col_item1, col_cast)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_substring,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:substring(xs:string?, xs:double, xs:double)</code>
 */
struct PFla_pair_t
PFbui_fn_substring_dbl (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               disjunion (
                                   args[0].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[0].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               project (cast (args[1].rel,
                                              col_cast,
                                              col_item,
                                              aat_dbl),
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_cast)),
                               col_iter,
                               col_iter1),
                           project (cast (args[2].rel,
                                          col_cast,
                                          col_item,
                                          aat_dbl),
                                    proj (col_iter2, col_iter),
                                    proj (col_item2, col_cast)),
                           col_iter,
                           col_iter2),
                       alg_fun_fn_substring_dbl,
                       col_res,
                       collist (col_item, col_item1, col_item2)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:string-length(xs:string?)</code>
 */
struct PFla_pair_t
PFbui_fn_string_length (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       disjunion (
                           args[0].rel,
                           attach (
                               attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                               col_pos, lit_nat (1)),
                           col_item, lit_str (""))),
                       alg_fun_fn_string_length,
                       col_res,
                       collist(col_item)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for <code>fn:normalize-space(xs:string?)</code>
 */
struct PFla_pair_t
PFbui_fn_normalize_space (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       disjunion (
                           args[0].rel,
                           attach (
                               attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                               col_pos, lit_nat (1)),
                           col_item, lit_str (""))),
                       alg_fun_fn_normalize_space,
                       col_res,
                       collist(col_item)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for <code>fn:upper-case(xs:string?)</code>
 */
struct PFla_pair_t
PFbui_fn_upper_case (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       disjunion (
                           args[0].rel,
                           attach (
                               attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                               col_pos, lit_nat (1)),
                           col_item, lit_str (""))),
                       alg_fun_fn_upper_case,
                       col_res,
                       collist(col_item)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for <code>fn:lower-case(xs:string?)</code>
 */
struct PFla_pair_t
PFbui_fn_lower_case (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       disjunion (
                           args[0].rel,
                           attach (
                               attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                           col_item, lit_str (""))),
                       alg_fun_fn_lower_case,
                       col_res,
                       collist(col_item)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>fn:translate(xs:string?, xs:string, xs:string)</code>
 */
struct PFla_pair_t
PFbui_fn_translate (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               disjunion (
                                   args[0].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[0].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           project (args[2].rel,
                                    proj (col_iter2, col_iter),
                                    proj (col_item2, col_item)),
                           col_iter,
                           col_iter2),
                       alg_fun_fn_translate,
                       col_res,
                       collist (col_item, col_item1, col_item2)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),


        .frag = PFla_empty_set () };
}

/* ------------------------------------------ */
/* 7.5. Functions Based on Substring Matching */
/* ------------------------------------------ */

/**
 * Algebra implementation for <code>fn:contains (xs:string, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains (const PFla_op_t *loop,
                   bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_fn_contains,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),
        .frag = PFla_empty_set ()};
}

#ifdef HAVE_GEOXML
/**
 * Algebra implementation for <code>fn:contains (xs:string, xs:string)</code>.
 */

struct PFla_pair_t
PFbui_geoxml_wkb (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void)loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            cast (args[0].rel,
                                  col_cast,
                                  col_item,
                                  aat_str),
                            alg_fun_geo_wkb,
                            col_res,
                            collist (col_cast)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

struct PFla_pair_t
PFbui_geoxml_point (const PFla_op_t *loop, bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_geo_point,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),
        .frag = PFla_empty_set ()};
}

struct PFla_pair_t
PFbui_geoxml_distance (const PFla_op_t *loop, bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_geo_distance,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),
        .frag = PFla_empty_set ()};
}

/**
 *  Stolen from function pf:docname(node*) as string*
 */
struct PFla_pair_t
PFbui_geoxml_geometry (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (args[0].rel,
                                  alg_fun_geo_geometry,
                                  col_res,
                                  collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Algebra implementation for
 * geoxml:relate(xs:string?, xs:string, xs:string)</code>
 * stolen from fn:translate(xs:string?, xs:string, xs:string)</code>
 */
struct PFla_pair_t
PFbui_geoxml_relate (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               disjunion (
                                   args[0].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[0].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           project (args[2].rel,
                                    proj (col_iter2, col_iter),
                                    proj (col_item2, col_item)),
                           col_iter,
                           col_iter2),
                       alg_fun_geo_relate,
                       col_res,
                       collist (col_item, col_item1, col_item2)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),


        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * geoxml:intersection(xs:string, xs:string) : xs:string </code>
 * stolen from fn:concat 
 */
struct PFla_pair_t
PFbui_geoxml_intersection (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_geo_intersection,
                       col_res,
                       collist(col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),
        .frag = args[0].frag };
}

#endif

/**
 * Algebra implementation for <code>fn:contains (xs:string?, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains_opt (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   disjunion (
                       fun_1to1 (
                           eqjoin (
                               /* Give the empty sequence to correct type.
                                  (General optimization will remove cast). */
                               project (
                                   cast (args[0].rel,
                                         col_cast, col_item, aat_str),
                                   proj (col_iter, col_iter),
                                   proj (col_pos, col_pos),
                                   proj (col_item, col_cast)),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           alg_fun_fn_contains,
                           col_res,
                           collist (col_item, col_item1)),
                       /* Apply fn:contains for empty strings
                          separately to simplify contains operator
                          rewrite rules (constant optimization). */
                       fun_1to1 (
                           eqjoin (
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str ("")),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           alg_fun_fn_contains,
                           col_res,
                           collist (col_item, col_item1))),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:contains (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains_opt_opt (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               proj (col_iter1, col_iter),
                               proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_contains,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:starts-with (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_starts_with (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               proj (col_iter1, col_iter),
                               proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_starts_with,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:ends-with (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_ends_with (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               proj (col_iter1, col_iter),
                               proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_ends_with,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:substring-before (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_substring_before (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               proj (col_iter1, col_iter),
                               proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_substring_before,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:substring_after (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_substring_after (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               proj (col_iter1, col_iter),
                               proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_substring_after,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/* ----------------------------------------------- */
/* 7.6. String Functions that Use Pattern Matching */
/* ----------------------------------------------- */

/**
 * Algebra implementation for <code>fn:matches(xs:string?, xs:string)</code>
 */
struct PFla_pair_t
PFbui_fn_matches (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   col_item, lit_str (""))),
                           project (args[1].rel,
                                    proj (col_iter1, col_iter),
                                    proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_matches,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:matches(xs:string?, xs:string, xs:string)</code>
 */
struct PFla_pair_t
PFbui_fn_matches_str (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               disjunion (
                                   args[0].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[0].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           project (args[2].rel,
                                    proj (col_iter2, col_iter),
                                    proj (col_item2, col_item)),
                           col_iter,
                           col_iter2),
                       alg_fun_fn_matches_flag,
                       col_res,
                       collist (col_item, col_item1, col_item2)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:replace(xs:string?, xs:string, xs:string)</code>
 */
struct PFla_pair_t
PFbui_fn_replace (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               disjunion (
                                   args[0].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[0].rel,
                                                   proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                                       col_item, lit_str (""))),
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                           project (args[2].rel,
                                    proj (col_iter2, col_iter),
                                    proj (col_item2, col_item)),
                           col_iter,
                           col_iter2),
                       alg_fun_fn_replace,
                       col_res,
                       collist (col_item, col_item1, col_item2)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code>fn:replace(xs:string?, xs:string, xs:string, xs:string)</code>
 */
struct PFla_pair_t
PFbui_fn_replace_str (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               eqjoin (
                                   disjunion (
                                       args[0].rel,
                                       attach (
                                           attach (
                                               difference (
                                                   loop,
                                                   project (
                                                       args[0].rel,
                                                       proj (col_iter,
                                                             col_iter))),
                                               col_pos, lit_nat (1)),
                                           col_item, lit_str (""))),
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (args[2].rel,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            project (args[3].rel,
                                        proj (col_iter3, col_iter),
                                        proj (col_item3, col_item)),
                            col_iter,
                            col_iter3),
                       alg_fun_fn_replace_flag,
                       col_res,
                       collist (col_item, col_item1, col_item2, col_item3)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};

}

/* -------------------------------------------- */
/* 9. FUNCTIONS AND OPERATORS ON BOOLEAN VALUES */
/* -------------------------------------------- */
/* --------------------------------------------- */
/* 9.1. Additional Boolean Constructor Functions */
/* --------------------------------------------- */

static struct PFla_pair_t
PFbui_fn_bln_lit (const PFla_op_t *loop,
                  bool value)
{
    return (struct PFla_pair_t) {
        .rel = attach(
                   attach(loop,
                          col_pos,
                          lit_nat(1)),
                   col_item,
                   lit_bln(value)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for <code>fn:true ()</code>.
 */
struct PFla_pair_t
PFbui_fn_true (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args __attribute__((unused)))
{
    (void) ordering; (void) side_effects; (void) args;

    return PFbui_fn_bln_lit(loop, true);
}

/**
 * Algebra implementation for <code>fn:false ()</code>.
 */
struct PFla_pair_t
PFbui_fn_false (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args __attribute__((unused)))
{
    (void) ordering; (void) side_effects; (void) args;

    return PFbui_fn_bln_lit(loop, false);
}

/* -------------------------------- */
/* 9.2. Operators on Boolean Values */
/* -------------------------------- */
/* - - - - - - - - - - - - - - - - - - - - - - - -  */
/* see: 6.3. Comparison Operators on Numeric Values */
/* - - - - - - - - - - - - - - - - - - - - - - - -  */

/* -------------------------------- */
/* 9.3. Functions on Boolean Values */
/* -------------------------------- */

/**
 * Algebra implementation for fn:not (boolean) as boolean
 * @see un_op()
 */
struct PFla_pair_t
PFbui_fn_not_bln (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not, args[0]);
}

/* --------------------------------------------------------- */
/* 10. FUNCTIONS AND OPERATORS ON DURATIONS, DATES AND TIMES */
/* --------------------------------------------------------- */
/* ------------------------------------------------------------ */
/* 10.4. Comparison Operators on Duration, Date and Time Values */
/* ------------------------------------------------------------ */

/**
 * Algebra implementation for
 * <code> op:yearMonthDuration-less-than (yearMonthDuration, yearMonthDuration)
 *        as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_yearmonthduration_lt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_ymduration, PFla_gt, loop, ordering, side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

struct PFla_pair_t
PFbui_op_yearmonthduration_le (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_ymduration, PFla_gt,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:yearMonthDuration-greater-than (yearMonthDuration,
 *        yearMonthDuration) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_yearmonthduration_gt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_ymduration, PFla_gt, loop,
                     ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_yearmonthduration_ge (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_ymduration, PFla_gt,
                            loop, ordering, side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));

}

/**
 * Algebra implementation for
 * <code> op:dayTimeDuration-less-than (dayTimeDuration, dayTimeDuration)
 *        as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_daytimeduration_lt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_dtduration, PFla_gt, loop, ordering, side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

struct PFla_pair_t
PFbui_op_daytimeduration_le (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dtduration, PFla_gt,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:dayTimeDuration-greater-than (dayTimeDuration,
 *        dayTimeDuration) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_daytimeduration_gt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_dtduration, PFla_gt, loop,
                     ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_daytimeduration_ge (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dtduration, PFla_gt,
                            loop, ordering, side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));

}

/**
 * Algebra implementation for
 * <code> op:dateTime-equal (dateTime, dateTime) as boolean </code>
 * @see bin_op()
 */
struct PFla_pair_t
PFbui_op_datetime_eq (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_dtime, PFla_eq, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_datetime_ne (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dtime, PFla_eq,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:dateTime-less-than (dateTime, dateTime) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_datetime_lt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_dtime, PFla_gt, loop, ordering, side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

struct PFla_pair_t
PFbui_op_datetime_le (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dtime, PFla_gt,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:dateTime-greater-than (dateTime, dateTime) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_datetime_gt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_dtime, PFla_gt, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_datetime_ge (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dtime, PFla_gt,
                            loop, ordering, side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));

}

/**
 * Algebra implementation for
 * <code> op:date-equal (date, date) as boolean </code>
 * @see bin_op()
 */
struct PFla_pair_t
PFbui_op_date_eq (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_date, PFla_eq, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_date_ne (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_date, PFla_eq,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:date-less-than (date, date) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_date_lt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_date, PFla_gt, loop, ordering, side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

struct PFla_pair_t
PFbui_op_date_le (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_date, PFla_gt,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:date-greater-than (date, date) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_date_gt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_date, PFla_gt, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_date_ge (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_date, PFla_gt,
                            loop, ordering, side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));

}

/**
 * Algebra implementation for
 * <code> op:time-equal (time, time) as boolean </code>
 * @see bin_op()
 */
struct PFla_pair_t
PFbui_op_time_eq (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_time, PFla_eq, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_time_ne (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_time, PFla_eq,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:time-less-than (time, time) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_time_lt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_time, PFla_gt, loop, ordering, side_effects,
                     (struct PFla_pair_t []) { args[1], args[0] });
}

struct PFla_pair_t
PFbui_op_time_le (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_time, PFla_gt,
                            loop, ordering, side_effects, args));

}

/**
 * Algebra implementation for
 * <code> op:time-greater-than (time, time) as boolean </code>
 * @see bin_op()
 */

struct PFla_pair_t
PFbui_op_time_gt (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return bin_comp (aat_time, PFla_gt, loop, ordering, side_effects, args);

}

struct PFla_pair_t
PFbui_op_time_ge (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;


    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_time, PFla_gt,
                            loop, ordering, side_effects,
                            (struct PFla_pair_t []) { args[1], args[0] }));

}

/* ----------------------------------------------------------------- */
/* 10.5 Component Extraction Functions on Durations, Dates and Times */
/* ----------------------------------------------------------------- */

/**
 * Helper function for component extraction functions on date time
 */

struct PFla_pair_t
comp_extract (PFalg_fun_t kind, struct PFla_pair_t *args)
{
    return (struct PFla_pair_t) {
        .rel = project (
                        fun_1to1 (
                            args[0].rel,
                            kind,
                            col_res,
                            collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for
 * <code> fn:year-from-dateTime (dateTime?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_year_from_datetime (const PFla_op_t *loop,
                             bool ordering,
                             PFla_op_t **side_effects,
                             struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_year_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:month-from-dateTime (dateTime?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_month_from_datetime (const PFla_op_t *loop,
                              bool ordering,
                              PFla_op_t **side_effects,
                              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_month_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:day-from-dateTime (dateTime?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_day_from_datetime (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_day_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:hours-from-dateTime (dateTime?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_hours_from_datetime (const PFla_op_t *loop,
                              bool ordering,
                              PFla_op_t **side_effects,
                              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_hours_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:minutes-from-dateTime (dateTime?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_minutes_from_datetime (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_minutes_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:seconds-from-dateTime (dateTime?) as decimal? </code>
 */
struct PFla_pair_t
PFbui_fn_seconds_from_datetime (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_seconds_from_datetime, args);
}

/**
 * Algebra implementation for
 * <code> fn:year-from-date (date?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_year_from_date (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_year_from_date, args);
}

/**
 * Algebra implementation for
 * <code> fn:month-from-date (date?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_month_from_date (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_month_from_date, args);
}

/**
 * Algebra implementation for
 * <code> fn:day-from-date (date?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_day_from_date (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_day_from_date, args);
}

/**
 * Algebra implementation for
 * <code> fn:hours-from-time (time?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_hours_from_time (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_hours_from_time, args);
}

/**
 * Algebra implementation for
 * <code> fn:minutes-from-time (time?) as integer? </code>
 */
struct PFla_pair_t
PFbui_fn_minutes_from_time (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_minutes_from_time, args);
}

/**
 * Algebra implementation for
 * <code> fn:seconds-from-time (time?) as decimal? </code>
 */
struct PFla_pair_t
PFbui_fn_seconds_from_time (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return comp_extract(alg_fun_fn_seconds_from_time, args);
}

/* -------------------------------------- */
/* 10.6 Arithmetic Operators on Durations */
/* -------------------------------------- */

/*
 * Helper function for arithmetic operators on durations
 */
static PFla_op_t *
dur_arith (PFalg_fun_t kind,
           const PFla_op_t *n1,
           const PFla_op_t *n2)
{
    return project (fun_1to1 (
                           eqjoin (
                               eqjoin (
                                   n1,
                                   project (n2,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (n2,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            kind,
                            col_res,
                            collist (col_item, col_item1, col_item2)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res));
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_yearmonthduration_plus (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_add_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_yearmonthduration_minus (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_subtract_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_yearmonthduration_times (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_multiply_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_yearmonthduration_div_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_divide_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_yearmonthduration_div (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_divide_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_daytimeduration_plus (const PFla_op_t *loop,
                               bool ordering,
                               PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_add_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_daytimeduration_minus (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_subtract_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_daytimeduration_times (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_multiply_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_daytimeduration_div_dbl (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_divide_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for
 * <code>  </code>
 */
struct PFla_pair_t
PFbui_op_daytimeduration_div (const PFla_op_t *loop,
                              bool ordering,
                              PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = dur_arith (alg_fun_divide_dur, args[0].rel, args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/* ------------------------------- */
/* 11. FUNCTIONS RELATED TO QNAMES */
/* ------------------------------- */
/* ------------------------------------------------- */
/* 11.1. Additional Constructor Functions for QNames */
/* ------------------------------------------------- */

/**
 * fn:resolve-QName (xs:string)
 *
 * Our implementation is actually off the specs.  There, the function
 * is supposed to require two arguments: a string argument, as well as
 * node argument that provides "in-scope namespace declarations".  The
 * latter are then used to resolve namespace prefixes in the string
 * argument for a proper QName result.  (Currently) we neither have
 * a suitable element node available during compilation, nor do we
 * track in-scope namespaces at runtime.  Our back-end implementation
 * thus "cheats" a bit and simply generates a new URI.  Totally off
 * the specs---feel free to improve this if you feel like it.
 *
 * Note that fs.brg introduces calls the fn:resolve-QName() for
 * computed element and attribute constructors.  fn:resolve-QName()
 * is not even the right function to use there, because the constructors
 * should do their job based on _statically-known namespace declarations_,
 * not in-scope declarations.
 *
 * Furthermore a cast from string to xs:QName is only allowed at compile
 * time on constant strings. At runtime a cast to xs:QName is only allowed
 * for QNames inputs.
 */
struct PFla_pair_t
PFbui_fn_resolve_qname (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /* implement it as a simple cast to xs:QName */
    return (struct PFla_pair_t) {
        .rel = project (cast (args[0].rel, col_cast, col_item, aat_qname),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_cast)),
        .frag = PFla_empty_set () };
}

/**
 * fn:QName (xs:string?, xs:string)
 */
struct PFla_pair_t
PFbui_fn_qname (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               attach (
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (col_iter, col_iter))),
                                       col_pos, lit_nat (1)),
                                   /* use '|' as invalid uri */
                                   col_item, lit_str ("|"))),
                           project (args[1].rel,
                                    proj (col_iter1, col_iter),
                                    proj (col_item1, col_item)),
                           col_iter,
                           col_iter1),
                       alg_fun_fn_qname,
                       col_res,
                       collist (col_item, col_item1)),
                proj (col_iter, col_iter),
                proj (col_pos, col_pos),
                proj (col_item, col_res)),

        .frag = PFla_empty_set ()};
}

/* ----------------------------------- */
/* 14 FUNCTIONS AND OPERATORS ON NODES */
/* ----------------------------------- */
/* ------------ */
/* 14.1 fn:name */
/* ------------ */

struct PFla_pair_t
PFbui_fn_name (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *strings = project (
                             fun_1to1 (
                                 project (args[0].rel,
                                          proj (col_iter, col_iter),
                                          proj (col_pos, col_pos),
                                          proj (col_item, col_item)),
                                 alg_fun_fn_name,
                                 col_res,
                                 collist(col_item)),
                             proj (col_iter, col_iter),
                             proj (col_pos, col_pos),
                             proj (col_item, col_res));

    PFla_op_t *res = disjunion (
                         strings,
                         attach (
                             attach (
                                 difference (
                                     loop,
                                     project (
                                         strings,
                                         proj (col_iter, col_iter))),
                                 col_pos, lit_nat (1)),
                             col_item, lit_str ("")));

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set() };
}

/* ------------------- */
/* 14.2. fn:local-name */
/* ------------------- */

struct PFla_pair_t
PFbui_fn_local_name (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *strings = project (
                             fun_1to1 (
                                 project (args[0].rel,
                                          proj (col_iter, col_iter),
                                          proj (col_pos, col_pos),
                                          proj (col_item, col_item)),
                                 alg_fun_fn_local_name,
                                 col_res,
                                 collist(col_item)),
                             proj (col_iter, col_iter),
                             proj (col_pos, col_pos),
                             proj (col_item, col_res));

    PFla_op_t *res = disjunion (
                         strings,
                         attach (
                             attach (
                                 difference (
                                     loop,
                                     project (
                                         strings,
                                         proj (col_iter, col_iter))),
                                 col_pos, lit_nat (1)),
                             col_item, lit_str ("")));

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set() };
}

/* --------------------- */
/* 14.3. fn:namespace-uri */
/* --------------------- */

struct PFla_pair_t
PFbui_fn_namespace_uri (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *strings = project (
                             fun_1to1 (
                                 project (args[0].rel,
                                          proj (col_iter, col_iter),
                                          proj (col_pos, col_pos),
                                          proj (col_item, col_item)),
                                 alg_fun_fn_namespace_uri,
                                 col_res,
                                 collist(col_item)),
                             proj (col_iter, col_iter),
                             proj (col_pos, col_pos),
                             proj (col_item, col_res));

    PFla_op_t *res = disjunion (
                         strings,
                         attach (
                             attach (
                                 difference (
                                     loop,
                                     project (
                                         strings,
                                         proj (col_iter, col_iter))),
                                 col_pos, lit_nat (1)),
                             col_item, lit_str ("")));

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set() };
}

/* --------------- */
/* 14.4. fn:number */
/* --------------- */

struct PFla_pair_t
PFbui_fn_number (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) ordering;

    /* As we do not support the value NaN we need to generate an error
       for all tuples that are empty (instead of attaching NaN). */
    *side_effects = error (
                        *side_effects,
                        attach (
                            difference (
                                loop,
                                project (
                                    args[0].rel,
                                    proj (col_iter, col_iter))),
                            col_item,
                            lit_str ("We do not support the value NaN.")),
                        col_item);

    return (struct  PFla_pair_t) {
                 .rel = project (
                            fun_1to1 (
                                args[0].rel,
                                alg_fun_fn_number,
                                col_res,
                                collist (col_item)),
                            proj (col_iter, col_iter),
                            proj (col_pos, col_pos),
                            proj (col_item, col_res)),
                 .frag = args[0].frag };
}

/* --------------------- */
/* 14.6. op:is-same-node */
/* --------------------- */

/**
 * Algebra implementation for op:is-same-node (node?, node?)
 */
struct PFla_pair_t
PFbui_op_is_same_node (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (PFla_eq (
                        eqjoin (
                            project (args[0].rel,
                                     proj (col_iter, col_iter),
                                     proj (col_pos, col_pos),
                                     proj (col_item, col_item)),
                            project (args[1].rel,
                                     proj (col_iter1, col_iter),
                                     proj (col_item1, col_item)),
                            col_iter,
                            col_iter1),
                        col_res, col_item, col_item1),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos),
                    proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/* -------------------- */
/* 14.7. op:node-before */
/* -------------------- */

/**
 * Algebra implementation for op:node-before (node?, node?)
 */
struct PFla_pair_t
PFbui_op_node_before (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (PFla_gt (
                        eqjoin (
                            project (args[1].rel,
                                     proj (col_iter, col_iter),
                                     proj (col_pos, col_pos),
                                     proj (col_item, col_item)),
                            project (args[0].rel,
                                     proj (col_iter1, col_iter),
                                     proj (col_item1, col_item)),
                            col_iter,
                            col_iter1),
                        col_res, col_item, col_item1),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos),
                    proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/* ------------------- */
/* 14.8. op:node-after */
/* ------------------- */

/**
 * Algebra implementation for op:node-after (node?, node?)
 */
struct PFla_pair_t
PFbui_op_node_after (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (PFla_gt (
                        eqjoin (
                            project (args[0].rel,
                                     proj (col_iter, col_iter),
                                     proj (col_pos, col_pos),
                                     proj (col_item, col_item)),
                            project (args[1].rel,
                                     proj (col_iter1, col_iter),
                                     proj (col_item1, col_item)),
                            col_iter,
                            col_iter1),
                        col_res, col_item, col_item1),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos),
                    proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/* ------------- */
/* 14.9. fn:root */
/* ------------- */
struct PFla_pair_t
PFbui_fn_root (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t        *node_scj,
                     *sel;
    PFalg_step_spec_t anc_node_spec;
    
    anc_node_spec.axis = alg_anc_s;
    anc_node_spec.kind = node_kind_node;
    /* missing QName */
    anc_node_spec.qname = PFqname (PFns_wild, NULL);
    
    /* do an ancestor-or-self::node() step
       with exact position values */
    node_scj = rownum (
                   distinct (
                       project (
                           PFla_step_join_simple (
                               PFla_set_to_la (args[0].frag),
                               project (args[0].rel,
                                        proj (col_iter, col_iter),
                                        proj (col_item, col_item)),
                               anc_node_spec,
                               col_item, col_res),
                           proj (col_iter, col_iter),
                           proj (col_item, col_res))),
                   col_pos, sortby (col_item), col_iter);
    
    /* select the first ancestor */
    sel = project (
              select_ (
                  eq (attach (cast (node_scj, col_item1, col_pos, aat_int),
                              col_item2,
                              lit_int (1)),
                      col_res,
                      col_item1,
                      col_item2),
                  col_res),
              proj (col_iter, col_iter),
              proj (col_item, col_item));
    
    /* add the position values */
    return (struct PFla_pair_t) {
        .rel = attach (sel, col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}


/* ---------------------------------------- */
/* 15. FUNCTIONS AND OPERATORS ON SEQUENCES */
/* ---------------------------------------- */
/* -------------------------------------------------- */
/* 15.1. General Functions and Operators on Sequences */
/* -------------------------------------------------- */

/**
 * Algebra implementation for <code>fn:boolean (xs:boolean)</code>.
 *
 * If the operand is a single boolean value, the function returns
 * the boolean value itself.
 */
struct PFla_pair_t
PFbui_fn_boolean_bln (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    if (PFprop_type_of (args[0].rel, col_item) != aat_bln)
        return PFbui_fn_boolean_item (loop, ordering, side_effects, args);
    else
        return args[0];
}

/**
 * Algebra implementation for <code>fn:boolean (xs:boolean?)</code>.
 *
 * If the operand is an optional boolean value, the function returns
 * the value itself if there is one, and @c false if the operand is
 * the empty sequence.
 *
 *              env,loop,delta: e1 => q1,delta1
 *  --------------------------------------------------------------
 *             env,loop,delta: fn:boolean(e1) =>
 * (
 *         /                            pos|item  \
 *   q1 U | (loop \ (proj_iter (q1))) X ---+----   |
 *         \                             1 |false /
 * ,
 *   delta1
 * )
 */
struct PFla_pair_t
PFbui_fn_boolean_optbln (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    if (PFprop_type_of (args[0].rel, col_item) != aat_bln)
        return PFbui_fn_boolean_item (loop, ordering, side_effects, args);
    else
        return (struct PFla_pair_t) {
            .rel = disjunion (
                       args[0].rel,
                       attach (
                           attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                                           col_pos, lit_nat (1)),
                           col_item, lit_bln (false))),
            .frag = PFla_empty_set () };
}

/**
 * Helper function for PFbui_fn_boolean_item
 * Returns those rows with col_item != case_->params.
 */
static PFla_op_t *
fn_boolean_atomic (PFla_op_t *n, PFalg_atom_t literal)
{
    return project (not (eq (attach (n,
                                     col_item1, literal),
                             col_res, col_item, col_item1),
                         col_res1, col_res),
                    proj (col_iter, col_iter),
                    proj (col_item, col_res1),
                    proj (col_pos, col_pos));
}

/**
 * Helper function for PFbui_fn_boolean_item.
 * Returns true for every iteration (fn:boolean () is
 * always true when called with a non empty sequence of
 * nodes).
 */
static PFla_op_t *
fn_boolean_node (PFla_op_t *n)
{
    return attach (attach (distinct (project (n, proj (col_iter, col_iter))),
                           col_pos, lit_nat (1)),
                   col_item, lit_bln (true));
}

static PFla_op_t *
fn_boolean_callback (PFla_op_t *n, PFalg_simple_type_t type, void *params)
{
    (void) params;

    switch (type) {
    case aat_node:
        return fn_boolean_node (n);
    case aat_bln:
        return n;
    case aat_nat:
        return fn_boolean_atomic (n, lit_nat (0));
    case aat_int:
        return fn_boolean_atomic (n, lit_int (0));
    case aat_dbl:
        return fn_boolean_atomic (n, lit_dbl (0));
    case aat_dec:
        return fn_boolean_atomic (n, lit_dec (0));
    case aat_uA:
        return fn_boolean_atomic (project (
                                      cast (n, col_cast, col_item, aat_str),
                                      proj (col_iter, col_iter),
                                      proj (col_pos, col_pos),
                                      proj (col_item, col_cast)),
                                  lit_str (""));
    case aat_str:
        return fn_boolean_atomic (n, lit_str (""));
    default:
        assert (false);
    }
    return NULL;
}

/**
 * Algebra implementation for <code>fn:boolean (node()*|xs:boolean
 * xs:boolean|xs:integer|xs:decimal|xs:double|xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_boolean_item (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    /* Typeswitch, for helper functions see above. */
    PFla_op_t *res  = typeswitch (args[0].rel,
                                  8,
                                  (PFalg_simple_type_t [8])
                                    { aat_node,
                                      aat_bln,
                                      aat_nat,
                                      aat_int,
                                      aat_dbl,
                                      aat_dec,
                                      aat_uA,
                                      aat_str, },
                                  fn_boolean_callback,
                                  NULL);

    /* handle empty sequences. */
    return (struct PFla_pair_t) {
        .rel = disjunion (res,
                   attach (
                       attach (
                           difference (
                               loop,
                               project (
                                   args[0].rel,
                                   proj (col_iter,col_iter))),
                           col_item, lit_bln (false)),
                       col_pos, lit_nat (1))),
        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:empty(item*)</code>.
 *
 *            env,loop,delta: e1 => (q1, delta1)
 *  ---------------------------------------------------------------------
 *             env,loop,delta: fn:empty (e1) =>
 * (
 *  / //                      itm\     /                        itm\\    pos \
 * | || dist (proj_iter q1) X --- | U | (loop \ proj_iter q1) X --- || X ---  |
 *  \ \\                      fal/     \                        tru//     1  /
 * ,
 *  delta1
 * )
 */
struct PFla_pair_t
PFbui_fn_empty (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = attach (
               disjunion (
                   attach (
                       distinct (project (args[0].rel,
                                 proj (col_iter, col_iter))),
                       col_item, lit_bln (false)),
                   attach (
                       difference (
                           loop,
                           project (args[0].rel,
                                    proj (col_iter, col_iter))),
                       col_item, lit_bln (true))),
               col_pos, lit_nat (1)),
        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:exists(item*)</code>.
 *
 *            env,loop,delta: e1 => (q1, delta1)
 *  ---------------------------------------------------------------------
 *             env,loop,delta: fn:exists (e1) =>
 * (
 *  / //                      itm\     /                        itm\\    pos \
 * | || dist (proj_iter q1) X --- | U | (loop \ proj_iter q1) X --- || X ---  |
 *  \ \\                      tru/     \                        fal//     1  /
 * ,
 *  delta1
 * )
 */
struct PFla_pair_t
PFbui_fn_exists (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = attach (
               disjunion (
                   attach (
                       distinct (project (args[0].rel,
                                 proj (col_iter, col_iter))),
                       col_item, lit_bln (true)),
                   attach (
                       difference (
                           loop,
                           project (args[0].rel,
                                    proj (col_iter, col_iter))),
                       col_item, lit_bln (false))),
               col_pos, lit_nat (1)),
        .frag = PFla_empty_set ()};
}

/**
 * The fn:distinct-values function removes its duplicates
 */
struct PFla_pair_t
PFbui_fn_distinct_values (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    if (PFprop_type_of (args[0].rel, col_item) & aat_uA &&
        PFprop_type_of (args[0].rel, col_item) & aat_str) {
        PFla_op_t *type = type (args[0].rel, col_res, col_item, aat_uA);

        /* select those rows that have type "untypedAtomic" (part1) */
        PFla_op_t *part1 = project (
                               cast (
                                   type_assert_pos (
                                       select_ (type, col_res),
                                       col_item, aat_uA),
                                   col_item2, col_item, aat_str),
                               proj (col_iter, col_iter),
                               proj (col_item, col_item2));

        /* select the remaining rows (part2) */
        PFla_op_t *part2 = project (
                               type_assert_neg (
                                   select_ (not (type, col_res1, col_res),
                                            col_res1),
                                   col_item, aat_uA),
                               proj (col_iter, col_iter),
                               proj (col_item, col_item));

        return (struct PFla_pair_t) {
                      .rel = rowid (
                                 distinct (disjunion (part1, part2)),
                                 col_pos),
                      .frag = args[0].frag };
    }
    else
        return (struct PFla_pair_t) {
                      .rel = rowid (
                                 distinct (
                                     project (args[0].rel,
                                          proj (col_iter, col_iter),
                                          proj (col_item, col_item))),
                                 col_pos),
                      .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_insert_before (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    /* mark all rows true where the second argument is greather than
       the index position */
    PFla_op_t *partition = gt (eqjoin (
                                   cast (adjust_positions (args[0].rel),
                                         col_cast,
                                         col_pos,
                                         aat_int),
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               col_res,
                               col_item1,
                               col_cast);

    /* select all the tuples whose index position is smaller than
       the second argument */
    PFla_op_t *first = attach (
                           project (
                               select_ (partition, col_res),
                               proj (col_iter, col_iter),
                               proj (col_pos, col_pos),
                               proj (col_item, col_item)),
                           col_ord,
                           lit_nat (1));

    PFla_op_t *second = attach (args[2].rel, col_ord, lit_nat (2));

    /* select all the tuples whose index position is bigger or equal to
       the second argument */
    PFla_op_t *third = attach (
                           project (
                               select_ (
                                   not (partition, col_res1, col_res),
                                   col_res1),
                               proj (col_iter, col_iter),
                               proj (col_pos, col_pos),
                               proj (col_item, col_item)),
                           col_ord,
                           lit_nat (3));

    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        /* patch the third function argument into the middle*/
        .rel = project (
                    rank (
                        disjunion (first, disjunion (second, third)),
                        col_pos1,
                        sortby (col_ord, col_pos)),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_pos1),
                    proj (col_item, col_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_remove (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /*
     * Join the index position with the sequence,
     * cast the positions of the sequence to integer
     * and keep all rows whose columns are distinct from
     * the index position.
     */
    return (struct PFla_pair_t) {
        .rel = project (
                   select_ (
                       not (eq (eqjoin (
                                    cast (adjust_positions (args[0].rel),
                                          col_cast,
                                          col_pos,
                                          aat_int),
                                    project (args[1].rel,
                                             proj (col_iter1, col_iter),
                                             proj (col_item1, col_item)),
                                    col_iter,
                                    col_iter1),
                                col_res,
                                col_cast,
                                col_item1),
                            col_res1, col_res),
                       col_res1),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_reverse (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /*
     * Use column pos to introduce a new column pos
     * that is sorted in reverse order.
     */
    return (struct PFla_pair_t) {
        .rel  = project (
                    rank (
                        args[0].rel,
                        col_res,
                        PFord_refine (PFordering (), col_pos, DIR_DESC)),
                    proj (col_iter, col_iter),
                    proj (col_pos, col_res),
                    proj (col_item, col_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_subsequence_till_end (const PFla_op_t *loop,
                               bool ordering,
                               PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    PFla_op_t *startingLoc = args[1].rel;

    (void) loop; (void) ordering; (void) side_effects;

#ifndef NDEBUG
    /* make sure that the second argument is of type integer */
    for (unsigned int i = 0; i < startingLoc->schema.count; i++) {
        if (startingLoc->schema.items[i].name == col_item) {
            assert (startingLoc->schema.items[i].type == aat_int);
            break;
        }
    }
#endif

    /*
     * Join the index position with the sequence,
     * cast the positions of the sequence to integer
     * and keep all rows whose columns whose position values
     * are bigger or equal than the index position.
     */
    return (struct PFla_pair_t) {
        .rel = project (
                   select_ (
                       not (gt (eqjoin (
                                    cast (adjust_positions (args[0].rel),
                                          col_cast,
                                          col_pos,
                                          aat_int),
                                    project (startingLoc,
                                             proj (col_iter1, col_iter),
                                             proj (col_item1, col_item)),
                                    col_iter,
                                    col_iter1),
                                col_res,
                                col_item1,
                                col_cast),
                            col_res1, col_res),
                       col_res1),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_subsequence (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    PFla_op_t *startingLoc = args[1].rel;
    PFla_op_t *length      = args[2].rel;
    PFla_op_t *first_cond;

#ifndef NDEBUG
    /* make sure that the second and the third argument are of type integer */
    for (unsigned int i = 0; i < startingLoc->schema.count; i++) {
        if (startingLoc->schema.items[i].name == col_item) {
            assert (startingLoc->schema.items[i].type == aat_int);
            break;
        }
    }
    for (unsigned int i = 0; i < length->schema.count; i++) {
        if (length->schema.items[i].name == col_item) {
            assert (length->schema.items[i].type == aat_int);
            break;
        }
    }
#endif

    (void) loop; (void) ordering; (void) side_effects;

    /* evaluate the first condition (startingLoc) */
    first_cond = project (
                     select_ (
                         not (gt (eqjoin (
                                      cast (adjust_positions (args[0].rel),
                                            col_cast,
                                            col_pos,
                                            aat_int),
                                      project (startingLoc,
                                               proj (col_iter1, col_iter),
                                               proj (col_item1, col_item)),
                                      col_iter,
                                      col_iter1),
                                  col_res,
                                  col_item1,
                                  col_cast),
                              col_res1, col_res),
                         col_res1),
                     proj (col_iter, col_iter),
                     proj (col_pos, col_pos),
                     proj (col_item, col_item),
                     proj (col_cast, col_cast),    /* pos as int  */
                     proj (col_item1, col_item1)); /* startingLoc */

    /* evaluate the second condition ($pos < $startingLoc + $length)
       and fill in new position values afterwards */
    return (struct PFla_pair_t) {
        .rel = project (
                   select_ (
                       gt (fun_1to1 (
                               eqjoin (
                                   first_cond,
                                   project (length,
                                            proj (col_iter1, col_iter),
                                            proj (col_item2, col_item)),
                                   col_iter,
                                   col_iter1),
                               alg_fun_num_add,
                               col_res,
                               collist (col_item2, col_item1)),
                           col_res1,
                           col_res,
                           col_cast),
                       col_res1),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_unordered (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /*
     * project away pos column
     */
    return (struct PFla_pair_t) {
        .rel  = rowid (
                    project (args[0].rel,
                             proj (col_iter, col_iter),
                             proj (col_item, col_item)),
                    col_pos),
        .frag = args[0].frag };
}

/* ------------------------------------------------------ */
/* 15.2. Functions That Test the Cardinality of Sequences */
/* ------------------------------------------------------ */

/**
 * The fn:zero-or-one function checks at runtime the sequence
 * length of the input relation and triggers an error.
 */
struct PFla_pair_t
PFbui_fn_zero_or_one (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    PFla_op_t *count = select_ (
                           not (eq (attach (
                                        simple_aggr (
                                            project (args[0].rel,
                                                     proj (col_iter, col_iter)),
                                            col_iter,
                                            alg_aggr_count,
                                            col_item,
                                            col_NULL),
                                        col_item1, lit_int (1)),
                                    col_item2, col_item1, col_item),
                                col_res, col_item2),
                           col_res);

    char *err_string = "err:FORG0003, fn:zero-or-one called with "
                       "a sequence containing more than one item.";

    *side_effects = error (
                        *side_effects,
                        attach (
                            project (count, proj (col_iter, col_iter)),
                            col_item,
                            lit_str (err_string)),
                        col_item);

    (void) loop; (void) ordering;

    return (struct  PFla_pair_t) {
                 .rel = args[0].rel,
                 .frag = args[0].frag };
}

/**
 * The fn:exactly-one function checks at runtime the sequence
 * length of the input relation and triggers an error.
 */
struct PFla_pair_t
PFbui_fn_exactly_one (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    PFla_op_t *count = simple_aggr (
                           project (args[0].rel,
                                    proj (col_iter, col_iter)),
                           col_iter,
                           alg_aggr_count,
                           col_item,
                           col_NULL);

    char *err_string = "err:FORG0003, fn:exactly-one called with "
                       "a sequence containing more than one item.";

    *side_effects = error (
                        *side_effects,
                        attach (
                            project (
                                select_ (
                                    not (eq (attach (
                                                count,
                                                col_item1,
                                                lit_int (1)),
                                             col_item2,
                                             col_item1,
                                             col_item),
                                         col_res, col_item2),
                                    col_res),
                                proj (col_iter, col_iter)),
                            col_item,
                            lit_str (err_string)),
                        col_item);

    err_string = "err:FORG0005, fn:exactly-one called with "
                 "an empty sequence.";

    *side_effects = error (
                        *side_effects,
                        attach (
                            difference (
                                loop,
                                project (count, proj (col_iter, col_iter))),
                            col_item,
                            lit_str (err_string)),
                        col_item);

    (void) ordering;

    return (struct  PFla_pair_t) {
                 .rel = args[0].rel,
                 .frag = args[0].frag };
}

/* -------------------------------------------- */
/* 15.3. Equals, Union, Intersection and Except */
/* -------------------------------------------- */

/**
 * Algebra implementation for op:union (node*, node*)
 *
 * Constructs a sequence containing every node that occurs in the
 * values of either the first or the second parameter, eliminating
 * duplicate nodes. Nodes are returned in document order. Two nodes
 * are equal if they are op:is-same-node().
 */
struct PFla_pair_t
PFbui_op_union (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop; (void) side_effects;

    distinct = distinct (disjunion (
                             project (args[0].rel,
                                      proj (col_iter, col_iter),
                                      proj (col_item, col_item)),
                             project (args[1].rel,
                                      proj (col_iter, col_iter),
                                      proj (col_item, col_item))));

    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         col_pos, sortby (col_item)),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = rowid (distinct, col_pos),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
}

/**
 * Algebra implementation for op:intersect (node*, node*)
 *
 * Constructs a sequence containing every node that occurs in the
 * values of both the first and the second parameter, eliminating
 * duplicate nodes. Nodes are returned in document order. If either
 * operand is the empty sequence, the empty sequence is returned.
 * Two nodes are equal if they are op:is-same-node().
 */
struct PFla_pair_t
PFbui_op_intersect (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop; (void) side_effects;

    distinct = distinct (intersect (
                             project (args[0].rel,
                                      proj (col_iter, col_iter),
                                      proj (col_item, col_item)),
                             project (args[1].rel,
                                      proj (col_iter, col_iter),
                                      proj (col_item, col_item))));

    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         col_pos, sortby (col_item)),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = rowid (distinct, col_pos),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
}

/**
 * Algebra implementation for op:except (node*, node*)
 *
 * Constructs a sequence containing every node that occurs in the
 * value of the first parameter, but not in the value of the second
 * parameter, eliminating duplicate nodes. Nodes are returned in
 * document order. If the first parameter is the empty sequence, the
 * empty sequence is returned. If the second parameter is the empty
 * sequence, the first parameter is returned. Two nodes are equal if
 * they are op:is-same-node().
 */
struct PFla_pair_t
PFbui_op_except (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    PFla_op_t *difference;

    (void) loop; (void) side_effects;

    difference = difference (
                     distinct (project (args[0].rel,
                                        proj (col_iter, col_iter),
                                        proj (col_item, col_item))),
                     project (args[1].rel,
                              proj (col_iter, col_iter),
                              proj (col_item, col_item)));

    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (difference,
                         col_pos, sortby (col_item)),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = rowid (difference, col_pos),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
}

/* ------------------------- */
/* 15.4. Aggregate Functions */
/* ------------------------- */

/**
 * Build up operator tree for built-in function 'fn:count'.
 *
 *                env,loop: e => (q,delta)
 *  -------------------------------------------------------------------
 *                         env,loop: fn:count(e) =>
 *  //                             \     //                  \    item\\    pos
 * ||count_item/iter (proj_iter (q))| U ||loop \ proj_iter (q)| X ---- || X ---,
 *  \\                             /     \\                  /      0 //     1
 *                                    ()
 */
struct PFla_pair_t
PFbui_fn_count (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *count = simple_aggr (
                           project (args[0].rel,
                                    proj (col_iter, col_iter)),
                           col_iter,
                           alg_aggr_count,
                           col_item,
                           col_NULL);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    count,
                    attach (
                        difference (
                            loop,
                            project (count, proj (col_iter, col_iter))),
                        col_item, lit_int (0))),
                col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in aggregate functions 'fn:avg ($arg)',
 * 'fn:min ($arg)' and 'fn:max ($arg)'
 *
 *                env,loop: e => (q,delta)
 *  -------------------------------------------------------------------
 *                         env,loop: fn:___(e) =>
 *  //                                                     \
 * ||aggr_item:(item)/iter (proj_iter,item cast_item,t(q))) | @pos(0)
 *  \\                                                     /
 *                                ()
 */
static struct PFla_pair_t
fn_aggr (PFalg_simple_type_t t, PFalg_aggr_kind_t kind,
         const PFla_op_t *loop,
         bool ordering,
         PFla_op_t **side_effects,
         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = attach(simple_aggr (
                          project (cast(args[0].rel, col_cast, col_item, t),
                                   proj (col_iter, col_iter),
                                   proj (col_item, col_cast)),
                          col_iter,
                          kind,
                          col_item,
                          col_item),
                      col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:avg ($arg)'.
 */
struct PFla_pair_t
PFbui_fn_avg (const PFla_op_t *loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, alg_aggr_avg, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:max (string*)'.
 */
struct PFla_pair_t
PFbui_fn_max_str (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_str, alg_aggr_max, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:max (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_max_int (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_int, alg_aggr_max, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:max (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_max_dec (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dec, alg_aggr_max, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:max (double*)'.
 */
struct PFla_pair_t
PFbui_fn_max_dbl (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, alg_aggr_max, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:min (string*)'.
 */
struct PFla_pair_t
PFbui_fn_min_str (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_str, alg_aggr_min, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:min (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_min_int (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_int, alg_aggr_min, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:min (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_min_dec (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dec, alg_aggr_min, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:min (double*)'.
 */
struct PFla_pair_t
PFbui_fn_min_dbl (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, alg_aggr_min, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum ($arg)'.
 *
 *                env,loop: e => (q,delta)
 *  -------------------------------------------------------------------
 *                         env,loop: fn:sum(e) =>
 *  //                                                     \
 * ||sum_item:(item)/iter (proj_iter,item (cast_item,t(q))) |
 *  \\                                                     /
 *                         U
 *  //                  \    item\\    pos
 * ||loop \ proj_iter (q)| X ---- || X ---,
 *  \\                  /      0 //     1
 *                                ()
 */
static struct PFla_pair_t
fn_sum (PFalg_simple_type_t t,
        const PFla_op_t *loop,
        bool ordering,
        PFla_op_t **side_effects,
        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *sum = simple_aggr (
                         project (cast(args[0].rel, col_cast, col_item, t),
                                 proj (col_iter, col_iter),
                                 proj (col_item, col_cast)),
                         col_iter,
                         alg_aggr_sum,
                         col_item,
                         col_item);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    sum,
                    attach (
                        difference (
                            loop,
                            project (sum, proj (col_iter, col_iter))),
                        col_item, lit_int (0))),
                col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

static struct PFla_pair_t
fn_prod (PFalg_simple_type_t t,
        const PFla_op_t *loop,
        bool ordering,
        PFla_op_t **side_effects,
        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *prod = simple_aggr (
                          project (cast(args[0].rel, col_cast, col_item, t),
                                  proj (col_iter, col_iter),
                                  proj (col_item, col_cast)),
                          col_iter,
                          alg_aggr_prod,
                          col_item,
                          col_item);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    prod,
                    attach (
                        difference (
                            loop,
                            project (prod, proj (col_iter, col_iter))),
                        col_item, lit_int (1))),
                col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:sum (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_int (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_int, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_dec (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_dec, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum (double*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_dbl (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_dbl, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'pf:prod (double*)'.
 */
struct PFla_pair_t
PFbui_fn_prod_dbl (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    return fn_prod(aat_dbl, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum ($arg, $zero)'.
 *
 *  env,loop: e1 => (q1,delta1)             env,loop: e2 => (q2,delta2)
 *  -------------------------------------------------------------------
 *                         env,loop: fn:sum(e1, e2) =>
 *  //                                                      \
 * ||sum_item:(item)/iter (proj_iter,item (cast_item,t(q1))) |
 *  \\                                                      /
 *                                 U
 *  / /                   \
 * | |loop \ proj_iter (q1)| |X|(iter, iter1)
 *  \ \                   /
 *
 *    /                                     \ \     pos
 *   |proj_iter1:iter,item (cast_item,t(q2)) | | X  ---,
 *    \                                     / /      1
 *
 *                                ()
 */
static struct PFla_pair_t
fn_sum_zero (PFalg_simple_type_t t,
             const PFla_op_t *loop,
             bool ordering,
             PFla_op_t **side_effects,
             struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    PFla_op_t *sum = simple_aggr (
                         project (cast (args[0].rel, col_cast, col_item, t),
                                  proj (col_iter, col_iter),
                                  proj (col_item, col_cast)),
                         col_iter,
                         alg_aggr_sum,
                         col_item,
                         col_item);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    sum,
                    project (
                         eqjoin (
                              difference (
                                   loop,
                                   project (sum, proj (col_iter, col_iter))),
                              project (cast(args[1].rel, col_cast, col_item, t),
                                       proj (col_iter1, col_iter),
                                       proj (col_item, col_cast)),
                              col_iter, col_iter1),
                         proj (col_iter, col_iter),
                         proj (col_item, col_item))),
                col_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:sum (integer*, integer?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_int (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_int, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum (decimal*, decimal?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_dec (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_dec, loop, ordering, side_effects, args);
}

/**
 * Build up operator tree for built-in function 'fn:sum (double*, double?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_dbl (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_dbl, loop, ordering, side_effects, args);
}

/* ----------------------------------------------------- */
/* 15.5. Functions and Operators that Generate Sequences */
/* ----------------------------------------------------- */

/**
 * Build up operator tree for built-in function 'op:to (int, int)'.
 */
struct PFla_pair_t
PFbui_op_to (const PFla_op_t *loop,
             bool ordering,
             PFla_op_t **side_effects,
             struct PFla_pair_t *args)
{
    (void) loop; (void) side_effects;
    (void) ordering; (void) side_effects;

    PFla_op_t *to = project (
                        to (eqjoin (
                                project (
                                    args[0].rel,
                                    proj (col_iter, col_iter),
                                    proj (col_item, col_item)),
                                project (
                                    args[1].rel,
                                    proj (col_iter1, col_iter),
                                    proj (col_item1, col_item)),
                                col_iter,
                                col_iter1),
                            col_res,
                            col_item,
                            col_item1),
                        proj (col_iter, col_iter),
                        proj (col_item, col_res));

    return (struct PFla_pair_t) {
        .rel = rank (
                   to,
                   col_pos,
                   sortby (col_item)),
        .frag = PFla_empty_set () };
}


static struct PFla_pair_t
fn_id (const PFla_op_t *loop,
       bool ordering,
       PFla_op_t **side_effects,
       struct PFla_pair_t *args, bool id)
{
    PFla_op_t *in, *op, *doc;

    (void) loop; (void) side_effects;
    (void) ordering; (void) side_effects;

    in = project (
             eqjoin (
                 project (
                     args[0].rel,
                     proj (col_iter, col_iter),
                     proj (col_item, col_item)),
                 project (
                     args[1].rel,
                     proj (col_iter1, col_iter),
                     proj (col_item1, col_item)),
                 col_iter,
                 col_iter1),
             proj (col_iter, col_iter),
             proj (col_item, col_item),
             proj (col_item1, col_item1));

    doc = PFla_set_to_la (args[1].frag);

    op = project (
             doc_index_join (doc, in,
                             id ? la_dj_id : la_dj_idref,
                             col_item, col_res, col_item1, "*", "*", "*", "*"),
             proj (col_iter, col_iter),
             proj (col_item, col_res));

    if (ordering)
        return (struct PFla_pair_t) {
            .rel = rank (distinct (op), col_pos, sortby (col_item)),
            .frag = args[1].frag };
    else
        return (struct PFla_pair_t) {
            .rel = rowid (distinct (op), col_pos),
            .frag = args[1].frag };
}


/**
 * Build up operator tree for built-in function 'fn:id (string*, node)'.
 */
struct PFla_pair_t
PFbui_fn_id (const PFla_op_t *loop,
             bool ordering,
             PFla_op_t **side_effects,
             struct PFla_pair_t *args)
{
    return fn_id (loop, ordering, side_effects, args, true);
}


/**
 * Build up operator tree for built-in function 'fn:idref (string*, node)'.
 */
struct PFla_pair_t
PFbui_fn_idref (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args)
{
    return fn_id (loop, ordering, side_effects, args, false);
}


/**
 * Built-in function <code>fn:doc(xs:string?)</code>: Return root
 * node of document that is found under given URI. Return empty
 * sequence if the argument is the empty sequence.
 *
 * The latter (the ``empty sequence'') case is actually nice for
 * us. No need to access the loop relation in any way.
 */
struct PFla_pair_t
PFbui_fn_doc (const PFla_op_t *loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    PFla_op_t *doc = doc_tbl (args[0].rel, col_res, col_item, alg_dt_doc);

    /* Check if all documents are available and trigger an error otherwise. */

    /* check for the existence of the document ... */
    PFla_op_t *error = project (
                           select_ (
                               not (
                                   fun_1to1 (
                                       args[0].rel,
                                       alg_fun_fn_doc_available,
                                       col_res,
                                       collist (col_item)),
                                   col_item2,
                                   col_res),
                               col_item2),
                           proj (col_item, col_item));

    /* ... stitch together a meaningful error message ... */
    error = fun_1to1 (
                fun_1to1 (
                    attach (
                        attach (
                            error,
                            col_item2,
                            lit_str ("err:FODC0002, Error retrieving resource"
                                     " (no such document \"")),
                        col_item3,
                        lit_str ("\").")),
                    alg_fun_fn_concat,
                    col_res,
                    collist (col_item2, col_item)),
                alg_fun_fn_concat,
                col_res1,
                collist (col_res, col_item3));

    /* ... and add an error check to the list of side effects */
    *side_effects = error (
                        *side_effects,
                        error,
                        col_res1);

    return (struct PFla_pair_t) {
        .rel  = project (roots (doc),
                         proj (col_iter, col_iter),
                         proj (col_pos, col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_set (fragment (doc)) };
}

/*
 *  function fn:doc-available (string?) as boolean
 */
struct PFla_pair_t
PFbui_fn_doc_available (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       disjunion (
                           args[0].rel,
                           attach (
                               attach (
                               difference (
                                   loop,
                                   project (
                                       args[0].rel,
                                       proj (col_iter, col_iter))),
                               col_pos, lit_nat (1)),
                           col_item, lit_str (""))),
                       alg_fun_fn_doc_available,
                       col_res,
                       collist(col_item)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/*
 *  function fn:collection (string) as node*
 */
struct PFla_pair_t
PFbui_fn_collection (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects; (void) loop; (void) side_effects;

    /* collection root nodes */
    PFla_op_t *doc = doc_tbl (args[0].rel, col_res, col_item, alg_dt_col);

    struct PFla_pair_t p = {
        .rel  = roots (doc),
        .frag = PFla_set (fragment (doc)) };

    /* child step */
    struct PFalg_step_spec_t spec = {
        .axis = alg_chld,
        .kind = node_kind_node,
        .qname = PFqname (PFns_wild, NULL) };

    PFla_op_t *step = PFla_project (
                          PFla_step_join_simple (
                              PFla_set_to_la (p.frag),
                              p.rel,
                              spec,
                              col_res,
                              col_item1),
                          PFalg_proj (col_iter, col_iter),
                          PFalg_proj (col_item, col_item1));

    return (struct  PFla_pair_t) {
                   .rel = rank (step, col_pos, sortby (col_item)),
                   .frag = p.frag };
}

/* --------------------- */
/* 16. CONTEXT FUNCTIONS */
/* --------------------- */
/* ----------------- */
/* 16.1. fn:position */
/* ----------------- */
/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* this function has been already replaced during core rewriting */
/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

/* ------------- */
/* 16.2. fn:last */
/* ------------- */
/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */
/* this function has been already replaced during core rewriting */
/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - */

/* ---------------------------------------- */
/* #1. PATHFINDER SPECIFIC HELPER FUNCTIONS */
/* ---------------------------------------- */

/**
 * Algebra implementation for op:or (boolean, boolean) as boolean
 * @see bin_op()
 */
struct PFla_pair_t
PFbui_op_or_bln (const PFla_op_t *loop,
                 bool ordering,
                 PFla_op_t **side_effects,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = bin_op (aat_bln,
                             PFla_or,
                             args[0].rel,
                             args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:and (boolean, boolean) as boolean
 * @see bin_op()
 */
struct PFla_pair_t
PFbui_op_and_bln (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *res = bin_op (aat_bln,
                             PFla_and,
                             args[0].rel,
                             args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for pragma pf:cache.
 */
struct PFla_pair_t
PFbui_pf_query_cache (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering;

    /* We do forbidden side effects here --- They however don't harm us. */
    PFprop_infer_card ((PFla_op_t *) loop);

    /* If we are inside an iteration discard caching. */
    if (PFprop_card(loop->prop) != 1)
        return args[1];

    /* Otherwise introduce a side effect for the caching and replace
       the query by a cache lookup. */
    else {
        PFalg_simple_type_t ty = PFprop_type_of (args[1].rel, col_item);
        char *id;
        /* extract the id information */
        PFprop_infer_const (args[0].rel);
        assert (PFprop_const (args[0].rel->prop, col_item));
        id = (PFprop_const_val (args[0].rel->prop, col_item)).val.str;

        /* Provide a (possibly) new cache as side effect. */
        *side_effects = cache (
                            *side_effects,
                            args[1].rel,
                            id,
                            col_pos,
                            col_item);

        /* Use a cache lookup as the replacement. */
        PFla_op_t *res = fun_call (loop,                   /* loop relation  */
                                   fun_param (args[0].rel, /* query cache id */
                                              nil(),
                                              ipi_schema(aat_str)),
                                   ipi_schema(ty),         /* i|p|i schema   */
                                   alg_fun_call_cache,     /* function kind  */
                                   PFqname (PFns_wild, NULL), /* qname       */
                                   NULL,                   /* ctx            */
                                   col_iter,               /* iter           */
                                   alg_occ_unknown); /* occurrence indicator */

        return (struct PFla_pair_t) {
            .rel = res,
            .frag = (ty & aat_node) 
                  ? PFla_set (frag_extract (res, 2))
                  : PFla_empty_set () };
    }
}

/**
 * The fs:distinct-doc-order function sorts its input sequence of
 * nodes by document order and removes duplicates.
 */
struct PFla_pair_t
PFbui_pf_distinct_doc_order (const PFla_op_t *loop,
                             bool ordering,
                             PFla_op_t **side_effects,
                             struct PFla_pair_t *args)
{
    PFla_op_t *distinct = distinct (
                              project (args[0].rel,
                                   proj (col_iter, col_iter),
                                   proj (col_item, col_item)));

    (void) loop; (void) side_effects;

    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         col_pos, sortby (col_item)),
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = rowid (distinct, col_pos),
            .frag = args[0].frag };
}

/**
 *
 * Th fs:item-sequence-to-node-sequence function converts a sequence
 * of item values to nodes (see FS, Section 6.1.6).
 *
 * intput:
 *          iter | pos | item
 *         -------------------
 *               |     | "a"
 *               |     | 42
 *               |     | <foo/>
 *               |     | 1.2
 *
 * - if necessary select those rows that have type "node" (part1)
 * - select the remaining rows (atomic values in part2)
 * - convert all atomic values into strings
 * - concatenate consecutive strings by putting a space between them;
 *   (e.g. "a" . " " . "42");
 *   or add whitespace strings for consecutive strings
 * - create text nodes from the (concatenated) strings;
 * - add the new fragment of text nodes (frag) to the .frag field
 *   together with those nodes we had in the very beginning (those
 *   in part1, e.g. <foo/>)
 * - if necessary combine textnodes with other nodes (part1)
 * - create new position values to restore the original sort order
 */
static struct PFla_pair_t
pf_item_seq_to_node_seq_worker_single_atomic (const PFla_op_t *loop,
                                              const PFla_op_t *rel,
                                              PFla_set_t *frag)
{
    (void) loop;

    /*
     * convert item into string and construct a textnode out of it
     */
    PFla_op_t *t_nodes = twig (textnode (
                                   cast (rel,
                                         col_cast,
                                         col_item,
                                         aat_str),
                                   col_iter, col_cast),
                               col_iter, col_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (roots (t_nodes), col_pos, lit_nat (1)),
                 /* union of those nodes we had in the very beginning
                  * (those in frag) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_single_atomic
    (const PFla_op_t *loop,
     bool ordering,
     PFla_op_t **side_effects,
     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return pf_item_seq_to_node_seq_worker_single_atomic (loop,
                                                         args[0].rel,
                                                         args[0].frag);
}

static struct PFla_pair_t
pf_item_seq_to_node_seq_worker_atomic (const PFla_op_t *loop,
                                       const PFla_op_t *rel,
                                       PFla_set_t *frag)
{
    /*
     * convert all items into strings and concatenate them with
     * a single whitespace
     */
    PFla_op_t *strings = fn_string_join (
                             project (
                                 cast (rel,
                                       col_cast,
                                       col_item,
                                       aat_str),
                                 proj (col_iter, col_iter),
                                 proj (col_pos, col_pos),
                                 proj (col_item, col_cast)),
                             project (
                                 attach (
                                     attach (loop, col_pos, lit_nat (1)),
                                     col_item, lit_str (" ")),
                                 proj (col_iter, col_iter),
                                 proj (col_item, col_item)),
                         col_iter, col_pos, col_item,
                         col_iter, col_item,
                         col_iter, col_item);

    PFla_op_t *t_nodes = twig (textnode (
                                   strings,
                                   col_iter, col_item),
                               col_iter, col_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (roots (t_nodes), col_pos, lit_nat (1)),
                 /* union of those nodes we had in the very beginning
                  * (those in part1) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_atomic
    (const PFla_op_t *loop,
     bool ordering,
     PFla_op_t **side_effects,
     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return pf_item_seq_to_node_seq_worker_atomic (loop,
                                                  args[0].rel,
                                                  args[0].frag);
}

static struct PFla_pair_t
pf_item_seq_to_node_seq_worker_attr (
        const PFla_op_t *loop, const PFla_op_t *rel, PFla_set_t *frag,
        struct PFla_pair_t (*fun) (const PFla_op_t *loop,
                                   const PFla_op_t *rel,
                                   PFla_set_t *frag))
{
    /*
     * carry out specific type test on type anode (attr/afrag)
     */
    PFla_op_t *type = type (rel, col_res, col_item, aat_anode);

    /* select those rows that have type "attr" (part1) */
    PFla_op_t *part1 = project (
                           type_assert_pos (
                               select_ (type, col_res),
                               col_item, aat_anode),
                           proj (col_iter, col_iter),
                           proj (col_pos, col_pos),
                           proj (col_item, col_item));

    /* select the remaining rows (part2) */
    PFla_op_t *part2 = project (
                           type_assert_neg (
                               select_ (not (type, col_res1, col_res),
                                        col_res1),
                               col_item, aat_anode),
                           proj (col_iter, col_iter),
                           proj (col_pos, col_pos),
                           proj (col_item, col_item));

    /* call the translation of the atomics with only the
       atomic values (part2) */
    struct PFla_pair_t text = fun (loop, part2, frag);

    /* get the roots of the new text nodes, form union of roots and
     * part1, and sort result on col_ord and col_pos column
     */
    return (struct  PFla_pair_t) {
                 .rel = project (
                            rank (
                                disjunion (
                                    attach (part1, col_ord, lit_nat (1)),
                                    attach (text.rel, col_ord, lit_nat (2))),
                                col_pos1, sortby (col_ord, col_pos)),
                            proj (col_iter, col_iter),
                            proj (col_pos, col_pos1),
                            proj (col_item, col_item)),
                 /* fill in frag union generated in the textnode
                  * generation for atomic values (fun).
                  */
                 .frag = text.frag };
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_attr_single
    (const PFla_op_t *loop,
     bool ordering,
     PFla_op_t **side_effects,
     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return pf_item_seq_to_node_seq_worker_attr (
               loop, args[0].rel, args[0].frag,
               pf_item_seq_to_node_seq_worker_single_atomic);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_attr
    (const PFla_op_t *loop,
     bool ordering,
     PFla_op_t **side_effects,
     struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return pf_item_seq_to_node_seq_worker_attr (
               loop, args[0].rel, args[0].frag,
               pf_item_seq_to_node_seq_worker_atomic);
}

static struct PFla_pair_t
pf_item_seq_to_node_seq_worker (struct PFla_pair_t *args,
                                PFalg_simple_type_t split_type)
{
    /* assign correct row numbers as we need them later
       for finding adjacent textnodes */
    PFla_op_t *input = adjust_positions (args[0].rel);

    /*
     * carry out specific type test on type split_type
     * (either aat_pnode or aat_node)
     */
    PFla_op_t *type = type (input, col_res, col_item, split_type);

    /* select those rows that have type "node" (part1) */
    PFla_op_t *part1 = project (
                           type_assert_pos (
                               select_ (type, col_res),
                               col_item, split_type),
                           proj (col_iter, col_iter),
                           proj (col_pos, col_pos),
                           proj (col_item, col_item));

    /* select the remaining rows (part2) */
    PFla_op_t *part2 = project (
                           type_assert_neg (
                               select_ (not (type, col_res1, col_res),
                                        col_res1),
                               col_item, split_type),
                           proj (col_iter, col_iter),
                           proj (col_pos, col_pos),
                           proj (col_item, col_item));

    /*
     * convert all items in part2 into strings ...
     */
    PFla_op_t *strings = cast (part2, col_cast, col_item, aat_str);

    /*
     * compare columns pos and pos-1 to find adjacent strings
     */
    PFla_op_t *base = fun_1to1 (
                          cast (
                              attach (strings, col_item1, lit_int (1)),
                              col_pos1, col_pos, aat_int),
                          alg_fun_num_subtract,
                          col_res,
                          collist (col_pos1, col_item1));

    PFla_op_t *delim = project (
                           select_ (
                               eq (eqjoin (
                                       project (
                                           base,
                                           proj (col_iter1, col_iter),
                                           proj (col_res, col_res)),
                                       project (
                                           base,
                                           proj (col_iter, col_iter),
                                           proj (col_pos1, col_pos1),
                                           proj (col_pos, col_pos)),
                                       col_iter1, col_iter),
                                   col_res1, col_res, col_pos1),
                               col_res1),
                           proj (col_iter, col_iter),
                           proj (col_pos, col_pos));

    /*
     * for each pair of adjacent strings add a whitespace string
     */
    PFla_op_t *sep = attach (
                         attach (delim, col_item, lit_str (" ")),
                         col_ord, lit_nat (2));

    /*
     * create textnodes for each string
     * (casted atomic values and whitespace strings)
     */
    PFla_op_t *unq_strings = rowid (
                                 disjunion (
                                     attach (project (strings,
                                                     proj (col_iter, col_iter),
                                                     proj (col_pos, col_pos),
                                                     proj (col_item, col_cast)),
                                            col_ord, lit_nat (1)),
                                     sep),
                                 col_inner);

    PFla_op_t *t_nodes = twig (textnode (unq_strings, col_inner, col_item),
                               col_iter, col_item);

    /* get the roots of the new text nodes, form union of roots and
     * part1, and sort result on col_pos and col_ord column
     */
    return (struct  PFla_pair_t) {
                 .rel = project (
                            rank (
                                disjunion (
                                    project (
                                        eqjoin (
                                            roots (t_nodes),
                                            project (
                                                unq_strings,
                                                proj (col_outer, col_iter),
                                                proj (col_inner, col_inner),
                                                proj (col_pos, col_pos),
                                                proj (col_ord, col_ord)),
                                            col_iter, col_inner),
                                        proj (col_iter, col_outer),
                                        proj (col_pos, col_pos),
                                        proj (col_item, col_item),
                                        proj (col_ord, col_ord)),
                                    attach (part1, col_ord, lit_nat (1))),
                                col_pos1, sortby (col_pos, col_ord)),
                            proj (col_iter, col_iter),
                            proj (col_pos, col_pos1),
                            proj (col_item, col_item)),
                 /* union of those nodes we had in the very beginning
                  * (those in part1) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (args[0].frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_wo_attr
    (const PFla_op_t *loop,
     bool ordering,
     PFla_op_t **side_effects,
     struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /*
     * translate is2ns function using its worker
     */
    return pf_item_seq_to_node_seq_worker (args, aat_pnode);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq (const PFla_op_t *loop,
                               bool ordering,
                               PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    /*
     * translate is2ns function using its worker
     */
    return pf_item_seq_to_node_seq_worker (args, aat_node);
}

/**
 * Merge adjacent textnodes into one text node and delete empty text
 * nodes.
 *
 * Input: iter | pos | item table where all items are of type node.
 * Introduce new algebra operator which takes the current document and
 * the current algebra representation. It merges consecutive text nodes
 * (with same col_iter and consecutive col_pos values). If a text node
 * is empty, it is discarded.
 * The output are an algebra representation of all nodes (old and new,
 * i.e. unmerged and merged) and a fragment representation of the newly
 * created nodes only.
 */
struct PFla_pair_t
PFbui_pf_merge_adjacent_text_nodes (
        const PFla_op_t *loop,
        bool ordering,
        PFla_op_t **side_effects,
        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *merged
        = merge_adjacent (PFla_set_to_la (args[0].frag), args[0].rel,
                          col_iter, col_pos, col_item,
                          col_iter, col_pos, col_item);

    return (struct  PFla_pair_t) {
                 .rel  = roots (merged),
                 /* form union of old and new fragment */
                 .frag = PFla_set_union (args[0].frag,
                                         PFla_set (fragment (merged))) };
}

/**
 * Built-in function #pf:typed-value(attr="text").
 */
struct PFla_pair_t
PFbui_pf_string_value_attr (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     col_res, col_item, doc_atext),
                         proj (col_iter, col_iter),
                         proj (col_pos,  col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(text {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_text (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     col_res, col_item, doc_text),
                         proj (col_iter, col_iter),
                         proj (col_pos,  col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(processing-instruction {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_pi (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     col_res, col_item, doc_pi_text),
                         proj (col_iter, col_iter),
                         proj (col_pos,  col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(comment {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_comm (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     col_res, col_item, doc_comm),
                         proj (col_iter, col_iter),
                         proj (col_pos,  col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(<code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem (const PFla_op_t *loop,
                            bool ordering,
                            PFla_op_t **side_effects,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel  = project (
                    doc_access (
                        PFla_set_to_la (args[0].frag),
                        args[0].rel,
                        col_res, col_item, doc_atomize),
                    proj (col_iter, col_iter),
                    proj (col_pos,  col_pos),
                    proj (col_item, col_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(attribute foo {"text"}, <code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem_attr (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args)
{
    PFla_op_t *sel_attr, *sel_node, *attributes, *nodes;

    /* we know that we have no empty sequences and
       thus can skip the treating for empty sequences */
    (void) loop; (void) ordering; (void) side_effects;

    /* select all attributes and retrieve their string values */
    sel_attr = project (
                   type_assert_pos (
                       select_ (
                           type (args[0].rel, col_subty,
                                 col_item, aat_anode),
                           col_subty),
                       col_item, aat_anode),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_item));

    attributes = project (doc_access (PFla_set_to_la (args[0].frag),
                          sel_attr, col_res, col_item, doc_atext),
                          proj (col_iter, col_iter),
                          proj (col_pos, col_pos),
                          proj (col_item, col_res));

    /* select all other nodes and retrieve string values
       as in PFbui_pf_string_value_elem */
    sel_node = project (
                   type_assert_neg (
                       select_ (
                           not (type (args[0].rel, col_subty,
                                      col_item, aat_anode),
                                col_notsub, col_subty),
                           col_notsub),
                       col_item, aat_anode),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_item));

    nodes = project (
                doc_access (
                    PFla_set_to_la (args[0].frag),
                    sel_node,
                    col_res, col_item, doc_atomize),
                proj (col_iter, col_iter),
                proj (col_pos,  col_pos),
                proj (col_item, col_res));

    return (struct PFla_pair_t) {
        .rel  = disjunion (attributes, nodes),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function '#pf:string-value'.
 */
struct PFla_pair_t
PFbui_pf_string_value (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    /* We cannot cope with a mix of certain node kinds.
       E.g. comments and element nodes are not distinguishable
       in the algebra but still require a different treating in
       the backend.
       One solution could be to get all nodes with
       desc-or-self::text() U attribute::* U self::pi() U self::comment
       and then have a universal doc_access operator kind that
       can cope with all these kinds.

       The nicest solution however seems to be C primitive in
       the runtime that implements string-value. (This would make
       the algebra a lot easier to read and optimize.) */
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`#pf:string-value' is missing.");

    (void) loop; (void) ordering; (void) side_effects;

    return args[0];
}

/* ----------------------------------------------------- */
/* #2. PATHFINDER SPECIFIC DOCUMENT MANAGEMENT FUNCTIONS */
/* ----------------------------------------------------- */

/**
 * Build in function fn:put(node, xs:string) as empty-sequence()
 */
struct PFla_pair_t 
PFbui_fn_put (const PFla_op_t *loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args)
{
    (void) ordering, (void) loop; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = NULL,
        .frag = args[0].frag };
}

/**
 *  Build in function pf:documents() as element()*
 */
struct PFla_pair_t 
PFbui_pf_documents (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects; (void) args;

    PFla_op_t *res = fun_call (loop,                       /* loop relation */
                               nil(),                      /* param_list    */
                               ii_schema(aat_pnode),    /* iter_item schema */
                               alg_fun_call_pf_documents,  /* function kind */
                               PFqname (PFns_wild, NULL),  /* qname         */
                               NULL,                       /* ctx           */
                               col_iter,                   /* iter          */
                               alg_occ_unknown);    /* occurrence indicator */

    return (struct PFla_pair_t) {
        .rel = rank (res,
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_set (frag_extract (res, 1)) };
}

/**
 *  Build in function pf:documents-unsafe() as element()*
 */
struct PFla_pair_t
PFbui_pf_documents_unsafe (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects; (void) args;

    PFla_op_t *res = fun_call (loop,
                               nil(),
                               ii_schema(aat_pnode),
                               alg_fun_call_pf_documents_unsafe,
                               PFqname (PFns_wild, NULL),
                               NULL,
                               col_iter,
                               alg_occ_unknown);

    return (struct PFla_pair_t) {
        .rel = rank (res,
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_set (frag_extract (res, 1)) };
}

/**
 *  Build in function pf:documents(string) as element()*
 */
struct PFla_pair_t
PFbui_pf_documents_str (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = rank (fun_call (loop,
                               fun_param (args[0].rel,
                                          nil(),
                                          ipi_schema(aat_str)),
                               ii_schema(aat_pnode),
                               alg_fun_call_pf_documents_str,
                               PFqname (PFns_wild, NULL),
                               NULL,
                               col_iter,
                               alg_occ_unknown),
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_empty_set() };
}

/**
 *  Build in function pf:documents-unsafe(string) as element()*
 */
struct PFla_pair_t
PFbui_pf_documents_str_unsafe (const PFla_op_t *loop,
                               bool ordering,
                               PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = rank (fun_call (loop,
                               fun_param (args[0].rel,
                                          nil(),
                                          ipi_schema(aat_str)),
                               ii_schema(aat_pnode),
                               alg_fun_call_pf_documents_str_unsafe,
                               PFqname (PFns_wild, NULL),
                               NULL,
                               col_iter,
                               alg_occ_unknown),
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_empty_set() };
}

/**
 *  Build in function pf:docname(node*) as string*
 */
struct PFla_pair_t
PFbui_pf_docname (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (args[0].rel,
                                  alg_fun_pf_docname,
                                  col_res,
                                  collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 *  Build in function pf:collection(string) as node
 */
struct PFla_pair_t
PFbui_pf_collection (const PFla_op_t *loop,
                     bool ordering,
                     PFla_op_t **side_effects,
                     struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *doc = doc_tbl (args[0].rel, col_res, col_item, alg_dt_col);

    return (struct PFla_pair_t) {
        .rel  = project (roots (doc),
                         proj (col_iter, col_iter),
                         proj (col_pos, col_pos),
                         proj (col_item, col_res)),
        .frag = PFla_set (fragment (doc)) };
}

/**
 *  Build in function pf:collections() as element()*
 */
struct PFla_pair_t
PFbui_pf_collections (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects; (void) args;

    PFla_op_t *res = fun_call (loop,
                               nil(),
                               ii_schema(aat_pnode),
                               alg_fun_call_pf_collections,
                               PFqname (PFns_wild, NULL),
                               NULL,
                               col_iter,
                               alg_occ_unknown);

    return (struct PFla_pair_t) {
        .rel = rank (res,
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_empty_set() };

}

/**
 *  Build in function pf:collections-unsafe() as element()*
 */
struct PFla_pair_t
PFbui_pf_collections_unsafe (const PFla_op_t *loop,
                             bool ordering,
                             PFla_op_t **side_effects,
                             struct PFla_pair_t *args)
{
    (void) ordering; (void) side_effects; (void) args;

    PFla_op_t *res = fun_call (loop,
                               nil(),
                               ii_schema(aat_pnode),
                               alg_fun_call_pf_collections_unsafe,
                               PFqname (PFns_wild, NULL),
                               NULL,
                               col_iter,
                               alg_occ_unknown);

    return (struct PFla_pair_t) {
        .rel = rank (res,
                     col_pos,
                     sortby (col_item)),
        .frag = PFla_empty_set() };

}

/**
 * Build up operator tree for built-in function '#pf:fragment'.
 */
struct PFla_pair_t
PFbui_pf_fragment (const PFla_op_t *loop,
                   bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            args[0].rel,
                            alg_fun_pf_fragment,
                            col_res,
                            collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Build up operator tree for built-in function '#pf:attribute'.
 */
struct PFla_pair_t
PFbui_pf_attribute (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = rank (
                   distinct (
                       project (
                           doc_index_join (
                               PFla_set_to_la (args[0].frag),
                               eqjoin (project (args[0].rel,
                                                proj (col_iter, col_iter),
                                                proj (col_item, col_item)),
                                       project (args[5].rel,
                                                proj (col_iter1, col_iter),
                                                proj (col_item1, col_item)),
                                       col_iter,
                                       col_iter1), 
                                la_dj_attr,
                                col_item1,
                                col_res,
                                col_item,
                                ((struct PFla_op_t*) args[1].rel)->sem.attach.value.val.str,
                                ((struct PFla_op_t*) args[2].rel)->sem.attach.value.val.str,
                                ((struct PFla_op_t*) args[3].rel)->sem.attach.value.val.str,
                                ((struct PFla_op_t*) args[4].rel)->sem.attach.value.val.str),
                            proj (col_iter, col_iter),
                            proj (col_item, col_res))),
                   col_pos,
                   sortby (col_item)),
        .frag = args[0].frag };
}

/**
 * Build up operator tree for built-in function '#pf:text'.
 */
struct PFla_pair_t
PFbui_pf_text (const PFla_op_t *loop,
               bool ordering,
               PFla_op_t **side_effects,
               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = rank (
                   distinct (
                       project (
                           doc_index_join (
                               PFla_set_to_la (args[0].frag),
                               eqjoin (project (args[0].rel,
                                                proj (col_iter, col_iter),
                                                proj (col_item, col_item)),
                                       project (args[1].rel,
                                                proj (col_iter1, col_iter),
                                                proj (col_item1, col_item)),
                                       col_iter,
                                       col_iter1),
                                la_dj_text,
                                col_item1,
                                col_res,
                                col_item, "*", "*", "*", "*"),
                            proj (col_iter, col_iter),
                            proj (col_item, col_res))),
                   col_pos,
                   sortby (col_item)),
        .frag = args[0].frag };
}

/**
 * Build up operator tree for built-in function '#pf:supernode'.
 */
struct PFla_pair_t
PFbui_pf_supernode (const PFla_op_t *loop,
                    bool ordering,
                    PFla_op_t **side_effects,
                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            args[0].rel,
                            alg_fun_pf_supernode,
                            col_res,
                            collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),
        .frag = args[0].frag };
}

#ifdef HAVE_PFTIJAH
/* ----------------------------------------------------- */
/* #4. PFTIJAH SPECIFIC FUNCTIONS                        */
/* ----------------------------------------------------- */

static struct PFla_pair_t
pft_query_param0 (void)
{
    return (struct PFla_pair_t) {
        .rel  = nil(),
        .frag = PFla_empty_set () };
}

static struct PFla_pair_t
pft_query_param1 (struct PFla_pair_t *p1,PFalg_simple_type_t itemType)
{
    return (struct PFla_pair_t) {
        .rel  = fun_param(
                        p1->rel,
                        nil(),
                        ipi_schema(itemType)), 
        .frag = PFla_empty_set () };
}

static struct PFla_pair_t
pft_query_param2 (struct PFla_pair_t *p1, PFalg_simple_type_t itemType1,
                  struct PFla_pair_t *p2, PFalg_simple_type_t itemType2)
{
    return (struct PFla_pair_t) {
        .rel  = fun_param(
                        p1->rel,
                        fun_param(
                                p2->rel,
                                nil(),
                                ipi_schema(itemType2)), 
                        ipi_schema(itemType1)), 
        .frag = PFla_empty_set () };
}

static struct PFla_pair_t
pft_query_param3 (struct PFla_pair_t *p1, PFalg_simple_type_t itemType1,
                  struct PFla_pair_t *p2, PFalg_simple_type_t itemType2,
                  struct PFla_pair_t *p3, PFalg_simple_type_t itemType3)
{
    return (struct PFla_pair_t) {
        .rel  = fun_param(
                        p1->rel,
                        fun_param(
                                p2->rel,
                                fun_param(
                                        p3->rel,
                                        nil(),
                                        ipi_schema(itemType3)), 
                                ipi_schema(itemType2)), 
                        ipi_schema(itemType1)), 
        .frag = PFla_empty_set () };
}

/*
 * The main query function
 */

static struct PFla_pair_t
PFbui_tijah_query_HANDLER (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           char* query_name,
                           PFalg_simple_type_t funcall_t,
                           PFla_pair_t p_fun_param)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel =  fun_call(
                    loop,
                    p_fun_param.rel,
		    ((PFT_FUN_FTFUN(query_name)) ? 
		    	ipis_schema(funcall_t) : ipi_schema(funcall_t) ),
                    alg_fun_call_tijah,
                    PFqname (PFns_wild, query_name),
                    NULL, /* ctx */
                    col_iter, /* iter */
                    alg_occ_one_or_more  /* occ_ind */
                ),
        .frag = PFla_empty_set () };
}

/*
 * param creation helper functions
 */


/*
 * The 'id' returning functions
 */

struct PFla_pair_t
PFbui_tijah_query_i_xx (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_I_XX,
                aat_int,
                pft_query_param1(&args[0],aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_query_i_sx (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_I_SX,
                aat_int,
                pft_query_param2(&args[0],
                                 PFTIJAH_NODEKIND,
                                 &args[1],
                                 aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_query_i_xo (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_I_XO,
                aat_int,
                pft_query_param2(&args[0],
                                 aat_str,
                                 &args[1],
                                 PFTIJAH_NODEKIND)
                );
}

struct PFla_pair_t
PFbui_tijah_query_i_so (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_I_SO,
                aat_int,
                pft_query_param3(&args[0],
                                 PFTIJAH_NODEKIND,
                                 &args[1],
                                 aat_str,
                                 &args[2],
                                 PFTIJAH_NODEKIND)
                );
}

/*
 * The node returning functions
 */

struct PFla_pair_t
PFbui_tijah_query_n_xx (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_N_XX,
                PFTIJAH_NODEKIND,
                pft_query_param1(&args[0],
                                 aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_query_n_sx (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_N_SX,
                PFTIJAH_NODEKIND,
                pft_query_param2(&args[0],
                                 PFTIJAH_NODEKIND,
                                 &args[1],
                                 aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_query_n_xo (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_N_XO,
                PFTIJAH_NODEKIND,
                pft_query_param2(&args[0],
                                 aat_str,
                                 &args[1],
                                 PFTIJAH_NODEKIND)
                );
}

struct PFla_pair_t
PFbui_tijah_query_n_so (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_QUERY_N_SO,
                PFTIJAH_NODEKIND,
                pft_query_param3(&args[0],
                                 PFTIJAH_NODEKIND,
                                 &args[1],
                                 aat_str,
                                 &args[2],
                                 PFTIJAH_NODEKIND)
                );
}

/* The Fulltext fun stuff */

struct PFla_pair_t
PFbui_tijah_ftfun_b_sxx (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_FTFUN_B_SXX,
                aat_bln,
                pft_query_param2(&args[0],
                                 PFTIJAH_NODEKIND,
                                 &args[1],
                                 aat_str
				 )
                );
}

/*
 *
 */

struct PFla_pair_t
PFbui_tijah_manage_fti_HANDLER(
                const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                char* fun_name,
                PFla_pair_t p_fun_param)
{
    (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel =  fun_call(
                    loop,
                    p_fun_param.rel,
                    ipi_schema(DOCMGMTTYPE),
                    alg_fun_call_tijah,
                    PFqname (PFns_wild, fun_name),
                    NULL, /* ctx */
                    col_iter, /* iter */
                    alg_occ_one_or_more  /* occ_ind */
                ),
        .frag = PFla_empty_set () };
}

struct PFla_pair_t
PFbui_manage_fti_c_xx (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    (void)args;
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_C_XX,
                pft_query_param0()
           );
}

struct PFla_pair_t
PFbui_manage_fti_c_cx (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_C_CX,
                pft_query_param1(&args[0],
                                 aat_str)
           );
}

struct PFla_pair_t
PFbui_manage_fti_c_xo (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_C_XO,
                pft_query_param1(&args[0],
                                 PFTIJAH_NODEKIND)
           );
}

struct PFla_pair_t
PFbui_manage_fti_c_co (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_C_CO,
                pft_query_param2(&args[0],
                                 aat_str,
                                 &args[1],
                                 PFTIJAH_NODEKIND)
           );
}

struct PFla_pair_t
PFbui_manage_fti_e_cx (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_E_CX,
                pft_query_param1(&args[0],
                                 aat_str)
           );
}

struct PFla_pair_t
PFbui_manage_fti_e_co (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_E_CO,
                pft_query_param2(&args[0],
                                 aat_str,
                                 &args[1],
                                 PFTIJAH_NODEKIND)
           );
}

struct PFla_pair_t
PFbui_manage_fti_r_xx (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    (void) args;
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_R_XX,
                pft_query_param0()
           );
}

struct PFla_pair_t
PFbui_manage_fti_r_xo (const PFla_op_t *loop,
                       bool ordering,
                       PFla_op_t **side_effects,
                       struct PFla_pair_t *args)
{
    return PFbui_tijah_manage_fti_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_MANAGE_FTI_R_XO,
                pft_query_param1(&args[0],
                                 PFTIJAH_NODEKIND)
           );
}

/*
 *
 */

struct PFla_pair_t
PFbui_tijah_score (const PFla_op_t *loop,
                   bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_SCORE,
                aat_dbl,
                pft_query_param2(&args[0],
                                 aat_int,
                                 &args[1],
                                 PFTIJAH_NODEKIND)
                );
}

struct PFla_pair_t
PFbui_tijah_nodes (const PFla_op_t *loop,
                   bool ordering,
                   PFla_op_t **side_effects,
                   struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_NODES,
                PFTIJAH_NODEKIND,
                pft_query_param1(&args[0],
                                 aat_int)
                );
}

struct PFla_pair_t
PFbui_tijah_ft_index_info (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    (void) args;

    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_INFO,
                PFTIJAH_NODEKIND,
                pft_query_param0()
                );
}

struct PFla_pair_t
PFbui_tijah_ft_index_info_s (const PFla_op_t *loop,
                             bool ordering,
                             PFla_op_t **side_effects,
                             struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_INFO,
                PFTIJAH_NODEKIND,
                pft_query_param1(&args[0],
                                 aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_tokenize (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_TOKENIZE,
                aat_str,
                pft_query_param1(&args[0],
                                 aat_str)
                );
}

struct PFla_pair_t
PFbui_tijah_resultsize (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    return PFbui_tijah_query_HANDLER(
                loop,
                ordering,
                side_effects,
                PFT_RESSIZE,
                aat_int,
                pft_query_param1(&args[0],
                                 aat_int)
                );
}

#endif /* PFTIJAH */

/**
 * Built-in function pf:add-doc(string, string)
 */
struct PFla_pair_t
PFbui_pf_add_doc (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                           eqjoin (
                               eqjoin (
                                   args[0].rel,
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (args[1].rel,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            alg_fun_pf_add_doc_str,
                            col_res,
                            collist (col_item, col_item1, col_item2)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),

        .frag = args[0].frag };
}

/**
 * Built-in function pf:add-doc(string, string, string)
 */
struct PFla_pair_t
PFbui_pf_add_doc_str (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                           eqjoin (
                               eqjoin (
                                   args[0].rel,
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (args[2].rel,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            alg_fun_pf_add_doc_str,
                            col_res,
                            collist (col_item, col_item1, col_item2)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),

        .frag = args[0].frag };
}

/**
 * Built-in function pf:add-doc(string, string, int)
 */
struct PFla_pair_t
PFbui_pf_add_doc_int (const PFla_op_t *loop,
                      bool ordering,
                      PFla_op_t **side_effects,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               eqjoin (
                                   args[0].rel,
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (args[1].rel,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            project (args[2].rel,
                                     proj (col_iter3, col_iter),
                                     proj (col_item3, col_item)),
                            col_iter,
                            col_iter3),
                            alg_fun_pf_add_doc_str_int,
                            col_res,
                            collist (col_item,
                                     col_item1,
                                     col_item2,
                                     col_item3)),
                   proj(col_iter, col_iter),
                   proj(col_pos, col_pos),
                   proj(col_item, col_res)),

        .frag = args[0].frag };
}

/**
 * Built-in function pf:add-doc(string, string, string, int)
 */
struct PFla_pair_t
PFbui_pf_add_doc_str_int (const PFla_op_t *loop,
                          bool ordering,
                          PFla_op_t **side_effects,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (
                           eqjoin (
                               eqjoin (
                                   args[0].rel,
                                   project (args[1].rel,
                                            proj (col_iter1, col_iter),
                                            proj (col_item1, col_item)),
                                   col_iter,
                                   col_iter1),
                               project (args[2].rel,
                                        proj (col_iter2, col_iter),
                                        proj (col_item2, col_item)),
                               col_iter,
                               col_iter2),
                            project (args[3].rel,
                                     proj (col_iter3, col_iter),
                                     proj (col_item3, col_item)),
                            col_iter,
                            col_iter3),
                            alg_fun_pf_add_doc_str_int,
                            col_res,
                            collist (col_item,
                                     col_item1,
                                     col_item2,
                                     col_item3)),
                   proj(col_iter, col_iter),
                   proj(col_pos, col_pos),
                   proj(col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function pf:del-doc(string)
 */
struct PFla_pair_t
PFbui_pf_del_doc (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            args[0].rel,
                            alg_fun_pf_del_doc,
                            col_res,
                            collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),

        .frag = args[0].frag };
}

/**
 * Built-in function pf:nid(element) as string
 */
struct PFla_pair_t
PFbui_pf_nid (const PFla_op_t *loop,
              bool ordering,
              PFla_op_t **side_effects,
              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            args[0].rel,
                            alg_fun_pf_nid,
                            col_res,
                            collist (col_item)),
                        proj (col_iter, col_iter),
                        proj (col_pos, col_pos),
                        proj (col_item, col_res)),

        .frag = args[0].frag };
}

/* -------------------- */
/* #3. UPDATE FUNCTIONS */
/* -------------------- */

/**
 * Algebra implementation of updates functions. An update can be represented by
 * the same generic algebra operator (fun1to1) as any row based function
 * (e.g., the string functions). The mapping of multiple nodes to a target node
 * (e.g., the insert* functions) as well as the item reversal (needed for some
 * insert* functions) is be done during the translation from core to logical
 * algebra.
 */

/**
 * Built-in function upd:rename(node, QName)
 */
struct PFla_pair_t
PFbui_upd_rename (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_rename,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:delete(node)
 */
struct PFla_pair_t
PFbui_upd_delete (const PFla_op_t *loop,
                  bool ordering,
                  PFla_op_t **side_effects,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project ( fun_1to1 ( args[0].rel,
                                    alg_fun_upd_delete,
                                    col_res,
                                    collist (col_item)),
                         proj (col_iter, col_iter),
                         proj (col_pos, col_pos),
                         proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function
 * upd:insertIntoAsFirst(node, node*)
 * this should be node+ ...
 */
struct PFla_pair_t
PFbui_upd_insert_into_as_first (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;



    PFla_op_t *rev = project (rank (args[1].rel,
                                    col_pos1,
                                    PFord_refine (PFordering (),
                                                  col_pos,
                                                  DIR_DESC)),
                              proj (col_iter, col_iter),
                              proj (col_pos, col_pos1),
                              proj (col_item, col_item));

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (rev,
                                        proj (col_iter1, col_iter),
                                        proj (col_pos1, col_pos),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_insert_into_as_first,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos1),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function
 * upd:insertIntoAsLast(node, node*)
 * this should be node+ ...
 */
struct PFla_pair_t
PFbui_upd_insert_into_as_last (const PFla_op_t *loop,
                               bool ordering,
                               PFla_op_t **side_effects,
                               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_insert_into_as_last,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:insertBefore(node, node*)
 * this should be node+ ...
 */
struct PFla_pair_t
PFbui_upd_insert_before (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_insert_before,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:insertAfter(node, node*)
 * this should be node+ ...
 */
struct PFla_pair_t
PFbui_upd_insert_after (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    PFla_op_t *rev = project (rank (args[1].rel,
                                    col_pos1,
                                    PFord_refine (PFordering (),
                                                  col_pos,
                                                  DIR_DESC)),
                              proj (col_iter, col_iter),
                              proj (col_pos, col_pos1),
                              proj (col_item, col_item));

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (rev,
                                        proj (col_iter1, col_iter),
                                        proj (col_pos1, col_pos),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_insert_after,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos1),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:replaceValue(anyAttribute, untypedAtomic)
 */
struct PFla_pair_t
PFbui_upd_replace_value_att (const PFla_op_t *loop,
                             bool ordering,
                             PFla_op_t **side_effects,
                             struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_replace_value_att,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:replaceValue(text(), untypedAtomic)
 * Built-in function upd:replaceValue(processing-instr(), untypedAtomic)
 * Built-in function upd:replaceValue(comment(), untypedAtomic)
 */
struct PFla_pair_t
PFbui_upd_replace_value (const PFla_op_t *loop,
                         bool ordering,
                         PFla_op_t **side_effects,
                         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_replace_value,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:replaceElementContent(element(), text()?))
 */
struct PFla_pair_t
PFbui_upd_replace_element (const PFla_op_t *loop,
                           bool ordering,
                           PFla_op_t **side_effects,
                           struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_replace_element,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/**
 * Built-in function upd:replaceNode (node, node)
 */
struct PFla_pair_t
PFbui_upd_replace_node (const PFla_op_t *loop,
                        bool ordering,
                        PFla_op_t **side_effects,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; (void) side_effects;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (col_iter1, col_iter),
                                        proj (col_item1, col_item)),
                               col_iter,
                               col_iter1),
                       alg_fun_upd_replace_node,
                       col_res,
                       collist (col_item, col_item1)),
                   proj (col_iter, col_iter),
                   proj (col_pos, col_pos),
                   proj (col_item, col_res)),
        .frag = args[0].frag };
}

/* vim:set shiftwidth=4 expandtab: */
