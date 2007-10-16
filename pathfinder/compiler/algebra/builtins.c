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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

/* always include "pathfinder.h", first! */
#include "pathfinder.h"
#include "oops.h"

#include <assert.h>
#include <stdio.h>

#include "builtins.h"

#include "logical.h"
#include "logical_mnemonic.h"

/**
 * Assign correct row numbers in case we need the real values.
 */
static PFla_op_t *
adjust_positions (const PFla_op_t *n)
{
    return project (rownum (n, att_pos1, sortby (att_pos), att_iter),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos1),
                    proj (att_item, att_item));
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
                                 att_res,
                                 att_item, ty), 
                           att_res),
                    att_item, ty),
               proj (att_iter, att_iter),
               proj (att_pos, att_pos),
               proj (att_item, att_item));
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
        if (n->schema.items[i].name == att_item) {
            found = true;
            item_types = n->schema.items[i].type;
            break;
        }
    }
    if (!found)
        PFoops (OOPS_FATAL,
                "attribute `%s' referenced in typeswitch not found",
                PFatt_str (att_item));
    
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
        PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                          PFalg_att_t, PFalg_att_t),
        const PFla_op_t *n1,
        const PFla_op_t *n2)
{
    return project (OP (eqjoin (
                            project (cast (n1, 
                                           att_cast, att_item, t),
                                     proj (att_iter, att_iter),
                                     proj (att_pos, att_pos),
                                     proj (att_item, att_cast)),
                            project (cast (n2,
                                           att_cast, att_item, t),
                                     proj (att_iter1, att_iter),
                                     proj (att_item1, att_cast)),
                            att_iter,
                            att_iter1),
                        att_res, att_item, att_item1),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res));
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
                         PFalg_att_t,
                         PFalg_att_t),
       struct PFla_pair_t arg)
{
    return (struct PFla_pair_t) {
        .rel = project (OP (project (
                                cast (arg.rel,
                                      att_cast,
                                      att_item,
                                      t),
                                proj (att_iter, att_iter),
                                proj (att_pos, att_pos),
                                proj (att_item, att_cast)),
                            att_res,
                            att_item),
                        proj (att_iter, att_iter),
                        proj (att_pos, att_pos),
                        proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/* ------------ */
/* 2. ACCESSORS */
/* ------------ */
/* -------------- */
/* 2.3. fn:string */
/* -------------- */

/**
 * The fn:string function casts all values to string
 * It uses fn:data() for atomizing nodes.
 */
static struct PFla_pair_t
fn_string (struct PFla_pair_t (*data) 
             (const PFla_op_t *, bool, struct PFla_pair_t *),
           const PFla_op_t *loop,
           bool ordering,
           struct PFla_pair_t *args)
{
    /* as long as fn:data uses #pf:string-value()
       we can use it safely to convert the nodes */
    PFla_op_t *strings = project (
                             cast (
                                 data (
                                     loop,
                                     ordering,
                                     args).rel,
                                 att_cast,
                                 att_item,
                                 aat_str),
                             proj (att_iter, att_iter),
                             proj (att_pos, att_pos),
                             proj (att_item, att_cast));

    /* add the empty strings for the missing iterations */
    PFla_op_t *res = disjunion (
                         strings,
                         attach (
                             attach (
                                 difference (
                                     loop,
                                     project (
                                         strings,
                                         proj (att_iter, att_iter))),
                                 att_pos, lit_nat (1)),
                             att_item, lit_str ("")));

    return (struct PFla_pair_t) { .rel  = res, .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_attr (const PFla_op_t *loop,
                      bool ordering,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_attr, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_text (const PFla_op_t *loop,
                      bool ordering,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_text, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_pi (const PFla_op_t *loop,
                    bool ordering,
                    struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_pi, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_comm (const PFla_op_t *loop,
                      bool ordering,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_comm, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_elem (const PFla_op_t *loop,
                      bool ordering,
                      struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_elem, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string_elem_attr (const PFla_op_t *loop,
                           bool ordering,
                           struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data_elem_attr, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:string'.
 */
struct PFla_pair_t
PFbui_fn_string (const PFla_op_t *loop,
                 bool ordering,
                 struct PFla_pair_t *args)
{
    return fn_string (PFbui_fn_data, loop, ordering, args);
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
             (const PFla_op_t *, bool, struct PFla_pair_t *),
         PFalg_simple_type_t node_type,
         const PFla_op_t *loop,
         bool ordering,
         struct PFla_pair_t *args)
{
    (void) loop;

    /*
     * carry out specific type test on type
     */
    PFla_op_t *type = type (args[0].rel, att_res, att_item, node_type);

    /* select those rows that have type "node" */
    PFla_op_t *nodes = project (
                                type_assert_pos (
                                                 select_ (type, att_res),
                                                 att_item, node_type),
                                proj (att_iter, att_iter),
                                proj (att_pos, att_pos),
                                proj (att_item, att_item));

    /* select the remaining rows */
    PFla_op_t *atomics = project (type_assert_neg (select_ (not (type,
                                                                 att_res1,
                                                                 att_res),
                                                            att_res1),
                                                   att_item, node_type),
                                  proj (att_iter, att_iter),
                                  proj (att_pos, att_pos),
                                  proj (att_item, att_item));
    
    /* renumber */
    PFla_op_t *q = number (nodes, att_inner);
    
    PFla_op_t *map = project (q, 
                              proj (att_outer, att_iter), 
                              proj (att_inner, att_inner),
                              proj (att_pos1, att_pos));

    struct  PFla_pair_t str_args = {
        .rel = attach (
                       project (
                                q, 
                                proj (att_iter, att_inner), 
                                proj (att_item, att_item)),
                       att_pos, lit_nat(1)),
        .frag = args[0].frag };

    PFla_op_t *res = project(
                         eqjoin(
                             cast(str_val (project (q, proj (att_iter,
                                                             att_inner)),
                                           ordering,
                                           &str_args).rel,
                                  att_cast,
                                  att_item,
                                  aat_uA),
                             map,
                             att_iter,
                             att_inner),
                         proj(att_iter, att_outer),
                         proj(att_pos, att_pos1),
                         proj(att_item, att_cast));
    
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
                    struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_attr, aat_anode, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_text (const PFla_op_t *loop,
                    bool ordering,
                    struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_text, aat_pnode, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_pi (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_pi, aat_pnode, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_comm (const PFla_op_t *loop,
                    bool ordering,
                    struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_comm, aat_pnode, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_elem (const PFla_op_t *loop,
                    bool ordering,
                    struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_elem, aat_pnode, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data_elem_attr (const PFla_op_t *loop,
                         bool ordering,
                         struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value_elem_attr,
                   aat_node, loop, ordering, args);
}

/**
 * Build up operator tree for built-in function 'fn:data'.
 */
struct PFla_pair_t
PFbui_fn_data (const PFla_op_t *loop,
               bool ordering,
               struct PFla_pair_t *args)
{
    return fn_data(PFbui_pf_string_value, aat_node, loop, ordering, args);
}

/* --------------------- */
/* 3. THE ERROR FUNCTION */
/* --------------------- */

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
                                    PFalg_att_t,
                                    PFalg_att_t,
                                    PFalg_att_t)) params,
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
bin_arith (struct PFla_pair_t *args, 
           PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                             PFalg_att_t, PFalg_att_t),
           const PFla_op_t *loop, bool ordering)
{
    (void) loop; (void) ordering; /* keep compilers quiet */

    PFla_op_t *res = typeswitch2 (args[0].rel,
                                  args[1].rel,
                                  3,
                                  (PFalg_simple_type_t [3])
                                      { aat_int, aat_dbl, aat_dec },
                                  bin_arith_callback,
                                  OP);
    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-add ()
 * @see bin_arith()
 */
struct PFla_pair_t 
PFbui_op_numeric_add (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    return bin_arith (args, PFla_add, loop, ordering);
}

/**
 * Algebra implementation for op:numeric-subtract ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract (const PFla_op_t *loop, bool ordering,
                           struct PFla_pair_t *args)
{
    return bin_arith (args, PFla_subtract, loop, ordering);
}

/**
 * Algebra implementation for op:numeric-multiply ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply (const PFla_op_t *loop, bool ordering,
                           struct PFla_pair_t *args)
{
    return bin_arith (args, PFla_multiply, loop, ordering);
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
PFbui_op_numeric_divide (const PFla_op_t *loop, bool ordering,
                         struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

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
PFbui_op_numeric_idivide (const PFla_op_t *loop, bool ordering,
                              struct PFla_pair_t *args)
{
    return (struct PFla_pair_t) {
        .rel = project (
                   cast (bin_arith (
                             args,
                             PFla_divide,
                             loop, 
                             ordering).rel,
                         att_cast, att_item, aat_int),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_cast)),
        .frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-mod ()
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo (const PFla_op_t *loop, bool ordering,
                             struct PFla_pair_t *args)
{
    return bin_arith (args, PFla_modulo, loop, ordering);
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
          PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                            PFalg_att_t, PFalg_att_t),
          struct PFla_pair_t *args, 
          const PFla_op_t *loop,
          bool ordering)
{
    (void) loop; (void) ordering; /* keep compilers quiet */

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
PFbui_op_eq_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_eq_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_eq, args, loop, ordering);
}


/**
 * Algebra implementation for op:ne(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_int, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ne_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln,  PFla_not,
                  bin_comp (aat_str, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:lt(integer?,integer?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_lt_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:le(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_str, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_le_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_str, PFla_gt, args, loop, ordering));
}
/**
 * Algebra implementation for op:gt(integer?,integer?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_int, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(decimal?,decimal?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dec, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(double?,double?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_dbl, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(boolean?,boolean?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_bln, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(string?,string?)
 * @see bin_comp()
 */
struct PFla_pair_t
PFbui_op_gt_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_comp (aat_str, PFla_gt, args, loop, ordering);
}


/**
 * Algebra implementation for op:ge(integer?,integer?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_int,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(decimal?,decimal?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dec,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(double?,double?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_dbl,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(boolean?,boolean?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_bln,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(string?,string?)
 * @see un_op() and bin_comp()
 */
struct PFla_pair_t
PFbui_op_ge_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_op (aat_bln, PFla_not,
                  bin_comp (aat_str,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/* -------------------------------- */
/* 6.4. Functions on Numeric Values */
/* -------------------------------- */

static PFla_pair_t
numeric_fun_op (PFalg_simple_type_t t,
                PFalg_fun_t kind,
                const PFla_op_t *loop, bool ordering,
                struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (fun_1to1 (
                            cast (args[0].rel,
                                  att_cast,
                                  att_item,
                                  t),
                            kind,
                            att_res,
                            attlist (att_cast)),
                        proj (att_iter, att_iter),
                        proj (att_pos, att_pos),
                        proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

struct PFla_pair_t
PFbui_fn_abs_int (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_abs, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_abs_dec (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_abs, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_abs_dbl (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_abs, loop, ordering, args);
}


struct PFla_pair_t
PFbui_fn_ceiling_int (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_ceiling, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_ceiling_dec (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_ceiling, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_ceiling_dbl (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_ceiling, loop, ordering, args);
}


struct PFla_pair_t
PFbui_fn_floor_int (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_floor, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_floor_dec (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_floor, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_floor_dbl (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_floor, loop, ordering, args);
}


struct PFla_pair_t
PFbui_fn_round_int (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_int, alg_fun_fn_round, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_round_dec (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dec, alg_fun_fn_round, loop, ordering, args);
}

struct PFla_pair_t
PFbui_fn_round_dbl (const PFla_op_t *loop,
                  bool ordering,
                  struct PFla_pair_t *args)
{
    return numeric_fun_op (aat_dbl, alg_fun_fn_round, loop, ordering, args);
}

/* ----------------------- */
/* 7. FUNCTIONS ON STRINGS */
/* ----------------------- */
/* ------------------------------- */
/* 7.4. Functions on String Values */
/* ------------------------------- */

/**
 * The fn:concat function is wrapped in the generic function operator
 */
struct PFla_pair_t
PFbui_fn_concat (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
                .rel = project (
                           fun_1to1 (
                               eqjoin (args[0].rel,
                                       project (args[1].rel,
                                                proj (att_iter1, att_iter),
                                                proj (att_item1, att_item)),
                                       att_iter,
                                       att_iter1),
                               alg_fun_fn_concat,
                               att_res,
                               attlist(att_item, att_item1)),
                        proj (att_iter, att_iter),
                        proj (att_pos, att_pos),
                        proj (att_item, att_res)),
                .frag = args[0].frag };
}

/**
 * The fn:string_join function is also available as primitive
 */
struct PFla_pair_t
PFbui_fn_string_join (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
                .rel  = attach (
                            fn_string_join (args[0].rel,
                                            project (
                                                args[1].rel,
                                                proj (att_iter, att_iter),
                                                proj (att_item, att_item)),
                                            att_iter, att_pos, att_item,
                                            att_iter, att_item,
                                            att_iter, att_item),
                            att_pos, lit_nat (1)),
                .frag = args[0].frag };
}

/* ------------------------------------------ */
/* 7.5. Functions Based on Substring Matching */
/* ------------------------------------------ */

/**
 * Algebra implementation for <code>fn:contains (xs:string, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains (const PFla_op_t *loop, bool ordering,
                   struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (
                   fun_1to1 (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (att_iter1, att_iter),
                                        proj (att_item1, att_item)),
                               att_iter,
                               att_iter1),
                       alg_fun_fn_contains,
                       att_res,
                       attlist (att_item, att_item1)),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),
        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:contains (xs:string?, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains_opt (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    (void) ordering;

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
                                               proj (att_iter, att_iter))),
                                       att_pos, lit_nat (1)),
                                   att_item, lit_str (""))),
                           project (args[1].rel,
                                    proj (att_iter1, att_iter),
                                    proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       alg_fun_fn_contains,
                       att_res,
                       attlist (att_item, att_item1)),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for 
 * <code>fn:contains (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains_opt_opt (const PFla_op_t *loop, bool ordering,
                           struct PFla_pair_t *args)
{
    (void) ordering;

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
                                               proj (att_iter, att_iter))),
                                       att_pos, lit_nat (1)),
                                   att_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (att_iter, att_iter))),
                                           att_pos, lit_nat (1)),
                                       att_item, lit_str (""))),
                               proj (att_iter1, att_iter),
                               proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       alg_fun_fn_contains,
                       att_res,
                       attlist (att_item, att_item1)),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for 
 * <code>fn:starts-with (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_starts_with_opt_opt (const PFla_op_t *loop, bool ordering,
                              struct PFla_pair_t *args)
{
    (void) ordering;

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
                                               proj (att_iter, att_iter))),
                                       att_pos, lit_nat (1)),
                                   att_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (att_iter, att_iter))),
                                           att_pos, lit_nat (1)),
                                       att_item, lit_str (""))),
                               proj (att_iter1, att_iter),
                               proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       alg_fun_fn_starts_with,
                       att_res,
                       attlist (att_item, att_item1)),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),

        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for 
 * <code>fn:ends-with (xs:string?, xs:string?)</code>.
 */
struct PFla_pair_t
PFbui_fn_ends_with_opt_opt (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    (void) ordering;

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
                                               proj (att_iter, att_iter))),
                                       att_pos, lit_nat (1)),
                                   att_item, lit_str (""))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   attach (
                                       attach (
                                           difference (
                                               loop,
                                               project (
                                                   args[1].rel,
                                                   proj (att_iter, att_iter))),
                                           att_pos, lit_nat (1)),
                                       att_item, lit_str (""))),
                               proj (att_iter1, att_iter),
                               proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       alg_fun_fn_ends_with,
                       att_res,
                       attlist (att_item, att_item1)),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),

        .frag = PFla_empty_set ()};
}

/* ----------------------------------------------- */
/* 7.6. String Functions that Use Pattern Matching */
/* ----------------------------------------------- */

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
                          att_pos,
                          lit_nat(1)),
                   att_item, 
                   lit_bln(value)),
        .frag = PFla_empty_set () }; 
}

/**
 * Algebra implementation for <code>fn:true ()</code>.
 */
struct PFla_pair_t 
PFbui_fn_true (const PFla_op_t *loop, bool ordering,
               struct PFla_pair_t *args __attribute__((unused)))
{
    (void) ordering; (void) args;

    return PFbui_fn_bln_lit(loop, true);
}

/**
 * Algebra implementation for <code>fn:false ()</code>.
 */
struct PFla_pair_t 
PFbui_fn_false (const PFla_op_t *loop, bool ordering,
               struct PFla_pair_t *args __attribute__((unused)))
{
    (void) ordering; (void) args;

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
PFbui_fn_not_bln (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;
    
    return un_op (aat_bln, PFla_not, args[0]);
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
 */
struct PFla_pair_t
PFbui_fn_resolve_qname (const PFla_op_t *loop, bool ordering,
                        struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    /* implement it as a simple cast to xs:QName */
    return (struct PFla_pair_t) {
        .rel = project (cast (args[0].rel, att_cast, att_item, aat_qname),
                        proj (att_iter, att_iter),
                        proj (att_pos, att_pos),
                        proj (att_item, att_cast)),
        .frag = PFla_empty_set () };
}

/* ----------------------------------- */
/* 14 FUNCTIONS AND OPERATORS ON NODES */
/* ----------------------------------- */
/* ------------ */
/* 14.1 fn:name */
/* ------------ */

/* ------------------- */
/* 14.2. fn:local-name */
/* ------------------- */

/* --------------------- */
/* 14.3. fn:namespace-uri */
/* --------------------- */

/* --------------- */
/* 14.4. fn:number */    
/* --------------- */

struct PFla_pair_t
PFbui_fn_number (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args)
{
    /* As we do not support the value NaN we need to generate an error
       for all tuples that are empty (instead of attaching NaN). */
       
    char *err_string = "We do not support the value NaN.";
    
    (void) ordering;

    return (struct  PFla_pair_t) {
                 .rel = project (
                            fun_1to1 (
                                cond_err (
                                    args[0].rel,
                                    attach (
                                        difference (
                                            loop,
                                            project (
                                                args[0].rel,
                                                proj (att_iter, att_iter))),
                                        att_item,
                                        lit_bln (false)),
                                    att_item,
                                    err_string),
                                alg_fun_fn_number,
                                att_res,
                                attlist (att_item)),
                            proj (att_iter, att_iter),
                            proj (att_pos, att_pos),
                            proj (att_item, att_res)),
                 .frag = args[0].frag };
}

/* --------------------- */
/* 14.6. op:is-same-node */
/* --------------------- */

/**
 * Algebra implementation for op:is-same-node (node?, node?)
 */
struct PFla_pair_t
PFbui_op_is_same_node (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (PFla_eq (
                        eqjoin (
                            project (args[0].rel, 
                                     proj (att_iter, att_iter),
                                     proj (att_pos, att_pos),
                                     proj (att_item, att_item)),
                            project (args[1].rel,
                                     proj (att_iter1, att_iter),
                                     proj (att_item1, att_item)),
                            att_iter,
                            att_iter1),
                        att_res, att_item, att_item1),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/* -------------------- */
/* 14.7. op:node-before */
/* -------------------- */

/**
 * Algebra implementation for op:node-before (node?, node?)
 */
struct PFla_pair_t
PFbui_op_node_before (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (PFla_gt (
                        eqjoin (
                            project (args[1].rel, 
                                     proj (att_iter, att_iter),
                                     proj (att_pos, att_pos),
                                     proj (att_item, att_item)),
                            project (args[0].rel,
                                     proj (att_iter1, att_iter),
                                     proj (att_item1, att_item)),
                            att_iter,
                            att_iter1),
                        att_res, att_item, att_item1),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/* ------------------- */
/* 14.8. op:node-after */
/* ------------------- */

/**
 * Algebra implementation for op:node-after (node?, node?)
 */
struct PFla_pair_t
PFbui_op_node_after (const PFla_op_t *loop, bool ordering,
                     struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (PFla_gt (
                        eqjoin (
                            project (args[0].rel, 
                                     proj (att_iter, att_iter),
                                     proj (att_pos, att_pos),
                                     proj (att_item, att_item)),
                            project (args[1].rel,
                                     proj (att_iter1, att_iter),
                                     proj (att_item1, att_item)),
                            att_iter,
                            att_iter1),
                        att_res, att_item, att_item1),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/* ------------- */
/* 14.9. fn:root */
/* ------------- */

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
PFbui_fn_boolean_bln (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

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
PFbui_fn_boolean_optbln (const PFla_op_t *loop, bool ordering,
                         struct PFla_pair_t *args)
{
    (void) ordering;

    return (struct PFla_pair_t) {
        .rel = disjunion (
                   args[0].rel,
                   attach (
                       attach (
                           difference (
                               loop,
                               project (
                                   args[0].rel,
                                   proj (att_iter, att_iter))),
                                       att_pos, lit_nat (1)),
                       att_item, lit_bln (false))),
        .frag = PFla_empty_set () };
}

/** 
 * Helper function for PFbui_fn_boolean_item
 * Returns those rows with att_item != case_->params.
 */
static PFla_op_t *
fn_boolean_atomic (PFla_op_t *n, PFalg_atom_t literal)
{
    return project (not (eq (attach (n,
                                     att_item1, literal),
                             att_res, att_item, att_item1),
                         att_res1, att_res),
                    proj (att_iter, att_iter),
                    proj (att_item, att_res1),
                    proj (att_pos, att_pos));
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
    return attach (attach (distinct (project (n, proj (att_iter, att_iter))),
                           att_pos, lit_nat (1)),
                   att_item, lit_bln (true));
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
                                      cast (n, att_cast, att_item, aat_str),
                                      proj (att_iter, att_iter),
                                      proj (att_pos, att_pos),
                                      proj (att_item, att_cast)),
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
PFbui_fn_boolean_item (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    (void) ordering;

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
                                   proj (att_iter,att_iter))),
                           att_item, lit_bln (false)),
                       att_pos, lit_nat (1))),
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
PFbui_fn_empty (const PFla_op_t *loop, bool ordering,
                struct PFla_pair_t *args)
{
    (void) ordering;

    return (struct PFla_pair_t) {
        .rel = attach (
               disjunion (
                   attach (
                       distinct (project (args[0].rel,
                                 proj (att_iter, att_iter))),
                       att_item, lit_bln (false)),
                   attach (
                       difference (
                           loop,
                           project (args[0].rel,
                                    proj (att_iter, att_iter))),
                       att_item, lit_bln (true))),
               att_pos, lit_nat (1)),
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
PFbui_fn_exists (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = attach (
               disjunion (
                   attach (
                       distinct (project (args[0].rel,
                                 proj (att_iter, att_iter))),
                       att_item, lit_bln (true)),
                   attach (
                       difference (
                           loop,
                           project (args[0].rel,
                                    proj (att_iter, att_iter))),
                       att_item, lit_bln (false))),
               att_pos, lit_nat (1)),
        .frag = PFla_empty_set ()};
}

/**
 * The fn:distinct-values function removes its duplicates
 */
struct PFla_pair_t
PFbui_fn_distinct_values (const PFla_op_t *loop, bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
                  .rel = number (
                             distinct (
                                 project (args[0].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item))),
                             att_pos),
                  .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_insert_before (const PFla_op_t *loop, bool ordering,
                        struct PFla_pair_t *args)
{
    /* mark all rows true where the second argument is greather than
       the index position */
    PFla_op_t *partition = gt (eqjoin (
                                   cast (adjust_positions (args[0].rel),
                                         att_cast,
                                         att_pos,
                                         aat_int),
                                   project (args[1].rel,
                                            proj (att_iter1, att_iter),
                                            proj (att_item1, att_item)),
                                   att_iter,
                                   att_iter1),
                               att_res,
                               att_item1,
                               att_cast);

    /* select all the tuples whose index position is smaller than
       the second argument */
    PFla_op_t *first = attach (
                           project (
                               select_ (partition, att_res),
                               proj (att_iter, att_iter),
                               proj (att_pos, att_pos),
                               proj (att_item, att_item)),
                           att_ord,
                           lit_nat (1));
    
    PFla_op_t *second = attach (args[2].rel, att_ord, lit_nat (2));
    
    /* select all the tuples whose index position is bigger or equal to
       the second argument */
    PFla_op_t *third = attach (
                           project (
                               select_ (
                                   not (partition, att_res1, att_res),
                                   att_res1),
                               proj (att_iter, att_iter),
                               proj (att_pos, att_pos),
                               proj (att_item, att_item)),
                           att_ord,
                           lit_nat (3));
    
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        /* patch the third function argument into the middle*/
        .rel = project (
                    rank (
                        disjunion (first, disjunion (second, third)),
                        att_pos1,
                        sortby (att_ord, att_pos)),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos1),
                    proj (att_item, att_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_remove (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

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
                                          att_cast,
                                          att_pos,
                                          aat_int),
                                    project (args[1].rel,
                                             proj (att_iter1, att_iter),
                                             proj (att_item1, att_item)),
                                    att_iter,
                                    att_iter1),
                                att_res,
                                att_cast,
                                att_item1),
                            att_res1, att_res),
                       att_res1),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_reverse (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    /*
     * Use column pos to introduce a new column pos
     * that is sorted in reverse order.
     */
    return (struct PFla_pair_t) {
        .rel  = project (
                    rank (
                        args[0].rel,
                        att_res,
                        PFord_refine (PFordering (), att_pos, DIR_DESC)),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_res),
                    proj (att_item, att_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_subsequence_till_end (const PFla_op_t *loop, bool ordering,
                               struct PFla_pair_t *args)
{
    PFla_op_t *startingLoc = args[1].rel;
    
    (void) loop; (void) ordering;

#ifndef NDEBUG
    /* make sure that the second argument is of type integer */
    for (unsigned int i = 0; i < startingLoc->schema.count; i++) {
        if (startingLoc->schema.items[i].name == att_item) {
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
                                          att_cast,
                                          att_pos,
                                          aat_int),
                                    project (startingLoc,
                                             proj (att_iter1, att_iter),
                                             proj (att_item1, att_item)),
                                    att_iter,
                                    att_iter1),
                                att_res,
                                att_item1,
                                att_cast),
                            att_res1, att_res),
                       att_res1),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_subsequence (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    PFla_op_t *startingLoc = args[1].rel;
    PFla_op_t *length      = args[2].rel;
    PFla_op_t *first_cond;
    
#ifndef NDEBUG
    /* make sure that the second and the third argument are of type integer */
    for (unsigned int i = 0; i < startingLoc->schema.count; i++) {
        if (startingLoc->schema.items[i].name == att_item) {
            assert (startingLoc->schema.items[i].type == aat_int);
            break;
        }
    }
    for (unsigned int i = 0; i < length->schema.count; i++) {
        if (length->schema.items[i].name == att_item) {
            assert (length->schema.items[i].type == aat_int);
            break;
        }
    }
#endif
    
    (void) loop; (void) ordering;

    /* evaluate the first condition (startingLoc) */
    first_cond = project (
                     select_ (
                         not (gt (eqjoin (
                                      cast (adjust_positions (args[0].rel),
                                            att_cast,
                                            att_pos,
                                            aat_int),
                                      project (startingLoc,
                                               proj (att_iter1, att_iter),
                                               proj (att_item1, att_item)),
                                      att_iter,
                                      att_iter1),
                                  att_res,
                                  att_item1,
                                  att_cast),
                              att_res1, att_res),
                         att_res1),
                     proj (att_iter, att_iter),
                     proj (att_pos, att_pos),
                     proj (att_item, att_item),
                     proj (att_cast, att_cast),    /* pos as int  */
                     proj (att_item1, att_item1)); /* startingLoc */
    
    /* evaluate the second condition ($pos < $startingLoc + $length)
       and fill in new position values afterwards */
    return (struct PFla_pair_t) {
        .rel = project (
                   select_ (
                       gt (fun_1to1 (
                               eqjoin (
                                   first_cond,
                                   project (length,
                                            proj (att_iter1, att_iter),
                                            proj (att_item2, att_item)),
                                   att_iter,
                                   att_iter1),
                               alg_fun_num_add,
                               att_res,
                               attlist (att_item2, att_item1)),
                           att_res1,
                           att_res,
                           att_cast),
                       att_res1),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_item)),
        .frag = args[0].frag };
}

struct PFla_pair_t
PFbui_fn_unordered (const PFla_op_t *loop, bool ordering,
                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    /*
     * project away pos column
     */
    return (struct PFla_pair_t) {
        .rel  = number (
                    project (args[0].rel,
                             proj (att_iter, att_iter),
                             proj (att_item, att_item)),
                    att_pos),
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
PFbui_fn_zero_or_one (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    PFla_op_t *count = eq (attach (
                               count (
                                   project (args[0].rel, 
                                            proj (att_iter, att_iter)),
                                   att_item, att_iter),
                               att_item1, lit_int (1)),
                           att_res, att_item1, att_item); 

    char *err_string = "err:FORG0003, fn:zero-or-one called with "
                       "a sequence containing more than one item.";
    
    (void) loop; (void) ordering;

    return (struct  PFla_pair_t) {
                 .rel = cond_err (args[0].rel,
                                  count,
                                  att_res,
                                  err_string),
                 .frag = args[0].frag };
}

/**
 * The fn:exactly-one function checks at runtime the sequence 
 * length of the input relation and triggers an error.
 */
struct PFla_pair_t
PFbui_fn_exactly_one (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    PFla_op_t *count = eq (attach (
                               PFbui_fn_count (loop, ordering, args).rel,
                               att_item1, lit_int (1)),
                           att_res, att_item1, att_item); 

    char *err_string = "err:FORG0005, fn:exactly-one called with "
                       "a sequence containing zero or more than one item.";
    
    (void) ordering;

    return (struct  PFla_pair_t) {
                 .rel = cond_err (args[0].rel,
                                  count,
                                  att_res,
                                  err_string),
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
PFbui_op_union (const PFla_op_t *loop, bool ordering,
                struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;

    distinct = distinct (disjunion (
                             project (args[0].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item)),
                             project (args[1].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item))));
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         att_pos, sortby (att_item)),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos),
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
PFbui_op_intersect (const PFla_op_t *loop, bool ordering,
                    struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;

    distinct = distinct (intersect (
                             project (args[0].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item)),
                             project (args[1].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item))));
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         att_pos, sortby (att_item)),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos),
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
PFbui_op_except (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    PFla_op_t *difference;

    (void) loop;

    difference = difference (
                     distinct (project (args[0].rel,
                                        proj (att_iter, att_iter),
                                        proj (att_item, att_item))),
                     project (args[1].rel,
                              proj (att_iter, att_iter),
                              proj (att_item, att_item)));
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (difference,
                         att_pos, sortby (att_item)),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (difference, att_pos),
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
                struct PFla_pair_t *args)
{
    (void) ordering;

    PFla_op_t *count = count (project (args[0].rel, 
                                       proj (att_iter, att_iter)),
                              att_item, att_iter);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    count,
                    attach (
                        difference (
                            loop,
                            project (count, proj (att_iter, att_iter))),
                        att_item, lit_int (0))),
                att_pos, lit_nat (1)),
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
fn_aggr (PFalg_simple_type_t t, PFla_op_kind_t kind, struct PFla_pair_t *args,
         const PFla_op_t *loop, bool ordering)
{
    (void) loop; (void) ordering;
    
    return (struct PFla_pair_t) {
        .rel = attach(aggr (kind,
                            project (cast(args[0].rel, att_cast, att_item, t),
                                     proj (att_iter, att_iter),
                                     proj (att_item, att_cast)),
                            att_item, att_item, att_iter),
                      att_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:avg ($arg)'.
 */
struct PFla_pair_t
PFbui_fn_avg (const PFla_op_t *loop, bool ordering,
              struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, la_avg, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:max (string*)'.
 */
struct PFla_pair_t
PFbui_fn_max_str (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_str, la_max, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:max (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_max_int (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_int, la_max, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:max (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_max_dec (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dec, la_max, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:max (double*)'.
 */
struct PFla_pair_t
PFbui_fn_max_dbl (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, la_max, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:min (string*)'.
 */
struct PFla_pair_t
PFbui_fn_min_str (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_str, la_min, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:min (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_min_int (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_int, la_min, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:min (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_min_dec (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dec, la_min, args, loop, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:min (double*)'.
 */
struct PFla_pair_t
PFbui_fn_min_dbl (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_aggr(aat_dbl, la_min, args, loop, ordering);
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
fn_sum (PFalg_simple_type_t t, const PFla_op_t *loop, struct PFla_pair_t *args,
        bool ordering)
{
    (void) ordering;

    PFla_op_t *sum = aggr (la_sum,
                           project (cast(args[0].rel, att_cast, att_item, t),
                                   proj (att_iter, att_iter),
                                   proj (att_item, att_cast)),
                           att_item, att_item, att_iter);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    sum,
                    attach (
                        difference (
                            loop,
                            project (sum, proj (att_iter, att_iter))),
                        att_item, lit_int (0))),
                att_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:sum (integer*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_int (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_int, loop, args, ordering); 
}

/**
 * Build up operator tree for built-in function 'fn:sum (decimal*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_dec (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_dec, loop, args, ordering); 
}

/**
 * Build up operator tree for built-in function 'fn:sum (double*)'.
 */
struct PFla_pair_t
PFbui_fn_sum_dbl (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return fn_sum(aat_dbl, loop, args, ordering); 
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
fn_sum_zero (PFalg_simple_type_t t, const PFla_op_t *loop,
             struct PFla_pair_t *args, bool ordering)
{
    (void) ordering;

    PFla_op_t *sum = aggr (la_sum,
                           project (cast (args[0].rel, att_cast, att_item, t),
                                    proj (att_iter, att_iter),
                                    proj (att_item, att_cast)),
                           att_item, att_item, att_iter);

    return (struct PFla_pair_t) {
        .rel = attach (
                disjunion (
                    sum,
                    project (
                         eqjoin (
                              difference (
                                   loop,
                                   project (sum, proj (att_iter, att_iter))),
                              project (cast(args[1].rel, att_cast, att_item, t),
                                       proj (att_iter1, att_iter),
                                       proj (att_item, att_cast)),
                              att_iter, att_iter1),
                         proj (att_iter, att_iter),
                         proj (att_item, att_item))),
                att_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function 'fn:sum (integer*, integer?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_int (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_int, loop, args, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:sum (decimal*, decimal?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_dec (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_dec, loop, args, ordering);
}

/**
 * Build up operator tree for built-in function 'fn:sum (double*, double?)'.
 */
struct PFla_pair_t
PFbui_fn_sum_zero_dbl (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    return fn_sum_zero(aat_dbl, loop, args, ordering);
}

/* ----------------------------------------------------- */
/* 15.5. Functions and Operators that Generate Sequences */
/* ----------------------------------------------------- */

/**
 * Build up operator tree for built-in function 'op:to (int, int)'.
 */
struct PFla_pair_t
PFbui_op_to (const PFla_op_t *loop, bool ordering,
             struct PFla_pair_t *args)
{
    (void) loop;
    (void) ordering;

    PFla_op_t *to = to (project (
                            eqjoin (
                                project (
                                    args[0].rel,
                                    proj (att_iter, att_iter),
                                    proj (att_item, att_item)),
                                project (
                                    args[1].rel,
                                    proj (att_iter1, att_iter),
                                    proj (att_item1, att_item)),
                                att_iter,
                                att_iter1),
                            proj (att_iter, att_iter),
                            proj (att_item, att_item),
                            proj (att_item1, att_item1)),
                        att_item,
                        att_item,
                        att_item1,
                        att_iter);

    return (struct PFla_pair_t) {
        .rel = rank (
                   to,
                   att_pos,
                   sortby (att_item)),
        .frag = PFla_empty_set () };
}


static struct PFla_pair_t
fn_id (const PFla_op_t *loop, bool ordering,
       struct PFla_pair_t *args, bool id)
{
    PFla_op_t *in, *op, *doc;
    
    (void) loop;
    (void) ordering;

    in = project (
             eqjoin (
                 project (
                     args[0].rel,
                     proj (att_iter, att_iter),
                     proj (att_item, att_item)),
                 project (
                     args[1].rel,
                     proj (att_iter1, att_iter),
                     proj (att_item1, att_item)),
                 att_iter,
                 att_iter1),
             proj (att_iter, att_iter),
             proj (att_item, att_item),
             proj (att_item1, att_item1));

    doc = PFla_set_to_la (args[1].frag);
    
    if (id)
       op = id (doc, in, att_iter, att_item, att_item, att_item1);
    else
       op = idref (doc, in, att_iter, att_item, att_item, att_item1);

    if (ordering)
        return (struct PFla_pair_t) {
            .rel = rank (distinct (op), att_pos, sortby (att_item)),
            .frag = args[1].frag };
    else
        return (struct PFla_pair_t) {
            .rel = number (distinct (op), att_pos),
            .frag = args[1].frag };
}


/**
 * Build up operator tree for built-in function 'fn:id (string*, node)'.
 */
struct PFla_pair_t
PFbui_fn_id (const PFla_op_t *loop, bool ordering,
             struct PFla_pair_t *args)
{
    return fn_id (loop, ordering, args, true);
}


/**
 * Build up operator tree for built-in function 'fn:idref (string*, node)'.
 */
struct PFla_pair_t
PFbui_fn_idref (const PFla_op_t *loop, bool ordering,
                struct PFla_pair_t *args)
{
    return fn_id (loop, ordering, args, false);
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
PFbui_fn_doc (const PFla_op_t *loop, bool ordering,
              struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    PFla_op_t *doc = doc_tbl (project (args[0].rel,
                                       proj (att_iter, att_iter),
                                       proj (att_item, att_item)),
                              att_iter, att_item, att_item);

    return (struct PFla_pair_t) {
        .rel  = attach (roots (doc), att_pos, lit_nat (1)),
        .frag = PFla_set (fragment (doc)) };
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
PFbui_op_or_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; /* keep compilers quiet */

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
PFbui_op_and_bln (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering; /* keep compilers quiet */

    PFla_op_t *res = bin_op (aat_bln,
                             PFla_and,
                             args[0].rel,
                             args[1].rel);

    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}

/**
 * The fs:distinct-doc-order function sorts its input sequence of
 * nodes by document order and removes duplicates.
 */
struct PFla_pair_t
PFbui_pf_distinct_doc_order (const PFla_op_t *loop, bool ordering,
                             struct PFla_pair_t *args)
{
    PFla_op_t *distinct = distinct (
                              project (args[0].rel,
                                   proj (att_iter, att_iter),
                                   proj (att_item, att_item)));

    (void) loop;
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rank (distinct,
                         att_pos, sortby (att_item)),
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos),
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
                                         att_cast,
                                         att_item,
                                         aat_str),
                                   att_iter, att_cast),
                               att_iter, att_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (roots (t_nodes), att_pos, lit_nat (1)),
                 /* union of those nodes we had in the very beginning
                  * (those in frag) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_single_atomic
    (const PFla_op_t *loop, bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;

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
                                       att_cast,
                                       att_item,
                                       aat_str),
                                 proj (att_iter, att_iter),
                                 proj (att_pos, att_pos),
                                 proj (att_item, att_cast)),
                             project (
                                 attach (
                                     attach (loop, att_pos, lit_nat (1)),
                                     att_item, lit_str (" ")),
                                 proj (att_iter, att_iter),
                                 proj (att_item, att_item)),
                         att_iter, att_pos, att_item,
                         att_iter, att_item,
                         att_iter, att_item);

    PFla_op_t *t_nodes = twig (textnode (
                                   strings,
                                   att_iter, att_item),
                               att_iter, att_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (roots (t_nodes), att_pos, lit_nat (1)),
                 /* union of those nodes we had in the very beginning
                  * (those in part1) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_atomic
    (const PFla_op_t *loop, bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;

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
    PFla_op_t *type = type (rel, att_res, att_item, aat_anode);

    /* select those rows that have type "attr" (part1) */
    PFla_op_t *part1 = project (
                           type_assert_pos (
                               select_ (type, att_res),
                               att_item, aat_anode),
                           proj (att_iter, att_iter),
                           proj (att_pos, att_pos),
                           proj (att_item, att_item));

    /* select the remaining rows (part2) */
    PFla_op_t *part2 = project (
                           type_assert_neg (
                               select_ (not (type, att_res1, att_res),
                                        att_res1),
                               att_item, aat_anode),
                           proj (att_iter, att_iter),
                           proj (att_pos, att_pos),
                           proj (att_item, att_item));

    /* call the translation of the atomics with only the
       atomic values (part2) */
    struct PFla_pair_t text = fun (loop, part2, frag);
    
    /* get the roots of the new text nodes, form union of roots and
     * part1, and sort result on att_ord and att_pos column
     */
    return (struct  PFla_pair_t) {
                 .rel = project (
                            rank (
                                disjunion (
                                    attach (part1, att_ord, lit_nat (1)),
                                    attach (text.rel, att_ord, lit_nat (2))),
                                att_pos1, sortby (att_ord, att_pos)),
                            proj (att_iter, att_iter),
                            proj (att_pos, att_pos1),
                            proj (att_item, att_item)),
                 /* fill in frag union generated in the textnode
                  * generation for atomic values (fun).
                  */
                 .frag = text.frag };
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_attr_single
    (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args)
{
    (void) ordering;

    return pf_item_seq_to_node_seq_worker_attr (
               loop, args[0].rel, args[0].frag,
               pf_item_seq_to_node_seq_worker_single_atomic);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_attr
    (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args)
{
    (void) ordering;

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
    PFla_op_t *type = type (input, att_res, att_item, split_type);

    /* select those rows that have type "node" (part1) */
    PFla_op_t *part1 = project (
                           type_assert_pos (
                               select_ (type, att_res),
                               att_item, split_type),
                           proj (att_iter, att_iter),
                           proj (att_pos, att_pos),
                           proj (att_item, att_item));

    /* select the remaining rows (part2) */
    PFla_op_t *part2 = project (
                           type_assert_neg (
                               select_ (not (type, att_res1, att_res),
                                        att_res1),
                               att_item, split_type),
                           proj (att_iter, att_iter),
                           proj (att_pos, att_pos),
                           proj (att_item, att_item));

    /*
     * convert all items in part2 into strings ...
     */
    PFla_op_t *strings = cast (part2, att_cast, att_item, aat_str);

    /*
     * compare columns pos and pos-1 to find adjacent strings
     */
    PFla_op_t *base = fun_1to1 (
                          cast (
                              attach (strings, att_item1, lit_int (1)),
                              att_pos1, att_pos, aat_int),
                          alg_fun_num_subtract,
                          att_res,
                          attlist (att_pos1, att_item1));

    PFla_op_t *delim = project (
                           select_ (
                               eq (eqjoin (
                                       project (
                                           base,
                                           proj (att_iter1, att_iter),
                                           proj (att_res, att_res)),
                                       project (
                                           base,
                                           proj (att_iter, att_iter),
                                           proj (att_pos1, att_pos1),
                                           proj (att_pos, att_pos)),
                                       att_iter1, att_iter),
                                   att_res1, att_res, att_pos1),
                               att_res1),
                           proj (att_iter, att_iter),
                           proj (att_pos, att_pos));

    /*
     * for each pair of adjacent strings add a whitespace string
     */
    PFla_op_t *sep = attach (
                         attach (delim, att_item, lit_str (" ")),
                         att_ord, lit_nat (2));

    /*
     * create textnodes for each string
     * (casted atomic values and whitespace strings)
     */
    PFla_op_t *unq_strings = number (
                                 disjunion (
                                     attach (project (strings,
                                                     proj (att_iter, att_iter),
                                                     proj (att_pos, att_pos),
                                                     proj (att_item, att_cast)),
                                            att_ord, lit_nat (1)),
                                     sep),
                                 att_inner);

    PFla_op_t *t_nodes = twig (textnode (unq_strings, att_inner, att_item),
                               att_iter, att_item);
                         
    /* get the roots of the new text nodes, form union of roots and
     * part1, and sort result on att_pos and att_ord column
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
                                                proj (att_outer, att_iter),
                                                proj (att_inner, att_inner),
                                                proj (att_pos, att_pos),
                                                proj (att_ord, att_ord)),
                                            att_iter, att_inner),
                                        proj (att_iter, att_outer),
                                        proj (att_pos, att_pos),
                                        proj (att_item, att_item),
                                        proj (att_ord, att_ord)),
                                    attach (part1, att_ord, lit_nat (1))),
                                att_pos1, sortby (att_pos, att_ord)),
                            proj (att_iter, att_iter),
                            proj (att_pos, att_pos1),
                            proj (att_item, att_item)),
                 /* union of those nodes we had in the very beginning
                  * (those in part1) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (args[0].frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_wo_attr
    (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    /*
     * translate is2ns function using its worker
     */
    return pf_item_seq_to_node_seq_worker (args, aat_pnode);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq (const PFla_op_t *loop, bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

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
 * (with same att_iter and consecutive att_pos values). If a text node
 * is empty, it is discarded.
 * The output are an algebra representation of all nodes (old and new,
 * i.e. unmerged and merged) and a fragment representation of the newly
 * created nodes only.
 */
struct PFla_pair_t
PFbui_pf_merge_adjacent_text_nodes (
        const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    PFla_op_t *merged
        = merge_adjacent (PFla_set_to_la (args[0].frag), args[0].rel,
                          att_iter, att_pos, att_item,
                          att_iter, att_pos, att_item);

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
PFbui_pf_string_value_attr (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel, 
                                     att_res, att_item, doc_atext),
                         proj (att_iter, att_iter),
                         proj (att_pos,  att_pos),
                         proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(text {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_text (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel, 
                                     att_res, att_item, doc_text),
                         proj (att_iter, att_iter),
                         proj (att_pos,  att_pos),
                         proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(processing-instruction {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_pi (const PFla_op_t *loop, bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     att_res, att_item, doc_pi_text),
                         proj (att_iter, att_iter),
                         proj (att_pos,  att_pos),
                         proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(comment {"text"}).
 */
struct PFla_pair_t
PFbui_pf_string_value_comm (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel  = project (doc_access (PFla_set_to_la (args[0].frag),
                                     args[0].rel,
                                     att_res, att_item, doc_comm),
                         proj (att_iter, att_iter),
                         proj (att_pos,  att_pos),
                         proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(<code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    PFla_op_t *node_scj, *nodes;

    (void) ordering;

    /* retrieve all descendant textnodes (`/descendant-or-self::text()') */
    node_scj = rank (
                   PFla_step_simple (
                       PFla_set_to_la (args[0].frag),
                       project (args[0].rel,
                                proj (att_iter, att_iter),
                                proj (att_item, att_item)),
                       alg_desc_s, PFty_text (),
                       att_iter, att_item, att_item),
                   att_pos, sortby (att_item));

    /* concatenate all texts within an iteration using
       the empty string as delimiter */
    nodes = fn_string_join (
                project (
                    doc_access (
                        PFla_set_to_la (args[0].frag),
                        node_scj,
                        att_res, att_item, doc_text),
                    proj (att_iter, att_iter),
                    proj (att_pos,  att_pos),
                    proj (att_item, att_res)),
                project (
                    attach (
                        attach (loop, att_pos, lit_nat (1)),
                        att_item, lit_str ("")),
                    proj (att_iter, att_iter),
                    proj (att_item, att_item)),
                att_iter, att_pos, att_item,
                att_iter, att_item,
                att_iter, att_item);

    return (struct PFla_pair_t) {
        .rel  = attach (nodes, att_pos, lit_nat (1)), 
        .frag = PFla_empty_set () };
}

/**
 * Built-in function #pf:typed-value(attribute foo {"text"}, <code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem_attr (const PFla_op_t *loop, bool ordering,
                                 struct PFla_pair_t *args)
{
    PFla_op_t *sel_attr, *sel_node, *attributes, 
              *node_scj, *nodes;

    /* we know that we have no empty sequences and 
       thus can skip the treating for empty sequences */
    (void) loop; (void) ordering;

    /* select all attributes and retrieve their string values */
    sel_attr = project (
                   type_assert_pos (
                       select_ (
                           type (args[0].rel, att_subty, 
                                 att_item, aat_anode),
                           att_subty),
                       att_item, aat_anode),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_item));

    attributes = project (doc_access (PFla_set_to_la (args[0].frag),
                          sel_attr, att_res, att_item, doc_atext),
                          proj (att_iter, att_iter),
                          proj (att_item, att_res));

    /* select all other nodes and retrieve string values
       as in PFbui_pf_string_value_elem */
    sel_node = project (
                   type_assert_neg (
                       select_ (
                           not (type (args[0].rel, att_subty, 
                                      att_item, aat_anode),
                                att_notsub, att_subty),
                           att_notsub),
                       att_item, aat_anode),
                   proj (att_iter, att_iter),
                   proj (att_pos, att_pos),
                   proj (att_item, att_item));

    /* retrieve all descendant textnodes (`/descendant-or-self::text()') */
    node_scj = rank (
                   PFla_step_simple (
                       PFla_set_to_la (args[0].frag),
                       project (sel_node,
                                proj (att_iter, att_iter),
                                proj (att_item, att_item)),
                       alg_desc_s, PFty_text (),
                       att_iter, att_item, att_item),
                   att_pos, sortby (att_item));

    /* concatenate all texts within an iteration using
       the empty string as delimiter */
    nodes = fn_string_join (
                project (
                    doc_access (
                        PFla_set_to_la (args[0].frag),
                        node_scj,
                        att_res, att_item, doc_text),
                    proj (att_iter, att_iter),
                    proj (att_pos,  att_pos),
                    proj (att_item, att_res)),
                project (
                    attach (
                            project (
                                     sel_node, 
                                     proj(att_iter, att_iter),
                                     proj(att_pos, att_pos)),
                        att_item, lit_str ("")),
                    proj (att_iter, att_iter),
                    proj (att_item, att_item)),
                att_iter, att_pos, att_item,
                att_iter, att_item,
                att_iter, att_item);

    return (struct PFla_pair_t) {
        .rel  = attach (disjunion (attributes, nodes),
                        att_pos, lit_nat (1)),
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function '#pf:string-value'.
 */
struct PFla_pair_t
PFbui_pf_string_value (const PFla_op_t *loop, bool ordering,
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

    (void) loop; (void) ordering;

    return args[0];
}

/* vim:set shiftwidth=4 expandtab: */
