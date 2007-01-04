/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Creation of algebra translations for XQuery built-in functions.
 * The representation of such functions was extended by a function
 * pointer that references the functions defined in this file.
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
 * @param n The relation used as input for the typeswitch.
 * @param count The number of types in the types array
 * @param types The types to distinguish
 * @param cnst  A callback function, called for each type encounterd
 *              in the relation
 * @param params Optional parameters, passed to the callback function.
 */
PFla_op_t *
PFla_typeswitch (PFla_op_t *n, unsigned int count, 
                 const PFalg_type_t *types, 
                 PFla_op_t *(*cnst) (PFla_op_t *n, PFalg_type_t, void *),
                 void *params)
{
    PFla_op_t *res = NULL;
    PFalg_type_t item_types = 0;
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
                "attribute `%s' referenced in type switch not found",
                PFatt_str (att_item));
    
    for (unsigned int i = 0; i < count; i++) {
        PFalg_type_t ty = item_types & types[i];        
        if (ty == item_types) {
            res = cnst (n, types[i], params);
            break;
        }
        else if (ty != 0) {
            PFla_op_t *tsw = cnst (sel_type (n, ty), types[i], params);
            if (res != NULL)
                res = disjunion (res, tsw);
            else
                res = tsw;
        }
    }
    return res;
}

struct typeswitch2_params {
    PFla_op_t *n;
    PFalg_type_t t;
    int count;
    const PFalg_type_t *types;
    void *params;
    PFla_op_t *(*cnst) (PFla_op_t *n1, PFla_op_t *n2,
                        PFalg_type_t, PFalg_type_t, void *);
};

static PFla_op_t *
typeswitch2_helper2 (PFla_op_t *n2, PFalg_type_t t2, void *params) {
    struct typeswitch2_params *p = (struct typeswitch2_params *)params;
    return p->cnst (p->n, n2, p->t, t2, p->params);    
}

static PFla_op_t *
typeswitch2_helper (PFla_op_t *n1, PFalg_type_t t1, void *params) {
    struct typeswitch2_params *p = (struct typeswitch2_params *)params;
    return PFla_typeswitch (p->n, p->count, p->types, typeswitch2_helper2,
                            (struct typeswitch2_params[]) {{
                                .n = n1,
                                .t = t1,
                                .params = p->params,
                                .cnst = p->cnst
                            }});    
}

/**
 * Constructs a typeswitch subtree based on the 
 * algebra type of the item column.
 * Variant with two input relations.
 * @param n1 The first relation used as input for the typeswitch.
 * @param n2 The second relation used as input for the typeswitch.
 * @param count The number of types in the types array
 * @param types The types to distinguish
 * @param cnst  A callback function, called for each type combination 
 *              encounterd in the relations
 * @param params Optional parameters, passed to the callback function.
 */
PFla_op_t *
PFla_typeswitch2 (PFla_op_t *n1, PFla_op_t *n2, unsigned int count, 
                 const PFalg_type_t *types, 
                 PFla_op_t *(*cnst) (PFla_op_t *n1, PFla_op_t *n2,
                                     PFalg_type_t, PFalg_type_t, void *),
                 void *params)
{
    return PFla_typeswitch (n1, count, types, typeswitch2_helper,
                            (struct typeswitch2_params[]) {{
                                .n = n2,
                                .count = count,
                                .types = types,
                                .params = params,
                                .cnst = cnst
                            }});
}

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
 * @param op   Algebra tree node constructor for the algebra operator
 *             that shall be used for this operation. (Pick any of the
 *             construction functions in algebra.c.)
 * @param args Builtin function argument list as in the calling
 *             conventions for algebra implementations in the PFfun_t
 *             struct (field @c alg). (Our implementations for these
 *             functions do not require document representation and/or
 *             the loop relation. They are thus missing here.)
 */
static struct PFla_pair_t
bin_arith (PFalg_simple_type_t t,
           PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                             PFalg_att_t, PFalg_att_t),
           struct PFla_pair_t *args,
           const PFla_op_t *loop,
           bool ordering)
{
    (void) loop; (void) ordering;

    return (struct PFla_pair_t) {
        .rel = project (OP (eqjoin (
                            project (cast (args[0].rel, 
                                           att_cast, att_item, t),
                                     proj (att_iter, att_iter),
                                     proj (att_pos, att_pos),
                                     proj (att_item, att_cast)),
                            project (cast (args[1].rel,
                                           att_cast, att_item, t),
                                     proj (att_iter1, att_iter),
                                     proj (att_item1, att_cast)),
                            att_iter,
                            att_iter1),
                        att_res, att_item, att_item1),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res)),
        .frag = PFla_empty_set () };
}


/**
 * Worker function to construct algebra implementation of unary
 * functions (e.g., fn:not(), op:numeric-unary-plus(),...).
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
 * @param op   Algebra tree node constructor for the algebra operator
 *             that shall be used for this operation. (Pick any of the
 *             construction functions in algebra.c.)
 * @param args Builtin function argument list as in the calling
 *             conventions for algebra implementations in the PFfun_t
 *             struct (field @c alg). (Our implementations for these
 *             functions do not require document representation and/or
 *             the loop relation. They are thus missing here.)
 */
static struct PFla_pair_t
un_func (PFalg_simple_type_t t,
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

/** 
 * Callback function for the typeswitch in bin_arith_typeswitch.
 */
static PFla_op_t *
bin_arith_helper (PFla_op_t *n1,
                  PFla_op_t *n2,
                  PFalg_type_t t1,
                  PFalg_type_t t2,
                  void *params)
{
    /* get the correct type */
    PFalg_type_t t;
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

    return bin_arith (t,
                      (PFla_op_t *(*) (const PFla_op_t *,
                                       PFalg_att_t,
                                       PFalg_att_t,
                                       PFalg_att_t)) params,
                      (struct PFla_pair_t[])
                        { { .rel = n1, .frag = NULL },
                          { .rel = n2, .frag = NULL } },
                      NULL,
                      false).rel;
}

/**
 * Helper function for binary arithmetics with a typeswitch.
 * For every possible combination of input types, bin_arith is
 * called with the appropriate parameters.
 * @see bin_arith()
 */
static struct PFla_pair_t
bin_arith_typeswitch (struct PFla_pair_t *args, 
                      PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t,
                                        PFalg_att_t, PFalg_att_t),
                      const PFla_op_t *loop, bool ordering)
{
    (void) loop; (void) ordering; /* keep compilers quiet */

    PFla_op_t *res = PFla_typeswitch2 (args[0].rel,
                                       args[1].rel,
                                       3,
                                       (PFalg_type_t [3])
                                           { aat_int, aat_dbl, aat_dec },
                                       bin_arith_helper,
                                       OP);
    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
}
/**
 * Algebra implementation for op:numeric-add ()
 * @see bin_arith_typeswitch()
 */
struct PFla_pair_t 
PFbui_op_numeric_add (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    return bin_arith_typeswitch (args, PFla_add, loop, ordering);
}
/**
 * Algebra implementation for op:numeric-subtract ()
 * @see bin_arith_typeswitch()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract (const PFla_op_t *loop, bool ordering,
                           struct PFla_pair_t *args)
{
    return bin_arith_typeswitch (args, PFla_subtract, loop, ordering);
}
/**
 * Algebra implementation for op:numeric-multiply ()
 * @see bin_arith_typeswitch()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply (const PFla_op_t *loop, bool ordering,
                           struct PFla_pair_t *args)
{
    return bin_arith_typeswitch (args, PFla_multiply, loop, ordering);
}
/**
 * Algebra implementation for op:numeric-mod ()
 * @see bin_arith_typeswitch()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo (const PFla_op_t *loop, bool ordering,
                             struct PFla_pair_t *args)
{
    return bin_arith_typeswitch (args, PFla_modulo, loop, ordering);
}
/**
 * Algebra implementation for op:numeric-integer-divide ()
 * @see bin_arith_typeswitch()
 *
 * NB: ($a idiv $b) <=> ($a div $b) cast as xs:integer
 */
struct PFla_pair_t
PFbui_op_numeric_idivide (const PFla_op_t *loop, bool ordering,
                              struct PFla_pair_t *args)
{
    return (struct PFla_pair_t) {
        .rel = project (
                   cast (bin_arith_typeswitch (
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
 * Special typeswitch-callback function for divison.
 * If both types are integer, they are cast to decimal.
 */
static PFla_op_t *
divide_helper (PFla_op_t *n1, PFla_op_t *n2,
                PFalg_type_t t1, PFalg_type_t t2, void *params)
{
    (void) params;

    PFalg_type_t t;
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
    return bin_arith (t,
                      PFla_divide,
                      (struct PFla_pair_t[])
                        { { .rel = n1, .frag = NULL },
                          { .rel = n2, .frag = NULL } },
                      NULL,
                      false).rel;
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

    PFla_op_t *res = PFla_typeswitch2 (args[0].rel, args[1].rel,
                                       3,
                                       (PFalg_type_t [3])
                                         { aat_int, aat_dbl, aat_dec, },
                                       divide_helper,
                                       NULL);
    return (struct PFla_pair_t) { .rel = res, .frag = PFla_empty_set () };
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
 * Build up operator tree for built-in function 'fn:sum ($arg, $zero)'.
 *
 *  env,loop: e1 => (q1,delta1)             env,loop: e2 => (q2,delta2)
 *  -------------------------------------------------------------------
 *                         env,loop: fn:sum(e1, e2) =>
 *  //                                                      \    
 * ||sum_item:(item)/iter (proj_iter,item (cast_item,t(q1))) |
 *  \\                                                      / 
 *                                 U
 *  //                   \                    /                                     \\
 * ||loop \ proj_iter (q1)| |X|(iter, iter1) |proj_iter1:iter,item (cast_item,t(q2)) || X 
 *  \\                   /                    \                                     //
 * pos  
 * ---,
 *  1
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
 * The fn:string function is only a cast to string
 */
struct PFla_pair_t
PFbui_fn_string (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    PFoops (OOPS_FATAL,
            "fn:string not supported for mixed types yet.");

    return (struct PFla_pair_t) {
                .rel  = args[0].rel,
                .frag = args[0].frag };
}

/**
 * The fn:concat function is also available as primitive
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



/**
 * Algebra implementation for op:gt(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_int, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dec, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dbl, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_bln, PFla_gt, args, loop, ordering);
}

/**
 * Algebra implementation for op:gt(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_str, PFla_gt, args, loop, ordering);
}


/**
 * Algebra implementation for op:ge(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_int,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dec,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dbl,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_bln,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}

/**
 * Algebra implementation for op:ge(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_str,
                               PFla_gt,
                               (struct PFla_pair_t []) { args[1], args[0] },
                               loop,
                               ordering));
}


/**
 * Algebra implementation for op:lt(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_int, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dec, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dbl, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_bln, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}

/**
 * Algebra implementation for op:lt(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_str, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] },
                      loop, ordering);
}


/**
 * Algebra implementation for op:le(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_str, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dec, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dbl, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_bln, PFla_gt, args, loop, ordering));
}

/**
 * Algebra implementation for op:le(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_str, PFla_gt, args, loop, ordering));
}


/**
 * Algebra implementation for op:eq(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_int, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dec, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_dbl, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_bln, PFla_eq, args, loop, ordering);
}

/**
 * Algebra implementation for op:eq(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_str, PFla_eq, args, loop, ordering);
}


/**
 * Algebra implementation for op:ne(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_int (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_int, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_dec (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dec, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_dbl (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_dbl, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln,
                    PFla_not,
                    bin_arith (aat_bln, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for op:ne(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_str (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return un_func (aat_bln, 
                    PFla_not,
                    bin_arith (aat_str, PFla_eq, args, loop, ordering));
}

/**
 * Algebra implementation for fn:not (boolean) as boolean
 * @see un_func()
 */
struct PFla_pair_t
PFbui_fn_not_bln (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;
    
    return un_func (aat_bln, PFla_not, args[0]);
}

/**
 * Algebra implementation for op:or (boolean, boolean) as boolean
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_or_bln (const PFla_op_t *loop, bool ordering,
                 struct PFla_pair_t *args)
{
    return bin_arith (aat_bln, PFla_or, args, loop, ordering);
}

/**
 * Algebra implementation for op:and (boolean, boolean) as boolean
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_and_bln (const PFla_op_t *loop, bool ordering,
                  struct PFla_pair_t *args)
{
    return bin_arith (aat_bln, PFla_and, args, loop, ordering);
}

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
    return project (
                    not (
                         eq (
                             attach (
                                     n,
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
    return attach (
                   attach (
                           distinct (
                                     project (
                                              n,
                                              proj (att_iter, att_iter))),
                           att_pos, lit_nat (1)),
                   att_item, lit_bln (true));
}

static PFla_op_t *
fn_boolean_switch (PFla_op_t *n, PFalg_type_t type, void *params)
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

    /* Typeswitch, for helper function see above. */ 
    PFla_op_t *res  = PFla_typeswitch (
                                        args[0].rel, 
                                        8,
                                        (PFalg_type_t [8])
                                        {
                                            aat_node,
                                            aat_bln,
                                            aat_nat,
                                            aat_int,
                                            aat_dbl,
                                            aat_dec,
                                            aat_uA,
                                            aat_str,
                                        }, fn_boolean_switch, NULL);

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
 * Algebra implementation for op:is-same-node (node?, node?)
 * @see bin_arith()
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

/**
 * Algebra implementation for op:node-before (node?, node?)
 * @see bin_arith()
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

/**
 * Algebra implementation for op:node-after (node?, node?)
 * @see bin_arith()
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
            .rel = rownum (distinct,
                           att_pos, sortby (att_item), att_iter),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_iter),
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
            .rel = rownum (distinct,
                           att_pos, sortby (att_item), att_iter),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_iter),
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
    PFla_op_t *distinct;

    (void) loop;

    distinct = distinct (difference (
                             project (args[0].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item)),
                             project (args[1].rel,
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_item))));
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rownum (distinct,
                           att_pos, sortby (att_item), att_iter),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_iter),
            /* result nodes can only originate from first argument */
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
                               disjunion (
                                   count (
                                       project (args[0].rel, 
                                                proj (att_iter, att_iter)),
                                       att_item, att_iter),
                                   attach (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (att_iter, att_iter))),
                                       att_item, lit_int (0))),
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
                             att_pos, att_iter),
                  .frag = args[0].frag };
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
            .rel = rownum (distinct,
                           att_pos, sortby (att_item), att_iter),
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_iter),
            .frag = args[0].frag };
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

/**
 * Built-in function pf:typed-value(attr="text").
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
 * Built-in function pf:typed-value(text {"text"}).
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
 * Built-in function pf:typed-value(processing-instruction {"text"}).
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
 * Built-in function pf:typed-value(comment {"text"}).
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
 * Built-in function pf:typed-value(<code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem (const PFla_op_t *loop, bool ordering,
                            struct PFla_pair_t *args)
{
    PFla_op_t *node_scj, *nodes, *res;

    (void) ordering;

    /* retrieve all descendant textnodes (`/descendant-or-self::text()') */
    node_scj = rownum (
                   scjoin (PFla_set_to_la (args[0].frag),
                           project (args[0].rel,
                                    proj (att_iter, att_iter),
                                    proj (att_item, att_item)),
                           alg_desc_s, PFty_text (),
                           att_iter, att_item, att_item),
                   att_pos, sortby (att_item), att_iter);

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

    /* add empty strings for all empty sequences */
    res = attach (
              disjunion (
                  nodes,
                  attach (
                      difference (
                          loop,
                          project (nodes,
                                   proj (att_iter, att_iter))),
                      att_item, lit_str (""))),
              att_pos, lit_nat (1));

    return (struct PFla_pair_t) {
        .rel  = res,
        .frag = PFla_empty_set () };
}

/**
 * Built-in function pf:typed-value(attribute foo {"text"}, <code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem_attr (const PFla_op_t *loop, bool ordering,
                                 struct PFla_pair_t *args)
{
    PFla_op_t *sel_attr, *sel_node, *attributes, 
              *node_scj, *nodes, *res;

    (void) ordering;

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
    node_scj = rownum (
                   scjoin (PFla_set_to_la (args[0].frag),
                           project (sel_node,
                                    proj (att_iter, att_iter),
                                    proj (att_item, att_item)),
                           alg_desc_s, PFty_text (),
                           att_iter, att_item, att_item),
                   att_pos, sortby (att_item), att_iter);

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

    /* add empty strings for all empty sequences */
    res = attach (
              disjunion (
                  disjunion (attributes, nodes),
                  attach (
                      difference (
                          loop,
                          project (disjunion (attributes, nodes),
                                   proj (att_iter, att_iter))),
                      att_item, lit_str (""))),
              att_pos, lit_nat (1));

    return (struct PFla_pair_t) {
        .rel  = res,
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function '#pf:string-value'.
 */
struct PFla_pair_t
PFbui_pf_string_value (const PFla_op_t *loop, bool ordering,
                       struct PFla_pair_t *args)
{
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`#pf:string-value' is missing.");

    (void) loop; (void) ordering;

    return args[0];
}

/**
 * Build operator tree for built-in function 'fn:data'.
 * It uses pf:string-value() for atomizating nodes.
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
    PFla_op_t *atomics = project (
                                  type_assert_neg (
                                                   select_ (not (type, att_res1, att_res),
                                                            att_res1),
                                                   att_item, node_type),
                                  proj (att_iter, att_iter),
                                  proj (att_pos, att_pos),
                                  proj (att_item, att_item));
    
    /* renumber */
    PFla_op_t *q = number (nodes, att_inner, att_NULL);
    
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
                                    cast(
                                         str_val (
                                                  project (
                                                           q, 
                                                           proj (att_iter, att_inner)), 
                                                  ordering, 
                                                  &str_args).rel, 
                                         att_cast, att_item, aat_uA), 
                                         map, att_iter, att_inner),
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
    return fn_data(PFbui_pf_string_value_elem_attr, aat_node, loop, ordering, args);
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

/**
 * Build up operator tree for built-in function '#pf:typed-value'.
 */
struct PFla_pair_t
PFbui_pf_typed_value (const PFla_op_t *loop, bool ordering,
                      struct PFla_pair_t *args)
{
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`#pf:typed-value' is missing.");

    (void) loop; (void) ordering;

    return args[0];
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
    PFla_op_t *t_nodes = textnode (
                             cast (rel,
                                   att_cast,
                                   att_item,
                                   aat_str),
                             att_res, att_cast);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (
                             project (roots (t_nodes),
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_res)),
                             att_pos, lit_nat (1)),
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

    PFla_op_t *t_nodes = textnode (strings, att_res, att_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = attach (
                             project (roots (t_nodes),
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_res)),
                             att_pos, lit_nat (1)),
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
                            rownum (
                                disjunion (
                                    attach (part1, att_ord, lit_nat (1)),
                                    attach (text.rel, att_ord, lit_nat (2))),
                                att_pos1, sortby (att_ord, att_pos), att_iter),
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
    /*
     * carry out specific type test on type split_type
     * (either aat_pnode or aat_node)
     */
    PFla_op_t *type = type (args[0].rel, att_res, att_item, split_type);

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
    PFla_op_t *t_nodes = textnode (
                             disjunion (
                                 attach (project (strings,
                                                 proj (att_iter, att_iter),
                                                 proj (att_pos, att_pos),
                                                 proj (att_item, att_cast)),
                                        att_ord, lit_nat (1)),
                                 sep),
                                 att_res, att_item);
                         
    /* get the roots of the new text nodes, form union of roots and
     * part1, and sort result on att_pos and att_ord column
     */
    return (struct  PFla_pair_t) {
                 .rel = project (
                            rownum (
                                disjunion (
                                    project (roots (t_nodes),
                                             proj (att_iter, att_iter),
                                             proj (att_pos, att_pos),
                                             proj (att_item, att_res),
                                             proj (att_ord, att_ord)),
                                    attach (part1, att_ord, lit_nat (1))),
                                att_pos1, sortby (att_pos, att_ord), att_iter),
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

struct PFla_pair_t
PFbui_fn_unordered (const PFla_op_t *loop, bool ordering,
                    struct PFla_pair_t *args)
{
    (void) loop; (void) ordering;

    /*
     * project out pos column
     */
    return (struct PFla_pair_t) {
        .rel  = number (
                    project (args[0].rel,
                             proj (att_iter, att_iter),
                             proj (att_item, att_item)),
                    att_pos, att_iter),
        .frag = PFla_empty_set () };
}

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

/* vim:set shiftwidth=4 expandtab: */
