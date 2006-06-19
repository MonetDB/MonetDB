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
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
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
           struct PFla_pair_t *args)
{
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
	 PFla_op_t *(*OP) (const PFla_op_t *, PFalg_att_t, PFalg_att_t),
	 struct PFla_pair_t *args)
{
    return (struct PFla_pair_t) {
	.rel = project (OP (project (cast (args[0].rel, att_cast, att_item, t),
                                 proj (att_iter, att_iter),
                                 proj (att_pos, att_pos),
                                 proj (att_item, att_cast)),
                        att_res, att_item),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_res)),
	.frag = PFla_empty_set () };
}


/**
 * Algebra implementation for op:numeric-add(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_add_int (const PFla_op_t *loop __attribute__((unused)),
                          bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_add, args);
}

/**
 * Algebra implementation for op:numeric-add(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_add_dec (const PFla_op_t *loop __attribute__((unused)),
                          bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_add, args);
}

/**
 * Algebra implementation for op:numeric-add(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_add_dbl (const PFla_op_t *loop __attribute__((unused)),
                          bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_add, args);
}


/**
 * Algebra implementation for op:numeric-subtract(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract_int (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_subtract, args);
}

/**
 * Algebra implementation for op:numeric-subtract(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract_dec (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_subtract, args);
}

/**
 * Algebra implementation for op:numeric-subtract(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_subtract_dbl (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_subtract, args);
}


/**
 * Algebra implementation for op:numeric-multiply(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply_int (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_multiply, args);
}

/**
 * Algebra implementation for op:numeric-multiply(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply_dec (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_multiply, args);
}

/**
 * Algebra implementation for op:numeric-multiply(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_multiply_dbl (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_multiply, args);
}


/**
 * Algebra implementation for op:numeric-divide(decimal?,decimal?)
 * @see bin_arith()
 *
 * NB: A function for the division of two integer operators is required
 * because, according to the XQuery specifications, the division of two
 * integers returns a decimal number, i.e. we let the two operands be
 * promoted to decimal and use "PFbui_op_numeric_divide_dec".     
 */
struct PFla_pair_t
PFbui_op_numeric_divide_dec (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
                             struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_divide, args);
}

/**
 * Algebra implementation for op:numeric-divide(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_divide_dbl (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
                             struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_divide, args);
}


/**
 * Algebra implementation for op:numeric-integer-divide(integer?,integer?)
 * @see bin_arith()
 *
 * NB: ($a idiv $b) <=> ($a div $b) cast as xs:integer
 */
struct PFla_pair_t
PFbui_op_numeric_idivide_int (const PFla_op_t *loop __attribute__((unused)),
                              bool ordering,
                              struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return (struct PFla_pair_t) {
	.rel = project (cast (bin_arith (aat_int, PFla_divide, args).rel,
		                  att_cast, att_item, aat_int),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_cast)),
	.frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-integer-divide(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_idivide_dec (const PFla_op_t *loop __attribute__((unused)),
                              bool ordering,
                              struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return (struct PFla_pair_t) {
	.rel = project (cast (bin_arith (aat_dec, PFla_divide, args).rel,
		                  att_cast, att_item, aat_int),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_cast)),
	.frag = PFla_empty_set () };
}

/**
 * Algebra implementation for op:numeric-integer-divide(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_idivide_dbl (const PFla_op_t *loop __attribute__((unused)),
                              bool ordering,
                              struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return (struct PFla_pair_t) {
	.rel = project (cast (bin_arith (aat_dbl, PFla_divide, args).rel,
		                  att_cast, att_item, aat_int),
                    proj (att_iter, att_iter),
                    proj (att_pos, att_pos),
                    proj (att_item, att_cast)),
	.frag = PFla_empty_set () };
}


/**
 * Algebra implementation for op:numeric-mod(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo_int (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
                             struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_modulo, args);
}

/**
 * Algebra implementation for op:numeric-mod(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo_dec (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
                             struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_modulo, args);
}

/**
 * Algebra implementation for op:numeric-mod(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_numeric_modulo_dbl (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
                             struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_modulo, args);
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
PFbui_fn_count (const PFla_op_t *loop __attribute__((unused)),
                bool ordering,
                struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    PFla_op_t *count = count (project (args[0].rel, 
                                       proj (att_iter, att_iter)),
                              att_item, att_iter);

    return (struct PFla_pair_t) {
        .rel = cross (
                disjunion (
                    count,
                    cross (
                        difference (
                            loop,
                            project (count, proj (att_iter, att_iter))),
                        lit_tbl (attlist (att_item), tuple (lit_int (0))))),
                lit_tbl (attlist (att_pos), tuple (lit_nat (1)))),
        .frag = PFla_empty_set () };
}

/**
 * The fn:string function is only a cast to string
 */
struct PFla_pair_t
PFbui_fn_string (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_fn_concat (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return (struct PFla_pair_t) {
                .rel = project (
                           fn_concat (
                               eqjoin (args[0].rel,
                                       project (args[1].rel,
                                                proj (att_iter1, att_iter),
                                                proj (att_item1, att_item)),
                                       att_iter,
                                       att_iter1),
                               att_res, att_item, att_item1),
                        proj (att_iter, att_iter),
                        proj (att_pos, att_pos),
                        proj (att_item, att_res)),
                .frag = args[0].frag };
}

/**
 * The fn:string_join function is also available as primitive
 */
struct PFla_pair_t
PFbui_fn_string_join (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
                      struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return (struct PFla_pair_t) {
		        .rel  = cross (
                            fn_string_join (args[0].rel,
                                            project (
                                                args[1].rel,
                                                proj (att_iter, att_iter),
                                                proj (att_item, att_item))),
                            lit_tbl (attlist (att_pos), tuple (lit_nat (1)))),
                .frag = args[0].frag };
}



/**
 * Algebra implementation for op:gt(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_gt, args);
}

/**
 * Algebra implementation for op:gt(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_gt, args);
}

/**
 * Algebra implementation for op:gt(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_gt, args);
}

/**
 * Algebra implementation for op:gt(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_bln, PFla_gt, args);
}

/**
 * Algebra implementation for op:gt(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_gt_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_str, PFla_gt, args);
}


/**
 * Algebra implementation for op:ge(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_int, PFla_gt,
                  (struct PFla_pair_t []) { args[1], args[0] }) });
}

/**
 * Algebra implementation for op:ge(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dec, PFla_gt,
                  (struct PFla_pair_t []) { args[1], args[0] }) });
}

/**
 * Algebra implementation for op:ge(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dbl, PFla_gt,
                  (struct PFla_pair_t []) { args[1], args[0] }) });
}

/**
 * Algebra implementation for op:ge(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_bln, PFla_gt,
                  (struct PFla_pair_t []) { args[1], args[0] }) });
}

/**
 * Algebra implementation for op:ge(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ge_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_str, PFla_gt,
                  (struct PFla_pair_t []) { args[1], args[0] }) });
}


/**
 * Algebra implementation for op:lt(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return bin_arith (aat_int, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return bin_arith (aat_dec, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return bin_arith (aat_dbl, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_bln, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_lt_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return bin_arith (aat_str, PFla_gt,
                      (struct PFla_pair_t []) { args[1], args[0] });
}


/**
 * Algebra implementation for op:le(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_int, PFla_gt, args) });
}

/**
 * Algebra implementation for op:le(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dec, PFla_gt, args) });
}

/**
 * Algebra implementation for op:le(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dbl, PFla_gt, args) });
}

/**
 * Algebra implementation for op:le(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_bln, PFla_gt, args) });
}

/**
 * Algebra implementation for op:le(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_le_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_str, PFla_gt, args) });
}


/**
 * Algebra implementation for op:eq(integer?,integer?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_int, PFla_eq, args);
}

/**
 * Algebra implementation for op:eq(decimal?,decimal?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dec, PFla_eq, args);
}

/**
 * Algebra implementation for op:eq(double?,double?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_dbl, PFla_eq, args);
}

/**
 * Algebra implementation for op:eq(boolean?,boolean?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_bln, PFla_eq, args);
}

/**
 * Algebra implementation for op:eq(string?,string?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_eq_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_str, PFla_eq, args);
}


/**
 * Algebra implementation for op:ne(integer?,integer?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_int (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_int, PFla_eq, args) });
}

/**
 * Algebra implementation for op:ne(decimal?,decimal?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_dec (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dec, PFla_eq, args) });
}

/**
 * Algebra implementation for op:ne(double?,double?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_dbl (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_dbl, PFla_eq, args) });
}

/**
 * Algebra implementation for op:ne(boolean?,boolean?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    
    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_bln, PFla_eq, args) });
}

/**
 * Algebra implementation for op:ne(string?,string?)
 * @see un_func() and bin_arith()
 */
struct PFla_pair_t
PFbui_op_ne_str (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return un_func (aat_bln, PFla_not,
        (struct PFla_pair_t []) { bin_arith (aat_str, PFla_eq, args) });
}

/**
 * Algebra implementation for fn:not (boolean) as boolean
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_fn_not_bln (const PFla_op_t *loop __attribute__((unused)),
                  bool ordering,
                  struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return un_func (aat_bln, PFla_not, args);
}

/**
 * Algebra implementation for op:or (boolean, boolean) as boolean
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_or_bln (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
                 struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_bln, PFla_or, args);
}

/**
 * Algebra implementation for op:and (boolean, boolean) as boolean
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_and_bln (const PFla_op_t *loop __attribute__((unused)),
                  bool ordering,
		  struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    return bin_arith (aat_bln, PFla_and, args);
}

/**
 * Algebra implementation for <code>fn:boolean (xs:boolean)</code>.
 *
 * If the operand is a single boolean value, the function returns
 * the boolean value itself.
 */
struct PFla_pair_t
PFbui_fn_boolean_bln (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
                       struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_fn_boolean_optbln (const PFla_op_t *loop __attribute__((unused)),
                         bool ordering,
                         struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
        .rel = disjunion (
                   args[0].rel,
                   cross (
			           difference (
                           loop,
					       project (
                               args[0].rel,
						       proj (att_iter, att_iter))),
			           lit_tbl (attlist (att_pos, att_item),
				                tuple (lit_nat (1), lit_bln (false))))),
    	.frag = PFla_empty_set () };
}

/**
 * @bug FIXME: This needs to be changed. It's just to far off the XQuery
 *      semantics.
 */
struct PFla_pair_t
PFbui_fn_boolean_item (const PFla_op_t *loop __attribute__((unused)),
                       bool ordering,
                       struct PFla_pair_t *args)
{
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`fn:boolean' is missing.");

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
        .rel = cross (
                   disjunion (
                       cross (
                           distinct (project (args[0].rel,
                              proj (att_iter, att_iter))),
                           lit_tbl (attlist (att_item),
                                    tuple (lit_bln (true)))),
                       cross (
                           difference (
                               loop,
                               project (args[0].rel,
                                        proj (att_iter, att_iter))),
                           lit_tbl (attlist (att_item),
                                    tuple (lit_bln (false))))),
                   lit_tbl (attlist (att_pos), tuple (lit_nat (1)))),
        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:contains (xs:string, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains (const PFla_op_t *loop __attribute__((unused)),
                   bool ordering,
                   struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
        .rel = project (
                   fn_contains (
                       eqjoin (args[0].rel,
                               project (args[1].rel,
                                        proj (att_iter1, att_iter),
                                        proj (att_item1, att_item)),
                               att_iter,
                               att_iter1),
                       att_res, att_item, att_item1),
                proj (att_iter, att_iter),
                proj (att_pos, att_pos),
                proj (att_item, att_res)),
        .frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for <code>fn:contains (xs:string?, xs:string)</code>.
 */
struct PFla_pair_t
PFbui_fn_contains_opt (const PFla_op_t *loop __attribute__((unused)),
                       bool ordering,
                       struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
        .rel = project (
                   fn_contains (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               cross (
                                   difference (
                                       loop,
                                       project (
                                           args[0].rel,
                                           proj (att_iter, att_iter))),
                                   lit_tbl (attlist (att_pos, att_item),
                                            tuple (lit_nat (1),
                                                   lit_str (""))))),
                           project (args[1].rel,
                                    proj (att_iter1, att_iter),
                                    proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       att_res, att_item, att_item1),
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
PFbui_fn_contains_opt_opt (const PFla_op_t *loop __attribute__((unused)),
                           bool ordering,
                           struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
        .rel = project (
                   fn_contains (
                       eqjoin (
                           disjunion (
                               args[0].rel,
                               cross (
                                   difference (
                                       loop,
                                       project (
                                           args[0].rel,
                                           proj (att_iter, att_iter))),
                                   lit_tbl (attlist (att_pos, att_item),
                                            tuple (lit_nat (1),
                                                   lit_str (""))))),
                           project (
                               disjunion (
                                   args[1].rel,
                                   cross (
                                       difference (
                                           loop,
                                           project (
                                               args[1].rel,
                                               proj (att_iter, att_iter))),
                                       lit_tbl (attlist (att_pos, att_item),
                                                tuple (lit_nat (1),
                                                       lit_str (""))))),
                               proj (att_iter1, att_iter),
                               proj (att_item1, att_item)),
                           att_iter,
                           att_iter1),
                       att_res, att_item, att_item1),
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
PFbui_fn_empty (const PFla_op_t *loop __attribute__((unused)),
                bool ordering,
                struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return (struct PFla_pair_t) {
	.rel = cross (
               disjunion (
                   cross (
                       distinct (project (args[0].rel,
                                 proj (att_iter, att_iter))),
                       lit_tbl (attlist (att_item),
                                tuple (lit_bln (false)))),
                   cross (
                       difference (
                           loop,
                           project (args[0].rel,
                                    proj (att_iter, att_iter))),
                       lit_tbl (attlist (att_item),
                                tuple (lit_bln (true))))),
               lit_tbl (attlist (att_pos), tuple (lit_nat (1)))),
	.frag = PFla_empty_set ()};
}

/**
 * Algebra implementation for op:is-same-node (node?, node?)
 * @see bin_arith()
 */
struct PFla_pair_t
PFbui_op_is_same_node (const PFla_op_t *loop __attribute__((unused)),
                       bool ordering,
                       struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_op_node_before (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
                      struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_op_node_after (const PFla_op_t *loop __attribute__((unused)),
                     bool ordering,
                     struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_op_union (const PFla_op_t *loop __attribute__((unused)),
                bool ordering,
		struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                           att_pos, sortby (att_item), att_NULL),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_NULL),
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
PFbui_op_intersect (const PFla_op_t *loop __attribute__((unused)),
                    bool ordering,
                    struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                           att_pos, sortby (att_item), att_NULL),
            .frag = PFla_set_union (args[0].frag, args[1].frag) };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_NULL),
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
PFbui_op_except (const PFla_op_t *loop __attribute__((unused)),
                 bool ordering,
		 struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                           att_pos, sortby (att_item), att_NULL),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_NULL),
            /* result nodes can only originate from first argument */
            .frag = args[0].frag };
}

/**
 * The fn:exactly-one function checks at runtime the sequence 
 * length of the input relation and triggers an error.
 */
struct PFla_pair_t
PFbui_fn_exactly_one (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
                      struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    PFla_op_t *count = eq (cross (
                               disjunion (
                                   count (
                                       project (args[0].rel, 
                                                proj (att_iter, att_iter)),
                                       att_item, att_iter),
                                   cross (
                                       difference (
                                           loop,
                                           project (
                                               args[0].rel,
                                               proj (att_iter, att_iter))),
                                       lit_tbl (attlist (att_item),
                                                tuple (lit_int (0))))),
                               lit_tbl (attlist (att_item1),
                                        tuple (lit_int (1)))),
                           att_res, att_item1, att_item); 

    char *err_string = "err:FORG0005, fn:exactly-one called with "
                       "a sequence containing zero or more than one item.";
    
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
PFbui_fn_zero_or_one (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
			     struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    PFla_op_t *count = eq (cross (
                               count (
                                   project (args[0].rel, 
                                            proj (att_iter, att_iter)),
                                   att_item, att_iter),
                               lit_tbl (attlist (att_item1),
                                        tuple (lit_int (1)))),
                           att_res, att_item1, att_item); 

    char *err_string = "err:FORG0003, fn:zero-or-one called with "
                       "a sequence containing more than one item.";
    
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_fn_distinct_values (const PFla_op_t *loop __attribute__((unused)),
                          bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_pf_distinct_doc_order (const PFla_op_t *loop __attribute__((unused)),
                             bool ordering,
			     struct PFla_pair_t *args)
{
    PFla_op_t *distinct;

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    distinct = distinct (
                   project (args[0].rel,
                        proj (att_iter, att_iter),
                        proj (att_item, att_item)));
    
    if (ordering)
        return (struct  PFla_pair_t) {
            .rel = rownum (distinct,
                           att_pos, sortby (att_item), att_NULL),
            .frag = args[0].frag };
    else
        return (struct  PFla_pair_t) {
            .rel = number (distinct, att_pos, att_NULL),
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
PFbui_fn_doc (const PFla_op_t *loop __attribute__((unused)),
              bool ordering,
              struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    PFla_op_t *doc = doc_tbl (project (args[0].rel,
                                       proj (att_iter, att_iter),
                                       proj (att_item, att_item)));

    return (struct PFla_pair_t) {
        .rel  = cross (lit_tbl (attlist (att_pos), tuple (lit_nat (1))),
                       roots (doc)),
        .frag = PFla_set (fragment (doc)) };
}

/**
 * Built-in function pf:typed-value(attr="text").
 */
struct PFla_pair_t
PFbui_pf_string_value_attr (const PFla_op_t *loop __attribute__((unused)),
                            bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_pf_string_value_text (const PFla_op_t *loop __attribute__((unused)),
                            bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_pf_string_value_pi (const PFla_op_t *loop __attribute__((unused)),
                          bool ordering,
                          struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_pf_string_value_comm (const PFla_op_t *loop __attribute__((unused)),
                            bool ordering,
                            struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
PFbui_pf_string_value_elem (const PFla_op_t *loop __attribute__((unused)),
                            bool ordering,
                            struct PFla_pair_t *args)
{
    PFla_op_t *node_scj, *nodes, *res;

    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    /* retrieve all descendant textnodes (`/descendant-or-self::text()') */
    node_scj = rownum (
                   scjoin (PFla_set_to_la (args[0].frag),
                           project (args[0].rel,
                                    proj (att_iter, att_iter),
                                    proj (att_item, att_item)),
                           alg_desc_s, PFty_text ()),
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
                    cross (loop,
                           lit_tbl( attlist (att_pos, att_item),
                                    tuple (lit_nat (1),
                                           lit_str ("")))),
                    proj (att_iter, att_iter),
                    proj (att_item, att_item)));

    /* add empty strings for all empty sequences */
    res = cross (
              disjunion (
                  nodes,
                  cross (
                      difference (
                          loop,
                          project (nodes,
                                   proj (att_iter, att_iter))),
                      lit_tbl (attlist (att_item), 
                               tuple (lit_str (""))))),
              lit_tbl (attlist (att_pos), tuple (lit_nat (1))));

    return (struct PFla_pair_t) {
        .rel  = res,
        .frag = PFla_empty_set () };
}

/**
 * Built-in function pf:typed-value(attribute foo {"text"}, <code>text</code>).
 */
struct PFla_pair_t
PFbui_pf_string_value_elem_attr (const PFla_op_t *loop __attribute__((unused)),
                                 bool ordering,
                                 struct PFla_pair_t *args)
{
    PFla_op_t *sel_attr, *sel_node, *attributes, 
              *node_scj, *nodes, *res;

    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                           alg_desc_s, PFty_text ()),
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
                    cross (loop,
                           lit_tbl( attlist (att_pos, att_item),
                                    tuple (lit_nat (1),
                                           lit_str ("")))),
                    proj (att_iter, att_iter),
                    proj (att_item, att_item)));

    /* add empty strings for all empty sequences */
    res = cross (
              disjunion (
                  disjunion (attributes, nodes),
                  cross (
                      difference (
                          loop,
                          project (disjunion (attributes, nodes),
                                   proj (att_iter, att_iter))),
                      lit_tbl (attlist (att_item), 
                               tuple (lit_str (""))))),
              lit_tbl (attlist (att_pos), tuple (lit_nat (1))));

    return (struct PFla_pair_t) {
        .rel  = res,
        .frag = PFla_empty_set () };
}

/**
 * Build up operator tree for built-in function '#pf:string-value'.
 */
struct PFla_pair_t
PFbui_pf_string_value (const PFla_op_t *loop __attribute__((unused)),
                       bool ordering,
                       struct PFla_pair_t *args)
{
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`#pf:string-value' is missing.");

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return args[0];
}

/**
 * Build up operator tree for built-in function '#pf:typed-value'.
 */
struct PFla_pair_t
PFbui_pf_typed_value (const PFla_op_t *loop __attribute__((unused)),
                      bool ordering,
                      struct PFla_pair_t *args)
{
    PFoops (OOPS_FATAL,
            "Algebra implementation for function "
            "`#pf:typed-value' is missing.");

    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */


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
    (void) loop; /* pacify picky compilers that do not understand
                    "__attribute__((unused))" */

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
                 .rel  = cross (
                             project (roots (t_nodes),
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_res)),
                             lit_tbl (attlist (att_pos),
                                      tuple (lit_nat (1)))),
                 /* union of those nodes we had in the very beginning
                  * (those in frag) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
					                     PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_single_atomic
    (const PFla_op_t *loop __attribute__((unused)),
     bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                                 cross (loop,
                                        lit_tbl( attlist (att_pos, att_item),
                                                 tuple (lit_nat (1),
                                                        lit_str (" ")))),
                                 proj (att_iter, att_iter),
                                 proj (att_item, att_item)));

    PFla_op_t *t_nodes = textnode (strings, att_res, att_item);

    /* get the roots of the new text nodes and add pos column */
    return (struct  PFla_pair_t) {
                 .rel  = cross (
                             project (roots (t_nodes),
                                      proj (att_iter, att_iter),
                                      proj (att_item, att_res)),
                             lit_tbl (attlist (att_pos),
                                      tuple (lit_nat (1)))),
                 /* union of those nodes we had in the very beginning
                  * (those in part1) and those produced by text node
                  * creation
                  */
                 .frag = PFla_set_union (frag,
                                         PFla_set (fragment (t_nodes)))};
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_atomic
    (const PFla_op_t *loop __attribute__((unused)),
     bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
                                    cross (part1,
                                           lit_tbl (attlist (att_ord),
                                                    tuple (lit_nat (1)))),
                                    cross (text.rel,
                                           lit_tbl (attlist (att_ord),
                                                    tuple (lit_nat (2))))),
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
    (const PFla_op_t *loop __attribute__((unused)),
     bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    return pf_item_seq_to_node_seq_worker_attr (
               loop, args[0].rel, args[0].frag,
               pf_item_seq_to_node_seq_worker_single_atomic);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq_attr
    (const PFla_op_t *loop __attribute__((unused)),
     bool ordering,
     struct PFla_pair_t *args)
{
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
    PFla_op_t *base = subtract (
                          cast (
                              cross (strings,
                                  lit_tbl (attlist (att_item1),
                                           tuple (lit_int (1)))),
                              att_pos1, att_pos, aat_int),
                          att_res, att_pos1, att_item1);

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
    PFla_op_t *sep = cross (
                         cross (delim,
                                lit_tbl (attlist (att_item),
                                         tuple (lit_str (" ")))),
                         lit_tbl (attlist (att_ord),
                                  tuple (lit_nat (2))));

    /*
     * create textnodes for each string
     * (casted atomic values and whitespace strings)
     */
    PFla_op_t *t_nodes = textnode (
                             disjunion (
                                 cross (project (strings,
                                                 proj (att_iter, att_iter),
                                                 proj (att_pos, att_pos),
                                                 proj (att_item, att_cast)),
                                        lit_tbl (attlist (att_ord),
                                                 tuple (lit_nat (1)))),
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
                                    cross (part1,
                                           lit_tbl (attlist (att_ord),
                                                    tuple (lit_nat (1))))),
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
    (const PFla_op_t *loop __attribute__((unused)),
     bool ordering,
     struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    /*
     * translate is2ns function using its worker
     */
    return pf_item_seq_to_node_seq_worker (args, aat_pnode);
}

struct PFla_pair_t
PFbui_pf_item_seq_to_node_seq (const PFla_op_t *loop __attribute__((unused)),
                               bool ordering,
                               struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

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
        const PFla_op_t *loop __attribute__((unused)),
        bool ordering,
        struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    PFla_op_t *merged
        = merge_adjacent (PFla_set_to_la (args[0].frag), args[0].rel);

    return (struct  PFla_pair_t) {
                 .rel  = roots (merged),
                 /* form union of old and new fragment */
                 .frag = PFla_set_union (args[0].frag,
                                         PFla_set (fragment (merged))) };
}

struct PFla_pair_t
PFbui_fn_unordered (const PFla_op_t *loop __attribute__((unused)),
                    bool ordering,
			        struct PFla_pair_t *args)
{
    (void) loop;      /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */
    (void) ordering;  /* pacify picky compilers that do not understand
                         "__attribute__((unused))" */

    /*
     * project out pos column
     */
    return (struct PFla_pair_t) {
        .rel  = number (
                    project (args[0].rel,
                             proj (att_iter, att_iter),
                             proj (att_item, att_item)),
                    att_pos, att_NULL),
        .frag = PFla_empty_set () };
}
/* vim:set shiftwidth=4 expandtab: */
