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
 *          Sabine Mayer <mayers@inf.uni-konstanz.de>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#include "builtins.h"


/**
 * Worker function to construct algebra implementation of binary
 * arithmetic functions (e.g., op:numeric-add(), op:numeric-subtract(),...).
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
 * (where OP is the algebra operator that implements the arithmetic
 * function, and t is the implementation type that is used)
 *
 * @param t    Algebra type that should be used for this operator.
 *             All arguments will be cast to that type before invoking
 *             the actual algebra operator. This makes arithmetic
 *             operations always monomorphic (and thus reasonably
 *             efficient).
 * @param op   Algebra tree node constructor for the algebra operator
 *             that shall be used for this operation. (Pick any of the
 *             construction functions in algebra.c.)
 * @param args Builtin function argument list as in the calling
 *             conventions for algebra implementations in the PFfun_t
 *             struct (field @c alg). (Our implementations for arithmetic
 *             functions do not require document representation and/or
 *             the loop relation. They are thus missing here.)
 */
static PFalg_op_t *
bin_arith (PFalg_simple_type_t t,
           PFalg_op_t *(*OP) (PFalg_op_t *, PFalg_att_t,
                             PFalg_att_t, PFalg_att_t),
           PFalg_op_t **args)
{
    return project (OP (eqjoin (cast (args[0], "item", t),
                                project (cast (args[1], "item", t),
                                         proj ("iter1", "iter"),
                                         proj ("item1", "item")),
                                "iter",
                                "iter1"),
                        "res", "item", "item1"),
                    proj ("iter", "iter"),
                    proj ("pos", "pos"),
                    proj ("item", "res"));
}


/**
 * Algebra implementation for op:numeric-add(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_add_int (PFalg_op_t *loop __attribute__((unused)),
                          PFalg_op_t **delta __attribute__((unused)),
                          PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_add, args);
}

/**
 * Algebra implementation for op:numeric-add(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_add_dec (PFalg_op_t *loop __attribute__((unused)),
                          PFalg_op_t **delta __attribute__((unused)),
                          PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_add, args);
}

/**
 * Algebra implementation for op:numeric-add(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_add_dbl (PFalg_op_t *loop __attribute__((unused)),
                          PFalg_op_t **delta __attribute__((unused)),
                          PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_add, args);
}


/**
 * Algebra implementation for op:numeric-subtract(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_subtract_int (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_subtract, args);
}

/**
 * Algebra implementation for op:numeric-subtract(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_subtract_dec (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_subtract, args);
}

/**
 * Algebra implementation for op:numeric-subtract(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_subtract_dbl (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_subtract, args);
}


/**
 * Algebra implementation for op:numeric-multiply(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_multiply_int (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_multiply, args);
}

/**
 * Algebra implementation for op:numeric-multiply(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_multiply_dec (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_multiply, args);
}

/**
 * Algebra implementation for op:numeric-multiply(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_multiply_dbl (PFalg_op_t *loop __attribute__((unused)),
                               PFalg_op_t **delta __attribute__((unused)),
                               PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_multiply, args);
}

/**
 * Algebra implementation for op:numeric-divide(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_divide_int (PFalg_op_t *loop __attribute__((unused)),
                             PFalg_op_t **delta __attribute__((unused)),
                             PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_divide, args);
}

/**
 * Algebra implementation for op:numeric-divide(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_divide_dec (PFalg_op_t *loop __attribute__((unused)),
                             PFalg_op_t **delta __attribute__((unused)),
                             PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_divide, args);
}

/**
 * Algebra implementation for op:numeric-divide(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_numeric_divide_dbl (PFalg_op_t *loop __attribute__((unused)),
                             PFalg_op_t **delta __attribute__((unused)),
                             PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_divide, args);
}


/**
 * Algebra implementation for op:gt(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_gt_int (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_gt, args);
}

/**
 * Algebra implementation for op:gt(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_gt_dec (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_gt, args);
}

/**
 * Algebra implementation for op:gt(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_gt_dbl (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_gt, args);
}

/**
 * Algebra implementation for op:gt(boolean?,boolean?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_gt_bln (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_bln, PFalg_gt, args);
}

/**
 * Algebra implementation for op:gt(string?,string?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_gt_str (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_str, PFalg_gt, args);
}


/**
 * Algebra implementation for op:lt(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_lt_int (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_gt, (PFalg_op_t *[]) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_lt_dec (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_gt, (PFalg_op_t *[]) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_lt_dbl (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_gt, (PFalg_op_t *[]) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(boolean?,boolean?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_lt_bln (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_bln, PFalg_gt, (PFalg_op_t *[]) { args[1], args[0] });
}

/**
 * Algebra implementation for op:lt(string?,string?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_lt_str (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_str, PFalg_gt, (PFalg_op_t *[]) { args[1], args[0] });
}


/**
 * Algebra implementation for op:eq(integer?,integer?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_eq_int (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_int, PFalg_eq, args);
}

/**
 * Algebra implementation for op:eq(decimal?,decimal?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_eq_dec (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dec, PFalg_eq, args);
}

/**
 * Algebra implementation for op:eq(double?,double?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_eq_dbl (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_dbl, PFalg_eq, args);
}

/**
 * Algebra implementation for op:eq(boolean?,boolean?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_eq_bln (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_bln, PFalg_eq, args);
}

/**
 * Algebra implementation for op:eq(string?,string?)
 * @see bin_arith()
 */
PFalg_op_t *
PFbui_op_eq_str (PFalg_op_t *loop __attribute__((unused)),
                 PFalg_op_t **delta __attribute__((unused)),
                 PFalg_op_t **args)
{
    return bin_arith (aat_str, PFalg_eq, args);
}


/**
 * Algebra implementation for <code>fn:boolean (xs:boolean)</code>.
 *
 * If the operand is a single boolean value, the function returns
 * the boolean value itself.
 */
PFalg_op_t *
PFbui_fn_boolean_bool (PFalg_op_t *loop __attribute ((unused)),
                       PFalg_op_t **delta __attribute__((unused)),
                       PFalg_op_t **args)
{
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
 * ( q1 U
 *   /                                                               pos|item\
 *  |loop \ (proj_iter (q1 |X| (iter,iter1) proj_iter1:iter loop)) X ---+---- |
 *   \                                                                1 | fal/
 * ,
 *   delta1
 * )
 */
PFalg_op_t *
PFbui_fn_boolean_optbool (PFalg_op_t *loop __attribute ((unused)),
                          PFalg_op_t **delta __attribute__((unused)),
                          PFalg_op_t **args)
{
    return
        disjunion (
            args[0],
            cross (
                difference (
                    loop,
                    project (eqjoin (args[0],
                                     project (loop, proj ("iter1", "iter")),
                                     "iter", "iter1"),
                             proj ("iter", "iter"))
                    ),
                lit_tbl (attlist ("pos", "item"),
                         tuple (lit_nat (1), lit_bln (false)))));
}

/**
 * @bug FIXME: This needs to be changed. It's just to far off the XQuery
 *      semantics.
 */
PFalg_op_t *
PFbui_fn_boolean_item (PFalg_op_t *loop __attribute ((unused)),
                       PFalg_op_t **delta __attribute__((unused)),
                       PFalg_op_t **args)
{
    return
        cross (
            disjunion (
                cross (
                    distinct (project (args[0], proj ("iter", "iter"))),
                    lit_tbl (attlist ("item"), tuple (lit_bln (true)))),
                cross (
                    difference (
                        loop,
                        project (args[0], proj ("iter", "iter"))),
                    lit_tbl (attlist ("item"), tuple (lit_bln (false))))),
            lit_tbl (attlist ("pos"), tuple (lit_nat (1))));
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
PFalg_op_t *
PFbui_fn_empty (PFalg_op_t *loop __attribute ((unused)),
                PFalg_op_t **delta __attribute__((unused)),
                PFalg_op_t **args)
{
    return
        cross (
            disjunion (
                cross (
                    distinct (project (args[0], proj ("iter", "iter"))),
                    lit_tbl (attlist ("item"), tuple (lit_bln (false)))),
                cross (
                    difference (
                        loop,
                        project (args[0], proj ("iter", "iter"))),
                    lit_tbl (attlist ("item"), tuple (lit_bln (true))))),
            lit_tbl (attlist ("pos"), tuple (lit_nat (1))));
}


/**
 * Build up operator tree for built-in function 'fn:typed-value'.
 */
PFalg_op_t *
PFbui_op_typed_value (PFalg_op_t *loop __attribute__((unused)),
                      PFalg_op_t **delta __attribute__((unused)),
                      PFalg_op_t **args)
{
    return args[0];
}
