/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's representation of the built-in XQuery Functions & Operators
 * (XQuery F&O) [1].  We maintain a table of built-in F&O which may be
 * loaded into Pathfinder's function environment via #PFfun_xquery_fo ().
 *
 * References
 *
 * [1] XQuery 1.0 and XPath 2.0 Functions and Operators W3C Working
 *     Draft, 15 November 2002, see http://www.w3.org/TR/xquery-operators/.
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

/* always include pathfinder.h first! */
#include "pathfinder.h"

#include <assert.h>

#include "xquery_fo.h"

#include "ns.h"
#include "env.h"
#include "types.h"
#include "functions.h"
#include "builtins.h"

/**
 * maxmimum number of signatures for one function
 */
#define XQUERY_FO_MAX_SIGS 10

/**
 * Load XQuery built-in function signatures into function environment.
 */
void
PFfun_xquery_fo (void)
{
    struct {
        PFns_t ns;
        char *loc;
        unsigned int arity;
        unsigned int sig_count;
        PFfun_sig_t sigs[XQUERY_FO_MAX_SIGS];
        struct PFla_pair_t (*alg) (const struct PFla_op_t *,
                                   bool,
                                   struct PFla_pair_t *);
    } xquery_fo[] =
    /**
     * List all XQuery built-in functions here.
     *
     * Be aware that order is significant here:
     *
     *  - The first declaration that is found here that matches an
     *    application's argument types will be chosen
     *    (see semantics/typecheck.brg:overload; This allows us to
     *    give more specific and optimized implementations if we
     *    like.).
     *
     *  - Make sure that the *last* function listed is always the
     *    most generic variant (with a signature as stated in the
     *    W3C drafts). The parameter types of the last function (with
     *    correct number of arguments) will decide the function
     *    conversion (XQuery WD 3.1.5) rule to apply.
     *
     *  - The order of different functions reflects the order
     *    in the XQuery 1.0 and XPath 2.0 Functions and Operators
     *    recommendation (see http://www.w3.org/TR/xpath-functions/).
     */
    {
        
/* 2. ACCESSORS */
/* 2.3. fn:string */
      /* fn:string () as string */
      { .ns = PFns_fn, .loc = "string",
        .arity = 0, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_none () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_string}
    , /* fn:string (item?) as string */
      { .ns = PFns_fn, .loc = "string",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_item ()) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_string}
    
/* 2.4. fn:data */
      /* fn:data ((atomic|attribute)*) as atomic* */
      /* (F&O 2.4) */
    , { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (), PFty_xs_anyAttribute ())) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_attr }
    , /* fn:data ((atomic|text)*) as atomic* */
      /* (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (), PFty_text ())) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_text }
    , /* fn:data ((atomic|processing-instruction)*) as atomic* */
      /* (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (), PFty_pi (NULL))) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_pi }
    , /* fn:data ((atomic|comment)*) as atomic* */
      /* (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (), PFty_text ())) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_comm }
    , /* fn:data ((element)*) as atomic* */
      /* (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (),
                            PFty_choice (
                                PFty_xs_anyElement (),
                                PFty_choice (
                                    PFty_text (),
                                    PFty_doc (PFty_xs_anyType ()))))) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_elem }
    , /* fn:data ((atomic|element|doc|text|attribute)*) as atomic* */
      /* (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1,
        .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                        PFty_choice (
                            PFty_atomic (),
                            PFty_choice (
                                PFty_xs_anyElement (),
                                PFty_choice (
                                    PFty_doc (PFty_xs_anyType ()),
                                    PFty_choice (
                                        PFty_text (),
                                        PFty_xs_anyAttribute ()))))) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data_elem_attr }
    , /* fn:data (item*) as atomic*    (F&O 2.4) */
      { .ns = PFns_fn, .loc = "data",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_star (PFty_atomic ()) } },
        .alg = PFbui_fn_data }
    
    
/* 3. THE ERROR FUNCTION */
    , /* fn:error () as none */
      { .ns = PFns_fn, .loc = "error",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_none () } } }
    , /* fn:error (string?) as none */
      { .ns = PFns_fn, .loc = "error",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_none () } } }
    , /* fn:error (string?, string) as none */
      { .ns = PFns_fn, .loc = "error",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string () },
            .ret_ty = PFty_none () } } }

    
/* 4. THE TRACE FUNCTION */
    , /* fn:trace (item*, string) as item* */
      { .ns = PFns_fn, .loc = "trace",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()),
                                   PFty_xs_string () },
            .ret_ty = PFty_star (PFty_item ()) } } }


/* 6. FUNCTIONS AND OPERATORS ON NUMERICS */
/* 6.2. Operators on Numeric Values */
    , /* op:plus (integer, integer) as integer */
      { .ns = PFns_op, .loc = "plus",
        .arity = 2, .sig_count = 6, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                   PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                   PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                   PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_op_numeric_add }
    , /* op:minus (integer, integer) as integer */
      { .ns = PFns_op, .loc = "minus",
        .arity = 2, .sig_count = 6, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                   PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                   PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                   PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_op_numeric_subtract }
    , /* op:times (integer, integer) as integer */
      { .ns = PFns_op, .loc = "times",
        .arity = 2, .sig_count = 6, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                   PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                   PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                   PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_op_numeric_multiply }

    , /* op:div (decimal, decimal) as decimal */
      { .ns = PFns_op, .loc = "div",
        .arity = 2, .sig_count = 4, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                   PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                   PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_op_numeric_divide }
    , /* op:idiv (integer, integer) as integer */
      { .ns = PFns_op, .loc = "idiv",
        .arity = 2, .sig_count = 2, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                   PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_op_numeric_idivide }
    , /* op:mod (integer, integer) as integer */
      { .ns = PFns_op, .loc = "mod",
        .arity = 2, .sig_count = 6, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                   PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                   PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) }, {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () }, {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                   PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_op_numeric_modulo }
    
/* 6.3. Comparison Operators on Numeric Values */
    , /* op:eq (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_eq_int }
    , /* op:eq (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_eq_int }
    , /* op:eq (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_eq_dec }
    , /* op:eq (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_eq_dec }
    , /* op:eq (double, double) as boolean */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_eq_dbl }
    , /* op:eq (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_eq_dbl }
    , /* op:eq (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_eq_bln }
    , /* op:eq (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_eq_bln }
    , /* op:eq (string, string) as boolean */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_eq_str }
    , /* op:eq (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "eq",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_eq_str }

    , /* op:ne (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ne_int }
    , /* op:ne (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ne_int }
    , /* op:ne (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ne_dec }
    , /* op:ne (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ne_dec }
    , /* op:ne (double, double) as boolean */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ne_dbl }
    , /* op:ne (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ne_dbl }
    , /* op:ne (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ne_bln }
    , /* op:ne (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ne_bln }
    , /* op:ne (string, string) as boolean */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ne_str }
    , /* op:ne (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "ne",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ne_str }

    , /* op:lt (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_lt_int }
    , /* op:lt (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_lt_int }
    , /* op:lt (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_lt_dec }
    , /* op:lt (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_lt_dec }
    , /* op:lt (double, double) as boolean */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_lt_dbl }
    , /* op:lt (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_lt_dbl }
    , /* op:lt (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_lt_bln }
    , /* op:lt (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_lt_bln }
    , /* op:lt (string, string) as boolean */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_lt_str }
    , /* op:lt (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "lt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_lt_str }

    , /* op:le (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_le_int }
    , /* op:le (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_le_int }
    , /* op:le (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_le_dec }
    , /* op:le (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_le_dec }
    , /* op:le (double, double) as boolean */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_le_dbl }
    , /* op:le (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_le_dbl }
    , /* op:le (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_le_bln }
    , /* op:le (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_le_bln }
    , /* op:le (string, string) as boolean */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_le_str }
    , /* op:le (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "le",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_le_str }

    , /* op:gt (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_gt_int }
    , /* op:gt (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_gt_int }
    , /* op:gt (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_gt_dec }
    , /* op:gt (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_gt_dec }
    , /* op:gt (double, double) as boolean */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_gt_dbl }
    , /* op:gt (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_gt_dbl }
    , /* op:gt (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_gt_bln }
    , /* op:gt (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_gt_bln }
    , /* op:gt (string, string) as boolean */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_gt_str }
    , /* op:gt (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "gt",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_gt_str }

    , /* op:ge (integer, integer) as boolean */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (),
                                PFty_xs_integer () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ge_int }
    , /* op:ge (integer?, integer?) as boolean? */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ge_int }
    , /* op:ge (decimal, decimal) as boolean */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal (),
                                PFty_xs_decimal () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ge_dec }
    , /* op:ge (decimal?, decimal?) as boolean? */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ge_dec }
    , /* op:ge (double, double) as boolean */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ge_dbl }
    , /* op:ge (double?, double?) as boolean? */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ge_dbl }
    , /* op:ge (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (),
                                PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ge_bln }
    , /* op:ge (boolean?, boolean?) as boolean? */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()),
                                PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ge_bln }
    , /* op:ge (string, string) as boolean */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_ge_str }
    , /* op:ge (string?, string?) as boolean? */
      { .ns = PFns_op, .loc = "ge",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_boolean ()) } },
        .alg = PFbui_op_ge_str }

/* 6.4. Functions on Numeric Values */
    , /* fn:abs (integer) as integer */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_abs_int }
    , /* fn:abs (decimal) as decimal */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_abs_dec }
    , /* fn:abs (double) as double */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double () },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_abs_dbl }
    , /* fn:abs (integer?) as integer? */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_abs_int }
    , /* fn:abs (decimal?) as decimal? */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_abs_dec }
    , /* fn:abs (double?) as double? */
      { .ns = PFns_fn, .loc = "abs",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_abs_dbl }
    , /* fn:ceiling (integer) as integer */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_ceiling_int }
    , /* fn:ceiling (decimal) as decimal */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_ceiling_dec }
    , /* fn:ceiling (double) as double */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double () },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_ceiling_dbl }
    , /* fn:ceiling (integer?) as integer? */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_ceiling_int }
    , /* fn:ceiling (decimal?) as decimal? */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_ceiling_dec }
    , /* fn:ceiling (double?) as double? */
      { .ns = PFns_fn, .loc = "ceiling",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_ceiling_dbl }
    , /* fn:floor (integer) as integer */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_floor_int }
    , /* fn:floor (decimal) as decimal */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_floor_dec }
    , /* fn:floor (double) as double */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double () },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_floor_dbl }
    , /* fn:floor (integer?) as integer? */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_floor_int }
    , /* fn:floor (decimal?) as decimal? */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_floor_dec }
    , /* fn:floor (double?) as double? */
      { .ns = PFns_fn, .loc = "floor",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_floor_dbl }
    , /* fn:round (integer) as integer */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_round_int }
    , /* fn:round (decimal) as decimal */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_round_dec }
    , /* fn:round (double) as double */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_double () },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_round_dbl }
    , /* fn:round (integer?) as integer? */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_round_int }
    , /* fn:round (decimal?) as decimal? */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_round_dec }
    , /* fn:round (double?) as double? */
      { .ns = PFns_fn, .loc = "round",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_round_dbl }


/* 7. FUNCTIONS ON STRINGS */
/* 7.4. Functions on String Values */
    , /* fn:concat (string, string) as string */
      /* This is more strict that the W3C variant. Maybe we can do with */
      /* that strict variant. */
      { .ns = PFns_fn, .loc = "concat",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string(), PFty_xs_string() },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_concat}
    , /* fn:string-join (string*, string) as string */
      { .ns = PFns_fn, .loc = "string-join",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_string_join}
    , /* fn:substring (string?, double) as string */
      { .ns = PFns_fn, .loc = "substring",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:substring (string?, double, double) as string */
      { .ns = PFns_fn, .loc = "substring",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:string-length () as integer */
      { .ns = PFns_fn, .loc = "string-length",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_integer () } } }
    , /* fn:string-length (string?) as integer */
      { .ns = PFns_fn, .loc = "string-length",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_integer () } } }
    , /* fn:normalize-space () as string */
      { .ns = PFns_fn, .loc = "normalize-space",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_string () } } }
    , /* fn:normalize-space (string?) as string */
      { .ns = PFns_fn, .loc = "normalize-space",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:upper-case (string?) as string */
      { .ns = PFns_fn, .loc = "upper-case",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:lower-case (string?) as string */
      { .ns = PFns_fn, .loc = "lower-case",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:translate (string?, string, string) as string */
      { .ns = PFns_fn, .loc = "translate",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_string () } } }

/* 7.5. Functions Based on Substring Matching */
    , /* fn:contains (string, string) as boolean */
      { .ns = PFns_fn, .loc = "contains",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_contains }
    , /* fn:contains (string?, string) as boolean */
      { .ns = PFns_fn, .loc = "contains",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_contains_opt }
    , /* fn:contains (string?, string?) as boolean */
      { .ns = PFns_fn, .loc = "contains",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_contains_opt_opt }
    , /* fn:starts-with (string?, string?) as boolean */
      { .ns = PFns_fn, .loc = "starts-with",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_boolean () } } }
    , /* fn:ends-with (string?, string?) as boolean */
      { .ns = PFns_fn, .loc = "ends-with",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_boolean () } } }
    , /* fn:substring-before (string?, string?) as string */
      { .ns = PFns_fn, .loc = "substring-before",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:substring-after (string?, string?) as string */
      { .ns = PFns_fn, .loc = "substring-after",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } } }
    
/* 7.6. String Functions that Use Pattern Matching */
    , /* fn:matches(string?, string) as boolean */
      { .ns = PFns_fn, .loc = "matches",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } } }
    , /* fn:matches (string?, string, string) as boolean */
      { .ns = PFns_fn, .loc = "matches",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_boolean () } } }
    , /* fn:replace (string?, string, string) as string */
      { .ns = PFns_fn, .loc = "replace",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_string () } } }
    , /* fn:replace (string?, string, string, string) as string */
      { .ns = PFns_fn, .loc = "replace",
        .arity = 4, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()),
                                PFty_xs_string (),
                                PFty_xs_string (),
                                PFty_xs_string () },
            .ret_ty = PFty_xs_string () } } }


/* 9. FUNCTIONS AND OPERATORS ON BOOLEAN VALUES */
/* 9.1. Additional Boolean Constructor Functions */
    , /* fn:true () as boolean */
      { .ns = PFns_fn, .loc = "true",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_true }
    , /* fn:false () as boolean */
      { .ns = PFns_fn, .loc = "false",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_false }

/* 9.2. Operators on Boolean Values */
   /* see: 6.3. Comparison Operators on Numeric Values */

/* 9.3. Functions on Boolean Values */
    , /* fn:not (boolean) as boolean  (F&O 7.3.1) */
      { .ns = PFns_fn, .loc = "not",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_not_bln }
    , /* fn:not (item*) as boolean  (F&O 7.3.1) */
      /* Note: After type checking, fn:not(item*) should actually no      */
      /*       longer appear in Core trees.  The implicit semantics for   */
      /*       fn:not() in fs.brg wraps the argument of fn:not() into an  */
      /*       fn:boolean() call.  We need the signature fn:not(item*)    */
      /*       here nevertheless to determine the correct expected type   */
      /*       for fn:not() in fs.brg.                                    */
      /*       (see Rule FuncArgList: args(Expr, FuncArgList)             */
      { .ns = PFns_fn, .loc = "not",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = NULL }


/* 11. FUNCTIONS RELATED TO QNAMES */
/* 11.1. Additional Constructor Functions for QNames */
    , /* fn:resolve-QName (xs:string) as xs:QName */
      /* Note that we're off the specs here.  Refer to fs.brg for
       * details (computed element construction). */
      { .ns = PFns_fn, .loc = "resolve-QName",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t []) { PFty_xs_string () },
            .ret_ty = PFty_xs_QName () } },
        .alg = PFbui_fn_resolve_qname }


/* 14 FUNCTIONS AND OPERATORS ON NODES */
/* 14.1 fn:name */
    , /* fn:name (node) as string */
      { .ns = PFns_fn, .loc = "name",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_string () } } }
    , /* fn:name (node) as string */
      { .ns = PFns_fn, .loc = "name",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_node ()) },
            .ret_ty = PFty_xs_string () } } }

/* 14.2. fn:local-name */
    , /* fn:local-name (node) as string */
      { .ns = PFns_fn, .loc = "local-name",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_string () } } }
    , /* fn:local-name (node) as string */
      { .ns = PFns_fn, .loc = "local-name",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_node ()) },
            .ret_ty = PFty_xs_string () } } }

/* 14.3. fn:namespace-uri */
    , /* fn:namespace-uri (node) as string */
      { .ns = PFns_fn, .loc = "namespace-uri",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_string () } } }
    , /* fn:namespace-uri (node) as string */
      { .ns = PFns_fn, .loc = "namespace-uri",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_node ()) },
            .ret_ty = PFty_xs_string () } } }

/* 14.4. fn:number */    
    , /* fn:number () as double */
      { .ns = PFns_fn, .loc = "number",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_double () } },
        .alg = NULL }
    , /* fn:number (atomic?) as double */
      { .ns = PFns_fn, .loc = "number",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_atomic ()) },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_number }

/* 14.6. op:is-same-node */
    , /* op:is-same-node (node, node) as boolean */
      { .ns = PFns_op, .loc = "is-same-node",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (),
                                PFty_node ()},
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_is_same_node }

/* 14.7. op:node-before */
    , /* op:node-before (node, node) as boolean */
      { .ns = PFns_op, .loc = "node-before",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (),
                                PFty_node ()},
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_node_before }

/* 14.8. op:node-after */
    , /* op:node-after (node, node) as boolean */
      { .ns = PFns_op, .loc = "node-after",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (),
                                PFty_node ()},
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_node_after }

/* 14.9. fn:root */
    , /* fn:root () as node */
      { .ns = PFns_fn, .loc = "root",
        .arity = 0, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_none () },
            .ret_ty = PFty_node() } } }
    , /* fn:root (node?) as node? */
      { .ns = PFns_fn, .loc = "root",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_node ()) },
            .ret_ty = PFty_opt (PFty_node()) } } }
    

/* 15. FUNCTIONS AND OPERATORS ON SEQUENCES */
/* 15.1. General Functions and Operators on Sequences */
    , /* fn:boolean (boolean) as boolean */
      { .ns = PFns_fn, .loc = "boolean",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_boolean_bln }
    , /* fn:boolean (boolean?) as boolean */
      { .ns = PFns_fn, .loc = "boolean",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_boolean ()) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_boolean_optbln }
    , /* fn:boolean ( */
      /*      node*|boolean|string|integer|decimal|double) as boolean */
      { .ns = PFns_fn, .loc = "boolean",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_choice (
                                    PFty_star (PFty_node ()),
                                    PFty_choice (
                                        PFty_xs_boolean (),
                                        PFty_choice (
                                            PFty_xs_string (),
                                            PFty_choice (
                                                PFty_xs_integer (),
                                                PFty_choice (
                                                    PFty_xs_decimal (),
                                                    PFty_xs_double ()))))) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_boolean_item }
    , /* fn:empty (item*) as boolean  (F&O 14.2.5) */
      { .ns = PFns_fn, .loc = "empty",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_empty }
    , /* fn:exists (item*) as boolean */
      { .ns = PFns_fn, .loc = "exists",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_fn_exists }
      /* FIXME: distinct-values should be changed to anyAtomicType* */
    , /* fn:distinct-values (atomic*) as atomic* */
      { .ns = PFns_fn, .loc = "distinct-values",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_atomic ())},
            .ret_ty = PFty_star (PFty_untypedAtomic ()) } },
        .alg = PFbui_fn_distinct_values }
    , /* fn:insert-before(item*, int, item*) as item* */
      /* Note that typecheck.brg implements a specific typing rule! */
      { .ns = PFns_fn, .loc = "insert-before",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()),
                                   PFty_xs_integer (),
                                   PFty_star (PFty_item ()) },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_insert_before }
    , /* fn:remove(item*, int) as item* */
      /* Note that typecheck.brg implements a specific typing rule! */
      { .ns = PFns_fn, .loc = "remove",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()),
                                   PFty_xs_integer () },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_remove }
    , /* fn:reverse(item*) as item* */
      /* Note that typecheck.brg implements a specific typing rule! */
      { .ns = PFns_fn, .loc = "reverse",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_reverse }
    , /* fn:subsequence(item*, double) as item* */
      /* Note that typecheck.brg implements a specific typing rule! */
      { .ns = PFns_fn, .loc = "subsequence",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()),
                                PFty_xs_double () },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_subsequence_till_end }
    , /* fn:subsequence(item*, double, double) as item* */
      /* Note that typecheck.brg implements a specific typing rule! */
      { .ns = PFns_fn, .loc = "subsequence",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()),
                                PFty_xs_double (),
                                PFty_xs_double () },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_subsequence }
    , /* fn:unordered (item *) as item* */
      { .ns = PFns_fn, .loc = "unordered",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = PFbui_fn_unordered }

/* 15.2. Functions That Test the Cardinality of Sequences */
    , /* fn:zero-or-one (item *) as item? */
         /* Note that typecheck.brg implements a specific typing rule */
         /* replacing the occurrence indicator! */
      { .ns = PFns_fn, .loc = "zero-or-one",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_opt (PFty_item ()) } },
        .alg = PFbui_fn_zero_or_one }
    , /* fn:exactly-one (item *) as item */
         /* Note that typecheck.brg implements a specific typing rule */
         /* replacing the occurrence indicator! */
      { .ns = PFns_fn, .loc = "exactly-one",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_item () } },
        .alg = PFbui_fn_exactly_one }

/* 15.3. Equals, Union, Intersection and Except */
    , /* op:union (node*, node*) as node* */
      { .ns = PFns_op, .loc = "union",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_node ()),
                                PFty_star (PFty_node ()) },
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_op_union }
    , /* op:intersect (node*, node*) as node* */
      { .ns = PFns_op, .loc = "intersect",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_node ()),
                                PFty_star (PFty_node ()) },
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_op_intersect }
    , /* op:except (node*, node*) as node* */
      { .ns = PFns_op, .loc = "except",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_node ()),
                                PFty_star (PFty_node ()) },
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_op_except }

/* 15.4. Aggregate Functions */
    , /* fn:count (item*) as integer */
      { .ns = PFns_fn, .loc = "count",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_count }
    /* untypedAtomic needs to be casted into double therefore */
    /* fn:max (double*) is the last entry for fn:max */
    , /* fn:avg (double+) as double */
      { .ns = PFns_fn, .loc = "avg",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_double ()) },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_avg }
    , /* fn:avg (double*) as double? */
      { .ns = PFns_fn, .loc = "avg",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_avg }
    , /* fn:max (string+) as string */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_max_str }
    , /* fn:max (integer+) as integer */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_integer ()) },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_max_int }
    , /* fn:max (decimal+) as decimal */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_decimal ()) },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_max_dec }
    , /* fn:max (double+) as double */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_double ()) },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_max_dbl }
    , /* fn:max (string*) as string? */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_string ()) } },
        .alg = PFbui_fn_max_str }
    , /* fn:max (integer*) as integer? */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_max_int }
    , /* fn:max (decimal*) as decimal? */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_max_dec }
    , /* fn:max (double*) as double? */
      { .ns = PFns_fn, .loc = "max",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_max_dbl }
    , /* fn:min (string+) as string */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_string ()) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_fn_min_str }
    , /* fn:min (integer+) as integer */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_integer ()) },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_min_int }
    , /* fn:min (decimal+) as decimal */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_decimal ()) },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_min_dec }
    , /* fn:min (double+) as double */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_plus (PFty_xs_double ()) },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_min_dbl }
    , /* fn:min (string*) as string? */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_xs_string ()) } },
        .alg = PFbui_fn_min_str }
    , /* fn:min (integer*) as integer? */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_min_int }
    , /* fn:min (decimal*) as decimal? */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_min_dec }
    , /* fn:min (double*) as double? */
      { .ns = PFns_fn, .loc = "min",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_min_dbl }
    , /* fn:sum (integer*) as integer */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_integer ()) },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_sum_int }
    , /* fn:sum (decimal*) as decimal */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_decimal ()) },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_sum_dec }
    , /* fn:sum (double*) as double */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()) },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_sum_dbl }
    , /* fn:sum (integer*, integer) as integer */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_integer ()),
                                   PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } },
        .alg = PFbui_fn_sum_zero_int }
    , /* fn:sum (decimal*, decimal) as decimal */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_decimal ()),
                                   PFty_xs_decimal () },
            .ret_ty = PFty_xs_decimal () } },
        .alg = PFbui_fn_sum_zero_dec }
    , /* fn:sum (double*, double) as double */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()),
                                   PFty_xs_double () },
            .ret_ty = PFty_xs_double () } },
        .alg = PFbui_fn_sum_zero_dbl }
    , /* fn:sum (integer*, integer?) as integer? */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_integer ()),
                                PFty_opt (PFty_xs_integer ()) },
            .ret_ty = PFty_opt (PFty_xs_integer ()) } },
        .alg = PFbui_fn_sum_zero_int }
    , /* fn:sum (decimal*, decimal?) as decimal? */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_decimal ()),
                                PFty_opt (PFty_xs_decimal ()) },
            .ret_ty = PFty_opt (PFty_xs_decimal ()) } },
        .alg = PFbui_fn_sum_zero_dec }
    , /* fn:sum (double*, double?) as double? */
      { .ns = PFns_fn, .loc = "sum",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_double ()),
                                PFty_opt (PFty_xs_double ()) },
            .ret_ty = PFty_opt (PFty_xs_double ()) } },
        .alg = PFbui_fn_sum_zero_dbl }

/* 15.5. Functions and Operators that Generate Sequences */
    , /* op:to (integer, integer) as integer* */
      { .ns = PFns_op, .loc = "to",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer (), PFty_xs_integer () },
            .ret_ty = PFty_star (PFty_xs_integer ()) } } }
    , /* fn:id (string*) as element* */
      { .ns = PFns_fn, .loc = "id",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_star (PFty_xs_anyElement ()) } } }
    , /* fn:id (string*, node) as element* */
      { .ns = PFns_fn, .loc = "id",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()),
                                PFty_node () },
            .ret_ty = PFty_star (PFty_xs_anyElement ()) } } }
    , /* fn:idref (string*) as element* */
      { .ns = PFns_fn, .loc = "idref",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_star (PFty_xs_anyElement ()) } } }
    , /* fn:idref (string*, node) as element* */
      { .ns = PFns_fn, .loc = "idref",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()),
                                PFty_node () },
            .ret_ty = PFty_star (PFty_xs_anyElement ()) } } }
    , /* fn:doc (string?) as document? - FIXME: is type of PFty_doc right? */
      { .ns = PFns_fn, .loc = "doc",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_xs_string ()) },
            .ret_ty = PFty_opt (PFty_doc (PFty_xs_anyNode ())) } },
        .alg = PFbui_fn_doc }
    , /* fn:collection (string) as node* */
      { .ns = PFns_fn, .loc = "collection",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyNode ()) } } }
    

/* 16. CONTEXT FUNCTIONS */
/* 16.1. fn:position */
    , /* fn:position () as integer */
      { .ns = PFns_fn, .loc = "position",
        .arity = 0,  .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_integer () } },
        .alg = NULL }

/* 16.2. fn:last */
    , /* fn:last () as integer */
      { .ns = PFns_fn, .loc = "last",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_xs_integer () } },
        .alg = NULL }


/* #1. PATHFINDER SPECIFIC HELPER FUNCTIONS */
    , /* op:or (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "or",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (), PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_or_bln }
    , /* op:and (boolean, boolean) as boolean */
      { .ns = PFns_op, .loc = "and",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_boolean (), PFty_xs_boolean () },
            .ret_ty = PFty_xs_boolean () } },
        .alg = PFbui_op_and_bln }

    , /* #pf:distinct-doc-order-or-atomic-sequence (item*) as item* */
      { .ns = PFns_pf, .loc = "distinct-doc-order-or-atomic-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ()) },
            .ret_ty = PFty_star (PFty_item ()) } },
        .alg = NULL }
    , /* #pf:distinct-doc-order (node *) as node* */
      { .ns = PFns_pf, .loc = "distinct-doc-order",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_node ()) },
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_distinct_doc_order }
      /* the return type of #pf:item-sequence-to-node-sequence is         */
      /* generated using a special typing rule during typechecking        */
    , /* #pf:item-sequence-to-node-sequence (atomic) as text */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_atomic ()},
            .ret_ty = PFty_text () } },
        .alg = PFbui_pf_item_seq_to_node_seq_single_atomic }
    , /* #pf:item-sequence-to-node-sequence (atomic*) as text */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_atomic ())},
            .ret_ty = PFty_text () } },
        .alg = PFbui_pf_item_seq_to_node_seq_atomic }
    , /* #pf:item-sequence-to-node-sequence (attr*, atomic) as node* */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_seq (
                                        PFty_star (PFty_xs_anyAttribute ()),
                                        PFty_atomic ())},
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_item_seq_to_node_seq_attr_single }
    , /* #pf:item-sequence-to-node-sequence (attr*, atomic*) as node* */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                                    PFty_choice (
                                        PFty_atomic (),
                                        PFty_xs_anyAttribute ()))},
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_item_seq_to_node_seq_attr }
    , /* #pf:item-sequence-to-node-sequence (item*) as node* */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (
                                    PFty_choice (
                                        PFty_xs_anyElement (),
                                        PFty_choice (
                                            PFty_doc (PFty_xs_anyType ()),
                                            PFty_choice (
                                                PFty_text (),
                                                PFty_choice (
                                                    PFty_pi (NULL),
                                                    PFty_choice (
                                                        PFty_atomic (),
                                                        PFty_comm ()))))))},
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_item_seq_to_node_seq_wo_attr }
    , /* #pf:item-sequence-to-node-sequence (item*) as node* */
      { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ())},
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_item_seq_to_node_seq }
      /* FIXME:                                                     */
      /*   The W3C specs describe variants of is2uA for each node   */
      /*   (with differences in the empty sequence handling).       */
    , /* #pf:item-sequence-to-untypedAtomic (item*) as untypedAtomic */
      { .ns = PFns_pf, .loc = "item-sequence-to-untypedAtomic",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_item ())},
            .ret_ty = PFty_untypedAtomic () } },
        .alg = NULL }
    , /* #pf:merge-adjacent-text-nodes (node*) as node* */
      { .ns = PFns_pf, .loc = "merge-adjacent-text-nodes",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_node ())},
            .ret_ty = PFty_star (PFty_node ()) } },
        .alg = PFbui_pf_merge_adjacent_text_nodes }

    , /* #pf:typed-value (node) as untypedAtomic* */
      { .ns = PFns_pf, .loc = "typed-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node () },
            .ret_ty = PFty_star (PFty_untypedAtomic ()) } },
        /* FIXME: does this still fit or is it string-value? */
        .alg = PFbui_pf_string_value }
    , /* #pf:string-value (attribute) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyAttribute () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_attr }
    , /* #pf:string-value (text) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_text () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_text }
    , /* #pf:string-value (processing-instruction) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_pi (NULL) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_pi }
    , /* #pf:string-value (comment) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_comm () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_comm }
    , /* #pf:string-value (elem) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_choice (
                                    PFty_xs_anyElement (),
                                    PFty_choice (
                                        PFty_text (),
                                        PFty_doc (PFty_xs_anyType ()))) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_elem }
    , /* #pf:string-value (elem, attr) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_choice (
                                    PFty_xs_anyElement (),
                                    PFty_choice (
                                        PFty_doc (PFty_xs_anyType ()),
                                        PFty_choice (
                                            PFty_text (),
                                            PFty_xs_anyAttribute ()))) },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value_elem_attr }
    , /* #pf:string-value (node) as string */
      { .ns = PFns_pf, .loc = "string-value",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node () },
            .ret_ty = PFty_xs_string () } },
        .alg = PFbui_pf_string_value }


/* #2. PATHFINDER SPECIFIC DOCUMENT MANAGEMENT FUNCTIONS */
    , /* fn:put (node, string) as none */
      { .ns = PFns_fn, .loc = "put",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyNode (), PFty_xs_string () },
            .ret_ty = PFty_stmt () } } }
    , /* pf:documents () as element()* */
      { .ns = PFns_lib, .loc = "documents",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* pf:documents-unsafe () as element()* */
      { .ns = PFns_lib, .loc = "documents-unsafe",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* pf:documents (string*) as element()* */
      { .ns = PFns_lib, .loc = "documents",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* pf:documents-unsafe (string*) as element()* */
      { .ns = PFns_lib, .loc = "documents-unsafe",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* pf:docname (node*) as string* */
      { .ns = PFns_lib, .loc = "docname",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star( PFty_xs_anyNode ()) },
            .ret_ty = PFty_star( PFty_xs_string ()) } } }
    , /* pf:collection (string) as node */
      { .ns = PFns_lib, .loc = "collection",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_xs_anyNode () } } }
    , /* pf:collections () as element()* */
      { .ns = PFns_lib, .loc = "collections",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* pf:collections-unsafe () as element()* */
      { .ns = PFns_lib, .loc = "collections-unsafe",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    ,  /* pf:attribute (node()*, string) as element()* */
      { .ns = PFns_lib, .loc = "attribute",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star( PFty_xs_anyNode ()),
                                   PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    ,  /* pf:attribute (node()*, string, string, string) as element()* */
      { .ns = PFns_lib, .loc = "attribute",
        .arity = 4, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star( PFty_xs_anyNode ()),
                                   PFty_xs_string (),
                                   PFty_xs_string (),
                                   PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    ,  /* pf:text (node()*, string) as text()* */
      { .ns = PFns_lib, .loc = "text",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star( PFty_xs_anyNode ()),
                                   PFty_xs_string () },
            .ret_ty = PFty_star( PFty_text()) } } }
    ,  /* pf:add-doc (string, string) as docmgmt */
      { .ns = PFns_lib, .loc = "add-doc",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                   PFty_xs_string () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* pf:add-doc (string, string, string) as docmgmt */
      { .ns = PFns_lib, .loc = "add-doc",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                   PFty_xs_string (),
                                   PFty_xs_string () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* pf:add-doc (string, string, int) as docmgmt */
      { .ns = PFns_lib, .loc = "add-doc",
        .arity = 3, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                   PFty_xs_string (),
                                   PFty_xs_integer() },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* pf:add-doc (string, string, string, int) as docmgmt */
      { .ns = PFns_lib, .loc = "add-doc",
        .arity = 4, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string (),
                                   PFty_xs_string (),
                                   PFty_xs_string (),
                                   PFty_xs_integer() },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* pf:del-doc (string) as docmgmt */
      { .ns = PFns_lib, .loc = "del-doc",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* pf:mil (string) as item* */
      { .ns = PFns_lib, .loc = "mil",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_star( PFty_item ()) } } }
    , /* pf:nid (xs:element) as string */
      { .ns = PFns_lib, .loc = "nid",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyNode () },
            .ret_ty = PFty_xs_string () } } }


/* #3. UPDATE FUNCTIONS */
    /* Below are the function declarations for the UpdateX functions */
    ,  /* upd:rename(node, xs:QName) as stmt */
      { .ns = PFns_upd, .loc = "rename",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (), PFty_xs_QName () },
            .ret_ty = PFty_stmt () } } }
#if 0
    /* This signature apparently is too strict (or our static type
     * inference to coarse).  Relaxed the type of the first argument
     * for the time being (see above). */
    ,  /* upd:rename(xs:anyElement | xs:anyAttribute | p-i, xs:QName) as stmt */
      { .ns = PFns_upd, .loc = "rename",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_choice (PFty_xs_anyElement (),
                                       PFty_choice (PFty_xs_anyAttribute (),
                                                    PFty_pi (NULL))),
                                   PFty_xs_QName () },
            .ret_ty = PFty_stmt () } } }
#endif
    ,  /* upd:delete (node) as stmt */
      { .ns = PFns_upd, .loc = "delete",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node () },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:insertIntoAsFirst ((xs:anyElement | document-node())?,
                                 node*) as stmt */
       /* FIXME: should be `elem | doc, node+', which would make
        *        trouble during type checking.                     */
      { .ns = PFns_upd, .loc = "insertIntoAsFirst",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_choice (
                                     PFty_xs_anyElement (),
                                     PFty_doc (PFty_xs_anyNode ()))),
                                   PFty_star (PFty_node ()) },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:insertIntoAsLast ((xs:anyElement | document-node())?,
                                node*) as stmt */
       /* FIXME: should be `elem | doc, node+', which would make
        *        trouble during type checking.                     */
      { .ns = PFns_upd, .loc = "insertIntoAsLast",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_opt (PFty_choice (
                                     PFty_xs_anyElement (),
                                     PFty_doc (PFty_xs_anyNode ()))),
                                   PFty_star (PFty_node ()) },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:insertBefore (node, node*) as stmt */
       /* FIXME: should be node+, which would make trouble during
        *        type checking.                                    */
      { .ns = PFns_upd, .loc = "insertBefore",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (),
                                   PFty_star (PFty_node ()) },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:insertAfter (node, node*) as stmt */
       /* FIXME: should be node+, which would make trouble during
        *        type checking.                                    */
      { .ns = PFns_upd, .loc = "insertAfter",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_node (),
                                   PFty_star (PFty_node ()) },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:replaceValue (xs:anyAttribute, xdt:untypedAtomic) as stmt */
      { .ns = PFns_upd, .loc = "replaceValue",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyAttribute (),
                                   PFty_xdt_untypedAtomic () },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:replaceValue (text(), xdt:untypedAtomic) as stmt */
      { .ns = PFns_upd, .loc = "replaceValue",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_text (),
                                   PFty_xdt_untypedAtomic () },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:replaceValue (processing-instr(), xdt:untypedAtomic) as stmt */
      { .ns = PFns_upd, .loc = "replaceValue",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_pi (NULL),
                                   PFty_xdt_untypedAtomic () },
            .ret_ty = PFty_stmt () } } }
    ,  /* upd:replaceValue (comment(), xdt:untypedAtomic) as stmt */
      { .ns = PFns_upd, .loc = "replaceValue",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_comm (),
                                   PFty_xdt_untypedAtomic () },
            .ret_ty = PFty_stmt () } } }
    , /* upd:replaceElementContent (element(), text()?) as stmt */
      { .ns = PFns_upd, .loc = "replaceElementContent",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t []) { PFty_xs_anyElement (),
                                    PFty_opt (PFty_text ()) },
            .ret_ty = PFty_stmt () } } }


#ifdef HAVE_PFTIJAH
/* #4. PFTIJAH FUNCTIONS */
    , /* tijah:ft-index-info () as element()* */
      { .ns = PFns_tijah, .loc = "ft-index-info",
        .arity = 0, .sig_count = 1, .sigs = { {
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    , /* tijah:ft-index-info (string*) as element()* */
      { .ns = PFns_tijah, .loc = "ft-index-info",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_string () },
            .ret_ty = PFty_star( PFty_xs_anyElement ()) } } }
    ,  /* tijah:create-ft-index() as docmgmt */
      { .ns = PFns_tijah, .loc = "create-ft-index",
        .arity = 0, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:create-ft-index(string*) as docmgmt */
      { .ns = PFns_tijah, .loc = "create-ft-index",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:create-ft-index(item) as docmgmt */
      { .ns = PFns_tijah, .loc = "create-ft-index",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyNode () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:create-ft-index(string*,item) as docmgmt */
      { .ns = PFns_tijah, .loc = "create-ft-index",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()),
                                   PFty_xs_anyNode () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:extend-ft-index(string*) as docmgmt */
      { .ns = PFns_tijah, .loc = "extend-ft-index",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()) },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:extend-ft-index(string*,item) as docmgmt */
      { .ns = PFns_tijah, .loc = "extend-ft-index",
        .arity = 2, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_star (PFty_xs_string ()),
                                   PFty_xs_anyNode () },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:delete-ft-index() as docmgmt */
      { .ns = PFns_tijah, .loc = "delete-ft-index",
        .arity = 0, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { },
            .ret_ty = PFty_docmgmt () } } }
    ,  /* tijah:delete-ft-index(item) as docmgmt */
      { .ns = PFns_tijah, .loc = "delete-ft-index",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_anyNode () },
            .ret_ty = PFty_docmgmt () } } }
    , /* tijah:query-id(string) as integer */
      { .ns = PFns_tijah, .loc = "query-id",
        .arity = 1, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_string () },
             .ret_ty = PFty_xs_integer () },
	     } }
    , /* tijah:query-id(item*, string) as integer */
      { .ns = PFns_tijah, .loc = "query-id",
        .arity = 2, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_star (PFty_xs_anyNode ()),
                                    PFty_xs_string () },
             .ret_ty = PFty_xs_integer () },
	     } }
    , /* tijah:query-id(string, item) as integer */
      { .ns = PFns_tijah, .loc = "query-id",
        .arity = 2, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_string (),
				    PFty_xs_anyNode () },
             .ret_ty = PFty_xs_integer () },
	     } }
    , /* tijah:query-id(item*, string, item) as integer */
      { .ns = PFns_tijah, .loc = "query-id",
        .arity = 3, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_star (PFty_xs_anyNode ()),
                                    PFty_xs_string (),
	     			    PFty_xs_anyNode ()},
             .ret_ty = PFty_xs_integer () },
	     } }
    , /* tijah:query(string) as node* */
      { .ns = PFns_tijah, .loc = "query",
        .arity = 1, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_string () },
             .ret_ty = PFty_star (PFty_xs_anyNode ()) },
	     } }
    , /* tijah:query(item*, string) as node* */
      { .ns = PFns_tijah, .loc = "query",
        .arity = 2, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_star (PFty_xs_anyNode ()),
                                    PFty_xs_string () },
             .ret_ty = PFty_star (PFty_xs_anyNode ()) },
	     } }
    , /* tijah:query(string, item) as node* */
      { .ns = PFns_tijah, .loc = "query",
        .arity = 2, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_string (),
				    PFty_xs_anyNode () },
             .ret_ty = PFty_star (PFty_xs_anyNode ()) },
	     } }
    , /* tijah:query(item*, string, item) as node* */
      { .ns = PFns_tijah, .loc = "query",
        .arity = 3, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_star (PFty_xs_anyNode ()),
                                    PFty_xs_string (),
	     			    PFty_xs_anyNode ()},
             .ret_ty = PFty_star (PFty_xs_anyNode ()) },
	     } }
    , /* tijah:nodes(integer) as node* */
      { .ns = PFns_tijah, .loc = "nodes",
        .arity = 1, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_integer () },
             .ret_ty = PFty_star (PFty_xs_anyNode ()) } } }
    , /* tijah:score(integer, node) as double */
      { .ns = PFns_tijah, .loc = "score",
        .arity = 2, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_integer (),
	                            PFty_xs_anyNode () },
             .ret_ty = PFty_xs_double () } } }
    , /* tijah:tokenize(string?) as string */
      { .ns = PFns_tijah, .loc = "tokenize",
        .arity = 1, .sig_count = 1, .sigs = { {
	     .par_ty = (PFty_t[]) { PFty_xs_string () },
             .ret_ty = PFty_xs_string () } } }
    , /* tijah:resultsize(integer) as integer */
      { .ns = PFns_tijah, .loc = "resultsize",
        .arity = 1, .sig_count = 1, .sigs = { {
            .par_ty = (PFty_t[]) { PFty_xs_integer () },
            .ret_ty = PFty_xs_integer () } } }
#endif
    , { .loc = 0 }
    };

    PFqname_t    qn;
    unsigned int n;

    PFfun_env = PFenv ();

    for (n = 0; xquery_fo[n].loc; n++) {
        assert (xquery_fo[n].sig_count <= XQUERY_FO_MAX_SIGS);

        /* construct function name */
        qn = PFqname (xquery_fo[n].ns, xquery_fo[n].loc);

        /* insert built-in XQuery F&O into function environment */
        PFenv_bind (PFfun_env,
                    qn,
                    (void *) PFfun_new (qn,
                                        xquery_fo[n].arity,
                                        true,
                                        xquery_fo[n].sig_count,
                                        xquery_fo[n].sigs,
                                        xquery_fo[n].alg,
                                        NULL,
                                        NULL));
    }
}

/* vim:set shiftwidth=4 expandtab: */
