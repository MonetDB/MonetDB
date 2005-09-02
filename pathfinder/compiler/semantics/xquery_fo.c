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
 * The Initial Developer of the Original Code is the Database &
 * Information Systems Group at the University of Konstanz, Germany.
 * Portions created by the University of Konstanz are Copyright (C)
 * 2000-2005 University of Konstanz.  All Rights Reserved.
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
 * maxmimum arity for all XQuery F&O functions
 */
#define XQUERY_FO_MAX_ARITY 4

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
 */
#define XQUERY_FO                                                        \
{ /* fn:data (item*) as atomic*    (F&O 2.4) */                          \
  { .ns = PFns_fn, .loc = "data",                                        \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_atomic ()) }                               \
, /* fn:number () as double */                                           \
  { .ns = PFns_fn, .loc = "number",                                      \
    .arity = 0,                                                          \
    .ret_ty = PFty_double () }                                           \
, /* fn:number (atomic?) as double */                                    \
  { .ns = PFns_fn, .loc = "number",                                      \
    .arity = 1, .par_ty = { PFty_opt (PFty_atomic ()) },                 \
    .ret_ty = PFty_double () }                                           \
, /* fn:doc (string?) as document? - FIXME: is type of PFty_doc right? */\
  { .ns = PFns_fn, .loc = "doc",                                         \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_doc (PFty_xs_anyNode ())),                  \
    .alg = PFbui_fn_doc }                                                \
, /* fn:id (string*) as element* */                                      \
  { .ns = PFns_fn, .loc = "id",                                          \
    .arity = 1, .par_ty = { PFty_star (PFty_string ()) },                \
    .ret_ty = PFty_star (PFty_xs_anyElement ()) }                        \
, /* fn:id (string*, node) as element* */                                \
  { .ns = PFns_fn, .loc = "id",                                          \
    .arity = 2, .par_ty = { PFty_star (PFty_string ()),                  \
                            PFty_node () },                              \
    .ret_ty = PFty_star (PFty_xs_anyElement ()) }                        \
, /* fn:idref (string*) as element* */                                   \
  { .ns = PFns_fn, .loc = "idref",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_string ()) },                \
    .ret_ty = PFty_star (PFty_xs_anyElement ()) }                        \
, /* fn:idref (string*, node) as element* */                             \
  { .ns = PFns_fn, .loc = "idref",                                       \
    .arity = 2, .par_ty = { PFty_star (PFty_string ()),                  \
                            PFty_node () },                              \
    .ret_ty = PFty_star (PFty_xs_anyElement ()) }                        \
, /* pf:distinct-doc-order (node *) as node* */                          \
  { .ns = PFns_pf, .loc = "distinct-doc-order",                          \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_pf_distinct_doc_order }                                 \
    /* FIXME: the W3C defined exact-one not so strict, but otherwise
              the typeswich doesn't work anymore */                      \
, /* fn:exactly-one (node *) as node */                                  \
  { .ns = PFns_fn, .loc = "exactly-one",                                 \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_node () }                                             \
, /* fn:zero-or-one (node *) as node */                                  \
  { .ns = PFns_fn, .loc = "zero-or-one",                                 \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_opt (PFty_node ()) }                                  \
, /* fn:unordered (item *) as item */                                    \
  { .ns = PFns_fn, .loc = "unordered",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_opt (PFty_item ()) }                                  \
                                                                         \
, /* fn:root () as node */                                               \
  { .ns = PFns_fn, .loc = "root",                                        \
    .arity = 0, .par_ty = { PFty_none () },                              \
    .ret_ty = PFty_node() }                                              \
, /* fn:root (node?) as node? */                                         \
  { .ns = PFns_fn, .loc = "root",                                        \
    .arity = 1, .par_ty = { PFty_opt (PFty_node ()) },                   \
    .ret_ty = PFty_opt (PFty_node()) }                                   \
, /* fn:position () as integer */                                        \
  { .ns = PFns_fn, .loc = "position",                                    \
    .arity = 0,                                                          \
    .ret_ty = PFty_integer () }                                          \
, /* fn:last () as integer */                                            \
  { .ns = PFns_fn, .loc = "last",                                        \
    .arity = 0,                                                          \
    .ret_ty = PFty_integer () }                                          \
, /* fn:empty (item*) as boolean  (F&O 14.2.5) */                        \
  { .ns = PFns_fn, .loc = "empty",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_fn_empty }                                              \
, /* fn:exists (item*) as boolean */                                     \
  { .ns = PFns_fn, .loc = "exists",                                      \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:not (boolean) as boolean  (F&O 7.3.1) */                         \
  { .ns = PFns_fn, .loc = "not",                                         \
    .arity = 1, .par_ty = { PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_fn_not_bln }                                            \
, /* fn:true () as boolean */                                            \
  { .ns = PFns_fn, .loc = "true",                                        \
    .arity = 0,                                                          \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:false () as boolean */                                           \
  { .ns = PFns_fn, .loc = "false",                                       \
    .arity = 0,                                                          \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:boolean (boolean) as boolean */                                  \
  { .ns = PFns_fn, .loc = "boolean",                                     \
    .arity = 1, .par_ty = { PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_fn_boolean_bln }                                        \
, /* fn:boolean (boolean?) as boolean */                                 \
  { .ns = PFns_fn, .loc = "boolean",                                     \
    .arity = 1, .par_ty = { PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_fn_boolean_optbln }                                     \
, /* fn:boolean (item*) as boolean */                                    \
  { .ns = PFns_fn, .loc = "boolean",                                     \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_fn_boolean_item }                                       \
, /* fn:contains (string?, string?) as boolean */                        \
  { .ns = PFns_fn, .loc = "contains",                                    \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:error () as none */                                              \
  { .ns = PFns_fn, .loc = "error",                                       \
    .arity = 0,                                                          \
    .ret_ty = PFty_none () }                                             \
, /* fn:error (string?) as none */                                       \
  { .ns = PFns_fn, .loc = "error",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_none () }                                             \
, /* fn:error (string?, string) as none */                               \
  { .ns = PFns_fn, .loc = "error",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_string () },                            \
    .ret_ty = PFty_none () }                                             \
, /* op:or (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "or",                                          \
    .arity = 2, .par_ty = { PFty_boolean (), PFty_boolean () },          \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_or_bln }                                             \
, /* op:and (boolean, boolean) as boolean */                             \
  { .ns = PFns_op, .loc = "and",                                         \
    .arity = 2, .par_ty = { PFty_boolean (), PFty_boolean () },          \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_and_bln }                                            \
, /* op:eq (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq_int }                                             \
, /* op:eq (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq_int }                                             \
, /* op:eq (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq_dec }                                             \
, /* op:eq (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq_dec }                                             \
, /* op:eq (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq_dbl }                                             \
, /* op:eq (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq_dbl }                                             \
, /* op:eq (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq_bln }                                             \
, /* op:eq (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq_bln }                                             \
, /* op:eq (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq_str }                                             \
, /* op:eq (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq_str }                                             \
                                                                         \
, /* op:ne (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ne_int }                                             \
, /* op:ne (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ne_int }                                             \
, /* op:ne (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ne_dec }                                             \
, /* op:ne (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ne_dec }                                             \
, /* op:ne (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ne_dbl }                                             \
, /* op:ne (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ne_dbl }                                             \
, /* op:ne (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ne_bln }                                             \
, /* op:ne (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ne_bln }                                             \
, /* op:ne (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ne_str }                                             \
, /* op:ne (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ne_str }                                             \
                                                                         \
, /* op:lt (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt_int }                                             \
, /* op:lt (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt_int }                                             \
, /* op:lt (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt_dec }                                             \
, /* op:lt (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt_dec }                                             \
, /* op:lt (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt_dbl }                                             \
, /* op:lt (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt_dbl }                                             \
, /* op:lt (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt_bln }                                             \
, /* op:lt (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt_bln }                                             \
, /* op:lt (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt_str }                                             \
, /* op:lt (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt_str }                                             \
                                                                         \
, /* op:le (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_le_int }                                             \
, /* op:le (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_le_int }                                             \
, /* op:le (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_le_dec }                                             \
, /* op:le (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_le_dec }                                             \
, /* op:le (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_le_dbl }                                             \
, /* op:le (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_le_dbl }                                             \
, /* op:le (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_le_bln }                                             \
, /* op:le (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_le_bln }                                             \
, /* op:le (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_le_str }                                             \
, /* op:le (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_le_str }                                             \
                                                                         \
, /* op:gt (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt_int }                                             \
, /* op:gt (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt_int }                                             \
, /* op:gt (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt_dec }                                             \
, /* op:gt (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt_dec }                                             \
, /* op:gt (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt_dbl }                                             \
, /* op:gt (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt_dbl }                                             \
, /* op:gt (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt_bln }                                             \
, /* op:gt (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt_bln }                                             \
, /* op:gt (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt_str }                                             \
, /* op:gt (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt_str }                                             \
                                                                         \
, /* op:ge (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ge_int }                                             \
, /* op:ge (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ge_int }                                             \
, /* op:ge (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ge_dec }                                             \
, /* op:ge (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ge_dec }                                             \
, /* op:ge (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ge_dbl }                                             \
, /* op:ge (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ge_dbl }                                             \
, /* op:ge (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ge_bln }                                             \
, /* op:ge (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ge_bln }                                             \
, /* op:ge (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_ge_str }                                             \
, /* op:ge (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_ge_str }                                             \
                                                                         \
                                                                         \
, /* fn:count (item*) as integer */                                      \
  { .ns = PFns_fn, .loc = "count",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_fn_count }                                              \
/* untypedAtomic needs to be casted into double therefore */             \
/* fn:max (double*) is the last entry for fn:max */                      \
, /* fn:avg (integer*) as double */                                      \
  { .ns = PFns_fn, .loc = "avg",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_integer ()) },               \
    .ret_ty = PFty_double () }                                           \
, /* fn:avg (decimal*) as decimal */                                     \
  { .ns = PFns_fn, .loc = "avg",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_decimal ()) },               \
    .ret_ty = PFty_decimal () }                                          \
, /* fn:avg (double*) as double */                                       \
  { .ns = PFns_fn, .loc = "avg",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_double ()) },                \
    .ret_ty = PFty_double () }                                           \
, /* fn:max (string*) as string */                                       \
  { .ns = PFns_fn, .loc = "max",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_string ()) },                \
    .ret_ty = PFty_string () }                                           \
, /* fn:max (integer*) as integer */                                     \
  { .ns = PFns_fn, .loc = "max",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_integer ()) },               \
    .ret_ty = PFty_integer () }                                          \
, /* fn:max (decimal*) as decimal */                                     \
  { .ns = PFns_fn, .loc = "max",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_decimal ()) },               \
    .ret_ty = PFty_decimal () }                                          \
, /* fn:max (double*) as double */                                       \
  { .ns = PFns_fn, .loc = "max",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_double ()) },                \
    .ret_ty = PFty_double () }                                           \
, /* fn:min (string*) as string */                                       \
  { .ns = PFns_fn, .loc = "min",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_string ()) },                \
    .ret_ty = PFty_string () }                                           \
, /* fn:min (integer*) as integer */                                     \
  { .ns = PFns_fn, .loc = "min",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_integer ()) },               \
    .ret_ty = PFty_integer () }                                          \
, /* fn:min (decimal*) as decimal */                                     \
  { .ns = PFns_fn, .loc = "min",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_decimal ()) },               \
    .ret_ty = PFty_decimal () }                                          \
, /* fn:min (double*) as double */                                       \
  { .ns = PFns_fn, .loc = "min",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_double ()) },                \
    .ret_ty = PFty_double () }                                           \
, /* fn:sum (integer*) as integer */                                     \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_integer ()) },               \
    .ret_ty = PFty_integer () }                                          \
, /* fn:sum (decimal*) as decimal */                                     \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_decimal ()) },               \
    .ret_ty = PFty_decimal () }                                          \
, /* fn:sum (double*) as double */                                       \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 1, .par_ty = { PFty_star (PFty_double ()) },                \
    .ret_ty = PFty_double () }                                           \
, /* fn:sum (integer*, integer?) as integer */                           \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 2, .par_ty = { PFty_star (PFty_integer ()),                 \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_integer () }                                          \
, /* fn:sum (decimal*, decimal?) as decimal */                           \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 2, .par_ty = { PFty_star (PFty_decimal ()),                 \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_decimal () }                                          \
, /* fn:sum (double*, double?) as double */                              \
  { .ns = PFns_fn, .loc = "sum",                                         \
    .arity = 2, .par_ty = { PFty_star (PFty_double ()),                  \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_double () }                                           \
, /* fn:abs (integer?) as integer? */                                    \
  { .ns = PFns_fn, .loc = "abs",                                         \
    .arity = 1, .par_ty = { PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* fn:abs (decimal?) as decimal? */                                    \
  { .ns = PFns_fn, .loc = "abs",                                         \
    .arity = 1, .par_ty = { PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* fn:abs (double?) as double? */                                      \
  { .ns = PFns_fn, .loc = "abs",                                         \
    .arity = 1, .par_ty = { PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
, /* fn:ceiling (integer?) as integer? */                                \
  { .ns = PFns_fn, .loc = "ceiling",                                     \
    .arity = 1, .par_ty = { PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* fn:ceiling (decimal?) as decimal? */                                \
  { .ns = PFns_fn, .loc = "ceiling",                                     \
    .arity = 1, .par_ty = { PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* fn:ceiling (double?) as double? */                                  \
  { .ns = PFns_fn, .loc = "ceiling",                                     \
    .arity = 1, .par_ty = { PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
, /* fn:floor (integer?) as integer? */                                  \
  { .ns = PFns_fn, .loc = "floor",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* fn:floor (decimal?) as decimal? */                                  \
  { .ns = PFns_fn, .loc = "floor",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* fn:floor (double?) as double? */                                    \
  { .ns = PFns_fn, .loc = "floor",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
, /* fn:round (integer?) as integer? */                                  \
  { .ns = PFns_fn, .loc = "round",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* fn:round (decimal?) as decimal? */                                  \
  { .ns = PFns_fn, .loc = "round",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* fn:round (double?) as double? */                                    \
  { .ns = PFns_fn, .loc = "round",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
, /* op:plus (integer, integer) as integer */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_add_int }                                    \
, /* op:plus (integer?, integer?) as integer? */                         \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_add_int }                                    \
, /* op:plus (decimal, decimal) as decimal */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_add_dec }                                    \
, /* op:plus (decimal?, decimal?) as decimal? */                         \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_add_dec }                                    \
, /* op:plus (double, double) as double */                               \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_add_dbl }                                    \
, /* op:plus (double?, double?) as double? */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_add_dbl }                                    \
                                                                         \
, /* op:minus (integer, integer) as integer */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_subtract_int }                               \
, /* op:minus (integer?, integer?) as integer? */                        \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_subtract_int }                               \
, /* op:minus (decimal, decimal) as decimal */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_subtract_dec }                               \
, /* op:minus (decimal?, decimal?) as decimal? */                        \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_subtract_dec }                               \
, /* op:minus (double, double) as double */                              \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_subtract_dbl }                               \
, /* op:minus (double?, double?) as double? */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_subtract_dbl }                               \
                                                                         \
, /* op:times (integer, integer) as integer */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_multiply_int }                               \
, /* op:times (integer?, integer?) as integer? */                        \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_multiply_int }                               \
, /* op:times (decimal, decimal) as decimal */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_multiply_dec }                               \
, /* op:times (decimal?, decimal?) as decimal? */                        \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_multiply_dec }                               \
, /* op:times (double, double) as double */                              \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_multiply_dbl }                               \
, /* op:times (double?, double?) as double? */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_multiply_dbl }                               \
                                                                         \
, /* op:div (decimal, decimal) as decimal */                             \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_divide_dec }                                 \
, /* op:div (decimal?, decimal?) as decimal? */                          \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_divide_dec }                                 \
, /* op:div (double, double) as double */                                \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_divide_dbl }                                 \
, /* op:div (double?, double?) as double? */                             \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_divide_dbl }                                 \
                                                                         \
, /* op:idiv (integer, integer) as integer */                            \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_idivide_int }                                \
, /* op:idiv (integer?, integer?) as integer? */                         \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_idivide_int }                                \
, /* op:idiv (decimal, decimal) as integer */                            \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_idivide_dec }                                \
, /* op:idiv (decimal?, decimal?) as integer? */                         \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_idivide_dec }                                \
, /* op:idiv (double, double) as integer */                              \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_idivide_dbl }                                \
, /* op:idiv (double?, double?) as integer? */                           \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_idivide_dbl }                                \
                                                                         \
, /* op:mod (integer, integer) as integer */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_modulo_int }                                 \
, /* op:mod (integer?, integer?) as integer? */                          \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_modulo_int }                                 \
, /* op:mod (decimal, decimal) as decimal */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_modulo_dec }                                 \
, /* op:mod (decimal?, decimal?) as decimal? */                          \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_modulo_dec }                                 \
, /* op:mod (double, double) as double */                                \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_modulo_dbl }                                 \
, /* op:mod (double?, double?) as double? */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_modulo_dbl }                                 \
                                                                         \
                                                                         \
, /* pf:item-sequence-to-node-sequence (elem*) as elem* */               \
  { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",              \
    .arity = 1, .par_ty = { PFty_star (                                  \
                                PFty_elem (                              \
                                    wild,                                \
                                    PFty_star (PFty_xs_anyNode ()))) },  \
    .ret_ty = PFty_star ( PFty_elem (wild,                               \
                                     PFty_star (PFty_xs_anyNode ()))) }  \
, /* pf:item-sequence-to-node-sequence (item*) as node* */               \
  { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",              \
    .arity = 1, .par_ty = { PFty_star (PFty_item ())},                   \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_pf_item_seq_to_node_seq }                               \
, /* pf:item-sequence-to-untypedAtomic (item*) as untypedAtomic */       \
  { .ns = PFns_pf, .loc = "item-sequence-to-untypedAtomic",              \
    .arity = 1, .par_ty = { PFty_star (PFty_item ())},                   \
    .ret_ty = PFty_untypedAtomic () }                                    \
, /* pf:merge-adjacent-text-nodes (node*) as node* */                    \
  { .ns = PFns_pf, .loc = "merge-adjacent-text-nodes",                   \
    .arity = 1, .par_ty = { PFty_star (PFty_node ())},                   \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_pf_merge_adjacent_text_nodes }                          \
  /* FIXME: distinct-values should be changed to anyAtomicType* */       \
, /* fn:distinct-values (atomic*) as atomic* */                          \
  { .ns = PFns_fn, .loc = "distinct-values",                             \
    .arity = 1, .par_ty = { PFty_star (PFty_string ())},                 \
    .ret_ty = PFty_star (PFty_untypedAtomic ()) }                        \
, /* op:is-same-node (node, node) as boolean */                          \
  { .ns = PFns_op, .loc = "is-same-node",                                \
    .arity = 2, .par_ty = { PFty_node (),                                \
                            PFty_node ()},                               \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_is_same_node }                                       \
, /* op:node-before (node, node) as boolean */                           \
  { .ns = PFns_op, .loc = "node-before",                                 \
    .arity = 2, .par_ty = { PFty_node (),                                \
                            PFty_node ()},                               \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_node_before }                                        \
, /* op:node-after (node, node) as boolean */                            \
  { .ns = PFns_op, .loc = "node-after",                                  \
    .arity = 2, .par_ty = { PFty_node (),                                \
                            PFty_node ()},                               \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_node_after }                                         \
, /* op:union (node*, node*) as node* */                                 \
  { .ns = PFns_op, .loc = "union",                                       \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_op_union }                                              \
, /* op:intersect (node*, node*) as node* */                             \
  { .ns = PFns_op, .loc = "intersect",                                   \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_op_intersect }                                          \
, /* op:except (node*, node*) as node* */                                \
  { .ns = PFns_op, .loc = "except",                                      \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()),                                  \
    .alg = PFbui_op_except }                                             \
, /* op:to (integer, integer) as integer* */                             \
  { .ns = PFns_op, .loc = "to",                                          \
    .arity = 2, .par_ty = { PFty_integer (), PFty_integer () },          \
    .ret_ty = PFty_star (PFty_integer ()) }                              \
, /* pf:typed-value (node) as untypedAtomic* */                          \
  { .ns = PFns_pf, .loc = "typed-value",                                 \
    .arity = 1, .par_ty = { PFty_node () },                              \
    .ret_ty = PFty_star (PFty_untypedAtomic ()),                         \
    /* FIXME: does this still fit or is it string-value? */              \
    .alg = PFbui_op_typed_value }                                        \
, /* pf:string-value (node) as string */                                 \
  { .ns = PFns_pf, .loc = "string-value",                                \
    .arity = 1, .par_ty = { PFty_node () },                              \
    .ret_ty = PFty_string (),                                            \
    .alg = PFbui_pf_string_value }                                       \
, /* fn:name (node) as string */                                         \
  { .ns = PFns_fn, .loc = "name",                                        \
    .arity = 0,                                                          \
    .ret_ty = PFty_string () }                                           \
, /* fn:name (node) as string */                                         \
  { .ns = PFns_fn, .loc = "name",                                        \
    .arity = 1, .par_ty = { PFty_opt (PFty_node ()) },                   \
    .ret_ty = PFty_string () }                                           \
, /* fn:local-name (node) as string */                                   \
  { .ns = PFns_fn, .loc = "local-name",                                  \
    .arity = 0,                                                          \
    .ret_ty = PFty_string () }                                           \
, /* fn:local-name (node) as string */                                   \
  { .ns = PFns_fn, .loc = "local-name",                                  \
    .arity = 1, .par_ty = { PFty_opt (PFty_node ()) },                   \
    .ret_ty = PFty_string () }                                           \
, /* fn:namespace-uri (node) as string */                                \
  { .ns = PFns_fn, .loc = "namespace-uri",                               \
    .arity = 0,                                                          \
    .ret_ty = PFty_string () }                                           \
, /* fn:namespace-uri (node) as string */                                \
  { .ns = PFns_fn, .loc = "namespace-uri",                               \
    .arity = 1, .par_ty = { PFty_opt (PFty_node ()) },                   \
    .ret_ty = PFty_string () }                                           \
, /* fn:string () as string */                                           \
  { .ns = PFns_fn, .loc = "string",                                      \
    .arity = 0, .par_ty = { PFty_none () },                              \
    .ret_ty = PFty_string () }                                           \
, /* fn:string (item?) as string */                                      \
  { .ns = PFns_fn, .loc = "string",                                      \
    .arity = 1, .par_ty = { PFty_opt (PFty_item ()) },                   \
    .ret_ty = PFty_string () }                                           \
, /* fn:string-join (string*, string) as string */                       \
  { .ns = PFns_fn, .loc = "string-join",                                 \
    .arity = 2, .par_ty = { PFty_star (PFty_string ()),                  \
                            PFty_string () },                            \
    .ret_ty = PFty_string () }                                           \
, /* fn:concat (string, string) as string */                             \
  /* This is more strict that the W3C variant. Maybe we can do with */   \
  /* that strict variant. */                                             \
  { .ns = PFns_fn, .loc = "concat",                                      \
    .arity = 2, .par_ty = { PFty_string(), PFty_string() },              \
    .ret_ty = PFty_string () }                                           \
, /* fn:starts-with (string?, string?) as boolean */                     \
  { .ns = PFns_fn, .loc = "starts-with",                                 \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:ends-with (string?, string?) as boolean */                       \
  { .ns = PFns_fn, .loc = "ends-with",                                   \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:normalize-space () as string */                                  \
  { .ns = PFns_fn, .loc = "normalize-space",                             \
    .arity = 0,                                                          \
    .ret_ty = PFty_string () }                                           \
, /* fn:normalize-space (string?) as string */                           \
  { .ns = PFns_fn, .loc = "normalize-space",                             \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_string () }                                           \
, /* fn:lower-case (string?) as string */                                \
  { .ns = PFns_fn, .loc = "lower-case",                                  \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_string () }                                           \
, /* fn:upper-case (string?) as string */                                \
  { .ns = PFns_fn, .loc = "upper-case",                                  \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_string () }                                           \
, /* fn:substring (string?, double) as string */                         \
  { .ns = PFns_fn, .loc = "substring",                                   \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_double () },                            \
    .ret_ty = PFty_string () }                                           \
, /* fn:substring (string?, double, double) as string */                 \
  { .ns = PFns_fn, .loc = "substring",                                   \
    .arity = 3, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_string () }                                           \
, /* fn:substring-before (string?, string?) as string */                 \
  { .ns = PFns_fn, .loc = "substring-before",                            \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_string () }                                           \
, /* fn:substring-after (string?, string?) as string */                  \
  { .ns = PFns_fn, .loc = "substring-after",                             \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_string () }                                           \
, /* fn:string-length () as integer */                                   \
  { .ns = PFns_fn, .loc = "string-length",                               \
    .arity = 0,                                                          \
    .ret_ty = PFty_integer () }                                          \
, /* fn:string-length (string?) as integer */                            \
  { .ns = PFns_fn, .loc = "string-length",                               \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_integer () }                                          \
, /* fn:translate (string?, string, string) as string */                 \
  { .ns = PFns_fn, .loc = "translate",                                   \
    .arity = 3, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_string () }                                           \
, /* fn:replace (string?, string, string) as string? */                  \
  { .ns = PFns_fn, .loc = "replace",                                     \
    .arity = 3, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_opt( PFty_string ()) }                                \
, /* fn:replace (string?, string, string, string) as string? */          \
  { .ns = PFns_fn, .loc = "replace",                                     \
    .arity = 4, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_string (),                              \
                            PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_opt( PFty_string ()) }                                \
, /* pf:node-name-eq (xs:string, xs:string,                          */  \
  /*                  (xs:anyElement | xs:anyAttribute) *)           */  \
  /*     as xs:boolean                                               */  \
  { .ns = PFns_pf, .loc = "node-name-eq",                                \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (                                  \
                                PFty_choice (PFty_xs_anyElement (),      \
                                             PFty_xs_anyAttribute ()))}, \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* fn:subsequence(node*, double) as node* */                           \
  { .ns = PFns_fn, .loc = "subsequence",                                 \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_double () },                            \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* fn:subsequence(node*, double, double) as node* */                   \
  { .ns = PFns_fn, .loc = "subsequence",                                 \
    .arity = 3, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
                                                                         \
, { .loc = 0 }                                                           \
, /* fn:subsequence(item*, double) as item* */                           \
  { .ns = PFns_fn, .loc = "subsequence",                                 \
    .arity = 2, .par_ty = { PFty_star (PFty_item ()),                    \
                            PFty_double () },                            \
    .ret_ty = PFty_star (PFty_item ()) }                                 \
, /* fn:subsequence(item*, double, double) as item* */                   \
  { .ns = PFns_fn, .loc = "subsequence",                                 \
    .arity = 3, .par_ty = { PFty_star (PFty_item ()),                    \
                            PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_star (PFty_item ()) }                                 \
                                                                         \
, { .loc = 0 }                                                           \
}

void
PFfun_xquery_fo ()
{
    PFqname_t wild = { .ns = PFns_wild, .loc = 0 };

    struct {  
        PFns_t ns;
        char *loc;                                 
        unsigned int arity;
        PFty_t par_ty[XQUERY_FO_MAX_ARITY]; 
        PFty_t ret_ty;
        struct PFla_pair_t (*alg) (const struct PFla_op_t *,
                                   struct PFla_pair_t *);
    } xquery_fo[] = XQUERY_FO;

    PFqname_t    qn;
    unsigned int n;

    PFfun_env = PFenv ();

    for (n = 0; xquery_fo[n].loc; n++) {
        assert (xquery_fo[n].arity <= XQUERY_FO_MAX_ARITY);
        
        /* construct function name */
        qn = PFqname (xquery_fo[n].ns, xquery_fo[n].loc);
        
        /* insert built-in XQuery F&O into function environment */
        PFenv_bind (PFfun_env,
                    qn,
                    (void *) PFfun_new (qn,
                                        xquery_fo[n].arity, 
                                        true,
                                        xquery_fo[n].par_ty,
                                        &(xquery_fo[n].ret_ty),
                                        xquery_fo[n].alg,
                                        NULL));
    }
                                           
}

/* vim:set shiftwidth=4 expandtab: */
