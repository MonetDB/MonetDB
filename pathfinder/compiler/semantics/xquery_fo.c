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
 *          Maurice van Keulen <M.van.Keulen@bigfoot.com>
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

/*
 * NOTE (Revision Information):
 *
 * Changes in the Core2MIL_Summer2004 branch have been merged into
 * this file on July 15, 2004. I have tagged this file in the
 * Core2MIL_Summer2004 branch with `merged-into-main-15-07-2004'.
 *
 * For later merges from the Core2MIL_Summer2004, please only merge
 * the changes since this tag.
 *
 * Jens
 */
  
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

#define XQUERY_FO                                                        \
{ /* fn:data (item*) as atomic*    (F&O 2.4) */                          \
  { .ns = PFns_fn, .loc = "data",                                        \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_atomic ()) }                               \
, /* fn:doc (string?) as document? - FIXME: is type of PFty_doc right? */\
  { .ns = PFns_fn, .loc = "doc",                                         \
    .arity = 1, .par_ty = { PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_doc (PFty_xs_anyNode ())) }                 \
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
    .ret_ty = PFty_boolean () }                                          \
, /* fn:not (boolean) as boolean  (F&O 7.3.1) */                         \
  { .ns = PFns_fn, .loc = "not",                                         \
    .arity = 1, .par_ty = { PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:boolean (item*) as boolean */                                    \
  { .ns = PFns_fn, .loc = "boolean",                                     \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:contains (string?, string?) as boolean */                        \
  { .ns = PFns_fn, .loc = "contains",                                    \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:count (item*) as integer */                                      \
  { .ns = PFns_fn, .loc = "count",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_integer () }                                          \
, /* fn:error (item?) as none */                                         \
  { .ns = PFns_fn, .loc = "error",                                       \
    .arity = 1, .par_ty = { PFty_opt (PFty_item ()) },                   \
    .ret_ty = PFty_none () }                                             \
, /* op:or (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "or",                                          \
    .arity = 2, .par_ty = { PFty_boolean (), PFty_boolean () },          \
    .ret_ty = PFty_boolean () }                                          \
, /* op:and (boolean, boolean) as boolean */                             \
  { .ns = PFns_op, .loc = "and",                                         \
    .arity = 2, .par_ty = { PFty_boolean (), PFty_boolean () },          \
    .ret_ty = PFty_boolean () }                                          \
                                                                         \
, /* op:eq (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_eq }                                                 \
, /* op:eq (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:eq (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:eq (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:eq (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
, /* op:ne (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ne (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ne (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ne (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ne (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
, /* op:lt (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_lt }                                                 \
, /* op:lt (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:lt (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:lt (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:lt (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
, /* op:le (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:le (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:le (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:le (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:le (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
, /* op:gt (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt }                                                 \
, /* op:gt (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt }                                                 \
, /* op:gt (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt }                                                 \
, /* op:gt (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:gt (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean (),                                           \
    .alg = PFbui_op_gt }                                                 \
, /* op:gt (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()),                                \
    .alg = PFbui_op_gt }                                                 \
, /* op:gt (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:gt (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:gt (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:gt (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
, /* op:ge (integer, integer) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (integer?, integer?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ge (decimal, decimal) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (decimal?, decimal?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ge (double, double) as boolean */                                \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (double?, double?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ge (boolean, boolean) as boolean */                              \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_boolean (),                             \
                            PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (boolean?, boolean?) as boolean? */                           \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_boolean ()),                  \
                            PFty_opt (PFty_boolean ()) },                \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:ge (string, string) as boolean */                                \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_string (),                              \
                            PFty_string () },                            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (string?, string?) as boolean? */                             \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_opt (PFty_string ()),                   \
                            PFty_opt (PFty_string ()) },                 \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
                                                                         \
                                                                         \
, /* op:plus (integer, integer) as integer */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_add }                                        \
, /* op:plus (integer?, integer?) as integer? */                         \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_add }                                        \
, /* op:plus (decimal, decimal) as decimal */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_add }                                        \
, /* op:plus (decimal?, decimal?) as decimal? */                         \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_add }                                        \
, /* op:plus (double, double) as double */                               \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_add }                                        \
, /* op:plus (double?, double?) as double? */                            \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_add }                                        \
                                                                         \
, /* op:minus (integer, integer) as integer */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_subtract }                                   \
, /* op:minus (integer?, integer?) as integer? */                        \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_subtract }                                   \
, /* op:minus (decimal, decimal) as decimal */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_subtract }                                   \
, /* op:minus (decimal?, decimal?) as decimal? */                        \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_subtract }                                   \
, /* op:minus (double, double) as double */                              \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_subtract }                                   \
, /* op:minus (double?, double?) as double? */                           \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_subtract }                                   \
                                                                         \
, /* op:times (integer, integer) as integer */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_multiply }                                   \
, /* op:times (integer?, integer?) as integer? */                        \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_multiply }                                   \
, /* op:times (decimal, decimal) as decimal */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_multiply }                                   \
, /* op:times (decimal?, decimal?) as decimal? */                        \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_multiply }                                   \
, /* op:times (double, double) as double */                              \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_multiply }                                   \
, /* op:times (double?, double?) as double? */                           \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_multiply }                                   \
                                                                         \
, /* op:div (integer, integer) as integer */                             \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer (),                                           \
    .alg = PFbui_op_numeric_divide }                                     \
, /* op:div (integer?, integer?) as integer? */                          \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()),                                \
    .alg = PFbui_op_numeric_divide }                                     \
, /* op:div (decimal, decimal) as decimal */                             \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal (),                                           \
    .alg = PFbui_op_numeric_divide }                                     \
, /* op:div (decimal?, decimal?) as decimal? */                          \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()),                                \
    .alg = PFbui_op_numeric_divide }                                     \
, /* op:div (double, double) as double */                                \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double (),                                            \
    .alg = PFbui_op_numeric_divide }                                     \
, /* op:div (double?, double?) as double? */                             \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()),                                 \
    .alg = PFbui_op_numeric_divide }                                     \
                                                                         \
, /* op:idiv (integer, integer) as integer */                            \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer () }                                          \
, /* op:idiv (integer?, integer?) as integer? */                         \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* op:idiv (decimal, decimal) as decimal */                            \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal () }                                          \
, /* op:idiv (decimal?, decimal?) as decimal? */                         \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* op:idiv (double, double) as double */                               \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double () }                                           \
, /* op:idiv (double?, double?) as double? */                            \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
                                                                         \
, /* op:mod (integer, integer) as integer */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_integer (),                             \
                            PFty_integer () },                           \
    .ret_ty = PFty_integer () }                                          \
, /* op:mod (integer?, integer?) as integer? */                          \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_integer ()),                  \
                            PFty_opt (PFty_integer ()) },                \
    .ret_ty = PFty_opt (PFty_integer ()) }                               \
, /* op:mod (decimal, decimal) as decimal */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_decimal (),                             \
                            PFty_decimal () },                           \
    .ret_ty = PFty_decimal () }                                          \
, /* op:mod (decimal?, decimal?) as decimal? */                          \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_decimal ()),                  \
                            PFty_opt (PFty_decimal ()) },                \
    .ret_ty = PFty_opt (PFty_decimal ()) }                               \
, /* op:mod (double, double) as double */                                \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_double (),                              \
                            PFty_double () },                            \
    .ret_ty = PFty_double () }                                           \
, /* op:mod (double?, double?) as double? */                             \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_opt (PFty_double ()),                   \
                            PFty_opt (PFty_double ()) },                 \
    .ret_ty = PFty_opt (PFty_double ()) }                                \
                                                                         \
                                                                         \
, /* pf:item-sequence-to-node-sequence (item*) as node* */               \
  { .ns = PFns_pf, .loc = "item-sequence-to-node-sequence",              \
    .arity = 1, .par_ty = { PFty_star (PFty_item ())},                   \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:item-sequence-to-untypedAtomic (item*) as untypedAtomic */       \
  { .ns = PFns_pf, .loc = "item-sequence-to-untypedAtomic",              \
    .arity = 1, .par_ty = { PFty_star (PFty_item ())},                   \
    .ret_ty = PFty_untypedAtomic () }                                    \
  /* FIXME: distinct-values should be changed to anyAtomicType* */       \
, /* fn:distinct-values (atomic*) as atomic* */                          \
  { .ns = PFns_fn, .loc = "distinct-values",                             \
    .arity = 1, .par_ty = { PFty_star (PFty_string ())},                 \
    .ret_ty = PFty_star (PFty_untypedAtomic ()) }                        \
, /* op:is-same-node (node?, node?) as boolean? */                       \
  { .ns = PFns_op, .loc = "is-same-node",                                \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node ())},                    \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:node-before (node?, node?) as boolean? */                        \
  { .ns = PFns_op, .loc = "node-before",                                 \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node ())},                    \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:node-after (node?, node?) as boolean? */                         \
  { .ns = PFns_op, .loc = "node-after",                                  \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node())},                     \
    .ret_ty = PFty_opt (PFty_boolean ()) }                               \
, /* op:union (node*, node*) as node* */                                 \
  { .ns = PFns_op, .loc = "union",                                       \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* op:intersect (node*, node*) as node* */                             \
  { .ns = PFns_op, .loc = "intersect",                                   \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* op:except (node*, node*) as node* */                                \
  { .ns = PFns_op, .loc = "except",                                      \
    .arity = 2, .par_ty = { PFty_star (PFty_node ()),                    \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
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
, /* pf:distinct-doc-order (node *) as node* */                          \
  { .ns = PFns_pf, .loc = "distinct-doc-order",                          \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
                                                                         \
, /* pf:root () as document { xs:anyType } *  */                         \
  { .ns = PFns_pf, .loc = "root",                                        \
    .arity = 0, .par_ty = { PFty_none () },                              \
    .ret_ty = PFty_star (PFty_doc (PFty_xs_anyType ())) }                \
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
                                                                         \
, { .loc = 0 }                                                           \
}

void
PFfun_xquery_fo ()
{
    struct {  
        PFns_t ns;
        char *loc;                                 
        unsigned int arity;
        PFty_t par_ty[XQUERY_FO_MAX_ARITY]; 
        PFty_t ret_ty;
        struct PFalg_op_t * (*alg) (struct PFalg_op_t *, struct PFalg_op_t **,
                                    struct PFalg_op_t **);
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
                                        xquery_fo[n].alg));
    }
                                           
}

/* vim:set shiftwidth=4 expandtab: */
