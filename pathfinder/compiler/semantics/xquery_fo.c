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

#include <assert.h>

#include "pathfinder.h"
#include "xquery_fo.h"

#include "ns.h"
#include "env.h"
#include "types.h"
#include "functions.h"

/**
 * maxmimum arity for all XQuery F&O functions
 */
#define XQUERY_FO_MAX_ARITY 4

#define XQUERY_FO                                                        \
{ /* fn:data (item*) as atomic*    (F&O 2.4) */                          \
  { .ns = PFns_fn, .loc = "data",                                        \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_atomic ()) }                               \
, /* fn:empty (item*) as boolean  (F&O 14.2.5) */                        \
  { .ns = PFns_fn, .loc = "empty",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:not (boolean) as boolean  (F&O 7.3.1) */                         \
  { .ns = PFns_fn, .loc = "not",                                         \
    .arity = 1, .par_ty = { PFty_boolean () },                           \
    .ret_ty = PFty_boolean () }                                          \
, /* fn:boolean (item) as boolean */                                     \
  { .ns = PFns_fn, .loc = "boolean",                                     \
    .arity = 1, .par_ty = { PFty_item () },                              \
    .ret_ty = PFty_boolean () }                                          \
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
, /* op:eq (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "eq",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ne (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "ne",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:lt (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "lt",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:le (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "le",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:gt (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "gt",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:ge (atomic, atomic) as boolean */                                \
  { .ns = PFns_op, .loc = "ge",                                          \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_boolean () }                                          \
, /* op:plus (atomic, atomic) as atomic */                               \
  { .ns = PFns_op, .loc = "plus",                                        \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:minus (atomic, atomic) as atomic */                              \
  { .ns = PFns_op, .loc = "minus",                                       \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:times (atomic, atomic) as atomic */                              \
  { .ns = PFns_op, .loc = "times",                                       \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:div (atomic, atomic) as atomic */                                \
  { .ns = PFns_op, .loc = "div",                                         \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:idiv (atomic, atomic) as atomic */                               \
  { .ns = PFns_op, .loc = "idiv",                                        \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:mod (atomic, atomic) as atomic */                                \
  { .ns = PFns_op, .loc = "mod",                                         \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* op:is-same-node (node?, node?) as boolean? */                       \
  { .ns = PFns_op, .loc = "is-same-node",                                \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node ())},                    \
    .ret_ty = PFty_opt (PFty_node ()) }                                  \
, /* op:node-before (node?, node?) as boolean? */                        \
  { .ns = PFns_op, .loc = "node-before",                                 \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node ())},                    \
    .ret_ty = PFty_opt (PFty_node ()) }                                  \
, /* op:node-after (node?, node?) as boolean? */                         \
  { .ns = PFns_op, .loc = "node-after",                                  \
    .arity = 2, .par_ty = { PFty_opt (PFty_node ()),                     \
                            PFty_opt (PFty_node())},                     \
    .ret_ty = PFty_opt (PFty_node ()) }                                  \
, /* op:union (item*, item*) as item* */                                 \
  { .ns = PFns_op, .loc = "union",                                       \
    .arity = 2, .par_ty = { PFty_star (PFty_item ()),                    \
                            PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_item ()) }                                 \
, /* op:intersect (item*, item*) as item* */                             \
  { .ns = PFns_op, .loc = "intersect",                                   \
    .arity = 2, .par_ty = { PFty_star (PFty_item ()),                    \
                            PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_item ()) }                                 \
, /* op:except (item*, item*) as item* */                                \
  { .ns = PFns_op, .loc = "except",                                      \
    .arity = 2, .par_ty = { PFty_star (PFty_item ()),                    \
                            PFty_star (PFty_item ()) },                  \
    .ret_ty = PFty_star (PFty_item ()) }                                 \
, /* dm:typed-value (node) as atomic */                                  \
  { .ns = PFns_pf, .loc = "typed-value",                                 \
    .arity = 1, .par_ty = { PFty_node () },                              \
    .ret_ty = PFty_atomic () }                                           \
, /* pf:range (atomic, atomic) as atomic */                              \
  { .ns = PFns_pf, .loc = "range",                                       \
    .arity = 2, .par_ty = { PFty_atomic (), PFty_atomic () },            \
    .ret_ty = PFty_atomic () }                                           \
, /* pf:distinct-doc-order (node *) as node* */                          \
  { .ns = PFns_pf, .loc = "distinct-doc-order",                          \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:is-boolean (xs:anyItem*) as xs:boolean */                        \
  { .ns = PFns_pf, .loc = "is-boolean",                                  \
    .arity = 1, .par_ty = { PFty_star (PFty_xs_anyItem()) },             \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-integer (xs:anyItem*) as xs:boolean */                        \
  { .ns = PFns_pf, .loc = "is-integer",                                  \
    .arity = 1, .par_ty = { PFty_star (PFty_xs_anyItem()) },             \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-string (xs:anyItem*) as xs:boolean */                         \
  { .ns = PFns_pf, .loc = "is-string",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_xs_anyItem()) },             \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-double (xs:anyItem*) as xs:boolean */                         \
  { .ns = PFns_pf, .loc = "is-double",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_xs_anyItem()) },             \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-text-node (node *) as xs:boolean */                           \
  { .ns = PFns_pf, .loc = "is-text-node",                                \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-processing-instruction-node (node *) as xs:boolean */         \
  { .ns = PFns_pf, .loc = "is-processing-instruction-node",              \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-document-node (node *) as xs:boolean */                       \
  { .ns = PFns_pf, .loc = "is-document-node",                            \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-element (node *) as xs:boolean */                             \
  { .ns = PFns_pf, .loc = "is-element",                                  \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_xs_boolean () }                                       \
, /* pf:is-attribute (node *) as xs:boolean */                           \
  { .ns = PFns_pf, .loc = "is-attribute",                                \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_xs_boolean () }                                       \
                                                                         \
  /***** XPath accessor functions **********/                            \
                                                                         \
, /* pf:ancestor (node *) as node *  */                                  \
  { .ns = PFns_pf, .loc = "ancestor",                                    \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:ancestor-or-self (node *) as node *  */                          \
  { .ns = PFns_pf, .loc = "ancestor-or-self",                            \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:attribute (node *) as node *  */                                 \
  { .ns = PFns_pf, .loc = "attribute",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:child (node *) as node *  */                                     \
  { .ns = PFns_pf, .loc = "child",                                       \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:descendant (node *) as node *  */                                \
  { .ns = PFns_pf, .loc = "descendant",                                  \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:descendant-or-self (node *) as node *  */                        \
  { .ns = PFns_pf, .loc = "descendant-or-self",                          \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:following (node *) as node *  */                                 \
  { .ns = PFns_pf, .loc = "following",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:following-sibling (node *) as node *  */                         \
  { .ns = PFns_pf, .loc = "following-sibling",                           \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:parent (node *) as node *  */                                    \
  { .ns = PFns_pf, .loc = "parent",                                      \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:preceding (node *) as node *  */                                 \
  { .ns = PFns_pf, .loc = "preceding",                                   \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:preceding-sibling (node *) as node *  */                         \
  { .ns = PFns_pf, .loc = "preceding-sibling",                           \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:self (node *) as node *  */                                      \
  { .ns = PFns_pf, .loc = "self",                                        \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
                                                                         \
, /* pf:ancestor-nametest (node *) as node *  */                         \
  { .ns = PFns_pf, .loc = "ancestor-nametest",                           \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:ancestor-or-self-nametest (node *) as node *  */                 \
  { .ns = PFns_pf, .loc = "ancestor-or-self-nametest",                   \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:attribute-nametest (node *) as node *  */                        \
  { .ns = PFns_pf, .loc = "attribute-nametest",                          \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:child-nametest (node *) as node *  */                            \
  { .ns = PFns_pf, .loc = "child-nametest",                              \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:descendant-nametest (node *) as node *  */                       \
  { .ns = PFns_pf, .loc = "descendant-nametest",                         \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:descendant-or-self-nametest (node *) as node *  */               \
  { .ns = PFns_pf, .loc = "descendant-or-self-nametest",                 \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:following-nametest (node *) as node *  */                        \
  { .ns = PFns_pf, .loc = "following-nametest",                          \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:following-sibling-nametest (node *) as node *  */                \
  { .ns = PFns_pf, .loc = "following-sibling-nametest",                  \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:parent-nametest (node *) as node *  */                           \
  { .ns = PFns_pf, .loc = "parent-nametest",                             \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:preceding-nametest (node *) as node *  */                        \
  { .ns = PFns_pf, .loc = "preceding-nametest",                          \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:preceding-sibling-nametest (node *) as node *  */                \
  { .ns = PFns_pf, .loc = "preceding-sibling-nametest",                  \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
, /* pf:self-nametest (node *) as node *  */                             \
  { .ns = PFns_pf, .loc = "self-nametest",                               \
    .arity = 3, .par_ty = { PFty_xs_string (),                           \
                            PFty_xs_string (),                           \
                            PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_node ()) }                                 \
                                                                         \
, /* pf:comment-filter (node *) as comment *  */                         \
  { .ns = PFns_pf, .loc = "comment-filter",                              \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_comm ()) }                                 \
, /* pf:text-filter (node *) as text *  */                               \
  { .ns = PFns_pf, .loc = "text-filter",                                 \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_text ()) }                                 \
, /* pf:processing-instruction-filter (node *) as processing-instruction *  */ \
  { .ns = PFns_pf, .loc = "processing-instruction-filter",               \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_pi ()) }                                   \
, /* pf:document-filter (node *) as document { xs:anyType } *  */        \
  { .ns = PFns_pf, .loc = "document-filter",                             \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_doc (PFty_xs_anyType ())) }                \
, /* pf:element-filter (node *) as element *  */                         \
  { .ns = PFns_pf, .loc = "element-filter",                              \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_xs_anyElement ()) }                        \
, /* pf:attribute-filter (node *) as attribute *  */                     \
  { .ns = PFns_pf, .loc = "attribute-filter",                            \
    .arity = 1, .par_ty = { PFty_star (PFty_node ()) },                  \
    .ret_ty = PFty_star (PFty_xs_anyAttribute ()) }                      \
                                                                         \
, /* pf:root () as document { xs:anyType } *  */                         \
  { .ns = PFns_pf, .loc = "root",                                        \
    .arity = 0, .par_ty = { },                                           \
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
        PFns_t ns; char *loc;                                 
        unsigned int arity; PFty_t par_ty[XQUERY_FO_MAX_ARITY]; 
        PFty_t ret_ty; 
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
                                        &(xquery_fo[n].ret_ty)));
    }
                                           
}

/* vim:set shiftwidth=4 expandtab: */
