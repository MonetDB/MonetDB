/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for handling of built-in functions
 *
 * PFfun_t entries in the function environment contain function
 * pointers to functions declared in this file, given the function
 * is an XQuery built-in function for which we have an algebraic
 * implementation.
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

#ifndef BUITLINS_H
#define BUITLINS_H


#include "algebra.h"
#include "algebra_mnemonic.h"
#include "core2alg.h"

/* ----- arithmetic operators for various implementation types ----- */
struct PFalg_pair_t PFbui_op_numeric_add_int (PFalg_op_t *loop,
                                              struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_add_dec (PFalg_op_t *loop,
                                              struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_add_dbl (PFalg_op_t *loop,
                                              struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_numeric_subtract_int (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_subtract_dec (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_subtract_dbl (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_numeric_multiply_int (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_multiply_dec (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_multiply_dbl (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_numeric_divide_dec (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_divide_dbl (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_numeric_idivide_int (PFalg_op_t *loop,
                                                  struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_idivide_dec (PFalg_op_t *loop,
                                                  struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_idivide_dbl (PFalg_op_t *loop,
                                                  struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_numeric_modulo_int (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_modulo_dec (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_numeric_modulo_dbl (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_gt_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_gt_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_gt_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_gt_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_gt_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_ge_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ge_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ge_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ge_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ge_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_lt_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_lt_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_lt_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_lt_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_lt_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_le_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_le_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_le_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_le_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_le_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_eq_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_eq_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_eq_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_eq_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_eq_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_ne_int (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ne_dec (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ne_dbl (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ne_bln (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_ne_str (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_fn_not_bln (PFalg_op_t *loop,
                                      struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_or_bln (PFalg_op_t *loop ,
                                     struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_and_bln (PFalg_op_t *loop,
                                      struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_is_same_node (PFalg_op_t *loop,
                                           struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_node_before (PFalg_op_t *loop,
                                          struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_node_after (PFalg_op_t *loop,
                                         struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_union (PFalg_op_t *loop,
                                    struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_intersect (PFalg_op_t *loop,
                                        struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_op_except (PFalg_op_t *loop,
                                     struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_pf_item_seq_to_node_seq (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_pf_merge_adjacent_text_nodes (PFalg_op_t *loop,
                                                   struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_pf_distinct_doc_order (PFalg_op_t *loop,
                                                 struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_op_typed_value (PFalg_op_t *loop,
                                          struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_fn_boolean_bln (PFalg_op_t *loop,
                                          struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_fn_boolean_optbln (PFalg_op_t *loop,
                                             struct PFalg_pair_t *args);
struct PFalg_pair_t PFbui_fn_boolean_item (PFalg_op_t *loop,
                                           struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_fn_doc (PFalg_op_t *loop,
                                  struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_fn_empty (PFalg_op_t *loop,
                                    struct PFalg_pair_t *args);

struct PFalg_pair_t PFbui_pf_string_value (PFalg_op_t *loop,
                                           struct PFalg_pair_t *args);

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
