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


#include "logical.h"
#include "core2alg.h"

/* ----- arithmetic operators for various implementation types ----- */
struct PFla_pair_t PFbui_op_numeric_add_int (const PFla_op_t *loop,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_add_dec (const PFla_op_t *loop,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_add_dbl (const PFla_op_t *loop,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_subtract_int (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_subtract_dec (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_subtract_dbl (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_multiply_int (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_multiply_dec (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_multiply_dbl (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_divide_dec (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_divide_dbl (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_idivide_int (const PFla_op_t *loop,
                                                 struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_idivide_dec (const PFla_op_t *loop,
                                                 struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_idivide_dbl (const PFla_op_t *loop,
                                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_modulo_int (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_modulo_dec (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_numeric_modulo_dbl (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_gt_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ge_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_lt_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_le_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_eq_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ne_int (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dec (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dbl (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_str (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_not_bln (const PFla_op_t *loop,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_or_bln (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_and_bln (const PFla_op_t *loop,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_is_same_node (const PFla_op_t *loop,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_node_before (const PFla_op_t *loop,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_node_after (const PFla_op_t *loop,
                                        struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_union (const PFla_op_t *loop,
                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_intersect (const PFla_op_t *loop,
                                       struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_except (const PFla_op_t *loop,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_item_seq_to_node_seq (const PFla_op_t *loop,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_merge_adjacent_text_nodes (const PFla_op_t *loop,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_distinct_doc_order (const PFla_op_t *loop,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_typed_value (const PFla_op_t *loop,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_boolean_bln (const PFla_op_t *loop,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_boolean_optbln (const PFla_op_t *loop,
                                            struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_boolean_item (const PFla_op_t *loop,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_doc (const PFla_op_t *loop,
                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_empty (const PFla_op_t *loop,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_string_value_attr (const PFla_op_t *loop,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_text (const PFla_op_t *loop,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value (const PFla_op_t *loop,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_count (const PFla_op_t *loop,
                                   struct PFla_pair_t *args);

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
