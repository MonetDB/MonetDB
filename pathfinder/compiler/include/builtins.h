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

#ifndef BUITLINS_H
#define BUITLINS_H


#include "logical.h"
#include "core2alg.h"

/* 2. ACCESSORS */
/* 2.3. fn:string */
struct PFla_pair_t PFbui_fn_string_attr (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_text (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_pi (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_comm (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_elem (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_elem_attr (const PFla_op_t *loop,
                                              bool ordering,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

/* 2.4. fn:data */
struct PFla_pair_t PFbui_fn_data_attr (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_text (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_pi (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_comm (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_elem (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_elem_attr (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data (const PFla_op_t *loop,
                                  bool ordering,
                                  struct PFla_pair_t *args);

/* 3. THE ERROR FUNCTION */

/* 6. FUNCTIONS AND OPERATORS ON NUMERICS */
/* 6.2. Operators on Numeric Values */
struct PFla_pair_t PFbui_op_numeric_add (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_subtract (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_multiply (const PFla_op_t *loop,
                                              bool ordering,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_divide (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_idivide (const PFla_op_t *loop,
                                             bool ordering,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_modulo (const PFla_op_t *loop,
                                                bool ordering,
                                                struct PFla_pair_t *args);

/* 6.3. Comparison Operators on Numeric Values */
struct PFla_pair_t PFbui_op_eq_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ne_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_lt_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_le_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_gt_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ge_int (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_str (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

/* 6.4. Functions on Numeric Values */
struct PFla_pair_t PFbui_fn_abs_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_abs_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_abs_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_ceiling_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_ceiling_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_ceiling_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_floor_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_floor_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_floor_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_round_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_round_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_round_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

/* 7. FUNCTIONS ON STRINGS */
/* 7.4. Functions on String Values */
struct PFla_pair_t PFbui_fn_concat (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_join (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_length_opt (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_upper_case_opt (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_lower_case_opt (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

/* 7.5. Functions Based on Substring Matching */
struct PFla_pair_t PFbui_fn_contains (const PFla_op_t *loop,
                                      bool ordering,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_contains_opt (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_contains_opt_opt (const PFla_op_t *loop,
                                              bool ordering,
                                              struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_starts_with_opt_opt (const PFla_op_t *loop,
                                                 bool ordering,
                                                 struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_ends_with_opt_opt (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_substring_before_opt_opt (const PFla_op_t *loop,
                                                      bool ordering,
                                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_substring_after_opt_opt (const PFla_op_t *loop,
                                                     bool ordering,
                                                     struct PFla_pair_t *args);

/* 7.6. String Functions that Use Pattern Matching */

/* 9. FUNCTIONS AND OPERATORS ON BOOLEAN VALUES */
/* 9.1. Additional Boolean Constructor Functions */
struct PFla_pair_t PFbui_fn_true (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_false (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);

/* 9.2. Operators on Boolean Values */

/* 9.3. Functions on Boolean Values */
struct PFla_pair_t PFbui_fn_not_bln (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

/* 11. FUNCTIONS RELATED TO QNAMES */
/* 11.1. Additional Constructor Functions for QNames */
struct PFla_pair_t PFbui_fn_resolve_qname
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_qname (const PFla_op_t *loop, bool ordering,
                                   struct PFla_pair_t *args);

/* 14 FUNCTIONS AND OPERATORS ON NODES */
/* 14.1 fn:name */

/* 14.2. fn:local-name */

/* 14.3. fn:namespace-uri */

/* 14.4. fn:number */
struct PFla_pair_t PFbui_fn_number (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

/* 14.6. op:is-same-node */
struct PFla_pair_t PFbui_op_is_same_node (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);

/* 14.7. op:node-before */
struct PFla_pair_t PFbui_op_node_before (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

/* 14.8. op:node-after */
struct PFla_pair_t PFbui_op_node_after (const PFla_op_t *loop,
                                        bool ordering,
                                        struct PFla_pair_t *args);

/* 14.9. fn:root */

/* 15. FUNCTIONS AND OPERATORS ON SEQUENCES */
/* 15.1. General Functions and Operators on Sequences */
struct PFla_pair_t PFbui_fn_boolean_optbln (const PFla_op_t *loop,
                                            bool ordering,
                                            struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_boolean_item (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_boolean_bln (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_empty (const PFla_op_t *loop,
                                   bool ordering,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_exists (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_distinct_values (const PFla_op_t *loop,
                                             bool ordering,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_insert_before (const PFla_op_t *loop,
                                           bool ordering,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_remove (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_reverse (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_subsequence_till_end (const PFla_op_t *loop,
                                                  bool ordering,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_subsequence (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_unordered (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

/* 15.2. Functions That Test the Cardinality of Sequences */
struct PFla_pair_t PFbui_fn_zero_or_one (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_exactly_one (const PFla_op_t *loop,
                                         bool ordering,
                                         struct PFla_pair_t *args);

/* 15.3. Equals, Union, Intersection and Except */
struct PFla_pair_t PFbui_op_union (const PFla_op_t *loop,
                                   bool ordering,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_intersect (const PFla_op_t *loop,
                                       bool ordering,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_except (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

/* 15.4. Aggregate Functions */
struct PFla_pair_t PFbui_fn_count (const PFla_op_t *loop,
                                   bool ordering,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_avg (const PFla_op_t *loop,
                                 bool ordering,
                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_str (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_str (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_int (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_dec (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_dbl (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_int (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

/* 15.5. Functions and Operators that Generate Sequences */
struct PFla_pair_t PFbui_op_to (const PFla_op_t *loop,
                                bool ordering,
                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_id (const PFla_op_t *loop,
                                bool ordering,
                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_idref (const PFla_op_t *loop,
                                   bool ordering,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_doc (const PFla_op_t *loop,
                                 bool ordering,
                                 struct PFla_pair_t *args);

/* 16. CONTEXT FUNCTIONS */
/* 16.1. fn:position */

/* 16.2. fn:last */

/* #1. PATHFINDER SPECIFIC HELPER FUNCTIONS */
struct PFla_pair_t PFbui_op_or_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_and_bln (const PFla_op_t *loop,
                                     bool ordering,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_distinct_doc_order (const PFla_op_t *loop,
                                                bool ordering,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_single_atomic
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_atomic
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_attr_single
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_attr
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_wo_attr
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_merge_adjacent_text_nodes
               (const PFla_op_t *loop, bool ordering, struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_string_value_attr (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_text (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_pi (const PFla_op_t *loop,
                                             bool ordering,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_comm (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_elem (const PFla_op_t *loop,
                                               bool ordering,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_elem_attr (const PFla_op_t *loop,
                                                    bool ordering,
                                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value (const PFla_op_t *loop,
                                          bool ordering,
                                          struct PFla_pair_t *args);

/* #2. PATHFINDER SPECIFIC DOCUMENT MANAGEMENT FUNCTIONS */
struct PFla_pair_t PFbui_pf_fragment (const PFla_op_t *loop, bool ordering,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_attribute (const PFla_op_t *loop, bool ordering,
                                       struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_text (const PFla_op_t *loop, bool ordering,
                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_supernode (const PFla_op_t *loop, bool ordering,
                                       struct PFla_pair_t *args);

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
