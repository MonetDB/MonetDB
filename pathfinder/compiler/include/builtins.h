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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2009 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

#ifndef BUITLINS_H
#define BUITLINS_H


#include "logical.h"
#include "core2alg.h"

/* 2. ACCESSORS */
/* 2.1. fn:node_name */
struct PFla_pair_t PFfn_bui_node_name_attr (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFfn_bui_node_name_elem (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFfn_bui_node_name_node (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

/* 2.3. fn:string */
struct PFla_pair_t PFbui_fn_string_attr (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_text (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_pi (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_comm (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_elem (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_elem_attr (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

/* 2.4. fn:data */
struct PFla_pair_t PFbui_fn_data_attr (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_text (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_pi (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_comm (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_elem (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data_elem_attr (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_data (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args);

/* 3. THE ERROR FUNCTION */
struct PFla_pair_t PFbui_fn_error_empty (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_error (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_error_str (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

/* 6. FUNCTIONS AND OPERATORS ON NUMERICS */
/* 6.2. Operators on Numeric Values */
struct PFla_pair_t PFbui_op_numeric_add (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_subtract (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_multiply (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_divide (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_idivide (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_numeric_modulo (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

/* 6.3. Comparison Operators on Numeric Values */
struct PFla_pair_t PFbui_op_eq_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_eq_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ne_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ne_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_lt_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_lt_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_le_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_le_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_gt_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_gt_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_ge_int (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dec (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_dbl (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_ge_str (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

/* 6.4. Functions on Numeric Values */
struct PFla_pair_t PFbui_fn_abs_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_abs_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_abs_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_ceiling_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_ceiling_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_ceiling_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_floor_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_floor_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_floor_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_round_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_round_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_round_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_sqrt_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_sqrt_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_sqrt_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_log_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_log_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_log_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

/* 7. FUNCTIONS ON STRINGS */
/* 7.4. Functions on String Values */
struct PFla_pair_t PFbui_fn_concat (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_join (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_substring (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_substring_dbl (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_string_length (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_normalize_space (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_upper_case (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_lower_case (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_translate (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

/* 7.5. Functions Based on Substring Matching */
struct PFla_pair_t PFbui_fn_contains (const PFla_op_t *loop,
                                      bool ordering,
                                      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_contains_opt (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_contains_opt_opt (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_starts_with (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_ends_with (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_substring_before (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_substring_after (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);

/* 7.6. String Functions that Use Pattern Matching */

struct PFla_pair_t PFbui_fn_matches (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_matches_str (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_replace (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_replace_str (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

/* 9. FUNCTIONS AND OPERATORS ON BOOLEAN VALUES */
/* 9.1. Additional Boolean Constructor Functions */
struct PFla_pair_t PFbui_fn_true (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_false (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

/* 9.2. Operators on Boolean Values */

/* 9.3. Functions on Boolean Values */
struct PFla_pair_t PFbui_fn_not_bln (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

/* 10. FUNCTIONS AND OPERATORS ON DURATIONS, DATES AND TIMES */
/* 10.4 Comparison Operators on Duration, Date and Time Values */
///
struct PFla_pair_t PFbui_op_yearmonthduration_lt (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_le (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_gt (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_ge (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_lt (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_le (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_gt (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_ge (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_eq (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_ne (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_le (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_lt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_ge (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_datetime_gt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_date_eq (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_date_ne (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_date_le (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_date_lt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_date_ge (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_date_gt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_time_eq (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_time_ne (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_time_le (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_time_lt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_time_ge (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_time_gt (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

/* 10.5 Component Extraction Functions on Durations, Dates and Times */

struct PFla_pair_t PFbui_fn_year_from_datetime (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_month_from_datetime (const PFla_op_t *loop,
                                                 bool ordering,
                                                 PFla_op_t **side_effects,
                                                 struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_day_from_datetime (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_hours_from_datetime (const PFla_op_t *loop,
                                                 bool ordering,
                                                 PFla_op_t **side_effects,
                                                 struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_minutes_from_datetime (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_seconds_from_datetime (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_year_from_date (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_month_from_date (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_day_from_date (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_hours_from_time (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_minutes_from_time (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_seconds_from_time (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);

/* 10.6 Arithmetic Operators on Durations */
struct PFla_pair_t PFbui_op_yearmonthduration_plus (const PFla_op_t *loop,
                                                    bool ordering,
                                                    PFla_op_t **side_effects,
                                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_minus (const PFla_op_t *loop,
                                                     bool ordering,
                                                     PFla_op_t **side_effects,
                                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_times (const PFla_op_t *loop,
                                                     bool ordering,
                                                     PFla_op_t **side_effects,
                                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_div_dbl (const PFla_op_t *loop,
                                                     bool ordering,
                                                     PFla_op_t **side_effects,
                                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_yearmonthduration_div (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_plus (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_minus (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_times (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_div_dbl (const PFla_op_t *loop,
                                                     bool ordering,
                                                     PFla_op_t **side_effects,
                                                     struct PFla_pair_t *args);
struct PFla_pair_t PFbui_op_daytimeduration_div (const PFla_op_t *loop,
                                                 bool ordering,
                                                 PFla_op_t **side_effects,
                                                 struct PFla_pair_t *args);

/* 11. FUNCTIONS RELATED TO QNAMES */
/* 11.1. Additional Constructor Functions for QNames */
struct PFla_pair_t PFbui_fn_resolve_qname (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_qname (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

/* 14 FUNCTIONS AND OPERATORS ON NODES */
/* 14.1 fn:name */

struct PFla_pair_t PFbui_fn_name (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args);

/* 14.2. fn:local-name */

struct PFla_pair_t PFbui_fn_local_name (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

/* 14.3. fn:namespace-uri */

struct PFla_pair_t PFbui_fn_namespace_uri (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

/* 14.4. fn:number */
struct PFla_pair_t PFbui_fn_number (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

/* 14.6. op:is-same-node */
struct PFla_pair_t PFbui_op_is_same_node (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);

/* 14.7. op:node-before */
struct PFla_pair_t PFbui_op_node_before (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

/* 14.8. op:node-after */
struct PFla_pair_t PFbui_op_node_after (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

/* 14.9. fn:root */
struct PFla_pair_t PFbui_fn_root (const PFla_op_t *loop,
                                  bool ordering,
                                  PFla_op_t **side_effects,
                                  struct PFla_pair_t *args);

/* 15. FUNCTIONS AND OPERATORS ON SEQUENCES */
/* 15.1. General Functions and Operators on Sequences */
struct PFla_pair_t PFbui_fn_boolean_optbln (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_boolean_item (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_fn_boolean_bln (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_empty (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_exists (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_distinct_values (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_insert_before (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_remove (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_reverse (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_subsequence_till_end (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_subsequence (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_unordered (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

/* 15.2. Functions That Test the Cardinality of Sequences */
struct PFla_pair_t PFbui_fn_zero_or_one (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_exactly_one (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

/* 15.3. Equals, Union, Intersection and Except */
struct PFla_pair_t PFbui_op_union (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_intersect (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_except (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

/* 15.4. Aggregate Functions */
struct PFla_pair_t PFbui_fn_count (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_avg (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_str (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_max_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_str (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_min_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_int (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_dec (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_zero_dbl (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_int (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_dec (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_sum_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_prod_dbl (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

/* 15.5. Functions and Operators that Generate Sequences */
struct PFla_pair_t PFbui_op_to (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_id (const PFla_op_t *loop,
                                bool ordering,
                                PFla_op_t **side_effects,
                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_idref (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_doc (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_doc_available (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_fn_collection (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

/* 16. CONTEXT FUNCTIONS */
/* 16.1. fn:position */

/* 16.2. fn:last */

/* #1. PATHFINDER SPECIFIC HELPER FUNCTIONS */
struct PFla_pair_t PFbui_op_or_bln (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);

struct PFla_pair_t PFbui_op_and_bln (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_query_cache (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_distinct_doc_order (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_single_atomic
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_atomic
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_attr_single
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_attr
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq_wo_attr
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_item_seq_to_node_seq
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_merge_adjacent_text_nodes
               (const PFla_op_t *loop,
                bool ordering,
                PFla_op_t **side_effects,
                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_string_value_attr (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_text (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_pi (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_comm (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_elem (const PFla_op_t *loop,
                                               bool ordering,
                                               PFla_op_t **side_effects,
                                               struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value_elem_attr (const PFla_op_t *loop,
                                                    bool ordering,
                                                    PFla_op_t **side_effects,
                                                    struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_string_value (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_pf_number (const PFla_op_t *loop,
                                    bool ordering,
                                    PFla_op_t **side_effects,
                                    struct PFla_pair_t *args);


/* #2. PATHFINDER SPECIFIC DOCUMENT MANAGEMENT FUNCTIONS */

struct PFla_pair_t PFbui_fn_put (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_documents (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_documents_unsafe (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_documents_str (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_documents_str_unsafe (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_docname (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_collection (const PFla_op_t *loop,
                                        bool ordering,
                                        PFla_op_t **side_effects,
                                        struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_collections (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_collections_unsafe (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_fragment (const PFla_op_t *loop,
                                      bool ordering,
                                      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_attribute (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_text (const PFla_op_t *loop,
                                   bool ordering,
                                   PFla_op_t **side_effects,
                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_supernode (const PFla_op_t *loop,
                                       bool ordering,
                                       PFla_op_t **side_effects,
                                       struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_add_doc (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_add_doc_str (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_add_doc_int (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_add_doc_str_int (const PFla_op_t *loop,
                                             bool ordering,
                                             PFla_op_t **side_effects,
                                             struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_del_doc (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_pf_nid (const PFla_op_t *loop,
                                 bool ordering,
                                 PFla_op_t **side_effects,
                                 struct PFla_pair_t *args);

/* #3. UPDATE FUNCTIONS */
struct PFla_pair_t PFbui_upd_rename (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_delete (const PFla_op_t *loop,
                                     bool ordering,
                                     PFla_op_t **side_effects,
                                     struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_insert_into_as_first (const PFla_op_t *loop,
                                                   bool ordering,
                                                   PFla_op_t **side_effects,
                                                   struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_insert_into_as_last (const PFla_op_t *loop,
                                                  bool ordering,
                                                  PFla_op_t **side_effects,
                                                  struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_insert_before (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_insert_after (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_replace_value_att (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_replace_value (const PFla_op_t *loop,
                                            bool ordering,
                                            PFla_op_t **side_effects,
                                            struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_replace_element (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);

struct PFla_pair_t PFbui_upd_replace_node (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

#ifdef HAVE_GEOXML
struct PFla_pair_t PFbui_geoxml_wkb   (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_geoxml_point (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_geoxml_distance (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_geoxml_geometry (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_geoxml_relate (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
struct PFla_pair_t PFbui_geoxml_intersection (const PFla_op_t *loop,
                                      bool ordering,
				      PFla_op_t **side_effects,
                                      struct PFla_pair_t *args);
#endif

#ifdef HAVE_PFTIJAH
/* #3. PFTIJAH SPECIFIC FUNCTIONS */

/*
 * The fti management functions
 */
struct PFla_pair_t PFbui_manage_fti_c_xx (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_c_cx (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_c_xo (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_c_co (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_e_cx (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_e_co (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_r_xx (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);
struct PFla_pair_t PFbui_manage_fti_r_xo (const PFla_op_t *loop,
                                          bool ordering,
                                          PFla_op_t **side_effects,
                                          struct PFla_pair_t *args);

/*
 * The ftfun functions
 */
struct PFla_pair_t PFbui_tijah_ftfun_b_sxx(const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

/*
 * The main query functions
 */
struct PFla_pair_t PFbui_tijah_query_i_xx (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_i_xo (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_i_sx (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_i_so (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_n_xx (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_n_xo (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_n_sx (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_query_n_so (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_nodes      (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_score      (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_ft_index_info (const PFla_op_t *loop,
                                              bool ordering,
                                              PFla_op_t **side_effects,
                                              struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_ft_index_info_s (const PFla_op_t *loop,
                                                bool ordering,
                                                PFla_op_t **side_effects,
                                                struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_tokenize (const PFla_op_t *loop,
                                         bool ordering,
                                         PFla_op_t **side_effects,
                                         struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_resultsize (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

/* the new term functions */

struct PFla_pair_t PFbui_tijah_terms (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_terms_o (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_tfall (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_tfall_o (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_tf (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_tf_o (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_fbterms (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);
struct PFla_pair_t PFbui_tijah_fbterms_o (const PFla_op_t *loop,
                                           bool ordering,
                                           PFla_op_t **side_effects,
                                           struct PFla_pair_t *args);

#endif

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
