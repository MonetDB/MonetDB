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

#ifndef BUITLINS_H
#define BUITLINS_H


#include "algebra.h"
#include "algebra_mnemonic.h"

/* ----- arithmetic operators for various implementation types ----- */
PFalg_op_t * PFbui_op_numeric_add_int (PFalg_op_t *loop, PFalg_op_t **delta,
                                       PFalg_op_t **args);
PFalg_op_t * PFbui_op_numeric_add_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                                       PFalg_op_t **args);
PFalg_op_t * PFbui_op_numeric_add_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                                       PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_subtract_int (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_subtract_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_subtract_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_multiply_int (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_multiply_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_multiply_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                                           PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_divide_int (PFalg_op_t *loop, PFalg_op_t **delta,
                                         PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_divide_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                                         PFalg_op_t **args);
PFalg_op_t *PFbui_op_numeric_divide_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                                         PFalg_op_t **args);

PFalg_op_t *PFbui_op_gt_int (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_gt_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_gt_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_gt_bln (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_gt_str (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);

PFalg_op_t *PFbui_op_lt_int (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_lt_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_lt_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_lt_bln (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_lt_str (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);

PFalg_op_t *PFbui_op_eq_int (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_eq_dec (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_eq_dbl (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_eq_bln (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);
PFalg_op_t *PFbui_op_eq_str (PFalg_op_t *loop, PFalg_op_t **delta,
                             PFalg_op_t **args);

PFalg_op_t *PFbui_op_typed_value (PFalg_op_t *loop,
                                  PFalg_op_t **delta,
                                  PFalg_op_t **args);

PFalg_op_t *PFbui_fn_boolean_bool (PFalg_op_t *loop,
                                   PFalg_op_t **delta,
                                   PFalg_op_t **args);

PFalg_op_t *PFbui_fn_boolean_optbool (PFalg_op_t *loop,
                                      PFalg_op_t **d,
                                      PFalg_op_t **args);

PFalg_op_t *PFbui_fn_empty (PFalg_op_t *loop,
                            PFalg_op_t **d,
                            PFalg_op_t **args);

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
