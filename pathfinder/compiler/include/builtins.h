/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for handling og built-in functions
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

/**
 * Create algebra expressions to represent build-in XQuery
 * functions.
 * 'args' is an array of algebra operators which represents
 * the argument list of the function. It may be of variable
 * length.
 */
PFalg_op_t * PFbui_op_numeric_add (PFalg_op_t *loop, PFalg_op_t **delta,
                                   PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_subtract (PFalg_op_t *loop, PFalg_op_t **delta,
                                       PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_multiply (PFalg_op_t *loop, PFalg_op_t **delta,
                                       PFalg_op_t **args);

PFalg_op_t *PFbui_op_numeric_divide (PFalg_op_t *loop, PFalg_op_t **delta,
                                     PFalg_op_t **args);


PFalg_op_t *PFbui_op_typed_value (PFalg_op_t *loop __attribute__((unused)),
                                  PFalg_op_t **delta __attribute__((unused)),
                                  PFalg_op_t **args);

PFalg_op_t *PFbui_op_gt (PFalg_op_t *loop __attribute__((unused)),
                         PFalg_op_t **delta __attribute__((unused)),
                         PFalg_op_t **args);

PFalg_op_t *PFbui_op_lt (PFalg_op_t *loop __attribute__((unused)),
                         PFalg_op_t **delta __attribute__((unused)),
                         PFalg_op_t **args);

PFalg_op_t *PFbui_op_eq (PFalg_op_t *loop __attribute__((unused)),
                         PFalg_op_t **delta __attribute__((unused)),
                         PFalg_op_t **args);

#endif   /* BUITLINS_H */

/* vim:set shiftwidth=4 expandtab: */
