/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
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
 *          Jens Teubner <jens.teubner@uni-konstanz.de>
 *
 * $Id$
 */

#ifndef CORE2ALG_H
#define CORE2ALG_H


struct PFalg_pair_t {
    struct PFalg_op_t *result;
    struct PFalg_op_t *doc;
};

#include "core.h"

/** Compile XQuery Core into Relational Algebra */
struct PFalg_op_t *PFcore2alg (PFcnode_t *);

/* ............. environment entry specification .............. */

/**
 * Each entry in the (variable) environment consists of a reference to
 * the variable (pointer to PFvar_t) and the algebra operator that
 * represents the variable. The environment itself will be represented
 * by an array.
 */

/** environment entry node */
struct PFalg_env_t {
    PFvar_t        *var;
    struct PFalg_op_t     *result;
    struct PFalg_op_t     *doc;
};
/** environment entry node */
typedef struct PFalg_env_t PFalg_env_t;


#endif   /* CORE2ALG_H */

/* vim:set shiftwidth=4 expandtab: */
