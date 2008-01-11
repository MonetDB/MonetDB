/**
 * @file
 *
 * Introduce the borders of recursion bodies.
 * (This enables the MIL generation to detect expressions
 *  that are invariant to the recursion body.)
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
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef INTRO_REC_BORDER_H
#define INTRO_REC_BORDER_H

#include "physical.h"

/**
 * Introduce boundary operators for every recursion
 * such that the MIL generation detects expressions
 * that are invariant to the recursion body.
 */
PFpa_op_t * PFpa_intro_rec_borders (PFpa_op_t *n);

#endif  /* INTRO_REC_BORDER_H */

/* vim:set shiftwidth=4 expandtab: */
