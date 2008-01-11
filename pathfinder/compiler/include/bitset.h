/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Dynamic BitSet, based on PFArray
 *
 * Basic interface:
 * - #PFbitset ()            Create a new bitset, with all values set to false
 * - #PFbitset_set (a, n, b) Set the n-th bit to the value b 
 * - #PFbitset_get (a, n)    Get the n-th bit bit.
 * - #PFbitset_or (a, b)    Performs a logical or of the bitsets a |= b
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

#ifndef BITSET_H
#define BITSET_H

#include "array.h"

typedef PFarray_t PFbitset_t;

/** Creates a new bitset, initalized with false. */
PFbitset_t *PFbitset(void);

/* Sets the bit on position pos to value. */
void PFbitset_set(PFbitset_t *, unsigned int pos, bool value);

/* Gets the bit on position pos. */
bool PFbitset_get(PFbitset_t *, unsigned int pos);

/* Returns a copy of this bitset. */
PFbitset_t *PFbitset_copy (PFbitset_t *in);

/* Performs a logical or of two bitsets (base |= ext). */
void PFbitset_or (PFbitset_t *base, PFbitset_t *ext);
#endif
