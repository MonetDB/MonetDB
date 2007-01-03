/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Dynamic BitSet, based on PFArray
 *
 * Basic interface:
 * - #PFbitset ()            Create a new bitset, with all values set to false.
 * - #PFbitset_set (a, n, b) Set the n-th bit to the value b. 
 * - #PFbitset_get (a, n)    Get the n-th bit bit.
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


#include "pathfinder.h"

#ifdef HAVE_STDINT_H
#include <stdint.h>
#else
typedef unsigned char uint8_t;
#endif

#include "bitset.h"

typedef uint8_t bitset_unit;

#define ADDR_BITS_PER_UNIT 3
#define BITS_PER_UNIT (1 << ADDR_BITS_PER_UNIT)
#define BIT_IDX_MASK (BITS_PER_UNIT - 1)

#define BIT(x) (1 << ((x) & BIT_IDX_MASK))
#define UNIT_IDX(x) ((x) >> ADDR_BITS_PER_UNIT)
#define BITSET_UNIT_AT(a, b) (*((bitset_unit *)PFarray_at ((a), (b))))

PFbitset_t *PFbitset () 
{
    return PFarray (sizeof (bitset_unit)); 
}

void PFbitset_set (PFbitset_t *b, unsigned int pos, bool value)
{
    unsigned int size = PFarray_last (b);
    unsigned int unit_pos = UNIT_IDX (pos);

    if (unit_pos >= size)
        PFarray_nadd (b, unit_pos - size + 1); 

    /* Initialize new allocated part of array */
    for (unsigned int i = size; i <= unit_pos; i++)
        BITSET_UNIT_AT (b, i) = 0;

    if (value)
        BITSET_UNIT_AT (b, unit_pos) |= BIT (pos);
    else
        BITSET_UNIT_AT (b, unit_pos) &= ~BIT (pos);
}

bool PFbitset_get (PFbitset_t *b, unsigned int pos) 
{
    unsigned int size = PFarray_last (b);
    unsigned int unit_pos = UNIT_IDX (pos);
    if (unit_pos < size)
        return (BITSET_UNIT_AT(b, unit_pos) & BIT (pos)) != 0;
    return false;
}

PFbitset_t *PFbitset_copy (PFbitset_t *in) {
    return PFarray_copy (in);
}

void PFbitset_or (PFbitset_t *base, PFbitset_t *ext) {
    unsigned int ext_size = PFarray_last (ext);
    unsigned int base_size = PFarray_last (base);
    if (ext_size > base_size)
        /* ensure capacity */
        PFarray_nadd (base, ext_size - base_size); 
    for (unsigned int i = 0; i < ext_size; i++)
        BITSET_UNIT_AT (base, i) |= BITSET_UNIT_AT (ext, i);
}
