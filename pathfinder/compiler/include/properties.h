/**
 * @file
 *
 * Properties of logical algebra expressions.
 *
 * We consider some properties that can be derived on the logical
 * level of our algebra, like key properties, or the information
 * that a column contains only constant values.  These properties
 * may still be helpful for physical optimization; we will thus
 * propagate any logical property to the physical tree as well.
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
 * 2000-2006 University of Konstanz.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef PROPERTIES_H
#define PROPERTIES_H

#include <algebra.h>

typedef struct PFprop_t PFprop_t;

#include <logical.h>

/**
 * Create new property container.
 */
/* PFprop_t *PFprop (void); */

/**
 * Test if @a attr is marked constant in container @a prop.
 */
bool PFprop_const (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Mark @a attr as constant with value @a value in container @a prop.
 */
void PFprop_mark_const (PFprop_t *prop,
                        PFalg_att_t attr, PFalg_atom_t value);

/**
 * Lookup value of @a attr in property container @a prop.  Attribute
 * @a attr must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t PFprop_const_val (const PFprop_t *prop, PFalg_att_t attr);

/**
 * Return number of attributes marked const.
 */
unsigned int PFprop_const_count (const PFprop_t *prop);

/**
 * Return name of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_att_t PFprop_const_at (PFprop_t *prop, unsigned int i);

/**
 * Return value of constant attribute number @a i (in container @a prop).
 * (Needed, e.g., to iterate over constant columns.)
 */
PFalg_atom_t PFprop_const_val_at (PFprop_t *prop, unsigned int i);

/**
 * Test if @a attr is in the list of icol columns in container @a prop
 */
bool PFprop_icol (const PFprop_t *prop, PFalg_att_t attr); 

/* 
 * count number of icols attributes
 */
unsigned int PFprop_icols_count (const PFprop_t *prop);

/**
 * Return icols attributes as an attlist.
 */
PFalg_attlist_t PFprop_icols_to_attlist (const PFprop_t *prop);

/**
 * Infer properties of a logical algebra subtree
 */
void PFprop_infer (PFla_op_t *n);

/**
 * Update properties of a single algebra node
 * using the properties of its children
 */
void PFprop_update (PFla_op_t *n);

#endif  /* PROPERTIES_H */

/* vim:set shiftwidth=4 expandtab: */
