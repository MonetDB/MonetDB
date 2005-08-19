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
PFprop_t *PFprop (void);

/**
 * Test if @a attr is marked constant in container @a prop.
 */
bool PFprop_const (const PFprop_t *prop, const PFalg_att_t attr);

/**
 * Mark @a attr as constant with value @a value in container @a prop.
 */
void PFprop_mark_const (PFprop_t *prop,
                        const PFalg_att_t attr, PFalg_atom_t value);

/**
 * Lookup value of @a attr in property container @a prop.  Attribute
 * @a attr must be marked constant, otherwise the function will fail.
 */
PFalg_atom_t PFprop_const_val (const PFprop_t *prop, const PFalg_att_t attr);

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
 * Infer properties of a logical algebra subtree
 */
void PFprop_infer (PFla_op_t *n);

#endif  /* PROPERTIES_H */

/* vim:set shiftwidth=4 expandtab: */
