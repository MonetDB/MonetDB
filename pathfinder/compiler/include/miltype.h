/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for miltype.c (tag core tree with MIL implementation types)
 *
 * $Id$
 */

#ifndef MILTYPE_H
#define MILTYPE_H

#include "pathfinder.h"
#include "oops.h"
/* #include "milty.h" */
#include "core.h"

/** Find MIL implementation type corresponding to XQuery type */
PFmty_t PFty2mty (PFty_t);

/** tag core tree with Monet implementation types */
void PFmiltype (PFcnode_t *);

#endif   /* MILTYPE_H */

/* vim:set shiftwidth=4 expandtab: */
