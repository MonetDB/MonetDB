/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file abssynprint.h 
 * 
 * Debugging: dump XQuery abstract syntax tree in
 * AY&T dot format or human readable
 *
 * $Id$
 */

#ifndef ABSSYNPRINT_H
#define ABSSYNPRINT_H

/* FILE, ... */
#include <stdio.h>

/* node, ... */
#include "abssyn.h"

/** Node names to print out for all the abstract syntax tree nodes. */
extern char *p_id[];

PFrc_t PFabssyn_dot (FILE *f, PFpnode_t *root);

PFrc_t PFabssyn_pretty (FILE *f, PFpnode_t *root);

#endif

/* vim:set shiftwidth=4 expandtab: */
