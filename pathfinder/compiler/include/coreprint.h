/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Declarations for coreprint.c; dump XQuery core language
 * tree in AY&T dot format or human readable
 *
 * $Id$
 */

#ifndef COREPRINT_H
#define COREPRINT_H

/* FILE, ... */
#include <stdio.h>

/* node, ... */
#include "core.h"

extern char *c_id[];

void PFcore_dot (FILE *f, PFcnode_t *root);

void PFcore_pretty (FILE *f, PFcnode_t *root);

#endif     /* COREPRINT_H */

/* vim:set shiftwidth=4 expandtab: */
