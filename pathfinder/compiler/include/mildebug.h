/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Declarations for mildebug.c; dump MIL
 * tree in AY&T dot format or human readable
 *
 * $Id$
 */

#ifndef MILDEBUG_H
#define MILDEBUG_H

/* FILE, ... */
#include <stdio.h>

/* node, ... */
#include "mil.h"

extern char *m_id[];

PFrc_t PFmil_dot (FILE *f, PFmnode_t *root);

PFrc_t PFmil_pretty (FILE *f, PFmnode_t *root);

#endif     /* MILDEBUG_H */

/* vim:set shiftwidth=4 expandtab: */
