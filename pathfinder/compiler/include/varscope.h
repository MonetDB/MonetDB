/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Declarations for varscope.c (Variable scope checking)
 *
 * $Id$
 */

#ifndef VARSCOPE_H
#define VARSCOPE_H

#include "abssyn.h"

/* Check variable scoping rules */
PFrc_t PFvarscope (PFpnode_t *);

#endif  /* VARSCOPE_H */

/* vim:set shiftwidth=4 expandtab: */
