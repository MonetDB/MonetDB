/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for twig input file normalize.h.
 *
 * $Id$
 */

#ifndef NORMALIZE_H
#define NORMALIZE_H

#include "pathfinder.h"
#include "abssyn.h"

/**
 * Normalize abstract syntax tree.
 */
PFpnode_t *PFnormalize_abssyn (PFpnode_t *);

#endif   /* NORMALIZE_H */

/* vim:set shiftwidth=4 expandtab: */
