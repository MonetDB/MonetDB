/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Resolve XML namespaces (NS) in the abstract syntax tree.
 *
 * $Id$
 */

#ifndef NSRES_H
#define NSRES_H

#include "abssyn.h"

/**
 * Resolve NS usage in a query.
 */
void PFns_resolve (PFpnode_t *);

#endif /* NSRES_H */

/* vim:set shiftwidth=4 expandtab: */
