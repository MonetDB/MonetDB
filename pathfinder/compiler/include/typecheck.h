/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/*
 * @file
 *
 * Type inference (static semantics) and type checking for XQuery core.
 *
 * $Id$
 */

#ifndef TYPECHECK_H
#define TYPECHECK_H

#include "pathfinder.h"

/* PFcnode_t */
#include "core.h"

PFcnode_t *PFty_check (PFcnode_t *);

#endif

/* vim:set shiftwidth=4 expandtab: */
