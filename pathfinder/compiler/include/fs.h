/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Pathfinder's implementation of the XQuery Formal Semantics
 * (W3C WD November 15, 2002): map an abstract syntax tree to a core
 * language tree. 
 *
 * $Id$
 */

#ifndef FS_H
#define FS_H

#include "core.h"
#include "abssyn.h"

/* XQuery core mapping */
PFcnode_t *PFfs (PFpnode_t *);

#endif

/* vim:set shiftwidth=4 expandtab: */
