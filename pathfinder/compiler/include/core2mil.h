/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for mil/core2mil.c that maps the core tree into
 * a MIL program.
 *
 * $Id$
 */

#ifndef CORE2MIL_H
#define CORE2MIL_H

#include "pathfinder.h"
#include "core.h"
#include "mil.h"

/** Map core expression @a c to a MIL program @a m. */
PFrc_t PFcore2mil (PFcnode_t * c, PFmnode_t ** m);


#endif   /* CORE2MIL_H */

/* vim:set shiftwidth=4 expandtab: */
