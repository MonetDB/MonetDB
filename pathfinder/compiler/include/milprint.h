/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Convert the internal representation of a MIL programm into a
 * string. Declarations for mil/milprint.c
 *
 * $Id$
 */

#ifndef MILPRINT_H
#define MILPRINT_H

#include "pathfinder.h"
#include "mil.h"

/** Indentation width for MIL output */
#define INDENT_WIDTH 4

/**
 * Convert the internal representation of a MIL program into a
 * string representation that can serve as an input to Monet.
 * 
 * @param m   The MIL tree to print
 * @param str String representation of the MIL program.
 * @return Status code
 */

PFarray_t * PFmil_gen (PFmnode_t * m);
void PFmilprint (FILE *stream, PFarray_t * milprg);

#endif    /* MILPRINT_H */

/* vim:set shiftwidth=4 expandtab: */
