/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Generic pretty printer (based on [Oppen80]).
 *
 * $Id$
 */

#ifndef PRETTYP_H
#define PRETTYP_H

#include "pathfinder.h"

/** Support for colored pretty printing */
#define PFBLACK       '\xf0'
#define PFBLUE        '\xf1'
#define PFGREEN       '\xf2'
#define PFPINK        '\xf3'
#define PFRED         '\xf4'
#define PFYELLOW      '\xf5'
#define PFBOLD_BLACK  '\xf6'
#define PFBOLD_BLUE   '\xf7'
#define PFBOLD_GREEN  '\xf8'
#define PFBOLD_PINK   '\xf9'
#define PFBOLD_RED    '\xfa'
#define PFBOLD_YELLOW '\xfb'

/** Start and end block as in [Oppen80] */
#define START_BLOCK   '\x01'
#define END_BLOCK     '\x02'

void PFprettyprintf (const char *rep, ...)
  __attribute__ ((format (printf, 1, 2)));

void PFprettyp (FILE *f);

#endif   /* PRETTYP_H */

/* vim:set shiftwidth=4 expandtab: */
