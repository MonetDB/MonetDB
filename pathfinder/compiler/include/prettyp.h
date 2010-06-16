/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Generic pretty printer (based on [Oppen80]).
 *
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2010 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$
 */

#ifndef PRETTYP_H
#define PRETTYP_H

#include "array.h"

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

void PFprettyp_init (void);

void PFprettyprintf (const char *rep, ...)
  __attribute__ ((format (printf, 1, 2)));

void PFprettyp (PFchar_array_t *a);

void PFprettyp_extended (PFchar_array_t *a, 
                         unsigned int width,
                         unsigned int indent);

#endif   /* PRETTYP_H */

/* vim:set shiftwidth=4 expandtab: */
