/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Global definitions for Pathfinder compiler.
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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2008 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef PATHFINDER_H
#define PATHFINDER_H

#include <pf_config.h>

/* AIX requires this to be the first thing in the file.  */
#ifdef HAVE_ALLOCA_H
#  include <alloca.h>
#else
#  ifdef _AIX
#pragma alloca
#  else
#   ifndef alloca       /* predefined by HP cc +Olibcalls */
void *alloca(size_t);
#   endif
#  endif
#endif

#include <setjmp.h>

#ifndef NULL
/** Make sure we have NULL available */
#define NULL 0
#endif

/* boolean type `bool' and constants `true', `false' */
#ifdef HAVE_STDBOOL_H
#include <stdbool.h>
#elif !defined(HAVE_BOOL)
#define bool    char
#define true    (char)1
#define false   (char)0
#endif

/** fatalities now lead to a longjump instead of exit() */
extern jmp_buf PFexitPoint;       

/** information on textual location of XQuery parse tree node */
typedef struct PFloc_t PFloc_t;

/** information on textual location of XQuery parse tree node */
struct PFloc_t {
    unsigned int first_row;    /**< row number in which location starts. */
    unsigned int first_col;    /**< column number in which location starts. */
    unsigned int last_row;     /**< row number in which location ends. */
    unsigned int last_col;     /**< column number in which location ends. */
};

/**
 * We currently do not really implement the XQuery type xs:decimal.
 * For now, it is implemented as a C double (which actually way off
 * the XQuery specification).
 *
 * @warning Only few compiler phases actually use this typedef here!
 *          Most phases explicitly use double, so you have to change
 *          those first before you dare to change this typedef!
 */
typedef double dec;

/**
 * We still require the "milprint_summer" code, but started to
 * wrap it into ENABLE_MILPRINT_SUMMER conditions.
 */
#define ENABLE_MILPRINT_SUMMER 1

#endif  /* PATHFINDER_H */

/* vim:set shiftwidth=4 expandtab: */
