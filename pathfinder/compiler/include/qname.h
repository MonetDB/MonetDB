/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for QName handling functions; functions are
 * implemented in pathfinder/qname.c.
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

#ifndef QNAME_H
#define QNAME_H

/* PFns_t */
#include "ns.h"

typedef unsigned int PFqname_t ;

typedef struct PFqname_raw_t PFqname_raw_t;

struct PFqname_raw_t {
    char *prefix;
    char *loc;
};

/* is namespace the wildcard namespace? */
#define PFQNAME_NS_WILDCARD(qn) PFqname_ns_wildcard (qn)

/* is local name a wildcard (ns:*)? */
#define PFQNAME_LOC_WILDCARD(qn) PFqname_loc_wildcard (qn)

/* is QName the wildcard *:* ? */
#define PFQNAME_WILDCARD(qn) \
    (PFQNAME_NS_WILDCARD (qn) && PFQNAME_LOC_WILDCARD (qn))

/* Compare two QNames */
int 
PFqname_eq (PFqname_t, PFqname_t);

/* Create string representation of a QName */
char *
PFqname_str (PFqname_t);

/* Create string representation of a QName (use URI instead of NS prefix) */
char *
PFqname_uri_str (PFqname_t);

/** Return the prefix part of a QName */
char *
PFqname_prefix (PFqname_t qn);

/** Return namespace part of a QName */
PFns_t
PFqname_ns (PFqname_t qn);

/** Return the URI part of a QName */
char *
PFqname_uri (PFqname_t qn);

/** Return the local name of a QName */
char *
PFqname_loc (PFqname_t qn);

/* is namespace the wildcard namespace? */
bool PFqname_ns_wildcard (PFqname_t);

/* is local name a wildcard (ns:*)? */
bool PFqname_loc_wildcard (PFqname_t q);

/* Extract QName from a string */
PFqname_raw_t PFqname_raw (const char *);

char * PFqname_raw_str (PFqname_raw_t);

int PFqname_raw_eq (PFqname_raw_t, PFqname_raw_t);

PFqname_t PFqname (PFns_t, const char *);

void PFqname_init (void);

#endif   /* QNAME_H */

/* vim:set shiftwidth=4 expandtab: */
