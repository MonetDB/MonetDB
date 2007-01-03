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

/** XML namespace qualified name */
typedef struct PFqname_t PFqname_t;

/**
 * This represents a QName `ns:loc'
 * See top of qname.c for detailed semantics of this field.
 */
struct PFqname_t {
  PFns_t ns;       /**< namespace part */
  char   *loc;     /**< local part */
};

/* is namespace the wildcard namespace? */
#define PFQNAME_NS_WILDCARD(qn) ((qn).ns.prefix == NULL)

/* is local name a wildcard (ns:*)? */
#define PFQNAME_LOC_WILDCARD(qn) ((qn).loc == NULL)

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
PFqname_ns (PFqname_t qn);

/** Return the URI part of a QName */
char *
PFqname_uri (PFqname_t qn);

/** Return the local name of a QName */
char *
PFqname_loc (PFqname_t qn);

/* Extract QName from a string */
PFqname_t PFstr_qname (char *);

PFqname_t PFqname (PFns_t, char *);

#endif   /* QNAME_H */

/* vim:set shiftwidth=4 expandtab: */
