/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 *
 * Declarations for QName handling functions; functions are
 * implemented in pathfinder/qname.c.
 *
 * $Id$
 */

#ifndef QNAME_H
#define QNAME_H

/* PFns_t */
#include "ns.h"

/** XML namespace qualified name */
typedef struct PFqname_t PFqname_t;

/** This represents a QName `ns:loc'
 *  (a QName `loc' with no namespace has ns.ns = 0 and ns.uri = 0,
 *   see semantics/ns.h).
 */
struct PFqname_t {
  PFns_t ns;       /**< namespace part */
  char   *loc;     /**< local part */
};

/* is QName a wildcard (ns:*)? */
#define PFQNAME_WILDCARD(qn) ((qn).loc == 0)

/* Compare two QNames */
int 
PFqname_eq (PFqname_t, PFqname_t);

/* Create string representation of a QName */
char *
PFqname_str (PFqname_t);

/* Create string representation of a QName (use URI instead of NS prefix) */
char *
PFqname_uri_str (PFqname_t);

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
