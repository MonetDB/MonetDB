/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @brief Global XQuery information.
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

#ifndef PF_XQ_H
#define PF_XQ_H

/* PFqname_t */
#include "qname.h"    

enum PFempty_order_t {
      greatest
    , least
    , undefined
};
typedef enum PFempty_order_t PFempty_order_t;

enum PFrevalidation_t {
      revalid_strict
    , revalid_lax
    , revalid_skip
};
typedef enum PFrevalidation_t PFrevalidation_t;

/**
 * Declarations given in the input query (encoding, ordering mode, etc.)
 */
struct PFquery_t {
    char           *version;             /**< XQuery version in query */
    char           *encoding;            /**< Encoding specified in query */
    bool            ordering;            /**< ordering declaration in query */
    PFempty_order_t empty_order;         /**< `declare default order' */
    bool            inherit_ns;
    bool            pres_boundary_space; /**< perserve boundary space? */
    PFrevalidation_t revalid;
};
typedef struct PFquery_t PFquery_t;

struct PFsort_t {
  enum { p_asc, p_desc }       dir;     /**< ascending/descending */
  enum { p_greatest, p_least } empty;   /**< empty greatest/empty least */
  char                        *coll;    /**< collation (may be 0) */
};

/** XQuery `order by' modifier (see W3C XQuery, 3.8.3) */
typedef struct PFsort_t PFsort_t;

/**
 * Information in a Pragma:
 *   (# qname content #)
 */
struct PFpragma_t {
    union {
        PFqname_raw_t qname_raw;
        PFqname_t     qname;
    } qn;
    char       *content;
};
typedef struct PFpragma_t PFpragma_t;

#endif  /* PF_XQ_H */

/* vim:set shiftwidth=4 expandtab: */
