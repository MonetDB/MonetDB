/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * Definitions for guide nodes.
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
 */
#ifndef LOAD_STATS_H
#define LOAD_STATS_H

#include "qname.h"

typedef PFarray_t PFguide_list_t;
#define PFguide_list_last(g) PFarray_last((g))
#define PFguide_at(g,i) (*(PFguide_tree_t **) PFarray_at ((g),(i)))

/* The guide tree */
typedef struct PFguide_tree_t PFguide_tree_t;

#include "logical.h"

/* Kind of a guide node */
enum PFguide_kind_t {
      elem  /* element node */
    , attr  /* attribute node */
    , text  /* text node */
    , comm  /* comment node */
    , pi    /* processing instruction node */
    , doc   /* document node */
};
typedef enum PFguide_kind_t PFguide_kind_t;

struct PFguide_tree_t {
    unsigned int    guide;       /* unique guide number */
    unsigned int    count;       /* count of the same node */
    unsigned int    min;         /* miniumum number of occurrences */
    unsigned int    max;         /* maximum number of occurrences */
    int             level;       /* level of the node */
    PFguide_kind_t  kind;        /* kind of the node */
    PFqname_t       name;        /* name of the node */
    PFguide_tree_t *parent;      /* parent of the node */
    PFguide_list_t *child_list;  /* all childs of the node */
    int             origin;      /* origin of the guide: file # */
};

/* create guide trees from files */
PFguide_list_t *PFguide_list_load (void);

#endif  /* LOAD_STATS_H */ 

/* vim:set shiftwidth=4 expandtab filetype=c: */

