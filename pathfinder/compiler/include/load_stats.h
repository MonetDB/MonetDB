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
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 */
#include "ordering.h"

#ifndef LOAD_STATS_H
#define LOAD_STATS_H

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

/* The guide tree */
typedef struct PFguide_tree_t PFguide_tree_t;
struct PFguide_tree_t {
    unsigned int            guide;          /* unique guide number */
    unsigned int            count;          /* count of the same node */
    PFguide_kind_t          kind;           /* kind of the ndoe */
    char                   *tag_name;       /* name of the node */
    PFguide_tree_t         *parent;         /* parent of the node */
    PFarray_t              *child_list;     /* all childs of the node */
};

/* Maps a list of guides to a column */
typedef struct PFguide_mapping_t PFguide_mapping_t;
struct PFguide_mapping_t {
    PFalg_att_t   column;        /* name of the column */
    PFarray_t    *guide_list;    /* list of guide nodes */
};

/* create a guide tree from a file */
PFguide_tree_t *PFguide_tree (void);

#endif  /* LOAD_STATS_H */ 

/* vim:set shiftwidth=4 expandtab filetype=c: */

