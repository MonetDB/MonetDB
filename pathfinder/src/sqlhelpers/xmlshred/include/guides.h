/**
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

#ifndef GUIDES_H__
#define GUIDES_H__

#include "shred_helper.h"
#include "encoding.h"

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"

#define GUIDE_INIT 1

typedef struct child_list_t child_list_t;
struct child_list_t {
    child_list_t    *next_element;
    guide_tree_t    *node;
};

struct guide_tree_t {
    xmlChar      *uri;
    xmlChar      *localname;
    nat           count;
    nat           rel_count;
    nat           min;
    nat           max;
    guide_tree_t *parent;
    child_list_t *child_list;
    child_list_t *last_child;
    nat           guide;
    kind_t        kind;
};

/* insert a node in the guide tree */
guide_tree_t* insert_guide_node(const xmlChar *URI, const xmlChar *tag_name, 
                                guide_tree_t *parent, kind_t kind);

/* adjust the minimum and maximum values of guide nodes */
void adjust_guide_min_max (guide_tree_t *guide);

/* print the guide tree */
void print_guide_tree(FILE *guide_out, guide_tree_t *root, int tree_depth);

/* free the guide tree */
void free_guide_tree(guide_tree_t *root);

#endif /* GUIDES_H__ */
