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

#ifndef ENCODING_H__
#define ENCODING_H__

#include <stdio.h> 

/* SAX parser interface (libxml2) */
#include "libxml/parser.h"

#include "shred_helper.h"

/**
 * XML node kinds
 */
enum kind_t {
      elem = 1    /** < element node           */
    , attr = 2    /** < attribute              */
    , text = 3    /** < text node              */
    , comm = 4    /** < comment                */
    , pi   = 5    /** < processing instruction */
    , doc  = 6    /** < document node          */
};
typedef enum kind_t kind_t;

/**
 * Properties of a guide tree node (see guides.h)
 */
typedef struct guide_tree_t guide_tree_t;

/** 
 * Properties of an encoded XML node 
 */
typedef struct node_t node_t;

struct node_t {
    nat           pre;                  /* preorder rank */
    nat           post;                 /* postorder rank */
    nat           pre_stretched;        /* preorder in stretched plane */
    nat           post_stretched;       /* postorder in stretched plane */
    node_t       *parent;               /* pointer to parent */
    nat           size;                 /* # of nodes in subtree */
    int           level;                /* length of path from node to root */
    kind_t        kind;                 /* XML node kind */
    xmlChar      *localname;            /* localname of element/attribute */
    int           localname_id;         /* unique ID of localname */
    xmlChar      *uri;                  /* namespace URI of element/attribute */
    int           uri_id;               /* unique ID of namespace URI */
    xmlChar      *value;                /* node content (text, value) */
    guide_tree_t *guide;                /* pointer to this node's guide entry */
};


/**
 * Print decoded kind
 */
void print_kind (FILE *f, kind_t kind);


/**
 * Main shredding procedure 
 */
void SHshredder (const char *s, 
                 FILE *shout, 
                 FILE *attout, 
                 FILE *namesout, 
                 FILE *urisout,
                 FILE *guideout, 
                 shred_state_t *status);

#endif
