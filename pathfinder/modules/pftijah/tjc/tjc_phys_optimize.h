/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
* Copyright Notice:
* -----------------
*
* The contents of this file are subject to the PfTijah Public License
* Version 1.1 (the "License"); you may not use this file except in
* compliance with the License. You may obtain a copy of the License at
* http://dbappl.cs.utwente.nl/Legal/PfTijah-1.1.html
*
* Software distributed under the License is distributed on an "AS IS"
* basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
* License for the specific language governing rights and limitations
* under the License.
*
* The Original Code is the PfTijah system.
*
* The Initial Developer of the Original Code is the "University of Twente".
* Portions created by the "University of Twente" are
* Copyright (C) 2006-2010 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2010 "CWI".
*
* All Rights Reserved.
* 
* Author(s): Henning Rode 
*            Jan Flokstra
*/

#ifndef PHYS_OPTIMIZE_H
#define PHYS_OPTIMIZE_H

#include "tjc_conf.h"
#include "tjc_abssyn.h"
#include <gdk.h>

/** algebra tree node type indicators 
 * this is a simplified SRA algebra */
enum TJatype_t {
      a_select_element   =   0  /**< used to select elements of a given tag, all elements, or the startnode set */
    , a_contained_by     =   1  /**< used for upwards score propagation (and ancestor steps without propagation) */
    , a_containing       =   2  /**< used for downwards score propagation (and descendant steps without propagation) */
    , a_containing_query =   3  /**< used to score an element set by a query (term query, concept query, ...) */
    , a_and              =   4  /**< used for 'and-wise' combination of two scored node sets */
    , a_or               =   5  /**< used for 'or-wise' combination of two scored node sets */
    , a_nid2pre          =   6  /**< support operator to translate nid IDs to pre IDs */
    , a_pre2nid          =   7  /**< support operator to translate pre IDs to nid IDs */
    , a_add_pre          =   8  /**< support operator to associate pre IDs to nid IDs */
};

typedef enum TJatype_t TJatype_t;

/** NEXI SRA algebra tree node
 */
typedef struct TJanode_t TJanode_t;

struct TJanode_t {
    TJatype_t         kind;              /**< node kind */
    char	     *tag;		 
    TJanode_t        *child[TJPNODE_MAXCHILD];  /**< child node list */
    char	      qid;
    char              scored;            /**< indicates whether the output is (already) scored */
    char              nested;            /**< indicates whether output is (possibly) nested */
    char              preIDs;            /**< indicates whether the output yields NIDs or preIDs */
    char	     *op;		 /**< chosen physical operator */
    char	      visited;		 /**< number the node is visited so far 
                                              (used to check that a node is visited only once within a graph traversal) */
};

typedef struct TJatree_t TJatree_t;

struct TJatree_t {
    TJanode_t  *nodes;
    TJanode_t  *root;
    int 	capacity;
    int		length;
    TJqnode_t  *qnodes[TJPNODELIST_MAXSIZE];
    int		qlength;
};

extern struct TJatree_t*
phys_optimize(tjc_config *tjc_c, TJptree_t *ptree, TJpnode_t *proot, BAT* rtagbat);

extern void
free_atree(TJatree_t *atree);

extern void
printTJatree(tjc_config *tjc_c, TJatree_t *atree);

#endif  /* PYHS_OPTIMIZE_H */

/* vim:set shiftwidth=4 expandtab: */
