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

#ifndef ABSSYN_H
#define ABSSYN_H

#include "tjc_conf.h"

/** no type of parse tree node will need more than
 *  this many child nodes 
 */
#define TJPNODE_MAXCHILD 2
#define TJPTREE_MAXSIZE 250
#define TJPNODELIST_MAXSIZE 20
#define TJQTERMLIST_MAXSIZE 10

typedef enum TJqtype_t TJqtype_t;

enum TJqtype_t {
    q_term            = 0  /**< marking simple term queries */
  , q_ent             = 1  /**< marking entity queries */
  , q_phrase          = 2  /**< marking phrase queries */
  , q_term_plus       = 3  /**< marking mandatory term queries */
  , q_term_min        = 4  /**< marking negated term queries */
  , q_ent_plus        = 5  /**< marking mandatory entity queries */
  , q_ent_min         = 6  /**< marking negated entity queries */
};

typedef enum TJqkind_t TJqkind_t;

enum TJqkind_t {
      q_normal		= 0 /* normal keyword */
    , q_mandatory	= 1 /* mandatory keyword in query */
    , q_negated		= 2 /* negated keyword in query */
};

/** List of Query terms
 */ 
typedef struct TJqnode_t TJqnode_t;

struct TJqnode_t {
    TJqtype_t	      kind;
    char 	     **tlist;
    char             **elist;
    double            *wlist;
    TJqkind_t         *klist;
    int		      capacity;
    int	 	      length;
};

/** parse tree node type indicators */
enum TJptype_t {
      p_nexi             =   0  /**< root node of the query DAG */
    , p_desc             =   1  /**< descendant step with propagation */
    , p_anc              =   2  /**< ancestor step with propagation */
    , p_tag              =   3  /**< node selection by tagname */
    , p_query            =   4  /**< query node pointing to qnode object */
    , p_about            =   5  /**< scoring function about */
    , p_and              =   6  /**< and */
    , p_or               =   7  /**< or */
    , p_union            =   8  /**< union of nodes */
    , p_pred             =   9  /**< predicate node */
    , p_root             =  10  /**< collection root */
    , p_ctx              =  11  /**< placeholder for the context of parent predicate */
    , p_nil              =  12  /**< marks a node that is deleted from the tree, or a node 
				  at the end of the query list */
};

typedef enum TJptype_t TJptype_t;


/** semantic node information
 */
typedef union TJpsem_t TJpsem_t;

union TJpsem_t {
    TJqnode_t       *qnode;        /**< pointer to qnode object */
    char            *str;        /**< string value */
}; 

/** NEXI parse tree node
 */
typedef struct TJpnode_t TJpnode_t;

struct TJpnode_t {
    TJptype_t         kind;              /**< node kind */
    TJpsem_t          sem;               /**< semantic node information */
    TJpnode_t        *child[TJPNODE_MAXCHILD];  /**< child node list */
};


/*
 * In several cases, the semantic actions of a grammar rule cannot
 * construct a complete abstract syntax tree, e.g., consider the
 * generation of a right-deep abstract syntax tree from a left-recursive
 * grammar rule.
 *
 * Whenever such a situation arises, we let the semantic action
 * construct as much of the tree as possible with parts of the tree
 * unspecified.  The semantic action then returns the ROOT of this
 * tree as well as pointer to the node under which the yet unspecified
 * tree part will reside; this node is subsequently referred to as the
 * `fixme'.
 */
struct TJpfixme_t {
    TJpnode_t *root;
    TJpnode_t **fixme;
};

typedef struct TJpfixme_t TJpfixme_t;


typedef struct TJptree_t TJptree_t;

struct TJptree_t {
    TJpnode_t 	node[TJPTREE_MAXSIZE];
    int 	capacity;
    int		length;
    char	is_rel_path_exp;
};

struct TJpnode_child_t {
    TJpnode_t			*node;
    int				childno;
};

typedef struct TJpnode_child_t TJpnode_child_t;

/* interfaces to parse construction routines 
 */
extern struct TJpnode_t *
tjcp_leaf  (TJptree_t *t, TJptype_t type);

extern struct TJpnode_t *
tjcp_wire1 (TJptree_t *t, TJptype_t type, TJpnode_t *n1);

extern struct TJpnode_t *
tjcp_wire2 (TJptree_t *t, TJptype_t type, TJpnode_t *n1, TJpnode_t *n2);

extern struct TJpfixme_t *
tjcp_fixme (TJpnode_t *n1, TJpnode_t **n2);

extern struct TJqnode_t *
tjcq_initnode (int size);

void
tjcq_free (TJqnode_t *qn);
    
extern struct TJqnode_t *
tjcq_firstterm (char *term, char *entity, double weight, TJqkind_t kind);

extern struct TJqnode_t *
tjcq_addterm (TJqnode_t *n, char *term, char *entity, double weight, TJqkind_t kind);

struct TJqnode_t *
tjcq_addqnode (TJqnode_t *n, TJqnode_t *n1);

struct TJqnode_t *
tjcq_extractentities (TJqnode_t *n);

struct TJqnode_t *
tjcq_extractkind (TJqnode_t *n, TJqkind_t kind, int keep);

extern struct TJptree_t *
tjcp_inittree (void);

extern void 
tjcp_freetree (TJptree_t *ptree);

extern char *
TJstrndup (const char *str);

extern char *
TJsubstrndup (const char *str, int offset, int len);

TJpnode_t *
find_first(TJpnode_t *node, TJptype_t searchtype);

TJpnode_t *
find_first_par(TJpnode_t *node, TJptype_t searchtype);

int 
find_all(TJpnode_t *node, TJptype_t type, TJpnode_t **res, int length);

int 
find_all_tree(TJptree_t *ptree, TJptype_t type, TJpnode_t **res);

int 
find_all_par(TJpnode_t *node, TJptype_t type, TJpnode_child_t *res, int length);

int 
find_all_par1(TJpnode_t *node, TJptype_t type, TJpnode_child_t *res, int length);

int 
find_all_par_tree(TJptree_t *ptree, TJptype_t type, TJpnode_child_t *res);

void 
mark_deleted_nodes(TJpnode_t **nl, int length);

void 
printTJptree(tjc_config* tjc_c, TJpnode_t *root);

void 
printTJptree_flat(tjc_config* tjc_c, TJptree_t *ptree);

void
printTJqnode_flat(TJqnode_t *qn);

#endif  /* ABSSYN_H */

/* vim:set shiftwidth=4 expandtab: */
