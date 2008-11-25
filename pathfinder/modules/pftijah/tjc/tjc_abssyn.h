/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
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
#define TJQTERMLIST_MAXSIZE 20

typedef enum TJqtype_t TJqtype_t;

enum TJqtype_t {
    q_mixed           = 0  /**< marking mixed queries */
  , q_term            = 1  /**< marking term queries */
  , q_entity          = 2  /**< marking entity queries */
};

/** List of Query terms
 */ 
typedef struct TJqnode_t TJqnode_t;

struct TJqnode_t {
    TJqtype_t	      kind;
    char 	     *tlist[TJQTERMLIST_MAXSIZE];
    char             *elist[TJQTERMLIST_MAXSIZE];
    double            wlist[TJQTERMLIST_MAXSIZE];
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
    , p_nil              =  12  /**< marks a node that is deleted from the tree */
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
    short             nid;               /**< an identifier used in the optimizer */
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
};


/* interfaces to parse construction routines 
 */
TJpnode_t *
tjcp_leaf  (TJptree_t *t, TJptype_t type);

TJpnode_t *
tjcp_wire1 (TJptree_t *t, TJptype_t type, TJpnode_t *n1);

TJpnode_t *
tjcp_wire2 (TJptree_t *t, TJptype_t type, TJpnode_t *n1, TJpnode_t *n2);

TJpfixme_t *
tjcp_fixme (TJpnode_t *n1, TJpnode_t **n2);

TJqnode_t *
tjcq_initnode ();

TJqnode_t *
tjcq_firstterm (char *term, char *entity, double weight);

TJqnode_t *
tjcq_addterm (TJqnode_t *n, char *term, char *entity, double weight);

TJptree_t *
tjcp_inittree ();

char *
TJstrndup (const char *str);

char *
TJsubstrndup (const char *str, int offset, int len);

#endif  /* ABSSYN_H */

/* vim:set shiftwidth=4 expandtab: */
