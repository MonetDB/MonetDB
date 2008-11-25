/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

#ifndef NORMALIZE_H
#define NORMALIZE_H

#include "tjc_abssyn.h"

struct TJpnode_child_t {
    TJpnode_t			*node;
    int				childno;
};

typedef struct TJpnode_child_t TJpnode_child_t;

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
find_all_par_tree(TJptree_t *ptree, TJptype_t type, TJpnode_child_t *res);

void 
mark_deleted_nodes(TJpnode_t **nl, int length);

void 
normalize(TJptree_t *ptree, TJpnode_t *node);


#endif  /* NORMALIZE_H */

/* vim:set shiftwidth=4 expandtab: */
