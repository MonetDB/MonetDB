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
* Copyright (C) 2006-2009 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2009 "CWI".
*
* All Rights Reserved.
* 
* Author(s): Henning Rode 
*            Jan Flokstra
*/

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
