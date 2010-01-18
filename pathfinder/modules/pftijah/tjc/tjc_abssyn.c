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

#include <pf_config.h>
#include <gdk.h>
#include <string.h>
#include <stdlib.h>
#include "tjc_abssyn.h"

/** 
 * Allocates a new parse tree leaf and initializes 
 * its type.
 *
 * @param  kind kind of new leaf
 * @return new parse tree lead
 */
struct TJpnode_t * 
tjcp_leaf (TJptree_t *t, TJptype_t kind)
{
    TJpnode_t *n = NULL;
    int c;

    if (t->length < t->capacity) 
	n = &(t->node[t->length++]);
    else
    { GDKerror("query length exceeds allocated space for parse tree"); return 0; }

    for (c = 0; c < TJPNODE_MAXCHILD; c++)
        n->child[c] = 0;
    n->kind = kind;

    return n;
}

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires single child node @a n1.
 *
 * @param  type type of new node
 * @param  n1   pointer to child
 * @return new parse tree node
 */
struct TJpnode_t *
tjcp_wire1 (TJptree_t *t, TJptype_t type, TJpnode_t *n1) 
{
    TJpnode_t *n;

    n = tjcp_leaf (t, type);
    n->child[0] = n1;

    return n;
}

/**
 * Allocates and initializes a new parse tree node @a n with given @a type,
 * then wires two child nodes @a n1 and @a n2.
 *
 * @param  type type of new node
 * @param  n1   pointer to first child
 * @param  n2   pointer to second child
 * @return new parse tree node
 */
struct TJpnode_t *
tjcp_wire2 (TJptree_t *t, TJptype_t type, TJpnode_t *n1, TJpnode_t *n2) 
{
    TJpnode_t *n;

    n = tjcp_wire1 (t, type, n1);
    n->child[1] = n2;

    return n;
} 

struct TJpfixme_t *
tjcp_fixme (TJpnode_t *n1, TJpnode_t **n2)
{
    TJpfixme_t *fm;

    fm = (TJpfixme_t *) TJCmalloc (sizeof (TJpfixme_t));
    
    fm->root = n1;
    fm->fixme = n2;

    return fm;
}

struct TJqnode_t *
tjcq_initnode (int size)
{
    TJqnode_t *n;
    int i;
    
    if (size == 0) size = TJQTERMLIST_MAXSIZE;
    n = (TJqnode_t *) TJCmalloc (sizeof (TJqnode_t));
    n->tlist = TJCmalloc (sizeof (char*) * size);
    n->elist = TJCmalloc (sizeof (char*) * size);
    n->wlist = TJCmalloc (sizeof (double) * size);
    n->klist = TJCmalloc (sizeof (TJqkind_t) * size);
    for (i = 0; i < size; i++) {
        n->tlist[i] = NULL;
	n->elist[i] = NULL;
	n->wlist[i] = 0.0;
	n->klist[i] = q_normal;
    }
    n->kind = q_term;
    n->capacity = size;
    n->length = 0;

    return n;
}

void
tjcq_free (TJqnode_t *qn)
{
    if (qn) {
    	TJCfree(qn->tlist);
    	TJCfree(qn->elist);
    	TJCfree(qn->wlist);
    	TJCfree(qn->klist);
    	TJCfree(qn);
    }
}

struct TJqnode_t *
tjcq_firstterm (char *term, char *entity, double weight, TJqkind_t kind)
{
    TJqnode_t *n;
    n = tjcq_initnode(0);
    return tjcq_addterm (n, term, entity, weight, kind);
}

struct TJqnode_t *
tjcq_addterm (TJqnode_t *n, char *term, char *entity, double weight, TJqkind_t kind)
{
    TJqnode_t *new;
    int l = n->length;
    if (l < n->capacity) {
        n->tlist[l] = term;
        n->elist[l] = entity;
        n->wlist[l] = weight;
	n->klist[l] = kind;
	n->length = l + 1;
    }
    else
    { 
	//initialze five times larger qnode 
	new = tjcq_initnode(n->capacity * 5);
	new = tjcq_addqnode(new, n);
	tjcq_free(n);
	return tjcq_addterm(new, term, entity, weight, kind);
    }
    return n;
}

struct TJqnode_t *
tjcq_addqnode (TJqnode_t *n, TJqnode_t *n1)
{
    TJqnode_t *new;
    int l = n->length;
    int k = n1->length;
    int i;
    if ((l + k) <= n->capacity) {
	for (i = 0; i < k; i++, l++) {
	    n->tlist[l] = n1->tlist[i];
	    n->elist[l] = n1->elist[i];
	    n->wlist[l] = n1->wlist[i];
	    n->klist[l] = n1->klist[i];
	}
	n->length = l;
    }
    else
    { 
	//initialze five times larger qnode 
	new = tjcq_initnode(n->capacity * 5);
	new = tjcq_addqnode(new, n);
	tjcq_free(n);
	return tjcq_addqnode(new, n1);
    }
    return n;
}

struct TJqnode_t *
tjcq_extractentities (TJqnode_t *n)
{
    TJqnode_t *n_ext = NULL;
    int i,j,l;
    l = n->length;
    j = 0;

    for (i = 0; i < l; i++) {
        if (strcmp (n->elist[i], "!t") != 0) {
	    if (n_ext)
	    	n_ext = tjcq_addterm(n_ext, n->tlist[i], n->elist[i], n->wlist[i], n->klist[i]);
	    else
	    	n_ext = tjcq_firstterm(n->tlist[i], n->elist[i], n->wlist[i], n->klist[i]);
	} else {
	    n->tlist[j] = n->tlist[i];
	    n->elist[j] = n->elist[i];
	    n->wlist[j] = n->wlist[i];
	    n->klist[j] = n->klist[i];
	    j++;
	}
    }

    n->length = j;
    return n_ext;
}

/* param keep indicates whether extracted terms remain in the original list */
struct TJqnode_t *
tjcq_extractkind (TJqnode_t *n, TJqkind_t kind, int keep)
{
    TJqnode_t *n_ext = NULL;
    int i,j,l;
    l = n->length;
    j = 0;

    for (i = 0; i < l; i++) {
        if (n->klist[i] == kind) {
	    if (n_ext)
	    	n_ext = tjcq_addterm(n_ext, n->tlist[i], n->elist[i], n->wlist[i], n->klist[i]);
	    else
	    	n_ext = tjcq_firstterm(n->tlist[i], n->elist[i], n->wlist[i], n->klist[i]);
	} else if (keep == 0) {
	    n->tlist[j] = n->tlist[i];
	    n->elist[j] = n->elist[i];
	    n->wlist[j] = n->wlist[i];
	    n->klist[j] = n->klist[i];
	    j++;
	}
    }
    if (keep == 0)
	n->length = j;
    return n_ext;
}

struct TJptree_t *
tjcp_inittree ()
{
    TJptree_t *ptree;

    ptree = (TJptree_t *) TJCmalloc (sizeof (TJptree_t));
    if (!ptree) exit(0);

    ptree->capacity = TJPTREE_MAXSIZE;
    ptree->length = 0;
    ptree->is_rel_path_exp = 0;

    return ptree;
}

void tjcp_freetree(TJptree_t *ptree)
{
    int c;
    TJpnode_t *n;
    for (c = 0; c < ptree->length; c++) {
	n = &ptree->node[c];
	if (n->kind == p_query)
	    tjcq_free (n->sem.qnode);
	if (n->kind == p_tag && strcmp (n->sem.str, "*"))
	    TJCfree (n->sem.str);
    }
    TJCfree (ptree);
}

/**
 * Allocates enough memory to hold a copy of @a str
 * and return a pointer to this copy
 * If you specify @a n != 0, the copy will hold @a n characters (+ the
 * trailing '\\0') only.
 * @param str string to copy
 * @param len copy @a len characters only
 * @return pointer to newly allocated (partial) copy of @a str
 */
char *
TJstrndup (const char *str)
{
    char *copy;
    int len = strlen (str);

    /* + 1 to hold end of string marker '\0' */
    copy = (char *) TJCmalloc (len + 1); 
    (void) strncpy (copy, str, len);

    /* force end of string marker '\0' */
    copy[len] = '\0';

    return copy;
}

char *
TJsubstrndup (const char *str, int offset, int len)
{
    char *copy;

    /* check if offset + len remain in the limits of the string */
    if (strlen (str) < (size_t)(offset + len) ) return "";

    str = str + offset;
    /* + 1 to hold end of string marker '\0' */
    copy = (char *) TJCmalloc (len + 1); 
    (void) strncpy (copy, str, len);

    /* force end of string marker '\0' */
    copy[len] = '\0';

    return copy;
}

/* returns 0 if node is contained in the nodelist, otherwise 1 */
int not_in_list(TJpnode_t *node, TJpnode_t **nl, int length) {
    int i;
    for (i = 0; i < length; i++)
	if (nl[i] == node) return 0;
    return 1;
}

/* returns 0 if node is contained in the nodelist, otherwise 1 */
int not_in_parlist(TJpnode_t *node, int childno, TJpnode_child_t *nl, int length) {
    int i;
    for (i = 0; i < length; i++)
	if (nl[i].node == node && nl[i].childno == childno) return 0;
    return 1;
}

TJpnode_t* find_first(TJpnode_t *node, TJptype_t type) {
    int i;
    TJpnode_t *ret;
    if (node == NULL) return NULL;
    if (node->kind == type) return node;
    for (i = 0; i < TJPNODE_MAXCHILD; i++) {
        ret = find_first( node->child[i], type);
        if (ret != NULL) return ret;
    }
    return NULL;
}

TJpnode_t* find_first_par(TJpnode_t *node, TJptype_t type) {
    int i;
    TJpnode_t *ret = NULL;
    if (node == NULL) return ret;
    for (i = 0; i < TJPNODE_MAXCHILD; i++) {
        if (node->child[i] != NULL) {
	    if (node->child[i]->kind == type) return node;
	    ret = find_first_par( node->child[i], type);
            if (ret != NULL) return ret;
	}
    }
    return NULL;
}

/* important: return nodes of lower hierarchy before upper ones in the list */
int find_all(TJpnode_t *node, TJptype_t type, TJpnode_t **res, int length) {
    int i;
    if (node == NULL) return length;
    for (i = 0; i < TJPNODE_MAXCHILD; i++)
        length = find_all (node->child[i], type, res, length);
    if (node->kind == type && not_in_list (node, res, length))
	res[length++] = node; 
    return length;
}

/* non-recursive version scanning the entire array that holds the parse tree */
int find_all_tree(TJptree_t *ptree, TJptype_t type, TJpnode_t **res) {
    int i;
    int length = 0;
    TJpnode_t *node;
    for (i = 0; i < ptree->length; i++) {
	node = &(ptree->node[i]);
	if (node->kind == type) 
    	    res[length++] = node;
    }
    return length;
}		

/* important: return nodes of lower hierarchy before upper ones in the list */
int find_all_par(TJpnode_t *node, TJptype_t type, TJpnode_child_t *res, int length) {
    int i;
    if (node == NULL) return length;
    for (i = 0; i < TJPNODE_MAXCHILD; i++) {
        if (node->child[i] != NULL) {
            length = find_all_par (node->child[i], type, res, length);
            if (node->child[i]->kind == type && not_in_parlist (node, i, res, length)) {
		res[length].node = node;
		res[length].childno = i;
		length++;
	    }
	}
    }
    return length;
}

/* important: return nodes of lower hierarchy before upper ones in the list */
int find_all_par1(TJpnode_t *node, TJptype_t type, TJpnode_child_t *res, int length) {
    int i = 1;
    if (node == NULL) return length;
    if (node->child[i] != NULL) {
        length = find_all_par (node->child[i], type, res, length);
        if (node->child[i]->kind == type && not_in_parlist (node, i, res, length)) {
    	    res[length].node = node;
    	    res[length].childno = i;
    	    length++;
        }
    }
    return length;
}


/* non-recursive version scanning the entire array that holds the parse tree */
int find_all_par_tree(TJptree_t *ptree, TJptype_t type, TJpnode_child_t *res) {
    int i,j;
    int length = 0;
    TJpnode_t *node;
    for (i = 0; i < ptree->length; i++) {
	node = &(ptree->node[i]);
        for (j = 0; j < TJPNODE_MAXCHILD; j++) {
	    if (node->child[j] && node->kind != p_nil && node->child[j]->kind == type) {
    	        res[length].node = node;
    	        res[length].childno = j;
    	        length++;
            }
	}
    }
    return length;
}		

/* returns 1 if qnode contains entities and terms
void check_type(TJqnode_t *qn) {
    int e = 0;
    int t = 0;
    int c;
    for (c = 0; c < qn->length; c++) {
	if (!strcmp (qn->elist[c], "!t")) 
	    t++;
	else
	    e++;
    }
    if (e && !t) 
	qn->kind = q_entity;
    if (!e && t) 
	qn->kind = q_term;
}
*/

void mark_deleted_nodes(TJpnode_t **nl, int length) {
    int i;
    for (i = 0; i < length; i++) {
	nl[i]->kind = p_nil;
    }
}

void printTJqnode_flat(TJqnode_t *qn)
{
    char *type = "unknown";
    int c;
    switch (qn->kind) {
        case q_term      : type = "term"; break;
        case q_ent       : type = "entity"; break;
        case q_phrase    : type = "phrase"; break;
        case q_term_plus : type = "term_plus"; break;
        case q_term_min  : type = "term_min"; break;
        case q_ent_plus  : type = "entity_plus"; break;
        case q_ent_min   : type = "entity_min"; break;
    }
    printf("%s-", type);
    for (c = 0; c < qn->length; c++)
        printf("%s ", qn->tlist[c]);
    printf("\n");
}

void printTJqnode(tjc_config* tjc_c, TJqnode_t *qn)
{
    char *type = "unknown";
    int c;
    switch (qn->kind) {
        case q_term      : type = "term"; break;
        case q_ent       : type = "entity"; break;
        case q_phrase    : type = "phrase"; break;
        case q_term_plus : type = "term_plus"; break;
        case q_term_min  : type = "term_min"; break;
        case q_ent_plus  : type = "entity_plus"; break;
        case q_ent_min   : type = "entity_min"; break;
    }
    TJCPRINTF(DOTOUT,"%s-", type);
    for (c = 0; c < qn->length; c++)
        TJCPRINTF(DOTOUT,"%s ", qn->tlist[c]);
}

void printTJpnode(tjc_config* tjc_c, TJpnode_t *node, TJpnode_t *root, short parID)
{
    char *type = "unknown";
    TJptype_t num_type = node->kind;
    TJqnode_t *qn;
    short nID = root - node;
    int c;
    
    switch (num_type) {
	case p_nexi  : type = "nexi"; break;
	case p_desc  : type = "desc"; break;
	case p_anc   : type = "anc"; break;
	case p_tag   : type = "tag"; break;
	case p_query : type = "query"; break;
	case p_about : type = "about"; break;
	case p_and   : type = "and"; break;
	case p_or    : type = "or"; break;
	case p_union : type = "union"; break;
	case p_pred  : type = "pred"; break;
	case p_root  : type = "root"; break;
	case p_ctx   : type = "ctx"; break;
	case p_nil   : type = "NIL"; break;
    }
    
    if (num_type == p_tag)
    	TJCPRINTF(DOTOUT,"%d [label=\"%s-%s\"]\n", nID, type, node->sem.str);
    else if (num_type == p_query) {
	qn = node->sem.qnode;
        TJCPRINTF(DOTOUT,"%d [label=\"", nID);
        printTJqnode(tjc_c, qn);
        TJCPRINTF(DOTOUT,"\"]\n");
    }
    else
        TJCPRINTF(DOTOUT,"%d [label=\"%s\"]\n", nID, type);
    if (parID != 32000) 
	TJCPRINTF(DOTOUT,"%d -> %d\n", parID, nID);

    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
	if (node->child[c] != NULL) printTJpnode (tjc_c, node->child[c], root, nID);
    }
}

void printTJptree(tjc_config* tjc_c, TJpnode_t *root)
{
    TJCPRINTF(DOTOUT,"digraph G {\n");
    printTJpnode (tjc_c, root, root, 32000);
    TJCPRINTF(DOTOUT,"}\n");
}

void printTJptree_flat(tjc_config* tjc_c, TJptree_t *ptree)
{
    int c;
    char *type = "unknown";

    for (c = 0 ; c < ptree->length; c++) {
	switch (ptree->node[c].kind) {
	    case p_nexi  : type = "nexi"; break;
	    case p_desc  : type = "desc"; break;
	    case p_anc   : type = "anc"; break;
	    case p_tag   : type = "tag"; break;
	    case p_query : type = "query"; break;
	    case p_about : type = "about"; break;
	    case p_and   : type = "and"; break;
	    case p_or    : type = "or"; break;
	    case p_union : type = "union"; break;
	    case p_pred  : type = "pred"; break;
	    case p_root  : type = "root"; break;
	    case p_ctx   : type = "ctx"; break;
	    case p_nil   : type = "nil"; break;
        }
	TJCPRINTF(DOTOUT,"%d: %s\n", c, type);
    }
}

/* vim:set shiftwidth=4 expandtab: */
