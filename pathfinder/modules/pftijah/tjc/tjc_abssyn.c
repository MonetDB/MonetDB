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

    //n = (TJpnode_t *) TJCmalloc (sizeof (TJpnode_t));
    if (t->length < t->capacity) 
	n = &(t->node[t->length++]);
    else
    { /*TODO*/ }

    for (c = 0; c < TJPNODE_MAXCHILD; c++)
        n->child[c] = 0;
    n->kind = kind;
    n->nid = 0;

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
tjcq_initnode ()
{
    TJqnode_t *n;
    int i;

    n = (TJqnode_t *) TJCmalloc (sizeof (TJqnode_t));
    for (i = 0; i < TJQTERMLIST_MAXSIZE; i++) {
        n->tlist[i] = NULL;
	n->elist[i] = NULL;
	n->wlist[i] = 0.0;
    }
    n->kind = q_mixed;
    n->capacity = TJQTERMLIST_MAXSIZE;
    n->length = 0;

    return n;
}

struct TJqnode_t *
tjcq_firstterm (char *term, char *entity, double weight)
{
    TJqnode_t *n;
    n = tjcq_initnode();
    return tjcq_addterm (n, term, entity, weight);
}

struct TJqnode_t *
tjcq_addterm (TJqnode_t *n, char *term, char *entity, double weight)
{
    int l = n->length;
    if (l < n->capacity) {
        n->tlist[l] = term;
        n->elist[l] = entity;
        n->wlist[l] = weight;
	n->length = l + 1;
    }
    else { /*TODO*/ }

    return n;
}

struct TJptree_t *
tjcp_inittree ()
{
    TJptree_t *ptree;

    ptree = (TJptree_t *) TJCmalloc (sizeof (TJptree_t));
    if (!ptree) exit(0);

    ptree->capacity = TJPTREE_MAXSIZE;
    ptree->length = 0;

    return ptree;
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


/* vim:set shiftwidth=4 expandtab: */
