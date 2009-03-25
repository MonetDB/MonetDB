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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "tjc_normalize.h"

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

/* returns 1 if qnode contains entities and terms */
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

void mark_deleted_nodes(TJpnode_t **nl, int length) {
    int i;
    for (i = 0; i < length; i++) {
	nl[i]->kind = p_nil;
    }
}

/* replace ctx nodes in the tree by the respective context node set, remove predicate nodes
 *
 *  rule1:
 *        par                      
 *         |                  
 *       pred                par   
 *       /  \        -->      |     
 *      x   about           about
 *          /  \            /  \
 *        ctx   t          x    t
 *  
 *  rule2:
 *        par                   par   
 *         |                     |
 *       pred                   anc
 *       /  \        -->       /   \ 
 *      x1  about           about   x1
 *          /  \            /  \
 *        desc   t        desc  t
 *        / \             /  \
 *      ctx  x2         root  x2
 */ 
TJpnode_t* rule1_2(TJptree_t *ptree, TJpnode_t *root)
{
    (void)root;

    int childno_pred_par, childno_about_par, c, d, num_pred_par, num_about_par, num_del;
    TJpnode_child_t nl_pred[TJPNODELIST_MAXSIZE];
    TJpnode_child_t nl_about[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_pred_par, *n_pred, *n_about_par, *n_about, /* *n_desc,*/ *n_ctx, *n_anc; 

    num_del = 0;
    num_pred_par = find_all_par_tree (ptree, p_pred, nl_pred);
    for (c = 0; c < num_pred_par; c++) {
	n_pred_par = nl_pred[c].node;
	childno_pred_par = nl_pred[c].childno;
	n_pred = n_pred_par->child[childno_pred_par];

        num_about_par = find_all_par1 (n_pred, p_about, nl_about, 0);
        for (d = 0; d < num_about_par; d++) {
	    n_about_par = nl_about[d].node;
	    childno_about_par = nl_about[d].childno;
	    n_about = n_about_par->child[childno_about_par];
            
	    // rule 1 
	    if (n_about->child[0]->kind == p_ctx) {
		nl_del[num_del++] = n_about->child[0];
		n_about->child[0] = n_pred->child[0];
	    }
	    // rule 2 
	    else if (n_about->child[0]->kind == p_desc) {
		n_ctx = find_first (n_about->child[0], p_ctx);
		n_ctx->kind = p_root;
		n_anc = tjcp_wire2 (ptree, p_anc, n_about, n_pred->child[0]);
		n_about_par->child[childno_about_par] = n_anc;
	    }
	}
	nl_del[num_del++] = n_pred;
	n_pred_par->child[childno_pred_par] = n_pred->child[1];
    }
    mark_deleted_nodes(nl_del, num_del);

    return NULL; /* HENNING CHECK */
}

/* split item lists in term and entity lists 
 *     
 *        par                    par
 *         |                      |
 *       about                   or 
 *       /  \        -->       /     \
 *      x  query           about     about
 *                         /  \       /  \
 *                        x   tquery x  equery
 */
TJpnode_t* rule3(TJptree_t *ptree, TJpnode_t *root)
{
    (void) root;

    int childno, c, num_about;
    TJqnode_t *qn, *qn1, *qn2;
    TJpnode_child_t nl_about[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_about_par, *n_about, *n_query2, *n_about2, *n_or; 

    num_about = find_all_par_tree (ptree, p_about, nl_about);
    for (c = 0; c < num_about; c++) {
	n_about_par = nl_about[c].node;
	childno = nl_about[c].childno;
	n_about = n_about_par->child[childno];
        
	qn = n_about->child[1]->sem.qnode;
	check_type (qn);
	if (qn->kind == q_mixed) {
	    qn1 = tjcq_initnode();
	    qn1->kind = q_term;
	    qn2 = tjcq_initnode();
	    qn2->kind = q_entity;
            for (c = 0; c < qn->length; c++) {
                if (!strcmp (qn->elist[c], "!t")) 
                    tjcq_addterm (qn1, qn->tlist[c], qn->elist[c], qn->wlist[c]);
                else
                    tjcq_addterm (qn2, qn->tlist[c], qn->elist[c], qn->wlist[c]);
            }
	    n_about->child[1]->sem.qnode = qn1;
	    TJCfree (qn);
	    
	    n_query2 = tjcp_leaf (ptree, p_query);
	    n_query2->sem.qnode = qn2;
	    n_about2 = tjcp_wire2 (ptree, p_about, n_about->child[0], n_query2); 
	    n_or = tjcp_wire2 (ptree, p_or, n_about, n_about2);
	    n_about_par->child[childno] = n_or;
	}
    }
    return NULL; /* HENNING CHECK */
}

void normalize(TJptree_t *ptree, TJpnode_t *root)
{
    rule1_2(ptree, root);
    rule3(ptree, root);
}


/* vim:set shiftwidth=4 expandtab: */
