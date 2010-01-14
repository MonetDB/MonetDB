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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "tjc_optimize.h"

TJpnode_t* find_node_by_str(TJpnode_t **nl, int length, char *str)
{
    int i;
    for (i = 0; i < length; i++)
	if (strcmp (nl[i]->sem.str, str) == 0)
	    return nl[i];
    return NULL;
}	

TJpnode_t* find_node_by_children(TJpnode_t **nl, int length, TJpnode_t *n)
{
    int i;
    for (i = 0; i < length; i++)
        if (nl[i]->child[0] == n->child[0] && nl[i]->child[1] == n->child[1])
	    return nl[i];
    return NULL;
}	

/* desc step from root selects all x:
 *
 *    par            
 *     |             par 
 *    desc     -->    |
 *    /  \            x
 *  root  x
 */
void rule1(TJptree_t *ptree)
{
    int childno, c, num_desc, num_del;
    TJpnode_child_t nl_desc[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_par, *n_desc; 

    num_del = 0;
    num_desc = find_all_par_tree (ptree, p_desc, nl_desc);
    for (c = 0; c < num_desc; c++) {
	n_par = nl_desc[c].node;
	childno = nl_desc[c].childno;
	n_desc = n_par->child[childno];

	if (n_desc->child[0] && n_desc->child[1] && n_desc->child[0]->kind == p_root) {
	    nl_del[num_del++] = n_desc->child[0];
	    nl_del[num_del++] = n_desc;
	    n_par->child[childno] = n_desc->child[1];
	}
    }
    mark_deleted_nodes(nl_del, num_del);
}

/* join all equivalent tag selections
 *
 *        x                 x
 *      /   \              / \
 *   desc   desc   -->  desc desc    
 *   /  \   /  \        /  \ /  \
 *  t1  t2 t2  t3      t1   t2   t3
 */
void rule2(TJptree_t *ptree)
{
    int childno, c, num_tag_cur, num_tag, num_del;
    char *str;
    TJpnode_child_t nl_tag_cur[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_tag[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_tag_par, *n_tag, *n_tag1; 

    num_del = 0;
    num_tag = find_all_tree (ptree, p_tag, nl_tag);
    num_tag_cur = find_all_par_tree (ptree, p_tag, nl_tag_cur);
    for (c = 0; c < num_tag_cur; c++) {
	n_tag_par = nl_tag_cur[c].node;
	childno = nl_tag_cur[c].childno;
	n_tag = n_tag_par->child[childno];

	str = n_tag->sem.str;
	n_tag1 = find_node_by_str (nl_tag, num_tag, str);
	if (n_tag != n_tag1) {
	    nl_del[num_del++] = n_tag;
	    n_tag_par->child[childno] = n_tag1;
	}
    }
    mark_deleted_nodes(nl_del, num_del);
}

/* join all equivalent paths 
 *
 *       x             x
 *     /  \            |
 *   desc desc   -->  desc    
 *    | \/ |          /  \
 *    | /\ |         t1  t2
 *    t1  t2         
 */
void rule3(TJptree_t *ptree)
{
    int childno, c, num_desc_cur, num_desc, num_del;
    TJpnode_child_t nl_desc_cur[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_desc[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_desc_par, *n_desc, *n_desc1; 

    num_del = 0;
    num_desc = find_all_tree (ptree, p_desc, nl_desc);
    num_desc_cur = find_all_par_tree (ptree, p_desc, nl_desc_cur);
    for (c = 0; c < num_desc_cur; c++) {
	n_desc_par = nl_desc_cur[c].node;
	childno = nl_desc_cur[c].childno;
	n_desc = n_desc_par->child[childno];
        
	n_desc1 = find_node_by_children(nl_desc, num_desc, n_desc);
	if (n_desc != n_desc1) {
	    nl_del[num_del++] = n_desc;
	    n_desc_par->child[childno] = n_desc1;
	}
    }
    mark_deleted_nodes(nl_del, num_del);
}

/* union push-up: node set unions lead to nested node sets and should be
 * avoided in the lower tree.
 * unions on the right side of desc and left side of anc are never pushed up, 
 * since they do not influence the nestedness of the result.
 * we do not want to recursively push up unions
 * since the overhead of additional introduced operators in the tree
 * becomes too high.
 * trade-off: apply push-up first with 'about' parent,
 *            then with 'anc' parent, then with 'desc' parent,
 *            but not further then that.
 *
 *          par                 par              
 *           |                   |  
 *         about               union
 *         /   \     -->       /   \      
 *      union              about  about
 *      /   \              /  \   /  \
 *     t1   t2            t1     t2
 *
 *          par                par              
 *           |                  |  
 *          anc               union
 *          / \      -->      /   \      
 *           union          anc   anc
 *           /   \          / \   / \
 *          t1   t2           t1    t2
 *
 *          par                par              
 *           |                  |  
 *         desc               union
 *         /  \      -->      /   \      
 *      union              desc   desc
 *      /   \              /  \   /  \
 *     t1   t2            t1     t2
 *
 * if we find more than one union below about|anc|desc nodes:
 *
 *          par                par              
 *           |                  |  
 *         desc               union
 *         /  \      -->     /     \      
 *      union             union    desc
 *      /   \             /   \     / \
 *   union  t3          desc desc  t3
 *    /  \              / \  / \
 *   t1  t2            t1   t2
 */
void rule4(TJptree_t *ptree)
{
    int childno, c, num_about_par;
    TJpnode_child_t nl_about_par[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_about_par, *n_about, *n_union, *n_union_child;

    num_about_par = find_all_par_tree (ptree, p_about, nl_about_par);
    for (c = 0; c < num_about_par; c++) {
	n_about_par = nl_about_par[c].node;
	childno = nl_about_par[c].childno;
	n_about = n_about_par->child[childno];

	if (n_about->child[0]->kind == p_union) {
	    n_about_par->child[childno] = n_about->child[0];
	    n_union = n_about;
	    while (n_union->child[0]->kind == p_union) {
		n_union = n_union->child[0];
		n_union_child = n_union->child[1];
		n_union->child[1] = tjcp_wire2 (ptree, p_about, n_union_child, n_about->child[1]);
	    } 
	    n_union_child = n_union->child[0];
	    n_union->child[0] = n_about;
	    n_about->child[0] = n_union_child;
	}
    }

    // anc part: we do not introduce new variables, so about reads as anc here
    num_about_par = find_all_par_tree (ptree, p_anc, nl_about_par);
    for (c = 0; c < num_about_par; c++) {
	n_about_par = nl_about_par[c].node;
	childno = nl_about_par[c].childno;
	n_about = n_about_par->child[childno];

	if (n_about->child[1]->kind == p_union) {
 	    n_union = n_about->child[1];
	    n_about_par->child[childno] = n_union;
	    n_union_child = n_union->child[1];
	    n_union->child[1] = tjcp_wire2 (ptree, p_anc, n_about->child[0], n_union_child);
	    while (n_union->child[0]->kind == p_union) {
		n_union = n_union->child[0];
		n_union_child = n_union->child[1];
		n_union->child[1] = tjcp_wire2 (ptree, p_anc, n_about->child[0], n_union_child);
	    } 
	    n_union_child = n_union->child[0];
	    n_union->child[0] = n_about;
	    n_about->child[1] = n_union_child;
	}
    }

    // desc part: we do not introduce new variables, so about reads as desc here
    num_about_par = find_all_par_tree (ptree, p_desc, nl_about_par);
    for (c = 0; c < num_about_par; c++) {
	n_about_par = nl_about_par[c].node;
	childno = nl_about_par[c].childno;
	n_about = n_about_par->child[childno];

	if (n_about->child[0]->kind == p_union) {
	    n_about_par->child[childno] = n_about->child[0];
	    n_union = n_about;
	    while (n_union->child[0]->kind == p_union) {
		n_union = n_union->child[0];
		n_union_child = n_union->child[1];
		n_union->child[1] = tjcp_wire2 (ptree, p_desc, n_union_child, n_about->child[1]);
	    } 
	    n_union_child = n_union->child[0];
	    n_union->child[0] = n_about;
	    n_about->child[0] = n_union_child;
	}
    }
}

void optimize(TJptree_t *ptree)
{
    //root is not used in the current optimization rules
    rule1 (ptree);
    rule2 (ptree);
    rule3 (ptree);
    rule4 (ptree);
}


/* vim:set shiftwidth=4 expandtab: */
