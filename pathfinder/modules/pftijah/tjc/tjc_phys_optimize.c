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

/* about the physical optimization:
 * 1. step : translate the parse tree into an SRA algebra tree. the SRA algebra knows less
 *   operations (different node types), but also introduces a few additional support 
 *   node types for the mapping of identifiers.
 *
 * 2. step : annotate the parse tree. the main attributes are
 *   - scored / unscored : we keep track whether a node set is scored or unscored.
 *   - nested / unnested : shows whether the nodes of a node set can be nested in each other.
 *   - preIDs / nidIDs : tijah knows two node IDs pre and nid. if a nodes set holds nodes of 
 *   a single tagname, the nid IDs allow fast access to nid sorted indices. otherwise, the
 *   pre IDs should be used to evaluate containment on the pre-sorted pre-size table.
 *
 *   remarks: 
 *   - if a node set is unscored and using nid IDs, it always has pre IDs in the tail.
 *   - all node sets should be sorted at any time on pre-order (even node sets with nid IDs).
 *
 * 3. step : adding support nodes. whenever the result of an operator yields nodes of possibly
 *   different tagname, we switch from using nid to pre IDs. In order to make the switch
 *   transparent in the algebra tree, we add support translation nodes.
 *   Moreover, some SRA operators require implicit score combination (and/or). To simplify
 *   the basic operator implementation we also make such implicit score combination explicit
 *   by adding another score combination node to the tree. When working on nid IDs, we also
 *   add a support node (add_pre) to associate the corresponding pre IDs which are required 
 *   for the evaluation of containment joins.
 *
 * 4. step: set physical operators. in this phase, we make use of the node annotation to
 *   associate with each node the name of a physical operator.
 *
 *
*/
#include <pf_config.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "tjc_phys_optimize.h"

TJanode_t* add_node (TJatree_t *tree, TJatype_t type) {
    TJanode_t *node;
    int c;

    node = &(tree->nodes[tree->length]);
    tree->length++;
    node->kind = type;
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
	node->child[c] = NULL;
    node->tag = NULL;
    node->qid = 0;
    node->scored = 0;
    node->nested = 0;
    node->preIDs = 0;
    
    return node;
}

TJanode_t* add_node1 (TJatree_t *tree, TJatype_t type, TJanode_t *child0) {
    TJanode_t *node;

    node = add_node (tree, type);
    node->child[0] = child0;

    return node;
}

TJanode_t* add_node2 (TJatree_t *tree, TJatype_t type, TJanode_t *child0, TJanode_t *child1) {
    TJanode_t *node;

    node = add_node (tree, type);
    node->child[0] = child0;
    node->child[1] = child1;

    return node;
}

void annotate_node (TJanode_t *node, char *tag, char scored, char nested, char preIDs) {
    node->tag = tag;
    node->scored = scored;
    node->nested = nested;
    node->preIDs = preIDs;
}

TJatree_t* init_atree (TJptree_t *ptree)
{
    TJatree_t *atree;
    int capacity = ptree->length + 10 + (ptree->length / 2);

    atree = (TJatree_t *) TJCmalloc (sizeof (TJatree_t));
    atree->nodes = (TJanode_t *) TJCmalloc (sizeof (TJanode_t) * capacity);
    atree->capacity = capacity;
    atree->length = 0;
    atree->qlength = 0;
     
    return atree;
} 

void free_atree (TJatree_t *atree) {
    TJCfree (atree->nodes);
    TJCfree (atree);
}

TJanode_t* pnode2anode (TJatree_t *atree, TJpnode_t *s, TJpnode_t *pnode, short *nt)
{
    TJanode_t *anode;
    TJqnode_t *qnode;
    char qid;
    short nid = pnode - s;
    short i;

    //check whether node was created before
    if (nt[nid] != -1)
	return &(atree->nodes[nt[nid]]);

    //ensure that anode is initialized (only to satisfy the compiler)
    anode = atree->root;

    switch (pnode->kind) {
	case p_tag :
	    anode = add_node (atree, a_select_element);
	    anode->tag = pnode->sem.str;
	    break;
	case p_ctx :
	    anode = add_node (atree, a_select_element);
	    anode->tag = "!ctx";
	    break;
	case p_desc :
	    anode = add_node2 (atree, a_contained_by, pnode2anode (atree, s, pnode->child[1], nt), pnode2anode (atree, s, pnode->child[0], nt));
	    break;
	case p_anc :
	    anode = add_node2 (atree, a_containing, pnode2anode (atree, s, pnode->child[1], nt), pnode2anode (atree, s, pnode->child[0], nt));
	    break;
	case p_about :
	    anode = add_node1 (atree, a_containing_query, pnode2anode (atree, s, pnode->child[0], nt));
	    qnode = pnode->child[1]->sem.qnode;
            qid = -1;
            for (i = 0; i < atree->qlength; i++)
                if (atree->qnodes[i] == qnode) qid = (char)i;
            if (qid == -1) {
                qid = atree->qlength++;
	        atree->qnodes[(short)qid] = qnode;
            }
	    anode->qid = qid;
	    break;
	case p_and :
	    anode = add_node2 (atree, a_and, pnode2anode (atree, s, pnode->child[0], nt), pnode2anode (atree, s, pnode->child[1], nt));
	    break;
	case p_or :
	case p_union :
	    anode = add_node2 (atree, a_or, pnode2anode (atree, s, pnode->child[0], nt), pnode2anode (atree, s, pnode->child[1], nt));
	    break;
	case p_nexi :
	    anode = pnode2anode (atree, s, pnode->child[0], nt);
	    break;
	case p_query :
	    GDKerror("type p_query should not be found");
	    break;
	case p_pred :
	    GDKerror("type p_pred should not exist after normalization");
	    break;
	case p_root :
	    GDKerror("type p_root should not exist after normalization");
	    break;
	case p_nil :
	    GDKerror("type p_nil should not exist after normalization");
	    break;
    }

    nt[nid] = anode - atree->nodes;
    atree->root = anode;
    return anode;
}

TJatree_t* ptree2atree(TJptree_t *ptree, TJpnode_t *proot)
{
    TJatree_t *atree;
    short node_translated[TJPTREE_MAXSIZE];
    int i;

    for (i = 0; i < TJPTREE_MAXSIZE; i++) node_translated[i] = -1;
    atree = init_atree (ptree);
    pnode2anode(atree, ptree->node, proot, node_translated);
    return atree;
}

void printTJatree(tjc_config* tjc_c, TJatree_t *atree)
{
    TJCPRINTF(DOTOUT,"digraph G {\n");
    printTJanode (tjc_c, atree, atree->root, NULL); 
    TJCPRINTF(DOTOUT,"}\n");
}

void printTJanode (tjc_config *tjc_c, TJatree_t *tree, TJanode_t *node, TJanode_t *par)
{
    char *type = "unknown";
    TJatype_t num_type;
    int nID, parID = 0;
    int c;

    nID = node - tree->nodes;
    if (par) parID = par - tree->nodes;
    num_type = node->kind;

    switch (num_type) {
	case a_containing  : type = "containing"; break;
	case a_contained_by : type = "contained_by"; break;
	case a_select_element : type = "select_element"; break;
	case a_containing_query : type = "containing_query"; break;
	case a_and : type = "and"; break;
	case a_or : type = "or"; break;
	case a_nid2pre : type = "nid2pre"; break;
	case a_pre2nid : type = "pre2nid"; break;
	case a_add_pre : type = "add_pre"; break;
    }
    
    TJCPRINTF (DOTOUT, "%d [label=\"%s\\n%s:n%ds%dpre%d\\n%s\"]\n", nID, type, node->tag, node->nested, node->scored, node->preIDs, node->op);
    if (par) 
	TJCPRINTF (DOTOUT, "%d -> %d\n", parID, nID);

    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
	if (node->child[c]) printTJanode (tjc_c, tree, node->child[c], node);
    } 
}

void annotate_anode (TJanode_t *anode, BAT* rtagbat)
{
    int c;
    TJatype_t num_type;
   
    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
	if (anode->child[c]) annotate_anode (anode->child[c], rtagbat);
    }
    
    num_type = anode->kind;
    switch (num_type) {
	case a_select_element :
	    annotate_node (anode, anode->tag, 0, 0, 0); 
	    if (BUNfnd (BATmirror (rtagbat), anode->tag) != BUN_NONE)
		anode->nested = 1;
	    if (strcmp (anode->tag, "!ctx") == 0 || strcmp (anode->tag, "*") == 0) {
		anode->nested = 1;
		anode->preIDs = 1;
	    }
	    break;
	case a_containing :  		
	case a_contained_by : 		
	    annotate_node (anode, anode->child[0]->tag, 0, anode->child[0]->nested, anode->child[0]->preIDs);
	    if (anode->child[1]->scored)
		anode->scored = 1;
	    break;
	case a_containing_query :
	    annotate_node (anode, anode->child[0]->tag, 1, anode->child[0]->nested, anode->child[0]->preIDs);
	    break;
	case a_and : 				
	case a_or :
	    annotate_node (anode, anode->child[0]->tag, anode->child[0]->scored, anode->child[0]->nested, anode->child[0]->preIDs);
            if (strcmp (anode->child[0]->tag, anode->child[1]->tag) != 0) 
		annotate_node (anode, "*", anode->scored, 1, 1);
	    break;
	default :
	    break;
    }
}

/*TODO add support nodes when a nid operator has scored children */
void add_support_nodes(TJatree_t *tree, TJanode_t *node, TJanode_t *par)
{
    int c;
    TJanode_t *nn;

    //recursive function call and adding pre2nid or nid2pre translation if necessary
    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
	if (node->child[c]) {
	    add_support_nodes (tree, node->child[c], node);
	    if (node->preIDs == 0 && node->child[c]->preIDs == 1) {
		nn = add_node1 (tree, a_pre2nid, node->child[c]);
	        annotate_node (nn, node->child[c]->tag, node->child[c]->scored, node->child[c]->nested, 0);
		node->child[c] = nn;
	    }
	    else if (node->preIDs == 1 && node->child[c]->preIDs == 0) {
		nn = add_node1 (tree, a_nid2pre, node->child[c]);
	        annotate_node (nn, node->child[c]->tag, node->child[c]->scored, node->child[c]->nested, 0); 
		node->child[c] = nn;
	    }
	}
    }

    //add explicit score combination node if needed
    //(the containment operator will only perform score propagation no score combination)
    if (node->child[0] && node->child[0]->scored &&
	    (node->kind == a_contained_by || node->kind == a_containing || node->kind == a_containing_query)) {
	nn = add_node2 (tree, a_and, node, node->child[0]);
	annotate_node (nn, node->tag, 1, node->nested, node->preIDs);
	if (par) {
	    c = 0; if (par->child[0] != node) c = 1;
	    par->child[c] = nn;
	} else {
	    tree->root = nn;
	}
	// in case the node works with nids, we also need to associate pres
	if (node->preIDs == 0) {
	    nn = add_node1 (tree, a_add_pre, node->child[0]);
	    annotate_node (nn, node->child[0]->tag, 0, node->child[0]->nested, 0);
	    node->child[0] = nn;
	}
    }
}	    

void set_physical_operators(TJatree_t *tree)
{
    TJanode_t *node;
    TJatype_t num_type;
    char *param1, *param2, *param3; 
    char operator[50];
    int i;

    for (i = 0; i < tree->length; i++) {
	node = &(tree->nodes[i]); 
	num_type = node->kind;
        switch (num_type) {
	    case a_select_element : 
		if (strcmp (node->tag, "!ctx") == 0)
		    param1 = "startnodes";
		else if (strcmp (node->tag, "*") == 0)
		    param1 = "star";
		else
		    param1 = "tag";
		sprintf(operator, "select_%s", param1);
		break;
	    case a_contained_by :
		if (node->scored)
		    param1 = "prop";
		else
		    param1 = "noprop";
		if (node->nested)
		    param2 = "nest";
		else
		    param2 = "unnest";
		if (node->preIDs)
		    param3 = "pre";
		else
		    param3 = "nid";
		sprintf(operator, "contained_by_%s_%s_%s", param1, param2, param3);
		break;
	    case a_containing :
		if (node->scored)
		    param1 = "prop";
		else
		    param1 = "noprop";
		if (node->nested)
		    param2 = "nest";
		else
		    param2 = "unnest";
		if (node->preIDs)
		    param3 = "pre";
		else
		    param3 = "nid";
		sprintf(operator, "containing_%s_%s_%s", param1, param2, param3);
		break;
	    case a_containing_query :
		if (node->nested)
		    param1 = "nest";
		else
		    param1 = "unnest";
		if (node->preIDs)
		    param2 = "pre";
		else
		    param2 = "nid";
		sprintf(operator, "containing_query_%s_%s", param1, param2);
		break;
	    case a_and :
		if (node->scored)
		    param1 = "comb";
		else
		    param1 = "nocomb";
		sprintf(operator, "and_%s", param1);
		break;
	    case a_or :
		if (node->scored)
		    param1 = "comb";
		else
		    param1 = "nocomb";
		sprintf(operator, "or_%s", param1);
		break;
	    case a_nid2pre :
		sprintf(operator, "nid2pre");
		break;
	    case a_pre2nid :
		sprintf(operator, "pre2nid");
		break; 
	    case a_add_pre :
		sprintf(operator, "add_pre");
		break; 
	}
	node->op = GDKstrdup(operator);
    }
}


TJatree_t* phys_optimize(TJptree_t *ptree, TJpnode_t *proot, BAT* rtagbat)
{
    TJatree_t *atree;
    
    atree = ptree2atree (ptree, proot);
    annotate_anode (atree->root, rtagbat);
    add_support_nodes (atree, atree->root, NULL);
    set_physical_operators (atree);
    return atree;
}


/* vim:set shiftwidth=4 expandtab: */
