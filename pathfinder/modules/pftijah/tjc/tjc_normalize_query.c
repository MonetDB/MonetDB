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
#include <stdio.h>
#include "tjc_normalize_query.h"


/* join AND/OR connected abouts, concatenate term/entity list
 *
 *          par                                
 *           |                         
 *        and/or                    par         
 *         /   \         -->        |         
 *      about  about              about  
 *      /   \ /    \              /   \ 
 *   query   x    and            x    and
 *   		  /  \                /  \
 *   		 s1   p1             s1  and
 *   		                         /  \
 *   		                        p1  query
 */

void normalizeq1(TJptree_t *ptree)
{
    int childno, c, num_and, num_del;
    TJpnode_child_t nl_and[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_and_par, *n_and, *n_anc0, *n_anc1, *n_about0, *n_about1, *n_qand; 

// in the OR case we cannot combine query parts when they contain phrases or mandatory terms/entities
/*
    num_del = 0;
    num_and = find_all_par_tree (ptree, p_or, nl_and);
    for (c = 0; c < num_and; c++) {
	n_and_par = nl_and[c].node;
	childno = nl_and[c].childno;
	n_and = n_and_par->child[childno];

	n_anc0 = NULL;
	n_anc1 = NULL;
	n_about0 = NULL;
	n_about1 = NULL;
	
	// case1: with p_anc node between p_or and p_about
	if (n_and->child[0]->kind == p_anc && n_and->child[1]->kind == p_anc)
	{
	    n_anc0 = n_and->child[0];
	    n_anc1 = n_and->child[1];
	    if (n_anc0->child[0]->kind == p_about && n_anc1->child[0]->kind == p_about
		    && n_anc0->child[1] == n_anc1->child[1]) {
		n_about0 = n_anc0->child[0];
		n_about1 = n_anc1->child[0];
	    }
	}
	// case2: p_about directly under p_or
	if (n_and->child[0]->kind == p_about && n_and->child[1]->kind == p_about)
	{
	    n_about0 = n_and->child[0];
	    n_about1 = n_and->child[1];
	}
	// if one of the upper cases matched and both score the same context
	if (n_about0 && n_about1 && n_about0->child[0] == n_about1->child[0]) {
	    //find last p_and 
	    n_qand = n_about0->child[1];
	    while (n_qand->child[1]->kind == p_and)
		n_qand = n_qand->child[1];
	    //add query2 
	    n_qand->child[1] = n_about1->child[1];
	    nl_del[num_del++] = n_about1;
	    // case1 
	    if (n_anc0) { 
	        nl_del[num_del++] = n_anc1;
	        n_and_par->child[childno] = n_anc0;
	    }
	    // case2 
	    else
	        n_and_par->child[childno] = n_about0;
	}
    }
    mark_deleted_nodes(nl_del, num_del);
*/
    // now do the same for "AND" nodes
    num_del = 0;
    num_and = find_all_par_tree (ptree, p_and, nl_and);
    for (c = 0; c < num_and; c++) {
	n_and_par = nl_and[c].node;
	childno = nl_and[c].childno;
	n_and = n_and_par->child[childno];

	n_anc0 = NULL;
	n_anc1 = NULL;
	n_about0 = NULL;
	n_about1 = NULL;
	
	// case1: with p_anc node between p_or and p_about
	if (n_and->child[0]->kind == p_anc && n_and->child[1]->kind == p_anc)
	{
	    n_anc0 = n_and->child[0];
	    n_anc1 = n_and->child[1];
	    if (n_anc0->child[0]->kind == p_about && n_anc1->child[0]->kind == p_about
		    && n_anc0->child[1] == n_anc1->child[1]) {
		n_about0 = n_anc0->child[0];
		n_about1 = n_anc1->child[0];
	    }
	}
	// case2: p_about directly under p_or
	if (n_and->child[0]->kind == p_about && n_and->child[1]->kind == p_about)
	{
	    n_about0 = n_and->child[0];
	    n_about1 = n_and->child[1];
	}
	// if one of the upper cases matched and both score the same context
	if (n_about0 && n_about1 && n_about0->child[0] == n_about1->child[0]) {
	    //find last p_and 
	    n_qand = n_about0->child[1];
	    while (n_qand->child[1]->kind == p_and)
		n_qand = n_qand->child[1];
	    //add query2 
	    n_qand->child[1] = n_about1->child[1];
	    nl_del[num_del++] = n_about1;
	    // case1 
	    if (n_anc0) { 
	        nl_del[num_del++] = n_anc1;
	        n_and_par->child[childno] = n_anc0;
	    }
	    // case2 
	    else
	        n_and_par->child[childno] = n_about0;
	}
    }
    mark_deleted_nodes(nl_del, num_del);
}

/* 1. join simple queries in the same query, split by phrases in between
 *
 *     about                        about
 *     /   \                        /   \
 *    x    and                     x    and
 *         /  \                         /  \
 *        s1  and          -->        s1+2 and      
 *            /  \                         /  \
 *           p1  and                      p1  p2
 *               /  \                         
 *              s2  p2 
 *
 * 2. add all terms from all phrases into the one simple query
 * (if no simple query exists, create it)
 *
 * 3. extract mandatory and negated terms
 *
 */
void normalizeq2(TJptree_t *ptree)
{
    int c, d, i, num_about, num_and_par, num_del, childno_and_par;
    TJpnode_t *nl_about[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_old[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_new[TJPNODELIST_MAXSIZE];
    TJpnode_child_t nl_and[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_about, *n_and, *n_and_par, *n_query;
    TJqnode_t *nq_query0, *nq_query1, *nq_query2;

    for (i = 0; i < TJPNODELIST_MAXSIZE; i++) {
	nl_old[i] = NULL;
	nl_new[i] = NULL;
    }

    num_del = 0;
    num_about = find_all_tree (ptree, p_about, nl_about);
    //todo: several about nodes can point to the same node below. in that case the changes should apply to all those nodes
    for (c = 0; c < num_about; c++) {
	n_about = nl_about[c];
        n_query = n_about->child[1];
	//check wether node was dealt with before
	i = 0;
	while (nl_old[i] && nl_old[i] != n_query)
	    i++;
	if (nl_old[i] == n_query)
	    n_about->child[1] = nl_new[i];
	else {
	    nl_old[i] = n_query;

	    nq_query0 = tjcq_initnode(0);

	    //1.+2. descend query and join queries
	    num_and_par = find_all_par1 (n_about, p_and, nl_and, 0);
	    for (d = 0; d < num_and_par; d++) {
	        n_and_par = nl_and[d].node;
	        childno_and_par = nl_and[d].childno;
	        n_and = n_and_par->child[childno_and_par];
	        
	        // we only have to look for the left child, since the list is right deep
	        if (n_and->child[0]->kind == p_query) {
	    	nq_query1 = n_and->child[0]->sem.qnode;
	    	nq_query0 = tjcq_addqnode(nq_query0, nq_query1);
	    	//phrases should remain, all other nodes are deleted
	    	    if (nq_query1->kind != q_phrase) { 
	    	        tjcq_free(nq_query1);
	    	        n_and_par->child[childno_and_par] = n_and->child[1];
	                    nl_del[num_del++] = n_and->child[0];
	                    nl_del[num_del++] = n_and;
	    	    }
	        }
	    }
	    
	    //3. extract mandatory and negated terms as well as entities
	    //mandatory terms and entities
	    nq_query1 = tjcq_extractkind(nq_query0, q_mandatory, 1);
	    if (nq_query1) {
	        nq_query2 = tjcq_extractentities(nq_query1);
	        if (nq_query2) {
	    	nq_query2->kind = q_ent_plus;
	            n_query = tjcp_leaf(ptree, p_query);
	            n_query->sem.qnode = nq_query2;
	            n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
	            n_about->child[1] = n_and;
	        }
	        if (nq_query1->length > 0) {
	    	nq_query1->kind = q_term_plus;
	            n_query = tjcp_leaf(ptree, p_query);
	            n_query->sem.qnode = nq_query1;
	            n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
	            n_about->child[1] = n_and;
	        }
	    }
	    //negated terms and entities
	    nq_query1 = tjcq_extractkind(nq_query0, q_negated, 0);
	    if (nq_query1) {
	        nq_query2 = tjcq_extractentities(nq_query1);
	        if (nq_query2) {
	    	nq_query2->kind = q_ent_min;
	            n_query = tjcp_leaf(ptree, p_query);
	            n_query->sem.qnode = nq_query2;
	            n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
	            n_about->child[1] = n_and;
	        }
	        if (nq_query1->length > 0) {
	    	nq_query1->kind = q_term_min;
	            n_query = tjcp_leaf(ptree, p_query);
	            n_query->sem.qnode = nq_query1;
	            n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
	            n_about->child[1] = n_and;
	        }
	    }
	    nq_query1 = tjcq_extractentities(nq_query0);
	    if (nq_query1) {
	        nq_query1->kind = q_ent;
                n_query = tjcp_leaf(ptree, p_query);
                n_query->sem.qnode = nq_query1;
                n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
                n_about->child[1] = n_and;
	    }
	    if (nq_query0->length > 0) {
	        n_query = tjcp_leaf(ptree, p_query);
	        n_query->sem.qnode = nq_query0;
	        n_and = tjcp_wire2(ptree, p_and, n_query, n_about->child[1]);
	        n_about->child[1] = n_and;
	    } else {
	    //remove query node if it remains empty
	        tjcq_free(nq_query0);
	    }
	    nl_new[i] = n_about->child[1];
	}
    }
    mark_deleted_nodes(nl_del, num_del);
}

void normalize_query(tjc_config* tjc_c, TJptree_t *ptree)
{
    //rule 7 would change the semnatics for conjunctive retrieval models
    if (strcmp(tjc_c->irmodel, "LM") != 0)
    	normalizeq1 (ptree);
    normalizeq2 (ptree);
}
