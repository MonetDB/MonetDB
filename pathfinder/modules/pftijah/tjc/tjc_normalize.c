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
#include "tjc_normalize.h"


/* replace ctx nodes in the tree by the respective context node set, remove predicate nodes
 *
 *  normalize1:
 *        par                      
 *         |                  
 *       pred                par   
 *       /  \        -->      |     
 *      x   about           about
 *          /  \            /  \
 *        ctx   t          x    t
 *  
 *  normalize2:
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
void normalize1_2(TJptree_t *ptree)
{
    int childno_pred_par, childno_about_par, c, d, num_pred_par, num_about_par, num_del;
    TJpnode_child_t nl_pred[TJPNODELIST_MAXSIZE];
    TJpnode_child_t nl_about[TJPNODELIST_MAXSIZE];
    TJpnode_t *nl_del[TJPNODELIST_MAXSIZE];
    TJpnode_t *n_pred_par, *n_pred, *n_about_par, *n_about, *n_ctx, *n_anc; 

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

    return;
}

void normalize(TJptree_t *ptree)
{
    normalize1_2(ptree);
}


/* vim:set shiftwidth=4 expandtab: */
