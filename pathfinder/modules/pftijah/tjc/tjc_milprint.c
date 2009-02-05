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
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "tjc_milprint.h"


short assign_numbers (TJpnode_t *node, short cnt) {
    int c;
    for (c = 0; c < TJPNODE_MAXCHILD; c++)
        if (node->child[c]) 
	    cnt = assign_numbers (node->child[c], cnt);
    if (node->nid == 0) 
	node->nid = cnt++;
    return cnt;
}

void assign_scopes (TJpnode_t *node, short node_scope[]) {
    int c;
    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
        if (node->child[c]) {
	    assign_scopes (node->child[c], node_scope);
	    node_scope[node->child[c]->nid] = node->nid;
	}
    }
}

void assign_scopes2 (TJatree_t *tree, TJanode_t *node, short *node_scope, char *node_printed) {
    int c;
    short nid, nid_c;
    nid = node - tree->nodes;

    if (node_printed[nid]) return;

    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
        if (node->child[c]) {
	    assign_scopes2 (tree, node->child[c], node_scope, node_printed);
	}
    }
    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
        if (node->child[c]) {
	    nid_c = node->child[c] - tree->nodes;
	    node_scope[nid_c] = nid;
	}
    }

    node_printed[nid] = 1;
}

/**TODO: prepare text queries upfront. all terms should be
 * stemmed and translated to TIDs before executing the SRA operators
 * to 'cluster' access to TID tables and stemming
 */
void milprint_init (tjc_config *tjc_c) {
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := TRUE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := TRUE;\n");
    }
    if (tjc_c->returnall) {
        TJCPRINTF(MILOUT,"returnAllElements := TRUE;\n");
    }
    if (tjc_c->startNodes) {
        TJCPRINTF(MILOUT,"var startNodes := bat(\"%s\");\n", tjc_c->startNodes);
    }

    TJCPRINTF(MILOUT,"\n");
}

void milprint_init2 (tjc_config *tjc_c) {
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := TRUE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := TRUE;\n");
    }

    TJCPRINTF(MILOUT,"\n");
}

void milprint_end (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"\n");
    if (tjc_c->rmoverlap) {
	TJCPRINTF(MILOUT,"nexi_result := rm_overlap(nexi_result, qenv);\n");
    }
    if (tjc_c->prior) {
	TJCPRINTF(MILOUT,"nexi_result := prior_%s(nexi_result, qenv);\n", tjc_c->prior);
    }
    TJCPRINTF(MILOUT,"nexi_result := tj_nid2pre(nexi_result, qenv);\n");
    TJCPRINTF(MILOUT,"nexi_result := tsort_rev(nexi_result);\n");
    TJCPRINTF(MILOUT,"nexi_score_xfer := nexi_result;\n");
    TJCPRINTF(MILOUT,"nexi_result := nil;\n"); /* IMPORTANT, MEMORY LEAK */
    if (0) TJCPRINTF(MILOUT,"nexi_result := nexi_result.print();\n");
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := FALSE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := FALSE;\n");
    }
    if (tjc_c->returnall) {
        TJCPRINTF(MILOUT,"returnAllElements := FALSE;\n");
    } 
}

void milprint_end2 (tjc_config *tjc_c, TJatree_t *tree) {
    int qid;
    for (qid = 0; qid < tree->qlength; qid++) {
         TJCPRINTF(MILOUT,"Q%d := nil;\n", qid);
    }   
    TJCPRINTF(MILOUT,"\n");
    TJCPRINTF(MILOUT,"var nexi_result := R" PDFMT ";\n", tree->root - tree->nodes);
    TJCPRINTF(MILOUT,"R" PDFMT " := nil;\n", tree->root - tree->nodes);
    if (tree->root->preIDs == 0) TJCPRINTF(MILOUT,"nexi_result := tj_nid2pre(nexi_result);\n");
    if (tjc_c->rmoverlap && tree->root->nested) {
	TJCPRINTF(MILOUT,"nexi_result := tj_rm_overlap(nexi_result);\n");
    }
    if (tjc_c->prior) {
	TJCPRINTF(MILOUT,"nexi_result := tj_prior_%s(nexi_result);\n", tjc_c->prior);
    }
    TJCPRINTF(MILOUT,"nexi_result := tsort_rev(nexi_result);\n");
    TJCPRINTF(MILOUT,"nexi_score_xfer := nexi_result;\n");
    TJCPRINTF(MILOUT,"nexi_result := nil;\n"); /* IMPORTANT, MEMORY LEAK */
    if (0) TJCPRINTF(MILOUT,"nexi_result := nexi_result.print();\n");
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := FALSE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := FALSE;\n");
    }
    if (tjc_c->returnall) {
        TJCPRINTF(MILOUT,"returnAllElements := FALSE;\n");
    } 
}

void milprint_qenv2 (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"var ftindex := \"%s\";\n", tjc_c->ftindex);    
    TJCPRINTF(MILOUT,"var scorebase := dbl(%f);\n", tjc_c->scorebase);    
    TJCPRINTF(MILOUT,"var c_lambda := dbl(%f);\n", tjc_c->lambda);    
    TJCPRINTF(MILOUT,"var okapi_k1 := dbl(%f);\n", tjc_c->okapik1);    
    TJCPRINTF(MILOUT,"var okapi_b := dbl(%f);\n", tjc_c->okapib);    
    TJCPRINTF(MILOUT,"var downprop := \"%s\";\n", tjc_c->downprop);    
    TJCPRINTF(MILOUT,"var upprop := \"%s\";\n", tjc_c->upprop);    
    TJCPRINTF(MILOUT,"var andcomb := \"%s\";\n", tjc_c->andcomb);    
    TJCPRINTF(MILOUT,"var orcomb := \"%s\";\n", tjc_c->orcomb);    
    if (tjc_c->returnall) {
        TJCPRINTF(MILOUT,"var returnall := TRUE;\n");
    } else {
        TJCPRINTF(MILOUT,"var returnall := FALSE;\n");
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_qenv (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"var qenv := new(oid, str);\n");    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_FTINAME, \"%s\");\n", tjc_c->ftindex);    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_FTIBGNAME, \"%s\");\n", tjc_c->ftindex);    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_SCOREBASE, \"%f\");\n", tjc_c->scorebase);    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_C_LAMBDA, \"%f\");\n", tjc_c->lambda);    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_RECURSIVE_TAGS, \"0\");\n");    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_OKAPI_K1, \"%f\");\n", tjc_c->okapik1);    
    TJCPRINTF(MILOUT,"qenv.insert(QENV_OKAPI_B, \"%f\");\n", tjc_c->okapib);
    TJCPRINTF(MILOUT,"\n");
}

void milprint_qnode (tjc_config *tjc_c, TJqnode_t *qn, short nid) {
    int c;
    /* TJCPRINTF(MILOUT,"var R%d := new(str,dbl);\n", nid); */   
    TJCPRINTF(MILOUT,"var R%d := new(void,str).seqbase(0@0);\n", nid);   
    if (qn->kind == q_term)
        for (c = 0; c < qn->length; c++)
	    /* TJCPRINTF(MILOUT,"R%d.insert(\"%s\",%f);\n", nid, qn->tlist[c], qn->wlist[c]); */
	    TJCPRINTF(MILOUT,"R%d.append(\"%s\");\n", nid, qn->tlist[c]);
    if (qn->kind == q_entity)
        for (c = 0; c < qn->length; c++)
	    /* TJCPRINTF(MILOUT,"R%d.insert(\"%s:%s\",%f);\n", nid, qn->elist[c], qn->tlist[c], qn->wlist[c]); */
	    TJCPRINTF(MILOUT,"R%d.append(\"%s:%s\");\n", nid, qn->elist[c], qn->tlist[c]);
}

void milprint_qnode2 (tjc_config *tjc_c, TJatree_t *tree) {
    int qid, c;
    TJqnode_t *qn;

    for (qid = 0; qid < tree->qlength; qid++) {
	qn = tree->qnodes[qid];
        TJCPRINTF(MILOUT,"var Q%d := new(str,dbl);\n", qid);   
        if (qn->kind == q_term) {
            for (c = 0; c < qn->length; c++)
                TJCPRINTF(MILOUT,"Q%d.insert(\"%s\", dbl(%f));\n", qid, qn->tlist[c], qn->wlist[c]);
	    TJCPRINTF(MILOUT,"Q%d := tj_term2tid(Q%d);\n", qid, qid);
	}
        if (qn->kind == q_entity) {
            for (c = 0; c < qn->length; c++)
	        TJCPRINTF(MILOUT,"Q%d.insert(\"%s:%s\",%f);\n", qid, qn->elist[c], qn->tlist[c], qn->wlist[c]); 
	    TJCPRINTF(MILOUT,"Q%d := tj_ent2tid(Q%d);\n", qid, qid);
	}
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_node (tjc_config *tjc_c, TJpnode_t *node, short *node_scope, short *node_printed) {
    int c;
    short child[TJPNODE_MAXCHILD];
    short nid;

    nid = node->nid;
    // check if node was printed before
    if (node_printed[nid]) return;

    // print children
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (node->child[c]) {
	    milprint_node (tjc_c, node->child[c], node_scope, node_printed);
	    child[c] = node->child[c]->nid;
	}
        else
	    child[c] = 0;

    // print node 	
    switch (node->kind) { 
	case p_desc :
	    TJCPRINTF(MILOUT,"var R%d := p_contained_by_%s(R%d, R%d, qenv);\n", nid, tjc_c->downprop, child[1], child[0]);
	    break;
	case p_anc :
	    TJCPRINTF(MILOUT,"var R%d := p_containing_%s(R%d, R%d, qenv);\n", nid, tjc_c->upprop, child[1], child[0]);
	    break;
	case p_tag :
	    if (strcmp (node->sem.str, "*") == 0)
		TJCPRINTF(MILOUT,"var R%d := select_node(qenv);\n", nid);
	    else
		TJCPRINTF(MILOUT,"var R%d := select_node(\"%s\", qenv);\n", nid, node->sem.str);
	    break;
	case p_root :
	    TJCPRINTF(MILOUT,"var R%d := select_root(qenv);\n", nid);
	    break;
	case p_ctx :
	    TJCPRINTF(MILOUT,"var R%d := select_startnodes(startNodes, qenv);\n", nid);
	    break;
	case p_about :
	    TJCPRINTF(MILOUT,"var R%d := p_containing_q_%s(R%d, R%d, qenv);\n", nid, tjc_c->irmodel, child[0], child[1]);
	    break;
	case p_query :
	    milprint_qnode (tjc_c, node->sem.qnode, nid);
	    break;
	case p_and :
	    TJCPRINTF(MILOUT,"var R%d := and_%s(R%d, R%d);\n", nid, tjc_c->andcomb, child[0], child[1]);
	    break;
	case p_or :
	    TJCPRINTF(MILOUT,"var R%d := or_%s(R%d, R%d);\n", nid, tjc_c->orcomb, child[0], child[1]);
	    break;
	case p_union :
	    TJCPRINTF(MILOUT,"var R%d := set_union(R%d, R%d, qenv);\n", nid, child[0], child[1]);
	    break;
	case p_nexi :
	    TJCPRINTF(MILOUT,"var nexi_result := R%d;\n", child[0]);
	    break;
	case p_pred :
	    TJCPRINTF(MILOUT,"ERROR, p_pred not cased\n");
	    break;
	case p_nil :
	    TJCPRINTF(MILOUT,"ERROR, pnil not cased\n");
	    break;
    }
    node_printed[nid] = 1;

    // free children if their scope ends
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (child[c] && node_scope[child[c]] == nid)
	    TJCPRINTF(MILOUT,"R%d := nil;\n", child[c]);
}

void milprint_node2 (tjc_config *tjc_c, TJatree_t *tree, TJanode_t *node, short *node_scope, char *node_printed) {
    int c;
    short child[TJPNODE_MAXCHILD];
    short nid;

    nid = node - tree->nodes;
    // check if node was printed before
    if (node_printed[nid]) return;

    // print children
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (node->child[c]) {
	    milprint_node2 (tjc_c, tree, node->child[c], node_scope, node_printed);
	    child[c] = node->child[c] - tree->nodes;
	}
        else
	    child[c] = -1;

    // print node 	
    switch (node->kind) { 
	case a_select_element :
	    if (strcmp (node->op, "select_startnodes") == 0)
	        TJCPRINTF(MILOUT,"var R%d := tj_%s(\"%s\");\n", nid, node->op, tjc_c->startNodes);
	    else if (strcmp (node->op, "select_star") == 0)
	        TJCPRINTF(MILOUT,"var R%d := tj_%s();\n", nid, node->op);
	    else
		TJCPRINTF(MILOUT,"var R%d := tj_%s(\"%s\");\n", nid, node->op, node->tag);
	    break;
	case a_contained_by : 
	case a_containing :
	case a_and :
	case a_or :
	    TJCPRINTF(MILOUT,"var R%d := tj_%s(R%d, R%d);\n", nid, node->op, child[0], child[1]);
	    break;
	case a_containing_query :
	    TJCPRINTF(MILOUT,"var R%d := tj_%s_%s(R%d, Q%d);\n", nid, node->op, tjc_c->irmodel, child[0], node->qid);
	    TJCPRINTF(MILOUT,"Q%d := nil;\n", node->qid);
	    break;
	case a_nid2pre :
	case a_pre2nid :
	case a_add_pre :
	    TJCPRINTF(MILOUT,"var R%d := tj_%s(R%d);\n", nid, node->op, child[0]);
	    break;
    }
    node_printed[nid] = 1;

    // free children if their scope ends
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (child[c] != -1 && node_scope[child[c]] == nid)
	    TJCPRINTF(MILOUT,"R%d := nil;\n", child[c]);
}

char* milprint (tjc_config *tjc_c, TJpnode_t *root)
{
    /*
     * - assign node ids
     * - create node scope array: node -> last consuming node
     * - print mil  
     */

    int num;
    int c;
    short node_scope[TJPTREE_MAXSIZE];
    short node_printed[TJPTREE_MAXSIZE];
    
    num = assign_numbers (root, 1);
    for (c = 0; c < num; c++) {
	node_scope[c] = 0;
	node_printed[c] = 0;
    }
    assign_scopes (root, node_scope);
    //for (c = 0; c < num; c++) TJCPRINTF(MILOUT,"node: %d, scope: %d\n", c, node_scope[c]);
    
    milprint_init (tjc_c);
    milprint_qenv (tjc_c);
    milprint_node (tjc_c, root, node_scope, node_printed);
    milprint_end (tjc_c);

    return &tjc_c->milBUFF[0];
}

char* milprint2 (tjc_config *tjc_c, TJatree_t *tree)
{
    /*
     * - assign node ids
     * - create node scope array: node -> last consuming node
     * - print mil  
     */

    int num;
    int c;
    short node_scope[TJPTREE_MAXSIZE];
    char node_printed[TJPTREE_MAXSIZE];
   
    num = tree->length;
    for (c = 0; c < num; c++) {
	node_scope[c] = -1;
	node_printed[c] = 0;
    }
    assign_scopes2 (tree, tree->root, node_scope, node_printed);
    //for (c = 0; c < num; c++) TJCPRINTF(MILOUT,"node: %d, scope: %d\n", c, node_scope[c]);
    for (c = 0; c < num; c++) {
	node_printed[c] = 0;
    }
    
    milprint_init2 (tjc_c);
    milprint_qenv2 (tjc_c);
    milprint_qnode2 (tjc_c, tree);
    milprint_node2 (tjc_c, tree, tree->root, node_scope, node_printed);
    milprint_end2 (tjc_c, tree);

    return &tjc_c->milBUFF[0];
}

/* vim:set shiftwidth=4 expandtab: */
/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
