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
#include "tjc_milprint.h"


void assign_scopes (TJatree_t *tree, TJanode_t *node, short *node_scope, char visited) {
    int c;
    short nid, nid_c;
    nid = node - tree->nodes;

    if (node->visited > visited) return;

    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
        if (node->child[c]) {
	    assign_scopes (tree, node->child[c], node_scope, visited);
	}
    }
    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
        if (node->child[c]) {
	    nid_c = node->child[c] - tree->nodes;
	    node_scope[nid_c] = nid;
	}
    }

    node->visited++;
}

void milprint_init (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"{\n");
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := TRUE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := TRUE;\n");
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_end (tjc_config *tjc_c, TJatree_t *tree) {
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
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := FALSE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := FALSE;\n");
    }
    if (tjc_c->semantics == 1) {
        TJCPRINTF(MILOUT,"returnAllElements := FALSE;\n");
    } 
    TJCPRINTF(MILOUT,"}\n");
}

void milprint_end_merge (tjc_config *tjc_c, TJatree_t *tree) {
    int qid, c;
    TJCPRINTF(MILOUT,"\n");
    for (qid = 0; qid < tree->qlength; qid++) {
         TJCPRINTF(MILOUT,"Q%d := nil;\n", qid);
    }   
    TJCPRINTF(MILOUT,"\n");
    if (tjc_c->maxfrag == 0) {
        /* no merge, single frag */
        TJCPRINTF(MILOUT,"nexi_score_xfer := nexi_result0;\n");
        TJCPRINTF(MILOUT,"nexi_result0 := nil;\n"); /* IMPORTANT, MEMORY LEAK */
    } else {
        /* merge case */
        TJCPRINTF(MILOUT,"var res_frag := new(void,bat).seqbase(0@0);\n");
        for (c = 0; c <= tjc_c->maxfrag; c++) {     
            TJCPRINTF(MILOUT,"res_frag.append(nexi_result%d);\n", c);    
        }
        TJCPRINTF(MILOUT,"nexi_score_xfer := tj_merge_frag_results(res_frag, %d);\n", tjc_c->topk);
        for (c = 0; c <= tjc_c->maxfrag; c++) {     
            TJCPRINTF(MILOUT,"nexi_result%d := nil;\n", c);    
        }
        TJCPRINTF(MILOUT,"res_frag := nil;\n");    
    }
    if (tjc_c->debug) {
	TJCPRINTF(MILOUT,"trace := FALSE;\n");
    }
    if (tjc_c->timing) {
	TJCPRINTF(MILOUT,"timing := FALSE;\n");
    }
    if (tjc_c->semantics == 1) {
        TJCPRINTF(MILOUT,"returnAllElements := FALSE;\n");
    } 
    TJCPRINTF(MILOUT,"}\n");
}

void milprint_end_frag (tjc_config *tjc_c, TJatree_t *tree, int frag) {
    TJCPRINTF(MILOUT,"var nexi_result%d := R" PDFMT ";\n", frag, tree->root - tree->nodes);
    TJCPRINTF(MILOUT,"R" PDFMT " := nil;\n", tree->root - tree->nodes);
    if (tree->root->preIDs == 0) TJCPRINTF(MILOUT,"nexi_result%d := tj_nid2pre(nexi_result%d);\n", frag, frag);
    if (tjc_c->rmoverlap && tree->root->nested) {
	TJCPRINTF(MILOUT,"nexi_result%d := tj_rm_overlap(nexi_result%d);\n", frag, frag);
    }
    if (tjc_c->prior) {
	TJCPRINTF(MILOUT,"nexi_result%d := tj_prior_%s(nexi_result%d);\n", frag, tjc_c->prior, frag);
    }
    TJCPRINTF(MILOUT,"nexi_result%d := tsort_rev(nexi_result%d);\n", frag, frag);
    if (tjc_c->topk) {
        TJCPRINTF(MILOUT,"nexi_result%d := nexi_result%d.slice(0, %d);\n", frag, frag, tjc_c->topk - 1);
    }
    if (tjc_c->inexout) {
        TJCPRINTF(MILOUT,"nexi_result%d := tj_pre2inexpath(nexi_result%d);\n", frag, frag);
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_qenv (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"var ftiName := \"%s\";\n", tjc_c->ftindex);    
    TJCPRINTF(MILOUT,"var ftindex;\n");    
    TJCPRINTF(MILOUT,"tj_init_termHash(ftiName);\n");    
    TJCPRINTF(MILOUT,"tj_init_tagHash(ftiName);\n");    
    TJCPRINTF(MILOUT,"var scorebase := dbl(%f);\n", tjc_c->scorebase);    
    TJCPRINTF(MILOUT,"var c_lambda := dbl(%f);\n", tjc_c->lambda);    
    TJCPRINTF(MILOUT,"var okapi_k1 := dbl(%f);\n", tjc_c->okapik1);    
    TJCPRINTF(MILOUT,"var okapi_b := dbl(%f);\n", tjc_c->okapib);    
    TJCPRINTF(MILOUT,"var downprop := \"%s\";\n", tjc_c->downprop);    
    TJCPRINTF(MILOUT,"var upprop := \"%s\";\n", tjc_c->upprop);    
    TJCPRINTF(MILOUT,"var andcomb := \"%s\";\n", tjc_c->andcomb);    
    TJCPRINTF(MILOUT,"var orcomb := \"%s\";\n", tjc_c->orcomb);    
    if (tjc_c->semantics == 1) {
        TJCPRINTF(MILOUT,"var returnall := TRUE;\n");
    } else {
        TJCPRINTF(MILOUT,"var returnall := FALSE;\n");
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_qnode (tjc_config *tjc_c, TJatree_t *tree) {
    int qid, c;
    TJqnode_t *qn;

    for (qid = 0; qid < tree->qlength; qid++) {
	qn = tree->qnodes[qid];
        TJCPRINTF(MILOUT,"var Q%d := new(str,dbl);\n", qid);   
        if (qn->kind == q_term || qn->kind == q_term_plus || qn->kind == q_term_min || qn->kind == q_phrase) {
            for (c = 0; c < qn->length; c++)
                TJCPRINTF(MILOUT,"Q%d.insert(\"%s\", dbl(%f));\n", qid, qn->tlist[c], qn->wlist[c]);
	    TJCPRINTF(MILOUT,"Q%d := tj_prepare_query(Q%d);\n", qid, qid);
	}
        if (qn->kind == q_ent || qn->kind == q_ent_plus || qn->kind == q_ent_min) {
            for (c = 0; c < qn->length; c++)
	        TJCPRINTF(MILOUT,"Q%d.insert(\"%s:%s\", dbl(%f));\n", qid, qn->elist[c], qn->tlist[c], qn->wlist[c]); 
	    TJCPRINTF(MILOUT,"Q%d := tj_ent2tid(Q%d);\n", qid, qid);
	}
    }
    TJCPRINTF(MILOUT,"\n");
}

void milprint_node (tjc_config *tjc_c, TJatree_t *tree, TJanode_t *node, short *node_scope, char visited) {
    int c;
    short child[TJPNODE_MAXCHILD];
    short nid;

    nid = node - tree->nodes;
    // check if node was printed before
    if (node->visited > visited) return;

    // print children
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (node->child[c]) {
	    milprint_node (tjc_c, tree, node->child[c], node_scope, visited);
	    child[c] = node->child[c] - tree->nodes;
	}
        else
	    child[c] = -1;

    // print node 	
    switch (node->kind) { 
	case a_select_element :
	    if (strcmp (node->op, "select_startnodes") == 0)
	        TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s();\n", nid, node->op);
	    else if (strcmp (node->op, "select_star") == 0)
	        TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s();\n", nid, node->op);
	    else
		TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s(\"%s\");\n", nid, node->op, node->tag);
	    break;
	case a_contained_by : 
	case a_containing :
	case a_and :
	case a_or :
	    TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s(R%d, R%d);\n", nid, node->op, child[0], child[1]);
	    break;
	case a_containing_query :
	    switch ((tree->qnodes[(short)node->qid])->kind) {
		case q_term :
	            TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s_%s(R%d, Q%d);\n", nid, node->op, tjc_c->irmodel, child[0], node->qid);
		    break;
		case q_ent : 
		    TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s_%s(R%d, Q%d);\n", nid, node->op, tjc_c->conceptirmodel, child[0], node->qid);
		    break;
		case q_phrase :
		case q_term_plus :
		case q_term_min :
		case q_ent_plus :
		case q_ent_min :
	            TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s(R%d, Q%d);\n", nid, node->op, child[0], node->qid);
		    break;
	    }
	    break;
	case a_nid2pre :
	case a_pre2nid :
	case a_add_pre :
	    TJCPRINTF(MILFRAGOUT,"var R%d := tj_%s(R%d);\n", nid, node->op, child[0]);
	    break;
    }
    node->visited++;

    // debug output
    if (tjc_c->debug > 9)
	TJCPRINTF(MILFRAGOUT,"R%d.print();\n", nid);
 
    // free children if their scope ends
    for (c = 0; c < TJPNODE_MAXCHILD; c++) 
        if (child[c] != -1 && node_scope[child[c]] == nid)
	    TJCPRINTF(MILFRAGOUT,"R%d := nil;\n", child[c]);

}

// producing a correct empty mil-script for debugging purposes 
char* milprint_empty (tjc_config *tjc_c) {
    TJCPRINTF(MILOUT,"{\n");
    TJCPRINTF(MILOUT,"var nexi_result := new(oid,dbl);\n");
    TJCPRINTF(MILOUT,"nexi_score_xfer := nexi_result;\n");
    TJCPRINTF(MILOUT,"nexi_result := nil;\n");
    TJCPRINTF(MILOUT,"}\n");
    return &tjc_c->milBUFF[0];
}

char* milprint (tjc_config *tjc_c, TJatree_t *tree)
{
    /*
     * - assign node ids
     * - create node scope array: node -> last consuming node
     * - print mil  
     */

    int num;
    int c;
    short node_scope[TJPTREE_MAXSIZE];
    char visited;
   
    num = tree->length;
    for (c = 0; c < num; c++) {
	node_scope[c] = -1;
    }
    visited = tree->root->visited;
    assign_scopes (tree, tree->root, node_scope, visited);
    
    milprint_init (tjc_c);
    milprint_qenv (tjc_c);
    milprint_qnode (tjc_c, tree);
    visited = tree->root->visited;
    milprint_node (tjc_c, tree, tree->root, node_scope, visited);
   
    // for old index management 
    if (tjc_c->maxfrag < 0) {
        TJCPRINTF(MILOUT,"ftindex := ftiName;\n");    
        TJCPRINTF(MILOUT,"%s", &tjc_c->milfragBUFF[0]);
    } 
    for (c = 0; c <= tjc_c->maxfrag; c++) {     
        TJCPRINTF(MILOUT,"ftindex := ftiName + \"%d\";\n", c);    
        TJCPRINTF(MILOUT,"%s", &tjc_c->milfragBUFF[0]);
        milprint_end_frag (tjc_c, tree, c);
    }
    
    // for old index management
    if (tjc_c->maxfrag < 0) { 
        milprint_end (tjc_c, tree);
    } else {
        milprint_end_merge (tjc_c, tree);
    }


    return &tjc_c->milBUFF[0];
}

/* vim:set shiftwidth=4 expandtab: */
/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
