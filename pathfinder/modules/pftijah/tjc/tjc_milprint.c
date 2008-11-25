
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

void milprint_qnode (tjc_config *tjc_c, TJqnode_t *qn, short nid) {
    int c;
    TJCPRINTF(MILOUT,"var R%d := new(str,dbl);\n", nid);    
    if (qn->kind == q_term)
        for (c = 0; c < qn->length; c++)
	    TJCPRINTF(MILOUT,"R%d.insert(\"%s\",%f);\n", nid, qn->tlist[c], qn->wlist[c]);
    if (qn->kind == q_entity)
        for (c = 0; c < qn->length; c++)
	    TJCPRINTF(MILOUT,"R%d.insert(\"%s:%s\",%f);\n", nid, qn->elist[c], qn->tlist[c], qn->wlist[c]);
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
	    TJCPRINTF(MILOUT,"var R%d := p_contained_by(R%d, R%d, qenv);\n", nid, child[1], child[0]);
	    break;
	case p_anc :
	    TJCPRINTF(MILOUT,"var R%d := p_containing(R%d, R%d, qenv);\n", nid, child[1], child[0]);
	    break;
	case p_tag :
	    if (strcmp (node->sem.str, "*") == 0)
		TJCPRINTF(MILOUT,"var R%d := select_node(qenv);\n", nid);
	    else
		TJCPRINTF(MILOUT,"var R%d := select_node(%s, qenv);\n", nid, node->sem.str);
	    break;
	case p_root :
	    TJCPRINTF(MILOUT,"var R%d := select_root(qenv);\n", nid);
	    break;
	case p_ctx :
	    TJCPRINTF(MILOUT,"var R%d := select_startnodes(startNodes, qenv);\n", nid);
	    break;
	case p_about :
	    TJCPRINTF(MILOUT,"var R%d := p_containing_q_NLLR(R%d, R%d, qenv);\n", nid, child[0], child[1]);
	    break;
	case p_query :
	    milprint_qnode (tjc_c, node->sem.qnode, nid);
	    break;
	case p_and :
	    TJCPRINTF(MILOUT,"var R%d := and_sum(R%d, R%d);\n", nid, child[0], child[1]);
	    break;
	case p_or :
	    TJCPRINTF(MILOUT,"var R%d := or_sum(R%d, R%d);\n", nid, child[0], child[1]);
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

    milprint_node (tjc_c, root, node_scope, node_printed);

    return &tjc_c->milBUFF[0];
}


/* vim:set shiftwidth=4 expandtab: */
/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
