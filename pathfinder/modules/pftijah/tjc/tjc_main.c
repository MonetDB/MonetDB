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
#include <stdio.h>
#include "tjc_abssyn.h"
#include "tjc_normalize.h"
#include "tjc_optimize.h"
#include "tjc_phys_optimize.h"
#include "tjc_milprint.h"

#define DEBUG 0

void printTJpnode(tjc_config* tjc_c, TJpnode_t *node, short parID)
{
    char *type = "unknown";
    TJptype_t num_type = node->kind;
    TJqnode_t *qn;
    short nID = node->nid;
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
	case p_nil   : type = "ERROR-NIL"; break;
    }
    
    if (num_type == p_tag)
    	TJCPRINTF(DOTOUT,"%d [label=\"%s-%s\"]\n", nID, type, node->sem.str);
    else if (num_type == p_query) {
	qn = node->sem.qnode;
        switch (qn->kind) {
	    case q_mixed : type = "mixed"; break;
	    case q_term  : type = "term"; break;
	    case q_entity: type = "entity"; break;
	}
    	TJCPRINTF(DOTOUT,"%d [label=\"%s-", nID, type);
	for (c = 0; c < qn->length; c++)
    	    TJCPRINTF(DOTOUT,"%s ", qn->tlist[c]);
    	TJCPRINTF(DOTOUT,"\"]\n");
    }
    else
        TJCPRINTF(DOTOUT,"%d [label=\"%s\"]\n", nID, type);
    if (parID) 
	TJCPRINTF(DOTOUT,"%d -> %d\n", parID, nID);

    for (c = 0; c < TJPNODE_MAXCHILD; c++) {
	if (node->child[c] != NULL) printTJpnode (tjc_c, node->child[c], nID);
    }
}

void printTJptree(tjc_config* tjc_c, TJpnode_t *root)
{
    TJCPRINTF(DOTOUT,"digraph G {\n");
    printTJpnode (tjc_c, root, 0);
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

void free_tree(TJptree_t *ptree)
{
    int c;
    TJpnode_t *n;
    for (c = 0; c < ptree->length; c++) {
	n = &ptree->node[c];
	if (n->kind == p_query)
	    TJCfree (n->sem.qnode);
	if (n->kind == p_tag && strcmp (n->sem.str, "*"))
	    TJCfree (n->sem.str);
    }
    TJCfree (ptree);
}

/* THE NEW STUFF  */

tjc_config* tjc_c_GLOBAL;

extern int
tjc_parser (char* input, TJptree_t **res);

int interpret_options(tjc_config* tjc_c, BAT* optbat) {
    /* 
     * initialize vars first 
     */
    tjc_c->dotFile	= NULL;
    tjc_c->milFile	= NULL;
    tjc_c->milBUFF[0]	= 0;
    tjc_c->dotBUFF[0]	= 0;
    tjc_c->debug	= 0;
    tjc_c->timing	= 0;
    tjc_c->ftindex	= "DFLT_FT_INDEX";
    tjc_c->irmodel	= "NLLR";
    tjc_c->orcomb	= "sum";
    tjc_c->andcomb	= "prod";
    tjc_c->upprop	= "max";
    tjc_c->downprop	= "max";
    tjc_c->prior	= NULL;
    tjc_c->scorebase	= 0.0;
    tjc_c->lambda	= 0.8;
    tjc_c->okapik1	= 1.2;
    tjc_c->okapib	= 0.75;
    tjc_c->returnall	= 0;
    tjc_c->rmoverlap	= 0;

    /*
     * end of initialization
     */
    BUN p, q;
    BATiter optbati = bat_iterator(optbat);
    BATloop(optbat, p, q) {
        str optName = (str)BUNhead(optbati,p);
        str optVal  = (str)BUNtail(optbati,p);

        if ( strcmp(optName,"debug") == 0 ) {
	    tjc_c->debug = atoi(optVal);
	} else if ( strcmp(optName,"_query") == 0 ) {
	} else if ( strcmp(optName,"newversion") == 0 ) {
	} else if ( strcmp(optName,"timing") == 0 ) {
            if ( strcasecmp(optVal,"TRUE") == 0 ) 
                tjc_c->timing = 1;
	} else if ( strcmp(optName,"milfile") == 0 ) { 
	    tjc_c->milFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"dotfile") == 0 ) {
	    tjc_c->dotFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"ft-index") == 0 ) {
            tjc_c->ftindex = GDKstrdup(optVal);
        } else if ( strcmp(optName,"ir-model") == 0 ) {
            if ( strcasecmp(optVal,"LM") == 0 ) {
                tjc_c->irmodel = "LM";
                tjc_c->scorebase = 1.0;
            } else if ( strcasecmp(optVal,"LMS") == 0 ) {
                tjc_c->irmodel = "LMs";
                tjc_c->scorebase = 1.0;
            } else if ( strcasecmp(optVal,"OKAPI") == 0 ) {
                tjc_c->irmodel = "OKAPI";
            } else if ( strcasecmp(optVal,"NLLR") == 0 ) {
                tjc_c->irmodel = "NLLR";
            } else if ( strcasecmp(optVal,"PRFube") == 0 ) {
                tjc_c->irmodel = "PRFube";
            }
        } else if ( strcmp(optName,"orcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                tjc_c->orcomb = "sum";
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                tjc_c->orcomb = "max";
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                tjc_c->orcomb = "prob";
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                tjc_c->orcomb = "exp";
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                tjc_c->orcomb = "min";
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                tjc_c->orcomb = "prod";
            }
        } else if ( strcmp(optName,"andcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                tjc_c->andcomb = "sum";
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                tjc_c->andcomb = "max";
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                tjc_c->andcomb = "prob";
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                tjc_c->andcomb = "exp";
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                tjc_c->andcomb = "min";
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                tjc_c->andcomb = "prod";
            }
        } else if ( strcmp(optName,"upprop") == 0 ) {        
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                tjc_c->upprop = "sum";
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                tjc_c->upprop = "max";
            }
        } else if ( strcmp(optName,"downprop") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                tjc_c->downprop = "sum";
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                tjc_c->downprop = "max";
            }
        } else if ( strcmp(optName,"collection-lambda") == 0) { 
                tjc_c->lambda = atof (optVal);
        } else if ( strcmp(optName,"okapi-k1") == 0 ) {
                tjc_c->okapik1 = atof (optVal);
        } else if ( strcmp(optName,"okapi-b") == 0 ) {
                tjc_c->okapib = atof (optVal);
        } else if ( strcmp(optName,"return-all") == 0 ) {
            if ( strcasecmp(optVal,"TRUE") == 0 ) 
                tjc_c->returnall = 1;
        } else if (strcmp(optName, "prior") == 0) {
            if (strcasecmp(optVal, "LENGTH_PRIOR") == 0) {
                tjc_c->prior = "ls";
            }
        }  else if (strcmp(optName, "rmoverlap") == 0) {
           if (strcasecmp(optVal, "TRUE") == 0) 
                tjc_c->rmoverlap = 1;
        } else {
            stream_printf(GDKout,"TijahOptions: should handle: %s=%s\n",optName,optVal);
        }
    }
    return 1;
}

int save2file(char* name, char *content) {
	FILE *f;

	if ( !(f = fopen(name,"w")) ) {
	    stream_printf(GDKerr,"#! WARNING: cannot open output file: %s.\n",name);
	    return 0;
	}
	fprintf(f,content);
	fclose(f);
	return 1;
}

char* tjc_new_parse(char* query, BAT* optbat, BAT* rtagbat, char* startNodes_name, char** errBUFF)
{
    tjc_config *tjc_c = (tjc_config*)TJCmalloc(sizeof(struct tjc_config));

    tjc_c_GLOBAL = tjc_c;

    char *milres = NULL;

    TJptree_t *ptree;
    TJpnode_t *root;
    TJatree_t *atree;
  
    if (startNodes_name)
    	tjc_c->startNodes = startNodes_name;
    else
	tjc_c->startNodes = NULL;

    if (DEBUG) stream_printf(GDKout,"#!tjc interpreting options\n");
    if ( !interpret_options(tjc_c,optbat) ) {
    	*errBUFF = GDKstrdup("option handling error");
	return NULL;
    }
    if (DEBUG) stream_printf(GDKout,"#!tjc parsing[%s]\n!",query);
    int status = tjc_parser(query,&ptree);
    if (DEBUG) stream_printf(GDKout,"#!tjc status = %d\n",status);
    if (ptree) {
	root = &ptree->node[ptree->length - 1];
        if (DEBUG) stream_printf(GDKout,"#!tjc start normalize\n");
	normalize(ptree, root);
        if (DEBUG) stream_printf(GDKout,"#!tjc start optimize\n");
	optimize(ptree, root);
	atree = phys_optimize (ptree, root, rtagbat);

	// optional dot output
	if (tjc_c->dotFile) { 
	    printTJatree (tjc_c, atree); 
    	    save2file(tjc_c->dotFile, tjc_c->dotBUFF);
	}
        if (DEBUG) stream_printf(GDKout,"#!tjc start mil generation\n");
	// start henning
	milres = milprint2 (tjc_c, atree);
	milres = GDKstrdup(milres); // INCOMPLETE, check free
	tjc_c->milBUFF[0] = '\0';
	// optional mil output
        if ( tjc_c->milFile ) {
    	    save2file(tjc_c->milFile, milres);
        }
	// end henning
	milres = milprint (tjc_c, root);
	milres = GDKstrdup(milres); // INCOMPLETE, check free
	/* optional mil output
        if ( tjc_c->milFile ) {
    	    save2file(tjc_c->milFile, milres);
        } */
	if (DEBUG) stream_printf(GDKout,"#!tjc start of mil:\n%s",milres);
	if (DEBUG) stream_printf(GDKout,"#!tjc end of mil\n");
	
	free_tree(ptree);
    }
    else {
	*errBUFF = GDKstrdup(tjc_c->errBUFF);
        if (DEBUG) stream_printf(GDKout,"#!tjc error <%s>\n",errBUFF);
	return NULL;
    }
    if (DEBUG) stream_printf(GDKout,"#!tjc succesfull end \n");


    TJCfree(tjc_c);

    tjc_c_GLOBAL = NULL;

    return milres;
}

/* vim:set shiftwidth=4 expandtab: */
/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */
