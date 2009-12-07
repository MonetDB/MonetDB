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
#include "tjc_normalize_query.h"
#include "tjc_phys_optimize.h"
#include "tjc_milprint.h"

#define DEBUG 0

tjc_config* tjc_c_GLOBAL;

extern int
tjc_parser (char* input, TJptree_t **res);

int interpret_options(tjc_config* tjc_c, BAT* optbat) {
    /* 
     * initialize vars first 
     */
    tjc_c->parseDotFile	= NULL;
    tjc_c->algDotFile	= NULL;
    tjc_c->milFile	= NULL;
    tjc_c->milBUFF[0]	= 0;
    tjc_c->milfragBUFF[0] = 0;
    tjc_c->dotBUFF[0]	= 0;
    tjc_c->debug	= 0;
    tjc_c->timing	= 0;
    tjc_c->ftindex	= "DFLT_FT_INDEX";
    tjc_c->topk 	= 0;
    tjc_c->maxfrag	= 0;
    tjc_c->irmodel	= "NLLR";
    tjc_c->conceptirmodel = "LogSum";
    tjc_c->orcomb	= "sum";
    tjc_c->andcomb	= "prod";
    tjc_c->upprop	= "max";
    tjc_c->downprop	= "max";
    tjc_c->prior	= NULL;
    tjc_c->scorebase	= 0.0;
    tjc_c->lambda	= 0.8;
    tjc_c->okapik1	= 1.2;
    tjc_c->okapib	= 0.75;
    tjc_c->semantics	= 0;
    tjc_c->rmoverlap	= 0;
    tjc_c->inexout      = 0;

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
	} else if ( strcmp(optName,"parsedotfile") == 0 ) {
	    tjc_c->parseDotFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"algdotfile") == 0 ) {
	    tjc_c->algDotFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"returnNumber") == 0 ) {
            tjc_c->topk = atoi(optVal);
	} else if ( strcmp(optName,"maxfrag") == 0 ) {
            tjc_c->maxfrag = atoi(optVal);
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
            }
        } else if ( strcmp(optName,"concept-ir-model") == 0 ) {
            if ( strcasecmp(optVal,"LogSum") == 0 ) {
                tjc_c->conceptirmodel = "LogSum";
            } else if ( strcasecmp(optVal,"NLLR") == 0 ) {
                tjc_c->conceptirmodel = "NLLR";
            } else if ( strcasecmp(optVal,"LMs") == 0 ) {
                tjc_c->conceptirmodel = "LMs";
            } else if ( strcasecmp(optVal,"PRFube") == 0 ) {
                tjc_c->conceptirmodel = "PRFube";
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
                tjc_c->semantics = 1;
        } else if ( strcmp(optName,"semantics") == 0 ) {
            if ( strcasecmp(optVal,"strict") == 0 ) 
                tjc_c->semantics = 0;
            else if ( strcasecmp(optVal,"vague") == 0 ) 
                tjc_c->semantics = 1;
        } else if (strcmp(optName, "inexout") == 0) {
            if (strcasecmp(optVal, "TRUE") == 0) {
                tjc_c->inexout = 1;
            }
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
	fprintf(f,"%s",content);
	fclose(f);
	return 1;
}

char* tjc_new_parse(char* query, BAT* optbat, BAT* rtagbat, int use_sn, char** errBUFF)
{
    tjc_config *tjc_c = (tjc_config*)TJCmalloc(sizeof(struct tjc_config));

    tjc_c_GLOBAL = tjc_c;

    char *milres = NULL;

    TJptree_t *ptree;
    TJpnode_t *root;
    TJatree_t *atree;
  
    if (DEBUG) {
	stream_printf(GDKout,"#!tjc interpreting options\n");
	stream_printf(GDKout,"NEXI query: %s\n", query);
    }
    if ( !interpret_options(tjc_c,optbat) ) {
    	TJCfree(tjc_c);
    	*errBUFF = GDKstrdup("option handling error");
	return NULL;
    }

    if (DEBUG) stream_printf(GDKout,"#!tjc parsing[%s]\n!",query);
    int status = tjc_parser(query,&ptree);
    if (DEBUG) stream_printf(GDKout,"#!tjc parser status = %d\n",status);
    if (use_sn && (ptree->is_rel_path_exp == 0)) {
	*errBUFF = GDKstrdup("Error (new NEXI syntax): A query with startnodes should start with a relative path expression");
        if (DEBUG) stream_printf(GDKout,"#!tjc error <%s>\n",errBUFF);
	return NULL;
    }
    if ((use_sn == 0) && ptree->is_rel_path_exp) {
	*errBUFF = GDKstrdup("Error (new NEXI syntax): A query without startnodes should start with an absolute path expression");
        if (DEBUG) stream_printf(GDKout,"#!tjc error <%s>\n",errBUFF);
	return NULL;
    }
    if (!status) {
	root = &ptree->node[ptree->length - 1];
        if (DEBUG) stream_printf(GDKout,"#!tjc start normalize\n");
	normalize(ptree);
        if (DEBUG) stream_printf(GDKout,"#!tjc start optimize\n");
	optimize(ptree);
        if (DEBUG) stream_printf(GDKout,"#!tjc start normalize query\n");
        normalize_query(tjc_c, ptree);
        if (DEBUG) stream_printf(GDKout,"#!tjc start physical optimization\n");
	atree = phys_optimize (tjc_c, ptree, root, rtagbat);
        if (DEBUG) stream_printf(GDKout,"#!tjc start mil generation\n");
	milres = milprint (tjc_c, atree);
        
	// optional parse tree dot output
	if (tjc_c->parseDotFile) { 
	    tjc_c->dotBUFF[0] = '\0';
	    printTJptree (tjc_c, root); 
    	    save2file(tjc_c->parseDotFile, tjc_c->dotBUFF);
	}
	// optional parse tree dot output
	if (tjc_c->algDotFile) { 
	    tjc_c->dotBUFF[0] = '\0';
	    printTJatree (tjc_c, atree); 
    	    save2file(tjc_c->algDotFile, tjc_c->dotBUFF);
	}
	// optional mil output
        if ( tjc_c->milFile ) {
    	    save2file(tjc_c->milFile, milres);
        }
        //switch to see mil/dot output without the errors of an executed query
	if (0) {
	    tjc_c->milBUFF[0] = '\0';
	    milres = milprint_empty (tjc_c);
	}
	milres = GDKstrdup(milres); // INCOMPLETE, check free
	if (DEBUG) stream_printf(GDKout,"#!tjc start of mil:\n%s",milres);
	if (DEBUG) stream_printf(GDKout,"#!tjc end of mil\n");
	
	free_atree (atree);
	tjcp_freetree (ptree);
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
