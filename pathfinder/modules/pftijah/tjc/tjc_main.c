#include <pf_config.h>

#include <gdk.h>

#include <stdio.h>
#include "tjc_abssyn.h"
#include "tjc_normalize.h"
#include "tjc_optimize.h"
#include "tjc_milprint.h"

#define DEBUG 1

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
tjc_parser (char* input, TJptree_t **res, char* err);

int interpret_options(tjc_config* tjc_c, BAT* optbat) {
    /* 
     * initialize vars first 
     */
    tjc_c->dotFile	= NULL;
    tjc_c->milFile	= NULL;
    tjc_c->milBUFF[0]	= 0;
    tjc_c->dotBUFF[0]	= 0;
    /*
     * end of initialization
     */
    BUN p, q;
    BATiter optbati = bat_iterator(optbat);
    BATloop(optbat, p, q) {
        str optName = (str)BUNhead(optbati,p);
        str optVal  = (str)BUNtail(optbati,p);

        if ( strcmp(optName,"debug") == 0 ) {
	    // if ( 0 ) {
	        // /* set in serialize options for now, is earlier */
	        // int v = atoi(optVal);
	        // SET_TDEBUG(v);
	        // if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting debug value to %d.\n",v);
	    // }
        } else if (strcmp(optName, "newversion") == 0) {
        } else if (strcmp(optName, "_query") == 0) {
	        /* OK, this is the regular query transfer option */
	} else if ( strcmp(optName,"timing") == 0 ) {
            // if ( strcasecmp(optVal,"TRUE") == 0 ) {
                // MILPRINTF(MILOUT, "timing := TRUE;\n" );
            // } else {
                // MILPRINTF(MILOUT, "timing := FALSE;\n" );
            // }
	} else if ( strcmp(optName,"milfile") == 0 ) {
		tjc_c->milFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"dotfile") == 0 ) {
		tjc_c->dotFile = GDKstrdup(optVal);
	} else if ( strcmp(optName,"ft-index") == 0 ) {
            // parserCtx->collection = optVal;
	} else if ( strcmp(optName,"fragments") == 0 ) {
	      // if (TDEBUG(1)) stream_printf(GDKout,"# old_main: ignoring fragmentation setting.\n");
	} else if ( strcmp(optName,"background_collection") == 0 ) {
            // strcpy(background_collection, optVal);
#if 0
        } else if ( strcmp(optName,"returnNumber") == 0 ) {
            // int xx = atoi( optVal );
	    // if ( xx < 0 ) {
	    	// incomplete should check if number is OK
	    // }
#endif
        } else if ( strcmp(optName,"algebraType") == 0 ) {
            // if ( strcasecmp( optVal, "ASPECT" ) == 0 ) {
                // algebra_type = ASPECT;
            // } else if ( strcasecmp( optVal, "COARSE" ) == 0 ) {
                // algebra_type = COARSE;
            // } else if ( strcasecmp( optVal, "COARSE2" ) == 0 ) {
                // algebra_type = COARSE2;
            // }
        
        } else if ( strcmp(optName,"ir-model") == 0 ) { /* CHANGED: was txtmodel_model */
            if ( strcasecmp(optVal,"BOOL") == 0 ) {
                // txt_retr_model->model = MODEL_BOOL;
            } else if ( strcasecmp(optVal,"LM") == 0 ) {
                // txt_retr_model->model = MODEL_LM;
                // qenv_scorebase = "1";
            } else if ( strcasecmp(optVal,"LMS") == 0 ) {
                // txt_retr_model->model = MODEL_LMS;
                // qenv_scorebase = "1";
            } else if ( strcasecmp(optVal,"TFIDF") == 0 ) {
                // txt_retr_model->model = MODEL_TFIDF;
            } else if ( strcasecmp(optVal,"OKAPI") == 0 ) {
                // txt_retr_model->model = MODEL_OKAPI;
            } else if ( strcasecmp(optVal,"GPX") == 0 ) {
                // txt_retr_model->model = MODEL_GPX;
            } else if ( strcasecmp(optVal,"LMA") == 0 ) {
                // txt_retr_model->model = MODEL_LMA;
            } else if ( strcasecmp(optVal,"LMSE") == 0 ) {
                // txt_retr_model->model = MODEL_LMSE;
            } else if ( strcasecmp(optVal,"LMVFLT") == 0 ) {
                // txt_retr_model->model = MODEL_LMVFLT;
            } else if ( strcasecmp(optVal,"LMVFLT") == 0 ) {
                // txt_retr_model->model = MODEL_LMVLIN;
            } else if ( strcasecmp(optVal,"NLLR") == 0 ) {
                // txt_retr_model->model = MODEL_NLLR;
            } else if ( strcasecmp(optVal,"PRF") == 0 ) {
                // txt_retr_model->model = MODEL_PRF;
            }
            
        } else if ( strcmp(optName,"txtmodel_orcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                // txt_retr_model->or_comb = OR_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                // txt_retr_model->or_comb = OR_MAX;
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                // txt_retr_model->or_comb = OR_PROB;
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                // txt_retr_model->or_comb = OR_EXP;
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                // txt_retr_model->or_comb = OR_MIN;
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                // txt_retr_model->or_comb = OR_PROD;
            }
        } else if ( strcmp(optName,"txtmodel_andcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                // txt_retr_model->and_comb = AND_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                // txt_retr_model->and_comb = AND_MAX;
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                // txt_retr_model->and_comb = AND_PROB;
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                // txt_retr_model->and_comb = AND_EXP;
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                // txt_retr_model->and_comb = AND_MIN;
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                // txt_retr_model->and_comb = AND_PROD;
            }
        } else if ( strcmp(optName,"txtmodel_upprop") == 0 ) {        
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                // txt_retr_model->up_prop = UP_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                // txt_retr_model->up_prop = UP_MAX;
            } else if ( strcasecmp(optVal,"WSUMD") == 0 ) {
                // txt_retr_model->up_prop = UP_WSUMD;
            } else if ( strcasecmp(optVal,"WSUMA") == 0 ) {
                // txt_retr_model->up_prop = UP_WSUMA;
            }

        } else if ( strcmp(optName,"txtmodel_downprop") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                // txt_retr_model->down_prop = DOWN_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                // txt_retr_model->down_prop = DOWN_MAX;
            } else if ( strcasecmp(optVal,"WSUMD") == 0 ) {
                // txt_retr_model->down_prop = DOWN_WSUMD;
            } else if ( strcasecmp(optVal,"WSUMA") == 0 ) {
                // txt_retr_model->down_prop = DOWN_WSUMA;
            }
            
        } else if ( strcmp(optName,"ir-model-param1") == 0) {
            // txt_retr_model->param1 = atof( optVal );
        } else if ( strcmp(optName,"ir-model-param2") == 0) {
            // txt_retr_model->param2 = atof( optVal );
        } else if ( strcmp(optName,"ir-model-param3") == 0) {
            // txt_retr_model->param3 = atof( optVal );
        } else if ( strcmp(optName,"collection-lambda") == 0) { 
            // txt_retr_model->param1 = atof( optVal );
	    // qenv_c_lambda = optVal;
        } else if ( strcmp(optName,"okapi-k1") == 0 ) {
            // txt_retr_model->param1 = atof( optVal );
            // qenv_okapi_k1 = optVal;
        } else if ( strcmp(optName,"okapi-b") == 0 ) {
            // txt_retr_model->param2 = atof( optVal );
            // qenv_okapi_b = optVal;
        } else if ( strcmp(optName,"txtmodel_returnall") == 0 ) {
            // if ( strcasecmp(optVal,"TRUE") == 0 ) {
                // return_all = TRUE;
            // } else {
                // return_all = FALSE;
            // }
        
        } else if ( strcmp(optName,"preprocessing_type") == 0 ) {
	    /*
            if ( strcasecmp(optVal,"PLAIN") == 0 ) 
                preproc_type = PLAIN;
            else if ( strcasecmp(optVal,"NO_MODIFIER") == 0 ) 
                preproc_type = NO_MODIFIER;
            else if ( strcasecmp(optVal,"VAGUE_NO_PHRASE") == 0 ) 
                preproc_type = VAGUE_NO_PHRASE;
            else if ( strcasecmp(optVal,"STRICT_NO_PHRASE") == 0 ) 
                preproc_type = STRICT_NO_PHRASE;
            else if ( strcasecmp(optVal,"VAGUE") == 0 ) 
                preproc_type = VAGUE_MODIF;
            else if ( strcasecmp(optVal,"STRICT") == 0 ) 
                preproc_type = STRICT_MODIF;
	    */
        } else if ( strcmp(optName,"generator_type") == 0 ) {
	    /*
            if ( strcasecmp(optVal,"BASIC") == 0 ) 
                rewrite_type = BASIC;
            if ( strcasecmp(optVal,"SIMPLE") == 0 ) 
                rewrite_type = SIMPLE;
            if ( strcasecmp(optVal,"ADVANCED") == 0 ) 
                rewrite_type = ADVANCED;
            */
        } else if ( strstr( optName, "equivalence_class" ) ) {
            // if ( !eq_init ) {
                // MILPRINTF(MILOUT, "tj_initEquivalences();\n" );
                // eq_init = TRUE;
            // } 
            // MILPRINTF(MILOUT, "var eqclass := new(void, str).seqbase(oid(0));\n" );
            // char delims[] = ", ";
            // char *result = NULL;
            // result = strtok( optVal, delims );
            // while( result != NULL ) {
                // MILPRINTF(MILOUT, "eqclass.append(\"%s\");\n", result );
                // result = strtok( NULL, delims );
            // }
            // MILPRINTF(MILOUT, "tj_addEquivalenceClass( eqclass );\n" );
            
        } else if ( strcmp(optName,"use_equivalences") == 0 ) {
            // if ( strcasecmp(optVal,"TRUE") == 0 ) { 
                // strcpy(txt_retr_model->e_class, "TRUE");
            // } else {
                // strcpy(txt_retr_model->e_class, "FALSE");
            // }
            
        } else if ( strcmp(optName,"sra_tracefile") == 0 ) {
            // MILPRINTF(MILOUT, "trace     := TRUE;\n" );
            // MILPRINTF(MILOUT, "tracefile := \"%s\";\n", optVal );
           //  
 /*       } else if (strcmp(optName, "scoreBase") == 0) {
            // if (strcasecmp(optVal, "ONE") == 0) {
                // qenv_scorebase = "0";
            // } else {
                // qenv_scorebase = "1";
            // } */
        } else if (strcmp(optName, "stem_stop_query") == 0) {
            // if (strcasecmp(optVal, "TRUE") == 0) {
                // stem_stop_query = TRUE;
            // } else {
                // stem_stop_query = FALSE;
            // }
            
        } else if (strcmp(optName, "prior") == 0) {
            // if (strcasecmp(optVal, "LENGTH_PRIOR") == 0) {
                // txt_retr_model->prior_type  = LENGTH_PRIOR;
            // } else if (strcasecmp(optVal, "LOG_LENGTH_PRIOR") == 0) {
                // txt_retr_model->prior_type  = LOG_LENGTH_PRIOR;
            // } else {
                // txt_retr_model->prior_type  = NO_PRIOR;
            // }
            
        }  else if (strcmp(optName, "rmoverlap") == 0) {
           // if (strcasecmp(optVal, "TRUE") == 0) {
                // txt_retr_model->rmoverlap = TRUE;
           // } else {
               // txt_retr_model->rmoverlap=FALSE;
           // }
	} else if (strcmp(optName, "returnNumber") == 0) {
	    // ignore, is handled by milprint_summer
        } else if (strcmp(optName, "term-proximity") == 0) {
                // qenv_prox_val = (char*)strdup(optVal);
        } else if (strcmp(optName, "feedback-docs") == 0) {
                // qenv_fb_val = (char*)strdup(optVal);
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

char* tjc_new_parse(char* query, BAT* optbat, char* startNodes_name, char** errBUFF)
{
    (void) optbat;
    (void) startNodes_name;
    (void) errBUFF;

    tjc_config *tjc_c = (tjc_config*)TJCmalloc(sizeof(struct tjc_config));

    tjc_c_GLOBAL = tjc_c;

    char *milres = NULL;

    TJptree_t *ptree;
    TJpnode_t *root;
        
    if (DEBUG) stream_printf(GDKout,"#!tjc interpreting options\n");
    if ( !interpret_options(tjc_c,optbat) ) {
    	*errBUFF = GDKstrdup("option handling error");
	return NULL;
    }
    if (DEBUG) stream_printf(GDKout,"#!tjc parsing[%s]\n!",query);
    int status = tjc_parser(query,&ptree,&tjc_c->errBUFF[0]);
    if (DEBUG) stream_printf(GDKout,"#!tjc status = %d\n",status);
    if (ptree) {
	root = &ptree->node[ptree->length - 1];
        if (DEBUG) stream_printf(GDKout,"#!tjc start normalize\n");
	normalize(ptree, root);
        if (DEBUG) stream_printf(GDKout,"#!tjc start optimize\n");
	optimize(ptree, root);

	// dot or mil output
	if (tjc_c->dotFile) {
	    assign_numbers (root, 1); // BUG, HENNING THIS SCREWS UP THE TREE
    	    printTJptree (tjc_c, root);
    	    save2file(tjc_c->dotFile, tjc_c->dotBUFF);
	}
	else 
            if (DEBUG) stream_printf(GDKout,"#!tjc start mil generation\n");
	    milres = milprint (tjc_c,root);
	    milres = GDKstrdup(milres); // INCOMPLETE, check free
            if ( tjc_c->milFile ) {
    	        save2file(tjc_c->milFile, milres);
            }
	    if (DEBUG) stream_printf(GDKout,"#!tjc start of mil:\n%s",milres);
	    if (DEBUG) stream_printf(GDKout,"#!tjc end of mil\n");
	
	free_tree(ptree);
    }
    else {
	// error
	*errBUFF = GDKstrdup(tjc_c->errBUFF);
	return NULL;
    }


    TJCfree(tjc_c);

    tjc_c_GLOBAL = NULL;

    return milres;
}



#if 0
int main() 
{
	tjc_main();
}
#endif

