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
 * Copyright (C) 2006-2007 "University of Twente".
 * All Rights Reserved.
 *
 * Author(s): Vojkan Mihajlovic
 *	      Jan Flokstra
 *            Henning Rode
 *            Roel van Os
 *
 *    Main module for generating logical query plans and MIL query plans from NEXI queries
 */

/**
 * nexi.c:this is the main entry point of the NEXI-to-SRA compiler for the PF/Tijah 
 * IR querying module.
 *
 * The original code in this file was taken from TIJAH (written by Vojkan Mihajlovic et al.)
 * and gradually rewritten for PF/Tijah.
 *
 * NEXI queries are processed by old_main as follows:
 *
 *  1. parseNEXI(): nexi.l and nexi.y: first parsing of the NEXI query. 
 *     The tokens and commands are written to temporary files.
 *
 *  2. preprocess(): nexi_preprocessor.c: performs preprocessing for phrases and vague selection
 *     The temporary files created in step 1 are used for this.
 *     Output is written in the same files.
 *
 *  3. process(): nexi_preprocessor.c: performs stemming and stop word removal
 *     The temporary files created in step 1-2 are used as input. 
 *     Output is written in the same files.
 *
 *  4. COtoCPlan or CAStoCPlan(): nexi_rewriter.c: rewrites CO or CAS queries.
 *     Content-Only (CO) queries are rewritten to Content-And-Structure (CAS) queries,
 *     because the following step only supports CAS queries as input.
 *
 *     The temporary files created in step 1-3 are used as input. 
 *     Output is written in the same files.
 *    
 *  5. CAS_plan_gen(): nexi_generate_plan.c: converts queries into logical level plans.
 *     Input is read from the temporary files created and modified in steps 1-4.
 *     Output is a structure of command_tree instances (see nexi.h)
 *
 *  6. SRA_to_MIL(): nexi_generate_mil.c: converts logical level query plan to executable MIL code.
 *     Input is the command_tree structure from step 5.
 *     Output (MIL code) is written to a buffer. This buffer is executed.
 *
 *  
 */

#include <pf_config.h>

#include <monet.h>
#include <gdk.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "pftijah.h"
#include "nexi.h"
#include "pftijah.h"
#include "pftijah_util.h"

#define LOGFILE   GDKout
#define LOGPRINTF if ( 0 ) stream_printf

static TijahParserContext parserCtxStruct;

TijahParserContext* parserCtx = &parserCtxStruct;

/* main */

extern int old_main(BAT* optbat, char* startNodes_name);

char* tijahParse(BAT* optbat, char* startNodes_name, char* query, char** errBUFF) {
  /* setup TijahParserContext structure */
  LOGPRINTF(LOGFILE,"- tijahParse([%s])\n",query);
  parserCtx->collection   = "DFLT_FT_INDEX";
  parserCtx->queryText    = query;
  parserCtx->errBUFF[0]   = 0;
  parserCtx->milFILEname  = NULL;
  /* initialize the lists */
  if ( ! (
        tnl_init(&parserCtx->command_preLIST, "COMMAND_PRE", TSL_DFLTMAX) &&
        tsl_init(&parserCtx->token_preLIST, "TOKEN_PRE", TSL_DFLTMAX) &&
        tnl_init(&parserCtx->commandLIST, "COMMAND", TSL_DFLTMAX) &&
        tsl_init(&parserCtx->tokenLIST, "TOKEN", TSL_DFLTMAX)
     ) ) {
      sprintf(&parserCtx->errBUFF[0],"Error: cannot create LIST structures.\n");
      return FALSE;
  }
  parserCtx->milBUFF[0] = 0;
  /* */
  MILPRINTF(MILOUT, "#\n# Generated NEXI MIL by Pathfinder-PFTIJAH package \n#\n\n");
  MILPRINTF(MILOUT, "module(pftijah);\n");
  MILPRINTF(MILOUT, "{\n");
  if ( 0 ) {
    MILPRINTF(MILOUT, "loaded();\n");
    MILPRINTF(MILOUT, "sigs(\"pftijah\");\n");
    MILPRINTF(MILOUT, "tj_ping();\n");
  }
  /* */
  parserCtx->tjCtx     = NULL; /* INCOMPLETE, should be filled here */
  if ( !old_main(optbat,startNodes_name) )  {
      if ( errBUFF ) {
	  if ( parserCtx->errBUFF[0] ) /* a nexi error message is generated */
              *errBUFF = &parserCtx->errBUFF[0];
	  else
              *errBUFF = "tijahParse: parse error";
      }
      return NULL;
  }
  else {
      tnl_free(&parserCtx->command_preLIST);
      tnl_free(&parserCtx->commandLIST);
      tsl_free(&parserCtx->token_preLIST);
      tsl_free(&parserCtx->tokenLIST);
      return &parserCtx->milBUFF[0];
  }
}

extern command_tree **CAS_plan_gen(
	int query_num,
	int topic_type,
	struct_RMT *txt_retr_model,
	struct_RF *rel_feedback,
	bool alg_type,
	char *mil_filename,
	char *logical_filename,
	char *result_name,
	bool scale_on);

int old_main(BAT* optbat, char* startNodes_name)
{
    /* for the type of topics and for the preprocessing */
    bool use_startNodes = (startNodes_name != NULL);
    bool scale_on;
    bool phrase_in;
    bool stem_stop_query;
    int topic_type;
    int preproc_type;
    int query_num = 1;
    int query_end_num;
    int rewrite_type;
    int algebra_type;
    int return_all;
    char background_collection[20] = "";

    /* Result table name */
    char *res_table = "result_table";

    /* Intermediate file names */
    /* char *milpre_filename = "/tmp/mil_pre.mil"; */
    char *milpre_filename = NULL; // do not write the mil pre plan
    /* char *logical_filename = "/tmp/logical.sra"; */
    char *logical_filename = NULL; // do not write a logical plan
    char *mil_filename = "/tmp/query_plan.mil";

    /* pointer to SRA command tree structure */
    command_tree **p_command_array;

    /* returned value of the function that generates plans */
    int plan_ret;

    /* retrieval model and relevance feedback variables */
    struct_RMT *txt_retr_model;
    /* struct_RMI *img_retr_model , *img_retr_model1 ; */
    struct_RMI *img_retr_model;
    struct_RF *rel_feedback;

    /* structure initialization */
    txt_retr_model = GDKmalloc(MAX_QUERIES*sizeof(struct_RMT));
    img_retr_model = GDKmalloc(MAX_QUERIES*sizeof(struct_RMI));
    rel_feedback   = GDKmalloc(MAX_QUERIES*sizeof(struct_RF));
    if ( !txt_retr_model || !img_retr_model || !rel_feedback ) {
        stream_printf(GDKout,"nexi.c:old_main: GDKmalloc failed.\n");
        return 0;
    }


    /*** Set default configuration values here: ***/
    
    /** Text retrieval model parameters **/
    txt_retr_model->qnumber     = 0;
    txt_retr_model->model       = MODEL_NLLR;
    txt_retr_model->or_comb     = OR_SUM;
    txt_retr_model->and_comb    = AND_PROD;
    txt_retr_model->up_prop     = UP_WSUMD;
    txt_retr_model->down_prop   = DOWN_SUM;
    //strcpy(txt_retr_model->e_class, "TRUE");
    strcpy(txt_retr_model->e_class, "FALSE");
    txt_retr_model->stemming    = TRUE;
    txt_retr_model->size_type   = SIZE_TERM;
    txt_retr_model->param1      = 0.8;
    txt_retr_model->param2      = 0.5;
    txt_retr_model->param3      = 0;
    txt_retr_model->prior_type  = NO_PRIOR;
    //txt_retr_model->prior_type  = LENGTH_PRIOR;    
    //txt_retr_model->prior_size  = 0;
    txt_retr_model->prior_size  = 2;
    txt_retr_model->rmoverlap = FALSE;
    strcpy(txt_retr_model->context, "");
    txt_retr_model->extra       = 0.0;
    txt_retr_model->next        = NULL;
    
    /** Compiler parameters **/
#if 0
    // The number of elements to return
    int retNum          = -1; // -1 = unlimited
#endif
    algebra_type        = COARSE2;
    preproc_type        = PLAIN;
    scale_on            = FALSE;
    rewrite_type        = BASIC;
    return_all          = FALSE;
    stem_stop_query     = FALSE;
    bool eq_init        = FALSE;
   
    /* set query environment */
    MILPRINTF(MILOUT, "var qenv := create_qenv();\n");

    /* startup of argument options */
    if ( use_startNodes ) {
        MILPRINTF(MILOUT, "var startNodes := new(void,oid);\n");
        MILPRINTF(MILOUT, "if (isnil(CATCH(bat(\"%s\").count()))) {\n", startNodes_name );
        MILPRINTF(MILOUT, "  startNodes := bat(\"%s\");\n", startNodes_name);
        MILPRINTF(MILOUT, "  bat(\"%s\").persists(false);\n", startNodes_name);
        if ( TDEBUG(98) ) {
          MILPRINTF(MILOUT,"  printf(\"# tijah-mil-exec: contents of startNodes is:\\n\");\n");
          MILPRINTF(MILOUT,"  bat(\"%s\").print();\n",startNodes_name);
        }
        MILPRINTF(MILOUT, "}\n" );
    } 
    
    MILPRINTF(MILOUT, "var trace     := FALSE;\n" );
    MILPRINTF(MILOUT, "var tracefile := \"\";\n" );
    

    char* qenv_prox_val  = NULL;
    char* qenv_fb_val    = NULL;
    char* qenv_scorebase = "0"; //default setting
    char* qenv_c_lambda  = "0.8"; //default setting
    char* qenv_okapi_k1  = "1.2"; //default
    char* qenv_okapi_b   = "0.75"; //default

    BUN p, q;
    BATiter optbati = bat_iterator(optbat);
    BATloop(optbat, p, q) {
        str optName = (str)BUNhead(optbati,p);
        str optVal  = (str)BUNtail(optbati,p);

        if ( strcmp(optName,"debug") == 0 ) {
	    if ( 0 ) {
	        /* set in serialize options for now, is earlier */
	        int v = atoi(optVal);
	        SET_TDEBUG(v);
	        if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting debug value to %d.\n",v);
	    }
	} else if ( strcmp(optName,"timing") == 0 ) {
            if ( strcasecmp(optVal,"TRUE") == 0 ) {
                MILPRINTF(MILOUT, "timing := TRUE;\n" );
            } else {
                MILPRINTF(MILOUT, "timing := FALSE;\n" );
            }
	} else if ( strcmp(optName,"milfile") == 0 ) {
	    /* incomplete open file */
            parserCtx->milFILEname = optVal;
	} else if ( strcmp(optName,"ft-index") == 0 ) {
            parserCtx->collection = optVal;
	} else if ( strcmp(optName,"fragments") == 0 ) {
	      if (TDEBUG(1)) stream_printf(GDKout,"# old_main: ignoring fragmentation setting.\n");
	} else if ( strcmp(optName,"background_collection") == 0 ) {
            strcpy(background_collection, optVal);
#if 0
        } else if ( strcmp(optName,"returnNumber") == 0 ) {
            int xx = atoi( optVal );
	    if ( xx < 0 ) {
	    	// incomplete should check if number is OK
	    }
#endif
        } else if ( strcmp(optName,"algebraType") == 0 ) {
            if ( strcasecmp( optVal, "ASPECT" ) == 0 ) {
                algebra_type = ASPECT;
            } else if ( strcasecmp( optVal, "COARSE" ) == 0 ) {
                algebra_type = COARSE;
            } else if ( strcasecmp( optVal, "COARSE2" ) == 0 ) {
                algebra_type = COARSE2;
            }
        
        } else if ( strcmp(optName,"ir-model") == 0 ) { /* CHANGED: was txtmodel_model */
            if ( strcasecmp(optVal,"BOOL") == 0 ) {
                txt_retr_model->model = MODEL_BOOL;
            } else if ( strcasecmp(optVal,"LM") == 0 ) {
                txt_retr_model->model = MODEL_LM;
                qenv_scorebase = "1";
            } else if ( strcasecmp(optVal,"LMS") == 0 ) {
                txt_retr_model->model = MODEL_LMS;
                qenv_scorebase = "1";
            } else if ( strcasecmp(optVal,"TFIDF") == 0 ) {
                txt_retr_model->model = MODEL_TFIDF;
            } else if ( strcasecmp(optVal,"OKAPI") == 0 ) {
                txt_retr_model->model = MODEL_OKAPI;
            } else if ( strcasecmp(optVal,"GPX") == 0 ) {
                txt_retr_model->model = MODEL_GPX;
            } else if ( strcasecmp(optVal,"LMA") == 0 ) {
                txt_retr_model->model = MODEL_LMA;
            } else if ( strcasecmp(optVal,"LMSE") == 0 ) {
                txt_retr_model->model = MODEL_LMSE;
            } else if ( strcasecmp(optVal,"LMVFLT") == 0 ) {
                txt_retr_model->model = MODEL_LMVFLT;
            } else if ( strcasecmp(optVal,"LMVFLT") == 0 ) {
                txt_retr_model->model = MODEL_LMVLIN;
            } else if ( strcasecmp(optVal,"NLLR") == 0 ) {
                txt_retr_model->model = MODEL_NLLR;
            }
            
        } else if ( strcmp(optName,"txtmodel_orcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                txt_retr_model->or_comb = OR_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                txt_retr_model->or_comb = OR_MAX;
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                txt_retr_model->or_comb = OR_PROB;
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                txt_retr_model->or_comb = OR_EXP;
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                txt_retr_model->or_comb = OR_MIN;
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                txt_retr_model->or_comb = OR_PROD;
            }
        } else if ( strcmp(optName,"txtmodel_andcomb") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                txt_retr_model->and_comb = AND_SUM;
            } else if ( strcasecmp(optVal,"MAX") == 0 ) {
                txt_retr_model->and_comb = AND_MAX;
            } else if ( strcasecmp(optVal,"PROB") == 0 ) {
                txt_retr_model->and_comb = AND_PROB;
            } else if ( strcasecmp(optVal,"EXP") == 0 ) {
                txt_retr_model->and_comb = AND_EXP;
            } else if ( strcasecmp(optVal,"MIN") == 0 ) {
                txt_retr_model->and_comb = AND_MIN;
            } else if ( strcasecmp(optVal,"PROD") == 0 ) {
                txt_retr_model->and_comb = AND_PROD;
            }
        } else if ( strcmp(optName,"txtmodel_upprop") == 0 ) {        
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                txt_retr_model->up_prop = UP_SUM;
            } else if ( strcasecmp(optVal,"AVG") == 0 ) {
                txt_retr_model->up_prop = UP_AVG;
            } else if ( strcasecmp(optVal,"WSUMD") == 0 ) {
                txt_retr_model->up_prop = UP_WSUMD;
            } else if ( strcasecmp(optVal,"WSUMA") == 0 ) {
                txt_retr_model->up_prop = UP_WSUMA;
            }

        } else if ( strcmp(optName,"txtmodel_downprop") == 0 ) {
            if ( strcasecmp(optVal,"SUM") == 0 ) {
                txt_retr_model->down_prop = DOWN_SUM;
            } else if ( strcasecmp(optVal,"AVG") == 0 ) {
                txt_retr_model->down_prop = DOWN_AVG;
            } else if ( strcasecmp(optVal,"WSUMD") == 0 ) {
                txt_retr_model->down_prop = DOWN_WSUMD;
            } else if ( strcasecmp(optVal,"WSUMA") == 0 ) {
                txt_retr_model->down_prop = DOWN_WSUMA;
            }
            
        } else if ( strcmp(optName,"ir-model-param1") == 0) {
            txt_retr_model->param1 = atof( optVal );
        } else if ( strcmp(optName,"ir-model-param2") == 0) {
            txt_retr_model->param2 = atof( optVal );
        } else if ( strcmp(optName,"ir-model-param3") == 0) {
            txt_retr_model->param3 = atof( optVal );
        } else if ( strcmp(optName,"collection-lambda") == 0) { 
            txt_retr_model->param1 = atof( optVal );
	    qenv_c_lambda = optVal;
        } else if ( strcmp(optName,"okapi-k1") == 0 ) {
            txt_retr_model->param1 = atof( optVal );
            qenv_okapi_k1 = optVal;
        } else if ( strcmp(optName,"okapi-b") == 0 ) {
            txt_retr_model->param2 = atof( optVal );
            qenv_okapi_b = optVal;
        } else if ( strcmp(optName,"txtmodel_returnall") == 0 ) {
            if ( strcasecmp(optVal,"TRUE") == 0 ) {
                return_all = TRUE;
            } else {
                return_all = FALSE;
            }
        
        } else if ( strcmp(optName,"preprocessing_type") == 0 ) {
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
              
        } else if ( strcmp(optName,"generator_type") == 0 ) {
            if ( strcasecmp(optVal,"BASIC") == 0 ) 
                rewrite_type = BASIC;
            if ( strcasecmp(optVal,"SIMPLE") == 0 ) 
                rewrite_type = SIMPLE;
            if ( strcasecmp(optVal,"ADVANCED") == 0 ) 
                rewrite_type = ADVANCED;
        
        } else if ( strstr( optName, "equivalence_class" ) ) {
            if ( !eq_init ) {
                MILPRINTF(MILOUT, "tj_initEquivalences();\n" );
                eq_init = TRUE;
            } 
            MILPRINTF(MILOUT, "var eqclass := new(void, str).seqbase(oid(0));\n" );
            char delims[] = ", ";
            char *result = NULL;
            result = strtok( optVal, delims );
            while( result != NULL ) {
                MILPRINTF(MILOUT, "eqclass.append(\"%s\");\n", result );
                result = strtok( NULL, delims );
            }
            MILPRINTF(MILOUT, "tj_addEquivalenceClass( eqclass );\n" );
            
        } else if ( strcmp(optName,"use_equivalences") == 0 ) {
            if ( strcasecmp(optVal,"TRUE") == 0 ) { 
                strcpy(txt_retr_model->e_class, "TRUE");
            } else {
                strcpy(txt_retr_model->e_class, "FALSE");
            }
            
        } else if ( strcmp(optName,"sra_tracefile") == 0 ) {
            MILPRINTF(MILOUT, "trace     := TRUE;\n" );
            MILPRINTF(MILOUT, "tracefile := \"%s\";\n", optVal );
            
 /*       } else if (strcmp(optName, "scoreBase") == 0) {
            if (strcasecmp(optVal, "ONE") == 0) {
                qenv_scorebase = "0";
            } else {
                qenv_scorebase = "1";
            } */
        } else if (strcmp(optName, "stem_stop_query") == 0) {
            if (strcasecmp(optVal, "TRUE") == 0) {
                stem_stop_query = TRUE;
            } else {
                stem_stop_query = FALSE;
            }
            
        } else if (strcmp(optName, "prior") == 0) {
            if (strcasecmp(optVal, "LENGTH_PRIOR") == 0) {
                txt_retr_model->prior_type  = LENGTH_PRIOR;
            } else if (strcasecmp(optVal, "LOG_LENGTH_PRIOR") == 0) {
                txt_retr_model->prior_type  = LOG_LENGTH_PRIOR;
            } else {
                txt_retr_model->prior_type  = NO_PRIOR;
            }
            
        }  else if (strcmp(optName, "rmoverlap") == 0) {
           if (strcasecmp(optVal, "TRUE") == 0) {
                txt_retr_model->rmoverlap = TRUE;
           } else {
               txt_retr_model->rmoverlap=FALSE;
           }
	} else if (strcmp(optName, "returnNumber") == 0) {
	    // ignore, is handled by milprint_summer
        } else if (strcmp(optName, "term-proximity") == 0) {
                qenv_prox_val = (char*)strdup(optVal);
        } else if (strcmp(optName, "feedback-docs") == 0) {
                qenv_fb_val = (char*)strdup(optVal);
        } else {
            stream_printf(GDKout,"TijahOptions: should handle: %s=%s\n",optName,optVal);
        }
    }
    /*
     * Now find out if the collection is fragmented or not.
     */
    /* INCOMPLETE, ERROR HERE WITH REFCOUNTS IN HEAD */
    BAT* fb = pftu_lookup_bat(pftu_batname1("tj_%s_fragments",(char*)parserCtx->collection,0));
    if ( ! fb ) {
           stream_printf(GDKerr,"Error: cannot find fragments bat for collection \"%s\".\n",parserCtx->collection);
           return 0;
    }
    if ( BATcount(fb) > 1 ) {
	      if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting fragmentation ON.\n");
              parserCtx->useFragments = 1;
              parserCtx->ffPfx        = "_frag";
              parserCtx->flastPfx     = "";
    } else {
	      if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting fragmentation OFF.\n");
              parserCtx->useFragments = 0;
              parserCtx->ffPfx        = "";
              parserCtx->flastPfx     = ", str(1)";
    }
    BBPunfix(BBPcacheid(fb));
    fb = NULL;
    // Some special cases for NLLR, since NLLR only works with COARSE2 at the moment
    if ( txt_retr_model->model == MODEL_NLLR ) {
        // Switch to COARSE2 algebra for NLLR
        algebra_type = COARSE2;
    }
        
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_FTINAME,\"%s\");\n",parserCtx->collection);
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_FTIBGNAME,\"%s\");\n",parserCtx->collection);
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_SCOREBASE,\"%s\");\n",qenv_scorebase);
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_C_LAMBDA,\"%s\");\n",qenv_c_lambda);
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_RECURSIVE_TAGS,\"%s\");\n","0");
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_OKAPI_K1,\"%s\");\n",qenv_okapi_k1);
    MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_OKAPI_B,\"%s\");\n",qenv_okapi_b);

    // ensure the hash tables are hashed
    MILPRINTF(MILOUT, "tj_chk_dict_hash(bat(_tj_TagBat(\"%s\")),bat(_tj_TermBat(\"%s\")));\n",parserCtx->collection,parserCtx->collection);

    // Prepend some variables to the MIL code.
    if ( qenv_prox_val ) { 
        MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_TERM_PROXIMITY,\"%s\");\n",qenv_prox_val);
	free(qenv_prox_val);
	qenv_prox_val = NULL;
    }
    if ( qenv_fb_val ) { 
        MILPRINTF(MILOUT, "modify_qenv(qenv,QENV_FEEDBACK_DOCS,\"%s\");\n",qenv_fb_val);
	free(qenv_fb_val);
	qenv_fb_val = NULL;
    }

#if 0
    MILPRINTF(MILOUT, "retNum := %d;\n", retNum);
#endif
    MILPRINTF(MILOUT, "var stemmer := bat(\"tj_\"+ qenv.find(QENV_FTINAME) +\"_param\").find(\"stemmer\");\n");
    if (strcmp(background_collection,""))
    { MILPRINTF(MILOUT, "qenv := tj_setBackgroundCollName(\"%s\",qenv);\n", background_collection); }
    
    if ( return_all ) {
        MILPRINTF(MILOUT, "returnAllElements := true;\n" );
    } else {
        MILPRINTF(MILOUT, "returnAllElements := false;\n" );
    }
      
    /* specifying input and output files and result table name */

    FILE* milpre_file = NULL;
    if ( milpre_filename && !(milpre_file = fopen(milpre_filename,"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing fast mil code.\n");
        return 0;
    }

    FILE* logical_file = NULL;
    if ( logical_filename && !(logical_file = fopen(logical_filename,"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing logical plan.\n");
        return 0;
    }

    if ( milpre_file ) fclose(milpre_file);
    if ( logical_file ) fclose(logical_file);

    /*  Perform parsing of NEXI query */
    plan_ret = parseNEXI(parserCtx,&query_end_num);

    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"Query parsing was not successful.\n");
        return 0;
    } else {
        topic_type = plan_ret;
    }

    img_retr_model = NULL;

    /* preprocessing original query plans */
    plan_ret = preprocess(preproc_type);

    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"Preprocessing was not successful.\n");
        return 0;
    }
    
    // Find out which stemmer was used for collection to be queried
    char batName[100]; 
    sprintf( batName, "tj_%s_param", parserCtx->collection );
    BAT* collOptBat = BATdescriptor( BBPindex(batName) );
    BATiter collOptBati = bat_iterator(collOptBat);
    BUN bun = BUNfnd( collOptBat, (str)"stemmer" );
    char *stemmer = (char *)BUNtail( collOptBati, bun );

    /* processing original query plans: stemming and stop word removal */
    plan_ret = process(stemmer, stem_stop_query, FALSE);

    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"Processing was not successful.\n");
        return 0;
    }


    /* rewriting CO and CAS queries into internal representation of queries */
    if (topic_type == CO_TOPIC) {
        plan_ret = COtoCPlan(query_num, rewrite_type, txt_retr_model, rel_feedback);
    } else if (topic_type == CAS_TOPIC) {
        if (txt_retr_model->prior_type == NO_PRIOR)
            plan_ret = CAStoCPlan(query_num, rewrite_type, FALSE);
        else
            plan_ret = CAStoCPlan(query_num, rewrite_type, TRUE);
    }


    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"Query rewriting was not successful.\n");
        return 0;
    }


    /* generating logical query plans */
    p_command_array = CAS_plan_gen( query_num, 
                                    topic_type, 
                                    txt_retr_model, 
                                    rel_feedback, 
                                    algebra_type, 
                                    milpre_filename, 
                                    logical_filename, 
                                    res_table, 
                                    scale_on);

    if (p_command_array != NULL)
        ;
    else {
        LOGPRINTF(LOGFILE,"Logical query plan generation was unsuccessful.\n");
        return 0;
    }

    /* generating MIL plan */
    if ( (preproc_type == NO_MODIFIER) | (preproc_type == VAGUE_MODIF) | (preproc_type == STRICT_MODIF) )
        phrase_in = TRUE;
    else
        phrase_in = FALSE;

    parserCtx->milFILEdesc = NULL;
    if ( parserCtx->milFILEname ) {
      if ( !(parserCtx->milFILEdesc = fopen(parserCtx->milFILEname,"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing mil code.\n");
        return 0;
      }
    }

    if (phrase_in == TRUE)
        plan_ret = SRA_to_MIL(parserCtx, 
                              query_num, 
			      use_startNodes,
                              txt_retr_model, 
                              img_retr_model, 
                              rel_feedback, 
                              mil_filename, 
                              "INCOMPLETE", 
                              p_command_array, 
                              TRUE);
    else
        plan_ret = SRA_to_MIL(parserCtx, 
                              query_num, 
			      use_startNodes,
                              txt_retr_model, 
                              img_retr_model, 
                              rel_feedback, 
                              mil_filename, 
                              "INCOMPLETE", 
                              p_command_array, 
                              FALSE);

    LOGPRINTF(LOGFILE,"\tGenerated MIL in string, size=%d\n",strlen(parserCtx->milBUFF));
     if ( parserCtx->milFILEdesc ) {
       fprintf(parserCtx->milFILEdesc,"%s",parserCtx->milBUFF);
       fclose(parserCtx->milFILEdesc);
     }

    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"MIL query plan generation was not successful.\n");
        return 0;
    }

    /* memory cleaning */
    p_command_array = NULL;
    GDKfree(p_command_array); /* NONSENSE */
    txt_retr_model = NULL;
    img_retr_model = NULL;
    GDKfree(txt_retr_model);
    GDKfree(img_retr_model); /* NONSENSE */
    rel_feedback = NULL;
    GDKfree(rel_feedback); /* NONSENSE */

    return 1;
}

/*
 *
 *  The Tijah[Number|String]List implementations
 *
 */

/* #define DEBUG_LIST */

int   tsl_init(TijahStringList* tsl, char* label, int max) {
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# init LIST[%s].\n",label);
#endif
	tsl->label = label;
	tsl->cnt   = 0;
	tsl->max   = max;
	tsl->val   = GDKmalloc( tsl->max * sizeof(char*) );
	assert(tsl->val);
	return 1;
}

int tsl_clear(TijahStringList* tsl) {
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# clearing LIST[%s].\n",tsl->label);
#endif
	for (int i=0; i<tsl->cnt; i++) {
		GDKfree(tsl->val[i]);
	}
	tsl->cnt = 0;
	return 1;
}

int tsl_free(TijahStringList* tsl) {
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# free string LIST[%s].\n",tsl->label);
#endif
	for (int i=0; i<tsl->cnt; i++) {
		GDKfree(tsl->val[i]);
	}
	GDKfree(tsl->val);
	return 1;
}

char* tsl_append(TijahStringList* tsl, char* v) {
	if ( tsl->cnt >= tsl->max) {
		tsl->max *= 2;
		tsl->val = GDKrealloc(tsl->val,(tsl->max * sizeof(char*)));
		assert(tsl->val);
	}
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# appending \"%s\" to LIST[%s].\n",v,tsl->label);
#endif
	return (tsl->val[tsl->cnt++] = GDKstrdup(v));
}

char* tsl_appendq(TijahStringList* tsl, char* v) {
	static char b[128];

	sprintf(b,"\"%s\"",v);
	return tsl_append(tsl,b);
}

int   tnl_init(TijahNumberList* tnl, char* label, int max) {
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# init LIST[%s].\n",label);
#endif
	tnl->label = label;
	tnl->cnt   = 0;
	tnl->max   = max;
	tnl->val   = GDKmalloc( tnl->max * sizeof(int) );
	assert(tnl->val);
	return 1;
}

int tnl_clear(TijahNumberList* tnl) {
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# clearing LIST[%s].\n",tnl->label);
#endif
	tnl->cnt = 0;
	return 1;
}

int tnl_free(TijahNumberList* tnl) {
	GDKfree(tnl->val);
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# free number LIST[%s].\n",tnl->label);
#endif
	return 1;
}

int tnl_append(TijahNumberList* tnl, int v) {
	if ( tnl->cnt >= tnl->max) {
		tnl->max *= 2;
		tnl->val = GDKrealloc(tnl->val,tnl->max * sizeof(int));
		assert(tnl->val);
	}
#ifdef DEBUG_LIST
	stream_printf(GDKout,"# appending (%d) to LIST[%s].\n",v,tnl->label);
#endif
	tnl->val[tnl->cnt++] = v;
	return 1;
}
