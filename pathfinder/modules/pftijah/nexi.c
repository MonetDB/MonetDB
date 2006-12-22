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
 *
 * Original heading of this file:
 *    QPlanGen.c
 *    =========================
 *    Author: Vojkan Mihajlovic
 *    University of Twente
 *
 *    Main module for generating logical query plans and MIL query plans from NEXI queries
 *
 */

#include <pf_config.h>

#include <monet.h>
#include <gdk.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>

#include "pftijah.h"
#include "nexi.h"
#include "pftijah.h"

/*
 * #define LOGFILE   parserCtx->logFILE
 * #define LOGPRINTF if ( parserCtx->logFILE ) fprintf
 */
#define LOGFILE   GDKout
#define LOGPRINTF if ( 0 ) stream_printf

static TijahParserContext parserCtxStruct;

TijahParserContext* parserCtx = &parserCtxStruct;

/* main */

extern int old_main(BAT* optbat, char* startNodes_name);

char* tijahParse(BAT* optbat, char* startNodes_name, char* query, char** errBUFF) {
  /* setup TijahParserContext structure */
  LOGPRINTF(LOGFILE,"- tijahParse([%s])\n",query);
  parserCtx->collection   = "PFX";
  parserCtx->queryText    = query;
  parserCtx->logFILE      = NULL;
  parserCtx->errBUFF[0]   = 0;
  parserCtx->useFragments = 0;
  parserCtx->ffPfx        = ""; /* "_frag"*/;
#ifdef GENMILSTRING
  parserCtx->milBUFF[0] = 0;
#else
  ERROR: should open de miloutput file here
#endif
  /* */
  MILPRINTF(MILOUT, "#\n# Generated NEXI MIL by Pathfinder-PFTIJAH package \n#\n\n");
  MILPRINTF(MILOUT, "module(pftijah);\n");
  MILPRINTF(MILOUT, "{\n");
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
      return &parserCtx->milBUFF[0];
  }
}

extern command_tree **CAS_plan_gen(
	int query_num,
	int topic_type,
	struct_RMT *txt_retr_model,
	struct_RF *rel_feedback,
	bool alg_type,
	char *mil_fname,
	char *log_fname,
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
    int processing_type;
    int query_num = 1;
    int query_end_num;
    int rewrite_type;
    int language_type;
    int algebra_type;
    int return_all;
    int base_type;
    char background_collection[20] = "";

    /* Result table name */
    char *res_table = "result_table";

    /* Intermediate file names */
    char *milpre_fname = "mil_pre.mil";
    char *log_fname = "logical.sra";
    char *mil_fname = "query_plan.mil";

    /* pointer to SRA command tree structure */
    command_tree **p_command_array;

    /* returned value of the function that generates plans */
    int plan_ret;

    /* retrieval model and relevance feedback variables */
    struct_RMT *txt_retr_model;
    /* struct_RMI *img_retr_model , *img_retr_model1 ; */
    struct_RMI *img_retr_model;
    struct_RF *rel_feedback;

    /* files that store parser command and token output */
    FILE *command_file_pre;
    FILE *token_file_pre;
    /* files that store command and token output afte stop words  removal and stemming */
    FILE *command_file;
    FILE *token_file;

    /* structure initialization */
    /* 
       r.aly@ewi.utwente.nl (2006-11-24): 
       bad hack (2*) because of this function writes behind the allocated space 
       and code ist not understood

       roel.van.os@humanitech.nl (2006-12-15):
       This bug should be fixed now, with the cleanup of nexi_rewriter.c
    */
    txt_retr_model = calloc(MAX_QUERIES, sizeof(struct_RMT));
    img_retr_model = calloc(MAX_QUERIES, sizeof(struct_RMI));
    rel_feedback = calloc(MAX_QUERIES, sizeof(struct_RF));

    /*** Set default configuration values here: ***/
    
    /** Text retrieval model parameters **/
    txt_retr_model->qnumber     = 0;
    txt_retr_model->model       = MODEL_LMS;
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
    strcpy(txt_retr_model->context, "");
    txt_retr_model->extra       = 0.0;
    txt_retr_model->next        = NULL;
    
    /** Compiler parameters **/
    // The number of elements to return
    int retNum          = -1; // -1 = unlimited
    algebra_type        = COARSE2;
    preproc_type        = PLAIN;
    scale_on            = FALSE;
    processing_type     = NO_STOP_STEM;
    rewrite_type        = BASIC;
    language_type       = ENGLISH;
    base_type           = ONE;
    return_all          = FALSE;
    stem_stop_query     = FALSE;
    bool eq_init        = FALSE;
    
    /* startup of argument options */
    MILPRINTF(MILOUT, "var startNodes := nil;\n");
    if ( use_startNodes ) {
        MILPRINTF(MILOUT, "if ( view_bbp_name().reverse().exist(\"%s\") ) {\n", startNodes_name );
        MILPRINTF(MILOUT, "  startNodes := bat(\"%s\");\n", startNodes_name);
        MILPRINTF(MILOUT, "  bat(\"%s\").persists(false);\n", startNodes_name);
        MILPRINTF(MILOUT, "}\n" );
    } 
    
    MILPRINTF(MILOUT, "var trace     := FALSE;\n" );
    MILPRINTF(MILOUT, "var tracefile := \"\";\n" );
    
    BUN p, q;
    BATloop(optbat, p, q) {
        str optName = (str)BUNhead(optbat,p);
        str optVal  = (str)BUNtail(optbat,p);

        if ( strcmp(optName,"debug") == 0 ) {
	    if ( 0 ) {
	        /* set in serialize options for now, is earlier */
	        int v = atoi(optVal);
	        SET_TDEBUG(v);
	        if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting debug value to %d.\n",v);
	    }
	} else if ( strcmp(optName,"collection") == 0 ) {
            parserCtx->collection = optVal;
	} else if ( strcmp(optName,"fragments") == 0 ) {
	    if ( (strcmp(optVal,"true")==0) || 
	         (strcmp(optVal,"TRUE")==0) ||
	         (strcmp(optVal,"on")==0) ||
	         (strcmp(optVal,"ON")==0) 
	       ) {
	      if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting fragmentation ON.\n");
              parserCtx->useFragments = 1;
              parserCtx->ffPfx        = "_frag";
	    } else {
	      if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting fragmentation OFF.\n");
              parserCtx->useFragments = 0;
              parserCtx->ffPfx        = "";
	    }
	} else if ( strcmp(optName,"background_collection") == 0 ) {
            strcpy(background_collection, optVal);
            
        } else if ( strcmp(optName,"returnNumber") == 0 || 
                    strcmp(optName,"retNum") == 0 || 
                    strcmp(optName,"top") == 0 ) {
            retNum = atoi( optVal );
            
        } else if ( strcmp(optName,"algebraType") == 0 ) {
            if ( strcasecmp( optVal, "ASPECT" ) == 0 ) {
                algebra_type = ASPECT;
            } else if ( strcasecmp( optVal, "COARSE" ) == 0 ) {
                algebra_type = COARSE;
            } else if ( strcasecmp( optVal, "COARSE2" ) == 0 ) {
                algebra_type = COARSE2;
            }
        
        } else if ( strcmp(optName,"txtmodel_model") == 0 ) {
            if ( strcasecmp(optVal,"BOOL") == 0 ) {
                txt_retr_model->model = MODEL_BOOL;
            } else if ( strcasecmp(optVal,"LM") == 0 ) {
                txt_retr_model->model = MODEL_LM;
            } else if ( strcasecmp(optVal,"LMS") == 0 ) {
                txt_retr_model->model = MODEL_LMS;
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
            
        } else if ( strcmp(optName,"txtmodel_collectionLambda") == 0 || 
                    strcmp(optName,"txtmodel_param1") == 0) {
            txt_retr_model->param1 = atof( optVal );

        } else if ( strcmp(optName,"txtmodel_param2") == 0 ) {
            txt_retr_model->param2 = atof( optVal );
            
        } else if ( strcmp(optName,"txtmodel_param3") == 0 ) {
            txt_retr_model->param3 = atof( optVal );
        
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
            
        } else if (strcmp(optName, "scoreBase") == 0) {
            if (strcasecmp(optVal, "ONE") == 0) {
                MILPRINTF(MILOUT, "scoreBase := 1;\n");
                base_type = ONE;
            } else {
                MILPRINTF(MILOUT, "scoreBase := 0;\n");
                base_type = ZERO;
            }
            
        } else if (strcmp(optName, "stem_stop_query") == 0) {
            if (strcasecmp(optVal, "TRUE") == 0) {
                stem_stop_query = TRUE;
            } else {
                stem_stop_query = FALSE;
            }
            
        } else if (strcmp(optName, "prior") == 0) {
            if (strcasecmp(optVal, "LENGTH_PRIOR") == 0) {
                txt_retr_model->prior_type  = LENGTH_PRIOR;
            } else {
                txt_retr_model->prior_type  = NO_PRIOR;
            }
            
        } else {
            stream_printf(GDKout,"TijahOptions: should handle: %s=%s\n",optName,optVal);
        }
    }
    
    
    // Some special cases for NLLR, since NLLR only works with COARSE2 at the moment
    if ( txt_retr_model->model == MODEL_NLLR ) {
        // Switch to COARSE2 algebra for NLLR
        algebra_type = COARSE2;
    }
        
    
    // Prepend some variables to the MIL code.
    MILPRINTF(MILOUT, "tj_setCollName(\"%s\");\n", parserCtx->collection);
    MILPRINTF(MILOUT, "retNum := %d;\n", retNum);
    MILPRINTF(MILOUT, "var stemmer := bat(\"tj_\"+ collName +\"_param\").find(\"stemmer\");\n");
    if (strcmp(background_collection,""))
    { MILPRINTF(MILOUT, "tj_setBackgroundCollName(\"%s\");\n", background_collection); }
    
    if ( return_all ) {
        MILPRINTF(MILOUT, "returnAllElements := true;\n" );
    } else {
        MILPRINTF(MILOUT, "returnAllElements := false;\n" );
    }
      
    /* reseting the files */
    command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"w");
    token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"w");
    command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");
    token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");

    if (command_file_pre == NULL) {
        LOGPRINTF(LOGFILE,"Error: cannot open pre-command file.\n");
        return 0;
    }

    if (token_file_pre == NULL) {
        LOGPRINTF(LOGFILE,"Error: cannot open pre-token file.\n");
        return 0;
    }

    if (command_file == NULL) {
        LOGPRINTF(LOGFILE,"Error: cannot open command file.\n");
        return 0;
    }

    if (token_file == NULL) {
        LOGPRINTF(LOGFILE,"Error: cannot open token file.\n");
        return 0;
    }


    fclose(command_file_pre);
    fclose(token_file_pre);
    fclose(command_file);
    fclose(token_file);

    /* specifying input and output files and result table name */

    FILE* milpre_file = NULL;
    if ( milpre_fname && !(milpre_file = fopen(myfileName(WORKDIR,milpre_fname),"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing fast mil code.\n");
        return 0;
    }

    FILE* log_file = NULL;
    if ( log_fname && !(log_file = fopen(myfileName(WORKDIR,log_fname),"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing logical plan.\n");
        return 0;
    }

    if ( milpre_file ) fclose(milpre_file);
    if ( log_file ) fclose(log_file);

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
	
    BUN bun = BUNfnd( collOptBat, (str)"stemmer" );
    char *stemmer = (char *)BUNtail( collOptBat, bun );

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
                                    milpre_fname, 
                                    log_fname, 
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

    if ( !(parserCtx->milFILE = fopen(myfileName(WORKDIR,mil_fname),"w")) ) {
        LOGPRINTF(LOGFILE,"Error: cannot find file for writing mil code.\n");
        return 0;
    }

    if (phrase_in == TRUE)
        plan_ret = SRA_to_MIL(parserCtx, 
                              query_num, 
                              txt_retr_model, 
                              img_retr_model, 
                              rel_feedback, 
                              mil_fname, 
                              "INCOMPLETE", 
                              p_command_array, 
                              TRUE);
    else
        plan_ret = SRA_to_MIL(parserCtx, 
                              query_num, 
                              txt_retr_model, 
                              img_retr_model, 
                              rel_feedback, 
                              mil_fname, 
                              "INCOMPLETE", 
                              p_command_array, 
                              FALSE);

#ifdef GENMILSTRING
    LOGPRINTF(LOGFILE,"\tGenerated MIL in string, size=%d\n",strlen(parserCtx->milBUFF));
     if ( parserCtx->milFILE ) {
       fprintf(parserCtx->milFILE,"%s",parserCtx->milBUFF);
     }
#endif
     if ( parserCtx->milFILE ) {
       fclose(parserCtx->milFILE);
     }

    if (!plan_ret) {
        LOGPRINTF(LOGFILE,"MIL query plan generation was not successful.\n");
        return 0;
    }

    /* memory cleaning */
    p_command_array = NULL;
    free(p_command_array); /* NONSENSE */
    txt_retr_model = NULL;
    img_retr_model = NULL;
    free(txt_retr_model);
    free(img_retr_model); /* NONSENSE */
    rel_feedback = NULL;
    free(rel_feedback); /* NONSENSE */

    return 1;
}

/*
 *
 *
 */

char* myfileName(char* dirName, char* fileName) {
    static char buff[FILENAME_SIZE];
    char * userName = getenv( "USER" );
    sprintf(buff,"%s/%s_%d_%s",dirName,userName,(int)getpid(),fileName);
    return &buff[0];
}
