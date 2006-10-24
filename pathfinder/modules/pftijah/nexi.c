/*

     QPlanGen.c
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Main module for generating logical query plans and MIL query plans from NEXI queries

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

extern int old_main(int argc, char * const argv[], BAT* optbat, char* startNodes_name);

static char *dummy[] = {};

char* tijahParse(BAT* optbat, char* startNodes_name, char* query, char** errBUFF) {
  /* setup TijahParserContext structure */
  LOGPRINTF(LOGFILE,"- tijahParse([%s])\n",query);
  parserCtx->collection= "PFX";
  parserCtx->queryText = query;
  parserCtx->logFILE   = NULL;
  parserCtx->errBUFF[0]= 0;
#ifdef GENMILSTRING
  parserCtx->milBUFF[0] = 0;
#else
  ERROR: should open de miloutput file here
#endif
  /* */
  MILPRINTF(MILOUT, "#\n# Generated NEXI MIL by Pathfinder-PFTIJAH package \n#\n\n");
  MILPRINTF(MILOUT, "{\n");
  /* */
  parserCtx->tjCtx     = NULL; /* INCOMPLETE, should be filled here */
  if ( !old_main(0,dummy,optbat,startNodes_name) )  {
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

int old_main(int argc, char * const argv[], BAT* optbat, char* startNodes_name)
{
  /* input argument options */
  int arg_opt;
  bool use_startNodes = (startNodes_name != NULL);
  bool interact_set;
  bool pptype_set;
  bool ptype_set;
  bool qnum_set;
  bool language_set;
  bool algebra_set;
  bool rmtfname_set;
  bool rmifname_set;
  bool rffname_set;
  bool rewrite_set;
  bool ifilename_set;
  bool base_set;
  bool restable_set;
  bool ofilename_set;
  bool scale_on;
  bool orcomb_set;
  bool andcomb_set;

  bool query_set;

  bool phrase_in;

  /* for the type of topics and for the preprocessing */
  int topic_type;
  int preproc_type;
  int processing_type;
  int query_num;
  int query_end_num;
  int rewrite_type;
  int language_type;
  int algebra_type;
  int return_all;
  char ifile_name[FILENAME_SIZE];
  char res_table[FILENAME_SIZE];
  char rmt_fname[FILENAME_SIZE];
  char rmi_fname[FILENAME_SIZE];
  int base_type;
  char rf_fname[FILENAME_SIZE];
  char ofile_name[FILENAME_SIZE];
  /* char milpre_fname[FILENAME_SIZE]; */
  char *milpre_fname = NULL;
  /* char log_fname[FILENAME_SIZE]; */
  char *log_fname = NULL;
  char mil_fname[FILENAME_SIZE];
  char background_collection[20] = "";
  /* pointer to SRA command tree structure */
  command_tree **p_command_array;

  /* for the retrieval models and for the relevance feedback */
  int qnum_tmp;
/*
  int rmt_number;
  int rmt_qnumber;
  unsigned int rmt_model;
  unsigned int rmt_or_comb;
  unsigned int rmt_and_comb;
  unsigned int rmt_up_prop;
  unsigned int rmt_down_prop;
  char rmt_e_class[TERM_LENGTH];
  char rmt_exp_class[TERM_LENGTH];
  bool rmt_stemming;
  unsigned int rmt_size_type;
  float rmt_param1;
  float rmt_param2;
  int rmt_param3;
  int rmt_ptype;
  int rmt_psize;
  char rmt_context[TERM_LENGTH];
  float rmt_extra;*/

  int rmi_number;
  int rmi_qnumber;
  int rmi_model;
  char rmi_descriptor[TERM_LENGTH];
  char rmi_attr_name[TERM_LENGTH];
  int rmi_computation;

  int rf_number;
  int rf_qnumber;
  int rf_rtype;
  int rf_psize;
  char rf_jname[NAME_LENGTH];
  char rf_e1name[NAME_LENGTH];
  char rf_e2name[NAME_LENGTH];
  char rf_e3name[NAME_LENGTH];

  char query_text[QUERY_SIZE];

  /* returned value of the finction that generates plans */
  int plan_ret;

  /* retrieval model and relevance feedback variables */
  struct_RMT *txt_retr_model;
  /* struct_RMI *img_retr_model , *img_retr_model1 ; */
  struct_RMI *img_retr_model;
  struct_RF *rel_feedback, *rel_feedback1;;

  /* files that store parser command and token output */
  FILE *command_file_pre;
  FILE *token_file_pre;
  /* files that store command and token output afte stop words  removal and stemming */
  FILE *command_file;
  FILE *token_file;
  /* files for retrieval model and relevance feedback */
  //FILE *rmt_file;
  FILE *rmi_file;
  FILE *rf_file;

  /* input argument options */
  interact_set = FALSE;
  pptype_set = FALSE;
  ptype_set = FALSE;
  qnum_set = FALSE;
  language_set = FALSE;
  algebra_set = FALSE;
  rmtfname_set = FALSE;
  rmifname_set = FALSE;
  rffname_set = FALSE;
  rewrite_set = FALSE;
  ifilename_set = FALSE;
  base_set = FALSE;
  restable_set = FALSE;
  ifilename_set = FALSE;
  ofilename_set = FALSE;
  query_set = FALSE;
  scale_on = TRUE;
  orcomb_set = FALSE;
  andcomb_set = FALSE;
  
    /* structure initialization */
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
    pptype_set          = TRUE;
    scale_on            = FALSE;
    processing_type     = NO_STOP_STEM;
    rewrite_type        = BASIC;
    language_type       = ENGLISH;
    base_type           = ONE;
    return_all          = FALSE;
    
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

        if ( 1 ) {
            FILE* xx = fopen("/tmp/TijahOptions.log","a");
            fprintf(xx,"TijahOptions: handle: %s=%s\n",optName,optVal);
            fclose(xx);
        }
        
        if ( strcmp(optName,"debug") == 0 ) {
	    if ( 0 ) {
	        /* set in serialize options for now, is earlier */
	        int v = atoi(optVal);
	        SET_TDEBUG(v);
	        if (TDEBUG(1)) stream_printf(GDKout,"# old_main: setting debug value to %d.\n",v);
	    }
	} else if ( strcmp(optName,"collection") == 0 ) {
            parserCtx->collection = optVal;
            
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
            orcomb_set = TRUE;
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
            andcomb_set = TRUE;
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
            base_set = TRUE;
            
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
    
    
    // Some special cases for NLLR, since:
    //  - NLLR is log-based (therefore requiring different combination and propagation operators)
    //  - NLLR only works with COARSE2 at the moment
    if ( txt_retr_model->model == MODEL_NLLR ) {
        // Switch to COARSE2 algebra for NLLR
        algebra_type = COARSE2;

        if ( !orcomb_set ) 
            txt_retr_model->or_comb = OR_MAX;
        
        if ( !andcomb_set )
            txt_retr_model->and_comb = AND_SUM;
            
        if ( !base_set ) 
            base_type           = ONE;
        
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
    
  /*
   * Original option handling code. Comment out cases that have been implemented above.
   */

  while(1){

    arg_opt = getopt(argc, argv, "q:n:p:s:l:a:u:m:g:b:f:r:o:ihv");

    if (arg_opt == -1)
      break;

    switch(arg_opt) {

    case 'q':

      strncpy(query_text,optarg,strlen(optarg));
      query_set = TRUE;

      break;

    case 'i' :
      interact_set = TRUE;
      break;

    case 'n' :

      query_num = atoi(optarg);
      qnum_set = TRUE;
      break;
/*
    case 'p':

      if (strcmp(optarg,"plain") == 0 || strcmp(optarg,"PLAIN") == 0) {
        preproc_type = PLAIN;
	scale_on = FALSE;
	pptype_set = TRUE;
      }
      else if (strcmp(optarg,"no_modifier") == 0 || strcmp(optarg,"NO_MODIFIER") == 0) {
	preproc_type = NO_MODIFIER;
	scale_on = FALSE;
	pptype_set = TRUE;
      }
      else if (strcmp(optarg,"vague_no_phrase") == 0 || strcmp(optarg,"VAGUE_NO_PHRASE") == 0) {
	preproc_type = VAGUE_NO_PHRASE;
	scale_on = TRUE;
	pptype_set = TRUE;
      }
      else if (strcmp(optarg,"strict_no_phrase") == 0 || strcmp(optarg,"STRICT_NO_PHRASE") == 0) {
	preproc_type = STRICT_NO_PHRASE;
	scale_on = TRUE;
	pptype_set = TRUE;
      }
      else if (strcmp(optarg,"vague") == 0 || strcmp(optarg,"VAGUE") == 0) {
	preproc_type = VAGUE_MODIF;
	scale_on = TRUE;
	pptype_set = TRUE;
      }
      else if (strcmp(optarg,"strict") == 0 || strcmp(optarg,"STRICT") == 0) {
	preproc_type = STRICT_MODIF;
	scale_on = TRUE;
	pptype_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect preprocessing type.\n");
	pptype_set = FALSE;
	return 0;
      }
      break;
*/
    case 's' :

      if (strcmp(optarg,"no") == 0 || strcmp(optarg,"NO") == 0) {
        processing_type = NO_STOP_STEM;
	ptype_set = TRUE;
      }
      else if (strcmp(optarg,"stop") == 0 || strcmp(optarg,"STOP") == 0) {
	processing_type = STOP_WORD;
	ptype_set = TRUE;
      }
      else if (strcmp(optarg,"stem") == 0 || strcmp(optarg,"STEM") == 0) {
	processing_type = STEMMING;
	ptype_set = TRUE;
      }
      else if (strcmp(optarg,"stop_stem") == 0 || strcmp(optarg,"STOP_STEM") == 0) {
	processing_type = STOP_STEM;
	ptype_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect processing type\n");
	ptype_set = FALSE;
      }
      break;

    case 'l':

      if (strcmp(optarg,"english") == 0 || strcmp(optarg,"ENGLISH") == 0) {
        language_type = ENGLISH;
	language_set = TRUE;
      }
      else if (strcmp(optarg,"dutch") == 0 || strcmp(optarg,"DUTCH") == 0) {
	language_type = DUTCH;
	language_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect language set\n");
	language_set = FALSE;
      }
      break;
/*
    case 'a':

      if (strcmp(optarg,"aspect") == 0 || strcmp(optarg,"ASPECT") == 0) {
        algebra_type = ASPECT;
	algebra_set = TRUE;
      }
      else if (strcmp(optarg,"coarse") == 0 || strcmp(optarg,"COARSE") == 0) {
	algebra_type = COARSE;
	algebra_set = TRUE;
      }
      else if (strcmp(optarg,"coarse2") == 0 || strcmp(optarg,"COARSE2") == 0) {
         algebra_type = COARSE2;
	 algebra_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect algebra type\n");
	algebra_set = FALSE;
      }

      break;*/
    case 'u' :

      strncpy(rmt_fname,optarg,FILENAME_SIZE);

      rmtfname_set = TRUE;

      break;

    case 'm' :

      strncpy(rmi_fname,optarg,FILENAME_SIZE);

      rmifname_set = TRUE;

      break;
/*
    case 'g' :

      if (strcmp(optarg,"basic") == 0 || strcmp(optarg,"BASIC") == 0) {
        rewrite_type = BASIC;
	rewrite_set = TRUE;
      }
      else if (strcmp(optarg,"simple") == 0 || strcmp(optarg,"SIMPLE") == 0) {
	rewrite_type = SIMPLE;
	rewrite_set = TRUE;
      }
      else if (strcmp(optarg,"advanced") == 0 || strcmp(optarg,"ADVANCED") == 0) {
	rewrite_type = ADVANCED;
	rewrite_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect generator type\n");
	rewrite_set = FALSE;
      }

      break;
*/
/*    case 'b':

      if (strcmp(optarg,"zero") == 0 || strcmp(optarg,"ZERO") == 0) {
        base_type = ZERO;
	base_set = TRUE;
      }
      else if (strcmp(optarg,"one") == 0 || strcmp(optarg,"ONE") == 0) {
	base_type = ONE;
	base_set = TRUE;
      }
      else {
        LOGPRINTF(LOGFILE,"Incorrect base set\n");
	base_set = FALSE;
      }

      break;
*/
    case 'f':

      strncpy(rf_fname,optarg,FILENAME_SIZE);

      rffname_set = TRUE;

      break;

    case 'r' :

      strncpy(res_table,optarg,FILENAME_SIZE);
      restable_set = TRUE;
      break;

    case 'o' :

      strncpy(ofile_name,optarg,FILENAME_SIZE);
      ofilename_set = TRUE;
      break;

    case 'h' :

      fprintf (stderr,
	  "     Generates SRA (logical), MIL, plan by processing NEXI queries.\n"
	  "Usage: %s [-h] [-v] [-i] [-q <query>] [-t <topic type>] [-n <start topic number>] [-p <preprocessing type>] [-s <stopword-stem>] [-l <language>] [-a <algebra type>] [-u <text retrieval model>] [-u <image retrieval model>] [-g <generator type>] [-b <default score>] [-f <relevance feedback>] [-r <result table>] [-o <output file>] [<input file>]\n"
	  "    -h: help\n"
          "    -i: interactive mode - asks for processing details.\n"
	  "    -q: specifies the <query> for processing.\n"
	  "    -n: specifies the <start topic number> (default 1).\n"
          "    -p: specifies the <preprocessing type> (default PLAIN).\n\t Options <preprocessing type> : = { plain|PLAIN, no_modifier|NO_MODIFIER, vague_no_phrase|VAGUE_NO_PHRASE, strict_no_phrase|STRICT_NO_PHRASE, vague|VAGUE, strict|STRICT }.\n"
	  "    -s: specifies if stop word removal and|or stemming should be performed (default NO).\n\t Options <stopword_stem> := { no|NO, stop|STOP, stem|STEM, stop_stem|STOP_STEM }.\n"
	  "    -l: specifies the language used for querying (default ENGLISH).\n\t Options <language> := { english|ENGLISH, dutch|DUTCH }.\n"
	  "    -a: specifies the algebra type to be used when generating query plans (defaul ASPECT).\n\t Options <algebra type> := {aspect|ASPECT, coarse|COARSE|coarse2|COARSE2 }.\n"
	  "    -u: specifies the file name where text retrieval model parameters are stored.\n\t Format \"<text retrieval model>\" := <record_num> {<query_num> <model> <or_type> <and_type> <up_type> <down_type> <e_class> <stem> <size_type> <param_1> < param_2> <param_3> <l_type> <l_size>}*record_num,\n\t\t see text_retrieval_model.txt file for details.\n"
	  "    -m: specifies the file name where image retrieval model parameters are stored.\n\t Format \"<image retrieval model>\" := <record_num> {<query_num> <model> <descriptor> <attr_name>}*record_num,\n\t\t see image_retrieval_model.txt file for details.\n"
	  "    -g: specifies the <generator type>. The exact implementation of these generators depend on the topic type, i.e., CO or CAS (default BASIC).\n\t Options <generator type> := { basic|BASIC, simple|SIMPLE, advanced|ADVANCED }\n\t For CO: \n\t\t 1. basic := //*[about(.,CO)];\n\t\t 2. simple := //article//*[about(.,CO)];\n\t\t 3. advanced := //article[about(.,CO)]//*[about(.,CO)];\n\t For CAS: \n\t\t 1. basic := CAS;\n\t\t 2. simple := All terms in each subquery are expanded to the context element of that subquery;\n\t\t 3. advanced := simple + All terms from both subqueries are expanded to context elements of other subqueries;\n"
	  "    -b: specifies the default score for regions (default ONE).\n\t Options <default score> := { zero|ZERO, one|ONE }.\n"
	  "    -f: OBSOLETE - specifies the file name where <relevance feedback> is stored.\n\t Format \"<relevance feedback>\" := <record_num> {<query_num> <rf_type> <prior_size> <journal_name> <element_name>*3}*record_num,\n\t\t where <rf_type> := { 2(JOURNAL) | 3(ELEMENT) | 4(SIZE) | 5(JOURNAL_ELEMENT) | 6(JOURNAL_SIZE) | 7(ELEMENT_SIZE) | 8(ALL) } (if element names 2 and 3 are not given default \"EMPTY\" values must be stated in the file.\n"
	  "    -r: specifies the name of the MIL <result table> (default \"topic\").\n"
	  "    -o: specifies the <output file> name without extensions for SRA, MIL, and SQL plans (default \"query_file\").\n"
	  "    <input file> specifies the name of an input file (default \"file_query.nxi\").\n"
	  "\n",
          argv[0]);

      exit (EXIT_FAILURE);
      break;

    }

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
  if (!interact_set) {

    LOGPRINTF(LOGFILE,"  The query plans will be generated with default values (if not specified otherwise):\n");

    if (!ifilename_set) {

      strcpy(ifile_name,"file_query.nxi");

    }
    LOGPRINTF(LOGFILE,"\tInput file name is %s.\n",ifile_name);

    if(!qnum_set) {
      query_num = 1;
      LOGPRINTF(LOGFILE,"\tStart topic number is 1.\n");
    }
/*
    if (!pptype_set) {
      preproc_type = PLAIN;
      pptype_set = TRUE;
      scale_on = FALSE;
      LOGPRINTF(LOGFILE,"\tPreprocessing type is plain - removes phrases and modifiers.\n");
    }
*/
    if(!ptype_set) {
      processing_type = NO_STOP_STEM;
      LOGPRINTF(LOGFILE,"\tProcessing does not perform stop word removal and stemming.\n");
    }
/*
    if(!rewrite_set) {
      rewrite_type = BASIC;
      LOGPRINTF(LOGFILE,"\tRewriting type is \"BASIC\".\n");
    }
*/
    if(!language_set) {
       language_type = ENGLISH;
       LOGPRINTF(LOGFILE,"\tLanguage is English.\n");
    }

    /*
    if(!algebra_set) {
       algebra_type = ASPECT;
       LOGPRINTF(LOGFILE,"\tAlgebra type is \"ASPECT\".\n");
    }*/
/*
    if(!base_set) {
       base_type = ONE;
       LOGPRINTF(LOGFILE,"\tDefault region score is 1.0.\n");
    }
*/
    if (!restable_set) {
      strcpy(res_table, "topic");
      LOGPRINTF(LOGFILE,"\tResult MIL table name is \"topic\".\n");
    }

    if(!ofilename_set) {
      strcpy(ofile_name,"query_plan");
      LOGPRINTF(LOGFILE,"\tOutput files for query plans are \"query_plan_pre.mil\", \"query_plan.sra\", \"query_plan.mil\".\n");
    }

    /* strcat(strcpy(milpre_fname, ofile_name),"_pre.mil"); */
    /* strcat(strcpy(log_fname, ofile_name),".sra"); */
    strcat(strcpy(mil_fname, ofile_name),".mil");
  }

  else {

    /* interactive mode */

    /* specifying output file name */
    if (!ifilename_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the input file name:\t");
      scanf("%s",ifile_name);
      ifilename_set = TRUE;
    }

    /* specifying start topic number */
    if (!qnum_set) {
      LOGPRINTF(LOGFILE,"\nInsert (start) topic number:\t");
      scanf("%d", &query_num);
      qnum_set = TRUE;
    }

    /* specifying preprocessing type */
    if (!pptype_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the type of preprocessing you want:\n\t1. removing phrases and modifiers\n\t2. modifiers removal\n\t3. vague modifiers and phrase removal\n\t4. strict modifiers and phrase removal\n\t5. vague modifiers\n\t6. strict modifiers\n");
      scanf("%d", &preproc_type);
      if (preproc_type < 3)
        scale_on = FALSE;
      else
        scale_on = TRUE;
      pptype_set = TRUE;
    }

    /* specifying processing type */
    if (!ptype_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the type of processing you want:\n\t1. withou any\n\t2. stop words removal\n\t3. stemming\n\t4. stop words removal and stemming\n");
      scanf("%d", &processing_type);
      ptype_set = TRUE;
    }

    /* specifying language type */
    if (!language_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the language used for querying:\n\t1. English\n\t2. Dutch\n");
      scanf("%d", &language_type);
      language_set = TRUE;
    }

    /* specifying algebra type */
    if (!algebra_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the algebra type used for generating query plans:\n\t1. Aspect (with clear separation of four structured retrieval aspects)\n\t2. Coarse (with the high level \"about\" operator)\n");
      scanf("%d", &algebra_type);
      algebra_set = TRUE;
    }

    /* specifying text retrieval model file name */
    if (!rmtfname_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the text retrieval model file name (s to skip):\t");
      scanf("%s",rmt_fname);
      if (strcmp(rmt_fname,"s"))
	rmtfname_set = TRUE;
    }

    /* specifying image retrieval model file name */
    if (!rmifname_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the image retrieval model file name (s to skip):\t");
      scanf("%s",rmi_fname);
      if (strcmp(rmi_fname,"s"))
	rmifname_set = TRUE;
    }

    /* specifying rewriting type */
    if (!rewrite_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the type of query rewriting you want:\n\t1. basic\n\t2. simple\n\t3. advanced\n   For CO: \n\t 1. basic := //*[about(.,CO)];\n\t 2. simple := //article//*[about(.,CO)];\n\t 3. advanced := //article[about(.,CO)]//*[about(.,CO)];\n   For CAS:\n\t 1. basic := CAS;\n\t 2. simple := All terms in each subquery are expanded to the context element of that subquery;\n\t 3. advanced := simple + All terms from both subqueries are expanded to context elements of other subqueries;\n");
      scanf("%d", &rewrite_type);
      rewrite_set = TRUE;
    }

    /* specifying default region score */
    if (!base_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the default region score:\n\t1. 0.0\n\t2. 1.1\n");
      scanf("%d", &base_type);
      base_set = TRUE;
    }

    /* specifying relevance feedback file name */
    if (!rffname_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the relevance feedback file name (s to skip):\t");
      scanf("%s",rf_fname);
      if (strcmp(rf_fname,"s"))
	rffname_set = TRUE;
    }

    /* specifying the name of result MIL table */
    if (!restable_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the name of the MIL result table:\t");
      scanf("%s",res_table);
      restable_set = TRUE;
    }
    
    /* specifying output file name */
    if (!ofilename_set) {
      LOGPRINTF(LOGFILE,"\nSpecify the output file name without extensions for SRA and MIL query plan:\t");
      scanf("%s",ofile_name);
      strcat(strcpy(mil_fname, ofile_name),".mil");
      /* strcat(strcpy(log_fname, ofile_name),".log"); */
      ofilename_set = TRUE;
    }

  }

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

  /*
  Perform parsing of NEXI query
  */
  plan_ret = parseNEXI(parserCtx,&query_end_num);

  if (!plan_ret)
    return 0;
  else {
    topic_type = plan_ret;
  }
  /* specifying retrieval models */
/*
  txt_retr_model1 = txt_retr_model;
  img_retr_model1 = img_retr_model;

  if (rmtfname_set) {

    rmt_file = fopen(myfileName(WORKDIR,rmt_fname),"r");

    if (rmt_file == NULL) {
      LOGPRINTF(LOGFILE,"Error: cannot find file with text retrieval model specification.\n");
      return 0;
    }

    fscanf(rmt_file,"%d",&rmt_number);

    if (rmt_number == 1) {

      fscanf(rmt_file, "%d", &rmt_qnumber);
      fscanf(rmt_file, "%d", &rmt_model);
      fscanf(rmt_file, "%d", &rmt_or_comb);
      fscanf(rmt_file, "%d", &rmt_and_comb);
      fscanf(rmt_file, "%d", &rmt_up_prop);
      fscanf(rmt_file, "%d", &rmt_down_prop);
      fscanf(rmt_file, "%s", rmt_e_class);
      fscanf(rmt_file, "%s", rmt_exp_class);
      fscanf(rmt_file, "%d", &rmt_stemming);
      fscanf(rmt_file, "%d", &rmt_size_type);
      fscanf(rmt_file, "%f", &rmt_param1);
      fscanf(rmt_file, "%f", &rmt_param2);
      fscanf(rmt_file, "%d", &rmt_param3);
      fscanf(rmt_file, "%d", &rmt_ptype);
      fscanf(rmt_file, "%d", &rmt_psize);
      fscanf(rmt_file, "%s", rmt_context);
      fscanf(rmt_file, "%f", &rmt_extra);

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

	txt_retr_model->qnumber = qnum_tmp;
	txt_retr_model->model = rmt_model;
	txt_retr_model->or_comb = rmt_or_comb;
	txt_retr_model->and_comb = rmt_and_comb;
	txt_retr_model->up_prop = rmt_up_prop;
	txt_retr_model->down_prop = rmt_down_prop;
	strcpy(txt_retr_model->e_class, rmt_e_class);
	strcpy(txt_retr_model->exp_class, rmt_exp_class);
	txt_retr_model->stemming = rmt_stemming;
	txt_retr_model->size_type = rmt_size_type;
	txt_retr_model->param1 = rmt_param1;
	txt_retr_model->param2 = rmt_param2;
	txt_retr_model->param3 = rmt_param3;
	txt_retr_model->prior_type = rmt_ptype;
	txt_retr_model->prior_size = rmt_psize;
	strcpy(txt_retr_model->context, rmt_context);
	txt_retr_model->extra = rmt_extra;
	++txt_retr_model;
	txt_retr_model->next = txt_retr_model;

      }

    }

    else if (rmt_number == query_num + query_end_num - 1) {

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

        fscanf(rmt_file, "%d", &rmt_qnumber);
        fscanf(rmt_file, "%d", &rmt_model);
        fscanf(rmt_file, "%d", &rmt_or_comb);
        fscanf(rmt_file, "%d", &rmt_and_comb);
        fscanf(rmt_file, "%d", &rmt_up_prop);
        fscanf(rmt_file, "%d", &rmt_down_prop);
        fscanf(rmt_file, "%s", rmt_e_class);
	fscanf(rmt_file, "%s", rmt_exp_class);
        fscanf(rmt_file, "%d", &rmt_stemming);
        fscanf(rmt_file, "%d", &rmt_size_type);
        fscanf(rmt_file, "%f", &rmt_param1);
        fscanf(rmt_file, "%f", &rmt_param2);
        fscanf(rmt_file, "%d", &rmt_param3);
        fscanf(rmt_file, "%d", &rmt_ptype);
        fscanf(rmt_file, "%d", &rmt_psize);
        fscanf(rmt_file, "%s", rmt_context);
        fscanf(rmt_file, "%f", &rmt_extra);

        txt_retr_model->qnumber = qnum_tmp;
	txt_retr_model->model = rmt_model;
	txt_retr_model->or_comb = rmt_or_comb;
	txt_retr_model->and_comb = rmt_and_comb;
	txt_retr_model->up_prop = rmt_up_prop;
	txt_retr_model->down_prop = rmt_down_prop;
	strcpy(txt_retr_model->e_class, rmt_e_class);
	strcpy(txt_retr_model->exp_class, rmt_exp_class);
	txt_retr_model->stemming = rmt_stemming;
	txt_retr_model->size_type = rmt_size_type;
	txt_retr_model->param1 = rmt_param1;
	txt_retr_model->param2 = rmt_param2;
	txt_retr_model->param3 = rmt_param3;
	txt_retr_model->prior_type = rmt_ptype;
	txt_retr_model->prior_size = rmt_psize;
	strcpy(txt_retr_model->context, rmt_context);
	txt_retr_model->extra = rmt_extra;
	++txt_retr_model;
	txt_retr_model->next = txt_retr_model;
      }

    }

    fclose(rmt_file);

  }

  else {

    for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

      txt_retr_model->qnumber = qnum_tmp;
      txt_retr_model->model = MODEL_LMS;
      txt_retr_model->or_comb = OR_SUM;
      txt_retr_model->and_comb = AND_PROD;
      txt_retr_model->up_prop = UP_WSUMD;
      txt_retr_model->down_prop = DOWN_SUM;
      strcpy(txt_retr_model->e_class, "TRUE");
      strcpy(txt_retr_model->e_class, "FALSE");
      txt_retr_model->stemming = TRUE;
      txt_retr_model->size_type = SIZE_TERM;
      txt_retr_model->param1 = 0.8;
      txt_retr_model->param2 = 0.5;
      txt_retr_model->param3 = 0;
      txt_retr_model->prior_type = NO_PRIOR;
      txt_retr_model->prior_size = 0;
      strcpy(txt_retr_model->context, "");
      txt_retr_model->extra = 0.0;
      ++txt_retr_model;
      txt_retr_model->next = txt_retr_model;

    }

  }

  txt_retr_model = NULL;
  txt_retr_model = txt_retr_model1;*/

  if (rmifname_set) {
 LOGPRINTF(LOGFILE,"%s\n",rmi_fname);
    rmi_file = fopen(myfileName(WORKDIR,rmi_fname),"r");

    if (rmi_file == NULL) {
      LOGPRINTF(LOGFILE,"Error: cannot find file with image retrieval model specification.\n");
      return 0;
    }

    fscanf(rmi_file,"%d",&rmi_number);

    if (rmi_number == 1) {

      fscanf(rmi_file, "%d", &rmi_qnumber);
      fscanf(rmi_file, "%d", &rmi_model);
      fscanf(rmi_file, "%s", rmi_descriptor);
      fscanf(rmi_file, "%s", rmi_attr_name);
      fscanf(rmi_file, "%d", &rmi_computation);

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

	img_retr_model->qnumber = qnum_tmp;
	img_retr_model->model = rmi_model;
	strcpy(img_retr_model->descriptor, rmi_descriptor);
	strcpy(img_retr_model->attr_name, rmi_attr_name);
	img_retr_model->computation = rmi_computation;
	++img_retr_model;
	img_retr_model->next = img_retr_model;

      }

    }

    else if (rmi_number == query_num + query_end_num - 1) {

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

        fscanf(rmi_file, "%d", &rmi_qnumber);
        fscanf(rmi_file, "%d", &rmi_model);
        fscanf(rmi_file, "%s", rmi_descriptor);
        fscanf(rmi_file, "%s", rmi_attr_name);
	fscanf(rmi_file, "%d", &rmi_computation);

        img_retr_model->qnumber = qnum_tmp;
	img_retr_model->model = rmi_model;
	strcpy(img_retr_model->descriptor, rmi_descriptor);
	strcpy(img_retr_model->attr_name, rmi_attr_name);
	img_retr_model->computation = rmi_computation;
	++img_retr_model;
	img_retr_model->next = img_retr_model;

      }

    }

    fclose(rmi_file);

  }

  else {

    for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

        img_retr_model->qnumber = qnum_tmp;
	img_retr_model->model = MODEL_I_GMM;
	strcpy(img_retr_model->descriptor, "INEX05_LP");
	strcpy(img_retr_model->attr_name, "filename");
	img_retr_model->computation = IMAGE_AVG;
	++img_retr_model;
	img_retr_model->next = img_retr_model;

    }

  }

  img_retr_model = NULL;
 /*img_retr_model = img_retr_model1;*/


  /*

  else
    retr_model = NULL;

  */

  /* specifying relevance feedback model */
  if (rffname_set) {

    rf_file = fopen(myfileName(WORKDIR,rf_fname),"r");

    if (rf_file == NULL) {
      LOGPRINTF(LOGFILE,"Error: cannot find file with relevance feedback model specification.\n");
      return 0;
    }

    fscanf(rf_file,"%d",&rf_number);

    rel_feedback1 = rel_feedback;

    if (rf_number == 1) {

      fscanf(rf_file, "%d", &rf_qnumber);
      fscanf(rf_file, "%d", &rf_rtype);
      fscanf(rf_file, "%d", &rf_psize);
      fscanf(rf_file, "%s", rf_jname);
      fscanf(rf_file, "%s", rf_e1name);
      fscanf(rf_file, "%s", rf_e2name);
      fscanf(rf_file, "%s", rf_e3name);

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

	rel_feedback->qnumber = qnum_tmp;
	rel_feedback->rf_type = rf_rtype;
	rel_feedback->prior_size = rf_psize;
	strcpy(rel_feedback->journal_name, rf_jname);
	strcpy(rel_feedback->elem1_name, rf_e1name);
	strcpy(rel_feedback->elem2_name, rf_e2name);
	strcpy(rel_feedback->elem3_name, rf_e3name);
	++rel_feedback;
	rel_feedback->next = rel_feedback;

      }

    }

    else if (rf_number == query_num + query_end_num - 1) {

      for (qnum_tmp = query_num;  qnum_tmp < query_num + query_end_num; qnum_tmp++) {

	fscanf(rf_file, "%d", &rf_qnumber);
	fscanf(rf_file, "%d", &rf_rtype);
	fscanf(rf_file, "%d", &rf_psize);
	fscanf(rf_file, "%s", rf_jname);
	fscanf(rf_file, "%s", rf_e1name);
	fscanf(rf_file, "%s", rf_e2name);
	fscanf(rf_file, "%s", rf_e3name);

	rel_feedback->qnumber = qnum_tmp;
	rel_feedback->rf_type = rf_rtype;
	rel_feedback->prior_size = rf_psize;
	strcpy(rel_feedback->journal_name, rf_jname);
	strcpy(rel_feedback->elem1_name, rf_e1name);
	strcpy(rel_feedback->elem2_name, rf_e2name);
	strcpy(rel_feedback->elem3_name, rf_e3name);
	++rel_feedback;
	rel_feedback->next = rel_feedback;

      }

    }

    rel_feedback = NULL;
    rel_feedback = rel_feedback1;

     fclose(rf_file);

  }

  else
    rel_feedback = NULL;


  /* preprocessing original query plans */

  if (pptype_set) {

    plan_ret = preprocess(preproc_type);

    if (!plan_ret) {
      LOGPRINTF(LOGFILE,"Preprocessing was not successful.\n");
      return 0;
    }

  }

  /* processing original query plans: stemming and stop word removal */

  plan_ret = process(processing_type, language_type);

  if (!plan_ret) {
    LOGPRINTF(LOGFILE,"Processing was not successful.\n");
    return 0;
   }


  /* rewriting CO and CAS queries into internal representation of queries */
  if (topic_type == CO_TOPIC) {

     plan_ret = COtoCPlan(query_num, rewrite_type, txt_retr_model, rel_feedback);

  }
  else if (topic_type == CAS_TOPIC) {

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
  p_command_array = CAS_plan_gen(query_num, topic_type, txt_retr_model, rel_feedback, algebra_type, milpre_fname, log_fname, res_table, scale_on);

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
    plan_ret = SRA_to_MIL(parserCtx, query_num, txt_retr_model, img_retr_model, rel_feedback, mil_fname, "INCOMPLETE", p_command_array, TRUE);
  else
    plan_ret = SRA_to_MIL(parserCtx, query_num, txt_retr_model, img_retr_model, rel_feedback, mil_fname, "INCOMPLETE", p_command_array, FALSE);

#ifdef GENMILSTRING
  LOGPRINTF(LOGFILE,"\tGenerated MIL in string, size=%d\n",strlen(parserCtx->milBUFF));
  fprintf(parserCtx->milFILE,"%s",parserCtx->milBUFF);
#endif
  fclose(parserCtx->milFILE);

  if (!plan_ret) {
    LOGPRINTF(LOGFILE,"MIL query plan generation was not successful.\n");
    return 0;
  }

  if (!plan_ret) {
    LOGPRINTF(LOGFILE,"Test SQL query plan generation was not successful.\n");
    return 0;
  }

  /* memory cleaning */
  p_command_array = NULL;
  free(p_command_array);
  txt_retr_model = NULL;
  img_retr_model = NULL;
  free(txt_retr_model);
  free(img_retr_model);
  rel_feedback = NULL;
  free(rel_feedback);

  (void)query_set;

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
