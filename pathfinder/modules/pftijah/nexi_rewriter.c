/*

     query_rewriter.c
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Procedures for rewriting internal query representation
*/

#include <pf_config.h>

#include <monet.h>
#include <gdk.h>

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "nexi.h"
#include "nexi_rewriter.h"

#define LOGFILE   GDKout
#define LOGPRINTF if ( 0 ) stream_printf


/* function for rewriting CO queries */

int COtoCPlan(int query_num, int type, struct_RMT *txt_retr_model, struct_RF *rel_feedback) {
    (void) query_num;
    (void) rel_feedback;
    /* variables for reading parsed CO query */
    command com_var;
    char tok_var[30];

    int command_num;
    int term_num;

    /* variables for retrieval model selection and relevance feedback selection */
    bool rm_set;
    //bool rf_set;

    /* files that store parser command and token output */
    FILE *command_file_pre;
    FILE *token_file_pre;
    /* files that store command and token output afte stop words  removal and stemming */
    FILE *command_file;
    FILE *token_file;

    /* variables for stack structures */
    int term_sp;
    int com_sp;
    char term[30];
    command com_code;

    char term_lifo[STACK_MAX][TERM_LENGTH];
    command com_lifo[STACK_MAX];


    /* initialization */
    command_num = 0;
    term_num = 0;

    term_sp = 0;
    com_sp = 0;
    com_code = 0;

    /* files for rewriting CO queries */
    command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"w");
    token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"w");
    command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"r");
    token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"r");

    if (command_file_pre == NULL || token_file_pre == NULL || command_file == NULL || token_file == NULL ) {
        LOGPRINTF(LOGFILE,"Error: cannot open command or token file.\n");
        return 0;
    }


    // There are three generator types that convert CO to CAS queries
    // Example CO: information retrieval
    //  - BASIC:    //*[about(.,information retrieval)]
    //  - SIMPLE:   //article[about(.,information retrieval)]
    //  - ADVANCED: //article[about(.,information retrieval)]//*[about(.,information retrieval)]
    //
    if (type == BASIC) {

        fscanf(command_file, "%d", &com_var);
        while (!feof(command_file)) {
            if (txt_retr_model != NULL && 
                (txt_retr_model->prior_type == LENGTH_PRIOR || 
                 txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
                rm_set = TRUE;
            } else
                rm_set = FALSE;

            fprintf(command_file_pre, "%d\n", DSC);
            fprintf(command_file_pre, "%d\n", STAR);
            fprintf(command_file_pre, "%d\n", OPEN);
            fprintf(command_file_pre, "%d\n", ABOUT);
            fprintf(token_file_pre, "\"about\"\n");
            fprintf(command_file_pre, "%d\n", OB);
            fprintf(command_file_pre, "%d\n", CURRENT);
            fprintf(command_file_pre, "%d\n", COMMA);

            while (com_var != QUERY_END && !feof(command_file)) {
                fprintf(command_file_pre, "%d\n", com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
                    fscanf(token_file, "%s", tok_var);
                    fprintf(token_file_pre, "%s\n", tok_var);
                }
                fscanf(command_file, "%d", &com_var);
            }

            fprintf(command_file_pre, "%d\n", CB);
            fprintf(command_file_pre, "%d\n", CLOSE);

            if (rm_set)
                fprintf(command_file_pre, "%d\n", P_PRIOR);

            if (!feof(command_file))
                fprintf(command_file_pre, "%d\n", com_var);
            fscanf(command_file, "%d", &com_var);
        }

    } else if (type == SIMPLE) {

        fscanf(command_file, "%d", &com_var);
        while (!feof(command_file)) {
            if (txt_retr_model != NULL && 
                (txt_retr_model->prior_type == LENGTH_PRIOR || 
                 txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
                rm_set = TRUE;
            } else
                rm_set = FALSE;

            fprintf(command_file_pre, "%d\n", DSC);
            fprintf(command_file_pre, "%d\n", SELECT_NODE);
            fprintf(token_file_pre, "\"article\"\n");
            fprintf(command_file_pre, "%d\n", DSC);
            fprintf(command_file_pre, "%d\n", STAR);
            fprintf(command_file_pre, "%d\n", OPEN);
            fprintf(command_file_pre, "%d\n", ABOUT);
            fprintf(token_file_pre, "\"about\"\n");
            fprintf(command_file_pre, "%d\n", OB);
            fprintf(command_file_pre, "%d\n", CURRENT);
            fprintf(command_file_pre, "%d\n", COMMA);

            while (com_var != QUERY_END && !feof(command_file)) {
                fprintf(command_file_pre, "%d\n", com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
                    fscanf(token_file, "%s", tok_var);
                    fprintf(token_file_pre, "%s\n", tok_var);
                }
                fscanf(command_file, "%d", &com_var);
            }

            fprintf(command_file_pre, "%d\n", CB);
            fprintf(command_file_pre, "%d\n", CLOSE);

            if (rm_set)
                fprintf(command_file_pre, "%d\n", P_PRIOR);

            if (!feof(command_file))
                fprintf(command_file_pre, "%d\n", com_var);
            fscanf(command_file, "%d", &com_var);
        }

    } else if (type == ADVANCED) {

        if (txt_retr_model != NULL && 
            (txt_retr_model->prior_type == LENGTH_PRIOR ||
             txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
            rm_set = TRUE;
        } else
            rm_set = FALSE;

        fscanf(command_file, "%d", &com_var);
        while (!feof(command_file)) {

            command_num = 0;
            term_num = 0;
            term_sp = 0;
            com_sp = 0;
            com_code = 0;

            fprintf (command_file_pre, "%d\n", DSC);
            fprintf (command_file_pre, "%d\n", SELECT_NODE);
            fprintf (token_file_pre, "\"article\"\n");
            fprintf(command_file_pre, "%d\n", OPEN);
            fprintf(command_file_pre, "%d\n", ABOUT);
            fprintf(token_file_pre, "\"about\"\n");
            fprintf(command_file_pre, "%d\n", OB);
            fprintf(command_file_pre, "%d\n", CURRENT);
            fprintf(command_file_pre, "%d\n", COMMA);

            while (com_var != QUERY_END && !feof(command_file)) {
                fprintf(command_file_pre, "%d\n", com_var);

                com_sp++;
                PUSH_COM(com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
                    fscanf(token_file, "%s", tok_var);
                    fprintf(token_file_pre, "%s\n", tok_var);
                    term_sp++;
                    PUSH_TRM(tok_var);
                }
                fscanf(command_file, "%d", &com_var);
            }

            fprintf(command_file_pre, "%d\n", CB);
            fprintf(command_file_pre, "%d\n", CLOSE);

            fprintf(command_file_pre, "%d\n", DSC);
            fprintf(command_file_pre, "%d\n", STAR);
            fprintf(command_file_pre, "%d\n", OPEN);
            fprintf(command_file_pre, "%d\n", ABOUT);
            fprintf(token_file_pre, "\"about\"\n");
            fprintf(command_file_pre, "%d\n", OB);
            fprintf(command_file_pre, "%d\n", CURRENT);
            fprintf(command_file_pre, "%d\n", COMMA);

            command_num = com_sp;
            term_num = term_sp;

            com_sp = 0;
            term_sp =0;

            /* Write out the commands */
            while (com_sp < command_num) {
                com_sp++;
                POP_COM();
                fprintf(command_file_pre, "%d\n", com_code);
            }

            /* Write out the tokens */
            while (term_sp < term_num) {
                term_sp++;
                POP_TRM();
                fprintf(token_file_pre, "%s\n", term);
            }

            fprintf(command_file_pre, "%d\n", CB);
            fprintf(command_file_pre, "%d\n", CLOSE);

            if (rm_set)
                fprintf(command_file_pre, "%d\n", P_PRIOR);

            if (!feof(command_file))
                fprintf(command_file_pre, "%d\n", com_var);
            fscanf(command_file, "%d", &com_var);
        }

    } // else if ( type == ADVANCED )

    fclose(command_file_pre);
    fclose(token_file_pre);
    fclose(command_file);
    fclose(token_file);

    command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"r");
    token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"r");
    command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");
    token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");

    if (command_file_pre == NULL || token_file_pre == NULL || command_file == NULL || token_file == NULL ) {
        LOGPRINTF(LOGFILE,"Error: cannot open command or token file.\n");
        return 0;
    }

    /* Copy the contents of the token_pre to the token file */
    fscanf(token_file_pre, "%s", tok_var);
    while (!feof(token_file_pre)) {
        fprintf (token_file, "%s\n", tok_var);
        fscanf(token_file_pre, "%s", tok_var);
    }

    /* Copy the contents of the command_pre to the command file */
    fscanf(command_file_pre, "%d", &com_var);
    while (!feof(command_file_pre)) {
        fprintf (command_file, "%d\n", com_var);
        fscanf(command_file_pre, "%d", &com_var);
    }

    fclose(command_file_pre);
    fclose(token_file_pre);
    fclose(command_file);
    fclose(token_file);

    return 1;
}




/* function for rewriting CAS queries */

int CAStoCPlan(int query_num, int type, bool rm_set) {
  (void)query_num;

  /* variable for setting which terms should be recorded for insertion */
  bool record, irecord;

  /* variables for reading parsed CO query */
  command com_var, com_var_tmp;
  char tok_var[30];

  int command_num, mcommand_num, icommand_num;
  /* int term_num, mterm_num, iterm_num; */

  /* files that store parser command and token output */
  FILE *command_file_pre;
  FILE *token_file_pre;
  /* files that store command and token output afte stop words  removal and stemming */
  FILE *command_file;
  FILE *token_file;


  /* variables for stack structures */
  int term_sp, mterm_sp, iterm_sp, iterm_sp_mid;
  int com_sp, mcom_sp, icom_sp, icom_sp_mid;
  char term[30];
  command com_code;

  char term_lifo[STACK_MAX][TERM_LENGTH];
  char mterm_lifo[STACK_MAX][TERM_LENGTH];
  char iterm_lifo[STACK_MAX][TERM_LENGTH];
  command com_lifo[STACK_MAX];
  command mcom_lifo[STACK_MAX];
  command icom_lifo[STACK_MAX];

  /* initialization */
  record = FALSE;
  irecord = FALSE;

  command_num = 0;
  mcommand_num = 0;
  icommand_num =0;
  /* term_num = 0; */
  /* mterm_num = 0; */
  /* iterm_num = 0; */

  term_sp = 0;
  mterm_sp = 0;
  iterm_sp = 0;
  iterm_sp_mid = 0;
  com_sp = 0;
  mcom_sp = 0;
  icom_sp = 0;
  icom_sp_mid = 0;

  com_code = 0;

  /* files for rewriting CAS queries */
  command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"w");
  token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"w");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"r");
  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"r");

  if (command_file_pre == NULL) {
    printf("Error: cannot open CAS pre-command file.\n");
    return 0;
  }

  if (token_file_pre == NULL) {
    printf("Error: cannot open CAS pre-token file.\n");
    return 0;
  }

  if (command_file == NULL) {
    printf("Error: cannot open CAS command file.\n");
    return 0;
  }

  if (token_file == NULL) {
    printf("Error: cannot open CAS token file.\n");
    return 0;
  }

  com_var_tmp = 0;

  fscanf(command_file, "%d", &com_var);

  if (type == BASIC) {

    fscanf(token_file, "%s", tok_var);
    while (!feof(token_file)) {
      fprintf (token_file_pre, "%s\n", tok_var);
      fscanf(token_file, "%s", tok_var);
    }

    while (!feof(command_file)) {
      if (com_var == QUERY_END && rm_set)
        fprintf(command_file_pre, "%d\n", P_PRIOR);
      fprintf (command_file_pre, "%d\n", com_var);
      fscanf(command_file, "%d", &com_var);
    }

  }

  else if (type == SIMPLE) {

    while (!feof(command_file)) {

      if (com_var != QUERY_END) {

	if (com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == VAGUE) {

	  if (com_var == COMMA && (com_var_tmp == SELECT_NODE || com_var_tmp == CB)) {
	    record = TRUE;
	  }

	  else if (com_var == CB) {
	    record = FALSE;
	  }

	  else if (com_var == CLOSE && com_sp > 0) {

	    fprintf(command_file_pre, "%d\n", INTERSECT);
	    fprintf(token_file_pre, "\"and\"\n");
	    fprintf(command_file_pre, "%d\n", ABOUT);
	    fprintf(token_file_pre, "\"about\"\n");
	    fprintf(command_file_pre, "%d\n", OB);
	    fprintf(command_file_pre, "%d\n", CURRENT);
	    fprintf(command_file_pre, "%d\n", COMMA);

	    command_num = com_sp;
	    com_sp = 0;
	    /* term_num = term_sp; */
	    term_sp = 0;

	    while (com_sp < command_num) {

	      com_sp++;
	      POP_COM();
	      fprintf(command_file_pre, "%d\n", com_code);

	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION  || com_code == IMAGE_ABOUT) {
		term_sp++;
		POP_TRM();
		fprintf(token_file_pre, "%s\n", term);
	      }

	    }

	    com_sp = 0;
	    term_sp = 0;
	    record = FALSE;

	    fprintf(command_file_pre, "%d\n", CB);

	  }

	  fprintf(command_file_pre, "%d\n", com_var);

	}

	else if (com_var == QUOTE || com_var == PLUS || com_var == MINUS || com_var == MUST_NOT || com_var == MUST) {

	  if (record) {
	    com_sp++;
	    PUSH_COM(com_var);
	  }

	  fprintf (command_file_pre, "%d\n", com_var);

	}

	else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {

	  fscanf(token_file, "%s", tok_var);

	  if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION)) || (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION || com_var == IMAGE_ABOUT))) {

	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);

	  }

	  else {

	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }

	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);

	  }

	}

	else if (com_var == SELECT_NODE  || com_var == IMAGE_ABOUT) {

	  fscanf(token_file, "%s", tok_var);

	  if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR){
	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);

	  }

	  else {

	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }

	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);

	  }

	}

      }

      else {

	if (rm_set)
	  fprintf(command_file_pre, "%d\n", P_PRIOR);


	fprintf (command_file_pre, "%d\n", com_var);

      }

      com_var_tmp = com_var;
      fscanf(command_file, "%d", &com_var);

    }
  
  }

  else if (type == ADVANCED) {

    while (!feof(command_file)) {

      if (com_var != QUERY_END) {
     	
	if (com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == VAGUE) {
	  
	  if (com_var == COMMA && (com_var_tmp == SELECT_NODE || com_var_tmp == CB)) {
	    record = TRUE;
	  }
	  
	  else if (com_var == CB) {
	    record = FALSE;
	  }

	  else if (com_var == CLOSE) {

	    if (com_sp >0) {

	      mcom_sp++;
	      PUSH_MCOM(INTERSECT);
	      mterm_sp++;
	      PUSH_MTRM("\"and\"");
	      mcom_sp++;
	      PUSH_MCOM(ABOUT);
	      mterm_sp++;
	      PUSH_MTRM("\"about\"");
	      mcom_sp++;
	      PUSH_MCOM(OB);
	      mcom_sp++;
	      PUSH_MCOM(CURRENT);
	      mcom_sp++;
	      PUSH_MCOM(COMMA);

	      /*
	      fprintf(command_file_pre, "%d\n", INTERSECT);
	      fprintf(token_file_pre, "\"and\"\n");
	      fprintf(command_file_pre, "%d\n", ABOUT);
	      fprintf(token_file_pre, "\"about\"\n");
	      fprintf(command_file_pre, "%d\n", OB);
	      fprintf(command_file_pre, "%d\n", CURRENT);
	      fprintf(command_file_pre, "%d\n", COMMA);
	      */
	    
	      command_num = com_sp;
	      com_sp = 0;
	      /* term_num = term_sp; */
	      term_sp = 0;
	    
	      while (com_sp < command_num) {
	      
		com_sp++;
		POP_COM();

		mcom_sp++;
		PUSH_MCOM(com_code);

		/*
		fprintf(command_file_pre, "%d\n", com_code);
		*/

		if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT) {
		  term_sp++;
		  POP_TRM();

		  mterm_sp++;
		  PUSH_MTRM(term);
		  /*
		  fprintf(token_file_pre, "%s\n", term);
		  */

		}

	      }
	      
	      com_sp = 0;
	      term_sp = 0;
	      record = FALSE;

	      mcom_sp++;
	      PUSH_MCOM(CB);
	      /*
	      fprintf(command_file_pre, "%d\n", CB);
	      */

	    }

	    if (!irecord) {
	      icom_sp++;
	      PUSH_ICOM(END_ICOM);
     
	      icom_sp_mid = icom_sp;
	      iterm_sp_mid = iterm_sp;
	      irecord = TRUE;
	    }
	    else {
	      irecord = FALSE;
	    }

	  }
	  
	  mcom_sp++;
	  PUSH_MCOM(com_var);
	  /*
	    fprintf(command_file_pre, "%d\n", com_var);
	  */
	  
	}
	
	else if (com_var == QUOTE || com_var == PLUS || com_var == MINUS || com_var == MUST_NOT || com_var == MUST) {

	  if (record) {
	    com_sp++;
	    PUSH_COM(com_var);
	  }

	  mcom_sp++;
	  PUSH_MCOM(com_var);
	  /*
	  fprintf (command_file_pre, "%d\n", com_var);
	  */

	  icom_sp++;
	  PUSH_ICOM(com_var);

	}

	else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {
	  
	  fscanf(token_file, "%s", tok_var);
	  
	  if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION))|| (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION))) {

	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    /*
	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);
	    */

	  }
	  
	  else {
	  
	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }
	    
	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    icom_sp++;
	    PUSH_ICOM(com_var);
	    iterm_sp++;
	    PUSH_ITRM(tok_var);

	    /*
	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);	    
	    */
	  }
	  
	}

	else if (com_var == SELECT_NODE || com_var == IMAGE_ABOUT) {
	  
	  fscanf(token_file, "%s", tok_var);
	  
	  if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR){
	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    /*
	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);
	    */

	  }
	  
	  else {
	    
	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }

	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    icom_sp++;
	    PUSH_ICOM(com_var);
	    iterm_sp++;
	    PUSH_ITRM(tok_var);

	    /*
	    fprintf (command_file_pre, "%d\n", com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);
	    */

	  }
	  
	}
	
      }
      
      else {

	mcommand_num = mcom_sp;
	mcom_sp = 0;
	/* mterm_num = mterm_sp; */
	mterm_sp = 0;

	while (mcom_sp < mcommand_num) {
       
	  mcom_sp++;
	  POP_MCOM();

	  while (com_code != CLOSE) {

	    fprintf(command_file_pre, "%d\n", com_code);

	    if (com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == SELECT_NODE || com_code == IMAGE_ABOUT) {

	      mterm_sp++;
	      POP_MTRM();

	      fprintf(token_file_pre, "%s\n", term);

	    }

	    mcom_sp++;
	    POP_MCOM();

	  }


	  if (icom_sp_mid != icom_sp) {

	    fprintf(command_file_pre, "%d\n", INTERSECT);
	    fprintf(token_file_pre, "\"and\"\n");
	    fprintf(command_file_pre, "%d\n", ABOUT);
	    fprintf(token_file_pre, "\"about\"\n");
	    fprintf(command_file_pre, "%d\n", OB);
	    fprintf(command_file_pre, "%d\n", CURRENT);
	    fprintf(command_file_pre, "%d\n", COMMA);
	    
	    icommand_num = icom_sp;
	    icom_sp = icom_sp_mid;
	    /* iterm_num = iterm_sp; */
	    iterm_sp = iterm_sp_mid;

	    while (icom_sp < icommand_num) {
	      
	      icom_sp++;
	      POP_ICOM();
	      fprintf(command_file_pre, "%d\n", com_code);
	      
	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT) {
		iterm_sp++;
		POP_ITRM();
		fprintf(token_file_pre, "%s\n", term);
	      }

	    }
	    
	    fprintf(command_file_pre, "%d\n", CB);

	    fprintf(command_file_pre, "%d\n", CLOSE);

	    mcom_sp++;
	    POP_MCOM();

	    while (com_code != CLOSE) {

	      fprintf(command_file_pre, "%d\n", com_code);

	      if (com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == SELECT_NODE || com_code == IMAGE_ABOUT) {
 
		mterm_sp++;
		POP_MTRM();

		fprintf(token_file_pre, "%s\n", term);

	      }

	      mcom_sp++;
	      POP_MCOM();

	    }

	    fprintf(command_file_pre, "%d\n", INTERSECT);
	    fprintf(token_file_pre, "\"and\"\n");
	    fprintf(command_file_pre, "%d\n", ABOUT);
	    fprintf(token_file_pre, "\"about\"\n");
	    fprintf(command_file_pre, "%d\n", OB);
	    fprintf(command_file_pre, "%d\n", CURRENT);
	    fprintf(command_file_pre, "%d\n", COMMA);
	    
	    icommand_num = icom_sp_mid - 1;
	    icom_sp = 0;
	    /* iterm_num = iterm_sp_mid; */
	    iterm_sp = 0;
	    
	    while (icom_sp < icommand_num) {
	      
	      icom_sp++;
	      POP_ICOM();
	      fprintf(command_file_pre, "%d\n", com_code);
	      
	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT)  {
		iterm_sp++;
		POP_ITRM();
		fprintf(token_file_pre, "%s\n", term);
	      }

	    }

	    fprintf(command_file_pre, "%d\n", CB);

	  }

	  fprintf(command_file_pre, "%d\n", CLOSE);

	}

	icom_sp_mid = 0;
	iterm_sp_mid = 0;
	irecord = FALSE;

	term_sp = 0;
	mterm_sp = 0;
	iterm_sp = 0;
	iterm_sp_mid = 0;
	com_sp = 0;
	mcom_sp = 0;
	icom_sp = 0;
	icom_sp_mid = 0;

	if (rm_set)
	  fprintf(command_file_pre, "%d\n", P_PRIOR);


	fprintf (command_file_pre, "%d\n", QUERY_END);

      }

      com_var_tmp = com_var;
      fscanf(command_file, "%d", &com_var);

    }
  
  }

  fclose(command_file_pre);
  fclose(token_file_pre);
  fclose(command_file);
  fclose(token_file);

  command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"r");
  token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"r");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");
  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");
  
  if (command_file_pre == NULL) {
    printf("Error: cannot open CAS pre-command file.\n");
    return 0;
  }
  
  if (token_file_pre == NULL) {
    printf("Error: cannot open CAS pre-token file.\n");
    return 0;
  }
  
  if (command_file == NULL) {
    printf("Error: cannot open CAS command file.\n");
    return 0;
  }
  
  if (token_file == NULL) {
      printf("Error: cannot open CAS token file.\n");
      return 0;
  }

  
  fscanf(token_file_pre, "%s", tok_var);
  while (!feof(token_file_pre)) {
    fprintf (token_file, "%s\n", tok_var);
    fscanf(token_file_pre, "%s", tok_var);
  }

  fscanf(command_file_pre, "%d", &com_var);
  while (!feof(command_file_pre)) {
    fprintf (command_file, "%d\n", com_var);
    fscanf(command_file_pre, "%d", &com_var);
  }
  
  fclose(command_file_pre);
  fclose(token_file_pre);
  fclose(command_file);
  fclose(token_file);
  

  return 1;

}
