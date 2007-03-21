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
 * Procedures for rewriting internal query representation
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
    // char tok_var[30];
    char* tok_var;

    int command_num;
    int term_num;

    /* variables for retrieval model selection and relevance feedback selection */
    bool rm_set;
    //bool rf_set;

    /* files that store parser command and token output */
    int icp = 0;
    TijahNumberList* compre = &parserCtx->command_preLIST;
    int icm = 0;
    TijahNumberList* commain = &parserCtx->commandLIST;
    int itp = 0;
    TijahStringList* tokpre = &parserCtx->token_preLIST;
    int itm = 0;
    TijahStringList* tokmain = &parserCtx->tokenLIST;

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
    tnl_clear(compre); /* make the command pre list empty again, WHY Vojkan */

    icm = 0;

    // There are three generator types that convert CO to CAS queries
    // Example CO: information retrieval
    //  - BASIC:    //*[about(.,information retrieval)]
    //  - SIMPLE:   //article[about(.,information retrieval)]
    //  - ADVANCED: //article[about(.,information retrieval)]//*[about(.,information retrieval)]
    //
    if (type == BASIC) {

	com_var = commain->val[icm++];
	while ( icm<=commain->cnt ) {
            if (txt_retr_model != NULL && 
                (txt_retr_model->prior_type == LENGTH_PRIOR || 
                 txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
                rm_set = TRUE;
            } else
                rm_set = FALSE;

            tnl_append(compre, DSC);
            tnl_append(compre, STAR);
            tnl_append(compre, OPEN);
            tnl_append(compre, ABOUT);
            tsl_append(tokpre, "\"about\"");
            tnl_append(compre, OB);
            tnl_append(compre, CURRENT);
            tnl_append(compre, COMMA);

            while (com_var != QUERY_END && icm<=commain->cnt ) {
                tnl_append(compre, com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
		    tok_var = tokmain->val[itm++];
                    tsl_append(tokpre,tok_var);
                }
		com_var = commain->val[icm++];
            }

            tnl_append(compre, CB);
            tnl_append(compre, CLOSE);

            if (rm_set)
                tnl_append(compre, P_PRIOR);

            if ( icm<=commain->cnt )
                tnl_append(compre, com_var);
	    com_var = commain->val[icm++];
        }

    } else if (type == SIMPLE) {

	com_var = commain->val[icm++];
        while (icm<=commain->cnt) {
            if (txt_retr_model != NULL && 
                (txt_retr_model->prior_type == LENGTH_PRIOR || 
                 txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
                rm_set = TRUE;
            } else
                rm_set = FALSE;

            tnl_append(compre, DSC);
            tnl_append(compre, SELECT_NODE);
            tsl_append(tokpre, "\"article\"");
            tnl_append(compre, DSC);
            tnl_append(compre, STAR);
            tnl_append(compre, OPEN);
            tnl_append(compre, ABOUT);
            tsl_append(tokpre,"\"about\"");
            tnl_append(compre, OB);
            tnl_append(compre, CURRENT);
            tnl_append(compre, COMMA);

            while (com_var != QUERY_END && icm<=commain->cnt) {
                tnl_append(compre, com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
		    tok_var = tokmain->val[itm++];
                    tsl_append(tokpre,tok_var);
                }
		com_var = commain->val[icm++];
            }

            tnl_append(compre, CB);
            tnl_append(compre, CLOSE);

            if (rm_set)
                tnl_append(compre, P_PRIOR);

            if (icm<=commain->cnt)
                tnl_append(compre, com_var);
	    com_var = commain->val[icm++];
        }

    } else if (type == ADVANCED) {

        if (txt_retr_model != NULL && 
            (txt_retr_model->prior_type == LENGTH_PRIOR ||
             txt_retr_model->prior_type == LOG_NORMAL_PRIOR)) {
            rm_set = TRUE;
        } else
            rm_set = FALSE;

	com_var = commain->val[icm++];
        // while (!feof(command_file)) {
        while ( icm<=commain->cnt ) {

            command_num = 0;
            term_num = 0;
            term_sp = 0;
            com_sp = 0;
            com_code = 0;

            tnl_append(compre, DSC);
            tnl_append(compre, SELECT_NODE);
            tsl_append(tokpre, "\"article\"");
            tnl_append(compre, OPEN);
            tnl_append(compre, ABOUT);
            tsl_append(tokpre,"\"about\"");
            tnl_append(compre, OB);
            tnl_append(compre, CURRENT);
            tnl_append(compre, COMMA);

            while (com_var != QUERY_END && icm<=commain->cnt) {
                tnl_append(compre, com_var);

                com_sp++;
                PUSH_COM(com_var);

                if (com_var == SELECT_NODE || com_var == ABOUT || com_var == INTERSECT || com_var == UNION) {
		    tok_var = tokmain->val[itm++];
                    tsl_append(tokpre, tok_var);
                    term_sp++;
                    PUSH_TRM(tok_var);
                }
		com_var = commain->val[icm++];
            }

            tnl_append(compre, CB);
            tnl_append(compre, CLOSE);

            tnl_append(compre, DSC);
            tnl_append(compre, STAR);
            tnl_append(compre, OPEN);
            tnl_append(compre, ABOUT);
            tsl_append(tokpre, "\"about\"");
            tnl_append(compre, OB);
            tnl_append(compre, CURRENT);
            tnl_append(compre, COMMA);

            command_num = com_sp;
            term_num = term_sp;

            com_sp = 0;
            term_sp =0;

            /* Write out the commands */
            while (com_sp < command_num) {
                com_sp++;
                POP_COM();
                tnl_append(compre, com_code);
            }

            /* Write out the tokens */
            while (term_sp < term_num) {
                term_sp++;
                POP_TRM();
                tsl_append(tokpre,term);
            }

            tnl_append(compre, CB);
            tnl_append(compre, CLOSE);

            if (rm_set)
                tnl_append(compre, P_PRIOR);

            if (icm<=commain->cnt)
                tnl_append(compre, com_var);
	    com_var = commain->val[icm++];
        }

    } // else if ( type == ADVANCED )

    icp = 0; /* reset the reader */
    itp = 0; /* reset the reader */

    tnl_clear(commain); /* make the command list empty again, WHY Vojkan */
    tsl_clear(tokmain); /* make the token list empty again, WHY Vojkan */

    /* Copy the contents of the token_pre to the token file */
    char *ptok_var = tokpre->val[itp++];
    while ( itp <= tokpre->cnt ) {
	tsl_append(tokmain,ptok_var);
        ptok_var = tokpre->val[itp++];
    }

    /* Copy the contents of the command_pre to the command file */
    com_var = compre->val[icp++];

    while ( icp <= compre->cnt ) {
	tnl_append(commain,com_var);
        com_var = compre->val[icp++];
    }

    return 1;
}




/* function for rewriting CAS queries */

int CAStoCPlan(int query_num, int type, bool rm_set) {
  (void)query_num;

  /* variable for setting which terms should be recorded for insertion */
  bool record, irecord;

  /* variables for reading parsed CO query */
  command com_var, com_var_tmp;
  // char tok_var[30];
  char *tok_var;

  int command_num, mcommand_num, icommand_num;
  /* int term_num, mterm_num, iterm_num; */

  /* files that store parser command and token output */
  int icp = 0;
  TijahNumberList* compre = &parserCtx->command_preLIST;
  int icm = 0;
  TijahNumberList* commain = &parserCtx->commandLIST;
  int itp = 0;
  TijahStringList* tokpre = &parserCtx->token_preLIST;
  int itm = 0;
  TijahStringList* tokmain = &parserCtx->tokenLIST;
  /* files that store command and token output afte stop words  removal and stemming */

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
  tnl_clear(compre);
  tsl_clear(tokpre);
  icm = 0;

  com_var_tmp = 0;

  com_var = commain->val[icm++];

  if (type == BASIC) {

    tok_var = tokmain->val[itm++];
    while ( itm <= tokmain->cnt ) {
      tsl_append(tokpre, tok_var);
      tok_var = tokmain->val[itm++];
    }

    // while (!feof(command_file)) {
    while (icm<=commain->cnt) {
      if (com_var == QUERY_END && rm_set)
      tnl_append(compre, P_PRIOR);
      tnl_append(compre, com_var);
      com_var = commain->val[icm++];
    }

  }

  else if (type == SIMPLE) {

    while (icm<=commain->cnt) {

      if (com_var != QUERY_END) {

	if (com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var == PARAMNODES || com_var ==  CURRENT || com_var == VAGUE) {

	  if (com_var == COMMA && (com_var_tmp == SELECT_NODE || com_var_tmp == CB)) {
	    record = TRUE;
	  }

	  else if (com_var == CB) {
	    record = FALSE;
	  }

	  else if (com_var == CLOSE && com_sp > 0) {

	    tnl_append(compre, INTERSECT);
	    tsl_append(tokpre,"\"and\"");
	    tnl_append(compre, ABOUT);
	    tsl_append(tokpre, "\"about\"");
	    tnl_append(compre, OB);
	    tnl_append(compre, CURRENT);
	    tnl_append(compre, COMMA);

	    command_num = com_sp;
	    com_sp = 0;
	    /* term_num = term_sp; */
	    term_sp = 0;

	    while (com_sp < command_num) {

	      com_sp++;
	      POP_COM();
	      tnl_append(compre, com_code);

	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION  || com_code == IMAGE_ABOUT) {
		term_sp++;
		POP_TRM();
		tsl_append(tokpre,term);
	      }

	    }

	    com_sp = 0;
	    term_sp = 0;
	    record = FALSE;

	    tnl_append(compre, CB);

	  }

	  tnl_append(compre, com_var);

	}

	else if (com_var == QUOTE || com_var == PLUS || com_var == MINUS || com_var == MUST_NOT || com_var == MUST) {

	  if (record) {
	    com_sp++;
	    PUSH_COM(com_var);
	  }

	  tnl_append(compre, com_var);

	}

	else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {

	  tok_var = tokmain->val[itm++];

	  if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION)) || (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION || com_var == IMAGE_ABOUT))) {

	    tnl_append(compre, com_var);
	    tsl_append(tokpre, tok_var);

	  }

	  else {

	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }

	    tnl_append(compre, com_var);
	    tsl_append(tokpre, tok_var);

	  }

	}

	else if (com_var == SELECT_NODE  || com_var == IMAGE_ABOUT) {

	  tok_var = tokmain->val[itm++];

	  if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR){
	    tnl_append(compre, com_var);
	    tsl_append(tokpre, tok_var);

	  }

	  else {

	    if (record) {
	      term_sp++;
	      PUSH_TRM(tok_var);
	      com_sp++;
	      PUSH_COM(com_var);
	    }

	    tnl_append(compre, com_var);
	    tsl_append(tokpre, tok_var);

	  }

	}

      }

      else {

	if (rm_set)
	  tnl_append(compre, P_PRIOR);


	tnl_append(compre, com_var);

      }

      com_var_tmp = com_var;
      com_var = commain->val[icm++];

    }
  
  }

  else if (type == ADVANCED) {

    while (icm<=commain->cnt) {

      if (com_var != QUERY_END) {
     	
	if (com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var == PARAMNODES ||  com_var ==  CURRENT || com_var == VAGUE) {
	  
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
	      tnl_append(compre, INTERSECT);
	      tsl_append(tokpre, "\"and\"");
	      tnl_append(compre, ABOUT);
	      tsl_append(tokpre, "\"about\"");
	      tnl_append(compre, OB);
	      tnl_append(compre, CURRENT);
	      tnl_append(compre, COMMA);
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
		tnl_append(compre, com_code);
		*/

		if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT) {
		  term_sp++;
		  POP_TRM();

		  mterm_sp++;
		  PUSH_MTRM(term);
		  /*
		  tsl_append(tokpre, term);
		  */

		}

	      }
	      
	      com_sp = 0;
	      term_sp = 0;
	      record = FALSE;

	      mcom_sp++;
	      PUSH_MCOM(CB);
	      /*
	      tnl_append(compre, CB);
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
	    tnl_append(compre, com_var);
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
	  tnl_append(compre, com_var);
	  */

	  icom_sp++;
	  PUSH_ICOM(com_var);

	}

	else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {
	  
	  tok_var = tokmain->val[itm++];
	  
	  if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION))|| (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION))) {

	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    /*
	    tnl_append(compre, com_var);
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
	    tnl_append(compre, com_var);
	    fprintf (token_file_pre, "%s\n", tok_var);	    
	    */
	  }
	  
	}

	else if (com_var == SELECT_NODE || com_var == IMAGE_ABOUT) {
	  
	  tok_var = tokmain->val[itm++];
	  
	  if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR){
	    mcom_sp++;
	    PUSH_MCOM(com_var);
	    mterm_sp++;
	    PUSH_MTRM(tok_var);

	    /*
	    tnl_append(compre, com_var);
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
	    tnl_append(compre, com_var);
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

	    tnl_append(compre, com_code);

	    if (com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == SELECT_NODE || com_code == IMAGE_ABOUT) {

	      mterm_sp++;
	      POP_MTRM();

	      tsl_append(tokpre, term);

	    }

	    mcom_sp++;
	    POP_MCOM();

	  }


	  if (icom_sp_mid != icom_sp) {

	    tnl_append(compre, INTERSECT);
	    tsl_append(tokpre, "\"and\"");
	    tnl_append(compre, ABOUT);
	    tsl_append(tokpre, "\"about\"");
	    tnl_append(compre, OB);
	    tnl_append(compre, CURRENT);
	    tnl_append(compre, COMMA);
	    
	    icommand_num = icom_sp;
	    icom_sp = icom_sp_mid;
	    /* iterm_num = iterm_sp; */
	    iterm_sp = iterm_sp_mid;

	    while (icom_sp < icommand_num) {
	      
	      icom_sp++;
	      POP_ICOM();
	      tnl_append(compre, com_code);
	      
	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT) {
		iterm_sp++;
		POP_ITRM();
		tsl_append(tokpre, term);
	      }

	    }
	    
	    tnl_append(compre, CB);

	    tnl_append(compre, CLOSE);

	    mcom_sp++;
	    POP_MCOM();

	    while (com_code != CLOSE) {

	      tnl_append(compre, com_code);

	      if (com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == SELECT_NODE || com_code == IMAGE_ABOUT) {
 
		mterm_sp++;
		POP_MTRM();

		tsl_append(tokpre, term);

	      }

	      mcom_sp++;
	      POP_MCOM();

	    }

	    tnl_append(compre, INTERSECT);
	    tsl_append(tokpre, "\"and\"");
	    tnl_append(compre, ABOUT);
	    tsl_append(tokpre, "\"about\"");
	    tnl_append(compre, OB);
	    tnl_append(compre, CURRENT);
	    tnl_append(compre, COMMA);
	    
	    icommand_num = icom_sp_mid - 1;
	    icom_sp = 0;
	    /* iterm_num = iterm_sp_mid; */
	    iterm_sp = 0;
	    
	    while (icom_sp < icommand_num) {
	      
	      icom_sp++;
	      POP_ICOM();
	      tnl_append(compre, com_code);
	      
	      if (com_code == SELECT_NODE || com_code ==  ABOUT || com_code ==  INTERSECT || com_code ==  UNION || com_code == IMAGE_ABOUT)  {
		iterm_sp++;
		POP_ITRM();
		tsl_append(tokpre, term);
	      }

	    }

	    tnl_append(compre, CB);

	  }

	  tnl_append(compre, CLOSE);

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
	  tnl_append(compre, P_PRIOR);


	tnl_append(compre, QUERY_END);

      }

      com_var_tmp = com_var;
      com_var = commain->val[icm++];

    }
  
  }

  icp = 0; /* reset to start */
  itp = 0; /* reset to start */
  icm = 0;
  tnl_clear(commain);
  itm = 0;
  tsl_clear(tokmain);
  
  char *ptok_var = tokpre->val[itp++];
  while ( itp <= tokpre->cnt ) {
    tsl_append(tokmain,ptok_var);
    ptok_var = tokpre->val[itp++];
  }

  com_var = compre->val[icp++];
  while ( icp <= compre->cnt ) {
    tnl_append(commain,com_var);
    com_var = compre->val[icp++];
  }
  return 1;

}
