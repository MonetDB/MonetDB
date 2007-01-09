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
 * Procedures for preprocessing, stemming and stop word removal
 */

#include <pf_config.h>

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include "nexi.h"

#include <pftijah_stem.h>

/**
 * This function performs both stop word removal and stemming.
 *
 *  - when a term is a stop word (according to the stop word list
 *    associated with the stemmer), NULL is returned.
 *  - otherwise, the stemmed variant of the term is returned.
 */
char* stop_stem( tjStemCtx* stemCtx, char* term ) {
    // Find out if it is a stop word
    int i = 0;
    while ( stemCtx->stopWords[i] != NULL ) {
        if ( strcasecmp(term, stemCtx->stopWords[i]) == 0 )
            return NULL;
        i++;
    }
    
    if ( stemCtx->stem ) {
        // Convert to lowercase?
    
        // Perform initialization of udf structure if necessary
        if ( !stemCtx->udf && stemCtx->init ) stemCtx->init( stemCtx, NULL );

        // Perform stemming
        char* stemmed = (char *)stemCtx->stem( stemCtx, term );
        return stemmed;
    } else 
        return term;
}


/**
 * This function performs preprocessing: drop or leave in tokens for 
 * phrase search and term weighting (a.k.a. modifiers)
 *
 * There are several preprocessing types. Based on these types, 
 * tokens like +, - and " are output (or dropped).
 *
 *                     modifiers   phrases
 *             PLAIN:     no         no
 *       NO_MODIFIER:     no         yes
 *
 *       VAGUE_MODIF:     vague(1)   yes
 *      STRICT_MODIF:     strict(2)  yes
 *
 *   VAGUE_NO_PHRASE:     vague(1)   no
 *  STRICT_NO_PHRASE:     strict(2)  no
 *
 *  (1): vague means that + and - are output as is
 *  (2): strict means that + and - are converted to MUST and MUST_NOT
 *
 * When phrase search is disabled, any modifiers placed before the phrases
 * are placed before the terms that make up the phrase.
 */
int preprocess(int preproc_type) {

  /* token and command variables for preprocessing */
  int com_var;
  /* char tok_var[30]; */
  char* tok_var;

  /* files that store parser command and token output */
  int icp = 0;
  TijahNumberList* compre = &parserCtx->command_preLIST;
  int icm = 0;
  TijahNumberList* commain = &parserCtx->commandLIST;
  int itp = 0;
  TijahStringList* tokpre = &parserCtx->token_preLIST;
  int itm = 0;
  TijahStringList* tokmain = &parserCtx->tokenLIST;

  /* tsl_clear(commain); */ 
  
  com_var = compre->val[icp++];
  while ( icp <= compre->cnt ) {

    if (com_var ==  QUERY_END || com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == VAGUE) {

      tnl_append(commain,com_var);

    
    }
    else if (com_var == QUOTE) {
      
      if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF)
        tnl_append(commain,com_var);
    }

    else if (com_var == PLUS) {
      
      if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
        com_var = compre->val[icp++];
      
      } else {
	
	if (preproc_type == VAGUE_NO_PHRASE || preproc_type == VAGUE_MODIF)
          tnl_append(commain,com_var);
	else
	  // fprintf(command_file, "%d\n", MUST);
          tnl_append(commain,MUST);
	
        com_var = compre->val[icp++];

      }
      
      if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {

        tok_var = tokpre->val[itp++];
        tnl_append(commain,com_var);
	tsl_append(tokmain,tok_var);
	
      }
      
      else if (com_var == QUOTE) {	
	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {

	  if (preproc_type == PLAIN) {
            com_var = compre->val[icp++];
          }

	  else if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
            tnl_append(commain,com_var);
            com_var = compre->val[icp++];
	  }
	  
	  while (com_var != QUOTE) {
            tok_var = tokpre->val[itp++];
            tnl_append(commain,com_var);
	    tsl_append(tokmain,tok_var);
            com_var = compre->val[icp++];
	  }

	  if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
            tnl_append(commain,com_var);
	  }
	  
	}

	else if (preproc_type == VAGUE_NO_PHRASE || preproc_type == STRICT_NO_PHRASE) {

          com_var = compre->val[icp++];
	  
	  if (preproc_type == VAGUE_NO_PHRASE) {
	    
            tok_var = tokpre->val[itp++];
	    /*	    fprintf(command_file, "%d\n", PLUS); */
            tnl_append(commain,com_var);
	    tsl_append(tokmain,tok_var);
            com_var = compre->val[icp++];

	    while (com_var != QUOTE) {
              tok_var = tokpre->val[itp++];
              tnl_append(commain,PLUS);
              tnl_append(commain,com_var);
	      tsl_append(tokmain,tok_var);
              com_var = compre->val[icp++];
	    }
	    
	  }
	  
	  else {
	    
            tok_var = tokpre->val[itp++];
	    /*	    fprintf(command_file, "%d\n", MUST); */
            tnl_append(commain,com_var);
	    tsl_append(tokmain,tok_var);
            com_var = compre->val[icp++];
	    
	    while (com_var != QUOTE) {
              tok_var = tokpre->val[itp++];
              tnl_append(commain,MUST);
              tnl_append(commain,com_var);
	      tsl_append(tokmain,tok_var);
              com_var = compre->val[icp++];
	    }

	  }

	}

      }

    }

    else if (com_var == MINUS) {
         
      if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
        com_var = compre->val[icp++];
      }
      else {
	
	if (preproc_type == VAGUE_NO_PHRASE || preproc_type == VAGUE_MODIF)
          tnl_append(commain,com_var);
	else
          tnl_append(commain,MUST_NOT);
	
        com_var = compre->val[icp++];
      }

      if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {
	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
          tok_var = tokpre->val[itp++];
	  /*printf("%s ",tok_var); */
	}
	else {
          tok_var = tokpre->val[itp++];
          tnl_append(commain,com_var);
	  tsl_append(tokmain,tok_var);
	}

      }
      
      else if (com_var == QUOTE) {
     	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	  
	  if (preproc_type == PLAIN) {
            com_var = compre->val[icp++];

	  } else if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
            tnl_append(commain,com_var);
            com_var = compre->val[icp++];
	  }
	  
	  if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
	    
	    while (com_var != QUOTE) {
                tok_var = tokpre->val[itp++];
                com_var = compre->val[icp++];

	    }

	  }
	  
	  else {

	    while (com_var != QUOTE) {
              tok_var = tokpre->val[itp++];
              tnl_append(commain,com_var);
	      tsl_append(tokmain,tok_var);
              com_var = compre->val[icp++];
	    }
	    
	  }
	  
	  if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
            tnl_append(commain,com_var);
	  }
	  
	}
	
	else if (preproc_type == VAGUE_NO_PHRASE || preproc_type == STRICT_NO_PHRASE) {
	  
          com_var = compre->val[icp++];
	  
	  if (preproc_type == VAGUE_NO_PHRASE) {
	    
            tok_var = tokpre->val[itp++];
            tnl_append(commain,com_var);
	    tsl_append(tokmain,tok_var);
            com_var = compre->val[icp++];
	    
	      while (com_var != QUOTE) {
                tok_var = tokpre->val[itp++];
                tnl_append(commain,MINUS);
                tnl_append(commain,com_var);
	        tsl_append(tokmain,tok_var);
  		com_var = compre->val[icp++];
              }
	      
	  }
	  
	  else {
	    
            tok_var = tokpre->val[itp++];
	    /*	    fprintf(command_file, "%d\n", MUST_NOT); */
            tnl_append(commain,com_var);
	    tsl_append(tokmain,tok_var);
  	    com_var = compre->val[icp++];
	    
	    while (com_var != QUOTE) {
              tok_var = tokpre->val[itp++];
              tnl_append(commain,MUST_NOT);
              tnl_append(commain,com_var);
	      tsl_append(tokmain,tok_var);
              com_var = compre->val[icp++];
	    }
	    
	  }

	}
     	
      }
      
    }

    else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {
      
      tok_var = tokpre->val[itp++];
      tnl_append(commain,com_var);
      tsl_append(tokmain,tok_var);

    }

    else if (com_var ==  IMAGE_ABOUT) {

      tok_var = tokpre->val[itp++];
      tnl_append(commain,com_var);

      char* ch_img = strrchr(tok_var, '/');
      ch_img++;
      /*printf("%s\n", ch_img); */
      tsl_append(tokmain,tok_var);

    }
    com_var = compre->val[icp++];
  }

  tsl_clear(tokpre); /* make the token pre list empty again, WHY Vojkan */
  tnl_clear(compre); /* make the command pre list empty again, WHY Vojkan */

  icm = 0;

  itm = 0;
  tok_var = tokmain->val[itm++];
  while( itm<=tokmain->cnt ) {
    tsl_append(tokpre,tok_var);
    tok_var = tokmain->val[itm++];
  }

  com_var = commain->val[icm++];
  while ( icm<=commain->cnt ) {
    tnl_append(compre,com_var);
    com_var = commain->val[icm++];
  }

  tsl_clear(tokmain); /* make the token list empty again, WHY Vojkan */
  tnl_clear(commain); /* make the token list empty again, WHY Vojkan */

  return 1;

}

/**
 * This function performs stemming and stop word removal. Arguments:
 * 
 * - stemmer (string): name of the stemmer to use. This should be the same as was used on
 *   the collection to be queried.
 * - stem_stop (boolean): whether to perform stemming and stop word removal at all.
 *   (if not, you might consider not calling this function :-))
 * - stop_quoted (boolean): whether to perform stop word removal inside quoted query strings (phrases)
 */
int process(char* stemmer, bool stem_stop, bool stop_quoted) {
  /* token and command variables for preprocessing */
  int com_var;
  // char tok_var[30];
  char* tok_var;
  int com_var_tmp;
  int num_stopword;
  bool in_quote;

  /* files that store parser command and token output */
  int icp = 0;
  TijahNumberList* compre = &parserCtx->command_preLIST;
  // int icm = 0;
  TijahNumberList* commain = &parserCtx->commandLIST;
  int itp = 0;
  TijahStringList* tokpre = &parserCtx->token_preLIST;
  /* int itm = 0; */
  TijahStringList* tokmain = &parserCtx->tokenLIST;

  in_quote = FALSE;
  num_stopword = 0;

  tsl_clear(tokmain);
  tnl_clear(commain);

  // Initialize the stemmer
  tjStemCtx* stemCtx = getStemmingContext( stemmer );

  if ( stemCtx->stem && !stemCtx->udf && stemCtx->init ) 
      stemCtx->init( stemCtx, NULL );

  // Don't perform stemming and stop word removal at all,
  // just write input to output
  if (!stem_stop) {

    tok_var = tokpre->val[itp++];
    while ( itp <= tokpre->cnt ) {
      tsl_appendq(tokmain,tok_var);
      tok_var = tokpre->val[itp++];
    }

    com_var = compre->val[icp++];
    while ( icp <= compre->cnt ) {
      tnl_append(commain,com_var);
      com_var = compre->val[icp++];
    }

  }

  else {

    com_var_tmp = 0;

    com_var = compre->val[icp++];

    // while (!feof(command_file_pre)) {
    while ( icp <= compre->cnt ) {

      if (com_var ==  QUERY_END || com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == QUOTE || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == MINUS || com_var == PLUS || com_var == MUST_NOT || com_var == MUST || com_var == VAGUE) {

      	if (com_var == QUOTE) {

		if (in_quote == FALSE)
			in_quote = TRUE;
		else
			in_quote = FALSE;

	}

        tnl_append(commain,com_var);

	if (com_var == PLUS || com_var == MINUS || com_var == MUST || com_var == MUST_NOT) {

          com_var = compre->val[icp++];

	  if (com_var == SELECT_NODE) {

            tok_var = tokpre->val[itp++];
            tnl_append(commain,com_var);
            tsl_appendq(tokmain,tok_var);

	  }

	  else if (com_var == QUOTE) {

            tnl_append(commain,com_var);

            com_var = compre->val[icp++];

	    while (com_var != QUOTE) {
              tok_var = tokpre->val[itp++];
              tnl_append(commain,com_var);
              tsl_appendq(tokmain,tok_var);
              com_var = compre->val[icp++];
	    }

            tnl_append(commain,com_var);
	  }
        }
      }
      else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {
        tok_var = tokpre->val[itp++];

	if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION)) || (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION))) {

          tnl_append(commain,com_var);
          tsl_appendq(tokmain,tok_var);

	}
	else {
	  // Perform stemming and stop word detection
          char* stemmed = stop_stem( stemCtx, tok_var );
          if ( stemmed ) {
            // Output the stemmed version of the term to the token file
            tnl_append(commain,com_var);
            tsl_appendq(tokmain,tok_var);
          } else {
            // If we're inside a quoted string, and stop word removal inside
            // quoted strings is disabled, output the original token
            if ( in_quote && !stop_quoted ) {
              tnl_append(commain,com_var);
              tsl_appendq(tokmain,tok_var);
            } else { 
              // It is a stop word
              num_stopword++;
            }
          }
	}

      }

      else if (com_var == SELECT_NODE) {

        tok_var = tokpre->val[itp++];

	if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR || com_var_tmp == VAGUE){
          tnl_append(commain,com_var);
          tsl_appendq(tokmain,tok_var);

	}

	else {
	
	  // Perform stemming and stop word detection
          char* stemmed = stop_stem( stemCtx, tok_var );
          if ( stemmed ) {
            // Output the stemmed version of the term to the token file
            tnl_append(commain,com_var);
            tsl_appendq(tokmain,stemmed);
          } else {
            // If we're inside a quoted string, and stop word removal inside
            // quoted strings is disabled, output the original token
            if ( in_quote && !stop_quoted ) {
              tnl_append(commain,com_var);
              tsl_appendq(tokmain,tok_var);
            } else { 
              // It is a stop word
              num_stopword++;
            }
          }

	}

      }

      else if (com_var == IMAGE_ABOUT) {

         tok_var = tokpre->val[itp++];
         tnl_append(commain,com_var);
         tsl_appendq(tokmain,tok_var);

      }

      com_var_tmp = com_var;
      com_var = compre->val[icp++];

    }
  }
  return 1;
}
