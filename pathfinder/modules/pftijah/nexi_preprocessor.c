/*

     query_processor.c
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Procedures for preprocessing, stemming and stop word removal

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
  char tok_var[30];

  /* files that store parser command and token output */
  FILE *command_file_pre;
  FILE *token_file_pre;
  /* files that store command and token output afte stop words  removal and stemming */
  FILE *command_file;
  FILE *token_file;


  token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"r");
  command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"r");
  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");

  if (token_file_pre == NULL) {
    printf("Error: cannot create file for reading.\n");
    return 0;
  }

  if (token_file == NULL) {
    printf("Error: cannot create file for writing.\n");
    return 0;
  }

  if (command_file_pre == NULL) {
    printf("Error: cannot create file for reading.\n");
    return 0;
  }

  if (command_file == NULL) {
    printf("Error: cannot create file for writing.\n");
    return 0;
  }

  fscanf(command_file_pre, "%d", &com_var);

  while (!feof(command_file_pre)) {
    
    if (com_var ==  QUERY_END || com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == VAGUE) {

      fprintf(command_file, "%d\n", com_var);
    
    }
    else if (com_var == QUOTE) {
      
      if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF)
	
	fprintf (command_file, "%d\n", com_var);
      
    }

    else if (com_var == PLUS) {
      
      if (preproc_type == PLAIN || preproc_type == NO_MODIFIER)
	fscanf(command_file_pre, "%d", &com_var);
      
      else {
	
	if (preproc_type == VAGUE_NO_PHRASE || preproc_type == VAGUE_MODIF)
	  fprintf(command_file, "%d\n", com_var);
	else
	  fprintf(command_file, "%d\n", MUST);
	
	fscanf(command_file_pre, "%d", &com_var);

      }
      
      if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {

	fscanf(token_file_pre, "%s", tok_var);
	fprintf (command_file, "%d\n", com_var);
	fprintf (token_file, "%s\n", tok_var);
	
      }
      
      else if (com_var == QUOTE) {	
	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {

	  if (preproc_type == PLAIN)
	    fscanf(command_file_pre, "%d", &com_var);

	  else if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	    fprintf (command_file, "%d\n", com_var);
	    fscanf(command_file_pre, "%d", &com_var);
	  }
	  
	  while (com_var != QUOTE) {
	    fscanf(token_file_pre, "%s", tok_var);
	    fprintf (command_file, "%d\n", com_var);
	    fprintf (token_file, "%s\n", tok_var);
	    fscanf(command_file_pre, "%d", &com_var);
	  }

	  if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	    fprintf (command_file, "%d\n", com_var);
	  }
	  
	}

	else if (preproc_type == VAGUE_NO_PHRASE || preproc_type == STRICT_NO_PHRASE) {

	  fscanf(command_file_pre, "%d", &com_var);
	  
	  if (preproc_type == VAGUE_NO_PHRASE) {
	    
	    fscanf(token_file_pre, "%s", tok_var);
	    /*	    fprintf(command_file, "%d\n", PLUS); */
	    fprintf(command_file, "%d\n", com_var);
	    fprintf(token_file, "%s\n", tok_var);
	    fscanf(command_file_pre, "%d", &com_var);

	    while (com_var != QUOTE) {
	      fscanf(token_file_pre, "%s", tok_var);
	      fprintf(command_file, "%d\n", PLUS);
	      fprintf(command_file, "%d\n", com_var);
	      fprintf(token_file, "%s\n", tok_var);
	      fscanf(command_file_pre, "%d", &com_var);
	    }
	    
	  }
	  
	  else {
	    
	    fscanf(token_file_pre, "%s", tok_var);
	    /*	    fprintf(command_file, "%d\n", MUST); */
	    fprintf(command_file, "%d\n", com_var);
	    fprintf(token_file, "%s\n", tok_var);
	    fscanf(command_file_pre, "%d", &com_var);
	    
	    while (com_var != QUOTE) {
	      fscanf(token_file_pre, "%s", tok_var);
	      fprintf(command_file, "%d\n", MUST);
	      fprintf(command_file, "%d\n", com_var);
	      fprintf(token_file, "%s\n", tok_var);
	      fscanf(command_file_pre, "%d", &com_var);
	    }

	  }

	}

      }

    }

    else if (com_var == MINUS) {
         
      if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
	fscanf(command_file_pre, "%d", &com_var);
      }
      else {
	
	if (preproc_type == VAGUE_NO_PHRASE || preproc_type == VAGUE_MODIF)
	  fprintf(command_file, "%d\n", com_var);
	else
	  fprintf(command_file, "%d\n", MUST_NOT);
	
	fscanf(command_file_pre, "%d", &com_var);
	
      }

      if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {
	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
	  fscanf(token_file_pre, "%s", tok_var);
	  /*printf("%s ",tok_var); */
	}
	else {
	  fscanf(token_file_pre, "%s", tok_var);
	  fprintf(command_file, "%d\n", com_var);
	  fprintf(token_file, "%s\n", tok_var);
	}

      }
      
      else if (com_var == QUOTE) {
     	
	if (preproc_type == PLAIN || preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	  
	  if (preproc_type == PLAIN)
	    fscanf(command_file_pre, "%d", &com_var);

	  else if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	    fprintf(command_file, "%d\n", com_var);
	    fscanf(command_file_pre, "%d", &com_var);
	  }
	  
	  if (preproc_type == PLAIN || preproc_type == NO_MODIFIER) {
	    
	    while (com_var != QUOTE) {
                fscanf(token_file_pre, "%s", tok_var);
	        fscanf(command_file_pre, "%d", &com_var);

	    }

	  }
	  
	  else {

	    while (com_var != QUOTE) {
	      fscanf(token_file_pre, "%s", tok_var);
	      fprintf(command_file, "%d\n", com_var);
	      fprintf(token_file, "%s\n", tok_var);
	      fscanf(command_file_pre, "%d", &com_var);
	    }
	    
	  }
	  
	  if (preproc_type == NO_MODIFIER || preproc_type == VAGUE_MODIF || preproc_type == STRICT_MODIF) {
	    fprintf (command_file, "%d\n", com_var);
	  }
	  
	}
	
	else if (preproc_type == VAGUE_NO_PHRASE || preproc_type == STRICT_NO_PHRASE) {
	  
	  fscanf(command_file_pre, "%d", &com_var);
	  
	  if (preproc_type == VAGUE_NO_PHRASE) {
	    
	    fscanf(token_file_pre, "%s", tok_var);
	    /*	    fprintf(command_file, "%d\n", MINUS); */
	    fprintf(command_file, "%d\n", com_var);
	    fprintf(token_file, "%s\n", tok_var);
	    fscanf(command_file_pre, "%d", &com_var);
	    
	      while (com_var != QUOTE) {
                fscanf(token_file_pre, "%s", tok_var);
		fprintf(command_file, "%d\n", MINUS);
	        fprintf(command_file, "%d\n", com_var);
                fprintf(token_file, "%s\n", tok_var);
	        fscanf(command_file_pre, "%d", &com_var);
              }
	      
	  }
	  
	  else {
	    
	    fscanf(token_file_pre, "%s", tok_var);
	    /*	    fprintf(command_file, "%d\n", MUST_NOT); */
	    fprintf(command_file, "%d\n", com_var);
	    fprintf(token_file, "%s\n", tok_var);
	    fscanf(command_file_pre, "%d", &com_var);
	    
	    while (com_var != QUOTE) {
	      fscanf(token_file_pre, "%s", tok_var);
	      fprintf(command_file, "%d\n", MUST_NOT);
	      fprintf(command_file, "%d\n", com_var);
	      fprintf(token_file, "%s\n", tok_var);
	      fscanf(command_file_pre, "%d", &com_var);
	    }
	    
	  }

	}
     	
      }
      
    }

    else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION || com_var == SELECT_NODE) {
      
      fscanf(token_file_pre, "%s", tok_var);
      fprintf (command_file, "%d\n", com_var);
      fprintf (token_file, "%s\n", tok_var);

    }

    else if (com_var ==  IMAGE_ABOUT) {

      fscanf(token_file_pre, "%s", tok_var);
      fprintf (command_file, "%d\n", com_var);

      char* ch_img = strrchr(tok_var, '/');
      ch_img++;
      /*printf("%s\n", ch_img); */
      fprintf (token_file, "%s\n", ch_img);

    }

    fscanf(command_file_pre, "%d", &com_var);


    
  }
  
  fclose(token_file_pre);
  fclose(command_file_pre);
  fclose(token_file);
  fclose(command_file);

  token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"w");
  command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"w");
  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"r");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"r");

  if (token_file_pre == NULL) {
    printf("Error: cannot create file for rewriting.\n");
    return 0;
  }

  if (token_file == NULL) {
    printf("Error: cannot create file for rereading.\n");
    return 0;
  }

  if (command_file_pre == NULL) {
    printf("Error: cannot create file for rewriting.\n");
    return 0;
  }

  if (command_file == NULL) {
    printf("Error: cannot create file for rereading.\n");
    return 0;
  }

  fscanf(token_file, "%s", tok_var);
  while (!feof(token_file)) {
    fprintf (token_file_pre, "%s\n", tok_var);
    fscanf(token_file, "%s", tok_var);
  }

  fscanf(command_file, "%d", &com_var);
  while (!feof(command_file)) {
    fprintf (command_file_pre, "%d\n", com_var);
    fscanf(command_file, "%d", &com_var);
  }

  fclose(token_file_pre);
  fclose(command_file_pre);
  fclose(token_file);
  fclose(command_file);

  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");
  if (command_file == NULL || token_file == NULL) {
    printf("Error: cannot clear files for writing.\n");
    return 0;
  }
  else {
    fclose(command_file);
    fclose(token_file);
  }
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
  char tok_var[30];
  int com_var_tmp;
  int num_stopword;
  bool in_quote;

  /* files that store parser command and token output */
  FILE *command_file_pre;
  FILE *token_file_pre;
  /* files that store command and token output afte stop words  removal and stemming */
  FILE *command_file;
  FILE *token_file;

  in_quote = FALSE;
  num_stopword = 0;

  token_file_pre = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"r");
  command_file_pre = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"r");
  token_file = fopen(myfileName(WORKDIR,"file_token.nxi"),"w");
  command_file = fopen(myfileName(WORKDIR,"file_command.nxi"),"w");

  if (token_file_pre == NULL) {
    printf("Error: cannot create file for reading.\n");
    return 0;
  }

  if (token_file == NULL) {
    printf("Error: cannot create file for writing.\n");
    return 0;
  }

  if (command_file_pre == NULL) {
    printf("Error: cannot create file for reading.\n");
    return 0;
  }

  if (command_file == NULL) {
    printf("Error: cannot create file for writing.\n");
    return 0;
  }

  // Initialize the stemmer
  tjStemCtx* stemCtx = getStemmingContext( stemmer );

  if ( stemCtx->stem && !stemCtx->udf && stemCtx->init ) 
      stemCtx->init( stemCtx, NULL );

  // Don't perform stemming and stop word removal at all,
  // just write input to output
  if (!stem_stop) {

    fscanf(token_file_pre, "%s", tok_var);
    while (!feof(token_file_pre)) {
      fprintf (token_file, "\"%s\"\n", tok_var);
      fscanf(token_file_pre, "%s", tok_var);
    }

    fscanf(command_file_pre, "%d", &com_var);
    while (!feof(command_file_pre)) {
      fprintf (command_file, "%d\n", com_var);
      fscanf(command_file_pre, "%d", &com_var);
    }

  }

  else {

    com_var_tmp = 0;

    fscanf(command_file_pre, "%d", &com_var);

    while (!feof(command_file_pre)) {

      if (com_var ==  QUERY_END || com_var ==  GR || com_var ==  LS || com_var == EQ || com_var == DSC || com_var == COMMA || com_var == QUOTE || com_var == OPEN || com_var == CLOSE || com_var == OB || com_var == CB || com_var ==  STRUCT_OR || com_var == STAR || com_var ==  CURRENT || com_var == MINUS || com_var == PLUS || com_var == MUST_NOT || com_var == MUST || com_var == VAGUE) {

      	if (com_var == QUOTE) {

		if (in_quote == FALSE)
			in_quote = TRUE;
		else
			in_quote = FALSE;

	}

	fprintf (command_file, "%d\n", com_var);

	if (com_var == PLUS || com_var == MINUS || com_var == MUST || com_var == MUST_NOT) {

	  fscanf(command_file_pre, "%d", &com_var);

	  if (com_var == SELECT_NODE) {

            fscanf(token_file_pre, "%s", tok_var);
	    fprintf (command_file, "%d\n", com_var);
            fprintf (token_file, "\"%s\"\n", tok_var);

	  }

	  else if (com_var == QUOTE) {

	    fprintf (command_file, "%d\n", com_var);

	    fscanf(command_file_pre, "%d", &com_var);

	    while (com_var != QUOTE) {
              fscanf(token_file_pre, "%s", tok_var);
	      fprintf (command_file, "%d\n", com_var);
              fprintf (token_file, "\"%s\"\n", tok_var);
	      fscanf(command_file_pre, "%d", &com_var);
	    }

            fprintf (command_file, "%d\n", com_var);

	  }

        }

      }

      else if (com_var ==  ABOUT || com_var ==  INTERSECT || com_var ==  UNION) {

	fscanf(token_file_pre, "%s", tok_var);

	if ((com_var_tmp == CB  && (com_var ==  INTERSECT || com_var ==  UNION)) || (com_var == ABOUT && (com_var_tmp == OB || com_var_tmp == OPEN || com_var_tmp == INTERSECT || com_var_tmp == UNION))) {

	  fprintf (command_file, "%d\n", com_var);
	  fprintf (token_file, "\"%s\"\n", tok_var);

	}

	else {
	  // Perform stemming and stop word detection
          char* stemmed = stop_stem( stemCtx, tok_var );
          if ( stemmed ) {
            // Output the stemmed version of the term to the token file
            fprintf (command_file, "%d\n", com_var);
            fprintf (token_file, "\"%s\"\n", stemmed);
          } else {
            // If we're inside a quoted string, and stop word removal inside
            // quoted strings is disabled, output the original token
            if ( in_quote && !stop_quoted ) {
              fprintf (command_file, "%d\n", com_var);
              fprintf (token_file, "\"%s\"\n", tok_var);
            } else { 
              // It is a stop word
              num_stopword++;
            }
          }
	}

      }

      else if (com_var == SELECT_NODE) {

	fscanf(token_file_pre, "%s", tok_var);

	if (com_var_tmp == DSC || com_var_tmp == OB || com_var_tmp == STRUCT_OR || com_var_tmp == VAGUE){
	  fprintf (command_file, "%d\n", com_var);
	  fprintf (token_file, "\"%s\"\n", tok_var);

	}

	else {
	
	  // Perform stemming and stop word detection
          char* stemmed = stop_stem( stemCtx, tok_var );
          if ( stemmed ) {
            // Output the stemmed version of the term to the token file
            fprintf (command_file, "%d\n", com_var);
            fprintf (token_file, "\"%s\"\n", stemmed);
          } else {
            // If we're inside a quoted string, and stop word removal inside
            // quoted strings is disabled, output the original token
            if ( in_quote && !stop_quoted ) {
              fprintf (command_file, "%d\n", com_var);
              fprintf (token_file, "\"%s\"\n", tok_var);
            } else { 
              // It is a stop word
              num_stopword++;
            }
          }

	}

      }

      else if (com_var == IMAGE_ABOUT) {

         fscanf(token_file_pre, "%s", tok_var);
	 fprintf (command_file, "%d\n", com_var);
         fprintf (token_file, "\"%s\"\n", tok_var);

      }

      com_var_tmp = com_var;
      fscanf(command_file_pre, "%d", &com_var);

    }

  }

  fclose(token_file_pre);
  fclose(command_file_pre);
  fclose(token_file);
  fclose(command_file);

  return 1;

}
