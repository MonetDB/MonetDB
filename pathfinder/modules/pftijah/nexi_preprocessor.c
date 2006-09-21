/*

     query_processor.c
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Procedures for preprocessing, stemming and stop word removal

*/

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "nexi.h"

int DutchStem(char *word ) {
   (void)word;
   if ( 1 ) {
     fprintf(stderr,"DutchStem: not implemented yet.\n");
     exit(0);
   } else
     return 0;

}

int Stem( char *word ) {
   (void)word;
   if ( 1 ) {
     fprintf(stderr,"Stem: not implemented yet.\n");
     exit(0);
   } else
     return 0;
}

bool StopWord(char *term, int language) {

  bool comp;
  char st_ls[30];

  FILE *stop_list_file = NULL;

  if (language == ENGLISH)
    stop_list_file = fopen(myfileName(WORKDIR,"english_stop_list.sl"),"r");
  else if (language == DUTCH)
    stop_list_file = fopen(myfileName(WORKDIR,"dutch_stop_list.sl"),"r");

  if (stop_list_file == NULL) {
    printf("Error: cannot open stop list file for reading.\n");
    return 0;
  }

  comp = FALSE;

  while (!feof(stop_list_file)) {

    fscanf(stop_list_file, "%s", st_ls);

    if (!strcmp(st_ls, term)) {
      comp = TRUE;
    }

  }

  return comp;

}

/* Procedure for preprocessing query files */
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






/* Procedure for processing query files */

int process(int processing_type, int language) {

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


  if (processing_type == NO_STOP_STEM) {

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

  else if (processing_type == STOP_WORD || processing_type == STEMMING || processing_type == STOP_STEM) {

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
printf("%d\n", com_var);
              fprintf (token_file, "\"%s\"\n", tok_var);
printf("%s\n", tok_var);
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

	  if (processing_type == STOP_WORD){

	    if (!StopWord(tok_var, language)) {
	      fprintf (command_file, "%d\n", com_var);
	      fprintf (token_file, "\"%s\"\n", tok_var);
	    }
	    else {
	      num_stopword++;
	    }

	  }

	  else if (processing_type == STEMMING) {

	    if (language == ENGLISH)
	      Stem(tok_var);
	    else if (language == DUTCH)
	      DutchStem(tok_var);

	    fprintf (command_file, "%d\n", com_var);
	    fprintf (token_file, "\"%s\"\n", tok_var);

	  }

	  else if (processing_type == STOP_STEM) {

	    if (!StopWord(tok_var, language)) {

	      if (language == ENGLISH)
	        Stem(tok_var);
	      else if (language == DUTCH)
	        DutchStem(tok_var);

	      fprintf (command_file, "%d\n", com_var);
	      fprintf (token_file, "\"%s\"\n", tok_var);

	    }
            else {

	      if(in_quote == TRUE) {

		if (language == ENGLISH)
	          Stem(tok_var);
	        else if (language == DUTCH)
	          DutchStem(tok_var);

		 fprintf (command_file, "%d\n", com_var);
	      	 fprintf (token_file, "\"%s\"\n", tok_var);

	      }
              else{
	      	num_stopword++;
              }

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

	  if (processing_type == STOP_WORD){

	    if (!StopWord(tok_var, language)) {
	      fprintf (command_file, "%d\n", com_var);
	      fprintf (token_file, "\"%s\"\n", tok_var);
	    }
	    else {
	    	if(in_quote == TRUE) {
			fprintf (command_file, "%d\n", com_var);
	      		fprintf (token_file, "\"%s\"\n", tok_var);
		}
		else{
	      		num_stopword++;
		}
	    }

	  }

	  else if (processing_type == STEMMING) {

	    if (language == ENGLISH)
	      Stem(tok_var);
	    else if (language == DUTCH)
	      DutchStem(tok_var);

	    fprintf (command_file, "%d\n", com_var);
	    fprintf (token_file, "\"%s\"\n", tok_var);

	  }

	  else if (processing_type == STOP_STEM) {

	    if (!StopWord(tok_var,language)) {

	      if (language == ENGLISH)
	        Stem(tok_var);
	      else if (language == DUTCH)
	        DutchStem(tok_var);

	      fprintf (command_file, "%d\n", com_var);
	      fprintf (token_file, "\"%s\"\n", tok_var);

	    }
	    else {

	      if(in_quote == TRUE) {

		  if (language == ENGLISH)
	            Stem(tok_var);
	          else if (language == DUTCH)
	            DutchStem(tok_var);

		   fprintf (command_file, "%d\n", com_var);
	      	   fprintf (token_file, "\"%s\"\n", tok_var);

	      }
              else{
	      	num_stopword++;
              }

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
