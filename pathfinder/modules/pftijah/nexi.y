%{
/*

     NEXIparser.y
     =========================
     Author: Vojkan Mihajlovic
     University of Twente

     Flex script to tokenize NEXI queries
*/

/* #define YYDEBUG 1 */

#include <pf_config.h>

#include <stdio.h>
#include "nexi.h"

#define CO 1
#define CAS 2

int line_number = 1;
int char_number = 1;

int CO_number = 0;
int CAS_number = 0;

bool rep_err = FALSE;
unsigned int query_type = 0;

extern int nexilex(void);
extern char *nexitext;
#define yylex nexilex

void nexierror(char *err) /* 'yyerror()' called by yyparse in error */
{
  sprintf(&parserCtx->errBUFF[0],"NEXI error: line[%d], pos[%d]: %s at token {%s}\n", line_number, char_number, err, nexitext);
  rep_err = TRUE;

}


%}

%token NUMBER ALPHANUMERIC XMLTAG
%token Q_ABOUT NODE_QUALIFIER
%token Q_AND Q_OR
%token Q_GREATER Q_LESS Q_EQUAL
%token Q_IMAGE

%left Q_AND Q_OR

%%/* Grammar rules and actions */

input:
      | co { query_type = CO; CO_number++; }
      | cas { query_type = CAS; CAS_number++; }
      | Q_LESS { /* dummy error */ if (1) { YYERROR; } else { YYABORT; } /* pacify icc compiler ? */ } ;
;

cas: path cas_filter | path cas_filter path | path cas_filter path cas_filter;

cas_filter: '[' filtered_clause ']';

filtered_clause: filter | filtered_clause Q_AND filtered_clause
                        | filtered_clause Q_OR filtered_clause
                        | '(' filtered_clause ')';

filter: about_clause | arithmetic_clause;

about_clause: Q_ABOUT '(' relative_path ',' co ')';

arithmetic_clause: relative_path arithmetic_operator NUMBER;

arithmetic_operator: Q_GREATER | Q_LESS | Q_EQUAL | greater_equal | less_equal;

greater_equal: Q_GREATER Q_EQUAL;

less_equal: Q_LESS Q_EQUAL;

path: node_sequence | node_sequence attribute_node;

relative_path: '.' | '.' path;

node_sequence: node | node_sequence node;

any_node: NODE_QUALIFIER '*';

attribute_node: NODE_QUALIFIER '@' tag;

named_node: NODE_QUALIFIER tag;

tag_list: tag '|' tag | tag_list '|' tag;

tag_list_node: NODE_QUALIFIER '(' tag_list ')';

node: named_node | any_node | tag_list_node;

tag: alphanumeric | '~' alphanumeric | XMLTAG | '~' XMLTAG;

co: term | co term | image | co image;

term: term_restriction unrestricted_term;

term_restriction:  /* empty */ | '+' | '-';

unrestricted_term: word | phrase;

phrase: '"' word_list '"';

word_list: word word | word_list word;

word: NUMBER | alphanumeric;

alphanumeric: ALPHANUMERIC | Q_ABOUT | Q_AND | Q_OR;

image: Q_IMAGE;

/* location: '\\' loc_step | location '\\' loc_step;

loc_step: '\\' word | '\\' word '.' word; */

%%

/* parsing function */


int parseNEXI(TijahParserContext* parserCtx, int *query_end_num)
{
  rep_err = FALSE;
  char_number = 1;
  line_number = 1;
  CO_number = 0;
  CAS_number = 0;
  
  if ( !(parserCtx->commandFILE = fopen(myfileName(WORKDIR,"file_command_pre.nxi"),"a")) ) {
      sprintf(&parserCtx->errBUFF[0],"Error: cannot create command file for writing.\n");
      return FALSE;
  }
  if ( !(parserCtx->tokenFILE = fopen(myfileName(WORKDIR,"file_token_pre.nxi"),"a")) ) {
      sprintf(&parserCtx->errBUFF[0],"Error: cannot create token file for writing.\n");
      return FALSE;
  }
  /* */
  setNEXIscanstring(parserCtx->queryText);
  nexiparse();
  // Manually terminate the query, otherwise the rewriter gets confused
  fprintf(parserCtx->commandFILE, "%d\n", QUERY_END);
  /* */
  fclose(parserCtx->commandFILE);
  fclose(parserCtx->tokenFILE);

  if ((query_type == CO && CAS_number == 0) || (query_type == CAS && CO_number == 0)) {
    if (rep_err) {
      sprintf(&parserCtx->errBUFF[0],"Cannot generate query plans for query '%s'.\n", parserCtx->queryText);
      return (!rep_err);
    }
    else {
      if (query_type == CO) {
        *query_end_num = CO_number;
      }
      else {
        *query_end_num = CAS_number;
      }
      return query_type;
    }
  }
  else {
    return FALSE;
  }
}
