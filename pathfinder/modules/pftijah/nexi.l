%option noyywrap
%option never-interactive
%option prefix="nexi"
%option nounput
%{
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
 * Flex script to tokenize NEXI queries and to build logical and physical query plan
 */

#include <pf_config.h>

#include <string.h>
#include "nexi.h"
#include "nexi.tab.h"

int c;
extern YYSTYPE nexilval;
extern int line_number;
extern int char_number;

#define SELECT_NODE 1
#define SELECT_TERM 2
#define UNION 6
#define INTERSECT 7

#define CURRENT 21
#define DSC 22
#define STAR 23
#define PARAMNODES 99
#define VAGUE 25
#define IMAGE 26

#define GR 31
#define LS 32
#define EQ 33

#define ABOUT 41
#define OPEN 42
#define CLOSE 43
#define CTX 44
#define COMMA 45
#define OB 46
#define CB 47
#define QUOTE 48
#define STRUCT_OR 49

#define PLUS 72
#define MINUS 73

#define IMAGE_ABOUT 86

#define QUERY_END 100

#define FALSE 0
#define TRUE 1

#define WRITE_COMMAND(COMMAND) tnl_append(&parserCtx->command_preLIST,COMMAND)
#define WRITE_TOKEN(TOKEN)     tsl_append(&parserCtx->token_preLIST,TOKEN)

/* This define is to prevent problems with flex 2.5.33 and lower on Debian and
 * Gentoo systems. When flex.2.5.4 and hiher is obligatory this define
 * may be removed
 */
#ifndef YY_INPUT
#define YY_INPUT(buf,result,max_size) \
	if ( ((result = fread( buf, 1, max_size, yyin )) == 0) \
		  && ferror( yyin ) ) \
		YY_FATAL_ERROR( "input in flex scanner failed" );
#endif

%}

LETTER [a-zA-Z]
DIGIT [0-9]
LETTERDIGIT [a-zA-Z0-9]
LETTERDIGITEXTRAS [a-zA-Z0-9'\-]
XML_NAMECHAR [a-zA-Z0-9_:.\-]
XML_NAME [a-zA-Z:_]
IMAGE_PATH [a-zA-Z0-9/_.\-]

%%

"\'" {    // Ignore quotes for now
          char_number++;
     }

" " { char_number++; }

";" {   // Ignore semicolon
        char_number++; 
    }

"\r" { char_number++; }

"\n"    {
          line_number++;
          char_number = 1;
	  //WRITE_COMMAND(QUERY_END);
          //return yytext[0];
        }

"about" {
          char_number += 5;
          nexilval = yytext[0];
	  WRITE_COMMAND(ABOUT);
	  WRITE_TOKEN(yytext);
          return Q_ABOUT;
        }

"and"   {
          char_number += 3;
          nexilval = yytext[0];
	  WRITE_COMMAND(INTERSECT);
	  WRITE_TOKEN(yytext);
          return Q_AND;
        }

"AND"   {
          char_number += 3;
          nexilval = yytext[0];
	  WRITE_COMMAND(INTERSECT);
	  WRITE_TOKEN(yytext);
          return Q_AND;
        }

"or"    {
          char_number += 2;
          nexilval = yytext[0];
	  WRITE_COMMAND(UNION);
	  WRITE_TOKEN(yytext);
          return Q_OR;
        }

"OR"    {
          char_number += 2;
          nexilval = yytext[0];
	  WRITE_COMMAND(UNION);
	  WRITE_TOKEN(yytext);
          return Q_OR;
        }

"src:"{IMAGE_PATH}+  {
          char_number += strlen(yytext);
          nexilval = yytext[0];
	  WRITE_COMMAND(IMAGE_ABOUT);
	  WRITE_TOKEN(yytext+4);
          return Q_IMAGE;
        }

">"    {
          char_number++;
          nexilval = yytext[0];
	  WRITE_COMMAND(GR);
          return Q_GREATER;
        }

"<"    {
          char_number++;
          nexilval = yytext[0];
	  WRITE_COMMAND(LS);
          return Q_LESS;
        }

"="    {
          char_number++;
          nexilval = yytext[0];
	  WRITE_COMMAND(EQ);
          return Q_EQUAL;
        }

{LETTER}{LETTERDIGITEXTRAS}* {
          char_number += strlen(yytext);
	  char* token = (char*)yytext;
          nexilval = yytext[0];
	  WRITE_COMMAND(SELECT_NODE);
	  /* INCOMPLETE, what to do with 0 or >1 tokens? */
          // Token normalisation should not be done here:
          // at this level, we can't tell the difference between query terms and element names!
	  //token = normalizeTerm(parserCtx->tjCtx,token);
	  WRITE_TOKEN(token);
          return ALPHANUMERIC;
        }

"-"?{DIGIT}+ {
          char_number += strlen(yytext);
          nexilval = yytext[0];
	  WRITE_COMMAND(SELECT_NODE);
	  WRITE_TOKEN(yytext);
          return NUMBER;
        }

"//"    {
          char_number += 2;
          nexilval = yytext[0];
	  WRITE_COMMAND(DSC);
          return NODE_QUALIFIER;
        }


{XML_NAME}{XML_NAMECHAR}* {
          char_number += strlen(yytext);
          nexilval = yytext[0];
	  WRITE_COMMAND(SELECT_NODE);
	  WRITE_TOKEN(yytext);
          return XMLTAG;
        }


","    {
          char_number += 1;
          nexilval = yytext[0];
	  WRITE_COMMAND(COMMA);
          return yytext[0];
        }

"\""   {
          char_number += 1;
          nexilval = yytext[0];
	  WRITE_COMMAND(QUOTE);
          return yytext[0];
        }


"["    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(OPEN);
	  return yytext[0];
	}

"]"    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(CLOSE);
	  return yytext[0];
	}

"("    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(OB);
	  return yytext[0];
	}

")"    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(CB);
	  return yytext[0];
	}

"|"    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(STRUCT_OR);
	  return yytext[0];
	}

"*"    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(STAR);
	  return yytext[0];
	}

"%"    {
	  char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(PARAMNODES);
	  return yytext[0];
	}

"."     {
          char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(CURRENT);
          return yytext[0];
        }

"-"     {
          char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(MINUS);
          return yytext[0];
        }

"+"     {
          char_number++;
	  nexilval = yytext[0];
	  WRITE_COMMAND(PLUS);
          return yytext[0];
        }

"~"   {
	char_number++;
	nexilval = yytext[0];
	WRITE_COMMAND(VAGUE);
	return yytext[0];
      }

.     {
          char_number++;
          return yytext[0];
        }

%%

void setNEXIscanstring(const char *s) { yy_scan_string(s); }
