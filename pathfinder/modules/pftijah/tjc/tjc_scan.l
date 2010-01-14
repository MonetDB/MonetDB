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
* Copyright (C) 2006-2010 "University of Twente".
*
* Portions created by the "CWI" are
* Copyright (C) 2008-2010 "CWI".
*
* All Rights Reserved.
* 
* Author(s): Henning Rode 
*            Jan Flokstra
*
*/

%option noyywrap
%option never-interactive
%option prefix="tjc"
%option nounput
%{
#include <pf_config.h>

#include <stdlib.h>

#include "tjc_abssyn.h"
/* import tokens from parser specification */
#include "tjc_parse.tab.h"

#define yylex tjclex

#define YY_NO_INPUT

#define OUTSIDE_PF

#ifdef OUTSIDE_PF /* JF */

FILE* pf_in = NULL;
char* pf_input = NULL;
int   inputpos;
int   cur_col;
int   cur_row;
char  instring;

#endif

/**
 * define DEBUG_SCANNER to activate debugging output in parser
 * (if set, every token received is printed to stderr)
 */
#ifdef DEBUG_SCANNER
#define tj_yield(t) printf ("token: %s\n", #t); return (t)
#else
#define tj_yield(t) return (t)
#endif

#ifndef YY_INPUT
#define YY_INPUT(buf,result,max_size) \
	if ( ((result = fread( buf, 1, max_size, yyin )) == 0) \
		  && ferror( yyin ) ) \
		YY_FATAL_ERROR( "input in tijah flex scanner failed" );
#endif

#define ECHO	/* empty -- don't echo anything (we shouldn't get there anyway */

static void
gotoState (int s)
{
    BEGIN (s);
}

/**
 * Initialize lexical scanner before parsing the in-memory string
 * pointed to by @a buf.
 */
void
TJscanner_init (char *buf)
{
    /*
     * In case yyin/pf_in is not set, initialize it to /dev/null (Unix)
     * resp. C:nul (Windows); otherwise it gets initialized to stdin,
     * which might conflict with Mserver's console:
     * The lexer eventually calls fileno() for yyin/pf_in, which in
     * case of yyin/pf_in=stdin might be blocked as there is a waiting
     * select/read on the console(=stdin)...
     */
    if (!pf_in) {
#ifdef __MINGW32__              /* or any kind of native Windows */
        pf_in = fopen("C:nul","r");
#else
        pf_in = fopen("/dev/null","r");
#endif
    }
    if (pf_input) {
        YY_FLUSH_BUFFER;
        gotoState(0);
    }

    pf_input  = buf;
    inputpos = 0;
    cur_col  = 0;
    cur_row  = 1;
    instring = '\0';
}

%}

/* lexical states */
/*   1 PATH                            */
/*   2 PREDICATE                       */
/*   3 QUERY                           */

%x PATH
%x PREDICATE
%x QUERY

/*
 * XQuery 1.0 syntactic constructs
 *
 * lex/flex currently only supports 8 bit charsets. The character classes
 * are thus limited to the lower 8 bits.
 */

/* XML Character Classes (http://www.w3.org/TR/REC-xml/#CharClasses) */

/* see http://www.w3.org/TR/REC-xml/#NT-Letter */
Letter                           {BaseChar}|{Ideographic}

/* see http://www.w3.org/TR/REC-xml/#NT-BaseChar */
OneByteBaseChar                  [\x41-\x5a\x61-\x7a]
TwoByteBaseChar                  \xc3[\x80-\x96\x98-\xb6\xb8-\xbf]|\xc4[\x80-\xb1\xb4-\xbe]|\xc5[\x81-\x88\x8a-\xbe]|\xc6[\x80-\xbf]|\xc7[\x80-\x83\x8d-\xb0\xb4\xb5\xba-\xbf]|\xc8[\x80-\x97]|\xc9[\x90-\xbf]|\xca[\x80-\xa8\xbb-\xbf]|\xcb[\x80\x81]|\xce[\x86\x88-\x8a\x8c\x8e-\xa1\xa3-\xbf]|\xcf[\x80-\x8e\x90-\x96\x9a\x9c\x9e\xa0\xa2-\xb3]|\xd0[\x81-\x8c\x8e-\xbf]|\xd1[\x80-\x8f\x91-\x9c\x9e-\xbf]|\xd2[\x80\x81\x90-\xbf]|\xd3[\x80-\x84\x87\x88\x8b\x8c\x90-\xab\xae-\xb5\xb8\xb9]|\xd4[\xb1-\xbf]|\xd5[\x80-\x96\x99\xa1-\xbf]|\xd6[\x80-\x86]|\xd7[\x90-\xaa\xb0-\xb2]|\xd8[\xa1-\xba]|\xd9[\x81-\x8a\xb1-\xbf]|\xda[\x80-\xb7\xba-\xbe]|\xdb[\x80-\x8e\x90-\x93\x95\xa5\xa6]
ThreeByteBaseChar                \xe0(\xa4[\x85-\xb9\xbd]|\xa5[\x98-\xa1]|\xa6[\x85-\x8c\x8f-\x90\x93-\xa8\xaa-\xb0\xb2\xb6-\xb9]|\xa7[\x9c\x9d\x9f-\xa1\xb0\xb1]|\xa8[\x85-\x8a\x8f\x90\x93-\xa8\xaa-\xb0\xb2\xb3\xb5\xb6\xb8\xb9]|\xa9[\x99-\x9c\x9e\xb2-\xb4]|\xaa[\x85-\x8b\x8d\x8f-\x91\x93-\xa8\xaa-\xb0\xb2\xb3\xb5-\xb9\xbd]|\xab\xa0|\xac[\x85-\x8c\x8f\90\x93-\xa8\xaa-\xb0\xb2\xb3\xb6-\xb9\xbd]|\xad[\x9c\x9d\x9f-\xa1]|\xae[\x85-\x8a\x8e-\x90\x92-\x95\x99-\x9a\x9c\x9e\x9f\xa3\xa4\xa8-\xaa\xae-\xb5\xb7-\xb9]|\xb0[\x85-\x8c\x8e\x90\x92-\xa8\xaa-\xb3\xb5-\xb9]|\xb1[\xa0\xa1]|\xb2[\x85-\x8c\x8e-\x90\x92-\xa8\xaa-\xb3\xb5-\xb9]|\xb3[\x9e\xa0\xa1]|\xb4[\x85-\x8c\x8e-\x90\x92-\xa8\xaa-\xb9]|\xb5[\xa0\xa1]|\xb8[\x81-\xae\xb0\xb2\xb3]|\xb9[\x80-\x85]|\xba[\x81\x82\x84\x87\x88\x8a\x8d\x94-\x97\x99-\x9f\xa1-\xa3\xa5\xa7\xaa\xab\xad-\xae\xb0\xb2\xb3\xbd]|\xbb[\x80-\x84]|\xbd[\x80-\x87\x89-\xa9])|\xe1(\x82[\xa0-\xbf]|\x83[\x80-\x85\x90-\xb6]|\x84[\x80\x82\x83\x85-\x87\x89\x8b\x8c\x8e-\x92\xbc\xbe]|\x85[\x80\x8c\x8e\x90\x94\x95\x99\x9f-\xa1\xa3\xa5\xa7\xa9\xad\xae\xb2\xb3\xb5]|\x86[\x9e\xa8\xab\xae\xaf\xb7\xb8\xba\xbc-\xbf]|\x87[\x80-\x82\xab\xb0\xb9]|\xb8[\80-\xbf]|\xb9[\x80-\xbf]|\xba[\x80-\x9b\xa0-\xbf]|\xbb[\x80-\xb9]|\xbc[\x80-\x95\x98-\x9d\xa0-\xbf]|\xbd[\x80-\x85\x88-\x8d\x90-\x97\x99\x9b\x9d\x9f-\xbd]|\xbe[\x80-\xb4\xb6-\xbc\xbe]|\xbf[\x82-\x84\x86-\x8c\x90-\x93\x96-\x9b\xa0-\xac\xb2\xb4\xb6-\xbc])|\xe2(\x84[\xa6\xaa\xab\xae]|\x86[\x80-\x82])|\xe3(\x81[\x81-\xbf]|\x82[\x80-\x94\xa1-\xbf]|\x83[\x80-\xba]|\x84[\x85-\xac])|\xea[\xb0-\xbf][\x80-\xbf]|[\xeb\xec][\x80-\xbf][\x80-\xbf]|\xed([\x80-\x9d][\x80-\xbf]|\x9e[\x80-\xa3])

BaseChar                         {OneByteBaseChar}|{TwoByteBaseChar}|{ThreeByteBaseChar}

/* see http://www.w3.org/TR/REC-xml/#NT-Ideographic */
Ideographic                      \xe4[\xb8-\xbf][\x80-\xbf]|[\xe5-\xe8][\x80-\xbf][\x80-\xbf]|\xe9[\x80-\xbd][\x80-\xbf]|\xe9\xbe[\x80-\xa5]|\xe3\x80[\x87\xa1-\xa9]

/* see http://www.w3.org/TR/REC-xml/#NT-CombiningChar */
CombiningChar                    \xcc[\x80-\xbf]|\xcd[\x80-\x85\xa0\xa1]|\xd2[\x83-\x86]|\xd6[\x91-\xa1\xa3-\xb9\xbb-\xbd\xbf]|\xd7[\x81\x82\x84]|\xd9[\x8b-\x92\xb0]|\xdb[\x96-\xa4\xa7\xa8\xaa-\xad]|\xe0(\xa4[\x81-\x83\xbc\xbe\xbf]|\xa5[\x80-\x8d\x91-\x94\xa2\xa3]|\xa6[\x81-\x83\xbc\xbe\xbf]|\xa7[\x80-\x84\x87\x88\x8b-\x8d\x97\xa2\xa3]|\xa8[\x82\xbc\xbe\xbf]|\xa9[\x80-\x82\x87\x88\x8b-\x8d\xb0\xb1]|\xaa[\x81-\x83\xbc\xbe\xbf]|\xab[\x80-\x85\x87-\x89\x8b-\x8d]|\xac[\x81-\x83\xbc\xbe\xbf]|\xad[\x80-\x83\x87\x88\x8b-\x8d\x96\x97]|\xae[\x82\x83\xbe\xbf]|\xaf[\x80-\x82\x86-\x88\x8a-\x8d\x97]|\xb0[\x81-\x83\xbe\xbf]|\xb1[\x80-\x84\x86-\x88\x8a-\x8d\x95\x96]|\xb2[\x82\x83\xbe\xbf]|\xb3[\80-\x84\x86-\x88\x8a-\x8d\x95\x96]|\xb4[\x82\x83\xbe\xbf]|\xb5[\x80-\x83\x86-\x88\x8a-\x8d\x97]|\xb8[\xb1\xb4-\xba]|\xb9[\x87-\x8e]|\xba[\xb1\xb4-\xb9\xbb\xbc]|\xbb[\x88-\x8d]|\xbc[\x98\x99\xb5\xb7\xb9\xbe\xbf]|\xbd[\xb1-\xbf]|\xbe[\x80-\x84\x86-\x8b\x90-\x95\x97\x99-\xad\xb1-\xb7\xb9])|\xe2(\x83[\x90-\x9c\xa1])|\xe3(\x80[\xaa-\xaf]|\x82[\x99\x9a])

/* see http://www.w3.org/TR/REC-xml/#NT-Extender */
Extender                         \xc2\xb7|\xcb[\x90\x91]|\xce\x87|\xd9\x80|\e0[\xb9\xbb]\x86|\xe3\x80[\x85\xb1-\xb5]|\xe3\x82[\x9d\x9e]|\xe3\x83[\xbc-\xbe]

/* see http://www.w3.org/TR/REC-xml/#NT-Digit */
XMLDigit                         [\x30-\x39]|\xd9[\xa0-\xa9]|\xdb[\xb0-\xb9]|\xe0[\xa5\xa7\xa9\xab\xad\xb1\xb3\xb5][\xa6-\xaf]|\xe0\xaf[\xa7-\xaf]|\xe0[\xb9\xbb][\x90-\x99]|\xe0\xbc[\xa0-\xa9]

/* Digit according to the XQuery definition */
Digit                            [\x30-\x39]
Digits                           {Digit}+

/* XQuery WD [141] */
IntegerLiteral                   {Digits}
/* XQuery WD [142] */
DecimalLiteral                   ("."{Digits})|({Digits}"."[0-9]*)
/* XQuery WD [143] */
DoubleLiteral                    (("."{Digits})|({Digits}("."[0-9]*)?))[eE][-+]?{Digits}
/* XQuery WD [144] */
StringLiteral                    ("\""({PredefinedEntityRef}|{CharRef}|"\"\""|{MinimalChar}|[-'{}<#)])*"\"")|("'"({PredefinedEntityRef}|{CharRef}|"''"|{MinimalChar}|[-"{}<#)])*"'")
/* XQuery WD [145] */
PredefinedEntityRef              "&"("lt"|"gt"|"amp"|"quot"|"apos")";"


/* http://www.w3.org/TR/REC-xml/#NT-NameChar */
NameChar                         {Letter}|{XMLDigit}|"."|"-"|"_"|":"|{CombiningChar}|{Extender}

/* XQuery WD [153] or http://www.w3.org/TR/REC-xml/#NT-CharRef */
CharRef                          ("&#"[0-9]+";")|("&#x"[0-9a-fA-F]+";")

/* XQuery WD [154]: QName: http://www.w3.org/TR/REC-xml-names/#NT-QName */
QName                            ({Prefix}":")?{LocalPart}
Prefix                           {NCName}
LocalPart                        {NCName}

/* XQuery WD [155]: An XML Name, minus the ":"
   see http://www.w3.org/TR/REC-xml-names/#NT-NCName */
NCName                           ({Letter}|"_"){NCNameChar}*
NCNameChar                       {Letter}|{Digit}|"."|"-"|"_"|{CombiningChar}|{Extender}

/* XQuery WD [156]: Whitespace http://www.w3.org/TR/REC-xml/#NT-S */
S                                {WhiteSpaceChar}+
WhiteSpaceChar                   [\x20\x09\x0d\x0a]

/* XQuery WD [157]: http://www.w3.org/TR/REC-xml/#NT-Char */
/* all characters, except {}<&'"- (in other words: Char - [-{}<'"#)] */
MinimalChar                      [\x09\x0a\x0d\x20\x21\x24-\x25\x28\x2a-\x2c\x2e-\x3b\x3d-\x7a\x7c\x7e\x7f]|[\xc0-\xdf][\x80-\xbf]|[\xe0-\xef][\x80-\xbf][\x80-\xbf]|[\xf0-\xf7][\x80-\xbf][\x80-\xbf][\x80-\xbf]
Char                             {MinimalChar}|[-{}<&'"#)]

AlphaNum			 {AlphaNumChar}+

AlphaNumChar			 {Letter}|{Digit}|{CharRef}|[_]


/* whitespace/comments in compound keywords */
_                                {S}

%%

<INITIAL,PATH>{
"."			{ tj_yield (dot); }
","			{ gotoState(QUERY); tj_yield (comma); }
"*"			{ tj_yield (star); }
"//"			{ tj_yield (slash_slash); }
"|"			{ tj_yield (pipe_); }
"["			{ gotoState(PREDICATE); tj_yield (lbracket); }
"("			{ tj_yield (lparen); }
")"			{ tj_yield (rparen); }
{NCName}		{ tjclval.str = TJstrndup(yytext); tj_yield (TAG); }
}

<PREDICATE>{
"("			{ tj_yield (lparen); }
")"			{ tj_yield (rparen); }
"or"			{ tj_yield (or); }
"and"			{ tj_yield (and); }
"about"			{ gotoState(PATH); tj_yield (about); }
"]"			{ gotoState(PATH); tj_yield (rbracket); }
}

<QUERY>{
")"			{ gotoState(PREDICATE); tj_yield (rparen); }
{AlphaNum}		{ tjclval.str = TJstrndup(yytext); tj_yield (TERM); }
{NCName}":"{AlphaNum}	{ tjclval.str = TJstrndup(yytext); tj_yield (ENTITY); }
"{"{DecimalLiteral}"}"	{ tjclval.dbl = atof (TJsubstrndup(yytext, 1, yyleng - 2));
			  tj_yield (WEIGHT); }
"+"			{ tj_yield (plus); }
"-"			{ tj_yield (minus); }
"'"			{ tj_yield (apos); }
"\""			{ tj_yield (quote); }
}

%%

int
yyerror (char const *message)
{
   return fprintf (stderr, "%s\n", message);
}

void setTJCscanstring(const char *s) { tjc_scan_string(s); }


/* vim:set shiftwidth=4 expandtab: */

