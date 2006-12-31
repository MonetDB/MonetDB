/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/* Hey Emacs, this is -*-C-*- */

/**
 * @file
 *
 * XQuery (W3C WD October 29, 2004) terminals and lexer state machine.
 * Feed this into `flex'.
 *
 *
 * Copyright Notice:
 * -----------------
 *
 * The contents of this file are subject to the Pathfinder Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/PathfinderLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
 * the License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the Pathfinder system.
 *
 * The Original Code has initially been developed by the Database &
 * Information Systems Group at the University of Konstanz, Germany and
 * is now maintained by the Database Systems Group at the Technische
 * Universitaet Muenchen, Germany.  Portions created by the University of
 * Konstanz and the Technische Universitaet Muenchen are Copyright (C)
 * 2000-2005 University of Konstanz and (C) 2005-2006 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$
 */

%{

/*
#define DEBUG_SCANNER
#define DEBUG_LEXSTATE
*/

/**
 * This is a implementation of the W3C document ``XQuery 1.0:
 * An XML Query Language'', W3C Working Draft October 29, 2004.
 */

#include "pathfinder.h"

/* atoi, atof, strtoul */
#include <stdlib.h>
#include <assert.h>
/* strncmp */
#include <string.h>
/* isspace */
#include <ctype.h>

/* import XQuery abstract syntax definitions */
#include "abssyn.h"
/* arrays for lexical stack */
#include "array.h"
/* PFstrdup */
#include "mem.h"
/* PFoops */
#include "oops.h"

#include "scanner.h"

/* override YYLTYPE definition in "parser.tab.h" */
#define YYLTYPE PFloc_t

/* import tokens from parser specification */
#include "parser.tab.h"

/* bison 1.33 does not include this line in XQuery.tab.h. So we do it here. */
extern YYLTYPE pflloc;

/* input buffer */
extern char *pfinput;

/* current byte position within the buffer */
unsigned int inputpos;

/* current line and column within the query */
unsigned int cur_row;
unsigned int cur_col;

/**
 * keep track of whether we are within a string
 * (and don't ignore comments there)
 *
 * If we see an "opening" quote (single ' or double "),
 * store it here in instring.  Reset it to \0 when we
 * find the matching "closing" quote.
 */
static char instring;

/** how many spaces make up one tabstop */
#define TAB_STEP 8

/**
 * Executed (in the flex-generated scanner) each time a rule matches,
 * after yytext and yyleng were set, but before the action is
 * triggered.  Used to calculate length of a token and to increment
 * the line number in case of newline commands.
 */ 
#define YY_USER_ACTION                                      \
 do {                                                       \
   pflloc.first_col = pflloc.last_col + 1;                  \
   pflloc.first_row = pflloc.last_row;                      \
                                                            \
   pflloc.last_col  = cur_col;                              \
   pflloc.last_row  = cur_row;                              \
 } while(0);

/**
 * Lexical states in which we skip comments.  Any comment (possibly
 * nested) is returned as a single space in these states.
 */
#define IGNORE_COMMENTS \
    (!instring                                \
     && (YY_START == INITIAL                  \
         || YY_START == DEFAULT               \
         || YY_START == OPERATOR              \
         || YY_START == NAMESPACEDECL         \
         || YY_START == NAMESPACEKEYWORD      \
         || YY_START == VARNAME               \
         || YY_START == SINGLETYPE            \
         || YY_START == ITEMTYPE              \
         || YY_START == OCCURRENCEINDICATOR   \
         || YY_START == KINDTEST              \
         || YY_START == KINDTESTFORPI         \
         || YY_START == PRESERVE_MODE         \
         || YY_START == INHERIT_MODE          \
         || YY_START == NEXT_PRAGMA))         \

/**
 * Interface function for flex.  Skip (possibly nested) comments
 * as we go if we are in one of the states listed above.  Return
 * a single space for any comment we read, in compliance with
 * Section A.2.4.1 of the W3C XQuery specs (that is, the expression
 *
 *     foo(: comment )- bar
 *
 * is to be parsed as `foo - bar', i.e., "foo minus bar", not the
 * QName `foo-bar').
 *
 * While we are at it, also count current position within the query
 * file for helpful error messages.
 */
#define YY_INPUT(buf,result,max_size)                                    \
 {                                                                       \
     int level = 0;                                                      \
     do {                                                                \
         if (pfinput[inputpos] == 0) {                                   \
             result = YY_NULL;                                           \
             if (level > 0)                                              \
                 PFoops (OOPS_PARSE, "unterminated comment");            \
             break;                                                      \
         }                                                               \
                                                                         \
         /* update position information */                               \
         if (pfinput[inputpos] == '\n') {                                \
             cur_col = 0;                                                \
             cur_row++;                                                  \
         }                                                               \
                                                                         \
         /* Don't ignore comments in strings */                          \
         if (level == 0                                                  \
             && (pfinput[inputpos] == '\'' || pfinput[inputpos] == '"')) \
             instring = IGNORE_COMMENTS                                  \
                 ? pfinput[inputpos]                                     \
                 : (instring == pfinput[inputpos]) ? '\0' : instring;    \
                                                                         \
         if (IGNORE_COMMENTS                                             \
                  && pfinput[inputpos] == '('                            \
                  && pfinput[inputpos + 1] == ':') {                     \
             inputpos += 2;                                              \
             level++;                                                    \
             cur_col += 2;                                               \
         }                                                               \
         else if (IGNORE_COMMENTS                                        \
                  && level > 0                                           \
                  && pfinput[inputpos] == ':'                            \
                  && pfinput[inputpos + 1] == ')') {                     \
             inputpos += 2;                                              \
             level--;                                                    \
             cur_col += 2;                                               \
                                                                         \
             /* a comment behaves like whitespace (XQuery A.2.4.1) */    \
             if (level == 0) {                                           \
                 buf[0] = ' ';                                           \
                 result = 1;                                             \
                 break;                                                  \
             }                                                           \
         }                                                               \
         else {                                                          \
             cur_col++;                                                  \
             buf[0] = pfinput[inputpos++];                               \
             result = 1;                                                 \
         }                                                               \
                                                                         \
     } while (level > 0);                                                \
 }

/**
 * define DEBUG_SCANNER to activate debugging output in parser
 * (if set, every token received is printed to stderr)
 */
#ifdef DEBUG_SCANNER
#define yield(t) PFlog ("token: " #t); return (t)
#else
#define yield(t) return (t)
#endif

/* lexical state stack */
static PFarray_t *lexical_stack = NULL;
static unsigned int lexical_sp = 0;

static void
popState (void)
{
    if (lexical_sp == 0) {
        PFoops (OOPS_PARSE, "parsing problem: stack empty\n");
        PFexit(-1);
    }
        
#ifdef DEBUG_LEXSTATE
    fprintf (stderr, "popState: going to state %i\n",
                     *((int *) PFarray_at (lexical_stack, lexical_sp - 1)));
#endif

    lexical_sp--;
    BEGIN(*((int *) PFarray_at (lexical_stack, lexical_sp)));
}

static void
pushState (int s)
{
#ifdef DEBUG_LEXSTATE
    PFlog ("pushState: pushing state %i\n", s);
#endif

    *((int *) PFarray_at (lexical_stack, lexical_sp++)) = s;
}

static void
gotoState (int s)
{
#ifdef DEBUG_LEXSTATE
    PFlog ("gotoState: going to state %i\n", s);
#endif

    BEGIN (s);
}

/**
 * Initialize lexical scanner before parsing the in-memory string
 * pointed to by @a buf.
 */
void
PFscanner_init (char *buf)
{
    /* initialize lexical stack */
    lexical_sp = 0; 
    lexical_stack = PFarray (sizeof (int));

    /*
     * In case yyin/pfin is not set, initialize it to /dev/null (Unix)
     * resp. C:nul (Windows); otherwise it gets initialized to stdin,
     * which might conflict with Mserver's console:
     * The lexer eventually calls fileno() for yyin/pfin, which in
     * case of yyin/pfin=stdin might be blocked as there is a waiting
     * select/read on the console(=stdin)...
     */
    if (!pfin) {
#ifdef __MINGW32__              /* or any kind of native Windows */
        pfin = fopen("C:nul","r");
#else
        pfin = fopen("/dev/null","r");
#endif
    }
    if (pfinput) {
        YY_FLUSH_BUFFER;
        gotoState(0);
    }

    *PFerrbuf = 0;
    pfinput  = buf;
    inputpos = 0;
    cur_col  = 0;
    cur_row  = 1;
    instring = '\0';
}

/* forward declarations */
static char * entity_ref (const char *s, char **endptr);
static char *extract_string (const char *s);
static PFqname_t extract_qname (const char *s, unsigned int index);
static char *extract_ncname (const char *s, unsigned int index);

%}

/* no wrapping after EOF; hence, no libfl (-lfl) needed */
%option noyywrap

/* we need the `unput' feature of flex */
%option unput

/*
 * We don't use the REJECT command and indicate this here. Unfortunately,
 * flex still includes the code to support REJECT, resulting in the compiler
 * warning `label `find_rule' defined but not used'.
 */
%option noreject

/* lexical states */
/*   1 DEFAULT                         */
/*   2 OPERATOR                        */
/*   3 NAMESPACEDECL                   */
/*   4 NAMESPACEKEYWORD                */
/*   5 VARNAME                         */
/*   6 SINGLETYPE                      */
/*   7 ITEMTYPE                        */
/*   8 OCCURRENCEINDICATOR             */
/*   9 KINDTEST                        */
/*  10 KINDTESTFORPI                   */
/*  11 START_TAG                       */
/*  12 QUOT_ATTRIBUTE_CONTENT          */
/*  13 APOS_ATTRIBUTE_CONTENT          */
/*  14 ELEMENT_CONTENT                 */
/*  15 XML_COMMENT                     */
/*  16 PROCESSING_INSTRUCTION          */
/*  17 PROCESSING_INSTRUCTION_CONTENT  */
/*  18 CDATA_SECTION                   */
/*  19 END_TAG                         */
/*  20 PRESERVE_MODE                   */
/*  21 INHERIT_MODE                    */
/*  22 PRAGMA_TARGET                   */
/*  23 PRAGMA_CONTENT                  */
/*  24 NEXT_PRAGMA                     */

%x DEFAULT
%x OPERATOR
%x NAMESPACEDECL
%x NAMESPACEKEYWORD
%x VARNAME
%x SINGLETYPE
%x ITEMTYPE
%x OCCURRENCEINDICATOR
%x KINDTEST
%x KINDTESTFORPI
%x START_TAG
%x QUOT_ATTRIBUTE_CONTENT
%x APOS_ATTRIBUTE_CONTENT
%x ELEMENT_CONTENT
%x XML_COMMENT
%x PROCESSING_INSTRUCTION
%x PROCESSING_INSTRUCTION_CONTENT
%x CDATA_SECTION
%x END_TAG
%x PRESERVE_MODE
%x INHERIT_MODE
%x PRAGMA_TARGET
%x PRAGMA_CONTENT
%x NEXT_PRAGMA

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

/* XQuery WD [148]: Char - [{}<&] */
ElementContentChar               {MinimalChar}|[-'"#)]
/* XQuery WD [149]: Char - [{}<&"] */
QuotAttrContentChar              {MinimalChar}|[-'#)]
/* XQuery WD [150]: Char - [{}<&'] */
AposAttrContentChar              {MinimalChar}|[-"#)]

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


/* XQuery WD [152] http://www.w3.org/TR/REC-xml/#NT-PITarget
   !W3C: This pattern also matches "xml".
   We are catching "xml" in parser.y */
PITarget                         ({Letter}|"_"|":"){NameChar}*
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

PragmaContentChar                {MinimalChar}|[-{}<'"]
PragmaContent                    ({PragmaContentChar}|"#"({PragmaContentChar}|"#")|({PragmaContentChar}|")")")")*

/* whitespace/comments in compound keywords */
_                                {S}

%%

<INITIAL,DEFAULT>{

"xquery"{_}"version"               { yield (xquery_version); }

"module"{_}"namespace"             { gotoState (NAMESPACEDECL);
                                     yield (module_namespace); }

";"                                { yield (semicolon); }

"declare"{_}"namespace"            { gotoState (NAMESPACEDECL);
                                     yield (declare_namespace); }

"declare"{_}"boundary-space"{_}"preserve" {
                                     yield (declare_boundary_space_preserve); }
"declare"{_}"boundary-space"{_}"strip"   {
                                     yield (declare_boundary_space_strip); }

"declare"{_}"default"{_}"element"  { gotoState (NAMESPACEKEYWORD);
                                     yield (declare_default_element); }
"declare"{_}"default"{_}"function" { gotoState (NAMESPACEKEYWORD);
                                     yield (declare_default_function); }
"declare"{_}"ordering"{_}"ordered" { yield (declare_ordering_ordered); }
"declare"{_}"ordering"{_}"unordered" { yield (declare_ordering_unordered); }
"declare"{_}"default"{_}"order"    { yield (declare_default_order); }
"declare"{_}"copy-namespaces"      { gotoState (PRESERVE_MODE);
                                     yield (declare_copy_namespaces); }
"empty"{_}"greatest"               { yield (empty_greatest); }
"empty"{_}"least"                  { yield (empty_least); }
"declare"{_}"default"{_}"collation" { yield (declare_default_collation); }
"declare"{_}"base-uri"             { yield (declare_base_uri); }
"import"{_}"schema"                { gotoState (NAMESPACEKEYWORD);
                                     yield (import_schema); }
"import"{_}"module"                { gotoState (NAMESPACEKEYWORD);
                                     yield (import_module); }
"import"{_}"rpc-module"            { gotoState (NAMESPACEKEYWORD);
                                     yield (import_rpc_module); }
"at"{_}{StringLiteral}             { pflval.str = extract_string (yytext);
                                     yield (at_URILiteral);
                                   }
"declare"{_}"variable"{_}"$"       { gotoState (VARNAME);
                                     yield (declare_variable_dollar); }
"declare"{_}"construction"{_}"preserve" { yield (declare_construction_preserve); }
"declare"{_}"construction"{_}"strip" { yield (declare_construction_strip); }
"declare"{_}"function"             { yield (declare_function); }
"declare"{_}"updating"{_}"function" { yield (declare_updating_function); }
"declare"{_}"option"               { yield (declare_option); }
"declare"{_}"revalidation"{_}"strict" { yield (declare_revalidation_strict); }
"declare"{_}"revalidation"{_}"lax"    { yield (declare_revalidation_lax); }
"declare"{_}"revalidation"{_}"skip"   { yield (declare_revalidation_skip); }
"$"                                { gotoState (VARNAME); yield (dollar); }
")"                                { gotoState (OPERATOR); yield (rparen); }
"{"                                { yield (lbrace); }
"}"                                { popState (); yield (rbrace); }
","                                { yield (comma); }


"for"{_}"$"                        { gotoState (VARNAME); yield (for_dollar); }
"let"{_}"$"                        { gotoState (VARNAME); yield (let_dollar); }
"some"{_}"$"                       { gotoState (VARNAME); yield (some_dollar); }
"every"{_}"$"                      { gotoState (VARNAME); yield (every_dollar);}

"with"{_}"$"                       { /* Pathfinder extension: recursion */
                                     gotoState (VARNAME); yield (with_dollar); }

"typeswitch"{_}?"("/[^:]           { yield (typeswitch_lparen); }
"if"{_}?"("/[^:]                   { yield (if_lparen); }
"+"                                { yield (plus); /* unary plus */ }
"-"                                { yield (minus); /* unary minus */ }
"validate"{_}?"{"                  { pushState (OPERATOR);
                                     yield (validate_lbrace); }
"validate"{_}"lax"{_}?"{"          { pushState (OPERATOR);
                                     yield (validate_lax_lbrace); }
"validate"{_}"strict"{_}?"{"       { pushState (OPERATOR);
                                     yield (validate_strict_lbrace); }
"/"                                { yield (slash); }
"//"                               { yield (slash_slash); }
"child::"                          { yield (child_colon_colon); }
"descendant::"                     { yield (descendant_colon_colon); }
"attribute::"                      { yield (attribute_colon_colon); }
"self::"                           { yield (self_colon_colon); }
"descendant-or-self::"             { yield (descendant_or_self_colon_colon); }
"following-sibling::"              { yield (following_sibling_colon_colon); }
"following::"                      { yield (following_colon_colon); }
"@"                                { yield (atsign); }
"parent::"                         { yield (parent_colon_colon); }
"ancestor::"                       { yield (ancestor_colon_colon); }
"preceding-sibling::"              { yield (preceding_sibling_colon_colon); }
"preceding::"                      { yield (preceding_colon_colon); }
"ancestor-or-self::"               { yield (ancestor_or_self_colon_colon); }
"select-narrow::"                  { yield (select_narrow_colon_colon); }
"select-wide::"                    { yield (select_wide_colon_colon); }
"reject-narrow::"                  { yield (reject_narrow_colon_colon); }
"reject-wide::"                    { yield (reject_wide_colon_colon); }
".."                               { gotoState (OPERATOR); yield (dot_dot); }
"."                                { gotoState (OPERATOR); yield (dot); }
"*"                                { gotoState (OPERATOR); yield (star); }
{NCName}":*"                       { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (NCName_Colon_Star); }
"*:"{NCName}                       { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR); 
                                     yield (Star_Colon_NCName); }
"document-node"{_}?"("/[^:]        { pushState (OCCURRENCEINDICATOR);
                                     yield (document_node_lparen); }
"text"{_}?"("/[^:]                 { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (text_lparen); }
"comment"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (comment_lparen); }
"processing-instruction"{_}?"("/[^:] { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTESTFORPI);
                                     yield (pi_lparen); }
"attribute"{_}?"("/[^:]            { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (attribute_lparen); }
"schema-attribute"{_}?"("/[^:]     { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_attribute_lparen); }
"element"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (element_lparen); }
"schema-element"{_}?"("/[^:]       { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_element_lparen); }
"node"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (node_lrparens); }
"empty-sequence"{_}?"("{_}?")"     { gotoState (OPERATOR);
                                     yield (empty_sequence); }
"item"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (item_lrparens); }
"("                                { yield (lparen); }
"ordered"{_}?"{"                   { pushState (OPERATOR);
                                     yield (ordered_lbrace); }
"unordered"{_}?"{"                 { pushState (OPERATOR);
                                     yield (unordered_lbrace); }
"<"                                { pushState (OPERATOR);
                                     gotoState (START_TAG); yield (less_than); }
"document"{_}?"{"                  { pushState (OPERATOR);
                                     yield (document_lbrace); }
"element"{_}?"{"                   { pushState (OPERATOR); pushState (DEFAULT);
                                     yield (element_lbrace); }
"execute"{_}"at"                   { pushState (OPERATOR); pushState (DEFAULT);
                                     yield (execute_at); }
"attribute"{_}?"{"                 { pushState (OPERATOR); pushState (DEFAULT);
                                     yield (attribute_lbrace); }
"text"{_}?"{"                      { pushState (OPERATOR);
                                     yield (text_lbrace); }
"comment"{_}?"{"                   { pushState (OPERATOR);
                                     yield (comment_lbrace); }
"processing-instruction"{_}?"{"    { pushState (OPERATOR);
                                     yield (pi_lbrace); }
"element"{_}{QName}{_}?"{"         { pflval.qname = extract_qname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (Element_QName_LBrace);
                                   }
"attribute"{_}{QName}{_}?"{"       { pflval.qname = extract_qname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (Attribute_QName_LBrace);
                                   }
"processing-instruction"{_}{NCName}{_}?"{" {
                                     pflval.str = extract_ncname (yytext, 1);
                                     pushState (OPERATOR);
                                     yield (PI_NCName_LBrace);
                                   }

"<!--"                             { pushState (OPERATOR);
                                     gotoState (XML_COMMENT);
                                     yield (xml_comment_start); }
"<?"                               { pushState (OPERATOR);
                                     gotoState (PROCESSING_INSTRUCTION);
                                     yield (lt_question_mark); }
"<![CDATA["                        { pushState (OPERATOR);
                                     gotoState (CDATA_SECTION);
                                     yield (cdata_start); }

"(#"                               { gotoState (PRAGMA_TARGET);
                                     yield (paren_hash); }

"do"{_}"delete"                    { yield (do_delete); }
"do"{_}"insert"                    { yield (do_insert); }
"do"{_}"rename"                    { yield (do_rename); }
"do"{_}"replace"                   { yield (do_replace); }
"do"{_}"replace"{_}"value"{_}"of"  { yield (do_replace_value_of); }
"transform"{_}"copy"{_}"$"         { yield (transform_copy_dollar); }

{QName}{_}?"("/[^:]                { pflval.qname = extract_qname (yytext, 0);
                                     yield (QName_LParen);
                                   }
{IntegerLiteral}                   { pflval.num = atoll (yytext);
                                     gotoState (OPERATOR);
                                     yield (IntegerLiteral);
                                   }
{DecimalLiteral}                   { pflval.dec = atof (yytext);
                                     gotoState (OPERATOR);
                                     yield (DecimalLiteral);
                                   }
{DoubleLiteral}                    { pflval.dbl = atof (yytext);
                                     gotoState (OPERATOR);
                                     yield (DoubleLiteral);
                                   }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (OPERATOR);
                                     yield (StringLiteral);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<OPERATOR>{
"encoding"{_}{StringLiteral}       { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (encoding_StringLiteral);
                                   }

"as"                               { gotoState (ITEMTYPE); yield (as); }
":="                               { gotoState (DEFAULT); yield (colon_equals);}
"external"                         { gotoState (DEFAULT); yield (external_); }

"at"{_}"$"                         { gotoState (VARNAME); yield (at_dollar); }
"for"{_}"$"                        { gotoState (VARNAME); yield (for_dollar); }
"let"{_}"$"                        { gotoState (VARNAME); yield (let_dollar); }
"in"                               { gotoState (DEFAULT); yield (in_); }

"seeded"{_}"by"                    { /* Pathfinder extension: recursion */
                                     gotoState (DEFAULT); yield (seeded_by); }

"{"                                { pushState (OPERATOR);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
"}"                                { popState (); yield (rbrace); }
";"                                { gotoState (DEFAULT); yield (semicolon); }
","                                { gotoState (DEFAULT); yield (comma); }
"return"                           { gotoState (DEFAULT); yield (return_); }

"recurse"                          { /* Pathfinder extension: recursion */
                                     gotoState (DEFAULT); yield (recurse); }

"where"                            { gotoState (DEFAULT); yield (where); }
"order"{_}"by"                     { gotoState (DEFAULT); yield (order_by); }
"stable"{_}"order"{_}"by"          { gotoState (DEFAULT);
                                     yield (stable_order_by); }
"ascending"                        { yield (ascending); }
"descending"                       { yield (descending); }
"collation"                        { yield (collation); }
"empty"{_}"greatest"               { yield (empty_greatest); }
"empty"{_}"least"                  { yield (empty_least); }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     yield (StringLiteral);
                                   }
"satisfies"                        { gotoState (DEFAULT); yield (satisfies); }
")"                                { yield (rparen); }
"case"                             { gotoState (ITEMTYPE); yield (case_); }
"default"                          { yield (default_); }
"$"                                { gotoState (VARNAME); yield (dollar); }
"then"                             { gotoState (DEFAULT); yield (then); }
"else"                             { gotoState (DEFAULT); yield (else_); }
"or"                               { gotoState (DEFAULT); yield (or); }
"and"                              { gotoState (DEFAULT); yield (and); }
"to"                               { gotoState (DEFAULT); yield (to_); }
"union"                            { gotoState (DEFAULT); yield (union_); }
"|"                                { gotoState (DEFAULT); yield (pipe_); }
"intersect"                        { gotoState (DEFAULT); yield (intersect); }
"except"                           { gotoState (DEFAULT); yield (except); }
"+"                                { gotoState (DEFAULT); yield (plus); }
"-"                                { gotoState (DEFAULT); yield (minus); }
"*"                                { gotoState (DEFAULT); yield (star); }
"div"                              { gotoState (DEFAULT); yield (div_); }
"idiv"                             { gotoState (DEFAULT); yield (idiv); }
"mod"                              { gotoState (DEFAULT); yield (mod); }
"instance"{_}"of"                  { gotoState (ITEMTYPE); yield (instance_of);}
"treat"{_}"as"                     { gotoState (ITEMTYPE); yield (treat_as); }
"castable"{_}"as"                  { gotoState (SINGLETYPE);
                                     yield (castable_as); }
"cast"{_}"as"                      { gotoState (SINGLETYPE); yield (cast_as); }
"="                                { gotoState (DEFAULT); yield (equals); }
"!="                               { gotoState (DEFAULT); yield (excl_equals); }
"<"                                { gotoState (DEFAULT); yield (less_than); }
"<="                               { gotoState (DEFAULT);
                                     yield (less_than_equal); }
">"                                { gotoState (DEFAULT); yield (greater_than);}
">="                               { gotoState (DEFAULT);
                                     yield (greater_than_equal); }
"eq"                               { gotoState (DEFAULT); yield (eq); }
"ne"                               { gotoState (DEFAULT); yield (ne); }
"lt"                               { gotoState (DEFAULT); yield (lt); }
"le"                               { gotoState (DEFAULT); yield (le); }
"gt"                               { gotoState (DEFAULT); yield (gt); }
"ge"                               { gotoState (DEFAULT); yield (ge); }
"is"                               { gotoState (DEFAULT); yield (is); }
"<<"                               { gotoState (DEFAULT); yield (lt_lt); }
">>"                               { gotoState (DEFAULT); yield (gt_gt); }
"/"                                { gotoState (DEFAULT); yield (slash); }
"//"                               { gotoState (DEFAULT); yield (slash_slash); }
"["                                { gotoState (DEFAULT); yield (lbracket); }
"]"                                { yield (rbracket); }
"?"                                { yield (question_mark); }
"as"{_}"first"{_}"into"            { gotoState (DEFAULT);
                                     yield (as_first_into);}
"as"{_}"last"{_}"into"             { gotoState (DEFAULT); yield (as_last_into);}
"into"                             { gotoState (DEFAULT); yield (into);}
"after"                            { gotoState (DEFAULT); yield (after);}
"before"                           { gotoState (DEFAULT); yield (before);}
"with"                             { gotoState (DEFAULT); yield (with);}
"modify"                           { gotoState (DEFAULT); yield (modify);}
{_}                                ;
.                                  { yield (invalid_character); }
}

<NAMESPACEDECL>{
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (StringLiteral);
                                   }
{NCName}                           { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (NCName);
                                   }
"="                                { yield (equals); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<NAMESPACEKEYWORD>{
"namespace"                        { gotoState (NAMESPACEDECL);
                                     yield (namespace); }
"default"{_}"element"              { yield (default_element); }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     gotoState (DEFAULT);
                                     yield (StringLiteral);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<VARNAME>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
.                                  { yield (invalid_character); }
}

<SINGLETYPE>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OPERATOR);
                                     yield (QName);
                                   }
{_}                                ;
.                                  { yield (invalid_character); }
}

<ITEMTYPE>{
"$"                                { gotoState (VARNAME); yield (dollar); }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     gotoState (OCCURRENCEINDICATOR);
                                     yield (QName);
                                   }
"document-node"{_}?"("/[^:]        { pushState (OCCURRENCEINDICATOR);
                                     yield (document_node_lparen); }
"text"{_}?"("/[^:]                 { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (text_lparen); }
"comment"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (comment_lparen); }
"processing-instruction"{_}?"("/[^:] { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTESTFORPI);
                                     yield (pi_lparen); }
"attribute"{_}?"("/[^:]            { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (attribute_lparen); }
"schema-attribute"{_}?"("/[^:]     { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_attribute_lparen); }
"element"{_}?"("/[^:]              { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (element_lparen); }
"schema-element"{_}?"("/[^:]       { pushState (OCCURRENCEINDICATOR);
                                     gotoState (KINDTEST);
                                     yield (schema_element_lparen); }
"node"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (node_lrparens); }
"empty-sequence"{_}?"("{_}?")"     { gotoState (OPERATOR);
                                     yield (empty_sequence); }
"item"{_}?"("{_}?")"               { gotoState (OCCURRENCEINDICATOR);
                                     yield (item_lrparens); }
")"                                { gotoState (OCCURRENCEINDICATOR);
                                     yield (rparen); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<OCCURRENCEINDICATOR>{
"*"                                { gotoState (OPERATOR);
                                     yield (star); }
"?"                                { gotoState (OPERATOR);
                                     yield (question_mark); }
"+"                                { gotoState (OPERATOR);
                                     yield (plus); }
{_}                                ;
.                                  { unput (yytext[yyleng-1]);
                                     gotoState (OPERATOR); }
}

<KINDTEST>{
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
","                                { yield (comma); }
"?"                                { yield (question_mark); }
")"                                { popState (); yield (rparen); }
"*"                                { yield (star); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<KINDTESTFORPI>{
{NCName}                           { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (NCName);
                                   }
{StringLiteral}                    { pflval.str = extract_string (yytext);
                                     yield (StringLiteral);
                                   }
")"                                { popState (); yield (rparen); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<START_TAG>{
{QName}                            { pflval.qname 
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
{S}                                { yield (S); }
"="                                { yield (equals); }
"\""                               { gotoState (QUOT_ATTRIBUTE_CONTENT);
                                     yield (quot_); }
"'"                                { gotoState (APOS_ATTRIBUTE_CONTENT);
                                     yield (apos); }
"/>"                               { popState (); yield (slash_gt); }
">"                                { gotoState (ELEMENT_CONTENT);
                                     yield (greater_than); }
.                                  { yield (invalid_character); }
}

<QUOT_ATTRIBUTE_CONTENT>{
"\""                               { gotoState (START_TAG); yield (quot_); }
"\"\""                             { yield (EscapeQuot); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (QUOT_ATTRIBUTE_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
{QuotAttrContentChar}              { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (AttrContentChar);
                                   }
{PredefinedEntityRef}              { pflval.str = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.str = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
.                                  { yield (invalid_character); }
}

<APOS_ATTRIBUTE_CONTENT>{
"'"                                { gotoState (START_TAG);
                                     yield (apos); }
"''"                               { yield (EscapeApos); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (APOS_ATTRIBUTE_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
{AposAttrContentChar}              { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (AttrContentChar);
                                   }
{PredefinedEntityRef}              { pflval.str = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.str = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
.                                  { yield (invalid_character); }
}

<ELEMENT_CONTENT>{
"</"                               { gotoState (END_TAG); yield (lt_slash); }
"{{"                               { yield (lbrace_lbrace); }
"}}"                               { yield (rbrace_rbrace); }
"{"                                { pushState (ELEMENT_CONTENT);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
"<!--"                             { pushState (ELEMENT_CONTENT);
                                     gotoState (XML_COMMENT);
                                     yield (xml_comment_start); }
"<?"                               { pushState (ELEMENT_CONTENT);
                                     gotoState (PROCESSING_INSTRUCTION);
                                     yield (lt_question_mark); }
"<![CDATA["                        { pushState (ELEMENT_CONTENT);
                                     gotoState (CDATA_SECTION);
                                     yield (cdata_start); }
"<"                                { pushState (ELEMENT_CONTENT);
                                     gotoState (START_TAG); yield (less_than); }
{PredefinedEntityRef}              { pflval.str = entity_ref (yytext, NULL);
                                     yield (PredefinedEntityRef);
                                   }
{CharRef}                          { pflval.str = entity_ref (yytext, NULL);
                                     yield (CharRef);
                                   }
{ElementContentChar}               { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (ElementContentChar);
                                   }
.                                  { yield (invalid_character); }
}

<XML_COMMENT>{
"-->"                              { popState (); yield (xml_comment_end); }
{Char}                             { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<PROCESSING_INSTRUCTION>{
{S}                                { gotoState (PROCESSING_INSTRUCTION_CONTENT);
                                     yield (S); }
"?>"                               { popState (); yield (question_mark_gt); }
{PITarget}                         { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PITarget);
                                   }
.                                  { yield (invalid_character); }
}

<PROCESSING_INSTRUCTION_CONTENT>{
"?>"                               { popState (); yield (question_mark_gt); }
{Char}                             { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<CDATA_SECTION>{
"]]>"                              { popState (); yield (cdata_end); }
{Char}                             { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PFChar);
                                   }
.                                  { yield (invalid_character); }
}

<END_TAG>{
">"                                { popState (); yield (greater_than); }
{QName}                            { pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
{S}                                { yield (S); }
.                                  { yield (invalid_character); }
}

<PRESERVE_MODE>{
"preserve"                         { yield (copy_ns_preserve); }
"no-preserve"                      { yield (copy_ns_nopreserve); }
","                                { gotoState (INHERIT_MODE);
                                     yield (comma); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<INHERIT_MODE>{
"inherit"                          { yield (copy_ns_inherit); }
"no-inherit"                       { yield (copy_ns_noinherit); }
";"                                { gotoState (DEFAULT);
                                     yield (semicolon); }
{_}                                ;
.                                  { yield (invalid_character); }
}

<PRAGMA_TARGET>{
{S}                                { yield (S); }
{QName}                            { gotoState (PRAGMA_CONTENT);
                                     pflval.qname
                                         = PFstr_qname (PFstrndup (yytext,
                                                                   yyleng));
                                     yield (QName);
                                   }
.                                  { yield (invalid_character); }
}

<PRAGMA_CONTENT>{
{PragmaContent}                    { pflval.str = PFstrndup (yytext, yyleng);
                                     yield (PragmaContents); }
"#)"                               { gotoState (NEXT_PRAGMA);
                                     yield (hash_paren); }
.                                  { yield (invalid_character); }
}

<NEXT_PRAGMA>{
"(#"                               { gotoState (PRAGMA_TARGET);
                                     yield (paren_hash); }
"{"                                { pushState (OPERATOR);
                                     gotoState (DEFAULT);
                                     yield (lbrace); }
{_}                                ;
.                                  { yield (invalid_character); }
}
%%

static char *
entity_ref (const char *s, char **endptr)
{
    /* four characters max */
    char      *ret = memset (PFmalloc (4 * sizeof (char)), 0, 4);
    /* unicode value of the character we have to return */
    long int   code;

    assert (*s == '&');

    if (s[1] == '#') {
        /* this is a character reference (`&#') */
        if (s[2] == 'x') {
            code = strtol (s + 3, endptr, 16);
        } else {
            code = strtol (s + 2, endptr, 10);
        }

        if (code <= 0x0000007f)
            ret[0] = (char) code;
        else if (code <= 0x000007ff) {
            ret[0] = (char) ((code >> 6) | 0xc0);
            ret[1] = (char) ((code & 0x003f) | 0x80);
        }
        else if (code <= 0x0000ffff) {
            ret[0] = (char) ((code >> 12) | 0xe0);
            ret[1] = (char) (((code >> 6) & 0x003f) | 0x80);
            ret[2] = (char) ((code & 0x003f) | 0x80);
        }
        else
            PFoops_loc (OOPS_PARSE, pflloc, "illegal character entity");
    }
    else {
        /* this is a predefined entity */
        if (!strncmp (s + 1, "lt", 2)) {
            if (endptr)
                *endptr = (char *) s + 3;
            ret[0] = '<';
        }
        else if (!strncmp (s + 1, "gt", 2)) {
            if (endptr)
                *endptr = (char *) s + 3;
            ret[0] = '>';
        }
        else if (!strncmp (s + 1, "amp", 3)) {
            if (endptr)
                *endptr = (char *) s + 4;
            ret[0] = '&';
        }
        else if (!strncmp (s + 1, "quot", 4)) {
            if (endptr)
                *endptr = (char *) s + 5;
            ret[0] = '"';
        }
        else if (!strncmp (s + 1, "apos", 4)) {
            if (endptr)
                *endptr = (char *) s + 5;
            ret[0] = '\'';
        }
    }

    return ret;
}

/**
 * Extract a string literal from a pattern match.
 * The string may occur anywhere in the match. This function
 * also respects XQuery comments.
 */
static char *
extract_string (const char *s)
{
    unsigned int   start = 0;
    unsigned int   end = 0;
    PFarray_t     *ret = PFarray (sizeof (char));
    char          *endptr;
    char          *entity;
    unsigned int   i = 0;

    /* find first quote character */
    while (true) {

        assert (s[start]);

        if (s[start] != '"' && s[start] != '\'')
            start++;
        else
            break;
    }

    if (!s[start])
        return NULL;

    end = start;

    while (s[++end]) {
        /* test for quotes/apostrophs */
        if (s[end] == s[start]) {
            if (s[end + 1] && s[end] == s[end+1])
                *((char *) PFarray_at (ret, i++)) = s[end++];
            else
                return PFstrndup (ret->base, i);
        }
        /* test for entities */
        else if (s[end] == '&') {
            entity = entity_ref (s + end, &endptr);
            while (*entity) {
                *((char *) PFarray_at (ret, i++)) = *entity;
                entity++;
            }
            end = endptr - s;
        }
        /* everything else */
        else {
            *((char *) PFarray_at (ret, i++)) = s[end];
        }
    }
    PFoops (OOPS_PARSE, "Error in StringLiteral");
    assert(0); /* we should never come here! */
    return NULL; /* pacify picky compilers */
}

/**
 * Test if a character is an NC name char as defined by the W3C.
 * (See also the lex pattern `NCNameChar' above.)
 */
static bool
isncnamechar (const char c)
{
    return ((c >= '\x41' && c <= '\x5a')
            || (c >= '\x61' && c <= '\x7a')
            || (c >= '\xc0' && c <= '\xd6')
            || (c >= '\xd8' && c <= '\xf6')
            || (c >= '\xf8' && c <= '\xff')
            || (c >= '\x30' && c <= '\x39')
            || c == '.' || c == '-' || c == '_' || c == '\xb7');
}

/**
 * Extract word number @a index from a pattern match and return
 * it as a QName.
 */
static PFqname_t
extract_qname (const char *s, unsigned int index)
{
    unsigned int start = 0;
    unsigned int end;
    unsigned int word = 0;
    unsigned short ws_level = 0;

    while (word < index) {

        assert (s[start]);

        /* skip a word */
        while (s[start] && isncnamechar (s[start]))
            start++;

        /* skip whitespace (including comments) */
        while (true) {

            assert (s[start]);

            if (isspace (s[start]))
                start++;
            else if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                ws_level = 1;
                start += 2;
                while (ws_level > 0) {
                    assert (s[start]);
                    if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                        start += 2;
                        ws_level++;
                    }
                    else if (s[start] == ':' && s[start+1] && s[start+1]==')') {
                        start += 2;
                        ws_level--;
                    }
                    else
                        start++;
                }
            }
            else
                break;
        }

        word++;
    }

    for (end = start; s[end] && !isspace (s[end])
                             && s[end] != '{'
                             && s[end] != '('; end++)
        /* do nothing */;

    return PFstr_qname (PFstrndup (s + start, end - start));
}

/**
 * Extract word number @a index from a pattern match and return
 * it as a string.
 */
static char *
extract_ncname (const char *s, unsigned int index)
{
    unsigned int start = 0;
    unsigned int end;
    unsigned int word = 0;
    unsigned short ws_level = 0;

    while (word < index) {

        assert (s[start]);

        /* skip a word */
        while (s[start] && isncnamechar (s[start]))
            start++;

        /* skip whitespace (including comments) */
        while (true) {

            assert (s[start]);

            if (isspace (s[start]))
                start++;
            else if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                ws_level = 1;
                start += 2;
                while (ws_level > 0) {
                    assert (s[start]);
                    if (s[start] == '(' && s[start+1] && s[start+1] == ':') {
                        start += 2;
                        ws_level++;
                    }
                    else if (s[start] == ':' && s[start+1] && s[start+1]==')') {
                        start += 2;
                        ws_level--;
                    }
                    else
                        start++;
                }
            }
            else
                break;
        }

        word++;
    }

    for (end = start; s[end] && !isspace (s[end])
                             && s[end] != '{'
                             && s[end] != '('; end++)
        /* do nothing */;

    return PFstrndup (s + start, end - start);
}

/* vim:set shiftwidth=4 expandtab: */

