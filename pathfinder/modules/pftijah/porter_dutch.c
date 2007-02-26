/*****************************************************************************
                                   UPLIFT 
  
  This file contains a version of Porter's algorithm for DUTCH based on the
  implementation by Frakes (see below).

  The code is a result of the UPLIFT project (Utrecht Project: Linguistic
  Information for Free Text Retrieval) and is based on the joint efforts of
  Renee Pohlmann and Wessel Kraaij.

  Research Institute for Language and Speech, Utrecht University 1994.

  Copyright (C) 1995  Wessel Kraaij & Renee Pohlmann
    
  email: Wessel.Kraaij@let.ruu.nl  Renee.C.Pohlmann@let.ru.nl

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not, write to the Free
  Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.


*****************************************************************************/

/*******************************   stem.c   ***********************************

   Purpose:    Implementation of the Porter stemming algorithm documented 
               in: Porter, M.F., "An Algorithm For Suffix Stripping," 
               Program 14 (3), July 1980, pp. 130-137.

   Provenance: Written by B. Frakes and C. Cox, 1986.
               Changed by C. Fox, 1990.
                  - made measure function a DFA
                  - restructured structs
                  - renamed functions and variables
                  - restricted function and variable scopes
               Changed by C. Fox, July, 1991.
                  - added ANSI C declarations 
                  - branch tested to 90% coverage

   Notes:      This code will make little sense without the the Porter
               article.  The stemming function converts its input to
               lower case.
**/


/************************   Standard Include Files   *************************/
#include <pf_config.h>

#include <stdio.h>
#include <string.h>
#include <ctype.h>

/*****************************************************************************/
/*****************   Private Defines and Data Structures   *******************/

#include "porter_dutch.h"

/* define CMP as 1 for some diagnostic info on dehyphenating words */
#ifndef CMP
#define CMP 0
#endif


#define FALSE                         0
#define TRUE                          1
#define EOS                         '\0'

/* some shortcuts for sets of letters */

#define A_DIA       "a\344\341\340\342"
#define E_DIA       "e\353\351\350\352"
#define I_DIA       "i\357\355\354\356"
#define O_DIA       "o\366\363\362\364"
#define U_DIA       "u\374\372\371\373"
#define AEIOU "a\344\341\340\342e\353\351\350\352i\357\355\354\356o\366\363\362\364u\374\372\371\373"
#define AEIOUY "a\344\341\340\342e\353\351\350\352i\357\355\354\356o\366\363\362\364u\374\372\371\373y"
#define AEIOUWXY "a\344\341\340\342e\353\351\350\352i\357\355\354\356o\366\363\362\364u\374\372\371\373wxy"
#define AIOU "a\344\341\340\342i\357\355\354\356o\366\363\362\364u\374\372\371\373"
#define AIOUWXY "a\344\341\340\342i\357\355\354\356o\366\363\362\364u\374\372\371\373wxy"
#define AEOU "a\344\341\340\342e\353\351\350\352o\366\363\362\364u\374\372\371\373"

#define AOU "a\344\341\340\342o\366\363\362\364u\374\372\371\373"

/*
#define IsVowel(c) ('a'==(c)||'e'==(c)||'i'==(c)||'o'==(c)||'u'==(c)||\
                    'y'==(c)||\
		    '\353'==(c)||'\374'==(c)||'\366'==(c)||'\357'==(c)||\
      	   	    '\344'==(c)||'\351'==(c)||'\372'==(c)||'\363'==(c)||\
		    '\355'==(c)||'\341'==(c)||'\350'==(c)||'\371'==(c)||\
		    '\362'==(c)||'\354'==(c)||'\340'==(c)||'\352'==(c)||\
		    '\373'==(c)||'\364'==(c)||'\356'==(c)||'\342'==(c) )
		    */
#define IsVowel(c) strchr( AEIOUY ,c) != NULL
/**WK*************  added 'y' for dutch version ******************************/

typedef struct {
           int id;                 /* returned if rule fired */
           char *old_end;          /* suffix replaced */
           char *new_end;          /* suffix replacement */
           int old_offset;         /* from end of word to start of suffix */
           int new_offset;         /* from beginning to end of new suffix */
           int min_root_size;      /* min root word size for replacement */
           int (*condition)();     /* the replacement test function */
           } RuleList;

static char LAMBDA[1] = "";        /* the constant empty string */
static char *end;                  /* pointer to the end of the word */

/*****************************************************************************/
/********************   Private Function Declarations   **********************/

#ifdef __STDC__

static int WordSize( char *word );
#if 0
static int ContainsVowel( char *word );
#endif
static int DupVCond( char *word );
static int DuplicateV( char *word );
static int EndsWithV( char *word );
static int EndsWithVX( char *word );
static int EndsWithC( char *word );
static int ReplaceSuffix( char *word, RuleList *rule );
static int ReplacePrefix( char *word, RuleList *rule );
static int ReplaceInfix( char *word, RuleList *rule );
static int StripDashes( char *word );
static int gt2( char *word );
static int RemoveDia( char *c );
#else

static int WordSize( /* word */ );
#if 0
static int ContainsVowel( /* word */ );
#endif
static int DupVCond( /* word */ );
static int DuplicateV( /* word */ );
static int EndsWithC( /* word */ );
static int EndsWithV( /* word */ );
static int EndsWithVX( /* word */ );
static int ReplaceSuffix( /* word, rule */ );
static int ReplacePrefix( /* word, rule */ );
static int ReplaceInfix( /* word, rule */ );
static int StripDashes( /* word */ );
static int gt2( /* word */ );
static int RemoveDia( /* character */ );
#endif

/******************************************************************************/
/*****************   Initialized Private Data Structures   ********************/


static RuleList step1_rules[] =
           {
             {100,  "'s",        LAMBDA,  1, -1, -1,  NULL},
             {101,  "ts",        "ts",    1,  1,  0,  NULL},
             {102,  "s",         LAMBDA,  0, -1,  0,  EndsWithC},
             {103,  "ies",       "ie",    2,  1,  0,  NULL},
             {104,  "eres",      "er",    3,  1,  0,  EndsWithC},
             {105,  "ares",      "ar",    3,  1,  0,  EndsWithC},
             {106,  "es",        "e",     1,  0,  0,  EndsWithC},
	     {107,  "\351s",     "\351",  1,  0,  0,  NULL},
             {108,  "aus",       "au",    2,  1,  0,  EndsWithV},
             {109,  "heden",     "heid",  4,  3,  0,  NULL},
             {110,  "nden",      "nd",    3,  1, -1,  NULL},
             {111,  "nde",       "nd",    2,  1, -1,  NULL},
             {112,  "den",       LAMBDA,  2, -1,  0,  EndsWithC},
             {113,  "ien",       "i",     2,  0, -1,  EndsWithV},
             {114,  "jen",       "j",     2,  0, -1,  EndsWithV},
             {115,  "en",        LAMBDA,  1, -1,  0,  EndsWithC},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };


static RuleList step2_rules[] =
           { 
             {201,  "'tje",       LAMBDA,  3, -1, -1,  NULL},
             {202,  "etje",       LAMBDA,  3, -1,  0,  EndsWithC},
             {203,  "rntje",      "rn",    4,  1, -1,  NULL},
             {204,  "tje",        LAMBDA,  2, -1,  0,  EndsWithVX},
             {205,  "inkje",      "ing",   4,  2, -1,  NULL},
             {206,  "mpje",       "m",     3,  0, -1,  NULL},
             {207,  "'je",        LAMBDA,  2, -1,  0,  NULL},
             {208,  "je",         LAMBDA,  1, -1,  0,  EndsWithC},
             {209,  "ge",         "g",     1,  0,  0,  NULL},
             {210,  "lijke",      "lijk",  4,  3,  0,  NULL},
             {211,  "ische",      "isch",  4,  3,  0,  NULL},
             {212,  "de",         LAMBDA,  1, -1,  0,  EndsWithC},
             {213,  "te",         "t",     1,  0,  0,  NULL},
             {214,  "se",         "s",     1,  0,  0,  NULL},
             {215,  "re",         "r",     1,  0,  0,  NULL},
             {216,  "le",         "l",     1,  0,  0,  NULL},
             {217,  "ene",        "en",    2,  1,  0,  EndsWithC},
             {218,  "ieve",       "ief",   3,  2,  0,  NULL},
             {000,  NULL,         NULL,    0,  0,  0,  NULL}
           };

static RuleList step3_rules[] =
           { 
             {301,  "atie",      "eer",   3,  2,  0,  NULL},
             {302,  "iteit",     LAMBDA,  4, -1,  0,  NULL},
             {303,  "heid",      LAMBDA,  3, -1,  0,  NULL},
             {306,  "sel",       LAMBDA,  2, -1,  0,  NULL},            
             {307,  "ster",      LAMBDA,  3, -1,  0,  NULL},
             {308,  "rder",      "r",     3,  0, -1,  NULL},
             {312,  "ing",       LAMBDA,  2, -1,  0,  NULL},
             {313,  "isme",      LAMBDA,  3, -1,  0,  NULL},
             {314,  "erij",      LAMBDA,  3, -1,  0,  NULL},
             {315,  "arij",      "aar",   3,  2,  0,  EndsWithC},
             {316,  "fie",       "f",     2,  0,  1,  NULL},
             {317,  "gie",       "g",     2,  0,  1,  NULL},
             {318,  "tst",       "t",     2,  0,  0,  EndsWithC},
             {319,  "dst",       "d",     2,  0,  0,  EndsWithC},
	     {000,  NULL,         NULL,    0,  0,  0,  NULL}
           };

static RuleList step4_rules[] =
           { 
             {401,  "ioneel",    "ie",    5,  1,  0,  NULL},
             {402,  "atief",     "eer",   4,  2,  0,  NULL},
             {403,  "baar",      LAMBDA,  3, -1,  0,  NULL},
             {404,  "naar",      "n",     3,  0,  0,  EndsWithV},
             {405,  "laar",      "l",     3,  0,  0,  EndsWithV},
             {406,  "raar",      "r",     3,  0,  0,  EndsWithV},         
             {407,  "tant",      "teer",  3,  3,  0,  NULL},
             {408,  "lijker",    "lijk",  5,  3,  0,  NULL},
             {409,  "lijkst",    "lijk",  5,  3,  0,  NULL},
             {410,  "achtig",    LAMBDA,  5, -1,  0,  NULL}, 
             {410,  "achtiger",  LAMBDA,  7, -1,  0,  NULL}, 
             {410,  "achtigst",  LAMBDA,  7, -1,  0,  NULL}, 
             {411,  "eriger",    LAMBDA,  5, -1,  0,  EndsWithC},
             {412,  "erigst",    LAMBDA,  5, -1,  0,  EndsWithC},
             {413,  "iger",      LAMBDA,  3, -1,  0,  EndsWithC},
             {414,  "igst",      LAMBDA,  3, -1,  0,  EndsWithC},
             {415,  "erig",      LAMBDA,  3, -1,  0,  EndsWithC},
             {416,  "ig",        LAMBDA,  1, -1,  0,  EndsWithC},
             {417,  "end",       LAMBDA,  2, -1,  0,  EndsWithC},
	     {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

static RuleList step1a_rules[] =
           {

	     {501,  "ge",        LAMBDA,  1, -1,  0,  gt2},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

static RuleList step1b_rules[] =
           {
             {502,  "ge",        LAMBDA,  1, -1,  0,  gt2},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

static RuleList step1c_rules[] =
           {
             {503,  "nd",        "nd",    1,  1,  0,  NULL},
             {504,  "d",         LAMBDA,  0, -1,  0,  EndsWithC},
             {505,  "ht",        "ht",    1,  1,  0,  NULL},
             {506,  "t",         LAMBDA,  0, -1,  0,  EndsWithC},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

static RuleList step7_rules[] =
           {
             {701,  "kt",        "k",     1,  0, -1,  NULL},
             {701,  "ft",        "f",     1,  0, -1,  NULL},
             {701,  "pt",        "p",     1,  0, -1,  NULL},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

static RuleList step6_rules[] =
           {
             {601,  "bb",         "b",    1,  0, -1,  NULL},
             {602,  "cc",         "c",    1,  0, -1,  NULL},
             {603,  "dd",         "d",    1,  0, -1,  NULL},
             {604,  "ff",         "f",    1,  0, -1,  NULL},
             {605,  "gg",         "g",    1,  0, -1,  NULL},
             {606,  "hh",         "h",    1,  0, -1,  NULL},
             {607,  "jj",         "j",    1,  0, -1,  NULL},
             {608,  "kk",         "k",    1,  0, -1,  NULL},
             {609,  "ll",         "l",    1,  0, -1,  NULL},
             {610,  "mm",         "m",    1,  0, -1,  NULL},
             {611,  "nn",         "n",    1,  0, -1,  NULL},
             {612,  "pp",         "p",    1,  0, -1,  NULL},
             {613,  "qq",         "q",    1,  0, -1,  NULL},
             {614,  "rr",         "r",    1,  0, -1,  NULL},
             {615,  "ss",         "s",    1,  0, -1,  NULL},
             {616,  "tt",         "t",    1,  0, -1,  NULL},
             {617,  "vv",         "v",    1,  0, -1,  NULL},
             {618,  "ww",         "w",    1,  0, -1,  NULL},
             {619,  "xx",         "x",    1,  0, -1,  NULL},
             {620,  "zz",         "z",    1,  0, -1,  NULL},
             {621,  "v",          "f",    0,  0, -1,  NULL},
             {622,  "z",          "s",    0,  0, -1,  NULL},
             {000,  NULL,        NULL,    0,  0,  0,  NULL}
           };

/*****************************************************************************/
/********************   Private Function Declarations   **********************/

/*FN***************************************************************************

       WordSize( word )

   Returns: int -- a weird count of word size in adjusted syllables

   Purpose: Count syllables in a gt2 way:  count the number 
            vowel-consonant pairs in a word, disregarding initial 
            consonants and final vowels.  The letter "y" counts as a
            consonant at the beginning of a word and when it has a vowel
            in front of it; otherwise (when it follows a consonant) it
            is treated as a vowel.  For example, the WordSize of "cat" 
            is 1, of "any" is 1, of "amount" is 2, of "anything" is 3.

   Plan:    Run a DFA to compute the word size

   Notes:   The easiest and fastest way to compute this funny measure is
            with a finite state machine.  The initial state 0 checks
            the first letter.  If it is a vowel, then the machine changes
            to state 1, which is the "last letter was a vowel" state.
            If the first letter is a consonant or y, then it changes
            to state 2, the "last letter was a consonant state".  In
            state 1, a y is treated as a consonant (since it follows
            a vowel), but in state 2, y is treated as a vowel (since
            it follows a consonant.  The result counter is incremented
            on the transition from state 1 to state 2, since this
            transition only occurs after a vowel-consonant pair, which
            is what we are counting.

   Dutch version:
            Count 'y' as a vowel!
            'ij' rule: a 'j' is counted as a vowel after an 'i'.
**/

static int
WordSize( char *word )   /* in: word having its WordSize taken */
   {
   register int result;   /* WordSize of the word */
   register int state;    /* current state in machine */

   result = 0;
   state = 0;

                 /* Run a DFA to compute the word size */
   while ( EOS != *word )
      {
/*printf("%d %c\n",state,*word);*/
      switch ( state )
         {
         case 0: state = (IsVowel(*word)) ? 1 : 2;
                 break;
         case 1: if (IsVowel(*word))
	           state = 1;
	          else
		    if ('j' == *word)
		      if ('i' == *(word-1)) state = 1;
		      else state = 2;
		    else state = 2;
	         /* changed to treat the dutch composite 'ij' vowel */
                 if ( 2 == state ) result++;
                 break;
         case 2: state = (IsVowel(*word)) ? 1 : 2;
                 /* changed, y is a vowel */
	         break;
         }
      word++;
      }

  /* printf("%d\n",result); */
   return( result );

   } /* WordSize */

#if 0
/*FN**************************************************************************

       ContainsVowel( word )

   Returns: int -- TRUE (1) if the word parameter contains a vowel,
            FALSE (0) otherwise.

   Purpose: Some of the rewrite rules apply only to a root containing
            a vowel, where a vowel is one of "aeiou" or y with a
            consonant in front of it.

   Plan:    Obviously, under the definition of a vowel, a word contains
            a vowel iff either its first letter is one of "aeiou", or
            any of its other letters are "aeiouy".  The plan is to
            test this condition.

   Notes:   None
**/

static int
ContainsVowel( char *word )   /* in: buffer with word checked */
   {

   if ( EOS == *word )
      return( FALSE );
   else
      return( IsVowel(*word) || (NULL != strpbrk(word+1, AEIOUY)) );


   } /* ContainsVowel */
#endif
/*WK**************************************************************************

       DupVCond( word )

   Returns: int -- TRUE (1) if the current word ends with a
            consonant-vowel-consonant combination, and the second
            consonant is not w, x, or y, FALSE (0) otherwise.

   Purpose: Look whether the conditions to duplicate a vowel in the root hold.

   Plan:    Look at the last three characters.

   Notes:   
**/

static int
DupVCond( char *word )   /* in: buffer with the word checked */
{
   int length;
   char *butt; /* do not affect the global end pointer !! */
   
   length = strlen(word);
   butt = word + length - 1;

   if ( *(butt-1) == '\353' )  /* diaeresis stuff */
       { 
	return( (NULL == strchr( AEIOUWXY, *butt)) &&
		(NULL != strchr( "ei", *(butt-2))));
		
    } else {

   switch ( length ) {
       case 0:
       case 1: return ( FALSE );
       case 2: return( (NULL == strchr( AEIOUWXY ,*butt--))     /* VC */
		       && (NULL != strchr( AEOU ,  *butt--)) );  
	   
       default:
	   if (  (NULL == strchr( AEIOUWXY, *butt)) &&  /* CeC */
	         (NULL != strchr( E_DIA, *(butt-1) )) &&
		 (NULL == strchr( AEIOU, *(butt-2))) &&
	         (length > 3)) { /* e is a gt2 case ! */
	       switch ( length ) {
	       case 4:
		   return( NULL == strchr( AEIOU , *(butt-3))); /* !VCeC */
	       case 5:
		   return( NULL == strchr( AIOU , *(butt-3))); /*!(aiou)CeC */
	       default:
		   return(!(
		       
		       (NULL != strchr( AIOU, *(butt-3))) /* ! (aiou)CeC */

			   ||

		       ((NULL != strchr( AIOU, *(butt-4))) && /* !C(aiou)XCeC */
			(NULL == strchr( AEIOU, *(butt-5))))
		       )
		       );

	       }
	   }
       	       
	    else {
	       return( (NULL == strchr( AEIOUWXY,*butt--))     /* CVC */
                 && (NULL != strchr( AEOU,  *butt--))        
                 && (NULL == strchr( AEIOU,   *butt--  )));
	
	   }
   }
    }
} 

/* DupVCond */

/*WK**************************************************************************

       EndsWithV( word )

   Returns: int -- TRUE (1) if the current word ends with a vowel


   Purpose: Some of the rewrite rules apply only to a root with
            this characteristic.

   Plan:    Look at the last two characters.

   Notes:   None
**/

static int
EndsWithV( char *word )   /* in: buffer with the word checked */
   {
     char *butt;
     int len;

     len = strlen(word);
     if (len == 0 ) return(FALSE);
   butt = word + len - 1;
   if ( IsVowel(*butt)) 
     return( TRUE);
   else
     if (*butt == 'j' && *(butt-1) == 'i') 
       return(TRUE);
     else
       return(FALSE);
   } /* EndsWithV */


static int
EndsWithVX( char *word )   /* in: buffer with the word checked */
   {
     char *butt;
     int len;

     len = strlen(word);
     if (len == 0 ) return(FALSE);
   butt = word + len - 2;
   if ( IsVowel(*butt)) 
     return( TRUE);
   else
     if (*butt == 'j' && *(butt-1) == 'i') 
       return(TRUE);
     else
       return(FALSE);
   } /* EndsWithVX */


/*WK**************************************************************************

       EndsWithC( word )

   Returns: int -- TRUE (1) if the current word ends with a consonant


   Purpose: Some of the rewrite rules apply only to a root with
            this characteristic.

   Plan:    Look at the last two characters.

   Notes:   None
**/

static int
EndsWithC( char *word )   /* in: buffer with the word checked */
   {
     char *butt;
     int len;

     len = strlen(word);
     if ( len == 0) return(FALSE);
   butt = word + len - 1;
   if ( IsVowel(*butt)) 
     return( FALSE);
   else
     if (*butt == 'j' && *(butt-1) == 'i') 
       return(FALSE);
     else
       return(TRUE);

   } /* EndsWithC */

/*WK**************************************************************************

       gt2( word )

   Returns: 1 if succeeded

   Purpose: Length test


   Notes:   
**/


static int
gt2( char *word )   /* in: buffer with the word checked */
   {

     int len;

     len = strlen(word);
     if ( len < 3) return(FALSE);
     return(TRUE);
   } 


/*WK**************************************************************************

       DuplicateV( word )

   Returns: 1 if succeeded

   Purpose: Duplicate the Vowel

   Plan:    Look at the last three characters.

   Notes:   
**/

static int
DuplicateV( char *word )   /* in: buffer with the word checked */
   {
   int length;         /* for finding the last three characters */

   if ( (length = strlen(word)) < 2 )
      return( FALSE );
   else
      {
	  end = word + length - 1;
      *(end+2) = EOS;
      *(end+1) = *(end);

	  if ( *(end-1) == '\353') {
	      *(end) = 'e';
	      if ( *(end-2) == 'i') *(end-1) = 'e';
	  } else
	      *(end) = *(end-1);
      end = end +1;
	  
      return(TRUE);
   }
} 


/*FN**************************************************************************

       ReplaceSuffix( word, rule )

   Returns: int -- the id for the rule fired, 0 is none is fired

   Purpose: Apply a set of rules to replace the suffix of a word

   Plan:    Loop through the rule set until a match meeting all conditions
            is found.  If a rule fires, return its id, otherwise return 0.
            Connditions on the length of the root are checked as part of this
            function's processing because this check is so often made.

   Notes:   This is the main routine driving the stemmer.  It goes through
            a set of suffix replacement rules looking for a match on the
            current suffix.  When it finds one, if the root of the word
            is long enough, and it meets whatever other conditions are
            required, then the suffix is replaced, and the function returns.
**/

static int
ReplaceSuffix( char *word,        /* in/out: buffer with the stemmed word */
	       RuleList *rule )    /* in: data structure with replacement rules */
   {
   register char *ending;   /* set to start of possible stemmed suffix */
   char tmp_ch;             /* save replaced character when testing */

   while ( 0 != rule->id )
      {
      /*printf("testing rule %d on %s\n",rule->id,word);*/
      ending = end - rule->old_offset;
      if ( word <= ending )
         if ( 0 == strcmp(ending,rule->old_end) )
            {
            tmp_ch = *ending;
            *ending = EOS;
            if ( rule->min_root_size < WordSize(word) )
               if ( !rule->condition || (*rule->condition)(word) )
                  {
                    /*printf("rule %d fired\n",rule->id);*/
                  (void)strcat( word, rule->new_end );
                  end = ending + rule->new_offset;
                  break;
                  }
            *ending = tmp_ch;
            
            }
      rule++;
      }

   return( rule->id );

   } /* ReplaceSuffix */

/*WK**************************************************************************

       ReplacePrefix( word, rule )

   Returns: int -- the id for the rule fired, 0 if none is fired

   Purpose: Apply a set of rules to replace the prefix of a word

   Notes:  Derived from ReplaceSuffix because it is nice to be able to handle
           several prefixes. For Dutch however, only "ge" is removed.

           However: In the current version, ReplacePrefix is `abused' to test
	   whether a word starts with a certain prefix without actually
	   changing or deleting the prefix. The result code can be used to
	   perform a prefix dependent action (cf. the dStem procedure and
	   RuleList step5a_rules).
**/

static int
ReplacePrefix( char *word,        /* in/out: buffer with the stemmed word */
	       RuleList *rule )    /* in: data structure with replacement rules */
   {

   register char *root;   /* set to end of possible stemmed prefix */
   char tmp_ch;             /* save replaced character when testing */
   char *new_prefix;
   int i;
  
   /*printf("begin %s\n",word);*/
   while ( 0 != rule->id )
      {
      /*printf("testing rule %d on %s\n",rule->id,word);*/
      root = word + rule->old_offset;
      if ( root <= end )
	{
         tmp_ch = *(root+1);
         *(root+1) = EOS;
         
         if ( 0 == strcmp(word,rule->old_end) )
            {
            *(root+1) = tmp_ch;
            if ( rule->min_root_size < WordSize(root+1) )
               if ( !rule->condition || (*rule->condition)(root+1) )
                  {
                    /*printf("rule %d fired\n",rule->id);*/
		      /*printf("prefix eraf: %s\n",word);*/
		      if ( rule->new_offset < rule->old_offset )
			  {
			      (void)strcpy(word,rule->new_end);       
			      (void)strcat(word,root+1);
			  } else {
			      
			      new_prefix = rule->new_end;
			      i = 0;
			      while ( new_prefix[i] != EOS )
				  {
				      word[i] = new_prefix[i];i++;
				  }
			  }
		    end = end - rule->old_offset + rule->new_offset;
                  break;
                  }
	  }
	 else
	 *(root+1) = tmp_ch;  
       }
      rule++;
      }
   /* printf("ReplacePrefix eind %s %c\n",word,*end);*/
   return( rule->id );

   } /* ReplacePrefix */

/*WK**************************************************************************

       ReplaceInfix( word, rule )

   Returns: int -- the id for the rule fired, 0 is none is fired

   Purpose: Apply a set of rules to replace an infix of a word

   Notes:  Derived from ReplaceSuffix because it is nice to be able to handle
           several infixes. For Dutch however, only "ge" is removed.
**/
   



/* ReplaceInfix is derived from ReplacePrefix */
static int
ReplaceInfix( char *word,        /* in/out: buffer with the stemmed word */
	      RuleList *rule )    /* in: data structure with replacement rules */
{
  register char *root;   /* set to end of possible stemmed prefix */
  register char *infix;

  

   while ( 0 != rule->id )
      {

	infix = strstr(word+1,rule->old_end);
	if ( infix != NULL )
	  {
      root = infix + rule->old_offset;
      if ( root <= end )
	{
            if ( rule->min_root_size < WordSize(root+1) )
               if ( !rule->condition || (*rule->condition)(root+1) )
                  {
		    (void)strcpy(infix,rule->new_end);
		    (void)RemoveDia (root+1);
		    (void)strcat(infix,root+1);       
		    end = end - rule->old_offset + rule->new_offset;
                  break;
                  }
	  }
    }
      rule++;
      }
   /* printf("ReplaceInfix eind %s %c\n",word,*end);*/
   return( rule->id );

   } /* ReplaceInfix */



/*WK**************************************************************************

       StripDashes( word )

   Returns: -1 if the word must not be stemmed, 1 if the word can be stemmed
            in this case the hypen is removed.

   Purpose: Handle hyphenation in a smart way

   Plan: Remove hyphens when:
         a word starts or ends with a hyphen (the neighbour character must
	    be a letter)
	 a hyphen occurs between two letters


**/



/* StripDashes is derived from ReplaceInfix */
static int
StripDashes( char *word)        /* in/out: buffer with the stemmed word */
{
  register char *infix;

  infix = strchr(word,'-');
  if ( infix == NULL) return( FALSE);
  else
      {
	  if( infix == word && *(infix+1)== EOS)
	      {
		  if ( CMP )(void)fprintf(stderr,"term '-' not stemmed\n");
		  return( FALSE );
	      }
	  else
	      {

		  while ( infix != NULL )
		      {
			  
			  if(  (infix == word && isalpha(*(infix+1))) ||
			       /* dash as first character */
			       (isalpha(*(infix-1)) && isalpha(*(infix+1)))
			       /* dash as compounding sign */
			      )
			     {
				 if (CMP)(void)fprintf(stderr,"converting compound %s\n",word);
				 (void)strcpy(infix,infix+1);
				  end--;
				  infix = strchr(word,'-');
			     }
			  else if ( *(infix+1) == EOS ) /* agreement dash */
				 {
				     if (CMP)(void)fprintf(stderr,"aconverting compound %s\n",word);
				     *(infix) = EOS;end--;infix = NULL;

				 }
			     else 
				 
			     {
				 if (CMP)(void)fprintf(stderr,"'%s' will not be stemmed\n",word);				 return(-1);
			     }
		      }
		  if (CMP)(void)fprintf(stderr,"                ==> %s\n",word);
		  return( TRUE );
	      }
      }
} /* StripDashes */

/*WK**************************************************************************

       RemoveDia( word )

   Returns: -1 if the word must not be stemmed, 1 if the word can be stemmed
            in this case the hypen is removed.

   Purpose: Handle hyphenation in a smart way

   Plan: Remove hyphens when:
         a word starts or ends with a hyphen (the neighbour character must
	    be a letter)
	 a hyphen occurs between two letters

   Notes:  Derived from ReplaceSuffix because it is nice to be able to handle
           several prefixes. For Dutch however, only "ge" is removed.
**/

/* RemoveDia removes tremas */

static int
RemoveDia( char *c)
{
    if (*c == '\353') *c = 'e';
    if (*c == '\357') *c = 'i';
    return(TRUE);
}


/*****************************************************************************/
/*********************   Public Function Declarations   **********************/

/*FN***************************************************************************

       dStem( word )

   Returns: int -- FALSE (0) if the word contains non-alphabetic characters
            and hence is not stemmed, TRUE (1) otherwise
	    DUTCH version: Returns always TRUE
	    
   Purpose: Stem a word

   Plan:    (NOT USED :Part 1: Check to ensure the word is all alphabetic)
            Part 1a: Strip hyphens
            Part 2: Run through the Porter algorithm
            Part 3: Return an indication of successful stemming

   Notes:   This function implements the Porter stemming algorithm, with
            a few additions here and there.  See:

               Porter, M.F., "An Algorithm For Suffix Stripping,"
               Program 14 (3), July 1980, pp. 130-137.

            Porter's algorithm is an ad hoc set of rewrite rules with
            various conditions on rule firing.  The terminology of
            "step 1a" and so on, is taken directly from Porter's
            article, which unfortunately gives almost no justification
            for the various steps.  Thus this function more or less
            faithfully refects the opaque presentation in the article.
            Changes from the article amount to a few additions to the
            rewrite rules;  these are marked in the RuleList data
            structures with comments.
**/

int
dStem( char *word )  /* in/out: the word stemmed */
   {
   int rule;    /* which rule is fired in replacing an end */
   int cleanup;
            /* Part 1: Check to ensure the word is all alphabetic */
/*   for ( end = word; *end != EOS; end++ )
      if ( !isalpha(*end) ) return( FALSE );
      else *end = tolower( *end );
   
   
      end--;
   */
   end = &word[strlen(word)-1];
/* NOTE : end is initialised */
   cleanup = 0;



   if(StripDashes( word ) == -1) /* words containing dashes and digits */
       return(TRUE);
   
                /*  Part 2: Run through the Porter algorithm */
   rule = ReplaceSuffix( word, step1_rules);
   if (rule > 0) cleanup = 1;
   if ( (105 == rule || 115 == rule) && DupVCond( word ) )
    (void)DuplicateV( word );
   rule = ReplaceSuffix( word, step2_rules );
   if (rule > 0) cleanup = 1;
   if ( (216 == rule || 217 == rule) && DupVCond( word ) )
    (void)DuplicateV( word );
   rule = ReplaceSuffix( word, step3_rules );
   if (rule > 0) cleanup = 1;
   if ( (302 == rule || 312 == rule || 313 == rule || 314 == rule || 316 == rule || 317 == rule) && DupVCond( word ) )
    (void)DuplicateV( word );
   rule = ReplaceSuffix( word, step4_rules);
   if (rule > 0) cleanup = 1;
   if ( (411 == rule || 412 == rule || 413 == rule || 414 == rule || 415 == rule || 416 == rule || 417 == rule) && DupVCond( word ) )
    (void)DuplicateV( word );
   rule = ReplacePrefix( word, step1a_rules );
   if ( 501 == rule) RemoveDia( word );
   if (rule > 0) cleanup = 1;
   if ( (501 == rule) )
       rule = ReplaceSuffix( word, step1c_rules );
   rule = ReplaceInfix( word, step1b_rules );
   if (rule > 0) cleanup = 1;
   if ( (502 == rule) )
       rule = ReplaceSuffix( word, step1c_rules );
   rule = ReplaceSuffix( word, step7_rules);
   if (rule > 0) cleanup = 1;
   if ( (cleanup == 1) )
   (void)ReplaceSuffix( word, step6_rules );

           /* Part 3: Return an indication of successful stemming */

   return( TRUE );

   } /* Stem */





