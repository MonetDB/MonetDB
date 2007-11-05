/* -*- c-basic-offset:4; c-indentation-style:"k&r"; indent-tabs-mode:nil -*- */

/**
 * @file
 * 
 * Some generic String Utilities 
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
 * 2000-2005 University of Konstanz and (C) 2005-2007 Technische
 * Universitaet Muenchen, respectively.  All Rights Reserved.
 *
 * $Id$ 
 */


#include "pathfinder.h"

#include "string_utils.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>

typedef unsigned char byte;

/****************************************************
 ******** Locale insensitive ctype functions ********
 ****************************************************/
/* FIXME: replace them by a table lookup and macros */
int
ascii_isupper (int c)
{
    return c >= 'A' && c <= 'Z';
}

int
ascii_islower (int c)
{
    return c >= 'a' && c <= 'z';
}

int
ascii_toupper (int c)
{
    if (c >= 'a' && c <= 'z')
        c &= ~0x20;
    return c;
}

int
ascii_tolower (int c)
{
    if (c >= 'A' && c <= 'Z')
        c |= 0x20;
    return c;
}



char * 
PFstrUtils_substring (const char *start, const char *end)
{
  char *result = malloc (end - start + 1);
  char *scan_result = result;
  const char *scan = start;

  while (scan < end)
    *scan_result++ = *scan++;

  *scan_result = 0;
  return result;
}


/*	Function Name: SkipSpaces
 *	Arguments:     str -- string
 *	Description:   Skips the spaces in the head of str
 *	Returns:       A pointer to the first non space character in the string
 */

char* 
PFstrUtils_skipSpaces(char* str)
{
  while (*str++ == ' ');
  return(--str);
}



/*	Function Name: SkipUntilSubString
 *	Arguments:     str        -- string
 *                     sub_string -- substring until to skip
 *	Description:   Find a substring in str and skip to the next character
 *	Returns:       A pointer to the first character after the substring, the
 *                     same string str if the substring is not found
 */

char* 
PFstrUtils_skipUntilSubString(char* str, char* sub_string)
{
  char* initial_str;
  char* initial_sub_string;
  char current_char;
  
  initial_str = str;
  initial_sub_string = sub_string;
  current_char = *str++;
  while (current_char != '\0') {
    while(current_char == *sub_string++ &&
	  current_char != '\0'){
      if (*sub_string == '\0') 
	return(str);
      current_char = *str++;
    }
    sub_string = initial_sub_string;
    current_char = *str++;
  }
  return(initial_str);
}


/*	Function Name: DupUntil
 *	Arguments:     from_str   -- the string to duplicate
 *                     until_char -- char delimiter
 *	Description:   Returns the substring from the begining of from_str
 *                     to the char delimeter (without including it).
 *                     Returns the null string if until_char is not found. 
 *	Returns:       The duplicate string
 */

char* 
PFstrUtils_dupUntil(char* from_str, char until_char)
{
  char* initial_string;
  char* return_string;
  
  initial_string = from_str;
  while(*from_str != until_char && *from_str != '\0') {
    from_str++;
  }
  if (*from_str == '\0') 
    return(strdup(""));
  *from_str = '\0';
  return_string = strdup(initial_string);
  *from_str = until_char;
  return(return_string);
}


/*	Function Name: BeginsWith
 *	Arguments:     str1 -- greater string
 *          str2 -- smaller string
 *	Description:   Compares the beginnings of both string
 *	Returns:       True if str1 begins with str2
 */

bool 
PFstrUtils_beginsWith(const char* str1, const char* str2)
{

  char* str1_ = (char*)str1;
  char* str2_ = (char*)str2;
  if (*str2_ == '\0')
    return(1);
  if (*str1_ == '\0')
    return(0);
  while (*str1_++ == *str2_++) {
    if (*str2_ == '\0')
      return(1);
    if (*str1_ == '\0')
      return(0);
  }
  return(0);
}


/*
 * Look for the substring SUB in buffer and return a pointer to that
 * substring in BUFFER or NULL if not found.
 * Comparison is case-insensitive.
 */
const char *
PFstrUtils_memistr (const void *buffer, size_t buflen, const char *sub)
{
  const unsigned char *buf = buffer;
  const unsigned char *t = (const unsigned char *)buffer;
  const unsigned char *s = (const unsigned char *)sub;
  size_t n = buflen;

  for ( ; n ; t++, n-- )
    {
      if ( toupper (*t) == toupper (*s) )
        {
          for ( buf=t++, buflen = n--, s++;
                n && toupper (*t) == toupper (*s); t++, s++, n-- )
            ;
          if (!*s)
            return (const char*)buf;
          t = buf;
          s = (const unsigned char *)sub ;
          n = buflen;
	}
    }
  return NULL;
}


/****************
 * remove leading and trailing white spaces
 */
char *
PFstrUtils_trim_spaces( char *str )
{
    char *string, *p, *mark;

    string = str;
    /* find first non space character */
    for( p=string; *p && isspace( *(byte*)p ) ; p++ )
	;
    /* move characters */
    for( (mark = NULL); (*string = *p); string++, p++ )
	if( isspace( *(byte*)p ) ) {
	    if( !mark )
		mark = string ;
	}
	else
	    mark = NULL ;
    if( mark )
	*mark = '\0' ;  /* remove trailing spaces */

    return str ;
}

/****************
 * remove trailing white spaces
 */
char *
PFstrUtils_trim_trailing_spaces( char *string )
{
    char *p, *mark;

    for( mark = NULL, p = string; *p; p++ ) {
	if( isspace( *(byte*)p ) ) {
	    if( !mark )
		mark = p;
	}
	else
	    mark = NULL;
    }
    if( mark )
	*mark = '\0' ;

    return string ;
}


         
