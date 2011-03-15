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
 * the Database Group at the Technische Universitaet Muenchen, Germany.
 * It is now maintained by the Database Systems Group at the Eberhard
 * Karls Universitaet Tuebingen, Germany.  Portions created by the
 * University of Konstanz, the Technische Universitaet Muenchen, and the
 * Universitaet Tuebingen are Copyright (C) 2000-2005 University of
 * Konstanz, (C) 2005-2008 Technische Universitaet Muenchen, and (C)
 * 2008-2011 Eberhard Karls Universitaet Tuebingen, respectively.  All
 * Rights Reserved.
 *
 * $Id$ 
 */


#ifndef STRING_UTILS_H
#define STRING_UTILS_H

#ifdef HAVE_STDBOOL_H
    #include <stdbool.h>
#endif

#include <string.h>

char * 
PFstrUtils_substring (const char *start, const char *end);


/*	Function Name: SkipSpaces
 *	Arguments:     str -- string
 *	Description:   Skips the spaces in the head of str
 *	Returns:       A pointer to the first non space character in the string
 */

char* 
PFstrUtils_skipSpaces(char* str);




/*	Function Name: SkipUntilSubString
 *	Arguments:     str        -- string
 *                     sub_string -- substring until to skip
 *	Description:   Find a substring in str and skip to the next character
 *	Returns:       A pointer to the first character after the substring, the
 *                     same string str if the substring is not found
 */

char* 
PFstrUtils_skipUntilSubString(char* str, char* sub_string);


/*	Function Name: DupUntil
 *	Arguments:     from_str   -- the string to duplicate
 *                     until_char -- char delimiter
 *	Description:   Returns the substring from the begining of from_str
 *                     to the char delimeter (without including it).
 *                     Returns the null string if until_char is not found. 
 *	Returns:       The duplicate string
 */

char* 
PFstrUtils_dupUntil(char* from_str, char until_char);

/*	Function Name: BeginsWith
 *	Arguments:     str1 -- greater string
 *                 str2 -- smaller string
 *	Description:   Compares the beginnings of both string
 *	Returns:       True if str1 begins with str2
 */

bool 
PFstrUtils_beginsWith(const char* str1, const char* str2);

/*
 * Look for the substring SUB in buffer and return a pointer to that
 * substring in BUFFER or NULL if not found.
 * Comparison is case-insensitive.
 */
const char *
PFstrUtils_memistr (const void *buffer, size_t buflen, const char *sub);

/****************
 * remove leading and trailing white spaces
 */
char *
PFstrUtils_trim_spaces( char *str );


/****************
 * remove trailing white spaces
 */
char *
PFstrUtils_trim_trailing_spaces( char *string );


#endif             
