/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of helper functions for converting between utf8 and utf32. The reason this is necessary is 
   because Numpy stores Unicode strings in utf32 format, and MonetDB stores them in utf8 format.  
 */

#ifndef _UNICODE_LIB_
#define _UNICODE_LIB_

#include <stdint.h>
#include <stddef.h>

#include "pyheader.h"

//! Returns the amount of characters in the utf8 formatted string, returns -1 if the string is not a valid utf8 string
/* Arguments:
	utf8_str: A pointer to a utf8 formatted string.
	ascii: a pointer to a boolean, this is set to true if the string is ascii-encoded and to false otherwise
*/
int utf8_strlen(const char *utf8_str, bool *ascii);
int utf32_strlen(const Py_UNICODE *utf32_str);

//! Returns the length in bytes of a single utf8 character [1,2,3 or 4] based on the signature of the first byte, returns -1 if the character is not a valid utf8 character
/* Arguments:
	utf8_char: The first byte of a utf8 character.
*/
int utf8_length(unsigned char utf8_char);

//! Converts a utf32 string to a utf8 string, returns TRUE on success and FALSE on failure
/* Arguments:
	offset: The offset in the utf32 array in amount of utf32 characters (so 4 * bytes)
	size: The maximum length of the utf32 string in amount of utf32 characters (note: if a \0 character is encountered, the parser will stop early)
	utf8_storage: The storage container to put the resulting utf8 formatted string in. To ensure the utf8 string fits this has to be [size * 4] bytes.
	utf32: An array of utf32 characters
*/
bool utf32_to_utf8(size_t offset, size_t size, char *utf8_storage, const Py_UNICODE *utf32);

bool ucs2_to_utf8(size_t offset, size_t size, char *utf8_storage, const Py_UNICODE *ucs2);

bool unicode_to_utf8(size_t offset, size_t size, char *utf8_storage, const Py_UNICODE *unicode);
//! Converts a utf8 string to a utf32 string, returns TRUE on success and FALSE on failure
/* Arguments:
	offset: The offset in the utf8 array in bytes
	size: The maximum length of the utf8 string in bytes (note: if a \0 character is encountered, the parser will stop early)
	utf32_storage: The storage container to put the resulting utf32 formatted string in. To ensure the utf32 string fits this has to be [size] bytes.
	utf8: An array of utf8 characters
*/
bool utf8_to_utf32(size_t offset, size_t size, Py_UNICODE *utf32_storage, const unsigned char *utf8);

//! Converts a single utf8 char to a single utf32 char, returns the size of the utf-8 character in bytes on success [1,2,3 or 4], and -1 on failure
/* Arguments:
	position: 
	utf32_storage:
	offset:
	utf8_char:
*/
int utf8_char_to_utf32_char(size_t position, Py_UNICODE *utf32_storage, int offset, const unsigned char *utf8_char);

//! Converts a single utf32 char to a single utf8 char, returns the size of the utf-8 character in bytes on success [1,2,3 or 4], and -1 on failure
/* Arguments:
	position: 
	utf32_storage:
	utf8_char:
*/
int utf32_char_to_utf8_char(size_t position, char *utf8_storage, unsigned int utf32_char);

void _unicode_init(void);

#endif /* _UNICODE_LIB_ */
