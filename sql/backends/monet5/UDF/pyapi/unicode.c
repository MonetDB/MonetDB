/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "unicode.h"

#include <string.h>

int utf8_strlen(const char *utf8_str, bool *ascii)
{
	int utf8_char_count = 0;
	int i = 0;
	// we traverse the string and simply count the amount of utf8 characters in
	// the string
	while (true) {
		int offset;
		if (utf8_str[i] == '\0')
			break;
		offset = utf8_length(utf8_str[i]);
		if (offset < 0)
			return -1; // invalid utf8 character
		i += offset;
		utf8_char_count++;
	}
	if (ascii != NULL)
		*ascii = i == utf8_char_count;
	return utf8_char_count;
}

size_t utf32_strlen(const Py_UNICODE *utf32_str)
{
	size_t i = 0;
	while (utf32_str[i] != 0)
		i++;
	return i;
}

int utf8_length(unsigned char utf8_char)
{
	// the first byte tells us how many bytes the utf8 character uses
	if (utf8_char < 0x80)
		return 1;
	else if (utf8_char < 0xe0)
		return 2;
	else if (utf8_char < 0xf0)
		return 3;
	else if (utf8_char < 0xf8)
		return 4;
	else
		return -1; // invalid utf8 character, the maximum value of the first
				   // byte is 0xf7
}

int utf32_char_to_utf8_char(size_t position, char *utf8_storage,
							unsigned int utf32_char)
{
	int utf8_size = 4;
	if (utf32_char < 0x80)
		utf8_size = 1;
	else if (utf32_char < 0x800)
		utf8_size = 2;
	else if (utf32_char < 0x10000)
		utf8_size = 3;
	else if (utf32_char > 0x0010FFFF)
		return -1; // utf32 character is out of legal range

	switch (utf8_size) {
		case 4:
			utf8_storage[position + 3] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position + 2] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position + 1] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position] = (utf32_char | 0xf0);
			return utf8_size;
		case 3:
			utf8_storage[position + 2] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position + 1] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position] = (utf32_char | 0xe0);
			return utf8_size;
		case 2:
			utf8_storage[position + 1] = ((utf32_char | 0x80) & 0xbf);
			utf32_char >>= 6;
			utf8_storage[position] = (utf32_char | 0xc0);
			return utf8_size;
		default:
			utf8_storage[position] = (char)utf32_char;
			return utf8_size;
	}
}

bool ucs2_to_utf8(size_t offset, size_t size, char *utf8_storage,
				  const Py_UNICODE *ucs2)
{
	size_t i = 0;
	int position = 0;
	int shift;
	for (i = 0; i < size; i++) {
		if (ucs2[offset + i] == 0) {
			utf8_storage[position] = '\0';
			return true;
		}
		shift =
			utf32_char_to_utf8_char(position, utf8_storage, ucs2[offset + i]);
		if (shift < 0)
			return false;
		position += shift;
	}
	utf8_storage[position] = '\0';
	return true;
}

bool utf32_to_utf8(size_t offset, size_t size, char *utf8_storage,
				   const Py_UNICODE *utf32_input)
{
	size_t i = 0;
	int position = 0;
	int shift;
	unsigned int *utf32 = (unsigned int *)utf32_input;

	for (i = 0; i < size; i++) {
		if (utf32[offset + i] == 0) {
			utf8_storage[position] = '\0';
			return true;
		}

		shift =
			utf32_char_to_utf8_char(position, utf8_storage, utf32[offset + i]);
		if (shift < 0)
			return false;
		position += shift;
	}
	utf8_storage[position] = '\0';
	return true;
}

bool unicode_to_utf8(size_t offset, size_t size, char *utf8_storage,
					 const Py_UNICODE *unicode)
{
#if Py_UNICODE_SIZE == 2
	return ucs2_to_utf8(offset, size, utf8_storage, unicode);
#else
	return utf32_to_utf8(offset, size, utf8_storage, unicode);
#endif
}

int utf8_char_to_utf32_char(size_t position, Py_UNICODE *utf32_storage,
							int offset, const unsigned char *utf8_char)
{
	unsigned char bytes[4];
	int utf8_size = 4;
	bytes[0] = utf8_char[offset];
	bytes[1] = 0xFF;
	bytes[2] = 0xFF;
	bytes[3] = 0xFF;
	// the first byte tells us how many bytes the utf8 character uses
	if (bytes[0] < 0x80)
		utf8_size = 1;
	else if (bytes[0] < 0xe0)
		utf8_size = 2;
	else if (bytes[0] < 0xf0)
		utf8_size = 3;
	else if (bytes[0] < 0xf8)
		utf8_size = 4;
	else
		return -1; // invalid utf8 character, the maximum value of the first
				   // byte is 0xf7

#if Py_UNICODE_SIZE == 2
	if (utf8_size > 2) {
		// utf-8 character out of range on a UCS2 python compilation
		return -1;
	}
#endif

	switch (utf8_size) {
		case 4:
			bytes[3] = utf8_char[offset + 3];
			if (bytes[3] > 0xc0)
				return -1; // invalid utf8 character, the maximum value of the
						   // second, third and fourth bytes is 0xbf
			/* fall through */
		case 3:
			bytes[2] = utf8_char[offset + 2];
			if (bytes[2] > 0xc0)
				return -1;
			/* fall through */
		case 2:
			bytes[1] = utf8_char[offset + 1];
			if (bytes[1] > 0xc0)
				return -1;
	}

	utf32_storage[position] = 0;

	switch (utf8_size) {
		case 4:
			utf32_storage[position] |= (0x3f & bytes[3]);
			utf32_storage[position] |= (0x3f & bytes[2]) << 6;
			utf32_storage[position] |= (0x3f & bytes[1]) << 12;
			utf32_storage[position] |= (0x7 & bytes[0]) << 18;
			return utf8_size;
		case 3:
			utf32_storage[position] |= (0x3f & bytes[2]);
			utf32_storage[position] |= (0x3f & bytes[1]) << 6;
			utf32_storage[position] |= (0xf & bytes[0]) << 12;
			return utf8_size;
		case 2:
			utf32_storage[position] |= (0x3f & bytes[1]);
			utf32_storage[position] |= (0x1f & bytes[0]) << 6;
			return utf8_size;
		default:
			utf32_storage[position] |= 0x7f & bytes[0];
			return utf8_size;
	}
}

bool utf8_to_utf32(size_t offset, size_t size, Py_UNICODE *utf32_storage,
				   const unsigned char *utf8)
{
	size_t i = 0;
	int position = 0;
	int shift;
	for (i = 0; i < size; i++) {
		if (utf8[offset + position] == 0) {
			utf32_storage[i] = '\0';
			return true;
		}

		shift = utf8_char_to_utf32_char((int)i, utf32_storage,
										(int)(offset + position), utf8);
		if (shift < 0)
			return false;
		position += shift;
	}
	return true;
}

void _unicode_init(void) { _import_array(); }
