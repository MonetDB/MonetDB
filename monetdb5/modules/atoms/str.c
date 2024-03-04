/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 *  N.J. Nes, M.L. Kersten
 * The String Module
 * Strings can be created in many ways. Already in the built-in
 * operations each atom can be cast to a string using the str(atom)
 * mil command.  The string module gives the possibility of
 * construction string as a substring of the a given string (s). There
 * are two such construction functions.  The first is the substring
 * from some position (offset) until the end of the string. The second
 * start again on the given offset position but only copies count
 * number of bytes. The functions fail when the position and count
 * fall out of bounds. A negative position indicates that the position
 * is computed from the end of the source string.
 *
 * The strings can be compared using the "=" and "!=" operators.
 *
 * The operator "+" concatenates a string and an atom. The atom will
 * be converted to a string using the atom to string c function. The
 * string and the result of the conversion are concatenated to form a
 * new string. This string is returned.
 *
 * The length function returns the length of the string. The length is
 * the number of characters in the string. The maximum string length
 * handled by the kernel is 32-bits long.
 *
 * chrAt() returns the character at position index in the string
 * s. The function will fail when the index is out of range. The range
 * is from 0 to length(s)-1.
 *
 * The startsWith and endsWith functions test if the string s starts
 * with or ends with the given prefix or suffix.
 *
 * The toLower and toUpper functions cast the string to lower or upper
 * case characters.
 *
 * The search(str,chr) function searches for the first occurrence of a
 * character from the begining of the string. The search(chr,str)
 * searches for the last occurrence (or first from the end of the
 * string). The last search function locates the position of first
 * occurrence of the string s2 in string s. All search functions
 * return -1 when the search failed.  Otherwise the position is
 * returned.
 *
 * All string functions fail when an incorrect string (NULL pointer)
 * is given.  In the current implementation, a fail is signaled by
 * returning nil, since this facilitates the use of the string module
 * in bulk operations.
 *
 * All functions in the module have now been converted to
 * Unicode. Internally, we use UTF-8 to store strings as Unicode in
 * zero-terminated byte-sequences.
 */
#include "monetdb_config.h"
#include "str.h"
#include <string.h>
#ifdef HAVE_ICONV
#include <iconv.h>
#include <locale.h>
#endif
#ifdef HAVE_WCWIDTH
#include <wchar.h>
#endif
#include "mal_interpreter.h"

#define UTF8_assert(s)		assert(checkUTF8(s))

/* return the number of codepoints in `s' before `end'. */
static inline int
UTF8_strpos(const char *s, const char *end)
{
	int pos = 0;

	UTF8_assert(s);

	if (s > end) {
		return -1;
	}
	while (s < end) {
		/* just count leading bytes of encoded code points; only works
		 * for correctly encoded UTF-8 */
		pos += (*s++ & 0xC0) != 0x80;
	}
	return pos;
}

/* return a pointer to the byte that starts the pos'th (0-based)
 * codepoint in s */
static inline char *
UTF8_strtail(const char *s, int pos)
{
	UTF8_assert(s);
	while (*s) {
		if ((*s & 0xC0) != 0x80) {
			if (pos <= 0)
				break;
			pos--;
		}
		s++;
	}
	return (char *) s;
}

/* copy n Unicode codepoints from s to dst, return pointer to new end */
static inline str
UTF8_strncpy(char *restrict dst, const char *restrict s, int n)
{
	UTF8_assert(s);
	while (*s && n) {
		if ((*s & 0xF8) == 0xF0) {
			/* 4 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
		} else if ((*s & 0xF0) == 0xE0) {
			/* 3 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
			*dst++ = *s++;
		} else if ((*s & 0xE0) == 0xC0) {
			/* 2 byte UTF-8 sequence */
			*dst++ = *s++;
			*dst++ = *s++;
		} else {
			/* 1 byte UTF-8 "sequence" */
			*dst++ = *s++;
		}
		n--;
	}
	*dst = '\0';
	return dst;
}

/* return number of Unicode codepoints in s; s is not nil */
int
UTF8_strlen(const char *s)
{								/* This function assumes, s is never nil */
	size_t pos = 0;

	UTF8_assert(s);
	assert(!strNil(s));

	while (*s) {
		/* just count leading bytes of encoded code points; only works
		 * for correctly encoded UTF-8 */
		pos += (*s++ & 0xC0) != 0x80;
	}
	assert(pos < INT_MAX);
	return (int) pos;
}

/* return (int) strlen(s); s is not nil */
int
str_strlen(const char *s)
{								/* This function assumes s is never nil */
	UTF8_assert(s);
	assert(!strNil(s));

	return (int) strlen(s);
}

#ifndef HAVE_WCWIDTH
struct interval {
	int first;
	int last;
};

static const struct interval zero_width[] = {
	/* sorted list of non-overlapping ranges; ranges represent all
	 * codepoints with general_category Me, Mn or Cf except U+00AD (SOFT
	 * HYPHEN), all codepoints \U+1160 through U+11FF (Hangul Jamo
	 * medial vowels and final consonants) -- see
	 * https://www.cl.cam.ac.uk/~mgk25/ucs/wcwidth.c from which this is
	 * derived */
	{ 0x0300, 0x036F }, { 0x0483, 0x0489 }, { 0x0591, 0x05BD },
	{ 0x05BF, 0x05BF }, { 0x05C1, 0x05C2 }, { 0x05C4, 0x05C5 },
	{ 0x05C7, 0x05C7 }, { 0x0600, 0x0605 }, { 0x0610, 0x061A },
	{ 0x061C, 0x061C }, { 0x064B, 0x065F }, { 0x0670, 0x0670 },
	{ 0x06D6, 0x06DD }, { 0x06DF, 0x06E4 }, { 0x06E7, 0x06E8 },
	{ 0x06EA, 0x06ED }, { 0x070F, 0x070F }, { 0x0711, 0x0711 },
	{ 0x0730, 0x074A }, { 0x07A6, 0x07B0 }, { 0x07EB, 0x07F3 },
	{ 0x07FD, 0x07FD }, { 0x0816, 0x0819 }, { 0x081B, 0x0823 },
	{ 0x0825, 0x0827 }, { 0x0829, 0x082D }, { 0x0859, 0x085B },
	{ 0x0890, 0x0891 }, { 0x0898, 0x089F }, { 0x08CA, 0x0902 },
	{ 0x093A, 0x093A }, { 0x093C, 0x093C }, { 0x0941, 0x0948 },
	{ 0x094D, 0x094D }, { 0x0951, 0x0957 }, { 0x0962, 0x0963 },
	{ 0x0981, 0x0981 }, { 0x09BC, 0x09BC }, { 0x09C1, 0x09C4 },
	{ 0x09CD, 0x09CD }, { 0x09E2, 0x09E3 }, { 0x09FE, 0x09FE },
	{ 0x0A01, 0x0A02 }, { 0x0A3C, 0x0A3C }, { 0x0A41, 0x0A42 },
	{ 0x0A47, 0x0A48 }, { 0x0A4B, 0x0A4D }, { 0x0A51, 0x0A51 },
	{ 0x0A70, 0x0A71 }, { 0x0A75, 0x0A75 }, { 0x0A81, 0x0A82 },
	{ 0x0ABC, 0x0ABC }, { 0x0AC1, 0x0AC5 }, { 0x0AC7, 0x0AC8 },
	{ 0x0ACD, 0x0ACD }, { 0x0AE2, 0x0AE3 }, { 0x0AFA, 0x0AFF },
	{ 0x0B01, 0x0B01 }, { 0x0B3C, 0x0B3C }, { 0x0B3F, 0x0B3F },
	{ 0x0B41, 0x0B44 }, { 0x0B4D, 0x0B4D }, { 0x0B55, 0x0B56 },
	{ 0x0B62, 0x0B63 }, { 0x0B82, 0x0B82 }, { 0x0BC0, 0x0BC0 },
	{ 0x0BCD, 0x0BCD }, { 0x0C00, 0x0C00 }, { 0x0C04, 0x0C04 },
	{ 0x0C3C, 0x0C3C }, { 0x0C3E, 0x0C40 }, { 0x0C46, 0x0C48 },
	{ 0x0C4A, 0x0C4D }, { 0x0C55, 0x0C56 }, { 0x0C62, 0x0C63 },
	{ 0x0C81, 0x0C81 }, { 0x0CBC, 0x0CBC }, { 0x0CBF, 0x0CBF },
	{ 0x0CC6, 0x0CC6 }, { 0x0CCC, 0x0CCD }, { 0x0CE2, 0x0CE3 },
	{ 0x0D00, 0x0D01 }, { 0x0D3B, 0x0D3C }, { 0x0D41, 0x0D44 },
	{ 0x0D4D, 0x0D4D }, { 0x0D62, 0x0D63 }, { 0x0D81, 0x0D81 },
	{ 0x0DCA, 0x0DCA }, { 0x0DD2, 0x0DD4 }, { 0x0DD6, 0x0DD6 },
	{ 0x0E31, 0x0E31 }, { 0x0E34, 0x0E3A }, { 0x0E47, 0x0E4E },
	{ 0x0EB1, 0x0EB1 }, { 0x0EB4, 0x0EBC }, { 0x0EC8, 0x0ECE },
	{ 0x0F18, 0x0F19 }, { 0x0F35, 0x0F35 }, { 0x0F37, 0x0F37 },
	{ 0x0F39, 0x0F39 }, { 0x0F71, 0x0F7E }, { 0x0F80, 0x0F84 },
	{ 0x0F86, 0x0F87 }, { 0x0F8D, 0x0F97 }, { 0x0F99, 0x0FBC },
	{ 0x0FC6, 0x0FC6 }, { 0x102D, 0x1030 }, { 0x1032, 0x1037 },
	{ 0x1039, 0x103A }, { 0x103D, 0x103E }, { 0x1058, 0x1059 },
	{ 0x105E, 0x1060 }, { 0x1071, 0x1074 }, { 0x1082, 0x1082 },
	{ 0x1085, 0x1086 }, { 0x108D, 0x108D }, { 0x109D, 0x109D },
	{ 0x1160, 0x11FF }, { 0x135D, 0x135F }, { 0x1712, 0x1714 },
	{ 0x1732, 0x1733 }, { 0x1752, 0x1753 }, { 0x1772, 0x1773 },
	{ 0x17B4, 0x17B5 }, { 0x17B7, 0x17BD }, { 0x17C6, 0x17C6 },
	{ 0x17C9, 0x17D3 }, { 0x17DD, 0x17DD }, { 0x180B, 0x180F },
	{ 0x1885, 0x1886 }, { 0x18A9, 0x18A9 }, { 0x1920, 0x1922 },
	{ 0x1927, 0x1928 }, { 0x1932, 0x1932 }, { 0x1939, 0x193B },
	{ 0x1A17, 0x1A18 }, { 0x1A1B, 0x1A1B }, { 0x1A56, 0x1A56 },
	{ 0x1A58, 0x1A5E }, { 0x1A60, 0x1A60 }, { 0x1A62, 0x1A62 },
	{ 0x1A65, 0x1A6C }, { 0x1A73, 0x1A7C }, { 0x1A7F, 0x1A7F },
	{ 0x1AB0, 0x1ACE }, { 0x1B00, 0x1B03 }, { 0x1B34, 0x1B34 },
	{ 0x1B36, 0x1B3A }, { 0x1B3C, 0x1B3C }, { 0x1B42, 0x1B42 },
	{ 0x1B6B, 0x1B73 }, { 0x1B80, 0x1B81 }, { 0x1BA2, 0x1BA5 },
	{ 0x1BA8, 0x1BA9 }, { 0x1BAB, 0x1BAD }, { 0x1BE6, 0x1BE6 },
	{ 0x1BE8, 0x1BE9 }, { 0x1BED, 0x1BED }, { 0x1BEF, 0x1BF1 },
	{ 0x1C2C, 0x1C33 }, { 0x1C36, 0x1C37 }, { 0x1CD0, 0x1CD2 },
	{ 0x1CD4, 0x1CE0 }, { 0x1CE2, 0x1CE8 }, { 0x1CED, 0x1CED },
	{ 0x1CF4, 0x1CF4 }, { 0x1CF8, 0x1CF9 }, { 0x1DC0, 0x1DFF },
	{ 0x200B, 0x200F }, { 0x202A, 0x202E }, { 0x2060, 0x2064 },
	{ 0x2066, 0x206F }, { 0x20D0, 0x20F0 }, { 0x2CEF, 0x2CF1 },
	{ 0x2D7F, 0x2D7F }, { 0x2DE0, 0x2DFF }, { 0x302A, 0x302D },
	{ 0x3099, 0x309A }, { 0xA66F, 0xA672 }, { 0xA674, 0xA67D },
	{ 0xA69E, 0xA69F }, { 0xA6F0, 0xA6F1 }, { 0xA802, 0xA802 },
	{ 0xA806, 0xA806 }, { 0xA80B, 0xA80B }, { 0xA825, 0xA826 },
	{ 0xA82C, 0xA82C }, { 0xA8C4, 0xA8C5 }, { 0xA8E0, 0xA8F1 },
	{ 0xA8FF, 0xA8FF }, { 0xA926, 0xA92D }, { 0xA947, 0xA951 },
	{ 0xA980, 0xA982 }, { 0xA9B3, 0xA9B3 }, { 0xA9B6, 0xA9B9 },
	{ 0xA9BC, 0xA9BD }, { 0xA9E5, 0xA9E5 }, { 0xAA29, 0xAA2E },
	{ 0xAA31, 0xAA32 }, { 0xAA35, 0xAA36 }, { 0xAA43, 0xAA43 },
	{ 0xAA4C, 0xAA4C }, { 0xAA7C, 0xAA7C }, { 0xAAB0, 0xAAB0 },
	{ 0xAAB2, 0xAAB4 }, { 0xAAB7, 0xAAB8 }, { 0xAABE, 0xAABF },
	{ 0xAAC1, 0xAAC1 }, { 0xAAEC, 0xAAED }, { 0xAAF6, 0xAAF6 },
	{ 0xABE5, 0xABE5 }, { 0xABE8, 0xABE8 }, { 0xABED, 0xABED },
	{ 0xFB1E, 0xFB1E }, { 0xFE00, 0xFE0F }, { 0xFE20, 0xFE2F },
	{ 0xFEFF, 0xFEFF }, { 0xFFF9, 0xFFFB }, { 0x101FD, 0x101FD },
	{ 0x102E0, 0x102E0 }, { 0x10376, 0x1037A }, { 0x10A01, 0x10A03 },
	{ 0x10A05, 0x10A06 }, { 0x10A0C, 0x10A0F }, { 0x10A38, 0x10A3A },
	{ 0x10A3F, 0x10A3F }, { 0x10AE5, 0x10AE6 }, { 0x10D24, 0x10D27 },
	{ 0x10EAB, 0x10EAC }, { 0x10EFD, 0x10EFF }, { 0x10F46, 0x10F50 },
	{ 0x10F82, 0x10F85 }, { 0x11001, 0x11001 }, { 0x11038, 0x11046 },
	{ 0x11070, 0x11070 }, { 0x11073, 0x11074 }, { 0x1107F, 0x11081 },
	{ 0x110B3, 0x110B6 }, { 0x110B9, 0x110BA }, { 0x110BD, 0x110BD },
	{ 0x110C2, 0x110C2 }, { 0x110CD, 0x110CD }, { 0x11100, 0x11102 },
	{ 0x11127, 0x1112B }, { 0x1112D, 0x11134 }, { 0x11173, 0x11173 },
	{ 0x11180, 0x11181 }, { 0x111B6, 0x111BE }, { 0x111C9, 0x111CC },
	{ 0x111CF, 0x111CF }, { 0x1122F, 0x11231 }, { 0x11234, 0x11234 },
	{ 0x11236, 0x11237 }, { 0x1123E, 0x1123E }, { 0x11241, 0x11241 },
	{ 0x112DF, 0x112DF }, { 0x112E3, 0x112EA }, { 0x11300, 0x11301 },
	{ 0x1133B, 0x1133C }, { 0x11340, 0x11340 }, { 0x11366, 0x1136C },
	{ 0x11370, 0x11374 }, { 0x11438, 0x1143F }, { 0x11442, 0x11444 },
	{ 0x11446, 0x11446 }, { 0x1145E, 0x1145E }, { 0x114B3, 0x114B8 },
	{ 0x114BA, 0x114BA }, { 0x114BF, 0x114C0 }, { 0x114C2, 0x114C3 },
	{ 0x115B2, 0x115B5 }, { 0x115BC, 0x115BD }, { 0x115BF, 0x115C0 },
	{ 0x115DC, 0x115DD }, { 0x11633, 0x1163A }, { 0x1163D, 0x1163D },
	{ 0x1163F, 0x11640 }, { 0x116AB, 0x116AB }, { 0x116AD, 0x116AD },
	{ 0x116B0, 0x116B5 }, { 0x116B7, 0x116B7 }, { 0x1171D, 0x1171F },
	{ 0x11722, 0x11725 }, { 0x11727, 0x1172B }, { 0x1182F, 0x11837 },
	{ 0x11839, 0x1183A }, { 0x1193B, 0x1193C }, { 0x1193E, 0x1193E },
	{ 0x11943, 0x11943 }, { 0x119D4, 0x119D7 }, { 0x119DA, 0x119DB },
	{ 0x119E0, 0x119E0 }, { 0x11A01, 0x11A0A }, { 0x11A33, 0x11A38 },
	{ 0x11A3B, 0x11A3E }, { 0x11A47, 0x11A47 }, { 0x11A51, 0x11A56 },
	{ 0x11A59, 0x11A5B }, { 0x11A8A, 0x11A96 }, { 0x11A98, 0x11A99 },
	{ 0x11C30, 0x11C36 }, { 0x11C38, 0x11C3D }, { 0x11C3F, 0x11C3F },
	{ 0x11C92, 0x11CA7 }, { 0x11CAA, 0x11CB0 }, { 0x11CB2, 0x11CB3 },
	{ 0x11CB5, 0x11CB6 }, { 0x11D31, 0x11D36 }, { 0x11D3A, 0x11D3A },
	{ 0x11D3C, 0x11D3D }, { 0x11D3F, 0x11D45 }, { 0x11D47, 0x11D47 },
	{ 0x11D90, 0x11D91 }, { 0x11D95, 0x11D95 }, { 0x11D97, 0x11D97 },
	{ 0x11EF3, 0x11EF4 }, { 0x11F00, 0x11F01 }, { 0x11F36, 0x11F3A },
	{ 0x11F40, 0x11F40 }, { 0x11F42, 0x11F42 }, { 0x13430, 0x13440 },
	{ 0x13447, 0x13455 }, { 0x16AF0, 0x16AF4 }, { 0x16B30, 0x16B36 },
	{ 0x16F4F, 0x16F4F }, { 0x16F8F, 0x16F92 }, { 0x16FE4, 0x16FE4 },
	{ 0x1BC9D, 0x1BC9E }, { 0x1BCA0, 0x1BCA3 }, { 0x1CF00, 0x1CF2D },
	{ 0x1CF30, 0x1CF46 }, { 0x1D167, 0x1D169 }, { 0x1D173, 0x1D182 },
	{ 0x1D185, 0x1D18B }, { 0x1D1AA, 0x1D1AD }, { 0x1D242, 0x1D244 },
	{ 0x1DA00, 0x1DA36 }, { 0x1DA3B, 0x1DA6C }, { 0x1DA75, 0x1DA75 },
	{ 0x1DA84, 0x1DA84 }, { 0x1DA9B, 0x1DA9F }, { 0x1DAA1, 0x1DAAF },
	{ 0x1E000, 0x1E006 }, { 0x1E008, 0x1E018 }, { 0x1E01B, 0x1E021 },
	{ 0x1E023, 0x1E024 }, { 0x1E026, 0x1E02A }, { 0x1E08F, 0x1E08F },
	{ 0x1E130, 0x1E136 }, { 0x1E2AE, 0x1E2AE }, { 0x1E2EC, 0x1E2EF },
	{ 0x1E4EC, 0x1E4EF }, { 0x1E8D0, 0x1E8D6 }, { 0x1E944, 0x1E94A },
	{ 0xE0001, 0xE0001 }, { 0xE0020, 0xE007F }, { 0xE0100, 0xE01EF },
};

static const struct interval double_width[] = {
	/* sorted list of non-overlapping ranges; ranges represent all
	 * codepoints in the East Asian Wide (W) or East Asian Full-width
	 * (F) category as defined in the EastAsianWidth.txt file */
	{ 0x1100, 0x115F }, { 0x231A, 0x231B }, { 0x2329, 0x232A },
	{ 0x23E9, 0x23EC }, { 0x23F0, 0x23F0 }, { 0x23F3, 0x23F3 },
	{ 0x25FD, 0x25FE }, { 0x2614, 0x2615 }, { 0x2648, 0x2653 },
	{ 0x267F, 0x267F }, { 0x2693, 0x2693 }, { 0x26A1, 0x26A1 },
	{ 0x26AA, 0x26AB }, { 0x26BD, 0x26BE }, { 0x26C4, 0x26C5 },
	{ 0x26CE, 0x26CE }, { 0x26D4, 0x26D4 }, { 0x26EA, 0x26EA },
	{ 0x26F2, 0x26F3 }, { 0x26F5, 0x26F5 }, { 0x26FA, 0x26FA },
	{ 0x26FD, 0x26FD }, { 0x2705, 0x2705 }, { 0x270A, 0x270B },
	{ 0x2728, 0x2728 }, { 0x274C, 0x274C }, { 0x274E, 0x274E },
	{ 0x2753, 0x2755 }, { 0x2757, 0x2757 }, { 0x2795, 0x2797 },
	{ 0x27B0, 0x27B0 }, { 0x27BF, 0x27BF }, { 0x2B1B, 0x2B1C },
	{ 0x2B50, 0x2B50 }, { 0x2B55, 0x2B55 }, { 0x2E80, 0x2E99 },
	{ 0x2E9B, 0x2EF3 }, { 0x2F00, 0x2FD5 }, { 0x2FF0, 0x303E },
	{ 0x3041, 0x3096 }, { 0x3099, 0x30FF }, { 0x3105, 0x312F },
	{ 0x3131, 0x318E }, { 0x3190, 0x31E3 }, { 0x31EF, 0x321E },
	{ 0x3220, 0x3247 }, { 0x3250, 0x3400 }, { 0x4DBF, 0x4DBF },
	{ 0x4E00, 0x4E00 }, { 0x9FFF, 0xA48C }, { 0xA490, 0xA4C6 },
	{ 0xA960, 0xA97C }, { 0xAC00, 0xAC00 }, { 0xD7A3, 0xD7A3 },
	{ 0xF900, 0xFA6D }, { 0xFA70, 0xFAD9 }, { 0xFE10, 0xFE19 },
	{ 0xFE30, 0xFE52 }, { 0xFE54, 0xFE66 }, { 0xFE68, 0xFE6B },
	{ 0xFF01, 0xFF60 }, { 0xFFE0, 0xFFE6 }, { 0x16FE0, 0x16FE4 },
	{ 0x16FF0, 0x16FF1 }, { 0x17000, 0x17000 }, { 0x187F7, 0x187F7 },
	{ 0x18800, 0x18CD5 }, { 0x18D00, 0x18D00 }, { 0x18D08, 0x18D08 },
	{ 0x1AFF0, 0x1AFF3 }, { 0x1AFF5, 0x1AFFB }, { 0x1AFFD, 0x1AFFE },
	{ 0x1B000, 0x1B122 }, { 0x1B132, 0x1B132 }, { 0x1B150, 0x1B152 },
	{ 0x1B155, 0x1B155 }, { 0x1B164, 0x1B167 }, { 0x1B170, 0x1B2FB },
	{ 0x1F004, 0x1F004 }, { 0x1F0CF, 0x1F0CF }, { 0x1F18E, 0x1F18E },
	{ 0x1F191, 0x1F19A }, { 0x1F200, 0x1F202 }, { 0x1F210, 0x1F23B },
	{ 0x1F240, 0x1F248 }, { 0x1F250, 0x1F251 }, { 0x1F260, 0x1F265 },
	{ 0x1F300, 0x1F320 }, { 0x1F32D, 0x1F335 }, { 0x1F337, 0x1F37C },
	{ 0x1F37E, 0x1F393 }, { 0x1F3A0, 0x1F3CA }, { 0x1F3CF, 0x1F3D3 },
	{ 0x1F3E0, 0x1F3F0 }, { 0x1F3F4, 0x1F3F4 }, { 0x1F3F8, 0x1F43E },
	{ 0x1F440, 0x1F440 }, { 0x1F442, 0x1F4FC }, { 0x1F4FF, 0x1F53D },
	{ 0x1F54B, 0x1F54E }, { 0x1F550, 0x1F567 }, { 0x1F57A, 0x1F57A },
	{ 0x1F595, 0x1F596 }, { 0x1F5A4, 0x1F5A4 }, { 0x1F5FB, 0x1F64F },
	{ 0x1F680, 0x1F6C5 }, { 0x1F6CC, 0x1F6CC }, { 0x1F6D0, 0x1F6D2 },
	{ 0x1F6D5, 0x1F6D7 }, { 0x1F6DC, 0x1F6DF }, { 0x1F6EB, 0x1F6EC },
	{ 0x1F6F4, 0x1F6FC }, { 0x1F7E0, 0x1F7EB }, { 0x1F7F0, 0x1F7F0 },
	{ 0x1F90C, 0x1F93A }, { 0x1F93C, 0x1F945 }, { 0x1F947, 0x1F9FF },
	{ 0x1FA70, 0x1FA7C }, { 0x1FA80, 0x1FA88 }, { 0x1FA90, 0x1FABD },
	{ 0x1FABF, 0x1FAC5 }, { 0x1FACE, 0x1FADB }, { 0x1FAE0, 0x1FAE8 },
	{ 0x1FAF0, 0x1FAF8 }, { 0x20000, 0x20000 }, { 0x2A6DF, 0x2A6DF },
	{ 0x2A700, 0x2A700 }, { 0x2B739, 0x2B739 }, { 0x2B740, 0x2B740 },
	{ 0x2B81D, 0x2B81D }, { 0x2B820, 0x2B820 }, { 0x2CEA1, 0x2CEA1 },
	{ 0x2CEB0, 0x2CEB0 }, { 0x2EBE0, 0x2EBE0 }, { 0x2EBF0, 0x2EBF0 },
	{ 0x2EE5D, 0x2EE5D }, { 0x2F800, 0x2FA1D }, { 0x30000, 0x30000 },
	{ 0x3134A, 0x3134A }, { 0x31350, 0x31350 }, { 0x323AF, 0x323AF },
};

static inline bool
binsearch(int c, int max, const struct interval *tab)
{
	int min = 0;

	if (c >= tab[min].first && c <= tab[max].last) {
		while (max >= min) {
			int mid = (min + max) / 2;
			if (c > tab[mid].last)
				min = mid + 1;
			else if (c < tab[mid].first)
				max = mid - 1;
			else
				return true;
		}
	}
	return false;
}

static inline bool
iszerowidth(int c)
{
	return binsearch(c, (int) (sizeof(zero_width) / sizeof(zero_width[0])) - 1, zero_width);
}

static inline bool
isdoublewidth(int c)
{
	return binsearch(c, (int) (sizeof(double_width) / sizeof(double_width[0])) - 1, zero_width);
}

static int
wcwidth(int c)
{
	if (c == 0)
		return 0;
	if ((c & ~0x80) <= 0x1F || c == 0x007F)
		return -1;				/* control character or DELETE */
	if (iszerowidth(c))
		return 0;
	if (isdoublewidth(c))
		return 2;
	return 1;
}
#endif

/* return the display width of s */
int
UTF8_strwidth(const char *s)
{
	int len = 0;
	int c;
	int n;

	if (strNil(s))
		return int_nil;
	c = 0;
	n = 0;
	while (*s != 0) {
		if ((*s & 0x80) == 0) {
			assert(n == 0);
			len++;
			n = 0;
		} else if ((*s & 0xC0) == 0x80) {
			c = (c << 6) | (*s & 0x3F);
			if (--n == 0) {
				/* last byte of a multi-byte character */
				n = wcwidth(c);
				if (n >= 0)
					len += n;
				else
					len++;		/* assume width 1 if unprintable */
				n = 0;
			}
		} else if ((*s & 0xE0) == 0xC0) {
			assert(n == 0);
			n = 1;
			c = *s & 0x1F;
		} else if ((*s & 0xF0) == 0xE0) {
			assert(n == 0);
			n = 2;
			c = *s & 0x0F;
		} else if ((*s & 0xF8) == 0xF0) {
			assert(n == 0);
			n = 3;
			c = *s & 0x07;
		} else if ((*s & 0xFC) == 0xF8) {
			assert(n == 0);
			n = 4;
			c = *s & 0x03;
		} else {
			assert(0);
			n = 0;
		}
		s++;
	}
	return len;
}

/*
 * Here you find the wrappers around the version 4 library code
 * It also contains the direct implementation of the string
 * matching support routines.
 */
#include "mal_exception.h"

/*
 * The SQL like function return a boolean
 */
static bool
STRlike(const char *s, const char *pat, const char *esc)
{
	const char *t, *p;

	t = s;
	for (p = pat; *p && *t; p++) {
		if (esc && *p == *esc) {
			p++;
			if (*p != *t)
				return false;
			t++;
		} else if (*p == '_')
			t++;
		else if (*p == '%') {
			p++;
			while (*p == '%')
				p++;
			if (*p == 0)
				return true;	/* tail is acceptable */
			for (; *p && *t; t++)
				if (STRlike(t, p, esc))
					return true;
			if (*p == 0 && *t == 0)
				return true;
			return false;
		} else if (*p == *t)
			t++;
		else
			return false;
	}
	if (*p == '%' && *(p + 1) == 0)
		return true;
	return *t == 0 && *p == 0;
}

static str
STRlikewrap3(bit *ret, const str *s, const str *pat, const str *esc)
{
	if (strNil(*s) || strNil(*pat) || strNil(*esc))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, *esc);
	return MAL_SUCCEED;
}

static str
STRlikewrap(bit *ret, const str *s, const str *pat)
{
	if (strNil(*s) || strNil(*pat))
		*ret = bit_nil;
	else
		*ret = (bit) STRlike(*s, *pat, NULL);
	return MAL_SUCCEED;
}

static str
STRtostr(str *res, const str *src)
{
	if (*src == 0)
		*res = GDKstrdup(str_nil);
	else
		*res = GDKstrdup(*src);
	if (*res == NULL)
		throw(MAL, "str.str", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
STRLength(int *res, const str *arg1)
{
	const char *s = *arg1;

	*res = strNil(s) ? int_nil : UTF8_strlen(s);
	return MAL_SUCCEED;
}

static str
STRBytes(int *res, const str *arg1)
{
	const char *s = *arg1;

	*res = strNil(s) ? int_nil : str_strlen(s);
	return MAL_SUCCEED;
}

str
str_tail(str *buf, size_t *buflen, const char *s, int off)
{
	if (off < 0) {
		off += UTF8_strlen(s);
		if (off < 0)
			off = 0;
	}
	const char *tail = UTF8_strtail(s, off);
	size_t nextlen = strlen(tail) + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.tail");
	strcpy(*buf, tail);
	return MAL_SUCCEED;
}

static str
STRTail(str *res, const str *arg1, const int *offset)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset;

	if (strNil(s) || is_int_nil(off)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.tail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_tail(&buf, &buflen, s, off)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.tail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* copy the substring s[off:off+l] into *buf, replacing *buf with a
 * freshly allocated buffer if the substring doesn't fit; off is 0
 * based, and both off and l count in Unicode codepoints (i.e. not
 * bytes); if off < 0, off counts from the end of the string */
str
str_Sub_String(str *buf, size_t *buflen, const char *s, int off, int l)
{
	size_t len;

	if (off < 0) {
		off += UTF8_strlen(s);
		if (off < 0) {
			l += off;
			off = 0;
		}
	}
	/* here, off >= 0 */
	if (l < 0) {
		strcpy(*buf, "");
		return MAL_SUCCEED;
	}
	s = UTF8_strtail(s, off);
	len = (size_t) (UTF8_strtail(s, l) - s + 1);
	CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.substring");
	strcpy_len(*buf, s, len);
	return MAL_SUCCEED;
}

static str
STRSubString(str *res, const str *arg1, const int *offset, const int *length)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int off = *offset, len = *length;

	if (strNil(s) || is_int_nil(off) || is_int_nil(len)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, off, len)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substring",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_from_wchr(str *buf, size_t *buflen, int c)
{
	CHECK_STR_BUFFER_LENGTH(buf, buflen, 5, "str.unicode");
	str s = *buf;
	UTF8_PUTCHAR(c, s);
	*s = 0;
	return MAL_SUCCEED;
  illegal:
	throw(MAL, "str.unicode", SQLSTATE(42000) "Illegal Unicode code point");
}

static str
STRFromWChr(str *res, const int *c)
{
	str buf = NULL, msg = MAL_SUCCEED;
	int cc = *c;

	if (is_int_nil(cc)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = MAX(strlen(str_nil) + 1, 8);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.unicode", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_from_wchr(&buf, &buflen, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.unicode",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* return the Unicode code point of arg1 at position at */
str
str_wchr_at(int *res, const char *s, int at)
{
	/* 64bit: should have lng arg */
	if (strNil(s) || is_int_nil(at) || at < 0) {
		*res = int_nil;
		return MAL_SUCCEED;
	}
	s = UTF8_strtail(s, at);
	if (s == NULL || *s == 0) {
		*res = int_nil;
		return MAL_SUCCEED;
	}
	UTF8_GETCHAR(*res, s);
	return MAL_SUCCEED;
  illegal:
	throw(MAL, "str.unicodeAt", SQLSTATE(42000) "Illegal Unicode code point");
}

static str
STRWChrAt(int *res, const str *arg1, const int *at)
{
	return str_wchr_at(res, *arg1, *at);
}

static inline str
STRlower(str *res, const str *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lower", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (GDKtolower(&buf, &buflen, s) != GDK_SUCCEED) {
			GDKfree(buf);
			throw(MAL, "str.lower", GDK_EXCEPTION);
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lower",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRupper(str *res, const str *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.upper", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (GDKtoupper(&buf, &buflen, s) != GDK_SUCCEED) {
			GDKfree(buf);
			throw(MAL, "str.upper", GDK_EXCEPTION);
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.upper",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* returns whether arg1 starts with arg2 */
bit
str_is_prefix(const char *s, const char *prefix, int plen)
{
	return strncmp(s, prefix, plen) == 0;
}

bit
str_is_iprefix(const char *s, const char *prefix, int plen)
{
	return GDKstrncasecmp(s, prefix, SIZE_MAX, plen) == 0;
}

static str
STRstartswith(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bit *res = getArgReference(stk, pci, 0);
	const str *arg1 = getArgReference(stk, pci, 1),
		*arg2 = getArgReference(stk, pci, 2);
	bit icase = pci->argc == 4
			&& *getArgReference_bit(stk, pci, 3) ? true : false;
	str s = *arg1, prefix = *arg2, msg = MAL_SUCCEED;
	if (strNil(s) || strNil(prefix)) {
		*res = bit_nil;
	} else {
		int plen = str_strlen(prefix);

		*res = icase ?
			str_is_iprefix(s, prefix, plen) :
			str_is_prefix(s, prefix, plen);
	}
	return msg;
}

bit
str_is_suffix(const char *s, const char *suffix, int sul)
{
	int sl = str_strlen(s);

	if (sl < sul)
		return 0;
	else
		return strcmp(s + sl - sul, suffix) == 0;
}

bit
str_is_isuffix(const char *s, const char *suffix, int sul)
{
	(void) sul;
	int sufl = UTF8_strlen(suffix);
	int sl = UTF8_strlen(s);
	if (sufl > sl) {
		/* suffix too long */
		return 0;
	}
	return GDKstrcasecmp(UTF8_strtail(s, sl - sufl), suffix) == 0;
}


/* returns whether arg1 ends with arg2 */
static str
STRendswith(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bit *res = getArgReference(stk, pci, 0);
	const str *arg1 = getArgReference(stk, pci, 1),
		*arg2 = getArgReference(stk, pci, 2);
	bit icase = pci->argc == 4
			&& *getArgReference_bit(stk, pci, 3) ? true : false;
	str s = *arg1, suffix = *arg2, msg = MAL_SUCCEED;
	if (strNil(s) || strNil(suffix)) {
		*res = bit_nil;
	} else {
		int sul = str_strlen(suffix);

		*res = icase ?
			str_is_isuffix(s, suffix, sul) :
			str_is_suffix(s, suffix, sul);
	}
	return msg;
}

bit
str_contains(const char *h, const char *n, int nlen)
{
	(void) nlen;
	return (strstr(h, n) != NULL);
}

bit
str_icontains(const char *h, const char *n, int nlen)
{
	(void) nlen;
	return (GDKstrcasestr(h, n) != NULL);
}

/* returns whether haystack contains needle */
static str
STRcontains(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bit *res = getArgReference(stk, pci, 0);
	const str *arg1 = getArgReference(stk, pci, 1),
		*arg2 = getArgReference(stk, pci, 2);
	bit icase = pci->argc == 4
			&& *getArgReference_bit(stk, pci, 3) ? true : false;
	str haystack = *arg1, needle = *arg2, msg = MAL_SUCCEED;
	if (strNil(haystack) || strNil(needle)) {
		*res = bit_nil;
	} else {
		int needle_len = str_strlen(needle);

		*res = icase ?
			str_icontains(haystack, needle, needle_len) :
			str_contains(haystack, needle, needle_len);
	}
	return msg;
}

int
str_search(const char *haystack, const char *needle)
{
	needle = strstr(haystack, needle);
	if (needle == NULL)
		return -1;

	return UTF8_strpos(haystack, needle);
}

int
str_isearch(const char *haystack, const char *needle)
{
	needle = GDKstrcasestr(haystack, needle);
	if (needle == NULL)
		return -1;

	return UTF8_strpos(haystack, needle);
}

/* find first occurrence of needle in haystack */
static str
STRstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bit *res = getArgReference(stk, pci, 0);
	const str *haystack = getArgReference(stk, pci, 1),
		*needle = getArgReference(stk, pci, 2);
	bit icase = pci->argc == 4
			&& *getArgReference_bit(stk, pci, 3) ? true : false;
	str s = *haystack, h = *needle, msg = MAL_SUCCEED;
	if (strNil(s) || strNil(h)) {
		*res = bit_nil;
	} else {
		*res = icase ?
			str_isearch(s, h) :
			str_search(s, h);
	}
	return msg;
}

int
str_reverse_str_search(const char *haystack, const char *needle)
{
	int nulen = UTF8_strlen(needle);
	size_t nlen = strlen(needle);

	for (int pos = str_strlen(haystack) - 1; pos >= 0; pos--) {
		if ((haystack[pos] & 0xC0) != 0x80) {
			if (nulen > 0)
				nulen--;
			else if (strncmp(haystack + pos, needle, nlen) == 0)
				return pos;
		}
	}
	return -1;
}

int
str_reverse_str_isearch(const char *haystack, const char *needle)
{
	int nulen = UTF8_strlen(needle);
	size_t nlen = strlen(needle);

	for (int pos = str_strlen(haystack) - 1; pos >= 0; pos--) {
		if ((haystack[pos] & 0xC0) != 0x80) {
			if (nulen > 0)
				nulen--;
			else if (GDKstrncasecmp(haystack + pos, needle, SIZE_MAX, nlen) == 0)
				return pos;
		}
	}
	return -1;
}

/* find last occurrence of arg2 in arg1 */
static str
STRrevstr_search(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	int *res = getArgReference_int(stk, pci, 0);
	const str haystack = *getArgReference_str(stk, pci, 1);
	const str needle = *getArgReference_str(stk, pci, 2);
	bit icase = pci->argc == 4
			&& *getArgReference_bit(stk, pci, 3) ? true : false;

	if (strNil(haystack) || strNil(needle)) {
		*res = bit_nil;
	} else {
		*res = icase ?
			str_reverse_str_isearch(haystack, needle) :
			str_reverse_str_search(haystack, needle);
	}
	return MAL_SUCCEED;
}

str
str_splitpart(str *buf, size_t *buflen, const char *s, const char *s2, int f)
{
	size_t len;
	char *p = NULL;

	if (f <= 0)
		throw(MAL, "str.splitpart",
			  SQLSTATE(42000) "field position must be greater than zero");

	len = strlen(s2);
	if (len) {
		while ((p = strstr(s, s2)) != NULL && f > 1) {
			s = p + len;
			f--;
		}
	}

	if (f != 1) {
		strcpy(*buf, "");
		return MAL_SUCCEED;
	}

	if (p == NULL) {
		len = strlen(s);
	} else {
		len = (size_t) (p - s);
	}

	len++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.splitpart");
	strcpy_len(*buf, s, len);
	return MAL_SUCCEED;
}

static str
STRsplitpart(str *res, str *haystack, str *needle, int *field)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *haystack, *s2 = *needle;
	int f = *field;

	if (strNil(s) || strNil(s2) || is_int_nil(f)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.splitpart", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_splitpart(&buf, &buflen, s, s2, f)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.splitpart",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* returns number of bytes to remove from left to strip the codepoints in rm */
static size_t
lstrip(const char *s, size_t len, const int *rm, size_t nrm)
{
	int c;
	size_t i, n, skip = 0;

	while (len > 0) {
		UTF8_NEXTCHAR(c, n, s);
		assert(n > 0 && n <= len);
		for (i = 0; i < nrm; i++) {
			if (rm[i] == c) {
				s += n;
				skip += n;
				len -= n;
				break;
			}
		}
		if (i == nrm)
			break;
	}
	return skip;
}

/* returns the resulting length of s after stripping codepoints in rm
 * from the right */
static size_t
rstrip(const char *s, size_t len, const int *rm, size_t nrm)
{
	int c;
	size_t i, n;

	while (len > 0) {
		UTF8_LASTCHAR(c, n, s, len);
		assert(n > 0 && n <= len);
		for (i = 0; i < nrm; i++) {
			if (rm[i] == c) {
				len -= n;
				break;
			}
		}
		if (i == nrm)
			break;
	}
	return len;
}

const int whitespace[] = {
	' ',						/* space */
	'\t',						/* tab (character tabulation) */
	'\n',						/* line feed */
	'\r',						/* carriage return */
	'\f',						/* form feed */
	'\v',						/* vertical tab (line tabulation) */
/* below the code points that have the Unicode Zs (space separator) property */
	0x00A0,						/* no-break space */
	0x1680,						/* ogham space mark */
	0x2000,						/* en quad */
	0x2001,						/* em quad */
	0x2002,						/* en space */
	0x2003,						/* em space */
	0x2004,						/* three-per-em space */
	0x2005,						/* four-per-em space */
	0x2006,						/* six-per-em space */
	0x2007,						/* figure space */
	0x2008,						/* punctuation space */
	0x2009,						/* thin space */
	0x200A,						/* hair space */
	0x202F,						/* narrow no-break space */
	0x205F,						/* medium mathematical space */
	0x3000,						/* ideographic space */
};

#define NSPACES		(sizeof(whitespace) / sizeof(whitespace[0]))

str
str_strip(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = lstrip(s, len, whitespace, NSPACES);
	s += n;
	len -= n;
	n = rstrip(s, len, whitespace, NSPACES);

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.strip");
	strcpy_len(*buf, s, n);
	return MAL_SUCCEED;
}

/* remove all whitespace from either side of arg1 */
static str
STRStrip(str *res, const str *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.strip",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_ltrim(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = lstrip(s, len, whitespace, NSPACES);
	size_t nallocate = len - n + 1;

	CHECK_STR_BUFFER_LENGTH(buf, buflen, nallocate, "str.ltrim");
	strcpy_len(*buf, s + n, nallocate);
	return MAL_SUCCEED;
}

/* remove all whitespace from the start (left) of arg1 */
static str
STRLtrim(str *res, const str *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.ltrim",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rtrim(str *buf, size_t *buflen, const char *s)
{
	size_t len = strlen(s);
	size_t n = rstrip(s, len, whitespace, NSPACES);

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.rtrim");
	strcpy_len(*buf, s, n);
	return MAL_SUCCEED;
}

/* remove all whitespace from the end (right) of arg1 */
static str
STRRtrim(str *res, const str *arg1)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;

	if (strNil(s)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim(&buf, &buflen, s)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rtrim",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

/* return a list of codepoints in s */
static str
trimchars(str *buf, size_t *buflen, size_t *n, const char *s, size_t len_s,
		  const char *malfunc)
{
	size_t len = 0, nlen = len_s * sizeof(int);
	int c, *cbuf;

	assert(s);
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nlen, malfunc);
	cbuf = *(int **) buf;

	while (*s) {
		UTF8_GETCHAR(c, s);
		assert(!is_int_nil(c));
		cbuf[len++] = c;
	}
	*n = len;
	return MAL_SUCCEED;
  illegal:
	throw(MAL, malfunc, SQLSTATE(42000) "Illegal Unicode code point");
}

str
str_strip2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.strip2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.strip2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = lstrip(s, len, *(int **) buf, n3);
		s += n;
		len -= n;
		n = rstrip(s, len, *(int **) buf, n3);

		n++;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.strip2");
		strcpy_len(*buf, s, n);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * either side of arg1 */
static str
STRStrip2(str *res, const str *arg1, const str *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.strip2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_strip2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.strip2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_ltrim2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3, nallocate;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.ltrim2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.ltrim2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = lstrip(s, len, *(int **) buf, n3);
		nallocate = len - n + 1;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, nallocate, "str.ltrim2");
		strcpy_len(*buf, s + n, nallocate);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * the start (left) of arg1 */
static str
STRLtrim2(str *res, const str *arg1, const str *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.ltrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_ltrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.ltrim2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rtrim2(str *buf, size_t *buflen, const char *s, const char *s2)
{
	str msg = MAL_SUCCEED;
	size_t len, n, n2, n3;

	if ((n2 = strlen(s2)) == 0) {
		len = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, len, "str.rtrim2");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	} else {
		if ((msg = trimchars(buf, buflen, &n3, s2, n2, "str.ltrim2")) != MAL_SUCCEED)
			return msg;
		len = strlen(s);
		n = rstrip(s, len, *(int **) buf, n3);
		n++;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.rtrim2");
		strcpy_len(*buf, s, n);
		return MAL_SUCCEED;
	}
}

/* remove the longest string containing only characters from arg2 from
 * the end (right) of arg1 */
static str
STRRtrim2(str *res, const str *arg1, const str *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;

	if (strNil(s) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH * sizeof(int);

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rtrim2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rtrim2(&buf, &buflen, s, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rtrim2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
pad(str *buf, size_t *buflen, const char *s, const char *pad, int len, int left,
	const char *malfunc)
{
	size_t slen, padlen, repeats, residual, i, nlen;
	char *res;

	if (len < 0)
		len = 0;

	slen = (size_t) UTF8_strlen(s);
	if (slen > (size_t) len) {
		/* truncate */
		pad = UTF8_strtail(s, len);
		slen = pad - s + 1;

		CHECK_STR_BUFFER_LENGTH(buf, buflen, slen, malfunc);
		strcpy_len(*buf, s, slen);
		return MAL_SUCCEED;
	}

	padlen = (size_t) UTF8_strlen(pad);
	if (slen == (size_t) len || padlen == 0) {
		/* nothing to do (no padding if there is no pad string) */
		slen = strlen(s) + 1;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, slen, malfunc);
		strcpy(*buf, s);
		return MAL_SUCCEED;
	}

	repeats = ((size_t) len - slen) / padlen;
	residual = ((size_t) len - slen) % padlen;
	if (residual > 0)
		residual = (size_t) (UTF8_strtail(pad, (int) residual) - pad);
	padlen = strlen(pad);
	slen = strlen(s);

	nlen = slen + repeats * padlen + residual + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nlen, malfunc);
	res = *buf;
	if (left) {
		for (i = 0; i < repeats; i++)
			memcpy(res + i * padlen, pad, padlen);
		if (residual > 0)
			memcpy(res + repeats * padlen, pad, residual);
		if (slen > 0)
			memcpy(res + repeats * padlen + residual, s, slen);
	} else {
		if (slen > 0)
			memcpy(res, s, slen);
		for (i = 0; i < repeats; i++)
			memcpy(res + slen + i * padlen, pad, padlen);
		if (residual > 0)
			memcpy(res + slen + repeats * padlen, pad, residual);
	}
	res[repeats * padlen + residual + slen] = 0;
	return MAL_SUCCEED;
}

str
str_lpad(str *buf, size_t *buflen, const char *s, int len)
{
	return pad(buf, buflen, s, " ", len, 1, "str.lpad");
}

/* Fill up 'arg1' to length 'len' by prepending whitespaces.
 * If 'arg1' is already longer than 'len', then it's truncated on the right
 * (NB: this is the PostgreSQL definition).
 *
 * Example: lpad('hi', 5)
 * Result: '   hi'
 */
static str
STRLpad(str *res, const str *arg1, const int *len)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rpad(str *buf, size_t *buflen, const char *s, int len)
{
	return pad(buf, buflen, s, " ", len, 0, "str.lpad");
}

/* Fill up 'arg1' to length 'len' by appending whitespaces.
 * If 'arg1' is already longer than 'len', then it's truncated (on the right)
 * (NB: this is the PostgreSQL definition).
 *
 * Example: rpad('hi', 5)
 * Result: 'hi   '
 */
static str
STRRpad(str *res, const str *arg1, const int *len)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *len;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rpad", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_lpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
{
	return pad(buf, buflen, s, s2, len, 1, "str.lpad2");
}

/* Fill up 'arg1' to length 'len' by prepending characters from 'arg2'
 * If 'arg1' is already longer than 'len', then it's truncated on the right
 * (NB: this is the PostgreSQL definition).
 *
 * Example: lpad('hi', 5, 'xy')
 * Result: xyxhi
 */
static str
STRLpad3(str *res, const str *arg1, const int *len, const str *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.lpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_lpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.lpad2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_rpad3(str *buf, size_t *buflen, const char *s, int len, const char *s2)
{
	return pad(buf, buflen, s, s2, len, 0, "str.rpad2");
}

/* Fill up 'arg1' to length 'len' by appending characters from 'arg2'
 * If 'arg1' is already longer than 'len', then it's truncated (on the right)
 * (NB: this is the PostgreSQL definition).
 *
 * Example: rpad('hi', 5, 'xy')
 * Result: hixyx
 */
static str
STRRpad3(str *res, const str *arg1, const int *len, const str *arg2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2;
	int l = *len;

	if (strNil(s) || strNil(s2) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.rpad2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_rpad3(&buf, &buflen, s, l, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.rpad2",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_substitute(str *buf, size_t *buflen, const char *s, const char *src,
			   const char *dst, bit repeat)
{
	size_t lsrc = strlen(src), ldst = strlen(dst), n, l = strlen(s);
	char *b, *fnd;
	const char *pfnd;

	if (!lsrc || !l) {			/* s/src is an empty string, there's nothing to substitute */
		l++;
		CHECK_STR_BUFFER_LENGTH(buf, buflen, l, "str.substitute");
		strcpy(*buf, s);
		return MAL_SUCCEED;
	}

	n = l + ldst;
	if (repeat && ldst > lsrc)
		n = (ldst * l) / lsrc;	/* max length */

	n++;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, n, "str.substitute");
	b = *buf;
	pfnd = s;
	do {
		fnd = strstr(pfnd, src);
		if (fnd == NULL)
			break;
		n = fnd - pfnd;
		if (n > 0) {
			strcpy_len(b, pfnd, n + 1);
			b += n;
		}
		if (ldst > 0) {
			strcpy_len(b, dst, ldst + 1);
			b += ldst;
		}
		if (*fnd == 0)
			break;
		pfnd = fnd + lsrc;
	} while (repeat);
	strcpy(b, pfnd);
	return MAL_SUCCEED;
}

static str
STRSubstitute(str *res, const str *arg1, const str *arg2, const str *arg3,
			  const bit *g)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1, *s2 = *arg2, *s3 = *arg3;

	if (strNil(s) || strNil(s2) || strNil(s3)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substitute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substitute(&buf, &buflen, s, s2, s3, *g)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substitute",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRascii(int *ret, const str *s)
{
	return str_wchr_at(ret, *s, 0);
}

str
str_substring_tail(str *buf, size_t *buflen, const char *s, int start)
{
	if (start < 1)
		start = 1;
	start--;
	return str_tail(buf, buflen, s, start);
}

static str
STRsubstringTail(str *res, const str *arg1, const int *start)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start;

	if (strNil(s) || is_int_nil(st)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substringTail", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_substring_tail(&buf, &buflen, s, st)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substringTail",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_sub_string(str *buf, size_t *buflen, const char *s, int start, int l)
{
	if (start < 1)
		start = 1;
	start--;
	return str_Sub_String(buf, buflen, s, start, l);
}

static str
STRsubstring(str *res, const str *arg1, const int *start, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int st = *start, l = *ll;

	if (strNil(s) || is_int_nil(st) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.substring", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_sub_string(&buf, &buflen, s, st, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.substring",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRprefix(str *res, const str *arg1, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.prefix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_Sub_String(&buf, &buflen, s, 0, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.prefix",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

str
str_suffix(str *buf, size_t *buflen, const char *s, int l)
{
	int start = (int) (strlen(s) - l);
	return str_Sub_String(buf, buflen, s, start, l);
}

static str
STRsuffix(str *res, const str *arg1, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int l = *ll;

	if (strNil(s) || is_int_nil(l)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.suffix", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_suffix(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.suffix",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

int
str_locate2(const char *needle, const char *haystack, int start)
{
	int off, res;
	const char *s;

	off = start <= 0 ? 1 : start;
	s = UTF8_strtail(haystack, off - 1);
	res = str_search(s, needle);
	return res >= 0 ? res + off : 0;
}

static str
STRlocate3(int *ret, const str *needle, const str *haystack, const int *start)
{
	const char *s = *needle, *s2 = *haystack;
	int st = *start;

	*ret = (strNil(s) || strNil(s2) || is_int_nil(st)) ?
		int_nil :
		str_locate2(s, s2, st);
	return MAL_SUCCEED;
}

static str
STRlocate(int *ret, const str *needle, const str *haystack)
{
	const char *s = *needle, *s2 = *haystack;

	*ret = (strNil(s) || strNil(s2)) ? int_nil : str_locate2(s, s2, 1);
	return MAL_SUCCEED;
}

str
str_insert(str *buf, size_t *buflen, const char *s, int strt, int l,
		   const char *s2)
{
	str v;
	int l1 = UTF8_strlen(s);
	size_t nextlen;

	if (l < 0)
		throw(MAL, "str.insert",
			  SQLSTATE(42000)
			  "The number of characters for insert function must be non negative");
	if (strt < 0) {
		if (-strt <= l1)
			strt = l1 + strt;
		else
			strt = 0;
	}
	if (strt > l1)
		strt = l1;

	nextlen = strlen(s) + strlen(s2) + 1;
	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.insert");
	v = *buf;
	if (strt > 0)
		v = UTF8_strncpy(v, s, strt);
	strcpy(v, s2);
	if (strt + l < l1)
		strcat(v, UTF8_strtail(s, strt + l));
	return MAL_SUCCEED;
}

static str
STRinsert(str *res, const str *input, const int *start, const int *nchars,
		  const str *input2)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *input, *s2 = *input2;
	int st = *start, n = *nchars;

	if (strNil(s) || is_int_nil(st) || is_int_nil(n) || strNil(s2)) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.insert", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_insert(&buf, &buflen, s, st, n, s2)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.insert",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRreplace(str *ret, const str *s1, const str *s2, const str *s3)
{
	bit flag = TRUE;
	return STRSubstitute(ret, s1, s2, s3, &flag);
}

str
str_repeat(str *buf, size_t *buflen, const char *s, int c)
{
	size_t l = strlen(s), nextlen;

	if (l >= INT_MAX)
		throw(MAL, "str.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	nextlen = (size_t) c *l + 1;

	CHECK_STR_BUFFER_LENGTH(buf, buflen, nextlen, "str.repeat");
	str t = *buf;
	*t = 0;
	for (int i = c; i > 0; i--, t += l)
		strcpy(t, s);
	return MAL_SUCCEED;
}

static str
STRrepeat(str *res, const str *arg1, const int *c)
{
	str buf = NULL, msg = MAL_SUCCEED;
	const char *s = *arg1;
	int cc = *c;

	if (strNil(s) || is_int_nil(cc) || cc < 0) {
		*res = GDKstrdup(str_nil);
	} else {
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.repeat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_repeat(&buf, &buflen, s, cc)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.repeat",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRspace(str *res, const int *ll)
{
	str buf = NULL, msg = MAL_SUCCEED;
	int l = *ll;

	if (is_int_nil(l) || l < 0) {
		*res = GDKstrdup(str_nil);
	} else {
		const char space[] = " ", *s = space;
		size_t buflen = INITIAL_STR_BUFFER_LENGTH;

		*res = NULL;
		if (!(buf = GDKmalloc(buflen)))
			throw(MAL, "str.space", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if ((msg = str_repeat(&buf, &buflen, s, l)) != MAL_SUCCEED) {
			GDKfree(buf);
			return msg;
		}
		*res = GDKstrdup(buf);
	}

	GDKfree(buf);
	if (!*res)
		msg = createException(MAL, "str.space",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return msg;
}

static str
STRasciify(str *r, const str *s)
{
#ifdef HAVE_ICONV

	if (strNil(*s)) {
		if ((*r = GDKstrdup(str_nil)) == NULL)
			throw(MAL, "str.asciify", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		else
			return MAL_SUCCEED;
	}

	iconv_t cd;
	const str f = "UTF-8", t = "ASCII//TRANSLIT";
	str in = *s, out;
	size_t in_len = strlen(in), out_len = in_len * 4; /* oversized as a single utf8 char could change into multiple ascii char */

	if ((cd = iconv_open(t, f)) == (iconv_t) (-1))
		throw(MAL, "str.asciify", "ICONV: cannot convert from (%s) to (%s).", f, t);

	if ((*r = out = GDKmalloc(out_len)) == NULL) {
		iconv_close(cd);
		throw(MAL, "str.asciify", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	str o = out;

	if (iconv(cd, &in, &in_len, &o, &out_len) == (size_t) -1) {
		GDKfree(out);
		*r = NULL;
		iconv_close(cd);
		throw(MAL, "str.asciify", "Conversion failed, possibly due to system locale %s.", setlocale(0, NULL));
	}

	*o = '\0';
	iconv_close(cd);
	return MAL_SUCCEED;

#else
	throw(MAL, "str.asciify", "ICONV library not available.");
#endif
}

/* scan select loop with or without candidates */
#define scanloop(TEST, KEEP_NULLS)									    \
	do {																\
		TRC_DEBUG(ALGO,													\
				  "scanselect(b=%s#"BUNFMT",anti=%d): "					\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  anti, #TEST);											\
		if (!s || BATtdense(s)) {										\
			for (; p < q; p++) {										\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				const char *restrict v = BUNtvar(bi, p - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = p;									\
			}															\
		} else {														\
			for (; p < ncands; p++) {									\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				oid o = canditer_next(ci);								\
				const char *restrict v = BUNtvar(bi, o - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = o;									\
			}															\
		}																\
	} while (0)

/* scan select loop with or without candidates */
#define scanloop_anti(TEST, KEEP_NULLS)									\
	do {																\
		TRC_DEBUG(ALGO,													\
				  "scanselect(b=%s#"BUNFMT",anti=%d): "					\
				  "scanselect %s\n", BATgetId(b), BATcount(b),			\
				  anti, #TEST);											\
		if (!s || BATtdense(s)) {										\
			for (; p < q; p++) {										\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				const char *restrict v = BUNtvar(bi, p - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = p;									\
			}															\
		} else {														\
			for (; p < ncands; p++) {									\
				GDK_CHECK_TIMEOUT(qry_ctx, counter,						\
								  GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx)); \
				oid o = canditer_next(ci);								\
				const char *restrict v = BUNtvar(bi, o - off);			\
				if ((TEST) || ((KEEP_NULLS) && *v == '\200'))			\
					vals[cnt++] = o;									\
			}															\
		}																\
	} while (0)

static str
do_string_select(BAT *bn, BAT *b, BAT *s, struct canditer *ci, BUN p, BUN q,
				 BUN *rcnt, const char *key, bool anti,
				 bit (*str_cmp)(const char *, const char *, int),
				 bool keep_nulls)
{
	if (strNil(key))
		return MAL_SUCCEED;

	BATiter bi = bat_iterator(b);
	BUN cnt = 0, ncands = ci->ncand;
	oid off = b->hseqbase, *restrict vals = Tloc(bn, 0);
	str msg = MAL_SUCCEED;
	int klen = str_strlen(key);

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	if (anti)					/* keep nulls ? (use false for now) */
		scanloop_anti(v && *v != '\200'
					  && str_cmp(v, key, klen) == 0, keep_nulls);
	else
		scanloop(v && *v != '\200' && str_cmp(v, key, klen) != 0, keep_nulls);

  bailout:
	bat_iterator_end(&bi);
	*rcnt = cnt;
	return msg;
}

static str
string_select(bat *ret, const bat *bid, const bat *sid, const str *key,
			  const bit *anti, bit (*str_cmp)(const char *, const char *, int),
			  const str fname)
{
	BAT *b, *s = NULL, *bn = NULL, *old_s = NULL;;
	str msg = MAL_SUCCEED;
	BUN p = 0, q = 0, rcnt = 0;
	struct canditer ci;
	bool with_strimps = false, with_strimps_anti = false;

	if ((b = BATdescriptor(*bid)) == NULL) {
		msg = createException(MAL, fname,
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}
	if (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL) {
		msg = createException(MAL, fname,
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto bailout;
	}

	assert(ATOMstorage(b->ttype) == TYPE_str);

	if (BAThasstrimps(b)) {
		if (STRMPcreate(b, NULL) == GDK_SUCCEED) {
			BAT *tmp_s = STRMPfilter(b, s, *key, *anti);
			if (tmp_s) {
				old_s = s;
				s = tmp_s;
				if (!*anti)
					with_strimps = true;
				else
					with_strimps_anti = true;
			}
		} else {				/* If we cannot filter with the strimp just continue normally */
			GDKclrerr();
		}
	}

	MT_thread_setalgorithm(with_strimps ?
						   "string_select: strcmp function using strimps" :
						   (with_strimps_anti ?
							"string_select: strcmp function using strimps anti"
							:
							"string_select: strcmp function with no accelerator"));

	canditer_init(&ci, b, s);
	if (!(bn = COLnew(0, TYPE_oid, ci.ncand, TRANSIENT))) {
		msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	if (!s || BATtdense(s)) {
		if (s) {
			assert(BATtdense(s));
			p = (BUN) s->tseqbase;
			q = p + BATcount(s);
			if ((oid) p < b->hseqbase)
				p = b->hseqbase;
			if ((oid) q > b->hseqbase + BATcount(b))
				q = b->hseqbase + BATcount(b);
		} else {
			p = b->hseqbase;
			q = BATcount(b) + b->hseqbase;
		}
	}

	msg = do_string_select(bn, b, s, &ci, p, q, &rcnt, *key, *anti
						   && !with_strimps_anti, str_cmp, with_strimps_anti);

	if (!msg) {					/* set some properties */
		BATsetcount(bn, rcnt);
		bn->tsorted = true;
		bn->trevsorted = bn->batCount <= 1;
		bn->tkey = true;
		bn->tnil = false;
		bn->tnonil = true;
		bn->tseqbase = rcnt == 0 ? 0 : rcnt == 1 ? *(const oid *) Tloc(bn, 0) : rcnt == b->batCount ? b->hseqbase : oid_nil;
		if (with_strimps_anti) {
			BAT *rev;
			if (old_s) {
				rev = BATdiffcand(old_s, bn);
#ifndef NDEBUG
				BAT *is = BATintersectcand(old_s, bn);
				if (is) {
					assert(is->batCount == bn->batCount);
					BBPreclaim(is);
				}
				assert(rev->batCount == old_s->batCount - bn->batCount);
#endif
			}

			else
				rev = BATnegcands(b->batCount, bn);
			BBPunfix(bn->batCacheid);
			bn = rev;
			if (bn == NULL)
				msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}

  bailout:
	BBPreclaim(b);
	BBPreclaim(s);
	BBPreclaim(old_s);
	if (bn && !msg) {
		*ret = bn->batCacheid;
		BBPkeepref(bn);
	} else if (bn)
		BBPreclaim(bn);
	return msg;
}

static str
STRstartswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bat *ret = getArgReference(stk, pci, 0);
	const bat *bid = getArgReference(stk, pci, 1),
			*sid = getArgReference(stk, pci, 2);
	const str *key = getArgReference_str(stk, pci, 3);
	const bit icase = pci->argc == 5 ? false : true,
		*anti = pci->argc == 5 ? getArgReference_bit(stk, pci, 4) : getArgReference_bit(stk, pci, 5);

	return string_select(ret, bid, sid, key, anti,
						 icase ? str_is_iprefix : str_is_prefix,
						 "str.startswithselect");
}

static str
STRendswithselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bat *ret = getArgReference(stk, pci, 0);
	const bat *bid = getArgReference(stk, pci, 1),
		*sid = getArgReference(stk, pci, 2);
	const str *key = getArgReference_str(stk, pci, 3);
	const bit icase = pci->argc == 5 ? false : true,
		*anti = pci->argc == 5 ? getArgReference_bit(stk, pci, 4) : getArgReference_bit(stk, pci, 5);

	return string_select(ret, bid, sid, key, anti,
						 icase ? str_is_isuffix : str_is_suffix,
						 "str.endswithselect");
}

static str
STRcontainsselect(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bat *ret = getArgReference(stk, pci, 0);
	const bat *bid = getArgReference(stk, pci, 1),
		*sid = getArgReference(stk, pci, 2);
	const str *key = getArgReference_str(stk, pci, 3);
	const bit icase = pci->argc == 5 ? false : true,
		*anti = pci->argc == 5 ? getArgReference_bit(stk, pci, 4) : getArgReference_bit(stk, pci, 5);

	return string_select(ret, bid, sid, key, anti,
						 icase ? str_icontains : str_contains,
						 "str.containsselect");
}

#define APPEND(b, o) (((oid *) b->theap->base)[b->batCount++] = (o))
#define VALUE(s, x)  (s##vars + VarHeapVal(s##vals, (x), s##i.width))

#define set_empty_bat_props(B)					\
	do {										\
		B->tnil = false;						\
		B->tnonil = true;						\
		B->tkey = true;							\
		B->tsorted = true;						\
		B->trevsorted = true;					\
		B->tseqbase = 0;						\
	} while (0)

#define str_join_loop(STRCMP, STR_LEN)									\
	do {																\
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) {					\
			BAT *filtered_sl = NULL;									\
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(exit, qry_ctx)); \
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			if (strNil(vr))												\
				continue;												\
			rlen = STR_LEN;												\
			nl = 0;														\
			if (with_strimps)											\
				filtered_sl = STRMPfilter(l, sl, vr, anti);				\
			if (filtered_sl)											\
				canditer_init(&lci, l, filtered_sl);					\
			else														\
				canditer_init(&lci, l, sl);								\
			for (BUN lidx = 0; lidx < lci.ncand; lidx++) {				\
				lo = canditer_next(&lci);								\
				vl = VALUE(l, lo - lbase);								\
				if (strNil(vl)) {										\
					continue;											\
				} else if (!(STRCMP)) {									\
					continue;											\
				}														\
				if (BATcount(r1) == BATcapacity(r1)) {					\
					newcap = BATgrows(r1);								\
					BATsetcount(r1, BATcount(r1));						\
					if (r2)												\
						BATsetcount(r2, BATcount(r2));					\
					if (BATextend(r1, newcap) != GDK_SUCCEED ||			\
						(r2 && BATextend(r2, newcap) != GDK_SUCCEED)) { \
						msg = createException(MAL, "str.%s", fname, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						goto exit;										\
					}													\
					assert(!r2 || BATcapacity(r1) == BATcapacity(r2));	\
				}														\
				if (BATcount(r1) > 0) {									\
					if (lastl + 1 != lo)								\
						r1->tseqbase = oid_nil;							\
					if (nl == 0) {										\
						if (r2)											\
							r2->trevsorted = false;						\
						if (lastl > lo) {								\
							r1->tsorted = false;						\
							r1->tkey = false;							\
						} else if (lastl < lo) {						\
							r1->trevsorted = false;						\
						} else {										\
							r1->tkey = false;							\
						}												\
					}													\
				}														\
				APPEND(r1, lo);											\
				if (r2)													\
					APPEND(r2, ro);										\
				lastl = lo;												\
				nl++;													\
			}															\
			BBPreclaim(filtered_sl);									\
			if (r2) {													\
				if (nl > 1) {											\
					r2->tkey = false;									\
					r2->tseqbase = oid_nil;								\
					r1->trevsorted = false;								\
				} else if (nl == 0) {									\
					rskipped = BATcount(r2) > 0;						\
				} else if (rskipped) {									\
					r2->tseqbase = oid_nil;								\
				}														\
			} else if (nl > 1) {										\
				r1->trevsorted = false;									\
			}															\
		}																\
	} while (0)

#define str_antijoin_loop(STRCMP, STR_LEN)								\
	do {																\
		for (BUN ridx = 0; ridx < rci.ncand; ridx++) {					\
			GDK_CHECK_TIMEOUT(qry_ctx, counter, GOTO_LABEL_TIMEOUT_HANDLER(exit, qry_ctx)); \
			ro = canditer_next(&rci);									\
			vr = VALUE(r, ro - rbase);									\
			if (strNil(vr))												\
				continue;												\
			rlen = STR_LEN;												\
			nl = 0;														\
			canditer_init(&lci, l, sl);									\
			for (BUN lidx = 0; lidx < lci.ncand; lidx++) {				\
				lo = canditer_next(&lci);								\
				vl = VALUE(l, lo - lbase);								\
				if (strNil(vl)) {										\
					continue;											\
				} else if (!(STRCMP)) {									\
					continue;											\
				}														\
				if (BATcount(r1) == BATcapacity(r1)) {					\
					newcap = BATgrows(r1);								\
					BATsetcount(r1, BATcount(r1));						\
					if (r2)												\
						BATsetcount(r2, BATcount(r2));					\
					if (BATextend(r1, newcap) != GDK_SUCCEED ||			\
						(r2 && BATextend(r2, newcap) != GDK_SUCCEED)) { \
						msg = createException(MAL, "str.%s", fname, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
						goto exit;										\
					}													\
					assert(!r2 || BATcapacity(r1) == BATcapacity(r2));	\
				}														\
				if (BATcount(r1) > 0) {									\
					if (lastl + 1 != lo)								\
						r1->tseqbase = oid_nil;							\
					if (nl == 0) {										\
						if (r2)											\
							r2->trevsorted = false;						\
						if (lastl > lo) {								\
							r1->tsorted = false;						\
							r1->tkey = false;							\
						} else if (lastl < lo) {						\
							r1->trevsorted = false;						\
						} else {										\
							r1->tkey = false;							\
						}												\
					}													\
				}														\
				APPEND(r1, lo);											\
				if (r2)													\
					APPEND(r2, ro);										\
				lastl = lo;												\
				nl++;													\
			}															\
			if (r2) {													\
				if (nl > 1) {											\
					r2->tkey = false;									\
					r2->tseqbase = oid_nil;								\
					r1->trevsorted = false;								\
				} else if (nl == 0) {									\
					rskipped = BATcount(r2) > 0;						\
				} else if (rskipped) {									\
					r2->tseqbase = oid_nil;								\
				}														\
			} else if (nl > 1) {										\
				r1->trevsorted = false;									\
			}															\
		}																\
	} while (0)

static str
strjoin(BAT *r1, BAT *r2, BAT *l, BAT *r, BAT *sl, BAT *sr, bit anti,
		bit (*str_cmp)(const char *, const char *, int), const str fname)
{
	 (void) anti;
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars, *vl, *vr;
	int rskipped = 0, rlen = 0;
	oid lbase, rbase, lo, ro, lastl = 0;
	BUN nl, newcap;
	bool with_strimps = false;
	char *msg = MAL_SUCCEED;

	size_t counter = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	if (BAThasstrimps(l)) {
		with_strimps = true;
		if (STRMPcreate(l, NULL) != GDK_SUCCEED) {
			GDKclrerr();
			with_strimps = false;
		}
	}

	TRC_DEBUG(ALGO,
			  "(%s, l=%s#" BUNFMT "[%s]%s%s,"
			  "r=%s#" BUNFMT "[%s]%s%s,sl=%s#" BUNFMT "%s%s,"
			  "sr=%s#" BUNFMT "%s%s)\n",
			  fname,
			  BATgetId(l), BATcount(l), ATOMname(l->ttype),
			  l->tsorted ? "-sorted" : "",
			  l->trevsorted ? "-revsorted" : "",
			  BATgetId(r), BATcount(r), ATOMname(r->ttype),
			  r->tsorted ? "-sorted" : "",
			  r->trevsorted ? "-revsorted" : "",
			  sl ? BATgetId(sl) : "NULL", sl ? BATcount(sl) : 0,
			  sl && sl->tsorted ? "-sorted" : "",
			  sl && sl->trevsorted ? "-revsorted" : "",
			  sr ? BATgetId(sr) : "NULL", sr ? BATcount(sr) : 0,
			  sr && sr->tsorted ? "-sorted" : "",
			  sr && sr->trevsorted ? "-revsorted" : "");

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	canditer_init(&rci, r, sr);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);
	lbase = l->hseqbase;
	rbase = r->hseqbase;
	lvals = (const char *) li.base;
	rvals = (const char *) ri.base;
	assert(ri.vh && r->ttype);
	lvars = li.vh->base;
	rvars = ri.vh->base;

	if (anti)
		str_antijoin_loop(str_cmp(vl, vr, rlen) == 0, str_strlen(vr));
	else
		str_join_loop(str_cmp(vl, vr, rlen) != 0, str_strlen(vr));

	assert(!r2 || BATcount(r1) == BATcount(r2));
	BATsetcount(r1, BATcount(r1));
	if (r2)
		BATsetcount(r2, BATcount(r2));
	if (BATcount(r1) > 0) {
		if (BATtdense(r1))
			r1->tseqbase = ((oid *) r1->theap->base)[0];
		if (r2 && BATtdense(r2))
			r2->tseqbase = ((oid *) r2->theap->base)[0];
	} else {
		r1->tseqbase = 0;
		if (r2)
			r2->tseqbase = 0;
	}

	if (r2)
		TRC_DEBUG(ALGO,
				  "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s,%s#" BUNFMT "%s%s\n",
				  fname,
				  BATgetId(l), BATgetId(r),
				  BATgetId(r1), BATcount(r1),
				  r1->tsorted ? "-sorted" : "",
				  r1->trevsorted ? "-revsorted" : "",
				  BATgetId(r2), BATcount(r2),
				  r2->tsorted ? "-sorted" : "",
				  r2->trevsorted ? "-revsorted" : "");
	else
		TRC_DEBUG(ALGO,
				  "(%s, l=%s,r=%s)=(%s#" BUNFMT "%s%s\n",
				  fname,
				  BATgetId(l), BATgetId(r),
				  BATgetId(r1), BATcount(r1),
				  r1->tsorted ? "-sorted" : "",
				  r1->trevsorted ? "-revsorted" : "");
  exit:
	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	return msg;
}

static str
STRjoin(bat *r1, bat *r2, const bat lid, const bat rid, const bat slid,
		const bat srid, const bit anti, bit (*str_cmp)(const char *,
													   const char *, int),
		const str fname)
{
	BAT *left = NULL, *right = NULL, *cleft = NULL, *cright = NULL,
			*res1 = NULL, *res2 = NULL;
	str msg = MAL_SUCCEED;

	if (!(left = BATdescriptor(lid)) || !(right = BATdescriptor(rid))) {
		msg = createException(MAL, fname,
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	if ((!is_bat_nil(slid) && (cleft = BATdescriptor(slid)) == NULL) ||
		(!is_bat_nil(srid) && (cright = BATdescriptor(srid)) == NULL)) {
		msg = createException(MAL, fname,
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	res1 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (r2)
		res2 = COLnew(0, TYPE_oid, BATcount(left), TRANSIENT);
	if (!res1 || (r2 && !res2)) {
		BBPreclaim(res1);
		BBPreclaim(res2);
		msg = createException(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	set_empty_bat_props(res1);
	if (r2)
		set_empty_bat_props(res2);

	msg = strjoin(res1, res2, left, right, cleft, cright, anti, str_cmp, fname);

	if (!msg) {
		*r1 = res1->batCacheid;
		BBPkeepref(res1);
		if (r2) {
			*r2 = res2->batCacheid;
			BBPkeepref(res2);
		}
	} else {
		BBPreclaim(res1);
		BBPreclaim(res2);
	}

  exit:
	BBPreclaim(left);
	BBPreclaim(right);
	BBPreclaim(cleft);
	BBPreclaim(cright);
	return msg;
}

static str
join_caseignore(const bat *cid, bool *caseignore, str fname)
{
	BAT *c = NULL;

	if ((c = BATdescriptor(*cid)) == NULL)
		return createException(MAL, fname,
							   SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (BATcount(c) != 1)
		return createException(MAL, fname,
							   SQLSTATE(42000)
							   "At the moment, only one value is allowed for the case ignore input at %s join",
							   fname);
	BATiter bi = bat_iterator(c);
	*caseignore = *(bit *) BUNtloc(bi, 0);
	bat_iterator_end(&bi);
	BBPreclaim(c);
	return MAL_SUCCEED;
}

static str
STRstartswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 9 ? 4 : 5;
	bat *r1 = getArgReference(stk, pci, 0);
	bat *r2 = getArgReference(stk, pci, 1);
	const bat *lid = getArgReference(stk, pci, 2),
			*rid = getArgReference(stk, pci, 3),
			*cid = pci->argc == 9 ? NULL : getArgReference(stk, pci, 4),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 9 ? getArgReference_bit(stk, pci, 8) : getArgReference_bit(stk, pci, 9);
	bool caseignore = false;

	if (pci->argc != 9)
		msg = join_caseignore(cid, &caseignore, "str.startswithjoin");
	return msg ? msg : STRjoin(r1, r2, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_is_iprefix : str_is_prefix,
							   "str.startswithjoin");
}

static str
STRstartswithjoin1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 8 ? 3 : 4;
	bat *r1 = getArgReference(stk, pci, 0);
	const bat *lid = getArgReference(stk, pci, 1),
			*rid = getArgReference(stk, pci, 2),
			*cid = pci->argc == 8 ? NULL : getArgReference(stk, pci, 3),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 8 ? getArgReference_bit(stk, pci, 7) : getArgReference_bit(stk, pci, 8);
	bool caseignore = false;

	if (pci->argc != 8)
		msg = join_caseignore(cid, &caseignore, "str.startswithjoin1");
	return msg ? msg : STRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_is_iprefix : str_is_prefix,
							   "str.startswithjoin1");
}

static str
STRendswithjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 9 ? 4 : 5;
	bat *r1 = getArgReference(stk, pci, 0);
	bat *r2 = getArgReference(stk, pci, 1);
	const bat *lid = getArgReference(stk, pci, 2),
			*rid = getArgReference(stk, pci, 3),
			*cid = pci->argc == 9 ? NULL : getArgReference(stk, pci, 4),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 9 ? getArgReference_bit(stk, pci, 8) : getArgReference_bit(stk, pci, 9);
	bool caseignore = false;

	if (pci->argc != 9)
		msg = join_caseignore(cid, &caseignore, "str.endswithjoin");
	return msg ? msg : STRjoin(r1, r2, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_is_isuffix : str_is_suffix,
							   "str.endswithjoin");
}

static str
STRendswithjoin1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 8 ? 3 : 4;
	bat *r1 = getArgReference(stk, pci, 0);
	const bat *lid = getArgReference(stk, pci, 1),
			*rid = getArgReference(stk, pci, 2),
			*cid = pci->argc == 8 ? NULL : getArgReference(stk, pci, 3),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 8 ? getArgReference_bit(stk, pci, 7) : getArgReference_bit(stk, pci, 8);
	bool caseignore = false;

	if (pci->argc != 8)
		msg = join_caseignore(cid, &caseignore, "str.endswithjoin1");
	return msg ? msg : STRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_is_isuffix : str_is_suffix,
							   "str.endswithjoin1");
}

static str
STRcontainsjoin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 9 ? 4 : 5;
	bat *r1 = getArgReference(stk, pci, 0);
	bat *r2 = getArgReference(stk, pci, 1);
	const bat *lid = getArgReference(stk, pci, 2),
			*rid = getArgReference(stk, pci, 3),
			*cid = pci->argc == 9 ? NULL : getArgReference(stk, pci, 4),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 9 ? getArgReference_bit(stk, pci, 8) : getArgReference_bit(stk, pci, 9);
	bool caseignore = false;

	if (pci->argc != 9)
		msg = join_caseignore(cid, &caseignore, "str.containsjoin");
	return msg ? msg : STRjoin(r1, r2, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_icontains : str_contains,
							   "str.containsjoin");
}

static str
STRcontainsjoin1(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	str msg = MAL_SUCCEED;
	int i = pci->argc == 8 ? 3 : 4;
	bat *r1 = getArgReference(stk, pci, 0);
	const bat *lid = getArgReference(stk, pci, 1),
			*rid = getArgReference(stk, pci, 2),
			*cid = pci->argc == 8 ? NULL : getArgReference(stk, pci, 3),
			*slid = getArgReference(stk, pci, i++),
			*srid = getArgReference(stk, pci, i);
	const bit *anti = pci->argc == 8 ? getArgReference_bit(stk, pci, 7) : getArgReference_bit(stk, pci, 8); bool caseignore = false;

	if (pci->argc != 8)
		msg = join_caseignore(cid, &caseignore, "str.containsjoin");
	return msg ? msg : STRjoin(r1, NULL, *lid, *rid, slid ? *slid : 0,
							   srid ? *srid : 0, *anti,
							   (caseignore) ? str_icontains : str_contains,
							   "str.containsjoin1");
}

#include "mel.h"
mel_func str_init_funcs[] = {
 command("str", "str", STRtostr, false, "Noop routine.", args(1,2, arg("",str),arg("s",str))),
 command("str", "string", STRTail, false, "Return the tail s[offset..n]\nof a string s[0..n].", args(1,3, arg("",str),arg("s",str),arg("offset",int))),
 command("str", "string3", STRSubString, false, "Return substring s[offset..offset+count] of a string s[0..n]", args(1,4, arg("",str),arg("s",str),arg("offset",int),arg("count",int))),
 command("str", "length", STRLength, false, "Return the length of a string.", args(1,2, arg("",int),arg("s",str))),
 command("str", "nbytes", STRBytes, false, "Return the string length in bytes.", args(1,2, arg("",int),arg("s",str))),
 command("str", "unicodeAt", STRWChrAt, false, "get a unicode character\n(as an int) from a string position.", args(1,3, arg("",int),arg("s",str),arg("index",int))),
 command("str", "unicode", STRFromWChr, false, "convert a unicode to a character.", args(1,2, arg("",str),arg("wchar",int))),
 pattern("str", "startswith", STRstartswith, false, "Check if string starts with substring.", args(1,3, arg("",bit),arg("s",str),arg("prefix",str))),
 pattern("str", "startswith", STRstartswith, false, "Check if string starts with substring, icase flag.", args(1,4, arg("",bit),arg("s",str),arg("prefix",str),arg("icase",bit))),
 pattern("str", "endswith", STRendswith, false, "Check if string ends with substring.", args(1,3, arg("",bit),arg("s",str),arg("suffix",str))),
 pattern("str", "endswith", STRendswith, false, "Check if string ends with substring, icase flag.", args(1,4, arg("",bit),arg("s",str),arg("suffix",str),arg("icase",bit))),
 pattern("str", "contains", STRcontains, false, "Check if string haystack contains string needle.", args(1,3, arg("",bit),arg("haystack",str),arg("needle",str))),
 pattern("str", "contains", STRcontains, false, "Check if string chaystack contains string needle, icase flag.", args(1,4, arg("",bit),arg("haystack",str),arg("needle",str),arg("icase",bit))),
 command("str", "toLower", STRlower, false, "Convert a string to lower case.", args(1,2, arg("",str),arg("s",str))),
 command("str", "toUpper", STRupper, false, "Convert a string to upper case.", args(1,2, arg("",str),arg("s",str))),
 pattern("str", "search", STRstr_search, false, "Search for a substring. Returns\nposition, -1 if not found.", args(1,3, arg("",int),arg("s",str),arg("c",str))),
 pattern("str", "search", STRstr_search, false, "Search for a substring, icase flag. Returns\nposition, -1 if not found.", args(1,4, arg("",int),arg("s",str),arg("c",str),arg("icase",bit))),
 pattern("str", "r_search", STRrevstr_search, false, "Reverse search for a substring. Returns\nposition, -1 if not found.", args(1,3, arg("",int),arg("s",str),arg("c",str))),
 pattern("str", "r_search", STRrevstr_search, false, "Reverse search for a substring, icase flag. Returns\nposition, -1 if not found.", args(1,4, arg("",int),arg("s",str),arg("c",str),arg("icase",bit))),
 command("str", "splitpart", STRsplitpart, false, "Split string on delimiter. Returns\ngiven field (counting from one.)", args(1,4, arg("",str),arg("s",str),arg("needle",str),arg("field",int))),
 command("str", "trim", STRStrip, false, "Strip whitespaces around a string.", args(1,2, arg("",str),arg("s",str))),
 command("str", "ltrim", STRLtrim, false, "Strip whitespaces from start of a string.", args(1,2, arg("",str),arg("s",str))),
 command("str", "rtrim", STRRtrim, false, "Strip whitespaces from end of a string.", args(1,2, arg("",str),arg("s",str))),
 command("str", "trim2", STRStrip2, false, "Remove the longest string containing only characters from the second string around the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
 command("str", "ltrim2", STRLtrim2, false, "Remove the longest string containing only characters from the second string from the start of the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
 command("str", "rtrim2", STRRtrim2, false, "Remove the longest string containing only characters from the second string from the end of the first string.", args(1,3, arg("",str),arg("s",str),arg("s2",str))),
 command("str", "lpad", STRLpad, false, "Fill up a string to the given length prepending the whitespace character.", args(1,3, arg("",str),arg("s",str),arg("len",int))),
 command("str", "rpad", STRRpad, false, "Fill up a string to the given length appending the whitespace character.", args(1,3, arg("",str),arg("s",str),arg("len",int))),
 command("str", "lpad3", STRLpad3, false, "Fill up the first string to the given length prepending characters of the second string.", args(1,4, arg("",str),arg("s",str),arg("len",int),arg("s2",str))),
 command("str", "rpad3", STRRpad3, false, "Fill up the first string to the given length appending characters of the second string.", args(1,4, arg("",str),arg("s",str),arg("len",int),arg("s2",str))),
 command("str", "substitute", STRSubstitute, false, "Substitute first occurrence of 'src' by\n'dst'.  Iff repeated = true this is\nrepeated while 'src' can be found in the\nresult string. In order to prevent\nrecursion and result strings of unlimited\nsize, repeating is only done iff src is\nnot a substring of dst.", args(1,5, arg("",str),arg("s",str),arg("src",str),arg("dst",str),arg("rep",bit))),
 command("str", "like", STRlikewrap, false, "SQL pattern match function", args(1,3, arg("",bit),arg("s",str),arg("pat",str))),
 command("str", "like3", STRlikewrap3, false, "SQL pattern match function", args(1,4, arg("",bit),arg("s",str),arg("pat",str),arg("esc",str))),
 command("str", "ascii", STRascii, false, "Return unicode of head of string", args(1,2, arg("",int),arg("s",str))),
 command("str", "substring", STRsubstringTail, false, "Extract the tail of a string", args(1,3, arg("",str),arg("s",str),arg("start",int))),
 command("str", "substring3", STRsubstring, false, "Extract a substring from str starting at start, for length len", args(1,4, arg("",str),arg("s",str),arg("start",int),arg("len",int))),
 command("str", "prefix", STRprefix, false, "Extract the prefix of a given length", args(1,3, arg("",str),arg("s",str),arg("l",int))),
 command("str", "suffix", STRsuffix, false, "Extract the suffix of a given length", args(1,3, arg("",str),arg("s",str),arg("l",int))),
 command("str", "stringleft", STRprefix, false, "", args(1,3, arg("",str),arg("s",str),arg("l",int))),
 command("str", "stringright", STRsuffix, false, "", args(1,3, arg("",str),arg("s",str),arg("l",int))),
 command("str", "locate", STRlocate, false, "Locate the start position of a string", args(1,3, arg("",int),arg("s1",str),arg("s2",str))),
 command("str", "locate3", STRlocate3, false, "Locate the start position of a string", args(1,4, arg("",int),arg("s1",str),arg("s2",str),arg("start",int))),
 command("str", "insert", STRinsert, false, "Insert a string into another", args(1,5, arg("",str),arg("s",str),arg("start",int),arg("l",int),arg("s2",str))),
 command("str", "replace", STRreplace, false, "Insert a string into another", args(1,4, arg("",str),arg("s",str),arg("pat",str),arg("s2",str))),
 command("str", "repeat", STRrepeat, false, "", args(1,3, arg("",str),arg("s2",str),arg("c",int))),
 command("str", "space", STRspace, false, "", args(1,2, arg("",str),arg("l",int))),
 command("str", "asciify", STRasciify, false, "Transform string from UTF8 to ASCII", args(1, 2, arg("out",str), arg("in",str))),
 pattern("str", "startswithselect", STRstartswithselect, false, "Select all head values of the first input BAT for which the\ntail value starts with the given prefix.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("prefix",str),arg("anti",bit))),
 pattern("str", "startswithselect", STRstartswithselect, false, "Select all head values of the first input BAT for which the\ntail value starts with the given prefix + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("prefix",str),arg("caseignore",bit),arg("anti",bit))),
 pattern("str", "endswithselect", STRendswithselect, false, "Select all head values of the first input BAT for which the\ntail value end with the given suffix.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("suffix",str),arg("anti",bit))),
 pattern("str", "endswithselect", STRendswithselect, false, "Select all head values of the first input BAT for which the\ntail value end with the given suffix + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("suffix",str),arg("caseignore",bit),arg("anti",bit))),
 pattern("str", "containsselect", STRcontainsselect, false, "Select all head values of the first input BAT for which the\ntail value contains the given needle.", args(1,5, batarg("",oid),batarg("b",str),batarg("s",oid),arg("needle",str),arg("anti",bit))),
 pattern("str", "containsselect", STRcontainsselect, false, "Select all head values of the first input BAT for which the\ntail value contains the given needle + icase.", args(1,6, batarg("",oid),batarg("b",str),batarg("s",oid),arg("needle",str),arg("caseignore",bit),arg("anti",bit))),
 pattern("str", "startswithjoin", STRstartswithjoin, false, "Join the string bat L with the prefix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "startswithjoin", STRstartswithjoin, false, "Join the string bat L with the prefix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "startswithjoin", STRstartswithjoin1, false, "The same as STRstartswithjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 pattern("str", "startswithjoin", STRstartswithjoin1, false, "The same as STRstartswithjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 pattern("str", "endswithjoin", STRendswithjoin, false, "Join the string bat L with the suffix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "endswithjoin", STRendswithjoin, false, "Join the string bat L with the suffix bat R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "endswithjoin", STRendswithjoin1, false, "The same as STRendswithjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 pattern("str", "endswithjoin", STRendswithjoin1, false, "The same as STRendswithjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 pattern("str", "containsjoin", STRcontainsjoin, false, "Join the string bat L with the bat R if L contains the string of R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows.", args(2,9, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "containsjoin", STRcontainsjoin, false, "Join the string bat L with the bat R if L contains the string of R\nwith optional candidate lists SL and SR\nThe result is two aligned bats with oids of matching rows + icase.", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
 pattern("str", "containsjoin", STRcontainsjoin1, false, "The same as STRcontainsjoin, but only produce one output.", args(1,8,batarg("",oid),batarg("l",str),batarg("r",str),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 pattern("str", "containsjoin", STRcontainsjoin1, false, "The same as STRcontainsjoin, but only produce one output + icase.", args(1,9,batarg("",oid),batarg("l",str),batarg("r",str),batarg("caseignore",bit),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng), arg("anti",bit))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_str_mal)
{ mal_module2("str", NULL, str_init_funcs, NULL, NULL); }
