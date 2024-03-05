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

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_cand.h"

/* String Atom Implementation
 *
 * Strings are stored in two parts.  The first part is the normal tail
 * heap which contains a list of offsets.  The second part is the
 * theap which contains the actual strings.  The offsets in the tail
 * heap (a.k.a. offset heap) point into the theap (a.k.a. string
 * heap).  Strings are NULL-terminated and are stored without any
 * escape sequences.  Strings are encoded using the UTF-8 encoding
 * of Unicode.  This means that individual "characters" (really,
 * Unicode code points) can be between one and four bytes long.
 *
 * Because in many typical situations there are lots of duplicated
 * string values that are being stored in a table, but also in many
 * (other) typical situations there are very few duplicated string
 * values stored, a scheme has been introduced to cater to both
 * situations.
 *
 * When the string heap is "small" (defined as less than 64KiB), the
 * string heap is fully duplicate eliminated.  When the string heap
 * grows beyond this size, the heap is not kept free of duplicate
 * strings, but there is then a heuristic that tries to limit the
 * number of duplicates.
 *
 * This is done by having a fixed sized hash table at the start of the
 * string heap, and allocating space for collision lists in the first
 * 64KiB of the string heap.  After the first 64KiB no extra space is
 * allocated for lists, so hash collisions cannot be resolved.
 */

/* some of these macros are duplicates from gdk_atoms.c */
#define num08(x)	((x) >= '0' && (x) <= '7')
#define base08(x)	((x) - '0')
#define mult08(x)	((x) << 3)

#define num16(x)	isxdigit((unsigned char) (x))
#define base16(x)	(((x) >= 'a' && (x) <= 'f') ? ((x) - 'a' + 10) : ((x) >= 'A' && (x) <= 'F') ? ((x) - 'A' + 10) : (x) - '0')
#define mult16(x)	((x) << 4)

#define atommem(size)					\
	do {						\
		if (*dst == NULL || *len < (size)) {	\
			GDKfree(*dst);			\
			*len = (size);			\
			*dst = GDKmalloc(*len);		\
			if (*dst == NULL) {		\
				*len = 0;		\
				return -1;		\
			}				\
		}					\
	} while (0)

const char str_nil[2] = { '\200', 0 };

gdk_return
strHeap(Heap *d, size_t cap)
{
	size_t size;

	cap = MAX(cap, BATTINY);
	size = GDK_STRHASHTABLE * sizeof(stridx_t) + MIN(GDK_ELIMLIMIT, cap * GDK_VARALIGN);
	return HEAPalloc(d, size, 1);
}


void
strCleanHash(Heap *h, bool rebuild)
{
	stridx_t newhash[GDK_STRHASHTABLE];
	size_t pad, pos;
	BUN off, strhash;
	const char *s;

	(void) rebuild;
	if (!h->cleanhash)
		return;
	if (h->size < GDK_STRHASHTABLE * sizeof(stridx_t) &&
	    HEAPextend(h, GDK_STRHASHTABLE * sizeof(stridx_t) + BATTINY * GDK_VARALIGN, true) != GDK_SUCCEED) {
		GDKclrerr();
		if (h->size > 0)
			memset(h->base, 0, h->size);
		return;
	}

	/* rebuild hash table for double elimination
	 *
	 * If appending strings to the BAT was aborted, if the heap
	 * was memory mapped, the hash in the string heap may well be
	 * incorrect.  Therefore we don't trust it when we read in a
	 * string heap and we rebuild the complete table (it is small,
	 * so this won't take any time at all).
	 * Note that we will only do this the first time the heap is
	 * loaded, and only for heaps that existed when the server was
	 * started. */
	memset(newhash, 0, sizeof(newhash));
	pos = GDK_STRHASHSIZE;
	while (pos < h->free) {
		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad;
		if (pos >= GDK_ELIMLIMIT)
			break;
		s = h->base + pos;
		strhash = strHash(s);
		off = strhash & GDK_STRHASHMASK;
		newhash[off] = (stridx_t) (pos - sizeof(stridx_t));
		pos += strlen(s) + 1;
	}
	/* only set dirty flag if the hash table actually changed */
	if (memcmp(newhash, h->base, sizeof(newhash)) != 0) {
		memcpy(h->base, newhash, sizeof(newhash));
		if (h->storage == STORE_MMAP) {
			if (!(ATOMIC_GET(&GDKdebug) & NOSYNCMASK))
				(void) MT_msync(h->base, GDK_STRHASHSIZE);
		} else
			h->dirty = true;
	}
#ifndef NDEBUG
	if (GDK_ELIMDOUBLES(h)) {
		pos = GDK_STRHASHSIZE;
		while (pos < h->free) {
			pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
			if (pad < sizeof(stridx_t))
				pad += GDK_VARALIGN;
			pos += pad;
			s = h->base + pos;
			assert(strLocate(h, s) != 0);
			pos += strlen(s) + 1;
		}
	}
#endif
	h->cleanhash = false;
}

/*
 * The strPut routine. The routine strLocate can be used to identify
 * the location of a string in the heap if it exists. Otherwise it
 * returns (var_t) -2 (-1 is reserved for error).
 */
var_t
strLocate(Heap *h, const char *v)
{
	stridx_t *ref, *next;

	/* search hash-table, if double-elimination is still in place */
	BUN off;
	if (h->free == 0) {
		/* empty, so there are no strings */
		return (var_t) -2;
	}

	off = strHash(v);
	off &= GDK_STRHASHMASK;

	/* should only use strLocate iff fully double eliminated */
	assert(GDK_ELIMBASE(h->free) == 0);

	/* search the linked list */
	for (ref = ((stridx_t *) h->base) + off; *ref; ref = next) {
		next = (stridx_t *) (h->base + *ref);
		if (strcmp(v, (str) (next + 1)) == 0)
			return (var_t) ((sizeof(stridx_t) + *ref));	/* found */
	}
	return (var_t) -2;
}

var_t
strPut(BAT *b, var_t *dst, const void *V)
{
	const char *v = V;
	Heap *h = b->tvheap;
	size_t pad;
	size_t pos, len = strlen(v) + 1;
	stridx_t *bucket;
	BUN off;

	if (h->free == 0) {
		if (h->size < GDK_STRHASHTABLE * sizeof(stridx_t) + BATTINY * GDK_VARALIGN) {
			if (HEAPgrow(&b->tvheap, GDK_STRHASHTABLE * sizeof(stridx_t) + BATTINY * GDK_VARALIGN, true) != GDK_SUCCEED) {
				return (var_t) -1;
			}
			h = b->tvheap;
		}
		h->free = GDK_STRHASHTABLE * sizeof(stridx_t);
		h->dirty = true;
#ifdef NDEBUG
		memset(h->base, 0, h->free);
#else
		/* fill should solve initialization problems within valgrind */
		memset(h->base, 0, h->size);
#endif
		b->tascii = true;
	}

	off = strHash(v);
	off &= GDK_STRHASHMASK;
	bucket = ((stridx_t *) h->base) + off;

	if (*bucket) {
		assert(*bucket < h->free);
		/* the hash list is not empty */
		if (*bucket < GDK_ELIMLIMIT) {
			/* small string heap (<64KiB) -- fully double
			 * eliminated: search the linked list */
			const stridx_t *ref = bucket;

			do {
				pos = *ref + sizeof(stridx_t);
				assert(pos < h->free);
				if (strcmp(v, h->base + pos) == 0) {
					/* found */
					return *dst = (var_t) pos;
				}
				ref = (stridx_t *) (h->base + *ref);
			} while (*ref);
		} else {
			/* large string heap (>=64KiB) -- there is no
			 * linked list, so only look at single
			 * entry */
			pos = *bucket;
			if (strcmp(v, h->base + pos) == 0) {
				/* already in heap: reuse */
				return *dst = (var_t) pos;
			}
		}
	}
	/* the string was not found in the heap, we need to enter it */

	/* check that string is correctly encoded UTF-8; there was no
	 * need to do this earlier: if the string was found above, it
	 * must have gone through here in the past */
#ifndef NDEBUG
	if (!checkUTF8(v)) {
		GDKerror("incorrectly encoded UTF-8\n");
		return (var_t) -1;
	}
#endif

	pad = GDK_VARALIGN - (h->free & (GDK_VARALIGN - 1));
	if (GDK_ELIMBASE(h->free + pad) == 0) {	/* i.e. h->free+pad < GDK_ELIMLIMIT */
		if (pad < sizeof(stridx_t)) {
			/* make room for hash link */
			pad += GDK_VARALIGN;
		}
	} else if (GDK_ELIMBASE(h->free) != 0) {
		/* no extra padding needed when no hash links needed
		 * (but only when padding doesn't cross duplicate
		 * elimination boundary) */
		pad = 0;
	}

	/* check heap for space (limited to a certain maximum after
	 * which nils are inserted) */
	if (h->free + pad + len >= h->size) {
		size_t newsize = MAX(h->size, 4096);

		/* double the heap size until we have enough space */
		do {
			if (newsize < 4 * 1024 * 1024)
				newsize <<= 1;
			else
				newsize += 4 * 1024 * 1024;
		} while (newsize <= h->free + pad + len);

		assert(newsize);

		if (h->free + pad + len >= (size_t) VAR_MAX) {
			GDKerror("string heap gets larger than %zuGiB.\n", (size_t) VAR_MAX >> 30);
			return (var_t) -1;
		}
		TRC_DEBUG(HEAP, "HEAPextend in strPut %s %zu %zu\n", h->filename, h->size, newsize);
		if (HEAPgrow(&b->tvheap, newsize, true) != GDK_SUCCEED) {
			return (var_t) -1;
		}
		h = b->tvheap;

		/* make bucket point into the new heap */
		bucket = ((stridx_t *) h->base) + off;
	}

	/* insert string */
	pos = h->free + pad;
	*dst = (var_t) pos;
	if (pad > 0)
		memset(h->base + h->free, 0, pad);
	memcpy(h->base + pos, v, len);
	h->free += pad + len;
	h->dirty = true;

	/* maintain hash table */
	if (GDK_ELIMBASE(pos) == 0) {	/* small string heap: link the next pointer */
		/* the stridx_t next pointer directly precedes the
		 * string */
		pos -= sizeof(stridx_t);
		*(stridx_t *) (h->base + pos) = *bucket;
	}
	*bucket = (stridx_t) pos;	/* set bucket to the new string */

	if (b->tascii && !strNil(v)) {
		for (const uint8_t *p = (const uint8_t *) v; *p; p++) {
			if (*p >= 128) {
				b->tascii = false;
				break;
			}
		}
	}

	return *dst;
}

/*
 * Convert an "" separated string to a GDK string value, checking that
 * the input is correct UTF-8.
 */

#ifdef __GNUC__
/* __builtin_expect returns its first argument; it is expected to be
 * equal to the second argument */
#define unlikely(expr)	__builtin_expect((expr) != 0, 0)
#define likely(expr)	__builtin_expect((expr) != 0, 1)
#else
#define unlikely(expr)	(expr)
#define likely(expr)	(expr)
#endif

ssize_t
GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len, char quote)
{
	unsigned char *p = dst;
	const unsigned char *cur = src, *end = src + len;
	bool escaped = false;
	int mask = 0, n, c, utf8char = 0;

	if (len >= 2 && strNil((const char *) src)) {
		strcpy((char *) dst, str_nil);
		return 1;
	}

	/* copy it in, while performing the correct escapes */
	/* n is the number of follow-on bytes left in a multi-byte
	 * UTF-8 sequence */
	for (cur = src, n = 0; cur < end || escaped; cur++) {
		/* first convert any \ escapes and store value in c */
		if (escaped) {
			switch (*cur) {
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
				/* \ with up to three octal digits */
				c = base08(*cur);
				if (num08(cur[1])) {
					cur++;
					c = mult08(c) + base08(*cur);
					if (num08(cur[1])) {
						if (unlikely(c > 037)) {
							/* octal
							 * escape
							 * sequence
							 * out or
							 * range */
							GDKerror("not an octal number\n");
							return -1;
						}
						cur++;
						c = mult08(c) + base08(*cur);
						assert(c >= 0 && c <= 0377);
					}
				}
				break;
			case 'x':
				/* \x with one or two hexadecimal digits */
				if (num16(cur[1])) {
					cur++;
					c = base16(*cur);
					if (num16(cur[1])) {
						cur++;
						c = mult16(c) + base16(*cur);
					}
				} else
					c = 'x';
				break;
			case 'u':
			case 'U':
				/* \u with four hexadecimal digits or
				 * \U with eight hexadecimal digits */
				if (unlikely(n > 0)) {
					/* not when in the middle of a
					 * UTF-8 sequence */
					goto notutf8;
				}
				c = 0;
				for (n = *cur == 'U' ? 8 : 4; n > 0; n--) {
					cur++;
					if (unlikely(!num16(*cur))) {
						GDKerror("not a Unicode code point escape\n");
						return -1;
					}
					c = c << 4 | base16(*cur);
				}
				/* n == 0 now */
				if (unlikely(c == 0 || c > 0x10FFFF ||
					     (c & 0xFFF800) == 0xD800)) {
					GDKerror("illegal Unicode code point\n");
					return -1;
				}
				if (c < 0x80) {
					*p++ = (unsigned char) c;
				} else {
					if (c < 0x800) {
						*p++ = 0xC0 | (c >> 6);
					} else {
						if (c < 0x10000) {
							*p++ = 0xE0 | (c >> 12);
						} else {
							*p++ = 0xF0 | (c >> 18);
							*p++ = 0x80 | ((c >> 12) & 0x3F);
						}
						*p++ = 0x80 | ((c >> 6) & 0x3F);
					}
					*p++ = 0x80 | (c & 0x3F);
				}
				escaped = false;
				continue;
			case 'a':
				c = '\a';
				break;
			case 'b':
				c = '\b';
				break;
			case 'f':
				c = '\f';
				break;
			case 'n':
				c = '\n';
				break;
			case 'r':
				c = '\r';
				break;
			case 't':
				c = '\t';
				break;
			case '\0':
				c = '\\';
				break;
			case '\'':
			case '\\':
				/* \' and \\ can be handled by the
				 * default case */
			default:
				/* unrecognized \ escape, just copy
				 * the backslashed character */
				c = *cur;
				break;
			}
			escaped = false;
		} else if ((c = *cur) == '\\') {
			escaped = true;
			continue;
		} else if (c == quote && cur[1] == quote) {
			assert(c != 0);
			if (unlikely(n > 0))
				goto notutf8;
			*p++ = quote;
			cur++;
			continue;
		}

		if (n > 0) {
			/* we're still expecting follow-up bytes in a
			 * UTF-8 sequence */
			if (unlikely((c & 0xC0) != 0x80)) {
				/* incorrect UTF-8 sequence: byte is
				 * not 10xxxxxx */
				goto notutf8;
			}
			utf8char = (utf8char << 6) | (c & 0x3F);
			n--;
			if (n == 0) {
				/* this was the last byte in the sequence */
				if (unlikely((utf8char & mask) == 0)) {
					/* incorrect UTF-8 sequence:
					 * not shortest possible */
					goto notutf8;
				}
				if (unlikely(utf8char > 0x10FFFF)) {
					/* incorrect UTF-8 sequence:
					 * value too large */
					goto notutf8;
				}
				if (unlikely((utf8char & 0x1FFF800) == 0xD800)) {
					/* incorrect UTF-8 sequence:
					 * low or high surrogate
					 * encoded as UTF-8 */
					goto notutf8;
				}
			}
		} else if ((c & 0x80) == 0) {
			;
		} else if ((c & 0xE0) == 0xC0) {
			n = 1;
			mask = 0x000780;
			utf8char = c & 0x1F;
		} else if ((c & 0xF0) == 0xE0) {
			n = 2;
			mask = 0x00F800;
			utf8char = c & 0x0F;
		} else if ((c & 0xF8) == 0xF0) {
			n = 3;
			mask = 0x1F0000;
			utf8char = c & 0x07;
		} else {
			/* incorrect UTF-8 sequence */
			goto notutf8;
		}
		*p++ = c;
	}
	if (unlikely(n > 0)) {
		/* incomplete UTF-8 sequence */
		goto notutf8;
	}
	*p++ = 0;
	return len;
  notutf8:
	GDKerror("not a proper UTF-8 sequence\n");
	return -1;
}

ssize_t
strFromStr(const char *restrict src, size_t *restrict len, char **restrict dst, bool external)
{
	const char *cur = src, *start = NULL;
	size_t l = 1;
	bool escaped = false;

	if (!external) {
		size_t sz = strLen(src);
		atommem(sz);
		return (ssize_t) strcpy_len(*dst, src, sz);
	}

	if (strNil(src)) {
		atommem(2);
		strcpy(*dst, str_nil);
		return 1;
	}

	while (GDKisspace(*cur))
		cur++;
	if (*cur != '"') {
		if (strncmp(cur, "nil", 3) == 0) {
			atommem(2);
			strcpy(*dst, str_nil);
			return (ssize_t) (cur - src) + 3;
		}
		GDKerror("not a quoted string\n");
		return -1;
	}

	/* scout the string to find out its length and whether it was
	 * properly quoted */
	for (start = ++cur; *cur != '"' || escaped; cur++) {
		if (*cur == 0) {
			GDKerror("no closing quotes\n");
			return -1;
		} else if (*cur == '\\' && !escaped) {
			escaped = true;
		} else {
			escaped = false;
			l++;
		}
	}

	/* alloc new memory */
	if (*dst == NULL || *len < l) {
		GDKfree(*dst);
		*dst = GDKmalloc(*len = l);
		if (*dst == NULL) {
			*len = 0;
			return -1;
		}
	}

	return GDKstrFromStr((unsigned char *) *dst,
			     (const unsigned char *) start,
			     (ssize_t) (cur - start),
			     '\0');
}

/*
 * Convert a GDK string value to something printable.
 */
/* all but control characters (in range 0 to 31) and DEL */
#define printable_chr(ch)	((' ' <= (ch) && (ch) <= '~') || ((ch) & 0x80) != 0)

size_t
escapedStrlen(const char *restrict src, const char *sep1, const char *sep2, int quote)
{
	size_t end, sz = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (end = 0; src[end]; end++)
		if (src[end] == '\\'
		    || src[end] == quote
		    || (sep1len && strncmp(src + end, sep1, sep1len) == 0)
		    || (sep2len && strncmp(src + end, sep2, sep2len) == 0)) {
			sz += 2;
		} else if (src[end] == (char) '\302' &&
			   0200 <= ((int) src[end + 1] & 0377) &&
			   ((int) src[end + 1] & 0377) <= 0237) {
			/* Unicode control character (code point range
			 * U-00000080 through U-0000009F encoded in
			 * UTF-8 */
			/* for the first one of the two UTF-8 bytes we
			 * count a width of 7 and for the second one
			 * 1, together that's 8, i.e. the width of two
			 * backslash-escaped octal coded characters */
			sz += 7;
		} else if (!printable_chr(src[end])) {
			sz += 4;
		} else {
			sz++;
		}
	return sz;
}

size_t
escapedStr(char *restrict dst, const char *restrict src, size_t dstlen, const char *sep1, const char *sep2, int quote)
{
	size_t cur = 0, l = 0;
	size_t sep1len, sep2len;

	sep1len = sep1 ? strlen(sep1) : 0;
	sep2len = sep2 ? strlen(sep2) : 0;
	for (; src[cur] && l < dstlen; cur++)
		if (!printable_chr(src[cur])
		    || (src[cur] == '\302'
			&& 0200 <= (src[cur + 1] & 0377)
			&& ((int) src[cur + 1] & 0377) <= 0237)
		    || (cur > 0
			&& src[cur - 1] == '\302'
			&& 0200 <= (src[cur] & 0377)
			&& (src[cur] & 0377) <= 0237)) {
			dst[l++] = '\\';
			switch (src[cur]) {
			case '\t':
				dst[l++] = 't';
				break;
			case '\n':
				dst[l++] = 'n';
				break;
			case '\r':
				dst[l++] = 'r';
				break;
			case '\f':
				dst[l++] = 'f';
				break;
			default:
				snprintf(dst + l, dstlen - l, "%03o", (unsigned char) src[cur]);
				l += 3;
				break;
			}
		} else if (src[cur] == '\\'
			   || src[cur] == quote
			   || (sep1len && strncmp(src + cur, sep1, sep1len) == 0)
			   || (sep2len && strncmp(src + cur, sep2, sep2len) == 0)) {
			dst[l++] = '\\';
			dst[l++] = src[cur];
		} else {
			dst[l++] = src[cur];
		}
	assert(l < dstlen);
	dst[l] = 0;
	return l;
}

ssize_t
strToStr(char **restrict dst, size_t *restrict len, const char *restrict src, bool external)
{
	size_t sz;

	if (!external) {
		sz = strLen(src);
		atommem(sz);
		return (ssize_t) strcpy_len(*dst, src, sz);
	}
	if (strNil(src)) {
		atommem(4);
		strcpy(*dst, "nil");
		return 3;
	} else {
		ssize_t l = 0;
		size_t sz = escapedStrlen(src, NULL, NULL, '"');

		atommem(sz + 3);
		l = (ssize_t) escapedStr((*dst) + 1, src, *len - 1, NULL, NULL, '"');
		l++;
		(*dst)[0] = (*dst)[l++] = '"';
		(*dst)[l] = 0;
		return l;
	}
}

str
strRead(str a, size_t *dstlen, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1 || len < 0)
		return NULL;
	if (a == NULL || *dstlen < (size_t) len + 1) {
		if ((a = GDKrealloc(a, len + 1)) == NULL)
			return NULL;
		*dstlen = len + 1;
	}
	if (len && mnstr_read(s, a, len, 1) != 1) {
		GDKfree(a);
		return NULL;
	}
	a[len] = 0;
	return a;
}

gdk_return
strWrite(const char *a, stream *s, size_t cnt)
{
	size_t len = strlen(a);

	(void) cnt;
	assert(cnt == 1);
	if (!checkUTF8(a)) {
		GDKerror("incorrectly encoded UTF-8\n");
		return GDK_FAIL;
	}
	if (mnstr_writeInt(s, (int) len) && mnstr_write(s, a, len, 1) == 1)
		return GDK_SUCCEED;
	else
		return GDK_FAIL;
}

static gdk_return
concat_strings(BAT **bnp, ValPtr pt, BAT *b, oid seqb,
	       BUN ngrp, struct canditer *restrict ci,
	       const oid *restrict gids, oid min, oid max, bool skip_nils,
	       BAT *sep, const char *restrict separator, BUN *has_nils)
{
	oid gid;
	BUN i, p, nils = 0;
	size_t *restrict lengths = NULL, separator_length = 0, next_length;
	str *restrict astrings = NULL;
	BATiter bi, bis = (BATiter) {0};
	BAT *bn = NULL;
	gdk_return rres = GDK_FAIL;

	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	/* exactly one of bnp and pt must be NULL, the other non-NULL */
	assert((bnp == NULL) != (pt == NULL));
	/* if pt not NULL, only a single group allowed */
	assert(pt == NULL || ngrp == 1);

	if (bnp) {
		if ((bn = COLnew(min, TYPE_str, ngrp, TRANSIENT)) == NULL)
			return GDK_FAIL;
		*bnp = bn;
	}

	bi = bat_iterator(b);
	bis = bat_iterator(sep);
	if (separator)
		separator_length = strlen(separator);

	if (ngrp == 1) {
		size_t offset = 0, single_length = 0;
		bool empty = true;

		if (separator) {
			assert(sep == NULL);
			TIMEOUT_LOOP_IDX(i, ci->ncand, qry_ctx) {
				p = canditer_next(ci) - seqb;
				const char *s = BUNtvar(bi, p);
				if (strNil(s)) {
					if (!skip_nils) {
						nils = 1;
						break;
					}
				} else {
					single_length += strlen(s);
					if (!empty)
						single_length += separator_length;
					empty = false;
				}
			}
		} else { /* sep case */
			assert(sep != NULL);
			TIMEOUT_LOOP_IDX(i, ci->ncand, qry_ctx) {
				p = canditer_next(ci) - seqb;
				const char *s = BUNtvar(bi, p);
				const char *sl = BUNtvar(bis, p);
				if (strNil(s)) {
					if (!skip_nils) {
						nils = 1;
						break;
					}
				} else {
					single_length += strlen(s);
					if (!empty) {
						if (strNil(sl)) {
							if (!skip_nils) {
								nils = 1;
								break;
							}
						} else
							single_length += strlen(sl);
					}
					empty = false;
				}
			}
		}
		canditer_reset(ci);
		TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));

		if (nils == 0 && !empty) {
			char *single_str = NULL;

			if ((single_str = GDKmalloc(single_length + 1)) == NULL) {
				bat_iterator_end(&bi);
				bat_iterator_end(&bis);
				BBPreclaim(bn);
				return GDK_FAIL;
			}
			empty = true;
			if (separator) {
				TIMEOUT_LOOP_IDX(i, ci->ncand, qry_ctx) {
					p = canditer_next(ci) - seqb;
					const char *s = BUNtvar(bi, p);
					if (strNil(s))
						continue;
					if (!empty) {
						memcpy(single_str + offset, separator, separator_length);
						offset += separator_length;
					}
					next_length = strlen(s);
					memcpy(single_str + offset, s, next_length);
					offset += next_length;
					empty = false;
				}
			} else { /* sep case */
				assert(sep != NULL);
				TIMEOUT_LOOP_IDX(i, ci->ncand, qry_ctx) {
					p = canditer_next(ci) - seqb;
					const char *s = BUNtvar(bi, p);
					const char *sl = BUNtvar(bis, p);
					if (strNil(s))
						continue;
					if (!empty && !strNil(sl)) {
						next_length = strlen(sl);
						memcpy(single_str + offset, sl, next_length);
						offset += next_length;
					}
					next_length = strlen(s);
					memcpy(single_str + offset, s, next_length);
					offset += next_length;
					empty = false;
				}
			}

			single_str[offset] = '\0';
			TIMEOUT_CHECK(qry_ctx, do { GDKfree(single_str); GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx); } while (0));
			if (bn) {
				if (BUNappend(bn, single_str, false) != GDK_SUCCEED) {
					GDKfree(single_str);
					bat_iterator_end(&bi);
					bat_iterator_end(&bis);
					BBPreclaim(bn);
					return GDK_FAIL;
				}
			} else {
				pt->len = offset + 1;
				pt->val.sval = single_str;
				single_str = NULL;	/* don't free */
			}
			GDKfree(single_str);
		} else if (bn) {
			if (BUNappend(bn, str_nil, false) != GDK_SUCCEED) {
				bat_iterator_end(&bi);
				bat_iterator_end(&bis);
				BBPreclaim(bn);
				return GDK_FAIL;
			}
		} else {
			if (VALinit(pt, TYPE_str, str_nil) == NULL) {
				bat_iterator_end(&bi);
				bat_iterator_end(&bis);
				return GDK_FAIL;
			}
		}
		bat_iterator_end(&bi);
		bat_iterator_end(&bis);
		return GDK_SUCCEED;
	} else {
		/* first used to calculated the total length of
		 * each group, then the the total offset */
		lengths = GDKzalloc(ngrp * sizeof(*lengths));
		astrings = GDKmalloc(ngrp * sizeof(str));
		if (lengths == NULL || astrings == NULL) {
			goto finish;
		}
		/* at first, set astrings[i] to str_nil, then for each
		 * non-empty group (even if all strings in the group
		 * are empty), set to NULL */
		for (i = 0; i < ngrp; i++)
			astrings[i] = (char *) str_nil;

		if (separator) {
			TIMEOUT_LOOP_IDX(p, ci->ncand, qry_ctx) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (lengths[gid] == (size_t) -1)
						continue;
					const char *s = BUNtvar(bi, i);
					if (!strNil(s)) {
						lengths[gid] += strlen(s) + separator_length;
						astrings[gid] = NULL;
					} else if (!skip_nils) {
						nils++;
						lengths[gid] = (size_t) -1;
						astrings[gid] = (char *) str_nil;
					}
				}
			}
		} else { /* sep case */
			assert(sep != NULL);
			TIMEOUT_LOOP_IDX(p, ci->ncand, qry_ctx) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (lengths[gid] == (size_t) -1)
						continue;
					const char *s = BUNtvar(bi, i);
					const char *sl = BUNtvar(bis, i);
					if (!strNil(s)) {
						lengths[gid] += strlen(s);
						if (!strNil(sl)) {
							next_length = strlen(sl);
							lengths[gid] += next_length;
						}
						astrings[gid] = NULL;
					} else if (!skip_nils) {
						nils++;
						lengths[gid] = (size_t) -1;
						astrings[gid] = (char *) str_nil;
					}
				}
			}
		}
		TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(finish, qry_ctx));

		if (separator) {
			for (i = 0; i < ngrp; i++) {
				if (astrings[i] == NULL) {
					if ((astrings[i] = GDKmalloc(lengths[i] + 1)) == NULL) {
						goto finish;
					}
					astrings[i][0] = 0;
					lengths[i] = 0;
				} else
					astrings[i] = NULL;
			}
		} else { /* sep case */
			assert(sep != NULL);
			for (i = 0; i < ngrp; i++) {
				if (astrings[i] == NULL) {
					if ((astrings[i] = GDKmalloc(lengths[i] + 1)) == NULL) {
						goto finish;
					}
					astrings[i][0] = 0;
					lengths[i] = 0;
				} else
					astrings[i] = NULL;
			}
		}
		canditer_reset(ci);

		if (separator) {
			TIMEOUT_LOOP_IDX(p, ci->ncand, qry_ctx) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (astrings[gid]) {
						const char *s = BUNtvar(bi, i);
						if (strNil(s))
							continue;
						if (astrings[gid][lengths[gid]]) {
							memcpy(astrings[gid] + lengths[gid], separator, separator_length);
							lengths[gid] += separator_length;
						}
						next_length = strlen(s);
						memcpy(astrings[gid] + lengths[gid], s, next_length);
						lengths[gid] += next_length;
						astrings[gid][lengths[gid]] = 1;
					}
				}
			}
		} else { /* sep case */
			assert(sep != NULL);
			TIMEOUT_LOOP_IDX(p, ci->ncand, qry_ctx) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (astrings[gid]) {
						const char *s = BUNtvar(bi, i);
						const char *sl = BUNtvar(bis, i);
						if (strNil(s))
							continue;
						if (astrings[gid][lengths[gid]] && !strNil(sl)) {
							next_length = strlen(sl);
							memcpy(astrings[gid] + lengths[gid], sl, next_length);
							lengths[gid] += next_length;
						}
						next_length = strlen(s);
						memcpy(astrings[gid] + lengths[gid], s, next_length);
						lengths[gid] += next_length;
						astrings[gid][lengths[gid]] = 1;
					}
				}
			}
		}
		TIMEOUT_CHECK(qry_ctx, GOTO_LABEL_TIMEOUT_HANDLER(finish, qry_ctx));

		for (i = 0; i < ngrp; i++) {
			if (astrings[i]) {
				astrings[i][lengths[i]] = '\0';
				if (BUNappend(bn, astrings[i], false) != GDK_SUCCEED) {
					goto finish;
				}
			} else if (BUNappend(bn, str_nil, false) != GDK_SUCCEED) {
				goto finish;
			}
		}
		rres = GDK_SUCCEED;
	}

  finish:
	bat_iterator_end(&bi);
	bat_iterator_end(&bis);
	if (has_nils)
		*has_nils = nils;
	GDKfree(lengths);
	if (astrings) {
		for (i = 0; i < ngrp; i++) {
			if (astrings[i] != str_nil)
				GDKfree(astrings[i]);
		}
		GDKfree(astrings);
	}
	if (rres != GDK_SUCCEED)
		BBPreclaim(bn);

	return rres;

  bailout:
	bat_iterator_end(&bi);
	bat_iterator_end(&bis);
	BBPreclaim(bn);
	return GDK_FAIL;
}

gdk_return
BATstr_group_concat(ValPtr res, BAT *b, BAT *s, BAT *sep, bool skip_nils,
		    bool nil_if_empty, const char *restrict separator)
{
	struct canditer ci;
	gdk_return r = GDK_SUCCEED;
	bool free_nseparator = false;
	char *nseparator = (char *)separator;

	assert((nseparator && !sep) || (!nseparator && sep)); /* only one of them must be set */
	res->vtype = TYPE_str;

	canditer_init(&ci, b, s);

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		BATiter bi = bat_iterator(sep);
		nseparator = GDKstrdup(BUNtvar(bi, 0));
		bat_iterator_end(&bi);
		if (!nseparator)
			return GDK_FAIL;
		free_nseparator = true;
		sep = NULL;
	}

	if (ci.ncand == 0 || (nseparator && strNil(nseparator))) {
		if (VALinit(res, TYPE_str, nil_if_empty ? str_nil : "") == NULL)
			r = GDK_FAIL;
		if (free_nseparator)
			GDKfree(nseparator);
		return r;
	}

	r = concat_strings(NULL, res, b, b->hseqbase, 1, &ci, NULL, 0, 0,
			      skip_nils, sep, nseparator, NULL);
	if (free_nseparator)
		GDKfree(nseparator);
	return r;
}

BAT *
BATgroupstr_group_concat(BAT *b, BAT *g, BAT *e, BAT *s, BAT *sep, bool skip_nils,
			 const char *restrict separator)
{
	BAT *bn = NULL;
	oid min, max;
	BUN ngrp, nils = 0;
	struct canditer ci;
	const char *err;
	gdk_return res;
	bool free_nseparator = false;
	char *nseparator = (char *)separator;

	assert((nseparator && !sep) || (!nseparator && sep)); /* only one of them must be set */
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp,
				    &ci)) != NULL) {
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
		return NULL;
	}

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		BATiter bi = bat_iterator(sep);
		nseparator = GDKstrdup(BUNtvar(bi, 0));
		bat_iterator_end(&bi);
		if (!nseparator)
			return NULL;
		free_nseparator = true;
		sep = NULL;
	}

	if (ci.ncand == 0 || ngrp == 0 || (nseparator && strNil(nseparator))) {
		/* trivial: no strings to concat, so return bat
		 * aligned with g with nil in the tail */
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_str, str_nil, ngrp, TRANSIENT);
		goto done;
	}

	if (BATtdense(g) || (g->tkey && g->tnonil)) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		bn = BATconvert(b, s, TYPE_str, 0, 0, 0);
		goto done;
	}

	res = concat_strings(&bn, NULL, b, b->hseqbase, ngrp, &ci,
			     (const oid *) Tloc(g, 0), min, max, skip_nils, sep,
			     nseparator, &nils);
	if (res != GDK_SUCCEED)
		bn = NULL;

done:
	if (free_nseparator)
		GDKfree(nseparator);
	return bn;
}

#define compute_next_single_str(START, END)				\
	do {								\
		for (oid m = START; m < END; m++) {			\
			const char *sb = BUNtvar(bi, m);		\
									\
			if (separator) {				\
				if (!strNil(sb)) {			\
					next_group_length += strlen(sb); \
					if (!empty)			\
						next_group_length += separator_length; \
					empty = false;			\
				}					\
			} else { /* sep case */				\
				assert(sep != NULL);			\
				const char *sl = BUNtvar(sepi, m);	\
									\
				if (!strNil(sb)) {			\
					next_group_length += strlen(sb); \
					if (!empty && !strNil(sl))	\
						next_group_length += strlen(sl); \
					empty = false;			\
				}					\
			}						\
		}							\
		if (empty) {						\
			if (single_str == NULL) { /* reuse the same buffer, resize it when needed */ \
				max_group_length = 1;			\
				if ((single_str = GDKmalloc(max_group_length + 1)) == NULL) \
					goto allocation_error;		\
			} else if (1 > max_group_length) {		\
				max_group_length = 1;			\
				if ((next_single_str = GDKrealloc(single_str, max_group_length + 1)) == NULL) \
					goto allocation_error;		\
				single_str = next_single_str;		\
			}						\
			strcpy(single_str, str_nil);			\
			has_nils = true;				\
		} else {						\
			empty = true;					\
			if (single_str == NULL) { /* reuse the same buffer, resize it when needed */ \
				max_group_length = next_group_length;	\
				if ((single_str = GDKmalloc(max_group_length + 1)) == NULL) \
					goto allocation_error;		\
			} else if (next_group_length > max_group_length) { \
				max_group_length = next_group_length;	\
				if ((next_single_str = GDKrealloc(single_str, max_group_length + 1)) == NULL) \
					goto allocation_error;		\
				single_str = next_single_str;		\
			}						\
									\
			for (oid m = START; m < END; m++) {		\
				const char *sb = BUNtvar(bi, m);	\
									\
				if (separator) {			\
					if (strNil(sb))			\
						continue;		\
					if (!empty) {			\
						memcpy(single_str + offset, separator, separator_length); \
						offset += separator_length; \
					}				\
					next_length = strlen(sb);	\
					memcpy(single_str + offset, sb, next_length); \
					offset += next_length;		\
					empty = false;			\
				} else { /* sep case */			\
					assert(sep != NULL);		\
					const char *sl = BUNtvar(sepi, m); \
									\
					if (strNil(sb))			\
						continue;		\
					if (!empty && !strNil(sl)) {	\
						next_length = strlen(sl); \
						memcpy(single_str + offset, sl, next_length); \
						offset += next_length;	\
					}				\
					next_length = strlen(sb);	\
					memcpy(single_str + offset, sb, next_length); \
					offset += next_length;		\
					empty = false;			\
				}					\
			}						\
									\
			single_str[offset] = '\0';			\
		}							\
} while (0)

#define ANALYTICAL_STR_GROUP_CONCAT_UNBOUNDED_TILL_CURRENT_ROW		\
	do {								\
		size_t slice_length = 0;				\
		next_group_length = next_length = offset = 0;		\
		empty = true;						\
		compute_next_single_str(k, i); /* compute the entire string then slice it starting from the beginning */ \
		empty = true;						\
		for (; k < i;) {					\
			const char *nsep;				\
			oid m = k;					\
			j = k;						\
			do {						\
				k++;					\
			} while (k < i && !op[k]);			\
			for (; j < k; j++) {				\
				const char *nstr = BUNtvar(bi, j);	\
				if (!strNil(nstr)) {			\
					slice_length += strlen(nstr);	\
					if (!empty) {			\
						if (separator) {	\
							nsep = (const char *) separator; \
						} else { /* sep case */	\
							assert(sep != NULL); \
							nsep = BUNtvar(sepi, j); \
						}			\
						if (!strNil(nsep))	\
							slice_length += strlen(nsep); \
					}				\
					empty = false;			\
				}					\
			}						\
			if (empty) {					\
				for (j = m; j < k; j++)			\
					if (tfastins_nocheckVAR(r, j, str_nil) != GDK_SUCCEED) \
						goto allocation_error;	\
				has_nils = true;			\
			} else {					\
				char save = single_str[slice_length];	\
				single_str[slice_length] = '\0';	\
				for (j = m; j < k; j++)			\
					if (tfastins_nocheckVAR(r, j, single_str) != GDK_SUCCEED) \
						goto allocation_error;	\
				single_str[slice_length] = save;	\
			}						\
		}							\
	} while (0)

#define ANALYTICAL_STR_GROUP_CONCAT_ALL_ROWS				\
	do {								\
		next_group_length = next_length = offset = 0;		\
		empty = true;						\
		compute_next_single_str(k, i);				\
		for (; k < i; k++)					\
			if (tfastins_nocheckVAR(r, k, single_str) != GDK_SUCCEED) \
				goto allocation_error;			\
	} while (0)

#define ANALYTICAL_STR_GROUP_CONCAT_CURRENT_ROW				\
	do {								\
		for (; k < i; k++) {					\
			const char *next = BUNtvar(bi, k);		\
			if (tfastins_nocheckVAR(r, k, next) != GDK_SUCCEED) \
				goto allocation_error;			\
			has_nils |= strNil(next);			\
		}							\
	} while (0)

#define ANALYTICAL_STR_GROUP_CONCAT_OTHERS				\
	do {								\
		for (; k < i; k++) {					\
			next_group_length = next_length = offset = 0;	\
			empty = true;					\
			compute_next_single_str(start[k], end[k]);	\
			if (tfastins_nocheckVAR(r, k, single_str) != GDK_SUCCEED) \
				goto allocation_error;			\
		}							\
	} while (0)

#define ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(IMP)	\
	do {						\
		if (p) {				\
			for (; i < cnt; i++) {		\
				if (np[i])		\
					IMP;		\
			}				\
		}					\
		i = cnt;				\
		IMP;					\
	} while (0)

gdk_return
GDKanalytical_str_group_concat(BAT *r, BAT *p, BAT *o, BAT *b, BAT *sep, BAT *s, BAT *e, const char *restrict separator, int frame_type)
{
	bool has_nils = false, empty;
	BATiter pi = bat_iterator(p);
	BATiter oi = bat_iterator(o);
	BATiter bi = bat_iterator(b);
	BATiter sepi = bat_iterator(sep);
	BATiter si = bat_iterator(s);
	BATiter ei = bat_iterator(e);
	oid i = 0, j = 0, k = 0, cnt = bi.count, *restrict start = si.base, *restrict end = ei.base;
	bit *np = pi.base, *op = oi.base;
	str single_str = NULL, next_single_str;
	size_t separator_length = 0, next_group_length, max_group_length = 0, next_length, offset;

	assert((sep && !separator && bi.count == sepi.count) || (!sep && separator));
	if (b->ttype != TYPE_str || r->ttype != TYPE_str || (sep && sep->ttype != TYPE_str)) {
		GDKerror("only string type is supported\n");
		bat_iterator_end(&pi);
		bat_iterator_end(&oi);
		bat_iterator_end(&bi);
		bat_iterator_end(&sepi);
		bat_iterator_end(&si);
		bat_iterator_end(&ei);
		return GDK_FAIL;
	}
	if (sep && sepi.count == 1) { /* Only one element in sep */
		separator = BUNtvar(sepi, 0);
		sep = NULL;
	}

	if (sep == NULL)
		separator_length = strlen(separator);

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_UNBOUNDED_TILL_CURRENT_ROW);
			break;
		case 4: /* current row until unbounded */
			goto notimplemented;
		case 5: /* all rows */
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_ALL_ROWS);
			break;
		case 6: /* current row */
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_CURRENT_ROW);
			break;
		default:
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_OTHERS);
			break;
		}
	}

	bat_iterator_end(&pi);
	bat_iterator_end(&oi);
	bat_iterator_end(&bi);
	bat_iterator_end(&sepi);
	bat_iterator_end(&si);
	bat_iterator_end(&ei);
	GDKfree(single_str);
	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	return GDK_SUCCEED;
  allocation_error:
	bat_iterator_end(&pi);
	bat_iterator_end(&oi);
	bat_iterator_end(&bi);
	bat_iterator_end(&sepi);
	bat_iterator_end(&si);
	bat_iterator_end(&ei);
	GDKfree(single_str);
	return GDK_FAIL;
  notimplemented:
	bat_iterator_end(&pi);
	bat_iterator_end(&oi);
	bat_iterator_end(&bi);
	bat_iterator_end(&sepi);
	bat_iterator_end(&si);
	bat_iterator_end(&ei);
	GDKerror("str_group_concat not yet implemented for current row until unbounded case\n");
	return GDK_FAIL;
}

/* The two case conversion tables are specially crafted from the
 * UnicodeData.txt file for efficient lookup.
 *
 * For the first byte of a UTF-8 encoding, use the value as index into
 * the table.  If the value is zero, there are no conversions for any
 * UTF-8 string starting with this byte (this includes both multi-byte
 * sequences and single-byte sequences, though note that for single-byte
 * sequences (ASCII-compatible) the table is filled in completely at no
 * extra cost).  For a single-byte sequence, if the value is not zero,
 * it is the converted codepoint.  For a multi-byte sequence, if the
 * value is not zero, it is an offset into the same table.  The next
 * byte is added to the offset and again used as index into the table
 * (including the top two bits which are always 1 and 0 respectively).
 * The process then repeats: if zero, no conversions for any sequence
 * starting with the bytes looked up so far, if non-zero, if this is the
 * last byte of a sequence, it is the converted codepoint, and otherwise
 * a (new) offset into the same table. */
static const int lowercase[4288] = {
	[0x0] = 0x0,		/* U+0000: <control> */
	[0x1] = 0x1,		/* U+0001: <control> */
	[0x2] = 0x2,		/* U+0002: <control> */
	[0x3] = 0x3,		/* U+0003: <control> */
	[0x4] = 0x4,		/* U+0004: <control> */
	[0x5] = 0x5,		/* U+0005: <control> */
	[0x6] = 0x6,		/* U+0006: <control> */
	[0x7] = 0x7,		/* U+0007: <control> */
	[0x8] = 0x8,		/* U+0008: <control> */
	[0x9] = 0x9,		/* U+0009: <control> */
	[0xA] = 0xA,		/* U+000A: <control> */
	[0xB] = 0xB,		/* U+000B: <control> */
	[0xC] = 0xC,		/* U+000C: <control> */
	[0xD] = 0xD,		/* U+000D: <control> */
	[0xE] = 0xE,		/* U+000E: <control> */
	[0xF] = 0xF,		/* U+000F: <control> */
	[0x10] = 0x10,		/* U+0010: <control> */
	[0x11] = 0x11,		/* U+0011: <control> */
	[0x12] = 0x12,		/* U+0012: <control> */
	[0x13] = 0x13,		/* U+0013: <control> */
	[0x14] = 0x14,		/* U+0014: <control> */
	[0x15] = 0x15,		/* U+0015: <control> */
	[0x16] = 0x16,		/* U+0016: <control> */
	[0x17] = 0x17,		/* U+0017: <control> */
	[0x18] = 0x18,		/* U+0018: <control> */
	[0x19] = 0x19,		/* U+0019: <control> */
	[0x1A] = 0x1A,		/* U+001A: <control> */
	[0x1B] = 0x1B,		/* U+001B: <control> */
	[0x1C] = 0x1C,		/* U+001C: <control> */
	[0x1D] = 0x1D,		/* U+001D: <control> */
	[0x1E] = 0x1E,		/* U+001E: <control> */
	[0x1F] = 0x1F,		/* U+001F: <control> */
	[0x20] = 0x20,		/* U+0020: SPACE */
	[0x21] = 0x21,		/* U+0021: EXCLAMATION MARK */
	[0x22] = 0x22,		/* U+0022: QUOTATION MARK */
	[0x23] = 0x23,		/* U+0023: NUMBER SIGN */
	[0x24] = 0x24,		/* U+0024: DOLLAR SIGN */
	[0x25] = 0x25,		/* U+0025: PERCENT SIGN */
	[0x26] = 0x26,		/* U+0026: AMPERSAND */
	[0x27] = 0x27,		/* U+0027: APOSTROPHE */
	[0x28] = 0x28,		/* U+0028: LEFT PARENTHESIS */
	[0x29] = 0x29,		/* U+0029: RIGHT PARENTHESIS */
	[0x2A] = 0x2A,		/* U+002A: ASTERISK */
	[0x2B] = 0x2B,		/* U+002B: PLUS SIGN */
	[0x2C] = 0x2C,		/* U+002C: COMMA */
	[0x2D] = 0x2D,		/* U+002D: HYPHEN-MINUS */
	[0x2E] = 0x2E,		/* U+002E: FULL STOP */
	[0x2F] = 0x2F,		/* U+002F: SOLIDUS */
	[0x30] = 0x30,		/* U+0030: DIGIT ZERO */
	[0x31] = 0x31,		/* U+0031: DIGIT ONE */
	[0x32] = 0x32,		/* U+0032: DIGIT TWO */
	[0x33] = 0x33,		/* U+0033: DIGIT THREE */
	[0x34] = 0x34,		/* U+0034: DIGIT FOUR */
	[0x35] = 0x35,		/* U+0035: DIGIT FIVE */
	[0x36] = 0x36,		/* U+0036: DIGIT SIX */
	[0x37] = 0x37,		/* U+0037: DIGIT SEVEN */
	[0x38] = 0x38,		/* U+0038: DIGIT EIGHT */
	[0x39] = 0x39,		/* U+0039: DIGIT NINE */
	[0x3A] = 0x3A,		/* U+003A: COLON */
	[0x3B] = 0x3B,		/* U+003B: SEMICOLON */
	[0x3C] = 0x3C,		/* U+003C: LESS-THAN SIGN */
	[0x3D] = 0x3D,		/* U+003D: EQUALS SIGN */
	[0x3E] = 0x3E,		/* U+003E: GREATER-THAN SIGN */
	[0x3F] = 0x3F,		/* U+003F: QUESTION MARK */
	[0x40] = 0x40,		/* U+0040: COMMERCIAL AT */
	[0x41] = 0x61,		/* U+0041: LATIN CAPITAL LETTER A */
	[0x42] = 0x62,		/* U+0042: LATIN CAPITAL LETTER B */
	[0x43] = 0x63,		/* U+0043: LATIN CAPITAL LETTER C */
	[0x44] = 0x64,		/* U+0044: LATIN CAPITAL LETTER D */
	[0x45] = 0x65,		/* U+0045: LATIN CAPITAL LETTER E */
	[0x46] = 0x66,		/* U+0046: LATIN CAPITAL LETTER F */
	[0x47] = 0x67,		/* U+0047: LATIN CAPITAL LETTER G */
	[0x48] = 0x68,		/* U+0048: LATIN CAPITAL LETTER H */
	[0x49] = 0x69,		/* U+0049: LATIN CAPITAL LETTER I */
	[0x4A] = 0x6A,		/* U+004A: LATIN CAPITAL LETTER J */
	[0x4B] = 0x6B,		/* U+004B: LATIN CAPITAL LETTER K */
	[0x4C] = 0x6C,		/* U+004C: LATIN CAPITAL LETTER L */
	[0x4D] = 0x6D,		/* U+004D: LATIN CAPITAL LETTER M */
	[0x4E] = 0x6E,		/* U+004E: LATIN CAPITAL LETTER N */
	[0x4F] = 0x6F,		/* U+004F: LATIN CAPITAL LETTER O */
	[0x50] = 0x70,		/* U+0050: LATIN CAPITAL LETTER P */
	[0x51] = 0x71,		/* U+0051: LATIN CAPITAL LETTER Q */
	[0x52] = 0x72,		/* U+0052: LATIN CAPITAL LETTER R */
	[0x53] = 0x73,		/* U+0053: LATIN CAPITAL LETTER S */
	[0x54] = 0x74,		/* U+0054: LATIN CAPITAL LETTER T */
	[0x55] = 0x75,		/* U+0055: LATIN CAPITAL LETTER U */
	[0x56] = 0x76,		/* U+0056: LATIN CAPITAL LETTER V */
	[0x57] = 0x77,		/* U+0057: LATIN CAPITAL LETTER W */
	[0x58] = 0x78,		/* U+0058: LATIN CAPITAL LETTER X */
	[0x59] = 0x79,		/* U+0059: LATIN CAPITAL LETTER Y */
	[0x5A] = 0x7A,		/* U+005A: LATIN CAPITAL LETTER Z */
	[0x5B] = 0x5B,		/* U+005B: LEFT SQUARE BRACKET */
	[0x5C] = 0x5C,		/* U+005C: REVERSE SOLIDUS */
	[0x5D] = 0x5D,		/* U+005D: RIGHT SQUARE BRACKET */
	[0x5E] = 0x5E,		/* U+005E: CIRCUMFLEX ACCENT */
	[0x5F] = 0x5F,		/* U+005F: LOW LINE */
	[0x60] = 0x60,		/* U+0060: GRAVE ACCENT */
	[0x61] = 0x61,		/* U+0061: LATIN SMALL LETTER A */
	[0x62] = 0x62,		/* U+0062: LATIN SMALL LETTER B */
	[0x63] = 0x63,		/* U+0063: LATIN SMALL LETTER C */
	[0x64] = 0x64,		/* U+0064: LATIN SMALL LETTER D */
	[0x65] = 0x65,		/* U+0065: LATIN SMALL LETTER E */
	[0x66] = 0x66,		/* U+0066: LATIN SMALL LETTER F */
	[0x67] = 0x67,		/* U+0067: LATIN SMALL LETTER G */
	[0x68] = 0x68,		/* U+0068: LATIN SMALL LETTER H */
	[0x69] = 0x69,		/* U+0069: LATIN SMALL LETTER I */
	[0x6A] = 0x6A,		/* U+006A: LATIN SMALL LETTER J */
	[0x6B] = 0x6B,		/* U+006B: LATIN SMALL LETTER K */
	[0x6C] = 0x6C,		/* U+006C: LATIN SMALL LETTER L */
	[0x6D] = 0x6D,		/* U+006D: LATIN SMALL LETTER M */
	[0x6E] = 0x6E,		/* U+006E: LATIN SMALL LETTER N */
	[0x6F] = 0x6F,		/* U+006F: LATIN SMALL LETTER O */
	[0x70] = 0x70,		/* U+0070: LATIN SMALL LETTER P */
	[0x71] = 0x71,		/* U+0071: LATIN SMALL LETTER Q */
	[0x72] = 0x72,		/* U+0072: LATIN SMALL LETTER R */
	[0x73] = 0x73,		/* U+0073: LATIN SMALL LETTER S */
	[0x74] = 0x74,		/* U+0074: LATIN SMALL LETTER T */
	[0x75] = 0x75,		/* U+0075: LATIN SMALL LETTER U */
	[0x76] = 0x76,		/* U+0076: LATIN SMALL LETTER V */
	[0x77] = 0x77,		/* U+0077: LATIN SMALL LETTER W */
	[0x78] = 0x78,		/* U+0078: LATIN SMALL LETTER X */
	[0x79] = 0x79,		/* U+0079: LATIN SMALL LETTER Y */
	[0x7A] = 0x7A,		/* U+007A: LATIN SMALL LETTER Z */
	[0x7B] = 0x7B,		/* U+007B: LEFT CURLY BRACKET */
	[0x7C] = 0x7C,		/* U+007C: VERTICAL LINE */
	[0x7D] = 0x7D,		/* U+007D: RIGHT CURLY BRACKET */
	[0x7E] = 0x7E,		/* U+007E: TILDE */
	[0x7F] = 0x7F,		/* U+007F: <control> */
	[0xC3] = 256 - 0x80,	/* 303 ... */
	[256+0x0] = 0xE0,	/* U+00C0: LATIN CAPITAL LETTER A WITH GRAVE */
	[256+0x1] = 0xE1,	/* U+00C1: LATIN CAPITAL LETTER A WITH ACUTE */
	[256+0x2] = 0xE2,	/* U+00C2: LATIN CAPITAL LETTER A WITH CIRCUMFLEX */
	[256+0x3] = 0xE3,	/* U+00C3: LATIN CAPITAL LETTER A WITH TILDE */
	[256+0x4] = 0xE4,	/* U+00C4: LATIN CAPITAL LETTER A WITH DIAERESIS */
	[256+0x5] = 0xE5,	/* U+00C5: LATIN CAPITAL LETTER A WITH RING ABOVE */
	[256+0x6] = 0xE6,	/* U+00C6: LATIN CAPITAL LETTER AE */
	[256+0x7] = 0xE7,	/* U+00C7: LATIN CAPITAL LETTER C WITH CEDILLA */
	[256+0x8] = 0xE8,	/* U+00C8: LATIN CAPITAL LETTER E WITH GRAVE */
	[256+0x9] = 0xE9,	/* U+00C9: LATIN CAPITAL LETTER E WITH ACUTE */
	[256+0xA] = 0xEA,	/* U+00CA: LATIN CAPITAL LETTER E WITH CIRCUMFLEX */
	[256+0xB] = 0xEB,	/* U+00CB: LATIN CAPITAL LETTER E WITH DIAERESIS */
	[256+0xC] = 0xEC,	/* U+00CC: LATIN CAPITAL LETTER I WITH GRAVE */
	[256+0xD] = 0xED,	/* U+00CD: LATIN CAPITAL LETTER I WITH ACUTE */
	[256+0xE] = 0xEE,	/* U+00CE: LATIN CAPITAL LETTER I WITH CIRCUMFLEX */
	[256+0xF] = 0xEF,	/* U+00CF: LATIN CAPITAL LETTER I WITH DIAERESIS */
	[256+0x10] = 0xF0,	/* U+00D0: LATIN CAPITAL LETTER ETH */
	[256+0x11] = 0xF1,	/* U+00D1: LATIN CAPITAL LETTER N WITH TILDE */
	[256+0x12] = 0xF2,	/* U+00D2: LATIN CAPITAL LETTER O WITH GRAVE */
	[256+0x13] = 0xF3,	/* U+00D3: LATIN CAPITAL LETTER O WITH ACUTE */
	[256+0x14] = 0xF4,	/* U+00D4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX */
	[256+0x15] = 0xF5,	/* U+00D5: LATIN CAPITAL LETTER O WITH TILDE */
	[256+0x16] = 0xF6,	/* U+00D6: LATIN CAPITAL LETTER O WITH DIAERESIS */
	[256+0x18] = 0xF8,	/* U+00D8: LATIN CAPITAL LETTER O WITH STROKE */
	[256+0x19] = 0xF9,	/* U+00D9: LATIN CAPITAL LETTER U WITH GRAVE */
	[256+0x1A] = 0xFA,	/* U+00DA: LATIN CAPITAL LETTER U WITH ACUTE */
	[256+0x1B] = 0xFB,	/* U+00DB: LATIN CAPITAL LETTER U WITH CIRCUMFLEX */
	[256+0x1C] = 0xFC,	/* U+00DC: LATIN CAPITAL LETTER U WITH DIAERESIS */
	[256+0x1D] = 0xFD,	/* U+00DD: LATIN CAPITAL LETTER Y WITH ACUTE */
	[256+0x1E] = 0xFE,	/* U+00DE: LATIN CAPITAL LETTER THORN */
	[0xC4] = 320 - 0x80,	/* 304 ... */
	[320+0x0] = 0x101,	/* U+0100: LATIN CAPITAL LETTER A WITH MACRON */
	[320+0x2] = 0x103,	/* U+0102: LATIN CAPITAL LETTER A WITH BREVE */
	[320+0x4] = 0x105,	/* U+0104: LATIN CAPITAL LETTER A WITH OGONEK */
	[320+0x6] = 0x107,	/* U+0106: LATIN CAPITAL LETTER C WITH ACUTE */
	[320+0x8] = 0x109,	/* U+0108: LATIN CAPITAL LETTER C WITH CIRCUMFLEX */
	[320+0xA] = 0x10B,	/* U+010A: LATIN CAPITAL LETTER C WITH DOT ABOVE */
	[320+0xC] = 0x10D,	/* U+010C: LATIN CAPITAL LETTER C WITH CARON */
	[320+0xE] = 0x10F,	/* U+010E: LATIN CAPITAL LETTER D WITH CARON */
	[320+0x10] = 0x111,	/* U+0110: LATIN CAPITAL LETTER D WITH STROKE */
	[320+0x12] = 0x113,	/* U+0112: LATIN CAPITAL LETTER E WITH MACRON */
	[320+0x14] = 0x115,	/* U+0114: LATIN CAPITAL LETTER E WITH BREVE */
	[320+0x16] = 0x117,	/* U+0116: LATIN CAPITAL LETTER E WITH DOT ABOVE */
	[320+0x18] = 0x119,	/* U+0118: LATIN CAPITAL LETTER E WITH OGONEK */
	[320+0x1A] = 0x11B,	/* U+011A: LATIN CAPITAL LETTER E WITH CARON */
	[320+0x1C] = 0x11D,	/* U+011C: LATIN CAPITAL LETTER G WITH CIRCUMFLEX */
	[320+0x1E] = 0x11F,	/* U+011E: LATIN CAPITAL LETTER G WITH BREVE */
	[320+0x20] = 0x121,	/* U+0120: LATIN CAPITAL LETTER G WITH DOT ABOVE */
	[320+0x22] = 0x123,	/* U+0122: LATIN CAPITAL LETTER G WITH CEDILLA */
	[320+0x24] = 0x125,	/* U+0124: LATIN CAPITAL LETTER H WITH CIRCUMFLEX */
	[320+0x26] = 0x127,	/* U+0126: LATIN CAPITAL LETTER H WITH STROKE */
	[320+0x28] = 0x129,	/* U+0128: LATIN CAPITAL LETTER I WITH TILDE */
	[320+0x2A] = 0x12B,	/* U+012A: LATIN CAPITAL LETTER I WITH MACRON */
	[320+0x2C] = 0x12D,	/* U+012C: LATIN CAPITAL LETTER I WITH BREVE */
	[320+0x2E] = 0x12F,	/* U+012E: LATIN CAPITAL LETTER I WITH OGONEK */
	[320+0x30] = 0x69,	/* U+0130: LATIN CAPITAL LETTER I WITH DOT ABOVE */
	[320+0x32] = 0x133,	/* U+0132: LATIN CAPITAL LIGATURE IJ */
	[320+0x34] = 0x135,	/* U+0134: LATIN CAPITAL LETTER J WITH CIRCUMFLEX */
	[320+0x36] = 0x137,	/* U+0136: LATIN CAPITAL LETTER K WITH CEDILLA */
	[320+0x39] = 0x13A,	/* U+0139: LATIN CAPITAL LETTER L WITH ACUTE */
	[320+0x3B] = 0x13C,	/* U+013B: LATIN CAPITAL LETTER L WITH CEDILLA */
	[320+0x3D] = 0x13E,	/* U+013D: LATIN CAPITAL LETTER L WITH CARON */
	[320+0x3F] = 0x140,	/* U+013F: LATIN CAPITAL LETTER L WITH MIDDLE DOT */
	[0xC5] = 384 - 0x80,	/* 305 ... */
	[384+0x1] = 0x142,	/* U+0141: LATIN CAPITAL LETTER L WITH STROKE */
	[384+0x3] = 0x144,	/* U+0143: LATIN CAPITAL LETTER N WITH ACUTE */
	[384+0x5] = 0x146,	/* U+0145: LATIN CAPITAL LETTER N WITH CEDILLA */
	[384+0x7] = 0x148,	/* U+0147: LATIN CAPITAL LETTER N WITH CARON */
	[384+0xA] = 0x14B,	/* U+014A: LATIN CAPITAL LETTER ENG */
	[384+0xC] = 0x14D,	/* U+014C: LATIN CAPITAL LETTER O WITH MACRON */
	[384+0xE] = 0x14F,	/* U+014E: LATIN CAPITAL LETTER O WITH BREVE */
	[384+0x10] = 0x151,	/* U+0150: LATIN CAPITAL LETTER O WITH DOUBLE ACUTE */
	[384+0x12] = 0x153,	/* U+0152: LATIN CAPITAL LIGATURE OE */
	[384+0x14] = 0x155,	/* U+0154: LATIN CAPITAL LETTER R WITH ACUTE */
	[384+0x16] = 0x157,	/* U+0156: LATIN CAPITAL LETTER R WITH CEDILLA */
	[384+0x18] = 0x159,	/* U+0158: LATIN CAPITAL LETTER R WITH CARON */
	[384+0x1A] = 0x15B,	/* U+015A: LATIN CAPITAL LETTER S WITH ACUTE */
	[384+0x1C] = 0x15D,	/* U+015C: LATIN CAPITAL LETTER S WITH CIRCUMFLEX */
	[384+0x1E] = 0x15F,	/* U+015E: LATIN CAPITAL LETTER S WITH CEDILLA */
	[384+0x20] = 0x161,	/* U+0160: LATIN CAPITAL LETTER S WITH CARON */
	[384+0x22] = 0x163,	/* U+0162: LATIN CAPITAL LETTER T WITH CEDILLA */
	[384+0x24] = 0x165,	/* U+0164: LATIN CAPITAL LETTER T WITH CARON */
	[384+0x26] = 0x167,	/* U+0166: LATIN CAPITAL LETTER T WITH STROKE */
	[384+0x28] = 0x169,	/* U+0168: LATIN CAPITAL LETTER U WITH TILDE */
	[384+0x2A] = 0x16B,	/* U+016A: LATIN CAPITAL LETTER U WITH MACRON */
	[384+0x2C] = 0x16D,	/* U+016C: LATIN CAPITAL LETTER U WITH BREVE */
	[384+0x2E] = 0x16F,	/* U+016E: LATIN CAPITAL LETTER U WITH RING ABOVE */
	[384+0x30] = 0x171,	/* U+0170: LATIN CAPITAL LETTER U WITH DOUBLE ACUTE */
	[384+0x32] = 0x173,	/* U+0172: LATIN CAPITAL LETTER U WITH OGONEK */
	[384+0x34] = 0x175,	/* U+0174: LATIN CAPITAL LETTER W WITH CIRCUMFLEX */
	[384+0x36] = 0x177,	/* U+0176: LATIN CAPITAL LETTER Y WITH CIRCUMFLEX */
	[384+0x38] = 0xFF,	/* U+0178: LATIN CAPITAL LETTER Y WITH DIAERESIS */
	[384+0x39] = 0x17A,	/* U+0179: LATIN CAPITAL LETTER Z WITH ACUTE */
	[384+0x3B] = 0x17C,	/* U+017B: LATIN CAPITAL LETTER Z WITH DOT ABOVE */
	[384+0x3D] = 0x17E,	/* U+017D: LATIN CAPITAL LETTER Z WITH CARON */
	[0xC6] = 448 - 0x80,	/* 306 ... */
	[448+0x1] = 0x253,	/* U+0181: LATIN CAPITAL LETTER B WITH HOOK */
	[448+0x2] = 0x183,	/* U+0182: LATIN CAPITAL LETTER B WITH TOPBAR */
	[448+0x4] = 0x185,	/* U+0184: LATIN CAPITAL LETTER TONE SIX */
	[448+0x6] = 0x254,	/* U+0186: LATIN CAPITAL LETTER OPEN O */
	[448+0x7] = 0x188,	/* U+0187: LATIN CAPITAL LETTER C WITH HOOK */
	[448+0x9] = 0x256,	/* U+0189: LATIN CAPITAL LETTER AFRICAN D */
	[448+0xA] = 0x257,	/* U+018A: LATIN CAPITAL LETTER D WITH HOOK */
	[448+0xB] = 0x18C,	/* U+018B: LATIN CAPITAL LETTER D WITH TOPBAR */
	[448+0xE] = 0x1DD,	/* U+018E: LATIN CAPITAL LETTER REVERSED E */
	[448+0xF] = 0x259,	/* U+018F: LATIN CAPITAL LETTER SCHWA */
	[448+0x10] = 0x25B,	/* U+0190: LATIN CAPITAL LETTER OPEN E */
	[448+0x11] = 0x192,	/* U+0191: LATIN CAPITAL LETTER F WITH HOOK */
	[448+0x13] = 0x260,	/* U+0193: LATIN CAPITAL LETTER G WITH HOOK */
	[448+0x14] = 0x263,	/* U+0194: LATIN CAPITAL LETTER GAMMA */
	[448+0x16] = 0x269,	/* U+0196: LATIN CAPITAL LETTER IOTA */
	[448+0x17] = 0x268,	/* U+0197: LATIN CAPITAL LETTER I WITH STROKE */
	[448+0x18] = 0x199,	/* U+0198: LATIN CAPITAL LETTER K WITH HOOK */
	[448+0x1C] = 0x26F,	/* U+019C: LATIN CAPITAL LETTER TURNED M */
	[448+0x1D] = 0x272,	/* U+019D: LATIN CAPITAL LETTER N WITH LEFT HOOK */
	[448+0x1F] = 0x275,	/* U+019F: LATIN CAPITAL LETTER O WITH MIDDLE TILDE */
	[448+0x20] = 0x1A1,	/* U+01A0: LATIN CAPITAL LETTER O WITH HORN */
	[448+0x22] = 0x1A3,	/* U+01A2: LATIN CAPITAL LETTER OI */
	[448+0x24] = 0x1A5,	/* U+01A4: LATIN CAPITAL LETTER P WITH HOOK */
	[448+0x26] = 0x280,	/* U+01A6: LATIN LETTER YR */
	[448+0x27] = 0x1A8,	/* U+01A7: LATIN CAPITAL LETTER TONE TWO */
	[448+0x29] = 0x283,	/* U+01A9: LATIN CAPITAL LETTER ESH */
	[448+0x2C] = 0x1AD,	/* U+01AC: LATIN CAPITAL LETTER T WITH HOOK */
	[448+0x2E] = 0x288,	/* U+01AE: LATIN CAPITAL LETTER T WITH RETROFLEX HOOK */
	[448+0x2F] = 0x1B0,	/* U+01AF: LATIN CAPITAL LETTER U WITH HORN */
	[448+0x31] = 0x28A,	/* U+01B1: LATIN CAPITAL LETTER UPSILON */
	[448+0x32] = 0x28B,	/* U+01B2: LATIN CAPITAL LETTER V WITH HOOK */
	[448+0x33] = 0x1B4,	/* U+01B3: LATIN CAPITAL LETTER Y WITH HOOK */
	[448+0x35] = 0x1B6,	/* U+01B5: LATIN CAPITAL LETTER Z WITH STROKE */
	[448+0x37] = 0x292,	/* U+01B7: LATIN CAPITAL LETTER EZH */
	[448+0x38] = 0x1B9,	/* U+01B8: LATIN CAPITAL LETTER EZH REVERSED */
	[448+0x3C] = 0x1BD,	/* U+01BC: LATIN CAPITAL LETTER TONE FIVE */
	[0xC7] = 512 - 0x80,	/* 307 ... */
	[512+0x4] = 0x1C6,	/* U+01C4: LATIN CAPITAL LETTER DZ WITH CARON */
	[512+0x5] = 0x1C6,	/* U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON */
	[512+0x7] = 0x1C9,	/* U+01C7: LATIN CAPITAL LETTER LJ */
	[512+0x8] = 0x1C9,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[512+0xA] = 0x1CC,	/* U+01CA: LATIN CAPITAL LETTER NJ */
	[512+0xB] = 0x1CC,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[512+0xD] = 0x1CE,	/* U+01CD: LATIN CAPITAL LETTER A WITH CARON */
	[512+0xF] = 0x1D0,	/* U+01CF: LATIN CAPITAL LETTER I WITH CARON */
	[512+0x11] = 0x1D2,	/* U+01D1: LATIN CAPITAL LETTER O WITH CARON */
	[512+0x13] = 0x1D4,	/* U+01D3: LATIN CAPITAL LETTER U WITH CARON */
	[512+0x15] = 0x1D6,	/* U+01D5: LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON */
	[512+0x17] = 0x1D8,	/* U+01D7: LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE */
	[512+0x19] = 0x1DA,	/* U+01D9: LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON */
	[512+0x1B] = 0x1DC,	/* U+01DB: LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE */
	[512+0x1E] = 0x1DF,	/* U+01DE: LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON */
	[512+0x20] = 0x1E1,	/* U+01E0: LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON */
	[512+0x22] = 0x1E3,	/* U+01E2: LATIN CAPITAL LETTER AE WITH MACRON */
	[512+0x24] = 0x1E5,	/* U+01E4: LATIN CAPITAL LETTER G WITH STROKE */
	[512+0x26] = 0x1E7,	/* U+01E6: LATIN CAPITAL LETTER G WITH CARON */
	[512+0x28] = 0x1E9,	/* U+01E8: LATIN CAPITAL LETTER K WITH CARON */
	[512+0x2A] = 0x1EB,	/* U+01EA: LATIN CAPITAL LETTER O WITH OGONEK */
	[512+0x2C] = 0x1ED,	/* U+01EC: LATIN CAPITAL LETTER O WITH OGONEK AND MACRON */
	[512+0x2E] = 0x1EF,	/* U+01EE: LATIN CAPITAL LETTER EZH WITH CARON */
	[512+0x31] = 0x1F3,	/* U+01F1: LATIN CAPITAL LETTER DZ */
	[512+0x32] = 0x1F3,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[512+0x34] = 0x1F5,	/* U+01F4: LATIN CAPITAL LETTER G WITH ACUTE */
	[512+0x36] = 0x195,	/* U+01F6: LATIN CAPITAL LETTER HWAIR */
	[512+0x37] = 0x1BF,	/* U+01F7: LATIN CAPITAL LETTER WYNN */
	[512+0x38] = 0x1F9,	/* U+01F8: LATIN CAPITAL LETTER N WITH GRAVE */
	[512+0x3A] = 0x1FB,	/* U+01FA: LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE */
	[512+0x3C] = 0x1FD,	/* U+01FC: LATIN CAPITAL LETTER AE WITH ACUTE */
	[512+0x3E] = 0x1FF,	/* U+01FE: LATIN CAPITAL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 576 - 0x80,	/* 310 ... */
	[576+0x0] = 0x201,	/* U+0200: LATIN CAPITAL LETTER A WITH DOUBLE GRAVE */
	[576+0x2] = 0x203,	/* U+0202: LATIN CAPITAL LETTER A WITH INVERTED BREVE */
	[576+0x4] = 0x205,	/* U+0204: LATIN CAPITAL LETTER E WITH DOUBLE GRAVE */
	[576+0x6] = 0x207,	/* U+0206: LATIN CAPITAL LETTER E WITH INVERTED BREVE */
	[576+0x8] = 0x209,	/* U+0208: LATIN CAPITAL LETTER I WITH DOUBLE GRAVE */
	[576+0xA] = 0x20B,	/* U+020A: LATIN CAPITAL LETTER I WITH INVERTED BREVE */
	[576+0xC] = 0x20D,	/* U+020C: LATIN CAPITAL LETTER O WITH DOUBLE GRAVE */
	[576+0xE] = 0x20F,	/* U+020E: LATIN CAPITAL LETTER O WITH INVERTED BREVE */
	[576+0x10] = 0x211,	/* U+0210: LATIN CAPITAL LETTER R WITH DOUBLE GRAVE */
	[576+0x12] = 0x213,	/* U+0212: LATIN CAPITAL LETTER R WITH INVERTED BREVE */
	[576+0x14] = 0x215,	/* U+0214: LATIN CAPITAL LETTER U WITH DOUBLE GRAVE */
	[576+0x16] = 0x217,	/* U+0216: LATIN CAPITAL LETTER U WITH INVERTED BREVE */
	[576+0x18] = 0x219,	/* U+0218: LATIN CAPITAL LETTER S WITH COMMA BELOW */
	[576+0x1A] = 0x21B,	/* U+021A: LATIN CAPITAL LETTER T WITH COMMA BELOW */
	[576+0x1C] = 0x21D,	/* U+021C: LATIN CAPITAL LETTER YOGH */
	[576+0x1E] = 0x21F,	/* U+021E: LATIN CAPITAL LETTER H WITH CARON */
	[576+0x20] = 0x19E,	/* U+0220: LATIN CAPITAL LETTER N WITH LONG RIGHT LEG */
	[576+0x22] = 0x223,	/* U+0222: LATIN CAPITAL LETTER OU */
	[576+0x24] = 0x225,	/* U+0224: LATIN CAPITAL LETTER Z WITH HOOK */
	[576+0x26] = 0x227,	/* U+0226: LATIN CAPITAL LETTER A WITH DOT ABOVE */
	[576+0x28] = 0x229,	/* U+0228: LATIN CAPITAL LETTER E WITH CEDILLA */
	[576+0x2A] = 0x22B,	/* U+022A: LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON */
	[576+0x2C] = 0x22D,	/* U+022C: LATIN CAPITAL LETTER O WITH TILDE AND MACRON */
	[576+0x2E] = 0x22F,	/* U+022E: LATIN CAPITAL LETTER O WITH DOT ABOVE */
	[576+0x30] = 0x231,	/* U+0230: LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON */
	[576+0x32] = 0x233,	/* U+0232: LATIN CAPITAL LETTER Y WITH MACRON */
	[576+0x3A] = 0x2C65,	/* U+023A: LATIN CAPITAL LETTER A WITH STROKE */
	[576+0x3B] = 0x23C,	/* U+023B: LATIN CAPITAL LETTER C WITH STROKE */
	[576+0x3D] = 0x19A,	/* U+023D: LATIN CAPITAL LETTER L WITH BAR */
	[576+0x3E] = 0x2C66,	/* U+023E: LATIN CAPITAL LETTER T WITH DIAGONAL STROKE */
	[0xC9] = 640 - 0x80,	/* 311 ... */
	[640+0x1] = 0x242,	/* U+0241: LATIN CAPITAL LETTER GLOTTAL STOP */
	[640+0x3] = 0x180,	/* U+0243: LATIN CAPITAL LETTER B WITH STROKE */
	[640+0x4] = 0x289,	/* U+0244: LATIN CAPITAL LETTER U BAR */
	[640+0x5] = 0x28C,	/* U+0245: LATIN CAPITAL LETTER TURNED V */
	[640+0x6] = 0x247,	/* U+0246: LATIN CAPITAL LETTER E WITH STROKE */
	[640+0x8] = 0x249,	/* U+0248: LATIN CAPITAL LETTER J WITH STROKE */
	[640+0xA] = 0x24B,	/* U+024A: LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL */
	[640+0xC] = 0x24D,	/* U+024C: LATIN CAPITAL LETTER R WITH STROKE */
	[640+0xE] = 0x24F,	/* U+024E: LATIN CAPITAL LETTER Y WITH STROKE */
	[0xCD] = 704 - 0x80,	/* 315 ... */
	[704+0x30] = 0x371,	/* U+0370: GREEK CAPITAL LETTER HETA */
	[704+0x32] = 0x373,	/* U+0372: GREEK CAPITAL LETTER ARCHAIC SAMPI */
	[704+0x36] = 0x377,	/* U+0376: GREEK CAPITAL LETTER PAMPHYLIAN DIGAMMA */
	[704+0x3F] = 0x3F3,	/* U+037F: GREEK CAPITAL LETTER YOT */
	[0xCE] = 768 - 0x80,	/* 316 ... */
	[768+0x6] = 0x3AC,	/* U+0386: GREEK CAPITAL LETTER ALPHA WITH TONOS */
	[768+0x8] = 0x3AD,	/* U+0388: GREEK CAPITAL LETTER EPSILON WITH TONOS */
	[768+0x9] = 0x3AE,	/* U+0389: GREEK CAPITAL LETTER ETA WITH TONOS */
	[768+0xA] = 0x3AF,	/* U+038A: GREEK CAPITAL LETTER IOTA WITH TONOS */
	[768+0xC] = 0x3CC,	/* U+038C: GREEK CAPITAL LETTER OMICRON WITH TONOS */
	[768+0xE] = 0x3CD,	/* U+038E: GREEK CAPITAL LETTER UPSILON WITH TONOS */
	[768+0xF] = 0x3CE,	/* U+038F: GREEK CAPITAL LETTER OMEGA WITH TONOS */
	[768+0x11] = 0x3B1,	/* U+0391: GREEK CAPITAL LETTER ALPHA */
	[768+0x12] = 0x3B2,	/* U+0392: GREEK CAPITAL LETTER BETA */
	[768+0x13] = 0x3B3,	/* U+0393: GREEK CAPITAL LETTER GAMMA */
	[768+0x14] = 0x3B4,	/* U+0394: GREEK CAPITAL LETTER DELTA */
	[768+0x15] = 0x3B5,	/* U+0395: GREEK CAPITAL LETTER EPSILON */
	[768+0x16] = 0x3B6,	/* U+0396: GREEK CAPITAL LETTER ZETA */
	[768+0x17] = 0x3B7,	/* U+0397: GREEK CAPITAL LETTER ETA */
	[768+0x18] = 0x3B8,	/* U+0398: GREEK CAPITAL LETTER THETA */
	[768+0x19] = 0x3B9,	/* U+0399: GREEK CAPITAL LETTER IOTA */
	[768+0x1A] = 0x3BA,	/* U+039A: GREEK CAPITAL LETTER KAPPA */
	[768+0x1B] = 0x3BB,	/* U+039B: GREEK CAPITAL LETTER LAMDA */
	[768+0x1C] = 0x3BC,	/* U+039C: GREEK CAPITAL LETTER MU */
	[768+0x1D] = 0x3BD,	/* U+039D: GREEK CAPITAL LETTER NU */
	[768+0x1E] = 0x3BE,	/* U+039E: GREEK CAPITAL LETTER XI */
	[768+0x1F] = 0x3BF,	/* U+039F: GREEK CAPITAL LETTER OMICRON */
	[768+0x20] = 0x3C0,	/* U+03A0: GREEK CAPITAL LETTER PI */
	[768+0x21] = 0x3C1,	/* U+03A1: GREEK CAPITAL LETTER RHO */
	[768+0x23] = 0x3C3,	/* U+03A3: GREEK CAPITAL LETTER SIGMA */
	[768+0x24] = 0x3C4,	/* U+03A4: GREEK CAPITAL LETTER TAU */
	[768+0x25] = 0x3C5,	/* U+03A5: GREEK CAPITAL LETTER UPSILON */
	[768+0x26] = 0x3C6,	/* U+03A6: GREEK CAPITAL LETTER PHI */
	[768+0x27] = 0x3C7,	/* U+03A7: GREEK CAPITAL LETTER CHI */
	[768+0x28] = 0x3C8,	/* U+03A8: GREEK CAPITAL LETTER PSI */
	[768+0x29] = 0x3C9,	/* U+03A9: GREEK CAPITAL LETTER OMEGA */
	[768+0x2A] = 0x3CA,	/* U+03AA: GREEK CAPITAL LETTER IOTA WITH DIALYTIKA */
	[768+0x2B] = 0x3CB,	/* U+03AB: GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA */
	[0xCF] = 832 - 0x80,	/* 317 ... */
	[832+0xF] = 0x3D7,	/* U+03CF: GREEK CAPITAL KAI SYMBOL */
	[832+0x18] = 0x3D9,	/* U+03D8: GREEK LETTER ARCHAIC KOPPA */
	[832+0x1A] = 0x3DB,	/* U+03DA: GREEK LETTER STIGMA */
	[832+0x1C] = 0x3DD,	/* U+03DC: GREEK LETTER DIGAMMA */
	[832+0x1E] = 0x3DF,	/* U+03DE: GREEK LETTER KOPPA */
	[832+0x20] = 0x3E1,	/* U+03E0: GREEK LETTER SAMPI */
	[832+0x22] = 0x3E3,	/* U+03E2: COPTIC CAPITAL LETTER SHEI */
	[832+0x24] = 0x3E5,	/* U+03E4: COPTIC CAPITAL LETTER FEI */
	[832+0x26] = 0x3E7,	/* U+03E6: COPTIC CAPITAL LETTER KHEI */
	[832+0x28] = 0x3E9,	/* U+03E8: COPTIC CAPITAL LETTER HORI */
	[832+0x2A] = 0x3EB,	/* U+03EA: COPTIC CAPITAL LETTER GANGIA */
	[832+0x2C] = 0x3ED,	/* U+03EC: COPTIC CAPITAL LETTER SHIMA */
	[832+0x2E] = 0x3EF,	/* U+03EE: COPTIC CAPITAL LETTER DEI */
	[832+0x34] = 0x3B8,	/* U+03F4: GREEK CAPITAL THETA SYMBOL */
	[832+0x37] = 0x3F8,	/* U+03F7: GREEK CAPITAL LETTER SHO */
	[832+0x39] = 0x3F2,	/* U+03F9: GREEK CAPITAL LUNATE SIGMA SYMBOL */
	[832+0x3A] = 0x3FB,	/* U+03FA: GREEK CAPITAL LETTER SAN */
	[832+0x3D] = 0x37B,	/* U+03FD: GREEK CAPITAL REVERSED LUNATE SIGMA SYMBOL */
	[832+0x3E] = 0x37C,	/* U+03FE: GREEK CAPITAL DOTTED LUNATE SIGMA SYMBOL */
	[832+0x3F] = 0x37D,	/* U+03FF: GREEK CAPITAL REVERSED DOTTED LUNATE SIGMA SYMBOL */
	[0xD0] = 896 - 0x80,	/* 320 ... */
	[896+0x0] = 0x450,	/* U+0400: CYRILLIC CAPITAL LETTER IE WITH GRAVE */
	[896+0x1] = 0x451,	/* U+0401: CYRILLIC CAPITAL LETTER IO */
	[896+0x2] = 0x452,	/* U+0402: CYRILLIC CAPITAL LETTER DJE */
	[896+0x3] = 0x453,	/* U+0403: CYRILLIC CAPITAL LETTER GJE */
	[896+0x4] = 0x454,	/* U+0404: CYRILLIC CAPITAL LETTER UKRAINIAN IE */
	[896+0x5] = 0x455,	/* U+0405: CYRILLIC CAPITAL LETTER DZE */
	[896+0x6] = 0x456,	/* U+0406: CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I */
	[896+0x7] = 0x457,	/* U+0407: CYRILLIC CAPITAL LETTER YI */
	[896+0x8] = 0x458,	/* U+0408: CYRILLIC CAPITAL LETTER JE */
	[896+0x9] = 0x459,	/* U+0409: CYRILLIC CAPITAL LETTER LJE */
	[896+0xA] = 0x45A,	/* U+040A: CYRILLIC CAPITAL LETTER NJE */
	[896+0xB] = 0x45B,	/* U+040B: CYRILLIC CAPITAL LETTER TSHE */
	[896+0xC] = 0x45C,	/* U+040C: CYRILLIC CAPITAL LETTER KJE */
	[896+0xD] = 0x45D,	/* U+040D: CYRILLIC CAPITAL LETTER I WITH GRAVE */
	[896+0xE] = 0x45E,	/* U+040E: CYRILLIC CAPITAL LETTER SHORT U */
	[896+0xF] = 0x45F,	/* U+040F: CYRILLIC CAPITAL LETTER DZHE */
	[896+0x10] = 0x430,	/* U+0410: CYRILLIC CAPITAL LETTER A */
	[896+0x11] = 0x431,	/* U+0411: CYRILLIC CAPITAL LETTER BE */
	[896+0x12] = 0x432,	/* U+0412: CYRILLIC CAPITAL LETTER VE */
	[896+0x13] = 0x433,	/* U+0413: CYRILLIC CAPITAL LETTER GHE */
	[896+0x14] = 0x434,	/* U+0414: CYRILLIC CAPITAL LETTER DE */
	[896+0x15] = 0x435,	/* U+0415: CYRILLIC CAPITAL LETTER IE */
	[896+0x16] = 0x436,	/* U+0416: CYRILLIC CAPITAL LETTER ZHE */
	[896+0x17] = 0x437,	/* U+0417: CYRILLIC CAPITAL LETTER ZE */
	[896+0x18] = 0x438,	/* U+0418: CYRILLIC CAPITAL LETTER I */
	[896+0x19] = 0x439,	/* U+0419: CYRILLIC CAPITAL LETTER SHORT I */
	[896+0x1A] = 0x43A,	/* U+041A: CYRILLIC CAPITAL LETTER KA */
	[896+0x1B] = 0x43B,	/* U+041B: CYRILLIC CAPITAL LETTER EL */
	[896+0x1C] = 0x43C,	/* U+041C: CYRILLIC CAPITAL LETTER EM */
	[896+0x1D] = 0x43D,	/* U+041D: CYRILLIC CAPITAL LETTER EN */
	[896+0x1E] = 0x43E,	/* U+041E: CYRILLIC CAPITAL LETTER O */
	[896+0x1F] = 0x43F,	/* U+041F: CYRILLIC CAPITAL LETTER PE */
	[896+0x20] = 0x440,	/* U+0420: CYRILLIC CAPITAL LETTER ER */
	[896+0x21] = 0x441,	/* U+0421: CYRILLIC CAPITAL LETTER ES */
	[896+0x22] = 0x442,	/* U+0422: CYRILLIC CAPITAL LETTER TE */
	[896+0x23] = 0x443,	/* U+0423: CYRILLIC CAPITAL LETTER U */
	[896+0x24] = 0x444,	/* U+0424: CYRILLIC CAPITAL LETTER EF */
	[896+0x25] = 0x445,	/* U+0425: CYRILLIC CAPITAL LETTER HA */
	[896+0x26] = 0x446,	/* U+0426: CYRILLIC CAPITAL LETTER TSE */
	[896+0x27] = 0x447,	/* U+0427: CYRILLIC CAPITAL LETTER CHE */
	[896+0x28] = 0x448,	/* U+0428: CYRILLIC CAPITAL LETTER SHA */
	[896+0x29] = 0x449,	/* U+0429: CYRILLIC CAPITAL LETTER SHCHA */
	[896+0x2A] = 0x44A,	/* U+042A: CYRILLIC CAPITAL LETTER HARD SIGN */
	[896+0x2B] = 0x44B,	/* U+042B: CYRILLIC CAPITAL LETTER YERU */
	[896+0x2C] = 0x44C,	/* U+042C: CYRILLIC CAPITAL LETTER SOFT SIGN */
	[896+0x2D] = 0x44D,	/* U+042D: CYRILLIC CAPITAL LETTER E */
	[896+0x2E] = 0x44E,	/* U+042E: CYRILLIC CAPITAL LETTER YU */
	[896+0x2F] = 0x44F,	/* U+042F: CYRILLIC CAPITAL LETTER YA */
	[0xD1] = 960 - 0x80,	/* 321 ... */
	[960+0x20] = 0x461,	/* U+0460: CYRILLIC CAPITAL LETTER OMEGA */
	[960+0x22] = 0x463,	/* U+0462: CYRILLIC CAPITAL LETTER YAT */
	[960+0x24] = 0x465,	/* U+0464: CYRILLIC CAPITAL LETTER IOTIFIED E */
	[960+0x26] = 0x467,	/* U+0466: CYRILLIC CAPITAL LETTER LITTLE YUS */
	[960+0x28] = 0x469,	/* U+0468: CYRILLIC CAPITAL LETTER IOTIFIED LITTLE YUS */
	[960+0x2A] = 0x46B,	/* U+046A: CYRILLIC CAPITAL LETTER BIG YUS */
	[960+0x2C] = 0x46D,	/* U+046C: CYRILLIC CAPITAL LETTER IOTIFIED BIG YUS */
	[960+0x2E] = 0x46F,	/* U+046E: CYRILLIC CAPITAL LETTER KSI */
	[960+0x30] = 0x471,	/* U+0470: CYRILLIC CAPITAL LETTER PSI */
	[960+0x32] = 0x473,	/* U+0472: CYRILLIC CAPITAL LETTER FITA */
	[960+0x34] = 0x475,	/* U+0474: CYRILLIC CAPITAL LETTER IZHITSA */
	[960+0x36] = 0x477,	/* U+0476: CYRILLIC CAPITAL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT */
	[960+0x38] = 0x479,	/* U+0478: CYRILLIC CAPITAL LETTER UK */
	[960+0x3A] = 0x47B,	/* U+047A: CYRILLIC CAPITAL LETTER ROUND OMEGA */
	[960+0x3C] = 0x47D,	/* U+047C: CYRILLIC CAPITAL LETTER OMEGA WITH TITLO */
	[960+0x3E] = 0x47F,	/* U+047E: CYRILLIC CAPITAL LETTER OT */
	[0xD2] = 1024 - 0x80,	/* 322 ... */
	[1024+0x0] = 0x481,	/* U+0480: CYRILLIC CAPITAL LETTER KOPPA */
	[1024+0xA] = 0x48B,	/* U+048A: CYRILLIC CAPITAL LETTER SHORT I WITH TAIL */
	[1024+0xC] = 0x48D,	/* U+048C: CYRILLIC CAPITAL LETTER SEMISOFT SIGN */
	[1024+0xE] = 0x48F,	/* U+048E: CYRILLIC CAPITAL LETTER ER WITH TICK */
	[1024+0x10] = 0x491,	/* U+0490: CYRILLIC CAPITAL LETTER GHE WITH UPTURN */
	[1024+0x12] = 0x493,	/* U+0492: CYRILLIC CAPITAL LETTER GHE WITH STROKE */
	[1024+0x14] = 0x495,	/* U+0494: CYRILLIC CAPITAL LETTER GHE WITH MIDDLE HOOK */
	[1024+0x16] = 0x497,	/* U+0496: CYRILLIC CAPITAL LETTER ZHE WITH DESCENDER */
	[1024+0x18] = 0x499,	/* U+0498: CYRILLIC CAPITAL LETTER ZE WITH DESCENDER */
	[1024+0x1A] = 0x49B,	/* U+049A: CYRILLIC CAPITAL LETTER KA WITH DESCENDER */
	[1024+0x1C] = 0x49D,	/* U+049C: CYRILLIC CAPITAL LETTER KA WITH VERTICAL STROKE */
	[1024+0x1E] = 0x49F,	/* U+049E: CYRILLIC CAPITAL LETTER KA WITH STROKE */
	[1024+0x20] = 0x4A1,	/* U+04A0: CYRILLIC CAPITAL LETTER BASHKIR KA */
	[1024+0x22] = 0x4A3,	/* U+04A2: CYRILLIC CAPITAL LETTER EN WITH DESCENDER */
	[1024+0x24] = 0x4A5,	/* U+04A4: CYRILLIC CAPITAL LIGATURE EN GHE */
	[1024+0x26] = 0x4A7,	/* U+04A6: CYRILLIC CAPITAL LETTER PE WITH MIDDLE HOOK */
	[1024+0x28] = 0x4A9,	/* U+04A8: CYRILLIC CAPITAL LETTER ABKHASIAN HA */
	[1024+0x2A] = 0x4AB,	/* U+04AA: CYRILLIC CAPITAL LETTER ES WITH DESCENDER */
	[1024+0x2C] = 0x4AD,	/* U+04AC: CYRILLIC CAPITAL LETTER TE WITH DESCENDER */
	[1024+0x2E] = 0x4AF,	/* U+04AE: CYRILLIC CAPITAL LETTER STRAIGHT U */
	[1024+0x30] = 0x4B1,	/* U+04B0: CYRILLIC CAPITAL LETTER STRAIGHT U WITH STROKE */
	[1024+0x32] = 0x4B3,	/* U+04B2: CYRILLIC CAPITAL LETTER HA WITH DESCENDER */
	[1024+0x34] = 0x4B5,	/* U+04B4: CYRILLIC CAPITAL LIGATURE TE TSE */
	[1024+0x36] = 0x4B7,	/* U+04B6: CYRILLIC CAPITAL LETTER CHE WITH DESCENDER */
	[1024+0x38] = 0x4B9,	/* U+04B8: CYRILLIC CAPITAL LETTER CHE WITH VERTICAL STROKE */
	[1024+0x3A] = 0x4BB,	/* U+04BA: CYRILLIC CAPITAL LETTER SHHA */
	[1024+0x3C] = 0x4BD,	/* U+04BC: CYRILLIC CAPITAL LETTER ABKHASIAN CHE */
	[1024+0x3E] = 0x4BF,	/* U+04BE: CYRILLIC CAPITAL LETTER ABKHASIAN CHE WITH DESCENDER */
	[0xD3] = 1088 - 0x80,	/* 323 ... */
	[1088+0x0] = 0x4CF,	/* U+04C0: CYRILLIC LETTER PALOCHKA */
	[1088+0x1] = 0x4C2,	/* U+04C1: CYRILLIC CAPITAL LETTER ZHE WITH BREVE */
	[1088+0x3] = 0x4C4,	/* U+04C3: CYRILLIC CAPITAL LETTER KA WITH HOOK */
	[1088+0x5] = 0x4C6,	/* U+04C5: CYRILLIC CAPITAL LETTER EL WITH TAIL */
	[1088+0x7] = 0x4C8,	/* U+04C7: CYRILLIC CAPITAL LETTER EN WITH HOOK */
	[1088+0x9] = 0x4CA,	/* U+04C9: CYRILLIC CAPITAL LETTER EN WITH TAIL */
	[1088+0xB] = 0x4CC,	/* U+04CB: CYRILLIC CAPITAL LETTER KHAKASSIAN CHE */
	[1088+0xD] = 0x4CE,	/* U+04CD: CYRILLIC CAPITAL LETTER EM WITH TAIL */
	[1088+0x10] = 0x4D1,	/* U+04D0: CYRILLIC CAPITAL LETTER A WITH BREVE */
	[1088+0x12] = 0x4D3,	/* U+04D2: CYRILLIC CAPITAL LETTER A WITH DIAERESIS */
	[1088+0x14] = 0x4D5,	/* U+04D4: CYRILLIC CAPITAL LIGATURE A IE */
	[1088+0x16] = 0x4D7,	/* U+04D6: CYRILLIC CAPITAL LETTER IE WITH BREVE */
	[1088+0x18] = 0x4D9,	/* U+04D8: CYRILLIC CAPITAL LETTER SCHWA */
	[1088+0x1A] = 0x4DB,	/* U+04DA: CYRILLIC CAPITAL LETTER SCHWA WITH DIAERESIS */
	[1088+0x1C] = 0x4DD,	/* U+04DC: CYRILLIC CAPITAL LETTER ZHE WITH DIAERESIS */
	[1088+0x1E] = 0x4DF,	/* U+04DE: CYRILLIC CAPITAL LETTER ZE WITH DIAERESIS */
	[1088+0x20] = 0x4E1,	/* U+04E0: CYRILLIC CAPITAL LETTER ABKHASIAN DZE */
	[1088+0x22] = 0x4E3,	/* U+04E2: CYRILLIC CAPITAL LETTER I WITH MACRON */
	[1088+0x24] = 0x4E5,	/* U+04E4: CYRILLIC CAPITAL LETTER I WITH DIAERESIS */
	[1088+0x26] = 0x4E7,	/* U+04E6: CYRILLIC CAPITAL LETTER O WITH DIAERESIS */
	[1088+0x28] = 0x4E9,	/* U+04E8: CYRILLIC CAPITAL LETTER BARRED O */
	[1088+0x2A] = 0x4EB,	/* U+04EA: CYRILLIC CAPITAL LETTER BARRED O WITH DIAERESIS */
	[1088+0x2C] = 0x4ED,	/* U+04EC: CYRILLIC CAPITAL LETTER E WITH DIAERESIS */
	[1088+0x2E] = 0x4EF,	/* U+04EE: CYRILLIC CAPITAL LETTER U WITH MACRON */
	[1088+0x30] = 0x4F1,	/* U+04F0: CYRILLIC CAPITAL LETTER U WITH DIAERESIS */
	[1088+0x32] = 0x4F3,	/* U+04F2: CYRILLIC CAPITAL LETTER U WITH DOUBLE ACUTE */
	[1088+0x34] = 0x4F5,	/* U+04F4: CYRILLIC CAPITAL LETTER CHE WITH DIAERESIS */
	[1088+0x36] = 0x4F7,	/* U+04F6: CYRILLIC CAPITAL LETTER GHE WITH DESCENDER */
	[1088+0x38] = 0x4F9,	/* U+04F8: CYRILLIC CAPITAL LETTER YERU WITH DIAERESIS */
	[1088+0x3A] = 0x4FB,	/* U+04FA: CYRILLIC CAPITAL LETTER GHE WITH STROKE AND HOOK */
	[1088+0x3C] = 0x4FD,	/* U+04FC: CYRILLIC CAPITAL LETTER HA WITH HOOK */
	[1088+0x3E] = 0x4FF,	/* U+04FE: CYRILLIC CAPITAL LETTER HA WITH STROKE */
	[0xD4] = 1152 - 0x80,	/* 324 ... */
	[1152+0x0] = 0x501,	/* U+0500: CYRILLIC CAPITAL LETTER KOMI DE */
	[1152+0x2] = 0x503,	/* U+0502: CYRILLIC CAPITAL LETTER KOMI DJE */
	[1152+0x4] = 0x505,	/* U+0504: CYRILLIC CAPITAL LETTER KOMI ZJE */
	[1152+0x6] = 0x507,	/* U+0506: CYRILLIC CAPITAL LETTER KOMI DZJE */
	[1152+0x8] = 0x509,	/* U+0508: CYRILLIC CAPITAL LETTER KOMI LJE */
	[1152+0xA] = 0x50B,	/* U+050A: CYRILLIC CAPITAL LETTER KOMI NJE */
	[1152+0xC] = 0x50D,	/* U+050C: CYRILLIC CAPITAL LETTER KOMI SJE */
	[1152+0xE] = 0x50F,	/* U+050E: CYRILLIC CAPITAL LETTER KOMI TJE */
	[1152+0x10] = 0x511,	/* U+0510: CYRILLIC CAPITAL LETTER REVERSED ZE */
	[1152+0x12] = 0x513,	/* U+0512: CYRILLIC CAPITAL LETTER EL WITH HOOK */
	[1152+0x14] = 0x515,	/* U+0514: CYRILLIC CAPITAL LETTER LHA */
	[1152+0x16] = 0x517,	/* U+0516: CYRILLIC CAPITAL LETTER RHA */
	[1152+0x18] = 0x519,	/* U+0518: CYRILLIC CAPITAL LETTER YAE */
	[1152+0x1A] = 0x51B,	/* U+051A: CYRILLIC CAPITAL LETTER QA */
	[1152+0x1C] = 0x51D,	/* U+051C: CYRILLIC CAPITAL LETTER WE */
	[1152+0x1E] = 0x51F,	/* U+051E: CYRILLIC CAPITAL LETTER ALEUT KA */
	[1152+0x20] = 0x521,	/* U+0520: CYRILLIC CAPITAL LETTER EL WITH MIDDLE HOOK */
	[1152+0x22] = 0x523,	/* U+0522: CYRILLIC CAPITAL LETTER EN WITH MIDDLE HOOK */
	[1152+0x24] = 0x525,	/* U+0524: CYRILLIC CAPITAL LETTER PE WITH DESCENDER */
	[1152+0x26] = 0x527,	/* U+0526: CYRILLIC CAPITAL LETTER SHHA WITH DESCENDER */
	[1152+0x28] = 0x529,	/* U+0528: CYRILLIC CAPITAL LETTER EN WITH LEFT HOOK */
	[1152+0x2A] = 0x52B,	/* U+052A: CYRILLIC CAPITAL LETTER DZZHE */
	[1152+0x2C] = 0x52D,	/* U+052C: CYRILLIC CAPITAL LETTER DCHE */
	[1152+0x2E] = 0x52F,	/* U+052E: CYRILLIC CAPITAL LETTER EL WITH DESCENDER */
	[1152+0x31] = 0x561,	/* U+0531: ARMENIAN CAPITAL LETTER AYB */
	[1152+0x32] = 0x562,	/* U+0532: ARMENIAN CAPITAL LETTER BEN */
	[1152+0x33] = 0x563,	/* U+0533: ARMENIAN CAPITAL LETTER GIM */
	[1152+0x34] = 0x564,	/* U+0534: ARMENIAN CAPITAL LETTER DA */
	[1152+0x35] = 0x565,	/* U+0535: ARMENIAN CAPITAL LETTER ECH */
	[1152+0x36] = 0x566,	/* U+0536: ARMENIAN CAPITAL LETTER ZA */
	[1152+0x37] = 0x567,	/* U+0537: ARMENIAN CAPITAL LETTER EH */
	[1152+0x38] = 0x568,	/* U+0538: ARMENIAN CAPITAL LETTER ET */
	[1152+0x39] = 0x569,	/* U+0539: ARMENIAN CAPITAL LETTER TO */
	[1152+0x3A] = 0x56A,	/* U+053A: ARMENIAN CAPITAL LETTER ZHE */
	[1152+0x3B] = 0x56B,	/* U+053B: ARMENIAN CAPITAL LETTER INI */
	[1152+0x3C] = 0x56C,	/* U+053C: ARMENIAN CAPITAL LETTER LIWN */
	[1152+0x3D] = 0x56D,	/* U+053D: ARMENIAN CAPITAL LETTER XEH */
	[1152+0x3E] = 0x56E,	/* U+053E: ARMENIAN CAPITAL LETTER CA */
	[1152+0x3F] = 0x56F,	/* U+053F: ARMENIAN CAPITAL LETTER KEN */
	[0xD5] = 1216 - 0x80,	/* 325 ... */
	[1216+0x0] = 0x570,	/* U+0540: ARMENIAN CAPITAL LETTER HO */
	[1216+0x1] = 0x571,	/* U+0541: ARMENIAN CAPITAL LETTER JA */
	[1216+0x2] = 0x572,	/* U+0542: ARMENIAN CAPITAL LETTER GHAD */
	[1216+0x3] = 0x573,	/* U+0543: ARMENIAN CAPITAL LETTER CHEH */
	[1216+0x4] = 0x574,	/* U+0544: ARMENIAN CAPITAL LETTER MEN */
	[1216+0x5] = 0x575,	/* U+0545: ARMENIAN CAPITAL LETTER YI */
	[1216+0x6] = 0x576,	/* U+0546: ARMENIAN CAPITAL LETTER NOW */
	[1216+0x7] = 0x577,	/* U+0547: ARMENIAN CAPITAL LETTER SHA */
	[1216+0x8] = 0x578,	/* U+0548: ARMENIAN CAPITAL LETTER VO */
	[1216+0x9] = 0x579,	/* U+0549: ARMENIAN CAPITAL LETTER CHA */
	[1216+0xA] = 0x57A,	/* U+054A: ARMENIAN CAPITAL LETTER PEH */
	[1216+0xB] = 0x57B,	/* U+054B: ARMENIAN CAPITAL LETTER JHEH */
	[1216+0xC] = 0x57C,	/* U+054C: ARMENIAN CAPITAL LETTER RA */
	[1216+0xD] = 0x57D,	/* U+054D: ARMENIAN CAPITAL LETTER SEH */
	[1216+0xE] = 0x57E,	/* U+054E: ARMENIAN CAPITAL LETTER VEW */
	[1216+0xF] = 0x57F,	/* U+054F: ARMENIAN CAPITAL LETTER TIWN */
	[1216+0x10] = 0x580,	/* U+0550: ARMENIAN CAPITAL LETTER REH */
	[1216+0x11] = 0x581,	/* U+0551: ARMENIAN CAPITAL LETTER CO */
	[1216+0x12] = 0x582,	/* U+0552: ARMENIAN CAPITAL LETTER YIWN */
	[1216+0x13] = 0x583,	/* U+0553: ARMENIAN CAPITAL LETTER PIWR */
	[1216+0x14] = 0x584,	/* U+0554: ARMENIAN CAPITAL LETTER KEH */
	[1216+0x15] = 0x585,	/* U+0555: ARMENIAN CAPITAL LETTER OH */
	[1216+0x16] = 0x586,	/* U+0556: ARMENIAN CAPITAL LETTER FEH */
	[0xE1] = 1280 - 0x80,	/* 341 ... */
	[1280+0x2] = 1344 - 0x80,	/* 341 202 ... */
	[1344+0x20] = 0x2D00,	/* U+10A0: GEORGIAN CAPITAL LETTER AN */
	[1344+0x21] = 0x2D01,	/* U+10A1: GEORGIAN CAPITAL LETTER BAN */
	[1344+0x22] = 0x2D02,	/* U+10A2: GEORGIAN CAPITAL LETTER GAN */
	[1344+0x23] = 0x2D03,	/* U+10A3: GEORGIAN CAPITAL LETTER DON */
	[1344+0x24] = 0x2D04,	/* U+10A4: GEORGIAN CAPITAL LETTER EN */
	[1344+0x25] = 0x2D05,	/* U+10A5: GEORGIAN CAPITAL LETTER VIN */
	[1344+0x26] = 0x2D06,	/* U+10A6: GEORGIAN CAPITAL LETTER ZEN */
	[1344+0x27] = 0x2D07,	/* U+10A7: GEORGIAN CAPITAL LETTER TAN */
	[1344+0x28] = 0x2D08,	/* U+10A8: GEORGIAN CAPITAL LETTER IN */
	[1344+0x29] = 0x2D09,	/* U+10A9: GEORGIAN CAPITAL LETTER KAN */
	[1344+0x2A] = 0x2D0A,	/* U+10AA: GEORGIAN CAPITAL LETTER LAS */
	[1344+0x2B] = 0x2D0B,	/* U+10AB: GEORGIAN CAPITAL LETTER MAN */
	[1344+0x2C] = 0x2D0C,	/* U+10AC: GEORGIAN CAPITAL LETTER NAR */
	[1344+0x2D] = 0x2D0D,	/* U+10AD: GEORGIAN CAPITAL LETTER ON */
	[1344+0x2E] = 0x2D0E,	/* U+10AE: GEORGIAN CAPITAL LETTER PAR */
	[1344+0x2F] = 0x2D0F,	/* U+10AF: GEORGIAN CAPITAL LETTER ZHAR */
	[1344+0x30] = 0x2D10,	/* U+10B0: GEORGIAN CAPITAL LETTER RAE */
	[1344+0x31] = 0x2D11,	/* U+10B1: GEORGIAN CAPITAL LETTER SAN */
	[1344+0x32] = 0x2D12,	/* U+10B2: GEORGIAN CAPITAL LETTER TAR */
	[1344+0x33] = 0x2D13,	/* U+10B3: GEORGIAN CAPITAL LETTER UN */
	[1344+0x34] = 0x2D14,	/* U+10B4: GEORGIAN CAPITAL LETTER PHAR */
	[1344+0x35] = 0x2D15,	/* U+10B5: GEORGIAN CAPITAL LETTER KHAR */
	[1344+0x36] = 0x2D16,	/* U+10B6: GEORGIAN CAPITAL LETTER GHAN */
	[1344+0x37] = 0x2D17,	/* U+10B7: GEORGIAN CAPITAL LETTER QAR */
	[1344+0x38] = 0x2D18,	/* U+10B8: GEORGIAN CAPITAL LETTER SHIN */
	[1344+0x39] = 0x2D19,	/* U+10B9: GEORGIAN CAPITAL LETTER CHIN */
	[1344+0x3A] = 0x2D1A,	/* U+10BA: GEORGIAN CAPITAL LETTER CAN */
	[1344+0x3B] = 0x2D1B,	/* U+10BB: GEORGIAN CAPITAL LETTER JIL */
	[1344+0x3C] = 0x2D1C,	/* U+10BC: GEORGIAN CAPITAL LETTER CIL */
	[1344+0x3D] = 0x2D1D,	/* U+10BD: GEORGIAN CAPITAL LETTER CHAR */
	[1344+0x3E] = 0x2D1E,	/* U+10BE: GEORGIAN CAPITAL LETTER XAN */
	[1344+0x3F] = 0x2D1F,	/* U+10BF: GEORGIAN CAPITAL LETTER JHAN */
	[1280+0x3] = 1408 - 0x80,	/* 341 203 ... */
	[1408+0x0] = 0x2D20,	/* U+10C0: GEORGIAN CAPITAL LETTER HAE */
	[1408+0x1] = 0x2D21,	/* U+10C1: GEORGIAN CAPITAL LETTER HE */
	[1408+0x2] = 0x2D22,	/* U+10C2: GEORGIAN CAPITAL LETTER HIE */
	[1408+0x3] = 0x2D23,	/* U+10C3: GEORGIAN CAPITAL LETTER WE */
	[1408+0x4] = 0x2D24,	/* U+10C4: GEORGIAN CAPITAL LETTER HAR */
	[1408+0x5] = 0x2D25,	/* U+10C5: GEORGIAN CAPITAL LETTER HOE */
	[1408+0x7] = 0x2D27,	/* U+10C7: GEORGIAN CAPITAL LETTER YN */
	[1408+0xD] = 0x2D2D,	/* U+10CD: GEORGIAN CAPITAL LETTER AEN */
	[1280+0xE] = 1472 - 0x80,	/* 341 216 ... */
	[1472+0x20] = 0xAB70,	/* U+13A0: CHEROKEE LETTER A */
	[1472+0x21] = 0xAB71,	/* U+13A1: CHEROKEE LETTER E */
	[1472+0x22] = 0xAB72,	/* U+13A2: CHEROKEE LETTER I */
	[1472+0x23] = 0xAB73,	/* U+13A3: CHEROKEE LETTER O */
	[1472+0x24] = 0xAB74,	/* U+13A4: CHEROKEE LETTER U */
	[1472+0x25] = 0xAB75,	/* U+13A5: CHEROKEE LETTER V */
	[1472+0x26] = 0xAB76,	/* U+13A6: CHEROKEE LETTER GA */
	[1472+0x27] = 0xAB77,	/* U+13A7: CHEROKEE LETTER KA */
	[1472+0x28] = 0xAB78,	/* U+13A8: CHEROKEE LETTER GE */
	[1472+0x29] = 0xAB79,	/* U+13A9: CHEROKEE LETTER GI */
	[1472+0x2A] = 0xAB7A,	/* U+13AA: CHEROKEE LETTER GO */
	[1472+0x2B] = 0xAB7B,	/* U+13AB: CHEROKEE LETTER GU */
	[1472+0x2C] = 0xAB7C,	/* U+13AC: CHEROKEE LETTER GV */
	[1472+0x2D] = 0xAB7D,	/* U+13AD: CHEROKEE LETTER HA */
	[1472+0x2E] = 0xAB7E,	/* U+13AE: CHEROKEE LETTER HE */
	[1472+0x2F] = 0xAB7F,	/* U+13AF: CHEROKEE LETTER HI */
	[1472+0x30] = 0xAB80,	/* U+13B0: CHEROKEE LETTER HO */
	[1472+0x31] = 0xAB81,	/* U+13B1: CHEROKEE LETTER HU */
	[1472+0x32] = 0xAB82,	/* U+13B2: CHEROKEE LETTER HV */
	[1472+0x33] = 0xAB83,	/* U+13B3: CHEROKEE LETTER LA */
	[1472+0x34] = 0xAB84,	/* U+13B4: CHEROKEE LETTER LE */
	[1472+0x35] = 0xAB85,	/* U+13B5: CHEROKEE LETTER LI */
	[1472+0x36] = 0xAB86,	/* U+13B6: CHEROKEE LETTER LO */
	[1472+0x37] = 0xAB87,	/* U+13B7: CHEROKEE LETTER LU */
	[1472+0x38] = 0xAB88,	/* U+13B8: CHEROKEE LETTER LV */
	[1472+0x39] = 0xAB89,	/* U+13B9: CHEROKEE LETTER MA */
	[1472+0x3A] = 0xAB8A,	/* U+13BA: CHEROKEE LETTER ME */
	[1472+0x3B] = 0xAB8B,	/* U+13BB: CHEROKEE LETTER MI */
	[1472+0x3C] = 0xAB8C,	/* U+13BC: CHEROKEE LETTER MO */
	[1472+0x3D] = 0xAB8D,	/* U+13BD: CHEROKEE LETTER MU */
	[1472+0x3E] = 0xAB8E,	/* U+13BE: CHEROKEE LETTER NA */
	[1472+0x3F] = 0xAB8F,	/* U+13BF: CHEROKEE LETTER HNA */
	[1280+0xF] = 1536 - 0x80,	/* 341 217 ... */
	[1536+0x0] = 0xAB90,	/* U+13C0: CHEROKEE LETTER NAH */
	[1536+0x1] = 0xAB91,	/* U+13C1: CHEROKEE LETTER NE */
	[1536+0x2] = 0xAB92,	/* U+13C2: CHEROKEE LETTER NI */
	[1536+0x3] = 0xAB93,	/* U+13C3: CHEROKEE LETTER NO */
	[1536+0x4] = 0xAB94,	/* U+13C4: CHEROKEE LETTER NU */
	[1536+0x5] = 0xAB95,	/* U+13C5: CHEROKEE LETTER NV */
	[1536+0x6] = 0xAB96,	/* U+13C6: CHEROKEE LETTER QUA */
	[1536+0x7] = 0xAB97,	/* U+13C7: CHEROKEE LETTER QUE */
	[1536+0x8] = 0xAB98,	/* U+13C8: CHEROKEE LETTER QUI */
	[1536+0x9] = 0xAB99,	/* U+13C9: CHEROKEE LETTER QUO */
	[1536+0xA] = 0xAB9A,	/* U+13CA: CHEROKEE LETTER QUU */
	[1536+0xB] = 0xAB9B,	/* U+13CB: CHEROKEE LETTER QUV */
	[1536+0xC] = 0xAB9C,	/* U+13CC: CHEROKEE LETTER SA */
	[1536+0xD] = 0xAB9D,	/* U+13CD: CHEROKEE LETTER S */
	[1536+0xE] = 0xAB9E,	/* U+13CE: CHEROKEE LETTER SE */
	[1536+0xF] = 0xAB9F,	/* U+13CF: CHEROKEE LETTER SI */
	[1536+0x10] = 0xABA0,	/* U+13D0: CHEROKEE LETTER SO */
	[1536+0x11] = 0xABA1,	/* U+13D1: CHEROKEE LETTER SU */
	[1536+0x12] = 0xABA2,	/* U+13D2: CHEROKEE LETTER SV */
	[1536+0x13] = 0xABA3,	/* U+13D3: CHEROKEE LETTER DA */
	[1536+0x14] = 0xABA4,	/* U+13D4: CHEROKEE LETTER TA */
	[1536+0x15] = 0xABA5,	/* U+13D5: CHEROKEE LETTER DE */
	[1536+0x16] = 0xABA6,	/* U+13D6: CHEROKEE LETTER TE */
	[1536+0x17] = 0xABA7,	/* U+13D7: CHEROKEE LETTER DI */
	[1536+0x18] = 0xABA8,	/* U+13D8: CHEROKEE LETTER TI */
	[1536+0x19] = 0xABA9,	/* U+13D9: CHEROKEE LETTER DO */
	[1536+0x1A] = 0xABAA,	/* U+13DA: CHEROKEE LETTER DU */
	[1536+0x1B] = 0xABAB,	/* U+13DB: CHEROKEE LETTER DV */
	[1536+0x1C] = 0xABAC,	/* U+13DC: CHEROKEE LETTER DLA */
	[1536+0x1D] = 0xABAD,	/* U+13DD: CHEROKEE LETTER TLA */
	[1536+0x1E] = 0xABAE,	/* U+13DE: CHEROKEE LETTER TLE */
	[1536+0x1F] = 0xABAF,	/* U+13DF: CHEROKEE LETTER TLI */
	[1536+0x20] = 0xABB0,	/* U+13E0: CHEROKEE LETTER TLO */
	[1536+0x21] = 0xABB1,	/* U+13E1: CHEROKEE LETTER TLU */
	[1536+0x22] = 0xABB2,	/* U+13E2: CHEROKEE LETTER TLV */
	[1536+0x23] = 0xABB3,	/* U+13E3: CHEROKEE LETTER TSA */
	[1536+0x24] = 0xABB4,	/* U+13E4: CHEROKEE LETTER TSE */
	[1536+0x25] = 0xABB5,	/* U+13E5: CHEROKEE LETTER TSI */
	[1536+0x26] = 0xABB6,	/* U+13E6: CHEROKEE LETTER TSO */
	[1536+0x27] = 0xABB7,	/* U+13E7: CHEROKEE LETTER TSU */
	[1536+0x28] = 0xABB8,	/* U+13E8: CHEROKEE LETTER TSV */
	[1536+0x29] = 0xABB9,	/* U+13E9: CHEROKEE LETTER WA */
	[1536+0x2A] = 0xABBA,	/* U+13EA: CHEROKEE LETTER WE */
	[1536+0x2B] = 0xABBB,	/* U+13EB: CHEROKEE LETTER WI */
	[1536+0x2C] = 0xABBC,	/* U+13EC: CHEROKEE LETTER WO */
	[1536+0x2D] = 0xABBD,	/* U+13ED: CHEROKEE LETTER WU */
	[1536+0x2E] = 0xABBE,	/* U+13EE: CHEROKEE LETTER WV */
	[1536+0x2F] = 0xABBF,	/* U+13EF: CHEROKEE LETTER YA */
	[1536+0x30] = 0x13F8,	/* U+13F0: CHEROKEE LETTER YE */
	[1536+0x31] = 0x13F9,	/* U+13F1: CHEROKEE LETTER YI */
	[1536+0x32] = 0x13FA,	/* U+13F2: CHEROKEE LETTER YO */
	[1536+0x33] = 0x13FB,	/* U+13F3: CHEROKEE LETTER YU */
	[1536+0x34] = 0x13FC,	/* U+13F4: CHEROKEE LETTER YV */
	[1536+0x35] = 0x13FD,	/* U+13F5: CHEROKEE LETTER MV */
	[1280+0x32] = 1600 - 0x80,	/* 341 262 ... */
	[1600+0x10] = 0x10D0,	/* U+1C90: GEORGIAN MTAVRULI CAPITAL LETTER AN */
	[1600+0x11] = 0x10D1,	/* U+1C91: GEORGIAN MTAVRULI CAPITAL LETTER BAN */
	[1600+0x12] = 0x10D2,	/* U+1C92: GEORGIAN MTAVRULI CAPITAL LETTER GAN */
	[1600+0x13] = 0x10D3,	/* U+1C93: GEORGIAN MTAVRULI CAPITAL LETTER DON */
	[1600+0x14] = 0x10D4,	/* U+1C94: GEORGIAN MTAVRULI CAPITAL LETTER EN */
	[1600+0x15] = 0x10D5,	/* U+1C95: GEORGIAN MTAVRULI CAPITAL LETTER VIN */
	[1600+0x16] = 0x10D6,	/* U+1C96: GEORGIAN MTAVRULI CAPITAL LETTER ZEN */
	[1600+0x17] = 0x10D7,	/* U+1C97: GEORGIAN MTAVRULI CAPITAL LETTER TAN */
	[1600+0x18] = 0x10D8,	/* U+1C98: GEORGIAN MTAVRULI CAPITAL LETTER IN */
	[1600+0x19] = 0x10D9,	/* U+1C99: GEORGIAN MTAVRULI CAPITAL LETTER KAN */
	[1600+0x1A] = 0x10DA,	/* U+1C9A: GEORGIAN MTAVRULI CAPITAL LETTER LAS */
	[1600+0x1B] = 0x10DB,	/* U+1C9B: GEORGIAN MTAVRULI CAPITAL LETTER MAN */
	[1600+0x1C] = 0x10DC,	/* U+1C9C: GEORGIAN MTAVRULI CAPITAL LETTER NAR */
	[1600+0x1D] = 0x10DD,	/* U+1C9D: GEORGIAN MTAVRULI CAPITAL LETTER ON */
	[1600+0x1E] = 0x10DE,	/* U+1C9E: GEORGIAN MTAVRULI CAPITAL LETTER PAR */
	[1600+0x1F] = 0x10DF,	/* U+1C9F: GEORGIAN MTAVRULI CAPITAL LETTER ZHAR */
	[1600+0x20] = 0x10E0,	/* U+1CA0: GEORGIAN MTAVRULI CAPITAL LETTER RAE */
	[1600+0x21] = 0x10E1,	/* U+1CA1: GEORGIAN MTAVRULI CAPITAL LETTER SAN */
	[1600+0x22] = 0x10E2,	/* U+1CA2: GEORGIAN MTAVRULI CAPITAL LETTER TAR */
	[1600+0x23] = 0x10E3,	/* U+1CA3: GEORGIAN MTAVRULI CAPITAL LETTER UN */
	[1600+0x24] = 0x10E4,	/* U+1CA4: GEORGIAN MTAVRULI CAPITAL LETTER PHAR */
	[1600+0x25] = 0x10E5,	/* U+1CA5: GEORGIAN MTAVRULI CAPITAL LETTER KHAR */
	[1600+0x26] = 0x10E6,	/* U+1CA6: GEORGIAN MTAVRULI CAPITAL LETTER GHAN */
	[1600+0x27] = 0x10E7,	/* U+1CA7: GEORGIAN MTAVRULI CAPITAL LETTER QAR */
	[1600+0x28] = 0x10E8,	/* U+1CA8: GEORGIAN MTAVRULI CAPITAL LETTER SHIN */
	[1600+0x29] = 0x10E9,	/* U+1CA9: GEORGIAN MTAVRULI CAPITAL LETTER CHIN */
	[1600+0x2A] = 0x10EA,	/* U+1CAA: GEORGIAN MTAVRULI CAPITAL LETTER CAN */
	[1600+0x2B] = 0x10EB,	/* U+1CAB: GEORGIAN MTAVRULI CAPITAL LETTER JIL */
	[1600+0x2C] = 0x10EC,	/* U+1CAC: GEORGIAN MTAVRULI CAPITAL LETTER CIL */
	[1600+0x2D] = 0x10ED,	/* U+1CAD: GEORGIAN MTAVRULI CAPITAL LETTER CHAR */
	[1600+0x2E] = 0x10EE,	/* U+1CAE: GEORGIAN MTAVRULI CAPITAL LETTER XAN */
	[1600+0x2F] = 0x10EF,	/* U+1CAF: GEORGIAN MTAVRULI CAPITAL LETTER JHAN */
	[1600+0x30] = 0x10F0,	/* U+1CB0: GEORGIAN MTAVRULI CAPITAL LETTER HAE */
	[1600+0x31] = 0x10F1,	/* U+1CB1: GEORGIAN MTAVRULI CAPITAL LETTER HE */
	[1600+0x32] = 0x10F2,	/* U+1CB2: GEORGIAN MTAVRULI CAPITAL LETTER HIE */
	[1600+0x33] = 0x10F3,	/* U+1CB3: GEORGIAN MTAVRULI CAPITAL LETTER WE */
	[1600+0x34] = 0x10F4,	/* U+1CB4: GEORGIAN MTAVRULI CAPITAL LETTER HAR */
	[1600+0x35] = 0x10F5,	/* U+1CB5: GEORGIAN MTAVRULI CAPITAL LETTER HOE */
	[1600+0x36] = 0x10F6,	/* U+1CB6: GEORGIAN MTAVRULI CAPITAL LETTER FI */
	[1600+0x37] = 0x10F7,	/* U+1CB7: GEORGIAN MTAVRULI CAPITAL LETTER YN */
	[1600+0x38] = 0x10F8,	/* U+1CB8: GEORGIAN MTAVRULI CAPITAL LETTER ELIFI */
	[1600+0x39] = 0x10F9,	/* U+1CB9: GEORGIAN MTAVRULI CAPITAL LETTER TURNED GAN */
	[1600+0x3A] = 0x10FA,	/* U+1CBA: GEORGIAN MTAVRULI CAPITAL LETTER AIN */
	[1600+0x3D] = 0x10FD,	/* U+1CBD: GEORGIAN MTAVRULI CAPITAL LETTER AEN */
	[1600+0x3E] = 0x10FE,	/* U+1CBE: GEORGIAN MTAVRULI CAPITAL LETTER HARD SIGN */
	[1600+0x3F] = 0x10FF,	/* U+1CBF: GEORGIAN MTAVRULI CAPITAL LETTER LABIAL SIGN */
	[1280+0x38] = 1664 - 0x80,	/* 341 270 ... */
	[1664+0x0] = 0x1E01,	/* U+1E00: LATIN CAPITAL LETTER A WITH RING BELOW */
	[1664+0x2] = 0x1E03,	/* U+1E02: LATIN CAPITAL LETTER B WITH DOT ABOVE */
	[1664+0x4] = 0x1E05,	/* U+1E04: LATIN CAPITAL LETTER B WITH DOT BELOW */
	[1664+0x6] = 0x1E07,	/* U+1E06: LATIN CAPITAL LETTER B WITH LINE BELOW */
	[1664+0x8] = 0x1E09,	/* U+1E08: LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE */
	[1664+0xA] = 0x1E0B,	/* U+1E0A: LATIN CAPITAL LETTER D WITH DOT ABOVE */
	[1664+0xC] = 0x1E0D,	/* U+1E0C: LATIN CAPITAL LETTER D WITH DOT BELOW */
	[1664+0xE] = 0x1E0F,	/* U+1E0E: LATIN CAPITAL LETTER D WITH LINE BELOW */
	[1664+0x10] = 0x1E11,	/* U+1E10: LATIN CAPITAL LETTER D WITH CEDILLA */
	[1664+0x12] = 0x1E13,	/* U+1E12: LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW */
	[1664+0x14] = 0x1E15,	/* U+1E14: LATIN CAPITAL LETTER E WITH MACRON AND GRAVE */
	[1664+0x16] = 0x1E17,	/* U+1E16: LATIN CAPITAL LETTER E WITH MACRON AND ACUTE */
	[1664+0x18] = 0x1E19,	/* U+1E18: LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW */
	[1664+0x1A] = 0x1E1B,	/* U+1E1A: LATIN CAPITAL LETTER E WITH TILDE BELOW */
	[1664+0x1C] = 0x1E1D,	/* U+1E1C: LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE */
	[1664+0x1E] = 0x1E1F,	/* U+1E1E: LATIN CAPITAL LETTER F WITH DOT ABOVE */
	[1664+0x20] = 0x1E21,	/* U+1E20: LATIN CAPITAL LETTER G WITH MACRON */
	[1664+0x22] = 0x1E23,	/* U+1E22: LATIN CAPITAL LETTER H WITH DOT ABOVE */
	[1664+0x24] = 0x1E25,	/* U+1E24: LATIN CAPITAL LETTER H WITH DOT BELOW */
	[1664+0x26] = 0x1E27,	/* U+1E26: LATIN CAPITAL LETTER H WITH DIAERESIS */
	[1664+0x28] = 0x1E29,	/* U+1E28: LATIN CAPITAL LETTER H WITH CEDILLA */
	[1664+0x2A] = 0x1E2B,	/* U+1E2A: LATIN CAPITAL LETTER H WITH BREVE BELOW */
	[1664+0x2C] = 0x1E2D,	/* U+1E2C: LATIN CAPITAL LETTER I WITH TILDE BELOW */
	[1664+0x2E] = 0x1E2F,	/* U+1E2E: LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE */
	[1664+0x30] = 0x1E31,	/* U+1E30: LATIN CAPITAL LETTER K WITH ACUTE */
	[1664+0x32] = 0x1E33,	/* U+1E32: LATIN CAPITAL LETTER K WITH DOT BELOW */
	[1664+0x34] = 0x1E35,	/* U+1E34: LATIN CAPITAL LETTER K WITH LINE BELOW */
	[1664+0x36] = 0x1E37,	/* U+1E36: LATIN CAPITAL LETTER L WITH DOT BELOW */
	[1664+0x38] = 0x1E39,	/* U+1E38: LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON */
	[1664+0x3A] = 0x1E3B,	/* U+1E3A: LATIN CAPITAL LETTER L WITH LINE BELOW */
	[1664+0x3C] = 0x1E3D,	/* U+1E3C: LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW */
	[1664+0x3E] = 0x1E3F,	/* U+1E3E: LATIN CAPITAL LETTER M WITH ACUTE */
	[1280+0x39] = 1728 - 0x80,	/* 341 271 ... */
	[1728+0x0] = 0x1E41,	/* U+1E40: LATIN CAPITAL LETTER M WITH DOT ABOVE */
	[1728+0x2] = 0x1E43,	/* U+1E42: LATIN CAPITAL LETTER M WITH DOT BELOW */
	[1728+0x4] = 0x1E45,	/* U+1E44: LATIN CAPITAL LETTER N WITH DOT ABOVE */
	[1728+0x6] = 0x1E47,	/* U+1E46: LATIN CAPITAL LETTER N WITH DOT BELOW */
	[1728+0x8] = 0x1E49,	/* U+1E48: LATIN CAPITAL LETTER N WITH LINE BELOW */
	[1728+0xA] = 0x1E4B,	/* U+1E4A: LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW */
	[1728+0xC] = 0x1E4D,	/* U+1E4C: LATIN CAPITAL LETTER O WITH TILDE AND ACUTE */
	[1728+0xE] = 0x1E4F,	/* U+1E4E: LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS */
	[1728+0x10] = 0x1E51,	/* U+1E50: LATIN CAPITAL LETTER O WITH MACRON AND GRAVE */
	[1728+0x12] = 0x1E53,	/* U+1E52: LATIN CAPITAL LETTER O WITH MACRON AND ACUTE */
	[1728+0x14] = 0x1E55,	/* U+1E54: LATIN CAPITAL LETTER P WITH ACUTE */
	[1728+0x16] = 0x1E57,	/* U+1E56: LATIN CAPITAL LETTER P WITH DOT ABOVE */
	[1728+0x18] = 0x1E59,	/* U+1E58: LATIN CAPITAL LETTER R WITH DOT ABOVE */
	[1728+0x1A] = 0x1E5B,	/* U+1E5A: LATIN CAPITAL LETTER R WITH DOT BELOW */
	[1728+0x1C] = 0x1E5D,	/* U+1E5C: LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON */
	[1728+0x1E] = 0x1E5F,	/* U+1E5E: LATIN CAPITAL LETTER R WITH LINE BELOW */
	[1728+0x20] = 0x1E61,	/* U+1E60: LATIN CAPITAL LETTER S WITH DOT ABOVE */
	[1728+0x22] = 0x1E63,	/* U+1E62: LATIN CAPITAL LETTER S WITH DOT BELOW */
	[1728+0x24] = 0x1E65,	/* U+1E64: LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE */
	[1728+0x26] = 0x1E67,	/* U+1E66: LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE */
	[1728+0x28] = 0x1E69,	/* U+1E68: LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE */
	[1728+0x2A] = 0x1E6B,	/* U+1E6A: LATIN CAPITAL LETTER T WITH DOT ABOVE */
	[1728+0x2C] = 0x1E6D,	/* U+1E6C: LATIN CAPITAL LETTER T WITH DOT BELOW */
	[1728+0x2E] = 0x1E6F,	/* U+1E6E: LATIN CAPITAL LETTER T WITH LINE BELOW */
	[1728+0x30] = 0x1E71,	/* U+1E70: LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW */
	[1728+0x32] = 0x1E73,	/* U+1E72: LATIN CAPITAL LETTER U WITH DIAERESIS BELOW */
	[1728+0x34] = 0x1E75,	/* U+1E74: LATIN CAPITAL LETTER U WITH TILDE BELOW */
	[1728+0x36] = 0x1E77,	/* U+1E76: LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW */
	[1728+0x38] = 0x1E79,	/* U+1E78: LATIN CAPITAL LETTER U WITH TILDE AND ACUTE */
	[1728+0x3A] = 0x1E7B,	/* U+1E7A: LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS */
	[1728+0x3C] = 0x1E7D,	/* U+1E7C: LATIN CAPITAL LETTER V WITH TILDE */
	[1728+0x3E] = 0x1E7F,	/* U+1E7E: LATIN CAPITAL LETTER V WITH DOT BELOW */
	[1280+0x3A] = 1792 - 0x80,	/* 341 272 ... */
	[1792+0x0] = 0x1E81,	/* U+1E80: LATIN CAPITAL LETTER W WITH GRAVE */
	[1792+0x2] = 0x1E83,	/* U+1E82: LATIN CAPITAL LETTER W WITH ACUTE */
	[1792+0x4] = 0x1E85,	/* U+1E84: LATIN CAPITAL LETTER W WITH DIAERESIS */
	[1792+0x6] = 0x1E87,	/* U+1E86: LATIN CAPITAL LETTER W WITH DOT ABOVE */
	[1792+0x8] = 0x1E89,	/* U+1E88: LATIN CAPITAL LETTER W WITH DOT BELOW */
	[1792+0xA] = 0x1E8B,	/* U+1E8A: LATIN CAPITAL LETTER X WITH DOT ABOVE */
	[1792+0xC] = 0x1E8D,	/* U+1E8C: LATIN CAPITAL LETTER X WITH DIAERESIS */
	[1792+0xE] = 0x1E8F,	/* U+1E8E: LATIN CAPITAL LETTER Y WITH DOT ABOVE */
	[1792+0x10] = 0x1E91,	/* U+1E90: LATIN CAPITAL LETTER Z WITH CIRCUMFLEX */
	[1792+0x12] = 0x1E93,	/* U+1E92: LATIN CAPITAL LETTER Z WITH DOT BELOW */
	[1792+0x14] = 0x1E95,	/* U+1E94: LATIN CAPITAL LETTER Z WITH LINE BELOW */
	[1792+0x1E] = 0xDF,	/* U+1E9E: LATIN CAPITAL LETTER SHARP S */
	[1792+0x20] = 0x1EA1,	/* U+1EA0: LATIN CAPITAL LETTER A WITH DOT BELOW */
	[1792+0x22] = 0x1EA3,	/* U+1EA2: LATIN CAPITAL LETTER A WITH HOOK ABOVE */
	[1792+0x24] = 0x1EA5,	/* U+1EA4: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE */
	[1792+0x26] = 0x1EA7,	/* U+1EA6: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE */
	[1792+0x28] = 0x1EA9,	/* U+1EA8: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE */
	[1792+0x2A] = 0x1EAB,	/* U+1EAA: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE */
	[1792+0x2C] = 0x1EAD,	/* U+1EAC: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW */
	[1792+0x2E] = 0x1EAF,	/* U+1EAE: LATIN CAPITAL LETTER A WITH BREVE AND ACUTE */
	[1792+0x30] = 0x1EB1,	/* U+1EB0: LATIN CAPITAL LETTER A WITH BREVE AND GRAVE */
	[1792+0x32] = 0x1EB3,	/* U+1EB2: LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE */
	[1792+0x34] = 0x1EB5,	/* U+1EB4: LATIN CAPITAL LETTER A WITH BREVE AND TILDE */
	[1792+0x36] = 0x1EB7,	/* U+1EB6: LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW */
	[1792+0x38] = 0x1EB9,	/* U+1EB8: LATIN CAPITAL LETTER E WITH DOT BELOW */
	[1792+0x3A] = 0x1EBB,	/* U+1EBA: LATIN CAPITAL LETTER E WITH HOOK ABOVE */
	[1792+0x3C] = 0x1EBD,	/* U+1EBC: LATIN CAPITAL LETTER E WITH TILDE */
	[1792+0x3E] = 0x1EBF,	/* U+1EBE: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE */
	[1280+0x3B] = 1856 - 0x80,	/* 341 273 ... */
	[1856+0x0] = 0x1EC1,	/* U+1EC0: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[1856+0x2] = 0x1EC3,	/* U+1EC2: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[1856+0x4] = 0x1EC5,	/* U+1EC4: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE */
	[1856+0x6] = 0x1EC7,	/* U+1EC6: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[1856+0x8] = 0x1EC9,	/* U+1EC8: LATIN CAPITAL LETTER I WITH HOOK ABOVE */
	[1856+0xA] = 0x1ECB,	/* U+1ECA: LATIN CAPITAL LETTER I WITH DOT BELOW */
	[1856+0xC] = 0x1ECD,	/* U+1ECC: LATIN CAPITAL LETTER O WITH DOT BELOW */
	[1856+0xE] = 0x1ECF,	/* U+1ECE: LATIN CAPITAL LETTER O WITH HOOK ABOVE */
	[1856+0x10] = 0x1ED1,	/* U+1ED0: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE */
	[1856+0x12] = 0x1ED3,	/* U+1ED2: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE */
	[1856+0x14] = 0x1ED5,	/* U+1ED4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE */
	[1856+0x16] = 0x1ED7,	/* U+1ED6: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE */
	[1856+0x18] = 0x1ED9,	/* U+1ED8: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW */
	[1856+0x1A] = 0x1EDB,	/* U+1EDA: LATIN CAPITAL LETTER O WITH HORN AND ACUTE */
	[1856+0x1C] = 0x1EDD,	/* U+1EDC: LATIN CAPITAL LETTER O WITH HORN AND GRAVE */
	[1856+0x1E] = 0x1EDF,	/* U+1EDE: LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE */
	[1856+0x20] = 0x1EE1,	/* U+1EE0: LATIN CAPITAL LETTER O WITH HORN AND TILDE */
	[1856+0x22] = 0x1EE3,	/* U+1EE2: LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW */
	[1856+0x24] = 0x1EE5,	/* U+1EE4: LATIN CAPITAL LETTER U WITH DOT BELOW */
	[1856+0x26] = 0x1EE7,	/* U+1EE6: LATIN CAPITAL LETTER U WITH HOOK ABOVE */
	[1856+0x28] = 0x1EE9,	/* U+1EE8: LATIN CAPITAL LETTER U WITH HORN AND ACUTE */
	[1856+0x2A] = 0x1EEB,	/* U+1EEA: LATIN CAPITAL LETTER U WITH HORN AND GRAVE */
	[1856+0x2C] = 0x1EED,	/* U+1EEC: LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE */
	[1856+0x2E] = 0x1EEF,	/* U+1EEE: LATIN CAPITAL LETTER U WITH HORN AND TILDE */
	[1856+0x30] = 0x1EF1,	/* U+1EF0: LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW */
	[1856+0x32] = 0x1EF3,	/* U+1EF2: LATIN CAPITAL LETTER Y WITH GRAVE */
	[1856+0x34] = 0x1EF5,	/* U+1EF4: LATIN CAPITAL LETTER Y WITH DOT BELOW */
	[1856+0x36] = 0x1EF7,	/* U+1EF6: LATIN CAPITAL LETTER Y WITH HOOK ABOVE */
	[1856+0x38] = 0x1EF9,	/* U+1EF8: LATIN CAPITAL LETTER Y WITH TILDE */
	[1856+0x3A] = 0x1EFB,	/* U+1EFA: LATIN CAPITAL LETTER MIDDLE-WELSH LL */
	[1856+0x3C] = 0x1EFD,	/* U+1EFC: LATIN CAPITAL LETTER MIDDLE-WELSH V */
	[1856+0x3E] = 0x1EFF,	/* U+1EFE: LATIN CAPITAL LETTER Y WITH LOOP */
	[1280+0x3C] = 1920 - 0x80,	/* 341 274 ... */
	[1920+0x8] = 0x1F00,	/* U+1F08: GREEK CAPITAL LETTER ALPHA WITH PSILI */
	[1920+0x9] = 0x1F01,	/* U+1F09: GREEK CAPITAL LETTER ALPHA WITH DASIA */
	[1920+0xA] = 0x1F02,	/* U+1F0A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA */
	[1920+0xB] = 0x1F03,	/* U+1F0B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA */
	[1920+0xC] = 0x1F04,	/* U+1F0C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA */
	[1920+0xD] = 0x1F05,	/* U+1F0D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA */
	[1920+0xE] = 0x1F06,	/* U+1F0E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI */
	[1920+0xF] = 0x1F07,	/* U+1F0F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI */
	[1920+0x18] = 0x1F10,	/* U+1F18: GREEK CAPITAL LETTER EPSILON WITH PSILI */
	[1920+0x19] = 0x1F11,	/* U+1F19: GREEK CAPITAL LETTER EPSILON WITH DASIA */
	[1920+0x1A] = 0x1F12,	/* U+1F1A: GREEK CAPITAL LETTER EPSILON WITH PSILI AND VARIA */
	[1920+0x1B] = 0x1F13,	/* U+1F1B: GREEK CAPITAL LETTER EPSILON WITH DASIA AND VARIA */
	[1920+0x1C] = 0x1F14,	/* U+1F1C: GREEK CAPITAL LETTER EPSILON WITH PSILI AND OXIA */
	[1920+0x1D] = 0x1F15,	/* U+1F1D: GREEK CAPITAL LETTER EPSILON WITH DASIA AND OXIA */
	[1920+0x28] = 0x1F20,	/* U+1F28: GREEK CAPITAL LETTER ETA WITH PSILI */
	[1920+0x29] = 0x1F21,	/* U+1F29: GREEK CAPITAL LETTER ETA WITH DASIA */
	[1920+0x2A] = 0x1F22,	/* U+1F2A: GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA */
	[1920+0x2B] = 0x1F23,	/* U+1F2B: GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA */
	[1920+0x2C] = 0x1F24,	/* U+1F2C: GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA */
	[1920+0x2D] = 0x1F25,	/* U+1F2D: GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA */
	[1920+0x2E] = 0x1F26,	/* U+1F2E: GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI */
	[1920+0x2F] = 0x1F27,	/* U+1F2F: GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI */
	[1920+0x38] = 0x1F30,	/* U+1F38: GREEK CAPITAL LETTER IOTA WITH PSILI */
	[1920+0x39] = 0x1F31,	/* U+1F39: GREEK CAPITAL LETTER IOTA WITH DASIA */
	[1920+0x3A] = 0x1F32,	/* U+1F3A: GREEK CAPITAL LETTER IOTA WITH PSILI AND VARIA */
	[1920+0x3B] = 0x1F33,	/* U+1F3B: GREEK CAPITAL LETTER IOTA WITH DASIA AND VARIA */
	[1920+0x3C] = 0x1F34,	/* U+1F3C: GREEK CAPITAL LETTER IOTA WITH PSILI AND OXIA */
	[1920+0x3D] = 0x1F35,	/* U+1F3D: GREEK CAPITAL LETTER IOTA WITH DASIA AND OXIA */
	[1920+0x3E] = 0x1F36,	/* U+1F3E: GREEK CAPITAL LETTER IOTA WITH PSILI AND PERISPOMENI */
	[1920+0x3F] = 0x1F37,	/* U+1F3F: GREEK CAPITAL LETTER IOTA WITH DASIA AND PERISPOMENI */
	[1280+0x3D] = 1984 - 0x80,	/* 341 275 ... */
	[1984+0x8] = 0x1F40,	/* U+1F48: GREEK CAPITAL LETTER OMICRON WITH PSILI */
	[1984+0x9] = 0x1F41,	/* U+1F49: GREEK CAPITAL LETTER OMICRON WITH DASIA */
	[1984+0xA] = 0x1F42,	/* U+1F4A: GREEK CAPITAL LETTER OMICRON WITH PSILI AND VARIA */
	[1984+0xB] = 0x1F43,	/* U+1F4B: GREEK CAPITAL LETTER OMICRON WITH DASIA AND VARIA */
	[1984+0xC] = 0x1F44,	/* U+1F4C: GREEK CAPITAL LETTER OMICRON WITH PSILI AND OXIA */
	[1984+0xD] = 0x1F45,	/* U+1F4D: GREEK CAPITAL LETTER OMICRON WITH DASIA AND OXIA */
	[1984+0x19] = 0x1F51,	/* U+1F59: GREEK CAPITAL LETTER UPSILON WITH DASIA */
	[1984+0x1B] = 0x1F53,	/* U+1F5B: GREEK CAPITAL LETTER UPSILON WITH DASIA AND VARIA */
	[1984+0x1D] = 0x1F55,	/* U+1F5D: GREEK CAPITAL LETTER UPSILON WITH DASIA AND OXIA */
	[1984+0x1F] = 0x1F57,	/* U+1F5F: GREEK CAPITAL LETTER UPSILON WITH DASIA AND PERISPOMENI */
	[1984+0x28] = 0x1F60,	/* U+1F68: GREEK CAPITAL LETTER OMEGA WITH PSILI */
	[1984+0x29] = 0x1F61,	/* U+1F69: GREEK CAPITAL LETTER OMEGA WITH DASIA */
	[1984+0x2A] = 0x1F62,	/* U+1F6A: GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA */
	[1984+0x2B] = 0x1F63,	/* U+1F6B: GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA */
	[1984+0x2C] = 0x1F64,	/* U+1F6C: GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA */
	[1984+0x2D] = 0x1F65,	/* U+1F6D: GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA */
	[1984+0x2E] = 0x1F66,	/* U+1F6E: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI */
	[1984+0x2F] = 0x1F67,	/* U+1F6F: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI */
	[1280+0x3E] = 2048 - 0x80,	/* 341 276 ... */
	[2048+0x8] = 0x1F80,	/* U+1F88: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI */
	[2048+0x9] = 0x1F81,	/* U+1F89: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI */
	[2048+0xA] = 0x1F82,	/* U+1F8A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2048+0xB] = 0x1F83,	/* U+1F8B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2048+0xC] = 0x1F84,	/* U+1F8C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2048+0xD] = 0x1F85,	/* U+1F8D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2048+0xE] = 0x1F86,	/* U+1F8E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0xF] = 0x1F87,	/* U+1F8F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x18] = 0x1F90,	/* U+1F98: GREEK CAPITAL LETTER ETA WITH PSILI AND PROSGEGRAMMENI */
	[2048+0x19] = 0x1F91,	/* U+1F99: GREEK CAPITAL LETTER ETA WITH DASIA AND PROSGEGRAMMENI */
	[2048+0x1A] = 0x1F92,	/* U+1F9A: GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2048+0x1B] = 0x1F93,	/* U+1F9B: GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2048+0x1C] = 0x1F94,	/* U+1F9C: GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2048+0x1D] = 0x1F95,	/* U+1F9D: GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2048+0x1E] = 0x1F96,	/* U+1F9E: GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x1F] = 0x1F97,	/* U+1F9F: GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x28] = 0x1FA0,	/* U+1FA8: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI */
	[2048+0x29] = 0x1FA1,	/* U+1FA9: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI */
	[2048+0x2A] = 0x1FA2,	/* U+1FAA: GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2048+0x2B] = 0x1FA3,	/* U+1FAB: GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2048+0x2C] = 0x1FA4,	/* U+1FAC: GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2048+0x2D] = 0x1FA5,	/* U+1FAD: GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2048+0x2E] = 0x1FA6,	/* U+1FAE: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x2F] = 0x1FA7,	/* U+1FAF: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x38] = 0x1FB0,	/* U+1FB8: GREEK CAPITAL LETTER ALPHA WITH VRACHY */
	[2048+0x39] = 0x1FB1,	/* U+1FB9: GREEK CAPITAL LETTER ALPHA WITH MACRON */
	[2048+0x3A] = 0x1F70,	/* U+1FBA: GREEK CAPITAL LETTER ALPHA WITH VARIA */
	[2048+0x3B] = 0x1F71,	/* U+1FBB: GREEK CAPITAL LETTER ALPHA WITH OXIA */
	[2048+0x3C] = 0x1FB3,	/* U+1FBC: GREEK CAPITAL LETTER ALPHA WITH PROSGEGRAMMENI */
	[1280+0x3F] = 2112 - 0x80,	/* 341 277 ... */
	[2112+0x8] = 0x1F72,	/* U+1FC8: GREEK CAPITAL LETTER EPSILON WITH VARIA */
	[2112+0x9] = 0x1F73,	/* U+1FC9: GREEK CAPITAL LETTER EPSILON WITH OXIA */
	[2112+0xA] = 0x1F74,	/* U+1FCA: GREEK CAPITAL LETTER ETA WITH VARIA */
	[2112+0xB] = 0x1F75,	/* U+1FCB: GREEK CAPITAL LETTER ETA WITH OXIA */
	[2112+0xC] = 0x1FC3,	/* U+1FCC: GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI */
	[2112+0x18] = 0x1FD0,	/* U+1FD8: GREEK CAPITAL LETTER IOTA WITH VRACHY */
	[2112+0x19] = 0x1FD1,	/* U+1FD9: GREEK CAPITAL LETTER IOTA WITH MACRON */
	[2112+0x1A] = 0x1F76,	/* U+1FDA: GREEK CAPITAL LETTER IOTA WITH VARIA */
	[2112+0x1B] = 0x1F77,	/* U+1FDB: GREEK CAPITAL LETTER IOTA WITH OXIA */
	[2112+0x28] = 0x1FE0,	/* U+1FE8: GREEK CAPITAL LETTER UPSILON WITH VRACHY */
	[2112+0x29] = 0x1FE1,	/* U+1FE9: GREEK CAPITAL LETTER UPSILON WITH MACRON */
	[2112+0x2A] = 0x1F7A,	/* U+1FEA: GREEK CAPITAL LETTER UPSILON WITH VARIA */
	[2112+0x2B] = 0x1F7B,	/* U+1FEB: GREEK CAPITAL LETTER UPSILON WITH OXIA */
	[2112+0x2C] = 0x1FE5,	/* U+1FEC: GREEK CAPITAL LETTER RHO WITH DASIA */
	[2112+0x38] = 0x1F78,	/* U+1FF8: GREEK CAPITAL LETTER OMICRON WITH VARIA */
	[2112+0x39] = 0x1F79,	/* U+1FF9: GREEK CAPITAL LETTER OMICRON WITH OXIA */
	[2112+0x3A] = 0x1F7C,	/* U+1FFA: GREEK CAPITAL LETTER OMEGA WITH VARIA */
	[2112+0x3B] = 0x1F7D,	/* U+1FFB: GREEK CAPITAL LETTER OMEGA WITH OXIA */
	[2112+0x3C] = 0x1FF3,	/* U+1FFC: GREEK CAPITAL LETTER OMEGA WITH PROSGEGRAMMENI */
	[0xE2] = 2176 - 0x80,	/* 342 ... */
	[2176+0x4] = 2240 - 0x80,	/* 342 204 ... */
	[2240+0x26] = 0x3C9,	/* U+2126: OHM SIGN */
	[2240+0x2A] = 0x6B,	/* U+212A: KELVIN SIGN */
	[2240+0x2B] = 0xE5,	/* U+212B: ANGSTROM SIGN */
	[2240+0x32] = 0x214E,	/* U+2132: TURNED CAPITAL F */
	[2176+0x5] = 2304 - 0x80,	/* 342 205 ... */
	[2304+0x20] = 0x2170,	/* U+2160: ROMAN NUMERAL ONE */
	[2304+0x21] = 0x2171,	/* U+2161: ROMAN NUMERAL TWO */
	[2304+0x22] = 0x2172,	/* U+2162: ROMAN NUMERAL THREE */
	[2304+0x23] = 0x2173,	/* U+2163: ROMAN NUMERAL FOUR */
	[2304+0x24] = 0x2174,	/* U+2164: ROMAN NUMERAL FIVE */
	[2304+0x25] = 0x2175,	/* U+2165: ROMAN NUMERAL SIX */
	[2304+0x26] = 0x2176,	/* U+2166: ROMAN NUMERAL SEVEN */
	[2304+0x27] = 0x2177,	/* U+2167: ROMAN NUMERAL EIGHT */
	[2304+0x28] = 0x2178,	/* U+2168: ROMAN NUMERAL NINE */
	[2304+0x29] = 0x2179,	/* U+2169: ROMAN NUMERAL TEN */
	[2304+0x2A] = 0x217A,	/* U+216A: ROMAN NUMERAL ELEVEN */
	[2304+0x2B] = 0x217B,	/* U+216B: ROMAN NUMERAL TWELVE */
	[2304+0x2C] = 0x217C,	/* U+216C: ROMAN NUMERAL FIFTY */
	[2304+0x2D] = 0x217D,	/* U+216D: ROMAN NUMERAL ONE HUNDRED */
	[2304+0x2E] = 0x217E,	/* U+216E: ROMAN NUMERAL FIVE HUNDRED */
	[2304+0x2F] = 0x217F,	/* U+216F: ROMAN NUMERAL ONE THOUSAND */
	[2176+0x6] = 2368 - 0x80,	/* 342 206 ... */
	[2368+0x3] = 0x2184,	/* U+2183: ROMAN NUMERAL REVERSED ONE HUNDRED */
	[2176+0x12] = 2432 - 0x80,	/* 342 222 ... */
	[2432+0x36] = 0x24D0,	/* U+24B6: CIRCLED LATIN CAPITAL LETTER A */
	[2432+0x37] = 0x24D1,	/* U+24B7: CIRCLED LATIN CAPITAL LETTER B */
	[2432+0x38] = 0x24D2,	/* U+24B8: CIRCLED LATIN CAPITAL LETTER C */
	[2432+0x39] = 0x24D3,	/* U+24B9: CIRCLED LATIN CAPITAL LETTER D */
	[2432+0x3A] = 0x24D4,	/* U+24BA: CIRCLED LATIN CAPITAL LETTER E */
	[2432+0x3B] = 0x24D5,	/* U+24BB: CIRCLED LATIN CAPITAL LETTER F */
	[2432+0x3C] = 0x24D6,	/* U+24BC: CIRCLED LATIN CAPITAL LETTER G */
	[2432+0x3D] = 0x24D7,	/* U+24BD: CIRCLED LATIN CAPITAL LETTER H */
	[2432+0x3E] = 0x24D8,	/* U+24BE: CIRCLED LATIN CAPITAL LETTER I */
	[2432+0x3F] = 0x24D9,	/* U+24BF: CIRCLED LATIN CAPITAL LETTER J */
	[2176+0x13] = 2496 - 0x80,	/* 342 223 ... */
	[2496+0x0] = 0x24DA,	/* U+24C0: CIRCLED LATIN CAPITAL LETTER K */
	[2496+0x1] = 0x24DB,	/* U+24C1: CIRCLED LATIN CAPITAL LETTER L */
	[2496+0x2] = 0x24DC,	/* U+24C2: CIRCLED LATIN CAPITAL LETTER M */
	[2496+0x3] = 0x24DD,	/* U+24C3: CIRCLED LATIN CAPITAL LETTER N */
	[2496+0x4] = 0x24DE,	/* U+24C4: CIRCLED LATIN CAPITAL LETTER O */
	[2496+0x5] = 0x24DF,	/* U+24C5: CIRCLED LATIN CAPITAL LETTER P */
	[2496+0x6] = 0x24E0,	/* U+24C6: CIRCLED LATIN CAPITAL LETTER Q */
	[2496+0x7] = 0x24E1,	/* U+24C7: CIRCLED LATIN CAPITAL LETTER R */
	[2496+0x8] = 0x24E2,	/* U+24C8: CIRCLED LATIN CAPITAL LETTER S */
	[2496+0x9] = 0x24E3,	/* U+24C9: CIRCLED LATIN CAPITAL LETTER T */
	[2496+0xA] = 0x24E4,	/* U+24CA: CIRCLED LATIN CAPITAL LETTER U */
	[2496+0xB] = 0x24E5,	/* U+24CB: CIRCLED LATIN CAPITAL LETTER V */
	[2496+0xC] = 0x24E6,	/* U+24CC: CIRCLED LATIN CAPITAL LETTER W */
	[2496+0xD] = 0x24E7,	/* U+24CD: CIRCLED LATIN CAPITAL LETTER X */
	[2496+0xE] = 0x24E8,	/* U+24CE: CIRCLED LATIN CAPITAL LETTER Y */
	[2496+0xF] = 0x24E9,	/* U+24CF: CIRCLED LATIN CAPITAL LETTER Z */
	[2176+0x30] = 2560 - 0x80,	/* 342 260 ... */
	[2560+0x0] = 0x2C30,	/* U+2C00: GLAGOLITIC CAPITAL LETTER AZU */
	[2560+0x1] = 0x2C31,	/* U+2C01: GLAGOLITIC CAPITAL LETTER BUKY */
	[2560+0x2] = 0x2C32,	/* U+2C02: GLAGOLITIC CAPITAL LETTER VEDE */
	[2560+0x3] = 0x2C33,	/* U+2C03: GLAGOLITIC CAPITAL LETTER GLAGOLI */
	[2560+0x4] = 0x2C34,	/* U+2C04: GLAGOLITIC CAPITAL LETTER DOBRO */
	[2560+0x5] = 0x2C35,	/* U+2C05: GLAGOLITIC CAPITAL LETTER YESTU */
	[2560+0x6] = 0x2C36,	/* U+2C06: GLAGOLITIC CAPITAL LETTER ZHIVETE */
	[2560+0x7] = 0x2C37,	/* U+2C07: GLAGOLITIC CAPITAL LETTER DZELO */
	[2560+0x8] = 0x2C38,	/* U+2C08: GLAGOLITIC CAPITAL LETTER ZEMLJA */
	[2560+0x9] = 0x2C39,	/* U+2C09: GLAGOLITIC CAPITAL LETTER IZHE */
	[2560+0xA] = 0x2C3A,	/* U+2C0A: GLAGOLITIC CAPITAL LETTER INITIAL IZHE */
	[2560+0xB] = 0x2C3B,	/* U+2C0B: GLAGOLITIC CAPITAL LETTER I */
	[2560+0xC] = 0x2C3C,	/* U+2C0C: GLAGOLITIC CAPITAL LETTER DJERVI */
	[2560+0xD] = 0x2C3D,	/* U+2C0D: GLAGOLITIC CAPITAL LETTER KAKO */
	[2560+0xE] = 0x2C3E,	/* U+2C0E: GLAGOLITIC CAPITAL LETTER LJUDIJE */
	[2560+0xF] = 0x2C3F,	/* U+2C0F: GLAGOLITIC CAPITAL LETTER MYSLITE */
	[2560+0x10] = 0x2C40,	/* U+2C10: GLAGOLITIC CAPITAL LETTER NASHI */
	[2560+0x11] = 0x2C41,	/* U+2C11: GLAGOLITIC CAPITAL LETTER ONU */
	[2560+0x12] = 0x2C42,	/* U+2C12: GLAGOLITIC CAPITAL LETTER POKOJI */
	[2560+0x13] = 0x2C43,	/* U+2C13: GLAGOLITIC CAPITAL LETTER RITSI */
	[2560+0x14] = 0x2C44,	/* U+2C14: GLAGOLITIC CAPITAL LETTER SLOVO */
	[2560+0x15] = 0x2C45,	/* U+2C15: GLAGOLITIC CAPITAL LETTER TVRIDO */
	[2560+0x16] = 0x2C46,	/* U+2C16: GLAGOLITIC CAPITAL LETTER UKU */
	[2560+0x17] = 0x2C47,	/* U+2C17: GLAGOLITIC CAPITAL LETTER FRITU */
	[2560+0x18] = 0x2C48,	/* U+2C18: GLAGOLITIC CAPITAL LETTER HERU */
	[2560+0x19] = 0x2C49,	/* U+2C19: GLAGOLITIC CAPITAL LETTER OTU */
	[2560+0x1A] = 0x2C4A,	/* U+2C1A: GLAGOLITIC CAPITAL LETTER PE */
	[2560+0x1B] = 0x2C4B,	/* U+2C1B: GLAGOLITIC CAPITAL LETTER SHTA */
	[2560+0x1C] = 0x2C4C,	/* U+2C1C: GLAGOLITIC CAPITAL LETTER TSI */
	[2560+0x1D] = 0x2C4D,	/* U+2C1D: GLAGOLITIC CAPITAL LETTER CHRIVI */
	[2560+0x1E] = 0x2C4E,	/* U+2C1E: GLAGOLITIC CAPITAL LETTER SHA */
	[2560+0x1F] = 0x2C4F,	/* U+2C1F: GLAGOLITIC CAPITAL LETTER YERU */
	[2560+0x20] = 0x2C50,	/* U+2C20: GLAGOLITIC CAPITAL LETTER YERI */
	[2560+0x21] = 0x2C51,	/* U+2C21: GLAGOLITIC CAPITAL LETTER YATI */
	[2560+0x22] = 0x2C52,	/* U+2C22: GLAGOLITIC CAPITAL LETTER SPIDERY HA */
	[2560+0x23] = 0x2C53,	/* U+2C23: GLAGOLITIC CAPITAL LETTER YU */
	[2560+0x24] = 0x2C54,	/* U+2C24: GLAGOLITIC CAPITAL LETTER SMALL YUS */
	[2560+0x25] = 0x2C55,	/* U+2C25: GLAGOLITIC CAPITAL LETTER SMALL YUS WITH TAIL */
	[2560+0x26] = 0x2C56,	/* U+2C26: GLAGOLITIC CAPITAL LETTER YO */
	[2560+0x27] = 0x2C57,	/* U+2C27: GLAGOLITIC CAPITAL LETTER IOTATED SMALL YUS */
	[2560+0x28] = 0x2C58,	/* U+2C28: GLAGOLITIC CAPITAL LETTER BIG YUS */
	[2560+0x29] = 0x2C59,	/* U+2C29: GLAGOLITIC CAPITAL LETTER IOTATED BIG YUS */
	[2560+0x2A] = 0x2C5A,	/* U+2C2A: GLAGOLITIC CAPITAL LETTER FITA */
	[2560+0x2B] = 0x2C5B,	/* U+2C2B: GLAGOLITIC CAPITAL LETTER IZHITSA */
	[2560+0x2C] = 0x2C5C,	/* U+2C2C: GLAGOLITIC CAPITAL LETTER SHTAPIC */
	[2560+0x2D] = 0x2C5D,	/* U+2C2D: GLAGOLITIC CAPITAL LETTER TROKUTASTI A */
	[2560+0x2E] = 0x2C5E,	/* U+2C2E: GLAGOLITIC CAPITAL LETTER LATINATE MYSLITE */
	[2560+0x2F] = 0x2C5F,	/* U+2C2F: GLAGOLITIC CAPITAL LETTER CAUDATE CHRIVI */
	[2176+0x31] = 2624 - 0x80,	/* 342 261 ... */
	[2624+0x20] = 0x2C61,	/* U+2C60: LATIN CAPITAL LETTER L WITH DOUBLE BAR */
	[2624+0x22] = 0x26B,	/* U+2C62: LATIN CAPITAL LETTER L WITH MIDDLE TILDE */
	[2624+0x23] = 0x1D7D,	/* U+2C63: LATIN CAPITAL LETTER P WITH STROKE */
	[2624+0x24] = 0x27D,	/* U+2C64: LATIN CAPITAL LETTER R WITH TAIL */
	[2624+0x27] = 0x2C68,	/* U+2C67: LATIN CAPITAL LETTER H WITH DESCENDER */
	[2624+0x29] = 0x2C6A,	/* U+2C69: LATIN CAPITAL LETTER K WITH DESCENDER */
	[2624+0x2B] = 0x2C6C,	/* U+2C6B: LATIN CAPITAL LETTER Z WITH DESCENDER */
	[2624+0x2D] = 0x251,	/* U+2C6D: LATIN CAPITAL LETTER ALPHA */
	[2624+0x2E] = 0x271,	/* U+2C6E: LATIN CAPITAL LETTER M WITH HOOK */
	[2624+0x2F] = 0x250,	/* U+2C6F: LATIN CAPITAL LETTER TURNED A */
	[2624+0x30] = 0x252,	/* U+2C70: LATIN CAPITAL LETTER TURNED ALPHA */
	[2624+0x32] = 0x2C73,	/* U+2C72: LATIN CAPITAL LETTER W WITH HOOK */
	[2624+0x35] = 0x2C76,	/* U+2C75: LATIN CAPITAL LETTER HALF H */
	[2624+0x3E] = 0x23F,	/* U+2C7E: LATIN CAPITAL LETTER S WITH SWASH TAIL */
	[2624+0x3F] = 0x240,	/* U+2C7F: LATIN CAPITAL LETTER Z WITH SWASH TAIL */
	[2176+0x32] = 2688 - 0x80,	/* 342 262 ... */
	[2688+0x0] = 0x2C81,	/* U+2C80: COPTIC CAPITAL LETTER ALFA */
	[2688+0x2] = 0x2C83,	/* U+2C82: COPTIC CAPITAL LETTER VIDA */
	[2688+0x4] = 0x2C85,	/* U+2C84: COPTIC CAPITAL LETTER GAMMA */
	[2688+0x6] = 0x2C87,	/* U+2C86: COPTIC CAPITAL LETTER DALDA */
	[2688+0x8] = 0x2C89,	/* U+2C88: COPTIC CAPITAL LETTER EIE */
	[2688+0xA] = 0x2C8B,	/* U+2C8A: COPTIC CAPITAL LETTER SOU */
	[2688+0xC] = 0x2C8D,	/* U+2C8C: COPTIC CAPITAL LETTER ZATA */
	[2688+0xE] = 0x2C8F,	/* U+2C8E: COPTIC CAPITAL LETTER HATE */
	[2688+0x10] = 0x2C91,	/* U+2C90: COPTIC CAPITAL LETTER THETHE */
	[2688+0x12] = 0x2C93,	/* U+2C92: COPTIC CAPITAL LETTER IAUDA */
	[2688+0x14] = 0x2C95,	/* U+2C94: COPTIC CAPITAL LETTER KAPA */
	[2688+0x16] = 0x2C97,	/* U+2C96: COPTIC CAPITAL LETTER LAULA */
	[2688+0x18] = 0x2C99,	/* U+2C98: COPTIC CAPITAL LETTER MI */
	[2688+0x1A] = 0x2C9B,	/* U+2C9A: COPTIC CAPITAL LETTER NI */
	[2688+0x1C] = 0x2C9D,	/* U+2C9C: COPTIC CAPITAL LETTER KSI */
	[2688+0x1E] = 0x2C9F,	/* U+2C9E: COPTIC CAPITAL LETTER O */
	[2688+0x20] = 0x2CA1,	/* U+2CA0: COPTIC CAPITAL LETTER PI */
	[2688+0x22] = 0x2CA3,	/* U+2CA2: COPTIC CAPITAL LETTER RO */
	[2688+0x24] = 0x2CA5,	/* U+2CA4: COPTIC CAPITAL LETTER SIMA */
	[2688+0x26] = 0x2CA7,	/* U+2CA6: COPTIC CAPITAL LETTER TAU */
	[2688+0x28] = 0x2CA9,	/* U+2CA8: COPTIC CAPITAL LETTER UA */
	[2688+0x2A] = 0x2CAB,	/* U+2CAA: COPTIC CAPITAL LETTER FI */
	[2688+0x2C] = 0x2CAD,	/* U+2CAC: COPTIC CAPITAL LETTER KHI */
	[2688+0x2E] = 0x2CAF,	/* U+2CAE: COPTIC CAPITAL LETTER PSI */
	[2688+0x30] = 0x2CB1,	/* U+2CB0: COPTIC CAPITAL LETTER OOU */
	[2688+0x32] = 0x2CB3,	/* U+2CB2: COPTIC CAPITAL LETTER DIALECT-P ALEF */
	[2688+0x34] = 0x2CB5,	/* U+2CB4: COPTIC CAPITAL LETTER OLD COPTIC AIN */
	[2688+0x36] = 0x2CB7,	/* U+2CB6: COPTIC CAPITAL LETTER CRYPTOGRAMMIC EIE */
	[2688+0x38] = 0x2CB9,	/* U+2CB8: COPTIC CAPITAL LETTER DIALECT-P KAPA */
	[2688+0x3A] = 0x2CBB,	/* U+2CBA: COPTIC CAPITAL LETTER DIALECT-P NI */
	[2688+0x3C] = 0x2CBD,	/* U+2CBC: COPTIC CAPITAL LETTER CRYPTOGRAMMIC NI */
	[2688+0x3E] = 0x2CBF,	/* U+2CBE: COPTIC CAPITAL LETTER OLD COPTIC OOU */
	[2176+0x33] = 2752 - 0x80,	/* 342 263 ... */
	[2752+0x0] = 0x2CC1,	/* U+2CC0: COPTIC CAPITAL LETTER SAMPI */
	[2752+0x2] = 0x2CC3,	/* U+2CC2: COPTIC CAPITAL LETTER CROSSED SHEI */
	[2752+0x4] = 0x2CC5,	/* U+2CC4: COPTIC CAPITAL LETTER OLD COPTIC SHEI */
	[2752+0x6] = 0x2CC7,	/* U+2CC6: COPTIC CAPITAL LETTER OLD COPTIC ESH */
	[2752+0x8] = 0x2CC9,	/* U+2CC8: COPTIC CAPITAL LETTER AKHMIMIC KHEI */
	[2752+0xA] = 0x2CCB,	/* U+2CCA: COPTIC CAPITAL LETTER DIALECT-P HORI */
	[2752+0xC] = 0x2CCD,	/* U+2CCC: COPTIC CAPITAL LETTER OLD COPTIC HORI */
	[2752+0xE] = 0x2CCF,	/* U+2CCE: COPTIC CAPITAL LETTER OLD COPTIC HA */
	[2752+0x10] = 0x2CD1,	/* U+2CD0: COPTIC CAPITAL LETTER L-SHAPED HA */
	[2752+0x12] = 0x2CD3,	/* U+2CD2: COPTIC CAPITAL LETTER OLD COPTIC HEI */
	[2752+0x14] = 0x2CD5,	/* U+2CD4: COPTIC CAPITAL LETTER OLD COPTIC HAT */
	[2752+0x16] = 0x2CD7,	/* U+2CD6: COPTIC CAPITAL LETTER OLD COPTIC GANGIA */
	[2752+0x18] = 0x2CD9,	/* U+2CD8: COPTIC CAPITAL LETTER OLD COPTIC DJA */
	[2752+0x1A] = 0x2CDB,	/* U+2CDA: COPTIC CAPITAL LETTER OLD COPTIC SHIMA */
	[2752+0x1C] = 0x2CDD,	/* U+2CDC: COPTIC CAPITAL LETTER OLD NUBIAN SHIMA */
	[2752+0x1E] = 0x2CDF,	/* U+2CDE: COPTIC CAPITAL LETTER OLD NUBIAN NGI */
	[2752+0x20] = 0x2CE1,	/* U+2CE0: COPTIC CAPITAL LETTER OLD NUBIAN NYI */
	[2752+0x22] = 0x2CE3,	/* U+2CE2: COPTIC CAPITAL LETTER OLD NUBIAN WAU */
	[2752+0x2B] = 0x2CEC,	/* U+2CEB: COPTIC CAPITAL LETTER CRYPTOGRAMMIC SHEI */
	[2752+0x2D] = 0x2CEE,	/* U+2CED: COPTIC CAPITAL LETTER CRYPTOGRAMMIC GANGIA */
	[2752+0x32] = 0x2CF3,	/* U+2CF2: COPTIC CAPITAL LETTER BOHAIRIC KHEI */
	[0xEA] = 2816 - 0x80,	/* 352 ... */
	[2816+0x19] = 2880 - 0x80,	/* 352 231 ... */
	[2880+0x0] = 0xA641,	/* U+A640: CYRILLIC CAPITAL LETTER ZEMLYA */
	[2880+0x2] = 0xA643,	/* U+A642: CYRILLIC CAPITAL LETTER DZELO */
	[2880+0x4] = 0xA645,	/* U+A644: CYRILLIC CAPITAL LETTER REVERSED DZE */
	[2880+0x6] = 0xA647,	/* U+A646: CYRILLIC CAPITAL LETTER IOTA */
	[2880+0x8] = 0xA649,	/* U+A648: CYRILLIC CAPITAL LETTER DJERV */
	[2880+0xA] = 0xA64B,	/* U+A64A: CYRILLIC CAPITAL LETTER MONOGRAPH UK */
	[2880+0xC] = 0xA64D,	/* U+A64C: CYRILLIC CAPITAL LETTER BROAD OMEGA */
	[2880+0xE] = 0xA64F,	/* U+A64E: CYRILLIC CAPITAL LETTER NEUTRAL YER */
	[2880+0x10] = 0xA651,	/* U+A650: CYRILLIC CAPITAL LETTER YERU WITH BACK YER */
	[2880+0x12] = 0xA653,	/* U+A652: CYRILLIC CAPITAL LETTER IOTIFIED YAT */
	[2880+0x14] = 0xA655,	/* U+A654: CYRILLIC CAPITAL LETTER REVERSED YU */
	[2880+0x16] = 0xA657,	/* U+A656: CYRILLIC CAPITAL LETTER IOTIFIED A */
	[2880+0x18] = 0xA659,	/* U+A658: CYRILLIC CAPITAL LETTER CLOSED LITTLE YUS */
	[2880+0x1A] = 0xA65B,	/* U+A65A: CYRILLIC CAPITAL LETTER BLENDED YUS */
	[2880+0x1C] = 0xA65D,	/* U+A65C: CYRILLIC CAPITAL LETTER IOTIFIED CLOSED LITTLE YUS */
	[2880+0x1E] = 0xA65F,	/* U+A65E: CYRILLIC CAPITAL LETTER YN */
	[2880+0x20] = 0xA661,	/* U+A660: CYRILLIC CAPITAL LETTER REVERSED TSE */
	[2880+0x22] = 0xA663,	/* U+A662: CYRILLIC CAPITAL LETTER SOFT DE */
	[2880+0x24] = 0xA665,	/* U+A664: CYRILLIC CAPITAL LETTER SOFT EL */
	[2880+0x26] = 0xA667,	/* U+A666: CYRILLIC CAPITAL LETTER SOFT EM */
	[2880+0x28] = 0xA669,	/* U+A668: CYRILLIC CAPITAL LETTER MONOCULAR O */
	[2880+0x2A] = 0xA66B,	/* U+A66A: CYRILLIC CAPITAL LETTER BINOCULAR O */
	[2880+0x2C] = 0xA66D,	/* U+A66C: CYRILLIC CAPITAL LETTER DOUBLE MONOCULAR O */
	[2816+0x1A] = 2944 - 0x80,	/* 352 232 ... */
	[2944+0x0] = 0xA681,	/* U+A680: CYRILLIC CAPITAL LETTER DWE */
	[2944+0x2] = 0xA683,	/* U+A682: CYRILLIC CAPITAL LETTER DZWE */
	[2944+0x4] = 0xA685,	/* U+A684: CYRILLIC CAPITAL LETTER ZHWE */
	[2944+0x6] = 0xA687,	/* U+A686: CYRILLIC CAPITAL LETTER CCHE */
	[2944+0x8] = 0xA689,	/* U+A688: CYRILLIC CAPITAL LETTER DZZE */
	[2944+0xA] = 0xA68B,	/* U+A68A: CYRILLIC CAPITAL LETTER TE WITH MIDDLE HOOK */
	[2944+0xC] = 0xA68D,	/* U+A68C: CYRILLIC CAPITAL LETTER TWE */
	[2944+0xE] = 0xA68F,	/* U+A68E: CYRILLIC CAPITAL LETTER TSWE */
	[2944+0x10] = 0xA691,	/* U+A690: CYRILLIC CAPITAL LETTER TSSE */
	[2944+0x12] = 0xA693,	/* U+A692: CYRILLIC CAPITAL LETTER TCHE */
	[2944+0x14] = 0xA695,	/* U+A694: CYRILLIC CAPITAL LETTER HWE */
	[2944+0x16] = 0xA697,	/* U+A696: CYRILLIC CAPITAL LETTER SHWE */
	[2944+0x18] = 0xA699,	/* U+A698: CYRILLIC CAPITAL LETTER DOUBLE O */
	[2944+0x1A] = 0xA69B,	/* U+A69A: CYRILLIC CAPITAL LETTER CROSSED O */
	[2816+0x1C] = 3008 - 0x80,	/* 352 234 ... */
	[3008+0x22] = 0xA723,	/* U+A722: LATIN CAPITAL LETTER EGYPTOLOGICAL ALEF */
	[3008+0x24] = 0xA725,	/* U+A724: LATIN CAPITAL LETTER EGYPTOLOGICAL AIN */
	[3008+0x26] = 0xA727,	/* U+A726: LATIN CAPITAL LETTER HENG */
	[3008+0x28] = 0xA729,	/* U+A728: LATIN CAPITAL LETTER TZ */
	[3008+0x2A] = 0xA72B,	/* U+A72A: LATIN CAPITAL LETTER TRESILLO */
	[3008+0x2C] = 0xA72D,	/* U+A72C: LATIN CAPITAL LETTER CUATRILLO */
	[3008+0x2E] = 0xA72F,	/* U+A72E: LATIN CAPITAL LETTER CUATRILLO WITH COMMA */
	[3008+0x32] = 0xA733,	/* U+A732: LATIN CAPITAL LETTER AA */
	[3008+0x34] = 0xA735,	/* U+A734: LATIN CAPITAL LETTER AO */
	[3008+0x36] = 0xA737,	/* U+A736: LATIN CAPITAL LETTER AU */
	[3008+0x38] = 0xA739,	/* U+A738: LATIN CAPITAL LETTER AV */
	[3008+0x3A] = 0xA73B,	/* U+A73A: LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR */
	[3008+0x3C] = 0xA73D,	/* U+A73C: LATIN CAPITAL LETTER AY */
	[3008+0x3E] = 0xA73F,	/* U+A73E: LATIN CAPITAL LETTER REVERSED C WITH DOT */
	[2816+0x1D] = 3072 - 0x80,	/* 352 235 ... */
	[3072+0x0] = 0xA741,	/* U+A740: LATIN CAPITAL LETTER K WITH STROKE */
	[3072+0x2] = 0xA743,	/* U+A742: LATIN CAPITAL LETTER K WITH DIAGONAL STROKE */
	[3072+0x4] = 0xA745,	/* U+A744: LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE */
	[3072+0x6] = 0xA747,	/* U+A746: LATIN CAPITAL LETTER BROKEN L */
	[3072+0x8] = 0xA749,	/* U+A748: LATIN CAPITAL LETTER L WITH HIGH STROKE */
	[3072+0xA] = 0xA74B,	/* U+A74A: LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY */
	[3072+0xC] = 0xA74D,	/* U+A74C: LATIN CAPITAL LETTER O WITH LOOP */
	[3072+0xE] = 0xA74F,	/* U+A74E: LATIN CAPITAL LETTER OO */
	[3072+0x10] = 0xA751,	/* U+A750: LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER */
	[3072+0x12] = 0xA753,	/* U+A752: LATIN CAPITAL LETTER P WITH FLOURISH */
	[3072+0x14] = 0xA755,	/* U+A754: LATIN CAPITAL LETTER P WITH SQUIRREL TAIL */
	[3072+0x16] = 0xA757,	/* U+A756: LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER */
	[3072+0x18] = 0xA759,	/* U+A758: LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE */
	[3072+0x1A] = 0xA75B,	/* U+A75A: LATIN CAPITAL LETTER R ROTUNDA */
	[3072+0x1C] = 0xA75D,	/* U+A75C: LATIN CAPITAL LETTER RUM ROTUNDA */
	[3072+0x1E] = 0xA75F,	/* U+A75E: LATIN CAPITAL LETTER V WITH DIAGONAL STROKE */
	[3072+0x20] = 0xA761,	/* U+A760: LATIN CAPITAL LETTER VY */
	[3072+0x22] = 0xA763,	/* U+A762: LATIN CAPITAL LETTER VISIGOTHIC Z */
	[3072+0x24] = 0xA765,	/* U+A764: LATIN CAPITAL LETTER THORN WITH STROKE */
	[3072+0x26] = 0xA767,	/* U+A766: LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER */
	[3072+0x28] = 0xA769,	/* U+A768: LATIN CAPITAL LETTER VEND */
	[3072+0x2A] = 0xA76B,	/* U+A76A: LATIN CAPITAL LETTER ET */
	[3072+0x2C] = 0xA76D,	/* U+A76C: LATIN CAPITAL LETTER IS */
	[3072+0x2E] = 0xA76F,	/* U+A76E: LATIN CAPITAL LETTER CON */
	[3072+0x39] = 0xA77A,	/* U+A779: LATIN CAPITAL LETTER INSULAR D */
	[3072+0x3B] = 0xA77C,	/* U+A77B: LATIN CAPITAL LETTER INSULAR F */
	[3072+0x3D] = 0x1D79,	/* U+A77D: LATIN CAPITAL LETTER INSULAR G */
	[3072+0x3E] = 0xA77F,	/* U+A77E: LATIN CAPITAL LETTER TURNED INSULAR G */
	[2816+0x1E] = 3136 - 0x80,	/* 352 236 ... */
	[3136+0x0] = 0xA781,	/* U+A780: LATIN CAPITAL LETTER TURNED L */
	[3136+0x2] = 0xA783,	/* U+A782: LATIN CAPITAL LETTER INSULAR R */
	[3136+0x4] = 0xA785,	/* U+A784: LATIN CAPITAL LETTER INSULAR S */
	[3136+0x6] = 0xA787,	/* U+A786: LATIN CAPITAL LETTER INSULAR T */
	[3136+0xB] = 0xA78C,	/* U+A78B: LATIN CAPITAL LETTER SALTILLO */
	[3136+0xD] = 0x265,	/* U+A78D: LATIN CAPITAL LETTER TURNED H */
	[3136+0x10] = 0xA791,	/* U+A790: LATIN CAPITAL LETTER N WITH DESCENDER */
	[3136+0x12] = 0xA793,	/* U+A792: LATIN CAPITAL LETTER C WITH BAR */
	[3136+0x16] = 0xA797,	/* U+A796: LATIN CAPITAL LETTER B WITH FLOURISH */
	[3136+0x18] = 0xA799,	/* U+A798: LATIN CAPITAL LETTER F WITH STROKE */
	[3136+0x1A] = 0xA79B,	/* U+A79A: LATIN CAPITAL LETTER VOLAPUK AE */
	[3136+0x1C] = 0xA79D,	/* U+A79C: LATIN CAPITAL LETTER VOLAPUK OE */
	[3136+0x1E] = 0xA79F,	/* U+A79E: LATIN CAPITAL LETTER VOLAPUK UE */
	[3136+0x20] = 0xA7A1,	/* U+A7A0: LATIN CAPITAL LETTER G WITH OBLIQUE STROKE */
	[3136+0x22] = 0xA7A3,	/* U+A7A2: LATIN CAPITAL LETTER K WITH OBLIQUE STROKE */
	[3136+0x24] = 0xA7A5,	/* U+A7A4: LATIN CAPITAL LETTER N WITH OBLIQUE STROKE */
	[3136+0x26] = 0xA7A7,	/* U+A7A6: LATIN CAPITAL LETTER R WITH OBLIQUE STROKE */
	[3136+0x28] = 0xA7A9,	/* U+A7A8: LATIN CAPITAL LETTER S WITH OBLIQUE STROKE */
	[3136+0x2A] = 0x266,	/* U+A7AA: LATIN CAPITAL LETTER H WITH HOOK */
	[3136+0x2B] = 0x25C,	/* U+A7AB: LATIN CAPITAL LETTER REVERSED OPEN E */
	[3136+0x2C] = 0x261,	/* U+A7AC: LATIN CAPITAL LETTER SCRIPT G */
	[3136+0x2D] = 0x26C,	/* U+A7AD: LATIN CAPITAL LETTER L WITH BELT */
	[3136+0x2E] = 0x26A,	/* U+A7AE: LATIN CAPITAL LETTER SMALL CAPITAL I */
	[3136+0x30] = 0x29E,	/* U+A7B0: LATIN CAPITAL LETTER TURNED K */
	[3136+0x31] = 0x287,	/* U+A7B1: LATIN CAPITAL LETTER TURNED T */
	[3136+0x32] = 0x29D,	/* U+A7B2: LATIN CAPITAL LETTER J WITH CROSSED-TAIL */
	[3136+0x33] = 0xAB53,	/* U+A7B3: LATIN CAPITAL LETTER CHI */
	[3136+0x34] = 0xA7B5,	/* U+A7B4: LATIN CAPITAL LETTER BETA */
	[3136+0x36] = 0xA7B7,	/* U+A7B6: LATIN CAPITAL LETTER OMEGA */
	[3136+0x38] = 0xA7B9,	/* U+A7B8: LATIN CAPITAL LETTER U WITH STROKE */
	[3136+0x3A] = 0xA7BB,	/* U+A7BA: LATIN CAPITAL LETTER GLOTTAL A */
	[3136+0x3C] = 0xA7BD,	/* U+A7BC: LATIN CAPITAL LETTER GLOTTAL I */
	[3136+0x3E] = 0xA7BF,	/* U+A7BE: LATIN CAPITAL LETTER GLOTTAL U */
	[2816+0x1F] = 3200 - 0x80,	/* 352 237 ... */
	[3200+0x0] = 0xA7C1,	/* U+A7C0: LATIN CAPITAL LETTER OLD POLISH O */
	[3200+0x2] = 0xA7C3,	/* U+A7C2: LATIN CAPITAL LETTER ANGLICANA W */
	[3200+0x4] = 0xA794,	/* U+A7C4: LATIN CAPITAL LETTER C WITH PALATAL HOOK */
	[3200+0x5] = 0x282,	/* U+A7C5: LATIN CAPITAL LETTER S WITH HOOK */
	[3200+0x6] = 0x1D8E,	/* U+A7C6: LATIN CAPITAL LETTER Z WITH PALATAL HOOK */
	[3200+0x7] = 0xA7C8,	/* U+A7C7: LATIN CAPITAL LETTER D WITH SHORT STROKE OVERLAY */
	[3200+0x9] = 0xA7CA,	/* U+A7C9: LATIN CAPITAL LETTER S WITH SHORT STROKE OVERLAY */
	[3200+0x10] = 0xA7D1,	/* U+A7D0: LATIN CAPITAL LETTER CLOSED INSULAR G */
	[3200+0x16] = 0xA7D7,	/* U+A7D6: LATIN CAPITAL LETTER MIDDLE SCOTS S */
	[3200+0x18] = 0xA7D9,	/* U+A7D8: LATIN CAPITAL LETTER SIGMOID S */
	[3200+0x35] = 0xA7F6,	/* U+A7F5: LATIN CAPITAL LETTER REVERSED HALF H */
	[0xEF] = 3264 - 0x80,	/* 357 ... */
	[3264+0x3C] = 3328 - 0x80,	/* 357 274 ... */
	[3328+0x21] = 0xFF41,	/* U+FF21: FULLWIDTH LATIN CAPITAL LETTER A */
	[3328+0x22] = 0xFF42,	/* U+FF22: FULLWIDTH LATIN CAPITAL LETTER B */
	[3328+0x23] = 0xFF43,	/* U+FF23: FULLWIDTH LATIN CAPITAL LETTER C */
	[3328+0x24] = 0xFF44,	/* U+FF24: FULLWIDTH LATIN CAPITAL LETTER D */
	[3328+0x25] = 0xFF45,	/* U+FF25: FULLWIDTH LATIN CAPITAL LETTER E */
	[3328+0x26] = 0xFF46,	/* U+FF26: FULLWIDTH LATIN CAPITAL LETTER F */
	[3328+0x27] = 0xFF47,	/* U+FF27: FULLWIDTH LATIN CAPITAL LETTER G */
	[3328+0x28] = 0xFF48,	/* U+FF28: FULLWIDTH LATIN CAPITAL LETTER H */
	[3328+0x29] = 0xFF49,	/* U+FF29: FULLWIDTH LATIN CAPITAL LETTER I */
	[3328+0x2A] = 0xFF4A,	/* U+FF2A: FULLWIDTH LATIN CAPITAL LETTER J */
	[3328+0x2B] = 0xFF4B,	/* U+FF2B: FULLWIDTH LATIN CAPITAL LETTER K */
	[3328+0x2C] = 0xFF4C,	/* U+FF2C: FULLWIDTH LATIN CAPITAL LETTER L */
	[3328+0x2D] = 0xFF4D,	/* U+FF2D: FULLWIDTH LATIN CAPITAL LETTER M */
	[3328+0x2E] = 0xFF4E,	/* U+FF2E: FULLWIDTH LATIN CAPITAL LETTER N */
	[3328+0x2F] = 0xFF4F,	/* U+FF2F: FULLWIDTH LATIN CAPITAL LETTER O */
	[3328+0x30] = 0xFF50,	/* U+FF30: FULLWIDTH LATIN CAPITAL LETTER P */
	[3328+0x31] = 0xFF51,	/* U+FF31: FULLWIDTH LATIN CAPITAL LETTER Q */
	[3328+0x32] = 0xFF52,	/* U+FF32: FULLWIDTH LATIN CAPITAL LETTER R */
	[3328+0x33] = 0xFF53,	/* U+FF33: FULLWIDTH LATIN CAPITAL LETTER S */
	[3328+0x34] = 0xFF54,	/* U+FF34: FULLWIDTH LATIN CAPITAL LETTER T */
	[3328+0x35] = 0xFF55,	/* U+FF35: FULLWIDTH LATIN CAPITAL LETTER U */
	[3328+0x36] = 0xFF56,	/* U+FF36: FULLWIDTH LATIN CAPITAL LETTER V */
	[3328+0x37] = 0xFF57,	/* U+FF37: FULLWIDTH LATIN CAPITAL LETTER W */
	[3328+0x38] = 0xFF58,	/* U+FF38: FULLWIDTH LATIN CAPITAL LETTER X */
	[3328+0x39] = 0xFF59,	/* U+FF39: FULLWIDTH LATIN CAPITAL LETTER Y */
	[3328+0x3A] = 0xFF5A,	/* U+FF3A: FULLWIDTH LATIN CAPITAL LETTER Z */
	[0xF0] = 3392 - 0x80,	/* 360 ... */
	[3392+0x10] = 3456 - 0x80,	/* 360 220 ... */
	[3456+0x10] = 3520 - 0x80,	/* 360 220 220 ... */
	[3520+0x0] = 0x10428,	/* U+10400: DESERET CAPITAL LETTER LONG I */
	[3520+0x1] = 0x10429,	/* U+10401: DESERET CAPITAL LETTER LONG E */
	[3520+0x2] = 0x1042A,	/* U+10402: DESERET CAPITAL LETTER LONG A */
	[3520+0x3] = 0x1042B,	/* U+10403: DESERET CAPITAL LETTER LONG AH */
	[3520+0x4] = 0x1042C,	/* U+10404: DESERET CAPITAL LETTER LONG O */
	[3520+0x5] = 0x1042D,	/* U+10405: DESERET CAPITAL LETTER LONG OO */
	[3520+0x6] = 0x1042E,	/* U+10406: DESERET CAPITAL LETTER SHORT I */
	[3520+0x7] = 0x1042F,	/* U+10407: DESERET CAPITAL LETTER SHORT E */
	[3520+0x8] = 0x10430,	/* U+10408: DESERET CAPITAL LETTER SHORT A */
	[3520+0x9] = 0x10431,	/* U+10409: DESERET CAPITAL LETTER SHORT AH */
	[3520+0xA] = 0x10432,	/* U+1040A: DESERET CAPITAL LETTER SHORT O */
	[3520+0xB] = 0x10433,	/* U+1040B: DESERET CAPITAL LETTER SHORT OO */
	[3520+0xC] = 0x10434,	/* U+1040C: DESERET CAPITAL LETTER AY */
	[3520+0xD] = 0x10435,	/* U+1040D: DESERET CAPITAL LETTER OW */
	[3520+0xE] = 0x10436,	/* U+1040E: DESERET CAPITAL LETTER WU */
	[3520+0xF] = 0x10437,	/* U+1040F: DESERET CAPITAL LETTER YEE */
	[3520+0x10] = 0x10438,	/* U+10410: DESERET CAPITAL LETTER H */
	[3520+0x11] = 0x10439,	/* U+10411: DESERET CAPITAL LETTER PEE */
	[3520+0x12] = 0x1043A,	/* U+10412: DESERET CAPITAL LETTER BEE */
	[3520+0x13] = 0x1043B,	/* U+10413: DESERET CAPITAL LETTER TEE */
	[3520+0x14] = 0x1043C,	/* U+10414: DESERET CAPITAL LETTER DEE */
	[3520+0x15] = 0x1043D,	/* U+10415: DESERET CAPITAL LETTER CHEE */
	[3520+0x16] = 0x1043E,	/* U+10416: DESERET CAPITAL LETTER JEE */
	[3520+0x17] = 0x1043F,	/* U+10417: DESERET CAPITAL LETTER KAY */
	[3520+0x18] = 0x10440,	/* U+10418: DESERET CAPITAL LETTER GAY */
	[3520+0x19] = 0x10441,	/* U+10419: DESERET CAPITAL LETTER EF */
	[3520+0x1A] = 0x10442,	/* U+1041A: DESERET CAPITAL LETTER VEE */
	[3520+0x1B] = 0x10443,	/* U+1041B: DESERET CAPITAL LETTER ETH */
	[3520+0x1C] = 0x10444,	/* U+1041C: DESERET CAPITAL LETTER THEE */
	[3520+0x1D] = 0x10445,	/* U+1041D: DESERET CAPITAL LETTER ES */
	[3520+0x1E] = 0x10446,	/* U+1041E: DESERET CAPITAL LETTER ZEE */
	[3520+0x1F] = 0x10447,	/* U+1041F: DESERET CAPITAL LETTER ESH */
	[3520+0x20] = 0x10448,	/* U+10420: DESERET CAPITAL LETTER ZHEE */
	[3520+0x21] = 0x10449,	/* U+10421: DESERET CAPITAL LETTER ER */
	[3520+0x22] = 0x1044A,	/* U+10422: DESERET CAPITAL LETTER EL */
	[3520+0x23] = 0x1044B,	/* U+10423: DESERET CAPITAL LETTER EM */
	[3520+0x24] = 0x1044C,	/* U+10424: DESERET CAPITAL LETTER EN */
	[3520+0x25] = 0x1044D,	/* U+10425: DESERET CAPITAL LETTER ENG */
	[3520+0x26] = 0x1044E,	/* U+10426: DESERET CAPITAL LETTER OI */
	[3520+0x27] = 0x1044F,	/* U+10427: DESERET CAPITAL LETTER EW */
	[3456+0x12] = 3584 - 0x80,	/* 360 220 222 ... */
	[3584+0x30] = 0x104D8,	/* U+104B0: OSAGE CAPITAL LETTER A */
	[3584+0x31] = 0x104D9,	/* U+104B1: OSAGE CAPITAL LETTER AI */
	[3584+0x32] = 0x104DA,	/* U+104B2: OSAGE CAPITAL LETTER AIN */
	[3584+0x33] = 0x104DB,	/* U+104B3: OSAGE CAPITAL LETTER AH */
	[3584+0x34] = 0x104DC,	/* U+104B4: OSAGE CAPITAL LETTER BRA */
	[3584+0x35] = 0x104DD,	/* U+104B5: OSAGE CAPITAL LETTER CHA */
	[3584+0x36] = 0x104DE,	/* U+104B6: OSAGE CAPITAL LETTER EHCHA */
	[3584+0x37] = 0x104DF,	/* U+104B7: OSAGE CAPITAL LETTER E */
	[3584+0x38] = 0x104E0,	/* U+104B8: OSAGE CAPITAL LETTER EIN */
	[3584+0x39] = 0x104E1,	/* U+104B9: OSAGE CAPITAL LETTER HA */
	[3584+0x3A] = 0x104E2,	/* U+104BA: OSAGE CAPITAL LETTER HYA */
	[3584+0x3B] = 0x104E3,	/* U+104BB: OSAGE CAPITAL LETTER I */
	[3584+0x3C] = 0x104E4,	/* U+104BC: OSAGE CAPITAL LETTER KA */
	[3584+0x3D] = 0x104E5,	/* U+104BD: OSAGE CAPITAL LETTER EHKA */
	[3584+0x3E] = 0x104E6,	/* U+104BE: OSAGE CAPITAL LETTER KYA */
	[3584+0x3F] = 0x104E7,	/* U+104BF: OSAGE CAPITAL LETTER LA */
	[3456+0x13] = 3648 - 0x80,	/* 360 220 223 ... */
	[3648+0x0] = 0x104E8,	/* U+104C0: OSAGE CAPITAL LETTER MA */
	[3648+0x1] = 0x104E9,	/* U+104C1: OSAGE CAPITAL LETTER NA */
	[3648+0x2] = 0x104EA,	/* U+104C2: OSAGE CAPITAL LETTER O */
	[3648+0x3] = 0x104EB,	/* U+104C3: OSAGE CAPITAL LETTER OIN */
	[3648+0x4] = 0x104EC,	/* U+104C4: OSAGE CAPITAL LETTER PA */
	[3648+0x5] = 0x104ED,	/* U+104C5: OSAGE CAPITAL LETTER EHPA */
	[3648+0x6] = 0x104EE,	/* U+104C6: OSAGE CAPITAL LETTER SA */
	[3648+0x7] = 0x104EF,	/* U+104C7: OSAGE CAPITAL LETTER SHA */
	[3648+0x8] = 0x104F0,	/* U+104C8: OSAGE CAPITAL LETTER TA */
	[3648+0x9] = 0x104F1,	/* U+104C9: OSAGE CAPITAL LETTER EHTA */
	[3648+0xA] = 0x104F2,	/* U+104CA: OSAGE CAPITAL LETTER TSA */
	[3648+0xB] = 0x104F3,	/* U+104CB: OSAGE CAPITAL LETTER EHTSA */
	[3648+0xC] = 0x104F4,	/* U+104CC: OSAGE CAPITAL LETTER TSHA */
	[3648+0xD] = 0x104F5,	/* U+104CD: OSAGE CAPITAL LETTER DHA */
	[3648+0xE] = 0x104F6,	/* U+104CE: OSAGE CAPITAL LETTER U */
	[3648+0xF] = 0x104F7,	/* U+104CF: OSAGE CAPITAL LETTER WA */
	[3648+0x10] = 0x104F8,	/* U+104D0: OSAGE CAPITAL LETTER KHA */
	[3648+0x11] = 0x104F9,	/* U+104D1: OSAGE CAPITAL LETTER GHA */
	[3648+0x12] = 0x104FA,	/* U+104D2: OSAGE CAPITAL LETTER ZA */
	[3648+0x13] = 0x104FB,	/* U+104D3: OSAGE CAPITAL LETTER ZHA */
	[3456+0x15] = 3712 - 0x80,	/* 360 220 225 ... */
	[3712+0x30] = 0x10597,	/* U+10570: VITHKUQI CAPITAL LETTER A */
	[3712+0x31] = 0x10598,	/* U+10571: VITHKUQI CAPITAL LETTER BBE */
	[3712+0x32] = 0x10599,	/* U+10572: VITHKUQI CAPITAL LETTER BE */
	[3712+0x33] = 0x1059A,	/* U+10573: VITHKUQI CAPITAL LETTER CE */
	[3712+0x34] = 0x1059B,	/* U+10574: VITHKUQI CAPITAL LETTER CHE */
	[3712+0x35] = 0x1059C,	/* U+10575: VITHKUQI CAPITAL LETTER DE */
	[3712+0x36] = 0x1059D,	/* U+10576: VITHKUQI CAPITAL LETTER DHE */
	[3712+0x37] = 0x1059E,	/* U+10577: VITHKUQI CAPITAL LETTER EI */
	[3712+0x38] = 0x1059F,	/* U+10578: VITHKUQI CAPITAL LETTER E */
	[3712+0x39] = 0x105A0,	/* U+10579: VITHKUQI CAPITAL LETTER FE */
	[3712+0x3A] = 0x105A1,	/* U+1057A: VITHKUQI CAPITAL LETTER GA */
	[3712+0x3C] = 0x105A3,	/* U+1057C: VITHKUQI CAPITAL LETTER HA */
	[3712+0x3D] = 0x105A4,	/* U+1057D: VITHKUQI CAPITAL LETTER HHA */
	[3712+0x3E] = 0x105A5,	/* U+1057E: VITHKUQI CAPITAL LETTER I */
	[3712+0x3F] = 0x105A6,	/* U+1057F: VITHKUQI CAPITAL LETTER IJE */
	[3456+0x16] = 3776 - 0x80,	/* 360 220 226 ... */
	[3776+0x0] = 0x105A7,	/* U+10580: VITHKUQI CAPITAL LETTER JE */
	[3776+0x1] = 0x105A8,	/* U+10581: VITHKUQI CAPITAL LETTER KA */
	[3776+0x2] = 0x105A9,	/* U+10582: VITHKUQI CAPITAL LETTER LA */
	[3776+0x3] = 0x105AA,	/* U+10583: VITHKUQI CAPITAL LETTER LLA */
	[3776+0x4] = 0x105AB,	/* U+10584: VITHKUQI CAPITAL LETTER ME */
	[3776+0x5] = 0x105AC,	/* U+10585: VITHKUQI CAPITAL LETTER NE */
	[3776+0x6] = 0x105AD,	/* U+10586: VITHKUQI CAPITAL LETTER NJE */
	[3776+0x7] = 0x105AE,	/* U+10587: VITHKUQI CAPITAL LETTER O */
	[3776+0x8] = 0x105AF,	/* U+10588: VITHKUQI CAPITAL LETTER PE */
	[3776+0x9] = 0x105B0,	/* U+10589: VITHKUQI CAPITAL LETTER QA */
	[3776+0xA] = 0x105B1,	/* U+1058A: VITHKUQI CAPITAL LETTER RE */
	[3776+0xC] = 0x105B3,	/* U+1058C: VITHKUQI CAPITAL LETTER SE */
	[3776+0xD] = 0x105B4,	/* U+1058D: VITHKUQI CAPITAL LETTER SHE */
	[3776+0xE] = 0x105B5,	/* U+1058E: VITHKUQI CAPITAL LETTER TE */
	[3776+0xF] = 0x105B6,	/* U+1058F: VITHKUQI CAPITAL LETTER THE */
	[3776+0x10] = 0x105B7,	/* U+10590: VITHKUQI CAPITAL LETTER U */
	[3776+0x11] = 0x105B8,	/* U+10591: VITHKUQI CAPITAL LETTER VE */
	[3776+0x12] = 0x105B9,	/* U+10592: VITHKUQI CAPITAL LETTER XE */
	[3776+0x14] = 0x105BB,	/* U+10594: VITHKUQI CAPITAL LETTER Y */
	[3776+0x15] = 0x105BC,	/* U+10595: VITHKUQI CAPITAL LETTER ZE */
	[3456+0x32] = 3840 - 0x80,	/* 360 220 262 ... */
	[3840+0x0] = 0x10CC0,	/* U+10C80: OLD HUNGARIAN CAPITAL LETTER A */
	[3840+0x1] = 0x10CC1,	/* U+10C81: OLD HUNGARIAN CAPITAL LETTER AA */
	[3840+0x2] = 0x10CC2,	/* U+10C82: OLD HUNGARIAN CAPITAL LETTER EB */
	[3840+0x3] = 0x10CC3,	/* U+10C83: OLD HUNGARIAN CAPITAL LETTER AMB */
	[3840+0x4] = 0x10CC4,	/* U+10C84: OLD HUNGARIAN CAPITAL LETTER EC */
	[3840+0x5] = 0x10CC5,	/* U+10C85: OLD HUNGARIAN CAPITAL LETTER ENC */
	[3840+0x6] = 0x10CC6,	/* U+10C86: OLD HUNGARIAN CAPITAL LETTER ECS */
	[3840+0x7] = 0x10CC7,	/* U+10C87: OLD HUNGARIAN CAPITAL LETTER ED */
	[3840+0x8] = 0x10CC8,	/* U+10C88: OLD HUNGARIAN CAPITAL LETTER AND */
	[3840+0x9] = 0x10CC9,	/* U+10C89: OLD HUNGARIAN CAPITAL LETTER E */
	[3840+0xA] = 0x10CCA,	/* U+10C8A: OLD HUNGARIAN CAPITAL LETTER CLOSE E */
	[3840+0xB] = 0x10CCB,	/* U+10C8B: OLD HUNGARIAN CAPITAL LETTER EE */
	[3840+0xC] = 0x10CCC,	/* U+10C8C: OLD HUNGARIAN CAPITAL LETTER EF */
	[3840+0xD] = 0x10CCD,	/* U+10C8D: OLD HUNGARIAN CAPITAL LETTER EG */
	[3840+0xE] = 0x10CCE,	/* U+10C8E: OLD HUNGARIAN CAPITAL LETTER EGY */
	[3840+0xF] = 0x10CCF,	/* U+10C8F: OLD HUNGARIAN CAPITAL LETTER EH */
	[3840+0x10] = 0x10CD0,	/* U+10C90: OLD HUNGARIAN CAPITAL LETTER I */
	[3840+0x11] = 0x10CD1,	/* U+10C91: OLD HUNGARIAN CAPITAL LETTER II */
	[3840+0x12] = 0x10CD2,	/* U+10C92: OLD HUNGARIAN CAPITAL LETTER EJ */
	[3840+0x13] = 0x10CD3,	/* U+10C93: OLD HUNGARIAN CAPITAL LETTER EK */
	[3840+0x14] = 0x10CD4,	/* U+10C94: OLD HUNGARIAN CAPITAL LETTER AK */
	[3840+0x15] = 0x10CD5,	/* U+10C95: OLD HUNGARIAN CAPITAL LETTER UNK */
	[3840+0x16] = 0x10CD6,	/* U+10C96: OLD HUNGARIAN CAPITAL LETTER EL */
	[3840+0x17] = 0x10CD7,	/* U+10C97: OLD HUNGARIAN CAPITAL LETTER ELY */
	[3840+0x18] = 0x10CD8,	/* U+10C98: OLD HUNGARIAN CAPITAL LETTER EM */
	[3840+0x19] = 0x10CD9,	/* U+10C99: OLD HUNGARIAN CAPITAL LETTER EN */
	[3840+0x1A] = 0x10CDA,	/* U+10C9A: OLD HUNGARIAN CAPITAL LETTER ENY */
	[3840+0x1B] = 0x10CDB,	/* U+10C9B: OLD HUNGARIAN CAPITAL LETTER O */
	[3840+0x1C] = 0x10CDC,	/* U+10C9C: OLD HUNGARIAN CAPITAL LETTER OO */
	[3840+0x1D] = 0x10CDD,	/* U+10C9D: OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG OE */
	[3840+0x1E] = 0x10CDE,	/* U+10C9E: OLD HUNGARIAN CAPITAL LETTER RUDIMENTA OE */
	[3840+0x1F] = 0x10CDF,	/* U+10C9F: OLD HUNGARIAN CAPITAL LETTER OEE */
	[3840+0x20] = 0x10CE0,	/* U+10CA0: OLD HUNGARIAN CAPITAL LETTER EP */
	[3840+0x21] = 0x10CE1,	/* U+10CA1: OLD HUNGARIAN CAPITAL LETTER EMP */
	[3840+0x22] = 0x10CE2,	/* U+10CA2: OLD HUNGARIAN CAPITAL LETTER ER */
	[3840+0x23] = 0x10CE3,	/* U+10CA3: OLD HUNGARIAN CAPITAL LETTER SHORT ER */
	[3840+0x24] = 0x10CE4,	/* U+10CA4: OLD HUNGARIAN CAPITAL LETTER ES */
	[3840+0x25] = 0x10CE5,	/* U+10CA5: OLD HUNGARIAN CAPITAL LETTER ESZ */
	[3840+0x26] = 0x10CE6,	/* U+10CA6: OLD HUNGARIAN CAPITAL LETTER ET */
	[3840+0x27] = 0x10CE7,	/* U+10CA7: OLD HUNGARIAN CAPITAL LETTER ENT */
	[3840+0x28] = 0x10CE8,	/* U+10CA8: OLD HUNGARIAN CAPITAL LETTER ETY */
	[3840+0x29] = 0x10CE9,	/* U+10CA9: OLD HUNGARIAN CAPITAL LETTER ECH */
	[3840+0x2A] = 0x10CEA,	/* U+10CAA: OLD HUNGARIAN CAPITAL LETTER U */
	[3840+0x2B] = 0x10CEB,	/* U+10CAB: OLD HUNGARIAN CAPITAL LETTER UU */
	[3840+0x2C] = 0x10CEC,	/* U+10CAC: OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG UE */
	[3840+0x2D] = 0x10CED,	/* U+10CAD: OLD HUNGARIAN CAPITAL LETTER RUDIMENTA UE */
	[3840+0x2E] = 0x10CEE,	/* U+10CAE: OLD HUNGARIAN CAPITAL LETTER EV */
	[3840+0x2F] = 0x10CEF,	/* U+10CAF: OLD HUNGARIAN CAPITAL LETTER EZ */
	[3840+0x30] = 0x10CF0,	/* U+10CB0: OLD HUNGARIAN CAPITAL LETTER EZS */
	[3840+0x31] = 0x10CF1,	/* U+10CB1: OLD HUNGARIAN CAPITAL LETTER ENT-SHAPED SIGN */
	[3840+0x32] = 0x10CF2,	/* U+10CB2: OLD HUNGARIAN CAPITAL LETTER US */
	[3392+0x11] = 3904 - 0x80,	/* 360 221 ... */
	[3904+0x22] = 3968 - 0x80,	/* 360 221 242 ... */
	[3968+0x20] = 0x118C0,	/* U+118A0: WARANG CITI CAPITAL LETTER NGAA */
	[3968+0x21] = 0x118C1,	/* U+118A1: WARANG CITI CAPITAL LETTER A */
	[3968+0x22] = 0x118C2,	/* U+118A2: WARANG CITI CAPITAL LETTER WI */
	[3968+0x23] = 0x118C3,	/* U+118A3: WARANG CITI CAPITAL LETTER YU */
	[3968+0x24] = 0x118C4,	/* U+118A4: WARANG CITI CAPITAL LETTER YA */
	[3968+0x25] = 0x118C5,	/* U+118A5: WARANG CITI CAPITAL LETTER YO */
	[3968+0x26] = 0x118C6,	/* U+118A6: WARANG CITI CAPITAL LETTER II */
	[3968+0x27] = 0x118C7,	/* U+118A7: WARANG CITI CAPITAL LETTER UU */
	[3968+0x28] = 0x118C8,	/* U+118A8: WARANG CITI CAPITAL LETTER E */
	[3968+0x29] = 0x118C9,	/* U+118A9: WARANG CITI CAPITAL LETTER O */
	[3968+0x2A] = 0x118CA,	/* U+118AA: WARANG CITI CAPITAL LETTER ANG */
	[3968+0x2B] = 0x118CB,	/* U+118AB: WARANG CITI CAPITAL LETTER GA */
	[3968+0x2C] = 0x118CC,	/* U+118AC: WARANG CITI CAPITAL LETTER KO */
	[3968+0x2D] = 0x118CD,	/* U+118AD: WARANG CITI CAPITAL LETTER ENY */
	[3968+0x2E] = 0x118CE,	/* U+118AE: WARANG CITI CAPITAL LETTER YUJ */
	[3968+0x2F] = 0x118CF,	/* U+118AF: WARANG CITI CAPITAL LETTER UC */
	[3968+0x30] = 0x118D0,	/* U+118B0: WARANG CITI CAPITAL LETTER ENN */
	[3968+0x31] = 0x118D1,	/* U+118B1: WARANG CITI CAPITAL LETTER ODD */
	[3968+0x32] = 0x118D2,	/* U+118B2: WARANG CITI CAPITAL LETTER TTE */
	[3968+0x33] = 0x118D3,	/* U+118B3: WARANG CITI CAPITAL LETTER NUNG */
	[3968+0x34] = 0x118D4,	/* U+118B4: WARANG CITI CAPITAL LETTER DA */
	[3968+0x35] = 0x118D5,	/* U+118B5: WARANG CITI CAPITAL LETTER AT */
	[3968+0x36] = 0x118D6,	/* U+118B6: WARANG CITI CAPITAL LETTER AM */
	[3968+0x37] = 0x118D7,	/* U+118B7: WARANG CITI CAPITAL LETTER BU */
	[3968+0x38] = 0x118D8,	/* U+118B8: WARANG CITI CAPITAL LETTER PU */
	[3968+0x39] = 0x118D9,	/* U+118B9: WARANG CITI CAPITAL LETTER HIYO */
	[3968+0x3A] = 0x118DA,	/* U+118BA: WARANG CITI CAPITAL LETTER HOLO */
	[3968+0x3B] = 0x118DB,	/* U+118BB: WARANG CITI CAPITAL LETTER HORR */
	[3968+0x3C] = 0x118DC,	/* U+118BC: WARANG CITI CAPITAL LETTER HAR */
	[3968+0x3D] = 0x118DD,	/* U+118BD: WARANG CITI CAPITAL LETTER SSUU */
	[3968+0x3E] = 0x118DE,	/* U+118BE: WARANG CITI CAPITAL LETTER SII */
	[3968+0x3F] = 0x118DF,	/* U+118BF: WARANG CITI CAPITAL LETTER VIYO */
	[3392+0x16] = 4032 - 0x80,	/* 360 226 ... */
	[4032+0x39] = 4096 - 0x80,	/* 360 226 271 ... */
	[4096+0x0] = 0x16E60,	/* U+16E40: MEDEFAIDRIN CAPITAL LETTER M */
	[4096+0x1] = 0x16E61,	/* U+16E41: MEDEFAIDRIN CAPITAL LETTER S */
	[4096+0x2] = 0x16E62,	/* U+16E42: MEDEFAIDRIN CAPITAL LETTER V */
	[4096+0x3] = 0x16E63,	/* U+16E43: MEDEFAIDRIN CAPITAL LETTER W */
	[4096+0x4] = 0x16E64,	/* U+16E44: MEDEFAIDRIN CAPITAL LETTER ATIU */
	[4096+0x5] = 0x16E65,	/* U+16E45: MEDEFAIDRIN CAPITAL LETTER Z */
	[4096+0x6] = 0x16E66,	/* U+16E46: MEDEFAIDRIN CAPITAL LETTER KP */
	[4096+0x7] = 0x16E67,	/* U+16E47: MEDEFAIDRIN CAPITAL LETTER P */
	[4096+0x8] = 0x16E68,	/* U+16E48: MEDEFAIDRIN CAPITAL LETTER T */
	[4096+0x9] = 0x16E69,	/* U+16E49: MEDEFAIDRIN CAPITAL LETTER G */
	[4096+0xA] = 0x16E6A,	/* U+16E4A: MEDEFAIDRIN CAPITAL LETTER F */
	[4096+0xB] = 0x16E6B,	/* U+16E4B: MEDEFAIDRIN CAPITAL LETTER I */
	[4096+0xC] = 0x16E6C,	/* U+16E4C: MEDEFAIDRIN CAPITAL LETTER K */
	[4096+0xD] = 0x16E6D,	/* U+16E4D: MEDEFAIDRIN CAPITAL LETTER A */
	[4096+0xE] = 0x16E6E,	/* U+16E4E: MEDEFAIDRIN CAPITAL LETTER J */
	[4096+0xF] = 0x16E6F,	/* U+16E4F: MEDEFAIDRIN CAPITAL LETTER E */
	[4096+0x10] = 0x16E70,	/* U+16E50: MEDEFAIDRIN CAPITAL LETTER B */
	[4096+0x11] = 0x16E71,	/* U+16E51: MEDEFAIDRIN CAPITAL LETTER C */
	[4096+0x12] = 0x16E72,	/* U+16E52: MEDEFAIDRIN CAPITAL LETTER U */
	[4096+0x13] = 0x16E73,	/* U+16E53: MEDEFAIDRIN CAPITAL LETTER YU */
	[4096+0x14] = 0x16E74,	/* U+16E54: MEDEFAIDRIN CAPITAL LETTER L */
	[4096+0x15] = 0x16E75,	/* U+16E55: MEDEFAIDRIN CAPITAL LETTER Q */
	[4096+0x16] = 0x16E76,	/* U+16E56: MEDEFAIDRIN CAPITAL LETTER HP */
	[4096+0x17] = 0x16E77,	/* U+16E57: MEDEFAIDRIN CAPITAL LETTER NY */
	[4096+0x18] = 0x16E78,	/* U+16E58: MEDEFAIDRIN CAPITAL LETTER X */
	[4096+0x19] = 0x16E79,	/* U+16E59: MEDEFAIDRIN CAPITAL LETTER D */
	[4096+0x1A] = 0x16E7A,	/* U+16E5A: MEDEFAIDRIN CAPITAL LETTER OE */
	[4096+0x1B] = 0x16E7B,	/* U+16E5B: MEDEFAIDRIN CAPITAL LETTER N */
	[4096+0x1C] = 0x16E7C,	/* U+16E5C: MEDEFAIDRIN CAPITAL LETTER R */
	[4096+0x1D] = 0x16E7D,	/* U+16E5D: MEDEFAIDRIN CAPITAL LETTER O */
	[4096+0x1E] = 0x16E7E,	/* U+16E5E: MEDEFAIDRIN CAPITAL LETTER AI */
	[4096+0x1F] = 0x16E7F,	/* U+16E5F: MEDEFAIDRIN CAPITAL LETTER Y */
	[3392+0x1E] = 4160 - 0x80,	/* 360 236 ... */
	[4160+0x24] = 4224 - 0x80,	/* 360 236 244 ... */
	[4224+0x0] = 0x1E922,	/* U+1E900: ADLAM CAPITAL LETTER ALIF */
	[4224+0x1] = 0x1E923,	/* U+1E901: ADLAM CAPITAL LETTER DAALI */
	[4224+0x2] = 0x1E924,	/* U+1E902: ADLAM CAPITAL LETTER LAAM */
	[4224+0x3] = 0x1E925,	/* U+1E903: ADLAM CAPITAL LETTER MIIM */
	[4224+0x4] = 0x1E926,	/* U+1E904: ADLAM CAPITAL LETTER BA */
	[4224+0x5] = 0x1E927,	/* U+1E905: ADLAM CAPITAL LETTER SINNYIIYHE */
	[4224+0x6] = 0x1E928,	/* U+1E906: ADLAM CAPITAL LETTER PE */
	[4224+0x7] = 0x1E929,	/* U+1E907: ADLAM CAPITAL LETTER BHE */
	[4224+0x8] = 0x1E92A,	/* U+1E908: ADLAM CAPITAL LETTER RA */
	[4224+0x9] = 0x1E92B,	/* U+1E909: ADLAM CAPITAL LETTER E */
	[4224+0xA] = 0x1E92C,	/* U+1E90A: ADLAM CAPITAL LETTER FA */
	[4224+0xB] = 0x1E92D,	/* U+1E90B: ADLAM CAPITAL LETTER I */
	[4224+0xC] = 0x1E92E,	/* U+1E90C: ADLAM CAPITAL LETTER O */
	[4224+0xD] = 0x1E92F,	/* U+1E90D: ADLAM CAPITAL LETTER DHA */
	[4224+0xE] = 0x1E930,	/* U+1E90E: ADLAM CAPITAL LETTER YHE */
	[4224+0xF] = 0x1E931,	/* U+1E90F: ADLAM CAPITAL LETTER WAW */
	[4224+0x10] = 0x1E932,	/* U+1E910: ADLAM CAPITAL LETTER NUN */
	[4224+0x11] = 0x1E933,	/* U+1E911: ADLAM CAPITAL LETTER KAF */
	[4224+0x12] = 0x1E934,	/* U+1E912: ADLAM CAPITAL LETTER YA */
	[4224+0x13] = 0x1E935,	/* U+1E913: ADLAM CAPITAL LETTER U */
	[4224+0x14] = 0x1E936,	/* U+1E914: ADLAM CAPITAL LETTER JIIM */
	[4224+0x15] = 0x1E937,	/* U+1E915: ADLAM CAPITAL LETTER CHI */
	[4224+0x16] = 0x1E938,	/* U+1E916: ADLAM CAPITAL LETTER HA */
	[4224+0x17] = 0x1E939,	/* U+1E917: ADLAM CAPITAL LETTER QAAF */
	[4224+0x18] = 0x1E93A,	/* U+1E918: ADLAM CAPITAL LETTER GA */
	[4224+0x19] = 0x1E93B,	/* U+1E919: ADLAM CAPITAL LETTER NYA */
	[4224+0x1A] = 0x1E93C,	/* U+1E91A: ADLAM CAPITAL LETTER TU */
	[4224+0x1B] = 0x1E93D,	/* U+1E91B: ADLAM CAPITAL LETTER NHA */
	[4224+0x1C] = 0x1E93E,	/* U+1E91C: ADLAM CAPITAL LETTER VA */
	[4224+0x1D] = 0x1E93F,	/* U+1E91D: ADLAM CAPITAL LETTER KHA */
	[4224+0x1E] = 0x1E940,	/* U+1E91E: ADLAM CAPITAL LETTER GBE */
	[4224+0x1F] = 0x1E941,	/* U+1E91F: ADLAM CAPITAL LETTER ZAL */
	[4224+0x20] = 0x1E942,	/* U+1E920: ADLAM CAPITAL LETTER KPO */
	[4224+0x21] = 0x1E943,	/* U+1E921: ADLAM CAPITAL LETTER SHA */
};

static const int uppercase[4544] = {
	[0x0] = 0x0,		/* U+0000: <control> */
	[0x1] = 0x1,		/* U+0001: <control> */
	[0x2] = 0x2,		/* U+0002: <control> */
	[0x3] = 0x3,		/* U+0003: <control> */
	[0x4] = 0x4,		/* U+0004: <control> */
	[0x5] = 0x5,		/* U+0005: <control> */
	[0x6] = 0x6,		/* U+0006: <control> */
	[0x7] = 0x7,		/* U+0007: <control> */
	[0x8] = 0x8,		/* U+0008: <control> */
	[0x9] = 0x9,		/* U+0009: <control> */
	[0xA] = 0xA,		/* U+000A: <control> */
	[0xB] = 0xB,		/* U+000B: <control> */
	[0xC] = 0xC,		/* U+000C: <control> */
	[0xD] = 0xD,		/* U+000D: <control> */
	[0xE] = 0xE,		/* U+000E: <control> */
	[0xF] = 0xF,		/* U+000F: <control> */
	[0x10] = 0x10,		/* U+0010: <control> */
	[0x11] = 0x11,		/* U+0011: <control> */
	[0x12] = 0x12,		/* U+0012: <control> */
	[0x13] = 0x13,		/* U+0013: <control> */
	[0x14] = 0x14,		/* U+0014: <control> */
	[0x15] = 0x15,		/* U+0015: <control> */
	[0x16] = 0x16,		/* U+0016: <control> */
	[0x17] = 0x17,		/* U+0017: <control> */
	[0x18] = 0x18,		/* U+0018: <control> */
	[0x19] = 0x19,		/* U+0019: <control> */
	[0x1A] = 0x1A,		/* U+001A: <control> */
	[0x1B] = 0x1B,		/* U+001B: <control> */
	[0x1C] = 0x1C,		/* U+001C: <control> */
	[0x1D] = 0x1D,		/* U+001D: <control> */
	[0x1E] = 0x1E,		/* U+001E: <control> */
	[0x1F] = 0x1F,		/* U+001F: <control> */
	[0x20] = 0x20,		/* U+0020: SPACE */
	[0x21] = 0x21,		/* U+0021: EXCLAMATION MARK */
	[0x22] = 0x22,		/* U+0022: QUOTATION MARK */
	[0x23] = 0x23,		/* U+0023: NUMBER SIGN */
	[0x24] = 0x24,		/* U+0024: DOLLAR SIGN */
	[0x25] = 0x25,		/* U+0025: PERCENT SIGN */
	[0x26] = 0x26,		/* U+0026: AMPERSAND */
	[0x27] = 0x27,		/* U+0027: APOSTROPHE */
	[0x28] = 0x28,		/* U+0028: LEFT PARENTHESIS */
	[0x29] = 0x29,		/* U+0029: RIGHT PARENTHESIS */
	[0x2A] = 0x2A,		/* U+002A: ASTERISK */
	[0x2B] = 0x2B,		/* U+002B: PLUS SIGN */
	[0x2C] = 0x2C,		/* U+002C: COMMA */
	[0x2D] = 0x2D,		/* U+002D: HYPHEN-MINUS */
	[0x2E] = 0x2E,		/* U+002E: FULL STOP */
	[0x2F] = 0x2F,		/* U+002F: SOLIDUS */
	[0x30] = 0x30,		/* U+0030: DIGIT ZERO */
	[0x31] = 0x31,		/* U+0031: DIGIT ONE */
	[0x32] = 0x32,		/* U+0032: DIGIT TWO */
	[0x33] = 0x33,		/* U+0033: DIGIT THREE */
	[0x34] = 0x34,		/* U+0034: DIGIT FOUR */
	[0x35] = 0x35,		/* U+0035: DIGIT FIVE */
	[0x36] = 0x36,		/* U+0036: DIGIT SIX */
	[0x37] = 0x37,		/* U+0037: DIGIT SEVEN */
	[0x38] = 0x38,		/* U+0038: DIGIT EIGHT */
	[0x39] = 0x39,		/* U+0039: DIGIT NINE */
	[0x3A] = 0x3A,		/* U+003A: COLON */
	[0x3B] = 0x3B,		/* U+003B: SEMICOLON */
	[0x3C] = 0x3C,		/* U+003C: LESS-THAN SIGN */
	[0x3D] = 0x3D,		/* U+003D: EQUALS SIGN */
	[0x3E] = 0x3E,		/* U+003E: GREATER-THAN SIGN */
	[0x3F] = 0x3F,		/* U+003F: QUESTION MARK */
	[0x40] = 0x40,		/* U+0040: COMMERCIAL AT */
	[0x41] = 0x41,		/* U+0041: LATIN CAPITAL LETTER A */
	[0x42] = 0x42,		/* U+0042: LATIN CAPITAL LETTER B */
	[0x43] = 0x43,		/* U+0043: LATIN CAPITAL LETTER C */
	[0x44] = 0x44,		/* U+0044: LATIN CAPITAL LETTER D */
	[0x45] = 0x45,		/* U+0045: LATIN CAPITAL LETTER E */
	[0x46] = 0x46,		/* U+0046: LATIN CAPITAL LETTER F */
	[0x47] = 0x47,		/* U+0047: LATIN CAPITAL LETTER G */
	[0x48] = 0x48,		/* U+0048: LATIN CAPITAL LETTER H */
	[0x49] = 0x49,		/* U+0049: LATIN CAPITAL LETTER I */
	[0x4A] = 0x4A,		/* U+004A: LATIN CAPITAL LETTER J */
	[0x4B] = 0x4B,		/* U+004B: LATIN CAPITAL LETTER K */
	[0x4C] = 0x4C,		/* U+004C: LATIN CAPITAL LETTER L */
	[0x4D] = 0x4D,		/* U+004D: LATIN CAPITAL LETTER M */
	[0x4E] = 0x4E,		/* U+004E: LATIN CAPITAL LETTER N */
	[0x4F] = 0x4F,		/* U+004F: LATIN CAPITAL LETTER O */
	[0x50] = 0x50,		/* U+0050: LATIN CAPITAL LETTER P */
	[0x51] = 0x51,		/* U+0051: LATIN CAPITAL LETTER Q */
	[0x52] = 0x52,		/* U+0052: LATIN CAPITAL LETTER R */
	[0x53] = 0x53,		/* U+0053: LATIN CAPITAL LETTER S */
	[0x54] = 0x54,		/* U+0054: LATIN CAPITAL LETTER T */
	[0x55] = 0x55,		/* U+0055: LATIN CAPITAL LETTER U */
	[0x56] = 0x56,		/* U+0056: LATIN CAPITAL LETTER V */
	[0x57] = 0x57,		/* U+0057: LATIN CAPITAL LETTER W */
	[0x58] = 0x58,		/* U+0058: LATIN CAPITAL LETTER X */
	[0x59] = 0x59,		/* U+0059: LATIN CAPITAL LETTER Y */
	[0x5A] = 0x5A,		/* U+005A: LATIN CAPITAL LETTER Z */
	[0x5B] = 0x5B,		/* U+005B: LEFT SQUARE BRACKET */
	[0x5C] = 0x5C,		/* U+005C: REVERSE SOLIDUS */
	[0x5D] = 0x5D,		/* U+005D: RIGHT SQUARE BRACKET */
	[0x5E] = 0x5E,		/* U+005E: CIRCUMFLEX ACCENT */
	[0x5F] = 0x5F,		/* U+005F: LOW LINE */
	[0x60] = 0x60,		/* U+0060: GRAVE ACCENT */
	[0x61] = 0x41,		/* U+0061: LATIN SMALL LETTER A */
	[0x62] = 0x42,		/* U+0062: LATIN SMALL LETTER B */
	[0x63] = 0x43,		/* U+0063: LATIN SMALL LETTER C */
	[0x64] = 0x44,		/* U+0064: LATIN SMALL LETTER D */
	[0x65] = 0x45,		/* U+0065: LATIN SMALL LETTER E */
	[0x66] = 0x46,		/* U+0066: LATIN SMALL LETTER F */
	[0x67] = 0x47,		/* U+0067: LATIN SMALL LETTER G */
	[0x68] = 0x48,		/* U+0068: LATIN SMALL LETTER H */
	[0x69] = 0x49,		/* U+0069: LATIN SMALL LETTER I */
	[0x6A] = 0x4A,		/* U+006A: LATIN SMALL LETTER J */
	[0x6B] = 0x4B,		/* U+006B: LATIN SMALL LETTER K */
	[0x6C] = 0x4C,		/* U+006C: LATIN SMALL LETTER L */
	[0x6D] = 0x4D,		/* U+006D: LATIN SMALL LETTER M */
	[0x6E] = 0x4E,		/* U+006E: LATIN SMALL LETTER N */
	[0x6F] = 0x4F,		/* U+006F: LATIN SMALL LETTER O */
	[0x70] = 0x50,		/* U+0070: LATIN SMALL LETTER P */
	[0x71] = 0x51,		/* U+0071: LATIN SMALL LETTER Q */
	[0x72] = 0x52,		/* U+0072: LATIN SMALL LETTER R */
	[0x73] = 0x53,		/* U+0073: LATIN SMALL LETTER S */
	[0x74] = 0x54,		/* U+0074: LATIN SMALL LETTER T */
	[0x75] = 0x55,		/* U+0075: LATIN SMALL LETTER U */
	[0x76] = 0x56,		/* U+0076: LATIN SMALL LETTER V */
	[0x77] = 0x57,		/* U+0077: LATIN SMALL LETTER W */
	[0x78] = 0x58,		/* U+0078: LATIN SMALL LETTER X */
	[0x79] = 0x59,		/* U+0079: LATIN SMALL LETTER Y */
	[0x7A] = 0x5A,		/* U+007A: LATIN SMALL LETTER Z */
	[0x7B] = 0x7B,		/* U+007B: LEFT CURLY BRACKET */
	[0x7C] = 0x7C,		/* U+007C: VERTICAL LINE */
	[0x7D] = 0x7D,		/* U+007D: RIGHT CURLY BRACKET */
	[0x7E] = 0x7E,		/* U+007E: TILDE */
	[0x7F] = 0x7F,		/* U+007F: <control> */
	[0xC2] = 256 - 0x80,	/* 302 ... */
	[256+0x35] = 0x39C,	/* U+00B5: MICRO SIGN */
	[0xC3] = 320 - 0x80,	/* 303 ... */
	[320+0x20] = 0xC0,	/* U+00E0: LATIN SMALL LETTER A WITH GRAVE */
	[320+0x21] = 0xC1,	/* U+00E1: LATIN SMALL LETTER A WITH ACUTE */
	[320+0x22] = 0xC2,	/* U+00E2: LATIN SMALL LETTER A WITH CIRCUMFLEX */
	[320+0x23] = 0xC3,	/* U+00E3: LATIN SMALL LETTER A WITH TILDE */
	[320+0x24] = 0xC4,	/* U+00E4: LATIN SMALL LETTER A WITH DIAERESIS */
	[320+0x25] = 0xC5,	/* U+00E5: LATIN SMALL LETTER A WITH RING ABOVE */
	[320+0x26] = 0xC6,	/* U+00E6: LATIN SMALL LETTER AE */
	[320+0x27] = 0xC7,	/* U+00E7: LATIN SMALL LETTER C WITH CEDILLA */
	[320+0x28] = 0xC8,	/* U+00E8: LATIN SMALL LETTER E WITH GRAVE */
	[320+0x29] = 0xC9,	/* U+00E9: LATIN SMALL LETTER E WITH ACUTE */
	[320+0x2A] = 0xCA,	/* U+00EA: LATIN SMALL LETTER E WITH CIRCUMFLEX */
	[320+0x2B] = 0xCB,	/* U+00EB: LATIN SMALL LETTER E WITH DIAERESIS */
	[320+0x2C] = 0xCC,	/* U+00EC: LATIN SMALL LETTER I WITH GRAVE */
	[320+0x2D] = 0xCD,	/* U+00ED: LATIN SMALL LETTER I WITH ACUTE */
	[320+0x2E] = 0xCE,	/* U+00EE: LATIN SMALL LETTER I WITH CIRCUMFLEX */
	[320+0x2F] = 0xCF,	/* U+00EF: LATIN SMALL LETTER I WITH DIAERESIS */
	[320+0x30] = 0xD0,	/* U+00F0: LATIN SMALL LETTER ETH */
	[320+0x31] = 0xD1,	/* U+00F1: LATIN SMALL LETTER N WITH TILDE */
	[320+0x32] = 0xD2,	/* U+00F2: LATIN SMALL LETTER O WITH GRAVE */
	[320+0x33] = 0xD3,	/* U+00F3: LATIN SMALL LETTER O WITH ACUTE */
	[320+0x34] = 0xD4,	/* U+00F4: LATIN SMALL LETTER O WITH CIRCUMFLEX */
	[320+0x35] = 0xD5,	/* U+00F5: LATIN SMALL LETTER O WITH TILDE */
	[320+0x36] = 0xD6,	/* U+00F6: LATIN SMALL LETTER O WITH DIAERESIS */
	[320+0x38] = 0xD8,	/* U+00F8: LATIN SMALL LETTER O WITH STROKE */
	[320+0x39] = 0xD9,	/* U+00F9: LATIN SMALL LETTER U WITH GRAVE */
	[320+0x3A] = 0xDA,	/* U+00FA: LATIN SMALL LETTER U WITH ACUTE */
	[320+0x3B] = 0xDB,	/* U+00FB: LATIN SMALL LETTER U WITH CIRCUMFLEX */
	[320+0x3C] = 0xDC,	/* U+00FC: LATIN SMALL LETTER U WITH DIAERESIS */
	[320+0x3D] = 0xDD,	/* U+00FD: LATIN SMALL LETTER Y WITH ACUTE */
	[320+0x3E] = 0xDE,	/* U+00FE: LATIN SMALL LETTER THORN */
	[320+0x3F] = 0x178,	/* U+00FF: LATIN SMALL LETTER Y WITH DIAERESIS */
	[0xC4] = 384 - 0x80,	/* 304 ... */
	[384+0x1] = 0x100,	/* U+0101: LATIN SMALL LETTER A WITH MACRON */
	[384+0x3] = 0x102,	/* U+0103: LATIN SMALL LETTER A WITH BREVE */
	[384+0x5] = 0x104,	/* U+0105: LATIN SMALL LETTER A WITH OGONEK */
	[384+0x7] = 0x106,	/* U+0107: LATIN SMALL LETTER C WITH ACUTE */
	[384+0x9] = 0x108,	/* U+0109: LATIN SMALL LETTER C WITH CIRCUMFLEX */
	[384+0xB] = 0x10A,	/* U+010B: LATIN SMALL LETTER C WITH DOT ABOVE */
	[384+0xD] = 0x10C,	/* U+010D: LATIN SMALL LETTER C WITH CARON */
	[384+0xF] = 0x10E,	/* U+010F: LATIN SMALL LETTER D WITH CARON */
	[384+0x11] = 0x110,	/* U+0111: LATIN SMALL LETTER D WITH STROKE */
	[384+0x13] = 0x112,	/* U+0113: LATIN SMALL LETTER E WITH MACRON */
	[384+0x15] = 0x114,	/* U+0115: LATIN SMALL LETTER E WITH BREVE */
	[384+0x17] = 0x116,	/* U+0117: LATIN SMALL LETTER E WITH DOT ABOVE */
	[384+0x19] = 0x118,	/* U+0119: LATIN SMALL LETTER E WITH OGONEK */
	[384+0x1B] = 0x11A,	/* U+011B: LATIN SMALL LETTER E WITH CARON */
	[384+0x1D] = 0x11C,	/* U+011D: LATIN SMALL LETTER G WITH CIRCUMFLEX */
	[384+0x1F] = 0x11E,	/* U+011F: LATIN SMALL LETTER G WITH BREVE */
	[384+0x21] = 0x120,	/* U+0121: LATIN SMALL LETTER G WITH DOT ABOVE */
	[384+0x23] = 0x122,	/* U+0123: LATIN SMALL LETTER G WITH CEDILLA */
	[384+0x25] = 0x124,	/* U+0125: LATIN SMALL LETTER H WITH CIRCUMFLEX */
	[384+0x27] = 0x126,	/* U+0127: LATIN SMALL LETTER H WITH STROKE */
	[384+0x29] = 0x128,	/* U+0129: LATIN SMALL LETTER I WITH TILDE */
	[384+0x2B] = 0x12A,	/* U+012B: LATIN SMALL LETTER I WITH MACRON */
	[384+0x2D] = 0x12C,	/* U+012D: LATIN SMALL LETTER I WITH BREVE */
	[384+0x2F] = 0x12E,	/* U+012F: LATIN SMALL LETTER I WITH OGONEK */
	[384+0x31] = 0x49,	/* U+0131: LATIN SMALL LETTER DOTLESS I */
	[384+0x33] = 0x132,	/* U+0133: LATIN SMALL LIGATURE IJ */
	[384+0x35] = 0x134,	/* U+0135: LATIN SMALL LETTER J WITH CIRCUMFLEX */
	[384+0x37] = 0x136,	/* U+0137: LATIN SMALL LETTER K WITH CEDILLA */
	[384+0x3A] = 0x139,	/* U+013A: LATIN SMALL LETTER L WITH ACUTE */
	[384+0x3C] = 0x13B,	/* U+013C: LATIN SMALL LETTER L WITH CEDILLA */
	[384+0x3E] = 0x13D,	/* U+013E: LATIN SMALL LETTER L WITH CARON */
	[0xC5] = 448 - 0x80,	/* 305 ... */
	[448+0x0] = 0x13F,	/* U+0140: LATIN SMALL LETTER L WITH MIDDLE DOT */
	[448+0x2] = 0x141,	/* U+0142: LATIN SMALL LETTER L WITH STROKE */
	[448+0x4] = 0x143,	/* U+0144: LATIN SMALL LETTER N WITH ACUTE */
	[448+0x6] = 0x145,	/* U+0146: LATIN SMALL LETTER N WITH CEDILLA */
	[448+0x8] = 0x147,	/* U+0148: LATIN SMALL LETTER N WITH CARON */
	[448+0xB] = 0x14A,	/* U+014B: LATIN SMALL LETTER ENG */
	[448+0xD] = 0x14C,	/* U+014D: LATIN SMALL LETTER O WITH MACRON */
	[448+0xF] = 0x14E,	/* U+014F: LATIN SMALL LETTER O WITH BREVE */
	[448+0x11] = 0x150,	/* U+0151: LATIN SMALL LETTER O WITH DOUBLE ACUTE */
	[448+0x13] = 0x152,	/* U+0153: LATIN SMALL LIGATURE OE */
	[448+0x15] = 0x154,	/* U+0155: LATIN SMALL LETTER R WITH ACUTE */
	[448+0x17] = 0x156,	/* U+0157: LATIN SMALL LETTER R WITH CEDILLA */
	[448+0x19] = 0x158,	/* U+0159: LATIN SMALL LETTER R WITH CARON */
	[448+0x1B] = 0x15A,	/* U+015B: LATIN SMALL LETTER S WITH ACUTE */
	[448+0x1D] = 0x15C,	/* U+015D: LATIN SMALL LETTER S WITH CIRCUMFLEX */
	[448+0x1F] = 0x15E,	/* U+015F: LATIN SMALL LETTER S WITH CEDILLA */
	[448+0x21] = 0x160,	/* U+0161: LATIN SMALL LETTER S WITH CARON */
	[448+0x23] = 0x162,	/* U+0163: LATIN SMALL LETTER T WITH CEDILLA */
	[448+0x25] = 0x164,	/* U+0165: LATIN SMALL LETTER T WITH CARON */
	[448+0x27] = 0x166,	/* U+0167: LATIN SMALL LETTER T WITH STROKE */
	[448+0x29] = 0x168,	/* U+0169: LATIN SMALL LETTER U WITH TILDE */
	[448+0x2B] = 0x16A,	/* U+016B: LATIN SMALL LETTER U WITH MACRON */
	[448+0x2D] = 0x16C,	/* U+016D: LATIN SMALL LETTER U WITH BREVE */
	[448+0x2F] = 0x16E,	/* U+016F: LATIN SMALL LETTER U WITH RING ABOVE */
	[448+0x31] = 0x170,	/* U+0171: LATIN SMALL LETTER U WITH DOUBLE ACUTE */
	[448+0x33] = 0x172,	/* U+0173: LATIN SMALL LETTER U WITH OGONEK */
	[448+0x35] = 0x174,	/* U+0175: LATIN SMALL LETTER W WITH CIRCUMFLEX */
	[448+0x37] = 0x176,	/* U+0177: LATIN SMALL LETTER Y WITH CIRCUMFLEX */
	[448+0x3A] = 0x179,	/* U+017A: LATIN SMALL LETTER Z WITH ACUTE */
	[448+0x3C] = 0x17B,	/* U+017C: LATIN SMALL LETTER Z WITH DOT ABOVE */
	[448+0x3E] = 0x17D,	/* U+017E: LATIN SMALL LETTER Z WITH CARON */
	[448+0x3F] = 0x53,	/* U+017F: LATIN SMALL LETTER LONG S */
	[0xC6] = 512 - 0x80,	/* 306 ... */
	[512+0x0] = 0x243,	/* U+0180: LATIN SMALL LETTER B WITH STROKE */
	[512+0x3] = 0x182,	/* U+0183: LATIN SMALL LETTER B WITH TOPBAR */
	[512+0x5] = 0x184,	/* U+0185: LATIN SMALL LETTER TONE SIX */
	[512+0x8] = 0x187,	/* U+0188: LATIN SMALL LETTER C WITH HOOK */
	[512+0xC] = 0x18B,	/* U+018C: LATIN SMALL LETTER D WITH TOPBAR */
	[512+0x12] = 0x191,	/* U+0192: LATIN SMALL LETTER F WITH HOOK */
	[512+0x15] = 0x1F6,	/* U+0195: LATIN SMALL LETTER HV */
	[512+0x19] = 0x198,	/* U+0199: LATIN SMALL LETTER K WITH HOOK */
	[512+0x1A] = 0x23D,	/* U+019A: LATIN SMALL LETTER L WITH BAR */
	[512+0x1E] = 0x220,	/* U+019E: LATIN SMALL LETTER N WITH LONG RIGHT LEG */
	[512+0x21] = 0x1A0,	/* U+01A1: LATIN SMALL LETTER O WITH HORN */
	[512+0x23] = 0x1A2,	/* U+01A3: LATIN SMALL LETTER OI */
	[512+0x25] = 0x1A4,	/* U+01A5: LATIN SMALL LETTER P WITH HOOK */
	[512+0x28] = 0x1A7,	/* U+01A8: LATIN SMALL LETTER TONE TWO */
	[512+0x2D] = 0x1AC,	/* U+01AD: LATIN SMALL LETTER T WITH HOOK */
	[512+0x30] = 0x1AF,	/* U+01B0: LATIN SMALL LETTER U WITH HORN */
	[512+0x34] = 0x1B3,	/* U+01B4: LATIN SMALL LETTER Y WITH HOOK */
	[512+0x36] = 0x1B5,	/* U+01B6: LATIN SMALL LETTER Z WITH STROKE */
	[512+0x39] = 0x1B8,	/* U+01B9: LATIN SMALL LETTER EZH REVERSED */
	[512+0x3D] = 0x1BC,	/* U+01BD: LATIN SMALL LETTER TONE FIVE */
	[512+0x3F] = 0x1F7,	/* U+01BF: LATIN LETTER WYNN */
	[0xC7] = 576 - 0x80,	/* 307 ... */
	[576+0x5] = 0x1C4,	/* U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON */
	[576+0x6] = 0x1C4,	/* U+01C6: LATIN SMALL LETTER DZ WITH CARON */
	[576+0x8] = 0x1C7,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[576+0x9] = 0x1C7,	/* U+01C9: LATIN SMALL LETTER LJ */
	[576+0xB] = 0x1CA,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[576+0xC] = 0x1CA,	/* U+01CC: LATIN SMALL LETTER NJ */
	[576+0xE] = 0x1CD,	/* U+01CE: LATIN SMALL LETTER A WITH CARON */
	[576+0x10] = 0x1CF,	/* U+01D0: LATIN SMALL LETTER I WITH CARON */
	[576+0x12] = 0x1D1,	/* U+01D2: LATIN SMALL LETTER O WITH CARON */
	[576+0x14] = 0x1D3,	/* U+01D4: LATIN SMALL LETTER U WITH CARON */
	[576+0x16] = 0x1D5,	/* U+01D6: LATIN SMALL LETTER U WITH DIAERESIS AND MACRON */
	[576+0x18] = 0x1D7,	/* U+01D8: LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE */
	[576+0x1A] = 0x1D9,	/* U+01DA: LATIN SMALL LETTER U WITH DIAERESIS AND CARON */
	[576+0x1C] = 0x1DB,	/* U+01DC: LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE */
	[576+0x1D] = 0x18E,	/* U+01DD: LATIN SMALL LETTER TURNED E */
	[576+0x1F] = 0x1DE,	/* U+01DF: LATIN SMALL LETTER A WITH DIAERESIS AND MACRON */
	[576+0x21] = 0x1E0,	/* U+01E1: LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON */
	[576+0x23] = 0x1E2,	/* U+01E3: LATIN SMALL LETTER AE WITH MACRON */
	[576+0x25] = 0x1E4,	/* U+01E5: LATIN SMALL LETTER G WITH STROKE */
	[576+0x27] = 0x1E6,	/* U+01E7: LATIN SMALL LETTER G WITH CARON */
	[576+0x29] = 0x1E8,	/* U+01E9: LATIN SMALL LETTER K WITH CARON */
	[576+0x2B] = 0x1EA,	/* U+01EB: LATIN SMALL LETTER O WITH OGONEK */
	[576+0x2D] = 0x1EC,	/* U+01ED: LATIN SMALL LETTER O WITH OGONEK AND MACRON */
	[576+0x2F] = 0x1EE,	/* U+01EF: LATIN SMALL LETTER EZH WITH CARON */
	[576+0x32] = 0x1F1,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[576+0x33] = 0x1F1,	/* U+01F3: LATIN SMALL LETTER DZ */
	[576+0x35] = 0x1F4,	/* U+01F5: LATIN SMALL LETTER G WITH ACUTE */
	[576+0x39] = 0x1F8,	/* U+01F9: LATIN SMALL LETTER N WITH GRAVE */
	[576+0x3B] = 0x1FA,	/* U+01FB: LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE */
	[576+0x3D] = 0x1FC,	/* U+01FD: LATIN SMALL LETTER AE WITH ACUTE */
	[576+0x3F] = 0x1FE,	/* U+01FF: LATIN SMALL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 640 - 0x80,	/* 310 ... */
	[640+0x1] = 0x200,	/* U+0201: LATIN SMALL LETTER A WITH DOUBLE GRAVE */
	[640+0x3] = 0x202,	/* U+0203: LATIN SMALL LETTER A WITH INVERTED BREVE */
	[640+0x5] = 0x204,	/* U+0205: LATIN SMALL LETTER E WITH DOUBLE GRAVE */
	[640+0x7] = 0x206,	/* U+0207: LATIN SMALL LETTER E WITH INVERTED BREVE */
	[640+0x9] = 0x208,	/* U+0209: LATIN SMALL LETTER I WITH DOUBLE GRAVE */
	[640+0xB] = 0x20A,	/* U+020B: LATIN SMALL LETTER I WITH INVERTED BREVE */
	[640+0xD] = 0x20C,	/* U+020D: LATIN SMALL LETTER O WITH DOUBLE GRAVE */
	[640+0xF] = 0x20E,	/* U+020F: LATIN SMALL LETTER O WITH INVERTED BREVE */
	[640+0x11] = 0x210,	/* U+0211: LATIN SMALL LETTER R WITH DOUBLE GRAVE */
	[640+0x13] = 0x212,	/* U+0213: LATIN SMALL LETTER R WITH INVERTED BREVE */
	[640+0x15] = 0x214,	/* U+0215: LATIN SMALL LETTER U WITH DOUBLE GRAVE */
	[640+0x17] = 0x216,	/* U+0217: LATIN SMALL LETTER U WITH INVERTED BREVE */
	[640+0x19] = 0x218,	/* U+0219: LATIN SMALL LETTER S WITH COMMA BELOW */
	[640+0x1B] = 0x21A,	/* U+021B: LATIN SMALL LETTER T WITH COMMA BELOW */
	[640+0x1D] = 0x21C,	/* U+021D: LATIN SMALL LETTER YOGH */
	[640+0x1F] = 0x21E,	/* U+021F: LATIN SMALL LETTER H WITH CARON */
	[640+0x23] = 0x222,	/* U+0223: LATIN SMALL LETTER OU */
	[640+0x25] = 0x224,	/* U+0225: LATIN SMALL LETTER Z WITH HOOK */
	[640+0x27] = 0x226,	/* U+0227: LATIN SMALL LETTER A WITH DOT ABOVE */
	[640+0x29] = 0x228,	/* U+0229: LATIN SMALL LETTER E WITH CEDILLA */
	[640+0x2B] = 0x22A,	/* U+022B: LATIN SMALL LETTER O WITH DIAERESIS AND MACRON */
	[640+0x2D] = 0x22C,	/* U+022D: LATIN SMALL LETTER O WITH TILDE AND MACRON */
	[640+0x2F] = 0x22E,	/* U+022F: LATIN SMALL LETTER O WITH DOT ABOVE */
	[640+0x31] = 0x230,	/* U+0231: LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON */
	[640+0x33] = 0x232,	/* U+0233: LATIN SMALL LETTER Y WITH MACRON */
	[640+0x3C] = 0x23B,	/* U+023C: LATIN SMALL LETTER C WITH STROKE */
	[640+0x3F] = 0x2C7E,	/* U+023F: LATIN SMALL LETTER S WITH SWASH TAIL */
	[0xC9] = 704 - 0x80,	/* 311 ... */
	[704+0x0] = 0x2C7F,	/* U+0240: LATIN SMALL LETTER Z WITH SWASH TAIL */
	[704+0x2] = 0x241,	/* U+0242: LATIN SMALL LETTER GLOTTAL STOP */
	[704+0x7] = 0x246,	/* U+0247: LATIN SMALL LETTER E WITH STROKE */
	[704+0x9] = 0x248,	/* U+0249: LATIN SMALL LETTER J WITH STROKE */
	[704+0xB] = 0x24A,	/* U+024B: LATIN SMALL LETTER Q WITH HOOK TAIL */
	[704+0xD] = 0x24C,	/* U+024D: LATIN SMALL LETTER R WITH STROKE */
	[704+0xF] = 0x24E,	/* U+024F: LATIN SMALL LETTER Y WITH STROKE */
	[704+0x10] = 0x2C6F,	/* U+0250: LATIN SMALL LETTER TURNED A */
	[704+0x11] = 0x2C6D,	/* U+0251: LATIN SMALL LETTER ALPHA */
	[704+0x12] = 0x2C70,	/* U+0252: LATIN SMALL LETTER TURNED ALPHA */
	[704+0x13] = 0x181,	/* U+0253: LATIN SMALL LETTER B WITH HOOK */
	[704+0x14] = 0x186,	/* U+0254: LATIN SMALL LETTER OPEN O */
	[704+0x16] = 0x189,	/* U+0256: LATIN SMALL LETTER D WITH TAIL */
	[704+0x17] = 0x18A,	/* U+0257: LATIN SMALL LETTER D WITH HOOK */
	[704+0x19] = 0x18F,	/* U+0259: LATIN SMALL LETTER SCHWA */
	[704+0x1B] = 0x190,	/* U+025B: LATIN SMALL LETTER OPEN E */
	[704+0x1C] = 0xA7AB,	/* U+025C: LATIN SMALL LETTER REVERSED OPEN E */
	[704+0x20] = 0x193,	/* U+0260: LATIN SMALL LETTER G WITH HOOK */
	[704+0x21] = 0xA7AC,	/* U+0261: LATIN SMALL LETTER SCRIPT G */
	[704+0x23] = 0x194,	/* U+0263: LATIN SMALL LETTER GAMMA */
	[704+0x25] = 0xA78D,	/* U+0265: LATIN SMALL LETTER TURNED H */
	[704+0x26] = 0xA7AA,	/* U+0266: LATIN SMALL LETTER H WITH HOOK */
	[704+0x28] = 0x197,	/* U+0268: LATIN SMALL LETTER I WITH STROKE */
	[704+0x29] = 0x196,	/* U+0269: LATIN SMALL LETTER IOTA */
	[704+0x2A] = 0xA7AE,	/* U+026A: LATIN LETTER SMALL CAPITAL I */
	[704+0x2B] = 0x2C62,	/* U+026B: LATIN SMALL LETTER L WITH MIDDLE TILDE */
	[704+0x2C] = 0xA7AD,	/* U+026C: LATIN SMALL LETTER L WITH BELT */
	[704+0x2F] = 0x19C,	/* U+026F: LATIN SMALL LETTER TURNED M */
	[704+0x31] = 0x2C6E,	/* U+0271: LATIN SMALL LETTER M WITH HOOK */
	[704+0x32] = 0x19D,	/* U+0272: LATIN SMALL LETTER N WITH LEFT HOOK */
	[704+0x35] = 0x19F,	/* U+0275: LATIN SMALL LETTER BARRED O */
	[704+0x3D] = 0x2C64,	/* U+027D: LATIN SMALL LETTER R WITH TAIL */
	[0xCA] = 768 - 0x80,	/* 312 ... */
	[768+0x0] = 0x1A6,	/* U+0280: LATIN LETTER SMALL CAPITAL R */
	[768+0x2] = 0xA7C5,	/* U+0282: LATIN SMALL LETTER S WITH HOOK */
	[768+0x3] = 0x1A9,	/* U+0283: LATIN SMALL LETTER ESH */
	[768+0x7] = 0xA7B1,	/* U+0287: LATIN SMALL LETTER TURNED T */
	[768+0x8] = 0x1AE,	/* U+0288: LATIN SMALL LETTER T WITH RETROFLEX HOOK */
	[768+0x9] = 0x244,	/* U+0289: LATIN SMALL LETTER U BAR */
	[768+0xA] = 0x1B1,	/* U+028A: LATIN SMALL LETTER UPSILON */
	[768+0xB] = 0x1B2,	/* U+028B: LATIN SMALL LETTER V WITH HOOK */
	[768+0xC] = 0x245,	/* U+028C: LATIN SMALL LETTER TURNED V */
	[768+0x12] = 0x1B7,	/* U+0292: LATIN SMALL LETTER EZH */
	[768+0x1D] = 0xA7B2,	/* U+029D: LATIN SMALL LETTER J WITH CROSSED-TAIL */
	[768+0x1E] = 0xA7B0,	/* U+029E: LATIN SMALL LETTER TURNED K */
	[0xCD] = 832 - 0x80,	/* 315 ... */
	[832+0x5] = 0x399,	/* U+0345: COMBINING GREEK YPOGEGRAMMENI */
	[832+0x31] = 0x370,	/* U+0371: GREEK SMALL LETTER HETA */
	[832+0x33] = 0x372,	/* U+0373: GREEK SMALL LETTER ARCHAIC SAMPI */
	[832+0x37] = 0x376,	/* U+0377: GREEK SMALL LETTER PAMPHYLIAN DIGAMMA */
	[832+0x3B] = 0x3FD,	/* U+037B: GREEK SMALL REVERSED LUNATE SIGMA SYMBOL */
	[832+0x3C] = 0x3FE,	/* U+037C: GREEK SMALL DOTTED LUNATE SIGMA SYMBOL */
	[832+0x3D] = 0x3FF,	/* U+037D: GREEK SMALL REVERSED DOTTED LUNATE SIGMA SYMBOL */
	[0xCE] = 896 - 0x80,	/* 316 ... */
	[896+0x2C] = 0x386,	/* U+03AC: GREEK SMALL LETTER ALPHA WITH TONOS */
	[896+0x2D] = 0x388,	/* U+03AD: GREEK SMALL LETTER EPSILON WITH TONOS */
	[896+0x2E] = 0x389,	/* U+03AE: GREEK SMALL LETTER ETA WITH TONOS */
	[896+0x2F] = 0x38A,	/* U+03AF: GREEK SMALL LETTER IOTA WITH TONOS */
	[896+0x31] = 0x391,	/* U+03B1: GREEK SMALL LETTER ALPHA */
	[896+0x32] = 0x392,	/* U+03B2: GREEK SMALL LETTER BETA */
	[896+0x33] = 0x393,	/* U+03B3: GREEK SMALL LETTER GAMMA */
	[896+0x34] = 0x394,	/* U+03B4: GREEK SMALL LETTER DELTA */
	[896+0x35] = 0x395,	/* U+03B5: GREEK SMALL LETTER EPSILON */
	[896+0x36] = 0x396,	/* U+03B6: GREEK SMALL LETTER ZETA */
	[896+0x37] = 0x397,	/* U+03B7: GREEK SMALL LETTER ETA */
	[896+0x38] = 0x398,	/* U+03B8: GREEK SMALL LETTER THETA */
	[896+0x39] = 0x399,	/* U+03B9: GREEK SMALL LETTER IOTA */
	[896+0x3A] = 0x39A,	/* U+03BA: GREEK SMALL LETTER KAPPA */
	[896+0x3B] = 0x39B,	/* U+03BB: GREEK SMALL LETTER LAMDA */
	[896+0x3C] = 0x39C,	/* U+03BC: GREEK SMALL LETTER MU */
	[896+0x3D] = 0x39D,	/* U+03BD: GREEK SMALL LETTER NU */
	[896+0x3E] = 0x39E,	/* U+03BE: GREEK SMALL LETTER XI */
	[896+0x3F] = 0x39F,	/* U+03BF: GREEK SMALL LETTER OMICRON */
	[0xCF] = 960 - 0x80,	/* 317 ... */
	[960+0x0] = 0x3A0,	/* U+03C0: GREEK SMALL LETTER PI */
	[960+0x1] = 0x3A1,	/* U+03C1: GREEK SMALL LETTER RHO */
	[960+0x2] = 0x3A3,	/* U+03C2: GREEK SMALL LETTER FINAL SIGMA */
	[960+0x3] = 0x3A3,	/* U+03C3: GREEK SMALL LETTER SIGMA */
	[960+0x4] = 0x3A4,	/* U+03C4: GREEK SMALL LETTER TAU */
	[960+0x5] = 0x3A5,	/* U+03C5: GREEK SMALL LETTER UPSILON */
	[960+0x6] = 0x3A6,	/* U+03C6: GREEK SMALL LETTER PHI */
	[960+0x7] = 0x3A7,	/* U+03C7: GREEK SMALL LETTER CHI */
	[960+0x8] = 0x3A8,	/* U+03C8: GREEK SMALL LETTER PSI */
	[960+0x9] = 0x3A9,	/* U+03C9: GREEK SMALL LETTER OMEGA */
	[960+0xA] = 0x3AA,	/* U+03CA: GREEK SMALL LETTER IOTA WITH DIALYTIKA */
	[960+0xB] = 0x3AB,	/* U+03CB: GREEK SMALL LETTER UPSILON WITH DIALYTIKA */
	[960+0xC] = 0x38C,	/* U+03CC: GREEK SMALL LETTER OMICRON WITH TONOS */
	[960+0xD] = 0x38E,	/* U+03CD: GREEK SMALL LETTER UPSILON WITH TONOS */
	[960+0xE] = 0x38F,	/* U+03CE: GREEK SMALL LETTER OMEGA WITH TONOS */
	[960+0x10] = 0x392,	/* U+03D0: GREEK BETA SYMBOL */
	[960+0x11] = 0x398,	/* U+03D1: GREEK THETA SYMBOL */
	[960+0x15] = 0x3A6,	/* U+03D5: GREEK PHI SYMBOL */
	[960+0x16] = 0x3A0,	/* U+03D6: GREEK PI SYMBOL */
	[960+0x17] = 0x3CF,	/* U+03D7: GREEK KAI SYMBOL */
	[960+0x19] = 0x3D8,	/* U+03D9: GREEK SMALL LETTER ARCHAIC KOPPA */
	[960+0x1B] = 0x3DA,	/* U+03DB: GREEK SMALL LETTER STIGMA */
	[960+0x1D] = 0x3DC,	/* U+03DD: GREEK SMALL LETTER DIGAMMA */
	[960+0x1F] = 0x3DE,	/* U+03DF: GREEK SMALL LETTER KOPPA */
	[960+0x21] = 0x3E0,	/* U+03E1: GREEK SMALL LETTER SAMPI */
	[960+0x23] = 0x3E2,	/* U+03E3: COPTIC SMALL LETTER SHEI */
	[960+0x25] = 0x3E4,	/* U+03E5: COPTIC SMALL LETTER FEI */
	[960+0x27] = 0x3E6,	/* U+03E7: COPTIC SMALL LETTER KHEI */
	[960+0x29] = 0x3E8,	/* U+03E9: COPTIC SMALL LETTER HORI */
	[960+0x2B] = 0x3EA,	/* U+03EB: COPTIC SMALL LETTER GANGIA */
	[960+0x2D] = 0x3EC,	/* U+03ED: COPTIC SMALL LETTER SHIMA */
	[960+0x2F] = 0x3EE,	/* U+03EF: COPTIC SMALL LETTER DEI */
	[960+0x30] = 0x39A,	/* U+03F0: GREEK KAPPA SYMBOL */
	[960+0x31] = 0x3A1,	/* U+03F1: GREEK RHO SYMBOL */
	[960+0x32] = 0x3F9,	/* U+03F2: GREEK LUNATE SIGMA SYMBOL */
	[960+0x33] = 0x37F,	/* U+03F3: GREEK LETTER YOT */
	[960+0x35] = 0x395,	/* U+03F5: GREEK LUNATE EPSILON SYMBOL */
	[960+0x38] = 0x3F7,	/* U+03F8: GREEK SMALL LETTER SHO */
	[960+0x3B] = 0x3FA,	/* U+03FB: GREEK SMALL LETTER SAN */
	[0xD0] = 1024 - 0x80,	/* 320 ... */
	[1024+0x30] = 0x410,	/* U+0430: CYRILLIC SMALL LETTER A */
	[1024+0x31] = 0x411,	/* U+0431: CYRILLIC SMALL LETTER BE */
	[1024+0x32] = 0x412,	/* U+0432: CYRILLIC SMALL LETTER VE */
	[1024+0x33] = 0x413,	/* U+0433: CYRILLIC SMALL LETTER GHE */
	[1024+0x34] = 0x414,	/* U+0434: CYRILLIC SMALL LETTER DE */
	[1024+0x35] = 0x415,	/* U+0435: CYRILLIC SMALL LETTER IE */
	[1024+0x36] = 0x416,	/* U+0436: CYRILLIC SMALL LETTER ZHE */
	[1024+0x37] = 0x417,	/* U+0437: CYRILLIC SMALL LETTER ZE */
	[1024+0x38] = 0x418,	/* U+0438: CYRILLIC SMALL LETTER I */
	[1024+0x39] = 0x419,	/* U+0439: CYRILLIC SMALL LETTER SHORT I */
	[1024+0x3A] = 0x41A,	/* U+043A: CYRILLIC SMALL LETTER KA */
	[1024+0x3B] = 0x41B,	/* U+043B: CYRILLIC SMALL LETTER EL */
	[1024+0x3C] = 0x41C,	/* U+043C: CYRILLIC SMALL LETTER EM */
	[1024+0x3D] = 0x41D,	/* U+043D: CYRILLIC SMALL LETTER EN */
	[1024+0x3E] = 0x41E,	/* U+043E: CYRILLIC SMALL LETTER O */
	[1024+0x3F] = 0x41F,	/* U+043F: CYRILLIC SMALL LETTER PE */
	[0xD1] = 1088 - 0x80,	/* 321 ... */
	[1088+0x0] = 0x420,	/* U+0440: CYRILLIC SMALL LETTER ER */
	[1088+0x1] = 0x421,	/* U+0441: CYRILLIC SMALL LETTER ES */
	[1088+0x2] = 0x422,	/* U+0442: CYRILLIC SMALL LETTER TE */
	[1088+0x3] = 0x423,	/* U+0443: CYRILLIC SMALL LETTER U */
	[1088+0x4] = 0x424,	/* U+0444: CYRILLIC SMALL LETTER EF */
	[1088+0x5] = 0x425,	/* U+0445: CYRILLIC SMALL LETTER HA */
	[1088+0x6] = 0x426,	/* U+0446: CYRILLIC SMALL LETTER TSE */
	[1088+0x7] = 0x427,	/* U+0447: CYRILLIC SMALL LETTER CHE */
	[1088+0x8] = 0x428,	/* U+0448: CYRILLIC SMALL LETTER SHA */
	[1088+0x9] = 0x429,	/* U+0449: CYRILLIC SMALL LETTER SHCHA */
	[1088+0xA] = 0x42A,	/* U+044A: CYRILLIC SMALL LETTER HARD SIGN */
	[1088+0xB] = 0x42B,	/* U+044B: CYRILLIC SMALL LETTER YERU */
	[1088+0xC] = 0x42C,	/* U+044C: CYRILLIC SMALL LETTER SOFT SIGN */
	[1088+0xD] = 0x42D,	/* U+044D: CYRILLIC SMALL LETTER E */
	[1088+0xE] = 0x42E,	/* U+044E: CYRILLIC SMALL LETTER YU */
	[1088+0xF] = 0x42F,	/* U+044F: CYRILLIC SMALL LETTER YA */
	[1088+0x10] = 0x400,	/* U+0450: CYRILLIC SMALL LETTER IE WITH GRAVE */
	[1088+0x11] = 0x401,	/* U+0451: CYRILLIC SMALL LETTER IO */
	[1088+0x12] = 0x402,	/* U+0452: CYRILLIC SMALL LETTER DJE */
	[1088+0x13] = 0x403,	/* U+0453: CYRILLIC SMALL LETTER GJE */
	[1088+0x14] = 0x404,	/* U+0454: CYRILLIC SMALL LETTER UKRAINIAN IE */
	[1088+0x15] = 0x405,	/* U+0455: CYRILLIC SMALL LETTER DZE */
	[1088+0x16] = 0x406,	/* U+0456: CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I */
	[1088+0x17] = 0x407,	/* U+0457: CYRILLIC SMALL LETTER YI */
	[1088+0x18] = 0x408,	/* U+0458: CYRILLIC SMALL LETTER JE */
	[1088+0x19] = 0x409,	/* U+0459: CYRILLIC SMALL LETTER LJE */
	[1088+0x1A] = 0x40A,	/* U+045A: CYRILLIC SMALL LETTER NJE */
	[1088+0x1B] = 0x40B,	/* U+045B: CYRILLIC SMALL LETTER TSHE */
	[1088+0x1C] = 0x40C,	/* U+045C: CYRILLIC SMALL LETTER KJE */
	[1088+0x1D] = 0x40D,	/* U+045D: CYRILLIC SMALL LETTER I WITH GRAVE */
	[1088+0x1E] = 0x40E,	/* U+045E: CYRILLIC SMALL LETTER SHORT U */
	[1088+0x1F] = 0x40F,	/* U+045F: CYRILLIC SMALL LETTER DZHE */
	[1088+0x21] = 0x460,	/* U+0461: CYRILLIC SMALL LETTER OMEGA */
	[1088+0x23] = 0x462,	/* U+0463: CYRILLIC SMALL LETTER YAT */
	[1088+0x25] = 0x464,	/* U+0465: CYRILLIC SMALL LETTER IOTIFIED E */
	[1088+0x27] = 0x466,	/* U+0467: CYRILLIC SMALL LETTER LITTLE YUS */
	[1088+0x29] = 0x468,	/* U+0469: CYRILLIC SMALL LETTER IOTIFIED LITTLE YUS */
	[1088+0x2B] = 0x46A,	/* U+046B: CYRILLIC SMALL LETTER BIG YUS */
	[1088+0x2D] = 0x46C,	/* U+046D: CYRILLIC SMALL LETTER IOTIFIED BIG YUS */
	[1088+0x2F] = 0x46E,	/* U+046F: CYRILLIC SMALL LETTER KSI */
	[1088+0x31] = 0x470,	/* U+0471: CYRILLIC SMALL LETTER PSI */
	[1088+0x33] = 0x472,	/* U+0473: CYRILLIC SMALL LETTER FITA */
	[1088+0x35] = 0x474,	/* U+0475: CYRILLIC SMALL LETTER IZHITSA */
	[1088+0x37] = 0x476,	/* U+0477: CYRILLIC SMALL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT */
	[1088+0x39] = 0x478,	/* U+0479: CYRILLIC SMALL LETTER UK */
	[1088+0x3B] = 0x47A,	/* U+047B: CYRILLIC SMALL LETTER ROUND OMEGA */
	[1088+0x3D] = 0x47C,	/* U+047D: CYRILLIC SMALL LETTER OMEGA WITH TITLO */
	[1088+0x3F] = 0x47E,	/* U+047F: CYRILLIC SMALL LETTER OT */
	[0xD2] = 1152 - 0x80,	/* 322 ... */
	[1152+0x1] = 0x480,	/* U+0481: CYRILLIC SMALL LETTER KOPPA */
	[1152+0xB] = 0x48A,	/* U+048B: CYRILLIC SMALL LETTER SHORT I WITH TAIL */
	[1152+0xD] = 0x48C,	/* U+048D: CYRILLIC SMALL LETTER SEMISOFT SIGN */
	[1152+0xF] = 0x48E,	/* U+048F: CYRILLIC SMALL LETTER ER WITH TICK */
	[1152+0x11] = 0x490,	/* U+0491: CYRILLIC SMALL LETTER GHE WITH UPTURN */
	[1152+0x13] = 0x492,	/* U+0493: CYRILLIC SMALL LETTER GHE WITH STROKE */
	[1152+0x15] = 0x494,	/* U+0495: CYRILLIC SMALL LETTER GHE WITH MIDDLE HOOK */
	[1152+0x17] = 0x496,	/* U+0497: CYRILLIC SMALL LETTER ZHE WITH DESCENDER */
	[1152+0x19] = 0x498,	/* U+0499: CYRILLIC SMALL LETTER ZE WITH DESCENDER */
	[1152+0x1B] = 0x49A,	/* U+049B: CYRILLIC SMALL LETTER KA WITH DESCENDER */
	[1152+0x1D] = 0x49C,	/* U+049D: CYRILLIC SMALL LETTER KA WITH VERTICAL STROKE */
	[1152+0x1F] = 0x49E,	/* U+049F: CYRILLIC SMALL LETTER KA WITH STROKE */
	[1152+0x21] = 0x4A0,	/* U+04A1: CYRILLIC SMALL LETTER BASHKIR KA */
	[1152+0x23] = 0x4A2,	/* U+04A3: CYRILLIC SMALL LETTER EN WITH DESCENDER */
	[1152+0x25] = 0x4A4,	/* U+04A5: CYRILLIC SMALL LIGATURE EN GHE */
	[1152+0x27] = 0x4A6,	/* U+04A7: CYRILLIC SMALL LETTER PE WITH MIDDLE HOOK */
	[1152+0x29] = 0x4A8,	/* U+04A9: CYRILLIC SMALL LETTER ABKHASIAN HA */
	[1152+0x2B] = 0x4AA,	/* U+04AB: CYRILLIC SMALL LETTER ES WITH DESCENDER */
	[1152+0x2D] = 0x4AC,	/* U+04AD: CYRILLIC SMALL LETTER TE WITH DESCENDER */
	[1152+0x2F] = 0x4AE,	/* U+04AF: CYRILLIC SMALL LETTER STRAIGHT U */
	[1152+0x31] = 0x4B0,	/* U+04B1: CYRILLIC SMALL LETTER STRAIGHT U WITH STROKE */
	[1152+0x33] = 0x4B2,	/* U+04B3: CYRILLIC SMALL LETTER HA WITH DESCENDER */
	[1152+0x35] = 0x4B4,	/* U+04B5: CYRILLIC SMALL LIGATURE TE TSE */
	[1152+0x37] = 0x4B6,	/* U+04B7: CYRILLIC SMALL LETTER CHE WITH DESCENDER */
	[1152+0x39] = 0x4B8,	/* U+04B9: CYRILLIC SMALL LETTER CHE WITH VERTICAL STROKE */
	[1152+0x3B] = 0x4BA,	/* U+04BB: CYRILLIC SMALL LETTER SHHA */
	[1152+0x3D] = 0x4BC,	/* U+04BD: CYRILLIC SMALL LETTER ABKHASIAN CHE */
	[1152+0x3F] = 0x4BE,	/* U+04BF: CYRILLIC SMALL LETTER ABKHASIAN CHE WITH DESCENDER */
	[0xD3] = 1216 - 0x80,	/* 323 ... */
	[1216+0x2] = 0x4C1,	/* U+04C2: CYRILLIC SMALL LETTER ZHE WITH BREVE */
	[1216+0x4] = 0x4C3,	/* U+04C4: CYRILLIC SMALL LETTER KA WITH HOOK */
	[1216+0x6] = 0x4C5,	/* U+04C6: CYRILLIC SMALL LETTER EL WITH TAIL */
	[1216+0x8] = 0x4C7,	/* U+04C8: CYRILLIC SMALL LETTER EN WITH HOOK */
	[1216+0xA] = 0x4C9,	/* U+04CA: CYRILLIC SMALL LETTER EN WITH TAIL */
	[1216+0xC] = 0x4CB,	/* U+04CC: CYRILLIC SMALL LETTER KHAKASSIAN CHE */
	[1216+0xE] = 0x4CD,	/* U+04CE: CYRILLIC SMALL LETTER EM WITH TAIL */
	[1216+0xF] = 0x4C0,	/* U+04CF: CYRILLIC SMALL LETTER PALOCHKA */
	[1216+0x11] = 0x4D0,	/* U+04D1: CYRILLIC SMALL LETTER A WITH BREVE */
	[1216+0x13] = 0x4D2,	/* U+04D3: CYRILLIC SMALL LETTER A WITH DIAERESIS */
	[1216+0x15] = 0x4D4,	/* U+04D5: CYRILLIC SMALL LIGATURE A IE */
	[1216+0x17] = 0x4D6,	/* U+04D7: CYRILLIC SMALL LETTER IE WITH BREVE */
	[1216+0x19] = 0x4D8,	/* U+04D9: CYRILLIC SMALL LETTER SCHWA */
	[1216+0x1B] = 0x4DA,	/* U+04DB: CYRILLIC SMALL LETTER SCHWA WITH DIAERESIS */
	[1216+0x1D] = 0x4DC,	/* U+04DD: CYRILLIC SMALL LETTER ZHE WITH DIAERESIS */
	[1216+0x1F] = 0x4DE,	/* U+04DF: CYRILLIC SMALL LETTER ZE WITH DIAERESIS */
	[1216+0x21] = 0x4E0,	/* U+04E1: CYRILLIC SMALL LETTER ABKHASIAN DZE */
	[1216+0x23] = 0x4E2,	/* U+04E3: CYRILLIC SMALL LETTER I WITH MACRON */
	[1216+0x25] = 0x4E4,	/* U+04E5: CYRILLIC SMALL LETTER I WITH DIAERESIS */
	[1216+0x27] = 0x4E6,	/* U+04E7: CYRILLIC SMALL LETTER O WITH DIAERESIS */
	[1216+0x29] = 0x4E8,	/* U+04E9: CYRILLIC SMALL LETTER BARRED O */
	[1216+0x2B] = 0x4EA,	/* U+04EB: CYRILLIC SMALL LETTER BARRED O WITH DIAERESIS */
	[1216+0x2D] = 0x4EC,	/* U+04ED: CYRILLIC SMALL LETTER E WITH DIAERESIS */
	[1216+0x2F] = 0x4EE,	/* U+04EF: CYRILLIC SMALL LETTER U WITH MACRON */
	[1216+0x31] = 0x4F0,	/* U+04F1: CYRILLIC SMALL LETTER U WITH DIAERESIS */
	[1216+0x33] = 0x4F2,	/* U+04F3: CYRILLIC SMALL LETTER U WITH DOUBLE ACUTE */
	[1216+0x35] = 0x4F4,	/* U+04F5: CYRILLIC SMALL LETTER CHE WITH DIAERESIS */
	[1216+0x37] = 0x4F6,	/* U+04F7: CYRILLIC SMALL LETTER GHE WITH DESCENDER */
	[1216+0x39] = 0x4F8,	/* U+04F9: CYRILLIC SMALL LETTER YERU WITH DIAERESIS */
	[1216+0x3B] = 0x4FA,	/* U+04FB: CYRILLIC SMALL LETTER GHE WITH STROKE AND HOOK */
	[1216+0x3D] = 0x4FC,	/* U+04FD: CYRILLIC SMALL LETTER HA WITH HOOK */
	[1216+0x3F] = 0x4FE,	/* U+04FF: CYRILLIC SMALL LETTER HA WITH STROKE */
	[0xD4] = 1280 - 0x80,	/* 324 ... */
	[1280+0x1] = 0x500,	/* U+0501: CYRILLIC SMALL LETTER KOMI DE */
	[1280+0x3] = 0x502,	/* U+0503: CYRILLIC SMALL LETTER KOMI DJE */
	[1280+0x5] = 0x504,	/* U+0505: CYRILLIC SMALL LETTER KOMI ZJE */
	[1280+0x7] = 0x506,	/* U+0507: CYRILLIC SMALL LETTER KOMI DZJE */
	[1280+0x9] = 0x508,	/* U+0509: CYRILLIC SMALL LETTER KOMI LJE */
	[1280+0xB] = 0x50A,	/* U+050B: CYRILLIC SMALL LETTER KOMI NJE */
	[1280+0xD] = 0x50C,	/* U+050D: CYRILLIC SMALL LETTER KOMI SJE */
	[1280+0xF] = 0x50E,	/* U+050F: CYRILLIC SMALL LETTER KOMI TJE */
	[1280+0x11] = 0x510,	/* U+0511: CYRILLIC SMALL LETTER REVERSED ZE */
	[1280+0x13] = 0x512,	/* U+0513: CYRILLIC SMALL LETTER EL WITH HOOK */
	[1280+0x15] = 0x514,	/* U+0515: CYRILLIC SMALL LETTER LHA */
	[1280+0x17] = 0x516,	/* U+0517: CYRILLIC SMALL LETTER RHA */
	[1280+0x19] = 0x518,	/* U+0519: CYRILLIC SMALL LETTER YAE */
	[1280+0x1B] = 0x51A,	/* U+051B: CYRILLIC SMALL LETTER QA */
	[1280+0x1D] = 0x51C,	/* U+051D: CYRILLIC SMALL LETTER WE */
	[1280+0x1F] = 0x51E,	/* U+051F: CYRILLIC SMALL LETTER ALEUT KA */
	[1280+0x21] = 0x520,	/* U+0521: CYRILLIC SMALL LETTER EL WITH MIDDLE HOOK */
	[1280+0x23] = 0x522,	/* U+0523: CYRILLIC SMALL LETTER EN WITH MIDDLE HOOK */
	[1280+0x25] = 0x524,	/* U+0525: CYRILLIC SMALL LETTER PE WITH DESCENDER */
	[1280+0x27] = 0x526,	/* U+0527: CYRILLIC SMALL LETTER SHHA WITH DESCENDER */
	[1280+0x29] = 0x528,	/* U+0529: CYRILLIC SMALL LETTER EN WITH LEFT HOOK */
	[1280+0x2B] = 0x52A,	/* U+052B: CYRILLIC SMALL LETTER DZZHE */
	[1280+0x2D] = 0x52C,	/* U+052D: CYRILLIC SMALL LETTER DCHE */
	[1280+0x2F] = 0x52E,	/* U+052F: CYRILLIC SMALL LETTER EL WITH DESCENDER */
	[0xD5] = 1344 - 0x80,	/* 325 ... */
	[1344+0x21] = 0x531,	/* U+0561: ARMENIAN SMALL LETTER AYB */
	[1344+0x22] = 0x532,	/* U+0562: ARMENIAN SMALL LETTER BEN */
	[1344+0x23] = 0x533,	/* U+0563: ARMENIAN SMALL LETTER GIM */
	[1344+0x24] = 0x534,	/* U+0564: ARMENIAN SMALL LETTER DA */
	[1344+0x25] = 0x535,	/* U+0565: ARMENIAN SMALL LETTER ECH */
	[1344+0x26] = 0x536,	/* U+0566: ARMENIAN SMALL LETTER ZA */
	[1344+0x27] = 0x537,	/* U+0567: ARMENIAN SMALL LETTER EH */
	[1344+0x28] = 0x538,	/* U+0568: ARMENIAN SMALL LETTER ET */
	[1344+0x29] = 0x539,	/* U+0569: ARMENIAN SMALL LETTER TO */
	[1344+0x2A] = 0x53A,	/* U+056A: ARMENIAN SMALL LETTER ZHE */
	[1344+0x2B] = 0x53B,	/* U+056B: ARMENIAN SMALL LETTER INI */
	[1344+0x2C] = 0x53C,	/* U+056C: ARMENIAN SMALL LETTER LIWN */
	[1344+0x2D] = 0x53D,	/* U+056D: ARMENIAN SMALL LETTER XEH */
	[1344+0x2E] = 0x53E,	/* U+056E: ARMENIAN SMALL LETTER CA */
	[1344+0x2F] = 0x53F,	/* U+056F: ARMENIAN SMALL LETTER KEN */
	[1344+0x30] = 0x540,	/* U+0570: ARMENIAN SMALL LETTER HO */
	[1344+0x31] = 0x541,	/* U+0571: ARMENIAN SMALL LETTER JA */
	[1344+0x32] = 0x542,	/* U+0572: ARMENIAN SMALL LETTER GHAD */
	[1344+0x33] = 0x543,	/* U+0573: ARMENIAN SMALL LETTER CHEH */
	[1344+0x34] = 0x544,	/* U+0574: ARMENIAN SMALL LETTER MEN */
	[1344+0x35] = 0x545,	/* U+0575: ARMENIAN SMALL LETTER YI */
	[1344+0x36] = 0x546,	/* U+0576: ARMENIAN SMALL LETTER NOW */
	[1344+0x37] = 0x547,	/* U+0577: ARMENIAN SMALL LETTER SHA */
	[1344+0x38] = 0x548,	/* U+0578: ARMENIAN SMALL LETTER VO */
	[1344+0x39] = 0x549,	/* U+0579: ARMENIAN SMALL LETTER CHA */
	[1344+0x3A] = 0x54A,	/* U+057A: ARMENIAN SMALL LETTER PEH */
	[1344+0x3B] = 0x54B,	/* U+057B: ARMENIAN SMALL LETTER JHEH */
	[1344+0x3C] = 0x54C,	/* U+057C: ARMENIAN SMALL LETTER RA */
	[1344+0x3D] = 0x54D,	/* U+057D: ARMENIAN SMALL LETTER SEH */
	[1344+0x3E] = 0x54E,	/* U+057E: ARMENIAN SMALL LETTER VEW */
	[1344+0x3F] = 0x54F,	/* U+057F: ARMENIAN SMALL LETTER TIWN */
	[0xD6] = 1408 - 0x80,	/* 326 ... */
	[1408+0x0] = 0x550,	/* U+0580: ARMENIAN SMALL LETTER REH */
	[1408+0x1] = 0x551,	/* U+0581: ARMENIAN SMALL LETTER CO */
	[1408+0x2] = 0x552,	/* U+0582: ARMENIAN SMALL LETTER YIWN */
	[1408+0x3] = 0x553,	/* U+0583: ARMENIAN SMALL LETTER PIWR */
	[1408+0x4] = 0x554,	/* U+0584: ARMENIAN SMALL LETTER KEH */
	[1408+0x5] = 0x555,	/* U+0585: ARMENIAN SMALL LETTER OH */
	[1408+0x6] = 0x556,	/* U+0586: ARMENIAN SMALL LETTER FEH */
	[0xE1] = 1472 - 0x80,	/* 341 ... */
	[1472+0x3] = 1536 - 0x80,	/* 341 203 ... */
	[1536+0x10] = 0x1C90,	/* U+10D0: GEORGIAN LETTER AN */
	[1536+0x11] = 0x1C91,	/* U+10D1: GEORGIAN LETTER BAN */
	[1536+0x12] = 0x1C92,	/* U+10D2: GEORGIAN LETTER GAN */
	[1536+0x13] = 0x1C93,	/* U+10D3: GEORGIAN LETTER DON */
	[1536+0x14] = 0x1C94,	/* U+10D4: GEORGIAN LETTER EN */
	[1536+0x15] = 0x1C95,	/* U+10D5: GEORGIAN LETTER VIN */
	[1536+0x16] = 0x1C96,	/* U+10D6: GEORGIAN LETTER ZEN */
	[1536+0x17] = 0x1C97,	/* U+10D7: GEORGIAN LETTER TAN */
	[1536+0x18] = 0x1C98,	/* U+10D8: GEORGIAN LETTER IN */
	[1536+0x19] = 0x1C99,	/* U+10D9: GEORGIAN LETTER KAN */
	[1536+0x1A] = 0x1C9A,	/* U+10DA: GEORGIAN LETTER LAS */
	[1536+0x1B] = 0x1C9B,	/* U+10DB: GEORGIAN LETTER MAN */
	[1536+0x1C] = 0x1C9C,	/* U+10DC: GEORGIAN LETTER NAR */
	[1536+0x1D] = 0x1C9D,	/* U+10DD: GEORGIAN LETTER ON */
	[1536+0x1E] = 0x1C9E,	/* U+10DE: GEORGIAN LETTER PAR */
	[1536+0x1F] = 0x1C9F,	/* U+10DF: GEORGIAN LETTER ZHAR */
	[1536+0x20] = 0x1CA0,	/* U+10E0: GEORGIAN LETTER RAE */
	[1536+0x21] = 0x1CA1,	/* U+10E1: GEORGIAN LETTER SAN */
	[1536+0x22] = 0x1CA2,	/* U+10E2: GEORGIAN LETTER TAR */
	[1536+0x23] = 0x1CA3,	/* U+10E3: GEORGIAN LETTER UN */
	[1536+0x24] = 0x1CA4,	/* U+10E4: GEORGIAN LETTER PHAR */
	[1536+0x25] = 0x1CA5,	/* U+10E5: GEORGIAN LETTER KHAR */
	[1536+0x26] = 0x1CA6,	/* U+10E6: GEORGIAN LETTER GHAN */
	[1536+0x27] = 0x1CA7,	/* U+10E7: GEORGIAN LETTER QAR */
	[1536+0x28] = 0x1CA8,	/* U+10E8: GEORGIAN LETTER SHIN */
	[1536+0x29] = 0x1CA9,	/* U+10E9: GEORGIAN LETTER CHIN */
	[1536+0x2A] = 0x1CAA,	/* U+10EA: GEORGIAN LETTER CAN */
	[1536+0x2B] = 0x1CAB,	/* U+10EB: GEORGIAN LETTER JIL */
	[1536+0x2C] = 0x1CAC,	/* U+10EC: GEORGIAN LETTER CIL */
	[1536+0x2D] = 0x1CAD,	/* U+10ED: GEORGIAN LETTER CHAR */
	[1536+0x2E] = 0x1CAE,	/* U+10EE: GEORGIAN LETTER XAN */
	[1536+0x2F] = 0x1CAF,	/* U+10EF: GEORGIAN LETTER JHAN */
	[1536+0x30] = 0x1CB0,	/* U+10F0: GEORGIAN LETTER HAE */
	[1536+0x31] = 0x1CB1,	/* U+10F1: GEORGIAN LETTER HE */
	[1536+0x32] = 0x1CB2,	/* U+10F2: GEORGIAN LETTER HIE */
	[1536+0x33] = 0x1CB3,	/* U+10F3: GEORGIAN LETTER WE */
	[1536+0x34] = 0x1CB4,	/* U+10F4: GEORGIAN LETTER HAR */
	[1536+0x35] = 0x1CB5,	/* U+10F5: GEORGIAN LETTER HOE */
	[1536+0x36] = 0x1CB6,	/* U+10F6: GEORGIAN LETTER FI */
	[1536+0x37] = 0x1CB7,	/* U+10F7: GEORGIAN LETTER YN */
	[1536+0x38] = 0x1CB8,	/* U+10F8: GEORGIAN LETTER ELIFI */
	[1536+0x39] = 0x1CB9,	/* U+10F9: GEORGIAN LETTER TURNED GAN */
	[1536+0x3A] = 0x1CBA,	/* U+10FA: GEORGIAN LETTER AIN */
	[1536+0x3D] = 0x1CBD,	/* U+10FD: GEORGIAN LETTER AEN */
	[1536+0x3E] = 0x1CBE,	/* U+10FE: GEORGIAN LETTER HARD SIGN */
	[1536+0x3F] = 0x1CBF,	/* U+10FF: GEORGIAN LETTER LABIAL SIGN */
	[1472+0xF] = 1600 - 0x80,	/* 341 217 ... */
	[1600+0x38] = 0x13F0,	/* U+13F8: CHEROKEE SMALL LETTER YE */
	[1600+0x39] = 0x13F1,	/* U+13F9: CHEROKEE SMALL LETTER YI */
	[1600+0x3A] = 0x13F2,	/* U+13FA: CHEROKEE SMALL LETTER YO */
	[1600+0x3B] = 0x13F3,	/* U+13FB: CHEROKEE SMALL LETTER YU */
	[1600+0x3C] = 0x13F4,	/* U+13FC: CHEROKEE SMALL LETTER YV */
	[1600+0x3D] = 0x13F5,	/* U+13FD: CHEROKEE SMALL LETTER MV */
	[1472+0x32] = 1664 - 0x80,	/* 341 262 ... */
	[1664+0x0] = 0x412,	/* U+1C80: CYRILLIC SMALL LETTER ROUNDED VE */
	[1664+0x1] = 0x414,	/* U+1C81: CYRILLIC SMALL LETTER LONG-LEGGED DE */
	[1664+0x2] = 0x41E,	/* U+1C82: CYRILLIC SMALL LETTER NARROW O */
	[1664+0x3] = 0x421,	/* U+1C83: CYRILLIC SMALL LETTER WIDE ES */
	[1664+0x4] = 0x422,	/* U+1C84: CYRILLIC SMALL LETTER TALL TE */
	[1664+0x5] = 0x422,	/* U+1C85: CYRILLIC SMALL LETTER THREE-LEGGED TE */
	[1664+0x6] = 0x42A,	/* U+1C86: CYRILLIC SMALL LETTER TALL HARD SIGN */
	[1664+0x7] = 0x462,	/* U+1C87: CYRILLIC SMALL LETTER TALL YAT */
	[1664+0x8] = 0xA64A,	/* U+1C88: CYRILLIC SMALL LETTER UNBLENDED UK */
	[1472+0x35] = 1728 - 0x80,	/* 341 265 ... */
	[1728+0x39] = 0xA77D,	/* U+1D79: LATIN SMALL LETTER INSULAR G */
	[1728+0x3D] = 0x2C63,	/* U+1D7D: LATIN SMALL LETTER P WITH STROKE */
	[1472+0x36] = 1792 - 0x80,	/* 341 266 ... */
	[1792+0xE] = 0xA7C6,	/* U+1D8E: LATIN SMALL LETTER Z WITH PALATAL HOOK */
	[1472+0x38] = 1856 - 0x80,	/* 341 270 ... */
	[1856+0x1] = 0x1E00,	/* U+1E01: LATIN SMALL LETTER A WITH RING BELOW */
	[1856+0x3] = 0x1E02,	/* U+1E03: LATIN SMALL LETTER B WITH DOT ABOVE */
	[1856+0x5] = 0x1E04,	/* U+1E05: LATIN SMALL LETTER B WITH DOT BELOW */
	[1856+0x7] = 0x1E06,	/* U+1E07: LATIN SMALL LETTER B WITH LINE BELOW */
	[1856+0x9] = 0x1E08,	/* U+1E09: LATIN SMALL LETTER C WITH CEDILLA AND ACUTE */
	[1856+0xB] = 0x1E0A,	/* U+1E0B: LATIN SMALL LETTER D WITH DOT ABOVE */
	[1856+0xD] = 0x1E0C,	/* U+1E0D: LATIN SMALL LETTER D WITH DOT BELOW */
	[1856+0xF] = 0x1E0E,	/* U+1E0F: LATIN SMALL LETTER D WITH LINE BELOW */
	[1856+0x11] = 0x1E10,	/* U+1E11: LATIN SMALL LETTER D WITH CEDILLA */
	[1856+0x13] = 0x1E12,	/* U+1E13: LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW */
	[1856+0x15] = 0x1E14,	/* U+1E15: LATIN SMALL LETTER E WITH MACRON AND GRAVE */
	[1856+0x17] = 0x1E16,	/* U+1E17: LATIN SMALL LETTER E WITH MACRON AND ACUTE */
	[1856+0x19] = 0x1E18,	/* U+1E19: LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW */
	[1856+0x1B] = 0x1E1A,	/* U+1E1B: LATIN SMALL LETTER E WITH TILDE BELOW */
	[1856+0x1D] = 0x1E1C,	/* U+1E1D: LATIN SMALL LETTER E WITH CEDILLA AND BREVE */
	[1856+0x1F] = 0x1E1E,	/* U+1E1F: LATIN SMALL LETTER F WITH DOT ABOVE */
	[1856+0x21] = 0x1E20,	/* U+1E21: LATIN SMALL LETTER G WITH MACRON */
	[1856+0x23] = 0x1E22,	/* U+1E23: LATIN SMALL LETTER H WITH DOT ABOVE */
	[1856+0x25] = 0x1E24,	/* U+1E25: LATIN SMALL LETTER H WITH DOT BELOW */
	[1856+0x27] = 0x1E26,	/* U+1E27: LATIN SMALL LETTER H WITH DIAERESIS */
	[1856+0x29] = 0x1E28,	/* U+1E29: LATIN SMALL LETTER H WITH CEDILLA */
	[1856+0x2B] = 0x1E2A,	/* U+1E2B: LATIN SMALL LETTER H WITH BREVE BELOW */
	[1856+0x2D] = 0x1E2C,	/* U+1E2D: LATIN SMALL LETTER I WITH TILDE BELOW */
	[1856+0x2F] = 0x1E2E,	/* U+1E2F: LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE */
	[1856+0x31] = 0x1E30,	/* U+1E31: LATIN SMALL LETTER K WITH ACUTE */
	[1856+0x33] = 0x1E32,	/* U+1E33: LATIN SMALL LETTER K WITH DOT BELOW */
	[1856+0x35] = 0x1E34,	/* U+1E35: LATIN SMALL LETTER K WITH LINE BELOW */
	[1856+0x37] = 0x1E36,	/* U+1E37: LATIN SMALL LETTER L WITH DOT BELOW */
	[1856+0x39] = 0x1E38,	/* U+1E39: LATIN SMALL LETTER L WITH DOT BELOW AND MACRON */
	[1856+0x3B] = 0x1E3A,	/* U+1E3B: LATIN SMALL LETTER L WITH LINE BELOW */
	[1856+0x3D] = 0x1E3C,	/* U+1E3D: LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW */
	[1856+0x3F] = 0x1E3E,	/* U+1E3F: LATIN SMALL LETTER M WITH ACUTE */
	[1472+0x39] = 1920 - 0x80,	/* 341 271 ... */
	[1920+0x1] = 0x1E40,	/* U+1E41: LATIN SMALL LETTER M WITH DOT ABOVE */
	[1920+0x3] = 0x1E42,	/* U+1E43: LATIN SMALL LETTER M WITH DOT BELOW */
	[1920+0x5] = 0x1E44,	/* U+1E45: LATIN SMALL LETTER N WITH DOT ABOVE */
	[1920+0x7] = 0x1E46,	/* U+1E47: LATIN SMALL LETTER N WITH DOT BELOW */
	[1920+0x9] = 0x1E48,	/* U+1E49: LATIN SMALL LETTER N WITH LINE BELOW */
	[1920+0xB] = 0x1E4A,	/* U+1E4B: LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW */
	[1920+0xD] = 0x1E4C,	/* U+1E4D: LATIN SMALL LETTER O WITH TILDE AND ACUTE */
	[1920+0xF] = 0x1E4E,	/* U+1E4F: LATIN SMALL LETTER O WITH TILDE AND DIAERESIS */
	[1920+0x11] = 0x1E50,	/* U+1E51: LATIN SMALL LETTER O WITH MACRON AND GRAVE */
	[1920+0x13] = 0x1E52,	/* U+1E53: LATIN SMALL LETTER O WITH MACRON AND ACUTE */
	[1920+0x15] = 0x1E54,	/* U+1E55: LATIN SMALL LETTER P WITH ACUTE */
	[1920+0x17] = 0x1E56,	/* U+1E57: LATIN SMALL LETTER P WITH DOT ABOVE */
	[1920+0x19] = 0x1E58,	/* U+1E59: LATIN SMALL LETTER R WITH DOT ABOVE */
	[1920+0x1B] = 0x1E5A,	/* U+1E5B: LATIN SMALL LETTER R WITH DOT BELOW */
	[1920+0x1D] = 0x1E5C,	/* U+1E5D: LATIN SMALL LETTER R WITH DOT BELOW AND MACRON */
	[1920+0x1F] = 0x1E5E,	/* U+1E5F: LATIN SMALL LETTER R WITH LINE BELOW */
	[1920+0x21] = 0x1E60,	/* U+1E61: LATIN SMALL LETTER S WITH DOT ABOVE */
	[1920+0x23] = 0x1E62,	/* U+1E63: LATIN SMALL LETTER S WITH DOT BELOW */
	[1920+0x25] = 0x1E64,	/* U+1E65: LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE */
	[1920+0x27] = 0x1E66,	/* U+1E67: LATIN SMALL LETTER S WITH CARON AND DOT ABOVE */
	[1920+0x29] = 0x1E68,	/* U+1E69: LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE */
	[1920+0x2B] = 0x1E6A,	/* U+1E6B: LATIN SMALL LETTER T WITH DOT ABOVE */
	[1920+0x2D] = 0x1E6C,	/* U+1E6D: LATIN SMALL LETTER T WITH DOT BELOW */
	[1920+0x2F] = 0x1E6E,	/* U+1E6F: LATIN SMALL LETTER T WITH LINE BELOW */
	[1920+0x31] = 0x1E70,	/* U+1E71: LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW */
	[1920+0x33] = 0x1E72,	/* U+1E73: LATIN SMALL LETTER U WITH DIAERESIS BELOW */
	[1920+0x35] = 0x1E74,	/* U+1E75: LATIN SMALL LETTER U WITH TILDE BELOW */
	[1920+0x37] = 0x1E76,	/* U+1E77: LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW */
	[1920+0x39] = 0x1E78,	/* U+1E79: LATIN SMALL LETTER U WITH TILDE AND ACUTE */
	[1920+0x3B] = 0x1E7A,	/* U+1E7B: LATIN SMALL LETTER U WITH MACRON AND DIAERESIS */
	[1920+0x3D] = 0x1E7C,	/* U+1E7D: LATIN SMALL LETTER V WITH TILDE */
	[1920+0x3F] = 0x1E7E,	/* U+1E7F: LATIN SMALL LETTER V WITH DOT BELOW */
	[1472+0x3A] = 1984 - 0x80,	/* 341 272 ... */
	[1984+0x1] = 0x1E80,	/* U+1E81: LATIN SMALL LETTER W WITH GRAVE */
	[1984+0x3] = 0x1E82,	/* U+1E83: LATIN SMALL LETTER W WITH ACUTE */
	[1984+0x5] = 0x1E84,	/* U+1E85: LATIN SMALL LETTER W WITH DIAERESIS */
	[1984+0x7] = 0x1E86,	/* U+1E87: LATIN SMALL LETTER W WITH DOT ABOVE */
	[1984+0x9] = 0x1E88,	/* U+1E89: LATIN SMALL LETTER W WITH DOT BELOW */
	[1984+0xB] = 0x1E8A,	/* U+1E8B: LATIN SMALL LETTER X WITH DOT ABOVE */
	[1984+0xD] = 0x1E8C,	/* U+1E8D: LATIN SMALL LETTER X WITH DIAERESIS */
	[1984+0xF] = 0x1E8E,	/* U+1E8F: LATIN SMALL LETTER Y WITH DOT ABOVE */
	[1984+0x11] = 0x1E90,	/* U+1E91: LATIN SMALL LETTER Z WITH CIRCUMFLEX */
	[1984+0x13] = 0x1E92,	/* U+1E93: LATIN SMALL LETTER Z WITH DOT BELOW */
	[1984+0x15] = 0x1E94,	/* U+1E95: LATIN SMALL LETTER Z WITH LINE BELOW */
	[1984+0x1B] = 0x1E60,	/* U+1E9B: LATIN SMALL LETTER LONG S WITH DOT ABOVE */
	[1984+0x21] = 0x1EA0,	/* U+1EA1: LATIN SMALL LETTER A WITH DOT BELOW */
	[1984+0x23] = 0x1EA2,	/* U+1EA3: LATIN SMALL LETTER A WITH HOOK ABOVE */
	[1984+0x25] = 0x1EA4,	/* U+1EA5: LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE */
	[1984+0x27] = 0x1EA6,	/* U+1EA7: LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE */
	[1984+0x29] = 0x1EA8,	/* U+1EA9: LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE */
	[1984+0x2B] = 0x1EAA,	/* U+1EAB: LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE */
	[1984+0x2D] = 0x1EAC,	/* U+1EAD: LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW */
	[1984+0x2F] = 0x1EAE,	/* U+1EAF: LATIN SMALL LETTER A WITH BREVE AND ACUTE */
	[1984+0x31] = 0x1EB0,	/* U+1EB1: LATIN SMALL LETTER A WITH BREVE AND GRAVE */
	[1984+0x33] = 0x1EB2,	/* U+1EB3: LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE */
	[1984+0x35] = 0x1EB4,	/* U+1EB5: LATIN SMALL LETTER A WITH BREVE AND TILDE */
	[1984+0x37] = 0x1EB6,	/* U+1EB7: LATIN SMALL LETTER A WITH BREVE AND DOT BELOW */
	[1984+0x39] = 0x1EB8,	/* U+1EB9: LATIN SMALL LETTER E WITH DOT BELOW */
	[1984+0x3B] = 0x1EBA,	/* U+1EBB: LATIN SMALL LETTER E WITH HOOK ABOVE */
	[1984+0x3D] = 0x1EBC,	/* U+1EBD: LATIN SMALL LETTER E WITH TILDE */
	[1984+0x3F] = 0x1EBE,	/* U+1EBF: LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE */
	[1472+0x3B] = 2048 - 0x80,	/* 341 273 ... */
	[2048+0x1] = 0x1EC0,	/* U+1EC1: LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[2048+0x3] = 0x1EC2,	/* U+1EC3: LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[2048+0x5] = 0x1EC4,	/* U+1EC5: LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE */
	[2048+0x7] = 0x1EC6,	/* U+1EC7: LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[2048+0x9] = 0x1EC8,	/* U+1EC9: LATIN SMALL LETTER I WITH HOOK ABOVE */
	[2048+0xB] = 0x1ECA,	/* U+1ECB: LATIN SMALL LETTER I WITH DOT BELOW */
	[2048+0xD] = 0x1ECC,	/* U+1ECD: LATIN SMALL LETTER O WITH DOT BELOW */
	[2048+0xF] = 0x1ECE,	/* U+1ECF: LATIN SMALL LETTER O WITH HOOK ABOVE */
	[2048+0x11] = 0x1ED0,	/* U+1ED1: LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE */
	[2048+0x13] = 0x1ED2,	/* U+1ED3: LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE */
	[2048+0x15] = 0x1ED4,	/* U+1ED5: LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE */
	[2048+0x17] = 0x1ED6,	/* U+1ED7: LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE */
	[2048+0x19] = 0x1ED8,	/* U+1ED9: LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW */
	[2048+0x1B] = 0x1EDA,	/* U+1EDB: LATIN SMALL LETTER O WITH HORN AND ACUTE */
	[2048+0x1D] = 0x1EDC,	/* U+1EDD: LATIN SMALL LETTER O WITH HORN AND GRAVE */
	[2048+0x1F] = 0x1EDE,	/* U+1EDF: LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE */
	[2048+0x21] = 0x1EE0,	/* U+1EE1: LATIN SMALL LETTER O WITH HORN AND TILDE */
	[2048+0x23] = 0x1EE2,	/* U+1EE3: LATIN SMALL LETTER O WITH HORN AND DOT BELOW */
	[2048+0x25] = 0x1EE4,	/* U+1EE5: LATIN SMALL LETTER U WITH DOT BELOW */
	[2048+0x27] = 0x1EE6,	/* U+1EE7: LATIN SMALL LETTER U WITH HOOK ABOVE */
	[2048+0x29] = 0x1EE8,	/* U+1EE9: LATIN SMALL LETTER U WITH HORN AND ACUTE */
	[2048+0x2B] = 0x1EEA,	/* U+1EEB: LATIN SMALL LETTER U WITH HORN AND GRAVE */
	[2048+0x2D] = 0x1EEC,	/* U+1EED: LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE */
	[2048+0x2F] = 0x1EEE,	/* U+1EEF: LATIN SMALL LETTER U WITH HORN AND TILDE */
	[2048+0x31] = 0x1EF0,	/* U+1EF1: LATIN SMALL LETTER U WITH HORN AND DOT BELOW */
	[2048+0x33] = 0x1EF2,	/* U+1EF3: LATIN SMALL LETTER Y WITH GRAVE */
	[2048+0x35] = 0x1EF4,	/* U+1EF5: LATIN SMALL LETTER Y WITH DOT BELOW */
	[2048+0x37] = 0x1EF6,	/* U+1EF7: LATIN SMALL LETTER Y WITH HOOK ABOVE */
	[2048+0x39] = 0x1EF8,	/* U+1EF9: LATIN SMALL LETTER Y WITH TILDE */
	[2048+0x3B] = 0x1EFA,	/* U+1EFB: LATIN SMALL LETTER MIDDLE-WELSH LL */
	[2048+0x3D] = 0x1EFC,	/* U+1EFD: LATIN SMALL LETTER MIDDLE-WELSH V */
	[2048+0x3F] = 0x1EFE,	/* U+1EFF: LATIN SMALL LETTER Y WITH LOOP */
	[1472+0x3C] = 2112 - 0x80,	/* 341 274 ... */
	[2112+0x0] = 0x1F08,	/* U+1F00: GREEK SMALL LETTER ALPHA WITH PSILI */
	[2112+0x1] = 0x1F09,	/* U+1F01: GREEK SMALL LETTER ALPHA WITH DASIA */
	[2112+0x2] = 0x1F0A,	/* U+1F02: GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA */
	[2112+0x3] = 0x1F0B,	/* U+1F03: GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA */
	[2112+0x4] = 0x1F0C,	/* U+1F04: GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA */
	[2112+0x5] = 0x1F0D,	/* U+1F05: GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA */
	[2112+0x6] = 0x1F0E,	/* U+1F06: GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI */
	[2112+0x7] = 0x1F0F,	/* U+1F07: GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI */
	[2112+0x10] = 0x1F18,	/* U+1F10: GREEK SMALL LETTER EPSILON WITH PSILI */
	[2112+0x11] = 0x1F19,	/* U+1F11: GREEK SMALL LETTER EPSILON WITH DASIA */
	[2112+0x12] = 0x1F1A,	/* U+1F12: GREEK SMALL LETTER EPSILON WITH PSILI AND VARIA */
	[2112+0x13] = 0x1F1B,	/* U+1F13: GREEK SMALL LETTER EPSILON WITH DASIA AND VARIA */
	[2112+0x14] = 0x1F1C,	/* U+1F14: GREEK SMALL LETTER EPSILON WITH PSILI AND OXIA */
	[2112+0x15] = 0x1F1D,	/* U+1F15: GREEK SMALL LETTER EPSILON WITH DASIA AND OXIA */
	[2112+0x20] = 0x1F28,	/* U+1F20: GREEK SMALL LETTER ETA WITH PSILI */
	[2112+0x21] = 0x1F29,	/* U+1F21: GREEK SMALL LETTER ETA WITH DASIA */
	[2112+0x22] = 0x1F2A,	/* U+1F22: GREEK SMALL LETTER ETA WITH PSILI AND VARIA */
	[2112+0x23] = 0x1F2B,	/* U+1F23: GREEK SMALL LETTER ETA WITH DASIA AND VARIA */
	[2112+0x24] = 0x1F2C,	/* U+1F24: GREEK SMALL LETTER ETA WITH PSILI AND OXIA */
	[2112+0x25] = 0x1F2D,	/* U+1F25: GREEK SMALL LETTER ETA WITH DASIA AND OXIA */
	[2112+0x26] = 0x1F2E,	/* U+1F26: GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI */
	[2112+0x27] = 0x1F2F,	/* U+1F27: GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI */
	[2112+0x30] = 0x1F38,	/* U+1F30: GREEK SMALL LETTER IOTA WITH PSILI */
	[2112+0x31] = 0x1F39,	/* U+1F31: GREEK SMALL LETTER IOTA WITH DASIA */
	[2112+0x32] = 0x1F3A,	/* U+1F32: GREEK SMALL LETTER IOTA WITH PSILI AND VARIA */
	[2112+0x33] = 0x1F3B,	/* U+1F33: GREEK SMALL LETTER IOTA WITH DASIA AND VARIA */
	[2112+0x34] = 0x1F3C,	/* U+1F34: GREEK SMALL LETTER IOTA WITH PSILI AND OXIA */
	[2112+0x35] = 0x1F3D,	/* U+1F35: GREEK SMALL LETTER IOTA WITH DASIA AND OXIA */
	[2112+0x36] = 0x1F3E,	/* U+1F36: GREEK SMALL LETTER IOTA WITH PSILI AND PERISPOMENI */
	[2112+0x37] = 0x1F3F,	/* U+1F37: GREEK SMALL LETTER IOTA WITH DASIA AND PERISPOMENI */
	[1472+0x3D] = 2176 - 0x80,	/* 341 275 ... */
	[2176+0x0] = 0x1F48,	/* U+1F40: GREEK SMALL LETTER OMICRON WITH PSILI */
	[2176+0x1] = 0x1F49,	/* U+1F41: GREEK SMALL LETTER OMICRON WITH DASIA */
	[2176+0x2] = 0x1F4A,	/* U+1F42: GREEK SMALL LETTER OMICRON WITH PSILI AND VARIA */
	[2176+0x3] = 0x1F4B,	/* U+1F43: GREEK SMALL LETTER OMICRON WITH DASIA AND VARIA */
	[2176+0x4] = 0x1F4C,	/* U+1F44: GREEK SMALL LETTER OMICRON WITH PSILI AND OXIA */
	[2176+0x5] = 0x1F4D,	/* U+1F45: GREEK SMALL LETTER OMICRON WITH DASIA AND OXIA */
	[2176+0x11] = 0x1F59,	/* U+1F51: GREEK SMALL LETTER UPSILON WITH DASIA */
	[2176+0x13] = 0x1F5B,	/* U+1F53: GREEK SMALL LETTER UPSILON WITH DASIA AND VARIA */
	[2176+0x15] = 0x1F5D,	/* U+1F55: GREEK SMALL LETTER UPSILON WITH DASIA AND OXIA */
	[2176+0x17] = 0x1F5F,	/* U+1F57: GREEK SMALL LETTER UPSILON WITH DASIA AND PERISPOMENI */
	[2176+0x20] = 0x1F68,	/* U+1F60: GREEK SMALL LETTER OMEGA WITH PSILI */
	[2176+0x21] = 0x1F69,	/* U+1F61: GREEK SMALL LETTER OMEGA WITH DASIA */
	[2176+0x22] = 0x1F6A,	/* U+1F62: GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA */
	[2176+0x23] = 0x1F6B,	/* U+1F63: GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA */
	[2176+0x24] = 0x1F6C,	/* U+1F64: GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA */
	[2176+0x25] = 0x1F6D,	/* U+1F65: GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA */
	[2176+0x26] = 0x1F6E,	/* U+1F66: GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI */
	[2176+0x27] = 0x1F6F,	/* U+1F67: GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI */
	[2176+0x30] = 0x1FBA,	/* U+1F70: GREEK SMALL LETTER ALPHA WITH VARIA */
	[2176+0x31] = 0x1FBB,	/* U+1F71: GREEK SMALL LETTER ALPHA WITH OXIA */
	[2176+0x32] = 0x1FC8,	/* U+1F72: GREEK SMALL LETTER EPSILON WITH VARIA */
	[2176+0x33] = 0x1FC9,	/* U+1F73: GREEK SMALL LETTER EPSILON WITH OXIA */
	[2176+0x34] = 0x1FCA,	/* U+1F74: GREEK SMALL LETTER ETA WITH VARIA */
	[2176+0x35] = 0x1FCB,	/* U+1F75: GREEK SMALL LETTER ETA WITH OXIA */
	[2176+0x36] = 0x1FDA,	/* U+1F76: GREEK SMALL LETTER IOTA WITH VARIA */
	[2176+0x37] = 0x1FDB,	/* U+1F77: GREEK SMALL LETTER IOTA WITH OXIA */
	[2176+0x38] = 0x1FF8,	/* U+1F78: GREEK SMALL LETTER OMICRON WITH VARIA */
	[2176+0x39] = 0x1FF9,	/* U+1F79: GREEK SMALL LETTER OMICRON WITH OXIA */
	[2176+0x3A] = 0x1FEA,	/* U+1F7A: GREEK SMALL LETTER UPSILON WITH VARIA */
	[2176+0x3B] = 0x1FEB,	/* U+1F7B: GREEK SMALL LETTER UPSILON WITH OXIA */
	[2176+0x3C] = 0x1FFA,	/* U+1F7C: GREEK SMALL LETTER OMEGA WITH VARIA */
	[2176+0x3D] = 0x1FFB,	/* U+1F7D: GREEK SMALL LETTER OMEGA WITH OXIA */
	[1472+0x3E] = 2240 - 0x80,	/* 341 276 ... */
	[2240+0x0] = 0x1F88,	/* U+1F80: GREEK SMALL LETTER ALPHA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x1] = 0x1F89,	/* U+1F81: GREEK SMALL LETTER ALPHA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x2] = 0x1F8A,	/* U+1F82: GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x3] = 0x1F8B,	/* U+1F83: GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x4] = 0x1F8C,	/* U+1F84: GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x5] = 0x1F8D,	/* U+1F85: GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x6] = 0x1F8E,	/* U+1F86: GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x7] = 0x1F8F,	/* U+1F87: GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x10] = 0x1F98,	/* U+1F90: GREEK SMALL LETTER ETA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x11] = 0x1F99,	/* U+1F91: GREEK SMALL LETTER ETA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x12] = 0x1F9A,	/* U+1F92: GREEK SMALL LETTER ETA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x13] = 0x1F9B,	/* U+1F93: GREEK SMALL LETTER ETA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x14] = 0x1F9C,	/* U+1F94: GREEK SMALL LETTER ETA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x15] = 0x1F9D,	/* U+1F95: GREEK SMALL LETTER ETA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x16] = 0x1F9E,	/* U+1F96: GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x17] = 0x1F9F,	/* U+1F97: GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x20] = 0x1FA8,	/* U+1FA0: GREEK SMALL LETTER OMEGA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x21] = 0x1FA9,	/* U+1FA1: GREEK SMALL LETTER OMEGA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x22] = 0x1FAA,	/* U+1FA2: GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x23] = 0x1FAB,	/* U+1FA3: GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x24] = 0x1FAC,	/* U+1FA4: GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x25] = 0x1FAD,	/* U+1FA5: GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x26] = 0x1FAE,	/* U+1FA6: GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x27] = 0x1FAF,	/* U+1FA7: GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x30] = 0x1FB8,	/* U+1FB0: GREEK SMALL LETTER ALPHA WITH VRACHY */
	[2240+0x31] = 0x1FB9,	/* U+1FB1: GREEK SMALL LETTER ALPHA WITH MACRON */
	[2240+0x33] = 0x1FBC,	/* U+1FB3: GREEK SMALL LETTER ALPHA WITH YPOGEGRAMMENI */
	[2240+0x3E] = 0x399,	/* U+1FBE: GREEK PROSGEGRAMMENI */
	[1472+0x3F] = 2304 - 0x80,	/* 341 277 ... */
	[2304+0x3] = 0x1FCC,	/* U+1FC3: GREEK SMALL LETTER ETA WITH YPOGEGRAMMENI */
	[2304+0x10] = 0x1FD8,	/* U+1FD0: GREEK SMALL LETTER IOTA WITH VRACHY */
	[2304+0x11] = 0x1FD9,	/* U+1FD1: GREEK SMALL LETTER IOTA WITH MACRON */
	[2304+0x20] = 0x1FE8,	/* U+1FE0: GREEK SMALL LETTER UPSILON WITH VRACHY */
	[2304+0x21] = 0x1FE9,	/* U+1FE1: GREEK SMALL LETTER UPSILON WITH MACRON */
	[2304+0x25] = 0x1FEC,	/* U+1FE5: GREEK SMALL LETTER RHO WITH DASIA */
	[2304+0x33] = 0x1FFC,	/* U+1FF3: GREEK SMALL LETTER OMEGA WITH YPOGEGRAMMENI */
	[0xE2] = 2368 - 0x80,	/* 342 ... */
	[2368+0x5] = 2432 - 0x80,	/* 342 205 ... */
	[2432+0xE] = 0x2132,	/* U+214E: TURNED SMALL F */
	[2432+0x30] = 0x2160,	/* U+2170: SMALL ROMAN NUMERAL ONE */
	[2432+0x31] = 0x2161,	/* U+2171: SMALL ROMAN NUMERAL TWO */
	[2432+0x32] = 0x2162,	/* U+2172: SMALL ROMAN NUMERAL THREE */
	[2432+0x33] = 0x2163,	/* U+2173: SMALL ROMAN NUMERAL FOUR */
	[2432+0x34] = 0x2164,	/* U+2174: SMALL ROMAN NUMERAL FIVE */
	[2432+0x35] = 0x2165,	/* U+2175: SMALL ROMAN NUMERAL SIX */
	[2432+0x36] = 0x2166,	/* U+2176: SMALL ROMAN NUMERAL SEVEN */
	[2432+0x37] = 0x2167,	/* U+2177: SMALL ROMAN NUMERAL EIGHT */
	[2432+0x38] = 0x2168,	/* U+2178: SMALL ROMAN NUMERAL NINE */
	[2432+0x39] = 0x2169,	/* U+2179: SMALL ROMAN NUMERAL TEN */
	[2432+0x3A] = 0x216A,	/* U+217A: SMALL ROMAN NUMERAL ELEVEN */
	[2432+0x3B] = 0x216B,	/* U+217B: SMALL ROMAN NUMERAL TWELVE */
	[2432+0x3C] = 0x216C,	/* U+217C: SMALL ROMAN NUMERAL FIFTY */
	[2432+0x3D] = 0x216D,	/* U+217D: SMALL ROMAN NUMERAL ONE HUNDRED */
	[2432+0x3E] = 0x216E,	/* U+217E: SMALL ROMAN NUMERAL FIVE HUNDRED */
	[2432+0x3F] = 0x216F,	/* U+217F: SMALL ROMAN NUMERAL ONE THOUSAND */
	[2368+0x6] = 2496 - 0x80,	/* 342 206 ... */
	[2496+0x4] = 0x2183,	/* U+2184: LATIN SMALL LETTER REVERSED C */
	[2368+0x13] = 2560 - 0x80,	/* 342 223 ... */
	[2560+0x10] = 0x24B6,	/* U+24D0: CIRCLED LATIN SMALL LETTER A */
	[2560+0x11] = 0x24B7,	/* U+24D1: CIRCLED LATIN SMALL LETTER B */
	[2560+0x12] = 0x24B8,	/* U+24D2: CIRCLED LATIN SMALL LETTER C */
	[2560+0x13] = 0x24B9,	/* U+24D3: CIRCLED LATIN SMALL LETTER D */
	[2560+0x14] = 0x24BA,	/* U+24D4: CIRCLED LATIN SMALL LETTER E */
	[2560+0x15] = 0x24BB,	/* U+24D5: CIRCLED LATIN SMALL LETTER F */
	[2560+0x16] = 0x24BC,	/* U+24D6: CIRCLED LATIN SMALL LETTER G */
	[2560+0x17] = 0x24BD,	/* U+24D7: CIRCLED LATIN SMALL LETTER H */
	[2560+0x18] = 0x24BE,	/* U+24D8: CIRCLED LATIN SMALL LETTER I */
	[2560+0x19] = 0x24BF,	/* U+24D9: CIRCLED LATIN SMALL LETTER J */
	[2560+0x1A] = 0x24C0,	/* U+24DA: CIRCLED LATIN SMALL LETTER K */
	[2560+0x1B] = 0x24C1,	/* U+24DB: CIRCLED LATIN SMALL LETTER L */
	[2560+0x1C] = 0x24C2,	/* U+24DC: CIRCLED LATIN SMALL LETTER M */
	[2560+0x1D] = 0x24C3,	/* U+24DD: CIRCLED LATIN SMALL LETTER N */
	[2560+0x1E] = 0x24C4,	/* U+24DE: CIRCLED LATIN SMALL LETTER O */
	[2560+0x1F] = 0x24C5,	/* U+24DF: CIRCLED LATIN SMALL LETTER P */
	[2560+0x20] = 0x24C6,	/* U+24E0: CIRCLED LATIN SMALL LETTER Q */
	[2560+0x21] = 0x24C7,	/* U+24E1: CIRCLED LATIN SMALL LETTER R */
	[2560+0x22] = 0x24C8,	/* U+24E2: CIRCLED LATIN SMALL LETTER S */
	[2560+0x23] = 0x24C9,	/* U+24E3: CIRCLED LATIN SMALL LETTER T */
	[2560+0x24] = 0x24CA,	/* U+24E4: CIRCLED LATIN SMALL LETTER U */
	[2560+0x25] = 0x24CB,	/* U+24E5: CIRCLED LATIN SMALL LETTER V */
	[2560+0x26] = 0x24CC,	/* U+24E6: CIRCLED LATIN SMALL LETTER W */
	[2560+0x27] = 0x24CD,	/* U+24E7: CIRCLED LATIN SMALL LETTER X */
	[2560+0x28] = 0x24CE,	/* U+24E8: CIRCLED LATIN SMALL LETTER Y */
	[2560+0x29] = 0x24CF,	/* U+24E9: CIRCLED LATIN SMALL LETTER Z */
	[2368+0x30] = 2624 - 0x80,	/* 342 260 ... */
	[2624+0x30] = 0x2C00,	/* U+2C30: GLAGOLITIC SMALL LETTER AZU */
	[2624+0x31] = 0x2C01,	/* U+2C31: GLAGOLITIC SMALL LETTER BUKY */
	[2624+0x32] = 0x2C02,	/* U+2C32: GLAGOLITIC SMALL LETTER VEDE */
	[2624+0x33] = 0x2C03,	/* U+2C33: GLAGOLITIC SMALL LETTER GLAGOLI */
	[2624+0x34] = 0x2C04,	/* U+2C34: GLAGOLITIC SMALL LETTER DOBRO */
	[2624+0x35] = 0x2C05,	/* U+2C35: GLAGOLITIC SMALL LETTER YESTU */
	[2624+0x36] = 0x2C06,	/* U+2C36: GLAGOLITIC SMALL LETTER ZHIVETE */
	[2624+0x37] = 0x2C07,	/* U+2C37: GLAGOLITIC SMALL LETTER DZELO */
	[2624+0x38] = 0x2C08,	/* U+2C38: GLAGOLITIC SMALL LETTER ZEMLJA */
	[2624+0x39] = 0x2C09,	/* U+2C39: GLAGOLITIC SMALL LETTER IZHE */
	[2624+0x3A] = 0x2C0A,	/* U+2C3A: GLAGOLITIC SMALL LETTER INITIAL IZHE */
	[2624+0x3B] = 0x2C0B,	/* U+2C3B: GLAGOLITIC SMALL LETTER I */
	[2624+0x3C] = 0x2C0C,	/* U+2C3C: GLAGOLITIC SMALL LETTER DJERVI */
	[2624+0x3D] = 0x2C0D,	/* U+2C3D: GLAGOLITIC SMALL LETTER KAKO */
	[2624+0x3E] = 0x2C0E,	/* U+2C3E: GLAGOLITIC SMALL LETTER LJUDIJE */
	[2624+0x3F] = 0x2C0F,	/* U+2C3F: GLAGOLITIC SMALL LETTER MYSLITE */
	[2368+0x31] = 2688 - 0x80,	/* 342 261 ... */
	[2688+0x0] = 0x2C10,	/* U+2C40: GLAGOLITIC SMALL LETTER NASHI */
	[2688+0x1] = 0x2C11,	/* U+2C41: GLAGOLITIC SMALL LETTER ONU */
	[2688+0x2] = 0x2C12,	/* U+2C42: GLAGOLITIC SMALL LETTER POKOJI */
	[2688+0x3] = 0x2C13,	/* U+2C43: GLAGOLITIC SMALL LETTER RITSI */
	[2688+0x4] = 0x2C14,	/* U+2C44: GLAGOLITIC SMALL LETTER SLOVO */
	[2688+0x5] = 0x2C15,	/* U+2C45: GLAGOLITIC SMALL LETTER TVRIDO */
	[2688+0x6] = 0x2C16,	/* U+2C46: GLAGOLITIC SMALL LETTER UKU */
	[2688+0x7] = 0x2C17,	/* U+2C47: GLAGOLITIC SMALL LETTER FRITU */
	[2688+0x8] = 0x2C18,	/* U+2C48: GLAGOLITIC SMALL LETTER HERU */
	[2688+0x9] = 0x2C19,	/* U+2C49: GLAGOLITIC SMALL LETTER OTU */
	[2688+0xA] = 0x2C1A,	/* U+2C4A: GLAGOLITIC SMALL LETTER PE */
	[2688+0xB] = 0x2C1B,	/* U+2C4B: GLAGOLITIC SMALL LETTER SHTA */
	[2688+0xC] = 0x2C1C,	/* U+2C4C: GLAGOLITIC SMALL LETTER TSI */
	[2688+0xD] = 0x2C1D,	/* U+2C4D: GLAGOLITIC SMALL LETTER CHRIVI */
	[2688+0xE] = 0x2C1E,	/* U+2C4E: GLAGOLITIC SMALL LETTER SHA */
	[2688+0xF] = 0x2C1F,	/* U+2C4F: GLAGOLITIC SMALL LETTER YERU */
	[2688+0x10] = 0x2C20,	/* U+2C50: GLAGOLITIC SMALL LETTER YERI */
	[2688+0x11] = 0x2C21,	/* U+2C51: GLAGOLITIC SMALL LETTER YATI */
	[2688+0x12] = 0x2C22,	/* U+2C52: GLAGOLITIC SMALL LETTER SPIDERY HA */
	[2688+0x13] = 0x2C23,	/* U+2C53: GLAGOLITIC SMALL LETTER YU */
	[2688+0x14] = 0x2C24,	/* U+2C54: GLAGOLITIC SMALL LETTER SMALL YUS */
	[2688+0x15] = 0x2C25,	/* U+2C55: GLAGOLITIC SMALL LETTER SMALL YUS WITH TAIL */
	[2688+0x16] = 0x2C26,	/* U+2C56: GLAGOLITIC SMALL LETTER YO */
	[2688+0x17] = 0x2C27,	/* U+2C57: GLAGOLITIC SMALL LETTER IOTATED SMALL YUS */
	[2688+0x18] = 0x2C28,	/* U+2C58: GLAGOLITIC SMALL LETTER BIG YUS */
	[2688+0x19] = 0x2C29,	/* U+2C59: GLAGOLITIC SMALL LETTER IOTATED BIG YUS */
	[2688+0x1A] = 0x2C2A,	/* U+2C5A: GLAGOLITIC SMALL LETTER FITA */
	[2688+0x1B] = 0x2C2B,	/* U+2C5B: GLAGOLITIC SMALL LETTER IZHITSA */
	[2688+0x1C] = 0x2C2C,	/* U+2C5C: GLAGOLITIC SMALL LETTER SHTAPIC */
	[2688+0x1D] = 0x2C2D,	/* U+2C5D: GLAGOLITIC SMALL LETTER TROKUTASTI A */
	[2688+0x1E] = 0x2C2E,	/* U+2C5E: GLAGOLITIC SMALL LETTER LATINATE MYSLITE */
	[2688+0x1F] = 0x2C2F,	/* U+2C5F: GLAGOLITIC SMALL LETTER CAUDATE CHRIVI */
	[2688+0x21] = 0x2C60,	/* U+2C61: LATIN SMALL LETTER L WITH DOUBLE BAR */
	[2688+0x25] = 0x23A,	/* U+2C65: LATIN SMALL LETTER A WITH STROKE */
	[2688+0x26] = 0x23E,	/* U+2C66: LATIN SMALL LETTER T WITH DIAGONAL STROKE */
	[2688+0x28] = 0x2C67,	/* U+2C68: LATIN SMALL LETTER H WITH DESCENDER */
	[2688+0x2A] = 0x2C69,	/* U+2C6A: LATIN SMALL LETTER K WITH DESCENDER */
	[2688+0x2C] = 0x2C6B,	/* U+2C6C: LATIN SMALL LETTER Z WITH DESCENDER */
	[2688+0x33] = 0x2C72,	/* U+2C73: LATIN SMALL LETTER W WITH HOOK */
	[2688+0x36] = 0x2C75,	/* U+2C76: LATIN SMALL LETTER HALF H */
	[2368+0x32] = 2752 - 0x80,	/* 342 262 ... */
	[2752+0x1] = 0x2C80,	/* U+2C81: COPTIC SMALL LETTER ALFA */
	[2752+0x3] = 0x2C82,	/* U+2C83: COPTIC SMALL LETTER VIDA */
	[2752+0x5] = 0x2C84,	/* U+2C85: COPTIC SMALL LETTER GAMMA */
	[2752+0x7] = 0x2C86,	/* U+2C87: COPTIC SMALL LETTER DALDA */
	[2752+0x9] = 0x2C88,	/* U+2C89: COPTIC SMALL LETTER EIE */
	[2752+0xB] = 0x2C8A,	/* U+2C8B: COPTIC SMALL LETTER SOU */
	[2752+0xD] = 0x2C8C,	/* U+2C8D: COPTIC SMALL LETTER ZATA */
	[2752+0xF] = 0x2C8E,	/* U+2C8F: COPTIC SMALL LETTER HATE */
	[2752+0x11] = 0x2C90,	/* U+2C91: COPTIC SMALL LETTER THETHE */
	[2752+0x13] = 0x2C92,	/* U+2C93: COPTIC SMALL LETTER IAUDA */
	[2752+0x15] = 0x2C94,	/* U+2C95: COPTIC SMALL LETTER KAPA */
	[2752+0x17] = 0x2C96,	/* U+2C97: COPTIC SMALL LETTER LAULA */
	[2752+0x19] = 0x2C98,	/* U+2C99: COPTIC SMALL LETTER MI */
	[2752+0x1B] = 0x2C9A,	/* U+2C9B: COPTIC SMALL LETTER NI */
	[2752+0x1D] = 0x2C9C,	/* U+2C9D: COPTIC SMALL LETTER KSI */
	[2752+0x1F] = 0x2C9E,	/* U+2C9F: COPTIC SMALL LETTER O */
	[2752+0x21] = 0x2CA0,	/* U+2CA1: COPTIC SMALL LETTER PI */
	[2752+0x23] = 0x2CA2,	/* U+2CA3: COPTIC SMALL LETTER RO */
	[2752+0x25] = 0x2CA4,	/* U+2CA5: COPTIC SMALL LETTER SIMA */
	[2752+0x27] = 0x2CA6,	/* U+2CA7: COPTIC SMALL LETTER TAU */
	[2752+0x29] = 0x2CA8,	/* U+2CA9: COPTIC SMALL LETTER UA */
	[2752+0x2B] = 0x2CAA,	/* U+2CAB: COPTIC SMALL LETTER FI */
	[2752+0x2D] = 0x2CAC,	/* U+2CAD: COPTIC SMALL LETTER KHI */
	[2752+0x2F] = 0x2CAE,	/* U+2CAF: COPTIC SMALL LETTER PSI */
	[2752+0x31] = 0x2CB0,	/* U+2CB1: COPTIC SMALL LETTER OOU */
	[2752+0x33] = 0x2CB2,	/* U+2CB3: COPTIC SMALL LETTER DIALECT-P ALEF */
	[2752+0x35] = 0x2CB4,	/* U+2CB5: COPTIC SMALL LETTER OLD COPTIC AIN */
	[2752+0x37] = 0x2CB6,	/* U+2CB7: COPTIC SMALL LETTER CRYPTOGRAMMIC EIE */
	[2752+0x39] = 0x2CB8,	/* U+2CB9: COPTIC SMALL LETTER DIALECT-P KAPA */
	[2752+0x3B] = 0x2CBA,	/* U+2CBB: COPTIC SMALL LETTER DIALECT-P NI */
	[2752+0x3D] = 0x2CBC,	/* U+2CBD: COPTIC SMALL LETTER CRYPTOGRAMMIC NI */
	[2752+0x3F] = 0x2CBE,	/* U+2CBF: COPTIC SMALL LETTER OLD COPTIC OOU */
	[2368+0x33] = 2816 - 0x80,	/* 342 263 ... */
	[2816+0x1] = 0x2CC0,	/* U+2CC1: COPTIC SMALL LETTER SAMPI */
	[2816+0x3] = 0x2CC2,	/* U+2CC3: COPTIC SMALL LETTER CROSSED SHEI */
	[2816+0x5] = 0x2CC4,	/* U+2CC5: COPTIC SMALL LETTER OLD COPTIC SHEI */
	[2816+0x7] = 0x2CC6,	/* U+2CC7: COPTIC SMALL LETTER OLD COPTIC ESH */
	[2816+0x9] = 0x2CC8,	/* U+2CC9: COPTIC SMALL LETTER AKHMIMIC KHEI */
	[2816+0xB] = 0x2CCA,	/* U+2CCB: COPTIC SMALL LETTER DIALECT-P HORI */
	[2816+0xD] = 0x2CCC,	/* U+2CCD: COPTIC SMALL LETTER OLD COPTIC HORI */
	[2816+0xF] = 0x2CCE,	/* U+2CCF: COPTIC SMALL LETTER OLD COPTIC HA */
	[2816+0x11] = 0x2CD0,	/* U+2CD1: COPTIC SMALL LETTER L-SHAPED HA */
	[2816+0x13] = 0x2CD2,	/* U+2CD3: COPTIC SMALL LETTER OLD COPTIC HEI */
	[2816+0x15] = 0x2CD4,	/* U+2CD5: COPTIC SMALL LETTER OLD COPTIC HAT */
	[2816+0x17] = 0x2CD6,	/* U+2CD7: COPTIC SMALL LETTER OLD COPTIC GANGIA */
	[2816+0x19] = 0x2CD8,	/* U+2CD9: COPTIC SMALL LETTER OLD COPTIC DJA */
	[2816+0x1B] = 0x2CDA,	/* U+2CDB: COPTIC SMALL LETTER OLD COPTIC SHIMA */
	[2816+0x1D] = 0x2CDC,	/* U+2CDD: COPTIC SMALL LETTER OLD NUBIAN SHIMA */
	[2816+0x1F] = 0x2CDE,	/* U+2CDF: COPTIC SMALL LETTER OLD NUBIAN NGI */
	[2816+0x21] = 0x2CE0,	/* U+2CE1: COPTIC SMALL LETTER OLD NUBIAN NYI */
	[2816+0x23] = 0x2CE2,	/* U+2CE3: COPTIC SMALL LETTER OLD NUBIAN WAU */
	[2816+0x2C] = 0x2CEB,	/* U+2CEC: COPTIC SMALL LETTER CRYPTOGRAMMIC SHEI */
	[2816+0x2E] = 0x2CED,	/* U+2CEE: COPTIC SMALL LETTER CRYPTOGRAMMIC GANGIA */
	[2816+0x33] = 0x2CF2,	/* U+2CF3: COPTIC SMALL LETTER BOHAIRIC KHEI */
	[2368+0x34] = 2880 - 0x80,	/* 342 264 ... */
	[2880+0x0] = 0x10A0,	/* U+2D00: GEORGIAN SMALL LETTER AN */
	[2880+0x1] = 0x10A1,	/* U+2D01: GEORGIAN SMALL LETTER BAN */
	[2880+0x2] = 0x10A2,	/* U+2D02: GEORGIAN SMALL LETTER GAN */
	[2880+0x3] = 0x10A3,	/* U+2D03: GEORGIAN SMALL LETTER DON */
	[2880+0x4] = 0x10A4,	/* U+2D04: GEORGIAN SMALL LETTER EN */
	[2880+0x5] = 0x10A5,	/* U+2D05: GEORGIAN SMALL LETTER VIN */
	[2880+0x6] = 0x10A6,	/* U+2D06: GEORGIAN SMALL LETTER ZEN */
	[2880+0x7] = 0x10A7,	/* U+2D07: GEORGIAN SMALL LETTER TAN */
	[2880+0x8] = 0x10A8,	/* U+2D08: GEORGIAN SMALL LETTER IN */
	[2880+0x9] = 0x10A9,	/* U+2D09: GEORGIAN SMALL LETTER KAN */
	[2880+0xA] = 0x10AA,	/* U+2D0A: GEORGIAN SMALL LETTER LAS */
	[2880+0xB] = 0x10AB,	/* U+2D0B: GEORGIAN SMALL LETTER MAN */
	[2880+0xC] = 0x10AC,	/* U+2D0C: GEORGIAN SMALL LETTER NAR */
	[2880+0xD] = 0x10AD,	/* U+2D0D: GEORGIAN SMALL LETTER ON */
	[2880+0xE] = 0x10AE,	/* U+2D0E: GEORGIAN SMALL LETTER PAR */
	[2880+0xF] = 0x10AF,	/* U+2D0F: GEORGIAN SMALL LETTER ZHAR */
	[2880+0x10] = 0x10B0,	/* U+2D10: GEORGIAN SMALL LETTER RAE */
	[2880+0x11] = 0x10B1,	/* U+2D11: GEORGIAN SMALL LETTER SAN */
	[2880+0x12] = 0x10B2,	/* U+2D12: GEORGIAN SMALL LETTER TAR */
	[2880+0x13] = 0x10B3,	/* U+2D13: GEORGIAN SMALL LETTER UN */
	[2880+0x14] = 0x10B4,	/* U+2D14: GEORGIAN SMALL LETTER PHAR */
	[2880+0x15] = 0x10B5,	/* U+2D15: GEORGIAN SMALL LETTER KHAR */
	[2880+0x16] = 0x10B6,	/* U+2D16: GEORGIAN SMALL LETTER GHAN */
	[2880+0x17] = 0x10B7,	/* U+2D17: GEORGIAN SMALL LETTER QAR */
	[2880+0x18] = 0x10B8,	/* U+2D18: GEORGIAN SMALL LETTER SHIN */
	[2880+0x19] = 0x10B9,	/* U+2D19: GEORGIAN SMALL LETTER CHIN */
	[2880+0x1A] = 0x10BA,	/* U+2D1A: GEORGIAN SMALL LETTER CAN */
	[2880+0x1B] = 0x10BB,	/* U+2D1B: GEORGIAN SMALL LETTER JIL */
	[2880+0x1C] = 0x10BC,	/* U+2D1C: GEORGIAN SMALL LETTER CIL */
	[2880+0x1D] = 0x10BD,	/* U+2D1D: GEORGIAN SMALL LETTER CHAR */
	[2880+0x1E] = 0x10BE,	/* U+2D1E: GEORGIAN SMALL LETTER XAN */
	[2880+0x1F] = 0x10BF,	/* U+2D1F: GEORGIAN SMALL LETTER JHAN */
	[2880+0x20] = 0x10C0,	/* U+2D20: GEORGIAN SMALL LETTER HAE */
	[2880+0x21] = 0x10C1,	/* U+2D21: GEORGIAN SMALL LETTER HE */
	[2880+0x22] = 0x10C2,	/* U+2D22: GEORGIAN SMALL LETTER HIE */
	[2880+0x23] = 0x10C3,	/* U+2D23: GEORGIAN SMALL LETTER WE */
	[2880+0x24] = 0x10C4,	/* U+2D24: GEORGIAN SMALL LETTER HAR */
	[2880+0x25] = 0x10C5,	/* U+2D25: GEORGIAN SMALL LETTER HOE */
	[2880+0x27] = 0x10C7,	/* U+2D27: GEORGIAN SMALL LETTER YN */
	[2880+0x2D] = 0x10CD,	/* U+2D2D: GEORGIAN SMALL LETTER AEN */
	[0xEA] = 2944 - 0x80,	/* 352 ... */
	[2944+0x19] = 3008 - 0x80,	/* 352 231 ... */
	[3008+0x1] = 0xA640,	/* U+A641: CYRILLIC SMALL LETTER ZEMLYA */
	[3008+0x3] = 0xA642,	/* U+A643: CYRILLIC SMALL LETTER DZELO */
	[3008+0x5] = 0xA644,	/* U+A645: CYRILLIC SMALL LETTER REVERSED DZE */
	[3008+0x7] = 0xA646,	/* U+A647: CYRILLIC SMALL LETTER IOTA */
	[3008+0x9] = 0xA648,	/* U+A649: CYRILLIC SMALL LETTER DJERV */
	[3008+0xB] = 0xA64A,	/* U+A64B: CYRILLIC SMALL LETTER MONOGRAPH UK */
	[3008+0xD] = 0xA64C,	/* U+A64D: CYRILLIC SMALL LETTER BROAD OMEGA */
	[3008+0xF] = 0xA64E,	/* U+A64F: CYRILLIC SMALL LETTER NEUTRAL YER */
	[3008+0x11] = 0xA650,	/* U+A651: CYRILLIC SMALL LETTER YERU WITH BACK YER */
	[3008+0x13] = 0xA652,	/* U+A653: CYRILLIC SMALL LETTER IOTIFIED YAT */
	[3008+0x15] = 0xA654,	/* U+A655: CYRILLIC SMALL LETTER REVERSED YU */
	[3008+0x17] = 0xA656,	/* U+A657: CYRILLIC SMALL LETTER IOTIFIED A */
	[3008+0x19] = 0xA658,	/* U+A659: CYRILLIC SMALL LETTER CLOSED LITTLE YUS */
	[3008+0x1B] = 0xA65A,	/* U+A65B: CYRILLIC SMALL LETTER BLENDED YUS */
	[3008+0x1D] = 0xA65C,	/* U+A65D: CYRILLIC SMALL LETTER IOTIFIED CLOSED LITTLE YUS */
	[3008+0x1F] = 0xA65E,	/* U+A65F: CYRILLIC SMALL LETTER YN */
	[3008+0x21] = 0xA660,	/* U+A661: CYRILLIC SMALL LETTER REVERSED TSE */
	[3008+0x23] = 0xA662,	/* U+A663: CYRILLIC SMALL LETTER SOFT DE */
	[3008+0x25] = 0xA664,	/* U+A665: CYRILLIC SMALL LETTER SOFT EL */
	[3008+0x27] = 0xA666,	/* U+A667: CYRILLIC SMALL LETTER SOFT EM */
	[3008+0x29] = 0xA668,	/* U+A669: CYRILLIC SMALL LETTER MONOCULAR O */
	[3008+0x2B] = 0xA66A,	/* U+A66B: CYRILLIC SMALL LETTER BINOCULAR O */
	[3008+0x2D] = 0xA66C,	/* U+A66D: CYRILLIC SMALL LETTER DOUBLE MONOCULAR O */
	[2944+0x1A] = 3072 - 0x80,	/* 352 232 ... */
	[3072+0x1] = 0xA680,	/* U+A681: CYRILLIC SMALL LETTER DWE */
	[3072+0x3] = 0xA682,	/* U+A683: CYRILLIC SMALL LETTER DZWE */
	[3072+0x5] = 0xA684,	/* U+A685: CYRILLIC SMALL LETTER ZHWE */
	[3072+0x7] = 0xA686,	/* U+A687: CYRILLIC SMALL LETTER CCHE */
	[3072+0x9] = 0xA688,	/* U+A689: CYRILLIC SMALL LETTER DZZE */
	[3072+0xB] = 0xA68A,	/* U+A68B: CYRILLIC SMALL LETTER TE WITH MIDDLE HOOK */
	[3072+0xD] = 0xA68C,	/* U+A68D: CYRILLIC SMALL LETTER TWE */
	[3072+0xF] = 0xA68E,	/* U+A68F: CYRILLIC SMALL LETTER TSWE */
	[3072+0x11] = 0xA690,	/* U+A691: CYRILLIC SMALL LETTER TSSE */
	[3072+0x13] = 0xA692,	/* U+A693: CYRILLIC SMALL LETTER TCHE */
	[3072+0x15] = 0xA694,	/* U+A695: CYRILLIC SMALL LETTER HWE */
	[3072+0x17] = 0xA696,	/* U+A697: CYRILLIC SMALL LETTER SHWE */
	[3072+0x19] = 0xA698,	/* U+A699: CYRILLIC SMALL LETTER DOUBLE O */
	[3072+0x1B] = 0xA69A,	/* U+A69B: CYRILLIC SMALL LETTER CROSSED O */
	[2944+0x1C] = 3136 - 0x80,	/* 352 234 ... */
	[3136+0x23] = 0xA722,	/* U+A723: LATIN SMALL LETTER EGYPTOLOGICAL ALEF */
	[3136+0x25] = 0xA724,	/* U+A725: LATIN SMALL LETTER EGYPTOLOGICAL AIN */
	[3136+0x27] = 0xA726,	/* U+A727: LATIN SMALL LETTER HENG */
	[3136+0x29] = 0xA728,	/* U+A729: LATIN SMALL LETTER TZ */
	[3136+0x2B] = 0xA72A,	/* U+A72B: LATIN SMALL LETTER TRESILLO */
	[3136+0x2D] = 0xA72C,	/* U+A72D: LATIN SMALL LETTER CUATRILLO */
	[3136+0x2F] = 0xA72E,	/* U+A72F: LATIN SMALL LETTER CUATRILLO WITH COMMA */
	[3136+0x33] = 0xA732,	/* U+A733: LATIN SMALL LETTER AA */
	[3136+0x35] = 0xA734,	/* U+A735: LATIN SMALL LETTER AO */
	[3136+0x37] = 0xA736,	/* U+A737: LATIN SMALL LETTER AU */
	[3136+0x39] = 0xA738,	/* U+A739: LATIN SMALL LETTER AV */
	[3136+0x3B] = 0xA73A,	/* U+A73B: LATIN SMALL LETTER AV WITH HORIZONTAL BAR */
	[3136+0x3D] = 0xA73C,	/* U+A73D: LATIN SMALL LETTER AY */
	[3136+0x3F] = 0xA73E,	/* U+A73F: LATIN SMALL LETTER REVERSED C WITH DOT */
	[2944+0x1D] = 3200 - 0x80,	/* 352 235 ... */
	[3200+0x1] = 0xA740,	/* U+A741: LATIN SMALL LETTER K WITH STROKE */
	[3200+0x3] = 0xA742,	/* U+A743: LATIN SMALL LETTER K WITH DIAGONAL STROKE */
	[3200+0x5] = 0xA744,	/* U+A745: LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE */
	[3200+0x7] = 0xA746,	/* U+A747: LATIN SMALL LETTER BROKEN L */
	[3200+0x9] = 0xA748,	/* U+A749: LATIN SMALL LETTER L WITH HIGH STROKE */
	[3200+0xB] = 0xA74A,	/* U+A74B: LATIN SMALL LETTER O WITH LONG STROKE OVERLAY */
	[3200+0xD] = 0xA74C,	/* U+A74D: LATIN SMALL LETTER O WITH LOOP */
	[3200+0xF] = 0xA74E,	/* U+A74F: LATIN SMALL LETTER OO */
	[3200+0x11] = 0xA750,	/* U+A751: LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER */
	[3200+0x13] = 0xA752,	/* U+A753: LATIN SMALL LETTER P WITH FLOURISH */
	[3200+0x15] = 0xA754,	/* U+A755: LATIN SMALL LETTER P WITH SQUIRREL TAIL */
	[3200+0x17] = 0xA756,	/* U+A757: LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER */
	[3200+0x19] = 0xA758,	/* U+A759: LATIN SMALL LETTER Q WITH DIAGONAL STROKE */
	[3200+0x1B] = 0xA75A,	/* U+A75B: LATIN SMALL LETTER R ROTUNDA */
	[3200+0x1D] = 0xA75C,	/* U+A75D: LATIN SMALL LETTER RUM ROTUNDA */
	[3200+0x1F] = 0xA75E,	/* U+A75F: LATIN SMALL LETTER V WITH DIAGONAL STROKE */
	[3200+0x21] = 0xA760,	/* U+A761: LATIN SMALL LETTER VY */
	[3200+0x23] = 0xA762,	/* U+A763: LATIN SMALL LETTER VISIGOTHIC Z */
	[3200+0x25] = 0xA764,	/* U+A765: LATIN SMALL LETTER THORN WITH STROKE */
	[3200+0x27] = 0xA766,	/* U+A767: LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER */
	[3200+0x29] = 0xA768,	/* U+A769: LATIN SMALL LETTER VEND */
	[3200+0x2B] = 0xA76A,	/* U+A76B: LATIN SMALL LETTER ET */
	[3200+0x2D] = 0xA76C,	/* U+A76D: LATIN SMALL LETTER IS */
	[3200+0x2F] = 0xA76E,	/* U+A76F: LATIN SMALL LETTER CON */
	[3200+0x3A] = 0xA779,	/* U+A77A: LATIN SMALL LETTER INSULAR D */
	[3200+0x3C] = 0xA77B,	/* U+A77C: LATIN SMALL LETTER INSULAR F */
	[3200+0x3F] = 0xA77E,	/* U+A77F: LATIN SMALL LETTER TURNED INSULAR G */
	[2944+0x1E] = 3264 - 0x80,	/* 352 236 ... */
	[3264+0x1] = 0xA780,	/* U+A781: LATIN SMALL LETTER TURNED L */
	[3264+0x3] = 0xA782,	/* U+A783: LATIN SMALL LETTER INSULAR R */
	[3264+0x5] = 0xA784,	/* U+A785: LATIN SMALL LETTER INSULAR S */
	[3264+0x7] = 0xA786,	/* U+A787: LATIN SMALL LETTER INSULAR T */
	[3264+0xC] = 0xA78B,	/* U+A78C: LATIN SMALL LETTER SALTILLO */
	[3264+0x11] = 0xA790,	/* U+A791: LATIN SMALL LETTER N WITH DESCENDER */
	[3264+0x13] = 0xA792,	/* U+A793: LATIN SMALL LETTER C WITH BAR */
	[3264+0x14] = 0xA7C4,	/* U+A794: LATIN SMALL LETTER C WITH PALATAL HOOK */
	[3264+0x17] = 0xA796,	/* U+A797: LATIN SMALL LETTER B WITH FLOURISH */
	[3264+0x19] = 0xA798,	/* U+A799: LATIN SMALL LETTER F WITH STROKE */
	[3264+0x1B] = 0xA79A,	/* U+A79B: LATIN SMALL LETTER VOLAPUK AE */
	[3264+0x1D] = 0xA79C,	/* U+A79D: LATIN SMALL LETTER VOLAPUK OE */
	[3264+0x1F] = 0xA79E,	/* U+A79F: LATIN SMALL LETTER VOLAPUK UE */
	[3264+0x21] = 0xA7A0,	/* U+A7A1: LATIN SMALL LETTER G WITH OBLIQUE STROKE */
	[3264+0x23] = 0xA7A2,	/* U+A7A3: LATIN SMALL LETTER K WITH OBLIQUE STROKE */
	[3264+0x25] = 0xA7A4,	/* U+A7A5: LATIN SMALL LETTER N WITH OBLIQUE STROKE */
	[3264+0x27] = 0xA7A6,	/* U+A7A7: LATIN SMALL LETTER R WITH OBLIQUE STROKE */
	[3264+0x29] = 0xA7A8,	/* U+A7A9: LATIN SMALL LETTER S WITH OBLIQUE STROKE */
	[3264+0x35] = 0xA7B4,	/* U+A7B5: LATIN SMALL LETTER BETA */
	[3264+0x37] = 0xA7B6,	/* U+A7B7: LATIN SMALL LETTER OMEGA */
	[3264+0x39] = 0xA7B8,	/* U+A7B9: LATIN SMALL LETTER U WITH STROKE */
	[3264+0x3B] = 0xA7BA,	/* U+A7BB: LATIN SMALL LETTER GLOTTAL A */
	[3264+0x3D] = 0xA7BC,	/* U+A7BD: LATIN SMALL LETTER GLOTTAL I */
	[3264+0x3F] = 0xA7BE,	/* U+A7BF: LATIN SMALL LETTER GLOTTAL U */
	[2944+0x1F] = 3328 - 0x80,	/* 352 237 ... */
	[3328+0x1] = 0xA7C0,	/* U+A7C1: LATIN SMALL LETTER OLD POLISH O */
	[3328+0x3] = 0xA7C2,	/* U+A7C3: LATIN SMALL LETTER ANGLICANA W */
	[3328+0x8] = 0xA7C7,	/* U+A7C8: LATIN SMALL LETTER D WITH SHORT STROKE OVERLAY */
	[3328+0xA] = 0xA7C9,	/* U+A7CA: LATIN SMALL LETTER S WITH SHORT STROKE OVERLAY */
	[3328+0x11] = 0xA7D0,	/* U+A7D1: LATIN SMALL LETTER CLOSED INSULAR G */
	[3328+0x17] = 0xA7D6,	/* U+A7D7: LATIN SMALL LETTER MIDDLE SCOTS S */
	[3328+0x19] = 0xA7D8,	/* U+A7D9: LATIN SMALL LETTER SIGMOID S */
	[3328+0x36] = 0xA7F5,	/* U+A7F6: LATIN SMALL LETTER REVERSED HALF H */
	[2944+0x2D] = 3392 - 0x80,	/* 352 255 ... */
	[3392+0x13] = 0xA7B3,	/* U+AB53: LATIN SMALL LETTER CHI */
	[3392+0x30] = 0x13A0,	/* U+AB70: CHEROKEE SMALL LETTER A */
	[3392+0x31] = 0x13A1,	/* U+AB71: CHEROKEE SMALL LETTER E */
	[3392+0x32] = 0x13A2,	/* U+AB72: CHEROKEE SMALL LETTER I */
	[3392+0x33] = 0x13A3,	/* U+AB73: CHEROKEE SMALL LETTER O */
	[3392+0x34] = 0x13A4,	/* U+AB74: CHEROKEE SMALL LETTER U */
	[3392+0x35] = 0x13A5,	/* U+AB75: CHEROKEE SMALL LETTER V */
	[3392+0x36] = 0x13A6,	/* U+AB76: CHEROKEE SMALL LETTER GA */
	[3392+0x37] = 0x13A7,	/* U+AB77: CHEROKEE SMALL LETTER KA */
	[3392+0x38] = 0x13A8,	/* U+AB78: CHEROKEE SMALL LETTER GE */
	[3392+0x39] = 0x13A9,	/* U+AB79: CHEROKEE SMALL LETTER GI */
	[3392+0x3A] = 0x13AA,	/* U+AB7A: CHEROKEE SMALL LETTER GO */
	[3392+0x3B] = 0x13AB,	/* U+AB7B: CHEROKEE SMALL LETTER GU */
	[3392+0x3C] = 0x13AC,	/* U+AB7C: CHEROKEE SMALL LETTER GV */
	[3392+0x3D] = 0x13AD,	/* U+AB7D: CHEROKEE SMALL LETTER HA */
	[3392+0x3E] = 0x13AE,	/* U+AB7E: CHEROKEE SMALL LETTER HE */
	[3392+0x3F] = 0x13AF,	/* U+AB7F: CHEROKEE SMALL LETTER HI */
	[2944+0x2E] = 3456 - 0x80,	/* 352 256 ... */
	[3456+0x0] = 0x13B0,	/* U+AB80: CHEROKEE SMALL LETTER HO */
	[3456+0x1] = 0x13B1,	/* U+AB81: CHEROKEE SMALL LETTER HU */
	[3456+0x2] = 0x13B2,	/* U+AB82: CHEROKEE SMALL LETTER HV */
	[3456+0x3] = 0x13B3,	/* U+AB83: CHEROKEE SMALL LETTER LA */
	[3456+0x4] = 0x13B4,	/* U+AB84: CHEROKEE SMALL LETTER LE */
	[3456+0x5] = 0x13B5,	/* U+AB85: CHEROKEE SMALL LETTER LI */
	[3456+0x6] = 0x13B6,	/* U+AB86: CHEROKEE SMALL LETTER LO */
	[3456+0x7] = 0x13B7,	/* U+AB87: CHEROKEE SMALL LETTER LU */
	[3456+0x8] = 0x13B8,	/* U+AB88: CHEROKEE SMALL LETTER LV */
	[3456+0x9] = 0x13B9,	/* U+AB89: CHEROKEE SMALL LETTER MA */
	[3456+0xA] = 0x13BA,	/* U+AB8A: CHEROKEE SMALL LETTER ME */
	[3456+0xB] = 0x13BB,	/* U+AB8B: CHEROKEE SMALL LETTER MI */
	[3456+0xC] = 0x13BC,	/* U+AB8C: CHEROKEE SMALL LETTER MO */
	[3456+0xD] = 0x13BD,	/* U+AB8D: CHEROKEE SMALL LETTER MU */
	[3456+0xE] = 0x13BE,	/* U+AB8E: CHEROKEE SMALL LETTER NA */
	[3456+0xF] = 0x13BF,	/* U+AB8F: CHEROKEE SMALL LETTER HNA */
	[3456+0x10] = 0x13C0,	/* U+AB90: CHEROKEE SMALL LETTER NAH */
	[3456+0x11] = 0x13C1,	/* U+AB91: CHEROKEE SMALL LETTER NE */
	[3456+0x12] = 0x13C2,	/* U+AB92: CHEROKEE SMALL LETTER NI */
	[3456+0x13] = 0x13C3,	/* U+AB93: CHEROKEE SMALL LETTER NO */
	[3456+0x14] = 0x13C4,	/* U+AB94: CHEROKEE SMALL LETTER NU */
	[3456+0x15] = 0x13C5,	/* U+AB95: CHEROKEE SMALL LETTER NV */
	[3456+0x16] = 0x13C6,	/* U+AB96: CHEROKEE SMALL LETTER QUA */
	[3456+0x17] = 0x13C7,	/* U+AB97: CHEROKEE SMALL LETTER QUE */
	[3456+0x18] = 0x13C8,	/* U+AB98: CHEROKEE SMALL LETTER QUI */
	[3456+0x19] = 0x13C9,	/* U+AB99: CHEROKEE SMALL LETTER QUO */
	[3456+0x1A] = 0x13CA,	/* U+AB9A: CHEROKEE SMALL LETTER QUU */
	[3456+0x1B] = 0x13CB,	/* U+AB9B: CHEROKEE SMALL LETTER QUV */
	[3456+0x1C] = 0x13CC,	/* U+AB9C: CHEROKEE SMALL LETTER SA */
	[3456+0x1D] = 0x13CD,	/* U+AB9D: CHEROKEE SMALL LETTER S */
	[3456+0x1E] = 0x13CE,	/* U+AB9E: CHEROKEE SMALL LETTER SE */
	[3456+0x1F] = 0x13CF,	/* U+AB9F: CHEROKEE SMALL LETTER SI */
	[3456+0x20] = 0x13D0,	/* U+ABA0: CHEROKEE SMALL LETTER SO */
	[3456+0x21] = 0x13D1,	/* U+ABA1: CHEROKEE SMALL LETTER SU */
	[3456+0x22] = 0x13D2,	/* U+ABA2: CHEROKEE SMALL LETTER SV */
	[3456+0x23] = 0x13D3,	/* U+ABA3: CHEROKEE SMALL LETTER DA */
	[3456+0x24] = 0x13D4,	/* U+ABA4: CHEROKEE SMALL LETTER TA */
	[3456+0x25] = 0x13D5,	/* U+ABA5: CHEROKEE SMALL LETTER DE */
	[3456+0x26] = 0x13D6,	/* U+ABA6: CHEROKEE SMALL LETTER TE */
	[3456+0x27] = 0x13D7,	/* U+ABA7: CHEROKEE SMALL LETTER DI */
	[3456+0x28] = 0x13D8,	/* U+ABA8: CHEROKEE SMALL LETTER TI */
	[3456+0x29] = 0x13D9,	/* U+ABA9: CHEROKEE SMALL LETTER DO */
	[3456+0x2A] = 0x13DA,	/* U+ABAA: CHEROKEE SMALL LETTER DU */
	[3456+0x2B] = 0x13DB,	/* U+ABAB: CHEROKEE SMALL LETTER DV */
	[3456+0x2C] = 0x13DC,	/* U+ABAC: CHEROKEE SMALL LETTER DLA */
	[3456+0x2D] = 0x13DD,	/* U+ABAD: CHEROKEE SMALL LETTER TLA */
	[3456+0x2E] = 0x13DE,	/* U+ABAE: CHEROKEE SMALL LETTER TLE */
	[3456+0x2F] = 0x13DF,	/* U+ABAF: CHEROKEE SMALL LETTER TLI */
	[3456+0x30] = 0x13E0,	/* U+ABB0: CHEROKEE SMALL LETTER TLO */
	[3456+0x31] = 0x13E1,	/* U+ABB1: CHEROKEE SMALL LETTER TLU */
	[3456+0x32] = 0x13E2,	/* U+ABB2: CHEROKEE SMALL LETTER TLV */
	[3456+0x33] = 0x13E3,	/* U+ABB3: CHEROKEE SMALL LETTER TSA */
	[3456+0x34] = 0x13E4,	/* U+ABB4: CHEROKEE SMALL LETTER TSE */
	[3456+0x35] = 0x13E5,	/* U+ABB5: CHEROKEE SMALL LETTER TSI */
	[3456+0x36] = 0x13E6,	/* U+ABB6: CHEROKEE SMALL LETTER TSO */
	[3456+0x37] = 0x13E7,	/* U+ABB7: CHEROKEE SMALL LETTER TSU */
	[3456+0x38] = 0x13E8,	/* U+ABB8: CHEROKEE SMALL LETTER TSV */
	[3456+0x39] = 0x13E9,	/* U+ABB9: CHEROKEE SMALL LETTER WA */
	[3456+0x3A] = 0x13EA,	/* U+ABBA: CHEROKEE SMALL LETTER WE */
	[3456+0x3B] = 0x13EB,	/* U+ABBB: CHEROKEE SMALL LETTER WI */
	[3456+0x3C] = 0x13EC,	/* U+ABBC: CHEROKEE SMALL LETTER WO */
	[3456+0x3D] = 0x13ED,	/* U+ABBD: CHEROKEE SMALL LETTER WU */
	[3456+0x3E] = 0x13EE,	/* U+ABBE: CHEROKEE SMALL LETTER WV */
	[3456+0x3F] = 0x13EF,	/* U+ABBF: CHEROKEE SMALL LETTER YA */
	[0xEF] = 3520 - 0x80,	/* 357 ... */
	[3520+0x3D] = 3584 - 0x80,	/* 357 275 ... */
	[3584+0x1] = 0xFF21,	/* U+FF41: FULLWIDTH LATIN SMALL LETTER A */
	[3584+0x2] = 0xFF22,	/* U+FF42: FULLWIDTH LATIN SMALL LETTER B */
	[3584+0x3] = 0xFF23,	/* U+FF43: FULLWIDTH LATIN SMALL LETTER C */
	[3584+0x4] = 0xFF24,	/* U+FF44: FULLWIDTH LATIN SMALL LETTER D */
	[3584+0x5] = 0xFF25,	/* U+FF45: FULLWIDTH LATIN SMALL LETTER E */
	[3584+0x6] = 0xFF26,	/* U+FF46: FULLWIDTH LATIN SMALL LETTER F */
	[3584+0x7] = 0xFF27,	/* U+FF47: FULLWIDTH LATIN SMALL LETTER G */
	[3584+0x8] = 0xFF28,	/* U+FF48: FULLWIDTH LATIN SMALL LETTER H */
	[3584+0x9] = 0xFF29,	/* U+FF49: FULLWIDTH LATIN SMALL LETTER I */
	[3584+0xA] = 0xFF2A,	/* U+FF4A: FULLWIDTH LATIN SMALL LETTER J */
	[3584+0xB] = 0xFF2B,	/* U+FF4B: FULLWIDTH LATIN SMALL LETTER K */
	[3584+0xC] = 0xFF2C,	/* U+FF4C: FULLWIDTH LATIN SMALL LETTER L */
	[3584+0xD] = 0xFF2D,	/* U+FF4D: FULLWIDTH LATIN SMALL LETTER M */
	[3584+0xE] = 0xFF2E,	/* U+FF4E: FULLWIDTH LATIN SMALL LETTER N */
	[3584+0xF] = 0xFF2F,	/* U+FF4F: FULLWIDTH LATIN SMALL LETTER O */
	[3584+0x10] = 0xFF30,	/* U+FF50: FULLWIDTH LATIN SMALL LETTER P */
	[3584+0x11] = 0xFF31,	/* U+FF51: FULLWIDTH LATIN SMALL LETTER Q */
	[3584+0x12] = 0xFF32,	/* U+FF52: FULLWIDTH LATIN SMALL LETTER R */
	[3584+0x13] = 0xFF33,	/* U+FF53: FULLWIDTH LATIN SMALL LETTER S */
	[3584+0x14] = 0xFF34,	/* U+FF54: FULLWIDTH LATIN SMALL LETTER T */
	[3584+0x15] = 0xFF35,	/* U+FF55: FULLWIDTH LATIN SMALL LETTER U */
	[3584+0x16] = 0xFF36,	/* U+FF56: FULLWIDTH LATIN SMALL LETTER V */
	[3584+0x17] = 0xFF37,	/* U+FF57: FULLWIDTH LATIN SMALL LETTER W */
	[3584+0x18] = 0xFF38,	/* U+FF58: FULLWIDTH LATIN SMALL LETTER X */
	[3584+0x19] = 0xFF39,	/* U+FF59: FULLWIDTH LATIN SMALL LETTER Y */
	[3584+0x1A] = 0xFF3A,	/* U+FF5A: FULLWIDTH LATIN SMALL LETTER Z */
	[0xF0] = 3648 - 0x80,	/* 360 ... */
	[3648+0x10] = 3712 - 0x80,	/* 360 220 ... */
	[3712+0x10] = 3776 - 0x80,	/* 360 220 220 ... */
	[3776+0x28] = 0x10400,	/* U+10428: DESERET SMALL LETTER LONG I */
	[3776+0x29] = 0x10401,	/* U+10429: DESERET SMALL LETTER LONG E */
	[3776+0x2A] = 0x10402,	/* U+1042A: DESERET SMALL LETTER LONG A */
	[3776+0x2B] = 0x10403,	/* U+1042B: DESERET SMALL LETTER LONG AH */
	[3776+0x2C] = 0x10404,	/* U+1042C: DESERET SMALL LETTER LONG O */
	[3776+0x2D] = 0x10405,	/* U+1042D: DESERET SMALL LETTER LONG OO */
	[3776+0x2E] = 0x10406,	/* U+1042E: DESERET SMALL LETTER SHORT I */
	[3776+0x2F] = 0x10407,	/* U+1042F: DESERET SMALL LETTER SHORT E */
	[3776+0x30] = 0x10408,	/* U+10430: DESERET SMALL LETTER SHORT A */
	[3776+0x31] = 0x10409,	/* U+10431: DESERET SMALL LETTER SHORT AH */
	[3776+0x32] = 0x1040A,	/* U+10432: DESERET SMALL LETTER SHORT O */
	[3776+0x33] = 0x1040B,	/* U+10433: DESERET SMALL LETTER SHORT OO */
	[3776+0x34] = 0x1040C,	/* U+10434: DESERET SMALL LETTER AY */
	[3776+0x35] = 0x1040D,	/* U+10435: DESERET SMALL LETTER OW */
	[3776+0x36] = 0x1040E,	/* U+10436: DESERET SMALL LETTER WU */
	[3776+0x37] = 0x1040F,	/* U+10437: DESERET SMALL LETTER YEE */
	[3776+0x38] = 0x10410,	/* U+10438: DESERET SMALL LETTER H */
	[3776+0x39] = 0x10411,	/* U+10439: DESERET SMALL LETTER PEE */
	[3776+0x3A] = 0x10412,	/* U+1043A: DESERET SMALL LETTER BEE */
	[3776+0x3B] = 0x10413,	/* U+1043B: DESERET SMALL LETTER TEE */
	[3776+0x3C] = 0x10414,	/* U+1043C: DESERET SMALL LETTER DEE */
	[3776+0x3D] = 0x10415,	/* U+1043D: DESERET SMALL LETTER CHEE */
	[3776+0x3E] = 0x10416,	/* U+1043E: DESERET SMALL LETTER JEE */
	[3776+0x3F] = 0x10417,	/* U+1043F: DESERET SMALL LETTER KAY */
	[3712+0x11] = 3840 - 0x80,	/* 360 220 221 ... */
	[3840+0x0] = 0x10418,	/* U+10440: DESERET SMALL LETTER GAY */
	[3840+0x1] = 0x10419,	/* U+10441: DESERET SMALL LETTER EF */
	[3840+0x2] = 0x1041A,	/* U+10442: DESERET SMALL LETTER VEE */
	[3840+0x3] = 0x1041B,	/* U+10443: DESERET SMALL LETTER ETH */
	[3840+0x4] = 0x1041C,	/* U+10444: DESERET SMALL LETTER THEE */
	[3840+0x5] = 0x1041D,	/* U+10445: DESERET SMALL LETTER ES */
	[3840+0x6] = 0x1041E,	/* U+10446: DESERET SMALL LETTER ZEE */
	[3840+0x7] = 0x1041F,	/* U+10447: DESERET SMALL LETTER ESH */
	[3840+0x8] = 0x10420,	/* U+10448: DESERET SMALL LETTER ZHEE */
	[3840+0x9] = 0x10421,	/* U+10449: DESERET SMALL LETTER ER */
	[3840+0xA] = 0x10422,	/* U+1044A: DESERET SMALL LETTER EL */
	[3840+0xB] = 0x10423,	/* U+1044B: DESERET SMALL LETTER EM */
	[3840+0xC] = 0x10424,	/* U+1044C: DESERET SMALL LETTER EN */
	[3840+0xD] = 0x10425,	/* U+1044D: DESERET SMALL LETTER ENG */
	[3840+0xE] = 0x10426,	/* U+1044E: DESERET SMALL LETTER OI */
	[3840+0xF] = 0x10427,	/* U+1044F: DESERET SMALL LETTER EW */
	[3712+0x13] = 3904 - 0x80,	/* 360 220 223 ... */
	[3904+0x18] = 0x104B0,	/* U+104D8: OSAGE SMALL LETTER A */
	[3904+0x19] = 0x104B1,	/* U+104D9: OSAGE SMALL LETTER AI */
	[3904+0x1A] = 0x104B2,	/* U+104DA: OSAGE SMALL LETTER AIN */
	[3904+0x1B] = 0x104B3,	/* U+104DB: OSAGE SMALL LETTER AH */
	[3904+0x1C] = 0x104B4,	/* U+104DC: OSAGE SMALL LETTER BRA */
	[3904+0x1D] = 0x104B5,	/* U+104DD: OSAGE SMALL LETTER CHA */
	[3904+0x1E] = 0x104B6,	/* U+104DE: OSAGE SMALL LETTER EHCHA */
	[3904+0x1F] = 0x104B7,	/* U+104DF: OSAGE SMALL LETTER E */
	[3904+0x20] = 0x104B8,	/* U+104E0: OSAGE SMALL LETTER EIN */
	[3904+0x21] = 0x104B9,	/* U+104E1: OSAGE SMALL LETTER HA */
	[3904+0x22] = 0x104BA,	/* U+104E2: OSAGE SMALL LETTER HYA */
	[3904+0x23] = 0x104BB,	/* U+104E3: OSAGE SMALL LETTER I */
	[3904+0x24] = 0x104BC,	/* U+104E4: OSAGE SMALL LETTER KA */
	[3904+0x25] = 0x104BD,	/* U+104E5: OSAGE SMALL LETTER EHKA */
	[3904+0x26] = 0x104BE,	/* U+104E6: OSAGE SMALL LETTER KYA */
	[3904+0x27] = 0x104BF,	/* U+104E7: OSAGE SMALL LETTER LA */
	[3904+0x28] = 0x104C0,	/* U+104E8: OSAGE SMALL LETTER MA */
	[3904+0x29] = 0x104C1,	/* U+104E9: OSAGE SMALL LETTER NA */
	[3904+0x2A] = 0x104C2,	/* U+104EA: OSAGE SMALL LETTER O */
	[3904+0x2B] = 0x104C3,	/* U+104EB: OSAGE SMALL LETTER OIN */
	[3904+0x2C] = 0x104C4,	/* U+104EC: OSAGE SMALL LETTER PA */
	[3904+0x2D] = 0x104C5,	/* U+104ED: OSAGE SMALL LETTER EHPA */
	[3904+0x2E] = 0x104C6,	/* U+104EE: OSAGE SMALL LETTER SA */
	[3904+0x2F] = 0x104C7,	/* U+104EF: OSAGE SMALL LETTER SHA */
	[3904+0x30] = 0x104C8,	/* U+104F0: OSAGE SMALL LETTER TA */
	[3904+0x31] = 0x104C9,	/* U+104F1: OSAGE SMALL LETTER EHTA */
	[3904+0x32] = 0x104CA,	/* U+104F2: OSAGE SMALL LETTER TSA */
	[3904+0x33] = 0x104CB,	/* U+104F3: OSAGE SMALL LETTER EHTSA */
	[3904+0x34] = 0x104CC,	/* U+104F4: OSAGE SMALL LETTER TSHA */
	[3904+0x35] = 0x104CD,	/* U+104F5: OSAGE SMALL LETTER DHA */
	[3904+0x36] = 0x104CE,	/* U+104F6: OSAGE SMALL LETTER U */
	[3904+0x37] = 0x104CF,	/* U+104F7: OSAGE SMALL LETTER WA */
	[3904+0x38] = 0x104D0,	/* U+104F8: OSAGE SMALL LETTER KHA */
	[3904+0x39] = 0x104D1,	/* U+104F9: OSAGE SMALL LETTER GHA */
	[3904+0x3A] = 0x104D2,	/* U+104FA: OSAGE SMALL LETTER ZA */
	[3904+0x3B] = 0x104D3,	/* U+104FB: OSAGE SMALL LETTER ZHA */
	[3712+0x16] = 3968 - 0x80,	/* 360 220 226 ... */
	[3968+0x17] = 0x10570,	/* U+10597: VITHKUQI SMALL LETTER A */
	[3968+0x18] = 0x10571,	/* U+10598: VITHKUQI SMALL LETTER BBE */
	[3968+0x19] = 0x10572,	/* U+10599: VITHKUQI SMALL LETTER BE */
	[3968+0x1A] = 0x10573,	/* U+1059A: VITHKUQI SMALL LETTER CE */
	[3968+0x1B] = 0x10574,	/* U+1059B: VITHKUQI SMALL LETTER CHE */
	[3968+0x1C] = 0x10575,	/* U+1059C: VITHKUQI SMALL LETTER DE */
	[3968+0x1D] = 0x10576,	/* U+1059D: VITHKUQI SMALL LETTER DHE */
	[3968+0x1E] = 0x10577,	/* U+1059E: VITHKUQI SMALL LETTER EI */
	[3968+0x1F] = 0x10578,	/* U+1059F: VITHKUQI SMALL LETTER E */
	[3968+0x20] = 0x10579,	/* U+105A0: VITHKUQI SMALL LETTER FE */
	[3968+0x21] = 0x1057A,	/* U+105A1: VITHKUQI SMALL LETTER GA */
	[3968+0x23] = 0x1057C,	/* U+105A3: VITHKUQI SMALL LETTER HA */
	[3968+0x24] = 0x1057D,	/* U+105A4: VITHKUQI SMALL LETTER HHA */
	[3968+0x25] = 0x1057E,	/* U+105A5: VITHKUQI SMALL LETTER I */
	[3968+0x26] = 0x1057F,	/* U+105A6: VITHKUQI SMALL LETTER IJE */
	[3968+0x27] = 0x10580,	/* U+105A7: VITHKUQI SMALL LETTER JE */
	[3968+0x28] = 0x10581,	/* U+105A8: VITHKUQI SMALL LETTER KA */
	[3968+0x29] = 0x10582,	/* U+105A9: VITHKUQI SMALL LETTER LA */
	[3968+0x2A] = 0x10583,	/* U+105AA: VITHKUQI SMALL LETTER LLA */
	[3968+0x2B] = 0x10584,	/* U+105AB: VITHKUQI SMALL LETTER ME */
	[3968+0x2C] = 0x10585,	/* U+105AC: VITHKUQI SMALL LETTER NE */
	[3968+0x2D] = 0x10586,	/* U+105AD: VITHKUQI SMALL LETTER NJE */
	[3968+0x2E] = 0x10587,	/* U+105AE: VITHKUQI SMALL LETTER O */
	[3968+0x2F] = 0x10588,	/* U+105AF: VITHKUQI SMALL LETTER PE */
	[3968+0x30] = 0x10589,	/* U+105B0: VITHKUQI SMALL LETTER QA */
	[3968+0x31] = 0x1058A,	/* U+105B1: VITHKUQI SMALL LETTER RE */
	[3968+0x33] = 0x1058C,	/* U+105B3: VITHKUQI SMALL LETTER SE */
	[3968+0x34] = 0x1058D,	/* U+105B4: VITHKUQI SMALL LETTER SHE */
	[3968+0x35] = 0x1058E,	/* U+105B5: VITHKUQI SMALL LETTER TE */
	[3968+0x36] = 0x1058F,	/* U+105B6: VITHKUQI SMALL LETTER THE */
	[3968+0x37] = 0x10590,	/* U+105B7: VITHKUQI SMALL LETTER U */
	[3968+0x38] = 0x10591,	/* U+105B8: VITHKUQI SMALL LETTER VE */
	[3968+0x39] = 0x10592,	/* U+105B9: VITHKUQI SMALL LETTER XE */
	[3968+0x3B] = 0x10594,	/* U+105BB: VITHKUQI SMALL LETTER Y */
	[3968+0x3C] = 0x10595,	/* U+105BC: VITHKUQI SMALL LETTER ZE */
	[3712+0x33] = 4032 - 0x80,	/* 360 220 263 ... */
	[4032+0x0] = 0x10C80,	/* U+10CC0: OLD HUNGARIAN SMALL LETTER A */
	[4032+0x1] = 0x10C81,	/* U+10CC1: OLD HUNGARIAN SMALL LETTER AA */
	[4032+0x2] = 0x10C82,	/* U+10CC2: OLD HUNGARIAN SMALL LETTER EB */
	[4032+0x3] = 0x10C83,	/* U+10CC3: OLD HUNGARIAN SMALL LETTER AMB */
	[4032+0x4] = 0x10C84,	/* U+10CC4: OLD HUNGARIAN SMALL LETTER EC */
	[4032+0x5] = 0x10C85,	/* U+10CC5: OLD HUNGARIAN SMALL LETTER ENC */
	[4032+0x6] = 0x10C86,	/* U+10CC6: OLD HUNGARIAN SMALL LETTER ECS */
	[4032+0x7] = 0x10C87,	/* U+10CC7: OLD HUNGARIAN SMALL LETTER ED */
	[4032+0x8] = 0x10C88,	/* U+10CC8: OLD HUNGARIAN SMALL LETTER AND */
	[4032+0x9] = 0x10C89,	/* U+10CC9: OLD HUNGARIAN SMALL LETTER E */
	[4032+0xA] = 0x10C8A,	/* U+10CCA: OLD HUNGARIAN SMALL LETTER CLOSE E */
	[4032+0xB] = 0x10C8B,	/* U+10CCB: OLD HUNGARIAN SMALL LETTER EE */
	[4032+0xC] = 0x10C8C,	/* U+10CCC: OLD HUNGARIAN SMALL LETTER EF */
	[4032+0xD] = 0x10C8D,	/* U+10CCD: OLD HUNGARIAN SMALL LETTER EG */
	[4032+0xE] = 0x10C8E,	/* U+10CCE: OLD HUNGARIAN SMALL LETTER EGY */
	[4032+0xF] = 0x10C8F,	/* U+10CCF: OLD HUNGARIAN SMALL LETTER EH */
	[4032+0x10] = 0x10C90,	/* U+10CD0: OLD HUNGARIAN SMALL LETTER I */
	[4032+0x11] = 0x10C91,	/* U+10CD1: OLD HUNGARIAN SMALL LETTER II */
	[4032+0x12] = 0x10C92,	/* U+10CD2: OLD HUNGARIAN SMALL LETTER EJ */
	[4032+0x13] = 0x10C93,	/* U+10CD3: OLD HUNGARIAN SMALL LETTER EK */
	[4032+0x14] = 0x10C94,	/* U+10CD4: OLD HUNGARIAN SMALL LETTER AK */
	[4032+0x15] = 0x10C95,	/* U+10CD5: OLD HUNGARIAN SMALL LETTER UNK */
	[4032+0x16] = 0x10C96,	/* U+10CD6: OLD HUNGARIAN SMALL LETTER EL */
	[4032+0x17] = 0x10C97,	/* U+10CD7: OLD HUNGARIAN SMALL LETTER ELY */
	[4032+0x18] = 0x10C98,	/* U+10CD8: OLD HUNGARIAN SMALL LETTER EM */
	[4032+0x19] = 0x10C99,	/* U+10CD9: OLD HUNGARIAN SMALL LETTER EN */
	[4032+0x1A] = 0x10C9A,	/* U+10CDA: OLD HUNGARIAN SMALL LETTER ENY */
	[4032+0x1B] = 0x10C9B,	/* U+10CDB: OLD HUNGARIAN SMALL LETTER O */
	[4032+0x1C] = 0x10C9C,	/* U+10CDC: OLD HUNGARIAN SMALL LETTER OO */
	[4032+0x1D] = 0x10C9D,	/* U+10CDD: OLD HUNGARIAN SMALL LETTER NIKOLSBURG OE */
	[4032+0x1E] = 0x10C9E,	/* U+10CDE: OLD HUNGARIAN SMALL LETTER RUDIMENTA OE */
	[4032+0x1F] = 0x10C9F,	/* U+10CDF: OLD HUNGARIAN SMALL LETTER OEE */
	[4032+0x20] = 0x10CA0,	/* U+10CE0: OLD HUNGARIAN SMALL LETTER EP */
	[4032+0x21] = 0x10CA1,	/* U+10CE1: OLD HUNGARIAN SMALL LETTER EMP */
	[4032+0x22] = 0x10CA2,	/* U+10CE2: OLD HUNGARIAN SMALL LETTER ER */
	[4032+0x23] = 0x10CA3,	/* U+10CE3: OLD HUNGARIAN SMALL LETTER SHORT ER */
	[4032+0x24] = 0x10CA4,	/* U+10CE4: OLD HUNGARIAN SMALL LETTER ES */
	[4032+0x25] = 0x10CA5,	/* U+10CE5: OLD HUNGARIAN SMALL LETTER ESZ */
	[4032+0x26] = 0x10CA6,	/* U+10CE6: OLD HUNGARIAN SMALL LETTER ET */
	[4032+0x27] = 0x10CA7,	/* U+10CE7: OLD HUNGARIAN SMALL LETTER ENT */
	[4032+0x28] = 0x10CA8,	/* U+10CE8: OLD HUNGARIAN SMALL LETTER ETY */
	[4032+0x29] = 0x10CA9,	/* U+10CE9: OLD HUNGARIAN SMALL LETTER ECH */
	[4032+0x2A] = 0x10CAA,	/* U+10CEA: OLD HUNGARIAN SMALL LETTER U */
	[4032+0x2B] = 0x10CAB,	/* U+10CEB: OLD HUNGARIAN SMALL LETTER UU */
	[4032+0x2C] = 0x10CAC,	/* U+10CEC: OLD HUNGARIAN SMALL LETTER NIKOLSBURG UE */
	[4032+0x2D] = 0x10CAD,	/* U+10CED: OLD HUNGARIAN SMALL LETTER RUDIMENTA UE */
	[4032+0x2E] = 0x10CAE,	/* U+10CEE: OLD HUNGARIAN SMALL LETTER EV */
	[4032+0x2F] = 0x10CAF,	/* U+10CEF: OLD HUNGARIAN SMALL LETTER EZ */
	[4032+0x30] = 0x10CB0,	/* U+10CF0: OLD HUNGARIAN SMALL LETTER EZS */
	[4032+0x31] = 0x10CB1,	/* U+10CF1: OLD HUNGARIAN SMALL LETTER ENT-SHAPED SIGN */
	[4032+0x32] = 0x10CB2,	/* U+10CF2: OLD HUNGARIAN SMALL LETTER US */
	[3648+0x11] = 4096 - 0x80,	/* 360 221 ... */
	[4096+0x23] = 4160 - 0x80,	/* 360 221 243 ... */
	[4160+0x0] = 0x118A0,	/* U+118C0: WARANG CITI SMALL LETTER NGAA */
	[4160+0x1] = 0x118A1,	/* U+118C1: WARANG CITI SMALL LETTER A */
	[4160+0x2] = 0x118A2,	/* U+118C2: WARANG CITI SMALL LETTER WI */
	[4160+0x3] = 0x118A3,	/* U+118C3: WARANG CITI SMALL LETTER YU */
	[4160+0x4] = 0x118A4,	/* U+118C4: WARANG CITI SMALL LETTER YA */
	[4160+0x5] = 0x118A5,	/* U+118C5: WARANG CITI SMALL LETTER YO */
	[4160+0x6] = 0x118A6,	/* U+118C6: WARANG CITI SMALL LETTER II */
	[4160+0x7] = 0x118A7,	/* U+118C7: WARANG CITI SMALL LETTER UU */
	[4160+0x8] = 0x118A8,	/* U+118C8: WARANG CITI SMALL LETTER E */
	[4160+0x9] = 0x118A9,	/* U+118C9: WARANG CITI SMALL LETTER O */
	[4160+0xA] = 0x118AA,	/* U+118CA: WARANG CITI SMALL LETTER ANG */
	[4160+0xB] = 0x118AB,	/* U+118CB: WARANG CITI SMALL LETTER GA */
	[4160+0xC] = 0x118AC,	/* U+118CC: WARANG CITI SMALL LETTER KO */
	[4160+0xD] = 0x118AD,	/* U+118CD: WARANG CITI SMALL LETTER ENY */
	[4160+0xE] = 0x118AE,	/* U+118CE: WARANG CITI SMALL LETTER YUJ */
	[4160+0xF] = 0x118AF,	/* U+118CF: WARANG CITI SMALL LETTER UC */
	[4160+0x10] = 0x118B0,	/* U+118D0: WARANG CITI SMALL LETTER ENN */
	[4160+0x11] = 0x118B1,	/* U+118D1: WARANG CITI SMALL LETTER ODD */
	[4160+0x12] = 0x118B2,	/* U+118D2: WARANG CITI SMALL LETTER TTE */
	[4160+0x13] = 0x118B3,	/* U+118D3: WARANG CITI SMALL LETTER NUNG */
	[4160+0x14] = 0x118B4,	/* U+118D4: WARANG CITI SMALL LETTER DA */
	[4160+0x15] = 0x118B5,	/* U+118D5: WARANG CITI SMALL LETTER AT */
	[4160+0x16] = 0x118B6,	/* U+118D6: WARANG CITI SMALL LETTER AM */
	[4160+0x17] = 0x118B7,	/* U+118D7: WARANG CITI SMALL LETTER BU */
	[4160+0x18] = 0x118B8,	/* U+118D8: WARANG CITI SMALL LETTER PU */
	[4160+0x19] = 0x118B9,	/* U+118D9: WARANG CITI SMALL LETTER HIYO */
	[4160+0x1A] = 0x118BA,	/* U+118DA: WARANG CITI SMALL LETTER HOLO */
	[4160+0x1B] = 0x118BB,	/* U+118DB: WARANG CITI SMALL LETTER HORR */
	[4160+0x1C] = 0x118BC,	/* U+118DC: WARANG CITI SMALL LETTER HAR */
	[4160+0x1D] = 0x118BD,	/* U+118DD: WARANG CITI SMALL LETTER SSUU */
	[4160+0x1E] = 0x118BE,	/* U+118DE: WARANG CITI SMALL LETTER SII */
	[4160+0x1F] = 0x118BF,	/* U+118DF: WARANG CITI SMALL LETTER VIYO */
	[3648+0x16] = 4224 - 0x80,	/* 360 226 ... */
	[4224+0x39] = 4288 - 0x80,	/* 360 226 271 ... */
	[4288+0x20] = 0x16E40,	/* U+16E60: MEDEFAIDRIN SMALL LETTER M */
	[4288+0x21] = 0x16E41,	/* U+16E61: MEDEFAIDRIN SMALL LETTER S */
	[4288+0x22] = 0x16E42,	/* U+16E62: MEDEFAIDRIN SMALL LETTER V */
	[4288+0x23] = 0x16E43,	/* U+16E63: MEDEFAIDRIN SMALL LETTER W */
	[4288+0x24] = 0x16E44,	/* U+16E64: MEDEFAIDRIN SMALL LETTER ATIU */
	[4288+0x25] = 0x16E45,	/* U+16E65: MEDEFAIDRIN SMALL LETTER Z */
	[4288+0x26] = 0x16E46,	/* U+16E66: MEDEFAIDRIN SMALL LETTER KP */
	[4288+0x27] = 0x16E47,	/* U+16E67: MEDEFAIDRIN SMALL LETTER P */
	[4288+0x28] = 0x16E48,	/* U+16E68: MEDEFAIDRIN SMALL LETTER T */
	[4288+0x29] = 0x16E49,	/* U+16E69: MEDEFAIDRIN SMALL LETTER G */
	[4288+0x2A] = 0x16E4A,	/* U+16E6A: MEDEFAIDRIN SMALL LETTER F */
	[4288+0x2B] = 0x16E4B,	/* U+16E6B: MEDEFAIDRIN SMALL LETTER I */
	[4288+0x2C] = 0x16E4C,	/* U+16E6C: MEDEFAIDRIN SMALL LETTER K */
	[4288+0x2D] = 0x16E4D,	/* U+16E6D: MEDEFAIDRIN SMALL LETTER A */
	[4288+0x2E] = 0x16E4E,	/* U+16E6E: MEDEFAIDRIN SMALL LETTER J */
	[4288+0x2F] = 0x16E4F,	/* U+16E6F: MEDEFAIDRIN SMALL LETTER E */
	[4288+0x30] = 0x16E50,	/* U+16E70: MEDEFAIDRIN SMALL LETTER B */
	[4288+0x31] = 0x16E51,	/* U+16E71: MEDEFAIDRIN SMALL LETTER C */
	[4288+0x32] = 0x16E52,	/* U+16E72: MEDEFAIDRIN SMALL LETTER U */
	[4288+0x33] = 0x16E53,	/* U+16E73: MEDEFAIDRIN SMALL LETTER YU */
	[4288+0x34] = 0x16E54,	/* U+16E74: MEDEFAIDRIN SMALL LETTER L */
	[4288+0x35] = 0x16E55,	/* U+16E75: MEDEFAIDRIN SMALL LETTER Q */
	[4288+0x36] = 0x16E56,	/* U+16E76: MEDEFAIDRIN SMALL LETTER HP */
	[4288+0x37] = 0x16E57,	/* U+16E77: MEDEFAIDRIN SMALL LETTER NY */
	[4288+0x38] = 0x16E58,	/* U+16E78: MEDEFAIDRIN SMALL LETTER X */
	[4288+0x39] = 0x16E59,	/* U+16E79: MEDEFAIDRIN SMALL LETTER D */
	[4288+0x3A] = 0x16E5A,	/* U+16E7A: MEDEFAIDRIN SMALL LETTER OE */
	[4288+0x3B] = 0x16E5B,	/* U+16E7B: MEDEFAIDRIN SMALL LETTER N */
	[4288+0x3C] = 0x16E5C,	/* U+16E7C: MEDEFAIDRIN SMALL LETTER R */
	[4288+0x3D] = 0x16E5D,	/* U+16E7D: MEDEFAIDRIN SMALL LETTER O */
	[4288+0x3E] = 0x16E5E,	/* U+16E7E: MEDEFAIDRIN SMALL LETTER AI */
	[4288+0x3F] = 0x16E5F,	/* U+16E7F: MEDEFAIDRIN SMALL LETTER Y */
	[3648+0x1E] = 4352 - 0x80,	/* 360 236 ... */
	[4352+0x24] = 4416 - 0x80,	/* 360 236 244 ... */
	[4416+0x22] = 0x1E900,	/* U+1E922: ADLAM SMALL LETTER ALIF */
	[4416+0x23] = 0x1E901,	/* U+1E923: ADLAM SMALL LETTER DAALI */
	[4416+0x24] = 0x1E902,	/* U+1E924: ADLAM SMALL LETTER LAAM */
	[4416+0x25] = 0x1E903,	/* U+1E925: ADLAM SMALL LETTER MIIM */
	[4416+0x26] = 0x1E904,	/* U+1E926: ADLAM SMALL LETTER BA */
	[4416+0x27] = 0x1E905,	/* U+1E927: ADLAM SMALL LETTER SINNYIIYHE */
	[4416+0x28] = 0x1E906,	/* U+1E928: ADLAM SMALL LETTER PE */
	[4416+0x29] = 0x1E907,	/* U+1E929: ADLAM SMALL LETTER BHE */
	[4416+0x2A] = 0x1E908,	/* U+1E92A: ADLAM SMALL LETTER RA */
	[4416+0x2B] = 0x1E909,	/* U+1E92B: ADLAM SMALL LETTER E */
	[4416+0x2C] = 0x1E90A,	/* U+1E92C: ADLAM SMALL LETTER FA */
	[4416+0x2D] = 0x1E90B,	/* U+1E92D: ADLAM SMALL LETTER I */
	[4416+0x2E] = 0x1E90C,	/* U+1E92E: ADLAM SMALL LETTER O */
	[4416+0x2F] = 0x1E90D,	/* U+1E92F: ADLAM SMALL LETTER DHA */
	[4416+0x30] = 0x1E90E,	/* U+1E930: ADLAM SMALL LETTER YHE */
	[4416+0x31] = 0x1E90F,	/* U+1E931: ADLAM SMALL LETTER WAW */
	[4416+0x32] = 0x1E910,	/* U+1E932: ADLAM SMALL LETTER NUN */
	[4416+0x33] = 0x1E911,	/* U+1E933: ADLAM SMALL LETTER KAF */
	[4416+0x34] = 0x1E912,	/* U+1E934: ADLAM SMALL LETTER YA */
	[4416+0x35] = 0x1E913,	/* U+1E935: ADLAM SMALL LETTER U */
	[4416+0x36] = 0x1E914,	/* U+1E936: ADLAM SMALL LETTER JIIM */
	[4416+0x37] = 0x1E915,	/* U+1E937: ADLAM SMALL LETTER CHI */
	[4416+0x38] = 0x1E916,	/* U+1E938: ADLAM SMALL LETTER HA */
	[4416+0x39] = 0x1E917,	/* U+1E939: ADLAM SMALL LETTER QAAF */
	[4416+0x3A] = 0x1E918,	/* U+1E93A: ADLAM SMALL LETTER GA */
	[4416+0x3B] = 0x1E919,	/* U+1E93B: ADLAM SMALL LETTER NYA */
	[4416+0x3C] = 0x1E91A,	/* U+1E93C: ADLAM SMALL LETTER TU */
	[4416+0x3D] = 0x1E91B,	/* U+1E93D: ADLAM SMALL LETTER NHA */
	[4416+0x3E] = 0x1E91C,	/* U+1E93E: ADLAM SMALL LETTER VA */
	[4416+0x3F] = 0x1E91D,	/* U+1E93F: ADLAM SMALL LETTER KHA */
	[4352+0x25] = 4480 - 0x80,	/* 360 236 245 ... */
	[4480+0x0] = 0x1E91E,	/* U+1E940: ADLAM SMALL LETTER GBE */
	[4480+0x1] = 0x1E91F,	/* U+1E941: ADLAM SMALL LETTER ZAL */
	[4480+0x2] = 0x1E920,	/* U+1E942: ADLAM SMALL LETTER KPO */
	[4480+0x3] = 0x1E921,	/* U+1E943: ADLAM SMALL LETTER SHA */
};

/* convert the case of a UTF-8 encoded string given in `s' into the
 * buffer of length `*buflen' given in `*buf'; if the buffer is not
 * large enough, it is extended using GDKrealloc; on return (with or
 * without error), the current buffer is in *buf, and the current size
 * in *buflen. */
static gdk_return
convertcase(char **restrict buf, size_t *restrict buflen,
	    const uint8_t *restrict s, const int *restrict convtab)
{
	uint8_t *dst = (uint8_t *) *buf;
	size_t dstoff = 0;
	size_t bl;

	bl = strlen((const char *) s);
	if (*buf == NULL) {
		if (bl < 4090)
			bl = 4096;
		else
			bl += 5;
		dst = GDKmalloc(bl);
		if (dst == NULL)
			return GDK_FAIL;
		*buf = (char *) dst;
	} else if (bl + 5 > *buflen) {
		bl += 1024;
		dst = GDKrealloc(*buf, bl);
		if (dst == NULL)
			return GDK_FAIL;
		*buf = (char *) dst;
	} else {
		bl = *buflen;
	}

	size_t bl5 = bl - 5;	/* time for buffer extension */
	if (*s == 0x80) {
		/* nil */
		assert(s[1] == 0);
		dst[dstoff++] = 0x80;
	} else {
		/* special case for ASCII prefix */
		while ((int8_t) *s > 0) {
			/* no need to check for buffer size, we made
			 * sure there is enough space */
			dst[dstoff++] = (uint8_t) convtab[*s++];
		}
		while (*s) {
			/* we are at the start of a Unicode codepoint
			 * encoded in UTF-8 */
			if (dstoff > bl5) {
				/* make sure we have enough space for the
				 * largest codepoint, i.e. 4 bytes plus
				 * terminating NUL */
				size_t newlen = bl + 1024;
				dst = GDKrealloc(*buf, newlen);
				if (dst == NULL) {
					*buflen = bl;
					return GDK_FAIL;
				}
				*buf = (char *) dst;
				bl = newlen;
				bl5 = bl - 5;
			}
			const uint8_t *u = s;
			int v = convtab[*s++];
			while (v && (*s & 0xC0) == 0x80) {
				v = convtab[v + *s++];
			}
			if (v == 0) {
				/* no case conversion */
				/* copy the first part of the current
				 * codepoint */
				do {
					dst[dstoff++] = *u++;
				} while (u < s);
				/* copy rest of current codepoint */
				while ((*s & 0xC0) == 0x80) {
					dst[dstoff++] = *s++;
				}
			} else if (v <= 0x7F) {
				dst[dstoff++] = v;
			} else if (v <= 0x7FF) {
				dst[dstoff++] = 0xC0 | (v >> 6);
				dst[dstoff++] = 0x80 | (v & 0x3F);
			} else if (v <= 0xFFFF) {
				dst[dstoff++] = 0xE0 | (v >> 12);
				dst[dstoff++] = 0x80 | ((v >> 6) & 0x3F);
				dst[dstoff++] = 0x80 | (v & 0x3F);
			} else {
				dst[dstoff++] = 0xF0 | (v >> 18);
				dst[dstoff++] = 0x80 | ((v >> 12) & 0x3F);
				dst[dstoff++] = 0x80 | ((v >> 6) & 0x3F);
				dst[dstoff++] = 0x80 | (v & 0x3F);
			}
		}
	}
	if (dstoff + 1 > bl) {
		size_t newlen = dstoff + 1;
		dst = GDKrealloc(*buf, newlen);
		if (dst == NULL) {
			*buflen = bl;
			return GDK_FAIL;
		}
		*buf = (char *) dst;
		bl = newlen;
	}
	dst[dstoff] = '\0';
	*buflen = bl;
	return GDK_SUCCEED;
}

/* convert string to uppercase; see comment above for more information */
gdk_return
GDKtoupper(char **restrict buf, size_t *restrict buflen, const char *restrict s)
{
	return convertcase(buf, buflen, (const uint8_t *) s, uppercase);
}

/* convert string to lowercase; see comment above for more information */
gdk_return
GDKtolower(char **restrict buf, size_t *restrict buflen, const char *restrict s)
{
	return convertcase(buf, buflen, (const uint8_t *) s, lowercase);
}

static BAT *
BATcaseconvert(BAT *b, BAT *s, const int *restrict convtab,
	       const char *restrict func)
{
	lng t0 = 0;
	BAT *bn;
	struct canditer ci;
	BATiter bi;
	oid bhseqbase = b->hseqbase;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	BATcheck(b, NULL);
	canditer_init(&ci, b, s);
	bn = COLnew(ci.hseq, TYPE_str, ci.ncand, TRANSIENT);
	if (bn == NULL)
		return NULL;
	bi = bat_iterator(b);
	char *buf = NULL;
	size_t buflen = 0;
	TIMEOUT_LOOP_IDX_DECL(i, ci.ncand, qry_ctx) {
		BUN x = canditer_next(&ci) - bhseqbase;
		if (convertcase(&buf, &buflen, BUNtvar(bi, x),
				convtab) != GDK_SUCCEED ||
		    tfastins_nocheckVAR(bn, i, buf) != GDK_SUCCEED) {
			goto bailout;
		}
	}
	GDKfree(buf);
	BATsetcount(bn, ci.ncand);
	bat_iterator_end(&bi);
	TIMEOUT_CHECK(qry_ctx,
		      GOTO_LABEL_TIMEOUT_HANDLER(bailout, qry_ctx));
	bn->tnil = false;
	bn->tnonil = false;
	bn->tkey = BATcount(bn) <= 1;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->theap->dirty |= BATcount(bn) > 0;
	TRC_DEBUG(ALGO, "%s: b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  func,
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;

  bailout:
	GDKfree(buf);
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}

BAT *
BATtolower(BAT *b, BAT *s)
{
	return BATcaseconvert(b, s, lowercase, __func__);
}

BAT *
BATtoupper(BAT *b, BAT *s)
{
	return BATcaseconvert(b, s, uppercase, __func__);
}

/* Unicode-aware case insensitive string comparison of two UTF-8 encoded
 * string; do not look beyond the lengths in bytes of the two strings;
 * if either one reaches the end of the buffer (as given by the length
 * parameter) before any difference was found, strings compare equal; if
 * either string hits a NUL byte, that indicates the end of the string,
 * so if both strings hit a NUL byte at the same time, the strings also
 * compare equal (and else the shorter string is smaller than the longer
 * one) */
int
GDKstrncasecmp(const char *str1, const char *str2, size_t l1, size_t l2)
{
	const uint8_t *s1 = (const uint8_t *) str1;
	const uint8_t *s2 = (const uint8_t *) str2;
	int n1, n2;
	int v1, v2;

	for (;;) {
		/* check for the end */
		if (l1 == 0 || l2 == 0)
			return 0;
		if (*s1 == 0)
			return -(*s2 != 0);
		if (*s2 == 0)
			return 1;
		v1 = lowercase[*s1++];
		n1 = 1;
		l1--;
		while (v1 && l1 > 0 && (*s1 & 0xC0) == 0x80) {
			assert(n1 < 4);
			v1 = lowercase[v1 + *s1++];
			n1++;
			l1--;
		}
		if (v1 == 0) {
			while (l1 > 0 && (*s1 & 0xC0) == 0x80) {
				assert(n1 < 4);
				n1++;
				s1++;
				l1--;
			}
		}
		v2 = lowercase[*s2++];
		n2 = 1;
		l2--;
		while (v2 && l2 > 0 && (*s2 & 0xC0) == 0x80) {
			assert(n2 < 4);
			v2 = lowercase[v2 + *s2++];
			n2++;
			l2--;
		}
		if (v2 == 0) {
			while (l2 > 0 && (*s2 & 0xC0) == 0x80) {
				assert(n2 < 4);
				n2++;
				s2++;
				l2--;
			}
		}
		if (v1 == 0) {
			if (v2 == 0) {
				/* neither converted */
				if (n1 == n2) {
					/* at least the same length, so simple strncmp */
					n1 = strncmp((const char *) s1 - n1, (const char *) s2 - n2, n1);
					if (n1 != 0)
						return n1;
					/* still equal */
				} else if (n1 < n2) {
					/* sequence in s1 is shorter, so s1 < s2 */
					return -1;
				} else {
					/* sequence in s1 is longer, so s1 > s2 */
					return 1;
				}
			} else {
				switch (n1) {
				case 1:
					if (v2 >= 0x7F)
						return -1;
					if (s1[-1] != v2)
						return (s1[-1] > v2) - (s1[-1] < v2);
					break;
				case 2:
					if (v2 < 0x80)
						return 1;
					else if (v2 >= 0x7FF)
						return -1;
					v1 = ((s1[-2] & 0x1F) << 6) | (s1[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				case 3:
					if (v2 < 0x800)
						return 1;
					else if (v2 >= 0xFFFF)
						return -1;
					v1 = ((s1[-3] & 0x0F) << 12) | ((s1[-2] & 0x3F) << 6) | (s1[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				case 4:
					if (v2 < 0x10000)
						return 1;
					v1 = ((s1[-4] & 0x07) << 18) | ((s1[-3] & 0x3F) << 12) | ((s1[-2] & 0x3F) << 6) | (s1[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				default:
					MT_UNREACHABLE();
				}
			}
		} else {
			if (v2 == 0) {
				switch (n2) {
				case 1:
					if (v1 >= 0x7F)
						return 1;
					if (s2[-1] != v1)
						return (v1 > s2[-1]) - (v1 < s2[-1]);
					break;
				case 2:
					if (v1 < 0x80)
						return -1;
					else if (v1 >= 0x7FF)
						return 1;
					v2 = ((s2[-2] & 0x1F) << 6) | (s2[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				case 3:
					if (v1 < 0x800)
						return -1;
					else if (v1 >= 0xFFFF)
						return 1;
					v2 = ((s2[-3] & 0x0F) << 12) | ((s2[-2] & 0x3F) << 6) | (s2[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				case 4:
					if (v1 < 0x10000)
						return -1;
					v2 = ((s2[-4] & 0x07) << 18) | ((s2[-3] & 0x3F) << 12) | ((s2[-2] & 0x3F) << 6) | (s2[-1] & 0x3F);
					if (v1 != v2)
						return (v1 > v2) - (v1 < v2);
					break;
				default:
					MT_UNREACHABLE();
				}
			} else {
				if (v1 != v2) {
					/* both converted and they're not equal */
					return (v1 > v2) - (v1 < v2);
				}
			}
		}
	}
}

int
GDKstrcasecmp(const char *s1, const char *s2)
{
	return GDKstrncasecmp(s1, s2, SIZE_MAX, SIZE_MAX);
}

char *
GDKstrcasestr(const char *haystack, const char *needle)
{
	size_t nlen = strlen(needle);
	if (nlen == 0)
		return (char *) haystack;
	while (*haystack) {
		if ((*haystack & 0xC0) != 0x80 &&
		    GDKstrncasecmp(haystack, needle, SIZE_MAX, nlen) == 0)
			return (char *) haystack;
		haystack++;
	}
	return NULL;
}
