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
static const char *const specialcase[] = {
	NULL,
	"ss",
	"i\xCC\x87",
	"\xCA\xBCn",
	"j\xCC\x8C",
	"\xCE\xB9\xCC\x88\xCC\x81",
	"\xCF\x85\xCC\x88\xCC\x81",
	"\xD5\xA5\xD6\x82",
	"h\xCC\xB1",
	"t\xCC\x88",
	"w\xCC\x8A",
	"y\xCC\x8A",
	"a\xCA\xBE",
	"\xCF\x85\xCC\x93",
	"\xCF\x85\xCC\x93\xCC\x80",
	"\xCF\x85\xCC\x93\xCC\x81",
	"\xCF\x85\xCC\x93\xCD\x82",
	"\xE1\xBC\x80\xCE\xB9",
	"\xE1\xBC\x81\xCE\xB9",
	"\xE1\xBC\x82\xCE\xB9",
	"\xE1\xBC\x83\xCE\xB9",
	"\xE1\xBC\x84\xCE\xB9",
	"\xE1\xBC\x85\xCE\xB9",
	"\xE1\xBC\x86\xCE\xB9",
	"\xE1\xBC\x87\xCE\xB9",
	"\xE1\xBC\xA0\xCE\xB9",
	"\xE1\xBC\xA1\xCE\xB9",
	"\xE1\xBC\xA2\xCE\xB9",
	"\xE1\xBC\xA3\xCE\xB9",
	"\xE1\xBC\xA4\xCE\xB9",
	"\xE1\xBC\xA5\xCE\xB9",
	"\xE1\xBC\xA6\xCE\xB9",
	"\xE1\xBC\xA7\xCE\xB9",
	"\xE1\xBD\xA0\xCE\xB9",
	"\xE1\xBD\xA1\xCE\xB9",
	"\xE1\xBD\xA2\xCE\xB9",
	"\xE1\xBD\xA3\xCE\xB9",
	"\xE1\xBD\xA4\xCE\xB9",
	"\xE1\xBD\xA5\xCE\xB9",
	"\xE1\xBD\xA6\xCE\xB9",
	"\xE1\xBD\xA7\xCE\xB9",
	"\xE1\xBD\xB0\xCE\xB9",
	"\xCE\xB1\xCE\xB9",
	"\xCE\xAC\xCE\xB9",
	"\xCE\xB1\xCD\x82",
	"\xCE\xB1\xCD\x82\xCE\xB9",
	"\xE1\xBD\xB4\xCE\xB9",
	"\xCE\xB7\xCE\xB9",
	"\xCE\xAE\xCE\xB9",
	"\xCE\xB7\xCD\x82",
	"\xCE\xB7\xCD\x82\xCE\xB9",
	"\xCE\xB9\xCC\x88\xCC\x80",
	"\xCE\xB9\xCD\x82",
	"\xCE\xB9\xCC\x88\xCD\x82",
	"\xCF\x85\xCC\x88\xCC\x80",
	"\xCF\x81\xCC\x93",
	"\xCF\x85\xCD\x82",
	"\xCF\x85\xCC\x88\xCD\x82",
	"\xE1\xBD\xBC\xCE\xB9",
	"\xCF\x89\xCE\xB9",
	"\xCF\x8E\xCE\xB9",
	"\xCF\x89\xCD\x82",
	"\xCF\x89\xCD\x82\xCE\xB9",
	"ff",
	"fi",
	"fl",
	"ffi",
	"ffl",
	"st",
	"\xD5\xB4\xD5\xB6",
	"\xD5\xB4\xD5\xA5",
	"\xD5\xB4\xD5\xAB",
	"\xD5\xBE\xD5\xB6",
	"\xD5\xB4\xD5\xAD",
	"SS",
	"FF",
	"FI",
	"FL",
	"FFI",
	"FFL",
	"ST",
	"\xD4\xB5\xD5\x92",
	"\xD5\x84\xD5\x86",
	"\xD5\x84\xD4\xB5",
	"\xD5\x84\xD4\xBB",
	"\xD5\x8E\xD5\x86",
	"\xD5\x84\xD4\xBD",
	"\xCA\xBCN",
	"\xCE\x99\xCC\x88\xCC\x81",
	"\xCE\xA5\xCC\x88\xCC\x81",
	"J\xCC\x8C",
	"H\xCC\xB1",
	"T\xCC\x88",
	"W\xCC\x8A",
	"Y\xCC\x8A",
	"A\xCA\xBE",
	"\xCE\xA5\xCC\x93",
	"\xCE\xA5\xCC\x93\xCC\x80",
	"\xCE\xA5\xCC\x93\xCC\x81",
	"\xCE\xA5\xCC\x93\xCD\x82",
	"\xCE\x91\xCD\x82",
	"\xCE\x97\xCD\x82",
	"\xCE\x99\xCC\x88\xCC\x80",
	"\xCE\x99\xCD\x82",
	"\xCE\x99\xCC\x88\xCD\x82",
	"\xCE\xA5\xCC\x88\xCC\x80",
	"\xCE\xA1\xCC\x93",
	"\xCE\xA5\xCD\x82",
	"\xCE\xA5\xCC\x88\xCD\x82",
	"\xCE\xA9\xCD\x82",
	"\xE1\xBC\x88\xCE\x99",
	"\xE1\xBC\x89\xCE\x99",
	"\xE1\xBC\x8A\xCE\x99",
	"\xE1\xBC\x8B\xCE\x99",
	"\xE1\xBC\x8C\xCE\x99",
	"\xE1\xBC\x8D\xCE\x99",
	"\xE1\xBC\x8E\xCE\x99",
	"\xE1\xBC\x8F\xCE\x99",
	"\xE1\xBC\xA8\xCE\x99",
	"\xE1\xBC\xA9\xCE\x99",
	"\xE1\xBC\xAA\xCE\x99",
	"\xE1\xBC\xAB\xCE\x99",
	"\xE1\xBC\xAC\xCE\x99",
	"\xE1\xBC\xAD\xCE\x99",
	"\xE1\xBC\xAE\xCE\x99",
	"\xE1\xBC\xAF\xCE\x99",
	"\xE1\xBD\xA8\xCE\x99",
	"\xE1\xBD\xA9\xCE\x99",
	"\xE1\xBD\xAA\xCE\x99",
	"\xE1\xBD\xAB\xCE\x99",
	"\xE1\xBD\xAC\xCE\x99",
	"\xE1\xBD\xAD\xCE\x99",
	"\xE1\xBD\xAE\xCE\x99",
	"\xE1\xBD\xAF\xCE\x99",
	"\xCE\x91\xCE\x99",
	"\xCE\x97\xCE\x99",
	"\xCE\xA9\xCE\x99",
	"\xE1\xBE\xBA\xCE\x99",
	"\xCE\x86\xCE\x99",
	"\xE1\xBF\x8A\xCE\x99",
	"\xCE\x89\xCE\x99",
	"\xE1\xBF\xBA\xCE\x99",
	"\xCE\x8F\xCE\x99",
	"\xCE\x91\xCD\x82\xCE\x99",
	"\xCE\x97\xCD\x82\xCE\x99",
	"\xCE\xA9\xCD\x82\xCE\x99",
};
static const int lowercase[4288] = {
	[0x00] = 0x0000,	/* U+0000: <control> */
	[0x01] = 0x0001,	/* U+0001: <control> */
	[0x02] = 0x0002,	/* U+0002: <control> */
	[0x03] = 0x0003,	/* U+0003: <control> */
	[0x04] = 0x0004,	/* U+0004: <control> */
	[0x05] = 0x0005,	/* U+0005: <control> */
	[0x06] = 0x0006,	/* U+0006: <control> */
	[0x07] = 0x0007,	/* U+0007: <control> */
	[0x08] = 0x0008,	/* U+0008: <control> */
	[0x09] = 0x0009,	/* U+0009: <control> */
	[0x0A] = 0x000A,	/* U+000A: <control> */
	[0x0B] = 0x000B,	/* U+000B: <control> */
	[0x0C] = 0x000C,	/* U+000C: <control> */
	[0x0D] = 0x000D,	/* U+000D: <control> */
	[0x0E] = 0x000E,	/* U+000E: <control> */
	[0x0F] = 0x000F,	/* U+000F: <control> */
	[0x10] = 0x0010,	/* U+0010: <control> */
	[0x11] = 0x0011,	/* U+0011: <control> */
	[0x12] = 0x0012,	/* U+0012: <control> */
	[0x13] = 0x0013,	/* U+0013: <control> */
	[0x14] = 0x0014,	/* U+0014: <control> */
	[0x15] = 0x0015,	/* U+0015: <control> */
	[0x16] = 0x0016,	/* U+0016: <control> */
	[0x17] = 0x0017,	/* U+0017: <control> */
	[0x18] = 0x0018,	/* U+0018: <control> */
	[0x19] = 0x0019,	/* U+0019: <control> */
	[0x1A] = 0x001A,	/* U+001A: <control> */
	[0x1B] = 0x001B,	/* U+001B: <control> */
	[0x1C] = 0x001C,	/* U+001C: <control> */
	[0x1D] = 0x001D,	/* U+001D: <control> */
	[0x1E] = 0x001E,	/* U+001E: <control> */
	[0x1F] = 0x001F,	/* U+001F: <control> */
	[0x20] = 0x0020,	/* U+0020: SPACE */
	[0x21] = 0x0021,	/* U+0021: EXCLAMATION MARK */
	[0x22] = 0x0022,	/* U+0022: QUOTATION MARK */
	[0x23] = 0x0023,	/* U+0023: NUMBER SIGN */
	[0x24] = 0x0024,	/* U+0024: DOLLAR SIGN */
	[0x25] = 0x0025,	/* U+0025: PERCENT SIGN */
	[0x26] = 0x0026,	/* U+0026: AMPERSAND */
	[0x27] = 0x0027,	/* U+0027: APOSTROPHE */
	[0x28] = 0x0028,	/* U+0028: LEFT PARENTHESIS */
	[0x29] = 0x0029,	/* U+0029: RIGHT PARENTHESIS */
	[0x2A] = 0x002A,	/* U+002A: ASTERISK */
	[0x2B] = 0x002B,	/* U+002B: PLUS SIGN */
	[0x2C] = 0x002C,	/* U+002C: COMMA */
	[0x2D] = 0x002D,	/* U+002D: HYPHEN-MINUS */
	[0x2E] = 0x002E,	/* U+002E: FULL STOP */
	[0x2F] = 0x002F,	/* U+002F: SOLIDUS */
	[0x30] = 0x0030,	/* U+0030: DIGIT ZERO */
	[0x31] = 0x0031,	/* U+0031: DIGIT ONE */
	[0x32] = 0x0032,	/* U+0032: DIGIT TWO */
	[0x33] = 0x0033,	/* U+0033: DIGIT THREE */
	[0x34] = 0x0034,	/* U+0034: DIGIT FOUR */
	[0x35] = 0x0035,	/* U+0035: DIGIT FIVE */
	[0x36] = 0x0036,	/* U+0036: DIGIT SIX */
	[0x37] = 0x0037,	/* U+0037: DIGIT SEVEN */
	[0x38] = 0x0038,	/* U+0038: DIGIT EIGHT */
	[0x39] = 0x0039,	/* U+0039: DIGIT NINE */
	[0x3A] = 0x003A,	/* U+003A: COLON */
	[0x3B] = 0x003B,	/* U+003B: SEMICOLON */
	[0x3C] = 0x003C,	/* U+003C: LESS-THAN SIGN */
	[0x3D] = 0x003D,	/* U+003D: EQUALS SIGN */
	[0x3E] = 0x003E,	/* U+003E: GREATER-THAN SIGN */
	[0x3F] = 0x003F,	/* U+003F: QUESTION MARK */
	[0x40] = 0x0040,	/* U+0040: COMMERCIAL AT */
	[0x41] = 0x0061,	/* U+0041: LATIN CAPITAL LETTER A */
	[0x42] = 0x0062,	/* U+0042: LATIN CAPITAL LETTER B */
	[0x43] = 0x0063,	/* U+0043: LATIN CAPITAL LETTER C */
	[0x44] = 0x0064,	/* U+0044: LATIN CAPITAL LETTER D */
	[0x45] = 0x0065,	/* U+0045: LATIN CAPITAL LETTER E */
	[0x46] = 0x0066,	/* U+0046: LATIN CAPITAL LETTER F */
	[0x47] = 0x0067,	/* U+0047: LATIN CAPITAL LETTER G */
	[0x48] = 0x0068,	/* U+0048: LATIN CAPITAL LETTER H */
	[0x49] = 0x0069,	/* U+0049: LATIN CAPITAL LETTER I */
	[0x4A] = 0x006A,	/* U+004A: LATIN CAPITAL LETTER J */
	[0x4B] = 0x006B,	/* U+004B: LATIN CAPITAL LETTER K */
	[0x4C] = 0x006C,	/* U+004C: LATIN CAPITAL LETTER L */
	[0x4D] = 0x006D,	/* U+004D: LATIN CAPITAL LETTER M */
	[0x4E] = 0x006E,	/* U+004E: LATIN CAPITAL LETTER N */
	[0x4F] = 0x006F,	/* U+004F: LATIN CAPITAL LETTER O */
	[0x50] = 0x0070,	/* U+0050: LATIN CAPITAL LETTER P */
	[0x51] = 0x0071,	/* U+0051: LATIN CAPITAL LETTER Q */
	[0x52] = 0x0072,	/* U+0052: LATIN CAPITAL LETTER R */
	[0x53] = 0x0073,	/* U+0053: LATIN CAPITAL LETTER S */
	[0x54] = 0x0074,	/* U+0054: LATIN CAPITAL LETTER T */
	[0x55] = 0x0075,	/* U+0055: LATIN CAPITAL LETTER U */
	[0x56] = 0x0076,	/* U+0056: LATIN CAPITAL LETTER V */
	[0x57] = 0x0077,	/* U+0057: LATIN CAPITAL LETTER W */
	[0x58] = 0x0078,	/* U+0058: LATIN CAPITAL LETTER X */
	[0x59] = 0x0079,	/* U+0059: LATIN CAPITAL LETTER Y */
	[0x5A] = 0x007A,	/* U+005A: LATIN CAPITAL LETTER Z */
	[0x5B] = 0x005B,	/* U+005B: LEFT SQUARE BRACKET */
	[0x5C] = 0x005C,	/* U+005C: REVERSE SOLIDUS */
	[0x5D] = 0x005D,	/* U+005D: RIGHT SQUARE BRACKET */
	[0x5E] = 0x005E,	/* U+005E: CIRCUMFLEX ACCENT */
	[0x5F] = 0x005F,	/* U+005F: LOW LINE */
	[0x60] = 0x0060,	/* U+0060: GRAVE ACCENT */
	[0x61] = 0x0061,	/* U+0061: LATIN SMALL LETTER A */
	[0x62] = 0x0062,	/* U+0062: LATIN SMALL LETTER B */
	[0x63] = 0x0063,	/* U+0063: LATIN SMALL LETTER C */
	[0x64] = 0x0064,	/* U+0064: LATIN SMALL LETTER D */
	[0x65] = 0x0065,	/* U+0065: LATIN SMALL LETTER E */
	[0x66] = 0x0066,	/* U+0066: LATIN SMALL LETTER F */
	[0x67] = 0x0067,	/* U+0067: LATIN SMALL LETTER G */
	[0x68] = 0x0068,	/* U+0068: LATIN SMALL LETTER H */
	[0x69] = 0x0069,	/* U+0069: LATIN SMALL LETTER I */
	[0x6A] = 0x006A,	/* U+006A: LATIN SMALL LETTER J */
	[0x6B] = 0x006B,	/* U+006B: LATIN SMALL LETTER K */
	[0x6C] = 0x006C,	/* U+006C: LATIN SMALL LETTER L */
	[0x6D] = 0x006D,	/* U+006D: LATIN SMALL LETTER M */
	[0x6E] = 0x006E,	/* U+006E: LATIN SMALL LETTER N */
	[0x6F] = 0x006F,	/* U+006F: LATIN SMALL LETTER O */
	[0x70] = 0x0070,	/* U+0070: LATIN SMALL LETTER P */
	[0x71] = 0x0071,	/* U+0071: LATIN SMALL LETTER Q */
	[0x72] = 0x0072,	/* U+0072: LATIN SMALL LETTER R */
	[0x73] = 0x0073,	/* U+0073: LATIN SMALL LETTER S */
	[0x74] = 0x0074,	/* U+0074: LATIN SMALL LETTER T */
	[0x75] = 0x0075,	/* U+0075: LATIN SMALL LETTER U */
	[0x76] = 0x0076,	/* U+0076: LATIN SMALL LETTER V */
	[0x77] = 0x0077,	/* U+0077: LATIN SMALL LETTER W */
	[0x78] = 0x0078,	/* U+0078: LATIN SMALL LETTER X */
	[0x79] = 0x0079,	/* U+0079: LATIN SMALL LETTER Y */
	[0x7A] = 0x007A,	/* U+007A: LATIN SMALL LETTER Z */
	[0x7B] = 0x007B,	/* U+007B: LEFT CURLY BRACKET */
	[0x7C] = 0x007C,	/* U+007C: VERTICAL LINE */
	[0x7D] = 0x007D,	/* U+007D: RIGHT CURLY BRACKET */
	[0x7E] = 0x007E,	/* U+007E: TILDE */
	[0x7F] = 0x007F,	/* U+007F: <control> */
	[0xC3] = 256 - 0x80,	/* 303 ... */
	[256+0x00] = 0x00E0,	/* U+00C0: LATIN CAPITAL LETTER A WITH GRAVE */
	[256+0x01] = 0x00E1,	/* U+00C1: LATIN CAPITAL LETTER A WITH ACUTE */
	[256+0x02] = 0x00E2,	/* U+00C2: LATIN CAPITAL LETTER A WITH CIRCUMFLEX */
	[256+0x03] = 0x00E3,	/* U+00C3: LATIN CAPITAL LETTER A WITH TILDE */
	[256+0x04] = 0x00E4,	/* U+00C4: LATIN CAPITAL LETTER A WITH DIAERESIS */
	[256+0x05] = 0x00E5,	/* U+00C5: LATIN CAPITAL LETTER A WITH RING ABOVE */
	[256+0x06] = 0x00E6,	/* U+00C6: LATIN CAPITAL LETTER AE */
	[256+0x07] = 0x00E7,	/* U+00C7: LATIN CAPITAL LETTER C WITH CEDILLA */
	[256+0x08] = 0x00E8,	/* U+00C8: LATIN CAPITAL LETTER E WITH GRAVE */
	[256+0x09] = 0x00E9,	/* U+00C9: LATIN CAPITAL LETTER E WITH ACUTE */
	[256+0x0A] = 0x00EA,	/* U+00CA: LATIN CAPITAL LETTER E WITH CIRCUMFLEX */
	[256+0x0B] = 0x00EB,	/* U+00CB: LATIN CAPITAL LETTER E WITH DIAERESIS */
	[256+0x0C] = 0x00EC,	/* U+00CC: LATIN CAPITAL LETTER I WITH GRAVE */
	[256+0x0D] = 0x00ED,	/* U+00CD: LATIN CAPITAL LETTER I WITH ACUTE */
	[256+0x0E] = 0x00EE,	/* U+00CE: LATIN CAPITAL LETTER I WITH CIRCUMFLEX */
	[256+0x0F] = 0x00EF,	/* U+00CF: LATIN CAPITAL LETTER I WITH DIAERESIS */
	[256+0x10] = 0x00F0,	/* U+00D0: LATIN CAPITAL LETTER ETH */
	[256+0x11] = 0x00F1,	/* U+00D1: LATIN CAPITAL LETTER N WITH TILDE */
	[256+0x12] = 0x00F2,	/* U+00D2: LATIN CAPITAL LETTER O WITH GRAVE */
	[256+0x13] = 0x00F3,	/* U+00D3: LATIN CAPITAL LETTER O WITH ACUTE */
	[256+0x14] = 0x00F4,	/* U+00D4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX */
	[256+0x15] = 0x00F5,	/* U+00D5: LATIN CAPITAL LETTER O WITH TILDE */
	[256+0x16] = 0x00F6,	/* U+00D6: LATIN CAPITAL LETTER O WITH DIAERESIS */
	[256+0x18] = 0x00F8,	/* U+00D8: LATIN CAPITAL LETTER O WITH STROKE */
	[256+0x19] = 0x00F9,	/* U+00D9: LATIN CAPITAL LETTER U WITH GRAVE */
	[256+0x1A] = 0x00FA,	/* U+00DA: LATIN CAPITAL LETTER U WITH ACUTE */
	[256+0x1B] = 0x00FB,	/* U+00DB: LATIN CAPITAL LETTER U WITH CIRCUMFLEX */
	[256+0x1C] = 0x00FC,	/* U+00DC: LATIN CAPITAL LETTER U WITH DIAERESIS */
	[256+0x1D] = 0x00FD,	/* U+00DD: LATIN CAPITAL LETTER Y WITH ACUTE */
	[256+0x1E] = 0x00FE,	/* U+00DE: LATIN CAPITAL LETTER THORN */
	[0xC4] = 320 - 0x80,	/* 304 ... */
	[320+0x00] = 0x0101,	/* U+0100: LATIN CAPITAL LETTER A WITH MACRON */
	[320+0x02] = 0x0103,	/* U+0102: LATIN CAPITAL LETTER A WITH BREVE */
	[320+0x04] = 0x0105,	/* U+0104: LATIN CAPITAL LETTER A WITH OGONEK */
	[320+0x06] = 0x0107,	/* U+0106: LATIN CAPITAL LETTER C WITH ACUTE */
	[320+0x08] = 0x0109,	/* U+0108: LATIN CAPITAL LETTER C WITH CIRCUMFLEX */
	[320+0x0A] = 0x010B,	/* U+010A: LATIN CAPITAL LETTER C WITH DOT ABOVE */
	[320+0x0C] = 0x010D,	/* U+010C: LATIN CAPITAL LETTER C WITH CARON */
	[320+0x0E] = 0x010F,	/* U+010E: LATIN CAPITAL LETTER D WITH CARON */
	[320+0x10] = 0x0111,	/* U+0110: LATIN CAPITAL LETTER D WITH STROKE */
	[320+0x12] = 0x0113,	/* U+0112: LATIN CAPITAL LETTER E WITH MACRON */
	[320+0x14] = 0x0115,	/* U+0114: LATIN CAPITAL LETTER E WITH BREVE */
	[320+0x16] = 0x0117,	/* U+0116: LATIN CAPITAL LETTER E WITH DOT ABOVE */
	[320+0x18] = 0x0119,	/* U+0118: LATIN CAPITAL LETTER E WITH OGONEK */
	[320+0x1A] = 0x011B,	/* U+011A: LATIN CAPITAL LETTER E WITH CARON */
	[320+0x1C] = 0x011D,	/* U+011C: LATIN CAPITAL LETTER G WITH CIRCUMFLEX */
	[320+0x1E] = 0x011F,	/* U+011E: LATIN CAPITAL LETTER G WITH BREVE */
	[320+0x20] = 0x0121,	/* U+0120: LATIN CAPITAL LETTER G WITH DOT ABOVE */
	[320+0x22] = 0x0123,	/* U+0122: LATIN CAPITAL LETTER G WITH CEDILLA */
	[320+0x24] = 0x0125,	/* U+0124: LATIN CAPITAL LETTER H WITH CIRCUMFLEX */
	[320+0x26] = 0x0127,	/* U+0126: LATIN CAPITAL LETTER H WITH STROKE */
	[320+0x28] = 0x0129,	/* U+0128: LATIN CAPITAL LETTER I WITH TILDE */
	[320+0x2A] = 0x012B,	/* U+012A: LATIN CAPITAL LETTER I WITH MACRON */
	[320+0x2C] = 0x012D,	/* U+012C: LATIN CAPITAL LETTER I WITH BREVE */
	[320+0x2E] = 0x012F,	/* U+012E: LATIN CAPITAL LETTER I WITH OGONEK */
	[320+0x30] = 0x0069,	/* U+0130: LATIN CAPITAL LETTER I WITH DOT ABOVE */
	[320+0x32] = 0x0133,	/* U+0132: LATIN CAPITAL LIGATURE IJ */
	[320+0x34] = 0x0135,	/* U+0134: LATIN CAPITAL LETTER J WITH CIRCUMFLEX */
	[320+0x36] = 0x0137,	/* U+0136: LATIN CAPITAL LETTER K WITH CEDILLA */
	[320+0x39] = 0x013A,	/* U+0139: LATIN CAPITAL LETTER L WITH ACUTE */
	[320+0x3B] = 0x013C,	/* U+013B: LATIN CAPITAL LETTER L WITH CEDILLA */
	[320+0x3D] = 0x013E,	/* U+013D: LATIN CAPITAL LETTER L WITH CARON */
	[320+0x3F] = 0x0140,	/* U+013F: LATIN CAPITAL LETTER L WITH MIDDLE DOT */
	[0xC5] = 384 - 0x80,	/* 305 ... */
	[384+0x01] = 0x0142,	/* U+0141: LATIN CAPITAL LETTER L WITH STROKE */
	[384+0x03] = 0x0144,	/* U+0143: LATIN CAPITAL LETTER N WITH ACUTE */
	[384+0x05] = 0x0146,	/* U+0145: LATIN CAPITAL LETTER N WITH CEDILLA */
	[384+0x07] = 0x0148,	/* U+0147: LATIN CAPITAL LETTER N WITH CARON */
	[384+0x0A] = 0x014B,	/* U+014A: LATIN CAPITAL LETTER ENG */
	[384+0x0C] = 0x014D,	/* U+014C: LATIN CAPITAL LETTER O WITH MACRON */
	[384+0x0E] = 0x014F,	/* U+014E: LATIN CAPITAL LETTER O WITH BREVE */
	[384+0x10] = 0x0151,	/* U+0150: LATIN CAPITAL LETTER O WITH DOUBLE ACUTE */
	[384+0x12] = 0x0153,	/* U+0152: LATIN CAPITAL LIGATURE OE */
	[384+0x14] = 0x0155,	/* U+0154: LATIN CAPITAL LETTER R WITH ACUTE */
	[384+0x16] = 0x0157,	/* U+0156: LATIN CAPITAL LETTER R WITH CEDILLA */
	[384+0x18] = 0x0159,	/* U+0158: LATIN CAPITAL LETTER R WITH CARON */
	[384+0x1A] = 0x015B,	/* U+015A: LATIN CAPITAL LETTER S WITH ACUTE */
	[384+0x1C] = 0x015D,	/* U+015C: LATIN CAPITAL LETTER S WITH CIRCUMFLEX */
	[384+0x1E] = 0x015F,	/* U+015E: LATIN CAPITAL LETTER S WITH CEDILLA */
	[384+0x20] = 0x0161,	/* U+0160: LATIN CAPITAL LETTER S WITH CARON */
	[384+0x22] = 0x0163,	/* U+0162: LATIN CAPITAL LETTER T WITH CEDILLA */
	[384+0x24] = 0x0165,	/* U+0164: LATIN CAPITAL LETTER T WITH CARON */
	[384+0x26] = 0x0167,	/* U+0166: LATIN CAPITAL LETTER T WITH STROKE */
	[384+0x28] = 0x0169,	/* U+0168: LATIN CAPITAL LETTER U WITH TILDE */
	[384+0x2A] = 0x016B,	/* U+016A: LATIN CAPITAL LETTER U WITH MACRON */
	[384+0x2C] = 0x016D,	/* U+016C: LATIN CAPITAL LETTER U WITH BREVE */
	[384+0x2E] = 0x016F,	/* U+016E: LATIN CAPITAL LETTER U WITH RING ABOVE */
	[384+0x30] = 0x0171,	/* U+0170: LATIN CAPITAL LETTER U WITH DOUBLE ACUTE */
	[384+0x32] = 0x0173,	/* U+0172: LATIN CAPITAL LETTER U WITH OGONEK */
	[384+0x34] = 0x0175,	/* U+0174: LATIN CAPITAL LETTER W WITH CIRCUMFLEX */
	[384+0x36] = 0x0177,	/* U+0176: LATIN CAPITAL LETTER Y WITH CIRCUMFLEX */
	[384+0x38] = 0x00FF,	/* U+0178: LATIN CAPITAL LETTER Y WITH DIAERESIS */
	[384+0x39] = 0x017A,	/* U+0179: LATIN CAPITAL LETTER Z WITH ACUTE */
	[384+0x3B] = 0x017C,	/* U+017B: LATIN CAPITAL LETTER Z WITH DOT ABOVE */
	[384+0x3D] = 0x017E,	/* U+017D: LATIN CAPITAL LETTER Z WITH CARON */
	[0xC6] = 448 - 0x80,	/* 306 ... */
	[448+0x01] = 0x0253,	/* U+0181: LATIN CAPITAL LETTER B WITH HOOK */
	[448+0x02] = 0x0183,	/* U+0182: LATIN CAPITAL LETTER B WITH TOPBAR */
	[448+0x04] = 0x0185,	/* U+0184: LATIN CAPITAL LETTER TONE SIX */
	[448+0x06] = 0x0254,	/* U+0186: LATIN CAPITAL LETTER OPEN O */
	[448+0x07] = 0x0188,	/* U+0187: LATIN CAPITAL LETTER C WITH HOOK */
	[448+0x09] = 0x0256,	/* U+0189: LATIN CAPITAL LETTER AFRICAN D */
	[448+0x0A] = 0x0257,	/* U+018A: LATIN CAPITAL LETTER D WITH HOOK */
	[448+0x0B] = 0x018C,	/* U+018B: LATIN CAPITAL LETTER D WITH TOPBAR */
	[448+0x0E] = 0x01DD,	/* U+018E: LATIN CAPITAL LETTER REVERSED E */
	[448+0x0F] = 0x0259,	/* U+018F: LATIN CAPITAL LETTER SCHWA */
	[448+0x10] = 0x025B,	/* U+0190: LATIN CAPITAL LETTER OPEN E */
	[448+0x11] = 0x0192,	/* U+0191: LATIN CAPITAL LETTER F WITH HOOK */
	[448+0x13] = 0x0260,	/* U+0193: LATIN CAPITAL LETTER G WITH HOOK */
	[448+0x14] = 0x0263,	/* U+0194: LATIN CAPITAL LETTER GAMMA */
	[448+0x16] = 0x0269,	/* U+0196: LATIN CAPITAL LETTER IOTA */
	[448+0x17] = 0x0268,	/* U+0197: LATIN CAPITAL LETTER I WITH STROKE */
	[448+0x18] = 0x0199,	/* U+0198: LATIN CAPITAL LETTER K WITH HOOK */
	[448+0x1C] = 0x026F,	/* U+019C: LATIN CAPITAL LETTER TURNED M */
	[448+0x1D] = 0x0272,	/* U+019D: LATIN CAPITAL LETTER N WITH LEFT HOOK */
	[448+0x1F] = 0x0275,	/* U+019F: LATIN CAPITAL LETTER O WITH MIDDLE TILDE */
	[448+0x20] = 0x01A1,	/* U+01A0: LATIN CAPITAL LETTER O WITH HORN */
	[448+0x22] = 0x01A3,	/* U+01A2: LATIN CAPITAL LETTER OI */
	[448+0x24] = 0x01A5,	/* U+01A4: LATIN CAPITAL LETTER P WITH HOOK */
	[448+0x26] = 0x0280,	/* U+01A6: LATIN LETTER YR */
	[448+0x27] = 0x01A8,	/* U+01A7: LATIN CAPITAL LETTER TONE TWO */
	[448+0x29] = 0x0283,	/* U+01A9: LATIN CAPITAL LETTER ESH */
	[448+0x2C] = 0x01AD,	/* U+01AC: LATIN CAPITAL LETTER T WITH HOOK */
	[448+0x2E] = 0x0288,	/* U+01AE: LATIN CAPITAL LETTER T WITH RETROFLEX HOOK */
	[448+0x2F] = 0x01B0,	/* U+01AF: LATIN CAPITAL LETTER U WITH HORN */
	[448+0x31] = 0x028A,	/* U+01B1: LATIN CAPITAL LETTER UPSILON */
	[448+0x32] = 0x028B,	/* U+01B2: LATIN CAPITAL LETTER V WITH HOOK */
	[448+0x33] = 0x01B4,	/* U+01B3: LATIN CAPITAL LETTER Y WITH HOOK */
	[448+0x35] = 0x01B6,	/* U+01B5: LATIN CAPITAL LETTER Z WITH STROKE */
	[448+0x37] = 0x0292,	/* U+01B7: LATIN CAPITAL LETTER EZH */
	[448+0x38] = 0x01B9,	/* U+01B8: LATIN CAPITAL LETTER EZH REVERSED */
	[448+0x3C] = 0x01BD,	/* U+01BC: LATIN CAPITAL LETTER TONE FIVE */
	[0xC7] = 512 - 0x80,	/* 307 ... */
	[512+0x04] = 0x01C6,	/* U+01C4: LATIN CAPITAL LETTER DZ WITH CARON */
	[512+0x05] = 0x01C6,	/* U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON */
	[512+0x07] = 0x01C9,	/* U+01C7: LATIN CAPITAL LETTER LJ */
	[512+0x08] = 0x01C9,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[512+0x0A] = 0x01CC,	/* U+01CA: LATIN CAPITAL LETTER NJ */
	[512+0x0B] = 0x01CC,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[512+0x0D] = 0x01CE,	/* U+01CD: LATIN CAPITAL LETTER A WITH CARON */
	[512+0x0F] = 0x01D0,	/* U+01CF: LATIN CAPITAL LETTER I WITH CARON */
	[512+0x11] = 0x01D2,	/* U+01D1: LATIN CAPITAL LETTER O WITH CARON */
	[512+0x13] = 0x01D4,	/* U+01D3: LATIN CAPITAL LETTER U WITH CARON */
	[512+0x15] = 0x01D6,	/* U+01D5: LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON */
	[512+0x17] = 0x01D8,	/* U+01D7: LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE */
	[512+0x19] = 0x01DA,	/* U+01D9: LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON */
	[512+0x1B] = 0x01DC,	/* U+01DB: LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE */
	[512+0x1E] = 0x01DF,	/* U+01DE: LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON */
	[512+0x20] = 0x01E1,	/* U+01E0: LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON */
	[512+0x22] = 0x01E3,	/* U+01E2: LATIN CAPITAL LETTER AE WITH MACRON */
	[512+0x24] = 0x01E5,	/* U+01E4: LATIN CAPITAL LETTER G WITH STROKE */
	[512+0x26] = 0x01E7,	/* U+01E6: LATIN CAPITAL LETTER G WITH CARON */
	[512+0x28] = 0x01E9,	/* U+01E8: LATIN CAPITAL LETTER K WITH CARON */
	[512+0x2A] = 0x01EB,	/* U+01EA: LATIN CAPITAL LETTER O WITH OGONEK */
	[512+0x2C] = 0x01ED,	/* U+01EC: LATIN CAPITAL LETTER O WITH OGONEK AND MACRON */
	[512+0x2E] = 0x01EF,	/* U+01EE: LATIN CAPITAL LETTER EZH WITH CARON */
	[512+0x31] = 0x01F3,	/* U+01F1: LATIN CAPITAL LETTER DZ */
	[512+0x32] = 0x01F3,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[512+0x34] = 0x01F5,	/* U+01F4: LATIN CAPITAL LETTER G WITH ACUTE */
	[512+0x36] = 0x0195,	/* U+01F6: LATIN CAPITAL LETTER HWAIR */
	[512+0x37] = 0x01BF,	/* U+01F7: LATIN CAPITAL LETTER WYNN */
	[512+0x38] = 0x01F9,	/* U+01F8: LATIN CAPITAL LETTER N WITH GRAVE */
	[512+0x3A] = 0x01FB,	/* U+01FA: LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE */
	[512+0x3C] = 0x01FD,	/* U+01FC: LATIN CAPITAL LETTER AE WITH ACUTE */
	[512+0x3E] = 0x01FF,	/* U+01FE: LATIN CAPITAL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 576 - 0x80,	/* 310 ... */
	[576+0x00] = 0x0201,	/* U+0200: LATIN CAPITAL LETTER A WITH DOUBLE GRAVE */
	[576+0x02] = 0x0203,	/* U+0202: LATIN CAPITAL LETTER A WITH INVERTED BREVE */
	[576+0x04] = 0x0205,	/* U+0204: LATIN CAPITAL LETTER E WITH DOUBLE GRAVE */
	[576+0x06] = 0x0207,	/* U+0206: LATIN CAPITAL LETTER E WITH INVERTED BREVE */
	[576+0x08] = 0x0209,	/* U+0208: LATIN CAPITAL LETTER I WITH DOUBLE GRAVE */
	[576+0x0A] = 0x020B,	/* U+020A: LATIN CAPITAL LETTER I WITH INVERTED BREVE */
	[576+0x0C] = 0x020D,	/* U+020C: LATIN CAPITAL LETTER O WITH DOUBLE GRAVE */
	[576+0x0E] = 0x020F,	/* U+020E: LATIN CAPITAL LETTER O WITH INVERTED BREVE */
	[576+0x10] = 0x0211,	/* U+0210: LATIN CAPITAL LETTER R WITH DOUBLE GRAVE */
	[576+0x12] = 0x0213,	/* U+0212: LATIN CAPITAL LETTER R WITH INVERTED BREVE */
	[576+0x14] = 0x0215,	/* U+0214: LATIN CAPITAL LETTER U WITH DOUBLE GRAVE */
	[576+0x16] = 0x0217,	/* U+0216: LATIN CAPITAL LETTER U WITH INVERTED BREVE */
	[576+0x18] = 0x0219,	/* U+0218: LATIN CAPITAL LETTER S WITH COMMA BELOW */
	[576+0x1A] = 0x021B,	/* U+021A: LATIN CAPITAL LETTER T WITH COMMA BELOW */
	[576+0x1C] = 0x021D,	/* U+021C: LATIN CAPITAL LETTER YOGH */
	[576+0x1E] = 0x021F,	/* U+021E: LATIN CAPITAL LETTER H WITH CARON */
	[576+0x20] = 0x019E,	/* U+0220: LATIN CAPITAL LETTER N WITH LONG RIGHT LEG */
	[576+0x22] = 0x0223,	/* U+0222: LATIN CAPITAL LETTER OU */
	[576+0x24] = 0x0225,	/* U+0224: LATIN CAPITAL LETTER Z WITH HOOK */
	[576+0x26] = 0x0227,	/* U+0226: LATIN CAPITAL LETTER A WITH DOT ABOVE */
	[576+0x28] = 0x0229,	/* U+0228: LATIN CAPITAL LETTER E WITH CEDILLA */
	[576+0x2A] = 0x022B,	/* U+022A: LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON */
	[576+0x2C] = 0x022D,	/* U+022C: LATIN CAPITAL LETTER O WITH TILDE AND MACRON */
	[576+0x2E] = 0x022F,	/* U+022E: LATIN CAPITAL LETTER O WITH DOT ABOVE */
	[576+0x30] = 0x0231,	/* U+0230: LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON */
	[576+0x32] = 0x0233,	/* U+0232: LATIN CAPITAL LETTER Y WITH MACRON */
	[576+0x3A] = 0x2C65,	/* U+023A: LATIN CAPITAL LETTER A WITH STROKE */
	[576+0x3B] = 0x023C,	/* U+023B: LATIN CAPITAL LETTER C WITH STROKE */
	[576+0x3D] = 0x019A,	/* U+023D: LATIN CAPITAL LETTER L WITH BAR */
	[576+0x3E] = 0x2C66,	/* U+023E: LATIN CAPITAL LETTER T WITH DIAGONAL STROKE */
	[0xC9] = 640 - 0x80,	/* 311 ... */
	[640+0x01] = 0x0242,	/* U+0241: LATIN CAPITAL LETTER GLOTTAL STOP */
	[640+0x03] = 0x0180,	/* U+0243: LATIN CAPITAL LETTER B WITH STROKE */
	[640+0x04] = 0x0289,	/* U+0244: LATIN CAPITAL LETTER U BAR */
	[640+0x05] = 0x028C,	/* U+0245: LATIN CAPITAL LETTER TURNED V */
	[640+0x06] = 0x0247,	/* U+0246: LATIN CAPITAL LETTER E WITH STROKE */
	[640+0x08] = 0x0249,	/* U+0248: LATIN CAPITAL LETTER J WITH STROKE */
	[640+0x0A] = 0x024B,	/* U+024A: LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL */
	[640+0x0C] = 0x024D,	/* U+024C: LATIN CAPITAL LETTER R WITH STROKE */
	[640+0x0E] = 0x024F,	/* U+024E: LATIN CAPITAL LETTER Y WITH STROKE */
	[0xCD] = 704 - 0x80,	/* 315 ... */
	[704+0x30] = 0x0371,	/* U+0370: GREEK CAPITAL LETTER HETA */
	[704+0x32] = 0x0373,	/* U+0372: GREEK CAPITAL LETTER ARCHAIC SAMPI */
	[704+0x36] = 0x0377,	/* U+0376: GREEK CAPITAL LETTER PAMPHYLIAN DIGAMMA */
	[704+0x3F] = 0x03F3,	/* U+037F: GREEK CAPITAL LETTER YOT */
	[0xCE] = 768 - 0x80,	/* 316 ... */
	[768+0x06] = 0x03AC,	/* U+0386: GREEK CAPITAL LETTER ALPHA WITH TONOS */
	[768+0x08] = 0x03AD,	/* U+0388: GREEK CAPITAL LETTER EPSILON WITH TONOS */
	[768+0x09] = 0x03AE,	/* U+0389: GREEK CAPITAL LETTER ETA WITH TONOS */
	[768+0x0A] = 0x03AF,	/* U+038A: GREEK CAPITAL LETTER IOTA WITH TONOS */
	[768+0x0C] = 0x03CC,	/* U+038C: GREEK CAPITAL LETTER OMICRON WITH TONOS */
	[768+0x0E] = 0x03CD,	/* U+038E: GREEK CAPITAL LETTER UPSILON WITH TONOS */
	[768+0x0F] = 0x03CE,	/* U+038F: GREEK CAPITAL LETTER OMEGA WITH TONOS */
	[768+0x11] = 0x03B1,	/* U+0391: GREEK CAPITAL LETTER ALPHA */
	[768+0x12] = 0x03B2,	/* U+0392: GREEK CAPITAL LETTER BETA */
	[768+0x13] = 0x03B3,	/* U+0393: GREEK CAPITAL LETTER GAMMA */
	[768+0x14] = 0x03B4,	/* U+0394: GREEK CAPITAL LETTER DELTA */
	[768+0x15] = 0x03B5,	/* U+0395: GREEK CAPITAL LETTER EPSILON */
	[768+0x16] = 0x03B6,	/* U+0396: GREEK CAPITAL LETTER ZETA */
	[768+0x17] = 0x03B7,	/* U+0397: GREEK CAPITAL LETTER ETA */
	[768+0x18] = 0x03B8,	/* U+0398: GREEK CAPITAL LETTER THETA */
	[768+0x19] = 0x03B9,	/* U+0399: GREEK CAPITAL LETTER IOTA */
	[768+0x1A] = 0x03BA,	/* U+039A: GREEK CAPITAL LETTER KAPPA */
	[768+0x1B] = 0x03BB,	/* U+039B: GREEK CAPITAL LETTER LAMDA */
	[768+0x1C] = 0x03BC,	/* U+039C: GREEK CAPITAL LETTER MU */
	[768+0x1D] = 0x03BD,	/* U+039D: GREEK CAPITAL LETTER NU */
	[768+0x1E] = 0x03BE,	/* U+039E: GREEK CAPITAL LETTER XI */
	[768+0x1F] = 0x03BF,	/* U+039F: GREEK CAPITAL LETTER OMICRON */
	[768+0x20] = 0x03C0,	/* U+03A0: GREEK CAPITAL LETTER PI */
	[768+0x21] = 0x03C1,	/* U+03A1: GREEK CAPITAL LETTER RHO */
	[768+0x23] = 0x03C3,	/* U+03A3: GREEK CAPITAL LETTER SIGMA */
	[768+0x24] = 0x03C4,	/* U+03A4: GREEK CAPITAL LETTER TAU */
	[768+0x25] = 0x03C5,	/* U+03A5: GREEK CAPITAL LETTER UPSILON */
	[768+0x26] = 0x03C6,	/* U+03A6: GREEK CAPITAL LETTER PHI */
	[768+0x27] = 0x03C7,	/* U+03A7: GREEK CAPITAL LETTER CHI */
	[768+0x28] = 0x03C8,	/* U+03A8: GREEK CAPITAL LETTER PSI */
	[768+0x29] = 0x03C9,	/* U+03A9: GREEK CAPITAL LETTER OMEGA */
	[768+0x2A] = 0x03CA,	/* U+03AA: GREEK CAPITAL LETTER IOTA WITH DIALYTIKA */
	[768+0x2B] = 0x03CB,	/* U+03AB: GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA */
	[0xCF] = 832 - 0x80,	/* 317 ... */
	[832+0x0F] = 0x03D7,	/* U+03CF: GREEK CAPITAL KAI SYMBOL */
	[832+0x18] = 0x03D9,	/* U+03D8: GREEK LETTER ARCHAIC KOPPA */
	[832+0x1A] = 0x03DB,	/* U+03DA: GREEK LETTER STIGMA */
	[832+0x1C] = 0x03DD,	/* U+03DC: GREEK LETTER DIGAMMA */
	[832+0x1E] = 0x03DF,	/* U+03DE: GREEK LETTER KOPPA */
	[832+0x20] = 0x03E1,	/* U+03E0: GREEK LETTER SAMPI */
	[832+0x22] = 0x03E3,	/* U+03E2: COPTIC CAPITAL LETTER SHEI */
	[832+0x24] = 0x03E5,	/* U+03E4: COPTIC CAPITAL LETTER FEI */
	[832+0x26] = 0x03E7,	/* U+03E6: COPTIC CAPITAL LETTER KHEI */
	[832+0x28] = 0x03E9,	/* U+03E8: COPTIC CAPITAL LETTER HORI */
	[832+0x2A] = 0x03EB,	/* U+03EA: COPTIC CAPITAL LETTER GANGIA */
	[832+0x2C] = 0x03ED,	/* U+03EC: COPTIC CAPITAL LETTER SHIMA */
	[832+0x2E] = 0x03EF,	/* U+03EE: COPTIC CAPITAL LETTER DEI */
	[832+0x34] = 0x03B8,	/* U+03F4: GREEK CAPITAL THETA SYMBOL */
	[832+0x37] = 0x03F8,	/* U+03F7: GREEK CAPITAL LETTER SHO */
	[832+0x39] = 0x03F2,	/* U+03F9: GREEK CAPITAL LUNATE SIGMA SYMBOL */
	[832+0x3A] = 0x03FB,	/* U+03FA: GREEK CAPITAL LETTER SAN */
	[832+0x3D] = 0x037B,	/* U+03FD: GREEK CAPITAL REVERSED LUNATE SIGMA SYMBOL */
	[832+0x3E] = 0x037C,	/* U+03FE: GREEK CAPITAL DOTTED LUNATE SIGMA SYMBOL */
	[832+0x3F] = 0x037D,	/* U+03FF: GREEK CAPITAL REVERSED DOTTED LUNATE SIGMA SYMBOL */
	[0xD0] = 896 - 0x80,	/* 320 ... */
	[896+0x00] = 0x0450,	/* U+0400: CYRILLIC CAPITAL LETTER IE WITH GRAVE */
	[896+0x01] = 0x0451,	/* U+0401: CYRILLIC CAPITAL LETTER IO */
	[896+0x02] = 0x0452,	/* U+0402: CYRILLIC CAPITAL LETTER DJE */
	[896+0x03] = 0x0453,	/* U+0403: CYRILLIC CAPITAL LETTER GJE */
	[896+0x04] = 0x0454,	/* U+0404: CYRILLIC CAPITAL LETTER UKRAINIAN IE */
	[896+0x05] = 0x0455,	/* U+0405: CYRILLIC CAPITAL LETTER DZE */
	[896+0x06] = 0x0456,	/* U+0406: CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I */
	[896+0x07] = 0x0457,	/* U+0407: CYRILLIC CAPITAL LETTER YI */
	[896+0x08] = 0x0458,	/* U+0408: CYRILLIC CAPITAL LETTER JE */
	[896+0x09] = 0x0459,	/* U+0409: CYRILLIC CAPITAL LETTER LJE */
	[896+0x0A] = 0x045A,	/* U+040A: CYRILLIC CAPITAL LETTER NJE */
	[896+0x0B] = 0x045B,	/* U+040B: CYRILLIC CAPITAL LETTER TSHE */
	[896+0x0C] = 0x045C,	/* U+040C: CYRILLIC CAPITAL LETTER KJE */
	[896+0x0D] = 0x045D,	/* U+040D: CYRILLIC CAPITAL LETTER I WITH GRAVE */
	[896+0x0E] = 0x045E,	/* U+040E: CYRILLIC CAPITAL LETTER SHORT U */
	[896+0x0F] = 0x045F,	/* U+040F: CYRILLIC CAPITAL LETTER DZHE */
	[896+0x10] = 0x0430,	/* U+0410: CYRILLIC CAPITAL LETTER A */
	[896+0x11] = 0x0431,	/* U+0411: CYRILLIC CAPITAL LETTER BE */
	[896+0x12] = 0x0432,	/* U+0412: CYRILLIC CAPITAL LETTER VE */
	[896+0x13] = 0x0433,	/* U+0413: CYRILLIC CAPITAL LETTER GHE */
	[896+0x14] = 0x0434,	/* U+0414: CYRILLIC CAPITAL LETTER DE */
	[896+0x15] = 0x0435,	/* U+0415: CYRILLIC CAPITAL LETTER IE */
	[896+0x16] = 0x0436,	/* U+0416: CYRILLIC CAPITAL LETTER ZHE */
	[896+0x17] = 0x0437,	/* U+0417: CYRILLIC CAPITAL LETTER ZE */
	[896+0x18] = 0x0438,	/* U+0418: CYRILLIC CAPITAL LETTER I */
	[896+0x19] = 0x0439,	/* U+0419: CYRILLIC CAPITAL LETTER SHORT I */
	[896+0x1A] = 0x043A,	/* U+041A: CYRILLIC CAPITAL LETTER KA */
	[896+0x1B] = 0x043B,	/* U+041B: CYRILLIC CAPITAL LETTER EL */
	[896+0x1C] = 0x043C,	/* U+041C: CYRILLIC CAPITAL LETTER EM */
	[896+0x1D] = 0x043D,	/* U+041D: CYRILLIC CAPITAL LETTER EN */
	[896+0x1E] = 0x043E,	/* U+041E: CYRILLIC CAPITAL LETTER O */
	[896+0x1F] = 0x043F,	/* U+041F: CYRILLIC CAPITAL LETTER PE */
	[896+0x20] = 0x0440,	/* U+0420: CYRILLIC CAPITAL LETTER ER */
	[896+0x21] = 0x0441,	/* U+0421: CYRILLIC CAPITAL LETTER ES */
	[896+0x22] = 0x0442,	/* U+0422: CYRILLIC CAPITAL LETTER TE */
	[896+0x23] = 0x0443,	/* U+0423: CYRILLIC CAPITAL LETTER U */
	[896+0x24] = 0x0444,	/* U+0424: CYRILLIC CAPITAL LETTER EF */
	[896+0x25] = 0x0445,	/* U+0425: CYRILLIC CAPITAL LETTER HA */
	[896+0x26] = 0x0446,	/* U+0426: CYRILLIC CAPITAL LETTER TSE */
	[896+0x27] = 0x0447,	/* U+0427: CYRILLIC CAPITAL LETTER CHE */
	[896+0x28] = 0x0448,	/* U+0428: CYRILLIC CAPITAL LETTER SHA */
	[896+0x29] = 0x0449,	/* U+0429: CYRILLIC CAPITAL LETTER SHCHA */
	[896+0x2A] = 0x044A,	/* U+042A: CYRILLIC CAPITAL LETTER HARD SIGN */
	[896+0x2B] = 0x044B,	/* U+042B: CYRILLIC CAPITAL LETTER YERU */
	[896+0x2C] = 0x044C,	/* U+042C: CYRILLIC CAPITAL LETTER SOFT SIGN */
	[896+0x2D] = 0x044D,	/* U+042D: CYRILLIC CAPITAL LETTER E */
	[896+0x2E] = 0x044E,	/* U+042E: CYRILLIC CAPITAL LETTER YU */
	[896+0x2F] = 0x044F,	/* U+042F: CYRILLIC CAPITAL LETTER YA */
	[0xD1] = 960 - 0x80,	/* 321 ... */
	[960+0x20] = 0x0461,	/* U+0460: CYRILLIC CAPITAL LETTER OMEGA */
	[960+0x22] = 0x0463,	/* U+0462: CYRILLIC CAPITAL LETTER YAT */
	[960+0x24] = 0x0465,	/* U+0464: CYRILLIC CAPITAL LETTER IOTIFIED E */
	[960+0x26] = 0x0467,	/* U+0466: CYRILLIC CAPITAL LETTER LITTLE YUS */
	[960+0x28] = 0x0469,	/* U+0468: CYRILLIC CAPITAL LETTER IOTIFIED LITTLE YUS */
	[960+0x2A] = 0x046B,	/* U+046A: CYRILLIC CAPITAL LETTER BIG YUS */
	[960+0x2C] = 0x046D,	/* U+046C: CYRILLIC CAPITAL LETTER IOTIFIED BIG YUS */
	[960+0x2E] = 0x046F,	/* U+046E: CYRILLIC CAPITAL LETTER KSI */
	[960+0x30] = 0x0471,	/* U+0470: CYRILLIC CAPITAL LETTER PSI */
	[960+0x32] = 0x0473,	/* U+0472: CYRILLIC CAPITAL LETTER FITA */
	[960+0x34] = 0x0475,	/* U+0474: CYRILLIC CAPITAL LETTER IZHITSA */
	[960+0x36] = 0x0477,	/* U+0476: CYRILLIC CAPITAL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT */
	[960+0x38] = 0x0479,	/* U+0478: CYRILLIC CAPITAL LETTER UK */
	[960+0x3A] = 0x047B,	/* U+047A: CYRILLIC CAPITAL LETTER ROUND OMEGA */
	[960+0x3C] = 0x047D,	/* U+047C: CYRILLIC CAPITAL LETTER OMEGA WITH TITLO */
	[960+0x3E] = 0x047F,	/* U+047E: CYRILLIC CAPITAL LETTER OT */
	[0xD2] = 1024 - 0x80,	/* 322 ... */
	[1024+0x00] = 0x0481,	/* U+0480: CYRILLIC CAPITAL LETTER KOPPA */
	[1024+0x0A] = 0x048B,	/* U+048A: CYRILLIC CAPITAL LETTER SHORT I WITH TAIL */
	[1024+0x0C] = 0x048D,	/* U+048C: CYRILLIC CAPITAL LETTER SEMISOFT SIGN */
	[1024+0x0E] = 0x048F,	/* U+048E: CYRILLIC CAPITAL LETTER ER WITH TICK */
	[1024+0x10] = 0x0491,	/* U+0490: CYRILLIC CAPITAL LETTER GHE WITH UPTURN */
	[1024+0x12] = 0x0493,	/* U+0492: CYRILLIC CAPITAL LETTER GHE WITH STROKE */
	[1024+0x14] = 0x0495,	/* U+0494: CYRILLIC CAPITAL LETTER GHE WITH MIDDLE HOOK */
	[1024+0x16] = 0x0497,	/* U+0496: CYRILLIC CAPITAL LETTER ZHE WITH DESCENDER */
	[1024+0x18] = 0x0499,	/* U+0498: CYRILLIC CAPITAL LETTER ZE WITH DESCENDER */
	[1024+0x1A] = 0x049B,	/* U+049A: CYRILLIC CAPITAL LETTER KA WITH DESCENDER */
	[1024+0x1C] = 0x049D,	/* U+049C: CYRILLIC CAPITAL LETTER KA WITH VERTICAL STROKE */
	[1024+0x1E] = 0x049F,	/* U+049E: CYRILLIC CAPITAL LETTER KA WITH STROKE */
	[1024+0x20] = 0x04A1,	/* U+04A0: CYRILLIC CAPITAL LETTER BASHKIR KA */
	[1024+0x22] = 0x04A3,	/* U+04A2: CYRILLIC CAPITAL LETTER EN WITH DESCENDER */
	[1024+0x24] = 0x04A5,	/* U+04A4: CYRILLIC CAPITAL LIGATURE EN GHE */
	[1024+0x26] = 0x04A7,	/* U+04A6: CYRILLIC CAPITAL LETTER PE WITH MIDDLE HOOK */
	[1024+0x28] = 0x04A9,	/* U+04A8: CYRILLIC CAPITAL LETTER ABKHASIAN HA */
	[1024+0x2A] = 0x04AB,	/* U+04AA: CYRILLIC CAPITAL LETTER ES WITH DESCENDER */
	[1024+0x2C] = 0x04AD,	/* U+04AC: CYRILLIC CAPITAL LETTER TE WITH DESCENDER */
	[1024+0x2E] = 0x04AF,	/* U+04AE: CYRILLIC CAPITAL LETTER STRAIGHT U */
	[1024+0x30] = 0x04B1,	/* U+04B0: CYRILLIC CAPITAL LETTER STRAIGHT U WITH STROKE */
	[1024+0x32] = 0x04B3,	/* U+04B2: CYRILLIC CAPITAL LETTER HA WITH DESCENDER */
	[1024+0x34] = 0x04B5,	/* U+04B4: CYRILLIC CAPITAL LIGATURE TE TSE */
	[1024+0x36] = 0x04B7,	/* U+04B6: CYRILLIC CAPITAL LETTER CHE WITH DESCENDER */
	[1024+0x38] = 0x04B9,	/* U+04B8: CYRILLIC CAPITAL LETTER CHE WITH VERTICAL STROKE */
	[1024+0x3A] = 0x04BB,	/* U+04BA: CYRILLIC CAPITAL LETTER SHHA */
	[1024+0x3C] = 0x04BD,	/* U+04BC: CYRILLIC CAPITAL LETTER ABKHASIAN CHE */
	[1024+0x3E] = 0x04BF,	/* U+04BE: CYRILLIC CAPITAL LETTER ABKHASIAN CHE WITH DESCENDER */
	[0xD3] = 1088 - 0x80,	/* 323 ... */
	[1088+0x00] = 0x04CF,	/* U+04C0: CYRILLIC LETTER PALOCHKA */
	[1088+0x01] = 0x04C2,	/* U+04C1: CYRILLIC CAPITAL LETTER ZHE WITH BREVE */
	[1088+0x03] = 0x04C4,	/* U+04C3: CYRILLIC CAPITAL LETTER KA WITH HOOK */
	[1088+0x05] = 0x04C6,	/* U+04C5: CYRILLIC CAPITAL LETTER EL WITH TAIL */
	[1088+0x07] = 0x04C8,	/* U+04C7: CYRILLIC CAPITAL LETTER EN WITH HOOK */
	[1088+0x09] = 0x04CA,	/* U+04C9: CYRILLIC CAPITAL LETTER EN WITH TAIL */
	[1088+0x0B] = 0x04CC,	/* U+04CB: CYRILLIC CAPITAL LETTER KHAKASSIAN CHE */
	[1088+0x0D] = 0x04CE,	/* U+04CD: CYRILLIC CAPITAL LETTER EM WITH TAIL */
	[1088+0x10] = 0x04D1,	/* U+04D0: CYRILLIC CAPITAL LETTER A WITH BREVE */
	[1088+0x12] = 0x04D3,	/* U+04D2: CYRILLIC CAPITAL LETTER A WITH DIAERESIS */
	[1088+0x14] = 0x04D5,	/* U+04D4: CYRILLIC CAPITAL LIGATURE A IE */
	[1088+0x16] = 0x04D7,	/* U+04D6: CYRILLIC CAPITAL LETTER IE WITH BREVE */
	[1088+0x18] = 0x04D9,	/* U+04D8: CYRILLIC CAPITAL LETTER SCHWA */
	[1088+0x1A] = 0x04DB,	/* U+04DA: CYRILLIC CAPITAL LETTER SCHWA WITH DIAERESIS */
	[1088+0x1C] = 0x04DD,	/* U+04DC: CYRILLIC CAPITAL LETTER ZHE WITH DIAERESIS */
	[1088+0x1E] = 0x04DF,	/* U+04DE: CYRILLIC CAPITAL LETTER ZE WITH DIAERESIS */
	[1088+0x20] = 0x04E1,	/* U+04E0: CYRILLIC CAPITAL LETTER ABKHASIAN DZE */
	[1088+0x22] = 0x04E3,	/* U+04E2: CYRILLIC CAPITAL LETTER I WITH MACRON */
	[1088+0x24] = 0x04E5,	/* U+04E4: CYRILLIC CAPITAL LETTER I WITH DIAERESIS */
	[1088+0x26] = 0x04E7,	/* U+04E6: CYRILLIC CAPITAL LETTER O WITH DIAERESIS */
	[1088+0x28] = 0x04E9,	/* U+04E8: CYRILLIC CAPITAL LETTER BARRED O */
	[1088+0x2A] = 0x04EB,	/* U+04EA: CYRILLIC CAPITAL LETTER BARRED O WITH DIAERESIS */
	[1088+0x2C] = 0x04ED,	/* U+04EC: CYRILLIC CAPITAL LETTER E WITH DIAERESIS */
	[1088+0x2E] = 0x04EF,	/* U+04EE: CYRILLIC CAPITAL LETTER U WITH MACRON */
	[1088+0x30] = 0x04F1,	/* U+04F0: CYRILLIC CAPITAL LETTER U WITH DIAERESIS */
	[1088+0x32] = 0x04F3,	/* U+04F2: CYRILLIC CAPITAL LETTER U WITH DOUBLE ACUTE */
	[1088+0x34] = 0x04F5,	/* U+04F4: CYRILLIC CAPITAL LETTER CHE WITH DIAERESIS */
	[1088+0x36] = 0x04F7,	/* U+04F6: CYRILLIC CAPITAL LETTER GHE WITH DESCENDER */
	[1088+0x38] = 0x04F9,	/* U+04F8: CYRILLIC CAPITAL LETTER YERU WITH DIAERESIS */
	[1088+0x3A] = 0x04FB,	/* U+04FA: CYRILLIC CAPITAL LETTER GHE WITH STROKE AND HOOK */
	[1088+0x3C] = 0x04FD,	/* U+04FC: CYRILLIC CAPITAL LETTER HA WITH HOOK */
	[1088+0x3E] = 0x04FF,	/* U+04FE: CYRILLIC CAPITAL LETTER HA WITH STROKE */
	[0xD4] = 1152 - 0x80,	/* 324 ... */
	[1152+0x00] = 0x0501,	/* U+0500: CYRILLIC CAPITAL LETTER KOMI DE */
	[1152+0x02] = 0x0503,	/* U+0502: CYRILLIC CAPITAL LETTER KOMI DJE */
	[1152+0x04] = 0x0505,	/* U+0504: CYRILLIC CAPITAL LETTER KOMI ZJE */
	[1152+0x06] = 0x0507,	/* U+0506: CYRILLIC CAPITAL LETTER KOMI DZJE */
	[1152+0x08] = 0x0509,	/* U+0508: CYRILLIC CAPITAL LETTER KOMI LJE */
	[1152+0x0A] = 0x050B,	/* U+050A: CYRILLIC CAPITAL LETTER KOMI NJE */
	[1152+0x0C] = 0x050D,	/* U+050C: CYRILLIC CAPITAL LETTER KOMI SJE */
	[1152+0x0E] = 0x050F,	/* U+050E: CYRILLIC CAPITAL LETTER KOMI TJE */
	[1152+0x10] = 0x0511,	/* U+0510: CYRILLIC CAPITAL LETTER REVERSED ZE */
	[1152+0x12] = 0x0513,	/* U+0512: CYRILLIC CAPITAL LETTER EL WITH HOOK */
	[1152+0x14] = 0x0515,	/* U+0514: CYRILLIC CAPITAL LETTER LHA */
	[1152+0x16] = 0x0517,	/* U+0516: CYRILLIC CAPITAL LETTER RHA */
	[1152+0x18] = 0x0519,	/* U+0518: CYRILLIC CAPITAL LETTER YAE */
	[1152+0x1A] = 0x051B,	/* U+051A: CYRILLIC CAPITAL LETTER QA */
	[1152+0x1C] = 0x051D,	/* U+051C: CYRILLIC CAPITAL LETTER WE */
	[1152+0x1E] = 0x051F,	/* U+051E: CYRILLIC CAPITAL LETTER ALEUT KA */
	[1152+0x20] = 0x0521,	/* U+0520: CYRILLIC CAPITAL LETTER EL WITH MIDDLE HOOK */
	[1152+0x22] = 0x0523,	/* U+0522: CYRILLIC CAPITAL LETTER EN WITH MIDDLE HOOK */
	[1152+0x24] = 0x0525,	/* U+0524: CYRILLIC CAPITAL LETTER PE WITH DESCENDER */
	[1152+0x26] = 0x0527,	/* U+0526: CYRILLIC CAPITAL LETTER SHHA WITH DESCENDER */
	[1152+0x28] = 0x0529,	/* U+0528: CYRILLIC CAPITAL LETTER EN WITH LEFT HOOK */
	[1152+0x2A] = 0x052B,	/* U+052A: CYRILLIC CAPITAL LETTER DZZHE */
	[1152+0x2C] = 0x052D,	/* U+052C: CYRILLIC CAPITAL LETTER DCHE */
	[1152+0x2E] = 0x052F,	/* U+052E: CYRILLIC CAPITAL LETTER EL WITH DESCENDER */
	[1152+0x31] = 0x0561,	/* U+0531: ARMENIAN CAPITAL LETTER AYB */
	[1152+0x32] = 0x0562,	/* U+0532: ARMENIAN CAPITAL LETTER BEN */
	[1152+0x33] = 0x0563,	/* U+0533: ARMENIAN CAPITAL LETTER GIM */
	[1152+0x34] = 0x0564,	/* U+0534: ARMENIAN CAPITAL LETTER DA */
	[1152+0x35] = 0x0565,	/* U+0535: ARMENIAN CAPITAL LETTER ECH */
	[1152+0x36] = 0x0566,	/* U+0536: ARMENIAN CAPITAL LETTER ZA */
	[1152+0x37] = 0x0567,	/* U+0537: ARMENIAN CAPITAL LETTER EH */
	[1152+0x38] = 0x0568,	/* U+0538: ARMENIAN CAPITAL LETTER ET */
	[1152+0x39] = 0x0569,	/* U+0539: ARMENIAN CAPITAL LETTER TO */
	[1152+0x3A] = 0x056A,	/* U+053A: ARMENIAN CAPITAL LETTER ZHE */
	[1152+0x3B] = 0x056B,	/* U+053B: ARMENIAN CAPITAL LETTER INI */
	[1152+0x3C] = 0x056C,	/* U+053C: ARMENIAN CAPITAL LETTER LIWN */
	[1152+0x3D] = 0x056D,	/* U+053D: ARMENIAN CAPITAL LETTER XEH */
	[1152+0x3E] = 0x056E,	/* U+053E: ARMENIAN CAPITAL LETTER CA */
	[1152+0x3F] = 0x056F,	/* U+053F: ARMENIAN CAPITAL LETTER KEN */
	[0xD5] = 1216 - 0x80,	/* 325 ... */
	[1216+0x00] = 0x0570,	/* U+0540: ARMENIAN CAPITAL LETTER HO */
	[1216+0x01] = 0x0571,	/* U+0541: ARMENIAN CAPITAL LETTER JA */
	[1216+0x02] = 0x0572,	/* U+0542: ARMENIAN CAPITAL LETTER GHAD */
	[1216+0x03] = 0x0573,	/* U+0543: ARMENIAN CAPITAL LETTER CHEH */
	[1216+0x04] = 0x0574,	/* U+0544: ARMENIAN CAPITAL LETTER MEN */
	[1216+0x05] = 0x0575,	/* U+0545: ARMENIAN CAPITAL LETTER YI */
	[1216+0x06] = 0x0576,	/* U+0546: ARMENIAN CAPITAL LETTER NOW */
	[1216+0x07] = 0x0577,	/* U+0547: ARMENIAN CAPITAL LETTER SHA */
	[1216+0x08] = 0x0578,	/* U+0548: ARMENIAN CAPITAL LETTER VO */
	[1216+0x09] = 0x0579,	/* U+0549: ARMENIAN CAPITAL LETTER CHA */
	[1216+0x0A] = 0x057A,	/* U+054A: ARMENIAN CAPITAL LETTER PEH */
	[1216+0x0B] = 0x057B,	/* U+054B: ARMENIAN CAPITAL LETTER JHEH */
	[1216+0x0C] = 0x057C,	/* U+054C: ARMENIAN CAPITAL LETTER RA */
	[1216+0x0D] = 0x057D,	/* U+054D: ARMENIAN CAPITAL LETTER SEH */
	[1216+0x0E] = 0x057E,	/* U+054E: ARMENIAN CAPITAL LETTER VEW */
	[1216+0x0F] = 0x057F,	/* U+054F: ARMENIAN CAPITAL LETTER TIWN */
	[1216+0x10] = 0x0580,	/* U+0550: ARMENIAN CAPITAL LETTER REH */
	[1216+0x11] = 0x0581,	/* U+0551: ARMENIAN CAPITAL LETTER CO */
	[1216+0x12] = 0x0582,	/* U+0552: ARMENIAN CAPITAL LETTER YIWN */
	[1216+0x13] = 0x0583,	/* U+0553: ARMENIAN CAPITAL LETTER PIWR */
	[1216+0x14] = 0x0584,	/* U+0554: ARMENIAN CAPITAL LETTER KEH */
	[1216+0x15] = 0x0585,	/* U+0555: ARMENIAN CAPITAL LETTER OH */
	[1216+0x16] = 0x0586,	/* U+0556: ARMENIAN CAPITAL LETTER FEH */
	[0xE1] = 1280 - 0x80,	/* 341 ... */
	[1280+0x02] = 1344 - 0x80,	/* 341 202 ... */
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
	[1280+0x03] = 1408 - 0x80,	/* 341 203 ... */
	[1408+0x00] = 0x2D20,	/* U+10C0: GEORGIAN CAPITAL LETTER HAE */
	[1408+0x01] = 0x2D21,	/* U+10C1: GEORGIAN CAPITAL LETTER HE */
	[1408+0x02] = 0x2D22,	/* U+10C2: GEORGIAN CAPITAL LETTER HIE */
	[1408+0x03] = 0x2D23,	/* U+10C3: GEORGIAN CAPITAL LETTER WE */
	[1408+0x04] = 0x2D24,	/* U+10C4: GEORGIAN CAPITAL LETTER HAR */
	[1408+0x05] = 0x2D25,	/* U+10C5: GEORGIAN CAPITAL LETTER HOE */
	[1408+0x07] = 0x2D27,	/* U+10C7: GEORGIAN CAPITAL LETTER YN */
	[1408+0x0D] = 0x2D2D,	/* U+10CD: GEORGIAN CAPITAL LETTER AEN */
	[1280+0x0E] = 1472 - 0x80,	/* 341 216 ... */
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
	[1280+0x0F] = 1536 - 0x80,	/* 341 217 ... */
	[1536+0x00] = 0xAB90,	/* U+13C0: CHEROKEE LETTER NAH */
	[1536+0x01] = 0xAB91,	/* U+13C1: CHEROKEE LETTER NE */
	[1536+0x02] = 0xAB92,	/* U+13C2: CHEROKEE LETTER NI */
	[1536+0x03] = 0xAB93,	/* U+13C3: CHEROKEE LETTER NO */
	[1536+0x04] = 0xAB94,	/* U+13C4: CHEROKEE LETTER NU */
	[1536+0x05] = 0xAB95,	/* U+13C5: CHEROKEE LETTER NV */
	[1536+0x06] = 0xAB96,	/* U+13C6: CHEROKEE LETTER QUA */
	[1536+0x07] = 0xAB97,	/* U+13C7: CHEROKEE LETTER QUE */
	[1536+0x08] = 0xAB98,	/* U+13C8: CHEROKEE LETTER QUI */
	[1536+0x09] = 0xAB99,	/* U+13C9: CHEROKEE LETTER QUO */
	[1536+0x0A] = 0xAB9A,	/* U+13CA: CHEROKEE LETTER QUU */
	[1536+0x0B] = 0xAB9B,	/* U+13CB: CHEROKEE LETTER QUV */
	[1536+0x0C] = 0xAB9C,	/* U+13CC: CHEROKEE LETTER SA */
	[1536+0x0D] = 0xAB9D,	/* U+13CD: CHEROKEE LETTER S */
	[1536+0x0E] = 0xAB9E,	/* U+13CE: CHEROKEE LETTER SE */
	[1536+0x0F] = 0xAB9F,	/* U+13CF: CHEROKEE LETTER SI */
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
	[1664+0x00] = 0x1E01,	/* U+1E00: LATIN CAPITAL LETTER A WITH RING BELOW */
	[1664+0x02] = 0x1E03,	/* U+1E02: LATIN CAPITAL LETTER B WITH DOT ABOVE */
	[1664+0x04] = 0x1E05,	/* U+1E04: LATIN CAPITAL LETTER B WITH DOT BELOW */
	[1664+0x06] = 0x1E07,	/* U+1E06: LATIN CAPITAL LETTER B WITH LINE BELOW */
	[1664+0x08] = 0x1E09,	/* U+1E08: LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE */
	[1664+0x0A] = 0x1E0B,	/* U+1E0A: LATIN CAPITAL LETTER D WITH DOT ABOVE */
	[1664+0x0C] = 0x1E0D,	/* U+1E0C: LATIN CAPITAL LETTER D WITH DOT BELOW */
	[1664+0x0E] = 0x1E0F,	/* U+1E0E: LATIN CAPITAL LETTER D WITH LINE BELOW */
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
	[1728+0x00] = 0x1E41,	/* U+1E40: LATIN CAPITAL LETTER M WITH DOT ABOVE */
	[1728+0x02] = 0x1E43,	/* U+1E42: LATIN CAPITAL LETTER M WITH DOT BELOW */
	[1728+0x04] = 0x1E45,	/* U+1E44: LATIN CAPITAL LETTER N WITH DOT ABOVE */
	[1728+0x06] = 0x1E47,	/* U+1E46: LATIN CAPITAL LETTER N WITH DOT BELOW */
	[1728+0x08] = 0x1E49,	/* U+1E48: LATIN CAPITAL LETTER N WITH LINE BELOW */
	[1728+0x0A] = 0x1E4B,	/* U+1E4A: LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW */
	[1728+0x0C] = 0x1E4D,	/* U+1E4C: LATIN CAPITAL LETTER O WITH TILDE AND ACUTE */
	[1728+0x0E] = 0x1E4F,	/* U+1E4E: LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS */
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
	[1792+0x00] = 0x1E81,	/* U+1E80: LATIN CAPITAL LETTER W WITH GRAVE */
	[1792+0x02] = 0x1E83,	/* U+1E82: LATIN CAPITAL LETTER W WITH ACUTE */
	[1792+0x04] = 0x1E85,	/* U+1E84: LATIN CAPITAL LETTER W WITH DIAERESIS */
	[1792+0x06] = 0x1E87,	/* U+1E86: LATIN CAPITAL LETTER W WITH DOT ABOVE */
	[1792+0x08] = 0x1E89,	/* U+1E88: LATIN CAPITAL LETTER W WITH DOT BELOW */
	[1792+0x0A] = 0x1E8B,	/* U+1E8A: LATIN CAPITAL LETTER X WITH DOT ABOVE */
	[1792+0x0C] = 0x1E8D,	/* U+1E8C: LATIN CAPITAL LETTER X WITH DIAERESIS */
	[1792+0x0E] = 0x1E8F,	/* U+1E8E: LATIN CAPITAL LETTER Y WITH DOT ABOVE */
	[1792+0x10] = 0x1E91,	/* U+1E90: LATIN CAPITAL LETTER Z WITH CIRCUMFLEX */
	[1792+0x12] = 0x1E93,	/* U+1E92: LATIN CAPITAL LETTER Z WITH DOT BELOW */
	[1792+0x14] = 0x1E95,	/* U+1E94: LATIN CAPITAL LETTER Z WITH LINE BELOW */
	[1792+0x1E] = 0x00DF,	/* U+1E9E: LATIN CAPITAL LETTER SHARP S */
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
	[1856+0x00] = 0x1EC1,	/* U+1EC0: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[1856+0x02] = 0x1EC3,	/* U+1EC2: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[1856+0x04] = 0x1EC5,	/* U+1EC4: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE */
	[1856+0x06] = 0x1EC7,	/* U+1EC6: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[1856+0x08] = 0x1EC9,	/* U+1EC8: LATIN CAPITAL LETTER I WITH HOOK ABOVE */
	[1856+0x0A] = 0x1ECB,	/* U+1ECA: LATIN CAPITAL LETTER I WITH DOT BELOW */
	[1856+0x0C] = 0x1ECD,	/* U+1ECC: LATIN CAPITAL LETTER O WITH DOT BELOW */
	[1856+0x0E] = 0x1ECF,	/* U+1ECE: LATIN CAPITAL LETTER O WITH HOOK ABOVE */
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
	[1920+0x08] = 0x1F00,	/* U+1F08: GREEK CAPITAL LETTER ALPHA WITH PSILI */
	[1920+0x09] = 0x1F01,	/* U+1F09: GREEK CAPITAL LETTER ALPHA WITH DASIA */
	[1920+0x0A] = 0x1F02,	/* U+1F0A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA */
	[1920+0x0B] = 0x1F03,	/* U+1F0B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA */
	[1920+0x0C] = 0x1F04,	/* U+1F0C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA */
	[1920+0x0D] = 0x1F05,	/* U+1F0D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA */
	[1920+0x0E] = 0x1F06,	/* U+1F0E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI */
	[1920+0x0F] = 0x1F07,	/* U+1F0F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI */
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
	[1984+0x08] = 0x1F40,	/* U+1F48: GREEK CAPITAL LETTER OMICRON WITH PSILI */
	[1984+0x09] = 0x1F41,	/* U+1F49: GREEK CAPITAL LETTER OMICRON WITH DASIA */
	[1984+0x0A] = 0x1F42,	/* U+1F4A: GREEK CAPITAL LETTER OMICRON WITH PSILI AND VARIA */
	[1984+0x0B] = 0x1F43,	/* U+1F4B: GREEK CAPITAL LETTER OMICRON WITH DASIA AND VARIA */
	[1984+0x0C] = 0x1F44,	/* U+1F4C: GREEK CAPITAL LETTER OMICRON WITH PSILI AND OXIA */
	[1984+0x0D] = 0x1F45,	/* U+1F4D: GREEK CAPITAL LETTER OMICRON WITH DASIA AND OXIA */
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
	[2048+0x08] = 0x1F80,	/* U+1F88: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI */
	[2048+0x09] = 0x1F81,	/* U+1F89: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI */
	[2048+0x0A] = 0x1F82,	/* U+1F8A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2048+0x0B] = 0x1F83,	/* U+1F8B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2048+0x0C] = 0x1F84,	/* U+1F8C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2048+0x0D] = 0x1F85,	/* U+1F8D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2048+0x0E] = 0x1F86,	/* U+1F8E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2048+0x0F] = 0x1F87,	/* U+1F8F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
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
	[2112+0x08] = 0x1F72,	/* U+1FC8: GREEK CAPITAL LETTER EPSILON WITH VARIA */
	[2112+0x09] = 0x1F73,	/* U+1FC9: GREEK CAPITAL LETTER EPSILON WITH OXIA */
	[2112+0x0A] = 0x1F74,	/* U+1FCA: GREEK CAPITAL LETTER ETA WITH VARIA */
	[2112+0x0B] = 0x1F75,	/* U+1FCB: GREEK CAPITAL LETTER ETA WITH OXIA */
	[2112+0x0C] = 0x1FC3,	/* U+1FCC: GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI */
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
	[2176+0x04] = 2240 - 0x80,	/* 342 204 ... */
	[2240+0x26] = 0x03C9,	/* U+2126: OHM SIGN */
	[2240+0x2A] = 0x006B,	/* U+212A: KELVIN SIGN */
	[2240+0x2B] = 0x00E5,	/* U+212B: ANGSTROM SIGN */
	[2240+0x32] = 0x214E,	/* U+2132: TURNED CAPITAL F */
	[2176+0x05] = 2304 - 0x80,	/* 342 205 ... */
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
	[2176+0x06] = 2368 - 0x80,	/* 342 206 ... */
	[2368+0x03] = 0x2184,	/* U+2183: ROMAN NUMERAL REVERSED ONE HUNDRED */
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
	[2496+0x00] = 0x24DA,	/* U+24C0: CIRCLED LATIN CAPITAL LETTER K */
	[2496+0x01] = 0x24DB,	/* U+24C1: CIRCLED LATIN CAPITAL LETTER L */
	[2496+0x02] = 0x24DC,	/* U+24C2: CIRCLED LATIN CAPITAL LETTER M */
	[2496+0x03] = 0x24DD,	/* U+24C3: CIRCLED LATIN CAPITAL LETTER N */
	[2496+0x04] = 0x24DE,	/* U+24C4: CIRCLED LATIN CAPITAL LETTER O */
	[2496+0x05] = 0x24DF,	/* U+24C5: CIRCLED LATIN CAPITAL LETTER P */
	[2496+0x06] = 0x24E0,	/* U+24C6: CIRCLED LATIN CAPITAL LETTER Q */
	[2496+0x07] = 0x24E1,	/* U+24C7: CIRCLED LATIN CAPITAL LETTER R */
	[2496+0x08] = 0x24E2,	/* U+24C8: CIRCLED LATIN CAPITAL LETTER S */
	[2496+0x09] = 0x24E3,	/* U+24C9: CIRCLED LATIN CAPITAL LETTER T */
	[2496+0x0A] = 0x24E4,	/* U+24CA: CIRCLED LATIN CAPITAL LETTER U */
	[2496+0x0B] = 0x24E5,	/* U+24CB: CIRCLED LATIN CAPITAL LETTER V */
	[2496+0x0C] = 0x24E6,	/* U+24CC: CIRCLED LATIN CAPITAL LETTER W */
	[2496+0x0D] = 0x24E7,	/* U+24CD: CIRCLED LATIN CAPITAL LETTER X */
	[2496+0x0E] = 0x24E8,	/* U+24CE: CIRCLED LATIN CAPITAL LETTER Y */
	[2496+0x0F] = 0x24E9,	/* U+24CF: CIRCLED LATIN CAPITAL LETTER Z */
	[2176+0x30] = 2560 - 0x80,	/* 342 260 ... */
	[2560+0x00] = 0x2C30,	/* U+2C00: GLAGOLITIC CAPITAL LETTER AZU */
	[2560+0x01] = 0x2C31,	/* U+2C01: GLAGOLITIC CAPITAL LETTER BUKY */
	[2560+0x02] = 0x2C32,	/* U+2C02: GLAGOLITIC CAPITAL LETTER VEDE */
	[2560+0x03] = 0x2C33,	/* U+2C03: GLAGOLITIC CAPITAL LETTER GLAGOLI */
	[2560+0x04] = 0x2C34,	/* U+2C04: GLAGOLITIC CAPITAL LETTER DOBRO */
	[2560+0x05] = 0x2C35,	/* U+2C05: GLAGOLITIC CAPITAL LETTER YESTU */
	[2560+0x06] = 0x2C36,	/* U+2C06: GLAGOLITIC CAPITAL LETTER ZHIVETE */
	[2560+0x07] = 0x2C37,	/* U+2C07: GLAGOLITIC CAPITAL LETTER DZELO */
	[2560+0x08] = 0x2C38,	/* U+2C08: GLAGOLITIC CAPITAL LETTER ZEMLJA */
	[2560+0x09] = 0x2C39,	/* U+2C09: GLAGOLITIC CAPITAL LETTER IZHE */
	[2560+0x0A] = 0x2C3A,	/* U+2C0A: GLAGOLITIC CAPITAL LETTER INITIAL IZHE */
	[2560+0x0B] = 0x2C3B,	/* U+2C0B: GLAGOLITIC CAPITAL LETTER I */
	[2560+0x0C] = 0x2C3C,	/* U+2C0C: GLAGOLITIC CAPITAL LETTER DJERVI */
	[2560+0x0D] = 0x2C3D,	/* U+2C0D: GLAGOLITIC CAPITAL LETTER KAKO */
	[2560+0x0E] = 0x2C3E,	/* U+2C0E: GLAGOLITIC CAPITAL LETTER LJUDIJE */
	[2560+0x0F] = 0x2C3F,	/* U+2C0F: GLAGOLITIC CAPITAL LETTER MYSLITE */
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
	[2624+0x22] = 0x026B,	/* U+2C62: LATIN CAPITAL LETTER L WITH MIDDLE TILDE */
	[2624+0x23] = 0x1D7D,	/* U+2C63: LATIN CAPITAL LETTER P WITH STROKE */
	[2624+0x24] = 0x027D,	/* U+2C64: LATIN CAPITAL LETTER R WITH TAIL */
	[2624+0x27] = 0x2C68,	/* U+2C67: LATIN CAPITAL LETTER H WITH DESCENDER */
	[2624+0x29] = 0x2C6A,	/* U+2C69: LATIN CAPITAL LETTER K WITH DESCENDER */
	[2624+0x2B] = 0x2C6C,	/* U+2C6B: LATIN CAPITAL LETTER Z WITH DESCENDER */
	[2624+0x2D] = 0x0251,	/* U+2C6D: LATIN CAPITAL LETTER ALPHA */
	[2624+0x2E] = 0x0271,	/* U+2C6E: LATIN CAPITAL LETTER M WITH HOOK */
	[2624+0x2F] = 0x0250,	/* U+2C6F: LATIN CAPITAL LETTER TURNED A */
	[2624+0x30] = 0x0252,	/* U+2C70: LATIN CAPITAL LETTER TURNED ALPHA */
	[2624+0x32] = 0x2C73,	/* U+2C72: LATIN CAPITAL LETTER W WITH HOOK */
	[2624+0x35] = 0x2C76,	/* U+2C75: LATIN CAPITAL LETTER HALF H */
	[2624+0x3E] = 0x023F,	/* U+2C7E: LATIN CAPITAL LETTER S WITH SWASH TAIL */
	[2624+0x3F] = 0x0240,	/* U+2C7F: LATIN CAPITAL LETTER Z WITH SWASH TAIL */
	[2176+0x32] = 2688 - 0x80,	/* 342 262 ... */
	[2688+0x00] = 0x2C81,	/* U+2C80: COPTIC CAPITAL LETTER ALFA */
	[2688+0x02] = 0x2C83,	/* U+2C82: COPTIC CAPITAL LETTER VIDA */
	[2688+0x04] = 0x2C85,	/* U+2C84: COPTIC CAPITAL LETTER GAMMA */
	[2688+0x06] = 0x2C87,	/* U+2C86: COPTIC CAPITAL LETTER DALDA */
	[2688+0x08] = 0x2C89,	/* U+2C88: COPTIC CAPITAL LETTER EIE */
	[2688+0x0A] = 0x2C8B,	/* U+2C8A: COPTIC CAPITAL LETTER SOU */
	[2688+0x0C] = 0x2C8D,	/* U+2C8C: COPTIC CAPITAL LETTER ZATA */
	[2688+0x0E] = 0x2C8F,	/* U+2C8E: COPTIC CAPITAL LETTER HATE */
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
	[2752+0x00] = 0x2CC1,	/* U+2CC0: COPTIC CAPITAL LETTER SAMPI */
	[2752+0x02] = 0x2CC3,	/* U+2CC2: COPTIC CAPITAL LETTER CROSSED SHEI */
	[2752+0x04] = 0x2CC5,	/* U+2CC4: COPTIC CAPITAL LETTER OLD COPTIC SHEI */
	[2752+0x06] = 0x2CC7,	/* U+2CC6: COPTIC CAPITAL LETTER OLD COPTIC ESH */
	[2752+0x08] = 0x2CC9,	/* U+2CC8: COPTIC CAPITAL LETTER AKHMIMIC KHEI */
	[2752+0x0A] = 0x2CCB,	/* U+2CCA: COPTIC CAPITAL LETTER DIALECT-P HORI */
	[2752+0x0C] = 0x2CCD,	/* U+2CCC: COPTIC CAPITAL LETTER OLD COPTIC HORI */
	[2752+0x0E] = 0x2CCF,	/* U+2CCE: COPTIC CAPITAL LETTER OLD COPTIC HA */
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
	[2880+0x00] = 0xA641,	/* U+A640: CYRILLIC CAPITAL LETTER ZEMLYA */
	[2880+0x02] = 0xA643,	/* U+A642: CYRILLIC CAPITAL LETTER DZELO */
	[2880+0x04] = 0xA645,	/* U+A644: CYRILLIC CAPITAL LETTER REVERSED DZE */
	[2880+0x06] = 0xA647,	/* U+A646: CYRILLIC CAPITAL LETTER IOTA */
	[2880+0x08] = 0xA649,	/* U+A648: CYRILLIC CAPITAL LETTER DJERV */
	[2880+0x0A] = 0xA64B,	/* U+A64A: CYRILLIC CAPITAL LETTER MONOGRAPH UK */
	[2880+0x0C] = 0xA64D,	/* U+A64C: CYRILLIC CAPITAL LETTER BROAD OMEGA */
	[2880+0x0E] = 0xA64F,	/* U+A64E: CYRILLIC CAPITAL LETTER NEUTRAL YER */
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
	[2944+0x00] = 0xA681,	/* U+A680: CYRILLIC CAPITAL LETTER DWE */
	[2944+0x02] = 0xA683,	/* U+A682: CYRILLIC CAPITAL LETTER DZWE */
	[2944+0x04] = 0xA685,	/* U+A684: CYRILLIC CAPITAL LETTER ZHWE */
	[2944+0x06] = 0xA687,	/* U+A686: CYRILLIC CAPITAL LETTER CCHE */
	[2944+0x08] = 0xA689,	/* U+A688: CYRILLIC CAPITAL LETTER DZZE */
	[2944+0x0A] = 0xA68B,	/* U+A68A: CYRILLIC CAPITAL LETTER TE WITH MIDDLE HOOK */
	[2944+0x0C] = 0xA68D,	/* U+A68C: CYRILLIC CAPITAL LETTER TWE */
	[2944+0x0E] = 0xA68F,	/* U+A68E: CYRILLIC CAPITAL LETTER TSWE */
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
	[3072+0x00] = 0xA741,	/* U+A740: LATIN CAPITAL LETTER K WITH STROKE */
	[3072+0x02] = 0xA743,	/* U+A742: LATIN CAPITAL LETTER K WITH DIAGONAL STROKE */
	[3072+0x04] = 0xA745,	/* U+A744: LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE */
	[3072+0x06] = 0xA747,	/* U+A746: LATIN CAPITAL LETTER BROKEN L */
	[3072+0x08] = 0xA749,	/* U+A748: LATIN CAPITAL LETTER L WITH HIGH STROKE */
	[3072+0x0A] = 0xA74B,	/* U+A74A: LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY */
	[3072+0x0C] = 0xA74D,	/* U+A74C: LATIN CAPITAL LETTER O WITH LOOP */
	[3072+0x0E] = 0xA74F,	/* U+A74E: LATIN CAPITAL LETTER OO */
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
	[3136+0x00] = 0xA781,	/* U+A780: LATIN CAPITAL LETTER TURNED L */
	[3136+0x02] = 0xA783,	/* U+A782: LATIN CAPITAL LETTER INSULAR R */
	[3136+0x04] = 0xA785,	/* U+A784: LATIN CAPITAL LETTER INSULAR S */
	[3136+0x06] = 0xA787,	/* U+A786: LATIN CAPITAL LETTER INSULAR T */
	[3136+0x0B] = 0xA78C,	/* U+A78B: LATIN CAPITAL LETTER SALTILLO */
	[3136+0x0D] = 0x0265,	/* U+A78D: LATIN CAPITAL LETTER TURNED H */
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
	[3136+0x2A] = 0x0266,	/* U+A7AA: LATIN CAPITAL LETTER H WITH HOOK */
	[3136+0x2B] = 0x025C,	/* U+A7AB: LATIN CAPITAL LETTER REVERSED OPEN E */
	[3136+0x2C] = 0x0261,	/* U+A7AC: LATIN CAPITAL LETTER SCRIPT G */
	[3136+0x2D] = 0x026C,	/* U+A7AD: LATIN CAPITAL LETTER L WITH BELT */
	[3136+0x2E] = 0x026A,	/* U+A7AE: LATIN CAPITAL LETTER SMALL CAPITAL I */
	[3136+0x30] = 0x029E,	/* U+A7B0: LATIN CAPITAL LETTER TURNED K */
	[3136+0x31] = 0x0287,	/* U+A7B1: LATIN CAPITAL LETTER TURNED T */
	[3136+0x32] = 0x029D,	/* U+A7B2: LATIN CAPITAL LETTER J WITH CROSSED-TAIL */
	[3136+0x33] = 0xAB53,	/* U+A7B3: LATIN CAPITAL LETTER CHI */
	[3136+0x34] = 0xA7B5,	/* U+A7B4: LATIN CAPITAL LETTER BETA */
	[3136+0x36] = 0xA7B7,	/* U+A7B6: LATIN CAPITAL LETTER OMEGA */
	[3136+0x38] = 0xA7B9,	/* U+A7B8: LATIN CAPITAL LETTER U WITH STROKE */
	[3136+0x3A] = 0xA7BB,	/* U+A7BA: LATIN CAPITAL LETTER GLOTTAL A */
	[3136+0x3C] = 0xA7BD,	/* U+A7BC: LATIN CAPITAL LETTER GLOTTAL I */
	[3136+0x3E] = 0xA7BF,	/* U+A7BE: LATIN CAPITAL LETTER GLOTTAL U */
	[2816+0x1F] = 3200 - 0x80,	/* 352 237 ... */
	[3200+0x00] = 0xA7C1,	/* U+A7C0: LATIN CAPITAL LETTER OLD POLISH O */
	[3200+0x02] = 0xA7C3,	/* U+A7C2: LATIN CAPITAL LETTER ANGLICANA W */
	[3200+0x04] = 0xA794,	/* U+A7C4: LATIN CAPITAL LETTER C WITH PALATAL HOOK */
	[3200+0x05] = 0x0282,	/* U+A7C5: LATIN CAPITAL LETTER S WITH HOOK */
	[3200+0x06] = 0x1D8E,	/* U+A7C6: LATIN CAPITAL LETTER Z WITH PALATAL HOOK */
	[3200+0x07] = 0xA7C8,	/* U+A7C7: LATIN CAPITAL LETTER D WITH SHORT STROKE OVERLAY */
	[3200+0x09] = 0xA7CA,	/* U+A7C9: LATIN CAPITAL LETTER S WITH SHORT STROKE OVERLAY */
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
	[3520+0x00] = 0x10428,	/* U+10400: DESERET CAPITAL LETTER LONG I */
	[3520+0x01] = 0x10429,	/* U+10401: DESERET CAPITAL LETTER LONG E */
	[3520+0x02] = 0x1042A,	/* U+10402: DESERET CAPITAL LETTER LONG A */
	[3520+0x03] = 0x1042B,	/* U+10403: DESERET CAPITAL LETTER LONG AH */
	[3520+0x04] = 0x1042C,	/* U+10404: DESERET CAPITAL LETTER LONG O */
	[3520+0x05] = 0x1042D,	/* U+10405: DESERET CAPITAL LETTER LONG OO */
	[3520+0x06] = 0x1042E,	/* U+10406: DESERET CAPITAL LETTER SHORT I */
	[3520+0x07] = 0x1042F,	/* U+10407: DESERET CAPITAL LETTER SHORT E */
	[3520+0x08] = 0x10430,	/* U+10408: DESERET CAPITAL LETTER SHORT A */
	[3520+0x09] = 0x10431,	/* U+10409: DESERET CAPITAL LETTER SHORT AH */
	[3520+0x0A] = 0x10432,	/* U+1040A: DESERET CAPITAL LETTER SHORT O */
	[3520+0x0B] = 0x10433,	/* U+1040B: DESERET CAPITAL LETTER SHORT OO */
	[3520+0x0C] = 0x10434,	/* U+1040C: DESERET CAPITAL LETTER AY */
	[3520+0x0D] = 0x10435,	/* U+1040D: DESERET CAPITAL LETTER OW */
	[3520+0x0E] = 0x10436,	/* U+1040E: DESERET CAPITAL LETTER WU */
	[3520+0x0F] = 0x10437,	/* U+1040F: DESERET CAPITAL LETTER YEE */
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
	[3648+0x00] = 0x104E8,	/* U+104C0: OSAGE CAPITAL LETTER MA */
	[3648+0x01] = 0x104E9,	/* U+104C1: OSAGE CAPITAL LETTER NA */
	[3648+0x02] = 0x104EA,	/* U+104C2: OSAGE CAPITAL LETTER O */
	[3648+0x03] = 0x104EB,	/* U+104C3: OSAGE CAPITAL LETTER OIN */
	[3648+0x04] = 0x104EC,	/* U+104C4: OSAGE CAPITAL LETTER PA */
	[3648+0x05] = 0x104ED,	/* U+104C5: OSAGE CAPITAL LETTER EHPA */
	[3648+0x06] = 0x104EE,	/* U+104C6: OSAGE CAPITAL LETTER SA */
	[3648+0x07] = 0x104EF,	/* U+104C7: OSAGE CAPITAL LETTER SHA */
	[3648+0x08] = 0x104F0,	/* U+104C8: OSAGE CAPITAL LETTER TA */
	[3648+0x09] = 0x104F1,	/* U+104C9: OSAGE CAPITAL LETTER EHTA */
	[3648+0x0A] = 0x104F2,	/* U+104CA: OSAGE CAPITAL LETTER TSA */
	[3648+0x0B] = 0x104F3,	/* U+104CB: OSAGE CAPITAL LETTER EHTSA */
	[3648+0x0C] = 0x104F4,	/* U+104CC: OSAGE CAPITAL LETTER TSHA */
	[3648+0x0D] = 0x104F5,	/* U+104CD: OSAGE CAPITAL LETTER DHA */
	[3648+0x0E] = 0x104F6,	/* U+104CE: OSAGE CAPITAL LETTER U */
	[3648+0x0F] = 0x104F7,	/* U+104CF: OSAGE CAPITAL LETTER WA */
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
	[3776+0x00] = 0x105A7,	/* U+10580: VITHKUQI CAPITAL LETTER JE */
	[3776+0x01] = 0x105A8,	/* U+10581: VITHKUQI CAPITAL LETTER KA */
	[3776+0x02] = 0x105A9,	/* U+10582: VITHKUQI CAPITAL LETTER LA */
	[3776+0x03] = 0x105AA,	/* U+10583: VITHKUQI CAPITAL LETTER LLA */
	[3776+0x04] = 0x105AB,	/* U+10584: VITHKUQI CAPITAL LETTER ME */
	[3776+0x05] = 0x105AC,	/* U+10585: VITHKUQI CAPITAL LETTER NE */
	[3776+0x06] = 0x105AD,	/* U+10586: VITHKUQI CAPITAL LETTER NJE */
	[3776+0x07] = 0x105AE,	/* U+10587: VITHKUQI CAPITAL LETTER O */
	[3776+0x08] = 0x105AF,	/* U+10588: VITHKUQI CAPITAL LETTER PE */
	[3776+0x09] = 0x105B0,	/* U+10589: VITHKUQI CAPITAL LETTER QA */
	[3776+0x0A] = 0x105B1,	/* U+1058A: VITHKUQI CAPITAL LETTER RE */
	[3776+0x0C] = 0x105B3,	/* U+1058C: VITHKUQI CAPITAL LETTER SE */
	[3776+0x0D] = 0x105B4,	/* U+1058D: VITHKUQI CAPITAL LETTER SHE */
	[3776+0x0E] = 0x105B5,	/* U+1058E: VITHKUQI CAPITAL LETTER TE */
	[3776+0x0F] = 0x105B6,	/* U+1058F: VITHKUQI CAPITAL LETTER THE */
	[3776+0x10] = 0x105B7,	/* U+10590: VITHKUQI CAPITAL LETTER U */
	[3776+0x11] = 0x105B8,	/* U+10591: VITHKUQI CAPITAL LETTER VE */
	[3776+0x12] = 0x105B9,	/* U+10592: VITHKUQI CAPITAL LETTER XE */
	[3776+0x14] = 0x105BB,	/* U+10594: VITHKUQI CAPITAL LETTER Y */
	[3776+0x15] = 0x105BC,	/* U+10595: VITHKUQI CAPITAL LETTER ZE */
	[3456+0x32] = 3840 - 0x80,	/* 360 220 262 ... */
	[3840+0x00] = 0x10CC0,	/* U+10C80: OLD HUNGARIAN CAPITAL LETTER A */
	[3840+0x01] = 0x10CC1,	/* U+10C81: OLD HUNGARIAN CAPITAL LETTER AA */
	[3840+0x02] = 0x10CC2,	/* U+10C82: OLD HUNGARIAN CAPITAL LETTER EB */
	[3840+0x03] = 0x10CC3,	/* U+10C83: OLD HUNGARIAN CAPITAL LETTER AMB */
	[3840+0x04] = 0x10CC4,	/* U+10C84: OLD HUNGARIAN CAPITAL LETTER EC */
	[3840+0x05] = 0x10CC5,	/* U+10C85: OLD HUNGARIAN CAPITAL LETTER ENC */
	[3840+0x06] = 0x10CC6,	/* U+10C86: OLD HUNGARIAN CAPITAL LETTER ECS */
	[3840+0x07] = 0x10CC7,	/* U+10C87: OLD HUNGARIAN CAPITAL LETTER ED */
	[3840+0x08] = 0x10CC8,	/* U+10C88: OLD HUNGARIAN CAPITAL LETTER AND */
	[3840+0x09] = 0x10CC9,	/* U+10C89: OLD HUNGARIAN CAPITAL LETTER E */
	[3840+0x0A] = 0x10CCA,	/* U+10C8A: OLD HUNGARIAN CAPITAL LETTER CLOSE E */
	[3840+0x0B] = 0x10CCB,	/* U+10C8B: OLD HUNGARIAN CAPITAL LETTER EE */
	[3840+0x0C] = 0x10CCC,	/* U+10C8C: OLD HUNGARIAN CAPITAL LETTER EF */
	[3840+0x0D] = 0x10CCD,	/* U+10C8D: OLD HUNGARIAN CAPITAL LETTER EG */
	[3840+0x0E] = 0x10CCE,	/* U+10C8E: OLD HUNGARIAN CAPITAL LETTER EGY */
	[3840+0x0F] = 0x10CCF,	/* U+10C8F: OLD HUNGARIAN CAPITAL LETTER EH */
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
	[4096+0x00] = 0x16E60,	/* U+16E40: MEDEFAIDRIN CAPITAL LETTER M */
	[4096+0x01] = 0x16E61,	/* U+16E41: MEDEFAIDRIN CAPITAL LETTER S */
	[4096+0x02] = 0x16E62,	/* U+16E42: MEDEFAIDRIN CAPITAL LETTER V */
	[4096+0x03] = 0x16E63,	/* U+16E43: MEDEFAIDRIN CAPITAL LETTER W */
	[4096+0x04] = 0x16E64,	/* U+16E44: MEDEFAIDRIN CAPITAL LETTER ATIU */
	[4096+0x05] = 0x16E65,	/* U+16E45: MEDEFAIDRIN CAPITAL LETTER Z */
	[4096+0x06] = 0x16E66,	/* U+16E46: MEDEFAIDRIN CAPITAL LETTER KP */
	[4096+0x07] = 0x16E67,	/* U+16E47: MEDEFAIDRIN CAPITAL LETTER P */
	[4096+0x08] = 0x16E68,	/* U+16E48: MEDEFAIDRIN CAPITAL LETTER T */
	[4096+0x09] = 0x16E69,	/* U+16E49: MEDEFAIDRIN CAPITAL LETTER G */
	[4096+0x0A] = 0x16E6A,	/* U+16E4A: MEDEFAIDRIN CAPITAL LETTER F */
	[4096+0x0B] = 0x16E6B,	/* U+16E4B: MEDEFAIDRIN CAPITAL LETTER I */
	[4096+0x0C] = 0x16E6C,	/* U+16E4C: MEDEFAIDRIN CAPITAL LETTER K */
	[4096+0x0D] = 0x16E6D,	/* U+16E4D: MEDEFAIDRIN CAPITAL LETTER A */
	[4096+0x0E] = 0x16E6E,	/* U+16E4E: MEDEFAIDRIN CAPITAL LETTER J */
	[4096+0x0F] = 0x16E6F,	/* U+16E4F: MEDEFAIDRIN CAPITAL LETTER E */
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
	[4224+0x00] = 0x1E922,	/* U+1E900: ADLAM CAPITAL LETTER ALIF */
	[4224+0x01] = 0x1E923,	/* U+1E901: ADLAM CAPITAL LETTER DAALI */
	[4224+0x02] = 0x1E924,	/* U+1E902: ADLAM CAPITAL LETTER LAAM */
	[4224+0x03] = 0x1E925,	/* U+1E903: ADLAM CAPITAL LETTER MIIM */
	[4224+0x04] = 0x1E926,	/* U+1E904: ADLAM CAPITAL LETTER BA */
	[4224+0x05] = 0x1E927,	/* U+1E905: ADLAM CAPITAL LETTER SINNYIIYHE */
	[4224+0x06] = 0x1E928,	/* U+1E906: ADLAM CAPITAL LETTER PE */
	[4224+0x07] = 0x1E929,	/* U+1E907: ADLAM CAPITAL LETTER BHE */
	[4224+0x08] = 0x1E92A,	/* U+1E908: ADLAM CAPITAL LETTER RA */
	[4224+0x09] = 0x1E92B,	/* U+1E909: ADLAM CAPITAL LETTER E */
	[4224+0x0A] = 0x1E92C,	/* U+1E90A: ADLAM CAPITAL LETTER FA */
	[4224+0x0B] = 0x1E92D,	/* U+1E90B: ADLAM CAPITAL LETTER I */
	[4224+0x0C] = 0x1E92E,	/* U+1E90C: ADLAM CAPITAL LETTER O */
	[4224+0x0D] = 0x1E92F,	/* U+1E90D: ADLAM CAPITAL LETTER DHA */
	[4224+0x0E] = 0x1E930,	/* U+1E90E: ADLAM CAPITAL LETTER YHE */
	[4224+0x0F] = 0x1E931,	/* U+1E90F: ADLAM CAPITAL LETTER WAW */
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
static const int uppercase[4608] = {
	[0x00] = 0x0000,	/* U+0000: <control> */
	[0x01] = 0x0001,	/* U+0001: <control> */
	[0x02] = 0x0002,	/* U+0002: <control> */
	[0x03] = 0x0003,	/* U+0003: <control> */
	[0x04] = 0x0004,	/* U+0004: <control> */
	[0x05] = 0x0005,	/* U+0005: <control> */
	[0x06] = 0x0006,	/* U+0006: <control> */
	[0x07] = 0x0007,	/* U+0007: <control> */
	[0x08] = 0x0008,	/* U+0008: <control> */
	[0x09] = 0x0009,	/* U+0009: <control> */
	[0x0A] = 0x000A,	/* U+000A: <control> */
	[0x0B] = 0x000B,	/* U+000B: <control> */
	[0x0C] = 0x000C,	/* U+000C: <control> */
	[0x0D] = 0x000D,	/* U+000D: <control> */
	[0x0E] = 0x000E,	/* U+000E: <control> */
	[0x0F] = 0x000F,	/* U+000F: <control> */
	[0x10] = 0x0010,	/* U+0010: <control> */
	[0x11] = 0x0011,	/* U+0011: <control> */
	[0x12] = 0x0012,	/* U+0012: <control> */
	[0x13] = 0x0013,	/* U+0013: <control> */
	[0x14] = 0x0014,	/* U+0014: <control> */
	[0x15] = 0x0015,	/* U+0015: <control> */
	[0x16] = 0x0016,	/* U+0016: <control> */
	[0x17] = 0x0017,	/* U+0017: <control> */
	[0x18] = 0x0018,	/* U+0018: <control> */
	[0x19] = 0x0019,	/* U+0019: <control> */
	[0x1A] = 0x001A,	/* U+001A: <control> */
	[0x1B] = 0x001B,	/* U+001B: <control> */
	[0x1C] = 0x001C,	/* U+001C: <control> */
	[0x1D] = 0x001D,	/* U+001D: <control> */
	[0x1E] = 0x001E,	/* U+001E: <control> */
	[0x1F] = 0x001F,	/* U+001F: <control> */
	[0x20] = 0x0020,	/* U+0020: SPACE */
	[0x21] = 0x0021,	/* U+0021: EXCLAMATION MARK */
	[0x22] = 0x0022,	/* U+0022: QUOTATION MARK */
	[0x23] = 0x0023,	/* U+0023: NUMBER SIGN */
	[0x24] = 0x0024,	/* U+0024: DOLLAR SIGN */
	[0x25] = 0x0025,	/* U+0025: PERCENT SIGN */
	[0x26] = 0x0026,	/* U+0026: AMPERSAND */
	[0x27] = 0x0027,	/* U+0027: APOSTROPHE */
	[0x28] = 0x0028,	/* U+0028: LEFT PARENTHESIS */
	[0x29] = 0x0029,	/* U+0029: RIGHT PARENTHESIS */
	[0x2A] = 0x002A,	/* U+002A: ASTERISK */
	[0x2B] = 0x002B,	/* U+002B: PLUS SIGN */
	[0x2C] = 0x002C,	/* U+002C: COMMA */
	[0x2D] = 0x002D,	/* U+002D: HYPHEN-MINUS */
	[0x2E] = 0x002E,	/* U+002E: FULL STOP */
	[0x2F] = 0x002F,	/* U+002F: SOLIDUS */
	[0x30] = 0x0030,	/* U+0030: DIGIT ZERO */
	[0x31] = 0x0031,	/* U+0031: DIGIT ONE */
	[0x32] = 0x0032,	/* U+0032: DIGIT TWO */
	[0x33] = 0x0033,	/* U+0033: DIGIT THREE */
	[0x34] = 0x0034,	/* U+0034: DIGIT FOUR */
	[0x35] = 0x0035,	/* U+0035: DIGIT FIVE */
	[0x36] = 0x0036,	/* U+0036: DIGIT SIX */
	[0x37] = 0x0037,	/* U+0037: DIGIT SEVEN */
	[0x38] = 0x0038,	/* U+0038: DIGIT EIGHT */
	[0x39] = 0x0039,	/* U+0039: DIGIT NINE */
	[0x3A] = 0x003A,	/* U+003A: COLON */
	[0x3B] = 0x003B,	/* U+003B: SEMICOLON */
	[0x3C] = 0x003C,	/* U+003C: LESS-THAN SIGN */
	[0x3D] = 0x003D,	/* U+003D: EQUALS SIGN */
	[0x3E] = 0x003E,	/* U+003E: GREATER-THAN SIGN */
	[0x3F] = 0x003F,	/* U+003F: QUESTION MARK */
	[0x40] = 0x0040,	/* U+0040: COMMERCIAL AT */
	[0x41] = 0x0041,	/* U+0041: LATIN CAPITAL LETTER A */
	[0x42] = 0x0042,	/* U+0042: LATIN CAPITAL LETTER B */
	[0x43] = 0x0043,	/* U+0043: LATIN CAPITAL LETTER C */
	[0x44] = 0x0044,	/* U+0044: LATIN CAPITAL LETTER D */
	[0x45] = 0x0045,	/* U+0045: LATIN CAPITAL LETTER E */
	[0x46] = 0x0046,	/* U+0046: LATIN CAPITAL LETTER F */
	[0x47] = 0x0047,	/* U+0047: LATIN CAPITAL LETTER G */
	[0x48] = 0x0048,	/* U+0048: LATIN CAPITAL LETTER H */
	[0x49] = 0x0049,	/* U+0049: LATIN CAPITAL LETTER I */
	[0x4A] = 0x004A,	/* U+004A: LATIN CAPITAL LETTER J */
	[0x4B] = 0x004B,	/* U+004B: LATIN CAPITAL LETTER K */
	[0x4C] = 0x004C,	/* U+004C: LATIN CAPITAL LETTER L */
	[0x4D] = 0x004D,	/* U+004D: LATIN CAPITAL LETTER M */
	[0x4E] = 0x004E,	/* U+004E: LATIN CAPITAL LETTER N */
	[0x4F] = 0x004F,	/* U+004F: LATIN CAPITAL LETTER O */
	[0x50] = 0x0050,	/* U+0050: LATIN CAPITAL LETTER P */
	[0x51] = 0x0051,	/* U+0051: LATIN CAPITAL LETTER Q */
	[0x52] = 0x0052,	/* U+0052: LATIN CAPITAL LETTER R */
	[0x53] = 0x0053,	/* U+0053: LATIN CAPITAL LETTER S */
	[0x54] = 0x0054,	/* U+0054: LATIN CAPITAL LETTER T */
	[0x55] = 0x0055,	/* U+0055: LATIN CAPITAL LETTER U */
	[0x56] = 0x0056,	/* U+0056: LATIN CAPITAL LETTER V */
	[0x57] = 0x0057,	/* U+0057: LATIN CAPITAL LETTER W */
	[0x58] = 0x0058,	/* U+0058: LATIN CAPITAL LETTER X */
	[0x59] = 0x0059,	/* U+0059: LATIN CAPITAL LETTER Y */
	[0x5A] = 0x005A,	/* U+005A: LATIN CAPITAL LETTER Z */
	[0x5B] = 0x005B,	/* U+005B: LEFT SQUARE BRACKET */
	[0x5C] = 0x005C,	/* U+005C: REVERSE SOLIDUS */
	[0x5D] = 0x005D,	/* U+005D: RIGHT SQUARE BRACKET */
	[0x5E] = 0x005E,	/* U+005E: CIRCUMFLEX ACCENT */
	[0x5F] = 0x005F,	/* U+005F: LOW LINE */
	[0x60] = 0x0060,	/* U+0060: GRAVE ACCENT */
	[0x61] = 0x0041,	/* U+0061: LATIN SMALL LETTER A */
	[0x62] = 0x0042,	/* U+0062: LATIN SMALL LETTER B */
	[0x63] = 0x0043,	/* U+0063: LATIN SMALL LETTER C */
	[0x64] = 0x0044,	/* U+0064: LATIN SMALL LETTER D */
	[0x65] = 0x0045,	/* U+0065: LATIN SMALL LETTER E */
	[0x66] = 0x0046,	/* U+0066: LATIN SMALL LETTER F */
	[0x67] = 0x0047,	/* U+0067: LATIN SMALL LETTER G */
	[0x68] = 0x0048,	/* U+0068: LATIN SMALL LETTER H */
	[0x69] = 0x0049,	/* U+0069: LATIN SMALL LETTER I */
	[0x6A] = 0x004A,	/* U+006A: LATIN SMALL LETTER J */
	[0x6B] = 0x004B,	/* U+006B: LATIN SMALL LETTER K */
	[0x6C] = 0x004C,	/* U+006C: LATIN SMALL LETTER L */
	[0x6D] = 0x004D,	/* U+006D: LATIN SMALL LETTER M */
	[0x6E] = 0x004E,	/* U+006E: LATIN SMALL LETTER N */
	[0x6F] = 0x004F,	/* U+006F: LATIN SMALL LETTER O */
	[0x70] = 0x0050,	/* U+0070: LATIN SMALL LETTER P */
	[0x71] = 0x0051,	/* U+0071: LATIN SMALL LETTER Q */
	[0x72] = 0x0052,	/* U+0072: LATIN SMALL LETTER R */
	[0x73] = 0x0053,	/* U+0073: LATIN SMALL LETTER S */
	[0x74] = 0x0054,	/* U+0074: LATIN SMALL LETTER T */
	[0x75] = 0x0055,	/* U+0075: LATIN SMALL LETTER U */
	[0x76] = 0x0056,	/* U+0076: LATIN SMALL LETTER V */
	[0x77] = 0x0057,	/* U+0077: LATIN SMALL LETTER W */
	[0x78] = 0x0058,	/* U+0078: LATIN SMALL LETTER X */
	[0x79] = 0x0059,	/* U+0079: LATIN SMALL LETTER Y */
	[0x7A] = 0x005A,	/* U+007A: LATIN SMALL LETTER Z */
	[0x7B] = 0x007B,	/* U+007B: LEFT CURLY BRACKET */
	[0x7C] = 0x007C,	/* U+007C: VERTICAL LINE */
	[0x7D] = 0x007D,	/* U+007D: RIGHT CURLY BRACKET */
	[0x7E] = 0x007E,	/* U+007E: TILDE */
	[0x7F] = 0x007F,	/* U+007F: <control> */
	[0xC2] = 256 - 0x80,	/* 302 ... */
	[256+0x35] = 0x039C,	/* U+00B5: MICRO SIGN */
	[0xC3] = 320 - 0x80,	/* 303 ... */
	[320+0x1F] = -74,	/* U+00DF: LATIN SMALL LETTER SHARP S */
	[320+0x20] = 0x00C0,	/* U+00E0: LATIN SMALL LETTER A WITH GRAVE */
	[320+0x21] = 0x00C1,	/* U+00E1: LATIN SMALL LETTER A WITH ACUTE */
	[320+0x22] = 0x00C2,	/* U+00E2: LATIN SMALL LETTER A WITH CIRCUMFLEX */
	[320+0x23] = 0x00C3,	/* U+00E3: LATIN SMALL LETTER A WITH TILDE */
	[320+0x24] = 0x00C4,	/* U+00E4: LATIN SMALL LETTER A WITH DIAERESIS */
	[320+0x25] = 0x00C5,	/* U+00E5: LATIN SMALL LETTER A WITH RING ABOVE */
	[320+0x26] = 0x00C6,	/* U+00E6: LATIN SMALL LETTER AE */
	[320+0x27] = 0x00C7,	/* U+00E7: LATIN SMALL LETTER C WITH CEDILLA */
	[320+0x28] = 0x00C8,	/* U+00E8: LATIN SMALL LETTER E WITH GRAVE */
	[320+0x29] = 0x00C9,	/* U+00E9: LATIN SMALL LETTER E WITH ACUTE */
	[320+0x2A] = 0x00CA,	/* U+00EA: LATIN SMALL LETTER E WITH CIRCUMFLEX */
	[320+0x2B] = 0x00CB,	/* U+00EB: LATIN SMALL LETTER E WITH DIAERESIS */
	[320+0x2C] = 0x00CC,	/* U+00EC: LATIN SMALL LETTER I WITH GRAVE */
	[320+0x2D] = 0x00CD,	/* U+00ED: LATIN SMALL LETTER I WITH ACUTE */
	[320+0x2E] = 0x00CE,	/* U+00EE: LATIN SMALL LETTER I WITH CIRCUMFLEX */
	[320+0x2F] = 0x00CF,	/* U+00EF: LATIN SMALL LETTER I WITH DIAERESIS */
	[320+0x30] = 0x00D0,	/* U+00F0: LATIN SMALL LETTER ETH */
	[320+0x31] = 0x00D1,	/* U+00F1: LATIN SMALL LETTER N WITH TILDE */
	[320+0x32] = 0x00D2,	/* U+00F2: LATIN SMALL LETTER O WITH GRAVE */
	[320+0x33] = 0x00D3,	/* U+00F3: LATIN SMALL LETTER O WITH ACUTE */
	[320+0x34] = 0x00D4,	/* U+00F4: LATIN SMALL LETTER O WITH CIRCUMFLEX */
	[320+0x35] = 0x00D5,	/* U+00F5: LATIN SMALL LETTER O WITH TILDE */
	[320+0x36] = 0x00D6,	/* U+00F6: LATIN SMALL LETTER O WITH DIAERESIS */
	[320+0x38] = 0x00D8,	/* U+00F8: LATIN SMALL LETTER O WITH STROKE */
	[320+0x39] = 0x00D9,	/* U+00F9: LATIN SMALL LETTER U WITH GRAVE */
	[320+0x3A] = 0x00DA,	/* U+00FA: LATIN SMALL LETTER U WITH ACUTE */
	[320+0x3B] = 0x00DB,	/* U+00FB: LATIN SMALL LETTER U WITH CIRCUMFLEX */
	[320+0x3C] = 0x00DC,	/* U+00FC: LATIN SMALL LETTER U WITH DIAERESIS */
	[320+0x3D] = 0x00DD,	/* U+00FD: LATIN SMALL LETTER Y WITH ACUTE */
	[320+0x3E] = 0x00DE,	/* U+00FE: LATIN SMALL LETTER THORN */
	[320+0x3F] = 0x0178,	/* U+00FF: LATIN SMALL LETTER Y WITH DIAERESIS */
	[0xC4] = 384 - 0x80,	/* 304 ... */
	[384+0x01] = 0x0100,	/* U+0101: LATIN SMALL LETTER A WITH MACRON */
	[384+0x03] = 0x0102,	/* U+0103: LATIN SMALL LETTER A WITH BREVE */
	[384+0x05] = 0x0104,	/* U+0105: LATIN SMALL LETTER A WITH OGONEK */
	[384+0x07] = 0x0106,	/* U+0107: LATIN SMALL LETTER C WITH ACUTE */
	[384+0x09] = 0x0108,	/* U+0109: LATIN SMALL LETTER C WITH CIRCUMFLEX */
	[384+0x0B] = 0x010A,	/* U+010B: LATIN SMALL LETTER C WITH DOT ABOVE */
	[384+0x0D] = 0x010C,	/* U+010D: LATIN SMALL LETTER C WITH CARON */
	[384+0x0F] = 0x010E,	/* U+010F: LATIN SMALL LETTER D WITH CARON */
	[384+0x11] = 0x0110,	/* U+0111: LATIN SMALL LETTER D WITH STROKE */
	[384+0x13] = 0x0112,	/* U+0113: LATIN SMALL LETTER E WITH MACRON */
	[384+0x15] = 0x0114,	/* U+0115: LATIN SMALL LETTER E WITH BREVE */
	[384+0x17] = 0x0116,	/* U+0117: LATIN SMALL LETTER E WITH DOT ABOVE */
	[384+0x19] = 0x0118,	/* U+0119: LATIN SMALL LETTER E WITH OGONEK */
	[384+0x1B] = 0x011A,	/* U+011B: LATIN SMALL LETTER E WITH CARON */
	[384+0x1D] = 0x011C,	/* U+011D: LATIN SMALL LETTER G WITH CIRCUMFLEX */
	[384+0x1F] = 0x011E,	/* U+011F: LATIN SMALL LETTER G WITH BREVE */
	[384+0x21] = 0x0120,	/* U+0121: LATIN SMALL LETTER G WITH DOT ABOVE */
	[384+0x23] = 0x0122,	/* U+0123: LATIN SMALL LETTER G WITH CEDILLA */
	[384+0x25] = 0x0124,	/* U+0125: LATIN SMALL LETTER H WITH CIRCUMFLEX */
	[384+0x27] = 0x0126,	/* U+0127: LATIN SMALL LETTER H WITH STROKE */
	[384+0x29] = 0x0128,	/* U+0129: LATIN SMALL LETTER I WITH TILDE */
	[384+0x2B] = 0x012A,	/* U+012B: LATIN SMALL LETTER I WITH MACRON */
	[384+0x2D] = 0x012C,	/* U+012D: LATIN SMALL LETTER I WITH BREVE */
	[384+0x2F] = 0x012E,	/* U+012F: LATIN SMALL LETTER I WITH OGONEK */
	[384+0x31] = 0x0049,	/* U+0131: LATIN SMALL LETTER DOTLESS I */
	[384+0x33] = 0x0132,	/* U+0133: LATIN SMALL LIGATURE IJ */
	[384+0x35] = 0x0134,	/* U+0135: LATIN SMALL LETTER J WITH CIRCUMFLEX */
	[384+0x37] = 0x0136,	/* U+0137: LATIN SMALL LETTER K WITH CEDILLA */
	[384+0x3A] = 0x0139,	/* U+013A: LATIN SMALL LETTER L WITH ACUTE */
	[384+0x3C] = 0x013B,	/* U+013C: LATIN SMALL LETTER L WITH CEDILLA */
	[384+0x3E] = 0x013D,	/* U+013E: LATIN SMALL LETTER L WITH CARON */
	[0xC5] = 448 - 0x80,	/* 305 ... */
	[448+0x00] = 0x013F,	/* U+0140: LATIN SMALL LETTER L WITH MIDDLE DOT */
	[448+0x02] = 0x0141,	/* U+0142: LATIN SMALL LETTER L WITH STROKE */
	[448+0x04] = 0x0143,	/* U+0144: LATIN SMALL LETTER N WITH ACUTE */
	[448+0x06] = 0x0145,	/* U+0146: LATIN SMALL LETTER N WITH CEDILLA */
	[448+0x08] = 0x0147,	/* U+0148: LATIN SMALL LETTER N WITH CARON */
	[448+0x09] = -87,	/* U+0149: LATIN SMALL LETTER N PRECEDED BY APOSTROPHE */
	[448+0x0B] = 0x014A,	/* U+014B: LATIN SMALL LETTER ENG */
	[448+0x0D] = 0x014C,	/* U+014D: LATIN SMALL LETTER O WITH MACRON */
	[448+0x0F] = 0x014E,	/* U+014F: LATIN SMALL LETTER O WITH BREVE */
	[448+0x11] = 0x0150,	/* U+0151: LATIN SMALL LETTER O WITH DOUBLE ACUTE */
	[448+0x13] = 0x0152,	/* U+0153: LATIN SMALL LIGATURE OE */
	[448+0x15] = 0x0154,	/* U+0155: LATIN SMALL LETTER R WITH ACUTE */
	[448+0x17] = 0x0156,	/* U+0157: LATIN SMALL LETTER R WITH CEDILLA */
	[448+0x19] = 0x0158,	/* U+0159: LATIN SMALL LETTER R WITH CARON */
	[448+0x1B] = 0x015A,	/* U+015B: LATIN SMALL LETTER S WITH ACUTE */
	[448+0x1D] = 0x015C,	/* U+015D: LATIN SMALL LETTER S WITH CIRCUMFLEX */
	[448+0x1F] = 0x015E,	/* U+015F: LATIN SMALL LETTER S WITH CEDILLA */
	[448+0x21] = 0x0160,	/* U+0161: LATIN SMALL LETTER S WITH CARON */
	[448+0x23] = 0x0162,	/* U+0163: LATIN SMALL LETTER T WITH CEDILLA */
	[448+0x25] = 0x0164,	/* U+0165: LATIN SMALL LETTER T WITH CARON */
	[448+0x27] = 0x0166,	/* U+0167: LATIN SMALL LETTER T WITH STROKE */
	[448+0x29] = 0x0168,	/* U+0169: LATIN SMALL LETTER U WITH TILDE */
	[448+0x2B] = 0x016A,	/* U+016B: LATIN SMALL LETTER U WITH MACRON */
	[448+0x2D] = 0x016C,	/* U+016D: LATIN SMALL LETTER U WITH BREVE */
	[448+0x2F] = 0x016E,	/* U+016F: LATIN SMALL LETTER U WITH RING ABOVE */
	[448+0x31] = 0x0170,	/* U+0171: LATIN SMALL LETTER U WITH DOUBLE ACUTE */
	[448+0x33] = 0x0172,	/* U+0173: LATIN SMALL LETTER U WITH OGONEK */
	[448+0x35] = 0x0174,	/* U+0175: LATIN SMALL LETTER W WITH CIRCUMFLEX */
	[448+0x37] = 0x0176,	/* U+0177: LATIN SMALL LETTER Y WITH CIRCUMFLEX */
	[448+0x3A] = 0x0179,	/* U+017A: LATIN SMALL LETTER Z WITH ACUTE */
	[448+0x3C] = 0x017B,	/* U+017C: LATIN SMALL LETTER Z WITH DOT ABOVE */
	[448+0x3E] = 0x017D,	/* U+017E: LATIN SMALL LETTER Z WITH CARON */
	[448+0x3F] = 0x0053,	/* U+017F: LATIN SMALL LETTER LONG S */
	[0xC6] = 512 - 0x80,	/* 306 ... */
	[512+0x00] = 0x0243,	/* U+0180: LATIN SMALL LETTER B WITH STROKE */
	[512+0x03] = 0x0182,	/* U+0183: LATIN SMALL LETTER B WITH TOPBAR */
	[512+0x05] = 0x0184,	/* U+0185: LATIN SMALL LETTER TONE SIX */
	[512+0x08] = 0x0187,	/* U+0188: LATIN SMALL LETTER C WITH HOOK */
	[512+0x0C] = 0x018B,	/* U+018C: LATIN SMALL LETTER D WITH TOPBAR */
	[512+0x12] = 0x0191,	/* U+0192: LATIN SMALL LETTER F WITH HOOK */
	[512+0x15] = 0x01F6,	/* U+0195: LATIN SMALL LETTER HV */
	[512+0x19] = 0x0198,	/* U+0199: LATIN SMALL LETTER K WITH HOOK */
	[512+0x1A] = 0x023D,	/* U+019A: LATIN SMALL LETTER L WITH BAR */
	[512+0x1E] = 0x0220,	/* U+019E: LATIN SMALL LETTER N WITH LONG RIGHT LEG */
	[512+0x21] = 0x01A0,	/* U+01A1: LATIN SMALL LETTER O WITH HORN */
	[512+0x23] = 0x01A2,	/* U+01A3: LATIN SMALL LETTER OI */
	[512+0x25] = 0x01A4,	/* U+01A5: LATIN SMALL LETTER P WITH HOOK */
	[512+0x28] = 0x01A7,	/* U+01A8: LATIN SMALL LETTER TONE TWO */
	[512+0x2D] = 0x01AC,	/* U+01AD: LATIN SMALL LETTER T WITH HOOK */
	[512+0x30] = 0x01AF,	/* U+01B0: LATIN SMALL LETTER U WITH HORN */
	[512+0x34] = 0x01B3,	/* U+01B4: LATIN SMALL LETTER Y WITH HOOK */
	[512+0x36] = 0x01B5,	/* U+01B6: LATIN SMALL LETTER Z WITH STROKE */
	[512+0x39] = 0x01B8,	/* U+01B9: LATIN SMALL LETTER EZH REVERSED */
	[512+0x3D] = 0x01BC,	/* U+01BD: LATIN SMALL LETTER TONE FIVE */
	[512+0x3F] = 0x01F7,	/* U+01BF: LATIN LETTER WYNN */
	[0xC7] = 576 - 0x80,	/* 307 ... */
	[576+0x05] = 0x01C4,	/* U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON */
	[576+0x06] = 0x01C4,	/* U+01C6: LATIN SMALL LETTER DZ WITH CARON */
	[576+0x08] = 0x01C7,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[576+0x09] = 0x01C7,	/* U+01C9: LATIN SMALL LETTER LJ */
	[576+0x0B] = 0x01CA,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[576+0x0C] = 0x01CA,	/* U+01CC: LATIN SMALL LETTER NJ */
	[576+0x0E] = 0x01CD,	/* U+01CE: LATIN SMALL LETTER A WITH CARON */
	[576+0x10] = 0x01CF,	/* U+01D0: LATIN SMALL LETTER I WITH CARON */
	[576+0x12] = 0x01D1,	/* U+01D2: LATIN SMALL LETTER O WITH CARON */
	[576+0x14] = 0x01D3,	/* U+01D4: LATIN SMALL LETTER U WITH CARON */
	[576+0x16] = 0x01D5,	/* U+01D6: LATIN SMALL LETTER U WITH DIAERESIS AND MACRON */
	[576+0x18] = 0x01D7,	/* U+01D8: LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE */
	[576+0x1A] = 0x01D9,	/* U+01DA: LATIN SMALL LETTER U WITH DIAERESIS AND CARON */
	[576+0x1C] = 0x01DB,	/* U+01DC: LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE */
	[576+0x1D] = 0x018E,	/* U+01DD: LATIN SMALL LETTER TURNED E */
	[576+0x1F] = 0x01DE,	/* U+01DF: LATIN SMALL LETTER A WITH DIAERESIS AND MACRON */
	[576+0x21] = 0x01E0,	/* U+01E1: LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON */
	[576+0x23] = 0x01E2,	/* U+01E3: LATIN SMALL LETTER AE WITH MACRON */
	[576+0x25] = 0x01E4,	/* U+01E5: LATIN SMALL LETTER G WITH STROKE */
	[576+0x27] = 0x01E6,	/* U+01E7: LATIN SMALL LETTER G WITH CARON */
	[576+0x29] = 0x01E8,	/* U+01E9: LATIN SMALL LETTER K WITH CARON */
	[576+0x2B] = 0x01EA,	/* U+01EB: LATIN SMALL LETTER O WITH OGONEK */
	[576+0x2D] = 0x01EC,	/* U+01ED: LATIN SMALL LETTER O WITH OGONEK AND MACRON */
	[576+0x2F] = 0x01EE,	/* U+01EF: LATIN SMALL LETTER EZH WITH CARON */
	[576+0x30] = -90,	/* U+01F0: LATIN SMALL LETTER J WITH CARON */
	[576+0x32] = 0x01F1,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[576+0x33] = 0x01F1,	/* U+01F3: LATIN SMALL LETTER DZ */
	[576+0x35] = 0x01F4,	/* U+01F5: LATIN SMALL LETTER G WITH ACUTE */
	[576+0x39] = 0x01F8,	/* U+01F9: LATIN SMALL LETTER N WITH GRAVE */
	[576+0x3B] = 0x01FA,	/* U+01FB: LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE */
	[576+0x3D] = 0x01FC,	/* U+01FD: LATIN SMALL LETTER AE WITH ACUTE */
	[576+0x3F] = 0x01FE,	/* U+01FF: LATIN SMALL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 640 - 0x80,	/* 310 ... */
	[640+0x01] = 0x0200,	/* U+0201: LATIN SMALL LETTER A WITH DOUBLE GRAVE */
	[640+0x03] = 0x0202,	/* U+0203: LATIN SMALL LETTER A WITH INVERTED BREVE */
	[640+0x05] = 0x0204,	/* U+0205: LATIN SMALL LETTER E WITH DOUBLE GRAVE */
	[640+0x07] = 0x0206,	/* U+0207: LATIN SMALL LETTER E WITH INVERTED BREVE */
	[640+0x09] = 0x0208,	/* U+0209: LATIN SMALL LETTER I WITH DOUBLE GRAVE */
	[640+0x0B] = 0x020A,	/* U+020B: LATIN SMALL LETTER I WITH INVERTED BREVE */
	[640+0x0D] = 0x020C,	/* U+020D: LATIN SMALL LETTER O WITH DOUBLE GRAVE */
	[640+0x0F] = 0x020E,	/* U+020F: LATIN SMALL LETTER O WITH INVERTED BREVE */
	[640+0x11] = 0x0210,	/* U+0211: LATIN SMALL LETTER R WITH DOUBLE GRAVE */
	[640+0x13] = 0x0212,	/* U+0213: LATIN SMALL LETTER R WITH INVERTED BREVE */
	[640+0x15] = 0x0214,	/* U+0215: LATIN SMALL LETTER U WITH DOUBLE GRAVE */
	[640+0x17] = 0x0216,	/* U+0217: LATIN SMALL LETTER U WITH INVERTED BREVE */
	[640+0x19] = 0x0218,	/* U+0219: LATIN SMALL LETTER S WITH COMMA BELOW */
	[640+0x1B] = 0x021A,	/* U+021B: LATIN SMALL LETTER T WITH COMMA BELOW */
	[640+0x1D] = 0x021C,	/* U+021D: LATIN SMALL LETTER YOGH */
	[640+0x1F] = 0x021E,	/* U+021F: LATIN SMALL LETTER H WITH CARON */
	[640+0x23] = 0x0222,	/* U+0223: LATIN SMALL LETTER OU */
	[640+0x25] = 0x0224,	/* U+0225: LATIN SMALL LETTER Z WITH HOOK */
	[640+0x27] = 0x0226,	/* U+0227: LATIN SMALL LETTER A WITH DOT ABOVE */
	[640+0x29] = 0x0228,	/* U+0229: LATIN SMALL LETTER E WITH CEDILLA */
	[640+0x2B] = 0x022A,	/* U+022B: LATIN SMALL LETTER O WITH DIAERESIS AND MACRON */
	[640+0x2D] = 0x022C,	/* U+022D: LATIN SMALL LETTER O WITH TILDE AND MACRON */
	[640+0x2F] = 0x022E,	/* U+022F: LATIN SMALL LETTER O WITH DOT ABOVE */
	[640+0x31] = 0x0230,	/* U+0231: LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON */
	[640+0x33] = 0x0232,	/* U+0233: LATIN SMALL LETTER Y WITH MACRON */
	[640+0x3C] = 0x023B,	/* U+023C: LATIN SMALL LETTER C WITH STROKE */
	[640+0x3F] = 0x2C7E,	/* U+023F: LATIN SMALL LETTER S WITH SWASH TAIL */
	[0xC9] = 704 - 0x80,	/* 311 ... */
	[704+0x00] = 0x2C7F,	/* U+0240: LATIN SMALL LETTER Z WITH SWASH TAIL */
	[704+0x02] = 0x0241,	/* U+0242: LATIN SMALL LETTER GLOTTAL STOP */
	[704+0x07] = 0x0246,	/* U+0247: LATIN SMALL LETTER E WITH STROKE */
	[704+0x09] = 0x0248,	/* U+0249: LATIN SMALL LETTER J WITH STROKE */
	[704+0x0B] = 0x024A,	/* U+024B: LATIN SMALL LETTER Q WITH HOOK TAIL */
	[704+0x0D] = 0x024C,	/* U+024D: LATIN SMALL LETTER R WITH STROKE */
	[704+0x0F] = 0x024E,	/* U+024F: LATIN SMALL LETTER Y WITH STROKE */
	[704+0x10] = 0x2C6F,	/* U+0250: LATIN SMALL LETTER TURNED A */
	[704+0x11] = 0x2C6D,	/* U+0251: LATIN SMALL LETTER ALPHA */
	[704+0x12] = 0x2C70,	/* U+0252: LATIN SMALL LETTER TURNED ALPHA */
	[704+0x13] = 0x0181,	/* U+0253: LATIN SMALL LETTER B WITH HOOK */
	[704+0x14] = 0x0186,	/* U+0254: LATIN SMALL LETTER OPEN O */
	[704+0x16] = 0x0189,	/* U+0256: LATIN SMALL LETTER D WITH TAIL */
	[704+0x17] = 0x018A,	/* U+0257: LATIN SMALL LETTER D WITH HOOK */
	[704+0x19] = 0x018F,	/* U+0259: LATIN SMALL LETTER SCHWA */
	[704+0x1B] = 0x0190,	/* U+025B: LATIN SMALL LETTER OPEN E */
	[704+0x1C] = 0xA7AB,	/* U+025C: LATIN SMALL LETTER REVERSED OPEN E */
	[704+0x20] = 0x0193,	/* U+0260: LATIN SMALL LETTER G WITH HOOK */
	[704+0x21] = 0xA7AC,	/* U+0261: LATIN SMALL LETTER SCRIPT G */
	[704+0x23] = 0x0194,	/* U+0263: LATIN SMALL LETTER GAMMA */
	[704+0x25] = 0xA78D,	/* U+0265: LATIN SMALL LETTER TURNED H */
	[704+0x26] = 0xA7AA,	/* U+0266: LATIN SMALL LETTER H WITH HOOK */
	[704+0x28] = 0x0197,	/* U+0268: LATIN SMALL LETTER I WITH STROKE */
	[704+0x29] = 0x0196,	/* U+0269: LATIN SMALL LETTER IOTA */
	[704+0x2A] = 0xA7AE,	/* U+026A: LATIN LETTER SMALL CAPITAL I */
	[704+0x2B] = 0x2C62,	/* U+026B: LATIN SMALL LETTER L WITH MIDDLE TILDE */
	[704+0x2C] = 0xA7AD,	/* U+026C: LATIN SMALL LETTER L WITH BELT */
	[704+0x2F] = 0x019C,	/* U+026F: LATIN SMALL LETTER TURNED M */
	[704+0x31] = 0x2C6E,	/* U+0271: LATIN SMALL LETTER M WITH HOOK */
	[704+0x32] = 0x019D,	/* U+0272: LATIN SMALL LETTER N WITH LEFT HOOK */
	[704+0x35] = 0x019F,	/* U+0275: LATIN SMALL LETTER BARRED O */
	[704+0x3D] = 0x2C64,	/* U+027D: LATIN SMALL LETTER R WITH TAIL */
	[0xCA] = 768 - 0x80,	/* 312 ... */
	[768+0x00] = 0x01A6,	/* U+0280: LATIN LETTER SMALL CAPITAL R */
	[768+0x02] = 0xA7C5,	/* U+0282: LATIN SMALL LETTER S WITH HOOK */
	[768+0x03] = 0x01A9,	/* U+0283: LATIN SMALL LETTER ESH */
	[768+0x07] = 0xA7B1,	/* U+0287: LATIN SMALL LETTER TURNED T */
	[768+0x08] = 0x01AE,	/* U+0288: LATIN SMALL LETTER T WITH RETROFLEX HOOK */
	[768+0x09] = 0x0244,	/* U+0289: LATIN SMALL LETTER U BAR */
	[768+0x0A] = 0x01B1,	/* U+028A: LATIN SMALL LETTER UPSILON */
	[768+0x0B] = 0x01B2,	/* U+028B: LATIN SMALL LETTER V WITH HOOK */
	[768+0x0C] = 0x0245,	/* U+028C: LATIN SMALL LETTER TURNED V */
	[768+0x12] = 0x01B7,	/* U+0292: LATIN SMALL LETTER EZH */
	[768+0x1D] = 0xA7B2,	/* U+029D: LATIN SMALL LETTER J WITH CROSSED-TAIL */
	[768+0x1E] = 0xA7B0,	/* U+029E: LATIN SMALL LETTER TURNED K */
	[0xCD] = 832 - 0x80,	/* 315 ... */
	[832+0x05] = 0x0399,	/* U+0345: COMBINING GREEK YPOGEGRAMMENI */
	[832+0x31] = 0x0370,	/* U+0371: GREEK SMALL LETTER HETA */
	[832+0x33] = 0x0372,	/* U+0373: GREEK SMALL LETTER ARCHAIC SAMPI */
	[832+0x37] = 0x0376,	/* U+0377: GREEK SMALL LETTER PAMPHYLIAN DIGAMMA */
	[832+0x3B] = 0x03FD,	/* U+037B: GREEK SMALL REVERSED LUNATE SIGMA SYMBOL */
	[832+0x3C] = 0x03FE,	/* U+037C: GREEK SMALL DOTTED LUNATE SIGMA SYMBOL */
	[832+0x3D] = 0x03FF,	/* U+037D: GREEK SMALL REVERSED DOTTED LUNATE SIGMA SYMBOL */
	[0xCE] = 896 - 0x80,	/* 316 ... */
	[896+0x10] = -88,	/* U+0390: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND TONOS */
	[896+0x2C] = 0x0386,	/* U+03AC: GREEK SMALL LETTER ALPHA WITH TONOS */
	[896+0x2D] = 0x0388,	/* U+03AD: GREEK SMALL LETTER EPSILON WITH TONOS */
	[896+0x2E] = 0x0389,	/* U+03AE: GREEK SMALL LETTER ETA WITH TONOS */
	[896+0x2F] = 0x038A,	/* U+03AF: GREEK SMALL LETTER IOTA WITH TONOS */
	[896+0x30] = -89,	/* U+03B0: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND TONOS */
	[896+0x31] = 0x0391,	/* U+03B1: GREEK SMALL LETTER ALPHA */
	[896+0x32] = 0x0392,	/* U+03B2: GREEK SMALL LETTER BETA */
	[896+0x33] = 0x0393,	/* U+03B3: GREEK SMALL LETTER GAMMA */
	[896+0x34] = 0x0394,	/* U+03B4: GREEK SMALL LETTER DELTA */
	[896+0x35] = 0x0395,	/* U+03B5: GREEK SMALL LETTER EPSILON */
	[896+0x36] = 0x0396,	/* U+03B6: GREEK SMALL LETTER ZETA */
	[896+0x37] = 0x0397,	/* U+03B7: GREEK SMALL LETTER ETA */
	[896+0x38] = 0x0398,	/* U+03B8: GREEK SMALL LETTER THETA */
	[896+0x39] = 0x0399,	/* U+03B9: GREEK SMALL LETTER IOTA */
	[896+0x3A] = 0x039A,	/* U+03BA: GREEK SMALL LETTER KAPPA */
	[896+0x3B] = 0x039B,	/* U+03BB: GREEK SMALL LETTER LAMDA */
	[896+0x3C] = 0x039C,	/* U+03BC: GREEK SMALL LETTER MU */
	[896+0x3D] = 0x039D,	/* U+03BD: GREEK SMALL LETTER NU */
	[896+0x3E] = 0x039E,	/* U+03BE: GREEK SMALL LETTER XI */
	[896+0x3F] = 0x039F,	/* U+03BF: GREEK SMALL LETTER OMICRON */
	[0xCF] = 960 - 0x80,	/* 317 ... */
	[960+0x00] = 0x03A0,	/* U+03C0: GREEK SMALL LETTER PI */
	[960+0x01] = 0x03A1,	/* U+03C1: GREEK SMALL LETTER RHO */
	[960+0x02] = 0x03A3,	/* U+03C2: GREEK SMALL LETTER FINAL SIGMA */
	[960+0x03] = 0x03A3,	/* U+03C3: GREEK SMALL LETTER SIGMA */
	[960+0x04] = 0x03A4,	/* U+03C4: GREEK SMALL LETTER TAU */
	[960+0x05] = 0x03A5,	/* U+03C5: GREEK SMALL LETTER UPSILON */
	[960+0x06] = 0x03A6,	/* U+03C6: GREEK SMALL LETTER PHI */
	[960+0x07] = 0x03A7,	/* U+03C7: GREEK SMALL LETTER CHI */
	[960+0x08] = 0x03A8,	/* U+03C8: GREEK SMALL LETTER PSI */
	[960+0x09] = 0x03A9,	/* U+03C9: GREEK SMALL LETTER OMEGA */
	[960+0x0A] = 0x03AA,	/* U+03CA: GREEK SMALL LETTER IOTA WITH DIALYTIKA */
	[960+0x0B] = 0x03AB,	/* U+03CB: GREEK SMALL LETTER UPSILON WITH DIALYTIKA */
	[960+0x0C] = 0x038C,	/* U+03CC: GREEK SMALL LETTER OMICRON WITH TONOS */
	[960+0x0D] = 0x038E,	/* U+03CD: GREEK SMALL LETTER UPSILON WITH TONOS */
	[960+0x0E] = 0x038F,	/* U+03CE: GREEK SMALL LETTER OMEGA WITH TONOS */
	[960+0x10] = 0x0392,	/* U+03D0: GREEK BETA SYMBOL */
	[960+0x11] = 0x0398,	/* U+03D1: GREEK THETA SYMBOL */
	[960+0x15] = 0x03A6,	/* U+03D5: GREEK PHI SYMBOL */
	[960+0x16] = 0x03A0,	/* U+03D6: GREEK PI SYMBOL */
	[960+0x17] = 0x03CF,	/* U+03D7: GREEK KAI SYMBOL */
	[960+0x19] = 0x03D8,	/* U+03D9: GREEK SMALL LETTER ARCHAIC KOPPA */
	[960+0x1B] = 0x03DA,	/* U+03DB: GREEK SMALL LETTER STIGMA */
	[960+0x1D] = 0x03DC,	/* U+03DD: GREEK SMALL LETTER DIGAMMA */
	[960+0x1F] = 0x03DE,	/* U+03DF: GREEK SMALL LETTER KOPPA */
	[960+0x21] = 0x03E0,	/* U+03E1: GREEK SMALL LETTER SAMPI */
	[960+0x23] = 0x03E2,	/* U+03E3: COPTIC SMALL LETTER SHEI */
	[960+0x25] = 0x03E4,	/* U+03E5: COPTIC SMALL LETTER FEI */
	[960+0x27] = 0x03E6,	/* U+03E7: COPTIC SMALL LETTER KHEI */
	[960+0x29] = 0x03E8,	/* U+03E9: COPTIC SMALL LETTER HORI */
	[960+0x2B] = 0x03EA,	/* U+03EB: COPTIC SMALL LETTER GANGIA */
	[960+0x2D] = 0x03EC,	/* U+03ED: COPTIC SMALL LETTER SHIMA */
	[960+0x2F] = 0x03EE,	/* U+03EF: COPTIC SMALL LETTER DEI */
	[960+0x30] = 0x039A,	/* U+03F0: GREEK KAPPA SYMBOL */
	[960+0x31] = 0x03A1,	/* U+03F1: GREEK RHO SYMBOL */
	[960+0x32] = 0x03F9,	/* U+03F2: GREEK LUNATE SIGMA SYMBOL */
	[960+0x33] = 0x037F,	/* U+03F3: GREEK LETTER YOT */
	[960+0x35] = 0x0395,	/* U+03F5: GREEK LUNATE EPSILON SYMBOL */
	[960+0x38] = 0x03F7,	/* U+03F8: GREEK SMALL LETTER SHO */
	[960+0x3B] = 0x03FA,	/* U+03FB: GREEK SMALL LETTER SAN */
	[0xD0] = 1024 - 0x80,	/* 320 ... */
	[1024+0x30] = 0x0410,	/* U+0430: CYRILLIC SMALL LETTER A */
	[1024+0x31] = 0x0411,	/* U+0431: CYRILLIC SMALL LETTER BE */
	[1024+0x32] = 0x0412,	/* U+0432: CYRILLIC SMALL LETTER VE */
	[1024+0x33] = 0x0413,	/* U+0433: CYRILLIC SMALL LETTER GHE */
	[1024+0x34] = 0x0414,	/* U+0434: CYRILLIC SMALL LETTER DE */
	[1024+0x35] = 0x0415,	/* U+0435: CYRILLIC SMALL LETTER IE */
	[1024+0x36] = 0x0416,	/* U+0436: CYRILLIC SMALL LETTER ZHE */
	[1024+0x37] = 0x0417,	/* U+0437: CYRILLIC SMALL LETTER ZE */
	[1024+0x38] = 0x0418,	/* U+0438: CYRILLIC SMALL LETTER I */
	[1024+0x39] = 0x0419,	/* U+0439: CYRILLIC SMALL LETTER SHORT I */
	[1024+0x3A] = 0x041A,	/* U+043A: CYRILLIC SMALL LETTER KA */
	[1024+0x3B] = 0x041B,	/* U+043B: CYRILLIC SMALL LETTER EL */
	[1024+0x3C] = 0x041C,	/* U+043C: CYRILLIC SMALL LETTER EM */
	[1024+0x3D] = 0x041D,	/* U+043D: CYRILLIC SMALL LETTER EN */
	[1024+0x3E] = 0x041E,	/* U+043E: CYRILLIC SMALL LETTER O */
	[1024+0x3F] = 0x041F,	/* U+043F: CYRILLIC SMALL LETTER PE */
	[0xD1] = 1088 - 0x80,	/* 321 ... */
	[1088+0x00] = 0x0420,	/* U+0440: CYRILLIC SMALL LETTER ER */
	[1088+0x01] = 0x0421,	/* U+0441: CYRILLIC SMALL LETTER ES */
	[1088+0x02] = 0x0422,	/* U+0442: CYRILLIC SMALL LETTER TE */
	[1088+0x03] = 0x0423,	/* U+0443: CYRILLIC SMALL LETTER U */
	[1088+0x04] = 0x0424,	/* U+0444: CYRILLIC SMALL LETTER EF */
	[1088+0x05] = 0x0425,	/* U+0445: CYRILLIC SMALL LETTER HA */
	[1088+0x06] = 0x0426,	/* U+0446: CYRILLIC SMALL LETTER TSE */
	[1088+0x07] = 0x0427,	/* U+0447: CYRILLIC SMALL LETTER CHE */
	[1088+0x08] = 0x0428,	/* U+0448: CYRILLIC SMALL LETTER SHA */
	[1088+0x09] = 0x0429,	/* U+0449: CYRILLIC SMALL LETTER SHCHA */
	[1088+0x0A] = 0x042A,	/* U+044A: CYRILLIC SMALL LETTER HARD SIGN */
	[1088+0x0B] = 0x042B,	/* U+044B: CYRILLIC SMALL LETTER YERU */
	[1088+0x0C] = 0x042C,	/* U+044C: CYRILLIC SMALL LETTER SOFT SIGN */
	[1088+0x0D] = 0x042D,	/* U+044D: CYRILLIC SMALL LETTER E */
	[1088+0x0E] = 0x042E,	/* U+044E: CYRILLIC SMALL LETTER YU */
	[1088+0x0F] = 0x042F,	/* U+044F: CYRILLIC SMALL LETTER YA */
	[1088+0x10] = 0x0400,	/* U+0450: CYRILLIC SMALL LETTER IE WITH GRAVE */
	[1088+0x11] = 0x0401,	/* U+0451: CYRILLIC SMALL LETTER IO */
	[1088+0x12] = 0x0402,	/* U+0452: CYRILLIC SMALL LETTER DJE */
	[1088+0x13] = 0x0403,	/* U+0453: CYRILLIC SMALL LETTER GJE */
	[1088+0x14] = 0x0404,	/* U+0454: CYRILLIC SMALL LETTER UKRAINIAN IE */
	[1088+0x15] = 0x0405,	/* U+0455: CYRILLIC SMALL LETTER DZE */
	[1088+0x16] = 0x0406,	/* U+0456: CYRILLIC SMALL LETTER BYELORUSSIAN-UKRAINIAN I */
	[1088+0x17] = 0x0407,	/* U+0457: CYRILLIC SMALL LETTER YI */
	[1088+0x18] = 0x0408,	/* U+0458: CYRILLIC SMALL LETTER JE */
	[1088+0x19] = 0x0409,	/* U+0459: CYRILLIC SMALL LETTER LJE */
	[1088+0x1A] = 0x040A,	/* U+045A: CYRILLIC SMALL LETTER NJE */
	[1088+0x1B] = 0x040B,	/* U+045B: CYRILLIC SMALL LETTER TSHE */
	[1088+0x1C] = 0x040C,	/* U+045C: CYRILLIC SMALL LETTER KJE */
	[1088+0x1D] = 0x040D,	/* U+045D: CYRILLIC SMALL LETTER I WITH GRAVE */
	[1088+0x1E] = 0x040E,	/* U+045E: CYRILLIC SMALL LETTER SHORT U */
	[1088+0x1F] = 0x040F,	/* U+045F: CYRILLIC SMALL LETTER DZHE */
	[1088+0x21] = 0x0460,	/* U+0461: CYRILLIC SMALL LETTER OMEGA */
	[1088+0x23] = 0x0462,	/* U+0463: CYRILLIC SMALL LETTER YAT */
	[1088+0x25] = 0x0464,	/* U+0465: CYRILLIC SMALL LETTER IOTIFIED E */
	[1088+0x27] = 0x0466,	/* U+0467: CYRILLIC SMALL LETTER LITTLE YUS */
	[1088+0x29] = 0x0468,	/* U+0469: CYRILLIC SMALL LETTER IOTIFIED LITTLE YUS */
	[1088+0x2B] = 0x046A,	/* U+046B: CYRILLIC SMALL LETTER BIG YUS */
	[1088+0x2D] = 0x046C,	/* U+046D: CYRILLIC SMALL LETTER IOTIFIED BIG YUS */
	[1088+0x2F] = 0x046E,	/* U+046F: CYRILLIC SMALL LETTER KSI */
	[1088+0x31] = 0x0470,	/* U+0471: CYRILLIC SMALL LETTER PSI */
	[1088+0x33] = 0x0472,	/* U+0473: CYRILLIC SMALL LETTER FITA */
	[1088+0x35] = 0x0474,	/* U+0475: CYRILLIC SMALL LETTER IZHITSA */
	[1088+0x37] = 0x0476,	/* U+0477: CYRILLIC SMALL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT */
	[1088+0x39] = 0x0478,	/* U+0479: CYRILLIC SMALL LETTER UK */
	[1088+0x3B] = 0x047A,	/* U+047B: CYRILLIC SMALL LETTER ROUND OMEGA */
	[1088+0x3D] = 0x047C,	/* U+047D: CYRILLIC SMALL LETTER OMEGA WITH TITLO */
	[1088+0x3F] = 0x047E,	/* U+047F: CYRILLIC SMALL LETTER OT */
	[0xD2] = 1152 - 0x80,	/* 322 ... */
	[1152+0x01] = 0x0480,	/* U+0481: CYRILLIC SMALL LETTER KOPPA */
	[1152+0x0B] = 0x048A,	/* U+048B: CYRILLIC SMALL LETTER SHORT I WITH TAIL */
	[1152+0x0D] = 0x048C,	/* U+048D: CYRILLIC SMALL LETTER SEMISOFT SIGN */
	[1152+0x0F] = 0x048E,	/* U+048F: CYRILLIC SMALL LETTER ER WITH TICK */
	[1152+0x11] = 0x0490,	/* U+0491: CYRILLIC SMALL LETTER GHE WITH UPTURN */
	[1152+0x13] = 0x0492,	/* U+0493: CYRILLIC SMALL LETTER GHE WITH STROKE */
	[1152+0x15] = 0x0494,	/* U+0495: CYRILLIC SMALL LETTER GHE WITH MIDDLE HOOK */
	[1152+0x17] = 0x0496,	/* U+0497: CYRILLIC SMALL LETTER ZHE WITH DESCENDER */
	[1152+0x19] = 0x0498,	/* U+0499: CYRILLIC SMALL LETTER ZE WITH DESCENDER */
	[1152+0x1B] = 0x049A,	/* U+049B: CYRILLIC SMALL LETTER KA WITH DESCENDER */
	[1152+0x1D] = 0x049C,	/* U+049D: CYRILLIC SMALL LETTER KA WITH VERTICAL STROKE */
	[1152+0x1F] = 0x049E,	/* U+049F: CYRILLIC SMALL LETTER KA WITH STROKE */
	[1152+0x21] = 0x04A0,	/* U+04A1: CYRILLIC SMALL LETTER BASHKIR KA */
	[1152+0x23] = 0x04A2,	/* U+04A3: CYRILLIC SMALL LETTER EN WITH DESCENDER */
	[1152+0x25] = 0x04A4,	/* U+04A5: CYRILLIC SMALL LIGATURE EN GHE */
	[1152+0x27] = 0x04A6,	/* U+04A7: CYRILLIC SMALL LETTER PE WITH MIDDLE HOOK */
	[1152+0x29] = 0x04A8,	/* U+04A9: CYRILLIC SMALL LETTER ABKHASIAN HA */
	[1152+0x2B] = 0x04AA,	/* U+04AB: CYRILLIC SMALL LETTER ES WITH DESCENDER */
	[1152+0x2D] = 0x04AC,	/* U+04AD: CYRILLIC SMALL LETTER TE WITH DESCENDER */
	[1152+0x2F] = 0x04AE,	/* U+04AF: CYRILLIC SMALL LETTER STRAIGHT U */
	[1152+0x31] = 0x04B0,	/* U+04B1: CYRILLIC SMALL LETTER STRAIGHT U WITH STROKE */
	[1152+0x33] = 0x04B2,	/* U+04B3: CYRILLIC SMALL LETTER HA WITH DESCENDER */
	[1152+0x35] = 0x04B4,	/* U+04B5: CYRILLIC SMALL LIGATURE TE TSE */
	[1152+0x37] = 0x04B6,	/* U+04B7: CYRILLIC SMALL LETTER CHE WITH DESCENDER */
	[1152+0x39] = 0x04B8,	/* U+04B9: CYRILLIC SMALL LETTER CHE WITH VERTICAL STROKE */
	[1152+0x3B] = 0x04BA,	/* U+04BB: CYRILLIC SMALL LETTER SHHA */
	[1152+0x3D] = 0x04BC,	/* U+04BD: CYRILLIC SMALL LETTER ABKHASIAN CHE */
	[1152+0x3F] = 0x04BE,	/* U+04BF: CYRILLIC SMALL LETTER ABKHASIAN CHE WITH DESCENDER */
	[0xD3] = 1216 - 0x80,	/* 323 ... */
	[1216+0x02] = 0x04C1,	/* U+04C2: CYRILLIC SMALL LETTER ZHE WITH BREVE */
	[1216+0x04] = 0x04C3,	/* U+04C4: CYRILLIC SMALL LETTER KA WITH HOOK */
	[1216+0x06] = 0x04C5,	/* U+04C6: CYRILLIC SMALL LETTER EL WITH TAIL */
	[1216+0x08] = 0x04C7,	/* U+04C8: CYRILLIC SMALL LETTER EN WITH HOOK */
	[1216+0x0A] = 0x04C9,	/* U+04CA: CYRILLIC SMALL LETTER EN WITH TAIL */
	[1216+0x0C] = 0x04CB,	/* U+04CC: CYRILLIC SMALL LETTER KHAKASSIAN CHE */
	[1216+0x0E] = 0x04CD,	/* U+04CE: CYRILLIC SMALL LETTER EM WITH TAIL */
	[1216+0x0F] = 0x04C0,	/* U+04CF: CYRILLIC SMALL LETTER PALOCHKA */
	[1216+0x11] = 0x04D0,	/* U+04D1: CYRILLIC SMALL LETTER A WITH BREVE */
	[1216+0x13] = 0x04D2,	/* U+04D3: CYRILLIC SMALL LETTER A WITH DIAERESIS */
	[1216+0x15] = 0x04D4,	/* U+04D5: CYRILLIC SMALL LIGATURE A IE */
	[1216+0x17] = 0x04D6,	/* U+04D7: CYRILLIC SMALL LETTER IE WITH BREVE */
	[1216+0x19] = 0x04D8,	/* U+04D9: CYRILLIC SMALL LETTER SCHWA */
	[1216+0x1B] = 0x04DA,	/* U+04DB: CYRILLIC SMALL LETTER SCHWA WITH DIAERESIS */
	[1216+0x1D] = 0x04DC,	/* U+04DD: CYRILLIC SMALL LETTER ZHE WITH DIAERESIS */
	[1216+0x1F] = 0x04DE,	/* U+04DF: CYRILLIC SMALL LETTER ZE WITH DIAERESIS */
	[1216+0x21] = 0x04E0,	/* U+04E1: CYRILLIC SMALL LETTER ABKHASIAN DZE */
	[1216+0x23] = 0x04E2,	/* U+04E3: CYRILLIC SMALL LETTER I WITH MACRON */
	[1216+0x25] = 0x04E4,	/* U+04E5: CYRILLIC SMALL LETTER I WITH DIAERESIS */
	[1216+0x27] = 0x04E6,	/* U+04E7: CYRILLIC SMALL LETTER O WITH DIAERESIS */
	[1216+0x29] = 0x04E8,	/* U+04E9: CYRILLIC SMALL LETTER BARRED O */
	[1216+0x2B] = 0x04EA,	/* U+04EB: CYRILLIC SMALL LETTER BARRED O WITH DIAERESIS */
	[1216+0x2D] = 0x04EC,	/* U+04ED: CYRILLIC SMALL LETTER E WITH DIAERESIS */
	[1216+0x2F] = 0x04EE,	/* U+04EF: CYRILLIC SMALL LETTER U WITH MACRON */
	[1216+0x31] = 0x04F0,	/* U+04F1: CYRILLIC SMALL LETTER U WITH DIAERESIS */
	[1216+0x33] = 0x04F2,	/* U+04F3: CYRILLIC SMALL LETTER U WITH DOUBLE ACUTE */
	[1216+0x35] = 0x04F4,	/* U+04F5: CYRILLIC SMALL LETTER CHE WITH DIAERESIS */
	[1216+0x37] = 0x04F6,	/* U+04F7: CYRILLIC SMALL LETTER GHE WITH DESCENDER */
	[1216+0x39] = 0x04F8,	/* U+04F9: CYRILLIC SMALL LETTER YERU WITH DIAERESIS */
	[1216+0x3B] = 0x04FA,	/* U+04FB: CYRILLIC SMALL LETTER GHE WITH STROKE AND HOOK */
	[1216+0x3D] = 0x04FC,	/* U+04FD: CYRILLIC SMALL LETTER HA WITH HOOK */
	[1216+0x3F] = 0x04FE,	/* U+04FF: CYRILLIC SMALL LETTER HA WITH STROKE */
	[0xD4] = 1280 - 0x80,	/* 324 ... */
	[1280+0x01] = 0x0500,	/* U+0501: CYRILLIC SMALL LETTER KOMI DE */
	[1280+0x03] = 0x0502,	/* U+0503: CYRILLIC SMALL LETTER KOMI DJE */
	[1280+0x05] = 0x0504,	/* U+0505: CYRILLIC SMALL LETTER KOMI ZJE */
	[1280+0x07] = 0x0506,	/* U+0507: CYRILLIC SMALL LETTER KOMI DZJE */
	[1280+0x09] = 0x0508,	/* U+0509: CYRILLIC SMALL LETTER KOMI LJE */
	[1280+0x0B] = 0x050A,	/* U+050B: CYRILLIC SMALL LETTER KOMI NJE */
	[1280+0x0D] = 0x050C,	/* U+050D: CYRILLIC SMALL LETTER KOMI SJE */
	[1280+0x0F] = 0x050E,	/* U+050F: CYRILLIC SMALL LETTER KOMI TJE */
	[1280+0x11] = 0x0510,	/* U+0511: CYRILLIC SMALL LETTER REVERSED ZE */
	[1280+0x13] = 0x0512,	/* U+0513: CYRILLIC SMALL LETTER EL WITH HOOK */
	[1280+0x15] = 0x0514,	/* U+0515: CYRILLIC SMALL LETTER LHA */
	[1280+0x17] = 0x0516,	/* U+0517: CYRILLIC SMALL LETTER RHA */
	[1280+0x19] = 0x0518,	/* U+0519: CYRILLIC SMALL LETTER YAE */
	[1280+0x1B] = 0x051A,	/* U+051B: CYRILLIC SMALL LETTER QA */
	[1280+0x1D] = 0x051C,	/* U+051D: CYRILLIC SMALL LETTER WE */
	[1280+0x1F] = 0x051E,	/* U+051F: CYRILLIC SMALL LETTER ALEUT KA */
	[1280+0x21] = 0x0520,	/* U+0521: CYRILLIC SMALL LETTER EL WITH MIDDLE HOOK */
	[1280+0x23] = 0x0522,	/* U+0523: CYRILLIC SMALL LETTER EN WITH MIDDLE HOOK */
	[1280+0x25] = 0x0524,	/* U+0525: CYRILLIC SMALL LETTER PE WITH DESCENDER */
	[1280+0x27] = 0x0526,	/* U+0527: CYRILLIC SMALL LETTER SHHA WITH DESCENDER */
	[1280+0x29] = 0x0528,	/* U+0529: CYRILLIC SMALL LETTER EN WITH LEFT HOOK */
	[1280+0x2B] = 0x052A,	/* U+052B: CYRILLIC SMALL LETTER DZZHE */
	[1280+0x2D] = 0x052C,	/* U+052D: CYRILLIC SMALL LETTER DCHE */
	[1280+0x2F] = 0x052E,	/* U+052F: CYRILLIC SMALL LETTER EL WITH DESCENDER */
	[0xD5] = 1344 - 0x80,	/* 325 ... */
	[1344+0x21] = 0x0531,	/* U+0561: ARMENIAN SMALL LETTER AYB */
	[1344+0x22] = 0x0532,	/* U+0562: ARMENIAN SMALL LETTER BEN */
	[1344+0x23] = 0x0533,	/* U+0563: ARMENIAN SMALL LETTER GIM */
	[1344+0x24] = 0x0534,	/* U+0564: ARMENIAN SMALL LETTER DA */
	[1344+0x25] = 0x0535,	/* U+0565: ARMENIAN SMALL LETTER ECH */
	[1344+0x26] = 0x0536,	/* U+0566: ARMENIAN SMALL LETTER ZA */
	[1344+0x27] = 0x0537,	/* U+0567: ARMENIAN SMALL LETTER EH */
	[1344+0x28] = 0x0538,	/* U+0568: ARMENIAN SMALL LETTER ET */
	[1344+0x29] = 0x0539,	/* U+0569: ARMENIAN SMALL LETTER TO */
	[1344+0x2A] = 0x053A,	/* U+056A: ARMENIAN SMALL LETTER ZHE */
	[1344+0x2B] = 0x053B,	/* U+056B: ARMENIAN SMALL LETTER INI */
	[1344+0x2C] = 0x053C,	/* U+056C: ARMENIAN SMALL LETTER LIWN */
	[1344+0x2D] = 0x053D,	/* U+056D: ARMENIAN SMALL LETTER XEH */
	[1344+0x2E] = 0x053E,	/* U+056E: ARMENIAN SMALL LETTER CA */
	[1344+0x2F] = 0x053F,	/* U+056F: ARMENIAN SMALL LETTER KEN */
	[1344+0x30] = 0x0540,	/* U+0570: ARMENIAN SMALL LETTER HO */
	[1344+0x31] = 0x0541,	/* U+0571: ARMENIAN SMALL LETTER JA */
	[1344+0x32] = 0x0542,	/* U+0572: ARMENIAN SMALL LETTER GHAD */
	[1344+0x33] = 0x0543,	/* U+0573: ARMENIAN SMALL LETTER CHEH */
	[1344+0x34] = 0x0544,	/* U+0574: ARMENIAN SMALL LETTER MEN */
	[1344+0x35] = 0x0545,	/* U+0575: ARMENIAN SMALL LETTER YI */
	[1344+0x36] = 0x0546,	/* U+0576: ARMENIAN SMALL LETTER NOW */
	[1344+0x37] = 0x0547,	/* U+0577: ARMENIAN SMALL LETTER SHA */
	[1344+0x38] = 0x0548,	/* U+0578: ARMENIAN SMALL LETTER VO */
	[1344+0x39] = 0x0549,	/* U+0579: ARMENIAN SMALL LETTER CHA */
	[1344+0x3A] = 0x054A,	/* U+057A: ARMENIAN SMALL LETTER PEH */
	[1344+0x3B] = 0x054B,	/* U+057B: ARMENIAN SMALL LETTER JHEH */
	[1344+0x3C] = 0x054C,	/* U+057C: ARMENIAN SMALL LETTER RA */
	[1344+0x3D] = 0x054D,	/* U+057D: ARMENIAN SMALL LETTER SEH */
	[1344+0x3E] = 0x054E,	/* U+057E: ARMENIAN SMALL LETTER VEW */
	[1344+0x3F] = 0x054F,	/* U+057F: ARMENIAN SMALL LETTER TIWN */
	[0xD6] = 1408 - 0x80,	/* 326 ... */
	[1408+0x00] = 0x0550,	/* U+0580: ARMENIAN SMALL LETTER REH */
	[1408+0x01] = 0x0551,	/* U+0581: ARMENIAN SMALL LETTER CO */
	[1408+0x02] = 0x0552,	/* U+0582: ARMENIAN SMALL LETTER YIWN */
	[1408+0x03] = 0x0553,	/* U+0583: ARMENIAN SMALL LETTER PIWR */
	[1408+0x04] = 0x0554,	/* U+0584: ARMENIAN SMALL LETTER KEH */
	[1408+0x05] = 0x0555,	/* U+0585: ARMENIAN SMALL LETTER OH */
	[1408+0x06] = 0x0556,	/* U+0586: ARMENIAN SMALL LETTER FEH */
	[1408+0x07] = -81,	/* U+0587: ARMENIAN SMALL LIGATURE ECH YIWN */
	[0xE1] = 1472 - 0x80,	/* 341 ... */
	[1472+0x03] = 1536 - 0x80,	/* 341 203 ... */
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
	[1472+0x0F] = 1600 - 0x80,	/* 341 217 ... */
	[1600+0x38] = 0x13F0,	/* U+13F8: CHEROKEE SMALL LETTER YE */
	[1600+0x39] = 0x13F1,	/* U+13F9: CHEROKEE SMALL LETTER YI */
	[1600+0x3A] = 0x13F2,	/* U+13FA: CHEROKEE SMALL LETTER YO */
	[1600+0x3B] = 0x13F3,	/* U+13FB: CHEROKEE SMALL LETTER YU */
	[1600+0x3C] = 0x13F4,	/* U+13FC: CHEROKEE SMALL LETTER YV */
	[1600+0x3D] = 0x13F5,	/* U+13FD: CHEROKEE SMALL LETTER MV */
	[1472+0x32] = 1664 - 0x80,	/* 341 262 ... */
	[1664+0x00] = 0x0412,	/* U+1C80: CYRILLIC SMALL LETTER ROUNDED VE */
	[1664+0x01] = 0x0414,	/* U+1C81: CYRILLIC SMALL LETTER LONG-LEGGED DE */
	[1664+0x02] = 0x041E,	/* U+1C82: CYRILLIC SMALL LETTER NARROW O */
	[1664+0x03] = 0x0421,	/* U+1C83: CYRILLIC SMALL LETTER WIDE ES */
	[1664+0x04] = 0x0422,	/* U+1C84: CYRILLIC SMALL LETTER TALL TE */
	[1664+0x05] = 0x0422,	/* U+1C85: CYRILLIC SMALL LETTER THREE-LEGGED TE */
	[1664+0x06] = 0x042A,	/* U+1C86: CYRILLIC SMALL LETTER TALL HARD SIGN */
	[1664+0x07] = 0x0462,	/* U+1C87: CYRILLIC SMALL LETTER TALL YAT */
	[1664+0x08] = 0xA64A,	/* U+1C88: CYRILLIC SMALL LETTER UNBLENDED UK */
	[1472+0x35] = 1728 - 0x80,	/* 341 265 ... */
	[1728+0x39] = 0xA77D,	/* U+1D79: LATIN SMALL LETTER INSULAR G */
	[1728+0x3D] = 0x2C63,	/* U+1D7D: LATIN SMALL LETTER P WITH STROKE */
	[1472+0x36] = 1792 - 0x80,	/* 341 266 ... */
	[1792+0x0E] = 0xA7C6,	/* U+1D8E: LATIN SMALL LETTER Z WITH PALATAL HOOK */
	[1472+0x38] = 1856 - 0x80,	/* 341 270 ... */
	[1856+0x01] = 0x1E00,	/* U+1E01: LATIN SMALL LETTER A WITH RING BELOW */
	[1856+0x03] = 0x1E02,	/* U+1E03: LATIN SMALL LETTER B WITH DOT ABOVE */
	[1856+0x05] = 0x1E04,	/* U+1E05: LATIN SMALL LETTER B WITH DOT BELOW */
	[1856+0x07] = 0x1E06,	/* U+1E07: LATIN SMALL LETTER B WITH LINE BELOW */
	[1856+0x09] = 0x1E08,	/* U+1E09: LATIN SMALL LETTER C WITH CEDILLA AND ACUTE */
	[1856+0x0B] = 0x1E0A,	/* U+1E0B: LATIN SMALL LETTER D WITH DOT ABOVE */
	[1856+0x0D] = 0x1E0C,	/* U+1E0D: LATIN SMALL LETTER D WITH DOT BELOW */
	[1856+0x0F] = 0x1E0E,	/* U+1E0F: LATIN SMALL LETTER D WITH LINE BELOW */
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
	[1920+0x01] = 0x1E40,	/* U+1E41: LATIN SMALL LETTER M WITH DOT ABOVE */
	[1920+0x03] = 0x1E42,	/* U+1E43: LATIN SMALL LETTER M WITH DOT BELOW */
	[1920+0x05] = 0x1E44,	/* U+1E45: LATIN SMALL LETTER N WITH DOT ABOVE */
	[1920+0x07] = 0x1E46,	/* U+1E47: LATIN SMALL LETTER N WITH DOT BELOW */
	[1920+0x09] = 0x1E48,	/* U+1E49: LATIN SMALL LETTER N WITH LINE BELOW */
	[1920+0x0B] = 0x1E4A,	/* U+1E4B: LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW */
	[1920+0x0D] = 0x1E4C,	/* U+1E4D: LATIN SMALL LETTER O WITH TILDE AND ACUTE */
	[1920+0x0F] = 0x1E4E,	/* U+1E4F: LATIN SMALL LETTER O WITH TILDE AND DIAERESIS */
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
	[1984+0x01] = 0x1E80,	/* U+1E81: LATIN SMALL LETTER W WITH GRAVE */
	[1984+0x03] = 0x1E82,	/* U+1E83: LATIN SMALL LETTER W WITH ACUTE */
	[1984+0x05] = 0x1E84,	/* U+1E85: LATIN SMALL LETTER W WITH DIAERESIS */
	[1984+0x07] = 0x1E86,	/* U+1E87: LATIN SMALL LETTER W WITH DOT ABOVE */
	[1984+0x09] = 0x1E88,	/* U+1E89: LATIN SMALL LETTER W WITH DOT BELOW */
	[1984+0x0B] = 0x1E8A,	/* U+1E8B: LATIN SMALL LETTER X WITH DOT ABOVE */
	[1984+0x0D] = 0x1E8C,	/* U+1E8D: LATIN SMALL LETTER X WITH DIAERESIS */
	[1984+0x0F] = 0x1E8E,	/* U+1E8F: LATIN SMALL LETTER Y WITH DOT ABOVE */
	[1984+0x11] = 0x1E90,	/* U+1E91: LATIN SMALL LETTER Z WITH CIRCUMFLEX */
	[1984+0x13] = 0x1E92,	/* U+1E93: LATIN SMALL LETTER Z WITH DOT BELOW */
	[1984+0x15] = 0x1E94,	/* U+1E95: LATIN SMALL LETTER Z WITH LINE BELOW */
	[1984+0x16] = -91,	/* U+1E96: LATIN SMALL LETTER H WITH LINE BELOW */
	[1984+0x17] = -92,	/* U+1E97: LATIN SMALL LETTER T WITH DIAERESIS */
	[1984+0x18] = -93,	/* U+1E98: LATIN SMALL LETTER W WITH RING ABOVE */
	[1984+0x19] = -94,	/* U+1E99: LATIN SMALL LETTER Y WITH RING ABOVE */
	[1984+0x1A] = -95,	/* U+1E9A: LATIN SMALL LETTER A WITH RIGHT HALF RING */
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
	[2048+0x01] = 0x1EC0,	/* U+1EC1: LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[2048+0x03] = 0x1EC2,	/* U+1EC3: LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[2048+0x05] = 0x1EC4,	/* U+1EC5: LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE */
	[2048+0x07] = 0x1EC6,	/* U+1EC7: LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[2048+0x09] = 0x1EC8,	/* U+1EC9: LATIN SMALL LETTER I WITH HOOK ABOVE */
	[2048+0x0B] = 0x1ECA,	/* U+1ECB: LATIN SMALL LETTER I WITH DOT BELOW */
	[2048+0x0D] = 0x1ECC,	/* U+1ECD: LATIN SMALL LETTER O WITH DOT BELOW */
	[2048+0x0F] = 0x1ECE,	/* U+1ECF: LATIN SMALL LETTER O WITH HOOK ABOVE */
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
	[2112+0x00] = 0x1F08,	/* U+1F00: GREEK SMALL LETTER ALPHA WITH PSILI */
	[2112+0x01] = 0x1F09,	/* U+1F01: GREEK SMALL LETTER ALPHA WITH DASIA */
	[2112+0x02] = 0x1F0A,	/* U+1F02: GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA */
	[2112+0x03] = 0x1F0B,	/* U+1F03: GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA */
	[2112+0x04] = 0x1F0C,	/* U+1F04: GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA */
	[2112+0x05] = 0x1F0D,	/* U+1F05: GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA */
	[2112+0x06] = 0x1F0E,	/* U+1F06: GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI */
	[2112+0x07] = 0x1F0F,	/* U+1F07: GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI */
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
	[2176+0x00] = 0x1F48,	/* U+1F40: GREEK SMALL LETTER OMICRON WITH PSILI */
	[2176+0x01] = 0x1F49,	/* U+1F41: GREEK SMALL LETTER OMICRON WITH DASIA */
	[2176+0x02] = 0x1F4A,	/* U+1F42: GREEK SMALL LETTER OMICRON WITH PSILI AND VARIA */
	[2176+0x03] = 0x1F4B,	/* U+1F43: GREEK SMALL LETTER OMICRON WITH DASIA AND VARIA */
	[2176+0x04] = 0x1F4C,	/* U+1F44: GREEK SMALL LETTER OMICRON WITH PSILI AND OXIA */
	[2176+0x05] = 0x1F4D,	/* U+1F45: GREEK SMALL LETTER OMICRON WITH DASIA AND OXIA */
	[2176+0x10] = -96,	/* U+1F50: GREEK SMALL LETTER UPSILON WITH PSILI */
	[2176+0x11] = 0x1F59,	/* U+1F51: GREEK SMALL LETTER UPSILON WITH DASIA */
	[2176+0x12] = -97,	/* U+1F52: GREEK SMALL LETTER UPSILON WITH PSILI AND VARIA */
	[2176+0x13] = 0x1F5B,	/* U+1F53: GREEK SMALL LETTER UPSILON WITH DASIA AND VARIA */
	[2176+0x14] = -98,	/* U+1F54: GREEK SMALL LETTER UPSILON WITH PSILI AND OXIA */
	[2176+0x15] = 0x1F5D,	/* U+1F55: GREEK SMALL LETTER UPSILON WITH DASIA AND OXIA */
	[2176+0x16] = -99,	/* U+1F56: GREEK SMALL LETTER UPSILON WITH PSILI AND PERISPOMENI */
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
	[2240+0x00] = 0x1F88,	/* U+1F80: GREEK SMALL LETTER ALPHA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x01] = 0x1F89,	/* U+1F81: GREEK SMALL LETTER ALPHA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x02] = 0x1F8A,	/* U+1F82: GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x03] = 0x1F8B,	/* U+1F83: GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x04] = 0x1F8C,	/* U+1F84: GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x05] = 0x1F8D,	/* U+1F85: GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x06] = 0x1F8E,	/* U+1F86: GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x07] = 0x1F8F,	/* U+1F87: GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x08] = -110,	/* U+1F88: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI */
	[2240+0x09] = -111,	/* U+1F89: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI */
	[2240+0x0A] = -112,	/* U+1F8A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2240+0x0B] = -113,	/* U+1F8B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2240+0x0C] = -114,	/* U+1F8C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2240+0x0D] = -115,	/* U+1F8D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2240+0x0E] = -116,	/* U+1F8E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x0F] = -117,	/* U+1F8F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x10] = 0x1F98,	/* U+1F90: GREEK SMALL LETTER ETA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x11] = 0x1F99,	/* U+1F91: GREEK SMALL LETTER ETA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x12] = 0x1F9A,	/* U+1F92: GREEK SMALL LETTER ETA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x13] = 0x1F9B,	/* U+1F93: GREEK SMALL LETTER ETA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x14] = 0x1F9C,	/* U+1F94: GREEK SMALL LETTER ETA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x15] = 0x1F9D,	/* U+1F95: GREEK SMALL LETTER ETA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x16] = 0x1F9E,	/* U+1F96: GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x17] = 0x1F9F,	/* U+1F97: GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x18] = -118,	/* U+1F98: GREEK CAPITAL LETTER ETA WITH PSILI AND PROSGEGRAMMENI */
	[2240+0x19] = -119,	/* U+1F99: GREEK CAPITAL LETTER ETA WITH DASIA AND PROSGEGRAMMENI */
	[2240+0x1A] = -120,	/* U+1F9A: GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2240+0x1B] = -121,	/* U+1F9B: GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2240+0x1C] = -122,	/* U+1F9C: GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2240+0x1D] = -123,	/* U+1F9D: GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2240+0x1E] = -124,	/* U+1F9E: GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x1F] = -125,	/* U+1F9F: GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x20] = 0x1FA8,	/* U+1FA0: GREEK SMALL LETTER OMEGA WITH PSILI AND YPOGEGRAMMENI */
	[2240+0x21] = 0x1FA9,	/* U+1FA1: GREEK SMALL LETTER OMEGA WITH DASIA AND YPOGEGRAMMENI */
	[2240+0x22] = 0x1FAA,	/* U+1FA2: GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2240+0x23] = 0x1FAB,	/* U+1FA3: GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2240+0x24] = 0x1FAC,	/* U+1FA4: GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2240+0x25] = 0x1FAD,	/* U+1FA5: GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2240+0x26] = 0x1FAE,	/* U+1FA6: GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x27] = 0x1FAF,	/* U+1FA7: GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x28] = -126,	/* U+1FA8: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI */
	[2240+0x29] = -127,	/* U+1FA9: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI */
	[2240+0x2A] = -128,	/* U+1FAA: GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2240+0x2B] = -129,	/* U+1FAB: GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2240+0x2C] = -130,	/* U+1FAC: GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2240+0x2D] = -131,	/* U+1FAD: GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2240+0x2E] = -132,	/* U+1FAE: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x2F] = -133,	/* U+1FAF: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2240+0x30] = 0x1FB8,	/* U+1FB0: GREEK SMALL LETTER ALPHA WITH VRACHY */
	[2240+0x31] = 0x1FB9,	/* U+1FB1: GREEK SMALL LETTER ALPHA WITH MACRON */
	[2240+0x32] = -137,	/* U+1FB2: GREEK SMALL LETTER ALPHA WITH VARIA AND YPOGEGRAMMENI */
	[2240+0x33] = 0x1FBC,	/* U+1FB3: GREEK SMALL LETTER ALPHA WITH YPOGEGRAMMENI */
	[2240+0x34] = -138,	/* U+1FB4: GREEK SMALL LETTER ALPHA WITH OXIA AND YPOGEGRAMMENI */
	[2240+0x36] = -100,	/* U+1FB6: GREEK SMALL LETTER ALPHA WITH PERISPOMENI */
	[2240+0x37] = -143,	/* U+1FB7: GREEK SMALL LETTER ALPHA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2240+0x3C] = -134,	/* U+1FBC: GREEK CAPITAL LETTER ALPHA WITH PROSGEGRAMMENI */
	[2240+0x3E] = 0x0399,	/* U+1FBE: GREEK PROSGEGRAMMENI */
	[1472+0x3F] = 2304 - 0x80,	/* 341 277 ... */
	[2304+0x02] = -139,	/* U+1FC2: GREEK SMALL LETTER ETA WITH VARIA AND YPOGEGRAMMENI */
	[2304+0x03] = 0x1FCC,	/* U+1FC3: GREEK SMALL LETTER ETA WITH YPOGEGRAMMENI */
	[2304+0x04] = -140,	/* U+1FC4: GREEK SMALL LETTER ETA WITH OXIA AND YPOGEGRAMMENI */
	[2304+0x06] = -101,	/* U+1FC6: GREEK SMALL LETTER ETA WITH PERISPOMENI */
	[2304+0x07] = -144,	/* U+1FC7: GREEK SMALL LETTER ETA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2304+0x0C] = -135,	/* U+1FCC: GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI */
	[2304+0x10] = 0x1FD8,	/* U+1FD0: GREEK SMALL LETTER IOTA WITH VRACHY */
	[2304+0x11] = 0x1FD9,	/* U+1FD1: GREEK SMALL LETTER IOTA WITH MACRON */
	[2304+0x12] = -102,	/* U+1FD2: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND VARIA */
	[2304+0x13] = -88,	/* U+1FD3: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND OXIA */
	[2304+0x16] = -103,	/* U+1FD6: GREEK SMALL LETTER IOTA WITH PERISPOMENI */
	[2304+0x17] = -104,	/* U+1FD7: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND PERISPOMENI */
	[2304+0x20] = 0x1FE8,	/* U+1FE0: GREEK SMALL LETTER UPSILON WITH VRACHY */
	[2304+0x21] = 0x1FE9,	/* U+1FE1: GREEK SMALL LETTER UPSILON WITH MACRON */
	[2304+0x22] = -105,	/* U+1FE2: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND VARIA */
	[2304+0x23] = -89,	/* U+1FE3: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND OXIA */
	[2304+0x24] = -106,	/* U+1FE4: GREEK SMALL LETTER RHO WITH PSILI */
	[2304+0x25] = 0x1FEC,	/* U+1FE5: GREEK SMALL LETTER RHO WITH DASIA */
	[2304+0x26] = -107,	/* U+1FE6: GREEK SMALL LETTER UPSILON WITH PERISPOMENI */
	[2304+0x27] = -108,	/* U+1FE7: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND PERISPOMENI */
	[2304+0x32] = -141,	/* U+1FF2: GREEK SMALL LETTER OMEGA WITH VARIA AND YPOGEGRAMMENI */
	[2304+0x33] = 0x1FFC,	/* U+1FF3: GREEK SMALL LETTER OMEGA WITH YPOGEGRAMMENI */
	[2304+0x34] = -142,	/* U+1FF4: GREEK SMALL LETTER OMEGA WITH OXIA AND YPOGEGRAMMENI */
	[2304+0x36] = -109,	/* U+1FF6: GREEK SMALL LETTER OMEGA WITH PERISPOMENI */
	[2304+0x37] = -145,	/* U+1FF7: GREEK SMALL LETTER OMEGA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2304+0x3C] = -136,	/* U+1FFC: GREEK CAPITAL LETTER OMEGA WITH PROSGEGRAMMENI */
	[0xE2] = 2368 - 0x80,	/* 342 ... */
	[2368+0x05] = 2432 - 0x80,	/* 342 205 ... */
	[2432+0x0E] = 0x2132,	/* U+214E: TURNED SMALL F */
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
	[2368+0x06] = 2496 - 0x80,	/* 342 206 ... */
	[2496+0x04] = 0x2183,	/* U+2184: LATIN SMALL LETTER REVERSED C */
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
	[2688+0x00] = 0x2C10,	/* U+2C40: GLAGOLITIC SMALL LETTER NASHI */
	[2688+0x01] = 0x2C11,	/* U+2C41: GLAGOLITIC SMALL LETTER ONU */
	[2688+0x02] = 0x2C12,	/* U+2C42: GLAGOLITIC SMALL LETTER POKOJI */
	[2688+0x03] = 0x2C13,	/* U+2C43: GLAGOLITIC SMALL LETTER RITSI */
	[2688+0x04] = 0x2C14,	/* U+2C44: GLAGOLITIC SMALL LETTER SLOVO */
	[2688+0x05] = 0x2C15,	/* U+2C45: GLAGOLITIC SMALL LETTER TVRIDO */
	[2688+0x06] = 0x2C16,	/* U+2C46: GLAGOLITIC SMALL LETTER UKU */
	[2688+0x07] = 0x2C17,	/* U+2C47: GLAGOLITIC SMALL LETTER FRITU */
	[2688+0x08] = 0x2C18,	/* U+2C48: GLAGOLITIC SMALL LETTER HERU */
	[2688+0x09] = 0x2C19,	/* U+2C49: GLAGOLITIC SMALL LETTER OTU */
	[2688+0x0A] = 0x2C1A,	/* U+2C4A: GLAGOLITIC SMALL LETTER PE */
	[2688+0x0B] = 0x2C1B,	/* U+2C4B: GLAGOLITIC SMALL LETTER SHTA */
	[2688+0x0C] = 0x2C1C,	/* U+2C4C: GLAGOLITIC SMALL LETTER TSI */
	[2688+0x0D] = 0x2C1D,	/* U+2C4D: GLAGOLITIC SMALL LETTER CHRIVI */
	[2688+0x0E] = 0x2C1E,	/* U+2C4E: GLAGOLITIC SMALL LETTER SHA */
	[2688+0x0F] = 0x2C1F,	/* U+2C4F: GLAGOLITIC SMALL LETTER YERU */
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
	[2688+0x25] = 0x023A,	/* U+2C65: LATIN SMALL LETTER A WITH STROKE */
	[2688+0x26] = 0x023E,	/* U+2C66: LATIN SMALL LETTER T WITH DIAGONAL STROKE */
	[2688+0x28] = 0x2C67,	/* U+2C68: LATIN SMALL LETTER H WITH DESCENDER */
	[2688+0x2A] = 0x2C69,	/* U+2C6A: LATIN SMALL LETTER K WITH DESCENDER */
	[2688+0x2C] = 0x2C6B,	/* U+2C6C: LATIN SMALL LETTER Z WITH DESCENDER */
	[2688+0x33] = 0x2C72,	/* U+2C73: LATIN SMALL LETTER W WITH HOOK */
	[2688+0x36] = 0x2C75,	/* U+2C76: LATIN SMALL LETTER HALF H */
	[2368+0x32] = 2752 - 0x80,	/* 342 262 ... */
	[2752+0x01] = 0x2C80,	/* U+2C81: COPTIC SMALL LETTER ALFA */
	[2752+0x03] = 0x2C82,	/* U+2C83: COPTIC SMALL LETTER VIDA */
	[2752+0x05] = 0x2C84,	/* U+2C85: COPTIC SMALL LETTER GAMMA */
	[2752+0x07] = 0x2C86,	/* U+2C87: COPTIC SMALL LETTER DALDA */
	[2752+0x09] = 0x2C88,	/* U+2C89: COPTIC SMALL LETTER EIE */
	[2752+0x0B] = 0x2C8A,	/* U+2C8B: COPTIC SMALL LETTER SOU */
	[2752+0x0D] = 0x2C8C,	/* U+2C8D: COPTIC SMALL LETTER ZATA */
	[2752+0x0F] = 0x2C8E,	/* U+2C8F: COPTIC SMALL LETTER HATE */
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
	[2816+0x01] = 0x2CC0,	/* U+2CC1: COPTIC SMALL LETTER SAMPI */
	[2816+0x03] = 0x2CC2,	/* U+2CC3: COPTIC SMALL LETTER CROSSED SHEI */
	[2816+0x05] = 0x2CC4,	/* U+2CC5: COPTIC SMALL LETTER OLD COPTIC SHEI */
	[2816+0x07] = 0x2CC6,	/* U+2CC7: COPTIC SMALL LETTER OLD COPTIC ESH */
	[2816+0x09] = 0x2CC8,	/* U+2CC9: COPTIC SMALL LETTER AKHMIMIC KHEI */
	[2816+0x0B] = 0x2CCA,	/* U+2CCB: COPTIC SMALL LETTER DIALECT-P HORI */
	[2816+0x0D] = 0x2CCC,	/* U+2CCD: COPTIC SMALL LETTER OLD COPTIC HORI */
	[2816+0x0F] = 0x2CCE,	/* U+2CCF: COPTIC SMALL LETTER OLD COPTIC HA */
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
	[2880+0x00] = 0x10A0,	/* U+2D00: GEORGIAN SMALL LETTER AN */
	[2880+0x01] = 0x10A1,	/* U+2D01: GEORGIAN SMALL LETTER BAN */
	[2880+0x02] = 0x10A2,	/* U+2D02: GEORGIAN SMALL LETTER GAN */
	[2880+0x03] = 0x10A3,	/* U+2D03: GEORGIAN SMALL LETTER DON */
	[2880+0x04] = 0x10A4,	/* U+2D04: GEORGIAN SMALL LETTER EN */
	[2880+0x05] = 0x10A5,	/* U+2D05: GEORGIAN SMALL LETTER VIN */
	[2880+0x06] = 0x10A6,	/* U+2D06: GEORGIAN SMALL LETTER ZEN */
	[2880+0x07] = 0x10A7,	/* U+2D07: GEORGIAN SMALL LETTER TAN */
	[2880+0x08] = 0x10A8,	/* U+2D08: GEORGIAN SMALL LETTER IN */
	[2880+0x09] = 0x10A9,	/* U+2D09: GEORGIAN SMALL LETTER KAN */
	[2880+0x0A] = 0x10AA,	/* U+2D0A: GEORGIAN SMALL LETTER LAS */
	[2880+0x0B] = 0x10AB,	/* U+2D0B: GEORGIAN SMALL LETTER MAN */
	[2880+0x0C] = 0x10AC,	/* U+2D0C: GEORGIAN SMALL LETTER NAR */
	[2880+0x0D] = 0x10AD,	/* U+2D0D: GEORGIAN SMALL LETTER ON */
	[2880+0x0E] = 0x10AE,	/* U+2D0E: GEORGIAN SMALL LETTER PAR */
	[2880+0x0F] = 0x10AF,	/* U+2D0F: GEORGIAN SMALL LETTER ZHAR */
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
	[3008+0x01] = 0xA640,	/* U+A641: CYRILLIC SMALL LETTER ZEMLYA */
	[3008+0x03] = 0xA642,	/* U+A643: CYRILLIC SMALL LETTER DZELO */
	[3008+0x05] = 0xA644,	/* U+A645: CYRILLIC SMALL LETTER REVERSED DZE */
	[3008+0x07] = 0xA646,	/* U+A647: CYRILLIC SMALL LETTER IOTA */
	[3008+0x09] = 0xA648,	/* U+A649: CYRILLIC SMALL LETTER DJERV */
	[3008+0x0B] = 0xA64A,	/* U+A64B: CYRILLIC SMALL LETTER MONOGRAPH UK */
	[3008+0x0D] = 0xA64C,	/* U+A64D: CYRILLIC SMALL LETTER BROAD OMEGA */
	[3008+0x0F] = 0xA64E,	/* U+A64F: CYRILLIC SMALL LETTER NEUTRAL YER */
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
	[3072+0x01] = 0xA680,	/* U+A681: CYRILLIC SMALL LETTER DWE */
	[3072+0x03] = 0xA682,	/* U+A683: CYRILLIC SMALL LETTER DZWE */
	[3072+0x05] = 0xA684,	/* U+A685: CYRILLIC SMALL LETTER ZHWE */
	[3072+0x07] = 0xA686,	/* U+A687: CYRILLIC SMALL LETTER CCHE */
	[3072+0x09] = 0xA688,	/* U+A689: CYRILLIC SMALL LETTER DZZE */
	[3072+0x0B] = 0xA68A,	/* U+A68B: CYRILLIC SMALL LETTER TE WITH MIDDLE HOOK */
	[3072+0x0D] = 0xA68C,	/* U+A68D: CYRILLIC SMALL LETTER TWE */
	[3072+0x0F] = 0xA68E,	/* U+A68F: CYRILLIC SMALL LETTER TSWE */
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
	[3200+0x01] = 0xA740,	/* U+A741: LATIN SMALL LETTER K WITH STROKE */
	[3200+0x03] = 0xA742,	/* U+A743: LATIN SMALL LETTER K WITH DIAGONAL STROKE */
	[3200+0x05] = 0xA744,	/* U+A745: LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE */
	[3200+0x07] = 0xA746,	/* U+A747: LATIN SMALL LETTER BROKEN L */
	[3200+0x09] = 0xA748,	/* U+A749: LATIN SMALL LETTER L WITH HIGH STROKE */
	[3200+0x0B] = 0xA74A,	/* U+A74B: LATIN SMALL LETTER O WITH LONG STROKE OVERLAY */
	[3200+0x0D] = 0xA74C,	/* U+A74D: LATIN SMALL LETTER O WITH LOOP */
	[3200+0x0F] = 0xA74E,	/* U+A74F: LATIN SMALL LETTER OO */
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
	[3264+0x01] = 0xA780,	/* U+A781: LATIN SMALL LETTER TURNED L */
	[3264+0x03] = 0xA782,	/* U+A783: LATIN SMALL LETTER INSULAR R */
	[3264+0x05] = 0xA784,	/* U+A785: LATIN SMALL LETTER INSULAR S */
	[3264+0x07] = 0xA786,	/* U+A787: LATIN SMALL LETTER INSULAR T */
	[3264+0x0C] = 0xA78B,	/* U+A78C: LATIN SMALL LETTER SALTILLO */
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
	[3328+0x01] = 0xA7C0,	/* U+A7C1: LATIN SMALL LETTER OLD POLISH O */
	[3328+0x03] = 0xA7C2,	/* U+A7C3: LATIN SMALL LETTER ANGLICANA W */
	[3328+0x08] = 0xA7C7,	/* U+A7C8: LATIN SMALL LETTER D WITH SHORT STROKE OVERLAY */
	[3328+0x0A] = 0xA7C9,	/* U+A7CA: LATIN SMALL LETTER S WITH SHORT STROKE OVERLAY */
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
	[3456+0x00] = 0x13B0,	/* U+AB80: CHEROKEE SMALL LETTER HO */
	[3456+0x01] = 0x13B1,	/* U+AB81: CHEROKEE SMALL LETTER HU */
	[3456+0x02] = 0x13B2,	/* U+AB82: CHEROKEE SMALL LETTER HV */
	[3456+0x03] = 0x13B3,	/* U+AB83: CHEROKEE SMALL LETTER LA */
	[3456+0x04] = 0x13B4,	/* U+AB84: CHEROKEE SMALL LETTER LE */
	[3456+0x05] = 0x13B5,	/* U+AB85: CHEROKEE SMALL LETTER LI */
	[3456+0x06] = 0x13B6,	/* U+AB86: CHEROKEE SMALL LETTER LO */
	[3456+0x07] = 0x13B7,	/* U+AB87: CHEROKEE SMALL LETTER LU */
	[3456+0x08] = 0x13B8,	/* U+AB88: CHEROKEE SMALL LETTER LV */
	[3456+0x09] = 0x13B9,	/* U+AB89: CHEROKEE SMALL LETTER MA */
	[3456+0x0A] = 0x13BA,	/* U+AB8A: CHEROKEE SMALL LETTER ME */
	[3456+0x0B] = 0x13BB,	/* U+AB8B: CHEROKEE SMALL LETTER MI */
	[3456+0x0C] = 0x13BC,	/* U+AB8C: CHEROKEE SMALL LETTER MO */
	[3456+0x0D] = 0x13BD,	/* U+AB8D: CHEROKEE SMALL LETTER MU */
	[3456+0x0E] = 0x13BE,	/* U+AB8E: CHEROKEE SMALL LETTER NA */
	[3456+0x0F] = 0x13BF,	/* U+AB8F: CHEROKEE SMALL LETTER HNA */
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
	[3520+0x2C] = 3584 - 0x80,	/* 357 254 ... */
	[3584+0x00] = -75,	/* U+FB00: LATIN SMALL LIGATURE FF */
	[3584+0x01] = -76,	/* U+FB01: LATIN SMALL LIGATURE FI */
	[3584+0x02] = -77,	/* U+FB02: LATIN SMALL LIGATURE FL */
	[3584+0x03] = -78,	/* U+FB03: LATIN SMALL LIGATURE FFI */
	[3584+0x04] = -79,	/* U+FB04: LATIN SMALL LIGATURE FFL */
	[3584+0x05] = -80,	/* U+FB05: LATIN SMALL LIGATURE LONG S T */
	[3584+0x06] = -80,	/* U+FB06: LATIN SMALL LIGATURE ST */
	[3584+0x13] = -82,	/* U+FB13: ARMENIAN SMALL LIGATURE MEN NOW */
	[3584+0x14] = -83,	/* U+FB14: ARMENIAN SMALL LIGATURE MEN ECH */
	[3584+0x15] = -84,	/* U+FB15: ARMENIAN SMALL LIGATURE MEN INI */
	[3584+0x16] = -85,	/* U+FB16: ARMENIAN SMALL LIGATURE VEW NOW */
	[3584+0x17] = -86,	/* U+FB17: ARMENIAN SMALL LIGATURE MEN XEH */
	[3520+0x3D] = 3648 - 0x80,	/* 357 275 ... */
	[3648+0x01] = 0xFF21,	/* U+FF41: FULLWIDTH LATIN SMALL LETTER A */
	[3648+0x02] = 0xFF22,	/* U+FF42: FULLWIDTH LATIN SMALL LETTER B */
	[3648+0x03] = 0xFF23,	/* U+FF43: FULLWIDTH LATIN SMALL LETTER C */
	[3648+0x04] = 0xFF24,	/* U+FF44: FULLWIDTH LATIN SMALL LETTER D */
	[3648+0x05] = 0xFF25,	/* U+FF45: FULLWIDTH LATIN SMALL LETTER E */
	[3648+0x06] = 0xFF26,	/* U+FF46: FULLWIDTH LATIN SMALL LETTER F */
	[3648+0x07] = 0xFF27,	/* U+FF47: FULLWIDTH LATIN SMALL LETTER G */
	[3648+0x08] = 0xFF28,	/* U+FF48: FULLWIDTH LATIN SMALL LETTER H */
	[3648+0x09] = 0xFF29,	/* U+FF49: FULLWIDTH LATIN SMALL LETTER I */
	[3648+0x0A] = 0xFF2A,	/* U+FF4A: FULLWIDTH LATIN SMALL LETTER J */
	[3648+0x0B] = 0xFF2B,	/* U+FF4B: FULLWIDTH LATIN SMALL LETTER K */
	[3648+0x0C] = 0xFF2C,	/* U+FF4C: FULLWIDTH LATIN SMALL LETTER L */
	[3648+0x0D] = 0xFF2D,	/* U+FF4D: FULLWIDTH LATIN SMALL LETTER M */
	[3648+0x0E] = 0xFF2E,	/* U+FF4E: FULLWIDTH LATIN SMALL LETTER N */
	[3648+0x0F] = 0xFF2F,	/* U+FF4F: FULLWIDTH LATIN SMALL LETTER O */
	[3648+0x10] = 0xFF30,	/* U+FF50: FULLWIDTH LATIN SMALL LETTER P */
	[3648+0x11] = 0xFF31,	/* U+FF51: FULLWIDTH LATIN SMALL LETTER Q */
	[3648+0x12] = 0xFF32,	/* U+FF52: FULLWIDTH LATIN SMALL LETTER R */
	[3648+0x13] = 0xFF33,	/* U+FF53: FULLWIDTH LATIN SMALL LETTER S */
	[3648+0x14] = 0xFF34,	/* U+FF54: FULLWIDTH LATIN SMALL LETTER T */
	[3648+0x15] = 0xFF35,	/* U+FF55: FULLWIDTH LATIN SMALL LETTER U */
	[3648+0x16] = 0xFF36,	/* U+FF56: FULLWIDTH LATIN SMALL LETTER V */
	[3648+0x17] = 0xFF37,	/* U+FF57: FULLWIDTH LATIN SMALL LETTER W */
	[3648+0x18] = 0xFF38,	/* U+FF58: FULLWIDTH LATIN SMALL LETTER X */
	[3648+0x19] = 0xFF39,	/* U+FF59: FULLWIDTH LATIN SMALL LETTER Y */
	[3648+0x1A] = 0xFF3A,	/* U+FF5A: FULLWIDTH LATIN SMALL LETTER Z */
	[0xF0] = 3712 - 0x80,	/* 360 ... */
	[3712+0x10] = 3776 - 0x80,	/* 360 220 ... */
	[3776+0x10] = 3840 - 0x80,	/* 360 220 220 ... */
	[3840+0x28] = 0x10400,	/* U+10428: DESERET SMALL LETTER LONG I */
	[3840+0x29] = 0x10401,	/* U+10429: DESERET SMALL LETTER LONG E */
	[3840+0x2A] = 0x10402,	/* U+1042A: DESERET SMALL LETTER LONG A */
	[3840+0x2B] = 0x10403,	/* U+1042B: DESERET SMALL LETTER LONG AH */
	[3840+0x2C] = 0x10404,	/* U+1042C: DESERET SMALL LETTER LONG O */
	[3840+0x2D] = 0x10405,	/* U+1042D: DESERET SMALL LETTER LONG OO */
	[3840+0x2E] = 0x10406,	/* U+1042E: DESERET SMALL LETTER SHORT I */
	[3840+0x2F] = 0x10407,	/* U+1042F: DESERET SMALL LETTER SHORT E */
	[3840+0x30] = 0x10408,	/* U+10430: DESERET SMALL LETTER SHORT A */
	[3840+0x31] = 0x10409,	/* U+10431: DESERET SMALL LETTER SHORT AH */
	[3840+0x32] = 0x1040A,	/* U+10432: DESERET SMALL LETTER SHORT O */
	[3840+0x33] = 0x1040B,	/* U+10433: DESERET SMALL LETTER SHORT OO */
	[3840+0x34] = 0x1040C,	/* U+10434: DESERET SMALL LETTER AY */
	[3840+0x35] = 0x1040D,	/* U+10435: DESERET SMALL LETTER OW */
	[3840+0x36] = 0x1040E,	/* U+10436: DESERET SMALL LETTER WU */
	[3840+0x37] = 0x1040F,	/* U+10437: DESERET SMALL LETTER YEE */
	[3840+0x38] = 0x10410,	/* U+10438: DESERET SMALL LETTER H */
	[3840+0x39] = 0x10411,	/* U+10439: DESERET SMALL LETTER PEE */
	[3840+0x3A] = 0x10412,	/* U+1043A: DESERET SMALL LETTER BEE */
	[3840+0x3B] = 0x10413,	/* U+1043B: DESERET SMALL LETTER TEE */
	[3840+0x3C] = 0x10414,	/* U+1043C: DESERET SMALL LETTER DEE */
	[3840+0x3D] = 0x10415,	/* U+1043D: DESERET SMALL LETTER CHEE */
	[3840+0x3E] = 0x10416,	/* U+1043E: DESERET SMALL LETTER JEE */
	[3840+0x3F] = 0x10417,	/* U+1043F: DESERET SMALL LETTER KAY */
	[3776+0x11] = 3904 - 0x80,	/* 360 220 221 ... */
	[3904+0x00] = 0x10418,	/* U+10440: DESERET SMALL LETTER GAY */
	[3904+0x01] = 0x10419,	/* U+10441: DESERET SMALL LETTER EF */
	[3904+0x02] = 0x1041A,	/* U+10442: DESERET SMALL LETTER VEE */
	[3904+0x03] = 0x1041B,	/* U+10443: DESERET SMALL LETTER ETH */
	[3904+0x04] = 0x1041C,	/* U+10444: DESERET SMALL LETTER THEE */
	[3904+0x05] = 0x1041D,	/* U+10445: DESERET SMALL LETTER ES */
	[3904+0x06] = 0x1041E,	/* U+10446: DESERET SMALL LETTER ZEE */
	[3904+0x07] = 0x1041F,	/* U+10447: DESERET SMALL LETTER ESH */
	[3904+0x08] = 0x10420,	/* U+10448: DESERET SMALL LETTER ZHEE */
	[3904+0x09] = 0x10421,	/* U+10449: DESERET SMALL LETTER ER */
	[3904+0x0A] = 0x10422,	/* U+1044A: DESERET SMALL LETTER EL */
	[3904+0x0B] = 0x10423,	/* U+1044B: DESERET SMALL LETTER EM */
	[3904+0x0C] = 0x10424,	/* U+1044C: DESERET SMALL LETTER EN */
	[3904+0x0D] = 0x10425,	/* U+1044D: DESERET SMALL LETTER ENG */
	[3904+0x0E] = 0x10426,	/* U+1044E: DESERET SMALL LETTER OI */
	[3904+0x0F] = 0x10427,	/* U+1044F: DESERET SMALL LETTER EW */
	[3776+0x13] = 3968 - 0x80,	/* 360 220 223 ... */
	[3968+0x18] = 0x104B0,	/* U+104D8: OSAGE SMALL LETTER A */
	[3968+0x19] = 0x104B1,	/* U+104D9: OSAGE SMALL LETTER AI */
	[3968+0x1A] = 0x104B2,	/* U+104DA: OSAGE SMALL LETTER AIN */
	[3968+0x1B] = 0x104B3,	/* U+104DB: OSAGE SMALL LETTER AH */
	[3968+0x1C] = 0x104B4,	/* U+104DC: OSAGE SMALL LETTER BRA */
	[3968+0x1D] = 0x104B5,	/* U+104DD: OSAGE SMALL LETTER CHA */
	[3968+0x1E] = 0x104B6,	/* U+104DE: OSAGE SMALL LETTER EHCHA */
	[3968+0x1F] = 0x104B7,	/* U+104DF: OSAGE SMALL LETTER E */
	[3968+0x20] = 0x104B8,	/* U+104E0: OSAGE SMALL LETTER EIN */
	[3968+0x21] = 0x104B9,	/* U+104E1: OSAGE SMALL LETTER HA */
	[3968+0x22] = 0x104BA,	/* U+104E2: OSAGE SMALL LETTER HYA */
	[3968+0x23] = 0x104BB,	/* U+104E3: OSAGE SMALL LETTER I */
	[3968+0x24] = 0x104BC,	/* U+104E4: OSAGE SMALL LETTER KA */
	[3968+0x25] = 0x104BD,	/* U+104E5: OSAGE SMALL LETTER EHKA */
	[3968+0x26] = 0x104BE,	/* U+104E6: OSAGE SMALL LETTER KYA */
	[3968+0x27] = 0x104BF,	/* U+104E7: OSAGE SMALL LETTER LA */
	[3968+0x28] = 0x104C0,	/* U+104E8: OSAGE SMALL LETTER MA */
	[3968+0x29] = 0x104C1,	/* U+104E9: OSAGE SMALL LETTER NA */
	[3968+0x2A] = 0x104C2,	/* U+104EA: OSAGE SMALL LETTER O */
	[3968+0x2B] = 0x104C3,	/* U+104EB: OSAGE SMALL LETTER OIN */
	[3968+0x2C] = 0x104C4,	/* U+104EC: OSAGE SMALL LETTER PA */
	[3968+0x2D] = 0x104C5,	/* U+104ED: OSAGE SMALL LETTER EHPA */
	[3968+0x2E] = 0x104C6,	/* U+104EE: OSAGE SMALL LETTER SA */
	[3968+0x2F] = 0x104C7,	/* U+104EF: OSAGE SMALL LETTER SHA */
	[3968+0x30] = 0x104C8,	/* U+104F0: OSAGE SMALL LETTER TA */
	[3968+0x31] = 0x104C9,	/* U+104F1: OSAGE SMALL LETTER EHTA */
	[3968+0x32] = 0x104CA,	/* U+104F2: OSAGE SMALL LETTER TSA */
	[3968+0x33] = 0x104CB,	/* U+104F3: OSAGE SMALL LETTER EHTSA */
	[3968+0x34] = 0x104CC,	/* U+104F4: OSAGE SMALL LETTER TSHA */
	[3968+0x35] = 0x104CD,	/* U+104F5: OSAGE SMALL LETTER DHA */
	[3968+0x36] = 0x104CE,	/* U+104F6: OSAGE SMALL LETTER U */
	[3968+0x37] = 0x104CF,	/* U+104F7: OSAGE SMALL LETTER WA */
	[3968+0x38] = 0x104D0,	/* U+104F8: OSAGE SMALL LETTER KHA */
	[3968+0x39] = 0x104D1,	/* U+104F9: OSAGE SMALL LETTER GHA */
	[3968+0x3A] = 0x104D2,	/* U+104FA: OSAGE SMALL LETTER ZA */
	[3968+0x3B] = 0x104D3,	/* U+104FB: OSAGE SMALL LETTER ZHA */
	[3776+0x16] = 4032 - 0x80,	/* 360 220 226 ... */
	[4032+0x17] = 0x10570,	/* U+10597: VITHKUQI SMALL LETTER A */
	[4032+0x18] = 0x10571,	/* U+10598: VITHKUQI SMALL LETTER BBE */
	[4032+0x19] = 0x10572,	/* U+10599: VITHKUQI SMALL LETTER BE */
	[4032+0x1A] = 0x10573,	/* U+1059A: VITHKUQI SMALL LETTER CE */
	[4032+0x1B] = 0x10574,	/* U+1059B: VITHKUQI SMALL LETTER CHE */
	[4032+0x1C] = 0x10575,	/* U+1059C: VITHKUQI SMALL LETTER DE */
	[4032+0x1D] = 0x10576,	/* U+1059D: VITHKUQI SMALL LETTER DHE */
	[4032+0x1E] = 0x10577,	/* U+1059E: VITHKUQI SMALL LETTER EI */
	[4032+0x1F] = 0x10578,	/* U+1059F: VITHKUQI SMALL LETTER E */
	[4032+0x20] = 0x10579,	/* U+105A0: VITHKUQI SMALL LETTER FE */
	[4032+0x21] = 0x1057A,	/* U+105A1: VITHKUQI SMALL LETTER GA */
	[4032+0x23] = 0x1057C,	/* U+105A3: VITHKUQI SMALL LETTER HA */
	[4032+0x24] = 0x1057D,	/* U+105A4: VITHKUQI SMALL LETTER HHA */
	[4032+0x25] = 0x1057E,	/* U+105A5: VITHKUQI SMALL LETTER I */
	[4032+0x26] = 0x1057F,	/* U+105A6: VITHKUQI SMALL LETTER IJE */
	[4032+0x27] = 0x10580,	/* U+105A7: VITHKUQI SMALL LETTER JE */
	[4032+0x28] = 0x10581,	/* U+105A8: VITHKUQI SMALL LETTER KA */
	[4032+0x29] = 0x10582,	/* U+105A9: VITHKUQI SMALL LETTER LA */
	[4032+0x2A] = 0x10583,	/* U+105AA: VITHKUQI SMALL LETTER LLA */
	[4032+0x2B] = 0x10584,	/* U+105AB: VITHKUQI SMALL LETTER ME */
	[4032+0x2C] = 0x10585,	/* U+105AC: VITHKUQI SMALL LETTER NE */
	[4032+0x2D] = 0x10586,	/* U+105AD: VITHKUQI SMALL LETTER NJE */
	[4032+0x2E] = 0x10587,	/* U+105AE: VITHKUQI SMALL LETTER O */
	[4032+0x2F] = 0x10588,	/* U+105AF: VITHKUQI SMALL LETTER PE */
	[4032+0x30] = 0x10589,	/* U+105B0: VITHKUQI SMALL LETTER QA */
	[4032+0x31] = 0x1058A,	/* U+105B1: VITHKUQI SMALL LETTER RE */
	[4032+0x33] = 0x1058C,	/* U+105B3: VITHKUQI SMALL LETTER SE */
	[4032+0x34] = 0x1058D,	/* U+105B4: VITHKUQI SMALL LETTER SHE */
	[4032+0x35] = 0x1058E,	/* U+105B5: VITHKUQI SMALL LETTER TE */
	[4032+0x36] = 0x1058F,	/* U+105B6: VITHKUQI SMALL LETTER THE */
	[4032+0x37] = 0x10590,	/* U+105B7: VITHKUQI SMALL LETTER U */
	[4032+0x38] = 0x10591,	/* U+105B8: VITHKUQI SMALL LETTER VE */
	[4032+0x39] = 0x10592,	/* U+105B9: VITHKUQI SMALL LETTER XE */
	[4032+0x3B] = 0x10594,	/* U+105BB: VITHKUQI SMALL LETTER Y */
	[4032+0x3C] = 0x10595,	/* U+105BC: VITHKUQI SMALL LETTER ZE */
	[3776+0x33] = 4096 - 0x80,	/* 360 220 263 ... */
	[4096+0x00] = 0x10C80,	/* U+10CC0: OLD HUNGARIAN SMALL LETTER A */
	[4096+0x01] = 0x10C81,	/* U+10CC1: OLD HUNGARIAN SMALL LETTER AA */
	[4096+0x02] = 0x10C82,	/* U+10CC2: OLD HUNGARIAN SMALL LETTER EB */
	[4096+0x03] = 0x10C83,	/* U+10CC3: OLD HUNGARIAN SMALL LETTER AMB */
	[4096+0x04] = 0x10C84,	/* U+10CC4: OLD HUNGARIAN SMALL LETTER EC */
	[4096+0x05] = 0x10C85,	/* U+10CC5: OLD HUNGARIAN SMALL LETTER ENC */
	[4096+0x06] = 0x10C86,	/* U+10CC6: OLD HUNGARIAN SMALL LETTER ECS */
	[4096+0x07] = 0x10C87,	/* U+10CC7: OLD HUNGARIAN SMALL LETTER ED */
	[4096+0x08] = 0x10C88,	/* U+10CC8: OLD HUNGARIAN SMALL LETTER AND */
	[4096+0x09] = 0x10C89,	/* U+10CC9: OLD HUNGARIAN SMALL LETTER E */
	[4096+0x0A] = 0x10C8A,	/* U+10CCA: OLD HUNGARIAN SMALL LETTER CLOSE E */
	[4096+0x0B] = 0x10C8B,	/* U+10CCB: OLD HUNGARIAN SMALL LETTER EE */
	[4096+0x0C] = 0x10C8C,	/* U+10CCC: OLD HUNGARIAN SMALL LETTER EF */
	[4096+0x0D] = 0x10C8D,	/* U+10CCD: OLD HUNGARIAN SMALL LETTER EG */
	[4096+0x0E] = 0x10C8E,	/* U+10CCE: OLD HUNGARIAN SMALL LETTER EGY */
	[4096+0x0F] = 0x10C8F,	/* U+10CCF: OLD HUNGARIAN SMALL LETTER EH */
	[4096+0x10] = 0x10C90,	/* U+10CD0: OLD HUNGARIAN SMALL LETTER I */
	[4096+0x11] = 0x10C91,	/* U+10CD1: OLD HUNGARIAN SMALL LETTER II */
	[4096+0x12] = 0x10C92,	/* U+10CD2: OLD HUNGARIAN SMALL LETTER EJ */
	[4096+0x13] = 0x10C93,	/* U+10CD3: OLD HUNGARIAN SMALL LETTER EK */
	[4096+0x14] = 0x10C94,	/* U+10CD4: OLD HUNGARIAN SMALL LETTER AK */
	[4096+0x15] = 0x10C95,	/* U+10CD5: OLD HUNGARIAN SMALL LETTER UNK */
	[4096+0x16] = 0x10C96,	/* U+10CD6: OLD HUNGARIAN SMALL LETTER EL */
	[4096+0x17] = 0x10C97,	/* U+10CD7: OLD HUNGARIAN SMALL LETTER ELY */
	[4096+0x18] = 0x10C98,	/* U+10CD8: OLD HUNGARIAN SMALL LETTER EM */
	[4096+0x19] = 0x10C99,	/* U+10CD9: OLD HUNGARIAN SMALL LETTER EN */
	[4096+0x1A] = 0x10C9A,	/* U+10CDA: OLD HUNGARIAN SMALL LETTER ENY */
	[4096+0x1B] = 0x10C9B,	/* U+10CDB: OLD HUNGARIAN SMALL LETTER O */
	[4096+0x1C] = 0x10C9C,	/* U+10CDC: OLD HUNGARIAN SMALL LETTER OO */
	[4096+0x1D] = 0x10C9D,	/* U+10CDD: OLD HUNGARIAN SMALL LETTER NIKOLSBURG OE */
	[4096+0x1E] = 0x10C9E,	/* U+10CDE: OLD HUNGARIAN SMALL LETTER RUDIMENTA OE */
	[4096+0x1F] = 0x10C9F,	/* U+10CDF: OLD HUNGARIAN SMALL LETTER OEE */
	[4096+0x20] = 0x10CA0,	/* U+10CE0: OLD HUNGARIAN SMALL LETTER EP */
	[4096+0x21] = 0x10CA1,	/* U+10CE1: OLD HUNGARIAN SMALL LETTER EMP */
	[4096+0x22] = 0x10CA2,	/* U+10CE2: OLD HUNGARIAN SMALL LETTER ER */
	[4096+0x23] = 0x10CA3,	/* U+10CE3: OLD HUNGARIAN SMALL LETTER SHORT ER */
	[4096+0x24] = 0x10CA4,	/* U+10CE4: OLD HUNGARIAN SMALL LETTER ES */
	[4096+0x25] = 0x10CA5,	/* U+10CE5: OLD HUNGARIAN SMALL LETTER ESZ */
	[4096+0x26] = 0x10CA6,	/* U+10CE6: OLD HUNGARIAN SMALL LETTER ET */
	[4096+0x27] = 0x10CA7,	/* U+10CE7: OLD HUNGARIAN SMALL LETTER ENT */
	[4096+0x28] = 0x10CA8,	/* U+10CE8: OLD HUNGARIAN SMALL LETTER ETY */
	[4096+0x29] = 0x10CA9,	/* U+10CE9: OLD HUNGARIAN SMALL LETTER ECH */
	[4096+0x2A] = 0x10CAA,	/* U+10CEA: OLD HUNGARIAN SMALL LETTER U */
	[4096+0x2B] = 0x10CAB,	/* U+10CEB: OLD HUNGARIAN SMALL LETTER UU */
	[4096+0x2C] = 0x10CAC,	/* U+10CEC: OLD HUNGARIAN SMALL LETTER NIKOLSBURG UE */
	[4096+0x2D] = 0x10CAD,	/* U+10CED: OLD HUNGARIAN SMALL LETTER RUDIMENTA UE */
	[4096+0x2E] = 0x10CAE,	/* U+10CEE: OLD HUNGARIAN SMALL LETTER EV */
	[4096+0x2F] = 0x10CAF,	/* U+10CEF: OLD HUNGARIAN SMALL LETTER EZ */
	[4096+0x30] = 0x10CB0,	/* U+10CF0: OLD HUNGARIAN SMALL LETTER EZS */
	[4096+0x31] = 0x10CB1,	/* U+10CF1: OLD HUNGARIAN SMALL LETTER ENT-SHAPED SIGN */
	[4096+0x32] = 0x10CB2,	/* U+10CF2: OLD HUNGARIAN SMALL LETTER US */
	[3712+0x11] = 4160 - 0x80,	/* 360 221 ... */
	[4160+0x23] = 4224 - 0x80,	/* 360 221 243 ... */
	[4224+0x00] = 0x118A0,	/* U+118C0: WARANG CITI SMALL LETTER NGAA */
	[4224+0x01] = 0x118A1,	/* U+118C1: WARANG CITI SMALL LETTER A */
	[4224+0x02] = 0x118A2,	/* U+118C2: WARANG CITI SMALL LETTER WI */
	[4224+0x03] = 0x118A3,	/* U+118C3: WARANG CITI SMALL LETTER YU */
	[4224+0x04] = 0x118A4,	/* U+118C4: WARANG CITI SMALL LETTER YA */
	[4224+0x05] = 0x118A5,	/* U+118C5: WARANG CITI SMALL LETTER YO */
	[4224+0x06] = 0x118A6,	/* U+118C6: WARANG CITI SMALL LETTER II */
	[4224+0x07] = 0x118A7,	/* U+118C7: WARANG CITI SMALL LETTER UU */
	[4224+0x08] = 0x118A8,	/* U+118C8: WARANG CITI SMALL LETTER E */
	[4224+0x09] = 0x118A9,	/* U+118C9: WARANG CITI SMALL LETTER O */
	[4224+0x0A] = 0x118AA,	/* U+118CA: WARANG CITI SMALL LETTER ANG */
	[4224+0x0B] = 0x118AB,	/* U+118CB: WARANG CITI SMALL LETTER GA */
	[4224+0x0C] = 0x118AC,	/* U+118CC: WARANG CITI SMALL LETTER KO */
	[4224+0x0D] = 0x118AD,	/* U+118CD: WARANG CITI SMALL LETTER ENY */
	[4224+0x0E] = 0x118AE,	/* U+118CE: WARANG CITI SMALL LETTER YUJ */
	[4224+0x0F] = 0x118AF,	/* U+118CF: WARANG CITI SMALL LETTER UC */
	[4224+0x10] = 0x118B0,	/* U+118D0: WARANG CITI SMALL LETTER ENN */
	[4224+0x11] = 0x118B1,	/* U+118D1: WARANG CITI SMALL LETTER ODD */
	[4224+0x12] = 0x118B2,	/* U+118D2: WARANG CITI SMALL LETTER TTE */
	[4224+0x13] = 0x118B3,	/* U+118D3: WARANG CITI SMALL LETTER NUNG */
	[4224+0x14] = 0x118B4,	/* U+118D4: WARANG CITI SMALL LETTER DA */
	[4224+0x15] = 0x118B5,	/* U+118D5: WARANG CITI SMALL LETTER AT */
	[4224+0x16] = 0x118B6,	/* U+118D6: WARANG CITI SMALL LETTER AM */
	[4224+0x17] = 0x118B7,	/* U+118D7: WARANG CITI SMALL LETTER BU */
	[4224+0x18] = 0x118B8,	/* U+118D8: WARANG CITI SMALL LETTER PU */
	[4224+0x19] = 0x118B9,	/* U+118D9: WARANG CITI SMALL LETTER HIYO */
	[4224+0x1A] = 0x118BA,	/* U+118DA: WARANG CITI SMALL LETTER HOLO */
	[4224+0x1B] = 0x118BB,	/* U+118DB: WARANG CITI SMALL LETTER HORR */
	[4224+0x1C] = 0x118BC,	/* U+118DC: WARANG CITI SMALL LETTER HAR */
	[4224+0x1D] = 0x118BD,	/* U+118DD: WARANG CITI SMALL LETTER SSUU */
	[4224+0x1E] = 0x118BE,	/* U+118DE: WARANG CITI SMALL LETTER SII */
	[4224+0x1F] = 0x118BF,	/* U+118DF: WARANG CITI SMALL LETTER VIYO */
	[3712+0x16] = 4288 - 0x80,	/* 360 226 ... */
	[4288+0x39] = 4352 - 0x80,	/* 360 226 271 ... */
	[4352+0x20] = 0x16E40,	/* U+16E60: MEDEFAIDRIN SMALL LETTER M */
	[4352+0x21] = 0x16E41,	/* U+16E61: MEDEFAIDRIN SMALL LETTER S */
	[4352+0x22] = 0x16E42,	/* U+16E62: MEDEFAIDRIN SMALL LETTER V */
	[4352+0x23] = 0x16E43,	/* U+16E63: MEDEFAIDRIN SMALL LETTER W */
	[4352+0x24] = 0x16E44,	/* U+16E64: MEDEFAIDRIN SMALL LETTER ATIU */
	[4352+0x25] = 0x16E45,	/* U+16E65: MEDEFAIDRIN SMALL LETTER Z */
	[4352+0x26] = 0x16E46,	/* U+16E66: MEDEFAIDRIN SMALL LETTER KP */
	[4352+0x27] = 0x16E47,	/* U+16E67: MEDEFAIDRIN SMALL LETTER P */
	[4352+0x28] = 0x16E48,	/* U+16E68: MEDEFAIDRIN SMALL LETTER T */
	[4352+0x29] = 0x16E49,	/* U+16E69: MEDEFAIDRIN SMALL LETTER G */
	[4352+0x2A] = 0x16E4A,	/* U+16E6A: MEDEFAIDRIN SMALL LETTER F */
	[4352+0x2B] = 0x16E4B,	/* U+16E6B: MEDEFAIDRIN SMALL LETTER I */
	[4352+0x2C] = 0x16E4C,	/* U+16E6C: MEDEFAIDRIN SMALL LETTER K */
	[4352+0x2D] = 0x16E4D,	/* U+16E6D: MEDEFAIDRIN SMALL LETTER A */
	[4352+0x2E] = 0x16E4E,	/* U+16E6E: MEDEFAIDRIN SMALL LETTER J */
	[4352+0x2F] = 0x16E4F,	/* U+16E6F: MEDEFAIDRIN SMALL LETTER E */
	[4352+0x30] = 0x16E50,	/* U+16E70: MEDEFAIDRIN SMALL LETTER B */
	[4352+0x31] = 0x16E51,	/* U+16E71: MEDEFAIDRIN SMALL LETTER C */
	[4352+0x32] = 0x16E52,	/* U+16E72: MEDEFAIDRIN SMALL LETTER U */
	[4352+0x33] = 0x16E53,	/* U+16E73: MEDEFAIDRIN SMALL LETTER YU */
	[4352+0x34] = 0x16E54,	/* U+16E74: MEDEFAIDRIN SMALL LETTER L */
	[4352+0x35] = 0x16E55,	/* U+16E75: MEDEFAIDRIN SMALL LETTER Q */
	[4352+0x36] = 0x16E56,	/* U+16E76: MEDEFAIDRIN SMALL LETTER HP */
	[4352+0x37] = 0x16E57,	/* U+16E77: MEDEFAIDRIN SMALL LETTER NY */
	[4352+0x38] = 0x16E58,	/* U+16E78: MEDEFAIDRIN SMALL LETTER X */
	[4352+0x39] = 0x16E59,	/* U+16E79: MEDEFAIDRIN SMALL LETTER D */
	[4352+0x3A] = 0x16E5A,	/* U+16E7A: MEDEFAIDRIN SMALL LETTER OE */
	[4352+0x3B] = 0x16E5B,	/* U+16E7B: MEDEFAIDRIN SMALL LETTER N */
	[4352+0x3C] = 0x16E5C,	/* U+16E7C: MEDEFAIDRIN SMALL LETTER R */
	[4352+0x3D] = 0x16E5D,	/* U+16E7D: MEDEFAIDRIN SMALL LETTER O */
	[4352+0x3E] = 0x16E5E,	/* U+16E7E: MEDEFAIDRIN SMALL LETTER AI */
	[4352+0x3F] = 0x16E5F,	/* U+16E7F: MEDEFAIDRIN SMALL LETTER Y */
	[3712+0x1E] = 4416 - 0x80,	/* 360 236 ... */
	[4416+0x24] = 4480 - 0x80,	/* 360 236 244 ... */
	[4480+0x22] = 0x1E900,	/* U+1E922: ADLAM SMALL LETTER ALIF */
	[4480+0x23] = 0x1E901,	/* U+1E923: ADLAM SMALL LETTER DAALI */
	[4480+0x24] = 0x1E902,	/* U+1E924: ADLAM SMALL LETTER LAAM */
	[4480+0x25] = 0x1E903,	/* U+1E925: ADLAM SMALL LETTER MIIM */
	[4480+0x26] = 0x1E904,	/* U+1E926: ADLAM SMALL LETTER BA */
	[4480+0x27] = 0x1E905,	/* U+1E927: ADLAM SMALL LETTER SINNYIIYHE */
	[4480+0x28] = 0x1E906,	/* U+1E928: ADLAM SMALL LETTER PE */
	[4480+0x29] = 0x1E907,	/* U+1E929: ADLAM SMALL LETTER BHE */
	[4480+0x2A] = 0x1E908,	/* U+1E92A: ADLAM SMALL LETTER RA */
	[4480+0x2B] = 0x1E909,	/* U+1E92B: ADLAM SMALL LETTER E */
	[4480+0x2C] = 0x1E90A,	/* U+1E92C: ADLAM SMALL LETTER FA */
	[4480+0x2D] = 0x1E90B,	/* U+1E92D: ADLAM SMALL LETTER I */
	[4480+0x2E] = 0x1E90C,	/* U+1E92E: ADLAM SMALL LETTER O */
	[4480+0x2F] = 0x1E90D,	/* U+1E92F: ADLAM SMALL LETTER DHA */
	[4480+0x30] = 0x1E90E,	/* U+1E930: ADLAM SMALL LETTER YHE */
	[4480+0x31] = 0x1E90F,	/* U+1E931: ADLAM SMALL LETTER WAW */
	[4480+0x32] = 0x1E910,	/* U+1E932: ADLAM SMALL LETTER NUN */
	[4480+0x33] = 0x1E911,	/* U+1E933: ADLAM SMALL LETTER KAF */
	[4480+0x34] = 0x1E912,	/* U+1E934: ADLAM SMALL LETTER YA */
	[4480+0x35] = 0x1E913,	/* U+1E935: ADLAM SMALL LETTER U */
	[4480+0x36] = 0x1E914,	/* U+1E936: ADLAM SMALL LETTER JIIM */
	[4480+0x37] = 0x1E915,	/* U+1E937: ADLAM SMALL LETTER CHI */
	[4480+0x38] = 0x1E916,	/* U+1E938: ADLAM SMALL LETTER HA */
	[4480+0x39] = 0x1E917,	/* U+1E939: ADLAM SMALL LETTER QAAF */
	[4480+0x3A] = 0x1E918,	/* U+1E93A: ADLAM SMALL LETTER GA */
	[4480+0x3B] = 0x1E919,	/* U+1E93B: ADLAM SMALL LETTER NYA */
	[4480+0x3C] = 0x1E91A,	/* U+1E93C: ADLAM SMALL LETTER TU */
	[4480+0x3D] = 0x1E91B,	/* U+1E93D: ADLAM SMALL LETTER NHA */
	[4480+0x3E] = 0x1E91C,	/* U+1E93E: ADLAM SMALL LETTER VA */
	[4480+0x3F] = 0x1E91D,	/* U+1E93F: ADLAM SMALL LETTER KHA */
	[4416+0x25] = 4544 - 0x80,	/* 360 236 245 ... */
	[4544+0x00] = 0x1E91E,	/* U+1E940: ADLAM SMALL LETTER GBE */
	[4544+0x01] = 0x1E91F,	/* U+1E941: ADLAM SMALL LETTER ZAL */
	[4544+0x02] = 0x1E920,	/* U+1E942: ADLAM SMALL LETTER KPO */
	[4544+0x03] = 0x1E921,	/* U+1E943: ADLAM SMALL LETTER SHA */
};
static const int casefold[4544] = {
	[0x00] = 0x0000,	/* U+0000: <control> */
	[0x01] = 0x0001,	/* U+0001: <control> */
	[0x02] = 0x0002,	/* U+0002: <control> */
	[0x03] = 0x0003,	/* U+0003: <control> */
	[0x04] = 0x0004,	/* U+0004: <control> */
	[0x05] = 0x0005,	/* U+0005: <control> */
	[0x06] = 0x0006,	/* U+0006: <control> */
	[0x07] = 0x0007,	/* U+0007: <control> */
	[0x08] = 0x0008,	/* U+0008: <control> */
	[0x09] = 0x0009,	/* U+0009: <control> */
	[0x0A] = 0x000A,	/* U+000A: <control> */
	[0x0B] = 0x000B,	/* U+000B: <control> */
	[0x0C] = 0x000C,	/* U+000C: <control> */
	[0x0D] = 0x000D,	/* U+000D: <control> */
	[0x0E] = 0x000E,	/* U+000E: <control> */
	[0x0F] = 0x000F,	/* U+000F: <control> */
	[0x10] = 0x0010,	/* U+0010: <control> */
	[0x11] = 0x0011,	/* U+0011: <control> */
	[0x12] = 0x0012,	/* U+0012: <control> */
	[0x13] = 0x0013,	/* U+0013: <control> */
	[0x14] = 0x0014,	/* U+0014: <control> */
	[0x15] = 0x0015,	/* U+0015: <control> */
	[0x16] = 0x0016,	/* U+0016: <control> */
	[0x17] = 0x0017,	/* U+0017: <control> */
	[0x18] = 0x0018,	/* U+0018: <control> */
	[0x19] = 0x0019,	/* U+0019: <control> */
	[0x1A] = 0x001A,	/* U+001A: <control> */
	[0x1B] = 0x001B,	/* U+001B: <control> */
	[0x1C] = 0x001C,	/* U+001C: <control> */
	[0x1D] = 0x001D,	/* U+001D: <control> */
	[0x1E] = 0x001E,	/* U+001E: <control> */
	[0x1F] = 0x001F,	/* U+001F: <control> */
	[0x20] = 0x0020,	/* U+0020: SPACE */
	[0x21] = 0x0021,	/* U+0021: EXCLAMATION MARK */
	[0x22] = 0x0022,	/* U+0022: QUOTATION MARK */
	[0x23] = 0x0023,	/* U+0023: NUMBER SIGN */
	[0x24] = 0x0024,	/* U+0024: DOLLAR SIGN */
	[0x25] = 0x0025,	/* U+0025: PERCENT SIGN */
	[0x26] = 0x0026,	/* U+0026: AMPERSAND */
	[0x27] = 0x0027,	/* U+0027: APOSTROPHE */
	[0x28] = 0x0028,	/* U+0028: LEFT PARENTHESIS */
	[0x29] = 0x0029,	/* U+0029: RIGHT PARENTHESIS */
	[0x2A] = 0x002A,	/* U+002A: ASTERISK */
	[0x2B] = 0x002B,	/* U+002B: PLUS SIGN */
	[0x2C] = 0x002C,	/* U+002C: COMMA */
	[0x2D] = 0x002D,	/* U+002D: HYPHEN-MINUS */
	[0x2E] = 0x002E,	/* U+002E: FULL STOP */
	[0x2F] = 0x002F,	/* U+002F: SOLIDUS */
	[0x30] = 0x0030,	/* U+0030: DIGIT ZERO */
	[0x31] = 0x0031,	/* U+0031: DIGIT ONE */
	[0x32] = 0x0032,	/* U+0032: DIGIT TWO */
	[0x33] = 0x0033,	/* U+0033: DIGIT THREE */
	[0x34] = 0x0034,	/* U+0034: DIGIT FOUR */
	[0x35] = 0x0035,	/* U+0035: DIGIT FIVE */
	[0x36] = 0x0036,	/* U+0036: DIGIT SIX */
	[0x37] = 0x0037,	/* U+0037: DIGIT SEVEN */
	[0x38] = 0x0038,	/* U+0038: DIGIT EIGHT */
	[0x39] = 0x0039,	/* U+0039: DIGIT NINE */
	[0x3A] = 0x003A,	/* U+003A: COLON */
	[0x3B] = 0x003B,	/* U+003B: SEMICOLON */
	[0x3C] = 0x003C,	/* U+003C: LESS-THAN SIGN */
	[0x3D] = 0x003D,	/* U+003D: EQUALS SIGN */
	[0x3E] = 0x003E,	/* U+003E: GREATER-THAN SIGN */
	[0x3F] = 0x003F,	/* U+003F: QUESTION MARK */
	[0x40] = 0x0040,	/* U+0040: COMMERCIAL AT */
	[0x41] = 0x0061,	/* U+0041: LATIN CAPITAL LETTER A */
	[0x42] = 0x0062,	/* U+0042: LATIN CAPITAL LETTER B */
	[0x43] = 0x0063,	/* U+0043: LATIN CAPITAL LETTER C */
	[0x44] = 0x0064,	/* U+0044: LATIN CAPITAL LETTER D */
	[0x45] = 0x0065,	/* U+0045: LATIN CAPITAL LETTER E */
	[0x46] = 0x0066,	/* U+0046: LATIN CAPITAL LETTER F */
	[0x47] = 0x0067,	/* U+0047: LATIN CAPITAL LETTER G */
	[0x48] = 0x0068,	/* U+0048: LATIN CAPITAL LETTER H */
	[0x49] = 0x0069,	/* U+0049: LATIN CAPITAL LETTER I */
	[0x4A] = 0x006A,	/* U+004A: LATIN CAPITAL LETTER J */
	[0x4B] = 0x006B,	/* U+004B: LATIN CAPITAL LETTER K */
	[0x4C] = 0x006C,	/* U+004C: LATIN CAPITAL LETTER L */
	[0x4D] = 0x006D,	/* U+004D: LATIN CAPITAL LETTER M */
	[0x4E] = 0x006E,	/* U+004E: LATIN CAPITAL LETTER N */
	[0x4F] = 0x006F,	/* U+004F: LATIN CAPITAL LETTER O */
	[0x50] = 0x0070,	/* U+0050: LATIN CAPITAL LETTER P */
	[0x51] = 0x0071,	/* U+0051: LATIN CAPITAL LETTER Q */
	[0x52] = 0x0072,	/* U+0052: LATIN CAPITAL LETTER R */
	[0x53] = 0x0073,	/* U+0053: LATIN CAPITAL LETTER S */
	[0x54] = 0x0074,	/* U+0054: LATIN CAPITAL LETTER T */
	[0x55] = 0x0075,	/* U+0055: LATIN CAPITAL LETTER U */
	[0x56] = 0x0076,	/* U+0056: LATIN CAPITAL LETTER V */
	[0x57] = 0x0077,	/* U+0057: LATIN CAPITAL LETTER W */
	[0x58] = 0x0078,	/* U+0058: LATIN CAPITAL LETTER X */
	[0x59] = 0x0079,	/* U+0059: LATIN CAPITAL LETTER Y */
	[0x5A] = 0x007A,	/* U+005A: LATIN CAPITAL LETTER Z */
	[0x5B] = 0x005B,	/* U+005B: LEFT SQUARE BRACKET */
	[0x5C] = 0x005C,	/* U+005C: REVERSE SOLIDUS */
	[0x5D] = 0x005D,	/* U+005D: RIGHT SQUARE BRACKET */
	[0x5E] = 0x005E,	/* U+005E: CIRCUMFLEX ACCENT */
	[0x5F] = 0x005F,	/* U+005F: LOW LINE */
	[0x60] = 0x0060,	/* U+0060: GRAVE ACCENT */
	[0x61] = 0x0061,	/* U+0061: LATIN SMALL LETTER A */
	[0x62] = 0x0062,	/* U+0062: LATIN SMALL LETTER B */
	[0x63] = 0x0063,	/* U+0063: LATIN SMALL LETTER C */
	[0x64] = 0x0064,	/* U+0064: LATIN SMALL LETTER D */
	[0x65] = 0x0065,	/* U+0065: LATIN SMALL LETTER E */
	[0x66] = 0x0066,	/* U+0066: LATIN SMALL LETTER F */
	[0x67] = 0x0067,	/* U+0067: LATIN SMALL LETTER G */
	[0x68] = 0x0068,	/* U+0068: LATIN SMALL LETTER H */
	[0x69] = 0x0069,	/* U+0069: LATIN SMALL LETTER I */
	[0x6A] = 0x006A,	/* U+006A: LATIN SMALL LETTER J */
	[0x6B] = 0x006B,	/* U+006B: LATIN SMALL LETTER K */
	[0x6C] = 0x006C,	/* U+006C: LATIN SMALL LETTER L */
	[0x6D] = 0x006D,	/* U+006D: LATIN SMALL LETTER M */
	[0x6E] = 0x006E,	/* U+006E: LATIN SMALL LETTER N */
	[0x6F] = 0x006F,	/* U+006F: LATIN SMALL LETTER O */
	[0x70] = 0x0070,	/* U+0070: LATIN SMALL LETTER P */
	[0x71] = 0x0071,	/* U+0071: LATIN SMALL LETTER Q */
	[0x72] = 0x0072,	/* U+0072: LATIN SMALL LETTER R */
	[0x73] = 0x0073,	/* U+0073: LATIN SMALL LETTER S */
	[0x74] = 0x0074,	/* U+0074: LATIN SMALL LETTER T */
	[0x75] = 0x0075,	/* U+0075: LATIN SMALL LETTER U */
	[0x76] = 0x0076,	/* U+0076: LATIN SMALL LETTER V */
	[0x77] = 0x0077,	/* U+0077: LATIN SMALL LETTER W */
	[0x78] = 0x0078,	/* U+0078: LATIN SMALL LETTER X */
	[0x79] = 0x0079,	/* U+0079: LATIN SMALL LETTER Y */
	[0x7A] = 0x007A,	/* U+007A: LATIN SMALL LETTER Z */
	[0x7B] = 0x007B,	/* U+007B: LEFT CURLY BRACKET */
	[0x7C] = 0x007C,	/* U+007C: VERTICAL LINE */
	[0x7D] = 0x007D,	/* U+007D: RIGHT CURLY BRACKET */
	[0x7E] = 0x007E,	/* U+007E: TILDE */
	[0x7F] = 0x007F,	/* U+007F: <control> */
	[0xC2] = 256 - 0x80,	/* 302 ... */
	[256+0x35] = 0x03BC,	/* U+00B5: MICRO SIGN */
	[0xC3] = 320 - 0x80,	/* 303 ... */
	[320+0x00] = 0x00E0,	/* U+00C0: LATIN CAPITAL LETTER A WITH GRAVE */
	[320+0x01] = 0x00E1,	/* U+00C1: LATIN CAPITAL LETTER A WITH ACUTE */
	[320+0x02] = 0x00E2,	/* U+00C2: LATIN CAPITAL LETTER A WITH CIRCUMFLEX */
	[320+0x03] = 0x00E3,	/* U+00C3: LATIN CAPITAL LETTER A WITH TILDE */
	[320+0x04] = 0x00E4,	/* U+00C4: LATIN CAPITAL LETTER A WITH DIAERESIS */
	[320+0x05] = 0x00E5,	/* U+00C5: LATIN CAPITAL LETTER A WITH RING ABOVE */
	[320+0x06] = 0x00E6,	/* U+00C6: LATIN CAPITAL LETTER AE */
	[320+0x07] = 0x00E7,	/* U+00C7: LATIN CAPITAL LETTER C WITH CEDILLA */
	[320+0x08] = 0x00E8,	/* U+00C8: LATIN CAPITAL LETTER E WITH GRAVE */
	[320+0x09] = 0x00E9,	/* U+00C9: LATIN CAPITAL LETTER E WITH ACUTE */
	[320+0x0A] = 0x00EA,	/* U+00CA: LATIN CAPITAL LETTER E WITH CIRCUMFLEX */
	[320+0x0B] = 0x00EB,	/* U+00CB: LATIN CAPITAL LETTER E WITH DIAERESIS */
	[320+0x0C] = 0x00EC,	/* U+00CC: LATIN CAPITAL LETTER I WITH GRAVE */
	[320+0x0D] = 0x00ED,	/* U+00CD: LATIN CAPITAL LETTER I WITH ACUTE */
	[320+0x0E] = 0x00EE,	/* U+00CE: LATIN CAPITAL LETTER I WITH CIRCUMFLEX */
	[320+0x0F] = 0x00EF,	/* U+00CF: LATIN CAPITAL LETTER I WITH DIAERESIS */
	[320+0x10] = 0x00F0,	/* U+00D0: LATIN CAPITAL LETTER ETH */
	[320+0x11] = 0x00F1,	/* U+00D1: LATIN CAPITAL LETTER N WITH TILDE */
	[320+0x12] = 0x00F2,	/* U+00D2: LATIN CAPITAL LETTER O WITH GRAVE */
	[320+0x13] = 0x00F3,	/* U+00D3: LATIN CAPITAL LETTER O WITH ACUTE */
	[320+0x14] = 0x00F4,	/* U+00D4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX */
	[320+0x15] = 0x00F5,	/* U+00D5: LATIN CAPITAL LETTER O WITH TILDE */
	[320+0x16] = 0x00F6,	/* U+00D6: LATIN CAPITAL LETTER O WITH DIAERESIS */
	[320+0x18] = 0x00F8,	/* U+00D8: LATIN CAPITAL LETTER O WITH STROKE */
	[320+0x19] = 0x00F9,	/* U+00D9: LATIN CAPITAL LETTER U WITH GRAVE */
	[320+0x1A] = 0x00FA,	/* U+00DA: LATIN CAPITAL LETTER U WITH ACUTE */
	[320+0x1B] = 0x00FB,	/* U+00DB: LATIN CAPITAL LETTER U WITH CIRCUMFLEX */
	[320+0x1C] = 0x00FC,	/* U+00DC: LATIN CAPITAL LETTER U WITH DIAERESIS */
	[320+0x1D] = 0x00FD,	/* U+00DD: LATIN CAPITAL LETTER Y WITH ACUTE */
	[320+0x1E] = 0x00FE,	/* U+00DE: LATIN CAPITAL LETTER THORN */
	[320+0x1F] = -1,	/* U+00DF: LATIN SMALL LETTER SHARP S */
	[0xC4] = 384 - 0x80,	/* 304 ... */
	[384+0x00] = 0x0101,	/* U+0100: LATIN CAPITAL LETTER A WITH MACRON */
	[384+0x02] = 0x0103,	/* U+0102: LATIN CAPITAL LETTER A WITH BREVE */
	[384+0x04] = 0x0105,	/* U+0104: LATIN CAPITAL LETTER A WITH OGONEK */
	[384+0x06] = 0x0107,	/* U+0106: LATIN CAPITAL LETTER C WITH ACUTE */
	[384+0x08] = 0x0109,	/* U+0108: LATIN CAPITAL LETTER C WITH CIRCUMFLEX */
	[384+0x0A] = 0x010B,	/* U+010A: LATIN CAPITAL LETTER C WITH DOT ABOVE */
	[384+0x0C] = 0x010D,	/* U+010C: LATIN CAPITAL LETTER C WITH CARON */
	[384+0x0E] = 0x010F,	/* U+010E: LATIN CAPITAL LETTER D WITH CARON */
	[384+0x10] = 0x0111,	/* U+0110: LATIN CAPITAL LETTER D WITH STROKE */
	[384+0x12] = 0x0113,	/* U+0112: LATIN CAPITAL LETTER E WITH MACRON */
	[384+0x14] = 0x0115,	/* U+0114: LATIN CAPITAL LETTER E WITH BREVE */
	[384+0x16] = 0x0117,	/* U+0116: LATIN CAPITAL LETTER E WITH DOT ABOVE */
	[384+0x18] = 0x0119,	/* U+0118: LATIN CAPITAL LETTER E WITH OGONEK */
	[384+0x1A] = 0x011B,	/* U+011A: LATIN CAPITAL LETTER E WITH CARON */
	[384+0x1C] = 0x011D,	/* U+011C: LATIN CAPITAL LETTER G WITH CIRCUMFLEX */
	[384+0x1E] = 0x011F,	/* U+011E: LATIN CAPITAL LETTER G WITH BREVE */
	[384+0x20] = 0x0121,	/* U+0120: LATIN CAPITAL LETTER G WITH DOT ABOVE */
	[384+0x22] = 0x0123,	/* U+0122: LATIN CAPITAL LETTER G WITH CEDILLA */
	[384+0x24] = 0x0125,	/* U+0124: LATIN CAPITAL LETTER H WITH CIRCUMFLEX */
	[384+0x26] = 0x0127,	/* U+0126: LATIN CAPITAL LETTER H WITH STROKE */
	[384+0x28] = 0x0129,	/* U+0128: LATIN CAPITAL LETTER I WITH TILDE */
	[384+0x2A] = 0x012B,	/* U+012A: LATIN CAPITAL LETTER I WITH MACRON */
	[384+0x2C] = 0x012D,	/* U+012C: LATIN CAPITAL LETTER I WITH BREVE */
	[384+0x2E] = 0x012F,	/* U+012E: LATIN CAPITAL LETTER I WITH OGONEK */
	[384+0x30] = -2,	/* U+0130: LATIN CAPITAL LETTER I WITH DOT ABOVE */
	[384+0x32] = 0x0133,	/* U+0132: LATIN CAPITAL LIGATURE IJ */
	[384+0x34] = 0x0135,	/* U+0134: LATIN CAPITAL LETTER J WITH CIRCUMFLEX */
	[384+0x36] = 0x0137,	/* U+0136: LATIN CAPITAL LETTER K WITH CEDILLA */
	[384+0x39] = 0x013A,	/* U+0139: LATIN CAPITAL LETTER L WITH ACUTE */
	[384+0x3B] = 0x013C,	/* U+013B: LATIN CAPITAL LETTER L WITH CEDILLA */
	[384+0x3D] = 0x013E,	/* U+013D: LATIN CAPITAL LETTER L WITH CARON */
	[384+0x3F] = 0x0140,	/* U+013F: LATIN CAPITAL LETTER L WITH MIDDLE DOT */
	[0xC5] = 448 - 0x80,	/* 305 ... */
	[448+0x01] = 0x0142,	/* U+0141: LATIN CAPITAL LETTER L WITH STROKE */
	[448+0x03] = 0x0144,	/* U+0143: LATIN CAPITAL LETTER N WITH ACUTE */
	[448+0x05] = 0x0146,	/* U+0145: LATIN CAPITAL LETTER N WITH CEDILLA */
	[448+0x07] = 0x0148,	/* U+0147: LATIN CAPITAL LETTER N WITH CARON */
	[448+0x09] = -3,	/* U+0149: LATIN SMALL LETTER N PRECEDED BY APOSTROPHE */
	[448+0x0A] = 0x014B,	/* U+014A: LATIN CAPITAL LETTER ENG */
	[448+0x0C] = 0x014D,	/* U+014C: LATIN CAPITAL LETTER O WITH MACRON */
	[448+0x0E] = 0x014F,	/* U+014E: LATIN CAPITAL LETTER O WITH BREVE */
	[448+0x10] = 0x0151,	/* U+0150: LATIN CAPITAL LETTER O WITH DOUBLE ACUTE */
	[448+0x12] = 0x0153,	/* U+0152: LATIN CAPITAL LIGATURE OE */
	[448+0x14] = 0x0155,	/* U+0154: LATIN CAPITAL LETTER R WITH ACUTE */
	[448+0x16] = 0x0157,	/* U+0156: LATIN CAPITAL LETTER R WITH CEDILLA */
	[448+0x18] = 0x0159,	/* U+0158: LATIN CAPITAL LETTER R WITH CARON */
	[448+0x1A] = 0x015B,	/* U+015A: LATIN CAPITAL LETTER S WITH ACUTE */
	[448+0x1C] = 0x015D,	/* U+015C: LATIN CAPITAL LETTER S WITH CIRCUMFLEX */
	[448+0x1E] = 0x015F,	/* U+015E: LATIN CAPITAL LETTER S WITH CEDILLA */
	[448+0x20] = 0x0161,	/* U+0160: LATIN CAPITAL LETTER S WITH CARON */
	[448+0x22] = 0x0163,	/* U+0162: LATIN CAPITAL LETTER T WITH CEDILLA */
	[448+0x24] = 0x0165,	/* U+0164: LATIN CAPITAL LETTER T WITH CARON */
	[448+0x26] = 0x0167,	/* U+0166: LATIN CAPITAL LETTER T WITH STROKE */
	[448+0x28] = 0x0169,	/* U+0168: LATIN CAPITAL LETTER U WITH TILDE */
	[448+0x2A] = 0x016B,	/* U+016A: LATIN CAPITAL LETTER U WITH MACRON */
	[448+0x2C] = 0x016D,	/* U+016C: LATIN CAPITAL LETTER U WITH BREVE */
	[448+0x2E] = 0x016F,	/* U+016E: LATIN CAPITAL LETTER U WITH RING ABOVE */
	[448+0x30] = 0x0171,	/* U+0170: LATIN CAPITAL LETTER U WITH DOUBLE ACUTE */
	[448+0x32] = 0x0173,	/* U+0172: LATIN CAPITAL LETTER U WITH OGONEK */
	[448+0x34] = 0x0175,	/* U+0174: LATIN CAPITAL LETTER W WITH CIRCUMFLEX */
	[448+0x36] = 0x0177,	/* U+0176: LATIN CAPITAL LETTER Y WITH CIRCUMFLEX */
	[448+0x38] = 0x00FF,	/* U+0178: LATIN CAPITAL LETTER Y WITH DIAERESIS */
	[448+0x39] = 0x017A,	/* U+0179: LATIN CAPITAL LETTER Z WITH ACUTE */
	[448+0x3B] = 0x017C,	/* U+017B: LATIN CAPITAL LETTER Z WITH DOT ABOVE */
	[448+0x3D] = 0x017E,	/* U+017D: LATIN CAPITAL LETTER Z WITH CARON */
	[448+0x3F] = 0x0073,	/* U+017F: LATIN SMALL LETTER LONG S */
	[0xC6] = 512 - 0x80,	/* 306 ... */
	[512+0x01] = 0x0253,	/* U+0181: LATIN CAPITAL LETTER B WITH HOOK */
	[512+0x02] = 0x0183,	/* U+0182: LATIN CAPITAL LETTER B WITH TOPBAR */
	[512+0x04] = 0x0185,	/* U+0184: LATIN CAPITAL LETTER TONE SIX */
	[512+0x06] = 0x0254,	/* U+0186: LATIN CAPITAL LETTER OPEN O */
	[512+0x07] = 0x0188,	/* U+0187: LATIN CAPITAL LETTER C WITH HOOK */
	[512+0x09] = 0x0256,	/* U+0189: LATIN CAPITAL LETTER AFRICAN D */
	[512+0x0A] = 0x0257,	/* U+018A: LATIN CAPITAL LETTER D WITH HOOK */
	[512+0x0B] = 0x018C,	/* U+018B: LATIN CAPITAL LETTER D WITH TOPBAR */
	[512+0x0E] = 0x01DD,	/* U+018E: LATIN CAPITAL LETTER REVERSED E */
	[512+0x0F] = 0x0259,	/* U+018F: LATIN CAPITAL LETTER SCHWA */
	[512+0x10] = 0x025B,	/* U+0190: LATIN CAPITAL LETTER OPEN E */
	[512+0x11] = 0x0192,	/* U+0191: LATIN CAPITAL LETTER F WITH HOOK */
	[512+0x13] = 0x0260,	/* U+0193: LATIN CAPITAL LETTER G WITH HOOK */
	[512+0x14] = 0x0263,	/* U+0194: LATIN CAPITAL LETTER GAMMA */
	[512+0x16] = 0x0269,	/* U+0196: LATIN CAPITAL LETTER IOTA */
	[512+0x17] = 0x0268,	/* U+0197: LATIN CAPITAL LETTER I WITH STROKE */
	[512+0x18] = 0x0199,	/* U+0198: LATIN CAPITAL LETTER K WITH HOOK */
	[512+0x1C] = 0x026F,	/* U+019C: LATIN CAPITAL LETTER TURNED M */
	[512+0x1D] = 0x0272,	/* U+019D: LATIN CAPITAL LETTER N WITH LEFT HOOK */
	[512+0x1F] = 0x0275,	/* U+019F: LATIN CAPITAL LETTER O WITH MIDDLE TILDE */
	[512+0x20] = 0x01A1,	/* U+01A0: LATIN CAPITAL LETTER O WITH HORN */
	[512+0x22] = 0x01A3,	/* U+01A2: LATIN CAPITAL LETTER OI */
	[512+0x24] = 0x01A5,	/* U+01A4: LATIN CAPITAL LETTER P WITH HOOK */
	[512+0x26] = 0x0280,	/* U+01A6: LATIN LETTER YR */
	[512+0x27] = 0x01A8,	/* U+01A7: LATIN CAPITAL LETTER TONE TWO */
	[512+0x29] = 0x0283,	/* U+01A9: LATIN CAPITAL LETTER ESH */
	[512+0x2C] = 0x01AD,	/* U+01AC: LATIN CAPITAL LETTER T WITH HOOK */
	[512+0x2E] = 0x0288,	/* U+01AE: LATIN CAPITAL LETTER T WITH RETROFLEX HOOK */
	[512+0x2F] = 0x01B0,	/* U+01AF: LATIN CAPITAL LETTER U WITH HORN */
	[512+0x31] = 0x028A,	/* U+01B1: LATIN CAPITAL LETTER UPSILON */
	[512+0x32] = 0x028B,	/* U+01B2: LATIN CAPITAL LETTER V WITH HOOK */
	[512+0x33] = 0x01B4,	/* U+01B3: LATIN CAPITAL LETTER Y WITH HOOK */
	[512+0x35] = 0x01B6,	/* U+01B5: LATIN CAPITAL LETTER Z WITH STROKE */
	[512+0x37] = 0x0292,	/* U+01B7: LATIN CAPITAL LETTER EZH */
	[512+0x38] = 0x01B9,	/* U+01B8: LATIN CAPITAL LETTER EZH REVERSED */
	[512+0x3C] = 0x01BD,	/* U+01BC: LATIN CAPITAL LETTER TONE FIVE */
	[0xC7] = 576 - 0x80,	/* 307 ... */
	[576+0x04] = 0x01C6,	/* U+01C4: LATIN CAPITAL LETTER DZ WITH CARON */
	[576+0x05] = 0x01C6,	/* U+01C5: LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON */
	[576+0x07] = 0x01C9,	/* U+01C7: LATIN CAPITAL LETTER LJ */
	[576+0x08] = 0x01C9,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[576+0x0A] = 0x01CC,	/* U+01CA: LATIN CAPITAL LETTER NJ */
	[576+0x0B] = 0x01CC,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[576+0x0D] = 0x01CE,	/* U+01CD: LATIN CAPITAL LETTER A WITH CARON */
	[576+0x0F] = 0x01D0,	/* U+01CF: LATIN CAPITAL LETTER I WITH CARON */
	[576+0x11] = 0x01D2,	/* U+01D1: LATIN CAPITAL LETTER O WITH CARON */
	[576+0x13] = 0x01D4,	/* U+01D3: LATIN CAPITAL LETTER U WITH CARON */
	[576+0x15] = 0x01D6,	/* U+01D5: LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON */
	[576+0x17] = 0x01D8,	/* U+01D7: LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE */
	[576+0x19] = 0x01DA,	/* U+01D9: LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON */
	[576+0x1B] = 0x01DC,	/* U+01DB: LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE */
	[576+0x1E] = 0x01DF,	/* U+01DE: LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON */
	[576+0x20] = 0x01E1,	/* U+01E0: LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON */
	[576+0x22] = 0x01E3,	/* U+01E2: LATIN CAPITAL LETTER AE WITH MACRON */
	[576+0x24] = 0x01E5,	/* U+01E4: LATIN CAPITAL LETTER G WITH STROKE */
	[576+0x26] = 0x01E7,	/* U+01E6: LATIN CAPITAL LETTER G WITH CARON */
	[576+0x28] = 0x01E9,	/* U+01E8: LATIN CAPITAL LETTER K WITH CARON */
	[576+0x2A] = 0x01EB,	/* U+01EA: LATIN CAPITAL LETTER O WITH OGONEK */
	[576+0x2C] = 0x01ED,	/* U+01EC: LATIN CAPITAL LETTER O WITH OGONEK AND MACRON */
	[576+0x2E] = 0x01EF,	/* U+01EE: LATIN CAPITAL LETTER EZH WITH CARON */
	[576+0x30] = -4,	/* U+01F0: LATIN SMALL LETTER J WITH CARON */
	[576+0x31] = 0x01F3,	/* U+01F1: LATIN CAPITAL LETTER DZ */
	[576+0x32] = 0x01F3,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[576+0x34] = 0x01F5,	/* U+01F4: LATIN CAPITAL LETTER G WITH ACUTE */
	[576+0x36] = 0x0195,	/* U+01F6: LATIN CAPITAL LETTER HWAIR */
	[576+0x37] = 0x01BF,	/* U+01F7: LATIN CAPITAL LETTER WYNN */
	[576+0x38] = 0x01F9,	/* U+01F8: LATIN CAPITAL LETTER N WITH GRAVE */
	[576+0x3A] = 0x01FB,	/* U+01FA: LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE */
	[576+0x3C] = 0x01FD,	/* U+01FC: LATIN CAPITAL LETTER AE WITH ACUTE */
	[576+0x3E] = 0x01FF,	/* U+01FE: LATIN CAPITAL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 640 - 0x80,	/* 310 ... */
	[640+0x00] = 0x0201,	/* U+0200: LATIN CAPITAL LETTER A WITH DOUBLE GRAVE */
	[640+0x02] = 0x0203,	/* U+0202: LATIN CAPITAL LETTER A WITH INVERTED BREVE */
	[640+0x04] = 0x0205,	/* U+0204: LATIN CAPITAL LETTER E WITH DOUBLE GRAVE */
	[640+0x06] = 0x0207,	/* U+0206: LATIN CAPITAL LETTER E WITH INVERTED BREVE */
	[640+0x08] = 0x0209,	/* U+0208: LATIN CAPITAL LETTER I WITH DOUBLE GRAVE */
	[640+0x0A] = 0x020B,	/* U+020A: LATIN CAPITAL LETTER I WITH INVERTED BREVE */
	[640+0x0C] = 0x020D,	/* U+020C: LATIN CAPITAL LETTER O WITH DOUBLE GRAVE */
	[640+0x0E] = 0x020F,	/* U+020E: LATIN CAPITAL LETTER O WITH INVERTED BREVE */
	[640+0x10] = 0x0211,	/* U+0210: LATIN CAPITAL LETTER R WITH DOUBLE GRAVE */
	[640+0x12] = 0x0213,	/* U+0212: LATIN CAPITAL LETTER R WITH INVERTED BREVE */
	[640+0x14] = 0x0215,	/* U+0214: LATIN CAPITAL LETTER U WITH DOUBLE GRAVE */
	[640+0x16] = 0x0217,	/* U+0216: LATIN CAPITAL LETTER U WITH INVERTED BREVE */
	[640+0x18] = 0x0219,	/* U+0218: LATIN CAPITAL LETTER S WITH COMMA BELOW */
	[640+0x1A] = 0x021B,	/* U+021A: LATIN CAPITAL LETTER T WITH COMMA BELOW */
	[640+0x1C] = 0x021D,	/* U+021C: LATIN CAPITAL LETTER YOGH */
	[640+0x1E] = 0x021F,	/* U+021E: LATIN CAPITAL LETTER H WITH CARON */
	[640+0x20] = 0x019E,	/* U+0220: LATIN CAPITAL LETTER N WITH LONG RIGHT LEG */
	[640+0x22] = 0x0223,	/* U+0222: LATIN CAPITAL LETTER OU */
	[640+0x24] = 0x0225,	/* U+0224: LATIN CAPITAL LETTER Z WITH HOOK */
	[640+0x26] = 0x0227,	/* U+0226: LATIN CAPITAL LETTER A WITH DOT ABOVE */
	[640+0x28] = 0x0229,	/* U+0228: LATIN CAPITAL LETTER E WITH CEDILLA */
	[640+0x2A] = 0x022B,	/* U+022A: LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON */
	[640+0x2C] = 0x022D,	/* U+022C: LATIN CAPITAL LETTER O WITH TILDE AND MACRON */
	[640+0x2E] = 0x022F,	/* U+022E: LATIN CAPITAL LETTER O WITH DOT ABOVE */
	[640+0x30] = 0x0231,	/* U+0230: LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON */
	[640+0x32] = 0x0233,	/* U+0232: LATIN CAPITAL LETTER Y WITH MACRON */
	[640+0x3A] = 0x2C65,	/* U+023A: LATIN CAPITAL LETTER A WITH STROKE */
	[640+0x3B] = 0x023C,	/* U+023B: LATIN CAPITAL LETTER C WITH STROKE */
	[640+0x3D] = 0x019A,	/* U+023D: LATIN CAPITAL LETTER L WITH BAR */
	[640+0x3E] = 0x2C66,	/* U+023E: LATIN CAPITAL LETTER T WITH DIAGONAL STROKE */
	[0xC9] = 704 - 0x80,	/* 311 ... */
	[704+0x01] = 0x0242,	/* U+0241: LATIN CAPITAL LETTER GLOTTAL STOP */
	[704+0x03] = 0x0180,	/* U+0243: LATIN CAPITAL LETTER B WITH STROKE */
	[704+0x04] = 0x0289,	/* U+0244: LATIN CAPITAL LETTER U BAR */
	[704+0x05] = 0x028C,	/* U+0245: LATIN CAPITAL LETTER TURNED V */
	[704+0x06] = 0x0247,	/* U+0246: LATIN CAPITAL LETTER E WITH STROKE */
	[704+0x08] = 0x0249,	/* U+0248: LATIN CAPITAL LETTER J WITH STROKE */
	[704+0x0A] = 0x024B,	/* U+024A: LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL */
	[704+0x0C] = 0x024D,	/* U+024C: LATIN CAPITAL LETTER R WITH STROKE */
	[704+0x0E] = 0x024F,	/* U+024E: LATIN CAPITAL LETTER Y WITH STROKE */
	[0xCD] = 768 - 0x80,	/* 315 ... */
	[768+0x05] = 0x03B9,	/* U+0345: COMBINING GREEK YPOGEGRAMMENI */
	[768+0x30] = 0x0371,	/* U+0370: GREEK CAPITAL LETTER HETA */
	[768+0x32] = 0x0373,	/* U+0372: GREEK CAPITAL LETTER ARCHAIC SAMPI */
	[768+0x36] = 0x0377,	/* U+0376: GREEK CAPITAL LETTER PAMPHYLIAN DIGAMMA */
	[768+0x3F] = 0x03F3,	/* U+037F: GREEK CAPITAL LETTER YOT */
	[0xCE] = 832 - 0x80,	/* 316 ... */
	[832+0x06] = 0x03AC,	/* U+0386: GREEK CAPITAL LETTER ALPHA WITH TONOS */
	[832+0x08] = 0x03AD,	/* U+0388: GREEK CAPITAL LETTER EPSILON WITH TONOS */
	[832+0x09] = 0x03AE,	/* U+0389: GREEK CAPITAL LETTER ETA WITH TONOS */
	[832+0x0A] = 0x03AF,	/* U+038A: GREEK CAPITAL LETTER IOTA WITH TONOS */
	[832+0x0C] = 0x03CC,	/* U+038C: GREEK CAPITAL LETTER OMICRON WITH TONOS */
	[832+0x0E] = 0x03CD,	/* U+038E: GREEK CAPITAL LETTER UPSILON WITH TONOS */
	[832+0x0F] = 0x03CE,	/* U+038F: GREEK CAPITAL LETTER OMEGA WITH TONOS */
	[832+0x10] = -5,	/* U+0390: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND TONOS */
	[832+0x11] = 0x03B1,	/* U+0391: GREEK CAPITAL LETTER ALPHA */
	[832+0x12] = 0x03B2,	/* U+0392: GREEK CAPITAL LETTER BETA */
	[832+0x13] = 0x03B3,	/* U+0393: GREEK CAPITAL LETTER GAMMA */
	[832+0x14] = 0x03B4,	/* U+0394: GREEK CAPITAL LETTER DELTA */
	[832+0x15] = 0x03B5,	/* U+0395: GREEK CAPITAL LETTER EPSILON */
	[832+0x16] = 0x03B6,	/* U+0396: GREEK CAPITAL LETTER ZETA */
	[832+0x17] = 0x03B7,	/* U+0397: GREEK CAPITAL LETTER ETA */
	[832+0x18] = 0x03B8,	/* U+0398: GREEK CAPITAL LETTER THETA */
	[832+0x19] = 0x03B9,	/* U+0399: GREEK CAPITAL LETTER IOTA */
	[832+0x1A] = 0x03BA,	/* U+039A: GREEK CAPITAL LETTER KAPPA */
	[832+0x1B] = 0x03BB,	/* U+039B: GREEK CAPITAL LETTER LAMDA */
	[832+0x1C] = 0x03BC,	/* U+039C: GREEK CAPITAL LETTER MU */
	[832+0x1D] = 0x03BD,	/* U+039D: GREEK CAPITAL LETTER NU */
	[832+0x1E] = 0x03BE,	/* U+039E: GREEK CAPITAL LETTER XI */
	[832+0x1F] = 0x03BF,	/* U+039F: GREEK CAPITAL LETTER OMICRON */
	[832+0x20] = 0x03C0,	/* U+03A0: GREEK CAPITAL LETTER PI */
	[832+0x21] = 0x03C1,	/* U+03A1: GREEK CAPITAL LETTER RHO */
	[832+0x23] = 0x03C3,	/* U+03A3: GREEK CAPITAL LETTER SIGMA */
	[832+0x24] = 0x03C4,	/* U+03A4: GREEK CAPITAL LETTER TAU */
	[832+0x25] = 0x03C5,	/* U+03A5: GREEK CAPITAL LETTER UPSILON */
	[832+0x26] = 0x03C6,	/* U+03A6: GREEK CAPITAL LETTER PHI */
	[832+0x27] = 0x03C7,	/* U+03A7: GREEK CAPITAL LETTER CHI */
	[832+0x28] = 0x03C8,	/* U+03A8: GREEK CAPITAL LETTER PSI */
	[832+0x29] = 0x03C9,	/* U+03A9: GREEK CAPITAL LETTER OMEGA */
	[832+0x2A] = 0x03CA,	/* U+03AA: GREEK CAPITAL LETTER IOTA WITH DIALYTIKA */
	[832+0x2B] = 0x03CB,	/* U+03AB: GREEK CAPITAL LETTER UPSILON WITH DIALYTIKA */
	[832+0x30] = -6,	/* U+03B0: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND TONOS */
	[0xCF] = 896 - 0x80,	/* 317 ... */
	[896+0x02] = 0x03C3,	/* U+03C2: GREEK SMALL LETTER FINAL SIGMA */
	[896+0x0F] = 0x03D7,	/* U+03CF: GREEK CAPITAL KAI SYMBOL */
	[896+0x10] = 0x03B2,	/* U+03D0: GREEK BETA SYMBOL */
	[896+0x11] = 0x03B8,	/* U+03D1: GREEK THETA SYMBOL */
	[896+0x15] = 0x03C6,	/* U+03D5: GREEK PHI SYMBOL */
	[896+0x16] = 0x03C0,	/* U+03D6: GREEK PI SYMBOL */
	[896+0x18] = 0x03D9,	/* U+03D8: GREEK LETTER ARCHAIC KOPPA */
	[896+0x1A] = 0x03DB,	/* U+03DA: GREEK LETTER STIGMA */
	[896+0x1C] = 0x03DD,	/* U+03DC: GREEK LETTER DIGAMMA */
	[896+0x1E] = 0x03DF,	/* U+03DE: GREEK LETTER KOPPA */
	[896+0x20] = 0x03E1,	/* U+03E0: GREEK LETTER SAMPI */
	[896+0x22] = 0x03E3,	/* U+03E2: COPTIC CAPITAL LETTER SHEI */
	[896+0x24] = 0x03E5,	/* U+03E4: COPTIC CAPITAL LETTER FEI */
	[896+0x26] = 0x03E7,	/* U+03E6: COPTIC CAPITAL LETTER KHEI */
	[896+0x28] = 0x03E9,	/* U+03E8: COPTIC CAPITAL LETTER HORI */
	[896+0x2A] = 0x03EB,	/* U+03EA: COPTIC CAPITAL LETTER GANGIA */
	[896+0x2C] = 0x03ED,	/* U+03EC: COPTIC CAPITAL LETTER SHIMA */
	[896+0x2E] = 0x03EF,	/* U+03EE: COPTIC CAPITAL LETTER DEI */
	[896+0x30] = 0x03BA,	/* U+03F0: GREEK KAPPA SYMBOL */
	[896+0x31] = 0x03C1,	/* U+03F1: GREEK RHO SYMBOL */
	[896+0x34] = 0x03B8,	/* U+03F4: GREEK CAPITAL THETA SYMBOL */
	[896+0x35] = 0x03B5,	/* U+03F5: GREEK LUNATE EPSILON SYMBOL */
	[896+0x37] = 0x03F8,	/* U+03F7: GREEK CAPITAL LETTER SHO */
	[896+0x39] = 0x03F2,	/* U+03F9: GREEK CAPITAL LUNATE SIGMA SYMBOL */
	[896+0x3A] = 0x03FB,	/* U+03FA: GREEK CAPITAL LETTER SAN */
	[896+0x3D] = 0x037B,	/* U+03FD: GREEK CAPITAL REVERSED LUNATE SIGMA SYMBOL */
	[896+0x3E] = 0x037C,	/* U+03FE: GREEK CAPITAL DOTTED LUNATE SIGMA SYMBOL */
	[896+0x3F] = 0x037D,	/* U+03FF: GREEK CAPITAL REVERSED DOTTED LUNATE SIGMA SYMBOL */
	[0xD0] = 960 - 0x80,	/* 320 ... */
	[960+0x00] = 0x0450,	/* U+0400: CYRILLIC CAPITAL LETTER IE WITH GRAVE */
	[960+0x01] = 0x0451,	/* U+0401: CYRILLIC CAPITAL LETTER IO */
	[960+0x02] = 0x0452,	/* U+0402: CYRILLIC CAPITAL LETTER DJE */
	[960+0x03] = 0x0453,	/* U+0403: CYRILLIC CAPITAL LETTER GJE */
	[960+0x04] = 0x0454,	/* U+0404: CYRILLIC CAPITAL LETTER UKRAINIAN IE */
	[960+0x05] = 0x0455,	/* U+0405: CYRILLIC CAPITAL LETTER DZE */
	[960+0x06] = 0x0456,	/* U+0406: CYRILLIC CAPITAL LETTER BYELORUSSIAN-UKRAINIAN I */
	[960+0x07] = 0x0457,	/* U+0407: CYRILLIC CAPITAL LETTER YI */
	[960+0x08] = 0x0458,	/* U+0408: CYRILLIC CAPITAL LETTER JE */
	[960+0x09] = 0x0459,	/* U+0409: CYRILLIC CAPITAL LETTER LJE */
	[960+0x0A] = 0x045A,	/* U+040A: CYRILLIC CAPITAL LETTER NJE */
	[960+0x0B] = 0x045B,	/* U+040B: CYRILLIC CAPITAL LETTER TSHE */
	[960+0x0C] = 0x045C,	/* U+040C: CYRILLIC CAPITAL LETTER KJE */
	[960+0x0D] = 0x045D,	/* U+040D: CYRILLIC CAPITAL LETTER I WITH GRAVE */
	[960+0x0E] = 0x045E,	/* U+040E: CYRILLIC CAPITAL LETTER SHORT U */
	[960+0x0F] = 0x045F,	/* U+040F: CYRILLIC CAPITAL LETTER DZHE */
	[960+0x10] = 0x0430,	/* U+0410: CYRILLIC CAPITAL LETTER A */
	[960+0x11] = 0x0431,	/* U+0411: CYRILLIC CAPITAL LETTER BE */
	[960+0x12] = 0x0432,	/* U+0412: CYRILLIC CAPITAL LETTER VE */
	[960+0x13] = 0x0433,	/* U+0413: CYRILLIC CAPITAL LETTER GHE */
	[960+0x14] = 0x0434,	/* U+0414: CYRILLIC CAPITAL LETTER DE */
	[960+0x15] = 0x0435,	/* U+0415: CYRILLIC CAPITAL LETTER IE */
	[960+0x16] = 0x0436,	/* U+0416: CYRILLIC CAPITAL LETTER ZHE */
	[960+0x17] = 0x0437,	/* U+0417: CYRILLIC CAPITAL LETTER ZE */
	[960+0x18] = 0x0438,	/* U+0418: CYRILLIC CAPITAL LETTER I */
	[960+0x19] = 0x0439,	/* U+0419: CYRILLIC CAPITAL LETTER SHORT I */
	[960+0x1A] = 0x043A,	/* U+041A: CYRILLIC CAPITAL LETTER KA */
	[960+0x1B] = 0x043B,	/* U+041B: CYRILLIC CAPITAL LETTER EL */
	[960+0x1C] = 0x043C,	/* U+041C: CYRILLIC CAPITAL LETTER EM */
	[960+0x1D] = 0x043D,	/* U+041D: CYRILLIC CAPITAL LETTER EN */
	[960+0x1E] = 0x043E,	/* U+041E: CYRILLIC CAPITAL LETTER O */
	[960+0x1F] = 0x043F,	/* U+041F: CYRILLIC CAPITAL LETTER PE */
	[960+0x20] = 0x0440,	/* U+0420: CYRILLIC CAPITAL LETTER ER */
	[960+0x21] = 0x0441,	/* U+0421: CYRILLIC CAPITAL LETTER ES */
	[960+0x22] = 0x0442,	/* U+0422: CYRILLIC CAPITAL LETTER TE */
	[960+0x23] = 0x0443,	/* U+0423: CYRILLIC CAPITAL LETTER U */
	[960+0x24] = 0x0444,	/* U+0424: CYRILLIC CAPITAL LETTER EF */
	[960+0x25] = 0x0445,	/* U+0425: CYRILLIC CAPITAL LETTER HA */
	[960+0x26] = 0x0446,	/* U+0426: CYRILLIC CAPITAL LETTER TSE */
	[960+0x27] = 0x0447,	/* U+0427: CYRILLIC CAPITAL LETTER CHE */
	[960+0x28] = 0x0448,	/* U+0428: CYRILLIC CAPITAL LETTER SHA */
	[960+0x29] = 0x0449,	/* U+0429: CYRILLIC CAPITAL LETTER SHCHA */
	[960+0x2A] = 0x044A,	/* U+042A: CYRILLIC CAPITAL LETTER HARD SIGN */
	[960+0x2B] = 0x044B,	/* U+042B: CYRILLIC CAPITAL LETTER YERU */
	[960+0x2C] = 0x044C,	/* U+042C: CYRILLIC CAPITAL LETTER SOFT SIGN */
	[960+0x2D] = 0x044D,	/* U+042D: CYRILLIC CAPITAL LETTER E */
	[960+0x2E] = 0x044E,	/* U+042E: CYRILLIC CAPITAL LETTER YU */
	[960+0x2F] = 0x044F,	/* U+042F: CYRILLIC CAPITAL LETTER YA */
	[0xD1] = 1024 - 0x80,	/* 321 ... */
	[1024+0x20] = 0x0461,	/* U+0460: CYRILLIC CAPITAL LETTER OMEGA */
	[1024+0x22] = 0x0463,	/* U+0462: CYRILLIC CAPITAL LETTER YAT */
	[1024+0x24] = 0x0465,	/* U+0464: CYRILLIC CAPITAL LETTER IOTIFIED E */
	[1024+0x26] = 0x0467,	/* U+0466: CYRILLIC CAPITAL LETTER LITTLE YUS */
	[1024+0x28] = 0x0469,	/* U+0468: CYRILLIC CAPITAL LETTER IOTIFIED LITTLE YUS */
	[1024+0x2A] = 0x046B,	/* U+046A: CYRILLIC CAPITAL LETTER BIG YUS */
	[1024+0x2C] = 0x046D,	/* U+046C: CYRILLIC CAPITAL LETTER IOTIFIED BIG YUS */
	[1024+0x2E] = 0x046F,	/* U+046E: CYRILLIC CAPITAL LETTER KSI */
	[1024+0x30] = 0x0471,	/* U+0470: CYRILLIC CAPITAL LETTER PSI */
	[1024+0x32] = 0x0473,	/* U+0472: CYRILLIC CAPITAL LETTER FITA */
	[1024+0x34] = 0x0475,	/* U+0474: CYRILLIC CAPITAL LETTER IZHITSA */
	[1024+0x36] = 0x0477,	/* U+0476: CYRILLIC CAPITAL LETTER IZHITSA WITH DOUBLE GRAVE ACCENT */
	[1024+0x38] = 0x0479,	/* U+0478: CYRILLIC CAPITAL LETTER UK */
	[1024+0x3A] = 0x047B,	/* U+047A: CYRILLIC CAPITAL LETTER ROUND OMEGA */
	[1024+0x3C] = 0x047D,	/* U+047C: CYRILLIC CAPITAL LETTER OMEGA WITH TITLO */
	[1024+0x3E] = 0x047F,	/* U+047E: CYRILLIC CAPITAL LETTER OT */
	[0xD2] = 1088 - 0x80,	/* 322 ... */
	[1088+0x00] = 0x0481,	/* U+0480: CYRILLIC CAPITAL LETTER KOPPA */
	[1088+0x0A] = 0x048B,	/* U+048A: CYRILLIC CAPITAL LETTER SHORT I WITH TAIL */
	[1088+0x0C] = 0x048D,	/* U+048C: CYRILLIC CAPITAL LETTER SEMISOFT SIGN */
	[1088+0x0E] = 0x048F,	/* U+048E: CYRILLIC CAPITAL LETTER ER WITH TICK */
	[1088+0x10] = 0x0491,	/* U+0490: CYRILLIC CAPITAL LETTER GHE WITH UPTURN */
	[1088+0x12] = 0x0493,	/* U+0492: CYRILLIC CAPITAL LETTER GHE WITH STROKE */
	[1088+0x14] = 0x0495,	/* U+0494: CYRILLIC CAPITAL LETTER GHE WITH MIDDLE HOOK */
	[1088+0x16] = 0x0497,	/* U+0496: CYRILLIC CAPITAL LETTER ZHE WITH DESCENDER */
	[1088+0x18] = 0x0499,	/* U+0498: CYRILLIC CAPITAL LETTER ZE WITH DESCENDER */
	[1088+0x1A] = 0x049B,	/* U+049A: CYRILLIC CAPITAL LETTER KA WITH DESCENDER */
	[1088+0x1C] = 0x049D,	/* U+049C: CYRILLIC CAPITAL LETTER KA WITH VERTICAL STROKE */
	[1088+0x1E] = 0x049F,	/* U+049E: CYRILLIC CAPITAL LETTER KA WITH STROKE */
	[1088+0x20] = 0x04A1,	/* U+04A0: CYRILLIC CAPITAL LETTER BASHKIR KA */
	[1088+0x22] = 0x04A3,	/* U+04A2: CYRILLIC CAPITAL LETTER EN WITH DESCENDER */
	[1088+0x24] = 0x04A5,	/* U+04A4: CYRILLIC CAPITAL LIGATURE EN GHE */
	[1088+0x26] = 0x04A7,	/* U+04A6: CYRILLIC CAPITAL LETTER PE WITH MIDDLE HOOK */
	[1088+0x28] = 0x04A9,	/* U+04A8: CYRILLIC CAPITAL LETTER ABKHASIAN HA */
	[1088+0x2A] = 0x04AB,	/* U+04AA: CYRILLIC CAPITAL LETTER ES WITH DESCENDER */
	[1088+0x2C] = 0x04AD,	/* U+04AC: CYRILLIC CAPITAL LETTER TE WITH DESCENDER */
	[1088+0x2E] = 0x04AF,	/* U+04AE: CYRILLIC CAPITAL LETTER STRAIGHT U */
	[1088+0x30] = 0x04B1,	/* U+04B0: CYRILLIC CAPITAL LETTER STRAIGHT U WITH STROKE */
	[1088+0x32] = 0x04B3,	/* U+04B2: CYRILLIC CAPITAL LETTER HA WITH DESCENDER */
	[1088+0x34] = 0x04B5,	/* U+04B4: CYRILLIC CAPITAL LIGATURE TE TSE */
	[1088+0x36] = 0x04B7,	/* U+04B6: CYRILLIC CAPITAL LETTER CHE WITH DESCENDER */
	[1088+0x38] = 0x04B9,	/* U+04B8: CYRILLIC CAPITAL LETTER CHE WITH VERTICAL STROKE */
	[1088+0x3A] = 0x04BB,	/* U+04BA: CYRILLIC CAPITAL LETTER SHHA */
	[1088+0x3C] = 0x04BD,	/* U+04BC: CYRILLIC CAPITAL LETTER ABKHASIAN CHE */
	[1088+0x3E] = 0x04BF,	/* U+04BE: CYRILLIC CAPITAL LETTER ABKHASIAN CHE WITH DESCENDER */
	[0xD3] = 1152 - 0x80,	/* 323 ... */
	[1152+0x00] = 0x04CF,	/* U+04C0: CYRILLIC LETTER PALOCHKA */
	[1152+0x01] = 0x04C2,	/* U+04C1: CYRILLIC CAPITAL LETTER ZHE WITH BREVE */
	[1152+0x03] = 0x04C4,	/* U+04C3: CYRILLIC CAPITAL LETTER KA WITH HOOK */
	[1152+0x05] = 0x04C6,	/* U+04C5: CYRILLIC CAPITAL LETTER EL WITH TAIL */
	[1152+0x07] = 0x04C8,	/* U+04C7: CYRILLIC CAPITAL LETTER EN WITH HOOK */
	[1152+0x09] = 0x04CA,	/* U+04C9: CYRILLIC CAPITAL LETTER EN WITH TAIL */
	[1152+0x0B] = 0x04CC,	/* U+04CB: CYRILLIC CAPITAL LETTER KHAKASSIAN CHE */
	[1152+0x0D] = 0x04CE,	/* U+04CD: CYRILLIC CAPITAL LETTER EM WITH TAIL */
	[1152+0x10] = 0x04D1,	/* U+04D0: CYRILLIC CAPITAL LETTER A WITH BREVE */
	[1152+0x12] = 0x04D3,	/* U+04D2: CYRILLIC CAPITAL LETTER A WITH DIAERESIS */
	[1152+0x14] = 0x04D5,	/* U+04D4: CYRILLIC CAPITAL LIGATURE A IE */
	[1152+0x16] = 0x04D7,	/* U+04D6: CYRILLIC CAPITAL LETTER IE WITH BREVE */
	[1152+0x18] = 0x04D9,	/* U+04D8: CYRILLIC CAPITAL LETTER SCHWA */
	[1152+0x1A] = 0x04DB,	/* U+04DA: CYRILLIC CAPITAL LETTER SCHWA WITH DIAERESIS */
	[1152+0x1C] = 0x04DD,	/* U+04DC: CYRILLIC CAPITAL LETTER ZHE WITH DIAERESIS */
	[1152+0x1E] = 0x04DF,	/* U+04DE: CYRILLIC CAPITAL LETTER ZE WITH DIAERESIS */
	[1152+0x20] = 0x04E1,	/* U+04E0: CYRILLIC CAPITAL LETTER ABKHASIAN DZE */
	[1152+0x22] = 0x04E3,	/* U+04E2: CYRILLIC CAPITAL LETTER I WITH MACRON */
	[1152+0x24] = 0x04E5,	/* U+04E4: CYRILLIC CAPITAL LETTER I WITH DIAERESIS */
	[1152+0x26] = 0x04E7,	/* U+04E6: CYRILLIC CAPITAL LETTER O WITH DIAERESIS */
	[1152+0x28] = 0x04E9,	/* U+04E8: CYRILLIC CAPITAL LETTER BARRED O */
	[1152+0x2A] = 0x04EB,	/* U+04EA: CYRILLIC CAPITAL LETTER BARRED O WITH DIAERESIS */
	[1152+0x2C] = 0x04ED,	/* U+04EC: CYRILLIC CAPITAL LETTER E WITH DIAERESIS */
	[1152+0x2E] = 0x04EF,	/* U+04EE: CYRILLIC CAPITAL LETTER U WITH MACRON */
	[1152+0x30] = 0x04F1,	/* U+04F0: CYRILLIC CAPITAL LETTER U WITH DIAERESIS */
	[1152+0x32] = 0x04F3,	/* U+04F2: CYRILLIC CAPITAL LETTER U WITH DOUBLE ACUTE */
	[1152+0x34] = 0x04F5,	/* U+04F4: CYRILLIC CAPITAL LETTER CHE WITH DIAERESIS */
	[1152+0x36] = 0x04F7,	/* U+04F6: CYRILLIC CAPITAL LETTER GHE WITH DESCENDER */
	[1152+0x38] = 0x04F9,	/* U+04F8: CYRILLIC CAPITAL LETTER YERU WITH DIAERESIS */
	[1152+0x3A] = 0x04FB,	/* U+04FA: CYRILLIC CAPITAL LETTER GHE WITH STROKE AND HOOK */
	[1152+0x3C] = 0x04FD,	/* U+04FC: CYRILLIC CAPITAL LETTER HA WITH HOOK */
	[1152+0x3E] = 0x04FF,	/* U+04FE: CYRILLIC CAPITAL LETTER HA WITH STROKE */
	[0xD4] = 1216 - 0x80,	/* 324 ... */
	[1216+0x00] = 0x0501,	/* U+0500: CYRILLIC CAPITAL LETTER KOMI DE */
	[1216+0x02] = 0x0503,	/* U+0502: CYRILLIC CAPITAL LETTER KOMI DJE */
	[1216+0x04] = 0x0505,	/* U+0504: CYRILLIC CAPITAL LETTER KOMI ZJE */
	[1216+0x06] = 0x0507,	/* U+0506: CYRILLIC CAPITAL LETTER KOMI DZJE */
	[1216+0x08] = 0x0509,	/* U+0508: CYRILLIC CAPITAL LETTER KOMI LJE */
	[1216+0x0A] = 0x050B,	/* U+050A: CYRILLIC CAPITAL LETTER KOMI NJE */
	[1216+0x0C] = 0x050D,	/* U+050C: CYRILLIC CAPITAL LETTER KOMI SJE */
	[1216+0x0E] = 0x050F,	/* U+050E: CYRILLIC CAPITAL LETTER KOMI TJE */
	[1216+0x10] = 0x0511,	/* U+0510: CYRILLIC CAPITAL LETTER REVERSED ZE */
	[1216+0x12] = 0x0513,	/* U+0512: CYRILLIC CAPITAL LETTER EL WITH HOOK */
	[1216+0x14] = 0x0515,	/* U+0514: CYRILLIC CAPITAL LETTER LHA */
	[1216+0x16] = 0x0517,	/* U+0516: CYRILLIC CAPITAL LETTER RHA */
	[1216+0x18] = 0x0519,	/* U+0518: CYRILLIC CAPITAL LETTER YAE */
	[1216+0x1A] = 0x051B,	/* U+051A: CYRILLIC CAPITAL LETTER QA */
	[1216+0x1C] = 0x051D,	/* U+051C: CYRILLIC CAPITAL LETTER WE */
	[1216+0x1E] = 0x051F,	/* U+051E: CYRILLIC CAPITAL LETTER ALEUT KA */
	[1216+0x20] = 0x0521,	/* U+0520: CYRILLIC CAPITAL LETTER EL WITH MIDDLE HOOK */
	[1216+0x22] = 0x0523,	/* U+0522: CYRILLIC CAPITAL LETTER EN WITH MIDDLE HOOK */
	[1216+0x24] = 0x0525,	/* U+0524: CYRILLIC CAPITAL LETTER PE WITH DESCENDER */
	[1216+0x26] = 0x0527,	/* U+0526: CYRILLIC CAPITAL LETTER SHHA WITH DESCENDER */
	[1216+0x28] = 0x0529,	/* U+0528: CYRILLIC CAPITAL LETTER EN WITH LEFT HOOK */
	[1216+0x2A] = 0x052B,	/* U+052A: CYRILLIC CAPITAL LETTER DZZHE */
	[1216+0x2C] = 0x052D,	/* U+052C: CYRILLIC CAPITAL LETTER DCHE */
	[1216+0x2E] = 0x052F,	/* U+052E: CYRILLIC CAPITAL LETTER EL WITH DESCENDER */
	[1216+0x31] = 0x0561,	/* U+0531: ARMENIAN CAPITAL LETTER AYB */
	[1216+0x32] = 0x0562,	/* U+0532: ARMENIAN CAPITAL LETTER BEN */
	[1216+0x33] = 0x0563,	/* U+0533: ARMENIAN CAPITAL LETTER GIM */
	[1216+0x34] = 0x0564,	/* U+0534: ARMENIAN CAPITAL LETTER DA */
	[1216+0x35] = 0x0565,	/* U+0535: ARMENIAN CAPITAL LETTER ECH */
	[1216+0x36] = 0x0566,	/* U+0536: ARMENIAN CAPITAL LETTER ZA */
	[1216+0x37] = 0x0567,	/* U+0537: ARMENIAN CAPITAL LETTER EH */
	[1216+0x38] = 0x0568,	/* U+0538: ARMENIAN CAPITAL LETTER ET */
	[1216+0x39] = 0x0569,	/* U+0539: ARMENIAN CAPITAL LETTER TO */
	[1216+0x3A] = 0x056A,	/* U+053A: ARMENIAN CAPITAL LETTER ZHE */
	[1216+0x3B] = 0x056B,	/* U+053B: ARMENIAN CAPITAL LETTER INI */
	[1216+0x3C] = 0x056C,	/* U+053C: ARMENIAN CAPITAL LETTER LIWN */
	[1216+0x3D] = 0x056D,	/* U+053D: ARMENIAN CAPITAL LETTER XEH */
	[1216+0x3E] = 0x056E,	/* U+053E: ARMENIAN CAPITAL LETTER CA */
	[1216+0x3F] = 0x056F,	/* U+053F: ARMENIAN CAPITAL LETTER KEN */
	[0xD5] = 1280 - 0x80,	/* 325 ... */
	[1280+0x00] = 0x0570,	/* U+0540: ARMENIAN CAPITAL LETTER HO */
	[1280+0x01] = 0x0571,	/* U+0541: ARMENIAN CAPITAL LETTER JA */
	[1280+0x02] = 0x0572,	/* U+0542: ARMENIAN CAPITAL LETTER GHAD */
	[1280+0x03] = 0x0573,	/* U+0543: ARMENIAN CAPITAL LETTER CHEH */
	[1280+0x04] = 0x0574,	/* U+0544: ARMENIAN CAPITAL LETTER MEN */
	[1280+0x05] = 0x0575,	/* U+0545: ARMENIAN CAPITAL LETTER YI */
	[1280+0x06] = 0x0576,	/* U+0546: ARMENIAN CAPITAL LETTER NOW */
	[1280+0x07] = 0x0577,	/* U+0547: ARMENIAN CAPITAL LETTER SHA */
	[1280+0x08] = 0x0578,	/* U+0548: ARMENIAN CAPITAL LETTER VO */
	[1280+0x09] = 0x0579,	/* U+0549: ARMENIAN CAPITAL LETTER CHA */
	[1280+0x0A] = 0x057A,	/* U+054A: ARMENIAN CAPITAL LETTER PEH */
	[1280+0x0B] = 0x057B,	/* U+054B: ARMENIAN CAPITAL LETTER JHEH */
	[1280+0x0C] = 0x057C,	/* U+054C: ARMENIAN CAPITAL LETTER RA */
	[1280+0x0D] = 0x057D,	/* U+054D: ARMENIAN CAPITAL LETTER SEH */
	[1280+0x0E] = 0x057E,	/* U+054E: ARMENIAN CAPITAL LETTER VEW */
	[1280+0x0F] = 0x057F,	/* U+054F: ARMENIAN CAPITAL LETTER TIWN */
	[1280+0x10] = 0x0580,	/* U+0550: ARMENIAN CAPITAL LETTER REH */
	[1280+0x11] = 0x0581,	/* U+0551: ARMENIAN CAPITAL LETTER CO */
	[1280+0x12] = 0x0582,	/* U+0552: ARMENIAN CAPITAL LETTER YIWN */
	[1280+0x13] = 0x0583,	/* U+0553: ARMENIAN CAPITAL LETTER PIWR */
	[1280+0x14] = 0x0584,	/* U+0554: ARMENIAN CAPITAL LETTER KEH */
	[1280+0x15] = 0x0585,	/* U+0555: ARMENIAN CAPITAL LETTER OH */
	[1280+0x16] = 0x0586,	/* U+0556: ARMENIAN CAPITAL LETTER FEH */
	[0xD6] = 1344 - 0x80,	/* 326 ... */
	[1344+0x07] = -7,	/* U+0587: ARMENIAN SMALL LIGATURE ECH YIWN */
	[0xE1] = 1408 - 0x80,	/* 341 ... */
	[1408+0x02] = 1472 - 0x80,	/* 341 202 ... */
	[1472+0x20] = 0x2D00,	/* U+10A0: GEORGIAN CAPITAL LETTER AN */
	[1472+0x21] = 0x2D01,	/* U+10A1: GEORGIAN CAPITAL LETTER BAN */
	[1472+0x22] = 0x2D02,	/* U+10A2: GEORGIAN CAPITAL LETTER GAN */
	[1472+0x23] = 0x2D03,	/* U+10A3: GEORGIAN CAPITAL LETTER DON */
	[1472+0x24] = 0x2D04,	/* U+10A4: GEORGIAN CAPITAL LETTER EN */
	[1472+0x25] = 0x2D05,	/* U+10A5: GEORGIAN CAPITAL LETTER VIN */
	[1472+0x26] = 0x2D06,	/* U+10A6: GEORGIAN CAPITAL LETTER ZEN */
	[1472+0x27] = 0x2D07,	/* U+10A7: GEORGIAN CAPITAL LETTER TAN */
	[1472+0x28] = 0x2D08,	/* U+10A8: GEORGIAN CAPITAL LETTER IN */
	[1472+0x29] = 0x2D09,	/* U+10A9: GEORGIAN CAPITAL LETTER KAN */
	[1472+0x2A] = 0x2D0A,	/* U+10AA: GEORGIAN CAPITAL LETTER LAS */
	[1472+0x2B] = 0x2D0B,	/* U+10AB: GEORGIAN CAPITAL LETTER MAN */
	[1472+0x2C] = 0x2D0C,	/* U+10AC: GEORGIAN CAPITAL LETTER NAR */
	[1472+0x2D] = 0x2D0D,	/* U+10AD: GEORGIAN CAPITAL LETTER ON */
	[1472+0x2E] = 0x2D0E,	/* U+10AE: GEORGIAN CAPITAL LETTER PAR */
	[1472+0x2F] = 0x2D0F,	/* U+10AF: GEORGIAN CAPITAL LETTER ZHAR */
	[1472+0x30] = 0x2D10,	/* U+10B0: GEORGIAN CAPITAL LETTER RAE */
	[1472+0x31] = 0x2D11,	/* U+10B1: GEORGIAN CAPITAL LETTER SAN */
	[1472+0x32] = 0x2D12,	/* U+10B2: GEORGIAN CAPITAL LETTER TAR */
	[1472+0x33] = 0x2D13,	/* U+10B3: GEORGIAN CAPITAL LETTER UN */
	[1472+0x34] = 0x2D14,	/* U+10B4: GEORGIAN CAPITAL LETTER PHAR */
	[1472+0x35] = 0x2D15,	/* U+10B5: GEORGIAN CAPITAL LETTER KHAR */
	[1472+0x36] = 0x2D16,	/* U+10B6: GEORGIAN CAPITAL LETTER GHAN */
	[1472+0x37] = 0x2D17,	/* U+10B7: GEORGIAN CAPITAL LETTER QAR */
	[1472+0x38] = 0x2D18,	/* U+10B8: GEORGIAN CAPITAL LETTER SHIN */
	[1472+0x39] = 0x2D19,	/* U+10B9: GEORGIAN CAPITAL LETTER CHIN */
	[1472+0x3A] = 0x2D1A,	/* U+10BA: GEORGIAN CAPITAL LETTER CAN */
	[1472+0x3B] = 0x2D1B,	/* U+10BB: GEORGIAN CAPITAL LETTER JIL */
	[1472+0x3C] = 0x2D1C,	/* U+10BC: GEORGIAN CAPITAL LETTER CIL */
	[1472+0x3D] = 0x2D1D,	/* U+10BD: GEORGIAN CAPITAL LETTER CHAR */
	[1472+0x3E] = 0x2D1E,	/* U+10BE: GEORGIAN CAPITAL LETTER XAN */
	[1472+0x3F] = 0x2D1F,	/* U+10BF: GEORGIAN CAPITAL LETTER JHAN */
	[1408+0x03] = 1536 - 0x80,	/* 341 203 ... */
	[1536+0x00] = 0x2D20,	/* U+10C0: GEORGIAN CAPITAL LETTER HAE */
	[1536+0x01] = 0x2D21,	/* U+10C1: GEORGIAN CAPITAL LETTER HE */
	[1536+0x02] = 0x2D22,	/* U+10C2: GEORGIAN CAPITAL LETTER HIE */
	[1536+0x03] = 0x2D23,	/* U+10C3: GEORGIAN CAPITAL LETTER WE */
	[1536+0x04] = 0x2D24,	/* U+10C4: GEORGIAN CAPITAL LETTER HAR */
	[1536+0x05] = 0x2D25,	/* U+10C5: GEORGIAN CAPITAL LETTER HOE */
	[1536+0x07] = 0x2D27,	/* U+10C7: GEORGIAN CAPITAL LETTER YN */
	[1536+0x0D] = 0x2D2D,	/* U+10CD: GEORGIAN CAPITAL LETTER AEN */
	[1408+0x0F] = 1600 - 0x80,	/* 341 217 ... */
	[1600+0x38] = 0x13F0,	/* U+13F8: CHEROKEE SMALL LETTER YE */
	[1600+0x39] = 0x13F1,	/* U+13F9: CHEROKEE SMALL LETTER YI */
	[1600+0x3A] = 0x13F2,	/* U+13FA: CHEROKEE SMALL LETTER YO */
	[1600+0x3B] = 0x13F3,	/* U+13FB: CHEROKEE SMALL LETTER YU */
	[1600+0x3C] = 0x13F4,	/* U+13FC: CHEROKEE SMALL LETTER YV */
	[1600+0x3D] = 0x13F5,	/* U+13FD: CHEROKEE SMALL LETTER MV */
	[1408+0x32] = 1664 - 0x80,	/* 341 262 ... */
	[1664+0x00] = 0x0432,	/* U+1C80: CYRILLIC SMALL LETTER ROUNDED VE */
	[1664+0x01] = 0x0434,	/* U+1C81: CYRILLIC SMALL LETTER LONG-LEGGED DE */
	[1664+0x02] = 0x043E,	/* U+1C82: CYRILLIC SMALL LETTER NARROW O */
	[1664+0x03] = 0x0441,	/* U+1C83: CYRILLIC SMALL LETTER WIDE ES */
	[1664+0x04] = 0x0442,	/* U+1C84: CYRILLIC SMALL LETTER TALL TE */
	[1664+0x05] = 0x0442,	/* U+1C85: CYRILLIC SMALL LETTER THREE-LEGGED TE */
	[1664+0x06] = 0x044A,	/* U+1C86: CYRILLIC SMALL LETTER TALL HARD SIGN */
	[1664+0x07] = 0x0463,	/* U+1C87: CYRILLIC SMALL LETTER TALL YAT */
	[1664+0x08] = 0xA64B,	/* U+1C88: CYRILLIC SMALL LETTER UNBLENDED UK */
	[1664+0x10] = 0x10D0,	/* U+1C90: GEORGIAN MTAVRULI CAPITAL LETTER AN */
	[1664+0x11] = 0x10D1,	/* U+1C91: GEORGIAN MTAVRULI CAPITAL LETTER BAN */
	[1664+0x12] = 0x10D2,	/* U+1C92: GEORGIAN MTAVRULI CAPITAL LETTER GAN */
	[1664+0x13] = 0x10D3,	/* U+1C93: GEORGIAN MTAVRULI CAPITAL LETTER DON */
	[1664+0x14] = 0x10D4,	/* U+1C94: GEORGIAN MTAVRULI CAPITAL LETTER EN */
	[1664+0x15] = 0x10D5,	/* U+1C95: GEORGIAN MTAVRULI CAPITAL LETTER VIN */
	[1664+0x16] = 0x10D6,	/* U+1C96: GEORGIAN MTAVRULI CAPITAL LETTER ZEN */
	[1664+0x17] = 0x10D7,	/* U+1C97: GEORGIAN MTAVRULI CAPITAL LETTER TAN */
	[1664+0x18] = 0x10D8,	/* U+1C98: GEORGIAN MTAVRULI CAPITAL LETTER IN */
	[1664+0x19] = 0x10D9,	/* U+1C99: GEORGIAN MTAVRULI CAPITAL LETTER KAN */
	[1664+0x1A] = 0x10DA,	/* U+1C9A: GEORGIAN MTAVRULI CAPITAL LETTER LAS */
	[1664+0x1B] = 0x10DB,	/* U+1C9B: GEORGIAN MTAVRULI CAPITAL LETTER MAN */
	[1664+0x1C] = 0x10DC,	/* U+1C9C: GEORGIAN MTAVRULI CAPITAL LETTER NAR */
	[1664+0x1D] = 0x10DD,	/* U+1C9D: GEORGIAN MTAVRULI CAPITAL LETTER ON */
	[1664+0x1E] = 0x10DE,	/* U+1C9E: GEORGIAN MTAVRULI CAPITAL LETTER PAR */
	[1664+0x1F] = 0x10DF,	/* U+1C9F: GEORGIAN MTAVRULI CAPITAL LETTER ZHAR */
	[1664+0x20] = 0x10E0,	/* U+1CA0: GEORGIAN MTAVRULI CAPITAL LETTER RAE */
	[1664+0x21] = 0x10E1,	/* U+1CA1: GEORGIAN MTAVRULI CAPITAL LETTER SAN */
	[1664+0x22] = 0x10E2,	/* U+1CA2: GEORGIAN MTAVRULI CAPITAL LETTER TAR */
	[1664+0x23] = 0x10E3,	/* U+1CA3: GEORGIAN MTAVRULI CAPITAL LETTER UN */
	[1664+0x24] = 0x10E4,	/* U+1CA4: GEORGIAN MTAVRULI CAPITAL LETTER PHAR */
	[1664+0x25] = 0x10E5,	/* U+1CA5: GEORGIAN MTAVRULI CAPITAL LETTER KHAR */
	[1664+0x26] = 0x10E6,	/* U+1CA6: GEORGIAN MTAVRULI CAPITAL LETTER GHAN */
	[1664+0x27] = 0x10E7,	/* U+1CA7: GEORGIAN MTAVRULI CAPITAL LETTER QAR */
	[1664+0x28] = 0x10E8,	/* U+1CA8: GEORGIAN MTAVRULI CAPITAL LETTER SHIN */
	[1664+0x29] = 0x10E9,	/* U+1CA9: GEORGIAN MTAVRULI CAPITAL LETTER CHIN */
	[1664+0x2A] = 0x10EA,	/* U+1CAA: GEORGIAN MTAVRULI CAPITAL LETTER CAN */
	[1664+0x2B] = 0x10EB,	/* U+1CAB: GEORGIAN MTAVRULI CAPITAL LETTER JIL */
	[1664+0x2C] = 0x10EC,	/* U+1CAC: GEORGIAN MTAVRULI CAPITAL LETTER CIL */
	[1664+0x2D] = 0x10ED,	/* U+1CAD: GEORGIAN MTAVRULI CAPITAL LETTER CHAR */
	[1664+0x2E] = 0x10EE,	/* U+1CAE: GEORGIAN MTAVRULI CAPITAL LETTER XAN */
	[1664+0x2F] = 0x10EF,	/* U+1CAF: GEORGIAN MTAVRULI CAPITAL LETTER JHAN */
	[1664+0x30] = 0x10F0,	/* U+1CB0: GEORGIAN MTAVRULI CAPITAL LETTER HAE */
	[1664+0x31] = 0x10F1,	/* U+1CB1: GEORGIAN MTAVRULI CAPITAL LETTER HE */
	[1664+0x32] = 0x10F2,	/* U+1CB2: GEORGIAN MTAVRULI CAPITAL LETTER HIE */
	[1664+0x33] = 0x10F3,	/* U+1CB3: GEORGIAN MTAVRULI CAPITAL LETTER WE */
	[1664+0x34] = 0x10F4,	/* U+1CB4: GEORGIAN MTAVRULI CAPITAL LETTER HAR */
	[1664+0x35] = 0x10F5,	/* U+1CB5: GEORGIAN MTAVRULI CAPITAL LETTER HOE */
	[1664+0x36] = 0x10F6,	/* U+1CB6: GEORGIAN MTAVRULI CAPITAL LETTER FI */
	[1664+0x37] = 0x10F7,	/* U+1CB7: GEORGIAN MTAVRULI CAPITAL LETTER YN */
	[1664+0x38] = 0x10F8,	/* U+1CB8: GEORGIAN MTAVRULI CAPITAL LETTER ELIFI */
	[1664+0x39] = 0x10F9,	/* U+1CB9: GEORGIAN MTAVRULI CAPITAL LETTER TURNED GAN */
	[1664+0x3A] = 0x10FA,	/* U+1CBA: GEORGIAN MTAVRULI CAPITAL LETTER AIN */
	[1664+0x3D] = 0x10FD,	/* U+1CBD: GEORGIAN MTAVRULI CAPITAL LETTER AEN */
	[1664+0x3E] = 0x10FE,	/* U+1CBE: GEORGIAN MTAVRULI CAPITAL LETTER HARD SIGN */
	[1664+0x3F] = 0x10FF,	/* U+1CBF: GEORGIAN MTAVRULI CAPITAL LETTER LABIAL SIGN */
	[1408+0x38] = 1728 - 0x80,	/* 341 270 ... */
	[1728+0x00] = 0x1E01,	/* U+1E00: LATIN CAPITAL LETTER A WITH RING BELOW */
	[1728+0x02] = 0x1E03,	/* U+1E02: LATIN CAPITAL LETTER B WITH DOT ABOVE */
	[1728+0x04] = 0x1E05,	/* U+1E04: LATIN CAPITAL LETTER B WITH DOT BELOW */
	[1728+0x06] = 0x1E07,	/* U+1E06: LATIN CAPITAL LETTER B WITH LINE BELOW */
	[1728+0x08] = 0x1E09,	/* U+1E08: LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE */
	[1728+0x0A] = 0x1E0B,	/* U+1E0A: LATIN CAPITAL LETTER D WITH DOT ABOVE */
	[1728+0x0C] = 0x1E0D,	/* U+1E0C: LATIN CAPITAL LETTER D WITH DOT BELOW */
	[1728+0x0E] = 0x1E0F,	/* U+1E0E: LATIN CAPITAL LETTER D WITH LINE BELOW */
	[1728+0x10] = 0x1E11,	/* U+1E10: LATIN CAPITAL LETTER D WITH CEDILLA */
	[1728+0x12] = 0x1E13,	/* U+1E12: LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW */
	[1728+0x14] = 0x1E15,	/* U+1E14: LATIN CAPITAL LETTER E WITH MACRON AND GRAVE */
	[1728+0x16] = 0x1E17,	/* U+1E16: LATIN CAPITAL LETTER E WITH MACRON AND ACUTE */
	[1728+0x18] = 0x1E19,	/* U+1E18: LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW */
	[1728+0x1A] = 0x1E1B,	/* U+1E1A: LATIN CAPITAL LETTER E WITH TILDE BELOW */
	[1728+0x1C] = 0x1E1D,	/* U+1E1C: LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE */
	[1728+0x1E] = 0x1E1F,	/* U+1E1E: LATIN CAPITAL LETTER F WITH DOT ABOVE */
	[1728+0x20] = 0x1E21,	/* U+1E20: LATIN CAPITAL LETTER G WITH MACRON */
	[1728+0x22] = 0x1E23,	/* U+1E22: LATIN CAPITAL LETTER H WITH DOT ABOVE */
	[1728+0x24] = 0x1E25,	/* U+1E24: LATIN CAPITAL LETTER H WITH DOT BELOW */
	[1728+0x26] = 0x1E27,	/* U+1E26: LATIN CAPITAL LETTER H WITH DIAERESIS */
	[1728+0x28] = 0x1E29,	/* U+1E28: LATIN CAPITAL LETTER H WITH CEDILLA */
	[1728+0x2A] = 0x1E2B,	/* U+1E2A: LATIN CAPITAL LETTER H WITH BREVE BELOW */
	[1728+0x2C] = 0x1E2D,	/* U+1E2C: LATIN CAPITAL LETTER I WITH TILDE BELOW */
	[1728+0x2E] = 0x1E2F,	/* U+1E2E: LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE */
	[1728+0x30] = 0x1E31,	/* U+1E30: LATIN CAPITAL LETTER K WITH ACUTE */
	[1728+0x32] = 0x1E33,	/* U+1E32: LATIN CAPITAL LETTER K WITH DOT BELOW */
	[1728+0x34] = 0x1E35,	/* U+1E34: LATIN CAPITAL LETTER K WITH LINE BELOW */
	[1728+0x36] = 0x1E37,	/* U+1E36: LATIN CAPITAL LETTER L WITH DOT BELOW */
	[1728+0x38] = 0x1E39,	/* U+1E38: LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON */
	[1728+0x3A] = 0x1E3B,	/* U+1E3A: LATIN CAPITAL LETTER L WITH LINE BELOW */
	[1728+0x3C] = 0x1E3D,	/* U+1E3C: LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW */
	[1728+0x3E] = 0x1E3F,	/* U+1E3E: LATIN CAPITAL LETTER M WITH ACUTE */
	[1408+0x39] = 1792 - 0x80,	/* 341 271 ... */
	[1792+0x00] = 0x1E41,	/* U+1E40: LATIN CAPITAL LETTER M WITH DOT ABOVE */
	[1792+0x02] = 0x1E43,	/* U+1E42: LATIN CAPITAL LETTER M WITH DOT BELOW */
	[1792+0x04] = 0x1E45,	/* U+1E44: LATIN CAPITAL LETTER N WITH DOT ABOVE */
	[1792+0x06] = 0x1E47,	/* U+1E46: LATIN CAPITAL LETTER N WITH DOT BELOW */
	[1792+0x08] = 0x1E49,	/* U+1E48: LATIN CAPITAL LETTER N WITH LINE BELOW */
	[1792+0x0A] = 0x1E4B,	/* U+1E4A: LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW */
	[1792+0x0C] = 0x1E4D,	/* U+1E4C: LATIN CAPITAL LETTER O WITH TILDE AND ACUTE */
	[1792+0x0E] = 0x1E4F,	/* U+1E4E: LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS */
	[1792+0x10] = 0x1E51,	/* U+1E50: LATIN CAPITAL LETTER O WITH MACRON AND GRAVE */
	[1792+0x12] = 0x1E53,	/* U+1E52: LATIN CAPITAL LETTER O WITH MACRON AND ACUTE */
	[1792+0x14] = 0x1E55,	/* U+1E54: LATIN CAPITAL LETTER P WITH ACUTE */
	[1792+0x16] = 0x1E57,	/* U+1E56: LATIN CAPITAL LETTER P WITH DOT ABOVE */
	[1792+0x18] = 0x1E59,	/* U+1E58: LATIN CAPITAL LETTER R WITH DOT ABOVE */
	[1792+0x1A] = 0x1E5B,	/* U+1E5A: LATIN CAPITAL LETTER R WITH DOT BELOW */
	[1792+0x1C] = 0x1E5D,	/* U+1E5C: LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON */
	[1792+0x1E] = 0x1E5F,	/* U+1E5E: LATIN CAPITAL LETTER R WITH LINE BELOW */
	[1792+0x20] = 0x1E61,	/* U+1E60: LATIN CAPITAL LETTER S WITH DOT ABOVE */
	[1792+0x22] = 0x1E63,	/* U+1E62: LATIN CAPITAL LETTER S WITH DOT BELOW */
	[1792+0x24] = 0x1E65,	/* U+1E64: LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE */
	[1792+0x26] = 0x1E67,	/* U+1E66: LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE */
	[1792+0x28] = 0x1E69,	/* U+1E68: LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE */
	[1792+0x2A] = 0x1E6B,	/* U+1E6A: LATIN CAPITAL LETTER T WITH DOT ABOVE */
	[1792+0x2C] = 0x1E6D,	/* U+1E6C: LATIN CAPITAL LETTER T WITH DOT BELOW */
	[1792+0x2E] = 0x1E6F,	/* U+1E6E: LATIN CAPITAL LETTER T WITH LINE BELOW */
	[1792+0x30] = 0x1E71,	/* U+1E70: LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW */
	[1792+0x32] = 0x1E73,	/* U+1E72: LATIN CAPITAL LETTER U WITH DIAERESIS BELOW */
	[1792+0x34] = 0x1E75,	/* U+1E74: LATIN CAPITAL LETTER U WITH TILDE BELOW */
	[1792+0x36] = 0x1E77,	/* U+1E76: LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW */
	[1792+0x38] = 0x1E79,	/* U+1E78: LATIN CAPITAL LETTER U WITH TILDE AND ACUTE */
	[1792+0x3A] = 0x1E7B,	/* U+1E7A: LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS */
	[1792+0x3C] = 0x1E7D,	/* U+1E7C: LATIN CAPITAL LETTER V WITH TILDE */
	[1792+0x3E] = 0x1E7F,	/* U+1E7E: LATIN CAPITAL LETTER V WITH DOT BELOW */
	[1408+0x3A] = 1856 - 0x80,	/* 341 272 ... */
	[1856+0x00] = 0x1E81,	/* U+1E80: LATIN CAPITAL LETTER W WITH GRAVE */
	[1856+0x02] = 0x1E83,	/* U+1E82: LATIN CAPITAL LETTER W WITH ACUTE */
	[1856+0x04] = 0x1E85,	/* U+1E84: LATIN CAPITAL LETTER W WITH DIAERESIS */
	[1856+0x06] = 0x1E87,	/* U+1E86: LATIN CAPITAL LETTER W WITH DOT ABOVE */
	[1856+0x08] = 0x1E89,	/* U+1E88: LATIN CAPITAL LETTER W WITH DOT BELOW */
	[1856+0x0A] = 0x1E8B,	/* U+1E8A: LATIN CAPITAL LETTER X WITH DOT ABOVE */
	[1856+0x0C] = 0x1E8D,	/* U+1E8C: LATIN CAPITAL LETTER X WITH DIAERESIS */
	[1856+0x0E] = 0x1E8F,	/* U+1E8E: LATIN CAPITAL LETTER Y WITH DOT ABOVE */
	[1856+0x10] = 0x1E91,	/* U+1E90: LATIN CAPITAL LETTER Z WITH CIRCUMFLEX */
	[1856+0x12] = 0x1E93,	/* U+1E92: LATIN CAPITAL LETTER Z WITH DOT BELOW */
	[1856+0x14] = 0x1E95,	/* U+1E94: LATIN CAPITAL LETTER Z WITH LINE BELOW */
	[1856+0x16] = -8,	/* U+1E96: LATIN SMALL LETTER H WITH LINE BELOW */
	[1856+0x17] = -9,	/* U+1E97: LATIN SMALL LETTER T WITH DIAERESIS */
	[1856+0x18] = -10,	/* U+1E98: LATIN SMALL LETTER W WITH RING ABOVE */
	[1856+0x19] = -11,	/* U+1E99: LATIN SMALL LETTER Y WITH RING ABOVE */
	[1856+0x1A] = -12,	/* U+1E9A: LATIN SMALL LETTER A WITH RIGHT HALF RING */
	[1856+0x1B] = 0x1E61,	/* U+1E9B: LATIN SMALL LETTER LONG S WITH DOT ABOVE */
	[1856+0x1E] = -1,	/* U+1E9E: LATIN CAPITAL LETTER SHARP S */
	[1856+0x20] = 0x1EA1,	/* U+1EA0: LATIN CAPITAL LETTER A WITH DOT BELOW */
	[1856+0x22] = 0x1EA3,	/* U+1EA2: LATIN CAPITAL LETTER A WITH HOOK ABOVE */
	[1856+0x24] = 0x1EA5,	/* U+1EA4: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE */
	[1856+0x26] = 0x1EA7,	/* U+1EA6: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE */
	[1856+0x28] = 0x1EA9,	/* U+1EA8: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE */
	[1856+0x2A] = 0x1EAB,	/* U+1EAA: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE */
	[1856+0x2C] = 0x1EAD,	/* U+1EAC: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW */
	[1856+0x2E] = 0x1EAF,	/* U+1EAE: LATIN CAPITAL LETTER A WITH BREVE AND ACUTE */
	[1856+0x30] = 0x1EB1,	/* U+1EB0: LATIN CAPITAL LETTER A WITH BREVE AND GRAVE */
	[1856+0x32] = 0x1EB3,	/* U+1EB2: LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE */
	[1856+0x34] = 0x1EB5,	/* U+1EB4: LATIN CAPITAL LETTER A WITH BREVE AND TILDE */
	[1856+0x36] = 0x1EB7,	/* U+1EB6: LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW */
	[1856+0x38] = 0x1EB9,	/* U+1EB8: LATIN CAPITAL LETTER E WITH DOT BELOW */
	[1856+0x3A] = 0x1EBB,	/* U+1EBA: LATIN CAPITAL LETTER E WITH HOOK ABOVE */
	[1856+0x3C] = 0x1EBD,	/* U+1EBC: LATIN CAPITAL LETTER E WITH TILDE */
	[1856+0x3E] = 0x1EBF,	/* U+1EBE: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE */
	[1408+0x3B] = 1920 - 0x80,	/* 341 273 ... */
	[1920+0x00] = 0x1EC1,	/* U+1EC0: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[1920+0x02] = 0x1EC3,	/* U+1EC2: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[1920+0x04] = 0x1EC5,	/* U+1EC4: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE */
	[1920+0x06] = 0x1EC7,	/* U+1EC6: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[1920+0x08] = 0x1EC9,	/* U+1EC8: LATIN CAPITAL LETTER I WITH HOOK ABOVE */
	[1920+0x0A] = 0x1ECB,	/* U+1ECA: LATIN CAPITAL LETTER I WITH DOT BELOW */
	[1920+0x0C] = 0x1ECD,	/* U+1ECC: LATIN CAPITAL LETTER O WITH DOT BELOW */
	[1920+0x0E] = 0x1ECF,	/* U+1ECE: LATIN CAPITAL LETTER O WITH HOOK ABOVE */
	[1920+0x10] = 0x1ED1,	/* U+1ED0: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE */
	[1920+0x12] = 0x1ED3,	/* U+1ED2: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE */
	[1920+0x14] = 0x1ED5,	/* U+1ED4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE */
	[1920+0x16] = 0x1ED7,	/* U+1ED6: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE */
	[1920+0x18] = 0x1ED9,	/* U+1ED8: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW */
	[1920+0x1A] = 0x1EDB,	/* U+1EDA: LATIN CAPITAL LETTER O WITH HORN AND ACUTE */
	[1920+0x1C] = 0x1EDD,	/* U+1EDC: LATIN CAPITAL LETTER O WITH HORN AND GRAVE */
	[1920+0x1E] = 0x1EDF,	/* U+1EDE: LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE */
	[1920+0x20] = 0x1EE1,	/* U+1EE0: LATIN CAPITAL LETTER O WITH HORN AND TILDE */
	[1920+0x22] = 0x1EE3,	/* U+1EE2: LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW */
	[1920+0x24] = 0x1EE5,	/* U+1EE4: LATIN CAPITAL LETTER U WITH DOT BELOW */
	[1920+0x26] = 0x1EE7,	/* U+1EE6: LATIN CAPITAL LETTER U WITH HOOK ABOVE */
	[1920+0x28] = 0x1EE9,	/* U+1EE8: LATIN CAPITAL LETTER U WITH HORN AND ACUTE */
	[1920+0x2A] = 0x1EEB,	/* U+1EEA: LATIN CAPITAL LETTER U WITH HORN AND GRAVE */
	[1920+0x2C] = 0x1EED,	/* U+1EEC: LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE */
	[1920+0x2E] = 0x1EEF,	/* U+1EEE: LATIN CAPITAL LETTER U WITH HORN AND TILDE */
	[1920+0x30] = 0x1EF1,	/* U+1EF0: LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW */
	[1920+0x32] = 0x1EF3,	/* U+1EF2: LATIN CAPITAL LETTER Y WITH GRAVE */
	[1920+0x34] = 0x1EF5,	/* U+1EF4: LATIN CAPITAL LETTER Y WITH DOT BELOW */
	[1920+0x36] = 0x1EF7,	/* U+1EF6: LATIN CAPITAL LETTER Y WITH HOOK ABOVE */
	[1920+0x38] = 0x1EF9,	/* U+1EF8: LATIN CAPITAL LETTER Y WITH TILDE */
	[1920+0x3A] = 0x1EFB,	/* U+1EFA: LATIN CAPITAL LETTER MIDDLE-WELSH LL */
	[1920+0x3C] = 0x1EFD,	/* U+1EFC: LATIN CAPITAL LETTER MIDDLE-WELSH V */
	[1920+0x3E] = 0x1EFF,	/* U+1EFE: LATIN CAPITAL LETTER Y WITH LOOP */
	[1408+0x3C] = 1984 - 0x80,	/* 341 274 ... */
	[1984+0x08] = 0x1F00,	/* U+1F08: GREEK CAPITAL LETTER ALPHA WITH PSILI */
	[1984+0x09] = 0x1F01,	/* U+1F09: GREEK CAPITAL LETTER ALPHA WITH DASIA */
	[1984+0x0A] = 0x1F02,	/* U+1F0A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA */
	[1984+0x0B] = 0x1F03,	/* U+1F0B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA */
	[1984+0x0C] = 0x1F04,	/* U+1F0C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA */
	[1984+0x0D] = 0x1F05,	/* U+1F0D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA */
	[1984+0x0E] = 0x1F06,	/* U+1F0E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI */
	[1984+0x0F] = 0x1F07,	/* U+1F0F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI */
	[1984+0x18] = 0x1F10,	/* U+1F18: GREEK CAPITAL LETTER EPSILON WITH PSILI */
	[1984+0x19] = 0x1F11,	/* U+1F19: GREEK CAPITAL LETTER EPSILON WITH DASIA */
	[1984+0x1A] = 0x1F12,	/* U+1F1A: GREEK CAPITAL LETTER EPSILON WITH PSILI AND VARIA */
	[1984+0x1B] = 0x1F13,	/* U+1F1B: GREEK CAPITAL LETTER EPSILON WITH DASIA AND VARIA */
	[1984+0x1C] = 0x1F14,	/* U+1F1C: GREEK CAPITAL LETTER EPSILON WITH PSILI AND OXIA */
	[1984+0x1D] = 0x1F15,	/* U+1F1D: GREEK CAPITAL LETTER EPSILON WITH DASIA AND OXIA */
	[1984+0x28] = 0x1F20,	/* U+1F28: GREEK CAPITAL LETTER ETA WITH PSILI */
	[1984+0x29] = 0x1F21,	/* U+1F29: GREEK CAPITAL LETTER ETA WITH DASIA */
	[1984+0x2A] = 0x1F22,	/* U+1F2A: GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA */
	[1984+0x2B] = 0x1F23,	/* U+1F2B: GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA */
	[1984+0x2C] = 0x1F24,	/* U+1F2C: GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA */
	[1984+0x2D] = 0x1F25,	/* U+1F2D: GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA */
	[1984+0x2E] = 0x1F26,	/* U+1F2E: GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI */
	[1984+0x2F] = 0x1F27,	/* U+1F2F: GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI */
	[1984+0x38] = 0x1F30,	/* U+1F38: GREEK CAPITAL LETTER IOTA WITH PSILI */
	[1984+0x39] = 0x1F31,	/* U+1F39: GREEK CAPITAL LETTER IOTA WITH DASIA */
	[1984+0x3A] = 0x1F32,	/* U+1F3A: GREEK CAPITAL LETTER IOTA WITH PSILI AND VARIA */
	[1984+0x3B] = 0x1F33,	/* U+1F3B: GREEK CAPITAL LETTER IOTA WITH DASIA AND VARIA */
	[1984+0x3C] = 0x1F34,	/* U+1F3C: GREEK CAPITAL LETTER IOTA WITH PSILI AND OXIA */
	[1984+0x3D] = 0x1F35,	/* U+1F3D: GREEK CAPITAL LETTER IOTA WITH DASIA AND OXIA */
	[1984+0x3E] = 0x1F36,	/* U+1F3E: GREEK CAPITAL LETTER IOTA WITH PSILI AND PERISPOMENI */
	[1984+0x3F] = 0x1F37,	/* U+1F3F: GREEK CAPITAL LETTER IOTA WITH DASIA AND PERISPOMENI */
	[1408+0x3D] = 2048 - 0x80,	/* 341 275 ... */
	[2048+0x08] = 0x1F40,	/* U+1F48: GREEK CAPITAL LETTER OMICRON WITH PSILI */
	[2048+0x09] = 0x1F41,	/* U+1F49: GREEK CAPITAL LETTER OMICRON WITH DASIA */
	[2048+0x0A] = 0x1F42,	/* U+1F4A: GREEK CAPITAL LETTER OMICRON WITH PSILI AND VARIA */
	[2048+0x0B] = 0x1F43,	/* U+1F4B: GREEK CAPITAL LETTER OMICRON WITH DASIA AND VARIA */
	[2048+0x0C] = 0x1F44,	/* U+1F4C: GREEK CAPITAL LETTER OMICRON WITH PSILI AND OXIA */
	[2048+0x0D] = 0x1F45,	/* U+1F4D: GREEK CAPITAL LETTER OMICRON WITH DASIA AND OXIA */
	[2048+0x10] = -13,	/* U+1F50: GREEK SMALL LETTER UPSILON WITH PSILI */
	[2048+0x12] = -14,	/* U+1F52: GREEK SMALL LETTER UPSILON WITH PSILI AND VARIA */
	[2048+0x14] = -15,	/* U+1F54: GREEK SMALL LETTER UPSILON WITH PSILI AND OXIA */
	[2048+0x16] = -16,	/* U+1F56: GREEK SMALL LETTER UPSILON WITH PSILI AND PERISPOMENI */
	[2048+0x19] = 0x1F51,	/* U+1F59: GREEK CAPITAL LETTER UPSILON WITH DASIA */
	[2048+0x1B] = 0x1F53,	/* U+1F5B: GREEK CAPITAL LETTER UPSILON WITH DASIA AND VARIA */
	[2048+0x1D] = 0x1F55,	/* U+1F5D: GREEK CAPITAL LETTER UPSILON WITH DASIA AND OXIA */
	[2048+0x1F] = 0x1F57,	/* U+1F5F: GREEK CAPITAL LETTER UPSILON WITH DASIA AND PERISPOMENI */
	[2048+0x28] = 0x1F60,	/* U+1F68: GREEK CAPITAL LETTER OMEGA WITH PSILI */
	[2048+0x29] = 0x1F61,	/* U+1F69: GREEK CAPITAL LETTER OMEGA WITH DASIA */
	[2048+0x2A] = 0x1F62,	/* U+1F6A: GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA */
	[2048+0x2B] = 0x1F63,	/* U+1F6B: GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA */
	[2048+0x2C] = 0x1F64,	/* U+1F6C: GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA */
	[2048+0x2D] = 0x1F65,	/* U+1F6D: GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA */
	[2048+0x2E] = 0x1F66,	/* U+1F6E: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI */
	[2048+0x2F] = 0x1F67,	/* U+1F6F: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI */
	[1408+0x3E] = 2112 - 0x80,	/* 341 276 ... */
	[2112+0x00] = -17,	/* U+1F80: GREEK SMALL LETTER ALPHA WITH PSILI AND YPOGEGRAMMENI */
	[2112+0x01] = -18,	/* U+1F81: GREEK SMALL LETTER ALPHA WITH DASIA AND YPOGEGRAMMENI */
	[2112+0x02] = -19,	/* U+1F82: GREEK SMALL LETTER ALPHA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2112+0x03] = -20,	/* U+1F83: GREEK SMALL LETTER ALPHA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2112+0x04] = -21,	/* U+1F84: GREEK SMALL LETTER ALPHA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2112+0x05] = -22,	/* U+1F85: GREEK SMALL LETTER ALPHA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2112+0x06] = -23,	/* U+1F86: GREEK SMALL LETTER ALPHA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x07] = -24,	/* U+1F87: GREEK SMALL LETTER ALPHA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x08] = -17,	/* U+1F88: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PROSGEGRAMMENI */
	[2112+0x09] = -18,	/* U+1F89: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PROSGEGRAMMENI */
	[2112+0x0A] = -19,	/* U+1F8A: GREEK CAPITAL LETTER ALPHA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2112+0x0B] = -20,	/* U+1F8B: GREEK CAPITAL LETTER ALPHA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2112+0x0C] = -21,	/* U+1F8C: GREEK CAPITAL LETTER ALPHA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2112+0x0D] = -22,	/* U+1F8D: GREEK CAPITAL LETTER ALPHA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2112+0x0E] = -23,	/* U+1F8E: GREEK CAPITAL LETTER ALPHA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x0F] = -24,	/* U+1F8F: GREEK CAPITAL LETTER ALPHA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x10] = -25,	/* U+1F90: GREEK SMALL LETTER ETA WITH PSILI AND YPOGEGRAMMENI */
	[2112+0x11] = -26,	/* U+1F91: GREEK SMALL LETTER ETA WITH DASIA AND YPOGEGRAMMENI */
	[2112+0x12] = -27,	/* U+1F92: GREEK SMALL LETTER ETA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2112+0x13] = -28,	/* U+1F93: GREEK SMALL LETTER ETA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2112+0x14] = -29,	/* U+1F94: GREEK SMALL LETTER ETA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2112+0x15] = -30,	/* U+1F95: GREEK SMALL LETTER ETA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2112+0x16] = -31,	/* U+1F96: GREEK SMALL LETTER ETA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x17] = -32,	/* U+1F97: GREEK SMALL LETTER ETA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x18] = -25,	/* U+1F98: GREEK CAPITAL LETTER ETA WITH PSILI AND PROSGEGRAMMENI */
	[2112+0x19] = -26,	/* U+1F99: GREEK CAPITAL LETTER ETA WITH DASIA AND PROSGEGRAMMENI */
	[2112+0x1A] = -27,	/* U+1F9A: GREEK CAPITAL LETTER ETA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2112+0x1B] = -28,	/* U+1F9B: GREEK CAPITAL LETTER ETA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2112+0x1C] = -29,	/* U+1F9C: GREEK CAPITAL LETTER ETA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2112+0x1D] = -30,	/* U+1F9D: GREEK CAPITAL LETTER ETA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2112+0x1E] = -31,	/* U+1F9E: GREEK CAPITAL LETTER ETA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x1F] = -32,	/* U+1F9F: GREEK CAPITAL LETTER ETA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x20] = -33,	/* U+1FA0: GREEK SMALL LETTER OMEGA WITH PSILI AND YPOGEGRAMMENI */
	[2112+0x21] = -34,	/* U+1FA1: GREEK SMALL LETTER OMEGA WITH DASIA AND YPOGEGRAMMENI */
	[2112+0x22] = -35,	/* U+1FA2: GREEK SMALL LETTER OMEGA WITH PSILI AND VARIA AND YPOGEGRAMMENI */
	[2112+0x23] = -36,	/* U+1FA3: GREEK SMALL LETTER OMEGA WITH DASIA AND VARIA AND YPOGEGRAMMENI */
	[2112+0x24] = -37,	/* U+1FA4: GREEK SMALL LETTER OMEGA WITH PSILI AND OXIA AND YPOGEGRAMMENI */
	[2112+0x25] = -38,	/* U+1FA5: GREEK SMALL LETTER OMEGA WITH DASIA AND OXIA AND YPOGEGRAMMENI */
	[2112+0x26] = -39,	/* U+1FA6: GREEK SMALL LETTER OMEGA WITH PSILI AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x27] = -40,	/* U+1FA7: GREEK SMALL LETTER OMEGA WITH DASIA AND PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x28] = -33,	/* U+1FA8: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PROSGEGRAMMENI */
	[2112+0x29] = -34,	/* U+1FA9: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PROSGEGRAMMENI */
	[2112+0x2A] = -35,	/* U+1FAA: GREEK CAPITAL LETTER OMEGA WITH PSILI AND VARIA AND PROSGEGRAMMENI */
	[2112+0x2B] = -36,	/* U+1FAB: GREEK CAPITAL LETTER OMEGA WITH DASIA AND VARIA AND PROSGEGRAMMENI */
	[2112+0x2C] = -37,	/* U+1FAC: GREEK CAPITAL LETTER OMEGA WITH PSILI AND OXIA AND PROSGEGRAMMENI */
	[2112+0x2D] = -38,	/* U+1FAD: GREEK CAPITAL LETTER OMEGA WITH DASIA AND OXIA AND PROSGEGRAMMENI */
	[2112+0x2E] = -39,	/* U+1FAE: GREEK CAPITAL LETTER OMEGA WITH PSILI AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x2F] = -40,	/* U+1FAF: GREEK CAPITAL LETTER OMEGA WITH DASIA AND PERISPOMENI AND PROSGEGRAMMENI */
	[2112+0x32] = -41,	/* U+1FB2: GREEK SMALL LETTER ALPHA WITH VARIA AND YPOGEGRAMMENI */
	[2112+0x33] = -42,	/* U+1FB3: GREEK SMALL LETTER ALPHA WITH YPOGEGRAMMENI */
	[2112+0x34] = -43,	/* U+1FB4: GREEK SMALL LETTER ALPHA WITH OXIA AND YPOGEGRAMMENI */
	[2112+0x36] = -44,	/* U+1FB6: GREEK SMALL LETTER ALPHA WITH PERISPOMENI */
	[2112+0x37] = -45,	/* U+1FB7: GREEK SMALL LETTER ALPHA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2112+0x38] = 0x1FB0,	/* U+1FB8: GREEK CAPITAL LETTER ALPHA WITH VRACHY */
	[2112+0x39] = 0x1FB1,	/* U+1FB9: GREEK CAPITAL LETTER ALPHA WITH MACRON */
	[2112+0x3A] = 0x1F70,	/* U+1FBA: GREEK CAPITAL LETTER ALPHA WITH VARIA */
	[2112+0x3B] = 0x1F71,	/* U+1FBB: GREEK CAPITAL LETTER ALPHA WITH OXIA */
	[2112+0x3C] = -42,	/* U+1FBC: GREEK CAPITAL LETTER ALPHA WITH PROSGEGRAMMENI */
	[2112+0x3E] = 0x03B9,	/* U+1FBE: GREEK PROSGEGRAMMENI */
	[1408+0x3F] = 2176 - 0x80,	/* 341 277 ... */
	[2176+0x02] = -46,	/* U+1FC2: GREEK SMALL LETTER ETA WITH VARIA AND YPOGEGRAMMENI */
	[2176+0x03] = -47,	/* U+1FC3: GREEK SMALL LETTER ETA WITH YPOGEGRAMMENI */
	[2176+0x04] = -48,	/* U+1FC4: GREEK SMALL LETTER ETA WITH OXIA AND YPOGEGRAMMENI */
	[2176+0x06] = -49,	/* U+1FC6: GREEK SMALL LETTER ETA WITH PERISPOMENI */
	[2176+0x07] = -50,	/* U+1FC7: GREEK SMALL LETTER ETA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2176+0x08] = 0x1F72,	/* U+1FC8: GREEK CAPITAL LETTER EPSILON WITH VARIA */
	[2176+0x09] = 0x1F73,	/* U+1FC9: GREEK CAPITAL LETTER EPSILON WITH OXIA */
	[2176+0x0A] = 0x1F74,	/* U+1FCA: GREEK CAPITAL LETTER ETA WITH VARIA */
	[2176+0x0B] = 0x1F75,	/* U+1FCB: GREEK CAPITAL LETTER ETA WITH OXIA */
	[2176+0x0C] = -47,	/* U+1FCC: GREEK CAPITAL LETTER ETA WITH PROSGEGRAMMENI */
	[2176+0x12] = -51,	/* U+1FD2: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND VARIA */
	[2176+0x13] = -5,	/* U+1FD3: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND OXIA */
	[2176+0x16] = -52,	/* U+1FD6: GREEK SMALL LETTER IOTA WITH PERISPOMENI */
	[2176+0x17] = -53,	/* U+1FD7: GREEK SMALL LETTER IOTA WITH DIALYTIKA AND PERISPOMENI */
	[2176+0x18] = 0x1FD0,	/* U+1FD8: GREEK CAPITAL LETTER IOTA WITH VRACHY */
	[2176+0x19] = 0x1FD1,	/* U+1FD9: GREEK CAPITAL LETTER IOTA WITH MACRON */
	[2176+0x1A] = 0x1F76,	/* U+1FDA: GREEK CAPITAL LETTER IOTA WITH VARIA */
	[2176+0x1B] = 0x1F77,	/* U+1FDB: GREEK CAPITAL LETTER IOTA WITH OXIA */
	[2176+0x22] = -54,	/* U+1FE2: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND VARIA */
	[2176+0x23] = -6,	/* U+1FE3: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND OXIA */
	[2176+0x24] = -55,	/* U+1FE4: GREEK SMALL LETTER RHO WITH PSILI */
	[2176+0x26] = -56,	/* U+1FE6: GREEK SMALL LETTER UPSILON WITH PERISPOMENI */
	[2176+0x27] = -57,	/* U+1FE7: GREEK SMALL LETTER UPSILON WITH DIALYTIKA AND PERISPOMENI */
	[2176+0x28] = 0x1FE0,	/* U+1FE8: GREEK CAPITAL LETTER UPSILON WITH VRACHY */
	[2176+0x29] = 0x1FE1,	/* U+1FE9: GREEK CAPITAL LETTER UPSILON WITH MACRON */
	[2176+0x2A] = 0x1F7A,	/* U+1FEA: GREEK CAPITAL LETTER UPSILON WITH VARIA */
	[2176+0x2B] = 0x1F7B,	/* U+1FEB: GREEK CAPITAL LETTER UPSILON WITH OXIA */
	[2176+0x2C] = 0x1FE5,	/* U+1FEC: GREEK CAPITAL LETTER RHO WITH DASIA */
	[2176+0x32] = -58,	/* U+1FF2: GREEK SMALL LETTER OMEGA WITH VARIA AND YPOGEGRAMMENI */
	[2176+0x33] = -59,	/* U+1FF3: GREEK SMALL LETTER OMEGA WITH YPOGEGRAMMENI */
	[2176+0x34] = -60,	/* U+1FF4: GREEK SMALL LETTER OMEGA WITH OXIA AND YPOGEGRAMMENI */
	[2176+0x36] = -61,	/* U+1FF6: GREEK SMALL LETTER OMEGA WITH PERISPOMENI */
	[2176+0x37] = -62,	/* U+1FF7: GREEK SMALL LETTER OMEGA WITH PERISPOMENI AND YPOGEGRAMMENI */
	[2176+0x38] = 0x1F78,	/* U+1FF8: GREEK CAPITAL LETTER OMICRON WITH VARIA */
	[2176+0x39] = 0x1F79,	/* U+1FF9: GREEK CAPITAL LETTER OMICRON WITH OXIA */
	[2176+0x3A] = 0x1F7C,	/* U+1FFA: GREEK CAPITAL LETTER OMEGA WITH VARIA */
	[2176+0x3B] = 0x1F7D,	/* U+1FFB: GREEK CAPITAL LETTER OMEGA WITH OXIA */
	[2176+0x3C] = -59,	/* U+1FFC: GREEK CAPITAL LETTER OMEGA WITH PROSGEGRAMMENI */
	[0xE2] = 2240 - 0x80,	/* 342 ... */
	[2240+0x04] = 2304 - 0x80,	/* 342 204 ... */
	[2304+0x26] = 0x03C9,	/* U+2126: OHM SIGN */
	[2304+0x2A] = 0x006B,	/* U+212A: KELVIN SIGN */
	[2304+0x2B] = 0x00E5,	/* U+212B: ANGSTROM SIGN */
	[2304+0x32] = 0x214E,	/* U+2132: TURNED CAPITAL F */
	[2240+0x05] = 2368 - 0x80,	/* 342 205 ... */
	[2368+0x20] = 0x2170,	/* U+2160: ROMAN NUMERAL ONE */
	[2368+0x21] = 0x2171,	/* U+2161: ROMAN NUMERAL TWO */
	[2368+0x22] = 0x2172,	/* U+2162: ROMAN NUMERAL THREE */
	[2368+0x23] = 0x2173,	/* U+2163: ROMAN NUMERAL FOUR */
	[2368+0x24] = 0x2174,	/* U+2164: ROMAN NUMERAL FIVE */
	[2368+0x25] = 0x2175,	/* U+2165: ROMAN NUMERAL SIX */
	[2368+0x26] = 0x2176,	/* U+2166: ROMAN NUMERAL SEVEN */
	[2368+0x27] = 0x2177,	/* U+2167: ROMAN NUMERAL EIGHT */
	[2368+0x28] = 0x2178,	/* U+2168: ROMAN NUMERAL NINE */
	[2368+0x29] = 0x2179,	/* U+2169: ROMAN NUMERAL TEN */
	[2368+0x2A] = 0x217A,	/* U+216A: ROMAN NUMERAL ELEVEN */
	[2368+0x2B] = 0x217B,	/* U+216B: ROMAN NUMERAL TWELVE */
	[2368+0x2C] = 0x217C,	/* U+216C: ROMAN NUMERAL FIFTY */
	[2368+0x2D] = 0x217D,	/* U+216D: ROMAN NUMERAL ONE HUNDRED */
	[2368+0x2E] = 0x217E,	/* U+216E: ROMAN NUMERAL FIVE HUNDRED */
	[2368+0x2F] = 0x217F,	/* U+216F: ROMAN NUMERAL ONE THOUSAND */
	[2240+0x06] = 2432 - 0x80,	/* 342 206 ... */
	[2432+0x03] = 0x2184,	/* U+2183: ROMAN NUMERAL REVERSED ONE HUNDRED */
	[2240+0x12] = 2496 - 0x80,	/* 342 222 ... */
	[2496+0x36] = 0x24D0,	/* U+24B6: CIRCLED LATIN CAPITAL LETTER A */
	[2496+0x37] = 0x24D1,	/* U+24B7: CIRCLED LATIN CAPITAL LETTER B */
	[2496+0x38] = 0x24D2,	/* U+24B8: CIRCLED LATIN CAPITAL LETTER C */
	[2496+0x39] = 0x24D3,	/* U+24B9: CIRCLED LATIN CAPITAL LETTER D */
	[2496+0x3A] = 0x24D4,	/* U+24BA: CIRCLED LATIN CAPITAL LETTER E */
	[2496+0x3B] = 0x24D5,	/* U+24BB: CIRCLED LATIN CAPITAL LETTER F */
	[2496+0x3C] = 0x24D6,	/* U+24BC: CIRCLED LATIN CAPITAL LETTER G */
	[2496+0x3D] = 0x24D7,	/* U+24BD: CIRCLED LATIN CAPITAL LETTER H */
	[2496+0x3E] = 0x24D8,	/* U+24BE: CIRCLED LATIN CAPITAL LETTER I */
	[2496+0x3F] = 0x24D9,	/* U+24BF: CIRCLED LATIN CAPITAL LETTER J */
	[2240+0x13] = 2560 - 0x80,	/* 342 223 ... */
	[2560+0x00] = 0x24DA,	/* U+24C0: CIRCLED LATIN CAPITAL LETTER K */
	[2560+0x01] = 0x24DB,	/* U+24C1: CIRCLED LATIN CAPITAL LETTER L */
	[2560+0x02] = 0x24DC,	/* U+24C2: CIRCLED LATIN CAPITAL LETTER M */
	[2560+0x03] = 0x24DD,	/* U+24C3: CIRCLED LATIN CAPITAL LETTER N */
	[2560+0x04] = 0x24DE,	/* U+24C4: CIRCLED LATIN CAPITAL LETTER O */
	[2560+0x05] = 0x24DF,	/* U+24C5: CIRCLED LATIN CAPITAL LETTER P */
	[2560+0x06] = 0x24E0,	/* U+24C6: CIRCLED LATIN CAPITAL LETTER Q */
	[2560+0x07] = 0x24E1,	/* U+24C7: CIRCLED LATIN CAPITAL LETTER R */
	[2560+0x08] = 0x24E2,	/* U+24C8: CIRCLED LATIN CAPITAL LETTER S */
	[2560+0x09] = 0x24E3,	/* U+24C9: CIRCLED LATIN CAPITAL LETTER T */
	[2560+0x0A] = 0x24E4,	/* U+24CA: CIRCLED LATIN CAPITAL LETTER U */
	[2560+0x0B] = 0x24E5,	/* U+24CB: CIRCLED LATIN CAPITAL LETTER V */
	[2560+0x0C] = 0x24E6,	/* U+24CC: CIRCLED LATIN CAPITAL LETTER W */
	[2560+0x0D] = 0x24E7,	/* U+24CD: CIRCLED LATIN CAPITAL LETTER X */
	[2560+0x0E] = 0x24E8,	/* U+24CE: CIRCLED LATIN CAPITAL LETTER Y */
	[2560+0x0F] = 0x24E9,	/* U+24CF: CIRCLED LATIN CAPITAL LETTER Z */
	[2240+0x30] = 2624 - 0x80,	/* 342 260 ... */
	[2624+0x00] = 0x2C30,	/* U+2C00: GLAGOLITIC CAPITAL LETTER AZU */
	[2624+0x01] = 0x2C31,	/* U+2C01: GLAGOLITIC CAPITAL LETTER BUKY */
	[2624+0x02] = 0x2C32,	/* U+2C02: GLAGOLITIC CAPITAL LETTER VEDE */
	[2624+0x03] = 0x2C33,	/* U+2C03: GLAGOLITIC CAPITAL LETTER GLAGOLI */
	[2624+0x04] = 0x2C34,	/* U+2C04: GLAGOLITIC CAPITAL LETTER DOBRO */
	[2624+0x05] = 0x2C35,	/* U+2C05: GLAGOLITIC CAPITAL LETTER YESTU */
	[2624+0x06] = 0x2C36,	/* U+2C06: GLAGOLITIC CAPITAL LETTER ZHIVETE */
	[2624+0x07] = 0x2C37,	/* U+2C07: GLAGOLITIC CAPITAL LETTER DZELO */
	[2624+0x08] = 0x2C38,	/* U+2C08: GLAGOLITIC CAPITAL LETTER ZEMLJA */
	[2624+0x09] = 0x2C39,	/* U+2C09: GLAGOLITIC CAPITAL LETTER IZHE */
	[2624+0x0A] = 0x2C3A,	/* U+2C0A: GLAGOLITIC CAPITAL LETTER INITIAL IZHE */
	[2624+0x0B] = 0x2C3B,	/* U+2C0B: GLAGOLITIC CAPITAL LETTER I */
	[2624+0x0C] = 0x2C3C,	/* U+2C0C: GLAGOLITIC CAPITAL LETTER DJERVI */
	[2624+0x0D] = 0x2C3D,	/* U+2C0D: GLAGOLITIC CAPITAL LETTER KAKO */
	[2624+0x0E] = 0x2C3E,	/* U+2C0E: GLAGOLITIC CAPITAL LETTER LJUDIJE */
	[2624+0x0F] = 0x2C3F,	/* U+2C0F: GLAGOLITIC CAPITAL LETTER MYSLITE */
	[2624+0x10] = 0x2C40,	/* U+2C10: GLAGOLITIC CAPITAL LETTER NASHI */
	[2624+0x11] = 0x2C41,	/* U+2C11: GLAGOLITIC CAPITAL LETTER ONU */
	[2624+0x12] = 0x2C42,	/* U+2C12: GLAGOLITIC CAPITAL LETTER POKOJI */
	[2624+0x13] = 0x2C43,	/* U+2C13: GLAGOLITIC CAPITAL LETTER RITSI */
	[2624+0x14] = 0x2C44,	/* U+2C14: GLAGOLITIC CAPITAL LETTER SLOVO */
	[2624+0x15] = 0x2C45,	/* U+2C15: GLAGOLITIC CAPITAL LETTER TVRIDO */
	[2624+0x16] = 0x2C46,	/* U+2C16: GLAGOLITIC CAPITAL LETTER UKU */
	[2624+0x17] = 0x2C47,	/* U+2C17: GLAGOLITIC CAPITAL LETTER FRITU */
	[2624+0x18] = 0x2C48,	/* U+2C18: GLAGOLITIC CAPITAL LETTER HERU */
	[2624+0x19] = 0x2C49,	/* U+2C19: GLAGOLITIC CAPITAL LETTER OTU */
	[2624+0x1A] = 0x2C4A,	/* U+2C1A: GLAGOLITIC CAPITAL LETTER PE */
	[2624+0x1B] = 0x2C4B,	/* U+2C1B: GLAGOLITIC CAPITAL LETTER SHTA */
	[2624+0x1C] = 0x2C4C,	/* U+2C1C: GLAGOLITIC CAPITAL LETTER TSI */
	[2624+0x1D] = 0x2C4D,	/* U+2C1D: GLAGOLITIC CAPITAL LETTER CHRIVI */
	[2624+0x1E] = 0x2C4E,	/* U+2C1E: GLAGOLITIC CAPITAL LETTER SHA */
	[2624+0x1F] = 0x2C4F,	/* U+2C1F: GLAGOLITIC CAPITAL LETTER YERU */
	[2624+0x20] = 0x2C50,	/* U+2C20: GLAGOLITIC CAPITAL LETTER YERI */
	[2624+0x21] = 0x2C51,	/* U+2C21: GLAGOLITIC CAPITAL LETTER YATI */
	[2624+0x22] = 0x2C52,	/* U+2C22: GLAGOLITIC CAPITAL LETTER SPIDERY HA */
	[2624+0x23] = 0x2C53,	/* U+2C23: GLAGOLITIC CAPITAL LETTER YU */
	[2624+0x24] = 0x2C54,	/* U+2C24: GLAGOLITIC CAPITAL LETTER SMALL YUS */
	[2624+0x25] = 0x2C55,	/* U+2C25: GLAGOLITIC CAPITAL LETTER SMALL YUS WITH TAIL */
	[2624+0x26] = 0x2C56,	/* U+2C26: GLAGOLITIC CAPITAL LETTER YO */
	[2624+0x27] = 0x2C57,	/* U+2C27: GLAGOLITIC CAPITAL LETTER IOTATED SMALL YUS */
	[2624+0x28] = 0x2C58,	/* U+2C28: GLAGOLITIC CAPITAL LETTER BIG YUS */
	[2624+0x29] = 0x2C59,	/* U+2C29: GLAGOLITIC CAPITAL LETTER IOTATED BIG YUS */
	[2624+0x2A] = 0x2C5A,	/* U+2C2A: GLAGOLITIC CAPITAL LETTER FITA */
	[2624+0x2B] = 0x2C5B,	/* U+2C2B: GLAGOLITIC CAPITAL LETTER IZHITSA */
	[2624+0x2C] = 0x2C5C,	/* U+2C2C: GLAGOLITIC CAPITAL LETTER SHTAPIC */
	[2624+0x2D] = 0x2C5D,	/* U+2C2D: GLAGOLITIC CAPITAL LETTER TROKUTASTI A */
	[2624+0x2E] = 0x2C5E,	/* U+2C2E: GLAGOLITIC CAPITAL LETTER LATINATE MYSLITE */
	[2624+0x2F] = 0x2C5F,	/* U+2C2F: GLAGOLITIC CAPITAL LETTER CAUDATE CHRIVI */
	[2240+0x31] = 2688 - 0x80,	/* 342 261 ... */
	[2688+0x20] = 0x2C61,	/* U+2C60: LATIN CAPITAL LETTER L WITH DOUBLE BAR */
	[2688+0x22] = 0x026B,	/* U+2C62: LATIN CAPITAL LETTER L WITH MIDDLE TILDE */
	[2688+0x23] = 0x1D7D,	/* U+2C63: LATIN CAPITAL LETTER P WITH STROKE */
	[2688+0x24] = 0x027D,	/* U+2C64: LATIN CAPITAL LETTER R WITH TAIL */
	[2688+0x27] = 0x2C68,	/* U+2C67: LATIN CAPITAL LETTER H WITH DESCENDER */
	[2688+0x29] = 0x2C6A,	/* U+2C69: LATIN CAPITAL LETTER K WITH DESCENDER */
	[2688+0x2B] = 0x2C6C,	/* U+2C6B: LATIN CAPITAL LETTER Z WITH DESCENDER */
	[2688+0x2D] = 0x0251,	/* U+2C6D: LATIN CAPITAL LETTER ALPHA */
	[2688+0x2E] = 0x0271,	/* U+2C6E: LATIN CAPITAL LETTER M WITH HOOK */
	[2688+0x2F] = 0x0250,	/* U+2C6F: LATIN CAPITAL LETTER TURNED A */
	[2688+0x30] = 0x0252,	/* U+2C70: LATIN CAPITAL LETTER TURNED ALPHA */
	[2688+0x32] = 0x2C73,	/* U+2C72: LATIN CAPITAL LETTER W WITH HOOK */
	[2688+0x35] = 0x2C76,	/* U+2C75: LATIN CAPITAL LETTER HALF H */
	[2688+0x3E] = 0x023F,	/* U+2C7E: LATIN CAPITAL LETTER S WITH SWASH TAIL */
	[2688+0x3F] = 0x0240,	/* U+2C7F: LATIN CAPITAL LETTER Z WITH SWASH TAIL */
	[2240+0x32] = 2752 - 0x80,	/* 342 262 ... */
	[2752+0x00] = 0x2C81,	/* U+2C80: COPTIC CAPITAL LETTER ALFA */
	[2752+0x02] = 0x2C83,	/* U+2C82: COPTIC CAPITAL LETTER VIDA */
	[2752+0x04] = 0x2C85,	/* U+2C84: COPTIC CAPITAL LETTER GAMMA */
	[2752+0x06] = 0x2C87,	/* U+2C86: COPTIC CAPITAL LETTER DALDA */
	[2752+0x08] = 0x2C89,	/* U+2C88: COPTIC CAPITAL LETTER EIE */
	[2752+0x0A] = 0x2C8B,	/* U+2C8A: COPTIC CAPITAL LETTER SOU */
	[2752+0x0C] = 0x2C8D,	/* U+2C8C: COPTIC CAPITAL LETTER ZATA */
	[2752+0x0E] = 0x2C8F,	/* U+2C8E: COPTIC CAPITAL LETTER HATE */
	[2752+0x10] = 0x2C91,	/* U+2C90: COPTIC CAPITAL LETTER THETHE */
	[2752+0x12] = 0x2C93,	/* U+2C92: COPTIC CAPITAL LETTER IAUDA */
	[2752+0x14] = 0x2C95,	/* U+2C94: COPTIC CAPITAL LETTER KAPA */
	[2752+0x16] = 0x2C97,	/* U+2C96: COPTIC CAPITAL LETTER LAULA */
	[2752+0x18] = 0x2C99,	/* U+2C98: COPTIC CAPITAL LETTER MI */
	[2752+0x1A] = 0x2C9B,	/* U+2C9A: COPTIC CAPITAL LETTER NI */
	[2752+0x1C] = 0x2C9D,	/* U+2C9C: COPTIC CAPITAL LETTER KSI */
	[2752+0x1E] = 0x2C9F,	/* U+2C9E: COPTIC CAPITAL LETTER O */
	[2752+0x20] = 0x2CA1,	/* U+2CA0: COPTIC CAPITAL LETTER PI */
	[2752+0x22] = 0x2CA3,	/* U+2CA2: COPTIC CAPITAL LETTER RO */
	[2752+0x24] = 0x2CA5,	/* U+2CA4: COPTIC CAPITAL LETTER SIMA */
	[2752+0x26] = 0x2CA7,	/* U+2CA6: COPTIC CAPITAL LETTER TAU */
	[2752+0x28] = 0x2CA9,	/* U+2CA8: COPTIC CAPITAL LETTER UA */
	[2752+0x2A] = 0x2CAB,	/* U+2CAA: COPTIC CAPITAL LETTER FI */
	[2752+0x2C] = 0x2CAD,	/* U+2CAC: COPTIC CAPITAL LETTER KHI */
	[2752+0x2E] = 0x2CAF,	/* U+2CAE: COPTIC CAPITAL LETTER PSI */
	[2752+0x30] = 0x2CB1,	/* U+2CB0: COPTIC CAPITAL LETTER OOU */
	[2752+0x32] = 0x2CB3,	/* U+2CB2: COPTIC CAPITAL LETTER DIALECT-P ALEF */
	[2752+0x34] = 0x2CB5,	/* U+2CB4: COPTIC CAPITAL LETTER OLD COPTIC AIN */
	[2752+0x36] = 0x2CB7,	/* U+2CB6: COPTIC CAPITAL LETTER CRYPTOGRAMMIC EIE */
	[2752+0x38] = 0x2CB9,	/* U+2CB8: COPTIC CAPITAL LETTER DIALECT-P KAPA */
	[2752+0x3A] = 0x2CBB,	/* U+2CBA: COPTIC CAPITAL LETTER DIALECT-P NI */
	[2752+0x3C] = 0x2CBD,	/* U+2CBC: COPTIC CAPITAL LETTER CRYPTOGRAMMIC NI */
	[2752+0x3E] = 0x2CBF,	/* U+2CBE: COPTIC CAPITAL LETTER OLD COPTIC OOU */
	[2240+0x33] = 2816 - 0x80,	/* 342 263 ... */
	[2816+0x00] = 0x2CC1,	/* U+2CC0: COPTIC CAPITAL LETTER SAMPI */
	[2816+0x02] = 0x2CC3,	/* U+2CC2: COPTIC CAPITAL LETTER CROSSED SHEI */
	[2816+0x04] = 0x2CC5,	/* U+2CC4: COPTIC CAPITAL LETTER OLD COPTIC SHEI */
	[2816+0x06] = 0x2CC7,	/* U+2CC6: COPTIC CAPITAL LETTER OLD COPTIC ESH */
	[2816+0x08] = 0x2CC9,	/* U+2CC8: COPTIC CAPITAL LETTER AKHMIMIC KHEI */
	[2816+0x0A] = 0x2CCB,	/* U+2CCA: COPTIC CAPITAL LETTER DIALECT-P HORI */
	[2816+0x0C] = 0x2CCD,	/* U+2CCC: COPTIC CAPITAL LETTER OLD COPTIC HORI */
	[2816+0x0E] = 0x2CCF,	/* U+2CCE: COPTIC CAPITAL LETTER OLD COPTIC HA */
	[2816+0x10] = 0x2CD1,	/* U+2CD0: COPTIC CAPITAL LETTER L-SHAPED HA */
	[2816+0x12] = 0x2CD3,	/* U+2CD2: COPTIC CAPITAL LETTER OLD COPTIC HEI */
	[2816+0x14] = 0x2CD5,	/* U+2CD4: COPTIC CAPITAL LETTER OLD COPTIC HAT */
	[2816+0x16] = 0x2CD7,	/* U+2CD6: COPTIC CAPITAL LETTER OLD COPTIC GANGIA */
	[2816+0x18] = 0x2CD9,	/* U+2CD8: COPTIC CAPITAL LETTER OLD COPTIC DJA */
	[2816+0x1A] = 0x2CDB,	/* U+2CDA: COPTIC CAPITAL LETTER OLD COPTIC SHIMA */
	[2816+0x1C] = 0x2CDD,	/* U+2CDC: COPTIC CAPITAL LETTER OLD NUBIAN SHIMA */
	[2816+0x1E] = 0x2CDF,	/* U+2CDE: COPTIC CAPITAL LETTER OLD NUBIAN NGI */
	[2816+0x20] = 0x2CE1,	/* U+2CE0: COPTIC CAPITAL LETTER OLD NUBIAN NYI */
	[2816+0x22] = 0x2CE3,	/* U+2CE2: COPTIC CAPITAL LETTER OLD NUBIAN WAU */
	[2816+0x2B] = 0x2CEC,	/* U+2CEB: COPTIC CAPITAL LETTER CRYPTOGRAMMIC SHEI */
	[2816+0x2D] = 0x2CEE,	/* U+2CED: COPTIC CAPITAL LETTER CRYPTOGRAMMIC GANGIA */
	[2816+0x32] = 0x2CF3,	/* U+2CF2: COPTIC CAPITAL LETTER BOHAIRIC KHEI */
	[0xEA] = 2880 - 0x80,	/* 352 ... */
	[2880+0x19] = 2944 - 0x80,	/* 352 231 ... */
	[2944+0x00] = 0xA641,	/* U+A640: CYRILLIC CAPITAL LETTER ZEMLYA */
	[2944+0x02] = 0xA643,	/* U+A642: CYRILLIC CAPITAL LETTER DZELO */
	[2944+0x04] = 0xA645,	/* U+A644: CYRILLIC CAPITAL LETTER REVERSED DZE */
	[2944+0x06] = 0xA647,	/* U+A646: CYRILLIC CAPITAL LETTER IOTA */
	[2944+0x08] = 0xA649,	/* U+A648: CYRILLIC CAPITAL LETTER DJERV */
	[2944+0x0A] = 0xA64B,	/* U+A64A: CYRILLIC CAPITAL LETTER MONOGRAPH UK */
	[2944+0x0C] = 0xA64D,	/* U+A64C: CYRILLIC CAPITAL LETTER BROAD OMEGA */
	[2944+0x0E] = 0xA64F,	/* U+A64E: CYRILLIC CAPITAL LETTER NEUTRAL YER */
	[2944+0x10] = 0xA651,	/* U+A650: CYRILLIC CAPITAL LETTER YERU WITH BACK YER */
	[2944+0x12] = 0xA653,	/* U+A652: CYRILLIC CAPITAL LETTER IOTIFIED YAT */
	[2944+0x14] = 0xA655,	/* U+A654: CYRILLIC CAPITAL LETTER REVERSED YU */
	[2944+0x16] = 0xA657,	/* U+A656: CYRILLIC CAPITAL LETTER IOTIFIED A */
	[2944+0x18] = 0xA659,	/* U+A658: CYRILLIC CAPITAL LETTER CLOSED LITTLE YUS */
	[2944+0x1A] = 0xA65B,	/* U+A65A: CYRILLIC CAPITAL LETTER BLENDED YUS */
	[2944+0x1C] = 0xA65D,	/* U+A65C: CYRILLIC CAPITAL LETTER IOTIFIED CLOSED LITTLE YUS */
	[2944+0x1E] = 0xA65F,	/* U+A65E: CYRILLIC CAPITAL LETTER YN */
	[2944+0x20] = 0xA661,	/* U+A660: CYRILLIC CAPITAL LETTER REVERSED TSE */
	[2944+0x22] = 0xA663,	/* U+A662: CYRILLIC CAPITAL LETTER SOFT DE */
	[2944+0x24] = 0xA665,	/* U+A664: CYRILLIC CAPITAL LETTER SOFT EL */
	[2944+0x26] = 0xA667,	/* U+A666: CYRILLIC CAPITAL LETTER SOFT EM */
	[2944+0x28] = 0xA669,	/* U+A668: CYRILLIC CAPITAL LETTER MONOCULAR O */
	[2944+0x2A] = 0xA66B,	/* U+A66A: CYRILLIC CAPITAL LETTER BINOCULAR O */
	[2944+0x2C] = 0xA66D,	/* U+A66C: CYRILLIC CAPITAL LETTER DOUBLE MONOCULAR O */
	[2880+0x1A] = 3008 - 0x80,	/* 352 232 ... */
	[3008+0x00] = 0xA681,	/* U+A680: CYRILLIC CAPITAL LETTER DWE */
	[3008+0x02] = 0xA683,	/* U+A682: CYRILLIC CAPITAL LETTER DZWE */
	[3008+0x04] = 0xA685,	/* U+A684: CYRILLIC CAPITAL LETTER ZHWE */
	[3008+0x06] = 0xA687,	/* U+A686: CYRILLIC CAPITAL LETTER CCHE */
	[3008+0x08] = 0xA689,	/* U+A688: CYRILLIC CAPITAL LETTER DZZE */
	[3008+0x0A] = 0xA68B,	/* U+A68A: CYRILLIC CAPITAL LETTER TE WITH MIDDLE HOOK */
	[3008+0x0C] = 0xA68D,	/* U+A68C: CYRILLIC CAPITAL LETTER TWE */
	[3008+0x0E] = 0xA68F,	/* U+A68E: CYRILLIC CAPITAL LETTER TSWE */
	[3008+0x10] = 0xA691,	/* U+A690: CYRILLIC CAPITAL LETTER TSSE */
	[3008+0x12] = 0xA693,	/* U+A692: CYRILLIC CAPITAL LETTER TCHE */
	[3008+0x14] = 0xA695,	/* U+A694: CYRILLIC CAPITAL LETTER HWE */
	[3008+0x16] = 0xA697,	/* U+A696: CYRILLIC CAPITAL LETTER SHWE */
	[3008+0x18] = 0xA699,	/* U+A698: CYRILLIC CAPITAL LETTER DOUBLE O */
	[3008+0x1A] = 0xA69B,	/* U+A69A: CYRILLIC CAPITAL LETTER CROSSED O */
	[2880+0x1C] = 3072 - 0x80,	/* 352 234 ... */
	[3072+0x22] = 0xA723,	/* U+A722: LATIN CAPITAL LETTER EGYPTOLOGICAL ALEF */
	[3072+0x24] = 0xA725,	/* U+A724: LATIN CAPITAL LETTER EGYPTOLOGICAL AIN */
	[3072+0x26] = 0xA727,	/* U+A726: LATIN CAPITAL LETTER HENG */
	[3072+0x28] = 0xA729,	/* U+A728: LATIN CAPITAL LETTER TZ */
	[3072+0x2A] = 0xA72B,	/* U+A72A: LATIN CAPITAL LETTER TRESILLO */
	[3072+0x2C] = 0xA72D,	/* U+A72C: LATIN CAPITAL LETTER CUATRILLO */
	[3072+0x2E] = 0xA72F,	/* U+A72E: LATIN CAPITAL LETTER CUATRILLO WITH COMMA */
	[3072+0x32] = 0xA733,	/* U+A732: LATIN CAPITAL LETTER AA */
	[3072+0x34] = 0xA735,	/* U+A734: LATIN CAPITAL LETTER AO */
	[3072+0x36] = 0xA737,	/* U+A736: LATIN CAPITAL LETTER AU */
	[3072+0x38] = 0xA739,	/* U+A738: LATIN CAPITAL LETTER AV */
	[3072+0x3A] = 0xA73B,	/* U+A73A: LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR */
	[3072+0x3C] = 0xA73D,	/* U+A73C: LATIN CAPITAL LETTER AY */
	[3072+0x3E] = 0xA73F,	/* U+A73E: LATIN CAPITAL LETTER REVERSED C WITH DOT */
	[2880+0x1D] = 3136 - 0x80,	/* 352 235 ... */
	[3136+0x00] = 0xA741,	/* U+A740: LATIN CAPITAL LETTER K WITH STROKE */
	[3136+0x02] = 0xA743,	/* U+A742: LATIN CAPITAL LETTER K WITH DIAGONAL STROKE */
	[3136+0x04] = 0xA745,	/* U+A744: LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE */
	[3136+0x06] = 0xA747,	/* U+A746: LATIN CAPITAL LETTER BROKEN L */
	[3136+0x08] = 0xA749,	/* U+A748: LATIN CAPITAL LETTER L WITH HIGH STROKE */
	[3136+0x0A] = 0xA74B,	/* U+A74A: LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY */
	[3136+0x0C] = 0xA74D,	/* U+A74C: LATIN CAPITAL LETTER O WITH LOOP */
	[3136+0x0E] = 0xA74F,	/* U+A74E: LATIN CAPITAL LETTER OO */
	[3136+0x10] = 0xA751,	/* U+A750: LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER */
	[3136+0x12] = 0xA753,	/* U+A752: LATIN CAPITAL LETTER P WITH FLOURISH */
	[3136+0x14] = 0xA755,	/* U+A754: LATIN CAPITAL LETTER P WITH SQUIRREL TAIL */
	[3136+0x16] = 0xA757,	/* U+A756: LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER */
	[3136+0x18] = 0xA759,	/* U+A758: LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE */
	[3136+0x1A] = 0xA75B,	/* U+A75A: LATIN CAPITAL LETTER R ROTUNDA */
	[3136+0x1C] = 0xA75D,	/* U+A75C: LATIN CAPITAL LETTER RUM ROTUNDA */
	[3136+0x1E] = 0xA75F,	/* U+A75E: LATIN CAPITAL LETTER V WITH DIAGONAL STROKE */
	[3136+0x20] = 0xA761,	/* U+A760: LATIN CAPITAL LETTER VY */
	[3136+0x22] = 0xA763,	/* U+A762: LATIN CAPITAL LETTER VISIGOTHIC Z */
	[3136+0x24] = 0xA765,	/* U+A764: LATIN CAPITAL LETTER THORN WITH STROKE */
	[3136+0x26] = 0xA767,	/* U+A766: LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER */
	[3136+0x28] = 0xA769,	/* U+A768: LATIN CAPITAL LETTER VEND */
	[3136+0x2A] = 0xA76B,	/* U+A76A: LATIN CAPITAL LETTER ET */
	[3136+0x2C] = 0xA76D,	/* U+A76C: LATIN CAPITAL LETTER IS */
	[3136+0x2E] = 0xA76F,	/* U+A76E: LATIN CAPITAL LETTER CON */
	[3136+0x39] = 0xA77A,	/* U+A779: LATIN CAPITAL LETTER INSULAR D */
	[3136+0x3B] = 0xA77C,	/* U+A77B: LATIN CAPITAL LETTER INSULAR F */
	[3136+0x3D] = 0x1D79,	/* U+A77D: LATIN CAPITAL LETTER INSULAR G */
	[3136+0x3E] = 0xA77F,	/* U+A77E: LATIN CAPITAL LETTER TURNED INSULAR G */
	[2880+0x1E] = 3200 - 0x80,	/* 352 236 ... */
	[3200+0x00] = 0xA781,	/* U+A780: LATIN CAPITAL LETTER TURNED L */
	[3200+0x02] = 0xA783,	/* U+A782: LATIN CAPITAL LETTER INSULAR R */
	[3200+0x04] = 0xA785,	/* U+A784: LATIN CAPITAL LETTER INSULAR S */
	[3200+0x06] = 0xA787,	/* U+A786: LATIN CAPITAL LETTER INSULAR T */
	[3200+0x0B] = 0xA78C,	/* U+A78B: LATIN CAPITAL LETTER SALTILLO */
	[3200+0x0D] = 0x0265,	/* U+A78D: LATIN CAPITAL LETTER TURNED H */
	[3200+0x10] = 0xA791,	/* U+A790: LATIN CAPITAL LETTER N WITH DESCENDER */
	[3200+0x12] = 0xA793,	/* U+A792: LATIN CAPITAL LETTER C WITH BAR */
	[3200+0x16] = 0xA797,	/* U+A796: LATIN CAPITAL LETTER B WITH FLOURISH */
	[3200+0x18] = 0xA799,	/* U+A798: LATIN CAPITAL LETTER F WITH STROKE */
	[3200+0x1A] = 0xA79B,	/* U+A79A: LATIN CAPITAL LETTER VOLAPUK AE */
	[3200+0x1C] = 0xA79D,	/* U+A79C: LATIN CAPITAL LETTER VOLAPUK OE */
	[3200+0x1E] = 0xA79F,	/* U+A79E: LATIN CAPITAL LETTER VOLAPUK UE */
	[3200+0x20] = 0xA7A1,	/* U+A7A0: LATIN CAPITAL LETTER G WITH OBLIQUE STROKE */
	[3200+0x22] = 0xA7A3,	/* U+A7A2: LATIN CAPITAL LETTER K WITH OBLIQUE STROKE */
	[3200+0x24] = 0xA7A5,	/* U+A7A4: LATIN CAPITAL LETTER N WITH OBLIQUE STROKE */
	[3200+0x26] = 0xA7A7,	/* U+A7A6: LATIN CAPITAL LETTER R WITH OBLIQUE STROKE */
	[3200+0x28] = 0xA7A9,	/* U+A7A8: LATIN CAPITAL LETTER S WITH OBLIQUE STROKE */
	[3200+0x2A] = 0x0266,	/* U+A7AA: LATIN CAPITAL LETTER H WITH HOOK */
	[3200+0x2B] = 0x025C,	/* U+A7AB: LATIN CAPITAL LETTER REVERSED OPEN E */
	[3200+0x2C] = 0x0261,	/* U+A7AC: LATIN CAPITAL LETTER SCRIPT G */
	[3200+0x2D] = 0x026C,	/* U+A7AD: LATIN CAPITAL LETTER L WITH BELT */
	[3200+0x2E] = 0x026A,	/* U+A7AE: LATIN CAPITAL LETTER SMALL CAPITAL I */
	[3200+0x30] = 0x029E,	/* U+A7B0: LATIN CAPITAL LETTER TURNED K */
	[3200+0x31] = 0x0287,	/* U+A7B1: LATIN CAPITAL LETTER TURNED T */
	[3200+0x32] = 0x029D,	/* U+A7B2: LATIN CAPITAL LETTER J WITH CROSSED-TAIL */
	[3200+0x33] = 0xAB53,	/* U+A7B3: LATIN CAPITAL LETTER CHI */
	[3200+0x34] = 0xA7B5,	/* U+A7B4: LATIN CAPITAL LETTER BETA */
	[3200+0x36] = 0xA7B7,	/* U+A7B6: LATIN CAPITAL LETTER OMEGA */
	[3200+0x38] = 0xA7B9,	/* U+A7B8: LATIN CAPITAL LETTER U WITH STROKE */
	[3200+0x3A] = 0xA7BB,	/* U+A7BA: LATIN CAPITAL LETTER GLOTTAL A */
	[3200+0x3C] = 0xA7BD,	/* U+A7BC: LATIN CAPITAL LETTER GLOTTAL I */
	[3200+0x3E] = 0xA7BF,	/* U+A7BE: LATIN CAPITAL LETTER GLOTTAL U */
	[2880+0x1F] = 3264 - 0x80,	/* 352 237 ... */
	[3264+0x00] = 0xA7C1,	/* U+A7C0: LATIN CAPITAL LETTER OLD POLISH O */
	[3264+0x02] = 0xA7C3,	/* U+A7C2: LATIN CAPITAL LETTER ANGLICANA W */
	[3264+0x04] = 0xA794,	/* U+A7C4: LATIN CAPITAL LETTER C WITH PALATAL HOOK */
	[3264+0x05] = 0x0282,	/* U+A7C5: LATIN CAPITAL LETTER S WITH HOOK */
	[3264+0x06] = 0x1D8E,	/* U+A7C6: LATIN CAPITAL LETTER Z WITH PALATAL HOOK */
	[3264+0x07] = 0xA7C8,	/* U+A7C7: LATIN CAPITAL LETTER D WITH SHORT STROKE OVERLAY */
	[3264+0x09] = 0xA7CA,	/* U+A7C9: LATIN CAPITAL LETTER S WITH SHORT STROKE OVERLAY */
	[3264+0x10] = 0xA7D1,	/* U+A7D0: LATIN CAPITAL LETTER CLOSED INSULAR G */
	[3264+0x16] = 0xA7D7,	/* U+A7D6: LATIN CAPITAL LETTER MIDDLE SCOTS S */
	[3264+0x18] = 0xA7D9,	/* U+A7D8: LATIN CAPITAL LETTER SIGMOID S */
	[3264+0x35] = 0xA7F6,	/* U+A7F5: LATIN CAPITAL LETTER REVERSED HALF H */
	[2880+0x2D] = 3328 - 0x80,	/* 352 255 ... */
	[3328+0x30] = 0x13A0,	/* U+AB70: CHEROKEE SMALL LETTER A */
	[3328+0x31] = 0x13A1,	/* U+AB71: CHEROKEE SMALL LETTER E */
	[3328+0x32] = 0x13A2,	/* U+AB72: CHEROKEE SMALL LETTER I */
	[3328+0x33] = 0x13A3,	/* U+AB73: CHEROKEE SMALL LETTER O */
	[3328+0x34] = 0x13A4,	/* U+AB74: CHEROKEE SMALL LETTER U */
	[3328+0x35] = 0x13A5,	/* U+AB75: CHEROKEE SMALL LETTER V */
	[3328+0x36] = 0x13A6,	/* U+AB76: CHEROKEE SMALL LETTER GA */
	[3328+0x37] = 0x13A7,	/* U+AB77: CHEROKEE SMALL LETTER KA */
	[3328+0x38] = 0x13A8,	/* U+AB78: CHEROKEE SMALL LETTER GE */
	[3328+0x39] = 0x13A9,	/* U+AB79: CHEROKEE SMALL LETTER GI */
	[3328+0x3A] = 0x13AA,	/* U+AB7A: CHEROKEE SMALL LETTER GO */
	[3328+0x3B] = 0x13AB,	/* U+AB7B: CHEROKEE SMALL LETTER GU */
	[3328+0x3C] = 0x13AC,	/* U+AB7C: CHEROKEE SMALL LETTER GV */
	[3328+0x3D] = 0x13AD,	/* U+AB7D: CHEROKEE SMALL LETTER HA */
	[3328+0x3E] = 0x13AE,	/* U+AB7E: CHEROKEE SMALL LETTER HE */
	[3328+0x3F] = 0x13AF,	/* U+AB7F: CHEROKEE SMALL LETTER HI */
	[2880+0x2E] = 3392 - 0x80,	/* 352 256 ... */
	[3392+0x00] = 0x13B0,	/* U+AB80: CHEROKEE SMALL LETTER HO */
	[3392+0x01] = 0x13B1,	/* U+AB81: CHEROKEE SMALL LETTER HU */
	[3392+0x02] = 0x13B2,	/* U+AB82: CHEROKEE SMALL LETTER HV */
	[3392+0x03] = 0x13B3,	/* U+AB83: CHEROKEE SMALL LETTER LA */
	[3392+0x04] = 0x13B4,	/* U+AB84: CHEROKEE SMALL LETTER LE */
	[3392+0x05] = 0x13B5,	/* U+AB85: CHEROKEE SMALL LETTER LI */
	[3392+0x06] = 0x13B6,	/* U+AB86: CHEROKEE SMALL LETTER LO */
	[3392+0x07] = 0x13B7,	/* U+AB87: CHEROKEE SMALL LETTER LU */
	[3392+0x08] = 0x13B8,	/* U+AB88: CHEROKEE SMALL LETTER LV */
	[3392+0x09] = 0x13B9,	/* U+AB89: CHEROKEE SMALL LETTER MA */
	[3392+0x0A] = 0x13BA,	/* U+AB8A: CHEROKEE SMALL LETTER ME */
	[3392+0x0B] = 0x13BB,	/* U+AB8B: CHEROKEE SMALL LETTER MI */
	[3392+0x0C] = 0x13BC,	/* U+AB8C: CHEROKEE SMALL LETTER MO */
	[3392+0x0D] = 0x13BD,	/* U+AB8D: CHEROKEE SMALL LETTER MU */
	[3392+0x0E] = 0x13BE,	/* U+AB8E: CHEROKEE SMALL LETTER NA */
	[3392+0x0F] = 0x13BF,	/* U+AB8F: CHEROKEE SMALL LETTER HNA */
	[3392+0x10] = 0x13C0,	/* U+AB90: CHEROKEE SMALL LETTER NAH */
	[3392+0x11] = 0x13C1,	/* U+AB91: CHEROKEE SMALL LETTER NE */
	[3392+0x12] = 0x13C2,	/* U+AB92: CHEROKEE SMALL LETTER NI */
	[3392+0x13] = 0x13C3,	/* U+AB93: CHEROKEE SMALL LETTER NO */
	[3392+0x14] = 0x13C4,	/* U+AB94: CHEROKEE SMALL LETTER NU */
	[3392+0x15] = 0x13C5,	/* U+AB95: CHEROKEE SMALL LETTER NV */
	[3392+0x16] = 0x13C6,	/* U+AB96: CHEROKEE SMALL LETTER QUA */
	[3392+0x17] = 0x13C7,	/* U+AB97: CHEROKEE SMALL LETTER QUE */
	[3392+0x18] = 0x13C8,	/* U+AB98: CHEROKEE SMALL LETTER QUI */
	[3392+0x19] = 0x13C9,	/* U+AB99: CHEROKEE SMALL LETTER QUO */
	[3392+0x1A] = 0x13CA,	/* U+AB9A: CHEROKEE SMALL LETTER QUU */
	[3392+0x1B] = 0x13CB,	/* U+AB9B: CHEROKEE SMALL LETTER QUV */
	[3392+0x1C] = 0x13CC,	/* U+AB9C: CHEROKEE SMALL LETTER SA */
	[3392+0x1D] = 0x13CD,	/* U+AB9D: CHEROKEE SMALL LETTER S */
	[3392+0x1E] = 0x13CE,	/* U+AB9E: CHEROKEE SMALL LETTER SE */
	[3392+0x1F] = 0x13CF,	/* U+AB9F: CHEROKEE SMALL LETTER SI */
	[3392+0x20] = 0x13D0,	/* U+ABA0: CHEROKEE SMALL LETTER SO */
	[3392+0x21] = 0x13D1,	/* U+ABA1: CHEROKEE SMALL LETTER SU */
	[3392+0x22] = 0x13D2,	/* U+ABA2: CHEROKEE SMALL LETTER SV */
	[3392+0x23] = 0x13D3,	/* U+ABA3: CHEROKEE SMALL LETTER DA */
	[3392+0x24] = 0x13D4,	/* U+ABA4: CHEROKEE SMALL LETTER TA */
	[3392+0x25] = 0x13D5,	/* U+ABA5: CHEROKEE SMALL LETTER DE */
	[3392+0x26] = 0x13D6,	/* U+ABA6: CHEROKEE SMALL LETTER TE */
	[3392+0x27] = 0x13D7,	/* U+ABA7: CHEROKEE SMALL LETTER DI */
	[3392+0x28] = 0x13D8,	/* U+ABA8: CHEROKEE SMALL LETTER TI */
	[3392+0x29] = 0x13D9,	/* U+ABA9: CHEROKEE SMALL LETTER DO */
	[3392+0x2A] = 0x13DA,	/* U+ABAA: CHEROKEE SMALL LETTER DU */
	[3392+0x2B] = 0x13DB,	/* U+ABAB: CHEROKEE SMALL LETTER DV */
	[3392+0x2C] = 0x13DC,	/* U+ABAC: CHEROKEE SMALL LETTER DLA */
	[3392+0x2D] = 0x13DD,	/* U+ABAD: CHEROKEE SMALL LETTER TLA */
	[3392+0x2E] = 0x13DE,	/* U+ABAE: CHEROKEE SMALL LETTER TLE */
	[3392+0x2F] = 0x13DF,	/* U+ABAF: CHEROKEE SMALL LETTER TLI */
	[3392+0x30] = 0x13E0,	/* U+ABB0: CHEROKEE SMALL LETTER TLO */
	[3392+0x31] = 0x13E1,	/* U+ABB1: CHEROKEE SMALL LETTER TLU */
	[3392+0x32] = 0x13E2,	/* U+ABB2: CHEROKEE SMALL LETTER TLV */
	[3392+0x33] = 0x13E3,	/* U+ABB3: CHEROKEE SMALL LETTER TSA */
	[3392+0x34] = 0x13E4,	/* U+ABB4: CHEROKEE SMALL LETTER TSE */
	[3392+0x35] = 0x13E5,	/* U+ABB5: CHEROKEE SMALL LETTER TSI */
	[3392+0x36] = 0x13E6,	/* U+ABB6: CHEROKEE SMALL LETTER TSO */
	[3392+0x37] = 0x13E7,	/* U+ABB7: CHEROKEE SMALL LETTER TSU */
	[3392+0x38] = 0x13E8,	/* U+ABB8: CHEROKEE SMALL LETTER TSV */
	[3392+0x39] = 0x13E9,	/* U+ABB9: CHEROKEE SMALL LETTER WA */
	[3392+0x3A] = 0x13EA,	/* U+ABBA: CHEROKEE SMALL LETTER WE */
	[3392+0x3B] = 0x13EB,	/* U+ABBB: CHEROKEE SMALL LETTER WI */
	[3392+0x3C] = 0x13EC,	/* U+ABBC: CHEROKEE SMALL LETTER WO */
	[3392+0x3D] = 0x13ED,	/* U+ABBD: CHEROKEE SMALL LETTER WU */
	[3392+0x3E] = 0x13EE,	/* U+ABBE: CHEROKEE SMALL LETTER WV */
	[3392+0x3F] = 0x13EF,	/* U+ABBF: CHEROKEE SMALL LETTER YA */
	[0xEF] = 3456 - 0x80,	/* 357 ... */
	[3456+0x2C] = 3520 - 0x80,	/* 357 254 ... */
	[3520+0x00] = -63,	/* U+FB00: LATIN SMALL LIGATURE FF */
	[3520+0x01] = -64,	/* U+FB01: LATIN SMALL LIGATURE FI */
	[3520+0x02] = -65,	/* U+FB02: LATIN SMALL LIGATURE FL */
	[3520+0x03] = -66,	/* U+FB03: LATIN SMALL LIGATURE FFI */
	[3520+0x04] = -67,	/* U+FB04: LATIN SMALL LIGATURE FFL */
	[3520+0x05] = -68,	/* U+FB05: LATIN SMALL LIGATURE LONG S T */
	[3520+0x06] = -68,	/* U+FB06: LATIN SMALL LIGATURE ST */
	[3520+0x13] = -69,	/* U+FB13: ARMENIAN SMALL LIGATURE MEN NOW */
	[3520+0x14] = -70,	/* U+FB14: ARMENIAN SMALL LIGATURE MEN ECH */
	[3520+0x15] = -71,	/* U+FB15: ARMENIAN SMALL LIGATURE MEN INI */
	[3520+0x16] = -72,	/* U+FB16: ARMENIAN SMALL LIGATURE VEW NOW */
	[3520+0x17] = -73,	/* U+FB17: ARMENIAN SMALL LIGATURE MEN XEH */
	[3456+0x3C] = 3584 - 0x80,	/* 357 274 ... */
	[3584+0x21] = 0xFF41,	/* U+FF21: FULLWIDTH LATIN CAPITAL LETTER A */
	[3584+0x22] = 0xFF42,	/* U+FF22: FULLWIDTH LATIN CAPITAL LETTER B */
	[3584+0x23] = 0xFF43,	/* U+FF23: FULLWIDTH LATIN CAPITAL LETTER C */
	[3584+0x24] = 0xFF44,	/* U+FF24: FULLWIDTH LATIN CAPITAL LETTER D */
	[3584+0x25] = 0xFF45,	/* U+FF25: FULLWIDTH LATIN CAPITAL LETTER E */
	[3584+0x26] = 0xFF46,	/* U+FF26: FULLWIDTH LATIN CAPITAL LETTER F */
	[3584+0x27] = 0xFF47,	/* U+FF27: FULLWIDTH LATIN CAPITAL LETTER G */
	[3584+0x28] = 0xFF48,	/* U+FF28: FULLWIDTH LATIN CAPITAL LETTER H */
	[3584+0x29] = 0xFF49,	/* U+FF29: FULLWIDTH LATIN CAPITAL LETTER I */
	[3584+0x2A] = 0xFF4A,	/* U+FF2A: FULLWIDTH LATIN CAPITAL LETTER J */
	[3584+0x2B] = 0xFF4B,	/* U+FF2B: FULLWIDTH LATIN CAPITAL LETTER K */
	[3584+0x2C] = 0xFF4C,	/* U+FF2C: FULLWIDTH LATIN CAPITAL LETTER L */
	[3584+0x2D] = 0xFF4D,	/* U+FF2D: FULLWIDTH LATIN CAPITAL LETTER M */
	[3584+0x2E] = 0xFF4E,	/* U+FF2E: FULLWIDTH LATIN CAPITAL LETTER N */
	[3584+0x2F] = 0xFF4F,	/* U+FF2F: FULLWIDTH LATIN CAPITAL LETTER O */
	[3584+0x30] = 0xFF50,	/* U+FF30: FULLWIDTH LATIN CAPITAL LETTER P */
	[3584+0x31] = 0xFF51,	/* U+FF31: FULLWIDTH LATIN CAPITAL LETTER Q */
	[3584+0x32] = 0xFF52,	/* U+FF32: FULLWIDTH LATIN CAPITAL LETTER R */
	[3584+0x33] = 0xFF53,	/* U+FF33: FULLWIDTH LATIN CAPITAL LETTER S */
	[3584+0x34] = 0xFF54,	/* U+FF34: FULLWIDTH LATIN CAPITAL LETTER T */
	[3584+0x35] = 0xFF55,	/* U+FF35: FULLWIDTH LATIN CAPITAL LETTER U */
	[3584+0x36] = 0xFF56,	/* U+FF36: FULLWIDTH LATIN CAPITAL LETTER V */
	[3584+0x37] = 0xFF57,	/* U+FF37: FULLWIDTH LATIN CAPITAL LETTER W */
	[3584+0x38] = 0xFF58,	/* U+FF38: FULLWIDTH LATIN CAPITAL LETTER X */
	[3584+0x39] = 0xFF59,	/* U+FF39: FULLWIDTH LATIN CAPITAL LETTER Y */
	[3584+0x3A] = 0xFF5A,	/* U+FF3A: FULLWIDTH LATIN CAPITAL LETTER Z */
	[0xF0] = 3648 - 0x80,	/* 360 ... */
	[3648+0x10] = 3712 - 0x80,	/* 360 220 ... */
	[3712+0x10] = 3776 - 0x80,	/* 360 220 220 ... */
	[3776+0x00] = 0x10428,	/* U+10400: DESERET CAPITAL LETTER LONG I */
	[3776+0x01] = 0x10429,	/* U+10401: DESERET CAPITAL LETTER LONG E */
	[3776+0x02] = 0x1042A,	/* U+10402: DESERET CAPITAL LETTER LONG A */
	[3776+0x03] = 0x1042B,	/* U+10403: DESERET CAPITAL LETTER LONG AH */
	[3776+0x04] = 0x1042C,	/* U+10404: DESERET CAPITAL LETTER LONG O */
	[3776+0x05] = 0x1042D,	/* U+10405: DESERET CAPITAL LETTER LONG OO */
	[3776+0x06] = 0x1042E,	/* U+10406: DESERET CAPITAL LETTER SHORT I */
	[3776+0x07] = 0x1042F,	/* U+10407: DESERET CAPITAL LETTER SHORT E */
	[3776+0x08] = 0x10430,	/* U+10408: DESERET CAPITAL LETTER SHORT A */
	[3776+0x09] = 0x10431,	/* U+10409: DESERET CAPITAL LETTER SHORT AH */
	[3776+0x0A] = 0x10432,	/* U+1040A: DESERET CAPITAL LETTER SHORT O */
	[3776+0x0B] = 0x10433,	/* U+1040B: DESERET CAPITAL LETTER SHORT OO */
	[3776+0x0C] = 0x10434,	/* U+1040C: DESERET CAPITAL LETTER AY */
	[3776+0x0D] = 0x10435,	/* U+1040D: DESERET CAPITAL LETTER OW */
	[3776+0x0E] = 0x10436,	/* U+1040E: DESERET CAPITAL LETTER WU */
	[3776+0x0F] = 0x10437,	/* U+1040F: DESERET CAPITAL LETTER YEE */
	[3776+0x10] = 0x10438,	/* U+10410: DESERET CAPITAL LETTER H */
	[3776+0x11] = 0x10439,	/* U+10411: DESERET CAPITAL LETTER PEE */
	[3776+0x12] = 0x1043A,	/* U+10412: DESERET CAPITAL LETTER BEE */
	[3776+0x13] = 0x1043B,	/* U+10413: DESERET CAPITAL LETTER TEE */
	[3776+0x14] = 0x1043C,	/* U+10414: DESERET CAPITAL LETTER DEE */
	[3776+0x15] = 0x1043D,	/* U+10415: DESERET CAPITAL LETTER CHEE */
	[3776+0x16] = 0x1043E,	/* U+10416: DESERET CAPITAL LETTER JEE */
	[3776+0x17] = 0x1043F,	/* U+10417: DESERET CAPITAL LETTER KAY */
	[3776+0x18] = 0x10440,	/* U+10418: DESERET CAPITAL LETTER GAY */
	[3776+0x19] = 0x10441,	/* U+10419: DESERET CAPITAL LETTER EF */
	[3776+0x1A] = 0x10442,	/* U+1041A: DESERET CAPITAL LETTER VEE */
	[3776+0x1B] = 0x10443,	/* U+1041B: DESERET CAPITAL LETTER ETH */
	[3776+0x1C] = 0x10444,	/* U+1041C: DESERET CAPITAL LETTER THEE */
	[3776+0x1D] = 0x10445,	/* U+1041D: DESERET CAPITAL LETTER ES */
	[3776+0x1E] = 0x10446,	/* U+1041E: DESERET CAPITAL LETTER ZEE */
	[3776+0x1F] = 0x10447,	/* U+1041F: DESERET CAPITAL LETTER ESH */
	[3776+0x20] = 0x10448,	/* U+10420: DESERET CAPITAL LETTER ZHEE */
	[3776+0x21] = 0x10449,	/* U+10421: DESERET CAPITAL LETTER ER */
	[3776+0x22] = 0x1044A,	/* U+10422: DESERET CAPITAL LETTER EL */
	[3776+0x23] = 0x1044B,	/* U+10423: DESERET CAPITAL LETTER EM */
	[3776+0x24] = 0x1044C,	/* U+10424: DESERET CAPITAL LETTER EN */
	[3776+0x25] = 0x1044D,	/* U+10425: DESERET CAPITAL LETTER ENG */
	[3776+0x26] = 0x1044E,	/* U+10426: DESERET CAPITAL LETTER OI */
	[3776+0x27] = 0x1044F,	/* U+10427: DESERET CAPITAL LETTER EW */
	[3712+0x12] = 3840 - 0x80,	/* 360 220 222 ... */
	[3840+0x30] = 0x104D8,	/* U+104B0: OSAGE CAPITAL LETTER A */
	[3840+0x31] = 0x104D9,	/* U+104B1: OSAGE CAPITAL LETTER AI */
	[3840+0x32] = 0x104DA,	/* U+104B2: OSAGE CAPITAL LETTER AIN */
	[3840+0x33] = 0x104DB,	/* U+104B3: OSAGE CAPITAL LETTER AH */
	[3840+0x34] = 0x104DC,	/* U+104B4: OSAGE CAPITAL LETTER BRA */
	[3840+0x35] = 0x104DD,	/* U+104B5: OSAGE CAPITAL LETTER CHA */
	[3840+0x36] = 0x104DE,	/* U+104B6: OSAGE CAPITAL LETTER EHCHA */
	[3840+0x37] = 0x104DF,	/* U+104B7: OSAGE CAPITAL LETTER E */
	[3840+0x38] = 0x104E0,	/* U+104B8: OSAGE CAPITAL LETTER EIN */
	[3840+0x39] = 0x104E1,	/* U+104B9: OSAGE CAPITAL LETTER HA */
	[3840+0x3A] = 0x104E2,	/* U+104BA: OSAGE CAPITAL LETTER HYA */
	[3840+0x3B] = 0x104E3,	/* U+104BB: OSAGE CAPITAL LETTER I */
	[3840+0x3C] = 0x104E4,	/* U+104BC: OSAGE CAPITAL LETTER KA */
	[3840+0x3D] = 0x104E5,	/* U+104BD: OSAGE CAPITAL LETTER EHKA */
	[3840+0x3E] = 0x104E6,	/* U+104BE: OSAGE CAPITAL LETTER KYA */
	[3840+0x3F] = 0x104E7,	/* U+104BF: OSAGE CAPITAL LETTER LA */
	[3712+0x13] = 3904 - 0x80,	/* 360 220 223 ... */
	[3904+0x00] = 0x104E8,	/* U+104C0: OSAGE CAPITAL LETTER MA */
	[3904+0x01] = 0x104E9,	/* U+104C1: OSAGE CAPITAL LETTER NA */
	[3904+0x02] = 0x104EA,	/* U+104C2: OSAGE CAPITAL LETTER O */
	[3904+0x03] = 0x104EB,	/* U+104C3: OSAGE CAPITAL LETTER OIN */
	[3904+0x04] = 0x104EC,	/* U+104C4: OSAGE CAPITAL LETTER PA */
	[3904+0x05] = 0x104ED,	/* U+104C5: OSAGE CAPITAL LETTER EHPA */
	[3904+0x06] = 0x104EE,	/* U+104C6: OSAGE CAPITAL LETTER SA */
	[3904+0x07] = 0x104EF,	/* U+104C7: OSAGE CAPITAL LETTER SHA */
	[3904+0x08] = 0x104F0,	/* U+104C8: OSAGE CAPITAL LETTER TA */
	[3904+0x09] = 0x104F1,	/* U+104C9: OSAGE CAPITAL LETTER EHTA */
	[3904+0x0A] = 0x104F2,	/* U+104CA: OSAGE CAPITAL LETTER TSA */
	[3904+0x0B] = 0x104F3,	/* U+104CB: OSAGE CAPITAL LETTER EHTSA */
	[3904+0x0C] = 0x104F4,	/* U+104CC: OSAGE CAPITAL LETTER TSHA */
	[3904+0x0D] = 0x104F5,	/* U+104CD: OSAGE CAPITAL LETTER DHA */
	[3904+0x0E] = 0x104F6,	/* U+104CE: OSAGE CAPITAL LETTER U */
	[3904+0x0F] = 0x104F7,	/* U+104CF: OSAGE CAPITAL LETTER WA */
	[3904+0x10] = 0x104F8,	/* U+104D0: OSAGE CAPITAL LETTER KHA */
	[3904+0x11] = 0x104F9,	/* U+104D1: OSAGE CAPITAL LETTER GHA */
	[3904+0x12] = 0x104FA,	/* U+104D2: OSAGE CAPITAL LETTER ZA */
	[3904+0x13] = 0x104FB,	/* U+104D3: OSAGE CAPITAL LETTER ZHA */
	[3712+0x15] = 3968 - 0x80,	/* 360 220 225 ... */
	[3968+0x30] = 0x10597,	/* U+10570: VITHKUQI CAPITAL LETTER A */
	[3968+0x31] = 0x10598,	/* U+10571: VITHKUQI CAPITAL LETTER BBE */
	[3968+0x32] = 0x10599,	/* U+10572: VITHKUQI CAPITAL LETTER BE */
	[3968+0x33] = 0x1059A,	/* U+10573: VITHKUQI CAPITAL LETTER CE */
	[3968+0x34] = 0x1059B,	/* U+10574: VITHKUQI CAPITAL LETTER CHE */
	[3968+0x35] = 0x1059C,	/* U+10575: VITHKUQI CAPITAL LETTER DE */
	[3968+0x36] = 0x1059D,	/* U+10576: VITHKUQI CAPITAL LETTER DHE */
	[3968+0x37] = 0x1059E,	/* U+10577: VITHKUQI CAPITAL LETTER EI */
	[3968+0x38] = 0x1059F,	/* U+10578: VITHKUQI CAPITAL LETTER E */
	[3968+0x39] = 0x105A0,	/* U+10579: VITHKUQI CAPITAL LETTER FE */
	[3968+0x3A] = 0x105A1,	/* U+1057A: VITHKUQI CAPITAL LETTER GA */
	[3968+0x3C] = 0x105A3,	/* U+1057C: VITHKUQI CAPITAL LETTER HA */
	[3968+0x3D] = 0x105A4,	/* U+1057D: VITHKUQI CAPITAL LETTER HHA */
	[3968+0x3E] = 0x105A5,	/* U+1057E: VITHKUQI CAPITAL LETTER I */
	[3968+0x3F] = 0x105A6,	/* U+1057F: VITHKUQI CAPITAL LETTER IJE */
	[3712+0x16] = 4032 - 0x80,	/* 360 220 226 ... */
	[4032+0x00] = 0x105A7,	/* U+10580: VITHKUQI CAPITAL LETTER JE */
	[4032+0x01] = 0x105A8,	/* U+10581: VITHKUQI CAPITAL LETTER KA */
	[4032+0x02] = 0x105A9,	/* U+10582: VITHKUQI CAPITAL LETTER LA */
	[4032+0x03] = 0x105AA,	/* U+10583: VITHKUQI CAPITAL LETTER LLA */
	[4032+0x04] = 0x105AB,	/* U+10584: VITHKUQI CAPITAL LETTER ME */
	[4032+0x05] = 0x105AC,	/* U+10585: VITHKUQI CAPITAL LETTER NE */
	[4032+0x06] = 0x105AD,	/* U+10586: VITHKUQI CAPITAL LETTER NJE */
	[4032+0x07] = 0x105AE,	/* U+10587: VITHKUQI CAPITAL LETTER O */
	[4032+0x08] = 0x105AF,	/* U+10588: VITHKUQI CAPITAL LETTER PE */
	[4032+0x09] = 0x105B0,	/* U+10589: VITHKUQI CAPITAL LETTER QA */
	[4032+0x0A] = 0x105B1,	/* U+1058A: VITHKUQI CAPITAL LETTER RE */
	[4032+0x0C] = 0x105B3,	/* U+1058C: VITHKUQI CAPITAL LETTER SE */
	[4032+0x0D] = 0x105B4,	/* U+1058D: VITHKUQI CAPITAL LETTER SHE */
	[4032+0x0E] = 0x105B5,	/* U+1058E: VITHKUQI CAPITAL LETTER TE */
	[4032+0x0F] = 0x105B6,	/* U+1058F: VITHKUQI CAPITAL LETTER THE */
	[4032+0x10] = 0x105B7,	/* U+10590: VITHKUQI CAPITAL LETTER U */
	[4032+0x11] = 0x105B8,	/* U+10591: VITHKUQI CAPITAL LETTER VE */
	[4032+0x12] = 0x105B9,	/* U+10592: VITHKUQI CAPITAL LETTER XE */
	[4032+0x14] = 0x105BB,	/* U+10594: VITHKUQI CAPITAL LETTER Y */
	[4032+0x15] = 0x105BC,	/* U+10595: VITHKUQI CAPITAL LETTER ZE */
	[3712+0x32] = 4096 - 0x80,	/* 360 220 262 ... */
	[4096+0x00] = 0x10CC0,	/* U+10C80: OLD HUNGARIAN CAPITAL LETTER A */
	[4096+0x01] = 0x10CC1,	/* U+10C81: OLD HUNGARIAN CAPITAL LETTER AA */
	[4096+0x02] = 0x10CC2,	/* U+10C82: OLD HUNGARIAN CAPITAL LETTER EB */
	[4096+0x03] = 0x10CC3,	/* U+10C83: OLD HUNGARIAN CAPITAL LETTER AMB */
	[4096+0x04] = 0x10CC4,	/* U+10C84: OLD HUNGARIAN CAPITAL LETTER EC */
	[4096+0x05] = 0x10CC5,	/* U+10C85: OLD HUNGARIAN CAPITAL LETTER ENC */
	[4096+0x06] = 0x10CC6,	/* U+10C86: OLD HUNGARIAN CAPITAL LETTER ECS */
	[4096+0x07] = 0x10CC7,	/* U+10C87: OLD HUNGARIAN CAPITAL LETTER ED */
	[4096+0x08] = 0x10CC8,	/* U+10C88: OLD HUNGARIAN CAPITAL LETTER AND */
	[4096+0x09] = 0x10CC9,	/* U+10C89: OLD HUNGARIAN CAPITAL LETTER E */
	[4096+0x0A] = 0x10CCA,	/* U+10C8A: OLD HUNGARIAN CAPITAL LETTER CLOSE E */
	[4096+0x0B] = 0x10CCB,	/* U+10C8B: OLD HUNGARIAN CAPITAL LETTER EE */
	[4096+0x0C] = 0x10CCC,	/* U+10C8C: OLD HUNGARIAN CAPITAL LETTER EF */
	[4096+0x0D] = 0x10CCD,	/* U+10C8D: OLD HUNGARIAN CAPITAL LETTER EG */
	[4096+0x0E] = 0x10CCE,	/* U+10C8E: OLD HUNGARIAN CAPITAL LETTER EGY */
	[4096+0x0F] = 0x10CCF,	/* U+10C8F: OLD HUNGARIAN CAPITAL LETTER EH */
	[4096+0x10] = 0x10CD0,	/* U+10C90: OLD HUNGARIAN CAPITAL LETTER I */
	[4096+0x11] = 0x10CD1,	/* U+10C91: OLD HUNGARIAN CAPITAL LETTER II */
	[4096+0x12] = 0x10CD2,	/* U+10C92: OLD HUNGARIAN CAPITAL LETTER EJ */
	[4096+0x13] = 0x10CD3,	/* U+10C93: OLD HUNGARIAN CAPITAL LETTER EK */
	[4096+0x14] = 0x10CD4,	/* U+10C94: OLD HUNGARIAN CAPITAL LETTER AK */
	[4096+0x15] = 0x10CD5,	/* U+10C95: OLD HUNGARIAN CAPITAL LETTER UNK */
	[4096+0x16] = 0x10CD6,	/* U+10C96: OLD HUNGARIAN CAPITAL LETTER EL */
	[4096+0x17] = 0x10CD7,	/* U+10C97: OLD HUNGARIAN CAPITAL LETTER ELY */
	[4096+0x18] = 0x10CD8,	/* U+10C98: OLD HUNGARIAN CAPITAL LETTER EM */
	[4096+0x19] = 0x10CD9,	/* U+10C99: OLD HUNGARIAN CAPITAL LETTER EN */
	[4096+0x1A] = 0x10CDA,	/* U+10C9A: OLD HUNGARIAN CAPITAL LETTER ENY */
	[4096+0x1B] = 0x10CDB,	/* U+10C9B: OLD HUNGARIAN CAPITAL LETTER O */
	[4096+0x1C] = 0x10CDC,	/* U+10C9C: OLD HUNGARIAN CAPITAL LETTER OO */
	[4096+0x1D] = 0x10CDD,	/* U+10C9D: OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG OE */
	[4096+0x1E] = 0x10CDE,	/* U+10C9E: OLD HUNGARIAN CAPITAL LETTER RUDIMENTA OE */
	[4096+0x1F] = 0x10CDF,	/* U+10C9F: OLD HUNGARIAN CAPITAL LETTER OEE */
	[4096+0x20] = 0x10CE0,	/* U+10CA0: OLD HUNGARIAN CAPITAL LETTER EP */
	[4096+0x21] = 0x10CE1,	/* U+10CA1: OLD HUNGARIAN CAPITAL LETTER EMP */
	[4096+0x22] = 0x10CE2,	/* U+10CA2: OLD HUNGARIAN CAPITAL LETTER ER */
	[4096+0x23] = 0x10CE3,	/* U+10CA3: OLD HUNGARIAN CAPITAL LETTER SHORT ER */
	[4096+0x24] = 0x10CE4,	/* U+10CA4: OLD HUNGARIAN CAPITAL LETTER ES */
	[4096+0x25] = 0x10CE5,	/* U+10CA5: OLD HUNGARIAN CAPITAL LETTER ESZ */
	[4096+0x26] = 0x10CE6,	/* U+10CA6: OLD HUNGARIAN CAPITAL LETTER ET */
	[4096+0x27] = 0x10CE7,	/* U+10CA7: OLD HUNGARIAN CAPITAL LETTER ENT */
	[4096+0x28] = 0x10CE8,	/* U+10CA8: OLD HUNGARIAN CAPITAL LETTER ETY */
	[4096+0x29] = 0x10CE9,	/* U+10CA9: OLD HUNGARIAN CAPITAL LETTER ECH */
	[4096+0x2A] = 0x10CEA,	/* U+10CAA: OLD HUNGARIAN CAPITAL LETTER U */
	[4096+0x2B] = 0x10CEB,	/* U+10CAB: OLD HUNGARIAN CAPITAL LETTER UU */
	[4096+0x2C] = 0x10CEC,	/* U+10CAC: OLD HUNGARIAN CAPITAL LETTER NIKOLSBURG UE */
	[4096+0x2D] = 0x10CED,	/* U+10CAD: OLD HUNGARIAN CAPITAL LETTER RUDIMENTA UE */
	[4096+0x2E] = 0x10CEE,	/* U+10CAE: OLD HUNGARIAN CAPITAL LETTER EV */
	[4096+0x2F] = 0x10CEF,	/* U+10CAF: OLD HUNGARIAN CAPITAL LETTER EZ */
	[4096+0x30] = 0x10CF0,	/* U+10CB0: OLD HUNGARIAN CAPITAL LETTER EZS */
	[4096+0x31] = 0x10CF1,	/* U+10CB1: OLD HUNGARIAN CAPITAL LETTER ENT-SHAPED SIGN */
	[4096+0x32] = 0x10CF2,	/* U+10CB2: OLD HUNGARIAN CAPITAL LETTER US */
	[3648+0x11] = 4160 - 0x80,	/* 360 221 ... */
	[4160+0x22] = 4224 - 0x80,	/* 360 221 242 ... */
	[4224+0x20] = 0x118C0,	/* U+118A0: WARANG CITI CAPITAL LETTER NGAA */
	[4224+0x21] = 0x118C1,	/* U+118A1: WARANG CITI CAPITAL LETTER A */
	[4224+0x22] = 0x118C2,	/* U+118A2: WARANG CITI CAPITAL LETTER WI */
	[4224+0x23] = 0x118C3,	/* U+118A3: WARANG CITI CAPITAL LETTER YU */
	[4224+0x24] = 0x118C4,	/* U+118A4: WARANG CITI CAPITAL LETTER YA */
	[4224+0x25] = 0x118C5,	/* U+118A5: WARANG CITI CAPITAL LETTER YO */
	[4224+0x26] = 0x118C6,	/* U+118A6: WARANG CITI CAPITAL LETTER II */
	[4224+0x27] = 0x118C7,	/* U+118A7: WARANG CITI CAPITAL LETTER UU */
	[4224+0x28] = 0x118C8,	/* U+118A8: WARANG CITI CAPITAL LETTER E */
	[4224+0x29] = 0x118C9,	/* U+118A9: WARANG CITI CAPITAL LETTER O */
	[4224+0x2A] = 0x118CA,	/* U+118AA: WARANG CITI CAPITAL LETTER ANG */
	[4224+0x2B] = 0x118CB,	/* U+118AB: WARANG CITI CAPITAL LETTER GA */
	[4224+0x2C] = 0x118CC,	/* U+118AC: WARANG CITI CAPITAL LETTER KO */
	[4224+0x2D] = 0x118CD,	/* U+118AD: WARANG CITI CAPITAL LETTER ENY */
	[4224+0x2E] = 0x118CE,	/* U+118AE: WARANG CITI CAPITAL LETTER YUJ */
	[4224+0x2F] = 0x118CF,	/* U+118AF: WARANG CITI CAPITAL LETTER UC */
	[4224+0x30] = 0x118D0,	/* U+118B0: WARANG CITI CAPITAL LETTER ENN */
	[4224+0x31] = 0x118D1,	/* U+118B1: WARANG CITI CAPITAL LETTER ODD */
	[4224+0x32] = 0x118D2,	/* U+118B2: WARANG CITI CAPITAL LETTER TTE */
	[4224+0x33] = 0x118D3,	/* U+118B3: WARANG CITI CAPITAL LETTER NUNG */
	[4224+0x34] = 0x118D4,	/* U+118B4: WARANG CITI CAPITAL LETTER DA */
	[4224+0x35] = 0x118D5,	/* U+118B5: WARANG CITI CAPITAL LETTER AT */
	[4224+0x36] = 0x118D6,	/* U+118B6: WARANG CITI CAPITAL LETTER AM */
	[4224+0x37] = 0x118D7,	/* U+118B7: WARANG CITI CAPITAL LETTER BU */
	[4224+0x38] = 0x118D8,	/* U+118B8: WARANG CITI CAPITAL LETTER PU */
	[4224+0x39] = 0x118D9,	/* U+118B9: WARANG CITI CAPITAL LETTER HIYO */
	[4224+0x3A] = 0x118DA,	/* U+118BA: WARANG CITI CAPITAL LETTER HOLO */
	[4224+0x3B] = 0x118DB,	/* U+118BB: WARANG CITI CAPITAL LETTER HORR */
	[4224+0x3C] = 0x118DC,	/* U+118BC: WARANG CITI CAPITAL LETTER HAR */
	[4224+0x3D] = 0x118DD,	/* U+118BD: WARANG CITI CAPITAL LETTER SSUU */
	[4224+0x3E] = 0x118DE,	/* U+118BE: WARANG CITI CAPITAL LETTER SII */
	[4224+0x3F] = 0x118DF,	/* U+118BF: WARANG CITI CAPITAL LETTER VIYO */
	[3648+0x16] = 4288 - 0x80,	/* 360 226 ... */
	[4288+0x39] = 4352 - 0x80,	/* 360 226 271 ... */
	[4352+0x00] = 0x16E60,	/* U+16E40: MEDEFAIDRIN CAPITAL LETTER M */
	[4352+0x01] = 0x16E61,	/* U+16E41: MEDEFAIDRIN CAPITAL LETTER S */
	[4352+0x02] = 0x16E62,	/* U+16E42: MEDEFAIDRIN CAPITAL LETTER V */
	[4352+0x03] = 0x16E63,	/* U+16E43: MEDEFAIDRIN CAPITAL LETTER W */
	[4352+0x04] = 0x16E64,	/* U+16E44: MEDEFAIDRIN CAPITAL LETTER ATIU */
	[4352+0x05] = 0x16E65,	/* U+16E45: MEDEFAIDRIN CAPITAL LETTER Z */
	[4352+0x06] = 0x16E66,	/* U+16E46: MEDEFAIDRIN CAPITAL LETTER KP */
	[4352+0x07] = 0x16E67,	/* U+16E47: MEDEFAIDRIN CAPITAL LETTER P */
	[4352+0x08] = 0x16E68,	/* U+16E48: MEDEFAIDRIN CAPITAL LETTER T */
	[4352+0x09] = 0x16E69,	/* U+16E49: MEDEFAIDRIN CAPITAL LETTER G */
	[4352+0x0A] = 0x16E6A,	/* U+16E4A: MEDEFAIDRIN CAPITAL LETTER F */
	[4352+0x0B] = 0x16E6B,	/* U+16E4B: MEDEFAIDRIN CAPITAL LETTER I */
	[4352+0x0C] = 0x16E6C,	/* U+16E4C: MEDEFAIDRIN CAPITAL LETTER K */
	[4352+0x0D] = 0x16E6D,	/* U+16E4D: MEDEFAIDRIN CAPITAL LETTER A */
	[4352+0x0E] = 0x16E6E,	/* U+16E4E: MEDEFAIDRIN CAPITAL LETTER J */
	[4352+0x0F] = 0x16E6F,	/* U+16E4F: MEDEFAIDRIN CAPITAL LETTER E */
	[4352+0x10] = 0x16E70,	/* U+16E50: MEDEFAIDRIN CAPITAL LETTER B */
	[4352+0x11] = 0x16E71,	/* U+16E51: MEDEFAIDRIN CAPITAL LETTER C */
	[4352+0x12] = 0x16E72,	/* U+16E52: MEDEFAIDRIN CAPITAL LETTER U */
	[4352+0x13] = 0x16E73,	/* U+16E53: MEDEFAIDRIN CAPITAL LETTER YU */
	[4352+0x14] = 0x16E74,	/* U+16E54: MEDEFAIDRIN CAPITAL LETTER L */
	[4352+0x15] = 0x16E75,	/* U+16E55: MEDEFAIDRIN CAPITAL LETTER Q */
	[4352+0x16] = 0x16E76,	/* U+16E56: MEDEFAIDRIN CAPITAL LETTER HP */
	[4352+0x17] = 0x16E77,	/* U+16E57: MEDEFAIDRIN CAPITAL LETTER NY */
	[4352+0x18] = 0x16E78,	/* U+16E58: MEDEFAIDRIN CAPITAL LETTER X */
	[4352+0x19] = 0x16E79,	/* U+16E59: MEDEFAIDRIN CAPITAL LETTER D */
	[4352+0x1A] = 0x16E7A,	/* U+16E5A: MEDEFAIDRIN CAPITAL LETTER OE */
	[4352+0x1B] = 0x16E7B,	/* U+16E5B: MEDEFAIDRIN CAPITAL LETTER N */
	[4352+0x1C] = 0x16E7C,	/* U+16E5C: MEDEFAIDRIN CAPITAL LETTER R */
	[4352+0x1D] = 0x16E7D,	/* U+16E5D: MEDEFAIDRIN CAPITAL LETTER O */
	[4352+0x1E] = 0x16E7E,	/* U+16E5E: MEDEFAIDRIN CAPITAL LETTER AI */
	[4352+0x1F] = 0x16E7F,	/* U+16E5F: MEDEFAIDRIN CAPITAL LETTER Y */
	[3648+0x1E] = 4416 - 0x80,	/* 360 236 ... */
	[4416+0x24] = 4480 - 0x80,	/* 360 236 244 ... */
	[4480+0x00] = 0x1E922,	/* U+1E900: ADLAM CAPITAL LETTER ALIF */
	[4480+0x01] = 0x1E923,	/* U+1E901: ADLAM CAPITAL LETTER DAALI */
	[4480+0x02] = 0x1E924,	/* U+1E902: ADLAM CAPITAL LETTER LAAM */
	[4480+0x03] = 0x1E925,	/* U+1E903: ADLAM CAPITAL LETTER MIIM */
	[4480+0x04] = 0x1E926,	/* U+1E904: ADLAM CAPITAL LETTER BA */
	[4480+0x05] = 0x1E927,	/* U+1E905: ADLAM CAPITAL LETTER SINNYIIYHE */
	[4480+0x06] = 0x1E928,	/* U+1E906: ADLAM CAPITAL LETTER PE */
	[4480+0x07] = 0x1E929,	/* U+1E907: ADLAM CAPITAL LETTER BHE */
	[4480+0x08] = 0x1E92A,	/* U+1E908: ADLAM CAPITAL LETTER RA */
	[4480+0x09] = 0x1E92B,	/* U+1E909: ADLAM CAPITAL LETTER E */
	[4480+0x0A] = 0x1E92C,	/* U+1E90A: ADLAM CAPITAL LETTER FA */
	[4480+0x0B] = 0x1E92D,	/* U+1E90B: ADLAM CAPITAL LETTER I */
	[4480+0x0C] = 0x1E92E,	/* U+1E90C: ADLAM CAPITAL LETTER O */
	[4480+0x0D] = 0x1E92F,	/* U+1E90D: ADLAM CAPITAL LETTER DHA */
	[4480+0x0E] = 0x1E930,	/* U+1E90E: ADLAM CAPITAL LETTER YHE */
	[4480+0x0F] = 0x1E931,	/* U+1E90F: ADLAM CAPITAL LETTER WAW */
	[4480+0x10] = 0x1E932,	/* U+1E910: ADLAM CAPITAL LETTER NUN */
	[4480+0x11] = 0x1E933,	/* U+1E911: ADLAM CAPITAL LETTER KAF */
	[4480+0x12] = 0x1E934,	/* U+1E912: ADLAM CAPITAL LETTER YA */
	[4480+0x13] = 0x1E935,	/* U+1E913: ADLAM CAPITAL LETTER U */
	[4480+0x14] = 0x1E936,	/* U+1E914: ADLAM CAPITAL LETTER JIIM */
	[4480+0x15] = 0x1E937,	/* U+1E915: ADLAM CAPITAL LETTER CHI */
	[4480+0x16] = 0x1E938,	/* U+1E916: ADLAM CAPITAL LETTER HA */
	[4480+0x17] = 0x1E939,	/* U+1E917: ADLAM CAPITAL LETTER QAAF */
	[4480+0x18] = 0x1E93A,	/* U+1E918: ADLAM CAPITAL LETTER GA */
	[4480+0x19] = 0x1E93B,	/* U+1E919: ADLAM CAPITAL LETTER NYA */
	[4480+0x1A] = 0x1E93C,	/* U+1E91A: ADLAM CAPITAL LETTER TU */
	[4480+0x1B] = 0x1E93D,	/* U+1E91B: ADLAM CAPITAL LETTER NHA */
	[4480+0x1C] = 0x1E93E,	/* U+1E91C: ADLAM CAPITAL LETTER VA */
	[4480+0x1D] = 0x1E93F,	/* U+1E91D: ADLAM CAPITAL LETTER KHA */
	[4480+0x1E] = 0x1E940,	/* U+1E91E: ADLAM CAPITAL LETTER GBE */
	[4480+0x1F] = 0x1E941,	/* U+1E91F: ADLAM CAPITAL LETTER ZAL */
	[4480+0x20] = 0x1E942,	/* U+1E920: ADLAM CAPITAL LETTER KPO */
	[4480+0x21] = 0x1E943,	/* U+1E921: ADLAM CAPITAL LETTER SHA */
};

/* convert the case of a UTF-8 encoded string given in `s' into the
 * buffer of length `*buflen' given in `*buf'; if the buffer is not
 * large enough, it is extended using GDKrealloc; on return (with or
 * without error), the current buffer is in *buf, and the current size
 * in *buflen. */
static gdk_return
convertcase(char **restrict buf, size_t *restrict buflen,
	    const uint8_t *restrict s, int direction)
{
	uint8_t *dst = (uint8_t *) *buf;
	size_t dstoff = 0;
	size_t bl;
	const int *restrict convtab;
	switch (direction) {
	case 'U':
		convtab = uppercase;
		break;
	case 'L':
		convtab = lowercase;
		break;
	case 'F':
		convtab = casefold;
		break;
	default:
		MT_UNREACHABLE();
	}

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
			} else if (v < 0) {
				for (const char *u = specialcase[-v]; *u; u++) {
					dst[dstoff++] = (uint8_t) *u;
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
	return convertcase(buf, buflen, (const uint8_t *) s, 'U');
}

/* convert string to lowercase; see comment above for more information */
gdk_return
GDKtolower(char **restrict buf, size_t *restrict buflen, const char *restrict s)
{
	return convertcase(buf, buflen, (const uint8_t *) s, 'L');
}

/* case fold string; see comment above for more information */
gdk_return
GDKcasefold(char **restrict buf, size_t *restrict buflen, const char *restrict s)
{
	return convertcase(buf, buflen, (const uint8_t *) s, 'F');
}

static BAT *
BATcaseconvert(BAT *b, BAT *s, int direction, const char *restrict func)
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
				direction) != GDK_SUCCEED ||
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
	return BATcaseconvert(b, s, 'L', __func__);
}

BAT *
BATtoupper(BAT *b, BAT *s)
{
	return BATcaseconvert(b, s, 'U', __func__);
}

BAT *
BATcasefold(BAT *b, BAT *s)
{
	return BATcaseconvert(b, s, 'F', __func__);
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

static const char *const valtab[] = {
	NULL,
	[1] = " ",
	[2] = "!",
	[3] = "c",
	[4] = "GBP",
	[5] = "JPY",
	[6] = "|",
	[7] = "(C)",
	[8] = "a",
	[9] = "<<",
	[10] = "-",
	[11] = "(R)",
	[12] = "+-",
	[13] = "2",
	[14] = "3",
	[15] = "'",
	[16] = "u",
	[17] = ".",
	[18] = ",",
	[19] = "1",
	[20] = "o",
	[21] = ">>",
	[22] = " 1/4 ",
	[23] = " 1/2 ",
	[24] = " 3/4 ",
	[25] = "A",
	[26] = "AE",
	[27] = "C",
	[28] = "E",
	[29] = "I",
	[30] = "D",
	[31] = "N",
	[32] = "O",
	[33] = "x",
	[34] = "U",
	[35] = "Y",
	[36] = "TH",
	[37] = "ss",
	[38] = "ae",
	[39] = "e",
	[40] = "i",
	[41] = "d",
	[42] = "n",
	[43] = "/",
	[44] = "y",
	[45] = "th",
	[46] = "G",
	[47] = "g",
	[48] = "H",
	[49] = "h",
	[50] = "IJ",
	[51] = "ij",
	[52] = "J",
	[53] = "j",
	[54] = "K",
	[55] = "k",
	[56] = "q",
	[57] = "L",
	[58] = "l",
	[59] = "'n",
	[60] = "OE",
	[61] = "oe",
	[62] = "R",
	[63] = "r",
	[64] = "S",
	[65] = "s",
	[66] = "T",
	[67] = "t",
	[68] = "W",
	[69] = "w",
	[70] = "Z",
	[71] = "z",
	[72] = "b",
	[73] = "B",
	[74] = "F",
	[75] = "f",
	[76] = "hv",
	[77] = "OI",
	[78] = "oi",
	[79] = "P",
	[80] = "p",
	[81] = "V",
	[82] = "LJ",
	[83] = "Lj",
	[84] = "lj",
	[85] = "NJ",
	[86] = "Nj",
	[87] = "nj",
	[88] = "DZ",
	[89] = "Dz",
	[90] = "dz",
	[91] = "db",
	[92] = "qp",
	[93] = "m",
	[94] = "v",
	[95] = "ts",
	[96] = "ls",
	[97] = "lz",
	[98] = "^",
	[99] = "`",
	[100] = "_",
	[101] = ":",
	[102] = "~",
	[103] = "''",
	[104] = "",
	[105] = ";",
	[106] = "AMD",
	[107] = "M",
	[108] = "ue",
	[109] = "X",
	[110] = "SS",
	[111] = "LL",
	[112] = "ll",
	[113] = "--",
	[114] = "\"",
	[115] = ",,",
	[116] = "+",
	[117] = "..",
	[118] = "...",
	[119] = "``",
	[120] = "```",
	[121] = "<",
	[122] = ">",
	[123] = "!!",
	[124] = "??",
	[125] = "?!",
	[126] = "!?",
	[127] = "&",
	[128] = "0",
	[129] = "4",
	[130] = "5",
	[131] = "6",
	[132] = "7",
	[133] = "8",
	[134] = "9",
	[135] = "=",
	[136] = "(",
	[137] = ")",
	[138] = "CE",
	[139] = "C=",
	[140] = "Cr",
	[141] = "Fr.",
	[142] = "L.",
	[143] = "Pts",
	[144] = "Rs",
	[145] = "KRW",
	[146] = "ILS",
	[147] = "Dong",
	[148] = "EUR",
	[149] = "GRD",
	[150] = "PHP",
	[151] = "UAH",
	[152] = "KZT",
	[153] = "INR",
	[154] = "TL",
	[155] = "RUB",
	[156] = "GEL",
	[157] = "a/c",
	[158] = "a/s",
	[159] = "c/o",
	[160] = "c/u",
	[161] = "No",
	[162] = "Q",
	[163] = "Rx",
	[164] = "SM",
	[165] = "TEL",
	[166] = "(TM)",
	[167] = "FAX",
	[168] = " 1/7 ",
	[169] = " 1/9 ",
	[170] = " 1/10 ",
	[171] = " 1/3 ",
	[172] = " 2/3 ",
	[173] = " 1/5 ",
	[174] = " 2/5 ",
	[175] = " 3/5 ",
	[176] = " 4/5 ",
	[177] = " 1/6 ",
	[178] = " 5/6 ",
	[179] = " 1/8 ",
	[180] = " 3/8 ",
	[181] = " 5/8 ",
	[182] = " 7/8 ",
	[183] = " 1/ ",
	[184] = "II",
	[185] = "III",
	[186] = "IV",
	[187] = "VI",
	[188] = "VII",
	[189] = "VIII",
	[190] = "IX",
	[191] = "XI",
	[192] = "XII",
	[193] = "ii",
	[194] = "iii",
	[195] = "iv",
	[196] = "vi",
	[197] = "vii",
	[198] = "viii",
	[199] = "ix",
	[200] = "xi",
	[201] = "xii",
	[202] = " 0/3 ",
	[203] = "<-",
	[204] = "->",
	[205] = "<->",
	[206] = "!<->",
	[207] = "!<=",
	[208] = "!<=>",
	[209] = "!=>",
	[210] = "<=",
	[211] = "=>",
	[212] = "<=>",
	[213] = "\\",
	[214] = "*",
	[215] = "||",
	[216] = "!~",
	[217] = "!~-",
	[218] = "!~=",
	[219] = "!~~",
	[220] = "!=",
	[221] = "!==",
	[222] = ">=",
	[223] = "!<",
	[224] = "!>",
	[225] = "!>=",
	[226] = "!<~",
	[227] = "!>~",
	[228] = "!<>",
	[229] = "!><",
	[230] = "<<<",
	[231] = ">>>",
	[232] = "NUL",
	[233] = "SOH",
	[234] = "STX",
	[235] = "ETX",
	[236] = "EOT",
	[237] = "ENQ",
	[238] = "ACK",
	[239] = "BEL",
	[240] = "BS",
	[241] = "HT",
	[242] = "LF",
	[243] = "VT",
	[244] = "FF",
	[245] = "CR",
	[246] = "SO",
	[247] = "SI",
	[248] = "DLE",
	[249] = "DC1",
	[250] = "DC2",
	[251] = "DC3",
	[252] = "DC4",
	[253] = "NAK",
	[254] = "SYN",
	[255] = "ETB",
	[256] = "CAN",
	[257] = "EM",
	[258] = "SUB",
	[259] = "ESC",
	[260] = "FS",
	[261] = "GS",
	[262] = "RS",
	[263] = "US",
	[264] = "SP",
	[265] = "DEL",
	[266] = "NL",
	[267] = "(1)",
	[268] = "(2)",
	[269] = "(3)",
	[270] = "(4)",
	[271] = "(5)",
	[272] = "(6)",
	[273] = "(7)",
	[274] = "(8)",
	[275] = "(9)",
	[276] = "(10)",
	[277] = "(11)",
	[278] = "(12)",
	[279] = "(13)",
	[280] = "(14)",
	[281] = "(15)",
	[282] = "(16)",
	[283] = "(17)",
	[284] = "(18)",
	[285] = "(19)",
	[286] = "(20)",
	[287] = "1.",
	[288] = "2.",
	[289] = "3.",
	[290] = "4.",
	[291] = "5.",
	[292] = "6.",
	[293] = "7.",
	[294] = "8.",
	[295] = "9.",
	[296] = "10.",
	[297] = "11.",
	[298] = "12.",
	[299] = "13.",
	[300] = "14.",
	[301] = "15.",
	[302] = "16.",
	[303] = "17.",
	[304] = "18.",
	[305] = "19.",
	[306] = "20.",
	[307] = "(a)",
	[308] = "(b)",
	[309] = "(c)",
	[310] = "(d)",
	[311] = "(e)",
	[312] = "(f)",
	[313] = "(g)",
	[314] = "(h)",
	[315] = "(i)",
	[316] = "(j)",
	[317] = "(k)",
	[318] = "(l)",
	[319] = "(m)",
	[320] = "(n)",
	[321] = "(o)",
	[322] = "(p)",
	[323] = "(q)",
	[324] = "(r)",
	[325] = "(s)",
	[326] = "(t)",
	[327] = "(u)",
	[328] = "(v)",
	[329] = "(w)",
	[330] = "(x)",
	[331] = "(y)",
	[332] = "(z)",
	[333] = "(A)",
	[334] = "(B)",
	[335] = "(D)",
	[336] = "(E)",
	[337] = "(F)",
	[338] = "(G)",
	[339] = "(H)",
	[340] = "(I)",
	[341] = "(J)",
	[342] = "(K)",
	[343] = "(L)",
	[344] = "(M)",
	[345] = "(N)",
	[346] = "(O)",
	[347] = "(P)",
	[348] = "(Q)",
	[349] = "(S)",
	[350] = "(T)",
	[351] = "(U)",
	[352] = "(V)",
	[353] = "(W)",
	[354] = "(X)",
	[355] = "(Y)",
	[356] = "(Z)",
	[357] = "(0)",
	[358] = ":)",
	[359] = "[|",
	[360] = "|]",
	[361] = "((",
	[362] = "))",
	[363] = "|||",
	[364] = "{|",
	[365] = "|}",
	[366] = "(|",
	[367] = "|)",
	[368] = "<|",
	[369] = "|>",
	[370] = "#",
	[371] = "::=",
	[372] = "==",
	[373] = "===",
	[374] = "PTE",
	[375] = "(21)",
	[376] = "(22)",
	[377] = "(23)",
	[378] = "(24)",
	[379] = "(25)",
	[380] = "(26)",
	[381] = "(27)",
	[382] = "(28)",
	[383] = "(29)",
	[384] = "(30)",
	[385] = "(31)",
	[386] = "(32)",
	[387] = "(33)",
	[388] = "(34)",
	[389] = "(35)",
	[390] = "(36)",
	[391] = "(37)",
	[392] = "(38)",
	[393] = "(39)",
	[394] = "(40)",
	[395] = "(41)",
	[396] = "(42)",
	[397] = "(43)",
	[398] = "(44)",
	[399] = "(45)",
	[400] = "(46)",
	[401] = "(47)",
	[402] = "(48)",
	[403] = "(49)",
	[404] = "(50)",
	[405] = "Hg",
	[406] = "erg",
	[407] = "eV",
	[408] = "LTD",
	[409] = "hPa",
	[410] = "da",
	[411] = "AU",
	[412] = "bar",
	[413] = "oV",
	[414] = "pc",
	[415] = "dm",
	[416] = "dm^2",
	[417] = "dm^3",
	[418] = "IU",
	[419] = "pA",
	[420] = "nA",
	[421] = "uA",
	[422] = "mA",
	[423] = "kA",
	[424] = "KB",
	[425] = "MB",
	[426] = "GB",
	[427] = "cal",
	[428] = "kcal",
	[429] = "pF",
	[430] = "nF",
	[431] = "uF",
	[432] = "ug",
	[433] = "mg",
	[434] = "kg",
	[435] = "Hz",
	[436] = "kHz",
	[437] = "MHz",
	[438] = "GHz",
	[439] = "THz",
	[440] = "ul",
	[441] = "ml",
	[442] = "dl",
	[443] = "kl",
	[444] = "fm",
	[445] = "nm",
	[446] = "um",
	[447] = "mm",
	[448] = "cm",
	[449] = "km",
	[450] = "mm^2",
	[451] = "cm^2",
	[452] = "m^2",
	[453] = "km^2",
	[454] = "mm^3",
	[455] = "cm^3",
	[456] = "m^3",
	[457] = "km^3",
	[458] = "m/s",
	[459] = "m/s^2",
	[460] = "Pa",
	[461] = "kPa",
	[462] = "MPa",
	[463] = "GPa",
	[464] = "rad",
	[465] = "rad/s",
	[466] = "rad/s^2",
	[467] = "ps",
	[468] = "ns",
	[469] = "us",
	[470] = "ms",
	[471] = "pV",
	[472] = "nV",
	[473] = "uV",
	[474] = "mV",
	[475] = "kV",
	[476] = "MV",
	[477] = "pW",
	[478] = "nW",
	[479] = "uW",
	[480] = "mW",
	[481] = "kW",
	[482] = "MW",
	[483] = "a.m.",
	[484] = "Bq",
	[485] = "cc",
	[486] = "cd",
	[487] = "C/kg",
	[488] = "Co.",
	[489] = "dB",
	[490] = "Gy",
	[491] = "ha",
	[492] = "HP",
	[493] = "in",
	[494] = "KK",
	[495] = "KM",
	[496] = "kt",
	[497] = "lm",
	[498] = "ln",
	[499] = "log",
	[500] = "lx",
	[501] = "mb",
	[502] = "mil",
	[503] = "mol",
	[504] = "PH",
	[505] = "p.m.",
	[506] = "PPM",
	[507] = "PR",
	[508] = "sr",
	[509] = "Sv",
	[510] = "Wb",
	[511] = "V/m",
	[512] = "A/m",
	[513] = "gal",
	[514] = "ff",
	[515] = "fi",
	[516] = "fl",
	[517] = "ffi",
	[518] = "ffl",
	[519] = "st",
	[520] = "{",
	[521] = "}",
	[522] = "[",
	[523] = "]",
	[524] = "$",
	[525] = "%",
	[526] = "@",
	[527] = "0.",
	[528] = "0,",
	[529] = "1,",
	[530] = "2,",
	[531] = "3,",
	[532] = "4,",
	[533] = "5,",
	[534] = "6,",
	[535] = "7,",
	[536] = "8,",
	[537] = "9,",
	[538] = "(CD)",
	[539] = "(WZ)",
	[540] = "HV",
	[541] = "SD",
	[542] = "PPV",
	[543] = "WC",
	[544] = "MC",
	[545] = "MD",
	[546] = "MR",
	[547] = "DJ",
};

static const int16_t asciify[6080] = {
	[0xC2] = 256 - 0x80,	/* 302 ... */
	[256+0x20] = 1,	/* U+00A0: NO-BREAK SPACE */
	[256+0x21] = 2,	/* U+00A1: INVERTED EXCLAMATION MARK */
	[256+0x22] = 3,	/* U+00A2: CENT SIGN */
	[256+0x23] = 4,	/* U+00A3: POUND SIGN */
	[256+0x25] = 5,	/* U+00A5: YEN SIGN */
	[256+0x26] = 6,	/* U+00A6: BROKEN BAR */
	[256+0x29] = 7,	/* U+00A9: COPYRIGHT SIGN */
	[256+0x2A] = 8,	/* U+00AA: FEMININE ORDINAL INDICATOR */
	[256+0x2B] = 9,	/* U+00AB: LEFT-POINTING DOUBLE ANGLE QUOTATION MARK */
	[256+0x2C] = 2,	/* U+00AC: NOT SIGN */
	[256+0x2D] = 10,	/* U+00AD: SOFT HYPHEN */
	[256+0x2E] = 11,	/* U+00AE: REGISTERED SIGN */
	[256+0x31] = 12,	/* U+00B1: PLUS-MINUS SIGN */
	[256+0x32] = 13,	/* U+00B2: SUPERSCRIPT TWO */
	[256+0x33] = 14,	/* U+00B3: SUPERSCRIPT THREE */
	[256+0x34] = 15,	/* U+00B4: ACUTE ACCENT */
	[256+0x35] = 16,	/* U+00B5: MICRO SIGN */
	[256+0x37] = 17,	/* U+00B7: MIDDLE DOT */
	[256+0x38] = 18,	/* U+00B8: CEDILLA */
	[256+0x39] = 19,	/* U+00B9: SUPERSCRIPT ONE */
	[256+0x3A] = 20,	/* U+00BA: MASCULINE ORDINAL INDICATOR */
	[256+0x3B] = 21,	/* U+00BB: RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK */
	[256+0x3C] = 22,	/* U+00BC: VULGAR FRACTION ONE QUARTER */
	[256+0x3D] = 23,	/* U+00BD: VULGAR FRACTION ONE HALF */
	[256+0x3E] = 24,	/* U+00BE: VULGAR FRACTION THREE QUARTERS */
	[0xC3] = 320 - 0x80,	/* 303 ... */
	[320+0x00] = 25,	/* U+00C0: LATIN CAPITAL LETTER A WITH GRAVE */
	[320+0x01] = 25,	/* U+00C1: LATIN CAPITAL LETTER A WITH ACUTE */
	[320+0x02] = 25,	/* U+00C2: LATIN CAPITAL LETTER A WITH CIRCUMFLEX */
	[320+0x03] = 25,	/* U+00C3: LATIN CAPITAL LETTER A WITH TILDE */
	[320+0x04] = 25,	/* U+00C4: LATIN CAPITAL LETTER A WITH DIAERESIS */
	[320+0x05] = 25,	/* U+00C5: LATIN CAPITAL LETTER A WITH RING ABOVE */
	[320+0x06] = 26,	/* U+00C6: LATIN CAPITAL LETTER AE */
	[320+0x07] = 27,	/* U+00C7: LATIN CAPITAL LETTER C WITH CEDILLA */
	[320+0x08] = 28,	/* U+00C8: LATIN CAPITAL LETTER E WITH GRAVE */
	[320+0x09] = 28,	/* U+00C9: LATIN CAPITAL LETTER E WITH ACUTE */
	[320+0x0A] = 28,	/* U+00CA: LATIN CAPITAL LETTER E WITH CIRCUMFLEX */
	[320+0x0B] = 28,	/* U+00CB: LATIN CAPITAL LETTER E WITH DIAERESIS */
	[320+0x0C] = 29,	/* U+00CC: LATIN CAPITAL LETTER I WITH GRAVE */
	[320+0x0D] = 29,	/* U+00CD: LATIN CAPITAL LETTER I WITH ACUTE */
	[320+0x0E] = 29,	/* U+00CE: LATIN CAPITAL LETTER I WITH CIRCUMFLEX */
	[320+0x0F] = 29,	/* U+00CF: LATIN CAPITAL LETTER I WITH DIAERESIS */
	[320+0x10] = 30,	/* U+00D0: LATIN CAPITAL LETTER ETH */
	[320+0x11] = 31,	/* U+00D1: LATIN CAPITAL LETTER N WITH TILDE */
	[320+0x12] = 32,	/* U+00D2: LATIN CAPITAL LETTER O WITH GRAVE */
	[320+0x13] = 32,	/* U+00D3: LATIN CAPITAL LETTER O WITH ACUTE */
	[320+0x14] = 32,	/* U+00D4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX */
	[320+0x15] = 32,	/* U+00D5: LATIN CAPITAL LETTER O WITH TILDE */
	[320+0x16] = 32,	/* U+00D6: LATIN CAPITAL LETTER O WITH DIAERESIS */
	[320+0x17] = 33,	/* U+00D7: MULTIPLICATION SIGN */
	[320+0x18] = 32,	/* U+00D8: LATIN CAPITAL LETTER O WITH STROKE */
	[320+0x19] = 34,	/* U+00D9: LATIN CAPITAL LETTER U WITH GRAVE */
	[320+0x1A] = 34,	/* U+00DA: LATIN CAPITAL LETTER U WITH ACUTE */
	[320+0x1B] = 34,	/* U+00DB: LATIN CAPITAL LETTER U WITH CIRCUMFLEX */
	[320+0x1C] = 34,	/* U+00DC: LATIN CAPITAL LETTER U WITH DIAERESIS */
	[320+0x1D] = 35,	/* U+00DD: LATIN CAPITAL LETTER Y WITH ACUTE */
	[320+0x1E] = 36,	/* U+00DE: LATIN CAPITAL LETTER THORN */
	[320+0x1F] = 37,	/* U+00DF: LATIN SMALL LETTER SHARP S */
	[320+0x20] = 8,	/* U+00E0: LATIN SMALL LETTER A WITH GRAVE */
	[320+0x21] = 8,	/* U+00E1: LATIN SMALL LETTER A WITH ACUTE */
	[320+0x22] = 8,	/* U+00E2: LATIN SMALL LETTER A WITH CIRCUMFLEX */
	[320+0x23] = 8,	/* U+00E3: LATIN SMALL LETTER A WITH TILDE */
	[320+0x24] = 8,	/* U+00E4: LATIN SMALL LETTER A WITH DIAERESIS */
	[320+0x25] = 8,	/* U+00E5: LATIN SMALL LETTER A WITH RING ABOVE */
	[320+0x26] = 38,	/* U+00E6: LATIN SMALL LETTER AE */
	[320+0x27] = 3,	/* U+00E7: LATIN SMALL LETTER C WITH CEDILLA */
	[320+0x28] = 39,	/* U+00E8: LATIN SMALL LETTER E WITH GRAVE */
	[320+0x29] = 39,	/* U+00E9: LATIN SMALL LETTER E WITH ACUTE */
	[320+0x2A] = 39,	/* U+00EA: LATIN SMALL LETTER E WITH CIRCUMFLEX */
	[320+0x2B] = 39,	/* U+00EB: LATIN SMALL LETTER E WITH DIAERESIS */
	[320+0x2C] = 40,	/* U+00EC: LATIN SMALL LETTER I WITH GRAVE */
	[320+0x2D] = 40,	/* U+00ED: LATIN SMALL LETTER I WITH ACUTE */
	[320+0x2E] = 40,	/* U+00EE: LATIN SMALL LETTER I WITH CIRCUMFLEX */
	[320+0x2F] = 40,	/* U+00EF: LATIN SMALL LETTER I WITH DIAERESIS */
	[320+0x30] = 41,	/* U+00F0: LATIN SMALL LETTER ETH */
	[320+0x31] = 42,	/* U+00F1: LATIN SMALL LETTER N WITH TILDE */
	[320+0x32] = 20,	/* U+00F2: LATIN SMALL LETTER O WITH GRAVE */
	[320+0x33] = 20,	/* U+00F3: LATIN SMALL LETTER O WITH ACUTE */
	[320+0x34] = 20,	/* U+00F4: LATIN SMALL LETTER O WITH CIRCUMFLEX */
	[320+0x35] = 20,	/* U+00F5: LATIN SMALL LETTER O WITH TILDE */
	[320+0x36] = 20,	/* U+00F6: LATIN SMALL LETTER O WITH DIAERESIS */
	[320+0x37] = 43,	/* U+00F7: DIVISION SIGN */
	[320+0x38] = 20,	/* U+00F8: LATIN SMALL LETTER O WITH STROKE */
	[320+0x39] = 16,	/* U+00F9: LATIN SMALL LETTER U WITH GRAVE */
	[320+0x3A] = 16,	/* U+00FA: LATIN SMALL LETTER U WITH ACUTE */
	[320+0x3B] = 16,	/* U+00FB: LATIN SMALL LETTER U WITH CIRCUMFLEX */
	[320+0x3C] = 16,	/* U+00FC: LATIN SMALL LETTER U WITH DIAERESIS */
	[320+0x3D] = 44,	/* U+00FD: LATIN SMALL LETTER Y WITH ACUTE */
	[320+0x3E] = 45,	/* U+00FE: LATIN SMALL LETTER THORN */
	[320+0x3F] = 44,	/* U+00FF: LATIN SMALL LETTER Y WITH DIAERESIS */
	[0xC4] = 384 - 0x80,	/* 304 ... */
	[384+0x00] = 25,	/* U+0100: LATIN CAPITAL LETTER A WITH MACRON */
	[384+0x01] = 8,	/* U+0101: LATIN SMALL LETTER A WITH MACRON */
	[384+0x02] = 25,	/* U+0102: LATIN CAPITAL LETTER A WITH BREVE */
	[384+0x03] = 8,	/* U+0103: LATIN SMALL LETTER A WITH BREVE */
	[384+0x04] = 25,	/* U+0104: LATIN CAPITAL LETTER A WITH OGONEK */
	[384+0x05] = 8,	/* U+0105: LATIN SMALL LETTER A WITH OGONEK */
	[384+0x06] = 27,	/* U+0106: LATIN CAPITAL LETTER C WITH ACUTE */
	[384+0x07] = 3,	/* U+0107: LATIN SMALL LETTER C WITH ACUTE */
	[384+0x08] = 27,	/* U+0108: LATIN CAPITAL LETTER C WITH CIRCUMFLEX */
	[384+0x09] = 3,	/* U+0109: LATIN SMALL LETTER C WITH CIRCUMFLEX */
	[384+0x0A] = 27,	/* U+010A: LATIN CAPITAL LETTER C WITH DOT ABOVE */
	[384+0x0B] = 3,	/* U+010B: LATIN SMALL LETTER C WITH DOT ABOVE */
	[384+0x0C] = 27,	/* U+010C: LATIN CAPITAL LETTER C WITH CARON */
	[384+0x0D] = 3,	/* U+010D: LATIN SMALL LETTER C WITH CARON */
	[384+0x0E] = 30,	/* U+010E: LATIN CAPITAL LETTER D WITH CARON */
	[384+0x0F] = 41,	/* U+010F: LATIN SMALL LETTER D WITH CARON */
	[384+0x10] = 30,	/* U+0110: LATIN CAPITAL LETTER D WITH STROKE */
	[384+0x11] = 41,	/* U+0111: LATIN SMALL LETTER D WITH STROKE */
	[384+0x12] = 28,	/* U+0112: LATIN CAPITAL LETTER E WITH MACRON */
	[384+0x13] = 39,	/* U+0113: LATIN SMALL LETTER E WITH MACRON */
	[384+0x14] = 28,	/* U+0114: LATIN CAPITAL LETTER E WITH BREVE */
	[384+0x15] = 39,	/* U+0115: LATIN SMALL LETTER E WITH BREVE */
	[384+0x16] = 28,	/* U+0116: LATIN CAPITAL LETTER E WITH DOT ABOVE */
	[384+0x17] = 39,	/* U+0117: LATIN SMALL LETTER E WITH DOT ABOVE */
	[384+0x18] = 28,	/* U+0118: LATIN CAPITAL LETTER E WITH OGONEK */
	[384+0x19] = 39,	/* U+0119: LATIN SMALL LETTER E WITH OGONEK */
	[384+0x1A] = 28,	/* U+011A: LATIN CAPITAL LETTER E WITH CARON */
	[384+0x1B] = 39,	/* U+011B: LATIN SMALL LETTER E WITH CARON */
	[384+0x1C] = 46,	/* U+011C: LATIN CAPITAL LETTER G WITH CIRCUMFLEX */
	[384+0x1D] = 47,	/* U+011D: LATIN SMALL LETTER G WITH CIRCUMFLEX */
	[384+0x1E] = 46,	/* U+011E: LATIN CAPITAL LETTER G WITH BREVE */
	[384+0x1F] = 47,	/* U+011F: LATIN SMALL LETTER G WITH BREVE */
	[384+0x20] = 46,	/* U+0120: LATIN CAPITAL LETTER G WITH DOT ABOVE */
	[384+0x21] = 47,	/* U+0121: LATIN SMALL LETTER G WITH DOT ABOVE */
	[384+0x22] = 46,	/* U+0122: LATIN CAPITAL LETTER G WITH CEDILLA */
	[384+0x23] = 47,	/* U+0123: LATIN SMALL LETTER G WITH CEDILLA */
	[384+0x24] = 48,	/* U+0124: LATIN CAPITAL LETTER H WITH CIRCUMFLEX */
	[384+0x25] = 49,	/* U+0125: LATIN SMALL LETTER H WITH CIRCUMFLEX */
	[384+0x26] = 48,	/* U+0126: LATIN CAPITAL LETTER H WITH STROKE */
	[384+0x27] = 49,	/* U+0127: LATIN SMALL LETTER H WITH STROKE */
	[384+0x28] = 29,	/* U+0128: LATIN CAPITAL LETTER I WITH TILDE */
	[384+0x29] = 40,	/* U+0129: LATIN SMALL LETTER I WITH TILDE */
	[384+0x2A] = 29,	/* U+012A: LATIN CAPITAL LETTER I WITH MACRON */
	[384+0x2B] = 40,	/* U+012B: LATIN SMALL LETTER I WITH MACRON */
	[384+0x2C] = 29,	/* U+012C: LATIN CAPITAL LETTER I WITH BREVE */
	[384+0x2D] = 40,	/* U+012D: LATIN SMALL LETTER I WITH BREVE */
	[384+0x2E] = 29,	/* U+012E: LATIN CAPITAL LETTER I WITH OGONEK */
	[384+0x2F] = 40,	/* U+012F: LATIN SMALL LETTER I WITH OGONEK */
	[384+0x30] = 29,	/* U+0130: LATIN CAPITAL LETTER I WITH DOT ABOVE */
	[384+0x31] = 40,	/* U+0131: LATIN SMALL LETTER DOTLESS I */
	[384+0x32] = 50,	/* U+0132: LATIN CAPITAL LIGATURE IJ */
	[384+0x33] = 51,	/* U+0133: LATIN SMALL LIGATURE IJ */
	[384+0x34] = 52,	/* U+0134: LATIN CAPITAL LETTER J WITH CIRCUMFLEX */
	[384+0x35] = 53,	/* U+0135: LATIN SMALL LETTER J WITH CIRCUMFLEX */
	[384+0x36] = 54,	/* U+0136: LATIN CAPITAL LETTER K WITH CEDILLA */
	[384+0x37] = 55,	/* U+0137: LATIN SMALL LETTER K WITH CEDILLA */
	[384+0x38] = 56,	/* U+0138: LATIN SMALL LETTER KRA */
	[384+0x39] = 57,	/* U+0139: LATIN CAPITAL LETTER L WITH ACUTE */
	[384+0x3A] = 58,	/* U+013A: LATIN SMALL LETTER L WITH ACUTE */
	[384+0x3B] = 57,	/* U+013B: LATIN CAPITAL LETTER L WITH CEDILLA */
	[384+0x3C] = 58,	/* U+013C: LATIN SMALL LETTER L WITH CEDILLA */
	[384+0x3D] = 57,	/* U+013D: LATIN CAPITAL LETTER L WITH CARON */
	[384+0x3E] = 58,	/* U+013E: LATIN SMALL LETTER L WITH CARON */
	[384+0x3F] = 57,	/* U+013F: LATIN CAPITAL LETTER L WITH MIDDLE DOT */
	[0xC5] = 448 - 0x80,	/* 305 ... */
	[448+0x00] = 58,	/* U+0140: LATIN SMALL LETTER L WITH MIDDLE DOT */
	[448+0x01] = 57,	/* U+0141: LATIN CAPITAL LETTER L WITH STROKE */
	[448+0x02] = 58,	/* U+0142: LATIN SMALL LETTER L WITH STROKE */
	[448+0x03] = 31,	/* U+0143: LATIN CAPITAL LETTER N WITH ACUTE */
	[448+0x04] = 42,	/* U+0144: LATIN SMALL LETTER N WITH ACUTE */
	[448+0x05] = 31,	/* U+0145: LATIN CAPITAL LETTER N WITH CEDILLA */
	[448+0x06] = 42,	/* U+0146: LATIN SMALL LETTER N WITH CEDILLA */
	[448+0x07] = 31,	/* U+0147: LATIN CAPITAL LETTER N WITH CARON */
	[448+0x08] = 42,	/* U+0148: LATIN SMALL LETTER N WITH CARON */
	[448+0x09] = 59,	/* U+0149: LATIN SMALL LETTER N PRECEDED BY APOSTROPHE */
	[448+0x0A] = 31,	/* U+014A: LATIN CAPITAL LETTER ENG */
	[448+0x0B] = 42,	/* U+014B: LATIN SMALL LETTER ENG */
	[448+0x0C] = 32,	/* U+014C: LATIN CAPITAL LETTER O WITH MACRON */
	[448+0x0D] = 20,	/* U+014D: LATIN SMALL LETTER O WITH MACRON */
	[448+0x0E] = 32,	/* U+014E: LATIN CAPITAL LETTER O WITH BREVE */
	[448+0x0F] = 20,	/* U+014F: LATIN SMALL LETTER O WITH BREVE */
	[448+0x10] = 32,	/* U+0150: LATIN CAPITAL LETTER O WITH DOUBLE ACUTE */
	[448+0x11] = 20,	/* U+0151: LATIN SMALL LETTER O WITH DOUBLE ACUTE */
	[448+0x12] = 60,	/* U+0152: LATIN CAPITAL LIGATURE OE */
	[448+0x13] = 61,	/* U+0153: LATIN SMALL LIGATURE OE */
	[448+0x14] = 62,	/* U+0154: LATIN CAPITAL LETTER R WITH ACUTE */
	[448+0x15] = 63,	/* U+0155: LATIN SMALL LETTER R WITH ACUTE */
	[448+0x16] = 62,	/* U+0156: LATIN CAPITAL LETTER R WITH CEDILLA */
	[448+0x17] = 63,	/* U+0157: LATIN SMALL LETTER R WITH CEDILLA */
	[448+0x18] = 62,	/* U+0158: LATIN CAPITAL LETTER R WITH CARON */
	[448+0x19] = 63,	/* U+0159: LATIN SMALL LETTER R WITH CARON */
	[448+0x1A] = 64,	/* U+015A: LATIN CAPITAL LETTER S WITH ACUTE */
	[448+0x1B] = 65,	/* U+015B: LATIN SMALL LETTER S WITH ACUTE */
	[448+0x1C] = 64,	/* U+015C: LATIN CAPITAL LETTER S WITH CIRCUMFLEX */
	[448+0x1D] = 65,	/* U+015D: LATIN SMALL LETTER S WITH CIRCUMFLEX */
	[448+0x1E] = 64,	/* U+015E: LATIN CAPITAL LETTER S WITH CEDILLA */
	[448+0x1F] = 65,	/* U+015F: LATIN SMALL LETTER S WITH CEDILLA */
	[448+0x20] = 64,	/* U+0160: LATIN CAPITAL LETTER S WITH CARON */
	[448+0x21] = 65,	/* U+0161: LATIN SMALL LETTER S WITH CARON */
	[448+0x22] = 66,	/* U+0162: LATIN CAPITAL LETTER T WITH CEDILLA */
	[448+0x23] = 67,	/* U+0163: LATIN SMALL LETTER T WITH CEDILLA */
	[448+0x24] = 66,	/* U+0164: LATIN CAPITAL LETTER T WITH CARON */
	[448+0x25] = 67,	/* U+0165: LATIN SMALL LETTER T WITH CARON */
	[448+0x26] = 66,	/* U+0166: LATIN CAPITAL LETTER T WITH STROKE */
	[448+0x27] = 67,	/* U+0167: LATIN SMALL LETTER T WITH STROKE */
	[448+0x28] = 34,	/* U+0168: LATIN CAPITAL LETTER U WITH TILDE */
	[448+0x29] = 16,	/* U+0169: LATIN SMALL LETTER U WITH TILDE */
	[448+0x2A] = 34,	/* U+016A: LATIN CAPITAL LETTER U WITH MACRON */
	[448+0x2B] = 16,	/* U+016B: LATIN SMALL LETTER U WITH MACRON */
	[448+0x2C] = 34,	/* U+016C: LATIN CAPITAL LETTER U WITH BREVE */
	[448+0x2D] = 16,	/* U+016D: LATIN SMALL LETTER U WITH BREVE */
	[448+0x2E] = 34,	/* U+016E: LATIN CAPITAL LETTER U WITH RING ABOVE */
	[448+0x2F] = 16,	/* U+016F: LATIN SMALL LETTER U WITH RING ABOVE */
	[448+0x30] = 34,	/* U+0170: LATIN CAPITAL LETTER U WITH DOUBLE ACUTE */
	[448+0x31] = 16,	/* U+0171: LATIN SMALL LETTER U WITH DOUBLE ACUTE */
	[448+0x32] = 34,	/* U+0172: LATIN CAPITAL LETTER U WITH OGONEK */
	[448+0x33] = 16,	/* U+0173: LATIN SMALL LETTER U WITH OGONEK */
	[448+0x34] = 68,	/* U+0174: LATIN CAPITAL LETTER W WITH CIRCUMFLEX */
	[448+0x35] = 69,	/* U+0175: LATIN SMALL LETTER W WITH CIRCUMFLEX */
	[448+0x36] = 35,	/* U+0176: LATIN CAPITAL LETTER Y WITH CIRCUMFLEX */
	[448+0x37] = 44,	/* U+0177: LATIN SMALL LETTER Y WITH CIRCUMFLEX */
	[448+0x38] = 35,	/* U+0178: LATIN CAPITAL LETTER Y WITH DIAERESIS */
	[448+0x39] = 70,	/* U+0179: LATIN CAPITAL LETTER Z WITH ACUTE */
	[448+0x3A] = 71,	/* U+017A: LATIN SMALL LETTER Z WITH ACUTE */
	[448+0x3B] = 70,	/* U+017B: LATIN CAPITAL LETTER Z WITH DOT ABOVE */
	[448+0x3C] = 71,	/* U+017C: LATIN SMALL LETTER Z WITH DOT ABOVE */
	[448+0x3D] = 70,	/* U+017D: LATIN CAPITAL LETTER Z WITH CARON */
	[448+0x3E] = 71,	/* U+017E: LATIN SMALL LETTER Z WITH CARON */
	[448+0x3F] = 65,	/* U+017F: LATIN SMALL LETTER LONG S */
	[0xC6] = 512 - 0x80,	/* 306 ... */
	[512+0x00] = 72,	/* U+0180: LATIN SMALL LETTER B WITH STROKE */
	[512+0x01] = 73,	/* U+0181: LATIN CAPITAL LETTER B WITH HOOK */
	[512+0x02] = 73,	/* U+0182: LATIN CAPITAL LETTER B WITH TOPBAR */
	[512+0x03] = 72,	/* U+0183: LATIN SMALL LETTER B WITH TOPBAR */
	[512+0x07] = 27,	/* U+0187: LATIN CAPITAL LETTER C WITH HOOK */
	[512+0x08] = 3,	/* U+0188: LATIN SMALL LETTER C WITH HOOK */
	[512+0x09] = 30,	/* U+0189: LATIN CAPITAL LETTER AFRICAN D */
	[512+0x0A] = 30,	/* U+018A: LATIN CAPITAL LETTER D WITH HOOK */
	[512+0x0B] = 30,	/* U+018B: LATIN CAPITAL LETTER D WITH TOPBAR */
	[512+0x0C] = 41,	/* U+018C: LATIN SMALL LETTER D WITH TOPBAR */
	[512+0x10] = 28,	/* U+0190: LATIN CAPITAL LETTER OPEN E */
	[512+0x11] = 74,	/* U+0191: LATIN CAPITAL LETTER F WITH HOOK */
	[512+0x12] = 75,	/* U+0192: LATIN SMALL LETTER F WITH HOOK */
	[512+0x13] = 46,	/* U+0193: LATIN CAPITAL LETTER G WITH HOOK */
	[512+0x15] = 76,	/* U+0195: LATIN SMALL LETTER HV */
	[512+0x16] = 29,	/* U+0196: LATIN CAPITAL LETTER IOTA */
	[512+0x17] = 29,	/* U+0197: LATIN CAPITAL LETTER I WITH STROKE */
	[512+0x18] = 54,	/* U+0198: LATIN CAPITAL LETTER K WITH HOOK */
	[512+0x19] = 55,	/* U+0199: LATIN SMALL LETTER K WITH HOOK */
	[512+0x1A] = 58,	/* U+019A: LATIN SMALL LETTER L WITH BAR */
	[512+0x1D] = 31,	/* U+019D: LATIN CAPITAL LETTER N WITH LEFT HOOK */
	[512+0x1E] = 42,	/* U+019E: LATIN SMALL LETTER N WITH LONG RIGHT LEG */
	[512+0x20] = 32,	/* U+01A0: LATIN CAPITAL LETTER O WITH HORN */
	[512+0x21] = 20,	/* U+01A1: LATIN SMALL LETTER O WITH HORN */
	[512+0x22] = 77,	/* U+01A2: LATIN CAPITAL LETTER OI */
	[512+0x23] = 78,	/* U+01A3: LATIN SMALL LETTER OI */
	[512+0x24] = 79,	/* U+01A4: LATIN CAPITAL LETTER P WITH HOOK */
	[512+0x25] = 80,	/* U+01A5: LATIN SMALL LETTER P WITH HOOK */
	[512+0x2B] = 67,	/* U+01AB: LATIN SMALL LETTER T WITH PALATAL HOOK */
	[512+0x2C] = 66,	/* U+01AC: LATIN CAPITAL LETTER T WITH HOOK */
	[512+0x2D] = 67,	/* U+01AD: LATIN SMALL LETTER T WITH HOOK */
	[512+0x2E] = 66,	/* U+01AE: LATIN CAPITAL LETTER T WITH RETROFLEX HOOK */
	[512+0x2F] = 34,	/* U+01AF: LATIN CAPITAL LETTER U WITH HORN */
	[512+0x30] = 16,	/* U+01B0: LATIN SMALL LETTER U WITH HORN */
	[512+0x32] = 81,	/* U+01B2: LATIN CAPITAL LETTER V WITH HOOK */
	[512+0x33] = 35,	/* U+01B3: LATIN CAPITAL LETTER Y WITH HOOK */
	[512+0x34] = 44,	/* U+01B4: LATIN SMALL LETTER Y WITH HOOK */
	[512+0x35] = 70,	/* U+01B5: LATIN CAPITAL LETTER Z WITH STROKE */
	[512+0x36] = 71,	/* U+01B6: LATIN SMALL LETTER Z WITH STROKE */
	[0xC7] = 576 - 0x80,	/* 307 ... */
	[576+0x07] = 82,	/* U+01C7: LATIN CAPITAL LETTER LJ */
	[576+0x08] = 83,	/* U+01C8: LATIN CAPITAL LETTER L WITH SMALL LETTER J */
	[576+0x09] = 84,	/* U+01C9: LATIN SMALL LETTER LJ */
	[576+0x0A] = 85,	/* U+01CA: LATIN CAPITAL LETTER NJ */
	[576+0x0B] = 86,	/* U+01CB: LATIN CAPITAL LETTER N WITH SMALL LETTER J */
	[576+0x0C] = 87,	/* U+01CC: LATIN SMALL LETTER NJ */
	[576+0x0D] = 25,	/* U+01CD: LATIN CAPITAL LETTER A WITH CARON */
	[576+0x0E] = 8,	/* U+01CE: LATIN SMALL LETTER A WITH CARON */
	[576+0x0F] = 29,	/* U+01CF: LATIN CAPITAL LETTER I WITH CARON */
	[576+0x10] = 40,	/* U+01D0: LATIN SMALL LETTER I WITH CARON */
	[576+0x11] = 32,	/* U+01D1: LATIN CAPITAL LETTER O WITH CARON */
	[576+0x12] = 20,	/* U+01D2: LATIN SMALL LETTER O WITH CARON */
	[576+0x13] = 34,	/* U+01D3: LATIN CAPITAL LETTER U WITH CARON */
	[576+0x14] = 16,	/* U+01D4: LATIN SMALL LETTER U WITH CARON */
	[576+0x15] = 34,	/* U+01D5: LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON */
	[576+0x16] = 16,	/* U+01D6: LATIN SMALL LETTER U WITH DIAERESIS AND MACRON */
	[576+0x17] = 34,	/* U+01D7: LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE */
	[576+0x18] = 16,	/* U+01D8: LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE */
	[576+0x19] = 34,	/* U+01D9: LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON */
	[576+0x1A] = 16,	/* U+01DA: LATIN SMALL LETTER U WITH DIAERESIS AND CARON */
	[576+0x1B] = 34,	/* U+01DB: LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE */
	[576+0x1C] = 16,	/* U+01DC: LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE */
	[576+0x1E] = 25,	/* U+01DE: LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON */
	[576+0x1F] = 8,	/* U+01DF: LATIN SMALL LETTER A WITH DIAERESIS AND MACRON */
	[576+0x20] = 25,	/* U+01E0: LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON */
	[576+0x21] = 8,	/* U+01E1: LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON */
	[576+0x22] = 26,	/* U+01E2: LATIN CAPITAL LETTER AE WITH MACRON */
	[576+0x23] = 38,	/* U+01E3: LATIN SMALL LETTER AE WITH MACRON */
	[576+0x24] = 46,	/* U+01E4: LATIN CAPITAL LETTER G WITH STROKE */
	[576+0x25] = 47,	/* U+01E5: LATIN SMALL LETTER G WITH STROKE */
	[576+0x26] = 46,	/* U+01E6: LATIN CAPITAL LETTER G WITH CARON */
	[576+0x27] = 47,	/* U+01E7: LATIN SMALL LETTER G WITH CARON */
	[576+0x28] = 54,	/* U+01E8: LATIN CAPITAL LETTER K WITH CARON */
	[576+0x29] = 55,	/* U+01E9: LATIN SMALL LETTER K WITH CARON */
	[576+0x2A] = 32,	/* U+01EA: LATIN CAPITAL LETTER O WITH OGONEK */
	[576+0x2B] = 20,	/* U+01EB: LATIN SMALL LETTER O WITH OGONEK */
	[576+0x2C] = 32,	/* U+01EC: LATIN CAPITAL LETTER O WITH OGONEK AND MACRON */
	[576+0x2D] = 20,	/* U+01ED: LATIN SMALL LETTER O WITH OGONEK AND MACRON */
	[576+0x30] = 53,	/* U+01F0: LATIN SMALL LETTER J WITH CARON */
	[576+0x31] = 88,	/* U+01F1: LATIN CAPITAL LETTER DZ */
	[576+0x32] = 89,	/* U+01F2: LATIN CAPITAL LETTER D WITH SMALL LETTER Z */
	[576+0x33] = 90,	/* U+01F3: LATIN SMALL LETTER DZ */
	[576+0x34] = 46,	/* U+01F4: LATIN CAPITAL LETTER G WITH ACUTE */
	[576+0x35] = 47,	/* U+01F5: LATIN SMALL LETTER G WITH ACUTE */
	[576+0x38] = 31,	/* U+01F8: LATIN CAPITAL LETTER N WITH GRAVE */
	[576+0x39] = 42,	/* U+01F9: LATIN SMALL LETTER N WITH GRAVE */
	[576+0x3A] = 25,	/* U+01FA: LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE */
	[576+0x3B] = 8,	/* U+01FB: LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE */
	[576+0x3C] = 26,	/* U+01FC: LATIN CAPITAL LETTER AE WITH ACUTE */
	[576+0x3D] = 38,	/* U+01FD: LATIN SMALL LETTER AE WITH ACUTE */
	[576+0x3E] = 32,	/* U+01FE: LATIN CAPITAL LETTER O WITH STROKE AND ACUTE */
	[576+0x3F] = 20,	/* U+01FF: LATIN SMALL LETTER O WITH STROKE AND ACUTE */
	[0xC8] = 640 - 0x80,	/* 310 ... */
	[640+0x00] = 25,	/* U+0200: LATIN CAPITAL LETTER A WITH DOUBLE GRAVE */
	[640+0x01] = 8,	/* U+0201: LATIN SMALL LETTER A WITH DOUBLE GRAVE */
	[640+0x02] = 25,	/* U+0202: LATIN CAPITAL LETTER A WITH INVERTED BREVE */
	[640+0x03] = 8,	/* U+0203: LATIN SMALL LETTER A WITH INVERTED BREVE */
	[640+0x04] = 28,	/* U+0204: LATIN CAPITAL LETTER E WITH DOUBLE GRAVE */
	[640+0x05] = 39,	/* U+0205: LATIN SMALL LETTER E WITH DOUBLE GRAVE */
	[640+0x06] = 28,	/* U+0206: LATIN CAPITAL LETTER E WITH INVERTED BREVE */
	[640+0x07] = 39,	/* U+0207: LATIN SMALL LETTER E WITH INVERTED BREVE */
	[640+0x08] = 29,	/* U+0208: LATIN CAPITAL LETTER I WITH DOUBLE GRAVE */
	[640+0x09] = 40,	/* U+0209: LATIN SMALL LETTER I WITH DOUBLE GRAVE */
	[640+0x0A] = 29,	/* U+020A: LATIN CAPITAL LETTER I WITH INVERTED BREVE */
	[640+0x0B] = 40,	/* U+020B: LATIN SMALL LETTER I WITH INVERTED BREVE */
	[640+0x0C] = 32,	/* U+020C: LATIN CAPITAL LETTER O WITH DOUBLE GRAVE */
	[640+0x0D] = 20,	/* U+020D: LATIN SMALL LETTER O WITH DOUBLE GRAVE */
	[640+0x0E] = 32,	/* U+020E: LATIN CAPITAL LETTER O WITH INVERTED BREVE */
	[640+0x0F] = 20,	/* U+020F: LATIN SMALL LETTER O WITH INVERTED BREVE */
	[640+0x10] = 62,	/* U+0210: LATIN CAPITAL LETTER R WITH DOUBLE GRAVE */
	[640+0x11] = 63,	/* U+0211: LATIN SMALL LETTER R WITH DOUBLE GRAVE */
	[640+0x12] = 62,	/* U+0212: LATIN CAPITAL LETTER R WITH INVERTED BREVE */
	[640+0x13] = 63,	/* U+0213: LATIN SMALL LETTER R WITH INVERTED BREVE */
	[640+0x14] = 34,	/* U+0214: LATIN CAPITAL LETTER U WITH DOUBLE GRAVE */
	[640+0x15] = 16,	/* U+0215: LATIN SMALL LETTER U WITH DOUBLE GRAVE */
	[640+0x16] = 34,	/* U+0216: LATIN CAPITAL LETTER U WITH INVERTED BREVE */
	[640+0x17] = 16,	/* U+0217: LATIN SMALL LETTER U WITH INVERTED BREVE */
	[640+0x18] = 64,	/* U+0218: LATIN CAPITAL LETTER S WITH COMMA BELOW */
	[640+0x19] = 65,	/* U+0219: LATIN SMALL LETTER S WITH COMMA BELOW */
	[640+0x1A] = 66,	/* U+021A: LATIN CAPITAL LETTER T WITH COMMA BELOW */
	[640+0x1B] = 67,	/* U+021B: LATIN SMALL LETTER T WITH COMMA BELOW */
	[640+0x1E] = 48,	/* U+021E: LATIN CAPITAL LETTER H WITH CARON */
	[640+0x1F] = 49,	/* U+021F: LATIN SMALL LETTER H WITH CARON */
	[640+0x21] = 41,	/* U+0221: LATIN SMALL LETTER D WITH CURL */
	[640+0x24] = 70,	/* U+0224: LATIN CAPITAL LETTER Z WITH HOOK */
	[640+0x25] = 71,	/* U+0225: LATIN SMALL LETTER Z WITH HOOK */
	[640+0x26] = 25,	/* U+0226: LATIN CAPITAL LETTER A WITH DOT ABOVE */
	[640+0x27] = 8,	/* U+0227: LATIN SMALL LETTER A WITH DOT ABOVE */
	[640+0x28] = 28,	/* U+0228: LATIN CAPITAL LETTER E WITH CEDILLA */
	[640+0x29] = 39,	/* U+0229: LATIN SMALL LETTER E WITH CEDILLA */
	[640+0x2A] = 32,	/* U+022A: LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON */
	[640+0x2B] = 20,	/* U+022B: LATIN SMALL LETTER O WITH DIAERESIS AND MACRON */
	[640+0x2C] = 32,	/* U+022C: LATIN CAPITAL LETTER O WITH TILDE AND MACRON */
	[640+0x2D] = 20,	/* U+022D: LATIN SMALL LETTER O WITH TILDE AND MACRON */
	[640+0x2E] = 32,	/* U+022E: LATIN CAPITAL LETTER O WITH DOT ABOVE */
	[640+0x2F] = 20,	/* U+022F: LATIN SMALL LETTER O WITH DOT ABOVE */
	[640+0x30] = 32,	/* U+0230: LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON */
	[640+0x31] = 20,	/* U+0231: LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON */
	[640+0x32] = 35,	/* U+0232: LATIN CAPITAL LETTER Y WITH MACRON */
	[640+0x33] = 44,	/* U+0233: LATIN SMALL LETTER Y WITH MACRON */
	[640+0x34] = 58,	/* U+0234: LATIN SMALL LETTER L WITH CURL */
	[640+0x35] = 42,	/* U+0235: LATIN SMALL LETTER N WITH CURL */
	[640+0x36] = 67,	/* U+0236: LATIN SMALL LETTER T WITH CURL */
	[640+0x37] = 53,	/* U+0237: LATIN SMALL LETTER DOTLESS J */
	[640+0x38] = 91,	/* U+0238: LATIN SMALL LETTER DB DIGRAPH */
	[640+0x39] = 92,	/* U+0239: LATIN SMALL LETTER QP DIGRAPH */
	[640+0x3A] = 25,	/* U+023A: LATIN CAPITAL LETTER A WITH STROKE */
	[640+0x3B] = 27,	/* U+023B: LATIN CAPITAL LETTER C WITH STROKE */
	[640+0x3C] = 3,	/* U+023C: LATIN SMALL LETTER C WITH STROKE */
	[640+0x3D] = 57,	/* U+023D: LATIN CAPITAL LETTER L WITH BAR */
	[640+0x3E] = 66,	/* U+023E: LATIN CAPITAL LETTER T WITH DIAGONAL STROKE */
	[640+0x3F] = 65,	/* U+023F: LATIN SMALL LETTER S WITH SWASH TAIL */
	[0xC9] = 704 - 0x80,	/* 311 ... */
	[704+0x00] = 71,	/* U+0240: LATIN SMALL LETTER Z WITH SWASH TAIL */
	[704+0x03] = 73,	/* U+0243: LATIN CAPITAL LETTER B WITH STROKE */
	[704+0x04] = 34,	/* U+0244: LATIN CAPITAL LETTER U BAR */
	[704+0x06] = 28,	/* U+0246: LATIN CAPITAL LETTER E WITH STROKE */
	[704+0x07] = 39,	/* U+0247: LATIN SMALL LETTER E WITH STROKE */
	[704+0x08] = 52,	/* U+0248: LATIN CAPITAL LETTER J WITH STROKE */
	[704+0x09] = 53,	/* U+0249: LATIN SMALL LETTER J WITH STROKE */
	[704+0x0C] = 62,	/* U+024C: LATIN CAPITAL LETTER R WITH STROKE */
	[704+0x0D] = 63,	/* U+024D: LATIN SMALL LETTER R WITH STROKE */
	[704+0x0E] = 35,	/* U+024E: LATIN CAPITAL LETTER Y WITH STROKE */
	[704+0x0F] = 44,	/* U+024F: LATIN SMALL LETTER Y WITH STROKE */
	[704+0x13] = 72,	/* U+0253: LATIN SMALL LETTER B WITH HOOK */
	[704+0x15] = 3,	/* U+0255: LATIN SMALL LETTER C WITH CURL */
	[704+0x16] = 41,	/* U+0256: LATIN SMALL LETTER D WITH TAIL */
	[704+0x17] = 41,	/* U+0257: LATIN SMALL LETTER D WITH HOOK */
	[704+0x1B] = 39,	/* U+025B: LATIN SMALL LETTER OPEN E */
	[704+0x1F] = 53,	/* U+025F: LATIN SMALL LETTER DOTLESS J WITH STROKE */
	[704+0x20] = 47,	/* U+0260: LATIN SMALL LETTER G WITH HOOK */
	[704+0x21] = 47,	/* U+0261: LATIN SMALL LETTER SCRIPT G */
	[704+0x22] = 46,	/* U+0262: LATIN LETTER SMALL CAPITAL G */
	[704+0x26] = 49,	/* U+0266: LATIN SMALL LETTER H WITH HOOK */
	[704+0x27] = 49,	/* U+0267: LATIN SMALL LETTER HENG WITH HOOK */
	[704+0x28] = 40,	/* U+0268: LATIN SMALL LETTER I WITH STROKE */
	[704+0x2A] = 29,	/* U+026A: LATIN LETTER SMALL CAPITAL I */
	[704+0x2B] = 58,	/* U+026B: LATIN SMALL LETTER L WITH MIDDLE TILDE */
	[704+0x2C] = 58,	/* U+026C: LATIN SMALL LETTER L WITH BELT */
	[704+0x2D] = 58,	/* U+026D: LATIN SMALL LETTER L WITH RETROFLEX HOOK */
	[704+0x31] = 93,	/* U+0271: LATIN SMALL LETTER M WITH HOOK */
	[704+0x32] = 42,	/* U+0272: LATIN SMALL LETTER N WITH LEFT HOOK */
	[704+0x33] = 42,	/* U+0273: LATIN SMALL LETTER N WITH RETROFLEX HOOK */
	[704+0x34] = 31,	/* U+0274: LATIN LETTER SMALL CAPITAL N */
	[704+0x36] = 60,	/* U+0276: LATIN LETTER SMALL CAPITAL OE */
	[704+0x3C] = 63,	/* U+027C: LATIN SMALL LETTER R WITH LONG LEG */
	[704+0x3D] = 63,	/* U+027D: LATIN SMALL LETTER R WITH TAIL */
	[704+0x3E] = 63,	/* U+027E: LATIN SMALL LETTER R WITH FISHHOOK */
	[0xCA] = 768 - 0x80,	/* 312 ... */
	[768+0x00] = 62,	/* U+0280: LATIN LETTER SMALL CAPITAL R */
	[768+0x02] = 65,	/* U+0282: LATIN SMALL LETTER S WITH HOOK */
	[768+0x08] = 67,	/* U+0288: LATIN SMALL LETTER T WITH RETROFLEX HOOK */
	[768+0x09] = 16,	/* U+0289: LATIN SMALL LETTER U BAR */
	[768+0x0B] = 94,	/* U+028B: LATIN SMALL LETTER V WITH HOOK */
	[768+0x0F] = 35,	/* U+028F: LATIN LETTER SMALL CAPITAL Y */
	[768+0x10] = 71,	/* U+0290: LATIN SMALL LETTER Z WITH RETROFLEX HOOK */
	[768+0x11] = 71,	/* U+0291: LATIN SMALL LETTER Z WITH CURL */
	[768+0x19] = 73,	/* U+0299: LATIN LETTER SMALL CAPITAL B */
	[768+0x1B] = 46,	/* U+029B: LATIN LETTER SMALL CAPITAL G WITH HOOK */
	[768+0x1C] = 48,	/* U+029C: LATIN LETTER SMALL CAPITAL H */
	[768+0x1D] = 53,	/* U+029D: LATIN SMALL LETTER J WITH CROSSED-TAIL */
	[768+0x1F] = 57,	/* U+029F: LATIN LETTER SMALL CAPITAL L */
	[768+0x20] = 56,	/* U+02A0: LATIN SMALL LETTER Q WITH HOOK */
	[768+0x23] = 90,	/* U+02A3: LATIN SMALL LETTER DZ DIGRAPH */
	[768+0x25] = 90,	/* U+02A5: LATIN SMALL LETTER DZ DIGRAPH WITH CURL */
	[768+0x26] = 95,	/* U+02A6: LATIN SMALL LETTER TS DIGRAPH */
	[768+0x2A] = 96,	/* U+02AA: LATIN SMALL LETTER LS DIGRAPH */
	[768+0x2B] = 97,	/* U+02AB: LATIN SMALL LETTER LZ DIGRAPH */
	[768+0x30] = 49,	/* U+02B0: MODIFIER LETTER SMALL H */
	[768+0x32] = 53,	/* U+02B2: MODIFIER LETTER SMALL J */
	[768+0x33] = 63,	/* U+02B3: MODIFIER LETTER SMALL R */
	[768+0x37] = 69,	/* U+02B7: MODIFIER LETTER SMALL W */
	[768+0x38] = 44,	/* U+02B8: MODIFIER LETTER SMALL Y */
	[768+0x3C] = 15,	/* U+02BC: MODIFIER LETTER APOSTROPHE */
	[0xCB] = 832 - 0x80,	/* 313 ... */
	[832+0x06] = 98,	/* U+02C6: MODIFIER LETTER CIRCUMFLEX ACCENT */
	[832+0x08] = 15,	/* U+02C8: MODIFIER LETTER VERTICAL LINE */
	[832+0x0B] = 99,	/* U+02CB: MODIFIER LETTER GRAVE ACCENT */
	[832+0x0D] = 100,	/* U+02CD: MODIFIER LETTER LOW MACRON */
	[832+0x10] = 101,	/* U+02D0: MODIFIER LETTER TRIANGULAR COLON */
	[832+0x1C] = 102,	/* U+02DC: SMALL TILDE */
	[832+0x1D] = 103,	/* U+02DD: DOUBLE ACUTE ACCENT */
	[832+0x21] = 58,	/* U+02E1: MODIFIER LETTER SMALL L */
	[832+0x22] = 65,	/* U+02E2: MODIFIER LETTER SMALL S */
	[832+0x23] = 33,	/* U+02E3: MODIFIER LETTER SMALL X */
	[0xCC] = 896 - 0x80,	/* 314 ... */
	[896+0x00] = 104,	/* U+0300: COMBINING GRAVE ACCENT */
	[896+0x01] = 104,	/* U+0301: COMBINING ACUTE ACCENT */
	[896+0x02] = 104,	/* U+0302: COMBINING CIRCUMFLEX ACCENT */
	[896+0x03] = 104,	/* U+0303: COMBINING TILDE */
	[896+0x04] = 104,	/* U+0304: COMBINING MACRON */
	[896+0x05] = 104,	/* U+0305: COMBINING OVERLINE */
	[896+0x06] = 104,	/* U+0306: COMBINING BREVE */
	[896+0x07] = 104,	/* U+0307: COMBINING DOT ABOVE */
	[896+0x08] = 104,	/* U+0308: COMBINING DIAERESIS */
	[896+0x09] = 104,	/* U+0309: COMBINING HOOK ABOVE */
	[896+0x0A] = 104,	/* U+030A: COMBINING RING ABOVE */
	[896+0x0B] = 104,	/* U+030B: COMBINING DOUBLE ACUTE ACCENT */
	[896+0x0C] = 104,	/* U+030C: COMBINING CARON */
	[896+0x0D] = 104,	/* U+030D: COMBINING VERTICAL LINE ABOVE */
	[896+0x0E] = 104,	/* U+030E: COMBINING DOUBLE VERTICAL LINE ABOVE */
	[896+0x0F] = 104,	/* U+030F: COMBINING DOUBLE GRAVE ACCENT */
	[896+0x10] = 104,	/* U+0310: COMBINING CANDRABINDU */
	[896+0x11] = 104,	/* U+0311: COMBINING INVERTED BREVE */
	[896+0x12] = 104,	/* U+0312: COMBINING TURNED COMMA ABOVE */
	[896+0x13] = 104,	/* U+0313: COMBINING COMMA ABOVE */
	[896+0x14] = 104,	/* U+0314: COMBINING REVERSED COMMA ABOVE */
	[896+0x15] = 104,	/* U+0315: COMBINING COMMA ABOVE RIGHT */
	[896+0x16] = 104,	/* U+0316: COMBINING GRAVE ACCENT BELOW */
	[896+0x17] = 104,	/* U+0317: COMBINING ACUTE ACCENT BELOW */
	[896+0x18] = 104,	/* U+0318: COMBINING LEFT TACK BELOW */
	[896+0x19] = 104,	/* U+0319: COMBINING RIGHT TACK BELOW */
	[896+0x1A] = 104,	/* U+031A: COMBINING LEFT ANGLE ABOVE */
	[896+0x1B] = 104,	/* U+031B: COMBINING HORN */
	[896+0x1C] = 104,	/* U+031C: COMBINING LEFT HALF RING BELOW */
	[896+0x1D] = 104,	/* U+031D: COMBINING UP TACK BELOW */
	[896+0x1E] = 104,	/* U+031E: COMBINING DOWN TACK BELOW */
	[896+0x1F] = 104,	/* U+031F: COMBINING PLUS SIGN BELOW */
	[896+0x20] = 104,	/* U+0320: COMBINING MINUS SIGN BELOW */
	[896+0x21] = 104,	/* U+0321: COMBINING PALATALIZED HOOK BELOW */
	[896+0x22] = 104,	/* U+0322: COMBINING RETROFLEX HOOK BELOW */
	[896+0x23] = 104,	/* U+0323: COMBINING DOT BELOW */
	[896+0x24] = 104,	/* U+0324: COMBINING DIAERESIS BELOW */
	[896+0x25] = 104,	/* U+0325: COMBINING RING BELOW */
	[896+0x26] = 104,	/* U+0326: COMBINING COMMA BELOW */
	[896+0x27] = 104,	/* U+0327: COMBINING CEDILLA */
	[896+0x28] = 104,	/* U+0328: COMBINING OGONEK */
	[896+0x29] = 104,	/* U+0329: COMBINING VERTICAL LINE BELOW */
	[896+0x2A] = 104,	/* U+032A: COMBINING BRIDGE BELOW */
	[896+0x2B] = 104,	/* U+032B: COMBINING INVERTED DOUBLE ARCH BELOW */
	[896+0x2C] = 104,	/* U+032C: COMBINING CARON BELOW */
	[896+0x2D] = 104,	/* U+032D: COMBINING CIRCUMFLEX ACCENT BELOW */
	[896+0x2E] = 104,	/* U+032E: COMBINING BREVE BELOW */
	[896+0x2F] = 104,	/* U+032F: COMBINING INVERTED BREVE BELOW */
	[896+0x30] = 104,	/* U+0330: COMBINING TILDE BELOW */
	[896+0x31] = 104,	/* U+0331: COMBINING MACRON BELOW */
	[896+0x32] = 104,	/* U+0332: COMBINING LOW LINE */
	[896+0x33] = 104,	/* U+0333: COMBINING DOUBLE LOW LINE */
	[896+0x34] = 104,	/* U+0334: COMBINING TILDE OVERLAY */
	[896+0x35] = 104,	/* U+0335: COMBINING SHORT STROKE OVERLAY */
	[896+0x36] = 104,	/* U+0336: COMBINING LONG STROKE OVERLAY */
	[896+0x37] = 104,	/* U+0337: COMBINING SHORT SOLIDUS OVERLAY */
	[896+0x38] = 104,	/* U+0338: COMBINING LONG SOLIDUS OVERLAY */
	[896+0x39] = 104,	/* U+0339: COMBINING RIGHT HALF RING BELOW */
	[896+0x3A] = 104,	/* U+033A: COMBINING INVERTED BRIDGE BELOW */
	[896+0x3B] = 104,	/* U+033B: COMBINING SQUARE BELOW */
	[896+0x3C] = 104,	/* U+033C: COMBINING SEAGULL BELOW */
	[896+0x3D] = 104,	/* U+033D: COMBINING X ABOVE */
	[896+0x3E] = 104,	/* U+033E: COMBINING VERTICAL TILDE */
	[896+0x3F] = 104,	/* U+033F: COMBINING DOUBLE OVERLINE */
	[0xCD] = 960 - 0x80,	/* 315 ... */
	[960+0x00] = 104,	/* U+0340: COMBINING GRAVE TONE MARK */
	[960+0x01] = 104,	/* U+0341: COMBINING ACUTE TONE MARK */
	[960+0x02] = 104,	/* U+0342: COMBINING GREEK PERISPOMENI */
	[960+0x03] = 104,	/* U+0343: COMBINING GREEK KORONIS */
	[960+0x04] = 104,	/* U+0344: COMBINING GREEK DIALYTIKA TONOS */
	[960+0x05] = 104,	/* U+0345: COMBINING GREEK YPOGEGRAMMENI */
	[960+0x06] = 104,	/* U+0346: COMBINING BRIDGE ABOVE */
	[960+0x07] = 104,	/* U+0347: COMBINING EQUALS SIGN BELOW */
	[960+0x08] = 104,	/* U+0348: COMBINING DOUBLE VERTICAL LINE BELOW */
	[960+0x09] = 104,	/* U+0349: COMBINING LEFT ANGLE BELOW */
	[960+0x0A] = 104,	/* U+034A: COMBINING NOT TILDE ABOVE */
	[960+0x0B] = 104,	/* U+034B: COMBINING HOMOTHETIC ABOVE */
	[960+0x0C] = 104,	/* U+034C: COMBINING ALMOST EQUAL TO ABOVE */
	[960+0x0D] = 104,	/* U+034D: COMBINING LEFT RIGHT ARROW BELOW */
	[960+0x0E] = 104,	/* U+034E: COMBINING UPWARDS ARROW BELOW */
	[960+0x0F] = 104,	/* U+034F: COMBINING GRAPHEME JOINER */
	[960+0x10] = 104,	/* U+0350: COMBINING RIGHT ARROWHEAD ABOVE */
	[960+0x11] = 104,	/* U+0351: COMBINING LEFT HALF RING ABOVE */
	[960+0x12] = 104,	/* U+0352: COMBINING FERMATA */
	[960+0x13] = 104,	/* U+0353: COMBINING X BELOW */
	[960+0x14] = 104,	/* U+0354: COMBINING LEFT ARROWHEAD BELOW */
	[960+0x15] = 104,	/* U+0355: COMBINING RIGHT ARROWHEAD BELOW */
	[960+0x16] = 104,	/* U+0356: COMBINING RIGHT ARROWHEAD AND UP ARROWHEAD BELOW */
	[960+0x17] = 104,	/* U+0357: COMBINING RIGHT HALF RING ABOVE */
	[960+0x18] = 104,	/* U+0358: COMBINING DOT ABOVE RIGHT */
	[960+0x19] = 104,	/* U+0359: COMBINING ASTERISK BELOW */
	[960+0x1A] = 104,	/* U+035A: COMBINING DOUBLE RING BELOW */
	[960+0x1B] = 104,	/* U+035B: COMBINING ZIGZAG ABOVE */
	[960+0x1C] = 104,	/* U+035C: COMBINING DOUBLE BREVE BELOW */
	[960+0x1D] = 104,	/* U+035D: COMBINING DOUBLE BREVE */
	[960+0x1E] = 104,	/* U+035E: COMBINING DOUBLE MACRON */
	[960+0x1F] = 104,	/* U+035F: COMBINING DOUBLE MACRON BELOW */
	[960+0x20] = 104,	/* U+0360: COMBINING DOUBLE TILDE */
	[960+0x21] = 104,	/* U+0361: COMBINING DOUBLE INVERTED BREVE */
	[960+0x22] = 104,	/* U+0362: COMBINING DOUBLE RIGHTWARDS ARROW BELOW */
	[960+0x23] = 104,	/* U+0363: COMBINING LATIN SMALL LETTER A */
	[960+0x24] = 104,	/* U+0364: COMBINING LATIN SMALL LETTER E */
	[960+0x25] = 104,	/* U+0365: COMBINING LATIN SMALL LETTER I */
	[960+0x26] = 104,	/* U+0366: COMBINING LATIN SMALL LETTER O */
	[960+0x27] = 104,	/* U+0367: COMBINING LATIN SMALL LETTER U */
	[960+0x28] = 104,	/* U+0368: COMBINING LATIN SMALL LETTER C */
	[960+0x29] = 104,	/* U+0369: COMBINING LATIN SMALL LETTER D */
	[960+0x2A] = 104,	/* U+036A: COMBINING LATIN SMALL LETTER H */
	[960+0x2B] = 104,	/* U+036B: COMBINING LATIN SMALL LETTER M */
	[960+0x2C] = 104,	/* U+036C: COMBINING LATIN SMALL LETTER R */
	[960+0x2D] = 104,	/* U+036D: COMBINING LATIN SMALL LETTER T */
	[960+0x2E] = 104,	/* U+036E: COMBINING LATIN SMALL LETTER V */
	[960+0x2F] = 104,	/* U+036F: COMBINING LATIN SMALL LETTER X */
	[960+0x3E] = 105,	/* U+037E: GREEK QUESTION MARK */
	[0xCE] = 1024 - 0x80,	/* 316 ... */
	[1024+0x3C] = 16,	/* U+03BC: GREEK SMALL LETTER MU */
	[0xD6] = 1088 - 0x80,	/* 326 ... */
	[1088+0x0F] = 106,	/* U+058F: ARMENIAN DRAM SIGN */
	[0xE1] = 1152 - 0x80,	/* 341 ... */
	[1152+0x2A] = 1216 - 0x80,	/* 341 252 ... */
	[1216+0x30] = 104,	/* U+1AB0: COMBINING DOUBLED CIRCUMFLEX ACCENT */
	[1216+0x31] = 104,	/* U+1AB1: COMBINING DIAERESIS-RING */
	[1216+0x32] = 104,	/* U+1AB2: COMBINING INFINITY */
	[1216+0x33] = 104,	/* U+1AB3: COMBINING DOWNWARDS ARROW */
	[1216+0x34] = 104,	/* U+1AB4: COMBINING TRIPLE DOT */
	[1216+0x35] = 104,	/* U+1AB5: COMBINING X-X BELOW */
	[1216+0x36] = 104,	/* U+1AB6: COMBINING WIGGLY LINE BELOW */
	[1216+0x37] = 104,	/* U+1AB7: COMBINING OPEN MARK BELOW */
	[1216+0x38] = 104,	/* U+1AB8: COMBINING DOUBLE OPEN MARK BELOW */
	[1216+0x39] = 104,	/* U+1AB9: COMBINING LIGHT CENTRALIZATION STROKE BELOW */
	[1216+0x3A] = 104,	/* U+1ABA: COMBINING STRONG CENTRALIZATION STROKE BELOW */
	[1216+0x3B] = 104,	/* U+1ABB: COMBINING PARENTHESES ABOVE */
	[1216+0x3C] = 104,	/* U+1ABC: COMBINING DOUBLE PARENTHESES ABOVE */
	[1216+0x3D] = 104,	/* U+1ABD: COMBINING PARENTHESES BELOW */
	[1216+0x3E] = 104,	/* U+1ABE: COMBINING PARENTHESES OVERLAY */
	[1216+0x3F] = 104,	/* U+1ABF: COMBINING LATIN SMALL LETTER W BELOW */
	[1152+0x2B] = 1280 - 0x80,	/* 341 253 ... */
	[1280+0x00] = 104,	/* U+1AC0: COMBINING LATIN SMALL LETTER TURNED W BELOW */
	[1280+0x01] = 104,	/* U+1AC1: COMBINING LEFT PARENTHESIS ABOVE LEFT */
	[1280+0x02] = 104,	/* U+1AC2: COMBINING RIGHT PARENTHESIS ABOVE RIGHT */
	[1280+0x03] = 104,	/* U+1AC3: COMBINING LEFT PARENTHESIS BELOW LEFT */
	[1280+0x04] = 104,	/* U+1AC4: COMBINING RIGHT PARENTHESIS BELOW RIGHT */
	[1280+0x05] = 104,	/* U+1AC5: COMBINING SQUARE BRACKETS ABOVE */
	[1280+0x06] = 104,	/* U+1AC6: COMBINING NUMBER SIGN ABOVE */
	[1280+0x07] = 104,	/* U+1AC7: COMBINING INVERTED DOUBLE ARCH ABOVE */
	[1280+0x08] = 104,	/* U+1AC8: COMBINING PLUS SIGN ABOVE */
	[1280+0x09] = 104,	/* U+1AC9: COMBINING DOUBLE PLUS SIGN ABOVE */
	[1280+0x0A] = 104,	/* U+1ACA: COMBINING DOUBLE PLUS SIGN BELOW */
	[1280+0x0B] = 104,	/* U+1ACB: COMBINING TRIPLE ACUTE ACCENT */
	[1280+0x0C] = 104,	/* U+1ACC: COMBINING LATIN SMALL LETTER INSULAR G */
	[1280+0x0D] = 104,	/* U+1ACD: COMBINING LATIN SMALL LETTER INSULAR R */
	[1280+0x0E] = 104,	/* U+1ACE: COMBINING LATIN SMALL LETTER INSULAR T */
	[1152+0x34] = 1344 - 0x80,	/* 341 264 ... */
	[1344+0x00] = 25,	/* U+1D00: LATIN LETTER SMALL CAPITAL A */
	[1344+0x01] = 26,	/* U+1D01: LATIN LETTER SMALL CAPITAL AE */
	[1344+0x03] = 73,	/* U+1D03: LATIN LETTER SMALL CAPITAL BARRED B */
	[1344+0x04] = 27,	/* U+1D04: LATIN LETTER SMALL CAPITAL C */
	[1344+0x05] = 30,	/* U+1D05: LATIN LETTER SMALL CAPITAL D */
	[1344+0x06] = 30,	/* U+1D06: LATIN LETTER SMALL CAPITAL ETH */
	[1344+0x07] = 28,	/* U+1D07: LATIN LETTER SMALL CAPITAL E */
	[1344+0x0A] = 52,	/* U+1D0A: LATIN LETTER SMALL CAPITAL J */
	[1344+0x0B] = 54,	/* U+1D0B: LATIN LETTER SMALL CAPITAL K */
	[1344+0x0C] = 57,	/* U+1D0C: LATIN LETTER SMALL CAPITAL L WITH STROKE */
	[1344+0x0D] = 107,	/* U+1D0D: LATIN LETTER SMALL CAPITAL M */
	[1344+0x0F] = 32,	/* U+1D0F: LATIN LETTER SMALL CAPITAL O */
	[1344+0x18] = 79,	/* U+1D18: LATIN LETTER SMALL CAPITAL P */
	[1344+0x1B] = 66,	/* U+1D1B: LATIN LETTER SMALL CAPITAL T */
	[1344+0x1C] = 34,	/* U+1D1C: LATIN LETTER SMALL CAPITAL U */
	[1344+0x20] = 81,	/* U+1D20: LATIN LETTER SMALL CAPITAL V */
	[1344+0x21] = 68,	/* U+1D21: LATIN LETTER SMALL CAPITAL W */
	[1344+0x22] = 70,	/* U+1D22: LATIN LETTER SMALL CAPITAL Z */
	[1344+0x2C] = 25,	/* U+1D2C: MODIFIER LETTER CAPITAL A */
	[1344+0x2E] = 73,	/* U+1D2E: MODIFIER LETTER CAPITAL B */
	[1344+0x30] = 30,	/* U+1D30: MODIFIER LETTER CAPITAL D */
	[1344+0x31] = 28,	/* U+1D31: MODIFIER LETTER CAPITAL E */
	[1344+0x33] = 46,	/* U+1D33: MODIFIER LETTER CAPITAL G */
	[1344+0x34] = 48,	/* U+1D34: MODIFIER LETTER CAPITAL H */
	[1344+0x35] = 29,	/* U+1D35: MODIFIER LETTER CAPITAL I */
	[1344+0x36] = 52,	/* U+1D36: MODIFIER LETTER CAPITAL J */
	[1344+0x37] = 54,	/* U+1D37: MODIFIER LETTER CAPITAL K */
	[1344+0x38] = 57,	/* U+1D38: MODIFIER LETTER CAPITAL L */
	[1344+0x39] = 107,	/* U+1D39: MODIFIER LETTER CAPITAL M */
	[1344+0x3A] = 31,	/* U+1D3A: MODIFIER LETTER CAPITAL N */
	[1344+0x3C] = 32,	/* U+1D3C: MODIFIER LETTER CAPITAL O */
	[1344+0x3E] = 79,	/* U+1D3E: MODIFIER LETTER CAPITAL P */
	[1344+0x3F] = 62,	/* U+1D3F: MODIFIER LETTER CAPITAL R */
	[1152+0x35] = 1408 - 0x80,	/* 341 265 ... */
	[1408+0x00] = 66,	/* U+1D40: MODIFIER LETTER CAPITAL T */
	[1408+0x01] = 34,	/* U+1D41: MODIFIER LETTER CAPITAL U */
	[1408+0x02] = 68,	/* U+1D42: MODIFIER LETTER CAPITAL W */
	[1408+0x03] = 8,	/* U+1D43: MODIFIER LETTER SMALL A */
	[1408+0x07] = 72,	/* U+1D47: MODIFIER LETTER SMALL B */
	[1408+0x08] = 41,	/* U+1D48: MODIFIER LETTER SMALL D */
	[1408+0x09] = 39,	/* U+1D49: MODIFIER LETTER SMALL E */
	[1408+0x0D] = 47,	/* U+1D4D: MODIFIER LETTER SMALL G */
	[1408+0x0F] = 55,	/* U+1D4F: MODIFIER LETTER SMALL K */
	[1408+0x10] = 93,	/* U+1D50: MODIFIER LETTER SMALL M */
	[1408+0x12] = 20,	/* U+1D52: MODIFIER LETTER SMALL O */
	[1408+0x16] = 80,	/* U+1D56: MODIFIER LETTER SMALL P */
	[1408+0x17] = 67,	/* U+1D57: MODIFIER LETTER SMALL T */
	[1408+0x18] = 16,	/* U+1D58: MODIFIER LETTER SMALL U */
	[1408+0x1B] = 94,	/* U+1D5B: MODIFIER LETTER SMALL V */
	[1408+0x22] = 40,	/* U+1D62: LATIN SUBSCRIPT SMALL LETTER I */
	[1408+0x23] = 63,	/* U+1D63: LATIN SUBSCRIPT SMALL LETTER R */
	[1408+0x24] = 16,	/* U+1D64: LATIN SUBSCRIPT SMALL LETTER U */
	[1408+0x25] = 94,	/* U+1D65: LATIN SUBSCRIPT SMALL LETTER V */
	[1408+0x2B] = 108,	/* U+1D6B: LATIN SMALL LETTER UE */
	[1408+0x2C] = 72,	/* U+1D6C: LATIN SMALL LETTER B WITH MIDDLE TILDE */
	[1408+0x2D] = 41,	/* U+1D6D: LATIN SMALL LETTER D WITH MIDDLE TILDE */
	[1408+0x2E] = 75,	/* U+1D6E: LATIN SMALL LETTER F WITH MIDDLE TILDE */
	[1408+0x2F] = 93,	/* U+1D6F: LATIN SMALL LETTER M WITH MIDDLE TILDE */
	[1408+0x30] = 42,	/* U+1D70: LATIN SMALL LETTER N WITH MIDDLE TILDE */
	[1408+0x31] = 80,	/* U+1D71: LATIN SMALL LETTER P WITH MIDDLE TILDE */
	[1408+0x32] = 63,	/* U+1D72: LATIN SMALL LETTER R WITH MIDDLE TILDE */
	[1408+0x33] = 63,	/* U+1D73: LATIN SMALL LETTER R WITH FISHHOOK AND MIDDLE TILDE */
	[1408+0x34] = 65,	/* U+1D74: LATIN SMALL LETTER S WITH MIDDLE TILDE */
	[1408+0x35] = 67,	/* U+1D75: LATIN SMALL LETTER T WITH MIDDLE TILDE */
	[1408+0x36] = 71,	/* U+1D76: LATIN SMALL LETTER Z WITH MIDDLE TILDE */
	[1408+0x3A] = 45,	/* U+1D7A: LATIN SMALL LETTER TH WITH STRIKETHROUGH */
	[1408+0x3B] = 29,	/* U+1D7B: LATIN SMALL CAPITAL LETTER I WITH STROKE */
	[1408+0x3D] = 80,	/* U+1D7D: LATIN SMALL LETTER P WITH STROKE */
	[1408+0x3E] = 34,	/* U+1D7E: LATIN SMALL CAPITAL LETTER U WITH STROKE */
	[1152+0x36] = 1472 - 0x80,	/* 341 266 ... */
	[1472+0x00] = 72,	/* U+1D80: LATIN SMALL LETTER B WITH PALATAL HOOK */
	[1472+0x01] = 41,	/* U+1D81: LATIN SMALL LETTER D WITH PALATAL HOOK */
	[1472+0x02] = 75,	/* U+1D82: LATIN SMALL LETTER F WITH PALATAL HOOK */
	[1472+0x03] = 47,	/* U+1D83: LATIN SMALL LETTER G WITH PALATAL HOOK */
	[1472+0x04] = 55,	/* U+1D84: LATIN SMALL LETTER K WITH PALATAL HOOK */
	[1472+0x05] = 58,	/* U+1D85: LATIN SMALL LETTER L WITH PALATAL HOOK */
	[1472+0x06] = 93,	/* U+1D86: LATIN SMALL LETTER M WITH PALATAL HOOK */
	[1472+0x07] = 42,	/* U+1D87: LATIN SMALL LETTER N WITH PALATAL HOOK */
	[1472+0x08] = 80,	/* U+1D88: LATIN SMALL LETTER P WITH PALATAL HOOK */
	[1472+0x09] = 63,	/* U+1D89: LATIN SMALL LETTER R WITH PALATAL HOOK */
	[1472+0x0A] = 65,	/* U+1D8A: LATIN SMALL LETTER S WITH PALATAL HOOK */
	[1472+0x0C] = 94,	/* U+1D8C: LATIN SMALL LETTER V WITH PALATAL HOOK */
	[1472+0x0D] = 33,	/* U+1D8D: LATIN SMALL LETTER X WITH PALATAL HOOK */
	[1472+0x0E] = 71,	/* U+1D8E: LATIN SMALL LETTER Z WITH PALATAL HOOK */
	[1472+0x0F] = 8,	/* U+1D8F: LATIN SMALL LETTER A WITH RETROFLEX HOOK */
	[1472+0x11] = 41,	/* U+1D91: LATIN SMALL LETTER D WITH HOOK AND TAIL */
	[1472+0x12] = 39,	/* U+1D92: LATIN SMALL LETTER E WITH RETROFLEX HOOK */
	[1472+0x13] = 39,	/* U+1D93: LATIN SMALL LETTER OPEN E WITH RETROFLEX HOOK */
	[1472+0x16] = 40,	/* U+1D96: LATIN SMALL LETTER I WITH RETROFLEX HOOK */
	[1472+0x19] = 16,	/* U+1D99: LATIN SMALL LETTER U WITH RETROFLEX HOOK */
	[1472+0x1C] = 3,	/* U+1D9C: MODIFIER LETTER SMALL C */
	[1472+0x20] = 75,	/* U+1DA0: MODIFIER LETTER SMALL F */
	[1472+0x3B] = 71,	/* U+1DBB: MODIFIER LETTER SMALL Z */
	[1152+0x37] = 1536 - 0x80,	/* 341 267 ... */
	[1536+0x00] = 104,	/* U+1DC0: COMBINING DOTTED GRAVE ACCENT */
	[1536+0x01] = 104,	/* U+1DC1: COMBINING DOTTED ACUTE ACCENT */
	[1536+0x02] = 104,	/* U+1DC2: COMBINING SNAKE BELOW */
	[1536+0x03] = 104,	/* U+1DC3: COMBINING SUSPENSION MARK */
	[1536+0x04] = 104,	/* U+1DC4: COMBINING MACRON-ACUTE */
	[1536+0x05] = 104,	/* U+1DC5: COMBINING GRAVE-MACRON */
	[1536+0x06] = 104,	/* U+1DC6: COMBINING MACRON-GRAVE */
	[1536+0x07] = 104,	/* U+1DC7: COMBINING ACUTE-MACRON */
	[1536+0x08] = 104,	/* U+1DC8: COMBINING GRAVE-ACUTE-GRAVE */
	[1536+0x09] = 104,	/* U+1DC9: COMBINING ACUTE-GRAVE-ACUTE */
	[1536+0x0A] = 104,	/* U+1DCA: COMBINING LATIN SMALL LETTER R BELOW */
	[1536+0x0B] = 104,	/* U+1DCB: COMBINING BREVE-MACRON */
	[1536+0x0C] = 104,	/* U+1DCC: COMBINING MACRON-BREVE */
	[1536+0x0D] = 104,	/* U+1DCD: COMBINING DOUBLE CIRCUMFLEX ABOVE */
	[1536+0x0E] = 104,	/* U+1DCE: COMBINING OGONEK ABOVE */
	[1536+0x0F] = 104,	/* U+1DCF: COMBINING ZIGZAG BELOW */
	[1536+0x10] = 104,	/* U+1DD0: COMBINING IS BELOW */
	[1536+0x11] = 104,	/* U+1DD1: COMBINING UR ABOVE */
	[1536+0x12] = 104,	/* U+1DD2: COMBINING US ABOVE */
	[1536+0x13] = 104,	/* U+1DD3: COMBINING LATIN SMALL LETTER FLATTENED OPEN A ABOVE */
	[1536+0x14] = 104,	/* U+1DD4: COMBINING LATIN SMALL LETTER AE */
	[1536+0x15] = 104,	/* U+1DD5: COMBINING LATIN SMALL LETTER AO */
	[1536+0x16] = 104,	/* U+1DD6: COMBINING LATIN SMALL LETTER AV */
	[1536+0x17] = 104,	/* U+1DD7: COMBINING LATIN SMALL LETTER C CEDILLA */
	[1536+0x18] = 104,	/* U+1DD8: COMBINING LATIN SMALL LETTER INSULAR D */
	[1536+0x19] = 104,	/* U+1DD9: COMBINING LATIN SMALL LETTER ETH */
	[1536+0x1A] = 104,	/* U+1DDA: COMBINING LATIN SMALL LETTER G */
	[1536+0x1B] = 104,	/* U+1DDB: COMBINING LATIN LETTER SMALL CAPITAL G */
	[1536+0x1C] = 104,	/* U+1DDC: COMBINING LATIN SMALL LETTER K */
	[1536+0x1D] = 104,	/* U+1DDD: COMBINING LATIN SMALL LETTER L */
	[1536+0x1E] = 104,	/* U+1DDE: COMBINING LATIN LETTER SMALL CAPITAL L */
	[1536+0x1F] = 104,	/* U+1DDF: COMBINING LATIN LETTER SMALL CAPITAL M */
	[1536+0x20] = 104,	/* U+1DE0: COMBINING LATIN SMALL LETTER N */
	[1536+0x21] = 104,	/* U+1DE1: COMBINING LATIN LETTER SMALL CAPITAL N */
	[1536+0x22] = 104,	/* U+1DE2: COMBINING LATIN LETTER SMALL CAPITAL R */
	[1536+0x23] = 104,	/* U+1DE3: COMBINING LATIN SMALL LETTER R ROTUNDA */
	[1536+0x24] = 104,	/* U+1DE4: COMBINING LATIN SMALL LETTER S */
	[1536+0x25] = 104,	/* U+1DE5: COMBINING LATIN SMALL LETTER LONG S */
	[1536+0x26] = 104,	/* U+1DE6: COMBINING LATIN SMALL LETTER Z */
	[1536+0x27] = 104,	/* U+1DE7: COMBINING LATIN SMALL LETTER ALPHA */
	[1536+0x28] = 104,	/* U+1DE8: COMBINING LATIN SMALL LETTER B */
	[1536+0x29] = 104,	/* U+1DE9: COMBINING LATIN SMALL LETTER BETA */
	[1536+0x2A] = 104,	/* U+1DEA: COMBINING LATIN SMALL LETTER SCHWA */
	[1536+0x2B] = 104,	/* U+1DEB: COMBINING LATIN SMALL LETTER F */
	[1536+0x2C] = 104,	/* U+1DEC: COMBINING LATIN SMALL LETTER L WITH DOUBLE MIDDLE TILDE */
	[1536+0x2D] = 104,	/* U+1DED: COMBINING LATIN SMALL LETTER O WITH LIGHT CENTRALIZATION STROKE */
	[1536+0x2E] = 104,	/* U+1DEE: COMBINING LATIN SMALL LETTER P */
	[1536+0x2F] = 104,	/* U+1DEF: COMBINING LATIN SMALL LETTER ESH */
	[1536+0x30] = 104,	/* U+1DF0: COMBINING LATIN SMALL LETTER U WITH LIGHT CENTRALIZATION STROKE */
	[1536+0x31] = 104,	/* U+1DF1: COMBINING LATIN SMALL LETTER W */
	[1536+0x32] = 104,	/* U+1DF2: COMBINING LATIN SMALL LETTER A WITH DIAERESIS */
	[1536+0x33] = 104,	/* U+1DF3: COMBINING LATIN SMALL LETTER O WITH DIAERESIS */
	[1536+0x34] = 104,	/* U+1DF4: COMBINING LATIN SMALL LETTER U WITH DIAERESIS */
	[1536+0x35] = 104,	/* U+1DF5: COMBINING UP TACK ABOVE */
	[1536+0x36] = 104,	/* U+1DF6: COMBINING KAVYKA ABOVE RIGHT */
	[1536+0x37] = 104,	/* U+1DF7: COMBINING KAVYKA ABOVE LEFT */
	[1536+0x38] = 104,	/* U+1DF8: COMBINING DOT ABOVE LEFT */
	[1536+0x39] = 104,	/* U+1DF9: COMBINING WIDE INVERTED BRIDGE BELOW */
	[1536+0x3A] = 104,	/* U+1DFA: COMBINING DOT BELOW LEFT */
	[1536+0x3B] = 104,	/* U+1DFB: COMBINING DELETION MARK */
	[1536+0x3C] = 104,	/* U+1DFC: COMBINING DOUBLE INVERTED BREVE BELOW */
	[1536+0x3D] = 104,	/* U+1DFD: COMBINING ALMOST EQUAL TO BELOW */
	[1536+0x3E] = 104,	/* U+1DFE: COMBINING LEFT ARROWHEAD ABOVE */
	[1536+0x3F] = 104,	/* U+1DFF: COMBINING RIGHT ARROWHEAD AND DOWN ARROWHEAD BELOW */
	[1152+0x38] = 1600 - 0x80,	/* 341 270 ... */
	[1600+0x00] = 25,	/* U+1E00: LATIN CAPITAL LETTER A WITH RING BELOW */
	[1600+0x01] = 8,	/* U+1E01: LATIN SMALL LETTER A WITH RING BELOW */
	[1600+0x02] = 73,	/* U+1E02: LATIN CAPITAL LETTER B WITH DOT ABOVE */
	[1600+0x03] = 72,	/* U+1E03: LATIN SMALL LETTER B WITH DOT ABOVE */
	[1600+0x04] = 73,	/* U+1E04: LATIN CAPITAL LETTER B WITH DOT BELOW */
	[1600+0x05] = 72,	/* U+1E05: LATIN SMALL LETTER B WITH DOT BELOW */
	[1600+0x06] = 73,	/* U+1E06: LATIN CAPITAL LETTER B WITH LINE BELOW */
	[1600+0x07] = 72,	/* U+1E07: LATIN SMALL LETTER B WITH LINE BELOW */
	[1600+0x08] = 27,	/* U+1E08: LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE */
	[1600+0x09] = 3,	/* U+1E09: LATIN SMALL LETTER C WITH CEDILLA AND ACUTE */
	[1600+0x0A] = 30,	/* U+1E0A: LATIN CAPITAL LETTER D WITH DOT ABOVE */
	[1600+0x0B] = 41,	/* U+1E0B: LATIN SMALL LETTER D WITH DOT ABOVE */
	[1600+0x0C] = 30,	/* U+1E0C: LATIN CAPITAL LETTER D WITH DOT BELOW */
	[1600+0x0D] = 41,	/* U+1E0D: LATIN SMALL LETTER D WITH DOT BELOW */
	[1600+0x0E] = 30,	/* U+1E0E: LATIN CAPITAL LETTER D WITH LINE BELOW */
	[1600+0x0F] = 41,	/* U+1E0F: LATIN SMALL LETTER D WITH LINE BELOW */
	[1600+0x10] = 30,	/* U+1E10: LATIN CAPITAL LETTER D WITH CEDILLA */
	[1600+0x11] = 41,	/* U+1E11: LATIN SMALL LETTER D WITH CEDILLA */
	[1600+0x12] = 30,	/* U+1E12: LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW */
	[1600+0x13] = 41,	/* U+1E13: LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW */
	[1600+0x14] = 28,	/* U+1E14: LATIN CAPITAL LETTER E WITH MACRON AND GRAVE */
	[1600+0x15] = 39,	/* U+1E15: LATIN SMALL LETTER E WITH MACRON AND GRAVE */
	[1600+0x16] = 28,	/* U+1E16: LATIN CAPITAL LETTER E WITH MACRON AND ACUTE */
	[1600+0x17] = 39,	/* U+1E17: LATIN SMALL LETTER E WITH MACRON AND ACUTE */
	[1600+0x18] = 28,	/* U+1E18: LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW */
	[1600+0x19] = 39,	/* U+1E19: LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW */
	[1600+0x1A] = 28,	/* U+1E1A: LATIN CAPITAL LETTER E WITH TILDE BELOW */
	[1600+0x1B] = 39,	/* U+1E1B: LATIN SMALL LETTER E WITH TILDE BELOW */
	[1600+0x1C] = 28,	/* U+1E1C: LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE */
	[1600+0x1D] = 39,	/* U+1E1D: LATIN SMALL LETTER E WITH CEDILLA AND BREVE */
	[1600+0x1E] = 74,	/* U+1E1E: LATIN CAPITAL LETTER F WITH DOT ABOVE */
	[1600+0x1F] = 75,	/* U+1E1F: LATIN SMALL LETTER F WITH DOT ABOVE */
	[1600+0x20] = 46,	/* U+1E20: LATIN CAPITAL LETTER G WITH MACRON */
	[1600+0x21] = 47,	/* U+1E21: LATIN SMALL LETTER G WITH MACRON */
	[1600+0x22] = 48,	/* U+1E22: LATIN CAPITAL LETTER H WITH DOT ABOVE */
	[1600+0x23] = 49,	/* U+1E23: LATIN SMALL LETTER H WITH DOT ABOVE */
	[1600+0x24] = 48,	/* U+1E24: LATIN CAPITAL LETTER H WITH DOT BELOW */
	[1600+0x25] = 49,	/* U+1E25: LATIN SMALL LETTER H WITH DOT BELOW */
	[1600+0x26] = 48,	/* U+1E26: LATIN CAPITAL LETTER H WITH DIAERESIS */
	[1600+0x27] = 49,	/* U+1E27: LATIN SMALL LETTER H WITH DIAERESIS */
	[1600+0x28] = 48,	/* U+1E28: LATIN CAPITAL LETTER H WITH CEDILLA */
	[1600+0x29] = 49,	/* U+1E29: LATIN SMALL LETTER H WITH CEDILLA */
	[1600+0x2A] = 48,	/* U+1E2A: LATIN CAPITAL LETTER H WITH BREVE BELOW */
	[1600+0x2B] = 49,	/* U+1E2B: LATIN SMALL LETTER H WITH BREVE BELOW */
	[1600+0x2C] = 29,	/* U+1E2C: LATIN CAPITAL LETTER I WITH TILDE BELOW */
	[1600+0x2D] = 40,	/* U+1E2D: LATIN SMALL LETTER I WITH TILDE BELOW */
	[1600+0x2E] = 29,	/* U+1E2E: LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE */
	[1600+0x2F] = 40,	/* U+1E2F: LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE */
	[1600+0x30] = 54,	/* U+1E30: LATIN CAPITAL LETTER K WITH ACUTE */
	[1600+0x31] = 55,	/* U+1E31: LATIN SMALL LETTER K WITH ACUTE */
	[1600+0x32] = 54,	/* U+1E32: LATIN CAPITAL LETTER K WITH DOT BELOW */
	[1600+0x33] = 55,	/* U+1E33: LATIN SMALL LETTER K WITH DOT BELOW */
	[1600+0x34] = 54,	/* U+1E34: LATIN CAPITAL LETTER K WITH LINE BELOW */
	[1600+0x35] = 55,	/* U+1E35: LATIN SMALL LETTER K WITH LINE BELOW */
	[1600+0x36] = 57,	/* U+1E36: LATIN CAPITAL LETTER L WITH DOT BELOW */
	[1600+0x37] = 58,	/* U+1E37: LATIN SMALL LETTER L WITH DOT BELOW */
	[1600+0x38] = 57,	/* U+1E38: LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON */
	[1600+0x39] = 58,	/* U+1E39: LATIN SMALL LETTER L WITH DOT BELOW AND MACRON */
	[1600+0x3A] = 57,	/* U+1E3A: LATIN CAPITAL LETTER L WITH LINE BELOW */
	[1600+0x3B] = 58,	/* U+1E3B: LATIN SMALL LETTER L WITH LINE BELOW */
	[1600+0x3C] = 57,	/* U+1E3C: LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW */
	[1600+0x3D] = 58,	/* U+1E3D: LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW */
	[1600+0x3E] = 107,	/* U+1E3E: LATIN CAPITAL LETTER M WITH ACUTE */
	[1600+0x3F] = 93,	/* U+1E3F: LATIN SMALL LETTER M WITH ACUTE */
	[1152+0x39] = 1664 - 0x80,	/* 341 271 ... */
	[1664+0x00] = 107,	/* U+1E40: LATIN CAPITAL LETTER M WITH DOT ABOVE */
	[1664+0x01] = 93,	/* U+1E41: LATIN SMALL LETTER M WITH DOT ABOVE */
	[1664+0x02] = 107,	/* U+1E42: LATIN CAPITAL LETTER M WITH DOT BELOW */
	[1664+0x03] = 93,	/* U+1E43: LATIN SMALL LETTER M WITH DOT BELOW */
	[1664+0x04] = 31,	/* U+1E44: LATIN CAPITAL LETTER N WITH DOT ABOVE */
	[1664+0x05] = 42,	/* U+1E45: LATIN SMALL LETTER N WITH DOT ABOVE */
	[1664+0x06] = 31,	/* U+1E46: LATIN CAPITAL LETTER N WITH DOT BELOW */
	[1664+0x07] = 42,	/* U+1E47: LATIN SMALL LETTER N WITH DOT BELOW */
	[1664+0x08] = 31,	/* U+1E48: LATIN CAPITAL LETTER N WITH LINE BELOW */
	[1664+0x09] = 42,	/* U+1E49: LATIN SMALL LETTER N WITH LINE BELOW */
	[1664+0x0A] = 31,	/* U+1E4A: LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW */
	[1664+0x0B] = 42,	/* U+1E4B: LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW */
	[1664+0x0C] = 32,	/* U+1E4C: LATIN CAPITAL LETTER O WITH TILDE AND ACUTE */
	[1664+0x0D] = 20,	/* U+1E4D: LATIN SMALL LETTER O WITH TILDE AND ACUTE */
	[1664+0x0E] = 32,	/* U+1E4E: LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS */
	[1664+0x0F] = 20,	/* U+1E4F: LATIN SMALL LETTER O WITH TILDE AND DIAERESIS */
	[1664+0x10] = 32,	/* U+1E50: LATIN CAPITAL LETTER O WITH MACRON AND GRAVE */
	[1664+0x11] = 20,	/* U+1E51: LATIN SMALL LETTER O WITH MACRON AND GRAVE */
	[1664+0x12] = 32,	/* U+1E52: LATIN CAPITAL LETTER O WITH MACRON AND ACUTE */
	[1664+0x13] = 20,	/* U+1E53: LATIN SMALL LETTER O WITH MACRON AND ACUTE */
	[1664+0x14] = 79,	/* U+1E54: LATIN CAPITAL LETTER P WITH ACUTE */
	[1664+0x15] = 80,	/* U+1E55: LATIN SMALL LETTER P WITH ACUTE */
	[1664+0x16] = 79,	/* U+1E56: LATIN CAPITAL LETTER P WITH DOT ABOVE */
	[1664+0x17] = 80,	/* U+1E57: LATIN SMALL LETTER P WITH DOT ABOVE */
	[1664+0x18] = 62,	/* U+1E58: LATIN CAPITAL LETTER R WITH DOT ABOVE */
	[1664+0x19] = 63,	/* U+1E59: LATIN SMALL LETTER R WITH DOT ABOVE */
	[1664+0x1A] = 62,	/* U+1E5A: LATIN CAPITAL LETTER R WITH DOT BELOW */
	[1664+0x1B] = 63,	/* U+1E5B: LATIN SMALL LETTER R WITH DOT BELOW */
	[1664+0x1C] = 62,	/* U+1E5C: LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON */
	[1664+0x1D] = 63,	/* U+1E5D: LATIN SMALL LETTER R WITH DOT BELOW AND MACRON */
	[1664+0x1E] = 62,	/* U+1E5E: LATIN CAPITAL LETTER R WITH LINE BELOW */
	[1664+0x1F] = 63,	/* U+1E5F: LATIN SMALL LETTER R WITH LINE BELOW */
	[1664+0x20] = 64,	/* U+1E60: LATIN CAPITAL LETTER S WITH DOT ABOVE */
	[1664+0x21] = 65,	/* U+1E61: LATIN SMALL LETTER S WITH DOT ABOVE */
	[1664+0x22] = 64,	/* U+1E62: LATIN CAPITAL LETTER S WITH DOT BELOW */
	[1664+0x23] = 65,	/* U+1E63: LATIN SMALL LETTER S WITH DOT BELOW */
	[1664+0x24] = 64,	/* U+1E64: LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE */
	[1664+0x25] = 65,	/* U+1E65: LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE */
	[1664+0x26] = 64,	/* U+1E66: LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE */
	[1664+0x27] = 65,	/* U+1E67: LATIN SMALL LETTER S WITH CARON AND DOT ABOVE */
	[1664+0x28] = 64,	/* U+1E68: LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE */
	[1664+0x29] = 65,	/* U+1E69: LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE */
	[1664+0x2A] = 66,	/* U+1E6A: LATIN CAPITAL LETTER T WITH DOT ABOVE */
	[1664+0x2B] = 67,	/* U+1E6B: LATIN SMALL LETTER T WITH DOT ABOVE */
	[1664+0x2C] = 66,	/* U+1E6C: LATIN CAPITAL LETTER T WITH DOT BELOW */
	[1664+0x2D] = 67,	/* U+1E6D: LATIN SMALL LETTER T WITH DOT BELOW */
	[1664+0x2E] = 66,	/* U+1E6E: LATIN CAPITAL LETTER T WITH LINE BELOW */
	[1664+0x2F] = 67,	/* U+1E6F: LATIN SMALL LETTER T WITH LINE BELOW */
	[1664+0x30] = 66,	/* U+1E70: LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW */
	[1664+0x31] = 67,	/* U+1E71: LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW */
	[1664+0x32] = 34,	/* U+1E72: LATIN CAPITAL LETTER U WITH DIAERESIS BELOW */
	[1664+0x33] = 16,	/* U+1E73: LATIN SMALL LETTER U WITH DIAERESIS BELOW */
	[1664+0x34] = 34,	/* U+1E74: LATIN CAPITAL LETTER U WITH TILDE BELOW */
	[1664+0x35] = 16,	/* U+1E75: LATIN SMALL LETTER U WITH TILDE BELOW */
	[1664+0x36] = 34,	/* U+1E76: LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW */
	[1664+0x37] = 16,	/* U+1E77: LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW */
	[1664+0x38] = 34,	/* U+1E78: LATIN CAPITAL LETTER U WITH TILDE AND ACUTE */
	[1664+0x39] = 16,	/* U+1E79: LATIN SMALL LETTER U WITH TILDE AND ACUTE */
	[1664+0x3A] = 34,	/* U+1E7A: LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS */
	[1664+0x3B] = 16,	/* U+1E7B: LATIN SMALL LETTER U WITH MACRON AND DIAERESIS */
	[1664+0x3C] = 81,	/* U+1E7C: LATIN CAPITAL LETTER V WITH TILDE */
	[1664+0x3D] = 94,	/* U+1E7D: LATIN SMALL LETTER V WITH TILDE */
	[1664+0x3E] = 81,	/* U+1E7E: LATIN CAPITAL LETTER V WITH DOT BELOW */
	[1664+0x3F] = 94,	/* U+1E7F: LATIN SMALL LETTER V WITH DOT BELOW */
	[1152+0x3A] = 1728 - 0x80,	/* 341 272 ... */
	[1728+0x00] = 68,	/* U+1E80: LATIN CAPITAL LETTER W WITH GRAVE */
	[1728+0x01] = 69,	/* U+1E81: LATIN SMALL LETTER W WITH GRAVE */
	[1728+0x02] = 68,	/* U+1E82: LATIN CAPITAL LETTER W WITH ACUTE */
	[1728+0x03] = 69,	/* U+1E83: LATIN SMALL LETTER W WITH ACUTE */
	[1728+0x04] = 68,	/* U+1E84: LATIN CAPITAL LETTER W WITH DIAERESIS */
	[1728+0x05] = 69,	/* U+1E85: LATIN SMALL LETTER W WITH DIAERESIS */
	[1728+0x06] = 68,	/* U+1E86: LATIN CAPITAL LETTER W WITH DOT ABOVE */
	[1728+0x07] = 69,	/* U+1E87: LATIN SMALL LETTER W WITH DOT ABOVE */
	[1728+0x08] = 68,	/* U+1E88: LATIN CAPITAL LETTER W WITH DOT BELOW */
	[1728+0x09] = 69,	/* U+1E89: LATIN SMALL LETTER W WITH DOT BELOW */
	[1728+0x0A] = 109,	/* U+1E8A: LATIN CAPITAL LETTER X WITH DOT ABOVE */
	[1728+0x0B] = 33,	/* U+1E8B: LATIN SMALL LETTER X WITH DOT ABOVE */
	[1728+0x0C] = 109,	/* U+1E8C: LATIN CAPITAL LETTER X WITH DIAERESIS */
	[1728+0x0D] = 33,	/* U+1E8D: LATIN SMALL LETTER X WITH DIAERESIS */
	[1728+0x0E] = 35,	/* U+1E8E: LATIN CAPITAL LETTER Y WITH DOT ABOVE */
	[1728+0x0F] = 44,	/* U+1E8F: LATIN SMALL LETTER Y WITH DOT ABOVE */
	[1728+0x10] = 70,	/* U+1E90: LATIN CAPITAL LETTER Z WITH CIRCUMFLEX */
	[1728+0x11] = 71,	/* U+1E91: LATIN SMALL LETTER Z WITH CIRCUMFLEX */
	[1728+0x12] = 70,	/* U+1E92: LATIN CAPITAL LETTER Z WITH DOT BELOW */
	[1728+0x13] = 71,	/* U+1E93: LATIN SMALL LETTER Z WITH DOT BELOW */
	[1728+0x14] = 70,	/* U+1E94: LATIN CAPITAL LETTER Z WITH LINE BELOW */
	[1728+0x15] = 71,	/* U+1E95: LATIN SMALL LETTER Z WITH LINE BELOW */
	[1728+0x16] = 49,	/* U+1E96: LATIN SMALL LETTER H WITH LINE BELOW */
	[1728+0x17] = 67,	/* U+1E97: LATIN SMALL LETTER T WITH DIAERESIS */
	[1728+0x18] = 69,	/* U+1E98: LATIN SMALL LETTER W WITH RING ABOVE */
	[1728+0x19] = 44,	/* U+1E99: LATIN SMALL LETTER Y WITH RING ABOVE */
	[1728+0x1A] = 8,	/* U+1E9A: LATIN SMALL LETTER A WITH RIGHT HALF RING */
	[1728+0x1C] = 65,	/* U+1E9C: LATIN SMALL LETTER LONG S WITH DIAGONAL STROKE */
	[1728+0x1D] = 65,	/* U+1E9D: LATIN SMALL LETTER LONG S WITH HIGH STROKE */
	[1728+0x1E] = 110,	/* U+1E9E: LATIN CAPITAL LETTER SHARP S */
	[1728+0x20] = 25,	/* U+1EA0: LATIN CAPITAL LETTER A WITH DOT BELOW */
	[1728+0x21] = 8,	/* U+1EA1: LATIN SMALL LETTER A WITH DOT BELOW */
	[1728+0x22] = 25,	/* U+1EA2: LATIN CAPITAL LETTER A WITH HOOK ABOVE */
	[1728+0x23] = 8,	/* U+1EA3: LATIN SMALL LETTER A WITH HOOK ABOVE */
	[1728+0x24] = 25,	/* U+1EA4: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE */
	[1728+0x25] = 8,	/* U+1EA5: LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE */
	[1728+0x26] = 25,	/* U+1EA6: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE */
	[1728+0x27] = 8,	/* U+1EA7: LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE */
	[1728+0x28] = 25,	/* U+1EA8: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE */
	[1728+0x29] = 8,	/* U+1EA9: LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE */
	[1728+0x2A] = 25,	/* U+1EAA: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE */
	[1728+0x2B] = 8,	/* U+1EAB: LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE */
	[1728+0x2C] = 25,	/* U+1EAC: LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW */
	[1728+0x2D] = 8,	/* U+1EAD: LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW */
	[1728+0x2E] = 25,	/* U+1EAE: LATIN CAPITAL LETTER A WITH BREVE AND ACUTE */
	[1728+0x2F] = 8,	/* U+1EAF: LATIN SMALL LETTER A WITH BREVE AND ACUTE */
	[1728+0x30] = 25,	/* U+1EB0: LATIN CAPITAL LETTER A WITH BREVE AND GRAVE */
	[1728+0x31] = 8,	/* U+1EB1: LATIN SMALL LETTER A WITH BREVE AND GRAVE */
	[1728+0x32] = 25,	/* U+1EB2: LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE */
	[1728+0x33] = 8,	/* U+1EB3: LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE */
	[1728+0x34] = 25,	/* U+1EB4: LATIN CAPITAL LETTER A WITH BREVE AND TILDE */
	[1728+0x35] = 8,	/* U+1EB5: LATIN SMALL LETTER A WITH BREVE AND TILDE */
	[1728+0x36] = 25,	/* U+1EB6: LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW */
	[1728+0x37] = 8,	/* U+1EB7: LATIN SMALL LETTER A WITH BREVE AND DOT BELOW */
	[1728+0x38] = 28,	/* U+1EB8: LATIN CAPITAL LETTER E WITH DOT BELOW */
	[1728+0x39] = 39,	/* U+1EB9: LATIN SMALL LETTER E WITH DOT BELOW */
	[1728+0x3A] = 28,	/* U+1EBA: LATIN CAPITAL LETTER E WITH HOOK ABOVE */
	[1728+0x3B] = 39,	/* U+1EBB: LATIN SMALL LETTER E WITH HOOK ABOVE */
	[1728+0x3C] = 28,	/* U+1EBC: LATIN CAPITAL LETTER E WITH TILDE */
	[1728+0x3D] = 39,	/* U+1EBD: LATIN SMALL LETTER E WITH TILDE */
	[1728+0x3E] = 28,	/* U+1EBE: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE */
	[1728+0x3F] = 39,	/* U+1EBF: LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE */
	[1152+0x3B] = 1792 - 0x80,	/* 341 273 ... */
	[1792+0x00] = 28,	/* U+1EC0: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[1792+0x01] = 39,	/* U+1EC1: LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE */
	[1792+0x02] = 28,	/* U+1EC2: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[1792+0x03] = 39,	/* U+1EC3: LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE */
	[1792+0x04] = 28,	/* U+1EC4: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE */
	[1792+0x05] = 39,	/* U+1EC5: LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE */
	[1792+0x06] = 28,	/* U+1EC6: LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[1792+0x07] = 39,	/* U+1EC7: LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW */
	[1792+0x08] = 29,	/* U+1EC8: LATIN CAPITAL LETTER I WITH HOOK ABOVE */
	[1792+0x09] = 40,	/* U+1EC9: LATIN SMALL LETTER I WITH HOOK ABOVE */
	[1792+0x0A] = 29,	/* U+1ECA: LATIN CAPITAL LETTER I WITH DOT BELOW */
	[1792+0x0B] = 40,	/* U+1ECB: LATIN SMALL LETTER I WITH DOT BELOW */
	[1792+0x0C] = 32,	/* U+1ECC: LATIN CAPITAL LETTER O WITH DOT BELOW */
	[1792+0x0D] = 20,	/* U+1ECD: LATIN SMALL LETTER O WITH DOT BELOW */
	[1792+0x0E] = 32,	/* U+1ECE: LATIN CAPITAL LETTER O WITH HOOK ABOVE */
	[1792+0x0F] = 20,	/* U+1ECF: LATIN SMALL LETTER O WITH HOOK ABOVE */
	[1792+0x10] = 32,	/* U+1ED0: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE */
	[1792+0x11] = 20,	/* U+1ED1: LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE */
	[1792+0x12] = 32,	/* U+1ED2: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE */
	[1792+0x13] = 20,	/* U+1ED3: LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE */
	[1792+0x14] = 32,	/* U+1ED4: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE */
	[1792+0x15] = 20,	/* U+1ED5: LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE */
	[1792+0x16] = 32,	/* U+1ED6: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE */
	[1792+0x17] = 20,	/* U+1ED7: LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE */
	[1792+0x18] = 32,	/* U+1ED8: LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW */
	[1792+0x19] = 20,	/* U+1ED9: LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW */
	[1792+0x1A] = 32,	/* U+1EDA: LATIN CAPITAL LETTER O WITH HORN AND ACUTE */
	[1792+0x1B] = 20,	/* U+1EDB: LATIN SMALL LETTER O WITH HORN AND ACUTE */
	[1792+0x1C] = 32,	/* U+1EDC: LATIN CAPITAL LETTER O WITH HORN AND GRAVE */
	[1792+0x1D] = 20,	/* U+1EDD: LATIN SMALL LETTER O WITH HORN AND GRAVE */
	[1792+0x1E] = 32,	/* U+1EDE: LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE */
	[1792+0x1F] = 20,	/* U+1EDF: LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE */
	[1792+0x20] = 32,	/* U+1EE0: LATIN CAPITAL LETTER O WITH HORN AND TILDE */
	[1792+0x21] = 20,	/* U+1EE1: LATIN SMALL LETTER O WITH HORN AND TILDE */
	[1792+0x22] = 32,	/* U+1EE2: LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW */
	[1792+0x23] = 20,	/* U+1EE3: LATIN SMALL LETTER O WITH HORN AND DOT BELOW */
	[1792+0x24] = 34,	/* U+1EE4: LATIN CAPITAL LETTER U WITH DOT BELOW */
	[1792+0x25] = 16,	/* U+1EE5: LATIN SMALL LETTER U WITH DOT BELOW */
	[1792+0x26] = 34,	/* U+1EE6: LATIN CAPITAL LETTER U WITH HOOK ABOVE */
	[1792+0x27] = 16,	/* U+1EE7: LATIN SMALL LETTER U WITH HOOK ABOVE */
	[1792+0x28] = 34,	/* U+1EE8: LATIN CAPITAL LETTER U WITH HORN AND ACUTE */
	[1792+0x29] = 16,	/* U+1EE9: LATIN SMALL LETTER U WITH HORN AND ACUTE */
	[1792+0x2A] = 34,	/* U+1EEA: LATIN CAPITAL LETTER U WITH HORN AND GRAVE */
	[1792+0x2B] = 16,	/* U+1EEB: LATIN SMALL LETTER U WITH HORN AND GRAVE */
	[1792+0x2C] = 34,	/* U+1EEC: LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE */
	[1792+0x2D] = 16,	/* U+1EED: LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE */
	[1792+0x2E] = 34,	/* U+1EEE: LATIN CAPITAL LETTER U WITH HORN AND TILDE */
	[1792+0x2F] = 16,	/* U+1EEF: LATIN SMALL LETTER U WITH HORN AND TILDE */
	[1792+0x30] = 34,	/* U+1EF0: LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW */
	[1792+0x31] = 16,	/* U+1EF1: LATIN SMALL LETTER U WITH HORN AND DOT BELOW */
	[1792+0x32] = 35,	/* U+1EF2: LATIN CAPITAL LETTER Y WITH GRAVE */
	[1792+0x33] = 44,	/* U+1EF3: LATIN SMALL LETTER Y WITH GRAVE */
	[1792+0x34] = 35,	/* U+1EF4: LATIN CAPITAL LETTER Y WITH DOT BELOW */
	[1792+0x35] = 44,	/* U+1EF5: LATIN SMALL LETTER Y WITH DOT BELOW */
	[1792+0x36] = 35,	/* U+1EF6: LATIN CAPITAL LETTER Y WITH HOOK ABOVE */
	[1792+0x37] = 44,	/* U+1EF7: LATIN SMALL LETTER Y WITH HOOK ABOVE */
	[1792+0x38] = 35,	/* U+1EF8: LATIN CAPITAL LETTER Y WITH TILDE */
	[1792+0x39] = 44,	/* U+1EF9: LATIN SMALL LETTER Y WITH TILDE */
	[1792+0x3A] = 111,	/* U+1EFA: LATIN CAPITAL LETTER MIDDLE-WELSH LL */
	[1792+0x3B] = 112,	/* U+1EFB: LATIN SMALL LETTER MIDDLE-WELSH LL */
	[1792+0x3C] = 81,	/* U+1EFC: LATIN CAPITAL LETTER MIDDLE-WELSH V */
	[1792+0x3D] = 94,	/* U+1EFD: LATIN SMALL LETTER MIDDLE-WELSH V */
	[1792+0x3E] = 35,	/* U+1EFE: LATIN CAPITAL LETTER Y WITH LOOP */
	[1792+0x3F] = 44,	/* U+1EFF: LATIN SMALL LETTER Y WITH LOOP */
	[1152+0x3F] = 1856 - 0x80,	/* 341 277 ... */
	[1856+0x2F] = 99,	/* U+1FEF: GREEK VARIA */
	[0xE2] = 1920 - 0x80,	/* 342 ... */
	[1920+0x00] = 1984 - 0x80,	/* 342 200 ... */
	[1984+0x00] = 1,	/* U+2000: EN QUAD */
	[1984+0x01] = 1,	/* U+2001: EM QUAD */
	[1984+0x02] = 1,	/* U+2002: EN SPACE */
	[1984+0x03] = 1,	/* U+2003: EM SPACE */
	[1984+0x04] = 1,	/* U+2004: THREE-PER-EM SPACE */
	[1984+0x05] = 1,	/* U+2005: FOUR-PER-EM SPACE */
	[1984+0x06] = 1,	/* U+2006: SIX-PER-EM SPACE */
	[1984+0x08] = 1,	/* U+2008: PUNCTUATION SPACE */
	[1984+0x09] = 1,	/* U+2009: THIN SPACE */
	[1984+0x0A] = 1,	/* U+200A: HAIR SPACE */
	[1984+0x0B] = 104,	/* U+200B: ZERO WIDTH SPACE */
	[1984+0x10] = 10,	/* U+2010: HYPHEN */
	[1984+0x11] = 10,	/* U+2011: NON-BREAKING HYPHEN */
	[1984+0x12] = 10,	/* U+2012: FIGURE DASH */
	[1984+0x13] = 10,	/* U+2013: EN DASH */
	[1984+0x14] = 113,	/* U+2014: EM DASH */
	[1984+0x15] = 10,	/* U+2015: HORIZONTAL BAR */
	[1984+0x18] = 15,	/* U+2018: LEFT SINGLE QUOTATION MARK */
	[1984+0x19] = 15,	/* U+2019: RIGHT SINGLE QUOTATION MARK */
	[1984+0x1A] = 18,	/* U+201A: SINGLE LOW-9 QUOTATION MARK */
	[1984+0x1B] = 15,	/* U+201B: SINGLE HIGH-REVERSED-9 QUOTATION MARK */
	[1984+0x1C] = 114,	/* U+201C: LEFT DOUBLE QUOTATION MARK */
	[1984+0x1D] = 114,	/* U+201D: RIGHT DOUBLE QUOTATION MARK */
	[1984+0x1E] = 115,	/* U+201E: DOUBLE LOW-9 QUOTATION MARK */
	[1984+0x1F] = 114,	/* U+201F: DOUBLE HIGH-REVERSED-9 QUOTATION MARK */
	[1984+0x20] = 116,	/* U+2020: DAGGER */
	[1984+0x22] = 20,	/* U+2022: BULLET */
	[1984+0x24] = 17,	/* U+2024: ONE DOT LEADER */
	[1984+0x25] = 117,	/* U+2025: TWO DOT LEADER */
	[1984+0x26] = 118,	/* U+2026: HORIZONTAL ELLIPSIS */
	[1984+0x2F] = 1,	/* U+202F: NARROW NO-BREAK SPACE */
	[1984+0x35] = 99,	/* U+2035: REVERSED PRIME */
	[1984+0x36] = 119,	/* U+2036: REVERSED DOUBLE PRIME */
	[1984+0x37] = 120,	/* U+2037: REVERSED TRIPLE PRIME */
	[1984+0x39] = 121,	/* U+2039: SINGLE LEFT-POINTING ANGLE QUOTATION MARK */
	[1984+0x3A] = 122,	/* U+203A: SINGLE RIGHT-POINTING ANGLE QUOTATION MARK */
	[1984+0x3C] = 123,	/* U+203C: DOUBLE EXCLAMATION MARK */
	[1920+0x01] = 2048 - 0x80,	/* 342 201 ... */
	[2048+0x04] = 43,	/* U+2044: FRACTION SLASH */
	[2048+0x07] = 124,	/* U+2047: DOUBLE QUESTION MARK */
	[2048+0x08] = 125,	/* U+2048: QUESTION EXCLAMATION MARK */
	[2048+0x09] = 126,	/* U+2049: EXCLAMATION QUESTION MARK */
	[2048+0x0A] = 127,	/* U+204A: TIRONIAN SIGN ET */
	[2048+0x1F] = 1,	/* U+205F: MEDIUM MATHEMATICAL SPACE */
	[2048+0x20] = 104,	/* U+2060: WORD JOINER */
	[2048+0x21] = 104,	/* U+2061: FUNCTION APPLICATION */
	[2048+0x22] = 104,	/* U+2062: INVISIBLE TIMES */
	[2048+0x23] = 104,	/* U+2063: INVISIBLE SEPARATOR */
	[2048+0x30] = 128,	/* U+2070: SUPERSCRIPT ZERO */
	[2048+0x31] = 40,	/* U+2071: SUPERSCRIPT LATIN SMALL LETTER I */
	[2048+0x34] = 129,	/* U+2074: SUPERSCRIPT FOUR */
	[2048+0x35] = 130,	/* U+2075: SUPERSCRIPT FIVE */
	[2048+0x36] = 131,	/* U+2076: SUPERSCRIPT SIX */
	[2048+0x37] = 132,	/* U+2077: SUPERSCRIPT SEVEN */
	[2048+0x38] = 133,	/* U+2078: SUPERSCRIPT EIGHT */
	[2048+0x39] = 134,	/* U+2079: SUPERSCRIPT NINE */
	[2048+0x3A] = 116,	/* U+207A: SUPERSCRIPT PLUS SIGN */
	[2048+0x3C] = 135,	/* U+207C: SUPERSCRIPT EQUALS SIGN */
	[2048+0x3D] = 136,	/* U+207D: SUPERSCRIPT LEFT PARENTHESIS */
	[2048+0x3E] = 137,	/* U+207E: SUPERSCRIPT RIGHT PARENTHESIS */
	[2048+0x3F] = 42,	/* U+207F: SUPERSCRIPT LATIN SMALL LETTER N */
	[1920+0x02] = 2112 - 0x80,	/* 342 202 ... */
	[2112+0x00] = 128,	/* U+2080: SUBSCRIPT ZERO */
	[2112+0x01] = 19,	/* U+2081: SUBSCRIPT ONE */
	[2112+0x02] = 13,	/* U+2082: SUBSCRIPT TWO */
	[2112+0x03] = 14,	/* U+2083: SUBSCRIPT THREE */
	[2112+0x04] = 129,	/* U+2084: SUBSCRIPT FOUR */
	[2112+0x05] = 130,	/* U+2085: SUBSCRIPT FIVE */
	[2112+0x06] = 131,	/* U+2086: SUBSCRIPT SIX */
	[2112+0x07] = 132,	/* U+2087: SUBSCRIPT SEVEN */
	[2112+0x08] = 133,	/* U+2088: SUBSCRIPT EIGHT */
	[2112+0x09] = 134,	/* U+2089: SUBSCRIPT NINE */
	[2112+0x0A] = 116,	/* U+208A: SUBSCRIPT PLUS SIGN */
	[2112+0x0C] = 135,	/* U+208C: SUBSCRIPT EQUALS SIGN */
	[2112+0x0D] = 136,	/* U+208D: SUBSCRIPT LEFT PARENTHESIS */
	[2112+0x0E] = 137,	/* U+208E: SUBSCRIPT RIGHT PARENTHESIS */
	[2112+0x10] = 8,	/* U+2090: LATIN SUBSCRIPT SMALL LETTER A */
	[2112+0x11] = 39,	/* U+2091: LATIN SUBSCRIPT SMALL LETTER E */
	[2112+0x12] = 20,	/* U+2092: LATIN SUBSCRIPT SMALL LETTER O */
	[2112+0x13] = 33,	/* U+2093: LATIN SUBSCRIPT SMALL LETTER X */
	[2112+0x15] = 49,	/* U+2095: LATIN SUBSCRIPT SMALL LETTER H */
	[2112+0x16] = 55,	/* U+2096: LATIN SUBSCRIPT SMALL LETTER K */
	[2112+0x17] = 58,	/* U+2097: LATIN SUBSCRIPT SMALL LETTER L */
	[2112+0x18] = 93,	/* U+2098: LATIN SUBSCRIPT SMALL LETTER M */
	[2112+0x19] = 42,	/* U+2099: LATIN SUBSCRIPT SMALL LETTER N */
	[2112+0x1A] = 80,	/* U+209A: LATIN SUBSCRIPT SMALL LETTER P */
	[2112+0x1B] = 65,	/* U+209B: LATIN SUBSCRIPT SMALL LETTER S */
	[2112+0x1C] = 67,	/* U+209C: LATIN SUBSCRIPT SMALL LETTER T */
	[2112+0x20] = 138,	/* U+20A0: EURO-CURRENCY SIGN */
	[2112+0x21] = 139,	/* U+20A1: COLON SIGN */
	[2112+0x22] = 140,	/* U+20A2: CRUZEIRO SIGN */
	[2112+0x23] = 141,	/* U+20A3: FRENCH FRANC SIGN */
	[2112+0x24] = 142,	/* U+20A4: LIRA SIGN */
	[2112+0x27] = 143,	/* U+20A7: PESETA SIGN */
	[2112+0x28] = 144,	/* U+20A8: RUPEE SIGN */
	[2112+0x29] = 145,	/* U+20A9: WON SIGN */
	[2112+0x2A] = 146,	/* U+20AA: NEW SHEQEL SIGN */
	[2112+0x2B] = 147,	/* U+20AB: DONG SIGN */
	[2112+0x2C] = 148,	/* U+20AC: EURO SIGN */
	[2112+0x2F] = 149,	/* U+20AF: DRACHMA SIGN */
	[2112+0x31] = 150,	/* U+20B1: PESO SIGN */
	[2112+0x34] = 151,	/* U+20B4: HRYVNIA SIGN */
	[2112+0x38] = 152,	/* U+20B8: TENGE SIGN */
	[2112+0x39] = 153,	/* U+20B9: INDIAN RUPEE SIGN */
	[2112+0x3A] = 154,	/* U+20BA: TURKISH LIRA SIGN */
	[2112+0x3D] = 155,	/* U+20BD: RUBLE SIGN */
	[2112+0x3E] = 156,	/* U+20BE: LARI SIGN */
	[1920+0x03] = 2176 - 0x80,	/* 342 203 ... */
	[2176+0x10] = 104,	/* U+20D0: COMBINING LEFT HARPOON ABOVE */
	[2176+0x11] = 104,	/* U+20D1: COMBINING RIGHT HARPOON ABOVE */
	[2176+0x12] = 104,	/* U+20D2: COMBINING LONG VERTICAL LINE OVERLAY */
	[2176+0x13] = 104,	/* U+20D3: COMBINING SHORT VERTICAL LINE OVERLAY */
	[2176+0x14] = 104,	/* U+20D4: COMBINING ANTICLOCKWISE ARROW ABOVE */
	[2176+0x15] = 104,	/* U+20D5: COMBINING CLOCKWISE ARROW ABOVE */
	[2176+0x16] = 104,	/* U+20D6: COMBINING LEFT ARROW ABOVE */
	[2176+0x17] = 104,	/* U+20D7: COMBINING RIGHT ARROW ABOVE */
	[2176+0x18] = 104,	/* U+20D8: COMBINING RING OVERLAY */
	[2176+0x19] = 104,	/* U+20D9: COMBINING CLOCKWISE RING OVERLAY */
	[2176+0x1A] = 104,	/* U+20DA: COMBINING ANTICLOCKWISE RING OVERLAY */
	[2176+0x1B] = 104,	/* U+20DB: COMBINING THREE DOTS ABOVE */
	[2176+0x1C] = 104,	/* U+20DC: COMBINING FOUR DOTS ABOVE */
	[2176+0x1D] = 104,	/* U+20DD: COMBINING ENCLOSING CIRCLE */
	[2176+0x1E] = 104,	/* U+20DE: COMBINING ENCLOSING SQUARE */
	[2176+0x1F] = 104,	/* U+20DF: COMBINING ENCLOSING DIAMOND */
	[2176+0x20] = 104,	/* U+20E0: COMBINING ENCLOSING CIRCLE BACKSLASH */
	[2176+0x21] = 104,	/* U+20E1: COMBINING LEFT RIGHT ARROW ABOVE */
	[2176+0x22] = 104,	/* U+20E2: COMBINING ENCLOSING SCREEN */
	[2176+0x23] = 104,	/* U+20E3: COMBINING ENCLOSING KEYCAP */
	[2176+0x24] = 104,	/* U+20E4: COMBINING ENCLOSING UPWARD POINTING TRIANGLE */
	[2176+0x25] = 104,	/* U+20E5: COMBINING REVERSE SOLIDUS OVERLAY */
	[2176+0x26] = 104,	/* U+20E6: COMBINING DOUBLE VERTICAL STROKE OVERLAY */
	[2176+0x27] = 104,	/* U+20E7: COMBINING ANNUITY SYMBOL */
	[2176+0x28] = 104,	/* U+20E8: COMBINING TRIPLE UNDERDOT */
	[2176+0x29] = 104,	/* U+20E9: COMBINING WIDE BRIDGE ABOVE */
	[2176+0x2A] = 104,	/* U+20EA: COMBINING LEFTWARDS ARROW OVERLAY */
	[2176+0x2B] = 104,	/* U+20EB: COMBINING LONG DOUBLE SOLIDUS OVERLAY */
	[2176+0x2C] = 104,	/* U+20EC: COMBINING RIGHTWARDS HARPOON WITH BARB DOWNWARDS */
	[2176+0x2D] = 104,	/* U+20ED: COMBINING LEFTWARDS HARPOON WITH BARB DOWNWARDS */
	[2176+0x2E] = 104,	/* U+20EE: COMBINING LEFT ARROW BELOW */
	[2176+0x2F] = 104,	/* U+20EF: COMBINING RIGHT ARROW BELOW */
	[2176+0x30] = 104,	/* U+20F0: COMBINING ASTERISK ABOVE */
	[1920+0x04] = 2240 - 0x80,	/* 342 204 ... */
	[2240+0x00] = 157,	/* U+2100: ACCOUNT OF */
	[2240+0x01] = 158,	/* U+2101: ADDRESSED TO THE SUBJECT */
	[2240+0x02] = 27,	/* U+2102: DOUBLE-STRUCK CAPITAL C */
	[2240+0x05] = 159,	/* U+2105: CARE OF */
	[2240+0x06] = 160,	/* U+2106: CADA UNA */
	[2240+0x0A] = 47,	/* U+210A: SCRIPT SMALL G */
	[2240+0x0B] = 48,	/* U+210B: SCRIPT CAPITAL H */
	[2240+0x0C] = 48,	/* U+210C: BLACK-LETTER CAPITAL H */
	[2240+0x0D] = 48,	/* U+210D: DOUBLE-STRUCK CAPITAL H */
	[2240+0x0E] = 49,	/* U+210E: PLANCK CONSTANT */
	[2240+0x10] = 29,	/* U+2110: SCRIPT CAPITAL I */
	[2240+0x11] = 29,	/* U+2111: BLACK-LETTER CAPITAL I */
	[2240+0x12] = 57,	/* U+2112: SCRIPT CAPITAL L */
	[2240+0x13] = 58,	/* U+2113: SCRIPT SMALL L */
	[2240+0x15] = 31,	/* U+2115: DOUBLE-STRUCK CAPITAL N */
	[2240+0x16] = 161,	/* U+2116: NUMERO SIGN */
	[2240+0x19] = 79,	/* U+2119: DOUBLE-STRUCK CAPITAL P */
	[2240+0x1A] = 162,	/* U+211A: DOUBLE-STRUCK CAPITAL Q */
	[2240+0x1B] = 62,	/* U+211B: SCRIPT CAPITAL R */
	[2240+0x1C] = 62,	/* U+211C: BLACK-LETTER CAPITAL R */
	[2240+0x1D] = 62,	/* U+211D: DOUBLE-STRUCK CAPITAL R */
	[2240+0x1E] = 163,	/* U+211E: PRESCRIPTION TAKE */
	[2240+0x20] = 164,	/* U+2120: SERVICE MARK */
	[2240+0x21] = 165,	/* U+2121: TELEPHONE SIGN */
	[2240+0x22] = 166,	/* U+2122: TRADE MARK SIGN */
	[2240+0x24] = 70,	/* U+2124: DOUBLE-STRUCK CAPITAL Z */
	[2240+0x28] = 70,	/* U+2128: BLACK-LETTER CAPITAL Z */
	[2240+0x2A] = 54,	/* U+212A: KELVIN SIGN */
	[2240+0x2B] = 25,	/* U+212B: ANGSTROM SIGN */
	[2240+0x2C] = 73,	/* U+212C: SCRIPT CAPITAL B */
	[2240+0x2D] = 27,	/* U+212D: BLACK-LETTER CAPITAL C */
	[2240+0x2E] = 39,	/* U+212E: ESTIMATED SYMBOL */
	[2240+0x2F] = 39,	/* U+212F: SCRIPT SMALL E */
	[2240+0x30] = 28,	/* U+2130: SCRIPT CAPITAL E */
	[2240+0x31] = 74,	/* U+2131: SCRIPT CAPITAL F */
	[2240+0x33] = 107,	/* U+2133: SCRIPT CAPITAL M */
	[2240+0x34] = 20,	/* U+2134: SCRIPT SMALL O */
	[2240+0x39] = 40,	/* U+2139: INFORMATION SOURCE */
	[2240+0x3B] = 167,	/* U+213B: FACSIMILE SIGN */
	[1920+0x05] = 2304 - 0x80,	/* 342 205 ... */
	[2304+0x05] = 30,	/* U+2145: DOUBLE-STRUCK ITALIC CAPITAL D */
	[2304+0x06] = 41,	/* U+2146: DOUBLE-STRUCK ITALIC SMALL D */
	[2304+0x07] = 39,	/* U+2147: DOUBLE-STRUCK ITALIC SMALL E */
	[2304+0x08] = 40,	/* U+2148: DOUBLE-STRUCK ITALIC SMALL I */
	[2304+0x09] = 53,	/* U+2149: DOUBLE-STRUCK ITALIC SMALL J */
	[2304+0x10] = 168,	/* U+2150: VULGAR FRACTION ONE SEVENTH */
	[2304+0x11] = 169,	/* U+2151: VULGAR FRACTION ONE NINTH */
	[2304+0x12] = 170,	/* U+2152: VULGAR FRACTION ONE TENTH */
	[2304+0x13] = 171,	/* U+2153: VULGAR FRACTION ONE THIRD */
	[2304+0x14] = 172,	/* U+2154: VULGAR FRACTION TWO THIRDS */
	[2304+0x15] = 173,	/* U+2155: VULGAR FRACTION ONE FIFTH */
	[2304+0x16] = 174,	/* U+2156: VULGAR FRACTION TWO FIFTHS */
	[2304+0x17] = 175,	/* U+2157: VULGAR FRACTION THREE FIFTHS */
	[2304+0x18] = 176,	/* U+2158: VULGAR FRACTION FOUR FIFTHS */
	[2304+0x19] = 177,	/* U+2159: VULGAR FRACTION ONE SIXTH */
	[2304+0x1A] = 178,	/* U+215A: VULGAR FRACTION FIVE SIXTHS */
	[2304+0x1B] = 179,	/* U+215B: VULGAR FRACTION ONE EIGHTH */
	[2304+0x1C] = 180,	/* U+215C: VULGAR FRACTION THREE EIGHTHS */
	[2304+0x1D] = 181,	/* U+215D: VULGAR FRACTION FIVE EIGHTHS */
	[2304+0x1E] = 182,	/* U+215E: VULGAR FRACTION SEVEN EIGHTHS */
	[2304+0x1F] = 183,	/* U+215F: FRACTION NUMERATOR ONE */
	[2304+0x20] = 29,	/* U+2160: ROMAN NUMERAL ONE */
	[2304+0x21] = 184,	/* U+2161: ROMAN NUMERAL TWO */
	[2304+0x22] = 185,	/* U+2162: ROMAN NUMERAL THREE */
	[2304+0x23] = 186,	/* U+2163: ROMAN NUMERAL FOUR */
	[2304+0x24] = 81,	/* U+2164: ROMAN NUMERAL FIVE */
	[2304+0x25] = 187,	/* U+2165: ROMAN NUMERAL SIX */
	[2304+0x26] = 188,	/* U+2166: ROMAN NUMERAL SEVEN */
	[2304+0x27] = 189,	/* U+2167: ROMAN NUMERAL EIGHT */
	[2304+0x28] = 190,	/* U+2168: ROMAN NUMERAL NINE */
	[2304+0x29] = 109,	/* U+2169: ROMAN NUMERAL TEN */
	[2304+0x2A] = 191,	/* U+216A: ROMAN NUMERAL ELEVEN */
	[2304+0x2B] = 192,	/* U+216B: ROMAN NUMERAL TWELVE */
	[2304+0x2C] = 57,	/* U+216C: ROMAN NUMERAL FIFTY */
	[2304+0x2D] = 27,	/* U+216D: ROMAN NUMERAL ONE HUNDRED */
	[2304+0x2E] = 30,	/* U+216E: ROMAN NUMERAL FIVE HUNDRED */
	[2304+0x2F] = 107,	/* U+216F: ROMAN NUMERAL ONE THOUSAND */
	[2304+0x30] = 40,	/* U+2170: SMALL ROMAN NUMERAL ONE */
	[2304+0x31] = 193,	/* U+2171: SMALL ROMAN NUMERAL TWO */
	[2304+0x32] = 194,	/* U+2172: SMALL ROMAN NUMERAL THREE */
	[2304+0x33] = 195,	/* U+2173: SMALL ROMAN NUMERAL FOUR */
	[2304+0x34] = 94,	/* U+2174: SMALL ROMAN NUMERAL FIVE */
	[2304+0x35] = 196,	/* U+2175: SMALL ROMAN NUMERAL SIX */
	[2304+0x36] = 197,	/* U+2176: SMALL ROMAN NUMERAL SEVEN */
	[2304+0x37] = 198,	/* U+2177: SMALL ROMAN NUMERAL EIGHT */
	[2304+0x38] = 199,	/* U+2178: SMALL ROMAN NUMERAL NINE */
	[2304+0x39] = 33,	/* U+2179: SMALL ROMAN NUMERAL TEN */
	[2304+0x3A] = 200,	/* U+217A: SMALL ROMAN NUMERAL ELEVEN */
	[2304+0x3B] = 201,	/* U+217B: SMALL ROMAN NUMERAL TWELVE */
	[2304+0x3C] = 58,	/* U+217C: SMALL ROMAN NUMERAL FIFTY */
	[2304+0x3D] = 3,	/* U+217D: SMALL ROMAN NUMERAL ONE HUNDRED */
	[2304+0x3E] = 41,	/* U+217E: SMALL ROMAN NUMERAL FIVE HUNDRED */
	[2304+0x3F] = 93,	/* U+217F: SMALL ROMAN NUMERAL ONE THOUSAND */
	[1920+0x06] = 2368 - 0x80,	/* 342 206 ... */
	[2368+0x09] = 202,	/* U+2189: VULGAR FRACTION ZERO THIRDS */
	[2368+0x10] = 203,	/* U+2190: LEFTWARDS ARROW */
	[2368+0x12] = 204,	/* U+2192: RIGHTWARDS ARROW */
	[2368+0x14] = 205,	/* U+2194: LEFT RIGHT ARROW */
	[2368+0x2E] = 206,	/* U+21AE: LEFT RIGHT ARROW WITH STROKE */
	[1920+0x07] = 2432 - 0x80,	/* 342 207 ... */
	[2432+0x0D] = 207,	/* U+21CD: LEFTWARDS DOUBLE ARROW WITH STROKE */
	[2432+0x0E] = 208,	/* U+21CE: LEFT RIGHT DOUBLE ARROW WITH STROKE */
	[2432+0x0F] = 209,	/* U+21CF: RIGHTWARDS DOUBLE ARROW WITH STROKE */
	[2432+0x10] = 210,	/* U+21D0: LEFTWARDS DOUBLE ARROW */
	[2432+0x12] = 211,	/* U+21D2: RIGHTWARDS DOUBLE ARROW */
	[2432+0x14] = 212,	/* U+21D4: LEFT RIGHT DOUBLE ARROW */
	[1920+0x08] = 2496 - 0x80,	/* 342 210 ... */
	[2496+0x12] = 10,	/* U+2212: MINUS SIGN */
	[2496+0x15] = 43,	/* U+2215: DIVISION SLASH */
	[2496+0x16] = 213,	/* U+2216: SET MINUS */
	[2496+0x17] = 214,	/* U+2217: ASTERISK OPERATOR */
	[2496+0x23] = 6,	/* U+2223: DIVIDES */
	[2496+0x25] = 215,	/* U+2225: PARALLEL TO */
	[2496+0x36] = 101,	/* U+2236: RATIO */
	[2496+0x3C] = 102,	/* U+223C: TILDE OPERATOR */
	[1920+0x09] = 2560 - 0x80,	/* 342 211 ... */
	[2560+0x01] = 216,	/* U+2241: NOT TILDE */
	[2560+0x04] = 217,	/* U+2244: NOT ASYMPTOTICALLY EQUAL TO */
	[2560+0x07] = 218,	/* U+2247: NEITHER APPROXIMATELY NOR ACTUALLY EQUAL TO */
	[2560+0x09] = 219,	/* U+2249: NOT ALMOST EQUAL TO */
	[2560+0x20] = 220,	/* U+2260: NOT EQUAL TO */
	[2560+0x22] = 221,	/* U+2262: NOT IDENTICAL TO */
	[2560+0x24] = 210,	/* U+2264: LESS-THAN OR EQUAL TO */
	[2560+0x25] = 222,	/* U+2265: GREATER-THAN OR EQUAL TO */
	[2560+0x2A] = 9,	/* U+226A: MUCH LESS-THAN */
	[2560+0x2B] = 21,	/* U+226B: MUCH GREATER-THAN */
	[2560+0x2E] = 223,	/* U+226E: NOT LESS-THAN */
	[2560+0x2F] = 224,	/* U+226F: NOT GREATER-THAN */
	[2560+0x30] = 207,	/* U+2270: NEITHER LESS-THAN NOR EQUAL TO */
	[2560+0x31] = 225,	/* U+2271: NEITHER GREATER-THAN NOR EQUAL TO */
	[2560+0x34] = 226,	/* U+2274: NEITHER LESS-THAN NOR EQUIVALENT TO */
	[2560+0x35] = 227,	/* U+2275: NEITHER GREATER-THAN NOR EQUIVALENT TO */
	[2560+0x38] = 228,	/* U+2278: NEITHER LESS-THAN NOR GREATER-THAN */
	[2560+0x39] = 229,	/* U+2279: NEITHER GREATER-THAN NOR LESS-THAN */
	[1920+0x0B] = 2624 - 0x80,	/* 342 213 ... */
	[2624+0x18] = 230,	/* U+22D8: VERY MUCH LESS-THAN */
	[2624+0x19] = 231,	/* U+22D9: VERY MUCH GREATER-THAN */
	[1920+0x0C] = 2688 - 0x80,	/* 342 214 ... */
	[2688+0x29] = 121,	/* U+2329: LEFT-POINTING ANGLE BRACKET */
	[2688+0x2A] = 122,	/* U+232A: RIGHT-POINTING ANGLE BRACKET */
	[1920+0x10] = 2752 - 0x80,	/* 342 220 ... */
	[2752+0x00] = 232,	/* U+2400: SYMBOL FOR NULL */
	[2752+0x01] = 233,	/* U+2401: SYMBOL FOR START OF HEADING */
	[2752+0x02] = 234,	/* U+2402: SYMBOL FOR START OF TEXT */
	[2752+0x03] = 235,	/* U+2403: SYMBOL FOR END OF TEXT */
	[2752+0x04] = 236,	/* U+2404: SYMBOL FOR END OF TRANSMISSION */
	[2752+0x05] = 237,	/* U+2405: SYMBOL FOR ENQUIRY */
	[2752+0x06] = 238,	/* U+2406: SYMBOL FOR ACKNOWLEDGE */
	[2752+0x07] = 239,	/* U+2407: SYMBOL FOR BELL */
	[2752+0x08] = 240,	/* U+2408: SYMBOL FOR BACKSPACE */
	[2752+0x09] = 241,	/* U+2409: SYMBOL FOR HORIZONTAL TABULATION */
	[2752+0x0A] = 242,	/* U+240A: SYMBOL FOR LINE FEED */
	[2752+0x0B] = 243,	/* U+240B: SYMBOL FOR VERTICAL TABULATION */
	[2752+0x0C] = 244,	/* U+240C: SYMBOL FOR FORM FEED */
	[2752+0x0D] = 245,	/* U+240D: SYMBOL FOR CARRIAGE RETURN */
	[2752+0x0E] = 246,	/* U+240E: SYMBOL FOR SHIFT OUT */
	[2752+0x0F] = 247,	/* U+240F: SYMBOL FOR SHIFT IN */
	[2752+0x10] = 248,	/* U+2410: SYMBOL FOR DATA LINK ESCAPE */
	[2752+0x11] = 249,	/* U+2411: SYMBOL FOR DEVICE CONTROL ONE */
	[2752+0x12] = 250,	/* U+2412: SYMBOL FOR DEVICE CONTROL TWO */
	[2752+0x13] = 251,	/* U+2413: SYMBOL FOR DEVICE CONTROL THREE */
	[2752+0x14] = 252,	/* U+2414: SYMBOL FOR DEVICE CONTROL FOUR */
	[2752+0x15] = 253,	/* U+2415: SYMBOL FOR NEGATIVE ACKNOWLEDGE */
	[2752+0x16] = 254,	/* U+2416: SYMBOL FOR SYNCHRONOUS IDLE */
	[2752+0x17] = 255,	/* U+2417: SYMBOL FOR END OF TRANSMISSION BLOCK */
	[2752+0x18] = 256,	/* U+2418: SYMBOL FOR CANCEL */
	[2752+0x19] = 257,	/* U+2419: SYMBOL FOR END OF MEDIUM */
	[2752+0x1A] = 258,	/* U+241A: SYMBOL FOR SUBSTITUTE */
	[2752+0x1B] = 259,	/* U+241B: SYMBOL FOR ESCAPE */
	[2752+0x1C] = 260,	/* U+241C: SYMBOL FOR FILE SEPARATOR */
	[2752+0x1D] = 261,	/* U+241D: SYMBOL FOR GROUP SEPARATOR */
	[2752+0x1E] = 262,	/* U+241E: SYMBOL FOR RECORD SEPARATOR */
	[2752+0x1F] = 263,	/* U+241F: SYMBOL FOR UNIT SEPARATOR */
	[2752+0x20] = 264,	/* U+2420: SYMBOL FOR SPACE */
	[2752+0x21] = 265,	/* U+2421: SYMBOL FOR DELETE */
	[2752+0x23] = 100,	/* U+2423: OPEN BOX */
	[2752+0x24] = 266,	/* U+2424: SYMBOL FOR NEWLINE */
	[1920+0x11] = 2816 - 0x80,	/* 342 221 ... */
	[2816+0x20] = 267,	/* U+2460: CIRCLED DIGIT ONE */
	[2816+0x21] = 268,	/* U+2461: CIRCLED DIGIT TWO */
	[2816+0x22] = 269,	/* U+2462: CIRCLED DIGIT THREE */
	[2816+0x23] = 270,	/* U+2463: CIRCLED DIGIT FOUR */
	[2816+0x24] = 271,	/* U+2464: CIRCLED DIGIT FIVE */
	[2816+0x25] = 272,	/* U+2465: CIRCLED DIGIT SIX */
	[2816+0x26] = 273,	/* U+2466: CIRCLED DIGIT SEVEN */
	[2816+0x27] = 274,	/* U+2467: CIRCLED DIGIT EIGHT */
	[2816+0x28] = 275,	/* U+2468: CIRCLED DIGIT NINE */
	[2816+0x29] = 276,	/* U+2469: CIRCLED NUMBER TEN */
	[2816+0x2A] = 277,	/* U+246A: CIRCLED NUMBER ELEVEN */
	[2816+0x2B] = 278,	/* U+246B: CIRCLED NUMBER TWELVE */
	[2816+0x2C] = 279,	/* U+246C: CIRCLED NUMBER THIRTEEN */
	[2816+0x2D] = 280,	/* U+246D: CIRCLED NUMBER FOURTEEN */
	[2816+0x2E] = 281,	/* U+246E: CIRCLED NUMBER FIFTEEN */
	[2816+0x2F] = 282,	/* U+246F: CIRCLED NUMBER SIXTEEN */
	[2816+0x30] = 283,	/* U+2470: CIRCLED NUMBER SEVENTEEN */
	[2816+0x31] = 284,	/* U+2471: CIRCLED NUMBER EIGHTEEN */
	[2816+0x32] = 285,	/* U+2472: CIRCLED NUMBER NINETEEN */
	[2816+0x33] = 286,	/* U+2473: CIRCLED NUMBER TWENTY */
	[2816+0x34] = 267,	/* U+2474: PARENTHESIZED DIGIT ONE */
	[2816+0x35] = 268,	/* U+2475: PARENTHESIZED DIGIT TWO */
	[2816+0x36] = 269,	/* U+2476: PARENTHESIZED DIGIT THREE */
	[2816+0x37] = 270,	/* U+2477: PARENTHESIZED DIGIT FOUR */
	[2816+0x38] = 271,	/* U+2478: PARENTHESIZED DIGIT FIVE */
	[2816+0x39] = 272,	/* U+2479: PARENTHESIZED DIGIT SIX */
	[2816+0x3A] = 273,	/* U+247A: PARENTHESIZED DIGIT SEVEN */
	[2816+0x3B] = 274,	/* U+247B: PARENTHESIZED DIGIT EIGHT */
	[2816+0x3C] = 275,	/* U+247C: PARENTHESIZED DIGIT NINE */
	[2816+0x3D] = 276,	/* U+247D: PARENTHESIZED NUMBER TEN */
	[2816+0x3E] = 277,	/* U+247E: PARENTHESIZED NUMBER ELEVEN */
	[2816+0x3F] = 278,	/* U+247F: PARENTHESIZED NUMBER TWELVE */
	[1920+0x12] = 2880 - 0x80,	/* 342 222 ... */
	[2880+0x00] = 279,	/* U+2480: PARENTHESIZED NUMBER THIRTEEN */
	[2880+0x01] = 280,	/* U+2481: PARENTHESIZED NUMBER FOURTEEN */
	[2880+0x02] = 281,	/* U+2482: PARENTHESIZED NUMBER FIFTEEN */
	[2880+0x03] = 282,	/* U+2483: PARENTHESIZED NUMBER SIXTEEN */
	[2880+0x04] = 283,	/* U+2484: PARENTHESIZED NUMBER SEVENTEEN */
	[2880+0x05] = 284,	/* U+2485: PARENTHESIZED NUMBER EIGHTEEN */
	[2880+0x06] = 285,	/* U+2486: PARENTHESIZED NUMBER NINETEEN */
	[2880+0x07] = 286,	/* U+2487: PARENTHESIZED NUMBER TWENTY */
	[2880+0x08] = 287,	/* U+2488: DIGIT ONE FULL STOP */
	[2880+0x09] = 288,	/* U+2489: DIGIT TWO FULL STOP */
	[2880+0x0A] = 289,	/* U+248A: DIGIT THREE FULL STOP */
	[2880+0x0B] = 290,	/* U+248B: DIGIT FOUR FULL STOP */
	[2880+0x0C] = 291,	/* U+248C: DIGIT FIVE FULL STOP */
	[2880+0x0D] = 292,	/* U+248D: DIGIT SIX FULL STOP */
	[2880+0x0E] = 293,	/* U+248E: DIGIT SEVEN FULL STOP */
	[2880+0x0F] = 294,	/* U+248F: DIGIT EIGHT FULL STOP */
	[2880+0x10] = 295,	/* U+2490: DIGIT NINE FULL STOP */
	[2880+0x11] = 296,	/* U+2491: NUMBER TEN FULL STOP */
	[2880+0x12] = 297,	/* U+2492: NUMBER ELEVEN FULL STOP */
	[2880+0x13] = 298,	/* U+2493: NUMBER TWELVE FULL STOP */
	[2880+0x14] = 299,	/* U+2494: NUMBER THIRTEEN FULL STOP */
	[2880+0x15] = 300,	/* U+2495: NUMBER FOURTEEN FULL STOP */
	[2880+0x16] = 301,	/* U+2496: NUMBER FIFTEEN FULL STOP */
	[2880+0x17] = 302,	/* U+2497: NUMBER SIXTEEN FULL STOP */
	[2880+0x18] = 303,	/* U+2498: NUMBER SEVENTEEN FULL STOP */
	[2880+0x19] = 304,	/* U+2499: NUMBER EIGHTEEN FULL STOP */
	[2880+0x1A] = 305,	/* U+249A: NUMBER NINETEEN FULL STOP */
	[2880+0x1B] = 306,	/* U+249B: NUMBER TWENTY FULL STOP */
	[2880+0x1C] = 307,	/* U+249C: PARENTHESIZED LATIN SMALL LETTER A */
	[2880+0x1D] = 308,	/* U+249D: PARENTHESIZED LATIN SMALL LETTER B */
	[2880+0x1E] = 309,	/* U+249E: PARENTHESIZED LATIN SMALL LETTER C */
	[2880+0x1F] = 310,	/* U+249F: PARENTHESIZED LATIN SMALL LETTER D */
	[2880+0x20] = 311,	/* U+24A0: PARENTHESIZED LATIN SMALL LETTER E */
	[2880+0x21] = 312,	/* U+24A1: PARENTHESIZED LATIN SMALL LETTER F */
	[2880+0x22] = 313,	/* U+24A2: PARENTHESIZED LATIN SMALL LETTER G */
	[2880+0x23] = 314,	/* U+24A3: PARENTHESIZED LATIN SMALL LETTER H */
	[2880+0x24] = 315,	/* U+24A4: PARENTHESIZED LATIN SMALL LETTER I */
	[2880+0x25] = 316,	/* U+24A5: PARENTHESIZED LATIN SMALL LETTER J */
	[2880+0x26] = 317,	/* U+24A6: PARENTHESIZED LATIN SMALL LETTER K */
	[2880+0x27] = 318,	/* U+24A7: PARENTHESIZED LATIN SMALL LETTER L */
	[2880+0x28] = 319,	/* U+24A8: PARENTHESIZED LATIN SMALL LETTER M */
	[2880+0x29] = 320,	/* U+24A9: PARENTHESIZED LATIN SMALL LETTER N */
	[2880+0x2A] = 321,	/* U+24AA: PARENTHESIZED LATIN SMALL LETTER O */
	[2880+0x2B] = 322,	/* U+24AB: PARENTHESIZED LATIN SMALL LETTER P */
	[2880+0x2C] = 323,	/* U+24AC: PARENTHESIZED LATIN SMALL LETTER Q */
	[2880+0x2D] = 324,	/* U+24AD: PARENTHESIZED LATIN SMALL LETTER R */
	[2880+0x2E] = 325,	/* U+24AE: PARENTHESIZED LATIN SMALL LETTER S */
	[2880+0x2F] = 326,	/* U+24AF: PARENTHESIZED LATIN SMALL LETTER T */
	[2880+0x30] = 327,	/* U+24B0: PARENTHESIZED LATIN SMALL LETTER U */
	[2880+0x31] = 328,	/* U+24B1: PARENTHESIZED LATIN SMALL LETTER V */
	[2880+0x32] = 329,	/* U+24B2: PARENTHESIZED LATIN SMALL LETTER W */
	[2880+0x33] = 330,	/* U+24B3: PARENTHESIZED LATIN SMALL LETTER X */
	[2880+0x34] = 331,	/* U+24B4: PARENTHESIZED LATIN SMALL LETTER Y */
	[2880+0x35] = 332,	/* U+24B5: PARENTHESIZED LATIN SMALL LETTER Z */
	[2880+0x36] = 333,	/* U+24B6: CIRCLED LATIN CAPITAL LETTER A */
	[2880+0x37] = 334,	/* U+24B7: CIRCLED LATIN CAPITAL LETTER B */
	[2880+0x38] = 7,	/* U+24B8: CIRCLED LATIN CAPITAL LETTER C */
	[2880+0x39] = 335,	/* U+24B9: CIRCLED LATIN CAPITAL LETTER D */
	[2880+0x3A] = 336,	/* U+24BA: CIRCLED LATIN CAPITAL LETTER E */
	[2880+0x3B] = 337,	/* U+24BB: CIRCLED LATIN CAPITAL LETTER F */
	[2880+0x3C] = 338,	/* U+24BC: CIRCLED LATIN CAPITAL LETTER G */
	[2880+0x3D] = 339,	/* U+24BD: CIRCLED LATIN CAPITAL LETTER H */
	[2880+0x3E] = 340,	/* U+24BE: CIRCLED LATIN CAPITAL LETTER I */
	[2880+0x3F] = 341,	/* U+24BF: CIRCLED LATIN CAPITAL LETTER J */
	[1920+0x13] = 2944 - 0x80,	/* 342 223 ... */
	[2944+0x00] = 342,	/* U+24C0: CIRCLED LATIN CAPITAL LETTER K */
	[2944+0x01] = 343,	/* U+24C1: CIRCLED LATIN CAPITAL LETTER L */
	[2944+0x02] = 344,	/* U+24C2: CIRCLED LATIN CAPITAL LETTER M */
	[2944+0x03] = 345,	/* U+24C3: CIRCLED LATIN CAPITAL LETTER N */
	[2944+0x04] = 346,	/* U+24C4: CIRCLED LATIN CAPITAL LETTER O */
	[2944+0x05] = 347,	/* U+24C5: CIRCLED LATIN CAPITAL LETTER P */
	[2944+0x06] = 348,	/* U+24C6: CIRCLED LATIN CAPITAL LETTER Q */
	[2944+0x07] = 11,	/* U+24C7: CIRCLED LATIN CAPITAL LETTER R */
	[2944+0x08] = 349,	/* U+24C8: CIRCLED LATIN CAPITAL LETTER S */
	[2944+0x09] = 350,	/* U+24C9: CIRCLED LATIN CAPITAL LETTER T */
	[2944+0x0A] = 351,	/* U+24CA: CIRCLED LATIN CAPITAL LETTER U */
	[2944+0x0B] = 352,	/* U+24CB: CIRCLED LATIN CAPITAL LETTER V */
	[2944+0x0C] = 353,	/* U+24CC: CIRCLED LATIN CAPITAL LETTER W */
	[2944+0x0D] = 354,	/* U+24CD: CIRCLED LATIN CAPITAL LETTER X */
	[2944+0x0E] = 355,	/* U+24CE: CIRCLED LATIN CAPITAL LETTER Y */
	[2944+0x0F] = 356,	/* U+24CF: CIRCLED LATIN CAPITAL LETTER Z */
	[2944+0x10] = 307,	/* U+24D0: CIRCLED LATIN SMALL LETTER A */
	[2944+0x11] = 308,	/* U+24D1: CIRCLED LATIN SMALL LETTER B */
	[2944+0x12] = 309,	/* U+24D2: CIRCLED LATIN SMALL LETTER C */
	[2944+0x13] = 310,	/* U+24D3: CIRCLED LATIN SMALL LETTER D */
	[2944+0x14] = 311,	/* U+24D4: CIRCLED LATIN SMALL LETTER E */
	[2944+0x15] = 312,	/* U+24D5: CIRCLED LATIN SMALL LETTER F */
	[2944+0x16] = 313,	/* U+24D6: CIRCLED LATIN SMALL LETTER G */
	[2944+0x17] = 314,	/* U+24D7: CIRCLED LATIN SMALL LETTER H */
	[2944+0x18] = 315,	/* U+24D8: CIRCLED LATIN SMALL LETTER I */
	[2944+0x19] = 316,	/* U+24D9: CIRCLED LATIN SMALL LETTER J */
	[2944+0x1A] = 317,	/* U+24DA: CIRCLED LATIN SMALL LETTER K */
	[2944+0x1B] = 318,	/* U+24DB: CIRCLED LATIN SMALL LETTER L */
	[2944+0x1C] = 319,	/* U+24DC: CIRCLED LATIN SMALL LETTER M */
	[2944+0x1D] = 320,	/* U+24DD: CIRCLED LATIN SMALL LETTER N */
	[2944+0x1E] = 321,	/* U+24DE: CIRCLED LATIN SMALL LETTER O */
	[2944+0x1F] = 322,	/* U+24DF: CIRCLED LATIN SMALL LETTER P */
	[2944+0x20] = 323,	/* U+24E0: CIRCLED LATIN SMALL LETTER Q */
	[2944+0x21] = 324,	/* U+24E1: CIRCLED LATIN SMALL LETTER R */
	[2944+0x22] = 325,	/* U+24E2: CIRCLED LATIN SMALL LETTER S */
	[2944+0x23] = 326,	/* U+24E3: CIRCLED LATIN SMALL LETTER T */
	[2944+0x24] = 327,	/* U+24E4: CIRCLED LATIN SMALL LETTER U */
	[2944+0x25] = 328,	/* U+24E5: CIRCLED LATIN SMALL LETTER V */
	[2944+0x26] = 329,	/* U+24E6: CIRCLED LATIN SMALL LETTER W */
	[2944+0x27] = 330,	/* U+24E7: CIRCLED LATIN SMALL LETTER X */
	[2944+0x28] = 331,	/* U+24E8: CIRCLED LATIN SMALL LETTER Y */
	[2944+0x29] = 332,	/* U+24E9: CIRCLED LATIN SMALL LETTER Z */
	[2944+0x2A] = 357,	/* U+24EA: CIRCLED DIGIT ZERO */
	[1920+0x14] = 3008 - 0x80,	/* 342 224 ... */
	[3008+0x00] = 10,	/* U+2500: BOX DRAWINGS LIGHT HORIZONTAL */
	[3008+0x02] = 6,	/* U+2502: BOX DRAWINGS LIGHT VERTICAL */
	[3008+0x0C] = 116,	/* U+250C: BOX DRAWINGS LIGHT DOWN AND RIGHT */
	[3008+0x10] = 116,	/* U+2510: BOX DRAWINGS LIGHT DOWN AND LEFT */
	[3008+0x14] = 116,	/* U+2514: BOX DRAWINGS LIGHT UP AND RIGHT */
	[3008+0x18] = 116,	/* U+2518: BOX DRAWINGS LIGHT UP AND LEFT */
	[3008+0x1C] = 116,	/* U+251C: BOX DRAWINGS LIGHT VERTICAL AND RIGHT */
	[3008+0x24] = 116,	/* U+2524: BOX DRAWINGS LIGHT VERTICAL AND LEFT */
	[3008+0x2C] = 116,	/* U+252C: BOX DRAWINGS LIGHT DOWN AND HORIZONTAL */
	[3008+0x34] = 116,	/* U+2534: BOX DRAWINGS LIGHT UP AND HORIZONTAL */
	[3008+0x3C] = 116,	/* U+253C: BOX DRAWINGS LIGHT VERTICAL AND HORIZONTAL */
	[1920+0x15] = 3072 - 0x80,	/* 342 225 ... */
	[3072+0x31] = 43,	/* U+2571: BOX DRAWINGS LIGHT DIAGONAL UPPER RIGHT TO LOWER LEFT */
	[3072+0x32] = 213,	/* U+2572: BOX DRAWINGS LIGHT DIAGONAL UPPER LEFT TO LOWER RIGHT */
	[1920+0x17] = 3136 - 0x80,	/* 342 227 ... */
	[3136+0x26] = 20,	/* U+25E6: WHITE BULLET */
	[1920+0x18] = 3200 - 0x80,	/* 342 230 ... */
	[3200+0x3A] = 358,	/* U+263A: WHITE SMILING FACE */
	[3200+0x3B] = 358,	/* U+263B: BLACK SMILING FACE */
	[1920+0x1F] = 3264 - 0x80,	/* 342 237 ... */
	[3264+0x0B] = 43,	/* U+27CB: MATHEMATICAL RISING DIAGONAL */
	[3264+0x0D] = 213,	/* U+27CD: MATHEMATICAL FALLING DIAGONAL */
	[3264+0x26] = 359,	/* U+27E6: MATHEMATICAL LEFT WHITE SQUARE BRACKET */
	[3264+0x27] = 360,	/* U+27E7: MATHEMATICAL RIGHT WHITE SQUARE BRACKET */
	[3264+0x28] = 121,	/* U+27E8: MATHEMATICAL LEFT ANGLE BRACKET */
	[3264+0x29] = 122,	/* U+27E9: MATHEMATICAL RIGHT ANGLE BRACKET */
	[3264+0x2A] = 9,	/* U+27EA: MATHEMATICAL LEFT DOUBLE ANGLE BRACKET */
	[3264+0x2B] = 21,	/* U+27EB: MATHEMATICAL RIGHT DOUBLE ANGLE BRACKET */
	[3264+0x2C] = 361,	/* U+27EC: MATHEMATICAL LEFT WHITE TORTOISE SHELL BRACKET */
	[3264+0x2D] = 362,	/* U+27ED: MATHEMATICAL RIGHT WHITE TORTOISE SHELL BRACKET */
	[3264+0x2E] = 136,	/* U+27EE: MATHEMATICAL LEFT FLATTENED PARENTHESIS */
	[3264+0x2F] = 137,	/* U+27EF: MATHEMATICAL RIGHT FLATTENED PARENTHESIS */
	[1920+0x26] = 3328 - 0x80,	/* 342 246 ... */
	[3328+0x00] = 363,	/* U+2980: TRIPLE VERTICAL BAR DELIMITER */
	[3328+0x03] = 364,	/* U+2983: LEFT WHITE CURLY BRACKET */
	[3328+0x04] = 365,	/* U+2984: RIGHT WHITE CURLY BRACKET */
	[3328+0x05] = 361,	/* U+2985: LEFT WHITE PARENTHESIS */
	[3328+0x06] = 362,	/* U+2986: RIGHT WHITE PARENTHESIS */
	[3328+0x07] = 366,	/* U+2987: Z NOTATION LEFT IMAGE BRACKET */
	[3328+0x08] = 367,	/* U+2988: Z NOTATION RIGHT IMAGE BRACKET */
	[3328+0x09] = 368,	/* U+2989: Z NOTATION LEFT BINDING BRACKET */
	[3328+0x0A] = 369,	/* U+298A: Z NOTATION RIGHT BINDING BRACKET */
	[1920+0x27] = 3392 - 0x80,	/* 342 247 ... */
	[3392+0x23] = 370,	/* U+29E3: EQUALS SIGN AND SLANTED PARALLEL */
	[3392+0x25] = 370,	/* U+29E5: IDENTICAL TO AND SLANTED PARALLEL */
	[3392+0x35] = 213,	/* U+29F5: REVERSE SOLIDUS OPERATOR */
	[3392+0x38] = 43,	/* U+29F8: BIG SOLIDUS */
	[3392+0x39] = 213,	/* U+29F9: BIG REVERSE SOLIDUS */
	[3392+0x3C] = 121,	/* U+29FC: LEFT-POINTING CURVED ANGLE BRACKET */
	[3392+0x3D] = 122,	/* U+29FD: RIGHT-POINTING CURVED ANGLE BRACKET */
	[3392+0x3E] = 116,	/* U+29FE: TINY */
	[3392+0x3F] = 10,	/* U+29FF: MINY */
	[1920+0x29] = 3456 - 0x80,	/* 342 251 ... */
	[3456+0x34] = 371,	/* U+2A74: DOUBLE COLON EQUAL */
	[3456+0x35] = 372,	/* U+2A75: TWO CONSECUTIVE EQUALS SIGNS */
	[3456+0x36] = 373,	/* U+2A76: THREE CONSECUTIVE EQUALS SIGNS */
	[1920+0x31] = 3520 - 0x80,	/* 342 261 ... */
	[3520+0x3C] = 53,	/* U+2C7C: LATIN SUBSCRIPT SMALL LETTER J */
	[3520+0x3D] = 81,	/* U+2C7D: MODIFIER LETTER CAPITAL V */
	[0xE3] = 3584 - 0x80,	/* 343 ... */
	[3584+0x00] = 3648 - 0x80,	/* 343 200 ... */
	[3648+0x00] = 1,	/* U+3000: IDEOGRAPHIC SPACE */
	[3648+0x08] = 121,	/* U+3008: LEFT ANGLE BRACKET */
	[3648+0x09] = 122,	/* U+3009: RIGHT ANGLE BRACKET */
	[3584+0x02] = 3712 - 0x80,	/* 343 202 ... */
	[3712+0x20] = 135,	/* U+30A0: KATAKANA-HIRAGANA DOUBLE HYPHEN */
	[3584+0x09] = 3776 - 0x80,	/* 343 211 ... */
	[3776+0x10] = 374,	/* U+3250: PARTNERSHIP SIGN */
	[3776+0x11] = 375,	/* U+3251: CIRCLED NUMBER TWENTY ONE */
	[3776+0x12] = 376,	/* U+3252: CIRCLED NUMBER TWENTY TWO */
	[3776+0x13] = 377,	/* U+3253: CIRCLED NUMBER TWENTY THREE */
	[3776+0x14] = 378,	/* U+3254: CIRCLED NUMBER TWENTY FOUR */
	[3776+0x15] = 379,	/* U+3255: CIRCLED NUMBER TWENTY FIVE */
	[3776+0x16] = 380,	/* U+3256: CIRCLED NUMBER TWENTY SIX */
	[3776+0x17] = 381,	/* U+3257: CIRCLED NUMBER TWENTY SEVEN */
	[3776+0x18] = 382,	/* U+3258: CIRCLED NUMBER TWENTY EIGHT */
	[3776+0x19] = 383,	/* U+3259: CIRCLED NUMBER TWENTY NINE */
	[3776+0x1A] = 384,	/* U+325A: CIRCLED NUMBER THIRTY */
	[3776+0x1B] = 385,	/* U+325B: CIRCLED NUMBER THIRTY ONE */
	[3776+0x1C] = 386,	/* U+325C: CIRCLED NUMBER THIRTY TWO */
	[3776+0x1D] = 387,	/* U+325D: CIRCLED NUMBER THIRTY THREE */
	[3776+0x1E] = 388,	/* U+325E: CIRCLED NUMBER THIRTY FOUR */
	[3776+0x1F] = 389,	/* U+325F: CIRCLED NUMBER THIRTY FIVE */
	[3584+0x0A] = 3840 - 0x80,	/* 343 212 ... */
	[3840+0x31] = 390,	/* U+32B1: CIRCLED NUMBER THIRTY SIX */
	[3840+0x32] = 391,	/* U+32B2: CIRCLED NUMBER THIRTY SEVEN */
	[3840+0x33] = 392,	/* U+32B3: CIRCLED NUMBER THIRTY EIGHT */
	[3840+0x34] = 393,	/* U+32B4: CIRCLED NUMBER THIRTY NINE */
	[3840+0x35] = 394,	/* U+32B5: CIRCLED NUMBER FORTY */
	[3840+0x36] = 395,	/* U+32B6: CIRCLED NUMBER FORTY ONE */
	[3840+0x37] = 396,	/* U+32B7: CIRCLED NUMBER FORTY TWO */
	[3840+0x38] = 397,	/* U+32B8: CIRCLED NUMBER FORTY THREE */
	[3840+0x39] = 398,	/* U+32B9: CIRCLED NUMBER FORTY FOUR */
	[3840+0x3A] = 399,	/* U+32BA: CIRCLED NUMBER FORTY FIVE */
	[3840+0x3B] = 400,	/* U+32BB: CIRCLED NUMBER FORTY SIX */
	[3840+0x3C] = 401,	/* U+32BC: CIRCLED NUMBER FORTY SEVEN */
	[3840+0x3D] = 402,	/* U+32BD: CIRCLED NUMBER FORTY EIGHT */
	[3840+0x3E] = 403,	/* U+32BE: CIRCLED NUMBER FORTY NINE */
	[3840+0x3F] = 404,	/* U+32BF: CIRCLED NUMBER FIFTY */
	[3584+0x0B] = 3904 - 0x80,	/* 343 213 ... */
	[3904+0x0C] = 405,	/* U+32CC: SQUARE HG */
	[3904+0x0D] = 406,	/* U+32CD: SQUARE ERG */
	[3904+0x0E] = 407,	/* U+32CE: SQUARE EV */
	[3904+0x0F] = 408,	/* U+32CF: LIMITED LIABILITY SIGN */
	[3584+0x0D] = 3968 - 0x80,	/* 343 215 ... */
	[3968+0x31] = 409,	/* U+3371: SQUARE HPA */
	[3968+0x32] = 410,	/* U+3372: SQUARE DA */
	[3968+0x33] = 411,	/* U+3373: SQUARE AU */
	[3968+0x34] = 412,	/* U+3374: SQUARE BAR */
	[3968+0x35] = 413,	/* U+3375: SQUARE OV */
	[3968+0x36] = 414,	/* U+3376: SQUARE PC */
	[3968+0x37] = 415,	/* U+3377: SQUARE DM */
	[3968+0x38] = 416,	/* U+3378: SQUARE DM SQUARED */
	[3968+0x39] = 417,	/* U+3379: SQUARE DM CUBED */
	[3968+0x3A] = 418,	/* U+337A: SQUARE IU */
	[3584+0x0E] = 4032 - 0x80,	/* 343 216 ... */
	[4032+0x00] = 419,	/* U+3380: SQUARE PA AMPS */
	[4032+0x01] = 420,	/* U+3381: SQUARE NA */
	[4032+0x02] = 421,	/* U+3382: SQUARE MU A */
	[4032+0x03] = 422,	/* U+3383: SQUARE MA */
	[4032+0x04] = 423,	/* U+3384: SQUARE KA */
	[4032+0x05] = 424,	/* U+3385: SQUARE KB */
	[4032+0x06] = 425,	/* U+3386: SQUARE MB */
	[4032+0x07] = 426,	/* U+3387: SQUARE GB */
	[4032+0x08] = 427,	/* U+3388: SQUARE CAL */
	[4032+0x09] = 428,	/* U+3389: SQUARE KCAL */
	[4032+0x0A] = 429,	/* U+338A: SQUARE PF */
	[4032+0x0B] = 430,	/* U+338B: SQUARE NF */
	[4032+0x0C] = 431,	/* U+338C: SQUARE MU F */
	[4032+0x0D] = 432,	/* U+338D: SQUARE MU G */
	[4032+0x0E] = 433,	/* U+338E: SQUARE MG */
	[4032+0x0F] = 434,	/* U+338F: SQUARE KG */
	[4032+0x10] = 435,	/* U+3390: SQUARE HZ */
	[4032+0x11] = 436,	/* U+3391: SQUARE KHZ */
	[4032+0x12] = 437,	/* U+3392: SQUARE MHZ */
	[4032+0x13] = 438,	/* U+3393: SQUARE GHZ */
	[4032+0x14] = 439,	/* U+3394: SQUARE THZ */
	[4032+0x15] = 440,	/* U+3395: SQUARE MU L */
	[4032+0x16] = 441,	/* U+3396: SQUARE ML */
	[4032+0x17] = 442,	/* U+3397: SQUARE DL */
	[4032+0x18] = 443,	/* U+3398: SQUARE KL */
	[4032+0x19] = 444,	/* U+3399: SQUARE FM */
	[4032+0x1A] = 445,	/* U+339A: SQUARE NM */
	[4032+0x1B] = 446,	/* U+339B: SQUARE MU M */
	[4032+0x1C] = 447,	/* U+339C: SQUARE MM */
	[4032+0x1D] = 448,	/* U+339D: SQUARE CM */
	[4032+0x1E] = 449,	/* U+339E: SQUARE KM */
	[4032+0x1F] = 450,	/* U+339F: SQUARE MM SQUARED */
	[4032+0x20] = 451,	/* U+33A0: SQUARE CM SQUARED */
	[4032+0x21] = 452,	/* U+33A1: SQUARE M SQUARED */
	[4032+0x22] = 453,	/* U+33A2: SQUARE KM SQUARED */
	[4032+0x23] = 454,	/* U+33A3: SQUARE MM CUBED */
	[4032+0x24] = 455,	/* U+33A4: SQUARE CM CUBED */
	[4032+0x25] = 456,	/* U+33A5: SQUARE M CUBED */
	[4032+0x26] = 457,	/* U+33A6: SQUARE KM CUBED */
	[4032+0x27] = 458,	/* U+33A7: SQUARE M OVER S */
	[4032+0x28] = 459,	/* U+33A8: SQUARE M OVER S SQUARED */
	[4032+0x29] = 460,	/* U+33A9: SQUARE PA */
	[4032+0x2A] = 461,	/* U+33AA: SQUARE KPA */
	[4032+0x2B] = 462,	/* U+33AB: SQUARE MPA */
	[4032+0x2C] = 463,	/* U+33AC: SQUARE GPA */
	[4032+0x2D] = 464,	/* U+33AD: SQUARE RAD */
	[4032+0x2E] = 465,	/* U+33AE: SQUARE RAD OVER S */
	[4032+0x2F] = 466,	/* U+33AF: SQUARE RAD OVER S SQUARED */
	[4032+0x30] = 467,	/* U+33B0: SQUARE PS */
	[4032+0x31] = 468,	/* U+33B1: SQUARE NS */
	[4032+0x32] = 469,	/* U+33B2: SQUARE MU S */
	[4032+0x33] = 470,	/* U+33B3: SQUARE MS */
	[4032+0x34] = 471,	/* U+33B4: SQUARE PV */
	[4032+0x35] = 472,	/* U+33B5: SQUARE NV */
	[4032+0x36] = 473,	/* U+33B6: SQUARE MU V */
	[4032+0x37] = 474,	/* U+33B7: SQUARE MV */
	[4032+0x38] = 475,	/* U+33B8: SQUARE KV */
	[4032+0x39] = 476,	/* U+33B9: SQUARE MV MEGA */
	[4032+0x3A] = 477,	/* U+33BA: SQUARE PW */
	[4032+0x3B] = 478,	/* U+33BB: SQUARE NW */
	[4032+0x3C] = 479,	/* U+33BC: SQUARE MU W */
	[4032+0x3D] = 480,	/* U+33BD: SQUARE MW */
	[4032+0x3E] = 481,	/* U+33BE: SQUARE KW */
	[4032+0x3F] = 482,	/* U+33BF: SQUARE MW MEGA */
	[3584+0x0F] = 4096 - 0x80,	/* 343 217 ... */
	[4096+0x02] = 483,	/* U+33C2: SQUARE AM */
	[4096+0x03] = 484,	/* U+33C3: SQUARE BQ */
	[4096+0x04] = 485,	/* U+33C4: SQUARE CC */
	[4096+0x05] = 486,	/* U+33C5: SQUARE CD */
	[4096+0x06] = 487,	/* U+33C6: SQUARE C OVER KG */
	[4096+0x07] = 488,	/* U+33C7: SQUARE CO */
	[4096+0x08] = 489,	/* U+33C8: SQUARE DB */
	[4096+0x09] = 490,	/* U+33C9: SQUARE GY */
	[4096+0x0A] = 491,	/* U+33CA: SQUARE HA */
	[4096+0x0B] = 492,	/* U+33CB: SQUARE HP */
	[4096+0x0C] = 493,	/* U+33CC: SQUARE IN */
	[4096+0x0D] = 494,	/* U+33CD: SQUARE KK */
	[4096+0x0E] = 495,	/* U+33CE: SQUARE KM CAPITAL */
	[4096+0x0F] = 496,	/* U+33CF: SQUARE KT */
	[4096+0x10] = 497,	/* U+33D0: SQUARE LM */
	[4096+0x11] = 498,	/* U+33D1: SQUARE LN */
	[4096+0x12] = 499,	/* U+33D2: SQUARE LOG */
	[4096+0x13] = 500,	/* U+33D3: SQUARE LX */
	[4096+0x14] = 501,	/* U+33D4: SQUARE MB SMALL */
	[4096+0x15] = 502,	/* U+33D5: SQUARE MIL */
	[4096+0x16] = 503,	/* U+33D6: SQUARE MOL */
	[4096+0x17] = 504,	/* U+33D7: SQUARE PH */
	[4096+0x18] = 505,	/* U+33D8: SQUARE PM */
	[4096+0x19] = 506,	/* U+33D9: SQUARE PPM */
	[4096+0x1A] = 507,	/* U+33DA: SQUARE PR */
	[4096+0x1B] = 508,	/* U+33DB: SQUARE SR */
	[4096+0x1C] = 509,	/* U+33DC: SQUARE SV */
	[4096+0x1D] = 510,	/* U+33DD: SQUARE WB */
	[4096+0x1E] = 511,	/* U+33DE: SQUARE V OVER M */
	[4096+0x1F] = 512,	/* U+33DF: SQUARE A OVER M */
	[4096+0x3F] = 513,	/* U+33FF: SQUARE GAL */
	[0xEA] = 4160 - 0x80,	/* 352 ... */
	[4160+0x1F] = 4224 - 0x80,	/* 352 237 ... */
	[4224+0x32] = 27,	/* U+A7F2: MODIFIER LETTER CAPITAL C */
	[4224+0x33] = 74,	/* U+A7F3: MODIFIER LETTER CAPITAL F */
	[4224+0x34] = 162,	/* U+A7F4: MODIFIER LETTER CAPITAL Q */
	[0xEF] = 4288 - 0x80,	/* 357 ... */
	[4288+0x2C] = 4352 - 0x80,	/* 357 254 ... */
	[4352+0x00] = 514,	/* U+FB00: LATIN SMALL LIGATURE FF */
	[4352+0x01] = 515,	/* U+FB01: LATIN SMALL LIGATURE FI */
	[4352+0x02] = 516,	/* U+FB02: LATIN SMALL LIGATURE FL */
	[4352+0x03] = 517,	/* U+FB03: LATIN SMALL LIGATURE FFI */
	[4352+0x04] = 518,	/* U+FB04: LATIN SMALL LIGATURE FFL */
	[4352+0x05] = 519,	/* U+FB05: LATIN SMALL LIGATURE LONG S T */
	[4352+0x06] = 519,	/* U+FB06: LATIN SMALL LIGATURE ST */
	[4352+0x29] = 116,	/* U+FB29: HEBREW LETTER ALTERNATIVE PLUS SIGN */
	[4288+0x38] = 4416 - 0x80,	/* 357 270 ... */
	[4416+0x00] = 104,	/* U+FE00: VARIATION SELECTOR-1 */
	[4416+0x01] = 104,	/* U+FE01: VARIATION SELECTOR-2 */
	[4416+0x02] = 104,	/* U+FE02: VARIATION SELECTOR-3 */
	[4416+0x03] = 104,	/* U+FE03: VARIATION SELECTOR-4 */
	[4416+0x04] = 104,	/* U+FE04: VARIATION SELECTOR-5 */
	[4416+0x05] = 104,	/* U+FE05: VARIATION SELECTOR-6 */
	[4416+0x06] = 104,	/* U+FE06: VARIATION SELECTOR-7 */
	[4416+0x07] = 104,	/* U+FE07: VARIATION SELECTOR-8 */
	[4416+0x08] = 104,	/* U+FE08: VARIATION SELECTOR-9 */
	[4416+0x09] = 104,	/* U+FE09: VARIATION SELECTOR-10 */
	[4416+0x0A] = 104,	/* U+FE0A: VARIATION SELECTOR-11 */
	[4416+0x0B] = 104,	/* U+FE0B: VARIATION SELECTOR-12 */
	[4416+0x0C] = 104,	/* U+FE0C: VARIATION SELECTOR-13 */
	[4416+0x0D] = 104,	/* U+FE0D: VARIATION SELECTOR-14 */
	[4416+0x0E] = 104,	/* U+FE0E: VARIATION SELECTOR-15 */
	[4416+0x0F] = 104,	/* U+FE0F: VARIATION SELECTOR-16 */
	[4416+0x10] = 18,	/* U+FE10: PRESENTATION FORM FOR VERTICAL COMMA */
	[4416+0x13] = 101,	/* U+FE13: PRESENTATION FORM FOR VERTICAL COLON */
	[4416+0x14] = 105,	/* U+FE14: PRESENTATION FORM FOR VERTICAL SEMICOLON */
	[4416+0x15] = 2,	/* U+FE15: PRESENTATION FORM FOR VERTICAL EXCLAMATION MARK */
	[4416+0x19] = 118,	/* U+FE19: PRESENTATION FORM FOR VERTICAL HORIZONTAL ELLIPSIS */
	[4416+0x20] = 104,	/* U+FE20: COMBINING LIGATURE LEFT HALF */
	[4416+0x21] = 104,	/* U+FE21: COMBINING LIGATURE RIGHT HALF */
	[4416+0x22] = 104,	/* U+FE22: COMBINING DOUBLE TILDE LEFT HALF */
	[4416+0x23] = 104,	/* U+FE23: COMBINING DOUBLE TILDE RIGHT HALF */
	[4416+0x24] = 104,	/* U+FE24: COMBINING MACRON LEFT HALF */
	[4416+0x25] = 104,	/* U+FE25: COMBINING MACRON RIGHT HALF */
	[4416+0x26] = 104,	/* U+FE26: COMBINING CONJOINING MACRON */
	[4416+0x27] = 104,	/* U+FE27: COMBINING LIGATURE LEFT HALF BELOW */
	[4416+0x28] = 104,	/* U+FE28: COMBINING LIGATURE RIGHT HALF BELOW */
	[4416+0x29] = 104,	/* U+FE29: COMBINING TILDE LEFT HALF BELOW */
	[4416+0x2A] = 104,	/* U+FE2A: COMBINING TILDE RIGHT HALF BELOW */
	[4416+0x2B] = 104,	/* U+FE2B: COMBINING MACRON LEFT HALF BELOW */
	[4416+0x2C] = 104,	/* U+FE2C: COMBINING MACRON RIGHT HALF BELOW */
	[4416+0x2D] = 104,	/* U+FE2D: COMBINING CONJOINING MACRON BELOW */
	[4416+0x30] = 117,	/* U+FE30: PRESENTATION FORM FOR VERTICAL TWO DOT LEADER */
	[4416+0x33] = 100,	/* U+FE33: PRESENTATION FORM FOR VERTICAL LOW LINE */
	[4416+0x34] = 100,	/* U+FE34: PRESENTATION FORM FOR VERTICAL WAVY LOW LINE */
	[4416+0x35] = 136,	/* U+FE35: PRESENTATION FORM FOR VERTICAL LEFT PARENTHESIS */
	[4416+0x36] = 137,	/* U+FE36: PRESENTATION FORM FOR VERTICAL RIGHT PARENTHESIS */
	[4416+0x37] = 520,	/* U+FE37: PRESENTATION FORM FOR VERTICAL LEFT CURLY BRACKET */
	[4416+0x38] = 521,	/* U+FE38: PRESENTATION FORM FOR VERTICAL RIGHT CURLY BRACKET */
	[4288+0x39] = 4480 - 0x80,	/* 357 271 ... */
	[4480+0x07] = 522,	/* U+FE47: PRESENTATION FORM FOR VERTICAL LEFT SQUARE BRACKET */
	[4480+0x08] = 523,	/* U+FE48: PRESENTATION FORM FOR VERTICAL RIGHT SQUARE BRACKET */
	[4480+0x0D] = 100,	/* U+FE4D: DASHED LOW LINE */
	[4480+0x0E] = 100,	/* U+FE4E: CENTRELINE LOW LINE */
	[4480+0x0F] = 100,	/* U+FE4F: WAVY LOW LINE */
	[4480+0x10] = 18,	/* U+FE50: SMALL COMMA */
	[4480+0x12] = 17,	/* U+FE52: SMALL FULL STOP */
	[4480+0x14] = 105,	/* U+FE54: SMALL SEMICOLON */
	[4480+0x15] = 101,	/* U+FE55: SMALL COLON */
	[4480+0x17] = 2,	/* U+FE57: SMALL EXCLAMATION MARK */
	[4480+0x19] = 136,	/* U+FE59: SMALL LEFT PARENTHESIS */
	[4480+0x1A] = 137,	/* U+FE5A: SMALL RIGHT PARENTHESIS */
	[4480+0x1B] = 520,	/* U+FE5B: SMALL LEFT CURLY BRACKET */
	[4480+0x1C] = 521,	/* U+FE5C: SMALL RIGHT CURLY BRACKET */
	[4480+0x1F] = 370,	/* U+FE5F: SMALL NUMBER SIGN */
	[4480+0x20] = 127,	/* U+FE60: SMALL AMPERSAND */
	[4480+0x21] = 214,	/* U+FE61: SMALL ASTERISK */
	[4480+0x22] = 116,	/* U+FE62: SMALL PLUS SIGN */
	[4480+0x23] = 10,	/* U+FE63: SMALL HYPHEN-MINUS */
	[4480+0x24] = 121,	/* U+FE64: SMALL LESS-THAN SIGN */
	[4480+0x25] = 122,	/* U+FE65: SMALL GREATER-THAN SIGN */
	[4480+0x26] = 135,	/* U+FE66: SMALL EQUALS SIGN */
	[4480+0x28] = 213,	/* U+FE68: SMALL REVERSE SOLIDUS */
	[4480+0x29] = 524,	/* U+FE69: SMALL DOLLAR SIGN */
	[4480+0x2A] = 525,	/* U+FE6A: SMALL PERCENT SIGN */
	[4480+0x2B] = 526,	/* U+FE6B: SMALL COMMERCIAL AT */
	[4288+0x3B] = 4544 - 0x80,	/* 357 273 ... */
	[4544+0x3F] = 104,	/* U+FEFF: ZERO WIDTH NO-BREAK SPACE */
	[4288+0x3C] = 4608 - 0x80,	/* 357 274 ... */
	[4608+0x01] = 2,	/* U+FF01: FULLWIDTH EXCLAMATION MARK */
	[4608+0x02] = 114,	/* U+FF02: FULLWIDTH QUOTATION MARK */
	[4608+0x03] = 370,	/* U+FF03: FULLWIDTH NUMBER SIGN */
	[4608+0x04] = 524,	/* U+FF04: FULLWIDTH DOLLAR SIGN */
	[4608+0x05] = 525,	/* U+FF05: FULLWIDTH PERCENT SIGN */
	[4608+0x06] = 127,	/* U+FF06: FULLWIDTH AMPERSAND */
	[4608+0x07] = 15,	/* U+FF07: FULLWIDTH APOSTROPHE */
	[4608+0x08] = 136,	/* U+FF08: FULLWIDTH LEFT PARENTHESIS */
	[4608+0x09] = 137,	/* U+FF09: FULLWIDTH RIGHT PARENTHESIS */
	[4608+0x0A] = 214,	/* U+FF0A: FULLWIDTH ASTERISK */
	[4608+0x0B] = 116,	/* U+FF0B: FULLWIDTH PLUS SIGN */
	[4608+0x0C] = 18,	/* U+FF0C: FULLWIDTH COMMA */
	[4608+0x0D] = 10,	/* U+FF0D: FULLWIDTH HYPHEN-MINUS */
	[4608+0x0E] = 17,	/* U+FF0E: FULLWIDTH FULL STOP */
	[4608+0x0F] = 43,	/* U+FF0F: FULLWIDTH SOLIDUS */
	[4608+0x10] = 128,	/* U+FF10: FULLWIDTH DIGIT ZERO */
	[4608+0x11] = 19,	/* U+FF11: FULLWIDTH DIGIT ONE */
	[4608+0x12] = 13,	/* U+FF12: FULLWIDTH DIGIT TWO */
	[4608+0x13] = 14,	/* U+FF13: FULLWIDTH DIGIT THREE */
	[4608+0x14] = 129,	/* U+FF14: FULLWIDTH DIGIT FOUR */
	[4608+0x15] = 130,	/* U+FF15: FULLWIDTH DIGIT FIVE */
	[4608+0x16] = 131,	/* U+FF16: FULLWIDTH DIGIT SIX */
	[4608+0x17] = 132,	/* U+FF17: FULLWIDTH DIGIT SEVEN */
	[4608+0x18] = 133,	/* U+FF18: FULLWIDTH DIGIT EIGHT */
	[4608+0x19] = 134,	/* U+FF19: FULLWIDTH DIGIT NINE */
	[4608+0x1A] = 101,	/* U+FF1A: FULLWIDTH COLON */
	[4608+0x1B] = 105,	/* U+FF1B: FULLWIDTH SEMICOLON */
	[4608+0x1C] = 121,	/* U+FF1C: FULLWIDTH LESS-THAN SIGN */
	[4608+0x1D] = 135,	/* U+FF1D: FULLWIDTH EQUALS SIGN */
	[4608+0x1E] = 122,	/* U+FF1E: FULLWIDTH GREATER-THAN SIGN */
	[4608+0x20] = 526,	/* U+FF20: FULLWIDTH COMMERCIAL AT */
	[4608+0x21] = 25,	/* U+FF21: FULLWIDTH LATIN CAPITAL LETTER A */
	[4608+0x22] = 73,	/* U+FF22: FULLWIDTH LATIN CAPITAL LETTER B */
	[4608+0x23] = 27,	/* U+FF23: FULLWIDTH LATIN CAPITAL LETTER C */
	[4608+0x24] = 30,	/* U+FF24: FULLWIDTH LATIN CAPITAL LETTER D */
	[4608+0x25] = 28,	/* U+FF25: FULLWIDTH LATIN CAPITAL LETTER E */
	[4608+0x26] = 74,	/* U+FF26: FULLWIDTH LATIN CAPITAL LETTER F */
	[4608+0x27] = 46,	/* U+FF27: FULLWIDTH LATIN CAPITAL LETTER G */
	[4608+0x28] = 48,	/* U+FF28: FULLWIDTH LATIN CAPITAL LETTER H */
	[4608+0x29] = 29,	/* U+FF29: FULLWIDTH LATIN CAPITAL LETTER I */
	[4608+0x2A] = 52,	/* U+FF2A: FULLWIDTH LATIN CAPITAL LETTER J */
	[4608+0x2B] = 54,	/* U+FF2B: FULLWIDTH LATIN CAPITAL LETTER K */
	[4608+0x2C] = 57,	/* U+FF2C: FULLWIDTH LATIN CAPITAL LETTER L */
	[4608+0x2D] = 107,	/* U+FF2D: FULLWIDTH LATIN CAPITAL LETTER M */
	[4608+0x2E] = 31,	/* U+FF2E: FULLWIDTH LATIN CAPITAL LETTER N */
	[4608+0x2F] = 32,	/* U+FF2F: FULLWIDTH LATIN CAPITAL LETTER O */
	[4608+0x30] = 79,	/* U+FF30: FULLWIDTH LATIN CAPITAL LETTER P */
	[4608+0x31] = 162,	/* U+FF31: FULLWIDTH LATIN CAPITAL LETTER Q */
	[4608+0x32] = 62,	/* U+FF32: FULLWIDTH LATIN CAPITAL LETTER R */
	[4608+0x33] = 64,	/* U+FF33: FULLWIDTH LATIN CAPITAL LETTER S */
	[4608+0x34] = 66,	/* U+FF34: FULLWIDTH LATIN CAPITAL LETTER T */
	[4608+0x35] = 34,	/* U+FF35: FULLWIDTH LATIN CAPITAL LETTER U */
	[4608+0x36] = 81,	/* U+FF36: FULLWIDTH LATIN CAPITAL LETTER V */
	[4608+0x37] = 68,	/* U+FF37: FULLWIDTH LATIN CAPITAL LETTER W */
	[4608+0x38] = 109,	/* U+FF38: FULLWIDTH LATIN CAPITAL LETTER X */
	[4608+0x39] = 35,	/* U+FF39: FULLWIDTH LATIN CAPITAL LETTER Y */
	[4608+0x3A] = 70,	/* U+FF3A: FULLWIDTH LATIN CAPITAL LETTER Z */
	[4608+0x3B] = 522,	/* U+FF3B: FULLWIDTH LEFT SQUARE BRACKET */
	[4608+0x3C] = 213,	/* U+FF3C: FULLWIDTH REVERSE SOLIDUS */
	[4608+0x3D] = 523,	/* U+FF3D: FULLWIDTH RIGHT SQUARE BRACKET */
	[4608+0x3E] = 98,	/* U+FF3E: FULLWIDTH CIRCUMFLEX ACCENT */
	[4608+0x3F] = 100,	/* U+FF3F: FULLWIDTH LOW LINE */
	[4288+0x3D] = 4672 - 0x80,	/* 357 275 ... */
	[4672+0x00] = 99,	/* U+FF40: FULLWIDTH GRAVE ACCENT */
	[4672+0x01] = 8,	/* U+FF41: FULLWIDTH LATIN SMALL LETTER A */
	[4672+0x02] = 72,	/* U+FF42: FULLWIDTH LATIN SMALL LETTER B */
	[4672+0x03] = 3,	/* U+FF43: FULLWIDTH LATIN SMALL LETTER C */
	[4672+0x04] = 41,	/* U+FF44: FULLWIDTH LATIN SMALL LETTER D */
	[4672+0x05] = 39,	/* U+FF45: FULLWIDTH LATIN SMALL LETTER E */
	[4672+0x06] = 75,	/* U+FF46: FULLWIDTH LATIN SMALL LETTER F */
	[4672+0x07] = 47,	/* U+FF47: FULLWIDTH LATIN SMALL LETTER G */
	[4672+0x08] = 49,	/* U+FF48: FULLWIDTH LATIN SMALL LETTER H */
	[4672+0x09] = 40,	/* U+FF49: FULLWIDTH LATIN SMALL LETTER I */
	[4672+0x0A] = 53,	/* U+FF4A: FULLWIDTH LATIN SMALL LETTER J */
	[4672+0x0B] = 55,	/* U+FF4B: FULLWIDTH LATIN SMALL LETTER K */
	[4672+0x0C] = 58,	/* U+FF4C: FULLWIDTH LATIN SMALL LETTER L */
	[4672+0x0D] = 93,	/* U+FF4D: FULLWIDTH LATIN SMALL LETTER M */
	[4672+0x0E] = 42,	/* U+FF4E: FULLWIDTH LATIN SMALL LETTER N */
	[4672+0x0F] = 20,	/* U+FF4F: FULLWIDTH LATIN SMALL LETTER O */
	[4672+0x10] = 80,	/* U+FF50: FULLWIDTH LATIN SMALL LETTER P */
	[4672+0x11] = 56,	/* U+FF51: FULLWIDTH LATIN SMALL LETTER Q */
	[4672+0x12] = 63,	/* U+FF52: FULLWIDTH LATIN SMALL LETTER R */
	[4672+0x13] = 65,	/* U+FF53: FULLWIDTH LATIN SMALL LETTER S */
	[4672+0x14] = 67,	/* U+FF54: FULLWIDTH LATIN SMALL LETTER T */
	[4672+0x15] = 16,	/* U+FF55: FULLWIDTH LATIN SMALL LETTER U */
	[4672+0x16] = 94,	/* U+FF56: FULLWIDTH LATIN SMALL LETTER V */
	[4672+0x17] = 69,	/* U+FF57: FULLWIDTH LATIN SMALL LETTER W */
	[4672+0x18] = 33,	/* U+FF58: FULLWIDTH LATIN SMALL LETTER X */
	[4672+0x19] = 44,	/* U+FF59: FULLWIDTH LATIN SMALL LETTER Y */
	[4672+0x1A] = 71,	/* U+FF5A: FULLWIDTH LATIN SMALL LETTER Z */
	[4672+0x1B] = 520,	/* U+FF5B: FULLWIDTH LEFT CURLY BRACKET */
	[4672+0x1C] = 6,	/* U+FF5C: FULLWIDTH VERTICAL LINE */
	[4672+0x1D] = 521,	/* U+FF5D: FULLWIDTH RIGHT CURLY BRACKET */
	[4672+0x1E] = 102,	/* U+FF5E: FULLWIDTH TILDE */
	[0xF0] = 4736 - 0x80,	/* 360 ... */
	[4736+0x10] = 4800 - 0x80,	/* 360 220 ... */
	[4800+0x1E] = 4864 - 0x80,	/* 360 220 236 ... */
	[4864+0x25] = 56,	/* U+107A5: MODIFIER LETTER SMALL Q */
	[4736+0x1D] = 4928 - 0x80,	/* 360 235 ... */
	[4928+0x10] = 4992 - 0x80,	/* 360 235 220 ... */
	[4992+0x00] = 25,	/* U+1D400: MATHEMATICAL BOLD CAPITAL A */
	[4992+0x01] = 73,	/* U+1D401: MATHEMATICAL BOLD CAPITAL B */
	[4992+0x02] = 27,	/* U+1D402: MATHEMATICAL BOLD CAPITAL C */
	[4992+0x03] = 30,	/* U+1D403: MATHEMATICAL BOLD CAPITAL D */
	[4992+0x04] = 28,	/* U+1D404: MATHEMATICAL BOLD CAPITAL E */
	[4992+0x05] = 74,	/* U+1D405: MATHEMATICAL BOLD CAPITAL F */
	[4992+0x06] = 46,	/* U+1D406: MATHEMATICAL BOLD CAPITAL G */
	[4992+0x07] = 48,	/* U+1D407: MATHEMATICAL BOLD CAPITAL H */
	[4992+0x08] = 29,	/* U+1D408: MATHEMATICAL BOLD CAPITAL I */
	[4992+0x09] = 52,	/* U+1D409: MATHEMATICAL BOLD CAPITAL J */
	[4992+0x0A] = 54,	/* U+1D40A: MATHEMATICAL BOLD CAPITAL K */
	[4992+0x0B] = 57,	/* U+1D40B: MATHEMATICAL BOLD CAPITAL L */
	[4992+0x0C] = 107,	/* U+1D40C: MATHEMATICAL BOLD CAPITAL M */
	[4992+0x0D] = 31,	/* U+1D40D: MATHEMATICAL BOLD CAPITAL N */
	[4992+0x0E] = 32,	/* U+1D40E: MATHEMATICAL BOLD CAPITAL O */
	[4992+0x0F] = 79,	/* U+1D40F: MATHEMATICAL BOLD CAPITAL P */
	[4992+0x10] = 162,	/* U+1D410: MATHEMATICAL BOLD CAPITAL Q */
	[4992+0x11] = 62,	/* U+1D411: MATHEMATICAL BOLD CAPITAL R */
	[4992+0x12] = 64,	/* U+1D412: MATHEMATICAL BOLD CAPITAL S */
	[4992+0x13] = 66,	/* U+1D413: MATHEMATICAL BOLD CAPITAL T */
	[4992+0x14] = 34,	/* U+1D414: MATHEMATICAL BOLD CAPITAL U */
	[4992+0x15] = 81,	/* U+1D415: MATHEMATICAL BOLD CAPITAL V */
	[4992+0x16] = 68,	/* U+1D416: MATHEMATICAL BOLD CAPITAL W */
	[4992+0x17] = 109,	/* U+1D417: MATHEMATICAL BOLD CAPITAL X */
	[4992+0x18] = 35,	/* U+1D418: MATHEMATICAL BOLD CAPITAL Y */
	[4992+0x19] = 70,	/* U+1D419: MATHEMATICAL BOLD CAPITAL Z */
	[4992+0x1A] = 8,	/* U+1D41A: MATHEMATICAL BOLD SMALL A */
	[4992+0x1B] = 72,	/* U+1D41B: MATHEMATICAL BOLD SMALL B */
	[4992+0x1C] = 3,	/* U+1D41C: MATHEMATICAL BOLD SMALL C */
	[4992+0x1D] = 41,	/* U+1D41D: MATHEMATICAL BOLD SMALL D */
	[4992+0x1E] = 39,	/* U+1D41E: MATHEMATICAL BOLD SMALL E */
	[4992+0x1F] = 75,	/* U+1D41F: MATHEMATICAL BOLD SMALL F */
	[4992+0x20] = 47,	/* U+1D420: MATHEMATICAL BOLD SMALL G */
	[4992+0x21] = 49,	/* U+1D421: MATHEMATICAL BOLD SMALL H */
	[4992+0x22] = 40,	/* U+1D422: MATHEMATICAL BOLD SMALL I */
	[4992+0x23] = 53,	/* U+1D423: MATHEMATICAL BOLD SMALL J */
	[4992+0x24] = 55,	/* U+1D424: MATHEMATICAL BOLD SMALL K */
	[4992+0x25] = 58,	/* U+1D425: MATHEMATICAL BOLD SMALL L */
	[4992+0x26] = 93,	/* U+1D426: MATHEMATICAL BOLD SMALL M */
	[4992+0x27] = 42,	/* U+1D427: MATHEMATICAL BOLD SMALL N */
	[4992+0x28] = 20,	/* U+1D428: MATHEMATICAL BOLD SMALL O */
	[4992+0x29] = 80,	/* U+1D429: MATHEMATICAL BOLD SMALL P */
	[4992+0x2A] = 56,	/* U+1D42A: MATHEMATICAL BOLD SMALL Q */
	[4992+0x2B] = 63,	/* U+1D42B: MATHEMATICAL BOLD SMALL R */
	[4992+0x2C] = 65,	/* U+1D42C: MATHEMATICAL BOLD SMALL S */
	[4992+0x2D] = 67,	/* U+1D42D: MATHEMATICAL BOLD SMALL T */
	[4992+0x2E] = 16,	/* U+1D42E: MATHEMATICAL BOLD SMALL U */
	[4992+0x2F] = 94,	/* U+1D42F: MATHEMATICAL BOLD SMALL V */
	[4992+0x30] = 69,	/* U+1D430: MATHEMATICAL BOLD SMALL W */
	[4992+0x31] = 33,	/* U+1D431: MATHEMATICAL BOLD SMALL X */
	[4992+0x32] = 44,	/* U+1D432: MATHEMATICAL BOLD SMALL Y */
	[4992+0x33] = 71,	/* U+1D433: MATHEMATICAL BOLD SMALL Z */
	[4992+0x34] = 25,	/* U+1D434: MATHEMATICAL ITALIC CAPITAL A */
	[4992+0x35] = 73,	/* U+1D435: MATHEMATICAL ITALIC CAPITAL B */
	[4992+0x36] = 27,	/* U+1D436: MATHEMATICAL ITALIC CAPITAL C */
	[4992+0x37] = 30,	/* U+1D437: MATHEMATICAL ITALIC CAPITAL D */
	[4992+0x38] = 28,	/* U+1D438: MATHEMATICAL ITALIC CAPITAL E */
	[4992+0x39] = 74,	/* U+1D439: MATHEMATICAL ITALIC CAPITAL F */
	[4992+0x3A] = 46,	/* U+1D43A: MATHEMATICAL ITALIC CAPITAL G */
	[4992+0x3B] = 48,	/* U+1D43B: MATHEMATICAL ITALIC CAPITAL H */
	[4992+0x3C] = 29,	/* U+1D43C: MATHEMATICAL ITALIC CAPITAL I */
	[4992+0x3D] = 52,	/* U+1D43D: MATHEMATICAL ITALIC CAPITAL J */
	[4992+0x3E] = 54,	/* U+1D43E: MATHEMATICAL ITALIC CAPITAL K */
	[4992+0x3F] = 57,	/* U+1D43F: MATHEMATICAL ITALIC CAPITAL L */
	[4928+0x11] = 5056 - 0x80,	/* 360 235 221 ... */
	[5056+0x00] = 107,	/* U+1D440: MATHEMATICAL ITALIC CAPITAL M */
	[5056+0x01] = 31,	/* U+1D441: MATHEMATICAL ITALIC CAPITAL N */
	[5056+0x02] = 32,	/* U+1D442: MATHEMATICAL ITALIC CAPITAL O */
	[5056+0x03] = 79,	/* U+1D443: MATHEMATICAL ITALIC CAPITAL P */
	[5056+0x04] = 162,	/* U+1D444: MATHEMATICAL ITALIC CAPITAL Q */
	[5056+0x05] = 62,	/* U+1D445: MATHEMATICAL ITALIC CAPITAL R */
	[5056+0x06] = 64,	/* U+1D446: MATHEMATICAL ITALIC CAPITAL S */
	[5056+0x07] = 66,	/* U+1D447: MATHEMATICAL ITALIC CAPITAL T */
	[5056+0x08] = 34,	/* U+1D448: MATHEMATICAL ITALIC CAPITAL U */
	[5056+0x09] = 81,	/* U+1D449: MATHEMATICAL ITALIC CAPITAL V */
	[5056+0x0A] = 68,	/* U+1D44A: MATHEMATICAL ITALIC CAPITAL W */
	[5056+0x0B] = 109,	/* U+1D44B: MATHEMATICAL ITALIC CAPITAL X */
	[5056+0x0C] = 35,	/* U+1D44C: MATHEMATICAL ITALIC CAPITAL Y */
	[5056+0x0D] = 70,	/* U+1D44D: MATHEMATICAL ITALIC CAPITAL Z */
	[5056+0x0E] = 8,	/* U+1D44E: MATHEMATICAL ITALIC SMALL A */
	[5056+0x0F] = 72,	/* U+1D44F: MATHEMATICAL ITALIC SMALL B */
	[5056+0x10] = 3,	/* U+1D450: MATHEMATICAL ITALIC SMALL C */
	[5056+0x11] = 41,	/* U+1D451: MATHEMATICAL ITALIC SMALL D */
	[5056+0x12] = 39,	/* U+1D452: MATHEMATICAL ITALIC SMALL E */
	[5056+0x13] = 75,	/* U+1D453: MATHEMATICAL ITALIC SMALL F */
	[5056+0x14] = 47,	/* U+1D454: MATHEMATICAL ITALIC SMALL G */
	[5056+0x16] = 40,	/* U+1D456: MATHEMATICAL ITALIC SMALL I */
	[5056+0x17] = 53,	/* U+1D457: MATHEMATICAL ITALIC SMALL J */
	[5056+0x18] = 55,	/* U+1D458: MATHEMATICAL ITALIC SMALL K */
	[5056+0x19] = 58,	/* U+1D459: MATHEMATICAL ITALIC SMALL L */
	[5056+0x1A] = 93,	/* U+1D45A: MATHEMATICAL ITALIC SMALL M */
	[5056+0x1B] = 42,	/* U+1D45B: MATHEMATICAL ITALIC SMALL N */
	[5056+0x1C] = 20,	/* U+1D45C: MATHEMATICAL ITALIC SMALL O */
	[5056+0x1D] = 80,	/* U+1D45D: MATHEMATICAL ITALIC SMALL P */
	[5056+0x1E] = 56,	/* U+1D45E: MATHEMATICAL ITALIC SMALL Q */
	[5056+0x1F] = 63,	/* U+1D45F: MATHEMATICAL ITALIC SMALL R */
	[5056+0x20] = 65,	/* U+1D460: MATHEMATICAL ITALIC SMALL S */
	[5056+0x21] = 67,	/* U+1D461: MATHEMATICAL ITALIC SMALL T */
	[5056+0x22] = 16,	/* U+1D462: MATHEMATICAL ITALIC SMALL U */
	[5056+0x23] = 94,	/* U+1D463: MATHEMATICAL ITALIC SMALL V */
	[5056+0x24] = 69,	/* U+1D464: MATHEMATICAL ITALIC SMALL W */
	[5056+0x25] = 33,	/* U+1D465: MATHEMATICAL ITALIC SMALL X */
	[5056+0x26] = 44,	/* U+1D466: MATHEMATICAL ITALIC SMALL Y */
	[5056+0x27] = 71,	/* U+1D467: MATHEMATICAL ITALIC SMALL Z */
	[5056+0x28] = 25,	/* U+1D468: MATHEMATICAL BOLD ITALIC CAPITAL A */
	[5056+0x29] = 73,	/* U+1D469: MATHEMATICAL BOLD ITALIC CAPITAL B */
	[5056+0x2A] = 27,	/* U+1D46A: MATHEMATICAL BOLD ITALIC CAPITAL C */
	[5056+0x2B] = 30,	/* U+1D46B: MATHEMATICAL BOLD ITALIC CAPITAL D */
	[5056+0x2C] = 28,	/* U+1D46C: MATHEMATICAL BOLD ITALIC CAPITAL E */
	[5056+0x2D] = 74,	/* U+1D46D: MATHEMATICAL BOLD ITALIC CAPITAL F */
	[5056+0x2E] = 46,	/* U+1D46E: MATHEMATICAL BOLD ITALIC CAPITAL G */
	[5056+0x2F] = 48,	/* U+1D46F: MATHEMATICAL BOLD ITALIC CAPITAL H */
	[5056+0x30] = 29,	/* U+1D470: MATHEMATICAL BOLD ITALIC CAPITAL I */
	[5056+0x31] = 52,	/* U+1D471: MATHEMATICAL BOLD ITALIC CAPITAL J */
	[5056+0x32] = 54,	/* U+1D472: MATHEMATICAL BOLD ITALIC CAPITAL K */
	[5056+0x33] = 57,	/* U+1D473: MATHEMATICAL BOLD ITALIC CAPITAL L */
	[5056+0x34] = 107,	/* U+1D474: MATHEMATICAL BOLD ITALIC CAPITAL M */
	[5056+0x35] = 31,	/* U+1D475: MATHEMATICAL BOLD ITALIC CAPITAL N */
	[5056+0x36] = 32,	/* U+1D476: MATHEMATICAL BOLD ITALIC CAPITAL O */
	[5056+0x37] = 79,	/* U+1D477: MATHEMATICAL BOLD ITALIC CAPITAL P */
	[5056+0x38] = 162,	/* U+1D478: MATHEMATICAL BOLD ITALIC CAPITAL Q */
	[5056+0x39] = 62,	/* U+1D479: MATHEMATICAL BOLD ITALIC CAPITAL R */
	[5056+0x3A] = 64,	/* U+1D47A: MATHEMATICAL BOLD ITALIC CAPITAL S */
	[5056+0x3B] = 66,	/* U+1D47B: MATHEMATICAL BOLD ITALIC CAPITAL T */
	[5056+0x3C] = 34,	/* U+1D47C: MATHEMATICAL BOLD ITALIC CAPITAL U */
	[5056+0x3D] = 81,	/* U+1D47D: MATHEMATICAL BOLD ITALIC CAPITAL V */
	[5056+0x3E] = 68,	/* U+1D47E: MATHEMATICAL BOLD ITALIC CAPITAL W */
	[5056+0x3F] = 109,	/* U+1D47F: MATHEMATICAL BOLD ITALIC CAPITAL X */
	[4928+0x12] = 5120 - 0x80,	/* 360 235 222 ... */
	[5120+0x00] = 35,	/* U+1D480: MATHEMATICAL BOLD ITALIC CAPITAL Y */
	[5120+0x01] = 70,	/* U+1D481: MATHEMATICAL BOLD ITALIC CAPITAL Z */
	[5120+0x02] = 8,	/* U+1D482: MATHEMATICAL BOLD ITALIC SMALL A */
	[5120+0x03] = 72,	/* U+1D483: MATHEMATICAL BOLD ITALIC SMALL B */
	[5120+0x04] = 3,	/* U+1D484: MATHEMATICAL BOLD ITALIC SMALL C */
	[5120+0x05] = 41,	/* U+1D485: MATHEMATICAL BOLD ITALIC SMALL D */
	[5120+0x06] = 39,	/* U+1D486: MATHEMATICAL BOLD ITALIC SMALL E */
	[5120+0x07] = 75,	/* U+1D487: MATHEMATICAL BOLD ITALIC SMALL F */
	[5120+0x08] = 47,	/* U+1D488: MATHEMATICAL BOLD ITALIC SMALL G */
	[5120+0x09] = 49,	/* U+1D489: MATHEMATICAL BOLD ITALIC SMALL H */
	[5120+0x0A] = 40,	/* U+1D48A: MATHEMATICAL BOLD ITALIC SMALL I */
	[5120+0x0B] = 53,	/* U+1D48B: MATHEMATICAL BOLD ITALIC SMALL J */
	[5120+0x0C] = 55,	/* U+1D48C: MATHEMATICAL BOLD ITALIC SMALL K */
	[5120+0x0D] = 58,	/* U+1D48D: MATHEMATICAL BOLD ITALIC SMALL L */
	[5120+0x0E] = 93,	/* U+1D48E: MATHEMATICAL BOLD ITALIC SMALL M */
	[5120+0x0F] = 42,	/* U+1D48F: MATHEMATICAL BOLD ITALIC SMALL N */
	[5120+0x10] = 20,	/* U+1D490: MATHEMATICAL BOLD ITALIC SMALL O */
	[5120+0x11] = 80,	/* U+1D491: MATHEMATICAL BOLD ITALIC SMALL P */
	[5120+0x12] = 56,	/* U+1D492: MATHEMATICAL BOLD ITALIC SMALL Q */
	[5120+0x13] = 63,	/* U+1D493: MATHEMATICAL BOLD ITALIC SMALL R */
	[5120+0x14] = 65,	/* U+1D494: MATHEMATICAL BOLD ITALIC SMALL S */
	[5120+0x15] = 67,	/* U+1D495: MATHEMATICAL BOLD ITALIC SMALL T */
	[5120+0x16] = 16,	/* U+1D496: MATHEMATICAL BOLD ITALIC SMALL U */
	[5120+0x17] = 94,	/* U+1D497: MATHEMATICAL BOLD ITALIC SMALL V */
	[5120+0x18] = 69,	/* U+1D498: MATHEMATICAL BOLD ITALIC SMALL W */
	[5120+0x19] = 33,	/* U+1D499: MATHEMATICAL BOLD ITALIC SMALL X */
	[5120+0x1A] = 44,	/* U+1D49A: MATHEMATICAL BOLD ITALIC SMALL Y */
	[5120+0x1B] = 71,	/* U+1D49B: MATHEMATICAL BOLD ITALIC SMALL Z */
	[5120+0x1C] = 25,	/* U+1D49C: MATHEMATICAL SCRIPT CAPITAL A */
	[5120+0x1E] = 27,	/* U+1D49E: MATHEMATICAL SCRIPT CAPITAL C */
	[5120+0x1F] = 30,	/* U+1D49F: MATHEMATICAL SCRIPT CAPITAL D */
	[5120+0x22] = 46,	/* U+1D4A2: MATHEMATICAL SCRIPT CAPITAL G */
	[5120+0x25] = 52,	/* U+1D4A5: MATHEMATICAL SCRIPT CAPITAL J */
	[5120+0x26] = 54,	/* U+1D4A6: MATHEMATICAL SCRIPT CAPITAL K */
	[5120+0x29] = 31,	/* U+1D4A9: MATHEMATICAL SCRIPT CAPITAL N */
	[5120+0x2A] = 32,	/* U+1D4AA: MATHEMATICAL SCRIPT CAPITAL O */
	[5120+0x2B] = 79,	/* U+1D4AB: MATHEMATICAL SCRIPT CAPITAL P */
	[5120+0x2C] = 162,	/* U+1D4AC: MATHEMATICAL SCRIPT CAPITAL Q */
	[5120+0x2E] = 64,	/* U+1D4AE: MATHEMATICAL SCRIPT CAPITAL S */
	[5120+0x2F] = 66,	/* U+1D4AF: MATHEMATICAL SCRIPT CAPITAL T */
	[5120+0x30] = 34,	/* U+1D4B0: MATHEMATICAL SCRIPT CAPITAL U */
	[5120+0x31] = 81,	/* U+1D4B1: MATHEMATICAL SCRIPT CAPITAL V */
	[5120+0x32] = 68,	/* U+1D4B2: MATHEMATICAL SCRIPT CAPITAL W */
	[5120+0x33] = 109,	/* U+1D4B3: MATHEMATICAL SCRIPT CAPITAL X */
	[5120+0x34] = 35,	/* U+1D4B4: MATHEMATICAL SCRIPT CAPITAL Y */
	[5120+0x35] = 70,	/* U+1D4B5: MATHEMATICAL SCRIPT CAPITAL Z */
	[5120+0x36] = 8,	/* U+1D4B6: MATHEMATICAL SCRIPT SMALL A */
	[5120+0x37] = 72,	/* U+1D4B7: MATHEMATICAL SCRIPT SMALL B */
	[5120+0x38] = 3,	/* U+1D4B8: MATHEMATICAL SCRIPT SMALL C */
	[5120+0x39] = 41,	/* U+1D4B9: MATHEMATICAL SCRIPT SMALL D */
	[5120+0x3B] = 75,	/* U+1D4BB: MATHEMATICAL SCRIPT SMALL F */
	[5120+0x3D] = 49,	/* U+1D4BD: MATHEMATICAL SCRIPT SMALL H */
	[5120+0x3E] = 40,	/* U+1D4BE: MATHEMATICAL SCRIPT SMALL I */
	[5120+0x3F] = 53,	/* U+1D4BF: MATHEMATICAL SCRIPT SMALL J */
	[4928+0x13] = 5184 - 0x80,	/* 360 235 223 ... */
	[5184+0x00] = 55,	/* U+1D4C0: MATHEMATICAL SCRIPT SMALL K */
	[5184+0x01] = 58,	/* U+1D4C1: MATHEMATICAL SCRIPT SMALL L */
	[5184+0x02] = 93,	/* U+1D4C2: MATHEMATICAL SCRIPT SMALL M */
	[5184+0x03] = 42,	/* U+1D4C3: MATHEMATICAL SCRIPT SMALL N */
	[5184+0x05] = 80,	/* U+1D4C5: MATHEMATICAL SCRIPT SMALL P */
	[5184+0x06] = 56,	/* U+1D4C6: MATHEMATICAL SCRIPT SMALL Q */
	[5184+0x07] = 63,	/* U+1D4C7: MATHEMATICAL SCRIPT SMALL R */
	[5184+0x08] = 65,	/* U+1D4C8: MATHEMATICAL SCRIPT SMALL S */
	[5184+0x09] = 67,	/* U+1D4C9: MATHEMATICAL SCRIPT SMALL T */
	[5184+0x0A] = 16,	/* U+1D4CA: MATHEMATICAL SCRIPT SMALL U */
	[5184+0x0B] = 94,	/* U+1D4CB: MATHEMATICAL SCRIPT SMALL V */
	[5184+0x0C] = 69,	/* U+1D4CC: MATHEMATICAL SCRIPT SMALL W */
	[5184+0x0D] = 33,	/* U+1D4CD: MATHEMATICAL SCRIPT SMALL X */
	[5184+0x0E] = 44,	/* U+1D4CE: MATHEMATICAL SCRIPT SMALL Y */
	[5184+0x0F] = 71,	/* U+1D4CF: MATHEMATICAL SCRIPT SMALL Z */
	[5184+0x10] = 25,	/* U+1D4D0: MATHEMATICAL BOLD SCRIPT CAPITAL A */
	[5184+0x11] = 73,	/* U+1D4D1: MATHEMATICAL BOLD SCRIPT CAPITAL B */
	[5184+0x12] = 27,	/* U+1D4D2: MATHEMATICAL BOLD SCRIPT CAPITAL C */
	[5184+0x13] = 30,	/* U+1D4D3: MATHEMATICAL BOLD SCRIPT CAPITAL D */
	[5184+0x14] = 28,	/* U+1D4D4: MATHEMATICAL BOLD SCRIPT CAPITAL E */
	[5184+0x15] = 74,	/* U+1D4D5: MATHEMATICAL BOLD SCRIPT CAPITAL F */
	[5184+0x16] = 46,	/* U+1D4D6: MATHEMATICAL BOLD SCRIPT CAPITAL G */
	[5184+0x17] = 48,	/* U+1D4D7: MATHEMATICAL BOLD SCRIPT CAPITAL H */
	[5184+0x18] = 29,	/* U+1D4D8: MATHEMATICAL BOLD SCRIPT CAPITAL I */
	[5184+0x19] = 52,	/* U+1D4D9: MATHEMATICAL BOLD SCRIPT CAPITAL J */
	[5184+0x1A] = 54,	/* U+1D4DA: MATHEMATICAL BOLD SCRIPT CAPITAL K */
	[5184+0x1B] = 57,	/* U+1D4DB: MATHEMATICAL BOLD SCRIPT CAPITAL L */
	[5184+0x1C] = 107,	/* U+1D4DC: MATHEMATICAL BOLD SCRIPT CAPITAL M */
	[5184+0x1D] = 31,	/* U+1D4DD: MATHEMATICAL BOLD SCRIPT CAPITAL N */
	[5184+0x1E] = 32,	/* U+1D4DE: MATHEMATICAL BOLD SCRIPT CAPITAL O */
	[5184+0x1F] = 79,	/* U+1D4DF: MATHEMATICAL BOLD SCRIPT CAPITAL P */
	[5184+0x20] = 162,	/* U+1D4E0: MATHEMATICAL BOLD SCRIPT CAPITAL Q */
	[5184+0x21] = 62,	/* U+1D4E1: MATHEMATICAL BOLD SCRIPT CAPITAL R */
	[5184+0x22] = 64,	/* U+1D4E2: MATHEMATICAL BOLD SCRIPT CAPITAL S */
	[5184+0x23] = 66,	/* U+1D4E3: MATHEMATICAL BOLD SCRIPT CAPITAL T */
	[5184+0x24] = 34,	/* U+1D4E4: MATHEMATICAL BOLD SCRIPT CAPITAL U */
	[5184+0x25] = 81,	/* U+1D4E5: MATHEMATICAL BOLD SCRIPT CAPITAL V */
	[5184+0x26] = 68,	/* U+1D4E6: MATHEMATICAL BOLD SCRIPT CAPITAL W */
	[5184+0x27] = 109,	/* U+1D4E7: MATHEMATICAL BOLD SCRIPT CAPITAL X */
	[5184+0x28] = 35,	/* U+1D4E8: MATHEMATICAL BOLD SCRIPT CAPITAL Y */
	[5184+0x29] = 70,	/* U+1D4E9: MATHEMATICAL BOLD SCRIPT CAPITAL Z */
	[5184+0x2A] = 8,	/* U+1D4EA: MATHEMATICAL BOLD SCRIPT SMALL A */
	[5184+0x2B] = 72,	/* U+1D4EB: MATHEMATICAL BOLD SCRIPT SMALL B */
	[5184+0x2C] = 3,	/* U+1D4EC: MATHEMATICAL BOLD SCRIPT SMALL C */
	[5184+0x2D] = 41,	/* U+1D4ED: MATHEMATICAL BOLD SCRIPT SMALL D */
	[5184+0x2E] = 39,	/* U+1D4EE: MATHEMATICAL BOLD SCRIPT SMALL E */
	[5184+0x2F] = 75,	/* U+1D4EF: MATHEMATICAL BOLD SCRIPT SMALL F */
	[5184+0x30] = 47,	/* U+1D4F0: MATHEMATICAL BOLD SCRIPT SMALL G */
	[5184+0x31] = 49,	/* U+1D4F1: MATHEMATICAL BOLD SCRIPT SMALL H */
	[5184+0x32] = 40,	/* U+1D4F2: MATHEMATICAL BOLD SCRIPT SMALL I */
	[5184+0x33] = 53,	/* U+1D4F3: MATHEMATICAL BOLD SCRIPT SMALL J */
	[5184+0x34] = 55,	/* U+1D4F4: MATHEMATICAL BOLD SCRIPT SMALL K */
	[5184+0x35] = 58,	/* U+1D4F5: MATHEMATICAL BOLD SCRIPT SMALL L */
	[5184+0x36] = 93,	/* U+1D4F6: MATHEMATICAL BOLD SCRIPT SMALL M */
	[5184+0x37] = 42,	/* U+1D4F7: MATHEMATICAL BOLD SCRIPT SMALL N */
	[5184+0x38] = 20,	/* U+1D4F8: MATHEMATICAL BOLD SCRIPT SMALL O */
	[5184+0x39] = 80,	/* U+1D4F9: MATHEMATICAL BOLD SCRIPT SMALL P */
	[5184+0x3A] = 56,	/* U+1D4FA: MATHEMATICAL BOLD SCRIPT SMALL Q */
	[5184+0x3B] = 63,	/* U+1D4FB: MATHEMATICAL BOLD SCRIPT SMALL R */
	[5184+0x3C] = 65,	/* U+1D4FC: MATHEMATICAL BOLD SCRIPT SMALL S */
	[5184+0x3D] = 67,	/* U+1D4FD: MATHEMATICAL BOLD SCRIPT SMALL T */
	[5184+0x3E] = 16,	/* U+1D4FE: MATHEMATICAL BOLD SCRIPT SMALL U */
	[5184+0x3F] = 94,	/* U+1D4FF: MATHEMATICAL BOLD SCRIPT SMALL V */
	[4928+0x14] = 5248 - 0x80,	/* 360 235 224 ... */
	[5248+0x00] = 69,	/* U+1D500: MATHEMATICAL BOLD SCRIPT SMALL W */
	[5248+0x01] = 33,	/* U+1D501: MATHEMATICAL BOLD SCRIPT SMALL X */
	[5248+0x02] = 44,	/* U+1D502: MATHEMATICAL BOLD SCRIPT SMALL Y */
	[5248+0x03] = 71,	/* U+1D503: MATHEMATICAL BOLD SCRIPT SMALL Z */
	[5248+0x04] = 25,	/* U+1D504: MATHEMATICAL FRAKTUR CAPITAL A */
	[5248+0x05] = 73,	/* U+1D505: MATHEMATICAL FRAKTUR CAPITAL B */
	[5248+0x07] = 30,	/* U+1D507: MATHEMATICAL FRAKTUR CAPITAL D */
	[5248+0x08] = 28,	/* U+1D508: MATHEMATICAL FRAKTUR CAPITAL E */
	[5248+0x09] = 74,	/* U+1D509: MATHEMATICAL FRAKTUR CAPITAL F */
	[5248+0x0A] = 46,	/* U+1D50A: MATHEMATICAL FRAKTUR CAPITAL G */
	[5248+0x0D] = 52,	/* U+1D50D: MATHEMATICAL FRAKTUR CAPITAL J */
	[5248+0x0E] = 54,	/* U+1D50E: MATHEMATICAL FRAKTUR CAPITAL K */
	[5248+0x0F] = 57,	/* U+1D50F: MATHEMATICAL FRAKTUR CAPITAL L */
	[5248+0x10] = 107,	/* U+1D510: MATHEMATICAL FRAKTUR CAPITAL M */
	[5248+0x11] = 31,	/* U+1D511: MATHEMATICAL FRAKTUR CAPITAL N */
	[5248+0x12] = 32,	/* U+1D512: MATHEMATICAL FRAKTUR CAPITAL O */
	[5248+0x13] = 79,	/* U+1D513: MATHEMATICAL FRAKTUR CAPITAL P */
	[5248+0x14] = 162,	/* U+1D514: MATHEMATICAL FRAKTUR CAPITAL Q */
	[5248+0x16] = 64,	/* U+1D516: MATHEMATICAL FRAKTUR CAPITAL S */
	[5248+0x17] = 66,	/* U+1D517: MATHEMATICAL FRAKTUR CAPITAL T */
	[5248+0x18] = 34,	/* U+1D518: MATHEMATICAL FRAKTUR CAPITAL U */
	[5248+0x19] = 81,	/* U+1D519: MATHEMATICAL FRAKTUR CAPITAL V */
	[5248+0x1A] = 68,	/* U+1D51A: MATHEMATICAL FRAKTUR CAPITAL W */
	[5248+0x1B] = 109,	/* U+1D51B: MATHEMATICAL FRAKTUR CAPITAL X */
	[5248+0x1C] = 35,	/* U+1D51C: MATHEMATICAL FRAKTUR CAPITAL Y */
	[5248+0x1E] = 8,	/* U+1D51E: MATHEMATICAL FRAKTUR SMALL A */
	[5248+0x1F] = 72,	/* U+1D51F: MATHEMATICAL FRAKTUR SMALL B */
	[5248+0x20] = 3,	/* U+1D520: MATHEMATICAL FRAKTUR SMALL C */
	[5248+0x21] = 41,	/* U+1D521: MATHEMATICAL FRAKTUR SMALL D */
	[5248+0x22] = 39,	/* U+1D522: MATHEMATICAL FRAKTUR SMALL E */
	[5248+0x23] = 75,	/* U+1D523: MATHEMATICAL FRAKTUR SMALL F */
	[5248+0x24] = 47,	/* U+1D524: MATHEMATICAL FRAKTUR SMALL G */
	[5248+0x25] = 49,	/* U+1D525: MATHEMATICAL FRAKTUR SMALL H */
	[5248+0x26] = 40,	/* U+1D526: MATHEMATICAL FRAKTUR SMALL I */
	[5248+0x27] = 53,	/* U+1D527: MATHEMATICAL FRAKTUR SMALL J */
	[5248+0x28] = 55,	/* U+1D528: MATHEMATICAL FRAKTUR SMALL K */
	[5248+0x29] = 58,	/* U+1D529: MATHEMATICAL FRAKTUR SMALL L */
	[5248+0x2A] = 93,	/* U+1D52A: MATHEMATICAL FRAKTUR SMALL M */
	[5248+0x2B] = 42,	/* U+1D52B: MATHEMATICAL FRAKTUR SMALL N */
	[5248+0x2C] = 20,	/* U+1D52C: MATHEMATICAL FRAKTUR SMALL O */
	[5248+0x2D] = 80,	/* U+1D52D: MATHEMATICAL FRAKTUR SMALL P */
	[5248+0x2E] = 56,	/* U+1D52E: MATHEMATICAL FRAKTUR SMALL Q */
	[5248+0x2F] = 63,	/* U+1D52F: MATHEMATICAL FRAKTUR SMALL R */
	[5248+0x30] = 65,	/* U+1D530: MATHEMATICAL FRAKTUR SMALL S */
	[5248+0x31] = 67,	/* U+1D531: MATHEMATICAL FRAKTUR SMALL T */
	[5248+0x32] = 16,	/* U+1D532: MATHEMATICAL FRAKTUR SMALL U */
	[5248+0x33] = 94,	/* U+1D533: MATHEMATICAL FRAKTUR SMALL V */
	[5248+0x34] = 69,	/* U+1D534: MATHEMATICAL FRAKTUR SMALL W */
	[5248+0x35] = 33,	/* U+1D535: MATHEMATICAL FRAKTUR SMALL X */
	[5248+0x36] = 44,	/* U+1D536: MATHEMATICAL FRAKTUR SMALL Y */
	[5248+0x37] = 71,	/* U+1D537: MATHEMATICAL FRAKTUR SMALL Z */
	[5248+0x38] = 25,	/* U+1D538: MATHEMATICAL DOUBLE-STRUCK CAPITAL A */
	[5248+0x39] = 73,	/* U+1D539: MATHEMATICAL DOUBLE-STRUCK CAPITAL B */
	[5248+0x3B] = 30,	/* U+1D53B: MATHEMATICAL DOUBLE-STRUCK CAPITAL D */
	[5248+0x3C] = 28,	/* U+1D53C: MATHEMATICAL DOUBLE-STRUCK CAPITAL E */
	[5248+0x3D] = 74,	/* U+1D53D: MATHEMATICAL DOUBLE-STRUCK CAPITAL F */
	[5248+0x3E] = 46,	/* U+1D53E: MATHEMATICAL DOUBLE-STRUCK CAPITAL G */
	[4928+0x15] = 5312 - 0x80,	/* 360 235 225 ... */
	[5312+0x00] = 29,	/* U+1D540: MATHEMATICAL DOUBLE-STRUCK CAPITAL I */
	[5312+0x01] = 52,	/* U+1D541: MATHEMATICAL DOUBLE-STRUCK CAPITAL J */
	[5312+0x02] = 54,	/* U+1D542: MATHEMATICAL DOUBLE-STRUCK CAPITAL K */
	[5312+0x03] = 57,	/* U+1D543: MATHEMATICAL DOUBLE-STRUCK CAPITAL L */
	[5312+0x04] = 107,	/* U+1D544: MATHEMATICAL DOUBLE-STRUCK CAPITAL M */
	[5312+0x06] = 32,	/* U+1D546: MATHEMATICAL DOUBLE-STRUCK CAPITAL O */
	[5312+0x0A] = 64,	/* U+1D54A: MATHEMATICAL DOUBLE-STRUCK CAPITAL S */
	[5312+0x0B] = 66,	/* U+1D54B: MATHEMATICAL DOUBLE-STRUCK CAPITAL T */
	[5312+0x0C] = 34,	/* U+1D54C: MATHEMATICAL DOUBLE-STRUCK CAPITAL U */
	[5312+0x0D] = 81,	/* U+1D54D: MATHEMATICAL DOUBLE-STRUCK CAPITAL V */
	[5312+0x0E] = 68,	/* U+1D54E: MATHEMATICAL DOUBLE-STRUCK CAPITAL W */
	[5312+0x0F] = 109,	/* U+1D54F: MATHEMATICAL DOUBLE-STRUCK CAPITAL X */
	[5312+0x10] = 35,	/* U+1D550: MATHEMATICAL DOUBLE-STRUCK CAPITAL Y */
	[5312+0x12] = 8,	/* U+1D552: MATHEMATICAL DOUBLE-STRUCK SMALL A */
	[5312+0x13] = 72,	/* U+1D553: MATHEMATICAL DOUBLE-STRUCK SMALL B */
	[5312+0x14] = 3,	/* U+1D554: MATHEMATICAL DOUBLE-STRUCK SMALL C */
	[5312+0x15] = 41,	/* U+1D555: MATHEMATICAL DOUBLE-STRUCK SMALL D */
	[5312+0x16] = 39,	/* U+1D556: MATHEMATICAL DOUBLE-STRUCK SMALL E */
	[5312+0x17] = 75,	/* U+1D557: MATHEMATICAL DOUBLE-STRUCK SMALL F */
	[5312+0x18] = 47,	/* U+1D558: MATHEMATICAL DOUBLE-STRUCK SMALL G */
	[5312+0x19] = 49,	/* U+1D559: MATHEMATICAL DOUBLE-STRUCK SMALL H */
	[5312+0x1A] = 40,	/* U+1D55A: MATHEMATICAL DOUBLE-STRUCK SMALL I */
	[5312+0x1B] = 53,	/* U+1D55B: MATHEMATICAL DOUBLE-STRUCK SMALL J */
	[5312+0x1C] = 55,	/* U+1D55C: MATHEMATICAL DOUBLE-STRUCK SMALL K */
	[5312+0x1D] = 58,	/* U+1D55D: MATHEMATICAL DOUBLE-STRUCK SMALL L */
	[5312+0x1E] = 93,	/* U+1D55E: MATHEMATICAL DOUBLE-STRUCK SMALL M */
	[5312+0x1F] = 42,	/* U+1D55F: MATHEMATICAL DOUBLE-STRUCK SMALL N */
	[5312+0x20] = 20,	/* U+1D560: MATHEMATICAL DOUBLE-STRUCK SMALL O */
	[5312+0x21] = 80,	/* U+1D561: MATHEMATICAL DOUBLE-STRUCK SMALL P */
	[5312+0x22] = 56,	/* U+1D562: MATHEMATICAL DOUBLE-STRUCK SMALL Q */
	[5312+0x23] = 63,	/* U+1D563: MATHEMATICAL DOUBLE-STRUCK SMALL R */
	[5312+0x24] = 65,	/* U+1D564: MATHEMATICAL DOUBLE-STRUCK SMALL S */
	[5312+0x25] = 67,	/* U+1D565: MATHEMATICAL DOUBLE-STRUCK SMALL T */
	[5312+0x26] = 16,	/* U+1D566: MATHEMATICAL DOUBLE-STRUCK SMALL U */
	[5312+0x27] = 94,	/* U+1D567: MATHEMATICAL DOUBLE-STRUCK SMALL V */
	[5312+0x28] = 69,	/* U+1D568: MATHEMATICAL DOUBLE-STRUCK SMALL W */
	[5312+0x29] = 33,	/* U+1D569: MATHEMATICAL DOUBLE-STRUCK SMALL X */
	[5312+0x2A] = 44,	/* U+1D56A: MATHEMATICAL DOUBLE-STRUCK SMALL Y */
	[5312+0x2B] = 71,	/* U+1D56B: MATHEMATICAL DOUBLE-STRUCK SMALL Z */
	[5312+0x2C] = 25,	/* U+1D56C: MATHEMATICAL BOLD FRAKTUR CAPITAL A */
	[5312+0x2D] = 73,	/* U+1D56D: MATHEMATICAL BOLD FRAKTUR CAPITAL B */
	[5312+0x2E] = 27,	/* U+1D56E: MATHEMATICAL BOLD FRAKTUR CAPITAL C */
	[5312+0x2F] = 30,	/* U+1D56F: MATHEMATICAL BOLD FRAKTUR CAPITAL D */
	[5312+0x30] = 28,	/* U+1D570: MATHEMATICAL BOLD FRAKTUR CAPITAL E */
	[5312+0x31] = 74,	/* U+1D571: MATHEMATICAL BOLD FRAKTUR CAPITAL F */
	[5312+0x32] = 46,	/* U+1D572: MATHEMATICAL BOLD FRAKTUR CAPITAL G */
	[5312+0x33] = 48,	/* U+1D573: MATHEMATICAL BOLD FRAKTUR CAPITAL H */
	[5312+0x34] = 29,	/* U+1D574: MATHEMATICAL BOLD FRAKTUR CAPITAL I */
	[5312+0x35] = 52,	/* U+1D575: MATHEMATICAL BOLD FRAKTUR CAPITAL J */
	[5312+0x36] = 54,	/* U+1D576: MATHEMATICAL BOLD FRAKTUR CAPITAL K */
	[5312+0x37] = 57,	/* U+1D577: MATHEMATICAL BOLD FRAKTUR CAPITAL L */
	[5312+0x38] = 107,	/* U+1D578: MATHEMATICAL BOLD FRAKTUR CAPITAL M */
	[5312+0x39] = 31,	/* U+1D579: MATHEMATICAL BOLD FRAKTUR CAPITAL N */
	[5312+0x3A] = 32,	/* U+1D57A: MATHEMATICAL BOLD FRAKTUR CAPITAL O */
	[5312+0x3B] = 79,	/* U+1D57B: MATHEMATICAL BOLD FRAKTUR CAPITAL P */
	[5312+0x3C] = 162,	/* U+1D57C: MATHEMATICAL BOLD FRAKTUR CAPITAL Q */
	[5312+0x3D] = 62,	/* U+1D57D: MATHEMATICAL BOLD FRAKTUR CAPITAL R */
	[5312+0x3E] = 64,	/* U+1D57E: MATHEMATICAL BOLD FRAKTUR CAPITAL S */
	[5312+0x3F] = 66,	/* U+1D57F: MATHEMATICAL BOLD FRAKTUR CAPITAL T */
	[4928+0x16] = 5376 - 0x80,	/* 360 235 226 ... */
	[5376+0x00] = 34,	/* U+1D580: MATHEMATICAL BOLD FRAKTUR CAPITAL U */
	[5376+0x01] = 81,	/* U+1D581: MATHEMATICAL BOLD FRAKTUR CAPITAL V */
	[5376+0x02] = 68,	/* U+1D582: MATHEMATICAL BOLD FRAKTUR CAPITAL W */
	[5376+0x03] = 109,	/* U+1D583: MATHEMATICAL BOLD FRAKTUR CAPITAL X */
	[5376+0x04] = 35,	/* U+1D584: MATHEMATICAL BOLD FRAKTUR CAPITAL Y */
	[5376+0x05] = 70,	/* U+1D585: MATHEMATICAL BOLD FRAKTUR CAPITAL Z */
	[5376+0x06] = 8,	/* U+1D586: MATHEMATICAL BOLD FRAKTUR SMALL A */
	[5376+0x07] = 72,	/* U+1D587: MATHEMATICAL BOLD FRAKTUR SMALL B */
	[5376+0x08] = 3,	/* U+1D588: MATHEMATICAL BOLD FRAKTUR SMALL C */
	[5376+0x09] = 41,	/* U+1D589: MATHEMATICAL BOLD FRAKTUR SMALL D */
	[5376+0x0A] = 39,	/* U+1D58A: MATHEMATICAL BOLD FRAKTUR SMALL E */
	[5376+0x0B] = 75,	/* U+1D58B: MATHEMATICAL BOLD FRAKTUR SMALL F */
	[5376+0x0C] = 47,	/* U+1D58C: MATHEMATICAL BOLD FRAKTUR SMALL G */
	[5376+0x0D] = 49,	/* U+1D58D: MATHEMATICAL BOLD FRAKTUR SMALL H */
	[5376+0x0E] = 40,	/* U+1D58E: MATHEMATICAL BOLD FRAKTUR SMALL I */
	[5376+0x0F] = 53,	/* U+1D58F: MATHEMATICAL BOLD FRAKTUR SMALL J */
	[5376+0x10] = 55,	/* U+1D590: MATHEMATICAL BOLD FRAKTUR SMALL K */
	[5376+0x11] = 58,	/* U+1D591: MATHEMATICAL BOLD FRAKTUR SMALL L */
	[5376+0x12] = 93,	/* U+1D592: MATHEMATICAL BOLD FRAKTUR SMALL M */
	[5376+0x13] = 42,	/* U+1D593: MATHEMATICAL BOLD FRAKTUR SMALL N */
	[5376+0x14] = 20,	/* U+1D594: MATHEMATICAL BOLD FRAKTUR SMALL O */
	[5376+0x15] = 80,	/* U+1D595: MATHEMATICAL BOLD FRAKTUR SMALL P */
	[5376+0x16] = 56,	/* U+1D596: MATHEMATICAL BOLD FRAKTUR SMALL Q */
	[5376+0x17] = 63,	/* U+1D597: MATHEMATICAL BOLD FRAKTUR SMALL R */
	[5376+0x18] = 65,	/* U+1D598: MATHEMATICAL BOLD FRAKTUR SMALL S */
	[5376+0x19] = 67,	/* U+1D599: MATHEMATICAL BOLD FRAKTUR SMALL T */
	[5376+0x1A] = 16,	/* U+1D59A: MATHEMATICAL BOLD FRAKTUR SMALL U */
	[5376+0x1B] = 94,	/* U+1D59B: MATHEMATICAL BOLD FRAKTUR SMALL V */
	[5376+0x1C] = 69,	/* U+1D59C: MATHEMATICAL BOLD FRAKTUR SMALL W */
	[5376+0x1D] = 33,	/* U+1D59D: MATHEMATICAL BOLD FRAKTUR SMALL X */
	[5376+0x1E] = 44,	/* U+1D59E: MATHEMATICAL BOLD FRAKTUR SMALL Y */
	[5376+0x1F] = 71,	/* U+1D59F: MATHEMATICAL BOLD FRAKTUR SMALL Z */
	[5376+0x20] = 25,	/* U+1D5A0: MATHEMATICAL SANS-SERIF CAPITAL A */
	[5376+0x21] = 73,	/* U+1D5A1: MATHEMATICAL SANS-SERIF CAPITAL B */
	[5376+0x22] = 27,	/* U+1D5A2: MATHEMATICAL SANS-SERIF CAPITAL C */
	[5376+0x23] = 30,	/* U+1D5A3: MATHEMATICAL SANS-SERIF CAPITAL D */
	[5376+0x24] = 28,	/* U+1D5A4: MATHEMATICAL SANS-SERIF CAPITAL E */
	[5376+0x25] = 74,	/* U+1D5A5: MATHEMATICAL SANS-SERIF CAPITAL F */
	[5376+0x26] = 46,	/* U+1D5A6: MATHEMATICAL SANS-SERIF CAPITAL G */
	[5376+0x27] = 48,	/* U+1D5A7: MATHEMATICAL SANS-SERIF CAPITAL H */
	[5376+0x28] = 29,	/* U+1D5A8: MATHEMATICAL SANS-SERIF CAPITAL I */
	[5376+0x29] = 52,	/* U+1D5A9: MATHEMATICAL SANS-SERIF CAPITAL J */
	[5376+0x2A] = 54,	/* U+1D5AA: MATHEMATICAL SANS-SERIF CAPITAL K */
	[5376+0x2B] = 57,	/* U+1D5AB: MATHEMATICAL SANS-SERIF CAPITAL L */
	[5376+0x2C] = 107,	/* U+1D5AC: MATHEMATICAL SANS-SERIF CAPITAL M */
	[5376+0x2D] = 31,	/* U+1D5AD: MATHEMATICAL SANS-SERIF CAPITAL N */
	[5376+0x2E] = 32,	/* U+1D5AE: MATHEMATICAL SANS-SERIF CAPITAL O */
	[5376+0x2F] = 79,	/* U+1D5AF: MATHEMATICAL SANS-SERIF CAPITAL P */
	[5376+0x30] = 162,	/* U+1D5B0: MATHEMATICAL SANS-SERIF CAPITAL Q */
	[5376+0x31] = 62,	/* U+1D5B1: MATHEMATICAL SANS-SERIF CAPITAL R */
	[5376+0x32] = 64,	/* U+1D5B2: MATHEMATICAL SANS-SERIF CAPITAL S */
	[5376+0x33] = 66,	/* U+1D5B3: MATHEMATICAL SANS-SERIF CAPITAL T */
	[5376+0x34] = 34,	/* U+1D5B4: MATHEMATICAL SANS-SERIF CAPITAL U */
	[5376+0x35] = 81,	/* U+1D5B5: MATHEMATICAL SANS-SERIF CAPITAL V */
	[5376+0x36] = 68,	/* U+1D5B6: MATHEMATICAL SANS-SERIF CAPITAL W */
	[5376+0x37] = 109,	/* U+1D5B7: MATHEMATICAL SANS-SERIF CAPITAL X */
	[5376+0x38] = 35,	/* U+1D5B8: MATHEMATICAL SANS-SERIF CAPITAL Y */
	[5376+0x39] = 70,	/* U+1D5B9: MATHEMATICAL SANS-SERIF CAPITAL Z */
	[5376+0x3A] = 8,	/* U+1D5BA: MATHEMATICAL SANS-SERIF SMALL A */
	[5376+0x3B] = 72,	/* U+1D5BB: MATHEMATICAL SANS-SERIF SMALL B */
	[5376+0x3C] = 3,	/* U+1D5BC: MATHEMATICAL SANS-SERIF SMALL C */
	[5376+0x3D] = 41,	/* U+1D5BD: MATHEMATICAL SANS-SERIF SMALL D */
	[5376+0x3E] = 39,	/* U+1D5BE: MATHEMATICAL SANS-SERIF SMALL E */
	[5376+0x3F] = 75,	/* U+1D5BF: MATHEMATICAL SANS-SERIF SMALL F */
	[4928+0x17] = 5440 - 0x80,	/* 360 235 227 ... */
	[5440+0x00] = 47,	/* U+1D5C0: MATHEMATICAL SANS-SERIF SMALL G */
	[5440+0x01] = 49,	/* U+1D5C1: MATHEMATICAL SANS-SERIF SMALL H */
	[5440+0x02] = 40,	/* U+1D5C2: MATHEMATICAL SANS-SERIF SMALL I */
	[5440+0x03] = 53,	/* U+1D5C3: MATHEMATICAL SANS-SERIF SMALL J */
	[5440+0x04] = 55,	/* U+1D5C4: MATHEMATICAL SANS-SERIF SMALL K */
	[5440+0x05] = 58,	/* U+1D5C5: MATHEMATICAL SANS-SERIF SMALL L */
	[5440+0x06] = 93,	/* U+1D5C6: MATHEMATICAL SANS-SERIF SMALL M */
	[5440+0x07] = 42,	/* U+1D5C7: MATHEMATICAL SANS-SERIF SMALL N */
	[5440+0x08] = 20,	/* U+1D5C8: MATHEMATICAL SANS-SERIF SMALL O */
	[5440+0x09] = 80,	/* U+1D5C9: MATHEMATICAL SANS-SERIF SMALL P */
	[5440+0x0A] = 56,	/* U+1D5CA: MATHEMATICAL SANS-SERIF SMALL Q */
	[5440+0x0B] = 63,	/* U+1D5CB: MATHEMATICAL SANS-SERIF SMALL R */
	[5440+0x0C] = 65,	/* U+1D5CC: MATHEMATICAL SANS-SERIF SMALL S */
	[5440+0x0D] = 67,	/* U+1D5CD: MATHEMATICAL SANS-SERIF SMALL T */
	[5440+0x0E] = 16,	/* U+1D5CE: MATHEMATICAL SANS-SERIF SMALL U */
	[5440+0x0F] = 94,	/* U+1D5CF: MATHEMATICAL SANS-SERIF SMALL V */
	[5440+0x10] = 69,	/* U+1D5D0: MATHEMATICAL SANS-SERIF SMALL W */
	[5440+0x11] = 33,	/* U+1D5D1: MATHEMATICAL SANS-SERIF SMALL X */
	[5440+0x12] = 44,	/* U+1D5D2: MATHEMATICAL SANS-SERIF SMALL Y */
	[5440+0x13] = 71,	/* U+1D5D3: MATHEMATICAL SANS-SERIF SMALL Z */
	[5440+0x14] = 25,	/* U+1D5D4: MATHEMATICAL SANS-SERIF BOLD CAPITAL A */
	[5440+0x15] = 73,	/* U+1D5D5: MATHEMATICAL SANS-SERIF BOLD CAPITAL B */
	[5440+0x16] = 27,	/* U+1D5D6: MATHEMATICAL SANS-SERIF BOLD CAPITAL C */
	[5440+0x17] = 30,	/* U+1D5D7: MATHEMATICAL SANS-SERIF BOLD CAPITAL D */
	[5440+0x18] = 28,	/* U+1D5D8: MATHEMATICAL SANS-SERIF BOLD CAPITAL E */
	[5440+0x19] = 74,	/* U+1D5D9: MATHEMATICAL SANS-SERIF BOLD CAPITAL F */
	[5440+0x1A] = 46,	/* U+1D5DA: MATHEMATICAL SANS-SERIF BOLD CAPITAL G */
	[5440+0x1B] = 48,	/* U+1D5DB: MATHEMATICAL SANS-SERIF BOLD CAPITAL H */
	[5440+0x1C] = 29,	/* U+1D5DC: MATHEMATICAL SANS-SERIF BOLD CAPITAL I */
	[5440+0x1D] = 52,	/* U+1D5DD: MATHEMATICAL SANS-SERIF BOLD CAPITAL J */
	[5440+0x1E] = 54,	/* U+1D5DE: MATHEMATICAL SANS-SERIF BOLD CAPITAL K */
	[5440+0x1F] = 57,	/* U+1D5DF: MATHEMATICAL SANS-SERIF BOLD CAPITAL L */
	[5440+0x20] = 107,	/* U+1D5E0: MATHEMATICAL SANS-SERIF BOLD CAPITAL M */
	[5440+0x21] = 31,	/* U+1D5E1: MATHEMATICAL SANS-SERIF BOLD CAPITAL N */
	[5440+0x22] = 32,	/* U+1D5E2: MATHEMATICAL SANS-SERIF BOLD CAPITAL O */
	[5440+0x23] = 79,	/* U+1D5E3: MATHEMATICAL SANS-SERIF BOLD CAPITAL P */
	[5440+0x24] = 162,	/* U+1D5E4: MATHEMATICAL SANS-SERIF BOLD CAPITAL Q */
	[5440+0x25] = 62,	/* U+1D5E5: MATHEMATICAL SANS-SERIF BOLD CAPITAL R */
	[5440+0x26] = 64,	/* U+1D5E6: MATHEMATICAL SANS-SERIF BOLD CAPITAL S */
	[5440+0x27] = 66,	/* U+1D5E7: MATHEMATICAL SANS-SERIF BOLD CAPITAL T */
	[5440+0x28] = 34,	/* U+1D5E8: MATHEMATICAL SANS-SERIF BOLD CAPITAL U */
	[5440+0x29] = 81,	/* U+1D5E9: MATHEMATICAL SANS-SERIF BOLD CAPITAL V */
	[5440+0x2A] = 68,	/* U+1D5EA: MATHEMATICAL SANS-SERIF BOLD CAPITAL W */
	[5440+0x2B] = 109,	/* U+1D5EB: MATHEMATICAL SANS-SERIF BOLD CAPITAL X */
	[5440+0x2C] = 35,	/* U+1D5EC: MATHEMATICAL SANS-SERIF BOLD CAPITAL Y */
	[5440+0x2D] = 70,	/* U+1D5ED: MATHEMATICAL SANS-SERIF BOLD CAPITAL Z */
	[5440+0x2E] = 8,	/* U+1D5EE: MATHEMATICAL SANS-SERIF BOLD SMALL A */
	[5440+0x2F] = 72,	/* U+1D5EF: MATHEMATICAL SANS-SERIF BOLD SMALL B */
	[5440+0x30] = 3,	/* U+1D5F0: MATHEMATICAL SANS-SERIF BOLD SMALL C */
	[5440+0x31] = 41,	/* U+1D5F1: MATHEMATICAL SANS-SERIF BOLD SMALL D */
	[5440+0x32] = 39,	/* U+1D5F2: MATHEMATICAL SANS-SERIF BOLD SMALL E */
	[5440+0x33] = 75,	/* U+1D5F3: MATHEMATICAL SANS-SERIF BOLD SMALL F */
	[5440+0x34] = 47,	/* U+1D5F4: MATHEMATICAL SANS-SERIF BOLD SMALL G */
	[5440+0x35] = 49,	/* U+1D5F5: MATHEMATICAL SANS-SERIF BOLD SMALL H */
	[5440+0x36] = 40,	/* U+1D5F6: MATHEMATICAL SANS-SERIF BOLD SMALL I */
	[5440+0x37] = 53,	/* U+1D5F7: MATHEMATICAL SANS-SERIF BOLD SMALL J */
	[5440+0x38] = 55,	/* U+1D5F8: MATHEMATICAL SANS-SERIF BOLD SMALL K */
	[5440+0x39] = 58,	/* U+1D5F9: MATHEMATICAL SANS-SERIF BOLD SMALL L */
	[5440+0x3A] = 93,	/* U+1D5FA: MATHEMATICAL SANS-SERIF BOLD SMALL M */
	[5440+0x3B] = 42,	/* U+1D5FB: MATHEMATICAL SANS-SERIF BOLD SMALL N */
	[5440+0x3C] = 20,	/* U+1D5FC: MATHEMATICAL SANS-SERIF BOLD SMALL O */
	[5440+0x3D] = 80,	/* U+1D5FD: MATHEMATICAL SANS-SERIF BOLD SMALL P */
	[5440+0x3E] = 56,	/* U+1D5FE: MATHEMATICAL SANS-SERIF BOLD SMALL Q */
	[5440+0x3F] = 63,	/* U+1D5FF: MATHEMATICAL SANS-SERIF BOLD SMALL R */
	[4928+0x18] = 5504 - 0x80,	/* 360 235 230 ... */
	[5504+0x00] = 65,	/* U+1D600: MATHEMATICAL SANS-SERIF BOLD SMALL S */
	[5504+0x01] = 67,	/* U+1D601: MATHEMATICAL SANS-SERIF BOLD SMALL T */
	[5504+0x02] = 16,	/* U+1D602: MATHEMATICAL SANS-SERIF BOLD SMALL U */
	[5504+0x03] = 94,	/* U+1D603: MATHEMATICAL SANS-SERIF BOLD SMALL V */
	[5504+0x04] = 69,	/* U+1D604: MATHEMATICAL SANS-SERIF BOLD SMALL W */
	[5504+0x05] = 33,	/* U+1D605: MATHEMATICAL SANS-SERIF BOLD SMALL X */
	[5504+0x06] = 44,	/* U+1D606: MATHEMATICAL SANS-SERIF BOLD SMALL Y */
	[5504+0x07] = 71,	/* U+1D607: MATHEMATICAL SANS-SERIF BOLD SMALL Z */
	[5504+0x08] = 25,	/* U+1D608: MATHEMATICAL SANS-SERIF ITALIC CAPITAL A */
	[5504+0x09] = 73,	/* U+1D609: MATHEMATICAL SANS-SERIF ITALIC CAPITAL B */
	[5504+0x0A] = 27,	/* U+1D60A: MATHEMATICAL SANS-SERIF ITALIC CAPITAL C */
	[5504+0x0B] = 30,	/* U+1D60B: MATHEMATICAL SANS-SERIF ITALIC CAPITAL D */
	[5504+0x0C] = 28,	/* U+1D60C: MATHEMATICAL SANS-SERIF ITALIC CAPITAL E */
	[5504+0x0D] = 74,	/* U+1D60D: MATHEMATICAL SANS-SERIF ITALIC CAPITAL F */
	[5504+0x0E] = 46,	/* U+1D60E: MATHEMATICAL SANS-SERIF ITALIC CAPITAL G */
	[5504+0x0F] = 48,	/* U+1D60F: MATHEMATICAL SANS-SERIF ITALIC CAPITAL H */
	[5504+0x10] = 29,	/* U+1D610: MATHEMATICAL SANS-SERIF ITALIC CAPITAL I */
	[5504+0x11] = 52,	/* U+1D611: MATHEMATICAL SANS-SERIF ITALIC CAPITAL J */
	[5504+0x12] = 54,	/* U+1D612: MATHEMATICAL SANS-SERIF ITALIC CAPITAL K */
	[5504+0x13] = 57,	/* U+1D613: MATHEMATICAL SANS-SERIF ITALIC CAPITAL L */
	[5504+0x14] = 107,	/* U+1D614: MATHEMATICAL SANS-SERIF ITALIC CAPITAL M */
	[5504+0x15] = 31,	/* U+1D615: MATHEMATICAL SANS-SERIF ITALIC CAPITAL N */
	[5504+0x16] = 32,	/* U+1D616: MATHEMATICAL SANS-SERIF ITALIC CAPITAL O */
	[5504+0x17] = 79,	/* U+1D617: MATHEMATICAL SANS-SERIF ITALIC CAPITAL P */
	[5504+0x18] = 162,	/* U+1D618: MATHEMATICAL SANS-SERIF ITALIC CAPITAL Q */
	[5504+0x19] = 62,	/* U+1D619: MATHEMATICAL SANS-SERIF ITALIC CAPITAL R */
	[5504+0x1A] = 64,	/* U+1D61A: MATHEMATICAL SANS-SERIF ITALIC CAPITAL S */
	[5504+0x1B] = 66,	/* U+1D61B: MATHEMATICAL SANS-SERIF ITALIC CAPITAL T */
	[5504+0x1C] = 34,	/* U+1D61C: MATHEMATICAL SANS-SERIF ITALIC CAPITAL U */
	[5504+0x1D] = 81,	/* U+1D61D: MATHEMATICAL SANS-SERIF ITALIC CAPITAL V */
	[5504+0x1E] = 68,	/* U+1D61E: MATHEMATICAL SANS-SERIF ITALIC CAPITAL W */
	[5504+0x1F] = 109,	/* U+1D61F: MATHEMATICAL SANS-SERIF ITALIC CAPITAL X */
	[5504+0x20] = 35,	/* U+1D620: MATHEMATICAL SANS-SERIF ITALIC CAPITAL Y */
	[5504+0x21] = 70,	/* U+1D621: MATHEMATICAL SANS-SERIF ITALIC CAPITAL Z */
	[5504+0x22] = 8,	/* U+1D622: MATHEMATICAL SANS-SERIF ITALIC SMALL A */
	[5504+0x23] = 72,	/* U+1D623: MATHEMATICAL SANS-SERIF ITALIC SMALL B */
	[5504+0x24] = 3,	/* U+1D624: MATHEMATICAL SANS-SERIF ITALIC SMALL C */
	[5504+0x25] = 41,	/* U+1D625: MATHEMATICAL SANS-SERIF ITALIC SMALL D */
	[5504+0x26] = 39,	/* U+1D626: MATHEMATICAL SANS-SERIF ITALIC SMALL E */
	[5504+0x27] = 75,	/* U+1D627: MATHEMATICAL SANS-SERIF ITALIC SMALL F */
	[5504+0x28] = 47,	/* U+1D628: MATHEMATICAL SANS-SERIF ITALIC SMALL G */
	[5504+0x29] = 49,	/* U+1D629: MATHEMATICAL SANS-SERIF ITALIC SMALL H */
	[5504+0x2A] = 40,	/* U+1D62A: MATHEMATICAL SANS-SERIF ITALIC SMALL I */
	[5504+0x2B] = 53,	/* U+1D62B: MATHEMATICAL SANS-SERIF ITALIC SMALL J */
	[5504+0x2C] = 55,	/* U+1D62C: MATHEMATICAL SANS-SERIF ITALIC SMALL K */
	[5504+0x2D] = 58,	/* U+1D62D: MATHEMATICAL SANS-SERIF ITALIC SMALL L */
	[5504+0x2E] = 93,	/* U+1D62E: MATHEMATICAL SANS-SERIF ITALIC SMALL M */
	[5504+0x2F] = 42,	/* U+1D62F: MATHEMATICAL SANS-SERIF ITALIC SMALL N */
	[5504+0x30] = 20,	/* U+1D630: MATHEMATICAL SANS-SERIF ITALIC SMALL O */
	[5504+0x31] = 80,	/* U+1D631: MATHEMATICAL SANS-SERIF ITALIC SMALL P */
	[5504+0x32] = 56,	/* U+1D632: MATHEMATICAL SANS-SERIF ITALIC SMALL Q */
	[5504+0x33] = 63,	/* U+1D633: MATHEMATICAL SANS-SERIF ITALIC SMALL R */
	[5504+0x34] = 65,	/* U+1D634: MATHEMATICAL SANS-SERIF ITALIC SMALL S */
	[5504+0x35] = 67,	/* U+1D635: MATHEMATICAL SANS-SERIF ITALIC SMALL T */
	[5504+0x36] = 16,	/* U+1D636: MATHEMATICAL SANS-SERIF ITALIC SMALL U */
	[5504+0x37] = 94,	/* U+1D637: MATHEMATICAL SANS-SERIF ITALIC SMALL V */
	[5504+0x38] = 69,	/* U+1D638: MATHEMATICAL SANS-SERIF ITALIC SMALL W */
	[5504+0x39] = 33,	/* U+1D639: MATHEMATICAL SANS-SERIF ITALIC SMALL X */
	[5504+0x3A] = 44,	/* U+1D63A: MATHEMATICAL SANS-SERIF ITALIC SMALL Y */
	[5504+0x3B] = 71,	/* U+1D63B: MATHEMATICAL SANS-SERIF ITALIC SMALL Z */
	[5504+0x3C] = 25,	/* U+1D63C: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL A */
	[5504+0x3D] = 73,	/* U+1D63D: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL B */
	[5504+0x3E] = 27,	/* U+1D63E: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL C */
	[5504+0x3F] = 30,	/* U+1D63F: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL D */
	[4928+0x19] = 5568 - 0x80,	/* 360 235 231 ... */
	[5568+0x00] = 28,	/* U+1D640: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL E */
	[5568+0x01] = 74,	/* U+1D641: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL F */
	[5568+0x02] = 46,	/* U+1D642: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL G */
	[5568+0x03] = 48,	/* U+1D643: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL H */
	[5568+0x04] = 29,	/* U+1D644: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL I */
	[5568+0x05] = 52,	/* U+1D645: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL J */
	[5568+0x06] = 54,	/* U+1D646: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL K */
	[5568+0x07] = 57,	/* U+1D647: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL L */
	[5568+0x08] = 107,	/* U+1D648: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL M */
	[5568+0x09] = 31,	/* U+1D649: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL N */
	[5568+0x0A] = 32,	/* U+1D64A: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL O */
	[5568+0x0B] = 79,	/* U+1D64B: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL P */
	[5568+0x0C] = 162,	/* U+1D64C: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Q */
	[5568+0x0D] = 62,	/* U+1D64D: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL R */
	[5568+0x0E] = 64,	/* U+1D64E: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL S */
	[5568+0x0F] = 66,	/* U+1D64F: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL T */
	[5568+0x10] = 34,	/* U+1D650: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL U */
	[5568+0x11] = 81,	/* U+1D651: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL V */
	[5568+0x12] = 68,	/* U+1D652: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL W */
	[5568+0x13] = 109,	/* U+1D653: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL X */
	[5568+0x14] = 35,	/* U+1D654: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Y */
	[5568+0x15] = 70,	/* U+1D655: MATHEMATICAL SANS-SERIF BOLD ITALIC CAPITAL Z */
	[5568+0x16] = 8,	/* U+1D656: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL A */
	[5568+0x17] = 72,	/* U+1D657: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL B */
	[5568+0x18] = 3,	/* U+1D658: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL C */
	[5568+0x19] = 41,	/* U+1D659: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL D */
	[5568+0x1A] = 39,	/* U+1D65A: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL E */
	[5568+0x1B] = 75,	/* U+1D65B: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL F */
	[5568+0x1C] = 47,	/* U+1D65C: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL G */
	[5568+0x1D] = 49,	/* U+1D65D: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL H */
	[5568+0x1E] = 40,	/* U+1D65E: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL I */
	[5568+0x1F] = 53,	/* U+1D65F: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL J */
	[5568+0x20] = 55,	/* U+1D660: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL K */
	[5568+0x21] = 58,	/* U+1D661: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL L */
	[5568+0x22] = 93,	/* U+1D662: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL M */
	[5568+0x23] = 42,	/* U+1D663: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL N */
	[5568+0x24] = 20,	/* U+1D664: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL O */
	[5568+0x25] = 80,	/* U+1D665: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL P */
	[5568+0x26] = 56,	/* U+1D666: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Q */
	[5568+0x27] = 63,	/* U+1D667: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL R */
	[5568+0x28] = 65,	/* U+1D668: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL S */
	[5568+0x29] = 67,	/* U+1D669: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL T */
	[5568+0x2A] = 16,	/* U+1D66A: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL U */
	[5568+0x2B] = 94,	/* U+1D66B: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL V */
	[5568+0x2C] = 69,	/* U+1D66C: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL W */
	[5568+0x2D] = 33,	/* U+1D66D: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL X */
	[5568+0x2E] = 44,	/* U+1D66E: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Y */
	[5568+0x2F] = 71,	/* U+1D66F: MATHEMATICAL SANS-SERIF BOLD ITALIC SMALL Z */
	[5568+0x30] = 25,	/* U+1D670: MATHEMATICAL MONOSPACE CAPITAL A */
	[5568+0x31] = 73,	/* U+1D671: MATHEMATICAL MONOSPACE CAPITAL B */
	[5568+0x32] = 27,	/* U+1D672: MATHEMATICAL MONOSPACE CAPITAL C */
	[5568+0x33] = 30,	/* U+1D673: MATHEMATICAL MONOSPACE CAPITAL D */
	[5568+0x34] = 28,	/* U+1D674: MATHEMATICAL MONOSPACE CAPITAL E */
	[5568+0x35] = 74,	/* U+1D675: MATHEMATICAL MONOSPACE CAPITAL F */
	[5568+0x36] = 46,	/* U+1D676: MATHEMATICAL MONOSPACE CAPITAL G */
	[5568+0x37] = 48,	/* U+1D677: MATHEMATICAL MONOSPACE CAPITAL H */
	[5568+0x38] = 29,	/* U+1D678: MATHEMATICAL MONOSPACE CAPITAL I */
	[5568+0x39] = 52,	/* U+1D679: MATHEMATICAL MONOSPACE CAPITAL J */
	[5568+0x3A] = 54,	/* U+1D67A: MATHEMATICAL MONOSPACE CAPITAL K */
	[5568+0x3B] = 57,	/* U+1D67B: MATHEMATICAL MONOSPACE CAPITAL L */
	[5568+0x3C] = 107,	/* U+1D67C: MATHEMATICAL MONOSPACE CAPITAL M */
	[5568+0x3D] = 31,	/* U+1D67D: MATHEMATICAL MONOSPACE CAPITAL N */
	[5568+0x3E] = 32,	/* U+1D67E: MATHEMATICAL MONOSPACE CAPITAL O */
	[5568+0x3F] = 79,	/* U+1D67F: MATHEMATICAL MONOSPACE CAPITAL P */
	[4928+0x1A] = 5632 - 0x80,	/* 360 235 232 ... */
	[5632+0x00] = 162,	/* U+1D680: MATHEMATICAL MONOSPACE CAPITAL Q */
	[5632+0x01] = 62,	/* U+1D681: MATHEMATICAL MONOSPACE CAPITAL R */
	[5632+0x02] = 64,	/* U+1D682: MATHEMATICAL MONOSPACE CAPITAL S */
	[5632+0x03] = 66,	/* U+1D683: MATHEMATICAL MONOSPACE CAPITAL T */
	[5632+0x04] = 34,	/* U+1D684: MATHEMATICAL MONOSPACE CAPITAL U */
	[5632+0x05] = 81,	/* U+1D685: MATHEMATICAL MONOSPACE CAPITAL V */
	[5632+0x06] = 68,	/* U+1D686: MATHEMATICAL MONOSPACE CAPITAL W */
	[5632+0x07] = 109,	/* U+1D687: MATHEMATICAL MONOSPACE CAPITAL X */
	[5632+0x08] = 35,	/* U+1D688: MATHEMATICAL MONOSPACE CAPITAL Y */
	[5632+0x09] = 70,	/* U+1D689: MATHEMATICAL MONOSPACE CAPITAL Z */
	[5632+0x0A] = 8,	/* U+1D68A: MATHEMATICAL MONOSPACE SMALL A */
	[5632+0x0B] = 72,	/* U+1D68B: MATHEMATICAL MONOSPACE SMALL B */
	[5632+0x0C] = 3,	/* U+1D68C: MATHEMATICAL MONOSPACE SMALL C */
	[5632+0x0D] = 41,	/* U+1D68D: MATHEMATICAL MONOSPACE SMALL D */
	[5632+0x0E] = 39,	/* U+1D68E: MATHEMATICAL MONOSPACE SMALL E */
	[5632+0x0F] = 75,	/* U+1D68F: MATHEMATICAL MONOSPACE SMALL F */
	[5632+0x10] = 47,	/* U+1D690: MATHEMATICAL MONOSPACE SMALL G */
	[5632+0x11] = 49,	/* U+1D691: MATHEMATICAL MONOSPACE SMALL H */
	[5632+0x12] = 40,	/* U+1D692: MATHEMATICAL MONOSPACE SMALL I */
	[5632+0x13] = 53,	/* U+1D693: MATHEMATICAL MONOSPACE SMALL J */
	[5632+0x14] = 55,	/* U+1D694: MATHEMATICAL MONOSPACE SMALL K */
	[5632+0x15] = 58,	/* U+1D695: MATHEMATICAL MONOSPACE SMALL L */
	[5632+0x16] = 93,	/* U+1D696: MATHEMATICAL MONOSPACE SMALL M */
	[5632+0x17] = 42,	/* U+1D697: MATHEMATICAL MONOSPACE SMALL N */
	[5632+0x18] = 20,	/* U+1D698: MATHEMATICAL MONOSPACE SMALL O */
	[5632+0x19] = 80,	/* U+1D699: MATHEMATICAL MONOSPACE SMALL P */
	[5632+0x1A] = 56,	/* U+1D69A: MATHEMATICAL MONOSPACE SMALL Q */
	[5632+0x1B] = 63,	/* U+1D69B: MATHEMATICAL MONOSPACE SMALL R */
	[5632+0x1C] = 65,	/* U+1D69C: MATHEMATICAL MONOSPACE SMALL S */
	[5632+0x1D] = 67,	/* U+1D69D: MATHEMATICAL MONOSPACE SMALL T */
	[5632+0x1E] = 16,	/* U+1D69E: MATHEMATICAL MONOSPACE SMALL U */
	[5632+0x1F] = 94,	/* U+1D69F: MATHEMATICAL MONOSPACE SMALL V */
	[5632+0x20] = 69,	/* U+1D6A0: MATHEMATICAL MONOSPACE SMALL W */
	[5632+0x21] = 33,	/* U+1D6A1: MATHEMATICAL MONOSPACE SMALL X */
	[5632+0x22] = 44,	/* U+1D6A2: MATHEMATICAL MONOSPACE SMALL Y */
	[5632+0x23] = 71,	/* U+1D6A3: MATHEMATICAL MONOSPACE SMALL Z */
	[4928+0x1F] = 5696 - 0x80,	/* 360 235 237 ... */
	[5696+0x0E] = 128,	/* U+1D7CE: MATHEMATICAL BOLD DIGIT ZERO */
	[5696+0x0F] = 19,	/* U+1D7CF: MATHEMATICAL BOLD DIGIT ONE */
	[5696+0x10] = 13,	/* U+1D7D0: MATHEMATICAL BOLD DIGIT TWO */
	[5696+0x11] = 14,	/* U+1D7D1: MATHEMATICAL BOLD DIGIT THREE */
	[5696+0x12] = 129,	/* U+1D7D2: MATHEMATICAL BOLD DIGIT FOUR */
	[5696+0x13] = 130,	/* U+1D7D3: MATHEMATICAL BOLD DIGIT FIVE */
	[5696+0x14] = 131,	/* U+1D7D4: MATHEMATICAL BOLD DIGIT SIX */
	[5696+0x15] = 132,	/* U+1D7D5: MATHEMATICAL BOLD DIGIT SEVEN */
	[5696+0x16] = 133,	/* U+1D7D6: MATHEMATICAL BOLD DIGIT EIGHT */
	[5696+0x17] = 134,	/* U+1D7D7: MATHEMATICAL BOLD DIGIT NINE */
	[5696+0x18] = 128,	/* U+1D7D8: MATHEMATICAL DOUBLE-STRUCK DIGIT ZERO */
	[5696+0x19] = 19,	/* U+1D7D9: MATHEMATICAL DOUBLE-STRUCK DIGIT ONE */
	[5696+0x1A] = 13,	/* U+1D7DA: MATHEMATICAL DOUBLE-STRUCK DIGIT TWO */
	[5696+0x1B] = 14,	/* U+1D7DB: MATHEMATICAL DOUBLE-STRUCK DIGIT THREE */
	[5696+0x1C] = 129,	/* U+1D7DC: MATHEMATICAL DOUBLE-STRUCK DIGIT FOUR */
	[5696+0x1D] = 130,	/* U+1D7DD: MATHEMATICAL DOUBLE-STRUCK DIGIT FIVE */
	[5696+0x1E] = 131,	/* U+1D7DE: MATHEMATICAL DOUBLE-STRUCK DIGIT SIX */
	[5696+0x1F] = 132,	/* U+1D7DF: MATHEMATICAL DOUBLE-STRUCK DIGIT SEVEN */
	[5696+0x20] = 133,	/* U+1D7E0: MATHEMATICAL DOUBLE-STRUCK DIGIT EIGHT */
	[5696+0x21] = 134,	/* U+1D7E1: MATHEMATICAL DOUBLE-STRUCK DIGIT NINE */
	[5696+0x22] = 128,	/* U+1D7E2: MATHEMATICAL SANS-SERIF DIGIT ZERO */
	[5696+0x23] = 19,	/* U+1D7E3: MATHEMATICAL SANS-SERIF DIGIT ONE */
	[5696+0x24] = 13,	/* U+1D7E4: MATHEMATICAL SANS-SERIF DIGIT TWO */
	[5696+0x25] = 14,	/* U+1D7E5: MATHEMATICAL SANS-SERIF DIGIT THREE */
	[5696+0x26] = 129,	/* U+1D7E6: MATHEMATICAL SANS-SERIF DIGIT FOUR */
	[5696+0x27] = 130,	/* U+1D7E7: MATHEMATICAL SANS-SERIF DIGIT FIVE */
	[5696+0x28] = 131,	/* U+1D7E8: MATHEMATICAL SANS-SERIF DIGIT SIX */
	[5696+0x29] = 132,	/* U+1D7E9: MATHEMATICAL SANS-SERIF DIGIT SEVEN */
	[5696+0x2A] = 133,	/* U+1D7EA: MATHEMATICAL SANS-SERIF DIGIT EIGHT */
	[5696+0x2B] = 134,	/* U+1D7EB: MATHEMATICAL SANS-SERIF DIGIT NINE */
	[5696+0x2C] = 128,	/* U+1D7EC: MATHEMATICAL SANS-SERIF BOLD DIGIT ZERO */
	[5696+0x2D] = 19,	/* U+1D7ED: MATHEMATICAL SANS-SERIF BOLD DIGIT ONE */
	[5696+0x2E] = 13,	/* U+1D7EE: MATHEMATICAL SANS-SERIF BOLD DIGIT TWO */
	[5696+0x2F] = 14,	/* U+1D7EF: MATHEMATICAL SANS-SERIF BOLD DIGIT THREE */
	[5696+0x30] = 129,	/* U+1D7F0: MATHEMATICAL SANS-SERIF BOLD DIGIT FOUR */
	[5696+0x31] = 130,	/* U+1D7F1: MATHEMATICAL SANS-SERIF BOLD DIGIT FIVE */
	[5696+0x32] = 131,	/* U+1D7F2: MATHEMATICAL SANS-SERIF BOLD DIGIT SIX */
	[5696+0x33] = 132,	/* U+1D7F3: MATHEMATICAL SANS-SERIF BOLD DIGIT SEVEN */
	[5696+0x34] = 133,	/* U+1D7F4: MATHEMATICAL SANS-SERIF BOLD DIGIT EIGHT */
	[5696+0x35] = 134,	/* U+1D7F5: MATHEMATICAL SANS-SERIF BOLD DIGIT NINE */
	[5696+0x36] = 128,	/* U+1D7F6: MATHEMATICAL MONOSPACE DIGIT ZERO */
	[5696+0x37] = 19,	/* U+1D7F7: MATHEMATICAL MONOSPACE DIGIT ONE */
	[5696+0x38] = 13,	/* U+1D7F8: MATHEMATICAL MONOSPACE DIGIT TWO */
	[5696+0x39] = 14,	/* U+1D7F9: MATHEMATICAL MONOSPACE DIGIT THREE */
	[5696+0x3A] = 129,	/* U+1D7FA: MATHEMATICAL MONOSPACE DIGIT FOUR */
	[5696+0x3B] = 130,	/* U+1D7FB: MATHEMATICAL MONOSPACE DIGIT FIVE */
	[5696+0x3C] = 131,	/* U+1D7FC: MATHEMATICAL MONOSPACE DIGIT SIX */
	[5696+0x3D] = 132,	/* U+1D7FD: MATHEMATICAL MONOSPACE DIGIT SEVEN */
	[5696+0x3E] = 133,	/* U+1D7FE: MATHEMATICAL MONOSPACE DIGIT EIGHT */
	[5696+0x3F] = 134,	/* U+1D7FF: MATHEMATICAL MONOSPACE DIGIT NINE */
	[4736+0x1F] = 5760 - 0x80,	/* 360 237 ... */
	[5760+0x04] = 5824 - 0x80,	/* 360 237 204 ... */
	[5824+0x00] = 527,	/* U+1F100: DIGIT ZERO FULL STOP */
	[5824+0x01] = 528,	/* U+1F101: DIGIT ZERO COMMA */
	[5824+0x02] = 529,	/* U+1F102: DIGIT ONE COMMA */
	[5824+0x03] = 530,	/* U+1F103: DIGIT TWO COMMA */
	[5824+0x04] = 531,	/* U+1F104: DIGIT THREE COMMA */
	[5824+0x05] = 532,	/* U+1F105: DIGIT FOUR COMMA */
	[5824+0x06] = 533,	/* U+1F106: DIGIT FIVE COMMA */
	[5824+0x07] = 534,	/* U+1F107: DIGIT SIX COMMA */
	[5824+0x08] = 535,	/* U+1F108: DIGIT SEVEN COMMA */
	[5824+0x09] = 536,	/* U+1F109: DIGIT EIGHT COMMA */
	[5824+0x0A] = 537,	/* U+1F10A: DIGIT NINE COMMA */
	[5824+0x10] = 333,	/* U+1F110: PARENTHESIZED LATIN CAPITAL LETTER A */
	[5824+0x11] = 334,	/* U+1F111: PARENTHESIZED LATIN CAPITAL LETTER B */
	[5824+0x12] = 7,	/* U+1F112: PARENTHESIZED LATIN CAPITAL LETTER C */
	[5824+0x13] = 335,	/* U+1F113: PARENTHESIZED LATIN CAPITAL LETTER D */
	[5824+0x14] = 336,	/* U+1F114: PARENTHESIZED LATIN CAPITAL LETTER E */
	[5824+0x15] = 337,	/* U+1F115: PARENTHESIZED LATIN CAPITAL LETTER F */
	[5824+0x16] = 338,	/* U+1F116: PARENTHESIZED LATIN CAPITAL LETTER G */
	[5824+0x17] = 339,	/* U+1F117: PARENTHESIZED LATIN CAPITAL LETTER H */
	[5824+0x18] = 340,	/* U+1F118: PARENTHESIZED LATIN CAPITAL LETTER I */
	[5824+0x19] = 341,	/* U+1F119: PARENTHESIZED LATIN CAPITAL LETTER J */
	[5824+0x1A] = 342,	/* U+1F11A: PARENTHESIZED LATIN CAPITAL LETTER K */
	[5824+0x1B] = 343,	/* U+1F11B: PARENTHESIZED LATIN CAPITAL LETTER L */
	[5824+0x1C] = 344,	/* U+1F11C: PARENTHESIZED LATIN CAPITAL LETTER M */
	[5824+0x1D] = 345,	/* U+1F11D: PARENTHESIZED LATIN CAPITAL LETTER N */
	[5824+0x1E] = 346,	/* U+1F11E: PARENTHESIZED LATIN CAPITAL LETTER O */
	[5824+0x1F] = 347,	/* U+1F11F: PARENTHESIZED LATIN CAPITAL LETTER P */
	[5824+0x20] = 348,	/* U+1F120: PARENTHESIZED LATIN CAPITAL LETTER Q */
	[5824+0x21] = 11,	/* U+1F121: PARENTHESIZED LATIN CAPITAL LETTER R */
	[5824+0x22] = 349,	/* U+1F122: PARENTHESIZED LATIN CAPITAL LETTER S */
	[5824+0x23] = 350,	/* U+1F123: PARENTHESIZED LATIN CAPITAL LETTER T */
	[5824+0x24] = 351,	/* U+1F124: PARENTHESIZED LATIN CAPITAL LETTER U */
	[5824+0x25] = 352,	/* U+1F125: PARENTHESIZED LATIN CAPITAL LETTER V */
	[5824+0x26] = 353,	/* U+1F126: PARENTHESIZED LATIN CAPITAL LETTER W */
	[5824+0x27] = 354,	/* U+1F127: PARENTHESIZED LATIN CAPITAL LETTER X */
	[5824+0x28] = 355,	/* U+1F128: PARENTHESIZED LATIN CAPITAL LETTER Y */
	[5824+0x29] = 356,	/* U+1F129: PARENTHESIZED LATIN CAPITAL LETTER Z */
	[5824+0x2B] = 7,	/* U+1F12B: CIRCLED ITALIC LATIN CAPITAL LETTER C */
	[5824+0x2C] = 11,	/* U+1F12C: CIRCLED ITALIC LATIN CAPITAL LETTER R */
	[5824+0x2D] = 538,	/* U+1F12D: CIRCLED CD */
	[5824+0x2E] = 539,	/* U+1F12E: CIRCLED WZ */
	[5824+0x30] = 25,	/* U+1F130: SQUARED LATIN CAPITAL LETTER A */
	[5824+0x31] = 73,	/* U+1F131: SQUARED LATIN CAPITAL LETTER B */
	[5824+0x32] = 27,	/* U+1F132: SQUARED LATIN CAPITAL LETTER C */
	[5824+0x33] = 30,	/* U+1F133: SQUARED LATIN CAPITAL LETTER D */
	[5824+0x34] = 28,	/* U+1F134: SQUARED LATIN CAPITAL LETTER E */
	[5824+0x35] = 74,	/* U+1F135: SQUARED LATIN CAPITAL LETTER F */
	[5824+0x36] = 46,	/* U+1F136: SQUARED LATIN CAPITAL LETTER G */
	[5824+0x37] = 48,	/* U+1F137: SQUARED LATIN CAPITAL LETTER H */
	[5824+0x38] = 29,	/* U+1F138: SQUARED LATIN CAPITAL LETTER I */
	[5824+0x39] = 52,	/* U+1F139: SQUARED LATIN CAPITAL LETTER J */
	[5824+0x3A] = 54,	/* U+1F13A: SQUARED LATIN CAPITAL LETTER K */
	[5824+0x3B] = 57,	/* U+1F13B: SQUARED LATIN CAPITAL LETTER L */
	[5824+0x3C] = 107,	/* U+1F13C: SQUARED LATIN CAPITAL LETTER M */
	[5824+0x3D] = 31,	/* U+1F13D: SQUARED LATIN CAPITAL LETTER N */
	[5824+0x3E] = 32,	/* U+1F13E: SQUARED LATIN CAPITAL LETTER O */
	[5824+0x3F] = 79,	/* U+1F13F: SQUARED LATIN CAPITAL LETTER P */
	[5760+0x05] = 5888 - 0x80,	/* 360 237 205 ... */
	[5888+0x00] = 162,	/* U+1F140: SQUARED LATIN CAPITAL LETTER Q */
	[5888+0x01] = 62,	/* U+1F141: SQUARED LATIN CAPITAL LETTER R */
	[5888+0x02] = 64,	/* U+1F142: SQUARED LATIN CAPITAL LETTER S */
	[5888+0x03] = 66,	/* U+1F143: SQUARED LATIN CAPITAL LETTER T */
	[5888+0x04] = 34,	/* U+1F144: SQUARED LATIN CAPITAL LETTER U */
	[5888+0x05] = 81,	/* U+1F145: SQUARED LATIN CAPITAL LETTER V */
	[5888+0x06] = 68,	/* U+1F146: SQUARED LATIN CAPITAL LETTER W */
	[5888+0x07] = 109,	/* U+1F147: SQUARED LATIN CAPITAL LETTER X */
	[5888+0x08] = 35,	/* U+1F148: SQUARED LATIN CAPITAL LETTER Y */
	[5888+0x09] = 70,	/* U+1F149: SQUARED LATIN CAPITAL LETTER Z */
	[5888+0x0A] = 540,	/* U+1F14A: SQUARED HV */
	[5888+0x0B] = 476,	/* U+1F14B: SQUARED MV */
	[5888+0x0C] = 541,	/* U+1F14C: SQUARED SD */
	[5888+0x0D] = 110,	/* U+1F14D: SQUARED SS */
	[5888+0x0E] = 542,	/* U+1F14E: SQUARED PPV */
	[5888+0x0F] = 543,	/* U+1F14F: SQUARED WC */
	[5888+0x2A] = 544,	/* U+1F16A: RAISED MC SIGN */
	[5888+0x2B] = 545,	/* U+1F16B: RAISED MD SIGN */
	[5888+0x2C] = 546,	/* U+1F16C: RAISED MR SIGN */
	[5760+0x06] = 5952 - 0x80,	/* 360 237 206 ... */
	[5952+0x10] = 547,	/* U+1F190: SQUARE DJ */
	[5760+0x2F] = 6016 - 0x80,	/* 360 237 257 ... */
	[6016+0x30] = 128,	/* U+1FBF0: SEGMENTED DIGIT ZERO */
	[6016+0x31] = 19,	/* U+1FBF1: SEGMENTED DIGIT ONE */
	[6016+0x32] = 13,	/* U+1FBF2: SEGMENTED DIGIT TWO */
	[6016+0x33] = 14,	/* U+1FBF3: SEGMENTED DIGIT THREE */
	[6016+0x34] = 129,	/* U+1FBF4: SEGMENTED DIGIT FOUR */
	[6016+0x35] = 130,	/* U+1FBF5: SEGMENTED DIGIT FIVE */
	[6016+0x36] = 131,	/* U+1FBF6: SEGMENTED DIGIT SIX */
	[6016+0x37] = 132,	/* U+1FBF7: SEGMENTED DIGIT SEVEN */
	[6016+0x38] = 133,	/* U+1FBF8: SEGMENTED DIGIT EIGHT */
	[6016+0x39] = 134,	/* U+1FBF9: SEGMENTED DIGIT NINE */
};

gdk_return
GDKasciify(char **restrict buf, size_t *restrict buflen,
	   const char *restrict s)
{
	uint8_t *dst = (uint8_t *) *buf;
	size_t dstoff = 0;
	size_t bl;

	bl = strlen(s);
	if (*buf == NULL) {
		if (bl < 4090)
			bl = 4096;
		else
			bl += 8;
		dst = GDKmalloc(bl);
		if (dst == NULL)
			return GDK_FAIL;
		*buf = (char *) dst;
	} else if (bl + 8 > *buflen) {
		bl += 1024;
		dst = GDKrealloc(*buf, bl);
		if (dst == NULL)
			return GDK_FAIL;
		*buf = (char *) dst;
	} else {
		bl = *buflen;
	}

	size_t bl8 = bl - 8;	/* time for buffer extension */
	if ((uint8_t) *s == 0x80) {
		/* nil */
		assert(s[1] == 0);
		dst[dstoff++] = 0x80;
	} else {
		/* special case for ASCII prefix */
		while ((int8_t) *s > 0) {
			/* no need to check for buffer size, we made
			 * sure there is enough space */
			dst[dstoff++] = (uint8_t) *s++;
		}
		while (*s) {
			/* we are at the start of a Unicode codepoint
			 * encoded in UTF-8 */
			if (dstoff > bl8) {
				/* make sure we have enough space for
				 * the largest asciification, i.e. 7
				 * bytes plus terminating NUL */
				size_t newlen = bl + 1024;
				dst = GDKrealloc(*buf, newlen);
				if (dst == NULL) {
					*buflen = bl;
					return GDK_FAIL;
				}
				*buf = (char *) dst;
				bl = newlen;
				bl8 = bl - 8;
			}
			const uint8_t *u = (const uint8_t *) s;
			int v = asciify[(uint8_t) *s++];
			while (v && ((uint8_t) *s & 0xC0) == 0x80) {
				v = asciify[v + (uint8_t) *s++];
			}
			if (v == 0) {
				if ((int8_t) *u > 0)
					dst[dstoff++] = *u;
				else
					dst[dstoff++] = '?';
				/* skip rest of current codepoint */
				while (((uint8_t) *s & 0xC0) == 0x80)
					s++;
			} else {
				for (u = (const uint8_t *) valtab[v]; *u; u++)
					dst[dstoff++] = *u;
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

BAT *
BATasciify(BAT *b, BAT *s)
{
	if (b->tascii) {
		if (s)
			return BATproject(s, b);
		return COLcopy(b, TYPE_str, false, TRANSIENT);
	}

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
		if (GDKasciify(&buf, &buflen, BUNtvar(bi, x)) != GDK_SUCCEED ||
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
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOBATFMT " " LLFMT "usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(s),
		  ALGOBATPAR(bn), GDKusec() - t0);
	return bn;

  bailout:
	GDKfree(buf);
	bat_iterator_end(&bi);
	BBPreclaim(bn);
	return NULL;
}
