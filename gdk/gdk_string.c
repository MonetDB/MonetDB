/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

void
strHeap(Heap *d, size_t cap)
{
	size_t size;

	cap = MAX(cap, BATTINY);
	size = GDK_STRHASHTABLE * sizeof(stridx_t) + MIN(GDK_ELIMLIMIT, cap * GDK_VARALIGN);
	if (HEAPalloc(d, size, 1) == GDK_SUCCEED) {
		d->free = GDK_STRHASHTABLE * sizeof(stridx_t);
		d->dirty = true;
		memset(d->base, 0, d->free);
		d->hashash = false;
#ifndef NDEBUG
		/* fill should solve initialization problems within valgrind */
		memset(d->base + d->free, 0, d->size - d->free);
#endif
	}
}


void
strCleanHash(Heap *h, bool rebuild)
{
	stridx_t newhash[GDK_STRHASHTABLE];
	size_t pad, pos;
	const size_t extralen = h->hashash ? EXTRALEN : 0;
	BUN off, strhash;
	const char *s;

	(void) rebuild;
	if (!h->cleanhash)
		return;
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
	while (pos < h->free && pos < GDK_ELIMLIMIT) {
		pad = GDK_VARALIGN - (pos & (GDK_VARALIGN - 1));
		if (pad < sizeof(stridx_t))
			pad += GDK_VARALIGN;
		pos += pad + extralen;
		s = h->base + pos;
		if (h->hashash)
			strhash = ((const BUN *) s)[-1];
		else
			strhash = strHash(s);
		off = strhash & GDK_STRHASHMASK;
		newhash[off] = (stridx_t) (pos - extralen - sizeof(stridx_t));
		pos += strLen(s);
	}
	/* only set dirty flag if the hash table actually changed */
	if (memcmp(newhash, h->base, sizeof(newhash)) != 0) {
		memcpy(h->base, newhash, sizeof(newhash));
		if (h->storage == STORE_MMAP) {
			if (!(GDKdebug & NOSYNCMASK))
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
			pos += pad + extralen;
			s = h->base + pos;
			assert(strLocate(h, s) != 0);
			pos += strLen(s);
		}
	}
#endif
	h->cleanhash = false;
}

/*
 * The strPut routine. The routine strLocate can be used to identify
 * the location of a string in the heap if it exists. Otherwise it
 * returns zero.
 */
var_t
strLocate(Heap *h, const char *v)
{
	stridx_t *ref, *next;
	const size_t extralen = h->hashash ? EXTRALEN : 0;

	/* search hash-table, if double-elimination is still in place */
	BUN off;
	off = strHash(v);
	off &= GDK_STRHASHMASK;

	/* should only use strLocate iff fully double eliminated */
	assert(GDK_ELIMBASE(h->free) == 0);

	/* search the linked list */
	for (ref = ((stridx_t *) h->base) + off; *ref; ref = next) {
		next = (stridx_t *) (h->base + *ref);
		if (strCmp(v, (str) (next + 1) + extralen) == 0)
			return (var_t) ((sizeof(stridx_t) + *ref + extralen));	/* found */
	}
	return 0;
}

var_t
strPut(Heap *h, var_t *dst, const char *v)
{
	size_t elimbase = GDK_ELIMBASE(h->free);
	size_t pad;
	size_t pos, len = strLen(v);
	const size_t extralen = h->hashash ? EXTRALEN : 0;
	stridx_t *bucket;
	BUN off, strhash;

	off = strHash(v);
	strhash = off;
	off &= GDK_STRHASHMASK;
	bucket = ((stridx_t *) h->base) + off;

	if (*bucket) {
		/* the hash list is not empty */
		if (*bucket < GDK_ELIMLIMIT) {
			/* small string heap (<64KiB) -- fully double
			 * eliminated: search the linked list */
			const stridx_t *ref = bucket;

			do {
				pos = *ref + sizeof(stridx_t) + extralen;
				if (strCmp(v, h->base + pos) == 0) {
					/* found */
					return *dst = (var_t) pos;
				}
				ref = (stridx_t *) (h->base + *ref);
			} while (*ref);
		} else {
			/* large string heap (>=64KiB) -- there is no
			 * linked list, so only look at single
			 * entry */
			pos = *bucket + extralen;
			if (strCmp(v, h->base + pos) == 0) {
				/* already in heap: reuse */
				return *dst = (var_t) pos;
			}
		}
	}
	/* the string was not found in the heap, we need to enter it */

	if (v[0] != '\200' || v[1] != '\0') {
		/* check that string is correctly encoded UTF-8; there
		 * was no need to do this earlier: if the string was
		 * found above, it must have gone through here in the
		 * past */
		int nutf8 = 0;
		int m = 0;
		for (size_t i = 0; v[i]; i++) {
			if (nutf8 > 0) {
				if ((v[i] & 0xC0) != 0x80 ||
				    (m != 0 && (v[i] & m) == 0)) {
				  badutf8:
					GDKerror("strPut: incorrectly encoded UTF-8");
					return 0;
				}
				m = 0;
				nutf8--;
			} else if ((v[i] & 0xE0) == 0xC0) {
				nutf8 = 1;
				if ((v[i] & 0x1E) == 0)
					goto badutf8;
			} else if ((v[i] & 0xF0) == 0xE0) {
				nutf8 = 2;
				if ((v[i] & 0x0F) == 0)
					m = 0x20;
			} else if ((v[i] & 0xF8) == 0xF0) {
				nutf8 = 3;
				if ((v[i] & 0x07) == 0)
					m = 0x30;
			} else if ((v[i] & 0x80) != 0) {
				goto badutf8;
			}
		}
	}

	pad = GDK_VARALIGN - (h->free & (GDK_VARALIGN - 1));
	if (elimbase == 0) {	/* i.e. h->free < GDK_ELIMLIMIT */
		if (pad < sizeof(stridx_t)) {
			/* make room for hash link */
			pad += GDK_VARALIGN;
		}
	} else if (extralen == 0) {	/* i.e., h->hashash == FALSE */
		/* no VARSHIFT and no string hash value stored => no
		 * padding/alignment needed */
		pad = 0;
	} else {
		/* pad to align on VARALIGN for VARSHIFT and/or string
		 * hash value */
		pad &= (GDK_VARALIGN - 1);
	}

	/* check heap for space (limited to a certain maximum after
	 * which nils are inserted) */
	if (h->free + pad + len + extralen >= h->size) {
		size_t newsize = MAX(h->size, 4096);

		/* double the heap size until we have enough space */
		do {
			if (newsize < 4 * 1024 * 1024)
				newsize <<= 1;
			else
				newsize += 4 * 1024 * 1024;
		} while (newsize <= h->free + pad + len + extralen);

		assert(newsize);

		if (h->free + pad + len + extralen >= (size_t) VAR_MAX) {
			GDKerror("strPut: string heaps gets larger than %zuGiB.\n", (size_t) VAR_MAX >> 30);
			return 0;
		}
		TRC_DEBUG(HEAP, "HEAPextend in strPut %s %zu %zu\n", h->filename, h->size, newsize);
		if (HEAPextend(h, newsize, true) != GDK_SUCCEED) {
			return 0;
		}
#ifndef NDEBUG
		/* fill should solve initialization problems within
		 * valgrind */
		memset(h->base + h->free, 0, h->size - h->free);
#endif

		/* make bucket point into the new heap */
		bucket = ((stridx_t *) h->base) + off;
	}

	/* insert string */
	pos = h->free + pad + extralen;
	*dst = (var_t) pos;
	memcpy(h->base + pos, v, len);
	if (h->hashash) {
		((BUN *) (h->base + pos))[-1] = strhash;
#if EXTRALEN > SIZEOF_BUN
		((BUN *) (h->base + pos))[-2] = (BUN) len;
#endif
	}
	h->free += pad + len + extralen;
	h->dirty = true;

	/* maintain hash table */
	pos -= extralen;
	if (elimbase == 0) {	/* small string heap: link the next pointer */
		/* the stridx_t next pointer directly precedes the
		 * string and optional (depending on hashash) hash
		 * value */
		pos -= sizeof(stridx_t);
		*(stridx_t *) (h->base + pos) = *bucket;
	}
	*bucket = (stridx_t) pos;	/* set bucket to the new string */

	return *dst;
}

/*
 * Convert an "" separated string to a GDK string value, checking that
 * the input is correct UTF-8.
 */

/*
   UTF-8 encoding is as follows:
U-00000000 - U-0000007F: 0xxxxxxx
U-00000080 - U-000007FF: 110xxxxx 10xxxxxx
U-00000800 - U-0000FFFF: 1110xxxx 10xxxxxx 10xxxxxx
U-00010000 - U-001FFFFF: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
U-00200000 - U-03FFFFFF: 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
U-04000000 - U-7FFFFFFF: 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
*/
/* To be correctly coded UTF-8, the sequence should be the shortest
 * possible encoding of the value being encoded.  This means that for
 * an encoding of length n+1 (1 <= n <= 5), at least one of the bits
 * in utf8chkmsk[n] should be non-zero (else the encoding could be
 * shorter). */
static int utf8chkmsk[] = {
	0x0000007f,
	0x00000780,
	0x0000f800,
	0x001f0000,
	0x03e00000,
	0x7c000000,
};

ssize_t
GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len)
{
	unsigned char *p = dst;
	const unsigned char *cur = src, *end = src + len;
	bool escaped = false;
	int mask = 0, n, c, utf8char = 0;

	if (len >= 2 && strcmp((const char *) src, str_nil) == 0) {
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
						if (c > 037) {
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
				if (n > 0) {
					/* not when in the middle of a
					 * UTF-8 sequence */
					goto notutf8;
				}
				c = 0;
				for (n = *cur == 'U' ? 8 : 4; n > 0; n--) {
					cur++;
					if (!num16(*cur)) {
						GDKerror("not a Unicode code point escape\n");
						return -1;
					}
					c = c << 4 | base16(*cur);
				}
				/* n == 0 now */
				if (c == 0 || c > 0x10FFFF ||
				    (c & 0xFFF800) == 0xD800) {
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
#if 0
		} else if (c == quote && cur[1] == quote) {
			assert(c != 0);
			if (n > 0)
				goto notutf8;
			*p++ = quote;
			cur++;
			continue;
#endif
		}

		if (n > 0) {
			/* we're still expecting follow-up bytes in a
			 * UTF-8 sequence */
			if ((c & 0xC0) != 0x80) {
				/* incorrect UTF-8 sequence: byte is
				 * not 10xxxxxx */
				goto notutf8;
			}
			utf8char = (utf8char << 6) | (c & 0x3F);
			n--;
			if (n == 0) {
				/* this was the last byte in the sequence */
				if ((utf8char & mask) == 0) {
					/* incorrect UTF-8 sequence:
					 * not shortest possible */
					goto notutf8;
				}
				if (utf8char > 0x10FFFF) {
					/* incorrect UTF-8 sequence:
					 * value too large */
					goto notutf8;
				}
				if ((utf8char & 0x1FFF800) == 0xD800) {
					/* incorrect UTF-8 sequence:
					 * low or high surrogate
					 * encoded as UTF-8 */
					goto notutf8;
				}
			}
		} else if (c >= 0x80) {
			int m;

			/* start of multi-byte UTF-8 character */
			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				;
			/* n now is number of 10xxxxxx bytes that
			 * should follow */
			if (n == 0 || n >= 4) {
				/* incorrect UTF-8 sequence */
				/* n==0: c == 10xxxxxx */
				/* n>=4: c == 11111xxx */
				goto notutf8;
			}
			mask = utf8chkmsk[n];
			/* collect the Unicode code point in utf8char */
			utf8char = c & ~(0xFFC0 >> n);	/* remove non-x bits */
		}
		*p++ = c;
	}
	if (n > 0) {
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
			     (ssize_t) (cur - start));
}

/*
 * Convert a GDK string value to something printable.
 */
/* all but control characters (in range 0 to 31) and DEL */
#ifdef ASCII_CHR
/* ASCII printable characters */
#define printable_chr(ch)	(' ' <= (ch) && (ch) <= '~')
#else
/* everything except ASCII control characters */
#define printable_chr(ch)	((' ' <= (ch) && (ch) <= '~') || ((ch) & 0x80) != 0)
#endif

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
#ifndef ASCII_CHR
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
#endif
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
#ifndef ASCII_CHR
		    || (src[cur] == '\302'
			&& 0200 <= (src[cur + 1] & 0377)
			&& ((int) src[cur + 1] & 0377) <= 0237)
		    || (cur > 0
			&& src[cur - 1] == '\302'
			&& 0200 <= (src[cur] & 0377)
			&& (src[cur] & 0377) <= 0237)
#endif
			) {
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
strRead(str a, stream *s, size_t cnt)
{
	int len;

	(void) cnt;
	assert(cnt == 1);
	if (mnstr_readInt(s, &len) != 1)
		return NULL;
	if ((a = GDKmalloc(len + 1)) == NULL)
		return NULL;
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
	if (mnstr_writeInt(s, (int) len) && mnstr_write(s, a, len, 1) == 1)
		return GDK_SUCCEED;
	else
		return GDK_FAIL;
}

static gdk_return
concat_strings(BAT **bnp, ValPtr pt, BAT *b, oid seqb,
			   BUN ngrp, struct canditer *restrict ci, BUN ncand,
			   const oid *restrict gids, oid min, oid max, bool skip_nils,
			   BAT *sep, const char *restrict separator, BUN *has_nils)
{
	oid gid;
	BUN i, p, nils = 0;
	size_t *restrict lengths = NULL, *restrict lastseplength = NULL, separator_length = 0, next_length;
	str *restrict astrings = NULL, s, sl;
	BATiter bi, bis = (BATiter) {0};
	BAT *bn = NULL;
	gdk_return rres = GDK_SUCCEED;

	/* exactly one of bnp and pt must be NULL, the other non-NULL */
	assert((bnp == NULL) != (pt == NULL));
	/* if pt not NULL, only a single group allowed */
	assert(pt == NULL || ngrp == 1);
	if (bnp) {
		if ((bn = COLnew(min, TYPE_str, ngrp, TRANSIENT)) == NULL) {
			rres = GDK_FAIL;
			goto finish;
		}
		*bnp = bn;
	}

	bi = bat_iterator(b);
	if (sep)
		bis = bat_iterator(sep);
	else
		separator_length = strlen(separator);

	if (ngrp == 1) {
		size_t offset = 0, single_length = 0;
		bool empty = true;

		if (separator) {
			for (i = 0; i < ncand; i++) {
				p = canditer_next(ci) - seqb;
				s = BUNtvar(bi, p);
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
			for (i = 0; i < ncand; i++) {
				p = canditer_next(ci) - seqb;
				s = BUNtvar(bi, p);
				sl = BUNtvar(bis, p);
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

		if (nils == 0) {
			char *single_str;

			if ((single_str = GDKmalloc(single_length + 1)) == NULL)
				return GDK_FAIL;
			empty = true;
			if (separator) {
				for (i = 0; i < ncand; i++) {
					p = canditer_next(ci) - seqb;
					s = BUNtvar(bi, p);
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
				for (i = 0; i < ncand; i++) {
					p = canditer_next(ci) - seqb;
					s = BUNtvar(bi, p);
					sl = BUNtvar(bis, p);
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
			if (bn) {
				if (BUNappend(bn, single_str, false) != GDK_SUCCEED) {
					GDKfree(single_str);
					return GDK_FAIL;
				}
			} else {
				pt->len = offset + 1;
				pt->val.sval = single_str;
				single_str = NULL;	/* don't free */
			}
			GDKfree(single_str);
		} else if (bn) {
			if (BUNappend(bn, str_nil, false) != GDK_SUCCEED)
				return GDK_FAIL;
		} else {
			if (VALinit(pt, TYPE_str, str_nil) == NULL)
				return GDK_FAIL;
		}
		return GDK_SUCCEED;
	} else {
		/* first used to calculated the total length of
		 * each group, then the the total offset */
		lengths = GDKzalloc(ngrp * sizeof(*lengths));
		astrings = GDKmalloc(ngrp * sizeof(str));
		if (sep)
			lastseplength = GDKzalloc(ngrp * sizeof(*lastseplength));
		if (lengths == NULL || astrings == NULL || (sep && lastseplength == NULL)) {
			rres = GDK_FAIL;
			goto finish;
		}
		/* at first, set astrings[i] to str_nil, then for each
		 * non-empty group (even if all strings in the group
		 * are empty), set to NULL */
		for (i = 0; i < ngrp; i++)
			astrings[i] = (char *) str_nil;
	
		if (separator) {
			for (p = 0; p < ncand; p++) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (lengths[gid] == (size_t) -1)
						continue;
					s = BUNtvar(bi, i);
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
			for (p = 0; p < ncand; p++) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (lengths[gid] == (size_t) -1)
						continue;
					s = BUNtvar(bi, i);
					sl = BUNtvar(bis, i);
					if (!strNil(s)) {
						lengths[gid] += strlen(s);
						if (!strNil(sl)) {
							next_length = strlen(sl);
							lengths[gid] += next_length;
							lastseplength[gid] = next_length;
						} else
							lastseplength[gid] = 0;
						astrings[gid] = NULL;
					} else if (!skip_nils) {
						nils++;
						lengths[gid] = (size_t) -1;
						lastseplength[gid] = 0;
						astrings[gid] = (char *) str_nil;
					}
				}
			}
		}

		if (separator) {
			for (i = 0; i < ngrp; i++) {
				if (astrings[i] == NULL) {
					if ((astrings[i] = GDKmalloc(lengths[i] + 1 - separator_length)) == NULL) {
						rres = GDK_FAIL;
						goto finish;
					}
					astrings[i][0] = 0;
					lengths[i] = 0;
				} else
					astrings[i] = NULL;
			}
		} else { /* sep case */
			for (i = 0; i < ngrp; i++) {
				if (astrings[i] == NULL) {
					if ((astrings[i] = GDKmalloc(lengths[i] + 1 - lastseplength[i])) == NULL) {
						rres = GDK_FAIL;
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
			for (p = 0; p < ncand; p++) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (astrings[gid]) {
						s = BUNtvar(bi, i);
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
			for (p = 0; p < ncand; p++) {
				i = canditer_next(ci) - seqb;
				if (gids[i] >= min && gids[i] <= max) {
					gid = gids[i] - min;
					if (astrings[gid]) {
						s = BUNtvar(bi, i);
						sl = BUNtvar(bis, i);
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

		for (i = 0; i < ngrp; i++) {
			if (astrings[i]) {
				astrings[i][lengths[i]] = '\0';
				if (BUNappend(bn, astrings[i], false) != GDK_SUCCEED) {
					rres = GDK_FAIL;
					goto finish;
				}
			} else if (BUNappend(bn, str_nil, false) != GDK_SUCCEED) {
				rres = GDK_FAIL;
				goto finish;
			}
		}
	}

finish:
	if (has_nils)
		*has_nils = nils;
	GDKfree(lengths);
	GDKfree(lastseplength);
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
}

gdk_return
BATstr_group_concat(ValPtr res, BAT *b, BAT *s, BAT *sep, bool skip_nils, 
					bool abort_on_error, bool nil_if_empty, const char *restrict separator)
{
	BUN ncand;
	struct canditer ci;

	(void) abort_on_error;
	assert((separator && !sep) || (!separator && sep)); /* only one of them must be set */
	res->vtype = TYPE_str;

	ncand = canditer_init(&ci, b, s);

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		BATiter bi = bat_iterator(sep);
		separator = BUNtvar(bi, 0);
		sep = NULL;
	}

	if (ncand == 0 || (separator && strNil(separator))) {
		if (VALinit(res, TYPE_str, nil_if_empty ? str_nil : "") == NULL)
			return GDK_FAIL;
		return GDK_SUCCEED;
	}

	return concat_strings(NULL, res, b, b->hseqbase, 1, &ci, ncand, NULL, 0, 0, 
						  skip_nils, sep, separator, NULL);
}

BAT *
BATgroupstr_group_concat(BAT *b, BAT *g, BAT *e, BAT *s, BAT *sep, bool skip_nils,
						 bool abort_on_error, const char *restrict separator)
{
	BAT *bn = NULL;
	oid min, max;
	BUN ngrp, ncand, nils = 0;
	struct canditer ci;
	const char *err;
	gdk_return res;

	assert((separator && !sep) || (!separator && sep)); /* only one of them must be set */
	(void) skip_nils;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp,
				    &ci, &ncand)) !=NULL) {
		GDKerror("BATgroupstr_group_concat: %s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("BATgroupstr_group_concat: b and g must be aligned\n");
		return NULL;
	}

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		BATiter bi = bat_iterator(sep);
		separator = BUNtvar(bi, 0);
		sep = NULL;
	}

	if (ncand == 0 || ngrp == 0 || (separator && strNil(separator))) {
		/* trivial: no strings to concat, so return bat
		 * aligned with g with nil in the tail */
		return BATconstant(ngrp == 0 ? 0 : min, TYPE_str, str_nil, ngrp, TRANSIENT);
	}

	if (BATtdense(g) || (g->tkey && g->tnonil)) {
		/* trivial: singleton groups, so all results are equal
		 * to the inputs (but possibly a different type) */
		return BATconvert(b, s, TYPE_str, abort_on_error);
	}

	res = concat_strings(&bn, NULL, b, b->hseqbase, ngrp, &ci, ncand,
						 (const oid *) Tloc(g, 0), min, max, skip_nils, sep, 
						 separator, &nils);
	if (res != GDK_SUCCEED)
		return NULL;

	return bn;
}

gdk_return
GDKanalytical_str_group_concat(BAT *r, BAT *b, BAT *sep, BAT *s, BAT *e, const char *restrict separator)
{
	BUN i = 0, cnt = BATcount(b);
	lng *restrict start, *restrict end, j, l;
	BATiter bi, bis = (BATiter) {0};
	str sb, sl, single_str = NULL, next_single_str;
	bool empty;
	size_t separator_length = 0, next_group_length, max_group_length = 0, next_length, offset;

	assert(s && e && ((sep && !separator && BATcount(b) == BATcount(sep)) || (!sep && separator)));
	start = (lng *) Tloc(s, 0);
	end = (lng *) Tloc(e, 0);

	if (b->ttype != TYPE_str || r->ttype != TYPE_str || (sep && sep->ttype != TYPE_str)) {
		GDKerror("BATgroupstr_group_concat: only string type is supported\n");
		return GDK_FAIL;
	}

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		bi = bat_iterator(sep);
		separator = BUNtvar(bi, 0);
		sep = NULL;
	}

	bi = bat_iterator(b);
	if (sep)
		bis = bat_iterator(sep);
	else
		separator_length = strlen(separator);

	for (; i < cnt; i++) {
		l = end[i];
		empty = true;
		next_group_length = next_length = offset = 0;

		for (j = start[i]; j < l; j++) {
			sb = BUNtvar(bi, (BUN) j);

			if (separator) {
				if (!strNil(sb)) {
					next_group_length += strlen(sb);
					if (!empty)
						next_group_length += separator_length;
					empty = false;
				}
			} else { /* sep case */
				sl = BUNtvar(bis, (BUN) j);

				if (!strNil(sb)) {
					next_group_length += strlen(sb);
					if (!empty && !strNil(sl))
						next_group_length += strlen(sl);
					empty = false;
				}
			}
		}

		empty = true;

		if (!single_str) { /* reuse the same buffer, resize it when needed */
			max_group_length = next_group_length;
			if ((single_str = GDKmalloc(max_group_length + 1)) == NULL)
				goto allocation_error;
		} else if (next_group_length > max_group_length) {
			max_group_length = next_group_length;
			if ((next_single_str = GDKrealloc(single_str, max_group_length + 1)) == NULL)
				goto allocation_error;
			single_str = next_single_str;
		}

		for (j = start[i]; j < l; j++) {
			sb = BUNtvar(bi, (BUN) j);

			if (separator) {
				if (strNil(sb))
					continue;
				if (!empty) {
					memcpy(single_str + offset, separator, separator_length);
					offset += separator_length;
				}
				next_length = strlen(sb);
				memcpy(single_str + offset, sb, next_length);
				offset += next_length;
				empty = false;
			} else { /* sep case */
				sl = BUNtvar(bis, (BUN) j);

				if (strNil(sb))
					continue;
				if (!empty && !strNil(sl)) {
					next_length = strlen(sl);
					memcpy(single_str + offset, sl, next_length);
					offset += next_length;
				}
				next_length = strlen(sb);
				memcpy(single_str + offset, sb, next_length);
				offset += next_length;
				empty = false;
			}
		}

		single_str[offset] = '\0';
		if (BUNappend(r, single_str, false) != GDK_SUCCEED)
			goto allocation_error;

	}

	GDKfree(single_str);
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return GDK_SUCCEED;
 allocation_error:
	GDKfree(single_str);
	GDKerror("%s: malloc failure\n", __func__);
	return GDK_FAIL;
}
