/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_cand.h"

/* String Atom Implementation
 *
 * Strings are stored in two parts.  The first part is the normal tail
 * heap (theap) which contains a list of offsets.  The second part is
 * the tvheap which contains the actual strings.  The offsets in the
 * tail heap (a.k.a. offset heap) point into the tvheap (a.k.a. string
 * heap).  Strings are NULL-terminated and are stored without any escape
 * sequences.  Strings are encoded using the UTF-8 encoding of Unicode.
 * This means that individual "characters" (really, Unicode code points)
 * can be between one and four bytes long.
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

struct hash {
	BUN mask1;
	BUN mask2;
	BUN nbucket;
	BUN nentries;
	BUN growlim;
	uint64_t *buckets;
	Heap heap;
};

#define STRHASH_VERSION	1
#define STRHASH_SYNCED	(UINT64_C(1) << 24)
#define EMPTY ((uint64_t) -1)
#define OFFSET 1
#define PSLSHIFT 40
#define PSLMASK ((UINT64_C(1) << PSLSHIFT) - 1)

static inline BUN
hash_str(struct hash *h, const char *value)
{
	BUN hsh = strHash(value);
	hsh &= h->mask2;
	if (hsh >= h->nbucket)
		hsh &= h->mask1;
	return hsh;
}

static inline var_t
str_enter(BAT *b, const char *value)
{
	size_t len = strlen(value) + 1;
	var_t pos = b->tvheap->free;
	if (pos + len > b->tvheap->size) {
		size_t newsize = b->tvheap->size & ~(size_t)(64 * 1024 - 1);
		do {
			newsize += 64 * 1024;
		} while (pos + len > newsize);
		if (HEAPgrow(&b->theaplock, &b->tvheap, newsize, true) != GDK_SUCCEED)
			return (var_t) -1;
	}
	memcpy(b->tvheap->base + pos, value, len);
	b->tvheap->free += len;
	b->tvheap->dirty = true;
	return pos;
}

static inline void
str_reinsert(struct hash *h, BUN hsh, var_t pos)
{
	uint64_t psl = 0;
	uint64_t swp = EMPTY;

	for (;;) {
		uint64_t bkt = h->buckets[hsh];
		if (bkt == EMPTY) {
			/* found an empty slot */
			if (swp == EMPTY) {
				swp = (uint64_t) pos;
			}
			h->buckets[hsh] = swp | psl << PSLSHIFT;
			return;
		}
		uint64_t psl2 = bkt >> PSLSHIFT;
		if (psl > psl2) {
			if (swp == EMPTY) {
				swp = (uint64_t) pos;
			}
			h->buckets[hsh] = swp | psl << PSLSHIFT;
			swp = bkt & PSLMASK;
			psl = psl2;
		}
		if (++hsh == h->nbucket)
			hsh = 0;
		psl++;
	}
}

static inline void
str_remove(struct hash *h, BUN pos)
{
	h->buckets[pos] = EMPTY;
	for (;;) {
		BUN hsh = pos + 1;
		if (hsh >= h->nbucket)
			hsh = 0;
		uint64_t p = h->buckets[hsh];
		if (p == EMPTY)
			break;
		uint64_t psl = p >> PSLSHIFT;
		if (psl == 0)
			break;
		h->buckets[pos] = (p & PSLMASK) | (psl - 1) << PSLSHIFT;
		h->buckets[hsh] = EMPTY;
		pos = hsh;
	}
}

static inline void
str_incPSL(struct hash *h)
{
	uint64_t hsh = 0;
	for (;;) {
		uint64_t bkt = h->buckets[hsh];
		if (bkt == EMPTY)
			break;
		uint64_t psl = bkt >> PSLSHIFT;
		if (psl <= hsh)
			break;
		h->buckets[hsh] = (bkt & PSLMASK) | (psl + 1) << PSLSHIFT;
		if (++hsh == h->nbucket)
			break;
	}
}

static inline gdk_return
str_growhash(BAT *b, struct hash *h)
{
	BUN oldmask1 = h->mask1;
	BUN oldnbucket = h->nbucket;

	if ((h->nbucket + OFFSET) * sizeof(uint64_t) >= h->heap.size) {
		if (HEAPextend(&h->heap, h->heap.size + 64 * 1024, true) != GDK_SUCCEED)
			return GDK_FAIL;
		h->buckets = (uint64_t *) h->heap.base + OFFSET;
	}
	if (h->nbucket == h->mask2) {
		h->mask1 = h->mask2;
		h->mask2 |= h->mask2 << 1; /* extend with one bit */
	}
	h->nbucket++;
	h->heap.free = (h->nbucket + OFFSET) * sizeof(uint64_t);
	h->growlim = (BUN) (h->nbucket * (0.75 - (h->nbucket & h->mask1) / (2.0 * h->mask2)));
	assert(h->mask1 < h->nbucket && h->nbucket <= h->mask2);

	str_incPSL(h);
	str_remove(h, h->nbucket - 1);

	BUN hsh = oldnbucket & oldmask1;
	uint64_t psl = 0;

	for (;;) {
		uint64_t bkt = h->buckets[hsh];
		if (bkt == EMPTY)
			break;
		uint64_t psl2 = bkt >> PSLSHIFT;
		if (psl2 < psl)
			break;
		if (psl2 == psl) {
			bkt &= PSLMASK;
			const char *v = b->tvheap->base + bkt;
			BUN c = strHash(v);
			if ((c & h->mask2) == oldnbucket) {
				/* this one should be moved */
				str_remove(h, hsh);
				str_reinsert(h, oldnbucket, (var_t) bkt);
				continue;
			}
		}
		if (++hsh == oldnbucket)
			hsh = 0;
		psl++;
	}
	return GDK_SUCCEED;
}

static var_t
str_insert(BAT *b, struct hash *h, const char *value)
{
	BUN hsh = hash_str(h, value);
	uint64_t psl = 0;	/* probe sequence length */
	uint64_t swp = EMPTY;	/* swapped entry */
	var_t new = (var_t) -1;	/* new entry */

	for (;;) {
		uint64_t bkt = h->buckets[hsh];
		if (bkt == EMPTY) {
			/* found an empty slot */
			if (swp == EMPTY) {
				assert(new == (var_t) -1);
				/* didn't enter value yet, so do it now */
				new = str_enter(b, value);
				if (new == (var_t) -1)
					return (var_t) -1;
				swp = (uint64_t) new;
				h->nentries++;
			}
			assert(new != (var_t) -1);
			h->buckets[hsh] = swp | psl << PSLSHIFT;
			while (h->nentries >= h->growlim)
				if (str_growhash(b, h) != GDK_SUCCEED)
					return (var_t) -1;
			return new;
		}
		uint64_t psl2 = bkt >> PSLSHIFT;
		bkt &= PSLMASK;
		if (psl == psl2) {
			const char *v = b->tvheap->base + bkt;
			if (strcmp(value, v) == 0) {
				/* found the value */
				assert(swp == EMPTY);
				assert(new == (var_t) -1);
				return (var_t) bkt;
			}
		} else
		if (psl > psl2) {
			if (swp == EMPTY) {
				assert(new == (var_t) -1);
				new = str_enter(b, value);
				if (new == (var_t) -1)
					return (var_t) -1;
				swp = (uint64_t) new;
				h->nentries++;
			}
			h->buckets[hsh] = swp | psl << PSLSHIFT;
			swp = bkt;
			psl = psl2;
		}
		if (++hsh == h->nbucket)
			hsh = 0;
		psl++;
	}
}

static struct hash *
str_hashinit(BAT *b)
{
	struct hash *hs = GDKmalloc(sizeof(struct hash));
	if (hs == NULL)
		return NULL;
	BUN minimum = b->tvheap->free / 8;
	if (minimum < 256)
		minimum = 256;
	BUN mask = minimum | minimum >> 1;
	mask |= mask >> 2;
	mask |= mask >> 4;
	mask |= mask >> 8;
	mask |= mask >> 16;
#if SIZEOF_BUN == 8
	mask |= mask >> 32;
#endif
	*hs = (struct hash) {
		.mask1 = mask >> 1,
		.mask2 = mask,
		.nbucket = minimum,
		.nentries = 0,
		.growlim = (minimum * 3) / 4,
		.heap.farmid = BBPselectfarm(b->batRole, TYPE_str, strhashheap),
		.heap.parentid = b->batCacheid,
		.heap.dirty = true,
	};
	strconcat_len(hs->heap.filename, sizeof(hs->heap.filename),
		      BBP_physical(b->batCacheid), ".tstrhash", NULL);

	if (HEAPalloc(&hs->heap, hs->nbucket + OFFSET, sizeof(uint64_t), 0) != GDK_SUCCEED) {
		GDKfree(hs);
		return NULL;
	}
	hs->heap.free = (hs->nbucket + OFFSET) * sizeof(uint64_t);
	*(uint64_t *) hs->heap.base = STRHASH_VERSION;
	hs->buckets = (uint64_t *) hs->heap.base + OFFSET;
	for (BUN i = 0; i < minimum; i++)
		hs->buckets[i] = EMPTY;
	Heap *hp = b->tvheap;
	for (var_t pos = 0, free = (var_t) hp->free; pos < free;) {
		const char *v = hp->base + pos;
		size_t len = strlen(v) + 1;
		BUN hsh = hash_str(hs, v);
		str_reinsert(hs, hsh, pos);
		hs->nentries++;
		while (hs->nentries >= hs->growlim) {
			if (str_growhash(b, hs) != GDK_SUCCEED) {
				HEAPfree(hp, true);
				GDKfree(hs);
				return NULL;
			}
		}
		pos += len;
	}
	return hs;
}

gdk_return
strHeap(Heap *d, size_t cap)
{
	cap = MAX(cap, BATTINY);
	return HEAPalloc(d, cap, 8, 1);
}

void
strCleanHash(Heap *h, bool rebuild)
{
	(void) h;
	(void) rebuild;
}

var_t
strPut(BAT *b, var_t *dst, const void *v)
{
	if (b->strhash == NULL && (b->strhash = str_hashinit(b)) == NULL)
		return (var_t) -1;
	var_t pos = str_insert(b, b->strhash, v);
	if (pos == (var_t) -1)
		return (var_t) -1;
	*dst = pos;
	return pos;
}

var_t
strLocate(BAT *b, const char *value)
{
	if (b->tvheap->parentid != b->batCacheid)
		return strLocate(BBP_cache(b->tvheap->parentid), value);

	if (b->strhash == NULL && (b->strhash = str_hashinit(b)) == NULL)
		return (var_t) -1;

	struct hash *h = b->strhash;
	BUN hsh = hash_str(h, value);
	uint64_t psl = 0;	/* probe sequence length */

	for (;;) {
		uint64_t bkt = h->buckets[hsh];
		if (bkt == EMPTY) {
			/* found an empty slot */
			return (var_t) -2;
		}
		uint64_t psl2 = bkt >> PSLSHIFT;
		bkt &= PSLMASK;
		if (psl == psl2) {
			const char *v = b->tvheap->base + bkt;
			if (strcmp(value, v) == 0) {
				/* found the value */
				return (var_t) bkt;
			}
		}
		if (psl > psl2) {
			/* found an entry with a lower PSL */
			return (var_t) -2;
		}
		if (++hsh == h->nbucket)
			hsh = 0;
		psl++;
	}
}

void
strDestroy(BAT *b)
{
	HEAPfree(&b->strhash->heap, true);
	GDKfree(b->strhash);
	b->strhash = NULL;
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
GDKstrFromStr(unsigned char *restrict dst, const unsigned char *restrict src, ssize_t len)
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
#if 0
		} else if (c == quote && cur[1] == quote) {
			assert(c != 0);
			if (unlikely(n > 0))
				goto notutf8;
			*p++ = quote;
			cur++;
			continue;
#endif
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
			     (ssize_t) (cur - start));
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

	bi = bat_iterator(b);
	if (sep)
		bis = bat_iterator(sep);
	else
		separator_length = strlen(separator);

	if (bnp) {
		if ((bn = COLnew(min, TYPE_str, ngrp, TRANSIENT)) == NULL) {
			rres = GDK_FAIL;
			goto finish;
		}
		*bnp = bn;
	}

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
			assert(sep != NULL);
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

		if (nils == 0 && !empty) {
			char *single_str = NULL;

			if ((single_str = GDKmalloc(single_length + 1)) == NULL) {
				bat_iterator_end(&bi);
				if (sep)
					bat_iterator_end(&bis);
				return GDK_FAIL;
			}
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
				assert(sep != NULL);
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
					bat_iterator_end(&bi);
					if (sep)
						bat_iterator_end(&bis);
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
				if (sep)
					bat_iterator_end(&bis);
				return GDK_FAIL;
			}
		} else {
			if (VALinit(pt, TYPE_str, str_nil) == NULL) {
				bat_iterator_end(&bi);
				if (sep)
					bat_iterator_end(&bis);
				return GDK_FAIL;
			}
		}
		bat_iterator_end(&bi);
		if (sep)
			bat_iterator_end(&bis);
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
			assert(sep != NULL);
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
			assert(sep != NULL);
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
			assert(sep != NULL);
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
	bat_iterator_end(&bi);
	if (sep)
		bat_iterator_end(&bis);
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
		bat_iterator_end(&bi);
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
		GDKerror("%s\n", err);
		return NULL;
	}
	if (g == NULL) {
		GDKerror("b and g must be aligned\n");
		return NULL;
	}

	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		BATiter bi = bat_iterator(sep);
		separator = BUNtvar(bi, 0);
		bat_iterator_end(&bi);
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
		return BATconvert(b, s, TYPE_str, abort_on_error, 0, 0, 0);
	}

	res = concat_strings(&bn, NULL, b, b->hseqbase, ngrp, &ci, ncand,
			     (const oid *) Tloc(g, 0), min, max, skip_nils, sep,
			     separator, &nils);
	if (res != GDK_SUCCEED)
		return NULL;

	return bn;
}

#define compute_next_single_str(START, END)				\
	do {								\
		for (oid m = START; m < END; m++) {			\
			sb = BUNtvar(bi, m);				\
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
				sl = BUNtvar(sepi, m);			\
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
				sb = BUNtvar(bi, m);			\
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
					sl = BUNtvar(sepi, m);		\
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
			str nsep, nstr;					\
			oid m = k;					\
			j = k;						\
			do {						\
				k++;					\
			} while (k < i && !op[k]);			\
			for (; j < k; j++) {				\
				nstr = BUNtvar(bi, j);			\
				if (!strNil(nstr)) {			\
					slice_length += strlen(nstr);	\
					if (!empty) {			\
						if (separator) {	\
							nsep = (str) separator; \
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
			str next = BUNtvar(bi, k);			\
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
	oid i = 0, j = 0, k = 0, cnt = BATcount(b), *restrict start = si.base, *restrict end = ei.base;
	bit *np = pi.base, *op = oi.base;
	str sb, sl, single_str = NULL, next_single_str;
	size_t separator_length = 0, next_group_length, max_group_length = 0, next_length, offset;

	assert((sep && !separator && BATcount(b) == BATcount(sep)) || (!sep && separator));
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
	if (sep && BATcount(sep) == 1) { /* Only one element in sep */
		separator = BUNtvar(sepi, 0);
		sep = NULL;
	}

	if (sep == NULL)
		separator_length = strlen(separator);

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */	{
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_UNBOUNDED_TILL_CURRENT_ROW);
		} break;
		case 4: /* current row until unbounded */
			goto notimplemented;
		case 5: /* all rows */	{
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_ALL_ROWS);
		} break;
		case 6: /* current row */ {
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_CURRENT_ROW);
		} break;
		default: {
			ANALYTICAL_STR_GROUP_CONCAT_PARTITIONS(ANALYTICAL_STR_GROUP_CONCAT_OTHERS);
		}
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
