/* This is an extract from kstring.h from the samtools sources.  It is
 * included because the libbam-dev package on Ubuntu doesn't include
 * kstring.h.  The copyright message is the one from samtools. */

/* The MIT License

   Copyright (c) by Attractive Chaos <attractor@live.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#ifndef KSTRING_T
typedef struct {
	size_t l, m;
	char *s;
} kstring_t;
#endif

#define kroundup32(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, ++(x))

static int ksprintf(kstring_t *s, const char *fmt, ...)
	__attribute__((__format__(__printf__, 2, 3)));
static int ksprintf(kstring_t *s, const char *fmt, ...)
{
	va_list ap;
	int l;
	va_start(ap, fmt);
	l = vsnprintf(s->s + s->l, s->m - s->l, fmt, ap); // This line does not work with glibc 2.0. See `man snprintf'.
	va_end(ap);
	if ((size_t) l + 1 > s->m - s->l) {
		s->m = s->l + l + 2;
		kroundup32(s->m);
		s->s = (char*)realloc(s->s, s->m);
		va_start(ap, fmt);
		l = vsnprintf(s->s + s->l, s->m - s->l, fmt, ap);
	}
	va_end(ap);
	s->l += l;
	return l;
}

static inline int kputsn(const char *p, int l, kstring_t *s)
{
	if (s->l + l + 1 >= s->m) {
		s->m = s->l + l + 2;
		kroundup32(s->m);
		s->s = (char*)realloc(s->s, s->m);
	}
	memcpy(s->s + s->l, p, l);
	s->l += l;
	s->s[s->l] = 0;
	return l;
}

static inline int kputs(const char *p, kstring_t *s)
{
	return kputsn(p, strlen(p), s);
}

static inline int kputc(int c, kstring_t *s)
{
	if (s->l + 1 >= s->m) {
		s->m = s->l + 2;
		kroundup32(s->m);
		s->s = (char*)realloc(s->s, s->m);
	}
	s->s[s->l++] = c;
	s->s[s->l] = 0;
	return c;
}

static inline int kputw(int c, kstring_t *s)
{
	char buf[16];
	int l, x;
	if (c == 0) return kputc('0', s);
        if(c < 0) for (l = 0, x = c; x < 0; x /= 10) buf[l++] = '0' - (x%10);
        else for (l = 0, x = c; x > 0; x /= 10) buf[l++] = x%10 + '0';
	if (c < 0) buf[l++] = '-';
	if (s->l + l + 1 >= s->m) {
		s->m = s->l + l + 2;
		kroundup32(s->m);
		s->s = (char*)realloc(s->s, s->m);
	}
	for (x = l - 1; x >= 0; --x) s->s[s->l++] = buf[x];
	s->s[s->l] = 0;
	return 0;
}

static inline int kputuw(unsigned c, kstring_t *s)
{
	char buf[16];
	int l, i;
	unsigned x;
	if (c == 0) return kputc('0', s);
	for (l = 0, x = c; x > 0; x /= 10) buf[l++] = x%10 + '0';
	if (s->l + l + 1 >= s->m) {
		s->m = s->l + l + 2;
		kroundup32(s->m);
		s->s = (char*)realloc(s->s, s->m);
	}
	for (i = l - 1; i >= 0; --i) s->s[s->l++] = buf[i];
	s->s[s->l] = 0;
	return 0;
}
