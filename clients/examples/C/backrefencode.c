/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"

#include "bincopybackref.h"

// #include <assert.h>
// #include <errno.h>
#include <limits.h>
// #include <stddef.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>

#ifdef _WIN32
  #include <io.h>
  #include <fcntl.h>
#endif

// There we go again
struct buf {
	char *base;
	size_t pos;
	size_t len;
	size_t cap;
};

static int
read_more(struct buf *buf, FILE *fin)
{
	// fprintf(stderr, "# read_more %zu %zu %zu\n", buf->pos, buf->len, buf->cap);
	if (buf->pos > 0) {
		if (buf->len > buf->pos)
			memmove(buf->base, buf->base + buf->pos, buf->len-buf->pos);
		buf->len -= buf->pos;
		buf->pos = 0;
	}
	if (buf->len == buf->cap) {
		// need to make room
		size_t newcap = buf->cap == 0 ? 128 * 1024 : 2 * buf->cap;
		if (newcap > INT_MAX) {
			errno = ENOMEM;
			return -1;
		}
		// fprintf(stderr, "reallocating %zu -> %zu\n", buf->cap, newcap);
		char *newbase = realloc(buf->base, newcap);
		if (newbase == NULL) {
			return -1;
		}
		buf->base = newbase;
		buf->cap = newcap;
	}
	assert(buf->cap > buf->len);

	size_t nread = fread(buf->base + buf->len, 1, buf->cap-buf->len, fin);
	// fprintf(stderr, "#   read %zu/%zu\n", nread, buf->cap-buf->len);
	if (nread == 0)
		return 0;
	buf->len += nread;

	// cap is limited to MAX_INT
	return (int) nread;
}


static int
encode(FILE *fin, FILE *fout)
{
	struct buf buf = { NULL };
	backref_memory *mem = backref_create();


	item_index index = 0;
	while (1) {
		int nread = read_more(&buf, fin);
		if (nread < 0) {
			fprintf(stderr, "read error: %s\n", strerror(errno));
			return 1;
		}
		if (nread == 0) {
			if (buf.pos == buf.len)
				break;
			fprintf(stderr, "trailing garbage\n");
			return 1;
		}

		// whatever was in the buffer before the read didn't contain a
		// NUL or we would have processed it already.
		// So we can start scanning at len-nread.
		char *item_start = buf.base + buf.pos;
		char *buf_end = buf.base + buf.len;
		char *p = buf_end - nread;
		assert(p >= item_start);

		while ((p = memchr(p, 0, buf_end - p)) != NULL) {
			p++;
			index++;

			size_t output_len;
			const char *output;
			output = backref_encode(mem, item_start, index, &output_len);
			assert(output_len > 0);
			while (output_len > 0) {
				size_t nwritten = fwrite(output, 1, output_len, fout);
				output += nwritten;
				output_len -= nwritten;
			}
			item_start = p;
		}
		buf.pos = item_start - buf.base;
	}

	fflush(fout);
	backref_destroy(mem);
	free(buf.base);
	return 0;
}

static int
decode(FILE *fin, FILE *fout)
{
	struct buf inbuf = { NULL };
	char **mem = NULL;
	bool *is_duplicate = NULL;
	size_t mem_len = 0;
	size_t mem_cap = 0;

	while (1) {
		int nread = read_more(&inbuf, fin);
		if (nread < 0) {
			fprintf(stderr, "read error: %s\n", strerror(errno));
			return 1;
		}
		if (nread == 0) {
			if (inbuf.pos == inbuf.len)
				break;
			fprintf(stderr, "trailing garbage\n");
			return 1;
		}

		// Process as much as possible
		unsigned char *pos = (unsigned char *)&inbuf.base[inbuf.pos];
		unsigned char *limit = (unsigned char *)&inbuf.base[inbuf.len];
		while (pos < limit) {
			if (mem_len == mem_cap) {
				size_t new_mem_cap = 1024 + 2 * mem_cap;
				// fprintf(stderr, "old buffer: %p size %zu\n", mem, mem_cap);
				mem = (char**) realloc(mem, sizeof(*mem) * new_mem_cap);
				is_duplicate = (bool*) realloc(is_duplicate, sizeof(*is_duplicate) * new_mem_cap);
				if (mem == NULL || is_duplicate == NULL) {
					fprintf(stderr, "alloc failed\n");
					return 1;
				}
				mem_cap = new_mem_cap;
				// fprintf(stderr, "new buffer: %p size %zu\n", mem, mem_cap);
			}
			assert(mem_len < mem_cap);
			if (*pos > 0x80 && *pos <= 0xBF) {
				// short backref
				size_t delta = *pos++ - 0x80;
				if (delta > mem_len) {
					fprintf(stderr, "invalid short backref\n");
					return 1;
				}
				// fprintf(stderr, "put %zu reuse %zu\n", mem_len, mem_len - delta);
				mem[mem_len] = mem[mem_len - delta];
				is_duplicate[mem_len] = true;
			} else if (*pos == 0x80 && limit - pos > 1 && pos[1] > 0) {
				// long backref, but maybe incomplete
				unsigned char *p = pos + 1;
				size_t delta = *p & 0x7F;
				int shift = 0;
				while (*p++ >= 0x80) {
					if (p == limit) {
						delta = 0;
						break;
					}
					shift += 7;
					delta += (*p & 0x7F) << shift;
				}
				if (delta == 0) {
					// incomplete long backref
					break;
				}
				if (delta > mem_len) {
					fprintf(stderr, "invalid long backref\n");
					return 1;
				}
				// fprintf(stderr, "put %zu reuse %zu\n", mem_len, mem_len - delta);
				mem[mem_len] = mem[mem_len - delta];
				is_duplicate[mem_len] = true;
				pos = p;
			} else {
				// nil or utf-8 encoded string
				unsigned char *p = memchr(pos, 0, limit - pos);
				if (p == NULL) {
					// incomplete
					break;
				}

				// fprintf(stderr, "put %zu '%s'\n", mem_len, (char*)pos);
				mem[mem_len] = strdup((char*)pos);
				is_duplicate[mem_len] = false;
				if (mem[mem_len] == NULL) {
					fprintf(stderr, "alloc failed\n");
					return 1;
				}
				pos = p + 1;
			}
			// end of if-then, mem[mem_len] has been set
			fputs(mem[mem_len], fout);
			fputc(0, fout);
			mem_len++;
		}
		// record what's been consumed
		inbuf.pos = (char*)pos - inbuf.base;
	}

	for (size_t i = 0; i < mem_len; i++)
		if (!is_duplicate[i])
			free(mem[i]);

	fflush(fout);
	free(mem);
	free(is_duplicate);
	free(inbuf.base);
	return 0;
}

int
main(int argc, char **argv)
{
	bool do_encode;

	switch (argc) {
		case 1:
			do_encode = true;
			break;
		case 2:
			if (strcmp(argv[1], "-d") == 0) {
				do_encode = false;
				break;
			}
			/* fallthrough */
		default:
			fprintf(stderr, "Usage: %s [-d] <input >output\n", argv[0]);
			return 1;
	}

	#ifdef _WIN32
	setmode(fileno(stdout),O_BINARY);
	setmode(fileno(stdin),O_BINARY);
	#endif

	if (setvbuf(stdout, NULL, _IOFBF, 128 * 1024) < 0) {
		fprintf(stderr, "setvbuf stdout: %s\n", strerror(errno));
	}

	if (do_encode)
		return encode(stdin, stdout);
	else
		return decode(stdin, stdout);
}
