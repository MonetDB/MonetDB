/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 * This file contains a model for Coverity Scan.
 * This file is not a normal source file.  It is not compiled by any
 * compiler, but instead it is uploaded to the Coverity site and used
 * during any analysis they do on our code.
 *
 * We model our use of the various allocation functions.
 * Things we want to do is model that GDKmalloc and friends are paired
 * with GDKfree, and that exceptions created by createException and
 * createMalException should be freed with freeException.
 *
 * author: Sjoerd Mullender
 */

int
rand(void)
{
	/* ignore */
}

typedef enum { GDK_FAIL, GDK_SUCCEED } gdk_return;

void
GDKfree(void *blk)
{
	if (blk) {
		__coverity_free__(blk);
		__coverity_mark_as_afm_freed__(blk, "GDKfree");
	}
}

void *
GDKmalloc(size_t size)
{
	int has_memory;
	__coverity_negative_sink__(size);
	if(has_memory) {
		void *p = __coverity_alloc__(size);
		__coverity_mark_as_afm_allocated__(p, "GDKfree");
		__coverity_mark_as_uninitialized_buffer__(p);
		return p;
	}
	return 0;
}

void *
GDKzalloc(size_t size)
{
	void *p = GDKmalloc(size);
	if (p) {
		size_t i;
		for (i = 0; i < size; i++)
			((char *) p)[i] = 0;
	}
	return p;
}

char *
GDKstrdup(const char *s)
{
	char *p;
	size_t i;
	int has_memory;
	if (s == 0)
		return 0;
	__coverity_string_null_sink__(s);
	__coverity_string_size_sink__(s);
	if (has_memory) {
		p = __coverity_alloc_nosize__();
		__coverity_mark_as_afm_allocated__(p, "GDKfree");
		for (i = 0; (p[i] = s[i]); i++)
			;
		return p;
	}
	return 0;
}

char *
GDKstrndup(const char *s, size_t size)
{
	char *p;
	size_t i;
	__coverity_negative_sink__(size);
	if (s == 0)
		return 0;
	p = GDKmalloc(size + 1);
	if (p) {
		for (i = 0; i < size && (p[i] = s[i]); i++)
			;
		p[i] = 0;
	}
	return p;
}

void *
GDKrealloc(void *blk, size_t size)
{
	void *p = GDKmalloc(size);
	if (p != 0)
		GDKfree(blk);
	return p;
}

void *
GDKmmap(const char *path, int mode, size_t size)
{
	int has_memory;
	__coverity_negative_sink__(size);
	if (has_memory) {
		void *p = __coverity_alloc__(size);
		__coverity_mark_as_afm_allocated__(p, "GDKmunmap");
		__coverity_writeall__(p);
		return p;
	}
	return 0;
}

gdk_return
GDKmunmap(void *p, size_t size)
{
	int failed;
	__coverity_free__(p);
	__coverity_mark_as_afm_freed__(p, "GDKmunmap");
	return failed ? GDK_FAIL : GDK_SUCCEED;
}

void *
GDKmremap(const char *path, int mode, void *old_address, size_t old_size, size_t *new_size)
{
	void *p = GDKmmap(path, mode, new_size);
	if (p) {
		(void) GDKmunmap(old_address, old_size);
	}
	return p;
}

typedef struct {} *MalBlkPtr;
enum malexception {
	MAL,
	ILLARG /*,
	OUTOFBNDS,
	IO,
	INVCRED,
	OPTIMIZER,
	STKOF,
	SYNTAX,
	TYPE,
	LOADER,
	PARSE,
	ARITH,
	PERMD,
	SQL */
};

char *
createException(enum malexception type, const char *fcn, const char *format, ...)
{
	char *p;
	__coverity_format_string_sink__(format);
	p = __coverity_alloc_nosize__();
	__coverity_mark_as_afm_allocated__(p, "freeException");
	return p;
}

char *
createMalException(MalBlkPtr mb, int pc, enum malexception type, const char *format, ...)
{
	char *p;
	__coverity_format_string_sink__(format);
	p = __coverity_alloc_nosize__();
	__coverity_mark_as_afm_allocated__(p, "freeException");
	return p;
}

char *
dupError(const char *err)
{
	char *p;
	p = __coverity_alloc_nosize__();
	__coverity_mark_as_afm_allocated__(p, "freeException");
	return p;
}

void
freeException(char *p)
{
	if (p) {
		__coverity_free__(p);
		__coverity_mark_as_afm_freed__(p, "freeException");
	}
}

char *
concatErrors(char *err1, const char *err2)
{
	char *p;
	freeException(err1);
	p = __coverity_alloc_nosize__();
	__coverity_mark_as_afm_allocated__(p, "freeException");
	return p;
}
