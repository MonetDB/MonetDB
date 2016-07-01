/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * author Lefteris Sidirourgos
 * Tokenizer
 * This module implements a vertical fragmented tokenizer for strings.
 * It is based on the ideas of the urlbox module by mk.
 *
 * The input string is tokenized according to a separator character.
 * Each token is inserted to the next BAT with the same order of
 * appearance in the string. We currently support 255 tokens in each
 * string as this module is intended for use with short and similar
 * strings such as URLs. In addition we maintain a 2-dimensional index
 * that points to the depth and height of the last token of each string.
 * The 2-dimensional index is combined to one BAT where the 8 least
 * significant bits represent the depth, and the rest bits the height.
 *
 * The tokenizer can be accessed in two ways. Given the oid retrieve the
 * re-constructed string, or given a string return its oid if present,
 * otherwise nil.
 *
 * Strings can be added either in batch (from a file or a bat of
 * strings) and by appending a single string. Duplicate elimination is
 * always performed.
 *
 * There can be only one tokenizer open at the same time. This is
 * achieved by setting a TRANSaction bat. This might change in the
 * future. However there can be more than one tokenizers stored in the
 * disk, each of which is identified by its name (usually the name of
 * the active schema of the db). These administrative issues and
 * security aspects (e.g., opening a tokenizer of a different schema)
 * should be addressed more thoroughly.
 */
#include "monetdb_config.h"
#include "bat5.h"
#include "tokenizer.h"
#include "mal_linker.h"

#define MAX_TKNZR_DEPTH 256
#define INDEX MAX_TKNZR_DEPTH
static int tokenDepth = 0;
struct {
	BAT *idx, *val; 
} tokenBAT[MAX_TKNZR_DEPTH + 1];

static BAT *TRANS = NULL;   /* the catalog of tokenizers */
static char name[128];

#if SIZEOF_OID == 4 /* 32-bit oid */
#define MAX_h ((((oid) 1) << 23) - 1)
#else /* 64-bit oid */
#define MAX_h ((((oid) 1) << 55) - 1)
#endif

#define COMP(h, d) ((h << 8) | (d & 255))
#define GET_d(x) ((sht) ((x) & 255))
#define GET_h(x) ((x) >> 8)

static int prvlocate(BAT* b, BAT* bidx, oid *prv, str part)
{
	BATiter bi = bat_iterator(b);
	BATiter biidx = bat_iterator(bidx);
	BUN p;

	if (BAThash(b, 2 * BATcount(b)) == GDK_SUCCEED) {
		HASHloop_str(bi, b->thash, p, part) {
			if (*((oid *) BUNtail(biidx, p)) == *prv) {
				*prv = (oid) p;
				return TRUE;
			}
		}
	} else {
		/* hash failed, slow scan */
		BUN q;

		BATloop(b, p, q) {
			if (*((oid *) BUNtail(biidx, p)) == *prv &&
				strcmp(BUNtail(bi, p), part) == 0) {
				*prv = (oid) p;
				return TRUE;
			}
		}
	}
	return FALSE;
}

str
TKNZRopen(void *ret, str *in)
{
	int depth;
	bat r;
	bat idx;
	str batname = NULL;
	BAT *b;

	(void) ret;
	if (strlen(*in) > 127)
		throw(MAL, "tokenizer.open",
				ILLEGAL_ARGUMENT " tokenizer name too long");

	MT_lock_set(&mal_contextLock);
	if (TRANS != NULL) {
		MT_lock_unset(&mal_contextLock);
		throw(MAL, "tokenizer.open", "Another tokenizer is already open");
	}

	for (depth = 0; depth < MAX_TKNZR_DEPTH; depth++) {
		tokenBAT[depth].idx = 0;
		tokenBAT[depth].val = 0;
	}
	tokenDepth = 0;

	TRANS = COLnew(0, TYPE_str, MAX_TKNZR_DEPTH + 1, TRANSIENT);
	if (TRANS == NULL) {
		MT_lock_unset(&mal_contextLock);
		throw(MAL, "tokenizer.open", MAL_MALLOC_FAIL);
	}
	/* now we are sure that none overwrites the tokenizer table*/
	MT_lock_unset(&mal_contextLock);

	snprintf(name, 128, "%s", *in);
	batname = (str) GDKmalloc(134 * sizeof(char));
	if( batname == NULL)
		throw(MAL, "tokenizer.open", MAL_MALLOC_FAIL);
	
	snprintf(batname, 134, "%s_index", name);
	idx = BBPindex(batname);

	if (idx == 0) { /* new tokenizer */
		b = COLnew(0, TYPE_oid, 1024, PERSISTENT);
		if (b == NULL)
			throw(MAL, "tokenizer.open", MAL_MALLOC_FAIL);
		tokenBAT[INDEX].val = b;
		if (BKCsetName(&r, &b->batCacheid, (const char*const*) &batname) != MAL_SUCCEED)
			throw(MAL, "tokenizer.open", OPERATION_FAILED);
		if (BKCsetPersistent(&r, &b->batCacheid) != MAL_SUCCEED)
			throw(MAL, "tokenizer.open", OPERATION_FAILED);
		BUNappend(TRANS, batname, FALSE);
	} else { /* existing tokenizer */
		tokenBAT[INDEX].val = BATdescriptor(idx);

		BUNappend(TRANS, batname, FALSE);

		for (depth = 0; depth < MAX_TKNZR_DEPTH; depth++) {
			snprintf(batname, 128, "%s_%d", name, depth);
			idx = BBPindex(batname);
			if (idx == 0)
				break;
			tokenBAT[depth].val = BATdescriptor(idx);
			BUNappend(TRANS, batname, FALSE);

			/* For idx BATs */
			snprintf(batname, 132, "%s_idx_%d", name, depth);
			idx = BBPindex(batname); 
			if (idx == 0)
                        	break;
			tokenBAT[depth].idx = BATdescriptor(idx);
			BUNappend(TRANS, batname, FALSE);

		}
		tokenDepth = depth;
	}

	GDKfree(batname);
	return MAL_SUCCEED;
}

str
TKNZRclose(void *r)
{
	int i;
	(void) r;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");

	TMsubcommit(TRANS);

	for (i = 0; i < tokenDepth; i++) {
		BBPunfix(tokenBAT[i].idx->batCacheid);
		BBPunfix(tokenBAT[i].val->batCacheid);
	}
	BBPunfix(tokenBAT[INDEX].val->batCacheid);
	tokenDepth = 0;

	BBPreclaim(TRANS);
	TRANS = NULL;
	return MAL_SUCCEED;
}

/*
 * Tokenize operations
 * The tokenizer operation assumes a private copy to mark the end of the
 * token separators with a zero byte. Tokens are separated by a single
 * character for simplicity.  Might be a good scheme to assume that
 * strings to be broken are properly ended with either 0 or nl, not
 * both.  It seems 0 can be assumed.
 */
static int
TKNZRtokenize(str in, str *parts, char tkn)
{
	char *s, *t;
	int depth = 0;

	s = in;
	while (*s && *s != '\n') {
		t = s;
		while (*t != tkn && *t != '\n' && *t)
			t++;
		parts[depth++] = s;
		s = t + (*t != 0);
		*t = 0;
		if (depth > MAX_TKNZR_DEPTH)
			break;
	}
	return depth;
}

str
TKNZRappend(oid *pos, str *s)
{
	str url;
	str batname;
	str parts[MAX_TKNZR_DEPTH];
	str msg;
	int i, new, depth;
	bat r;
	BAT *bVal;
	BAT *bIdx; 
	BUN p;
	BUN idx = 0;
	oid prv = 0;
	oid comp;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");

	if ((url = GDKstrdup(*s)) == NULL) {
		throw(MAL, "tokenizer.append", OPERATION_FAILED MAL_MALLOC_FAIL);
	}

	depth = TKNZRtokenize(url, parts, '/');
	new = depth;

	if (depth == 0) {
		GDKfree(url);
		return MAL_SUCCEED;
	}
	if (depth > MAX_TKNZR_DEPTH) {
		GDKfree(url);
		throw(MAL, "tokenizer",
				ILLEGAL_ARGUMENT "input string breaks to too many parts");
	}
	if (depth > tokenDepth || tokenBAT[0].val == NULL) {
		new = tokenDepth;
		for (i = tokenDepth; i < depth; i++) {
			/* make new bat for value */
			batname = (str) GDKmalloc(132 * sizeof(char));
			snprintf(batname, 128, "%s_%d", name, i);
			bVal = COLnew(0, TYPE_str, 1024, PERSISTENT);
			if (bVal == NULL) {
				GDKfree(batname);
				GDKfree(url);
				throw(MAL, "tokenizer.append", MAL_MALLOC_FAIL);
			}
			
			tokenBAT[i].val = bVal;

			if ((msg = BKCsetName(&r, &bVal->batCacheid, (const char*const*) &batname))
				!= MAL_SUCCEED) {
				GDKfree(batname);
				GDKfree(url);
				return msg;
			}
			if ((msg = BKCsetPersistent(&r, &bVal->batCacheid))
				!= MAL_SUCCEED) {
				GDKfree(batname);
				GDKfree(url);
				return msg;
			}
			BUNappend(TRANS, batname, FALSE);

			/* make new bat for index */
			snprintf(batname, 132, "%s_idx_%d", name, i);
			bIdx = COLnew(0, TYPE_oid, 1024, PERSISTENT);
			if (bIdx == NULL) {
				GDKfree(batname);
				GDKfree(url);
				throw(MAL, "tokenizer.append", MAL_MALLOC_FAIL);
			}
			
			tokenBAT[i].idx = bIdx;

			if ((msg = BKCsetName(&r, &bIdx->batCacheid, (const char*const*) &batname)) != MAL_SUCCEED) {
				GDKfree(batname);
				GDKfree(url);
				return msg;
			}
			if ( (msg=BKCsetPersistent(&r, &bIdx->batCacheid)) != MAL_SUCCEED) {
				GDKfree(batname);
				GDKfree(url);
				return msg;
			}
			BUNappend(TRANS, batname, FALSE);


			GDKfree(batname);
		}
		tokenDepth = depth;
	}

	/* findcommn */
	p = BUNfnd(tokenBAT[0].val, parts[0]);
	if (p != BUN_NONE) {
		prv = (oid) p;
		for (i = 1; i < new; i++) {
			if (!prvlocate(tokenBAT[i].val, tokenBAT[i].idx, &prv, parts[i]))
				break;
		}
	} else {
		i = 0;
	}

	if (i == depth) {
		comp = COMP(prv, depth);
		*pos = BUNfnd(tokenBAT[INDEX].val, (ptr) & comp);
		if (*pos != BUN_NONE) {
			/* the string is already there */
			/* printf("The string %s is already there",url); */
			GDKfree(url);
			return MAL_SUCCEED;
		}
	}

	/* insremainder */
	for (; i < depth; i++) {
		idx = BATcount(tokenBAT[i].val);
		if (idx > MAX_h) {
			GDKfree(url);
			throw(MAL, "tokenizer.append",
					OPERATION_FAILED " no more free oid's");
		}
		if (BUNappend(tokenBAT[i].val, parts[i], TRUE) != GDK_SUCCEED) {
			GDKfree(url);
			throw(MAL, "tokenizer.append",
					OPERATION_FAILED " could not append");
		}
		if (tokenBAT[i].val->thash == NULL ||
			tokenBAT[i].val->thash == (Hash *) 1 ||
			BATcount(tokenBAT[i].val) > 4 * tokenBAT[i].val->thash->mask) {
			HASHdestroy(tokenBAT[i].val);
			BAThash(tokenBAT[i].val, 2 * BATcount(tokenBAT[i].val));
		}

		if (BUNappend(tokenBAT[i].idx, (ptr) & prv, TRUE) != GDK_SUCCEED) {
			GDKfree(url);
			throw(MAL, "tokenizer.append",
					OPERATION_FAILED " could not append");
		}

		prv = (oid) idx;
	}

	*pos = (oid) BATcount(tokenBAT[INDEX].val);
	comp = COMP(prv, depth);
	BUNappend(tokenBAT[INDEX].val, (ptr) & comp, TRUE);
	if (tokenBAT[INDEX].val->thash == NULL ||
		tokenBAT[INDEX].val->thash == (Hash *) 1 ||
		BATcount(tokenBAT[INDEX].val) > 4 * tokenBAT[INDEX].val->thash->mask) {
		HASHdestroy(tokenBAT[INDEX].val);
		BAThash(tokenBAT[INDEX].val, 2 * BATcount(tokenBAT[INDEX].val));
	}

	GDKfree(url);
	return MAL_SUCCEED;
}

#define SIZE (1 * 1024 * 1024)
str
TKNZRdepositFile(void *r, str *fnme)
{
	stream *fs;
	bstream *bs;
	char *s, *t;
	int len = 0;
	char buf[PATHLENGTH];
	oid pos;
	str msg= MAL_SUCCEED;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");

	(void) r;
	if (**fnme == '/')
		snprintf(buf, PATHLENGTH, "%s", *fnme);
	else
		snprintf(buf, PATHLENGTH, "%s/%s", monet_cwd, *fnme);
	/* later, handle directory separator */
	fs = open_rastream(buf);
	if (fs == NULL)
		throw(MAL, "tokenizer.depositFile", RUNTIME_FILE_NOT_FOUND "%s", buf);
	if (mnstr_errnr(fs)) {
		close_stream(fs);
		throw(MAL, "tokenizer.depositFile", RUNTIME_FILE_NOT_FOUND "%s", buf);
	}
	bs = bstream_create(fs, SIZE);
	if (bs == NULL)
		throw(MAL, "tokenizer.depositFile", MAL_MALLOC_FAIL);
	while (bstream_read(bs, bs->size - (bs->len - bs->pos)) != 0 &&
		   !mnstr_errnr(bs->s))
	{
		s = bs->buf;
		for (t = s; *t;) {
			while (t < bs->buf + bs->len && *t && *t != '\n')
				t++;
			if (t == bs->buf + bs->len || *t != '\n') {
				/* read next block if possible after shift  */
				assert(t - s <= INT_MAX);
				len = (int) (t - s);
				memcpy(bs->buf, s, len);
				bs->len = len;
				bs->pos = 0;
				break;
			}
			/* found a string to be processed */
			*t = 0;
			msg = TKNZRappend(&pos, &s);
			if (msg ) {
				bstream_destroy(bs);
				mnstr_close(fs);
				mnstr_destroy(fs);
				return msg;
			}
			*t = '\n';
			s = t + 1;
			t = s;
		}
	}

	bstream_destroy(bs);
	mnstr_close(fs);
	mnstr_destroy(fs);
	return MAL_SUCCEED;
}

str
TKNZRlocate(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	oid pos;
	str url;
	str parts[MAX_TKNZR_DEPTH];
	int i = 0, depth;
	BUN p;
	oid prv = 0;
	oid comp;
	(void) cntxt;
	(void) mb;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");

	url = (str) GDKmalloc(sizeof(char) *
			(strlen(*getArgReference_str(stk, pci, 1)) + 1));
	if (url == NULL) {
		throw(MAL, "tokenizer.locate", MAL_MALLOC_FAIL);
	}
	strcpy(url, *getArgReference_str(stk, pci, 1));


	depth = TKNZRtokenize(url, parts, '/');

	if (depth == 0) {
		pos = oid_nil;
	} else if (depth > MAX_TKNZR_DEPTH) {
		GDKfree(url);
		throw(MAL, "tokenizer.locate",
				ILLEGAL_ARGUMENT "strings breaks to too many parts");
	} else if (depth > tokenDepth) {
		pos = oid_nil;
	} else {
		p = BUNfnd(tokenBAT[0].val, parts[0]);
		if (p != BUN_NONE) {
			prv = (oid) p;
			for (i = 1; i < depth; i++) {
				if (!prvlocate(tokenBAT[i].val, tokenBAT[i].idx, (ptr) & prv, parts[i]))
					break;
			}
			if (i < depth) {
				pos = oid_nil;
			} else {
				comp = COMP(prv, i);
				pos = BUNfnd(tokenBAT[INDEX].val, (ptr) & comp);
			}
		} else {
			pos = oid_nil;
		}
	}

	VALset(&stk->stk[pci->argv[0]], TYPE_oid, &pos);
	GDKfree(url);
	return MAL_SUCCEED;
}

str
takeOid(oid id, str *val)
{
	int i, depth;
	str parts[MAX_TKNZR_DEPTH];
	size_t lngth = 0;
	str s;
	BATiter biidx; /* Iterator for index bat */

	if (id >= BATcount(tokenBAT[INDEX].val)) {
		throw(MAL, "tokenizer.takeOid", OPERATION_FAILED " illegal oid");
	}

	id = *(oid *) Tloc(tokenBAT[INDEX].val, id);

	depth = GET_d(id);
	id = GET_h(id);

	for (i = depth - 1; i >= 0; i--) {
		BATiter bi = bat_iterator(tokenBAT[i].val);
		biidx = bat_iterator(tokenBAT[i].idx);
		parts[i] = (str) BUNtail(bi, id);
		id = *(oid *) BUNtail(biidx, id);
		lngth += strlen(parts[i]);
	}

	*val = (str) GDKmalloc(lngth+depth+1);
	if( *val == NULL)
		throw(MAL, "tokenizer.takeOid", MAL_MALLOC_FAIL);
	s = *val;

	for (i = 0; i < depth; i++) {
		strcpy(s, parts[i]);
		s += strlen(parts[i]);
		*s++ = '/';
	}
	*s = '\0';

	return MAL_SUCCEED;
}

str
TKNZRtakeOid(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	str ret, val = NULL;
	oid id;
	(void) cntxt;
	(void) mb;

	if (TRANS == NULL) {
		throw(MAL, "tokenizer", "no tokenizer store open");
	}
	id = *getArgReference_oid(stk, pci, 1);
	ret = takeOid(id, &val);
	if (ret == MAL_SUCCEED) {
		VALset(&stk->stk[pci->argv[0]], TYPE_str, val);
	}
	return ret;
}

str
TKNZRgetIndex(bat *r)
{
	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");
	*r = tokenBAT[INDEX].val->batCacheid;
	BBPincref(*r, TRUE);
	return MAL_SUCCEED;
}

str
TKNZRgetLevel(bat *r, int *level)
{
	BAT* view;
	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");
	if (*level < 0 || *level >= tokenDepth)
		throw(MAL, "tokenizer.getLevel", OPERATION_FAILED " illegal level");
	view = VIEWcreate(tokenBAT[*level].val->hseqbase, tokenBAT[*level].val);
	if (view == NULL)
		throw(MAL, "tokenizer.getLevel", MAL_MALLOC_FAIL);
	*r = view->batCacheid;

	BBPkeepref(*r);
	return MAL_SUCCEED;
}

str
TKNZRgetCount(bat *r)
{
	BAT *b;
	int i;
	lng cnt;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");
	b = COLnew(0, TYPE_lng, tokenDepth + 1, TRANSIENT);
	if (b == NULL)
		throw(MAL, "tokenizer.getCount", MAL_MALLOC_FAIL);
	for (i = 0; i < tokenDepth; i++) {
		cnt = (lng) BATcount(tokenBAT[i].val);
		BUNappend(b, &cnt, FALSE);
	}
	BATsetcount(b, tokenDepth);
	*r = b->batCacheid;
	BBPkeepref(*r);
	return MAL_SUCCEED;
}

str
TKNZRgetCardinality(bat *r)
{
	BAT *b, *en;
	int i;
	lng cnt;

	if (TRANS == NULL)
		throw(MAL, "tokenizer", "no tokenizer store open");
	b = COLnew(0, TYPE_lng, tokenDepth + 1, TRANSIENT);
	if (b == NULL)
		throw(MAL, "tokenizer.getCardinality", MAL_MALLOC_FAIL);
	for (i = 0; i < tokenDepth; i++) {
		if ((en = BATunique(tokenBAT[i].val, NULL)) == NULL) {
			BBPreclaim(b);
			throw(MAL, "tokenizer.getCardinality", GDK_EXCEPTION);
		}
		cnt = (lng) BATcount(en);
		BBPunfix(en->batCacheid);
		BUNappend(b, &cnt, FALSE);
	}

	BATsetcount(b, tokenDepth);
	*r = b->batCacheid;
	BBPkeepref(*r);
	return MAL_SUCCEED;
}

