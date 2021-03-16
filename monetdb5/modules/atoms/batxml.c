/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

/*
 *  M.L. Kersten
 *  XML multiplexes
 * SQL/XML requires a handful of instructions.
 * The collection of routines provided here are map operations
 * for the atom xml primitives.
 *
 * In line with the batcalc module, we assume that if two bat operands
 * are provided that they are aligned.
 *
 * The implementation is focussed on functionality. At a later stage
 * we may postpone string contstruction until it is really needed.
 */



#include "monetdb_config.h"
#include "gdk.h"
#include <ctype.h>
#include <string.h>
#ifdef HAVE_LIBXML
#include <libxml/parser.h>
#endif
#include "mal_interpreter.h"
#include "mal_function.h"
#include "xml.h"

#include "mel.h"

#ifdef HAVE_LIBXML

#define prepareResult(X,Y,tpe,Z,free)								\
	do {															\
		(X) = COLnew((Y)->hseqbase, (tpe), BATcount(Y), TRANSIENT);	\
		if ((X) == NULL) {											\
			BBPunfix((Y)->batCacheid);								\
			free;													\
			throw(MAL, "xml." Z, SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}															\
		(X)->tsorted =  false;										\
		(X)->trevsorted =  false;									\
		(X)->tnonil = true;											\
	} while (0)

#define finalizeResult(X,Y,Z)					\
	do {										\
		BATsetcount((Y), (Y)->batCount);		\
		(Y)->theap->dirty |= (Y)->batCount > 0;	\
		*(X) = (Y)->batCacheid;					\
		BBPkeepref(*(X));						\
		BBPunfix((Z)->batCacheid);				\
	} while (0)

static str
BATXMLxml2str(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xml.str", INTERNAL_BAT_ACCESS);
	prepareResult(bn, b, TYPE_str, "str", (void) 0);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);

		if (strNil(t)) {
			if (bunfastappVAR(bn, t) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
		} else {
			assert(*t == 'A' || *t == 'C' || *t == 'D');
			if (bunfastappVAR(bn, t + 1) != GDK_SUCCEED)
				goto bunins_failed;
		}
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.str", OPERATION_FAILED " during bulk coercion");
}

static str
BATXMLxmltext(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	size_t size = 0;
	str buf = NULL;
	xmlDocPtr doc = NULL;
	xmlNodePtr elem;
	str content = NULL;
	const char *err = OPERATION_FAILED;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xml.text", INTERNAL_BAT_ACCESS);
	prepareResult(bn, b, TYPE_str, "text", (void) 0);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		if (strNil(t)) {
			if (bunfastappVAR(bn, t) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}
		len = strlen(t);
		switch (*t) {
		case 'D': {
			xmlDocPtr d = xmlParseMemory(t + 1, (int) (len - 1));
			elem = xmlDocGetRootElement(d);
			content = (str) xmlNodeGetContent(elem);
			xmlFreeDoc(d);
			if (content == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
			break;
		}
		case 'C':
			if (doc == NULL)
				doc = xmlParseMemory("<doc/>", 6);
			xmlParseInNodeContext(xmlDocGetRootElement(doc), t + 1, (int) (len - 1), 0, &elem);
			content = (str) xmlNodeGetContent(elem);
			xmlFreeNodeList(elem);
			if (content == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
			break;
		case 'A': {
			str s;

			if (buf == NULL || size < len) {
				size = len + 128;
				if (buf != NULL)
					GDKfree(buf);
				buf = GDKmalloc(size);
				if (buf == NULL) {
					err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
			}
			s = buf;
			t++;
			while (*t) {
				if (*t == '"' || *t == '\'') {
					char q = *t++;

					s += XMLunquotestring(&t, q, s);
				}
				t++;
			}
			*s = 0;
			break;
		}
		default:
			assert(*t == 'A' || *t == 'C' || *t == 'D');
			if (bunfastappVAR(bn, str_nil) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}
		assert(content != NULL || buf != NULL);
		if (bunfastappVAR(bn, content != NULL ? content : buf) != GDK_SUCCEED)
			goto bunins_failed;
		if (content != NULL)
			GDKfree(content);
		content = NULL;
	}
	finalizeResult(ret, bn, b);
	if (buf != NULL)
		GDKfree(buf);
	if (doc != NULL)
		xmlFreeDoc(doc);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	if (doc != NULL)
		xmlFreeDoc(doc);
	if (content != NULL)
		GDKfree(content);
	throw(MAL, "xml.text", "%s", err);
}

/*
 * The core of the activity is str2xml, where the actual strings
 * are constructed.
 * To avoid repetitive copying we make sure that the garbage
 * collector does not remove the xml intermediates.
 * This way, we know that as long as the xml-variables are not
 * reused, the complete structure of the xml document(s) are available.
 * We merely have to collect the pieces.
 * [FOR LATER, FIRST GO FOR THE EASY IMPLEMENTATION]
 * XML values are represented by strings already.
 */
static str
BATXMLstr2xml(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf;
	const char *err= OPERATION_FAILED;
	BATiter bi;

	buf = GDKmalloc(size);
	if (buf == NULL)
		throw(MAL,"xml.str2xml", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.xml", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "xml", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		if (strNil(t)) {
			if (bunfastappVAR(bn, str_nil) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}

		len = strlen(t) * 6 + 1;
		if (size < len) {
			size = len + 128;
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'C';
		XMLquotestring(t, buf + 1, size - 1);
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.xml", "%s", err);
}

static str
BATXMLdocument(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	const char *err = OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL,"xml.document", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.document", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "document", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		xmlDocPtr doc;
		int len;
		xmlChar *s;

		if (strNil(t)) {
			if (bunfastappVAR(bn, str_nil) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}
		len = (int) strlen(t);
		doc = xmlParseMemory(t, len);
		if (doc == NULL) {
			err = OPERATION_FAILED XML_PARSE_ERROR;
			goto bunins_failed;
		}
		xmlDocDumpMemory(doc, &s, &len);
		xmlFreeDoc(doc);
		if ((size_t) len + 2 >= size) {
			GDKfree(buf);
			size = (size_t) len + 128;
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'D';
		strcpy(buf + 1, (char *) s);
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	GDKfree(buf);
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.document", "%s", err);
}

static str
BATXMLcontent(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	xmlDocPtr doc;
	xmlNodePtr root;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	const char *err = OPERATION_FAILED;
	xmlBufferPtr xbuf;

	if (buf == NULL)
		throw(MAL,"xml.content", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.content", INTERNAL_BAT_ACCESS);
	}
	doc = xmlParseMemory("<doc/>", 6);
	root = xmlDocGetRootElement(doc);
	prepareResult(bn, b, TYPE_xml, "content", GDKfree(buf));
	bi = bat_iterator(b);
	xbuf = xmlBufferCreate();
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;
		xmlNodePtr elem;
		xmlParserErrors xerr;
		const xmlChar *s;

		if (strNil(t)) {
			if (bunfastappVAR(bn, str_nil) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}
		len = strlen(t);
		xerr = xmlParseInNodeContext(root, t, (int) len, 0, &elem);
		if (xerr != XML_ERR_OK) {
			err = XML_PARSE_ERROR;
			goto bunins_failed;
		}
		xmlNodeDump(xbuf, doc, elem, 0, 0);
		s = xmlBufferContent(xbuf);
		len = strlen((const char *) s);
		if (len + 2 >= size) {
			GDKfree(buf);
			size = len + 128;
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'C';
		strcpy(buf + 1, (const char *) s);
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
		xmlBufferEmpty(xbuf);
		xmlFreeNodeList(elem);
	}
	xmlBufferFree(xbuf);
	xmlFreeDoc(doc);
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	xmlBufferFree(xbuf);
	xmlFreeDoc(doc);
	if (buf != NULL)
		GDKfree(buf);
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.document", "%s", err);
}

static str
BATXMLisdocument(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "xml.isdocument", INTERNAL_BAT_ACCESS);
	prepareResult(bn, b, TYPE_bit, "isdocument", (void) 0);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		xmlDocPtr doc;
		bit val;

		if (strNil(t)) {
			val = bit_nil;
			bn->tnonil = false;
		} else {
			doc = xmlParseMemory(t, (int) strlen(t));
			if (doc == NULL) {
				val = 0;
			} else {
				xmlFreeDoc(doc);
				val = 1;
			}
		}
		if (bunfastappTYPE(bit, bn, &val) != GDK_SUCCEED) {
			BBPunfix(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "xml.isdocument",
				  OPERATION_FAILED " During bulk processing");
		}
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
}

/*
 * The standard supports specific mappings for
 * NULL values,i.e. {null,absent,empty,nil,niloncontent)
 * in the context of an element and forest construction.
 * The standard should be studied in more detail, because
 * the syntax(rules) seem ambiguous.
 * It applies to all components of an element or their
 * concatenation.
 *
 * For the time being, the variaton on XMLtag seems the
 * most reasonable interpretation.
 */
static str
BATXMLoptions(bat *ret, const char * const *name, const char * const *options, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	str buf = GDKmalloc(BUFSIZ);
	str val = GDKmalloc(BUFSIZ);
	size_t size = BUFSIZ, len = strlen(*name);
	BATiter bi;
	const char *err = OPERATION_FAILED " During bulk options analysis";

	if (val == NULL || buf == NULL) {
		if (val != NULL)
			GDKfree(val);
		if (buf != NULL)
			GDKfree(buf);
		throw(MAL, "batxml.options", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(val);
		GDKfree(buf);
		throw(MAL, "xml.options", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "options", GDKfree(val); GDKfree(buf));

	if (strcmp(*options, "absent") == 0)
		buf[0] = 0;
	else if (strcmp(*options, "empty") == 0)
		snprintf(buf, size, "<%s></%s>", *name, *name);
	else if (strcmp(*options, "null") == 0)
		snprintf(buf, size, "null");
	else if (strcmp(*options, "nil") == 0)
		snprintf(buf, size, "nil");
	else {
		/*if(strcmp(*options,"niloncontent")==0) */
		err = SQLSTATE(0A000) PROGRAM_NYI;
		goto bunins_failed;
	}

	snprintf(val, size, "<%s>", *name);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);

		if (strNil(t)) {
			if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
				goto bunins_failed;
		} else {
			if (strlen(t) > size - 2 * len - 6) {
				char *tmp;
				size += strlen(t);
				tmp = (char *) GDKrealloc(val, size + strlen(t));
				if (tmp == NULL) {
					err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
				val = tmp;
			}
			snprintf(val + len + 2, size - len, "%s</%s>", t, *name);
			if (bunfastappVAR(bn, val) != GDK_SUCCEED)
				goto bunins_failed;
		}
	}
	GDKfree(val);
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	if (val != NULL)
		GDKfree(val);
	throw(MAL, "batxml.options", "%s", err);
}

static str
BATXMLcomment(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	const char *err= OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL, "xml.comment", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.comment", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "comment", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		if (strNil(t)) {
			if (bunfastappVAR(bn, str_nil) != GDK_SUCCEED)
				goto bunins_failed;
			bn->tnonil = false;
			continue;
		}
		if (strstr(t, "--") != NULL) {
			err = XML_COMMENT_ERROR;
			goto bunins_failed;
		}
		len = strlen(t);
		if (len + 9 >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		snprintf(buf, size, "C<!--%s-->", t);
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.comment", "%s", err);
}

static str
BATXMLparse(bat *ret, const char * const *doccont, const bat *bid, const char * const *option)
{
	(void) option;
	if (strcmp(*doccont, "content") == 0)
		return BATXMLcontent(ret, bid);
	if (strcmp(*doccont, "document") == 0)
		return BATXMLdocument(ret, bid);
	throw(MAL, "xml.parse", ILLEGAL_ARGUMENT " <document> or <content> expected");
}

static str
BATXMLpi(bat *ret, const char * const *target, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf;
	BATiter bi;
	size_t tgtlen;
	const char *err = OPERATION_FAILED;

	if (strNil(*target))
		throw(MAL, "xml.pi", XML_PI_ERROR);
	buf = GDKmalloc(size);
	if (buf == NULL)
		throw(MAL, "xml.pi", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	tgtlen = strlen(*target) + 6;
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.pi", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "pi", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		len = tgtlen;
		if (!strNil(t))
			len += strlen(t) * 6 + 1;
		if (len >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t))
			snprintf(buf, size, "C<?%s?>", *target);
		else {
			int n = snprintf(buf, size, "C<?%s ", *target);
			size_t m = XMLquotestring(t, buf + n, size - n);
			strcpy(buf + n + m, "?>");
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.pi", "%s", err);
}

static str
BATXMLroot(bat *ret, const bat *bid, const char * const *version, const char * const *standalone)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf;
	BATiter bi;
	size_t hdrlen;
	const char *err = OPERATION_FAILED;

	hdrlen = 8;
	if (!strNil(*version) && **version) {
		if (strcmp(*version, "1.0") != 0 && strcmp(*version, "1.1") != 0)
			throw(MAL, "xml.root", XML_VERSION_ERROR);
		hdrlen += 11 + strlen(*version);  /* strlen(" version=\"\"") */
	}
	if (!strNil(*standalone) && **standalone) {
		if (strcmp(*standalone, "yes") != 0 && strcmp(*standalone, "no") != 0)
			throw(MAL, "xml.root", XML_STANDALONE_ERROR "illegal XML standalone value");
		hdrlen += 14 + strlen(*standalone);  /* strlen(" standalone=\"\"") */
	}
	buf = GDKmalloc(size);
	if (buf == NULL)
		throw(MAL, "xml.root", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.pi", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "pi", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len, i;
		bit isdoc;

		len = hdrlen;
		if (!strNil(t))
			len += strlen(t);
		if (len >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t)) {
			strcpy(buf, str_nil);
			bn->tnonil = false;
		} else {
			strcpy(buf, "D<?xml");
			i = strlen(buf);
			if (!strNil(*version) && **version)
				i += snprintf(buf + i, len - i, " version=\"%s\"", *version);
			if (!strNil(*standalone) && **standalone)
				i += snprintf(buf + i, len - i, " standalone=\"%s\"", *standalone);
			snprintf(buf + i, len - i, "?>%s", t + 1);
			buf++;
			XMLisdocument(&isdoc, &buf); /* check well-formedness */
			buf--;
			if (!isdoc) {
				err = XML_NOT_WELL_FORMED;
				goto bunins_failed;
			}
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.root", "%s", err);
}

static str
BATXMLattribute(bat *ret, const char * const *name, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf;
	BATiter bi;
	size_t attrlen;
	const char *err = OPERATION_FAILED;

	if (strNil(*name))
		throw(MAL, "xml.attribute", XML_ATTRIBUTE_ERROR);
	if (xmlValidateName((xmlChar *) *name, 0) != 0)
		throw(MAL, "xml.attribute", XML_ATTRIBUTE_INVALID);
	attrlen = strlen(*name) + 5;
	buf = GDKmalloc(size);
	if (buf == NULL)
		throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.attribute", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "attribute", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		len = attrlen;
		if (!strNil(t))
			len += strlen(t) * 6 + 1;
		if (len >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t)) {
			strcpy(buf, str_nil);
			bn->tnonil = false;
		} else {
			int n = snprintf(buf, size, "A%s = \"", *name);
			size_t m = XMLquotestring(t, buf + n, size - n);
			strcpy(buf + n + m, "\"");
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.attribute", "%s", err);
}

static str
BATXMLelement(bat *ret, const char * const *name, xml *nspace, xml *attr, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf;
	BATiter bi;
	size_t elemlen, namelen;
	const char *err = OPERATION_FAILED;

	if (strNil(*name))
		throw(MAL, "xml.element", XML_NO_ELEMENT);
	if (xmlValidateName((xmlChar *) *name, 0) != 0)
		throw(MAL, "xml.element", XML_ATTRIBUTE_INVALID);
	if (nspace && !strNil(*nspace) && **nspace)
		throw(MAL, "xml.element", XML_NO_NAMESPACE);
	namelen = strlen(*name);
	elemlen = namelen + 5;
	if (nspace && !strNil(*nspace)) {
		if (**nspace != 'A')
			throw(MAL, "xml.element", XML_ILLEGAL_NAMESPACE);
		elemlen += strlen(*nspace);	/* " " + nspace (nspace contains initial 'A' which is replaced by space) */
	}
	if (attr && !strNil(*attr)) {
		if (**attr != 'A')
			throw(MAL, "xml.element", XML_ILLEGAL_ATTRIBUTE);
		elemlen += strlen(*attr);	/* " " + attr (attr contains initial 'A' which is replaced by space) */
	}
	buf = GDKmalloc(size);
	if (buf == NULL)
		throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.element", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "element", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtvar(bi, p);
		size_t len;

		len = elemlen;
		if (!strNil(t)) {
			if (*t != 'C') {
				err = XML_ILLEGAL_CONTENT;
				goto bunins_failed;
			}
			len += strlen(t + 1) + namelen + 2;  /* extra "<", ">", and name ("/" already counted) */
		}
		if (len >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t) && (!attr || strNil(*attr))) {
			strcpy(buf, str_nil);
			bn->tnonil = false;
		} else {
			int i = snprintf(buf, size, "C<%s", *name);
			if (nspace && !strNil(*nspace))
				i += snprintf(buf + i, size - i, " %s", *nspace + 1);
			if (attr && !strNil(*attr))
				i += snprintf(buf + i, size - i, " %s", *attr + 1);
			if (!strNil(t))
				i += snprintf(buf + i, size - i, ">%s</%s>", t + 1, *name);
			else
				i += snprintf(buf + i, size - i, "/>");
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.element", "%s", err);
}

static str
BATXMLelementSmall(bat *ret, const char * const *name, const bat *bid)
{
	return BATXMLelement(ret, name, NULL, NULL, bid);
}

static str
BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *ret = getArgReference_bat(stk, pci, 0);
	BAT *bn;
	BATiter *bi;
	BUN *p, *q;
	str buf;
	int i;
	size_t offset, len, size = BUFSIZ;
	const char *err = OPERATION_FAILED;

	(void) mb;
	(void) cntxt;
	buf = GDKmalloc(size);
	bi = GDKmalloc(sizeof(BATiter) * pci->argc);
	p = GDKmalloc(sizeof(BUN) * pci->argc);
	q = GDKmalloc(sizeof(BUN) * pci->argc);
	if (buf == NULL || bi == NULL || p == NULL || q == NULL) {
		if (buf)
			GDKfree(buf);
		if (bi)
			GDKfree(bi);
		if (p)
			GDKfree(p);
		if (q)
			GDKfree(q);
		throw(MAL, "xml.forest", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	/* collect the admin for the xml elements */
	for (i = pci->retc; i < pci->argc; i++) {
		if ((bi[i].b = BATdescriptor(*getArgReference_bat(stk, pci, i))) == NULL)
			break;
		p[i] = 0;
		q[i] = BUNlast(bi[i].b);
	}
	/* check for errors */
	if (i != pci->argc) {
		for (i--; i >= pci->retc; i--)
			if (bi[i].b)
				BBPunfix(bi[i].b->batCacheid);
		GDKfree(bi);
		GDKfree(p);
		GDKfree(q);
		GDKfree(buf);
		throw(MAL, "xml.forest", INTERNAL_BAT_ACCESS);
	}

	prepareResult(bn, bi[pci->retc].b, TYPE_xml, "forest",
				  for (i = pci->retc; i < pci->argc; i++) BBPunfix(bi[i].b->batCacheid);
				  GDKfree(bi); GDKfree(p); GDKfree(q); GDKfree(buf));

	while (p[pci->retc] < q[pci->retc]) {
		const char *t;

		/* fetch the elements */
		offset = 0;
		strcpy(buf, str_nil);
		for (i = pci->retc; i < pci->argc; i++) {
			int n;

			t = (const char *) BUNtvar(bi[i], p[i]);
			if (strNil(t))
				continue;

			if ((len = strlen(t)) >= size - offset) {
				char *tmp;
				size += len + 128;
				tmp = GDKrealloc(buf, size);
				if (tmp == NULL) {
					err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
				buf = tmp;
			}
			if (offset == 0)
				n = snprintf(buf, size, "%s", t);
			else if (buf[0] != *t) {
				err = "incompatible values in forest";
				goto bunins_failed;
			} else if (buf[0] == 'A')
				n = snprintf(buf + offset, size - offset, " %s", t + 1);
			else if (buf[0] == 'C')
				n = snprintf(buf + offset, size - offset, "%s", t + 1);
			else {
				err = "can only combine attributes and element content";
				goto bunins_failed;
			}
			offset += n;
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
		if (offset == 0)
			bn->tnonil = false;

		for (i = pci->retc; i < pci->argc; i++)
			if (bi[i].b)
				p[i]++;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, bi[pci->retc].b);
	GDKfree(bi);
	GDKfree(p);
	GDKfree(q);
	return MAL_SUCCEED;
bunins_failed:
	for (i = pci->retc; i < pci->argc; i++)
		if (bi[i].b)
			BBPunfix(bi[i].b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	GDKfree(bi);
	GDKfree(p);
	GDKfree(q);
	throw(MAL, "xml.forest", "%s", err);
}

static str
BATXMLconcat(bat *ret, const bat *bid, const bat *rid)
{
	BAT *b, *r = 0, *bn;
	BUN p, q, rp = 0;
	size_t len, size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi, ri;
	const char *err = OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL, "xml.concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	b = BATdescriptor(*bid);
	r = BATdescriptor(*rid);
	if (b == NULL || r == NULL) {
		GDKfree(buf);
		if (b)
			BBPunfix(b->batCacheid);
		if (r)
			BBPunfix(r->batCacheid);
		throw(MAL, "xml.concat", INTERNAL_BAT_ACCESS);
	}
	p = 0;
	q = BUNlast(b);
	rp = 0;

	prepareResult(bn, b, TYPE_xml, "concat",
				  GDKfree(buf); BBPunfix(r->batCacheid));

	bi = bat_iterator(b);
	ri = bat_iterator(r);
	while (p < q) {
		const char *t = (const char *) BUNtvar(bi, p);
		const char *v = (const char *) BUNtvar(ri, rp);

		len = strlen(t) + strlen(v) + 1;

		if (len >= size) {
			GDKfree(buf);
			size = len + 128;
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t)) {
			if (strNil(v)) {
				strcpy(buf, str_nil);
				bn->tnonil = false;
			} else
				strcpy(buf, v);
		} else {
			if (strNil(v))
			   strcpy(buf, t);
			else if (*t != *v) {
				err = "arguments not compatible";
				goto bunins_failed;
			} else if (*t == 'A')
				snprintf(buf, size, "A%s %s", t + 1, v + 1);
			else if (*t == 'C')
				snprintf(buf, size, "C%s%s", t + 1, v + 1);
			else {
				err = "can only concatenate attributes and element content";
				goto bunins_failed;
			}
		}
		if (bunfastappVAR(bn, buf) != GDK_SUCCEED)
			goto bunins_failed;
		rp++;
		p++;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(r->batCacheid);
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.concat", "%s", err);
}

static str
BATXMLgroup(xml *ret, const bat *bid)
{
	BAT *b;
	BUN p, q;
	const char *t;
	size_t len, size = BUFSIZ, offset;
	str buf = GDKmalloc(size);
	BATiter bi;
	const char *err = NULL;

	if (buf == NULL)
		throw(MAL, "xml.aggr", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.aggr", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	strcpy(buf, str_nil);
	offset = 0;
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		int n;

		t = (const char *) BUNtvar(bi, p);

		if (strNil(t))
			continue;
		len = strlen(t) + 1;
		if (len >= size - offset) {
			char *tmp;
			size += len + 128;
			tmp = GDKrealloc(buf, size);
			if (tmp == NULL) {
				err= MAL_MALLOC_FAIL;
				goto failed;
			}
			buf = tmp;
		}
		if (offset == 0)
			n = snprintf(buf, size, "%s", t);
		else if (buf[0] != *t) {
			err = "incompatible values in group";
			goto failed;
		} else if (buf[0] == 'A')
			n = snprintf(buf + offset, size - offset, " %s", t + 1);
		else if (buf[0] == 'C')
			n = snprintf(buf + offset, size - offset, "%s", t + 1);
		else {
			err = "can only group attributes and element content";
			goto failed;
		}
		offset += n;
	}
	BBPunfix(b->batCacheid);
	*ret = buf;
	return MAL_SUCCEED;
  failed:
	BBPunfix(b->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.aggr", "%s", err);
}

static const char *
BATxmlaggr(BAT **bnp, BAT *b, BAT *g, BAT *e, BAT *s, int skip_nils)
{
	BAT *bn = NULL, *t1, *t2 = NULL;
	BATiter bi;
	oid min, max;
	BUN ngrp;
	BUN nils = 0;
	BUN ncand;
	struct canditer ci;
	int isnil;
	const char *v;
	const oid *grps, *map;
	oid mapoff = 0;
	oid prev;
	BUN p, q;
	int freeb = 0, freeg = 0;
	char *buf = NULL;
	size_t buflen, maxlen, len;
	const char *err;
	char *tmp;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci, &ncand)) != NULL) {
		return err;
	}
	assert(b->ttype == TYPE_xml);
	if (BATcount(b) == 0 || ngrp == 0) {
		bn = BATconstant(ngrp == 0 ? 0 : min, TYPE_xml, ATOMnilptr(TYPE_xml), ngrp, TRANSIENT);
		if (bn == NULL)
			return MAL_MALLOC_FAIL;
		*bnp = bn;
		return NULL;
	}
	if (s) {
		b = BATproject(s, b);
		if (b == NULL) {
			err = "internal project failed";
			goto out;
		}
		freeb = 1;
		if (g) {
			g = BATproject(s, g);
			if (g == NULL) {
				err = "internal project failed";
				goto out;
			}
			freeg = 1;
		}
	}
	if (g && BATtdense(g)) {
		/* singleton groups: return group ID's (g's tail) and original
		 * values from b */
		bn = VIEWcreate(g->tseqbase, b);
		goto out;
	}

	maxlen = BUFSIZ;
	if ((buf = GDKmalloc(maxlen)) == NULL) {
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
		goto out;
	}
	buflen = 0;
	bn = COLnew(min, TYPE_xml, ngrp, TRANSIENT);
	if (bn == NULL) {
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, false, false, true) != GDK_SUCCEED) {
			BBPreclaim(bn);
			bn = NULL;
			err = "internal sort failed";
			goto out;
		}
		if (freeg)
			BBPunfix(g->batCacheid);
		g = t1;
		freeg = 1;
		if (t2->ttype == TYPE_void) {
			map = NULL;
			mapoff = b->tseqbase;
		} else {
			map = (const oid *) Tloc(t2, 0);
		}
		grps = (const oid *) Tloc(g, 0);
		prev = grps[0];
		isnil = 0;
		for (p = 0, q = BATcount(g); p <= q; p++) {
			if (p == q || grps[p] != prev) {
				while (BATcount(bn) < prev - min) {
					if (bunfastapp_nocheckVAR(bn, BUNlast(bn), str_nil, Tsize(bn)) != GDK_SUCCEED)
						goto bunins_failed;
					nils++;
				}
				if (bunfastapp_nocheckVAR(bn, BUNlast(bn), buf, Tsize(bn)) != GDK_SUCCEED)
					goto bunins_failed;
				nils += strNil(buf);
				strncpy(buf, str_nil, maxlen);
				buflen = 0;
				if (p == q)
					break;
				prev = grps[p];
				isnil = 0;
			}
			if (isnil)
				continue;
			v = (const char *) BUNtvar(bi, (map ? (BUN) map[p] : p + mapoff));
			if (strNil(v)) {
				if (skip_nils)
					continue;
				strncpy(buf, str_nil, buflen);
				isnil = 1;
			} else {
				len = strlen(v);
				if (len >= maxlen - buflen) {
					maxlen += len + BUFSIZ;
					tmp = GDKrealloc(buf, maxlen);
					if (tmp == NULL) {
						err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
						goto bunins_failed;
					}
					buf = tmp;
				}
				if (buflen == 0) {
					strncpy(buf, v, maxlen);
					buflen += len;
				} else if (buf[0] != v[0]) {
					err = "incompatible values in group";
					goto bunins_failed;
				} else if (buf[0] == 'A') {
					snprintf(buf + buflen, maxlen - buflen, " %s", v + 1);
					buflen += len;
				} else if (buf[0] == 'C') {
					snprintf(buf + buflen, maxlen - buflen, "%s", v + 1);
					buflen += len - 1;
				} else {
					err = "can only group attributes and element content";
					goto bunins_failed;
				}
			}
		}
		BBPunfix(t2->batCacheid);
		t2 = NULL;
	} else {
		for (p = 0, q = p + BATcount(b); p < q; p++) {
			v = (const char *) BUNtvar(bi, p);
			if (strNil(v)) {
				if (skip_nils)
					continue;
				strncpy(buf, str_nil, buflen);
				nils++;
				break;
			}
			len = strlen(v);
			if (len >= maxlen - buflen) {
				maxlen += len + BUFSIZ;
				tmp = GDKrealloc(buf, maxlen);
				if (tmp == NULL) {
					err = SQLSTATE(HY013) MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
				buf = tmp;
			}
			if (buflen == 0) {
				strncpy(buf, v, maxlen);
				buflen += len;
			} else if (buf[0] != v[0]) {
				err = "incompatible values in group";
				goto bunins_failed;
			} else if (buf[0] == 'A') {
				snprintf(buf + buflen, maxlen - buflen, " %s", v + 1);
				buflen += len;
			} else if (buf[0] == 'C') {
				snprintf(buf + buflen, maxlen - buflen, "%s", v + 1);
				buflen += len - 1;
			} else {
				err = "can only group attributes and element content";
				goto bunins_failed;
			}
		}
		if (bunfastapp_nocheckVAR(bn, BUNlast(bn), buf, Tsize(bn)) != GDK_SUCCEED)
			goto bunins_failed;
	}
	bn->theap->dirty = true;
	bn->tnil = nils != 0;
	bn->tnonil = nils == 0;
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tkey = BATcount(bn) <= 1;

  out:
	if (t2)
		BBPunfix(t2->batCacheid);
	if (freeb && b)
		BBPunfix(b->batCacheid);
	if (freeg && g)
		BBPunfix(g->batCacheid);
	if (buf)
		GDKfree(buf);
	*bnp = bn;
	return err;

  bunins_failed:
	BBPreclaim(bn);
	bn = NULL;
	if (err == NULL)
		err = SQLSTATE(HY013) MAL_MALLOC_FAIL;	/* insertion into result BAT failed */
	goto out;
}

static str
AGGRsubxmlcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils)
{
	BAT *b, *g, *e, *s, *bn = NULL;
	const char *err;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		throw(MAL, "aggr.subxml", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	if (sid && !is_bat_nil(*sid)) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPunfix(b->batCacheid);
			if (g)
				BBPunfix(g->batCacheid);
			if (e)
				BBPunfix(e->batCacheid);
			throw(MAL, "aggr.subxml", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
	} else {
		s = NULL;
	}
	err = BATxmlaggr(&bn, b, g, e, s, *skip_nils);
	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);
	if (err != NULL)
		throw(MAL, "aggr.subxml", "%s", err);

	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

static str
AGGRsubxml(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubxmlcand(retval, bid, gid, eid, NULL, skip_nils);
}

static str
BATXMLxquery(bat *ret, const bat *bid, const char * const *expr)
{
	(void) ret;
	(void) bid;
	(void) expr;
	/* use external library to solve this */
	throw(MAL, "xml.xquery", SQLSTATE(0A000) PROGRAM_NYI);
}

#else

#define NO_LIBXML_FATAL "batxml: MonetDB was built without libxml, but what you are trying to do requires it."

static str BATXMLxml2str(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.xml2str", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLxmltext(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.xmltext", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLstr2xml(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.str2xml", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLdocument(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.document", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLcontent(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.content", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLisdocument(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.isdocument", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLelementSmall(bat *ret, const char * const *name, const bat *bid) {
	(void) ret;
	(void) name;
	(void) bid;
	return createException(MAL, "batxml.elementSmall", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLoptions(bat *ret, const char * const *name, const char * const *options, const bat *bid) {
	(void) ret;
	(void) name;
	(void) options;
	(void) bid;
	return createException(MAL, "batxml.options", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLcomment(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.comment", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLparse(bat *ret, const char * const *doccont, const bat *bid, const char * const *option) {
	(void) ret;
	(void) doccont;
	(void) bid;
	(void) option;
	return createException(MAL, "batxml.parse", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLxquery(bat *ret, const bat *bid, const char * const *expr) {
	(void) ret;
	(void) bid;
	(void) expr;
	return createException(MAL, "batxml.xquery", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLpi(bat *ret, const char * const *tgt, const bat *bid) {
	(void) ret;
	(void) tgt;
	(void) bid;
	return createException(MAL, "batxml.pi", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLroot(bat *ret, const bat *bid, const char * const *version, const char * const *standalone) {
	(void) ret;
	(void) bid;
	(void) version;
	(void) standalone;
	return createException(MAL, "batxml.root", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLattribute(bat *ret, const char * const *name, const bat *bid) {
	(void) ret;
	(void) name;
	(void) bid;
	return createException(MAL, "batxml.attribute", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLelement(bat *ret, const char * const *name, xml *ns, xml *attr, const bat *bid) {
	(void) ret;
	(void) name;
	(void) ns;
	(void) attr;
	(void) bid;
	return createException(MAL, "batxml.element", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLconcat(bat *ret, const bat *bid, const bat *rid) {
	(void) ret;
	(void) bid;
	(void) rid;
	return createException(MAL, "batxml.concat", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p) {
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	return createException(MAL, "batxml.forest", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str BATXMLgroup(xml *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return createException(MAL, "batxml.group", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str AGGRsubxmlcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils) {
	(void) retval;
	(void) bid;
	(void) gid;
	(void) eid;
	(void) sid;
	(void) skip_nils;
	return createException(MAL, "batxml.subxmlcand", SQLSTATE(HY005) NO_LIBXML_FATAL);
}
static str AGGRsubxml(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils) {
	(void) retval;
	(void) bid;
	(void) gid;
	(void) eid;
	(void) skip_nils;
	return createException(MAL, "batxml.subxml", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

#endif /* HAVE_LIBXML */

#include "mel.h"
mel_func batxml_init_funcs[] = {
 command("batxml", "xml", BATXMLstr2xml, false, "Cast the string to an xml compliant string.", args(1,2, batarg("",xml),batarg("src",str))),
 command("batxml", "str", BATXMLxml2str, false, "Cast the xml to a string.", args(1,2, batarg("",str),batarg("src",xml))),
 command("batxml", "document", BATXMLdocument, false, "Parse the string as an XML document.", args(1,2, batarg("",xml),batarg("src",str))),
 command("batxml", "content", BATXMLcontent, false, "Parse the string as XML element content.", args(1,2, batarg("",xml),batarg("src",str))),
 command("batxml", "comment", BATXMLcomment, false, "Create an XML comment element.", args(1,2, batarg("",xml),batarg("val",str))),
 command("batxml", "parse", BATXMLparse, false, "Parse the XML document or element string values.", args(1,4, batarg("",xml),arg("doccont",str),batarg("val",str),arg("option",str))),
 command("batxml", "serialize", BATXMLxml2str, false, "Serialize the XML object to a string.", args(1,2, batarg("",str),batarg("val",xml))),
 command("batxml", "text", BATXMLxmltext, false, "Serialize the XML object to a string.", args(1,2, batarg("",str),batarg("val",xml))),
 command("batxml", "xquery", BATXMLxquery, false, "Execute the XQuery against the elements.", args(1,3, batarg("",xml),batarg("val",str),arg("expr",str))),
 command("batxml", "pi", BATXMLpi, false, "Construct a processing instruction.", args(1,3, batarg("",xml),arg("target",str),batarg("val",xml))),
 command("batxml", "attribute", BATXMLattribute, false, "Construct an attribute value pair.", args(1,3, batarg("",xml),arg("name",str),batarg("val",str))),
 command("batxml", "element", BATXMLelementSmall, false, "The basic building block for XML elements are namespaces, attributes and a sequence of XML elements. The name space and the attributes may be left unspecified.", args(1,3, batarg("",xml),arg("name",str),batarg("s",xml))),
 command("batxml", "options", BATXMLoptions, false, "Create the components including NULL conversions.", args(1,4, batarg("",xml),arg("tag",str),arg("option",str),batarg("left",xml))),
 command("batxml", "element", BATXMLelement, false, "The basic building block for XML elements are namespaces, attributes and a sequence of XML elements. The name space and the attributes may be left unspecified(=nil).", args(1,5, batarg("",xml),arg("name",str),arg("ns",xml),arg("attr",xml),batarg("s",xml))),
 command("batxml", "concat", BATXMLconcat, false, "Concatenate the XML values.", args(1,3, batarg("",xml),batarg("left",xml),batarg("right",xml))),
 pattern("batxml", "forest", BATXMLforest, false, "Construct an element list.", args(1,2, batarg("",xml),batvararg("val",xml))),
 command("batxml", "root", BATXMLroot, false, "Contruct the root nodes.", args(1,4, batarg("",xml),batarg("val",xml),arg("version",str),arg("standalone",str))),
 command("batxml", "isdocument", BATXMLisdocument, false, "Validate the string as a XML document.", args(1,2, batarg("",bit),batarg("val",str))),
 command("xml", "aggr", BATXMLgroup, false, "Aggregate the XML values.", args(1,2, arg("",xml),batarg("val",xml))),
 command("xml", "subaggr", AGGRsubxml, false, "Grouped aggregation of XML values.", args(1,5, batarg("",xml),batarg("val",xml),batarg("g",oid),batargany("e",1),arg("skip_nils",bit))),
 command("xml", "subaggr", AGGRsubxmlcand, false, "Grouped aggregation of XML values with candidates list.", args(1,6, batarg("",xml),batarg("val",xml),batarg("g",oid),batargany("e",1),batarg("s",oid),arg("skip_nils",bit))),
 command("batcalc", "xml", BATXMLstr2xml, false, "", args(1,2, batarg("",xml),batarg("src",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_batxml_mal)
{ mal_module("batxml", NULL, batxml_init_funcs); }
