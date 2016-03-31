/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 *  M.L. Kersten
 *  XML multiplexes
 * SQL/XML requires a handful of instructions.
 * The collection of routines provided here are map operations
 * for the atom xml primitives.
 *
 * In line with the batcalc module, we assume that
 * if two bat operands are provided that they are already
 * aligned on the head. Moreover, the head of the BATs
 * are limited to :oid.
 *
 * The implementation is focussed on functionality. At a later stage
 * we may postpone string contstruction until it is really needed.
 */



#include "monetdb_config.h"
#include <gdk.h>
#include "ctype.h"
#include <string.h>
#ifdef HAVE_LIBXML
#include <libxml/parser.h>
#endif
#include "mal_interpreter.h"
#include "mal_function.h"
#include "xml.h"

#ifdef WIN32
#define batxml_export extern __declspec(dllexport)
#else
#define batxml_export extern
#endif

batxml_export str BATXMLxml2str(bat *ret, const bat *bid);
batxml_export str BATXMLxmltext(bat *ret, const bat *bid);
batxml_export str BATXMLstr2xml(bat *ret, const bat *bid);
batxml_export str BATXMLdocument(bat *ret, const bat *bid);
batxml_export str BATXMLcontent(bat *ret, const bat *bid);
batxml_export str BATXMLisdocument(bat *ret, const bat *bid);
batxml_export str BATXMLelementSmall(bat *ret, const char * const *name, const bat *bid);
batxml_export str BATXMLoptions(bat *ret, const char * const *name, const char * const *options, const bat *bid);
batxml_export str BATXMLcomment(bat *ret, const bat *bid);
batxml_export str BATXMLparse(bat *ret, const char * const *doccont, const bat *bid, const char * const *option);
batxml_export str BATXMLxquery(bat *ret, const bat *bid, const char * const *expr);
batxml_export str BATXMLpi(bat *ret, const char * const *tgt, const bat *bid);
batxml_export str BATXMLroot(bat *ret, const bat *bid, const char * const *version, const char * const *standalone);
batxml_export str BATXMLattribute(bat *ret, const char * const *name, const bat *bid);
batxml_export str BATXMLelement(bat *ret, const char * const *name, xml *ns, xml *attr, const bat *bid);
batxml_export str BATXMLconcat(bat *ret, const bat *bid, const bat *rid);
batxml_export str BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
batxml_export str BATXMLgroup(xml *ret, const bat *bid);
batxml_export str AGGRsubxmlcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils);
batxml_export str AGGRsubxml(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils);

#ifdef HAVE_LIBXML

#define prepareResult(X,Y,tpe,Z,free)							\
	do {														\
		(X) = BATnew(TYPE_void, (tpe), BATcount(Y), TRANSIENT);	\
		if ((X) == NULL) {										\
			BBPunfix((Y)->batCacheid);							\
			free;												\
			throw(MAL, "xml." Z, MAL_MALLOC_FAIL);				\
		}														\
		BATseqbase((X), (Y)->hseqbase);							\
		(X)->hsorted = 1;										\
		(X)->hrevsorted = (Y)->hrevsorted;						\
		(X)->tsorted =  0;										\
		(X)->trevsorted =  0;									\
		(X)->H->nonil = (Y)->H->nonil;							\
		(X)->T->nonil = 1;										\
	} while (0)

#define finalizeResult(X,Y,Z)					\
	do {										\
		BATsetcount((Y), (Y)->batCount);		\
		if (!((Y)->batDirty & 2))				\
			BATsetaccess((Y), BAT_READ);		\
		*(X) = (Y)->batCacheid;					\
		BBPkeepref(*(X));						\
		BBPunfix((Z)->batCacheid);				\
	} while (0)

str
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
		const char *t = (const char *) BUNtail(bi, p);

		if (strNil(t)) {
			bunfastapp(bn, t);
			bn->T->nonil = 0;
		} else {
			assert(*t == 'A' || *t == 'C' || *t == 'D');
			bunfastapp(bn, t + 1);
		}
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.str", OPERATION_FAILED " during bulk coercion");
}

str
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
		const char *t = (const char *) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastapp(bn, t);
			bn->T->nonil = 0;
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
				err = MAL_MALLOC_FAIL;
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
				err = MAL_MALLOC_FAIL;
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
					err = MAL_MALLOC_FAIL;
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
			bunfastapp(bn, str_nil);
			bn->T->nonil = 0;
			continue;
		}
		assert(content != NULL || buf != NULL);
		bunfastapp(bn, content != NULL ? content : buf);
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
str
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
		throw(MAL,"xml.str2xml",MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.xml", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "xml", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastapp(bn, str_nil);
			bn->T->nonil = 0;
			continue;
		}

		len = strlen(t) * 6 + 1;
		if (size < len) {
			size = len + 128;
			GDKfree(buf);
			buf = GDKmalloc(size);
			if (buf == NULL) {
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'C';
		XMLquotestring(t, buf + 1, size - 1);
		bunfastapp(bn, buf);
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

str
BATXMLdocument(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	const char *err = OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL,"xml.document",MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.document", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "document", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
		xmlDocPtr doc;
		int len;
		xmlChar *s;

		if (strNil(t)) {
			bunfastapp(bn, str_nil);
			bn->T->nonil = 0;
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
		bunfastapp(bn, buf);
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

str
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
		throw(MAL,"xml.content",MAL_MALLOC_FAIL);
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
		const char *t = (const char *) BUNtail(bi, p);
		size_t len;
		xmlNodePtr elem;
		xmlParserErrors xerr;
		const xmlChar *s;

		if (strNil(t)) {
			bunfastapp(bn, str_nil);
			bn->T->nonil = 0;
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
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'C';
		strcpy(buf + 1, (const char *) s);
		bunfastapp(bn, buf);
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

str
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
		const char *t = (const char *) BUNtail(bi, p);
		xmlDocPtr doc;
		bit val;

		if (strNil(t)) {
			val = bit_nil;
			bn->T->nonil = 0;
		} else {
			doc = xmlParseMemory(t, (int) strlen(t));
			if (doc == NULL) {
				val = 0;
			} else {
				xmlFreeDoc(doc);
				val = 1;
			}
		}
		bunfastapp(bn, &val);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.isdocument", OPERATION_FAILED " During bulk processing");
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
str
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
		throw(MAL, "batxml.options", MAL_MALLOC_FAIL);
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
		err = PROGRAM_NYI;
		goto bunins_failed;
	}

	snprintf(val, size, "<%s>", *name);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);

		if (strNil(t)) {
			bunfastapp(bn, buf);
		} else {
			if (strlen(t) > size - 2 * len - 6) {
				size += strlen(t);
				val = (char *) GDKrealloc(val, size + strlen(t));
				if (val == NULL) {
					err = MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
			}
			snprintf(val + len + 2, size - len, "%s</%s>", t, *name);
			bunfastapp(bn, val);
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

str
BATXMLcomment(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	const char *err= OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL, "xml.comment", MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.comment", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "comment", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastapp(bn, str_nil);
			bn->T->nonil = 0;
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
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		snprintf(buf, size, "C<!--%s-->", t);
		bunfastapp(bn, buf);
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

str
BATXMLparse(bat *ret, const char * const *doccont, const bat *bid, const char * const *option)
{
	(void) option;
	if (strcmp(*doccont, "content") == 0)
		return BATXMLcontent(ret, bid);
	if (strcmp(*doccont, "document") == 0)
		return BATXMLdocument(ret, bid);
	throw(MAL, "xml.parse", ILLEGAL_ARGUMENT " <document> or <content> expected");
}

str
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
		throw(MAL, "xml.pi", MAL_MALLOC_FAIL);

	tgtlen = strlen(*target) + 6;
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.pi", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "pi", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
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
				err = MAL_MALLOC_FAIL;
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
		bunfastapp(bn, buf);
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

str
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
		throw(MAL, "xml.root", MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.pi", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "pi", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
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
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t)) {
			strcpy(buf, str_nil);
			bn->T->nonil = 0;
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
		bunfastapp(bn, buf);
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

str
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
		throw(MAL, "xml.attribute", MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.attribute", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "attribute", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
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
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t)) {
			strcpy(buf, str_nil);
			bn->T->nonil = 0;
		} else {
			int n = snprintf(buf, size, "A%s = \"", *name);
			size_t m = XMLquotestring(t, buf + n, size - n);
			strcpy(buf + n + m, "\"");
		}
		bunfastapp(bn, buf);
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

str
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
		throw(MAL, "xml.attribute", MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.element", INTERNAL_BAT_ACCESS);
	}
	prepareResult(bn, b, TYPE_xml, "element", GDKfree(buf));
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		const char *t = (const char *) BUNtail(bi, p);
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
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (strNil(t) && (!attr || strNil(*attr))) {
			strcpy(buf, str_nil);
			bn->T->nonil = 0;
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
		bunfastapp(bn, buf);
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

str
BATXMLelementSmall(bat *ret, const char * const *name, const bat *bid)
{
	return BATXMLelement(ret, name, NULL, NULL, bid);
}

str
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
		throw(MAL, "xml.forest", MAL_MALLOC_FAIL);
	}

	/* collect the admin for the xml elements */
	for (i = pci->retc; i < pci->argc; i++) {
		if ((bi[i].b = BATdescriptor(*getArgReference_bat(stk, pci, i))) == NULL)
			break;
		p[i] = BUNfirst(bi[i].b);
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

			t = (const char *) BUNtail(bi[i], p[i]);
			if (strNil(t))
				continue;

			if ((len = strlen(t)) >= size - offset) {
				size += len + 128;
				buf = GDKrealloc(buf, size);
				if (buf == NULL) {
					err = MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
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
		bunfastapp(bn, buf);
		if (offset == 0)
			bn->T->nonil = 0;

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

str
BATXMLconcat(bat *ret, const bat *bid, const bat *rid)
{
	BAT *b, *r = 0, *bn;
	BUN p, q, rp = 0;
	size_t len, size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi, ri;
	const char *err = OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL, "xml.concat", MAL_MALLOC_FAIL);
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
	p = BUNfirst(b);
	q = BUNlast(b);
	rp = BUNfirst(r);

	prepareResult(bn, b, TYPE_xml, "concat",
				  GDKfree(buf); BBPunfix(r->batCacheid));

	bi = bat_iterator(b);
	ri = bat_iterator(r);
	while (p < q) {
		const char *t = (const char *) BUNtail(bi, p);
		const char *v = (const char *) BUNtail(ri, rp);

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
				bn->T->nonil = 0;
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
		bunfastapp(bn, buf);
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

str
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
		throw(MAL, "xml.aggr",MAL_MALLOC_FAIL);
	if ((b = BATdescriptor(*bid)) == NULL) {
		GDKfree(buf);
		throw(MAL, "xml.aggr", RUNTIME_OBJECT_MISSING);
	}

	strcpy(buf, str_nil);
	offset = 0;
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		int n;

		t = (const char *) BUNtail(bi, p);

		if (strNil(t))
			continue;
		len = strlen(t) + 1;
		if (len >= size - offset) {
			size += len + 128;
			buf = GDKrealloc(buf, size);
			if (buf == NULL) {
				err= MAL_MALLOC_FAIL;
				goto failed;
			}
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
	BUN ngrp, start, end, cnt;
	BUN nils = 0;
	int isnil;
	const oid *cand = NULL, *candend = NULL;
	const char *v;
	const oid *grps, *map;
	oid mapoff = 0;
	oid prev;
	BUN p, q;
	int freeb = 0, freeg = 0;
	char *buf = NULL;
	size_t buflen, maxlen, len;
	const char *err;

	if ((err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &start, &end,
								&cnt, &cand, &candend)) != NULL) {
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
		bn = VIEWcreate(b->hseqbase, b);
		if (bn)
			BATseqbase(bn, g->tseqbase);
		goto out;
	}

	maxlen = BUFSIZ;
	if ((buf = GDKmalloc(maxlen)) == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	buflen = 0;
	bn = BATnew(TYPE_void, TYPE_xml, ngrp, TRANSIENT);
	if (bn == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsort(&t1, &t2, NULL, g, NULL, NULL, 0, 1) != GDK_SUCCEED) {
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
			map = (const oid *) Tloc(t2, BUNfirst(t2));
		}
		grps = (const oid *) Tloc(g, BUNfirst(g));
		prev = grps[0];
		isnil = 0;
		for (p = 0, q = BATcount(g); p <= q; p++) {
			if (p == q || grps[p] != prev) {
				while (BATcount(bn) < prev - min) {
					bunfastapp_nocheck(bn, BUNlast(bn), str_nil, Tsize(bn));
					nils++;
				}
				bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
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
			v = (const char *) BUNtail(bi, BUNfirst(b) + (map ? (BUN) map[p] : p + mapoff));
			if (strNil(v)) {
				if (skip_nils)
					continue;
				strncpy(buf, str_nil, buflen);
				isnil = 1;
			} else {
				len = strlen(v);
				if (len >= maxlen - buflen) {
					maxlen += len + BUFSIZ;
					buf = GDKrealloc(buf, maxlen);
					if (buf == NULL) {
						err = MAL_MALLOC_FAIL;
						goto bunins_failed;
					}
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
		for (p = BUNfirst(b), q = p + BATcount(b); p < q; p++) {
			v = (const char *) BUNtail(bi, p);
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
				buf = GDKrealloc(buf, maxlen);
				if (buf == NULL) {
					err = MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
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
		bunfastapp_nocheck(bn, BUNlast(bn), buf, Tsize(bn));
	}
	BATseqbase(bn, min);
	bn->T->nil = nils != 0;
	bn->T->nonil = nils == 0;
	bn->T->sorted = BATcount(bn) <= 1;
	bn->T->revsorted = BATcount(bn) <= 1;
	bn->T->key = BATcount(bn) <= 1;

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
		err = MAL_MALLOC_FAIL;	/* insertion into result BAT failed */
	goto out;
}

str
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
		throw(MAL, "aggr.subxml", RUNTIME_OBJECT_MISSING);
	}
	if (sid) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPunfix(b->batCacheid);
			if (g)
				BBPunfix(g->batCacheid);
			if (e)
				BBPunfix(e->batCacheid);
			throw(MAL, "aggr.subxml", RUNTIME_OBJECT_MISSING);
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

str
AGGRsubxml(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils)
{
	return AGGRsubxmlcand(retval, bid, gid, eid, NULL, skip_nils);
}

str
BATXMLxquery(bat *ret, const bat *bid, const char * const *expr)
{
	(void) ret;
	(void) bid;
	(void) expr;
	/* use external library to solve this */
	throw(MAL, "xml.xquery", PROGRAM_NYI);
}

#else

#define NO_LIBXML_FATAL "batxml: MonetDB was built without libxml, but what you are trying to do requires it."

str BATXMLxml2str(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLxmltext(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLstr2xml(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLdocument(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLcontent(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLisdocument(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLelementSmall(bat *ret, const char * const *name, const bat *bid) {
	(void) ret;
	(void) name;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLoptions(bat *ret, const char * const *name, const char * const *options, const bat *bid) {
	(void) ret;
	(void) name;
	(void) options;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLcomment(bat *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLparse(bat *ret, const char * const *doccont, const bat *bid, const char * const *option) {
	(void) ret;
	(void) doccont;
	(void) bid;
	(void) option;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLxquery(bat *ret, const bat *bid, const char * const *expr) {
	(void) ret;
	(void) bid;
	(void) expr;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLpi(bat *ret, const char * const *tgt, const bat *bid) {
	(void) ret;
	(void) tgt;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLroot(bat *ret, const bat *bid, const char * const *version, const char * const *standalone) {
	(void) ret;
	(void) bid;
	(void) version;
	(void) standalone;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLattribute(bat *ret, const char * const *name, const bat *bid) {
	(void) ret;
	(void) name;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLelement(bat *ret, const char * const *name, xml *ns, xml *attr, const bat *bid) {
	(void) ret;
	(void) name;
	(void) ns;
	(void) attr;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLconcat(bat *ret, const bat *bid, const bat *rid) {
	(void) ret;
	(void) bid;
	(void) rid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p) {
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str BATXMLgroup(xml *ret, const bat *bid) {
	(void) ret;
	(void) bid;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str AGGRsubxmlcand(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bat *sid, const bit *skip_nils) {
	(void) retval;
	(void) bid;
	(void) gid;
	(void) eid;
	(void) sid;
	(void) skip_nils;
	return GDKstrdup(NO_LIBXML_FATAL);
}
str AGGRsubxml(bat *retval, const bat *bid, const bat *gid, const bat *eid, const bit *skip_nils) {
	(void) retval;
	(void) bid;
	(void) gid;
	(void) eid;
	(void) skip_nils;
	return GDKstrdup(NO_LIBXML_FATAL);
}

#endif
