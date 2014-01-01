/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is the MonetDB Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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


#define getBATdescriptor(X1,X2,X3,X4) \
	if ((X2 = BATdescriptor(*X1)) == NULL) {\
		X4;\
		throw(MAL, X3, RUNTIME_OBJECT_MISSING);\
	}

#include "monetdb_config.h"
#include <gdk.h>
#include "ctype.h"
#include <string.h>
#include <libxml/parser.h>
#include "mal_interpreter.h"
#include "mal_function.h"
#include "xml.h"

#ifdef WIN32
#ifndef LIBATOMS
#define batxml_export extern __declspec(dllimport)
#else
#define batxml_export extern __declspec(dllexport)
#endif
#else
#define batxml_export extern
#endif

batxml_export str BATXMLxml2str(int *ret, int *bid);
batxml_export str BATXMLxmltext(int *ret, int *bid);
batxml_export str BATXMLstr2xml(int *x, int *s);
batxml_export str BATXMLdocument(int *x, int *s);
batxml_export str BATXMLcontent(int *x, int *s);
batxml_export str BATXMLisdocument(int *x, int *s);
batxml_export str BATXMLelementSmall(int *x, str *name, int *s);
batxml_export str BATXMLoptions(int *x, str *name, str *options, int *s);
batxml_export str BATXMLcomment(int *x, int *s);
batxml_export str BATXMLparse(int *x, str *doccont, int *s, str *option);
batxml_export str BATXMLxquery(int *x, int *s, str *expr);
batxml_export str BATXMLpi(int *x, str *tgt, int *s);
batxml_export str BATXMLroot(int *ret, int *bid, str *version, str *standalone);
batxml_export str BATXMLattribute(int *ret, str *name, int *bid);
batxml_export str BATXMLelement(int *ret, str *name, xml *ns, xml *attr, int *bid);
batxml_export str BATXMLconcat(int *ret, int *bid, int *rid);
batxml_export str BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p);
batxml_export str BATXMLagg(int *ret, int *bid, int *grp);
batxml_export str BATXMLagg3(int *ret, int *bid, int *grp, int *e);
batxml_export str BATXMLgroup(xml *ret, int *bid);
batxml_export str AGGRsubxmlcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils);
batxml_export str AGGRsubxml(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils);


#define prepareOperand(X,Y,Z)								\
    if (((X) = BATdescriptor(*(Y))) == NULL )				\
        throw(MAL, "xml." Z, INTERNAL_BAT_ACCESS);

#define prepareResult(X,Y,tpe,Z)					\
    (X) = BATnew((Y)->htype, (tpe), BATcount(Y));	\
    if ((X) == NULL) {								\
        BBPreleaseref((Y)->batCacheid);				\
        throw(MAL, "xml." Z, MAL_MALLOC_FAIL); \
    }												\
    if ((Y)->htype == TYPE_void)					\
        BATseqbase((X), (Y)->hseqbase);				\
    (X)->hsorted = (Y)->hsorted;					\
    (X)->hrevsorted = (Y)->hrevsorted;				\
    (X)->tsorted =  0;								\
    (X)->trevsorted =  0;							\
    (X)->H->nonil = (Y)->H->nonil;					\
	(X)->T->nonil = 1;

#define finalizeResult(X,Y,Z)					\
    if (!((Y)->batDirty & 2))					\
		(Y) = BATsetaccess((Y), BAT_READ);		\
    *(X) = (Y)->batCacheid;						\
    BBPkeepref(*(X));							\
    BBPreleaseref((Z)->batCacheid);

str
BATXMLxml2str(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;

	prepareOperand(b, bid, "str");
	prepareResult(bn, b, TYPE_str, "str");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);

		if (strNil(t)) {
			bunfastins(bn, h, t);
			bn->T->nonil = 0;
		} else {
			assert(*t == 'A' || *t == 'C' || *t == 'D');
			bunfastins(bn, h, t + 1);
		}
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.str", OPERATION_FAILED " During bulk coercion");
}

str
BATXMLxmltext(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	size_t size = 0;
	str buf = NULL;
	xmlDocPtr doc = NULL;
	xmlNodePtr elem;
	str content = NULL, err = OPERATION_FAILED;

	prepareOperand(b, bid, "text");
	prepareResult(bn, b, TYPE_str, "text");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastins(bn, h, t);
			bn->T->nonil = 0;
			continue;
		}
		assert(*t == 'A' || *t == 'C' || *t == 'D');
		len = strlen(t);
		if (*t == 'D') {
			xmlDocPtr d = xmlParseMemory(t + 1, (int) (len - 1));
			elem = xmlDocGetRootElement(d);
			content = (str) xmlNodeGetContent(elem);
			xmlFreeDoc(d);
			if (content == NULL) {
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		} else if (*t == 'C') {
			if (doc == NULL)
				doc = xmlParseMemory("<doc/>", 6);
			xmlParseInNodeContext(xmlDocGetRootElement(doc), t + 1, (int) (len - 1), 0, &elem);
			content = (str) xmlNodeGetContent(elem);
			xmlFreeNodeList(elem);
			if (content == NULL) {
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		} else {
			str s;

			if (buf == 0 || size < len) {
				size = len + 128;
				if (buf != NULL)
					GDKfree(buf);
				buf = GDKmalloc(size);
				if ( buf == 0) {
					err= MAL_MALLOC_FAIL;
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
		}
		assert(content != NULL || buf != NULL);
		bunfastins(bn, h, content != NULL ? content : buf);
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
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if (buf != NULL)
		GDKfree(buf);
	if (doc != NULL)
		xmlFreeDoc(doc);
	if (content != NULL)
		GDKfree(content);
	throw(MAL, "xml.text", "%s", err );
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
BATXMLstr2xml(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf, err= OPERATION_FAILED;
	BATiter bi;

	buf = GDKmalloc(size);
	if ( buf == NULL)
		throw(MAL,"xml.str2xml",MAL_MALLOC_FAIL);
	prepareOperand(b, bid, "xml");
	prepareResult(bn, b, TYPE_xml, "xml");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastins(bn, h, str_nil);
			bn->T->nonil = 0;
			continue;
		}

		len = strlen(t) * 6 + 1;
		if (size < len) {
			size = len + 128;
			GDKfree(buf);
			buf = GDKmalloc(size);
			if ( buf == NULL){
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'C';
		XMLquotestring(t, buf + 1, size - 1);
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	throw(MAL, "xml.xml", "%s", err);
}

str
BATXMLdocument(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL,"xml.document",MAL_MALLOC_FAIL);
	prepareOperand(b, bid, "document");
	prepareResult(bn, b, TYPE_xml, "document");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		xmlDocPtr doc;
		int len;
		xmlChar *s;

		if (strNil(t)) {
			bunfastins(bn, h, str_nil);
			bn->T->nonil = 0;
			continue;
		}
		len = (int) strlen(t);
		doc = xmlParseMemory(t, len);
		if (doc == NULL) {
			BBPreleaseref(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "xml.document", OPERATION_FAILED XML_PARSE_ERROR);
		}
		xmlDocDumpMemory(doc, &s, &len);
		xmlFreeDoc(doc);
		if ((size_t) len + 2 >= size) {
			GDKfree(buf);
			size = (size_t) len + 128;
			buf = GDKmalloc(size);
			if ( buf == NULL){
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		buf[0] = 'D';
		strcpy(buf + 1, (char *) s);
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	GDKfree(buf);
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.document", "%s", err);
}

str
BATXMLcontent(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;
	xmlDocPtr doc;
	xmlNodePtr root;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	str err = OPERATION_FAILED;
	xmlBufferPtr xbuf;

	if ( buf == NULL)
		throw(MAL,"xml.content",MAL_MALLOC_FAIL);
	doc = xmlParseMemory("<doc/>", 6);
	root = xmlDocGetRootElement(doc);
	prepareOperand(b, bid, "content");
	prepareResult(bn, b, TYPE_xml, "content");
	bi = bat_iterator(b);
	xbuf = xmlBufferCreate();
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		size_t len;
		xmlNodePtr elem;
		xmlParserErrors err;
		const xmlChar *s;

		if (strNil(t)) {
			bunfastins(bn, h, str_nil);
			bn->T->nonil = 0;
			continue;
		}
		len = strlen(t);
		err = xmlParseInNodeContext(root, t, (int) len, 0, &elem);
		if (err != XML_ERR_OK) {
			xmlBufferFree(xbuf);
			xmlFreeDoc(doc);
			BBPreleaseref(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "xml.content", XML_PARSE_ERROR);
		}
		xmlNodeDump(xbuf, doc, elem, 0, 0);
		s = xmlBufferContent(xbuf);
		len = strlen((const char *) s);
		if (len + 2 >= size) {
			GDKfree(buf);
			size = len + 128;
			buf = GDKmalloc(size);
			if ( buf == NULL){
				xmlBufferFree(xbuf);
				xmlFreeDoc(doc);
				BBPreleaseref(b->batCacheid);
				BBPunfix(bn->batCacheid);
				throw(MAL, "xml.document", MAL_MALLOC_FAIL);
			}
		}
		buf[0] = 'C';
		strcpy(buf + 1, (const char *) s);
		bunfastins(bn, h, buf);
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
	if ( buf != NULL )
		GDKfree(buf);
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	throw(MAL, "xml.document", "%s", err);
}

str
BATXMLisdocument(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	BATiter bi;

	prepareOperand(b, bid, "isdocument");
	prepareResult(bn, b, TYPE_bit, "isdocument");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
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
		bunfastins(bn, h, &val);
	}
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
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
BATXMLoptions(int *ret, str *name, str *options, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	str buf = GDKmalloc(BUFSIZ);
	str val = GDKmalloc(BUFSIZ);
	int size = BUFSIZ, len = (int) strlen(*name);
	BATiter bi;
	str err = OPERATION_FAILED " During bulk options analysis";

	if ( val == NULL || buf == NULL){
		if (val != NULL) GDKfree(val);
		if (buf != NULL) GDKfree(buf);
		throw(MAL, "batxml.options", MAL_MALLOC_FAIL);
	}
	prepareOperand(b, bid, "options");
	prepareResult(bn, b, TYPE_xml, "options");

	if (strcmp(*options, "absent") == 0)
		buf[0] = 0;
	else if (strcmp(*options, "empty") == 0)
		snprintf(buf, size, "<%s></%s>", *name, *name);
	else if (strcmp(*options, "null") == 0)
		snprintf(buf, size, "null");
	else if (strcmp(*options, "nil") == 0)
		snprintf(buf, size, "nil");
	else
		/*if( strcmp(*options,"niloncontent")==0) */
		throw(MAL, "batxml.options", PROGRAM_NYI);

	snprintf(val, size, "<%s>", *name);
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);

		if (strNil(t)) {
			bunfastins(bn, h, buf);
		} else {
			if ((int) strlen(t) > size - 2 * len - 6) {
				size += (int) strlen(t);
				val = (char *) GDKrealloc(val, size + strlen(t));
				if (val == NULL) {
					err = MAL_MALLOC_FAIL;
					goto bunins_failed;
				}
			}
			snprintf(val + len + 2, size - len, "%s</%s>", t, *name);
			bunfastins(bn, h, val);
		}
	}
	GDKfree(val);
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	if ( val != NULL)
		GDKfree(val);
	throw(MAL, "batxml.options", "%s", err);
}

str
BATXMLcomment(int *ret, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	str err= OPERATION_FAILED;

	if (buf == NULL)
		throw(MAL, "xml.comment", MAL_MALLOC_FAIL);
	prepareOperand(b, bid, "comment");
	prepareResult(bn, b, TYPE_xml, "comment");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		size_t len;

		if (strNil(t)) {
			bunfastins(bn, h, str_nil);
			bn->T->nonil = 0;
			continue;
		}
		if (strstr(t, "--") != NULL) {
			BBPreleaseref(b->batCacheid);
			BBPunfix(bn->batCacheid);
			throw(MAL, "xml.comment", XML_COMMENT_ERROR);
		}
		len = strlen(t);
		if (len + 9 >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if ( buf == NULL){
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		snprintf(buf, size, "C<!--%s-->", t);
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.comment", "%s", err);
}

str
BATXMLparse(int *ret, str *doccont, int *bid, str *option)
{
	(void) option;
	if (strcmp(*doccont, "content") == 0)
		return BATXMLcontent(ret, bid);
	if (strcmp(*doccont, "document") == 0)
		return BATXMLdocument(ret, bid);
	throw(MAL, "xml.parse", ILLEGAL_ARGUMENT " <document> or <content> expected");
}

str
BATXMLpi(int *ret, str *target, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	size_t tgtlen;
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL, "xml.pi", MAL_MALLOC_FAIL);
	if (strNil(*target))
		throw(MAL, "xml.pi", XML_PI_ERROR);

	tgtlen = strlen(*target) + 6;
	prepareOperand(b, bid, "pi");
	prepareResult(bn, b, TYPE_xml, "pi");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
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
			if ( buf == NULL){
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
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	throw(MAL, "xml.pi", "%s", err);
}

str
BATXMLroot(int *ret, int *bid, str *version, str *standalone)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	size_t hdrlen;
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL, "xml.root", MAL_MALLOC_FAIL);
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
	prepareOperand(b, bid, "pi");
	prepareResult(bn, b, TYPE_xml, "pi");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
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
			if ( buf == NULL){
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
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	throw(MAL, "xml.root", "%s", err);
}

str
BATXMLattribute(int *ret, str *name, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	size_t attrlen;
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL, "xml.attribute", MAL_MALLOC_FAIL);
	if (strNil(*name))
		throw(MAL, "xml.attribute", XML_ATTRIBUTE_ERROR);
	if (xmlValidateName((xmlChar *) *name, 0) != 0)
		throw(MAL, "xml.attribute", XML_ATTRIBUTE_INVALID);
	attrlen = strlen(*name) + 5;
	prepareOperand(b, bid, "attribute");
	prepareResult(bn, b, TYPE_xml, "attribute");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
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
			if ( buf == NULL){
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
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	throw(MAL, "xml.attribute", "%s", err);
}

str
BATXMLelement(int *ret, str *name, xml *nspace, xml *attr, int *bid)
{
	BAT *b, *bn;
	BUN p, q;
	size_t size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi;
	size_t elemlen, namelen;
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL, "xml.attribute", MAL_MALLOC_FAIL);
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
	prepareOperand(b, bid, "element");
	prepareResult(bn, b, TYPE_xml, "element");
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		ptr h = BUNhead(bi, p);
		str t = (str) BUNtail(bi, p);
		size_t len;

		len = elemlen;
		if (!strNil(t)) {
			if (*t != 'C') {
				BBPreleaseref(b->batCacheid);
				BBPunfix(bn->batCacheid);
				GDKfree(buf);
				throw(MAL, "xml.element", XML_ILLEGAL_CONTENT);
			}
			len += strlen(t + 1) + namelen + 2;  /* extra "<", ">", and name ("/" already counted) */
		}
		if (len >= size) {
			/* make sure there is enough space */
			size = len + 128;
			/* free/malloc so we don't copy */
			GDKfree(buf);
			buf = GDKmalloc(size);
			if ( buf == NULL){
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
		bunfastins(bn, h, buf);
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	throw(MAL, "xml.element", "%s", err);
}

str
BATXMLelementSmall(int *ret, str *name, int *bid)
{
	return BATXMLelement(ret, name, NULL, NULL, bid);
}

str
BATXMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *ret = (int *) getArgReference(stk, pci, 0);
	BAT *bn;
	BATiter *bi;
	BUN *p, *q;
	str buf = GDKmalloc(BUFSIZ);
	int i;
	size_t offset, len, size = BUFSIZ;
	str err = OPERATION_FAILED;

	(void) mb;
	(void) cntxt;
	if ( buf == NULL)
		throw(MAL, "xml.forest", MAL_MALLOC_FAIL);
	
	bi = GDKmalloc(sizeof(BATiter) * pci->argc);
	p = GDKmalloc(sizeof(BUN) * pci->argc);
	q = GDKmalloc(sizeof(BUN) * pci->argc);

	/* collect the admin for the xml elements */
	for (i = pci->retc; i < pci->argc; i++) {
		if ((bi[i].b = BATdescriptor(*(int *) getArgReference(stk, pci, i))) == NULL)
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
		throw(MAL, "xml.forest", INTERNAL_BAT_ACCESS);
	}

	prepareResult(bn, bi[pci->retc].b, TYPE_xml, "forest");

	while (p[pci->retc] < q[pci->retc]) {
		str t;
		oid *h;

		/* fetch the elements */
		h = (oid *) BUNhead(bi[pci->retc], p[pci->retc]);
		offset = 0;
		strcpy(buf, str_nil);
		for (i = pci->retc; i < pci->argc; i++) {
			int n;

			t = (str) BUNtail(bi[i], p[i]);
			if (strNil(t))
				continue;

			if ((len = strlen(t)) >= size - offset) {
				size += len + 128;
				buf = GDKrealloc(buf, size);
				if ( buf == NULL) {
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
		bunfastins(bn, h, buf);
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
			BBPreleaseref(bi[i].b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL )
		GDKfree(buf);
	GDKfree(bi);
	GDKfree(p);
	GDKfree(q);
	throw(MAL, "xml.forest", "%s", err);
}

str
BATXMLconcat(int *ret, int *bid, int *rid)
{
	BAT *b, *r = 0, *bn;
	BUN p, q, rp = 0;
	size_t len, size = BUFSIZ;
	str buf = GDKmalloc(size);
	BATiter bi, ri;
	str err = OPERATION_FAILED;

	if ( buf == NULL)
		throw(MAL, "xml.concat", MAL_MALLOC_FAIL);

	if (*rid && (r = BATdescriptor(*rid))) {
		rp = BUNfirst(r);
	}
	if (r == 0)
		throw(MAL, "xml.concat", INTERNAL_BAT_ACCESS);

	/* collect the admin for the xml elements */
	if ((b = BATdescriptor(*bid))) {
		p = BUNfirst(b);
		q = BUNlast(b);
	}
	if (b == NULL) {
		if (r)
			BBPunfix(r->batCacheid);
		throw(MAL, "xml.concat", INTERNAL_BAT_ACCESS);
	}

	prepareResult(bn, b, TYPE_xml, "concat");

	bi = bat_iterator(b);
	ri = bat_iterator(r);
	while (p < q) {
		str t = (str) BUNtail(bi, p);
		oid *h = (oid *) BUNhead(bi, p);
		str v = (str) BUNtail(ri, rp);

		len = strlen(t) + strlen(v) + 1;

		if (len >= size) {
			GDKfree(buf);
			size = len + 128;
			buf = GDKmalloc(size);
			if ( buf == NULL){
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
		bunfastins(bn, h, buf);
		rp++;
		p++;
	}
	GDKfree(buf);
	finalizeResult(ret, bn, b);
	return MAL_SUCCEED;
  bunins_failed:
	BBPreleaseref(r->batCacheid);
	BBPreleaseref(b->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.concat", "%s", err);
}

str
BATXMLagg3(int *ret, int *bid, int *grp, int *ext)
{
	BAT *j, *r, *g, *b, *bn, *e;
	BUN p, q;
	oid gid, o;
	int first;
	str t;
	size_t len, size = BUFSIZ, offset;
	str buf = GDKmalloc(size);
	BATiter ri;
	str err = "bunins failed";	/* default error message */

	if ( buf == NULL)
		throw(MAL,"xml.agg", MAL_MALLOC_FAIL);
	getBATdescriptor(ext,e,"xml.agg",);
	getBATdescriptor(grp,g,"xml.agg",BBPunfix(e->batCacheid));
	getBATdescriptor(bid,b,"xml.agg",(BBPunfix(e->batCacheid),BBPunfix(g->batCacheid)));

	bn = BATnew(TYPE_oid, b->ttype, BATcount(e));
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		BBPunfix(e->batCacheid);
		throw(MAL, "xml.agg", INTERNAL_OBJ_CREATE);
	}
	bn->T->nonil = 1;

	/* this will not work as it will corrupt the order of the column, ie
	   the order in which the data will be generated */
	j = BATjoin(BATmirror(g), b, BUN_NONE);
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(g->batCacheid);
	BBPreleaseref(e->batCacheid);
	r = BATsort(j);
	BBPunfix(j->batCacheid);

	/* now we can perform a simple scan and emit the group */

	strcpy(buf, str_nil);
	offset = 0;
	first = 1;
	gid = 0;
	ri = bat_iterator(r);
	BATloop(r, p, q) {
		int n;

		o = *(oid *) BUNhead(ri, p);
		t = (str) BUNtail(ri, p);

		if (gid != o && !first) {
			/* flush */
			bunfastins(bn, &gid, buf);
			if (offset == 0)
				bn->T->nonil = 0;
			strcpy(buf, str_nil);
			offset = 0;
		}
		gid = o;
		first = 0;
		if (strNil(t))
			continue;
		len = strlen(t) + 1;
		if (len >= size - offset) {
			size += len + 128;
			buf = GDKrealloc(buf, size);
			if ( buf == NULL){
				err= MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (offset == 0)
			n = snprintf(buf, size, "%s", t);
		else if (buf[0] != *t) {
			err = "incompatible values in group";
			goto bunins_failed;
		} else if (buf[0] == 'A')
			n = snprintf(buf + offset, size - offset, " %s", t + 1);
		else if (buf[0] == 'C')
			n = snprintf(buf + offset, size - offset, "%s", t + 1);
		else {
			err = "can only group attributes and element content";
			goto bunins_failed;
		}
		offset += n;
	}
	/* end the leftover element */
	if (!first) {
		bunfastins(bn, &gid, buf);
		if (offset == 0)
			bn->T->nonil = 0;
	}

	BBPunfix(r->batCacheid);
	GDKfree(buf);
	bn->hsorted = 1;
	bn->hrevsorted = bn->batCount <= 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->T->nonil = 0;
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(r->batCacheid);
	BBPunfix(bn->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.agg", "%s", err);
}

str
BATXMLagg(int *ret, int *bid, int *grp)
{
	BAT *j, *r, *g, *b, *bn;
	BUN p, q;
	oid gid = 0, o = 0;
	int first = 1;
	str t;
	size_t len, size = BUFSIZ, offset;
	str buf = GDKmalloc(size);
	BATiter ri;
	str err = "bunins failed";	/* default error message */

	if ( buf == NULL)
		throw(MAL,"xml.agg",MAL_MALLOC_FAIL);
	getBATdescriptor(grp,g,"xml.agg",);
	getBATdescriptor(bid,b,"xml.agg",BBPunfix(g->batCacheid));

	bn = BATnew(TYPE_oid, b->ttype, BATcount(g));
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		BBPunfix(g->batCacheid);
		throw(MAL, "xml.agg", INTERNAL_OBJ_CREATE);
	}
	bn->T->nonil = 1;

	j = BATjoin(BATmirror(g), b, BUN_NONE);
	BBPreleaseref(b->batCacheid);
	BBPreleaseref(g->batCacheid);
	r = BATsort(j);
	BBPunfix(j->batCacheid);

	/* now we can perform a simple scan and emit the group */

	strcpy(buf, str_nil);
	offset = 0;
	first = 1;
	gid = 0;
	ri = bat_iterator(r);
	BATloop(r, p, q) {
		int n;

		o = *(oid *) BUNhead(ri, p);
		t = (str) BUNtail(ri, p);

		if (gid != o && !first) {
			/* flush */
			bunfastins(bn, &gid, buf);
			if (offset == 0)
				bn->T->nonil = 0;
			strcpy(buf, str_nil);
			offset = 0;
		} else if (first) {
			bn->H->nil = (gid == oid_nil);
			bn->H->nonil = (gid != oid_nil);
			first = 0;
		}
		gid = o;
		if (strNil(t))
			continue;
		len = strlen(t) + 1;
		if (len >= size - offset) {
			size += len + 128;
			buf = GDKrealloc(buf, size);
			if ( buf == NULL ){
				err = MAL_MALLOC_FAIL;
				goto bunins_failed;
			}
		}
		if (offset == 0)
			n = snprintf(buf, size, "%s", t);
		else if (buf[0] != *t) {
			err = "incompatible values in group";
			goto bunins_failed;
		} else if (buf[0] == 'A')
			n = snprintf(buf + offset, size - offset, " %s", t + 1);
		else if (buf[0] == 'C')
			n = snprintf(buf + offset, size - offset, "%s", t + 1);
		else {
			err = "can only group attributes and element content";
			goto bunins_failed;
		}
		offset += n;
	}
	/* end the leftover element */
	if (!first) {
		bunfastins(bn, &gid, buf);
		if (offset == 0)
			bn->T->nonil = 0;
	}

	BBPunfix(r->batCacheid);
	GDKfree(buf);
	bn->hsorted = 1;
	bn->hrevsorted = bn->batCount <= 1;
	bn->hkey = 1;
	bn->tsorted = 0;
	bn->trevsorted = 0;
	bn->T->nonil = 0;
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
  bunins_failed:
	BBPunfix(bn->batCacheid);
	BBPunfix(r->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.agg", "%s", err);
}

str
BATXMLgroup(xml *ret, int *bid)
{
	BAT *b;
	BUN p, q;
	str t;
	size_t len, size = BUFSIZ, offset;
	str buf = GDKmalloc(size);
	BATiter bi;
	str err = NULL;

	if ( buf == NULL)
		throw( MAL, "xml.group",MAL_MALLOC_FAIL);
	getBATdescriptor(bid,b,"xml.agg",);

	strcpy(buf, str_nil);
	offset = 0;
	bi = bat_iterator(b);
	BATloop(b, p, q) {
		int n;

		t = (str) BUNtail(bi, p);

		if (strNil(t))
			continue;
		len = strlen(t) + 1;
		if (len >= size - offset) {
			size += len + 128;
			buf = GDKrealloc(buf, size);
			if ( buf == NULL){
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
	BBPreleaseref(b->batCacheid);
	*ret = buf;
	return MAL_SUCCEED;
  failed:
	BBPreleaseref(b->batCacheid);
	if ( buf != NULL)
		GDKfree(buf);
	throw(MAL, "xml.agg", "%s", err);
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
		bn = BATconstant(TYPE_xml, ATOMnilptr(TYPE_xml), ngrp);
		if (bn == NULL)
			return MAL_MALLOC_FAIL;
		BATseqbase(bn, ngrp == 0 ? 0 : min);
		*bnp = bn;
		return NULL;
	}
	if (s) {
		b = BATleftjoin(s, b, BATcount(s));
		if (b == NULL) {
			err = "internal leftjoin failed";
			goto out;
		}
		freeb = 1;
		if (b->htype != TYPE_void) {
			t1 = BATmirror(BATmark(BATmirror(b), 0));
			if (t1 == NULL) {
				err = "internal mark failed";
				goto out;
			}
			BBPunfix(b->batCacheid);
			b = t1;
		}
		if (g) {
			g = BATleftjoin(s, g, BATcount(s));
			if (g == NULL) {
				err = "internal leftjoin failed";
				goto out;
			}
			freeg = 1;
			if (g->htype != TYPE_void) {
				t1 = BATmirror(BATmark(BATmirror(g), 0));
				if (t1 == NULL) {
					err = "internal mark failed";
					goto out;
				}
				BBPunfix(g->batCacheid);
				g = t1;
			}
		}
	}
	if (g && BATtdense(g)) {
		/* singleton groups: return group ID's (g's tail) and original
		 * values from b */
		bn = VIEWcreate(BATmirror(g), b);
		goto out;
	}

	maxlen = BUFSIZ;
	if ((buf = GDKmalloc(maxlen)) == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	buflen = 0;
	bn = BATnew(TYPE_void, TYPE_xml, ngrp);
	if (bn == NULL) {
		err = MAL_MALLOC_FAIL;
		goto out;
	}
	bi = bat_iterator(b);
	if (g) {
		/* stable sort g */
		if (BATsubsort(&t1, &t2, NULL, g, NULL, NULL, 0, 1) == GDK_FAIL) {
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
					bunfastins_nocheck(bn, BUNlast(bn), 0, str_nil, 0, Tsize(bn));
					nils++;
				}
				bunfastins_nocheck(bn, BUNlast(bn), 0, buf, 0, Tsize(bn));
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
		bunfastins_nocheck(bn, BUNlast(bn), 0, buf, 0, Tsize(bn));
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
	if (bn)
		BBPreclaim(bn);
	bn = NULL;
	if (err == NULL)
		err = MAL_MALLOC_FAIL;	/* insertion into result BAT failed */
	goto out;
}

str
AGGRsubxmlcand(bat *retval, bat *bid, bat *gid, bat *eid, bat *sid, bit *skip_nils)
{
	BAT *b, *g, *e, *s, *bn = NULL;
	const char *err;

	b = BATdescriptor(*bid);
	g = gid ? BATdescriptor(*gid) : NULL;
	e = eid ? BATdescriptor(*eid) : NULL;
	if (b == NULL || (gid != NULL && g == NULL) || (eid != NULL && e == NULL)) {
		if (b)
			BBPreleaseref(b->batCacheid);
		if (g)
			BBPreleaseref(g->batCacheid);
		if (e)
			BBPreleaseref(e->batCacheid);
		throw(MAL, "aggr.subxml", RUNTIME_OBJECT_MISSING);
	}
	if (sid) {
		s = BATdescriptor(*sid);
		if (s == NULL) {
			BBPreleaseref(b->batCacheid);
			if (g)
				BBPreleaseref(g->batCacheid);
			if (e)
				BBPreleaseref(e->batCacheid);
			throw(MAL, "aggr.subxml", RUNTIME_OBJECT_MISSING);
		}
	} else {
		s = NULL;
	}
	err = BATxmlaggr(&bn, b, g, e, s, *skip_nils);
	BBPreleaseref(b->batCacheid);
	if (g)
		BBPreleaseref(g->batCacheid);
	if (e)
		BBPreleaseref(e->batCacheid);
	if (s)
		BBPreleaseref(s->batCacheid);
	if (err != NULL)
		throw(MAL, "aggr.subxml", "%s", err);

	*retval = bn->batCacheid;
	BBPkeepref(bn->batCacheid);
	return MAL_SUCCEED;
}

str
AGGRsubxml(bat *retval, bat *bid, bat *gid, bat *eid, bit *skip_nils)
{
	return AGGRsubxmlcand(retval, bid, gid, eid, NULL, skip_nils);
}

str
BATXMLxquery(int *ret, int *bid, str *expr)
{
	(void) ret;
	(void) bid;
	(void) expr;
	/* use external library to solve this */
	throw(MAL, "xml.xquery", PROGRAM_NYI);
}
