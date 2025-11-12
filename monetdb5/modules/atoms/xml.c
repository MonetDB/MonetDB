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

/*
 * @f xml
 * @a Sjoerd Mullender, Niels Nes, Martin Kersten
 * @v 0.1
 * @+ MAL support for XQL
 * This module contains the primitives needed in the SQL
 * front-end to support SQL/XML.
 */
#include "monetdb_config.h"
#include "xml.h"
#include "mal_interpreter.h"

#ifdef HAVE_LIBXML
#include <libxml/parser.h>
#include <libxml/xmlmemory.h>

/* The xml atom is used to represent XML data.  It is implemented as a
   subtype of str.  The first character of the string representation
   indicates the type of XML data.  There are three possibilities:
   * D - an XML document (possibly including <?xml?> and DOCTYPE);
   * C - XML content, i.e. something that can occur inside an XML element;
   * A - XML name/attribute pair.
*/

size_t
XMLquotestring(const char *s, char *buf, size_t len)
{
	size_t i = 0;

	assert(len > 0);
	while (*s && i + 6 < len) {
		if (*s == '&') {
			buf[i++] = '&';
			buf[i++] = 'a';
			buf[i++] = 'm';
			buf[i++] = 'p';
			buf[i++] = ';';
		} else if (*s == '<') {
			buf[i++] = '&';
			buf[i++] = 'l';
			buf[i++] = 't';
			buf[i++] = ';';
		} else if (*s == '>') {
			buf[i++] = '&';
			buf[i++] = 'g';
			buf[i++] = 't';
			buf[i++] = ';';
		} else if (*s == '"') {
			buf[i++] = '&';
			buf[i++] = 'q';
			buf[i++] = 'u';
			buf[i++] = 'o';
			buf[i++] = 't';
			buf[i++] = ';';
		} else if (*s == '\'') {
			buf[i++] = '&';
			buf[i++] = 'a';
			buf[i++] = 'p';
			buf[i++] = 'o';
			buf[i++] = 's';
			buf[i++] = ';';
		} else if ((*s & 0xFF) < 0x20) {
			int n = snprintf(buf, len - i, "&#%d;", *s & 0xFF);

			if (n < 0)
				break;
			i += n;
		} else {
			buf[i++] = *s;
		}
		s++;
	}
	if (i < len)
		buf[i] = 0;
	else
		buf[len - 1] = 0;
	return i;
}

size_t
XMLunquotestring(const char **p, char q, char *buf)
{
	const char *s = *p;
	size_t i = 0;

	while (*s && *s != q) {
		if (*s == '&') {
			s++;
			if (strncmp(s, "lt;", 3) == 0) {
				buf[i++] = '<';
				s += 3;
			} else if (strncmp(s, "gt;", 3) == 0) {
				buf[i++] = '>';
				s += 3;
			} else if (strncmp(s, "apos;", 5) == 0) {
				buf[i++] = '\'';
				s += 5;
			} else if (strncmp(s, "quot;", 5) == 0) {
				buf[i++] = '"';
				s += 5;
			} else if (strncmp(s, "amp;", 4) == 0) {
				buf[i++] = '&';
				s += 4;
			} else if (*s == '#') {
				char *e;
				int base;
				unsigned long n;	/* type unsigned long returned by strtoul() */

				s++;
				if (*s == 'x' || *s == 'X') {
					s++;
					base = 16;
				} else
					base = 10;
				n = strtoul(s, &e, base);
				assert(e > s && *e == ';');
				assert(n <= 0x7FFFFFFF);
				s = e + 1;
				if (n <= 0x7F)
					buf[i++] = (char) n;
				else if (n <= 0x7FF) {
					buf[i++] = (char) (0xC0 | (n >> 6));
					buf[i++] = (char) (0x80 | (n & 0x3F));
				} else if (n <= 0xFFFF) {
					buf[i++] = (char) (0xE0 | (n >> 12));
					buf[i++] = (char) (0x80 | ((n >> 6) & 0x3F));
					buf[i++] = (char) (0x80 | (n & 0x3F));
				} else if (n <= 0x1FFFFF) {
					buf[i++] = (char) (0xF0 | (n >> 18));
					buf[i++] = (char) (0x80 | ((n >> 12) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 6) & 0x3F));
					buf[i++] = (char) (0x80 | (n & 0x3F));
				} else if (n <= 0x3FFFFFF) {
					buf[i++] = (char) (0xF8 | (n >> 24));
					buf[i++] = (char) (0x80 | ((n >> 18) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 12) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 6) & 0x3F));
					buf[i++] = (char) (0x80 | (n & 0x3F));
				} else if (n <= 0x7FFFFFFF) {
					buf[i++] = (char) (0xFC | (n >> 30));
					buf[i++] = (char) (0x80 | ((n >> 24) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 18) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 12) & 0x3F));
					buf[i++] = (char) (0x80 | ((n >> 6) & 0x3F));
					buf[i++] = (char) (0x80 | (n & 0x3F));
				}
			} else {
				/* unrecognized entity: just leave it */
				buf[i++] = '&';
			}
		} else
			buf[i++] = *s++;
	}
	buf[i] = 0;
	*p = s;
	return i;
}

str
XMLxml2str(Client ctx, str *s, const xml *x)
{
	allocator *ma = ctx->curprg->def->ma;
	if (strNil(*x)) {
		*s = MA_STRDUP(ma, str_nil);
		return MAL_SUCCEED;
	}
	assert(**x == 'A' || **x == 'C' || **x == 'D');
	*s = MA_STRDUP(ma, *x + 1);
	return MAL_SUCCEED;
}

str
XMLstr2xml(Client ctx, xml *x, const char *const*val)
{
	allocator *ma = ctx->curprg->def->ma;
	const char *t = *val;
	str buf;
	size_t len;

	if (strNil(t)) {
		*x = (xml) MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			throw(MAL, "xml.xml", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	len = 6 * strlen(t) + 1;
	buf = ma_alloc(ma, len + 1);
	if (buf == NULL)
		throw(MAL, "xml.xml", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	buf[0] = 'C';
	XMLquotestring(t, buf + 1, len);
	*x = buf;
	return MAL_SUCCEED;
}

str
XMLxmltext(Client ctx, str *s, const xml *x)
{
	allocator *ma = ctx->curprg->def->ma;
	xmlDocPtr doc;
	xmlNodePtr elem;
	str content = NULL;

	if (strNil(*x)) {
		*s = MA_STRDUP(ma, str_nil);
		if (*s == NULL)
			throw(MAL, "xml.text", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if (**x == 'D') {
		doc = xmlParseMemory(*x + 1, (int) strlen(*x + 1));
		elem = xmlDocGetRootElement(doc);
		xmlChar *cont = xmlNodeGetContent(elem);
		xmlFreeDoc(doc);
		content = MA_STRDUP(ma, (const char *) cont);
		xmlFree(cont);
	} else if (**x == 'C') {
		doc = xmlParseMemory("<doc/>", 6);
		xmlParseInNodeContext(xmlDocGetRootElement(doc), *x + 1,
							  (int) strlen(*x + 1), 0, &elem);
		xmlChar *cont = xmlNodeGetContent(elem);
		xmlFreeNodeList(elem);
		xmlFreeDoc(doc);
		content = MA_STRDUP(ma, (const char *) cont);
		xmlFree(cont);
	} else if (**x == 'A') {
		const char *t = *x + 1;

		content = ma_alloc(ma, strlen(*x) + 1);
		if (content) {
			str p = content;
			while (*t) {
				if (*t == '"' || *t == '\'') {
					char q = *t++;

					p += XMLunquotestring(&t, q, p);
				}
				t++;
			}
			*p = 0;
		}
	} else {
		content = MA_STRDUP(ma, "");
	}
	if (content == NULL)
		throw(MAL, "xml.text", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	else
		*s = (str) content;
	return MAL_SUCCEED;
}

str
XMLxml2xml(Client ctx, xml *s, const xml *x)
{
	allocator *ma = ctx->curprg->def->ma;
	*s = MA_STRDUP(ma, *x);
	if (*s == NULL)
		throw(MAL, "xml.xml", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
XMLdocument(Client ctx, xml *x, const char * const *val)
{
	allocator *ma = ctx->curprg->def->ma;
	xmlDocPtr doc;

	if (strNil(*val)) {
		*x = (xml) MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			throw(MAL, "xml.document", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	/* call the libxml2 library to perform the test */
	doc = xmlParseMemory(*val, (int) strlen(*val));
	if (doc) {
		int len;
		xmlChar *buf;

		xmlDocDumpMemory(doc, &buf, &len);
		xmlFreeDoc(doc);
		*x = ma_alloc(ma, len + 2);
		if (*x == NULL)
			throw(MAL, "xml.document", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(*x, len + 2, "D%s", (char *) buf);
		xmlFree(buf);
		return MAL_SUCCEED;
	}
	throw(MAL, "xml.document", "Document parse error");
}

str
XMLcontent(Client ctx, xml *x, const char * const *val)
{
	allocator *ma = ctx->curprg->def->ma;
	xmlDocPtr doc;
	xmlNodePtr elem;
	xmlParserErrors err;
	xmlBufferPtr buf;
	const xmlChar *s;
	size_t len;

	if (strNil(*val)) {
		*x = (xml) MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			throw(MAL, "xml.content", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	/* call the libxml2 library to perform the test */
	doc = xmlParseMemory("<doc/>", 6);
	err = xmlParseInNodeContext(xmlDocGetRootElement(doc), *val,
								(int) strlen(*val), 0, &elem);
	if (err != XML_ERR_OK) {
		xmlFreeDoc(doc);
		throw(MAL, "xml.content", "Content parse error");
	}
	buf = xmlBufferCreate();
	xmlNodeDump(buf, doc, elem, 0, 0);
	s = xmlBufferContent(buf);
	len = strlen((const char *) s) + 2;
	*x = ma_alloc(ma, len);
	if (*x == NULL)
		throw(MAL, "xml.content", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(*x, len, "C%s", (const char *) s);
	xmlBufferFree(buf);
	xmlFreeNodeList(elem);
	xmlFreeDoc(doc);
	return MAL_SUCCEED;
}

str
XMLisdocument(Client ctx, bit *x, const char * const *s)
{
	(void) ctx;
	xmlDocPtr doc;

	/* call the libxml2 library to perform the test */
	if (strNil(*s))
		*x = bit_nil;
	else {
		doc = xmlParseMemory(*s, (int) strlen(*s));
		*x = doc != NULL;
		if (doc)
			xmlFreeDoc(doc);
	}
	return MAL_SUCCEED;
}

str
XMLcomment(Client ctx, xml *x, const char * const *s)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len;
	str buf;

	if (strNil(*s)) {
		*x = (xml) MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			throw(MAL, "xml.comment", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if (strstr(*s, "--") != NULL)
		throw(MAL, "xml.comment", "comment may not contain `--'");
	len = strlen(*s) + 9;
	buf = ma_alloc(ma, len);
	if (buf == NULL)
		throw(MAL, "xml.comment", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	snprintf(buf, len, "C<!--%s-->", *s);
	*x = buf;
	return MAL_SUCCEED;
}

str
XMLparse(Client ctx, xml *x, const char * const *doccont, const char * const *val, const char * const *option)
{
	(void) ctx;
	(void) option;
	if (strcmp(*doccont, "content") == 0)
		return XMLcontent(ctx, x, val);
	if (strcmp(*doccont, "document") == 0)
		return XMLdocument(ctx, x, val);
	throw(MAL, "xml.parse", "invalid parameter");
}

str
XMLpi(Client ctx, xml *ret, const char * const *target, const char * const *value)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len;
	str buf;
	str val = NULL;

	if (strNil(*target)) {
		*ret = MA_STRDUP(ma, str_nil);
		if (*ret == NULL)
			throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if (xmlValidateName((xmlChar *) *target, 0) != 0
		|| strcasecmp(*target, "xml") == 0)
		throw(MAL, "xml.attribute", "invalid processing instruction target");
	len = strlen(*target) + 6;
	if (strNil(*value) || **value == 0) {
		size_t n = 6 * strlen(*value) + 1;

		val = ma_alloc(ma, n);
		if (val == NULL)
			throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		len += XMLquotestring(*value, val, n) + 1;
	}
	buf = ma_alloc(ma, len);
	if (buf == NULL) {
		throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if (val == NULL) {
		snprintf(buf, len, "C<?%s?>", *target);
	} else {
		snprintf(buf, len, "C<?%s %s?>", *target, val);
	}
	*ret = buf;
	return MAL_SUCCEED;
}

str
XMLroot(Client ctx, xml *ret, const xml *val, const char * const *version, const char * const *standalone)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len = 0, i = 0;
	str buf;
	bit isdoc = 0;

	if (strNil(*val)) {
		*ret = MA_STRDUP(ma, str_nil);
		if (*ret == NULL)
			throw(MAL, "xml.root", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if (**val != 'C')
		throw(MAL, "xml.root", "value must be an XML node");
	len = 8;
	len = strlen(*val);
	if (!strNil(*version) && **version) {
		if (strcmp(*version, "1.0") != 0 && strcmp(*version, "1.1") != 0)
			throw(MAL, "xml.root", "illegal XML version");
		len += 11 + strlen(*version);	/* strlen(" version=\"\"") */
	}
	if (!strNil(*standalone) && **standalone) {
		if (strcmp(*standalone, "yes") != 0 && strcmp(*standalone, "no") != 0)
			throw(MAL, "xml.root", "illegal XML standalone value");
		len += 14 + strlen(*standalone);	/* strlen(" standalone=\"\"") */
	}
	buf = ma_alloc(ma, len);
	if (buf == NULL)
		throw(MAL, "xml.root", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	strcpy(buf, "D<?xml");
	i = strlen(buf);
	if (!strNil(*version) && **version)
		i += snprintf(buf + i, len - i, " version=\"%s\"", *version);
	if (!strNil(*standalone) && **standalone)
		i += snprintf(buf + i, len - i, " standalone=\"%s\"", *standalone);
	snprintf(buf + i, len - i, "?>%s", *val + 1);
	XMLisdocument(ctx, &isdoc, &(const char *){buf + 1});	/* check well-formedness */
	if (!isdoc) {
		throw(MAL, "xml.root", "resulting document not well-formed");
	}
	*ret = buf;
	return MAL_SUCCEED;
}

str
XMLattribute(Client ctx, xml *x, const char * const *name, const char * const *val)
{
	allocator *ma = ctx->curprg->def->ma;
	const char *t = *val;
	str buf;
	size_t len;

	if (strNil(t) || strNil(*name)) {
		*x = (xml) MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	if (xmlValidateName((xmlChar *) *name, 0) != 0)
		throw(MAL, "xml.attribute", "invalid attribute name");
	len = 6 * strlen(t) + 1;
	buf = ma_alloc(ma, len);
	if (buf == NULL)
		throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	len = XMLquotestring(t, buf, len);
	len += strlen(*name) + 5;
	*x = ma_alloc(ma, len);
	if (*x == NULL) {
		throw(MAL, "xml.attribute", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(*x, len, "A%s=\"%s\"", *name, buf);
	return MAL_SUCCEED;
}

str
XMLelement(Client ctx, xml *ret, const char * const *name, const xml *nspace, const xml *attr, const xml *val)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len, i, namelen;
	str buf;

	if (strNil(*name))
		throw(MAL, "xml.element", "no element name specified");
	if (xmlValidateName((xmlChar *) *name, 0) != 0)
		throw(MAL, "xml.element", "invalid element name");
	/* len is composed of several parts:
	   "C" + "<" + name + (nspace ? " " + nspace : "") + (attr ? " " + attr : "") + (val ? ">" + val + "</" + name + ">" : "/>")
	 */
	namelen = strlen(*name);
	len = namelen + 5;			/* "C", "<", "/", ">", terminating NUL */
	if (nspace && !strNil(*nspace)) {
		if (**nspace != 'A')
			throw(MAL, "xml.element", "illegal namespace");
		len += strlen(*nspace);	/* " " + nspace (nspace contains initial 'A' which is replaced by space) */
	}
	if (attr && !strNil(*attr)) {
		if (**attr != 'A')
			throw(MAL, "xml.element", "illegal attribute");
		len += strlen(*attr);	/* " " + attr (attr contains initial 'A' which is replaced by space) */
	}
	if (!strNil(*val) && **val != 0) {
		if (**val != 'C')
			throw(MAL, "xml.element", "illegal content");
		len += strlen(*val + 1) + namelen + 2;	/* extra "<", ">", and name ("/" already counted) */
	}
	buf = ma_alloc(ma, len);
	if (buf == NULL)
		throw(MAL, "xml.element", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (strNil(*val) && (!attr || strNil(*attr))) {
		strcpy(buf, str_nil);
	} else {
		i = snprintf(buf, len, "C<%s", *name);
		if (nspace && !strNil(*nspace))
			i += snprintf(buf + i, len - i, " %s", *nspace + 1);
		if (attr && !strNil(*attr))
			i += snprintf(buf + i, len - i, " %s", *attr + 1);
		if (!strNil(*val))
			i += snprintf(buf + i, len - i, ">%s</%s>", *val + 1, *name);
		else
			i += snprintf(buf + i, len - i, "/>");
	}
	*ret = buf;
	return MAL_SUCCEED;
}

str
XMLelementSmall(Client ctx, xml *ret, const char * const *name, const xml *val)
{
	(void) ctx;
	return XMLelement(ctx, ret, name, NULL, NULL, val);
}

str
XMLconcat(Client ctx, xml *ret, const xml *left, const xml *right)
{
	allocator *ma = ctx->curprg->def->ma;
	size_t len;
	str buf;

	/* if either side is nil, return the other, otherwise concatenate */
	if (strNil(*left))
		buf = MA_STRDUP(ma, *right);
	else if (strNil(*right))
		buf = MA_STRDUP(ma, *left);
	else if (**left != **right)
		throw(MAL, "xml.concat", "arguments not compatible");
	else if (**left == 'A') {
		len = strlen(*left) + strlen(*right) + 1;
		buf = ma_alloc(ma, len);
		if (buf == NULL)
			throw(MAL, "xml.concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(buf, len, "A%s %s", *left + 1, *right + 1);
	} else if (**left == 'C') {
		len = strlen(*left) + strlen(*right) + 2;
		buf = ma_alloc(ma, len);
		if (buf == NULL)
			throw(MAL, "xml.concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		snprintf(buf, len, "C%s%s", *left + 1, *right + 1);
	} else
		throw(MAL, "xml.concat",
			  "can only concatenate attributes and element content");
	if (buf == NULL)
		throw(MAL, "xml.concat", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*ret = buf;
	return MAL_SUCCEED;
}

str
XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	xml *ret = getArgReference_TYPE(stk, p, 0, xml);
	int i;
	size_t len;
	str buf;
	xml x;

	allocator *ma = cntxt->curprg->def->ma;
	(void) mb;

	len = 2;
	for (i = p->retc; i < p->argc; i++) {
		x = *getArgReference_TYPE(stk, p, i, xml);
		if (!strNil(x))
			if (*x != 'C')
				throw(MAL, "xml.forest", "arguments must be element content");
		len += strlen(x + 1);
	}
	buf = (str) ma_alloc(ma, len);
	if (buf == NULL)
		throw(MAL, "xml.forest", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*ret = buf;
	*buf++ = 'C';
	*buf = 0;

	for (i = p->retc; i < p->argc; i++) {
		x = *getArgReference_TYPE(stk, p, i, xml);
		if (!strNil(x)) {
			len = strlen(x + 1);
			strcpy(buf, x + 1);
			buf += len;
		}
	}
	return MAL_SUCCEED;
}

int TYPE_xml;

static str
XMLprelude(void)
{
	TYPE_xml = ATOMindex("xml");
	xmlInitParser();
	return MAL_SUCCEED;
}

str
XMLepilogue(Client ctx, void *ret)
{
	(void) ctx;
	(void) ret;
	xmlCleanupParser();
	return MAL_SUCCEED;
}

static ssize_t
XMLfromString(allocator *ma, const char *src, size_t *len, void **X, bool external)
{
	xml *x = (xml *) X;
	if (*x) {
		*x = NULL;
	}
	if (external && strcmp(src, "nil") == 0) {
		*x = MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			return -1;
		return 3;
	} else if (strNil(src)) {
		*x = MA_STRDUP(ma, str_nil);
		if (*x == NULL)
			return -1;
		return 1;
	} else {
		char *err = XMLstr2xml(/*ctx*/NULL, x, &src);
		if (err !=MAL_SUCCEED) {
			GDKerror("%s", getExceptionMessageAndState(err));
			freeException(err);
			return -1;
		}
	}
	*len = strlen(*x) + 1;
	return (ssize_t) *len - 1;
}

static ssize_t
XMLtoString(allocator *ma, str *s, size_t *len, const void *SRC, bool external)
{
	const char *src = SRC;
	size_t l;

	if (strNil(src))
		src = external ? "nil" : str_nil;
	else
		src++;
	l = strlen(src) + 1;
	if (l >= *len || *s == NULL) {
		*s = ma_alloc(ma, l);
		if (*s == NULL)
			return -1;
		*len = l;
	}
	strcpy(*s, src);
	return (ssize_t) l - 1;
}

#else

#define NO_LIBXML_FATAL "xml: MonetDB was built without libxml, but what you are trying to do requires it."

static ssize_t
XMLfromString(allocator *ma, const char *src, size_t *len, void **x, bool external)
{
	(void) src;
	(void) len;
	(void) x;
	(void) external;
	(void) ma;
	GDKerror("not implemented\n");
	return -1;
}

static ssize_t
XMLtoString(allocator *ma, str *s, size_t *len, const void *src, bool external)
{
	(void) s;
	(void) len;
	(void) src;
	(void) external;
	(void) ma;
	GDKerror("not implemented\n");
	return -1;
}

str
XMLxml2str(Client ctx, str *s, const xml *x)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.xml2str", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLstr2xml(Client ctx, xml *x, const char *const*val)
{
	(void) ctx;
	(void) val;
	(void) x;
	return createException(MAL, "xml.xml2str", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLxmltext(Client ctx, str *s, const xml *x)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.xmltext", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLxml2xml(Client ctx, xml *x, const xml *s)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.xml2xml", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLdocument(Client ctx, xml *x, const char * const *s)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.document",
						   SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLcontent(Client ctx, xml *x, const char * const *s)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.content", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLisdocument(Client ctx, bit *x, const char * const *s)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.isdocument",
						   SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLcomment(Client ctx, xml *x, const char * const *s)
{
	(void) ctx;
	(void) s;
	(void) x;
	return createException(MAL, "xml.comment", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLpi(Client ctx, xml *x, const char * const *target, const char * const *s)
{
	(void) ctx;
	(void) s;
	(void) target;
	(void) x;
	return createException(MAL, "xml.pi", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLroot(Client ctx, xml *x, const xml *v, const char * const *version, const char * const *standalone)
{
	(void) ctx;
	(void) x;
	(void) v;
	(void) version;
	(void) standalone;
	return createException(MAL, "xml.root", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLparse(Client ctx, xml *x, const char * const *doccont, const char * const *s, const char * const *option)
{
	(void) ctx;
	(void) x;
	(void) doccont;
	(void) s;
	(void) option;
	return createException(MAL, "xml.parse", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLattribute(Client ctx, xml *ret, const char * const *name, const char * const *val)
{
	(void) ctx;
	(void) ret;
	(void) name;
	(void) val;
	return createException(MAL, "xml.attribute",
						   SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLelement(Client ctx, xml *ret, const char * const *name, const xml *nspace, const xml *attr, const xml *val)
{
	(void) ctx;
	(void) ret;
	(void) name;
	(void) nspace;
	(void) attr;
	(void) val;
	return createException(MAL, "xml.element", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLelementSmall(Client ctx, xml *ret, const char * const *name, const xml *val)
{
	(void) ctx;
	(void) ret;
	(void) name;
	(void) val;
	return createException(MAL, "xml.elementSmall",
						   SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLconcat(Client ctx, xml *ret, const xml *left, const xml *right)
{
	(void) ctx;
	(void) ret;
	(void) left;
	(void) right;
	return createException(MAL, "xml.concat", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

str
XMLforest(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void) cntxt;
	(void) mb;
	(void) stk;
	(void) p;
	return createException(MAL, "xml.forest", SQLSTATE(HY005) NO_LIBXML_FATAL);
}

size_t
XMLquotestring(const char *s, char *buf, size_t len)
{
	(void) s;
	(void) buf;
	(void) len;
	return 0;
}

size_t
XMLunquotestring(const char **p, char q, char *buf)
{
	(void) p;
	(void) q;
	(void) buf;
	return 0;
}

static str
XMLprelude(void)
{
	return MAL_SUCCEED;			/* to not break init */
}

str
XMLepilogue(Client ctx, void *ret)
{
	(void) ctx;
	(void) ret;
	return MAL_SUCCEED;
}

#endif /* HAVE_LIBXML */

#include "mel.h"
mel_atom xml_init_atoms[] = {
 { .name="xml", .basetype="str", .fromstr=XMLfromString, .tostr=XMLtoString, },  { .cmp=NULL }
};
mel_func xml_init_funcs[] = {
 command("xml", "xml", XMLstr2xml, false, "Cast the string to an xml compliant string", args(1,2, arg("",xml),arg("src",str))),
 command("xml", "str", XMLxml2str, false, "Cast the string to an xml compliant string", args(1,2, arg("",str),arg("src",xml))),
 command("xml", "text", XMLxmltext, false, "Extract text from an xml atom", args(1,2, arg("",str),arg("src",xml))),
 command("xml", "comment", XMLcomment, false, "Construct an comment struction ", args(1,2, arg("",xml),arg("val",str))),
 command("xml", "parse", XMLparse, false, "Parse the XML document or element string values ", args(1,4, arg("",xml),arg("doccont",str),arg("val",str),arg("option",str))),
 command("xml", "pi", XMLpi, false, "Construct a processing instruction", args(1,3, arg("",xml),arg("target",str),arg("val",str))),
 command("xml", "document", XMLdocument, false, "Check the value for compliance as XML document", args(1,2, arg("",xml),arg("val",str))),
 command("xml", "content", XMLcontent, false, "Check the value for compliance as content, i.e.  it may contain multiple roots and character data.", args(1,2, arg("",xml),arg("val",str))),
 command("xml", "root", XMLroot, false, "Construct the root nodes", args(1,4, arg("",xml),arg("val",xml),arg("version",str),arg("standalone",str))),
 command("xml", "attribute", XMLattribute, false, "Construct an attribute value pair", args(1,3, arg("",xml),arg("name",str),arg("val",str))),
 command("xml", "element", XMLelement, false, "The basic building block for XML elements are namespaces, attributes and a sequence of xml elements. The name space and the attributes may be left unspecified(=nil:bat).", args(1,5, arg("",xml),arg("name",str),arg("ns",xml),arg("attr",xml),arg("s",xml))),
 command("xml", "element", XMLelementSmall, false, "The basic building block for XML elements are namespaces, attributes and a sequence of xml elements. The name space and the attributes may be left unspecified(=nil:bat).", args(1,3, arg("",xml),arg("name",str),arg("s",xml))),
 command("xml", "concat", XMLconcat, false, "Concatenate the xml values", args(1,3, arg("",xml),arg("val1",xml),arg("val2",xml))),
 pattern("xml", "forest", XMLforest, false, "Construct an element list", args(1,2, arg("",xml),vararg("val",xml))),
 command("xml", "isdocument", XMLisdocument, false, "Validate the string as a document", args(1,2, arg("",bit),arg("val",str))),
 command("xml", "epilogue", XMLepilogue, false, "", args(1,1, arg("",void))),
 command("calc", "xml", XMLstr2xml, false, "", args(1,2, arg("",xml),arg("src",str))),
 command("calc", "xml", XMLxml2xml, false, "", args(1,2, arg("",xml),arg("src",xml))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_xml_mal)
{ mal_module2("xml", xml_init_atoms, xml_init_funcs, XMLprelude, NULL); }
