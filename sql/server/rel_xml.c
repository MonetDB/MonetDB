/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_xml.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "sql_semantic.h"
#include "sql_parser.h"

static sql_exp *
rel_xmlelement(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	mvc *sql = query->sql;
	dnode *d = sym->data.lval->h;
	const char *tag = d->data.sval; 
	dlist *ns_attrs_elms = d->next->data.lval; 
	sql_exp *ns_st = NULL, *attr_st = NULL, *res = NULL;
	sql_type *t = NULL;
	sql_subtype xml_type;

	if ((t = mvc_bind_type(sql, "xml")) == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "XML: xml type missing, probably the xml module wasn't added");
	sql_init_subtype(&xml_type, t, 0, 0);
	if (ns_attrs_elms) {
		symbol *ns = ns_attrs_elms->h->data.sym;
		symbol *attr = ns_attrs_elms->h->next->data.sym;
		dlist *content = ns_attrs_elms->h->next->next->data.lval;

		/* loop over the content, create recursive element  */
		if (content) {
			dnode *n;
			dlist *cl = content->h->data.lval;

			for (n=cl->h; n; n = n->next) {
				symbol *c = n->data.sym;
				sql_subtype *st; 
				sql_exp *c_st = rel_value_exp(query, rel, c, f, knd);

				if (!c_st) 
					return NULL;

				st = exp_subtype(c_st);
				assert(st);
				if (type_cmp(st->type, xml_type.type) != 0) {
					sql_subtype str_type;

					sql_find_subtype(&str_type, "clob", 0, 0);
					/* convert to string first */
					c_st = rel_check_type(sql, &str_type, rel ? *rel : NULL, c_st, type_equal);
					/* then to xml */
					if (!c_st || (c_st = rel_check_type(sql, &xml_type, rel ? *rel : NULL, c_st, type_equal)) == NULL)
						return NULL;
				}
				
				/* lets glue the xml content together */
				if (res) {
					res = rel_binop_(sql, rel ? *rel : NULL, res, c_st, NULL, "concat", card_value);
				} else {
					res = c_st;
				}
			}
		}
		if (ns) { 
			ns_st = rel_value_exp(query, rel, ns, f, knd); 
			if (!ns_st)
				return NULL;
		}
		if (attr) {
			attr_st = rel_value_exp(query, rel, attr, f, knd); 
			if (!attr_st)
				return NULL;
		}
	}

	if (!ns_st)
		ns_st = exp_atom(sql->sa, atom_general(sql->sa, &xml_type, NULL));
	if (!attr_st)
		attr_st = exp_atom(sql->sa, atom_general(sql->sa, &xml_type, NULL));
	if (!res)
		res = exp_atom(sql->sa, atom_general(sql->sa, &xml_type, NULL));

	if (!ns_st || !attr_st || !res) 
		return NULL;
	return rel_nop_(query->sql, rel ? *rel : NULL, exp_atom_clob(sql->sa, tag), ns_st, attr_st, res, NULL, "element",
					card_value);
}

static sql_exp *
rel_xmlforest(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	mvc *sql = query->sql;
	dnode *d = sym->data.lval->h;
	symbol *ns = d->data.sym;
	dlist *elms = d->next->data.lval;  
	sql_exp *ns_st, *attr_st, *res = NULL;
	sql_type *t = NULL;
	sql_subtype xml_type;

	if ((t = mvc_bind_type(sql, "xml")) == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "XML: xml type missing, probably the xml module wasn't added");
	sql_init_subtype(&xml_type, t, 0, 0);
	if (ns) {
		ns_st = rel_value_exp(query, rel, ns, f, knd); 
	} else {
		ns_st = exp_atom(sql->sa, atom_general(sql->sa, &xml_type, NULL));
	}
	if (!ns_st)
		return NULL;
	attr_st = exp_atom(sql->sa, atom_general(sql->sa, &xml_type, NULL));
	if (elms) {
		dnode *e;

		for (e = elms->h; e; e = e->next) {
			dnode *cc = e->data.lval->h;
			symbol *c = cc->data.sym;
			const char *tag = cc->next->data.sval;

			sql_exp *c_st = rel_value_exp(query, rel, c, f, knd);
			sql_subtype *st;
			if (!c_st) 
				return NULL;

			st = exp_subtype(c_st);
			assert(st);
			if (type_cmp(st->type, xml_type.type) != 0) {
				sql_subtype str_type;

				sql_find_subtype(&str_type, "clob", 0, 0);
				/* convert to string first */
				c_st = rel_check_type(sql, &str_type, rel ? *rel : NULL, c_st, type_equal);
				/* then to xml */
				if (!c_st || (c_st = rel_check_type(sql, &xml_type, rel ? *rel : NULL, c_st, type_equal)) == NULL)
					return NULL;
			}

			if (!tag) {
				tag = exp_name(c_st);
				if (!tag)
					tag = "single_value";
			}
			c_st = rel_nop_(sql, rel ? *rel : NULL, exp_atom_clob(sql->sa, tag), ns_st, attr_st, c_st, NULL,
							"element", card_value);
			/* lets glue the xml content together */
			if (res) {
				res = rel_binop_(sql, rel ? *rel : NULL, res, c_st, NULL, "concat", card_value);
			} else {
				res = c_st;
			}
		}
	}
	return res;
}

static sql_exp *
rel_xmlcomment(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	symbol *comment = d->data.sym;
	sql_exp *comment_st;

	comment_st = rel_value_exp(query, rel, comment, f, knd); 
	if (!comment_st)
		return NULL;
	return rel_unop_(query->sql, rel ? *rel : NULL, comment_st, NULL, "comment", card_value);
}

static sql_exp *
rel_xmlattribute(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	const char *attr_name = d->data.sval;
	symbol *attr = d->next->data.sym;
	sql_exp *attr_st, *attr_name_st = NULL;

	attr_st = rel_value_exp(query, rel, attr, f, knd); 
	if (!attr_st)
		return NULL;
	if (!attr_name) {
		/*TODO:convert simple column names into valid attribute names */
		attr_name = exp_name(attr_st);
		if (!attr_name)
			attr_name = "single_value";
	}
	sql_subtype str_type;

	sql_find_subtype(&str_type, "clob", 0, 0);
	attr_name_st = exp_atom_str(query->sql->sa, attr_name, &str_type);
	return rel_binop_(query->sql, rel ? *rel : NULL, attr_name_st, attr_st, NULL, "attribute", card_value);
}

static sql_exp *
rel_xmlconcat(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	dnode *en = d->data.lval->h;
	sql_exp *concat_st, *res = NULL;

	for (; en; en = en->next) {
		symbol *c = en->data.sym;
		concat_st = rel_value_exp(query, rel, c, f, knd); 
		if (!concat_st) 
			return NULL;
		if (res)
			res = rel_binop_(query->sql, rel ? *rel : NULL, res, concat_st, NULL, "concat", card_value);
		else
			res = concat_st;
	}	
	return res;
}

static sql_exp *
rel_xmldocument(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	symbol *val = d->data.sym;
	sql_exp *val_st;

	val_st = rel_value_exp(query, rel, val, f, knd); 
	if (!val_st)
		return NULL;
	return rel_unop_(query->sql, rel ? *rel : NULL, val_st, NULL, "document", card_value);
}

static sql_exp *
rel_xmlpi(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	char *target = d->data.sval;
	symbol *val = d->next->data.sym;
	sql_exp *target_st, *val_st;
	sql_subtype str_type;

	sql_find_subtype(&str_type, "clob", 0, 0);
	target_st = exp_atom_str(query->sql->sa, target, &str_type);
	if (!val)
		val_st = rel_value_exp(query, rel, val, f, knd); 
	else
		val_st = exp_atom(query->sql->sa, atom_general(query->sql->sa, &str_type, NULL));
	if (!val_st) 
		return NULL;
	return rel_binop_(query->sql, rel ? *rel : NULL, target_st, val_st, NULL, "pi", card_value);
}

/* cast string too xml */
static sql_exp *
rel_xmltext(sql_query *query, sql_rel **rel, symbol *sym, int f, exp_kind knd) 
{
	dnode *d = sym->data.lval->h;
	symbol *text = d->data.sym;
	sql_exp *text_st;
	sql_type *t = NULL;
	sql_subtype xml_type;

	if ((t = mvc_bind_type(query->sql, "xml")) == NULL)
		return sql_error(query->sql, 02, SQLSTATE(42000) "XML: xml type missing, probably the xml module wasn't added");
	sql_init_subtype(&xml_type, t, 0, 0);
	text_st = rel_value_exp(query, rel, text, f, knd); 
	if (!text_st || (text_st = rel_check_type(query->sql, &xml_type, rel ? *rel : NULL, text_st, type_equal)) == NULL)
		return NULL;
	return text_st;
}

sql_exp *
rel_xml(sql_query *query, sql_rel **rel, symbol *s, int f, exp_kind knd)
{
	mvc *sql = query->sql;
	sql_exp *ret = NULL;

	switch (s->token) {
	case SQL_XMLELEMENT: 
		ret = rel_xmlelement(query, rel, s, f, knd);
		break;
	case SQL_XMLFOREST: 
		ret = rel_xmlforest(query, rel, s, f, knd);
		break;
	case SQL_XMLCOMMENT: 
		ret = rel_xmlcomment(query, rel, s, f, knd);
		break;
	case SQL_XMLATTRIBUTE: 
		ret = rel_xmlattribute(query, rel, s, f, knd);
		break;
	case SQL_XMLCONCAT: 
		ret = rel_xmlconcat(query, rel, s, f, knd);
		break;
	case SQL_XMLDOCUMENT: 
		ret = rel_xmldocument(query, rel, s, f, knd);
		break;
	case SQL_XMLPI: 
		ret = rel_xmlpi(query, rel, s, f, knd);
		break;
	case SQL_XMLTEXT: 
		ret = rel_xmltext(query, rel, s, f, knd);
		break;
	default:
		return sql_error(sql, 01, SQLSTATE(42000) "XML statement unknown symbol(%p)->token = %s", s, token2string(s->token));
	}
	return ret;
}

