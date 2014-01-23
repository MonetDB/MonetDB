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
 * Martin Kersten
 */
#include "monetdb_config.h"
#include "json.h"
#include "mal.h"
#include <mal_instruction.h>
#include <mal_interpreter.h>

// just validate the string according to www.json.org
// A straightforward recursive solution
#define skipblancs(J) for(; *(J); (J)++) \
	if( *(J) != ' ' && *(J) != '\n' && *(J) != '\t' && *(J) != '\f' && *(J) != '\r'  ) break;

#define hex(J) if ( *(J) >='0' && *(J) <='7') (J)++;

#define SEPARATOR ' '

int TYPE_json;

#define JSONlast(J) ((J)->free-1)

/* Internal constructors. */
static int jsonhint= 8;
static JSON *JSONparse(char *j, int silent);

static JSON *JSONnewtree( int size)
{
    JSON *js;
    if ( size == 0)
        size = jsonhint;
    js = (JSON*) GDKzalloc(sizeof(JSON));
    js->elm = (JSONterm*) GDKzalloc( sizeof(JSONterm) * size);
    js->size = size;
    return js;
}

static int JSONnew(JSON *js){
	JSONterm *term;
    if ( js->free == js->size){
        term = (JSONterm*) GDKrealloc(js->elm, sizeof(JSONterm) * (js->size + 8));
		if ( term == NULL){
			js->error = createException(MAL,"json.new",MAL_MALLOC_FAIL);
			return js->free -1;
		}
		else js->elm = term;
		memset( ((char*)term) + sizeof(JSONterm)*js->size, 0, 8 * sizeof(JSONterm));
        js->size += 8;
        if ( jsonhint < js->size)
            jsonhint = js->size;
    }
    return js->free++;
}

/* Delete a JSON structure. */
static void JSONfree(JSON *c)
{
	if( c == 0)
		return;
    GDKfree(c->elm);
    GDKfree(c);
}

int
JSONfromString(str src, int *len, json *j)
{
	ssize_t slen = (ssize_t) strlen(src);

	if ((ssize_t) *len <= slen)
		j = GDKrealloc(*j, slen + 1);
	if ( j == NULL){
		// raise global error
		GDKerror("JSONfromstring failed");
		return 0;
	}
	*len = (int) slen;
	if (GDKstrFromStr((unsigned char *) *j, (const unsigned char *) src, slen) < 0) {
		GDKfree(*j);
		*j = GDKstrdup(str_nil);
		*len = 2;
		return 0;
	}
	return *len;
}

int
JSONtoString(str *s, int *len, json src)
{
	size_t cnt;
	char *c, *dst;

	if (GDK_STRNIL(src)) {
		*s = GDKstrdup("null");
		return 0;
	}
	/* count how much space we need for the output string */
	cnt = 3;					/* two times " plus \0 */
	for (c = src; *c; c++)
		switch (*c) {
		case '"': case '\\': case '\n':
			cnt++;
			/* fall through */
		default:
			cnt++;
			break;
		}

	if (cnt > (size_t) *len) {
		GDKfree(*s);
		*s = (str) GDKmalloc(cnt);
		if (*s == NULL)
			return 0;
		*len = (int) cnt;
	}
	dst = *s;
	*dst++ = '"';
	for (c = src; *c; c++) {
		switch (*c) {
		case '"': case '\\':
			*dst++ = '\\';
			/* fall through */
		default:
			*dst++ = *c;
			break;
		case '\n':
			*dst++ = '\\';
			*dst++ = 'n';
			break;
		}
	}
	*dst++ = '"';
	*dst++ = 0;
	assert((size_t) (dst - *s) == cnt);
	return (int) (cnt - 1);		/* length without \0 */
}

#define tab(D) { int kk; for(kk=0; kk< D * 4; kk++) mnstr_printf(fd," ");}

static void
JSONdumpInternal(JSON *jt, int depth)
{
	int i,idx;
	JSONterm *je;
	stream *fd = GDKout;

	for( idx =0; idx <jt->free; idx++){
	je = jt->elm + idx;
	
	tab(depth); mnstr_printf(fd,"[%d] ",idx);
	switch(je->kind){
	case JSON_OBJECT:
		mnstr_printf(fd,"object ");
		break;
	case JSON_ARRAY:
		mnstr_printf(fd,"array ");
		break;
	case JSON_ELEMENT:
		mnstr_printf(fd,"element ");
		break;
	case JSON_VALUE:
		mnstr_printf(fd,"value ");
		break;
	case JSON_STRING:
		mnstr_printf(fd,"string ");
		break;
	case JSON_NUMBER:
		mnstr_printf(fd,"number ");
		break;
	case JSON_BOOL:
		mnstr_printf(fd,"bool ");
		break;
	case JSON_NULL:
		mnstr_printf(fd,"null ");
		break;
	default:
		mnstr_printf(fd,"unknown %d ",je->kind);
	}
	mnstr_printf(fd,"child %d list ", je->child);
	for( i = je->next; i; i = jt->elm[i].next)
		mnstr_printf(fd,"%d ",i);
	if ( je->name){
		mnstr_printf(fd, "%.*s : ", (int) je->namelen, je->name);
	}
	if ( je->value)
		mnstr_printf(fd, "%.*s", (int) je->valuelen, je->value);
	mnstr_printf(fd,"\n");
	}
}

str
JSONdump(int *ret, json *val)
{
	JSON *jt = JSONparse(*val, FALSE);

	(void) ret;
	JSONdumpInternal(jt,0);
	return MAL_SUCCEED;
}


str
JSONjson2str(str *ret, json *j)
{
	char *s = *j, *c;
	if (*s == '"')
		s++;
	s = GDKstrdup(s);
	c = s + strlen(s)-1;
	if ( *c == '"')
		*c = 0;
	*ret = s;
	return MAL_SUCCEED;
}

str
JSONstr2json(json *ret, str *j)
{
	JSON *jt = JSONparse(*j, FALSE);
	str msg = MAL_SUCCEED;

	if (jt && jt->error) {
		*ret = 0;
		msg = jt->error;
		jt->error = 0;
		JSONfree(jt);
		return msg;
	}
	if( jt) 
		JSONfree(jt);
	*ret = GDKstrdup(*j);
	return MAL_SUCCEED;
}

str
JSONisvalid(bit *ret, json *j)
{
	JSON *jt = JSONparse(*j, FALSE);

	if( jt){
		*ret = jt->error == MAL_SUCCEED;
		JSONfree(jt);
	}
	return MAL_SUCCEED;
}

str
JSONisobject(bit *ret, json *js)
{
	char *j= *js;
	skipblancs(j);
	*ret = *j == '{';
	return MAL_SUCCEED;
}

str
JSONisarray(bit *ret, json *js)
{
	char *j= *js;
	skipblancs(j);
	*ret = *j == '[';
	return MAL_SUCCEED;
}

str
JSONprelude(int *ret)
{
	(void) ret;
	TYPE_json = ATOMindex("json");
	return MAL_SUCCEED;
}

static void
JSONappend(JSON *jt, int idx, int nxt){
	int chld;

	if( jt->elm[nxt].kind == JSON_OBJECT || jt->elm[nxt].kind == JSON_ARRAY){
		chld = JSONnew(jt);
		jt->elm[chld].kind = jt->elm[nxt].kind;
		jt->elm[chld].name = jt->elm[nxt].name;
		jt->elm[chld].namelen = jt->elm[nxt].namelen;
		jt->elm[chld].value = jt->elm[nxt].value;
		jt->elm[chld].valuelen = jt->elm[nxt].valuelen;
		jt->elm[chld].child = jt->elm[nxt].child;
		jt->elm[chld].next = jt->elm[nxt].next;
		jt->elm[chld].tail = jt->elm[nxt].tail;
		jt->elm[chld].child = nxt;

		jt->elm[nxt].child =0;
		jt->elm[nxt].next =0;
		jt->elm[nxt].tail =0;
		nxt = chld;
	} 
	if ( jt->elm[idx].next == 0)
		jt->elm[idx].next = jt->elm[idx].tail = nxt;
	else{
		jt->elm[jt->elm[idx].tail].next = nxt;
		jt->elm[idx].tail = nxt;
	}
}

/*
 * The JSON filter operation takes a path expression which is purposely kept simple,
 * It provides step (.), multistep (..) and indexed ([nr]) access to the JSON elements.
 * A wildcard * can be used as placeholder for a step identifier.
 *
 * A path expression is always validated upfront and can only be applied to valid json strings.
 * Path samples:
 * .store.book
 * .store.book[0]
 * .store.book.author
 * ..author
 */
#define MAXTERMS 256
#define ROOT_STEP 0
#define CHILD_STEP 1
#define INDEX_STEP 2
#define ANY_STEP 3
#define END_STEP 4

typedef struct {
	int token;
	char *name;
	size_t namelen;
	int index;
	int first, last;
} pattern;

static str
JSONcompile(char *expr, pattern terms[])
{
	int t = 0;
	char *s, *beg;

	for (s = expr; *s; s++ ) {
		terms[t].token = CHILD_STEP;
		terms[t].index = INT_MAX;
		terms[t].first = INT_MAX;
		terms[t].last = INT_MAX;
		if ( *s == '$'){
			if( t )
				throw(MAL,"json.compile","Root node must be first");
			terms[t].token = ROOT_STEP;
			if ( !(*(s+1) == '.' || *(s+1) =='[' || *s == 0) )
				throw(MAL,"json.compile","Root node must be first");
		}
		if (*s == '.' && *(s + 1) == '.') {
			terms[t].token = ANY_STEP;
			s += 2;
			if ( *s == '.' )
				throw(MAL,"json.compile","Step identifier expected");
		} else if (*s == '.')
			s++;

		// child step
		if (*s != '[') {
			for (beg = s; *s; s++)
				if (*s == '.' || *s == '[')
					break;
			terms[t].name = GDKzalloc(s - beg + 1);
			terms[t].namelen = s - beg;
			strncpy(terms[t].name, beg, s - beg);
			if (*s == '.')
				s--;
			if (*s == 0){
				t++;
				break;
			}
		} 
		if ( *s == '['){
		// array step
			s++;
			skipblancs(s);
			if( *s != '*') {
				if ( *s >='0' && *s <='9') {
					terms[t].index = atoi(s);
					terms[t].first = terms[t].last = atoi(s);
				} else
					throw(MAL, "json.path", "'*' or digit expected");
			} 
			for (; *s; s++)
				if (*s == ']')
					break;
			if (*s == 0){
				t++;
				break;
			}
			if (*s != ']')
				throw(MAL, "json.path", "] expected");
		} 
		if (++t == MAXTERMS)
			throw(MAL, "json.path", "too many terms");
	}
	if (t >= MAXTERMS-1)
		throw(MAL, "json.path", "too many terms");
	terms[t].token = END_STEP;
	return MAL_SUCCEED;
}

static str
JSONgetValue(JSON *jt, int idx)
{
	str s;
	if ( jt->elm[idx].valuelen == 0)
		return GDKstrdup(str_nil);
	if ( strncmp(jt->elm[idx].value,"null",4)== 0)
		return GDKstrdup(str_nil);
	s= GDKzalloc(jt->elm[idx].valuelen+1);
	if( s )
		strncpy(s, jt->elm[idx].value, jt->elm[idx].valuelen);
	return s;
}

static str
JSONglue(str res, str r, char sep)
{
	size_t len, l;
	str n;
	if( r== 0 || *r == 0)
		return res;
	len =  r?strlen(r):0;
	if ( res == 0)
		res = GDKstrdup(r);
	else {
		l= strlen(res);
		n = GDKzalloc(l + len  + 3);
		strcpy(n,res);
		if( sep){
			n[l] = ',';
			strncpy(n+l+1, r, len);
			n[l+1+len]= 0;
		} else {
			strncpy(n+l, r, len);
			n[l+len]= 0;
		}
		res =n;
	}
	if(r)
		GDKfree(r);
	return res;
}

static str
JSONmatch(JSON *jt, int ji, pattern *terms, int ti)
{
	str r= NULL, res = NULL;
	int i,match;
	int cnt ;

	if ( terms[ti].token == ROOT_STEP){
			if( terms[ti+1].token == END_STEP)
				return JSONgetValue(jt,0);
		ti++;
	}

	switch(jt->elm[ji].kind){
	case JSON_ARRAY:
		if( terms[ti].name != 0 && terms[ti].token != ANY_STEP){
			if( terms[ti].token == END_STEP)
				r = JSONgetValue(jt, ji);
			return res;
		}
		cnt = 0;
		for( i=jt->elm[ji].next ; i && cnt >= 0; i = jt->elm[i].next, cnt++){
			if( terms[ti].index == INT_MAX || ( cnt >= terms[ti].first && cnt <= terms[ti].last)){
				if ( terms[ti].token == ANY_STEP){
					if( jt->elm[i].child)
						r = JSONmatch(jt, jt->elm[i].child, terms, ti);
					else r = 0;
				} else
				if ( terms[ti+1].token == END_STEP ){
					if ( jt->elm[i].kind == JSON_VALUE || jt->elm[i].kind == JSON_VALUE)
						r = JSONgetValue(jt, jt->elm[i].child);
					else
						r = JSONgetValue(jt, i);
				} else 
					r = JSONmatch(jt, jt->elm[i].child, terms, ti+1);
				res = JSONglue(res,r,',');
			}
		}
		break;
	case JSON_OBJECT:
		cnt = 0;
		for( i=jt->elm[ji].next ; i && cnt >= 0; i = jt->elm[i].next){
			// check the element label
			match =(terms[ti].name && jt->elm[i].valuelen == terms[ti].namelen && strncmp(terms[ti].name, jt->elm[i].value,terms[ti].namelen) == 0) || terms[ti].name == 0 || terms[ti].name[0] == '*';
			if ( match){
				if( terms[ti].index == INT_MAX || ( cnt >= terms[ti].first && cnt <= terms[ti].last)){
					if ( terms[ti+1].token == END_STEP)
						r = JSONgetValue(jt, jt->elm[i].child);
					else
						r = JSONmatch(jt, jt->elm[i].child, terms, ti+1);
					res = JSONglue(res,r,',');
				}
				cnt ++;
			} else
			if( terms[ti].token == ANY_STEP  && jt->elm[i].child){
				r = JSONmatch(jt, jt->elm[i].child, terms, ti);
				res = JSONglue(res,r,',');
				cnt++;
			} 
		}
	}
	return res;
}

static str
JSONfilterInternal(json *ret, json *js, str *expr, str other)
{
	pattern terms[MAXTERMS];
	JSON *jt;
	str j = *js, msg = MAL_SUCCEED, s;
	json result = 0;
	size_t l;

	(void) other;
	if ( strNil(j)){
		*ret = GDKstrdup(j);
		return MAL_SUCCEED;
	}
	memset((char *) terms, 0, MAXTERMS * sizeof(pattern));
	msg = JSONcompile(*expr, terms);
	if (msg)
		return msg;
	jt = JSONparse(j, TRUE);
	if ( jt->error){
		msg = jt->error;
		JSONfree(jt);
		return msg;
	}

	// unwrap the outer brackets before matching
	s = JSONmatch(jt, 0, terms, 0);
	if ( s ) {
		result = (char *) GDKzalloc(l = strlen(s) + 4);
		if (result )
			snprintf(result,l,"[%s]",s);
		else
			msg = createException(MAL, "json.path", MAL_MALLOC_FAIL);
	} else result = (char*) GDKstrdup("[]");
	for (l = 0; terms[l].token; l++)
		if (terms[l].name)
			GDKfree(terms[l].name);
	JSONfree(jt);
	*ret = result;
	return msg;
}


static str
JSONstringParser(char *j, char **next, int silent)
{
	if (*j == '"')
		j++;
	for (; *j; j++)
		switch (*j) {
		case '\\':
			// parse all escapes
			j++;
			switch (*j) {
			case '"': case '\\': case '/': case 'b': case 'f':
			case 'n': case 'r': case 't':
				break;
			case 'u':
				j++;
				hex(j); hex(j); hex(j); hex(j);
				break;
			default:
				if ( silent) {
					*next = j;
					return MAL_SUCCEED;
				}
				throw(MAL, "json.parser", "illegal escape char");
			}
			break;
		case '"':
			j++;
			*next = j;
			return MAL_SUCCEED;
		}
	if( !silent) 
		throw(MAL, "json.parser", "Nonterminated string");
	return MAL_SUCCEED;
}

static str
JSONnumberParser(char *j, char **next, int silent)
{
	char *backup= j;
	
	if (*j == '-')
		j++;
	skipblancs(j);
	if (*j < '0' || *j > '9'){
		if(!silent)
			throw(MAL, "json.parser", "Number expected");
		return MAL_SUCCEED;
	}
	if (*j == '0' && *(j + 1) != '.'){
		if(! silent)
			throw(MAL, "json.parser", "Decimal expected");
		return MAL_SUCCEED;
	}
	for (; *j; j++)
		if (*j < '0' || *j > '9')
			break;
	backup = j;
	skipblancs(j);
	if (*j == '.') {
		j++;
		skipblancs(j);
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
		backup = j;
	} else j = backup;
	skipblancs(j);
	if (*j == 'e' || *j == 'E') {
		j++;
		skipblancs(j);
		if (*j == '-')
			j++;
		skipblancs(j);
		for (; *j; j++)
			if (*j < '0' || *j > '9')
				break;
	} else j = backup;
	*next = j;
	return MAL_SUCCEED;
}

static int
JSONtoken(JSON *jt, char *j, char **next, int silent)
{
	str msg;
	int nxt, idx = JSONnew(jt);

	skipblancs(j);
	switch (*j) {
	case '{':
		jt->elm[idx].kind= JSON_OBJECT;
		jt->elm[idx].value = j;
		j++;
		while(*j ) {
			skipblancs(j);
			if ( *j == '}')
				break;
			nxt = JSONtoken(jt,j,next, silent);
			if (jt->elm[nxt].kind != JSON_ELEMENT  ){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : element expected");
				break;
			}
			JSONappend(jt,idx,nxt);
			j= *next;
			skipblancs(j);
			if ( *j == '}')
				break;
			if ( *j != '}' && *j != ','){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : ','  or '}' expected");
				break;
			}
			j++;
		} 
		if( *j != '}'){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : '}' expected");
		} else j++;
		*next = j;
		jt->elm[idx].valuelen = *next - jt->elm[idx].value;
		return idx;
	case '[':
		jt->elm[idx].kind= JSON_ARRAY;
		jt->elm[idx].value = j;
		j++;
		while(*j ) {
			skipblancs(j);
			if ( *j == ']')
				break;
			nxt = JSONtoken(jt,j,next, silent);
			switch( jt->elm[nxt].kind){
			case JSON_ELEMENT:{
				int k = JSONnew(jt);
				jt->elm[k].kind = JSON_OBJECT;
				jt->elm[k].child = nxt;
				nxt = k;
				} 
			case JSON_OBJECT:
			case JSON_ARRAY:
				if ( jt->elm[nxt].kind == JSON_OBJECT || jt->elm[nxt].kind == JSON_ARRAY){
					int k = JSONnew(jt);
					JSONappend(jt,idx,k);
					jt->elm[k].kind = JSON_VALUE;
					jt->elm[k].child = nxt;
				}
				break;
			default:
				JSONappend(jt,idx,nxt);
			}
			j= *next;
			skipblancs(j);
			if ( *j == ']')
				break;
			if ( jt->elm[nxt].kind == JSON_ELEMENT){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : Array value expected");
			}
			if ( *j != ']' && *j != ','){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : ','  or ']' expected");
				break;
			}
			j++;
			skipblancs(j);
		} 
		if( *j != ']'){
				if ( !silent)
					jt->error = createException(MAL, "json.parser", "Syntax error : ']' expected");
		} else j++;
		*next = j;
		jt->elm[idx].valuelen = *next - jt->elm[idx].value;
		return idx;
	case '"':
		JSONstringParser(j + 1, next, silent);
		jt->elm[idx].kind = JSON_STRING;
		jt->elm[idx].value = j;
		jt->elm[idx].valuelen = *next -j;
		j = *next;
		skipblancs(j);
		if ( * j == ':'){
			j++;
			skipblancs(j);
			jt->elm[idx].kind = JSON_ELEMENT;
			nxt= JSONtoken(jt,j,next,silent);
			jt->elm[idx].child = nxt;
			jt->elm[idx].value++;
			jt->elm[idx].valuelen -= 2;
		}
		return idx;
	case 'n':
		if (strncmp("null", j, 4) == 0) {
			*next = j+4;
			jt->elm[idx].kind = JSON_NULL;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		if( !silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: NULL expected");
		return idx;
	case 't':
		if (strncmp("true", j, 4) == 0) {
			*next = j+4;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 4;
			return idx;
		}
		if( !silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: True expected");
		return idx;
	case 'f':
		if (strncmp("false", j, 5) == 0) {
			*next = j+5;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = 5;
			return idx;
		}
		if( !silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: False expected");
		return idx;
	default:
		if (*j == '-' || (*j >= '0' && *j <= '9')){
			msg =JSONnumberParser(j, next,silent);
			if ( !silent)
				jt->error = msg;
			jt->elm[idx].kind = JSON_NUMBER;
			jt->elm[idx].value = j;
			jt->elm[idx].valuelen = *next -j;
			return idx;
		}
		if( !silent)
			jt->error = createException(MAL, "json.parser", "Syntax error: value expected");
	}
	return idx;
}


static JSON*
JSONparse(char *j, int silent)
{
	JSON *jt= JSONnewtree(0);

	JSONtoken(jt,j, &j, silent);
	skipblancs(j);
	if( *j ){
		if( !silent)
			jt->error = createException(MAL, "json.parser", "Syntax error");
	}
	return jt;
}

str
JSONlength(int *ret, json *j)
{
	int i, cnt = 0;
	JSON *jt = JSONparse(*j, TRUE);
	
	for(i= jt->elm[0].next; i; i = jt->elm[i].next)
		cnt++;
	*ret = cnt;
	return MAL_SUCCEED;
}

str
JSONfilterArray(json *ret, json *js, int *index)
{
	
	char expr[BUFSIZ], *s = expr;
	snprintf(expr,BUFSIZ,"[%d]",*index);
	return JSONfilterInternal(ret, js, &s, 0);
}

str
JSONfilterArrayDefault(json *ret, json *js, int *index, str *other)
{
	char expr[BUFSIZ], *s = expr;
	snprintf(expr,BUFSIZ,"[%d]",*index);
	return JSONfilterInternal(ret, js, &s, *other);
}

str
JSONfilter(json *ret, json *js, str *expr)
{
	return JSONfilterInternal(ret,js,expr, 0);
}

// glue all values together with an optional separator
// The json string should be valid

static char * 
JSONplaintext(char *r, JSON *jt, int idx, char sep)
{
	int i;
	size_t j;
	switch(jt->elm[idx].kind){
	case JSON_OBJECT:
		for( i= jt->elm[idx].next; i; i= jt->elm[i].next)
		if( jt->elm[i].child)
			r = JSONplaintext(r, jt,jt->elm[i].child,sep );
		break;
	case JSON_ARRAY:
		for( i= jt->elm[idx].next; i; i= jt->elm[i].next)
			r = JSONplaintext(r, jt,i,sep);
		break;
	case JSON_ELEMENT:
	case JSON_VALUE:
		if( jt->elm[idx].child)
			r = JSONplaintext(r, jt,jt->elm[idx].child,sep);
		break;
	case JSON_STRING:
		for(j=1; j< jt->elm[idx].valuelen-1; j++){
			if ( jt->elm[idx].value[j] == '\\')
				*r = jt->elm[idx].value[++j];
			else
				*r = jt->elm[idx].value[j];
			r++;
		}
		if(sep)
			*r++= sep;
		break;
	default:
		for(j=0; j< jt->elm[idx].valuelen; j++){
			*r = jt->elm[idx].value[j];
			r++;
		}
		if(sep)
			*r++= sep;
	}
	*r = 0;
	return r;
}

str
JSONjson2text(str *ret, json *js)
{	
	JSON *jt;
	size_t l;
	str s;

	jt = JSONparse(*js,0);

	s = GDKzalloc(strlen(*js));
	JSONplaintext(s,jt,0,' ');
	l = strlen(s);
	if ( l) s[l-1]= 0;
	*ret = s;
	return MAL_SUCCEED;
}

str
JSONjson2textSeparator(str *ret, json *js, str *sep)
{	
	JSON *jt;
	size_t l;
	str s;

	jt = JSONparse(*js,0);

	s = GDKzalloc(strlen(*js));
	JSONplaintext(s,jt,0,**sep);
	l = strlen(s);
	if ( l) s[l-1]= 0;
	*ret = s;
	return MAL_SUCCEED;
}

static void
JSONunfoldContainer(JSON *jt, int idx, BAT *bo, BAT *bk, BAT *bv, oid *o)
{
	int i, last;
	int cnt=0;
	char *r;

	last = jt->elm[idx].tail;
	if( jt->elm[idx].kind == JSON_OBJECT )
	for(i= jt->elm[idx].next; i; i = jt->elm[i].next){
		r= JSONgetValue(jt,i);
		BUNappend(bk, r, FALSE);
		GDKfree(r); r= JSONgetValue(jt,jt->elm[i].child);
		BUNappend(bv, r, FALSE);
		if( bo)
			BUNappend(bo, o, FALSE);
		(*o)++;
		GDKfree(r);
		if ( i == last ) break;
	} 
	else
	if( jt->elm[idx].kind == JSON_ARRAY)
	for(i= jt->elm[idx].next; i; i = jt->elm[i].next){
		r = GDKstrdup(str_nil);
		BUNappend(bk, r, FALSE);
		if ( jt->elm[i].kind == JSON_VALUE)
			r= JSONgetValue(jt,jt->elm[i].child);
		else
			r= JSONgetValue(jt,i);
		BUNappend(bv, r, FALSE);
		if( bo)
			BUNappend(bo, o, FALSE);
		(*o)++;
		cnt++;
		GDKfree(r);
		if ( i == last ) break;
	}
}

static str
JSONunfoldInternal(int *od, int *key, int *val, json *js)
{
	BAT *bo = NULL, *bk, *bv;
	oid o= 0;
	str msg = MAL_SUCCEED;

	JSON *jt = JSONparse(*js,TRUE);

	bk = BATnew(TYPE_void, TYPE_str, 64);
	if (bk == NULL){
		JSONfree(jt);
		throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
	}
	BATseqbase(bk, 0);
	bk->hsorted = 1;
	bk->hrevsorted = 0;
	bk->H->nonil = 1;
	bk->tsorted = 1;
	bk->trevsorted = 0;
	bk->T->nonil = 1;

	if( od){
		bo = BATnew(TYPE_void, TYPE_oid, 64);
		if (bo == NULL){
			BBPreleaseref(bk->batCacheid);
			JSONfree(jt);
			throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
		}
		BATseqbase(bo, 0);
		bo->hsorted = 1;
		bo->hrevsorted = 0;
		bo->H->nonil = 1;
		bo->tsorted = 1;
		bo->trevsorted = 0;
		bo->T->nonil = 1;
	}

	bv = BATnew(TYPE_void, TYPE_json, 64);
	if (bv == NULL) {
		JSONfree(jt);
		if( od)
			BBPreleaseref(bo->batCacheid);
		BBPreleaseref(bk->batCacheid);
		throw(MAL, "json.unfold", MAL_MALLOC_FAIL);
	}
	BATseqbase(bv, 0);
	bv->hsorted = 1;
	bv->hrevsorted = 0;
	bv->H->nonil = 1;
	bv->tsorted = 1;
	bv->trevsorted = 0;
	bv->T->nonil = 1;

	if( jt->elm[0].kind == JSON_ARRAY || jt->elm[0].kind == JSON_OBJECT)
		JSONunfoldContainer(jt, 0, (od?bo:0), bk, bv, &o);
	else
		msg = createException(MAL,"json.unfold","JSON object or array expected");
	JSONfree(jt);
	BBPkeepref(*key = bk->batCacheid);
	BBPkeepref(*val = bv->batCacheid);
	if( od)
		BBPkeepref(*od = bo->batCacheid);
	return msg;
}



str
JSONkeyTable(int *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt= JSONparse(*js, TRUE); // already validated
	bn = BATnew(TYPE_void, TYPE_str, 64);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	for( i = jt->elm[0].next; i; i= jt->elm[i].next){
		r = JSONgetValue(jt,i);
		BUNappend(bn, r, FALSE);
		GDKfree(r);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}
str
JSONkeyArray(json *ret, json *js)
{
	char *result = NULL;
	str r;
	int i;
	JSON *jt;

	jt= JSONparse(*js,TRUE); // already validated

	if ( jt->elm[0].kind == JSON_OBJECT)
		for( i = jt->elm[0].next; i; i= jt->elm[i].next){
			r= GDKzalloc(jt->elm[i].valuelen+3);
			if( jt->elm[i].valuelen )
				strncpy(r, jt->elm[i].value-1, jt->elm[i].valuelen+2);
			result = JSONglue(result,r,',');
		}
	else
		throw(MAL,"json.keyarray","Object expected");
	r= (char*) GDKstrdup("[");
	result = JSONglue(r,result,0);
	r= (char*) GDKstrdup("]");
	*ret = JSONglue(result,r,0);
	return MAL_SUCCEED;
}


str
JSONvalueTable(int *ret, json *js)
{
	BAT *bn;
	char *r;
	int i;
	JSON *jt;

	jt= JSONparse(*js, TRUE); // already validated
	bn = BATnew(TYPE_void, TYPE_str, 64);
	BATseqbase(bn, 0);
	bn->hsorted = 1;
	bn->hrevsorted = 0;
	bn->H->nonil = 1;
	bn->tsorted = 1;
	bn->trevsorted = 0;
	bn->T->nonil = 1;

	for( i = jt->elm[0].next; i; i= jt->elm[i].next){
		if ( jt->elm[i].kind == JSON_ELEMENT)
			r = JSONgetValue(jt,jt->elm[i].child);
		else
			r = JSONgetValue(jt,i);
		BUNappend(bn, r, FALSE);
		GDKfree(r);
	}
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
JSONvalueArray(json *ret, json *js)
{
	char *result = NULL;
	str r;
	int i;
	JSON *jt;

	jt= JSONparse(*js,TRUE); // already validated

	if ( jt->elm[0].kind == JSON_OBJECT)
		for( i = jt->elm[0].next; i; i= jt->elm[i].next){
			r = JSONgetValue(jt,jt->elm[i].child);
			result = JSONglue(result,r,',');
		}
	else throw(MAL,"json.valuearray","Object expected");
	r= (char*) GDKstrdup("[");
	result = JSONglue(r,result,0);
	r= (char*) GDKstrdup("]");
	*ret = JSONglue(result,r,0);
	return MAL_SUCCEED;
}

static BAT **
JSONargumentlist(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int i, error = 0, error2 = 0, bats = 0;
	BUN cnt = 0;
	BAT **bl;

	bl = (BAT **) GDKzalloc(sizeof(*bl) * pci->argc);
	for (i = pci->retc; i < pci->argc; i++)
		if (isaBatType(getArgType(mb, pci, i))) {
			bats++;
			bl[i] = BATdescriptor(stk->stk[getArg(pci, i)].val.bval);
			if (bl[i] == 0)
				error++;
			error2 |= (cnt > 0 && BATcount(bl[i]) != cnt);
			cnt = BATcount(bl[i]);
		}
	if (error + error2 || bats == 0) {
		GDKfree(bl);
		bl = 0;
	}
	return bl;
}

static str
JSONrenderRowObject(BAT **bl, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *name = 0, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '{';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	for (i = pci->retc; i < pci->argc; i += 2) {
		name = stk->stk[getArg(pci, i)].val.sval;
		bi = bat_iterator(bl[i + 1]);
		p = BUNtail(bi, BUNfirst(bl[i + 1]) + idx);
		tpe = getTailType(getArgType(mb, pci, i + 1));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(name) + strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim += BUFSIZ);
		snprintf(row + len, lim - len, "\"%s\":%s,", name, val);
		len += l + 4;
	}
	if (row[1])
		row[len - 1] = '}';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	GDKfree(val);
	return row;
}

str
JSONrenderobject(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT **bl;
	char *result, *row;
	int i;
	size_t len, lim, l;
	str *ret;
	BUN j, cnt;

	(void) cntxt;
	bl = JSONargumentlist(mb, stk, pci);
	if (bl == 0)
		throw(MAL, "json.renderobject", "Non-aligned BAT sizes");
	for (i = pci->retc; i < pci->argc; i += 2)
		if (getArgType(mb, pci, i) != TYPE_str)
			throw(MAL, "json.renderobject", "Keys missing");

	cnt = BATcount(bl[pci->retc + 1]);
	result = (char *) GDKmalloc(lim = BUFSIZ);
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		row = JSONrenderRowObject(bl, mb, stk, pci, j);
		l = strlen(row);
		while (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = (str *) getArgReference(stk, pci, 0);
	*ret = result;
	return MAL_SUCCEED;
}

static str
JSONrenderRowArray(BAT **bl, MalBlkPtr mb, InstrPtr pci, BUN idx)
{
	int i, tpe;
	char *row, *val = 0;
	size_t len, lim, l;
	void *p;
	BATiter bi;

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '[';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	for (i = pci->retc; i < pci->argc; i++) {
		bi = bat_iterator(bl[i]);
		p = BUNtail(bi, BUNfirst(bl[i]) + idx);
		tpe = getTailType(getArgType(mb, pci, i));
		ATOMformat(tpe, p, &val);
		if (strncmp(val, "nil", 3) == 0)
			strcpy(val, "null");
		l = strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim += BUFSIZ);
		snprintf(row + len, lim - len, "%s,", val);
		len += l + 1;
	}
	if (row[1])
		row[len - 1] = ']';
	else {
		row[1] = '}';
		row[2] = 0;
	}
	GDKfree(val);
	return row;
}

str
JSONrenderarray(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	BAT **bl;
	char *result, *row;
	size_t len, lim, l;
	str *ret;
	BUN j, cnt;

	(void) cntxt;
	bl = JSONargumentlist(mb, stk, pci);
	if (bl == 0)
		throw(MAL, "json.renderrray", "Non-aligned BAT sizes");

	cnt = BATcount(bl[pci->retc + 1]);
	result = (char *) GDKmalloc(lim = BUFSIZ);
	result[0] = '[';
	result[1] = 0;
	len = 1;

	for (j = 0; j < cnt; j++) {
		row = JSONrenderRowArray(bl, mb, pci, j);
		l = strlen(row);
		while (l + 2 > lim - len)
			row = (char *) GDKrealloc(row, lim = cnt * l <= lim ? cnt * l : lim + BUFSIZ);
		strcpy(result + len, row);
		GDKfree(row);
		len += l;
		result[len++] = ',';
		result[len] = 0;
	}
	result[len - 1] = ']';
	ret = (str *) getArgReference(stk, pci, 0);
	*ret = result;
	return MAL_SUCCEED;
}

static str
JSONfoldKeyValue(str *ret, int *id, int *key, int *values)
{
	BAT *bo = 0, *bk = 0, *bv;
	BATiter boi, bki, bvi;
	int tpe;
	char *row, *val = 0, *nme = 0;
	BUN i, cnt;
	size_t len, lim, l;
	void *p;
	oid o = 0;;

	if (key) {
		bk = BATdescriptor(*key);
		if (bk == NULL){
			*ret = GDKstrdup(str_nil);
			throw(MAL, "json.fold", RUNTIME_OBJECT_MISSING);
		}
	}

	bv = BATdescriptor(*values);
	if (bv == NULL) {
		if (bk)
			BBPreleaseref(bk->batCacheid);
		*ret = GDKstrdup(str_nil);
		throw(MAL, "json.fold", RUNTIME_OBJECT_MISSING);
	}
	tpe = bv->ttype;
	cnt = BATcount(bv);
	if (bk)
		bki = bat_iterator(bk);
	bvi = bat_iterator(bv);
	if (id) {
		bo = BATdescriptor(*id);
		if (bo == NULL) {
			if (bk)
				BBPreleaseref(bk->batCacheid);
			BBPreleaseref(bv->batCacheid);
			throw(MAL, "json.nest", RUNTIME_OBJECT_MISSING);
		}
	}

	row = (char *) GDKmalloc(lim = BUFSIZ);
	row[0] = '[';
	row[1] = 0;
	len = 1;
	val = (char *) GDKmalloc(BUFSIZ);
	if (id) {
		boi = bat_iterator(bo);
		o = *(oid *) BUNtail(boi, BUNfirst(bo));
	}
	if (bk)
		bki = bat_iterator(bk);
	bvi = bat_iterator(bv);

	for (i = 0; i < cnt; i++) {
		if (id &&bk) {
			p = BUNtail(boi, BUNfirst(bo) + i);
			if (*(oid *) p != o) {
				snprintf(row + len , lim - len, ", ");
				len += 2;
				o = *(oid *) p;
			}
		}

		if (bk) {
			nme = (str) BUNtail(bki, BUNfirst(bk) + i);
			l = strlen(nme);
			while (l + 3 > lim - len)
				row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);
			if ( row == NULL){
				*ret = GDKstrdup(str_nil);
				if (bo)
					BBPreleaseref(bo->batCacheid);
				if (bk)
					BBPreleaseref(bk->batCacheid);
				BBPreleaseref(bv->batCacheid);
				throw(MAL, "json.fold", MAL_MALLOC_FAIL);
			}
			if( strcmp(nme, str_nil) ){
				snprintf(row + len, lim - len, "\"%s\":", nme);
				len += l + 3;
			} 
		}

		bvi = bat_iterator(bv);
		p = BUNtail(bvi, BUNfirst(bv) + i);
		if ( tpe == TYPE_json)
			val = p;
		else{
			ATOMformat(tpe, p, &val);
			if (strncmp(val, "nil", 3) == 0)
				strcpy(val, "null");
		}
		l = strlen(val);
		while (l > lim - len)
			row = (char *) GDKrealloc(row, lim = (lim / (i + 1)) * cnt + BUFSIZ + l + 3);

		if ( row == NULL){
			if (bo)
				BBPreleaseref(bo->batCacheid);
			if (bk)
				BBPreleaseref(bk->batCacheid);
			BBPreleaseref(bv->batCacheid);
			*ret = GDKstrdup(str_nil);
			throw(MAL, "json.fold", MAL_MALLOC_FAIL);
		}
		strncpy(row + len, val, l);
		len += l;
		row[len++] = ',';
		row[len] = 0;
	}
	if (row[1]) {
		row[len - 1] = ']';
		row[len] = 0;
	} else {
		row[1] = ']';
		row[2] = 0;
	}
	if ( tpe != TYPE_json)
		GDKfree(val);
	if (bo)
		BBPreleaseref(bo->batCacheid);
	if (bk)
		BBPreleaseref(bk->batCacheid);
	BBPreleaseref(bv->batCacheid);
	*ret = row;
	return MAL_SUCCEED;
}

str
JSONunfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *id = 0, *key = 0, *val = 0;
	json *js;

	(void) cntxt;
	(void) mb;

	if (pci->retc == 1) {
		val = (int *) getArgReference(stk, pci, 0);
	} else if (pci->retc == 2) {
		id = 0;
		key = (int *) getArgReference(stk, pci, 0);
		val = (int *) getArgReference(stk, pci, 1);
	} else if (pci->retc == 3) {
		id = (int *) getArgReference(stk, pci, 0);
		key = (int *) getArgReference(stk, pci, 1);
		val = (int *) getArgReference(stk, pci, 2);
	}
	js = (json *) getArgReference(stk, pci, pci->retc);
	return JSONunfoldInternal(id, key, val, js);
}

str
JSONfold(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *id = 0, *key = 0, *val = 0;
	str *ret;

	(void) cntxt;
	(void) mb;

	if (pci->argc - pci->retc == 1) {
		val = (int *) getArgReference(stk, pci, 1);
	} else if (pci->argc - pci->retc == 2) {
		id = 0;
		key = (int *) getArgReference(stk, pci, 1);
		val = (int *) getArgReference(stk, pci, 2);
	} else if (pci->argc - pci->retc == 3) {
		id = (int *) getArgReference(stk, pci, 1);
		key = (int *) getArgReference(stk, pci, 2);
		val = (int *) getArgReference(stk, pci, 3);
	}
	ret = (str *) getArgReference(stk, pci, 0);
	return JSONfoldKeyValue(ret, id, key, val);
}

