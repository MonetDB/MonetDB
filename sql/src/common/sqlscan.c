
#include <unistd.h>
#include "symbol.h"
#include "sqlscan.h"
#include "sql.h"
#include <mem.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <stream.h>
#include <statement.h>

void sql_statement_add( context *lc, char *token ){
	int len = strlen(token); 
	if (len > lc->sqlsize){
		snprintf(lc->errstr, ERRSIZE, "%s(%d) token to long: %s", 
			lc->filename, lc->lineno, token);
		return;
	}
	if ((lc->sqllen + len + 1) > lc->sqlsize){
		lc->sqlsize *= 2;
		lc->sql = RENEW_ARRAY( char, lc->sql, lc->sqlsize);
	}
	strncpy(lc->sql+lc->sqllen-1, token, len);
	lc->sqllen += len + 1;
	lc->sql[lc->sqllen-2] = ' ';
	lc->sql[lc->sqllen-1] = '\0';
}

#define HASH_SIZE 128
#define HASH_MASK (HASH_SIZE-1)

typedef struct keyword {
	char *keyword;
	int len;
	int  token;
	struct keyword *next;
} keyword;

static int keywords_init_done = 0;
static keyword *keywords[HASH_SIZE];

static
int keyword_key(char *k, int *l){
	char *s = k;
	int h = 0;
	while(*k){
		h += tolower(*k)-'a';
		k++;
	}
	*l = k-s;
	return (h<0)?-h:h;
}

void keywords_insert( char *k, int token){
	keyword *kw = NEW(keyword);
	int len = 0;
	int bucket = keyword_key(k,&len)&HASH_MASK;
	kw->keyword = toLower(_strdup(k));
	kw->len = len;
	kw->token = token;
	kw->next = keywords[bucket];
	keywords[bucket] = kw;
}

void init_keywords(){
	int i;

	if (keywords_init_done) return;
	keywords_init_done = 1;

	for(i=0;i<HASH_SIZE; i++) keywords[i] = NULL;

	keywords_insert("ALL", ALL );
	keywords_insert("ALTER", ALTER );
	keywords_insert("ADD", ADD );
	keywords_insert("AND", AND );
	keywords_insert("AVG", AMMSC );
	keywords_insert("MIN", AMMSC );
	keywords_insert("MAX", AMMSC );
	keywords_insert("SUM", AMMSC );
	keywords_insert("COUNT", AMMSC );
	keywords_insert("ANY", ANY );
	keywords_insert("AS", AS );
	keywords_insert("ASC", ASC );
	keywords_insert("AUTHORIZATION", AUTHORIZATION );
	keywords_insert("BETWEEN", BETWEEN );
	keywords_insert("SYMMETRIC", SYMMETRIC );
	keywords_insert("ASYMMETRIC", ASYMMETRIC );
	keywords_insert("BY", BY );
	keywords_insert("CASCADE", CASCADE );
	keywords_insert("CAST", CAST );
	keywords_insert("CHAR", CHARACTER );
	keywords_insert("CHARACTER", CHARACTER );
	keywords_insert("CHECK", CHECK );
	/*
	keywords_insert("CLOSE", CLOSE );
	*/
	keywords_insert("COMMIT", COMMIT );
	keywords_insert("CONSTRAINT", CONSTRAINT );
	/*
	keywords_insert("CONTINUE", CONTINUE );
	*/
	keywords_insert("CREATE", CREATE );
	keywords_insert("CROSS", CROSS );
	/*
	keywords_insert("CURRENT", CURRENT );
	keywords_insert("CURSOR", CURSOR );
	*/
	keywords_insert("DEC", 	DECIMAL );
	keywords_insert("DECIMAL", DECIMAL );
	/*
	keywords_insert("DECLARE", DECLARE );
	*/
	keywords_insert("DEFAULT", DEFAULT );
	keywords_insert("DELETE", DELETE );
	keywords_insert("DESC", DESC );
	keywords_insert("DISTINCT", DISTINCT );
	keywords_insert("DOUBLE", DOUBLE );
	keywords_insert("DROP", DROP );
	keywords_insert("ESCAPE", ESCAPE );
	keywords_insert("EXISTS", EXISTS );
	keywords_insert("EXTRACT", EXTRACT );
	/*
	keywords_insert("FETCH", FETCH );
	*/
	keywords_insert("FLOAT", FLOAT );
	keywords_insert("FOR", FOR );
	keywords_insert("FOREIGN", FOREIGN );
	/*
	keywords_insert("FOUND", FOUND );
	*/
	keywords_insert("FROM", FROM );
	keywords_insert("FULL", FULL );
	/*
	keywords_insert("GO", GO );
	*/
	keywords_insert("GLOBAL", GLOBAL );
	keywords_insert("GRANT", GRANT );
	keywords_insert("GROUP", GROUP );
	keywords_insert("HAVING", HAVING );
	keywords_insert("IN", IN );
	keywords_insert("INDICATOR", INDICATOR );
	keywords_insert("INNER", INNER );
	keywords_insert("INSERT", INSERT );
	keywords_insert("INT", INTEGER );
	keywords_insert("INTEGER", INTEGER );
	keywords_insert("INTO", INTO );
	keywords_insert("IS", IS );
	keywords_insert("JOIN", JOIN );
	keywords_insert("KEY", KEY );
	/*
	keywords_insert("LANGUAGE", LANGUAGE );
	*/
	keywords_insert("LEFT", LEFT );
	keywords_insert("LIKE", LIKE );
	keywords_insert("LOCAL", LOCAL );
	keywords_insert("NATURAL", NATURAL );
	keywords_insert("NOT", NOT );
	keywords_insert("NULL", NULLX );
	keywords_insert("NUMERIC", NUMERIC );
	/*
	keywords_insert("OF", OF );
	*/
	keywords_insert("ON", ON );
	/*
	keywords_insert("OPEN", OPEN );
	*/
	keywords_insert("OPTIONS", OPTIONS );
	keywords_insert("OPTION", OPTION );
	keywords_insert("OR", OR );
	keywords_insert("ORDER", ORDER );
	keywords_insert("OUTER", OUTER );
	keywords_insert("PATH", PATH );
	keywords_insert("PRECISION", PRECISION );
	keywords_insert("PRIMARY", PRIMARY );
	keywords_insert("PRIVILEGES", PRIVILEGES );
	/*
	keywords_insert("PROCEDURE", PROCEDURE );
	*/
	keywords_insert("PUBLIC", PUBLIC );
	keywords_insert("REAL", REAL );
	keywords_insert("REFERENCES", REFERENCES );
	keywords_insert("RESTRICT", RESTRICT );
	keywords_insert("RIGHT", RIGHT );
	keywords_insert("ROLLBACK", ROLLBACK );
	keywords_insert("SCHEMA", SCHEMA );
	keywords_insert("SELECT", SELECT );
	keywords_insert("SET", SET );
	keywords_insert("SMALLINT", SMALLINT );
	keywords_insert("SOME", SOME );
	/*
	keywords_insert("SQLCODE", SQLCODE );
	*/
	keywords_insert("STRING", VARCHAR );
	keywords_insert("TABLE", TABLE );
	keywords_insert("TEMPORARY", TEMPORARY );
	keywords_insert("TEXT", VARCHAR );
	keywords_insert("TO", TO );
	/*
	keywords_insert("UNDER", UNDER );
	*/
	keywords_insert("UNION", UNION );
	keywords_insert("UNIQUE", UNIQUE );
	keywords_insert("UPDATE", UPDATE );
	keywords_insert("USER", USER );
	keywords_insert("USING", USING );
	keywords_insert("VALUES", VALUES );
	keywords_insert("VARCHAR", VARCHAR );
	keywords_insert("VIEW", VIEW );
	/*
	keywords_insert("WHENEVER", WHENEVER );
	*/
	keywords_insert("WHERE", WHERE );
	keywords_insert("WITH", WITH );
	keywords_insert("WORK", WORK );

	keywords_insert("DATE", DATE );
	keywords_insert("TIME", TIME );
	keywords_insert("TIMESTAMP", TIMESTAMP );
	keywords_insert("INTERVAL", INTERVAL );

	keywords_insert("YEAR", YEAR );
	keywords_insert("MONTH", MONTH );
	keywords_insert("DAY", DAY );
	keywords_insert("HOUR", HOUR );
	keywords_insert("MINUTE", MINUTE );
	keywords_insert("SECOND", SECOND );

	keywords_insert("SUBSTRING", SUBSTRING );

	keywords_insert("CASE", CASE );
	keywords_insert("WHEN", WHEN );
	keywords_insert("THEN", THEN );
	keywords_insert("ELSE", ELSE );
	keywords_insert("END", END );
	keywords_insert("NULLIF", NULLIF );
	keywords_insert("COALESCE", COALESCE );
}

void exit_keywords(){
	int i;

	if (keywords_init_done == 0) return;
	keywords_init_done = 0;

	for(i=0;i<HASH_SIZE; i++){
		keyword *k = keywords[i];
		while(k){
			keyword *l = k;
			k = k->next;
			_DELETE(l->keyword);
			_DELETE(l);
		}
	}
}

keyword *find_keyword( char *yytext ){
	int len = 0;
	int bucket = keyword_key(toLower(yytext), &len)&HASH_MASK;
	keyword *k = keywords[bucket];
	while(k){
		if (len == k->len){
			if (len < 4){
				if (*(short*)k->keyword == *(short*)yytext &&
		    			strcmp(k->keyword,yytext)==0) 
				return k;
			} else {
				if (*(int*)k->keyword == *(int*)yytext &&
		    			strcmp(k->keyword,yytext)==0) 
				return k;
			}
		}
		k = k->next;
	}
	return NULL;
}

static int lex_getc(context *lc ){
       	char ch;
	int c = EOF;
	if (lc->in){
       		if (lc->in->read(lc->in, &ch, 1, 1) == 1){
			c = (int)ch;
		}
	} else if (lc->buf && *lc->buf){
                c = *lc->buf++;
        }
	if (c == '\n') lc->lineno++;
	return c;
}

int keyword_or_ident(context *lc){
	keyword *k = NULL;
	char *yytext = lc->yytext;
	int cur = 0;
	int yylen = 1;
	int yysz = lc->yysize;

	yytext[0] = lc->cur;

	while( (cur = lex_getc(lc)) != EOF){
		if (!isalnum(cur) && cur != '_'){
			if (yylen == yysz){
				yytext = realloc(yytext, yysz<<1);
				yysz = yysz<<1;
			}
			yytext[yylen] = 0;
			k = find_keyword(yytext);
			if (k){
				lc->yyval = k->token;
			} else if (cat_bind_type( lc->cat, yytext ) != NULL){
				lc->yyval = TYPE;
			} else {
				lc->yyval = NAME;
			}
			lc->yytext = yytext;
			lc->yylen = yylen;
			lc->yysize = yysz;
			lc->cur = cur;
			return lc->yyval;
		}
		if (yylen == yysz){
			yytext = realloc(yytext, yysz<<1);
			yysz = yysz<<1;
		}
		yytext[yylen++] = (char)cur;
	}
	if (yylen == yysz){
		yytext = realloc(yytext, yysz<<1);
		yysz = yysz<<1;
	}
	yytext[yylen] = 0;
	k = find_keyword(yytext);
	if (k){
		lc->yyval = k->token;
	} else if (cat_bind_type( lc->cat, yytext ) != NULL){
		lc->yyval = TYPE;
	} else {
		lc->yyval = NAME;
	}
	lc->yytext = yytext;
	lc->yylen = yylen;
	lc->yysize = yysz;
	lc->cur = cur;
	return lc->yyval;
}

void skip_white_space( context *lc ){
	int cur;
	while( (cur = lex_getc(lc)) != EOF && isspace(cur)) ;
	lc->cur = cur;
}


int handle_error(context *lc){
	switch(lc->cur){
	case EOF:
		snprintf(lc->errstr, ERRSIZE, _("Unexpected end of file"));
		break;
	}
	return -1;
}

int skip_c_comment(context *lc){
	int cur;
	int prev = 0;
	while((cur = lex_getc(lc)) != EOF && !(cur == '/' &&  prev == '*')){
		prev = cur;
	}
	if (cur == '/')
		lc->cur = lex_getc(lc);
	return lc->cur;
}

int skip_sql_comment(context *lc){
	int cur;
	while((cur = lex_getc(lc)) != EOF && (cur != '\n'));
	if (cur == '\n')
		lc->cur = lex_getc(lc);
	return lc->cur;
}

int context_yychar(context *lc, int yychar ){
	lc->yytext[0] = (char)yychar;
	lc->yytext[1] = 0;
	lc->yyval = yychar;
	lc->yylen = 1;
	return lc->yyval;
}

int context_yy2char(context *lc, int yychar, int yyval ){
	lc->yytext[0] = (char)yychar;
	lc->yytext[1] = (char)yychar;
	lc->yytext[2] = 0;
	lc->yyval = yyval;
	lc->yylen = 2;
	return lc->yyval;
}

int context_comparison(context *lc, char *yychar ){
	int len = strlen(yychar);
	strncpy(lc->yytext, yychar, len);
	lc->yytext[len] = '\0';
	lc->yyval = COMPARISON;
	lc->yylen = len;
	return lc->yyval;
}

int sql_string( context *lc, int quote ){
	char *yytext = lc->yytext;
	int cur = 0;
	int yylen = 0;
	int yysz = lc->yysize;
	int escape = 0;

	while( (cur = lex_getc(lc)) != EOF && ( cur != quote || escape)){
		if (yylen == yysz){
			yytext = realloc(yytext, yysz<<1);
			yysz = yysz<<1;
		}
		if (cur == '\\')
			escape = 1;
		else
			escape = 0;
		yytext[yylen++] = (char)cur;
	}
	if (yylen == yysz){
		yytext = realloc(yytext, yysz<<1);
		yysz = yysz<<1;
	}
	yytext[yylen] = 0;
	lc->yyval = STRING;
	lc->yytext = yytext;
	lc->yylen = yylen;
	lc->yysize = yysz;

	lc->cur = lex_getc(lc);
	return lc->yyval;
}

static int tokenize(context *lc);

int lex_symbol(context *lc){
	int cur = lc->cur;
	if (cur == '/'){
		int next = lex_getc(lc);
		if (next == '*'){
			skip_c_comment(lc);
			return tokenize(lc);
		} else {
			lc->cur = next;
			return context_yychar(lc, cur);
		}
	} else if (cur == '-'){
		int next = lex_getc(lc);
		if (next == '-'){
			if (skip_sql_comment(lc) == EOF)
				return lc->cur;
			return tokenize(lc);
		} else {
			lc->cur = next;
			return context_yychar(lc, cur);
		}
	} else if (cur == '#'){
		if (skip_sql_comment(lc) == EOF)
			return lc->cur;
		return tokenize(lc);
	} else if (cur == '\'' || cur == '"'){
		return sql_string(lc, cur);
	}
	switch(cur){
		case '-': case '+':
		case '*': case '/':
		case '(': case ')':
		case ',': case '.':
		case ';': case '=':
			lc->cur = lex_getc(lc);
			return context_yychar(lc, cur);
		case '<':
			lc->cur = lex_getc(lc);
			if (lc->cur == '='){
				lc->cur = lex_getc(lc);
				return context_comparison(lc, "<=");
			} else if (lc->cur == '>'){
				lc->cur = lex_getc(lc);
				return context_comparison(lc, "<>");
			} else {
				return context_comparison(lc, "<");
			}
		case '>':
			lc->cur = lex_getc(lc);
			if (lc->cur != '='){
				return context_comparison(lc, ">");
			} else {
				lc->cur = lex_getc(lc);
				return context_comparison(lc, ">=");
			}
		case '|':
			lc->cur = lex_getc(lc);
			if (lc->cur == '|'){
				return context_yy2char(lc, cur, CONCATSTRING);
			}
	}
	snprintf(lc->errstr, ERRSIZE, _("Unknown symbol %c"), lc->cur);
	return -1;
}

int number(context *lc){
	char *yytext = lc->yytext;
	int token = INT;
	int cur = 0;
	int yylen = 1;
	int yysz = lc->yysize;
	/* todo start with . */

	yytext[0] = lc->cur;

	while( (cur = lex_getc(lc)) != EOF && isdigit(cur)){
		if (yylen == yysz){
			yytext = realloc(yytext, yysz<<1);
			yysz = yysz<<1;
		}
		yytext[yylen++] = (char)cur;
	}
	if (cur == '.'){
		token = INTNUM;
		if (yylen == yysz){
			yytext = realloc(yytext, yysz<<1);
			yysz = yysz<<1;
		}
		yytext[yylen++] = (char)cur;
		while( (cur = lex_getc(lc)) != EOF && isdigit(cur)){
			if (yylen == yysz){
				yytext = realloc(yytext, yysz<<1);
				yysz = yysz<<1;
			}
			yytext[yylen++] = (char)cur;
		}
		if (cur == 'e' || cur == 'E'){
			token = APPROXNUM;
			if (yylen == yysz){
				yytext = realloc(yytext, yysz<<1);
				yysz = yysz<<1;
			}
			yytext[yylen++] = (char)cur;
			if (lc->cur == '-' || lc->cur == '+'){
				if (yylen == yysz){
					yytext = realloc(yytext, yysz<<1);
					yysz = yysz<<1;
				}
				yytext[yylen++] = (char)lex_getc(lc); 
			}
			while( (cur = lex_getc(lc)) != EOF && isdigit(cur)){
				if (yylen == yysz){
					yytext = realloc(yytext, yysz<<1);
					yysz = yysz<<1;
				}
				yytext[yylen++] = (char)cur;
			}
		}
	}
	yytext[yylen] = 0;
	if (isdigit(yytext[yylen-1])){
		lc->yyval = token;
	} else if (yylen == 1){
		if (yytext[0] == '-'){
			if (cur == '-'){
				if (skip_sql_comment(lc) == EOF)
					return lc->cur;
				return tokenize(lc);
			} else {
				lc->cur = cur;
				return context_yychar(lc, yytext[0]);
			}
		} else if (yytext[0] == '+'){
			lc->cur = cur;
			return context_yychar(lc, yytext[0]);
		}
	} else {
		snprintf(lc->errstr, ERRSIZE, _("Unexpected symbol %c"), lc->cur);
		return -1;
	}
	lc->yytext = yytext;
	lc->yylen = yylen;
	lc->yysize = yysz;
	
	lc->cur = cur;
	return lc->yyval;
}

int tokenize(context *lc){
	while(1){
		if(isspace(lc->cur)){
			skip_white_space(lc); 
			if (lc->cur == EOF) return lc->cur;
		} else if (isdigit(lc->cur)){
			return number(lc);
		} else if (isalpha(lc->cur) || lc->cur == '_'){
			return keyword_or_ident(lc);
		} else if (ispunct(lc->cur)){
			return lex_symbol(lc);
		}
		if (lc->cur == EOF) return handle_error(lc);	
	}
}

int sqllex( YYSTYPE *yylval, void *parm ){
	context *lc = (context*)parm;
	int token = tokenize(lc);
	yylval->sval = lc->yytext;
	if (token == NAME || token == COMPARISON || token == STRING ||
		token == AMMSC || token == TYPE)
		yylval->sval = _strdup(lc->yytext);
	sql_statement_add(lc, lc->yytext);
	return token;
}

int
parse_error(context *lc, char *err)
{
	snprintf(lc->errstr, ERRSIZE, "%s(%d) %s at token: %s\n in statement: %s", 
		lc->filename, lc->lineno, err, lc->yytext, lc->sql);
	return 1;
}

