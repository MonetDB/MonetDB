
#include "sql.h"
#include <string.h>
#include <mem.h>

/* 
 * some string functions.
 */

/* implace cast to lower case string */
char *mkLower(char *s)
{
	char *r = s;
	while (*s) {
		*s = (char) tolower(*s);
		s++;
	}
	return r;
}

char *toLower(const char *s)
{
	char *r = _strdup(s);
	return mkLower(r);
}

/* concat s1,s2 into a new result string */
char *strconcat(const char *s1, const char *s2)
{
	int i, j, l1 = strlen(s1);
	int l2 = strlen(s2) + 1;
	char *new_s = NEW_ARRAY(char, l1 + l2);
	for (i = 0; i < l1; i++) {
		new_s[i] = s1[i];
	}
	for (j = 0; j < l2; j++, i++) {
		new_s[i] = s2[j];
	}
	return new_s;
}

char *strip_extra_zeros( char *s ){
	char *res = s;
	for(;*s && *s == '0'; s++);
	if (*s == '.') s--;
	res = s;
	for(;*s;s++); /* find end */
	s--;
	for(;*s && *s == '0';s--);
	s++;
	*s=0;
	return res;
}
