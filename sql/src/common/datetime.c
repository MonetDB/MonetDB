
#include <string.h>
#include <stdlib.h>
#include "datetime.h"
#include "sql.h"

int parse_interval_intern( context *sql, 
	int sign, char *str, int sk, int ek, int *i ){
	char *n = NULL;
	int val = 0;
	int mul = sign;
	char sep = ':';
	int type = -1;

	switch(sk){
	case iyear:	mul *= 12;
	case imonth:	sep = '-';
			break;
	case iday:	mul *= 24;
	case ihour:	mul *= 60;
	case imin:	mul *= 60;
	case isec:	mul *= 1;
			break;
	}

	val = strtol( str, &n, 10 ); 
	val *= mul;
	if (sk == imonth || sk == iyear){
		type = 0;
	} else {
		type = 1;
	}
	*i += val;
	if (ek != sk){
		if (*n != sep){
			snprintf(sql->errstr, ERRSIZE, 
			_("Interval field seperator \'%c\' missing\n"), sep );
			return -1;
		}
		return parse_interval_intern( sql, sign, n+1, sk+1, ek, i );
	} else {
		return type;
	}
}
int parse_interval( context *sql,
	int sign, char *str, struct dlist *pers, int *val){

	int sk = iyear, ek = isec;

	if (pers){
		dlist *s = pers->h->data.lval;

		ek = sk = s->h->data.ival;

		if (dlist_length(pers) == 2){
			dlist *e = pers->h->next->data.lval;

			sk = e->h->data.ival;
		}
	}
	*val = 0;
	if (sk < ek){
		snprintf(sql->errstr, ERRSIZE, 
		_("End interval field is larger than the start field\n") );
		return -1;
	}
	if ( (sk == iyear || sk == imonth) && ek > imonth ){
		snprintf(sql->errstr, ERRSIZE, 
		_("Correct interval ranges are year-month or day-seconds\n") );
		return -1;
	} 
	return parse_interval_intern( sql, sign, str, sk, ek, val ); 
}


const char *datetime_field( itype f ){
	switch(f){
	case iyear:  return "year";
	case imonth: return "month";
	case iday:   return "day";
	case ihour:  return "hour";
	case imin:   return "min";
	case isec:   return "sec";
	}
	return "year";
}
