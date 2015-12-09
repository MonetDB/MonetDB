#include <Rdefines.h>
#include "mal.h"

#define BAT_TO_SXP(bat,tpe,retsxp,newfun,ptrfun,ctype,naval,memcopy)\
	do {													\
		tpe v; size_t j;									\
		ctype *valptr = NULL;                               \
		tpe* p = (tpe*) Tloc(bat,BUNfirst(bat));            \
		retsxp = PROTECT(newfun(BATcount(bat)));		    \
		valptr = ptrfun(retsxp);                            \
		if (bat->T->nonil && !bat->T->nil) {                \
			if (memcopy) {									\
				memcpy(valptr, p,                           \
					BATcount(bat) * sizeof(tpe));           \
			} else {                                        \
				for (j = 0; j < BATcount(bat); j++) {       \
					valptr[j] = (ctype) p[j];               \
				}                                           \
			} 												\
		} else {                                            \
		for (j = 0; j < BATcount(bat); j++) {				\
			v = p[j];                                       \
			if ( v == tpe##_nil)							\
				valptr[j] = naval;	                        \
			else											\
				valptr[j] = (ctype) v;	                    \
		}}													\
	} while (0)

#define BAT_TO_INTSXP(bat,tpe,retsxp,memcopy)						\
	BAT_TO_SXP(bat,tpe,retsxp,NEW_INTEGER,INTEGER_POINTER,int,NA_INTEGER,memcopy)\

#define BAT_TO_REALSXP(bat,tpe,retsxp,memcopy)						\
	BAT_TO_SXP(bat,tpe,retsxp,NEW_NUMERIC,NUMERIC_POINTER,double,NA_REAL,memcopy)\


#define SCALAR_TO_INTSXP(tpe,retsxp)					\
	do {												\
		tpe v;											\
		retsxp = PROTECT(NEW_INTEGER(1));				\
		v = *getArgReference_##tpe(stk,pci,i);			\
		if ( v == tpe##_nil)							\
			INTEGER_POINTER(retsxp)[0] = 	NA_INTEGER; \
		else											\
			INTEGER_POINTER(retsxp)[0] = 	(int)v;		\
	} while (0)

#define SCALAR_TO_REALSXP(tpe,retsxp)                   \
	do {												\
		tpe v;											\
		retsxp = PROTECT(NEW_NUMERIC(1));				\
		v = * getArgReference_##tpe(stk,pci,i);			\
		if ( v == tpe##_nil)							\
			NUMERIC_POINTER(retsxp)[0] = 	NA_REAL;	\
		else											\
			NUMERIC_POINTER(retsxp)[0] = 	(double)v;	\
	} while (0)

#define SXP_TO_BAT(tpe,access_fun,na_check)								\
	do {																\
		tpe *p, prev = tpe##_nil; int j;								\
		b = BATnew(TYPE_void, TYPE_##tpe, cnt, TRANSIENT);				\
		BATseqbase(b, 0); b->T->nil = 0; b->T->nonil = 1; b->tkey = 0;	\
		b->tsorted = 1; b->trevsorted = 1;								\
		p = (tpe*) Tloc(b, BUNfirst(b));								\
		for( j =0; j< (int) cnt; j++, p++){								\
			*p = (tpe) access_fun(s)[j];							\
			if (na_check){ b->T->nil = 1; 	b->T->nonil = 0; 	*p= tpe##_nil;} \
			if (j > 0){													\
				if ( *p > prev && b->trevsorted){						\
					b->trevsorted = 0;									\
					if (*p != prev +1) b->tdense = 0;					\
				} else													\
					if ( *p < prev && b->tsorted){						\
						b->tsorted = 0;									\
						b->tdense = 0;									\
					}													\
			}															\
			prev = *p;													\
		}																\
		BATsetcount(b,cnt);												\
		BATsettrivprop(b);												\
	} while (0)


static SEXP bat_to_sexp(BAT* b) {
	SEXP varvalue = NULL;
	// TODO: deal with SQL types (DECIMAL/DATE)
	switch (ATOMstorage(getColumnType(b->T->type))) {
		case TYPE_bte:
			BAT_TO_INTSXP(b, bte, varvalue,0);
			break;
		case TYPE_sht:
			BAT_TO_INTSXP(b, sht, varvalue,0);
			break;
		case TYPE_int:
			// special case: memcpy for int-to-int conversion without NULLs
			BAT_TO_INTSXP(b, int, varvalue, 1);
			break;
#ifdef HAVE_HGE
		case TYPE_hge: /* R's integers are stored as int, so we cannot be sure hge will fit */
			BAT_TO_REALSXP(b, hge, varvalue, 0);
			break;
#endif
		case TYPE_flt:
			BAT_TO_REALSXP(b, flt, varvalue, 0);
			break;
		case TYPE_dbl:
			// special case: memcpy for double-to-double conversion without NULLs
			BAT_TO_REALSXP(b, dbl, varvalue, 1);
			break;
		case TYPE_lng: /* R's integers are stored as int, so we cannot be sure long will fit */
			BAT_TO_REALSXP(b, lng, varvalue, 0);
			break;
		case TYPE_str: { // there is only one string type, thus no macro here
			BUN p = 0, q = 0, j = 0;
			BATiter li;
			li = bat_iterator(b);
			varvalue = PROTECT(NEW_STRING(BATcount(b)));

			if (b->T->nonil && !b->T->nil) {
				BATloop(b, p, q) {
					SET_STRING_ELT(varvalue, j++, mkCharCE(
						(const char *) BUNtail(li, p), CE_UTF8));
				}
			}
			else {
				BATloop(b, p, q) {
					const char *t = (const char *) BUNtail(li, p);
					if (ATOMcmp(TYPE_str, t, str_nil) == 0) {
						SET_STRING_ELT(varvalue, j, NA_STRING);
					} else {
						SET_STRING_ELT(varvalue, j, mkCharCE(t, CE_UTF8));
					}
					j++;
				}
			}
		} 	break;
	}
	return varvalue;
}

static BAT* sexp_to_bat(SEXP s, int type) {
	BAT* b = NULL;
	BUN cnt = LENGTH(s);
	switch (type) {
	case TYPE_int: {
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(int, INTEGER_POINTER, *p==NA_INTEGER);
		break;
	}
	case TYPE_lng: {
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(lng, INTEGER_POINTER, *p==NA_INTEGER);
		break;
	}
#ifdef HAVE_HGE
	case TYPE_hge: {
		if (!IS_INTEGER(s)) {
			return NULL;
		}
		SXP_TO_BAT(hge, INTEGER_POINTER, *p==NA_INTEGER);
		break;
	}
#endif
	case TYPE_bte:
	case TYPE_bit: { // only R logical types fit into bit BATs
		if (!IS_LOGICAL(s)) {
			return NULL;
		}
		SXP_TO_BAT(bit, LOGICAL_POINTER, *p==NA_LOGICAL);
		break;
	}
	case TYPE_dbl: {
		if (!IS_NUMERIC(s)) {
			return NULL;
		}
		SXP_TO_BAT(dbl, NUMERIC_POINTER, ISNA(*p));
		break;
	}
	case TYPE_str: {
		SEXP levels;
		size_t j;
		if (!IS_CHARACTER(s) && !isFactor(s)) {
			return NULL;
		}
		b = BATnew(TYPE_void, TYPE_str, cnt, TRANSIENT);
		BATseqbase(b, 0);
		b->T->nil = 0;
		b->T->nonil = 1;
		b->tkey = 0;
		b->tsorted = 0;
		b->trevsorted = 0;
		b->tdense = 1;
		/* get levels once, since this is a function call */
		levels = GET_LEVELS(s);

		for (j = 0; j < cnt; j++) {
			SEXP rse;
			if (isFactor(s)) {
				int ii = INTEGER(s)[j];
				if (ii == NA_INTEGER) {
					rse = NA_STRING;
				} else {
					rse = STRING_ELT(levels, ii - 1);
				}
			} else {
				rse = STRING_ELT(s, j);
			}
			if (rse == NA_STRING) {
				b->T->nil = 1;
				b->T->nonil = 0;
				BUNappend(b, str_nil, FALSE);
			} else {
				BUNappend(b, CHAR(rse), FALSE);
			}
		}
		break;
	}
	}

	if (b != NULL) {
		BATsetcount(b, cnt);
		BBPkeepref(b->batCacheid);
	}
	return b;
}
