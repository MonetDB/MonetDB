#include <Rdefines.h>
#include "mal.h"

#define BAT_TO_INTSXP(bat,tpe,retsxp)						\
	do {													\
		tpe v;	size_t j;									\
		retsxp = PROTECT(NEW_INTEGER(BATcount(bat)));		\
		for (j = 0; j < BATcount(bat); j++) {				\
			v = ((tpe*) Tloc(bat, BUNfirst(bat)))[j];		\
			if ( v == tpe##_nil)							\
				INTEGER_POINTER(retsxp)[j] = 	NA_INTEGER; \
			else											\
				INTEGER_POINTER(retsxp)[j] = 	(int)v;		\
		}													\
	} while (0)

#define BAT_TO_REALSXP(bat,tpe,retsxp)						\
	do {													\
		tpe v; size_t j;									\
		retsxp = PROTECT(NEW_NUMERIC(BATcount(bat)));		\
		for (j = 0; j < BATcount(bat); j++) {				\
			v = ((tpe*) Tloc(bat, BUNfirst(bat)))[j];		\
			if ( v == tpe##_nil)							\
				NUMERIC_POINTER(retsxp)[j] = 	NA_REAL;	\
			else											\
				NUMERIC_POINTER(retsxp)[j] = 	(double)v;	\
		}													\
	} while (0)

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

#define SCALAR_TO_REALSXP(tpe,retsxp) \
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
	switch (ATOMstorage(getColumnType(b->T->type))) {
		case TYPE_bte:
			BAT_TO_INTSXP(b, bte, varvalue);
			break;
		case TYPE_sht:
			BAT_TO_INTSXP(b, sht, varvalue);
			break;
		case TYPE_int:
			BAT_TO_INTSXP(b, int, varvalue);
			break;
		case TYPE_flt:
			BAT_TO_REALSXP(b, flt, varvalue);
			break;
		case TYPE_dbl:
			BAT_TO_REALSXP(b, dbl, varvalue);
			break;
		case TYPE_lng: /* R's integers are stored as int, so we cannot be sure long will fit */
			BAT_TO_REALSXP(b, lng, varvalue);
			break;
		case TYPE_str: { // there is only one string type, thus no macro here
			BUN p = 0, q = 0, j = 0;
			BATiter li;
			li = bat_iterator(b);
			varvalue = PROTECT(NEW_STRING(BATcount(b)));
			BATloop(b, p, q) {
				const char *t = (const char *) BUNtail(li, p);
				if (ATOMcmp(TYPE_str, t, str_nil) == 0) {
					STRING_ELT(varvalue, j) = NA_STRING;
				} else {
					STRING_ELT(varvalue, j) = mkCharCE(t, CE_UTF8);
				}
				j++;
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
