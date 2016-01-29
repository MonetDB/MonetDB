/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * (c) M. Kersten
 * MAL Type System
 * The MAL type module overloads the atom structure managed in the GDK library.
 * For the time being, we assume GDK to support at most 127 different atomic types.
 * Type composition is limited to the  builtin scalar type and a column type.
 * Furthermore, the polymorphic MAL type :any can be qualified
 * with a type variable index :any_I, where I is a digit (1-9).
 * BEWARE, the TYPE_any is a speudo type known within MAL only.
 *
 * Within the MAL layer types are encoded in 32-bit integers using
 * bit stuffing to save some space.
 * The integer contains the following fields:
 * anyHeadIndex (bit 25-22), anyColumnIndex (bit 21-18),
 * batType (bit 17) headType (16-9) and tailType(8-0)
 * This encoding scheme permits a limited number of different bat types.
 * The headless case assumes all head types are TYPE_void/TYPE_oid
 */
#include "monetdb_config.h"
#include "mal_type.h"

/*
 * At any point we should be able to construct an ascii representation of
 * the type descriptor. Including the variable references.
 */
str
getTypeName(malType tpe)
{
	char buf[PATHLENGTH], *s;
	size_t l = PATHLENGTH;
	int k;

	if (tpe == TYPE_any)
		return GDKstrdup("any");
	if (isaBatType(tpe)) {
		snprintf(buf, l, "bat[");
		l -= strlen(buf);
		s = buf + strlen(buf);
		k = getColumnIndex(tpe);
		if (k)
			snprintf(s, l, ":any%c%d]",TMPMARKER,  k);
		else if (getColumnType(tpe) == TYPE_any)
			snprintf(s, l, ":any]");
		else
			snprintf(s, l, ":%s]", ATOMname(getColumnType(tpe)));
		return GDKstrdup(buf);
	}
	if (isAnyExpression(tpe)) {
		strncpy(buf, "any", 4);
		if (isAnyExpression(tpe))
			snprintf(buf + 3, PATHLENGTH - 3, "%c%d",
					TMPMARKER, getColumnIndex(tpe));
		return GDKstrdup(buf);
	}
	return GDKstrdup(ATOMname(tpe));
}
/*
 * It might be handy to encode the type information in an identifier
 * string for ease of comparison later.
 */
str
getTypeIdentifier(malType tpe){
	str s,t,v;
	s= getTypeName(tpe);
	for ( t=s; *t; t++)
		if ( !isalnum((int) *t) )
			*t='_';
	t--;
	if (*t == '_') *t = 0;
	for (v=s, t=s+1; *t; t++){
		if (  !(*t == '_' && *v == '_' ) )
			*++v = *t;
	}
	*++v =0;
	return s;
}


/*
 * In many places we need a confirmed type identifier.
 * GDK returns the next available index when it can not find the type.
 * This is not sufficient here, an error message may have to be generated.
 * It is assumed that the type table does not change in the mean time.
 * Use the information that identifiers are at least one character
 * and are terminated by a null to speedup comparison
 */

/*
 * The ATOMindex routine is pretty slow, because it performs a
 * linear search through the type table. This code should actually
 * be integrated with the kernel.
 */
#define qt(x) (nme[1]==x[1] && nme[2]==x[2] )

int
getTypeIndex(str nme, int len, int deftype)
{
	int i,k=0;
	char old=0;

	if (len == 3)
		switch (*nme) {
		case 'a':
			if (qt("any"))
				return TYPE_any;
			break;
		case 'b':
			if (qt("bat"))
				return TYPE_bat;
			if (qt("bit"))
				return TYPE_bit;
			if (qt("bte"))
				return TYPE_bte;
			break;
		case 'd':
			if (qt("dbl"))
				return TYPE_dbl;
			break;
		case 'i':
			if (qt("int"))
				return TYPE_int;
			break;
		case 'f':
			if (qt("flt"))
				return TYPE_flt;
			break;
		case 'l':
			if (qt("lng"))
				return TYPE_lng;
			break;
		case 'p':
			if (qt("ptr"))
				return TYPE_ptr;
			break;
#ifdef HAVE_HGE
		case 'h':
			if (qt("hge"))
				return TYPE_hge;
			break;
#endif
		case 'o':
			if (qt("oid"))
				return TYPE_oid;
			break;
		case 's':
			if (qt("str"))
				return TYPE_str;
			if (qt("sht"))
				return TYPE_sht;
			break;
		}
	if( nme[0]=='v' && qt("voi") && nme[3] == 'd')
				return TYPE_void;
	if( len > 0 ){
		old=  nme[k = MIN(IDLENGTH, len)];
		nme[k] = 0;
	}
	for(i= TYPE_str; i< GDKatomcnt; i++)
		if( BATatoms[i].name[0]==nme[0] &&
			strcmp(nme,BATatoms[i].name)==0) break;
	if( len > 0)
		nme[k]=old;
	if (i == GDKatomcnt)
		i = deftype;
	return i;
}

inline int
findGDKtype(int type)
{
	if (type == TYPE_any || type== TYPE_void)
		return TYPE_void;
	if (isaBatType(type))
		return TYPE_bat;
	return ATOMtype(type);
}

inline int
isTmpName(const char *n)
{
	return n && *n == TMPMARKER ;
}

int
isIdentifier(str s)
{
	if (!isalpha((int) *s))
		return -1;
	for (; s && *s; s++)
		if (!isalnum((int) *s) && *s != '_')
			return -1;
	return 0;
}
