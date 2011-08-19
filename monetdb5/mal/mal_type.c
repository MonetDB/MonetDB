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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @a M. Kersten
 * @v 0.0
 * @+ Type implementation
 * @section MAL Type System
 * The MAL type module overloads the atom structure managed in the GDK
 * library.
 * For the time being, we assume GDK to support at most 127 different
 * atomic types.
 * Type composition is limited to at most two builtin types to form
 * a BAT. Furthermore, the polymorphic type @sc{any} can be qualified
 * with a type variable index @sc{any$I}, where I is a digit (1-9).
 * Beware, the TYPE_any is a speudo type known within MAL only.
 *
 * Within the MAL layer types are encoded in 32-bit integers using
 * bit stuffing to save some space.
 * The integer contains the following fields:
 * anyHeadIndex (bit 25-22), anyTailIndex (bit 21-18),
 * batType (bit 17) headType (16-9) and tailType(8-0)
 * This encoding scheme permits a limited number of different bat types.
 */
/*
 * @-
 * At any point we should be able to construct an ascii representation of
 * the type descriptor. Including the variable references.
 */
#include "monetdb_config.h"
#include "mal_type.h"

str
getTypeName(malType tpe)
{
	char buf[PATHLENGTH], *s;
	size_t l = PATHLENGTH;
	int k;

	if (tpe == TYPE_any)
		return GDKstrdup("any");
	if (isaBatType(tpe)) {
		k = getHeadIndex(tpe);
		if (k)
			snprintf(buf, l, "bat[:any%c%d,", TMPMARKER, k);
		else if (getHeadType(tpe) == TYPE_any)
			snprintf(buf, l, "bat[:any,");
		else
			snprintf(buf, l, "bat[:%s,", ATOMname(getHeadType(tpe)));
		l -= strlen(buf);
		s = buf + strlen(buf);
		k = getTailIndex(tpe);
		if (k)
			snprintf(s, l, ":any%c%d]",TMPMARKER,  k);
		else if (getTailType(tpe) == TYPE_any)
			snprintf(s, l, ":any]");
		else
			snprintf(s, l, ":%s]", ATOMname(getTailType(tpe)));
		return GDKstrdup(buf);
	}
	if (isAnyExpression(tpe)) {
		strncpy(buf, "any", 4);
		if (isAnyExpression(tpe))
			snprintf(buf + 3, PATHLENGTH - 3, "%c%d",
					TMPMARKER, getTailIndex(tpe));
		return GDKstrdup(buf);
	}
	return GDKstrdup(ATOMname(tpe));
}
/*
 * @-
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
 * @+ Some utilities
 * In many places we need a confirmed type identifier.
 * GDK returns the next available index when it can not find the type.
 * This is not sufficient here, an error message may have to be generated.
 * It is assumed that the type table does not change in the mean time.
 * Use the information that identifiers are at least one character
 * and are terminated by a null to speedup comparison
 */
inline int
idcmp(str n, str m)
{
	assert(n != NULL);
	assert(m != NULL);
	if (*n == *m)
		return strcmp(n, m);
	return -1;
}

/*
 * @-
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
		case 'c':
			if (qt("chr"))
				return TYPE_chr;
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
		case 'o':
			if (qt("oid"))
				return TYPE_oid;
			break;
		case 'p':
			if (qt("ptr"))
				return TYPE_ptr;
			break;
		case 's':
			if (qt("str"))
				return TYPE_str;
			if (qt("sht"))
				return TYPE_sht;
			break;
		case 'w':
			if (qt("wrd"))
				return TYPE_wrd;
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
/*
 * @-
 * Literal constants are not necessary type specific, e.g.
 * the value '0' could represent sht,wrd,int,lng.
 * If the value is potential ambiguous then it should
 * be made type specific in listings
 */
int
isAmbiguousType(int type){
	switch(type){
	case TYPE_sht: case TYPE_wrd: case TYPE_int: case TYPE_lng:
		return type != TYPE_int;
	}
	return 0;
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

str
newTmpName(char tag, int i)
{
	char buf[PATHLENGTH];

	snprintf(buf, PATHLENGTH, "%c%d", tag, i);
	return GDKstrdup(buf);
}

inline int
isTmpName(str n)
{
	return n && *n == TMPMARKER ;
}

int
isTypeName(str n)
{
	int i = ATOMindex(n);

	return i >= 0;
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
