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
 * @f inet
 * @a Fabian Groffen
 * @v 1.0
 * @* The inet module
 * The inet module contains a collection of functions that operate on IPv4
 * addresses.  The most relevant functions are the `containment' functions
 * that deal with subnet masks.  The functionality of this module is
 * greatly inspired by the PostgreSQL inet atom.
 *
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "mal.h"
#include "mal_exception.h"

/*
 * @* Implementation Code
 * The first 4 bytes of the used lng are in use by the four quads of the
 * IPv4 address, stored in network order.  In the four bytes left,
 * additional information is stored.
 * Currently the fifth byte holds the number of bits from the IPv4 address
 * that should match (ie. /8, /16, /24, /32) also known as subnet mask.
 * The last byte holds whether inet atom represents the value nil or not.
 * The value nil is represented as (per byte) 0000 0001.
 *
 */
typedef struct _inet {
	/* use a union to force alignment compatible with lng */
	union {
		struct {
			unsigned char q1;
			unsigned char q2;
			unsigned char q3;
			unsigned char q4;
			unsigned char mask;
			unsigned char filler1;
			unsigned char filler2;
			unsigned char isnil;
		};
		lng alignment;
	};
} inet;

#ifdef WORDS_BIGENDIAN
/* HACK ALERT: once upon a time, lng_nil was used as inet_nil, but on
 * big endian hardware, the byte that is not zero is on the other end;
 * luckily, a mask of 0 is pretty useless, so we regard 128.0.0.0/0
 * also as nil */
#define is_inet_nil(i) ((((i)->q1 == 0 && (i)->isnil != 0) || ((i)->q1 == 128 && (i)->isnil == 0 && (i)->filler1 == 0 && (i)->filler2 == 0)) && (i)->q2 == 0 && (i)->q3 == 0 && (i)->q4 == 0 && (i)->mask == 0)
#else
#define is_inet_nil(i) ((i)->q1 == 0 && (i)->q2 == 0 && (i)->q3 == 0 && (i)->q4 == 0 && (i)->mask == 0 && (i)->isnil != 0)
#endif
#define in_setnil(i) (i)->q1 = (i)->q2 = (i)->q3 = (i)->q4 = (i)->mask = (i)->filler1 = (i)->filler2 = 0; (i)->isnil = 1

static const inet inet_nil = { {{0, 0, 0, 0, 0, 0, 0, 1}} };

/**
 * Creates a new inet from the given string.
 * Warning: GDK function, does NOT pass a string by reference, and wants
 * a pointer to a pointer for the retval!
 * Returns the number of chars read
 */
static ssize_t
INETfromString(const char *src, size_t *len, void **RETVAL, bool external)
{
	inet **retval = (inet **) RETVAL;
	int i, last, type;
	long parse;					/* type long returned by strtol() */
	char *endptr;
	char sep = '.';

	last = 0;
	type = 0;

	if (*len < sizeof(inet) || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKzalloc(sizeof(inet));
		if (*retval == NULL) {
			*len = 0;
			return -1;
		}
		*len = sizeof(inet);
	} else {
		**retval = (inet) {.q1 = 0, };
	}

	/* handle the nil string */
	if (external && strcmp(src, "nil") == 0) {
		in_setnil(*retval);
		return 3;
	}
	if (strNil(src)) {
		in_setnil(*retval);
		return 1;
	}

	/* use the DIY technique to guarantee maximum cross-platform
	 * portability */
	for (i = 0; src[i] != '\0'; i++) {
		if (src[i] == '.' || src[i] == '/') {
			sep = src[i];
			parse = strtol(src + last, &endptr, 10);
			if (*endptr != sep || last >= i) {
				GDKerror("Error while parsing, unexpected string '%s'", endptr);
				goto error;
			}
			if (parse > 255 || parse < 0) {
				GDKerror("Illegal quad value: %ld", parse);
				goto error;
			}
			switch (type) {
			case 0:
				(*retval)->q1 = (unsigned char) parse;
				break;
			case 1:
				(*retval)->q2 = (unsigned char) parse;
				break;
			case 2:
				(*retval)->q3 = (unsigned char) parse;
				break;
			case 3:
				(*retval)->q4 = (unsigned char) parse;
				break;
			}

			last = i + 1;
			type++;

			if (sep == '/') {
				/* zero out (default) unused bytes */
				switch (type) {
				case 1:
					(*retval)->q2 = (unsigned char) 0;
					/* fall through */
				case 2:
					(*retval)->q3 = (unsigned char) 0;
					/* fall through */
				case 3:
					(*retval)->q4 = (unsigned char) 0;
					break;
				}
				/* force evaluation of the mask below when we break
				 * out of this loop */
				type = 4;
				break;
			}
		}
	}
	/* parse the last quad
	 * the contract is that the caller makes sure the string is
	 * null-terminated here */
	parse = strtol(src + last, &endptr, 10);
	if (*endptr != '\0' || (sep != '/' && last >= i)) {
		GDKerror("Error while parsing, unexpected string '%s'", endptr);
		goto error;
	}
	if (type == 3) {
		if (parse > 255 || parse < 0) {
			GDKerror("Illegal quad value: %ld", parse);
			goto error;
		}
		(*retval)->q4 = (unsigned char) parse;
		/* default to an exact match (all bits) */
		(*retval)->mask = (unsigned char) 32;
	} else if (type == 4) {
		if (parse < 0 || parse > 32) {
			GDKerror("Illegal mask value: %ld", parse);
			goto error;
		}
		(*retval)->mask = (unsigned char) parse;
	} else {
		GDKerror("Error while parsing, unexpected string '%s'", endptr);
		goto error;
	}

	return (ssize_t) (endptr - src);

  error:
	in_setnil(*retval);
	return -1;
}

/**
 * Returns the string representation of the given inet value.
 * Warning: GDK function
 * Returns the length of the string
 */
static ssize_t
INETtoString(str *retval, size_t *len, const void *handle, bool external)
{
	const inet *value = (const inet *) handle;

	if (*len < 20 || *retval == NULL) {
		GDKfree(*retval);
		*retval = GDKmalloc(sizeof(char) * (*len = 20));
		if (*retval == NULL)
			return -1;
	}
	if (is_inet_nil(value)) {
		if (external)
			return snprintf(*retval, *len, "nil");
		strcpy(*retval, str_nil);
		return 1;
	} else if (value->mask == 32) {
		return snprintf(*retval, *len, "%d.%d.%d.%d",
						value->q1, value->q2, value->q3, value->q4);
	} else {
		return snprintf(*retval, *len, "%d.%d.%d.%d/%d",
						value->q1, value->q2, value->q3, value->q4,
						value->mask);
	}
}

/**
 * Returns a inet, parsed from a string.  The fromStr function is used
 * to parse the string.
 */
static str
INETnew(inet *retval, const char *const *in)
{
	ssize_t pos;
	size_t len = sizeof(inet);

	pos = INETfromString(*in, &len, (void **) &retval, false);
	if (pos < 0)
		throw(PARSE, "inet.new", GDK_EXCEPTION);

	return (MAL_SUCCEED);
}

/* === Operators === */
/**
 * Returns whether val represents a nil inet value
 */
static str
INET_isnil(bit *retval, const inet *val)
{
	*retval = is_inet_nil(val);

	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 and val2 are equal.
 */
static str
INET_comp_EQ(bit *retval, const inet *val1, const inet *val2)
{
	if (is_inet_nil(val1) || is_inet_nil(val2)) {
		*retval = bit_nil;
	} else if (val1->q1 == val2->q1 && val1->q2 == val2->q2 &&
			   val1->q3 == val2->q3 && val1->q4 == val2->q4 &&
			   val1->mask == val2->mask) {
		*retval = 1;
	} else {
		*retval = 0;
	}

	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 and val2 are not equal.
 */
static str
INET_comp_NEQ(bit *retval, const inet *val1, const inet *val2)
{
	if (is_inet_nil(val1) || is_inet_nil(val2)) {
		*retval = bit_nil;
	} else if (val1->q1 == val2->q1 && val1->q2 == val2->q2 &&
			   val1->q3 == val2->q3 && val1->q4 == val2->q4 &&
			   val1->mask == val2->mask) {
		*retval = 0;
	} else {
		*retval = 1;
	}

	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is smaller than val2.
 */
static str
INET_comp_LT(bit *retval, const inet *val1, const inet *val2)
{
	if (is_inet_nil(val1) || is_inet_nil(val2)) {
		*retval = bit_nil;
	} else if (val1->q1 < val2->q1) {
		*retval = 1;
	} else if (val1->q1 > val2->q1) {
		*retval = 0;
	} else if (val1->q2 < val2->q2) {
		*retval = 1;
	} else if (val1->q2 > val2->q2) {
		*retval = 0;
	} else if (val1->q3 < val2->q3) {
		*retval = 1;
	} else if (val1->q3 > val2->q3) {
		*retval = 0;
	} else if (val1->q4 < val2->q4) {
		*retval = 1;
	} else if (val1->q4 > val2->q4) {
		*retval = 0;
	} else if (val1->mask < val2->mask) {
		*retval = 1;
	} else {
		*retval = 0;
	}

	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is greater than val2.
 */
static str
INET_comp_GT(bit *retval, const inet *val1, const inet *val2)
{
	return (INET_comp_LT(retval, val2, val1));
}

/**
 * Returns whether val1 is smaller than or equal to val2.
 */
static str
INET_comp_LE(bit *retval, const inet *val1, const inet *val2)
{
	bit ret;

	INET_comp_LT(&ret, val1, val2);
	if (ret == 0)
		INET_comp_EQ(&ret, val1, val2);

	*retval = ret;
	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is smaller than or equal to val2.
 */
static str
INET_comp_GE(bit *retval, const inet *val1, const inet *val2)
{
	bit ret;

	/* warning: we use LT here with swapped arguments to avoid one
	 * method invocation, since inet_comp_GT does the same */
	INET_comp_LT(&ret, val2, val1);
	if (ret == 0)
		INET_comp_EQ(&ret, val1, val2);

	*retval = ret;
	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is contained within val2
 */
static str
INET_comp_CW(bit *retval, const inet *val1, const inet *val2)
{
	if (is_inet_nil(val1) || is_inet_nil(val2)) {
		*retval = bit_nil;
	} else if (val1->mask <= val2->mask) {
		/* if the mask is bigger (less specific) or equal it can never
		 * be contained within */
		*retval = 0;
	} else {
		unsigned int mask;
		unsigned char m[4];

		if (val2->mask > 0)
			mask = ~0U << (32 - val2->mask);
		else
			mask = 0;

		m[0] = (mask >> 24) & 0xFF;
		m[1] = (mask >> 16) & 0xFF;
		m[2] = (mask >> 8) & 0xFF;
		m[3] = mask & 0xFF;

		/* all operations here are done byte based, to avoid byte sex
		 * problems */
		/*
		   TRC_DEBUG(MAL_SERVER,
		   "%x %x %x %x => %x %x %x %x  %x %x %x %x\n",
		   m[0], m[1], m[2], m[3], val1->q1, val1->q2,
		   val1->q3, val1->q4, val2->q1, val2->q2, val2->q3,
		   val2->q4);
		 */

		if ((val1->q1 & m[0]) == (val2->q1 & m[0]) &&
			(val1->q2 & m[1]) == (val2->q2 & m[1]) &&
			(val1->q3 & m[2]) == (val2->q3 & m[2]) &&
			(val1->q4 & m[3]) == (val2->q4 & m[3])) {
			*retval = 1;
		} else {
			*retval = 0;
		}

		/* example: (hex notation)
		 * inet1: 10.0.0.0/24
		 * IP1:   10 00 00 00
		 * mask1: ff ff ff 00
		 * &1:    10 00 00 00
		 * inet2: 10.0.0.254
		 * IP2:   10 00 00 ef
		 * mask1: ff ff ff 00
		 * &2:    10 00 00 00
		 * &1 and &2 are equal, so inet2 is within inet1
		 */
	}
	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is contained within or equal to val2
 */
static str
INET_comp_CWE(bit *retval, const inet *val1, const inet *val2)
{
	bit ret;

	/* use existing code, not fully optimal, but cheap enough */
	INET_comp_CW(&ret, val1, val2);
	if (!ret)
		INET_comp_EQ(&ret, val1, val2);

	*retval = ret;
	return (MAL_SUCCEED);
}

/**
 * Returns whether val1 is contains val2
 */
static str
INET_comp_CS(bit *retval, const inet *val1, const inet *val2)
{
	/* swap the input arguments and call the contained within function */
	return (INET_comp_CW(retval, val2, val1));
}

/**
 * Returns whether val1 contains or is equal to val2
 */
static str
INET_comp_CSE(bit *retval, const inet *val1, const inet *val2)
{
	/* swap the input arguments and call the contained within function */
	return (INET_comp_CWE(retval, val2, val1));
}


static int
INETcompare(const void *L, const void *R)
{
	const inet *l = L, *r = R;
	bit res = 0;
	if (is_inet_nil(l))
		return is_inet_nil(r) ? 0 : -1;
	if (is_inet_nil(r))
		return 1;
	INET_comp_EQ(&res, l, r);
	if (res)
		return 0;
	INET_comp_LT(&res, l, r);
	if (res)
		return -1;
	return 1;
}

/* === Functions === */
/**
 * Returns the broadcast address for the network the inet represents.
 * If the subnet mask is 32, the given input inet is returned.
 */
static str
INETbroadcast(inet *retval, const inet *val)
{
	*retval = *val;
	if (!is_inet_nil(val) && val->mask != 32) {
		unsigned int mask;
		unsigned char m[4];

		if (val->mask > 0)
			mask = ~0U << (32 - val->mask);
		else
			mask = 0;

		mask = ~mask;			/* invert the mask */
		m[0] = (mask >> 24) & 0xFF;
		m[1] = (mask >> 16) & 0xFF;
		m[2] = (mask >> 8) & 0xFF;
		m[3] = mask & 0xFF;

		/*
		   TRC_DEBUG(MAL_SERVER,
		   "%x %x %x %x => %x %x %x %x\n",
		   m[0], m[1], m[2], m[3], val->q1, val->q2,
		   val->q3, val->q4);
		 */

		/* apply the inverted mask, so we get the broadcast */
		retval->q1 |= m[0];
		retval->q2 |= m[1];
		retval->q3 |= m[2];
		retval->q4 |= m[3];

		/* example: (hex notation)
		 * inet: 10.0.0.1/24
		 * IP:   10 00 00 01
		 * mask: 00 00 00 ff
		 * &:    10 00 00 ff
		 * results in 10.0.0.255
		 */
	}
	return (MAL_SUCCEED);
}

/**
 * Extract only the IP address as text.  Unlike the toString function,
 * this function never returns the netmask length.
 */
static str
INEThost(str *retval, const inet *val)
{
	str ip;

	if (is_inet_nil(val)) {
		*retval = GDKstrdup(str_nil);
		if (*retval == NULL)
			throw(MAL, "INEThost", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		ip = GDKmalloc(sizeof(char) * 16);
		if (ip == NULL)
			throw(MAL, "INEThost", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		sprintf(ip, "%d.%d.%d.%d", val->q1, val->q2, val->q3, val->q4);
		*retval = ip;
	}
	return (MAL_SUCCEED);
}

/**
 * Extract netmask length.
 */
static str
INETmasklen(int *retval, const inet *val)
{
	if (is_inet_nil(val)) {
		*retval = int_nil;
	} else {
		*retval = val->mask;
	}
	return (MAL_SUCCEED);
}

/**
 * Set netmask length for inet value.
 */
static str
INETsetmasklen(inet *retval, const inet *val, const int *mask)
{
	if (*mask < 0 || *mask > 32)
		throw(ILLARG, "inet.setmask", "Illegal netmask length value: %d",
			  *mask);

	*retval = *val;
	if (!is_inet_nil(val))
		retval->mask = *mask;

	return (MAL_SUCCEED);
}

/**
 * Construct netmask for network.
 */
static str
INETnetmask(inet *retval, const inet *val)
{
	*retval = *val;
	if (!is_inet_nil(val)) {
		unsigned int mask;
		unsigned char m[4];

		if (val->mask > 0)
			mask = ~0U << (32 - val->mask);
		else
			mask = 0;

		m[0] = (mask >> 24) & 0xFF;
		m[1] = (mask >> 16) & 0xFF;
		m[2] = (mask >> 8) & 0xFF;
		m[3] = mask & 0xFF;

		retval->q1 = m[0];
		retval->q2 = m[1];
		retval->q3 = m[2];
		retval->q4 = m[3];
		retval->mask = 32;

		/* example: (hex notation)
		 * inet: 10.0.0.1/24
		 * mask: ff ff ff 00
		 * results in 255.255.255.0
		 */
	}
	return (MAL_SUCCEED);
}

/**
 * Construct host mask for network.
 */
static str
INEThostmask(inet *retval, const inet *val)
{
	INETnetmask(retval, val);
	/* invert the netmask to obtain the host mask */
	if (!is_inet_nil(retval)) {
		retval->q1 = ~retval->q1;
		retval->q2 = ~retval->q2;
		retval->q3 = ~retval->q3;
		retval->q4 = ~retval->q4;
	}

	/* example: (hex notation)
	 * netmask: 255.255.255.0
	 * IP:      ff ff ff 00
	 * ~:       00 00 00 ff
	 * results in 0.0.0.255
	 */

	return (MAL_SUCCEED);
}

/**
 * Extract network part of address, returns the same inet if the netmask
 * is equal to 32.  This function basically zeros out values that are
 * not covered by the netmask.
 */
static str
INETnetwork(inet *retval, const inet *val)
{
	*retval = *val;
	if (!is_inet_nil(val)) {
		unsigned int mask;
		unsigned char m[4];

		if (val->mask > 0)
			mask = ~0U << (32 - val->mask);
		else
			mask = 0;

		m[0] = (mask >> 24) & 0xFF;
		m[1] = (mask >> 16) & 0xFF;
		m[2] = (mask >> 8) & 0xFF;
		m[3] = mask & 0xFF;

		retval->q1 &= m[0];
		retval->q2 &= m[1];
		retval->q3 &= m[2];
		retval->q4 &= m[3];

		/* example: (hex notation)
		 * inet: 10.0.0.1/24
		 * IP:   10 00 00 01
		 * mask: ff ff ff 00
		 * &:    10 00 00 00
		 * results in 10.0.0.0/24
		 */
	}
	return (MAL_SUCCEED);
}

/**
 * Extract IP address and netmask length as text.  Unlike the toStr
 * function, this function always prints the netmask length.
 */
static str
INETtext(str *retval, const inet *val)
{
	str ip;

	if (is_inet_nil(val)) {
		*retval = GDKstrdup(str_nil);
		if (*retval == NULL)
			throw(MAL, "INETtext", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		ip = GDKmalloc(sizeof(char) * 20);
		if (ip == NULL)
			throw(MAL, "INETtext", SQLSTATE(HY013) MAL_MALLOC_FAIL);

		snprintf(ip, sizeof(char) * 20, "%d.%d.%d.%d/%d",
				 val->q1, val->q2, val->q3, val->q4, val->mask);
		*retval = ip;
	}
	return (MAL_SUCCEED);
}

/**
 * Abbreviated display format as text.  The abbreviation is only made if
 * the value has no bits set to right of mask.  Otherwise the return of
 * this function is equal to the function text.
 */
static str
INETabbrev(str *retval, const inet *val)
{
	str ip;

	if (is_inet_nil(val)) {
		*retval = GDKstrdup(str_nil);
		if (*retval == NULL)
			throw(MAL, "inet.abbrev", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	} else {
		unsigned int mask;
		unsigned char m[4];

		if (val->mask > 0)
			mask = ~0U << (32 - val->mask);
		else
			mask = 0;
		mask = ~mask;			/* invert the mask */

		m[0] = (mask >> 24) & 0xFF;
		m[1] = (mask >> 16) & 0xFF;
		m[2] = (mask >> 8) & 0xFF;
		m[3] = mask & 0xFF;

		if ((val->q1 & m[0]) != 0 ||
			(val->q2 & m[1]) != 0 ||
			(val->q3 & m[2]) != 0 || (val->q4 & m[3]) != 0) {
			mask = 32;
		} else {
			mask = val->mask;
		}

		/* example: (hex notation)
		 * inet: 10.1.0.0/16
		 * IP:   10 01 00 00
		 * mask: 00 00 ff ff
		 * &:    00 00 00 00
		 * all zero, thus no bits on the right side of the mask
		 */

		ip = GDKmalloc(sizeof(char) * 20);
		if (ip == NULL)
			throw(MAL, "inet.abbrev", SQLSTATE(HY013) MAL_MALLOC_FAIL);

		if (mask > 24) {
			snprintf(ip, sizeof(char) * 20, "%d.%d.%d.%d/%d",
					 val->q1, val->q2, val->q3, val->q4, val->mask);
		} else if (mask > 16) {
			snprintf(ip, sizeof(char) * 20, "%d.%d.%d/%d",
					 val->q1, val->q2, val->q3, val->mask);
		} else if (mask > 8) {
			snprintf(ip, sizeof(char) * 20, "%d.%d/%d",
					 val->q1, val->q2, val->mask);
		} else if (mask > 0) {
			snprintf(ip, sizeof(char) * 20, "%d/%d", val->q1, val->mask);
		} else {
			snprintf(ip, sizeof(char) * 20, "/0");
		}

		*retval = ip;
	}
	return (MAL_SUCCEED);
}

static str
INET_inet(inet *d, const inet *s)
{
	*d = *s;
	return MAL_SUCCEED;
}

static str
INET_fromstr(inet *ret, const char *const *s)
{
	size_t len = sizeof(inet);
	if (INETfromString(*s, &len, (void **) &ret, false) < 0)
		throw(MAL, "inet.inet", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

static const void *
INETnull(void)
{
	return &inet_nil;
}

#include "mel.h"
mel_atom inet_init_atoms[] = {
 { .name="inet", .basetype="lng", .size=sizeof(inet), .null=INETnull, .cmp=INETcompare, .fromstr=INETfromString, .tostr=INETtoString, },  { .cmp=NULL }
};
mel_func inet_init_funcs[] = {
 command("inet", "new", INETnew, false, "Create an inet from a string literal", args(1,2, arg("",inet),arg("s",str))),
 command("inet", "isnil", INET_isnil, false, "Nil test for inet value", args(1,2, arg("",bit),arg("v",inet))),
 command("inet", "=", INET_comp_EQ, false, "Equality of two inets", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "!=", INET_comp_NEQ, false, "Inequality of two inets", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "<", INET_comp_LT, false, "Whether v is less than w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", ">", INET_comp_GT, false, "Whether v is greater than w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "<=", INET_comp_LE, false, "Whether v is less than or equal to w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", ">=", INET_comp_GE, false, "Whether v is equal to or greater than w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "<<", INET_comp_CW, false, "Whether v is contained within w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "<<=", INET_comp_CWE, false, "Whether v is contained within or is equal to w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", ">>", INET_comp_CS, false, "Whether v contains w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", ">>=", INET_comp_CSE, false, "Whether v contains or is equal to w", args(1,3, arg("",bit),arg("v",inet),arg("w",inet))),
 command("inet", "broadcast", INETbroadcast, false, "Returns the broadcast address for network", args(1,2, arg("",inet),arg("",inet))),
 command("inet", "host", INEThost, false, "Extract IP address as text", args(1,2, arg("",str),arg("",inet))),
 command("inet", "masklen", INETmasklen, false, "Extract netmask length", args(1,2, arg("",int),arg("",inet))),
 command("inet", "setmasklen", INETsetmasklen, false, "Set netmask length for inet value", args(1,3, arg("",inet),arg("",inet),arg("",int))),
 command("inet", "netmask", INETnetmask, false, "Construct netmask for network", args(1,2, arg("",inet),arg("",inet))),
 command("inet", "hostmask", INEThostmask, false, "Construct host mask for network", args(1,2, arg("",inet),arg("",inet))),
 command("inet", "network", INETnetwork, false, "Extract network part of address", args(1,2, arg("",inet),arg("",inet))),
 command("inet", "text", INETtext, false, "Extract IP address and netmask length as text", args(1,2, arg("",str),arg("",inet))),
 command("inet", "abbrev", INETabbrev, false, "Abbreviated display format as text", args(1,2, arg("",str),arg("",inet))),
 command("calc", "inet", INET_inet, false, "Convert a inet to an inet", args(1,2, arg("",inet),arg("s",inet))),
 command("calc", "inet", INET_fromstr, false, "Convert a string to an inet", args(1,2, arg("",inet),arg("s",str))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_inet_mal)
{ mal_module("inet", inet_init_atoms, inet_init_funcs); }
