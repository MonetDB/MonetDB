/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/**
 * utils
 * Fabian Groffen
 * Shared utility functions between merovingian and monetdb
 */

/* NOTE: for this file to work correctly, the random number generator
 * must have been seeded (srand) with something like the current time */

#include "monetdb_config.h"
#include "utils.h"
#include <stdio.h> /* fprintf, fgets */
#include <unistd.h> /* unlink */
#include <string.h> /* memcpy */
#include <strings.h> /* strcasecmp */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif
#ifdef HAVE_OPENSSL
#include <openssl/rand.h>		/* RAND_bytes */
#else
#ifdef HAVE_COMMONCRYPTO
#include <CommonCrypto/CommonCrypto.h>
#include <CommonCrypto/CommonRandom.h>
#endif
#endif

/**
 * Parses the given file stream matching the keys from list.  If a match
 * is found, the value is set in list->value.  Values are malloced.
 */
void
readConfFile(confkeyval *list, FILE *cnf) {
	char buf[1024];
	confkeyval *t;
	size_t len;
	char *err;

	while (fgets(buf, 1024, cnf) != NULL) {
		/* eliminate fgets' newline */
		buf[strlen(buf) - 1] = '\0';
		for (t = list; t->key != NULL; t++) {
			len = strlen(t->key);
			if (*buf && strncmp(buf, t->key, len) == 0 && buf[len] == '=') {
				if ((err = setConfVal(t, buf + len + 1)) != NULL)
					free(err); /* ignore, just fall back to default */
			}
		}
	}
}

/**
 * Parses the given file stream matching the and writes all values to the list.
 */
void
readConfFileFull(confkeyval *list, FILE *cnf) {
	char buf[1024];
	char *key, *val;
	char *separator = "=";
	char *err;
	confkeyval *t = list;

	/* iterate until the end of the array */
	while (list->key != NULL) {
		list++;
	}
	/* read the file a line at a time */
	while (fgets(buf, sizeof(buf), cnf) != NULL) {
		if (strlen(buf) > 1 && buf[0] != '#') {
			/* tokenize */
			key = strtok(buf, separator);
			val = strtok(NULL, separator);
			/* strip trailing newline */
			val = strtok(val, "\n");
			/* check if it is default property or not. those are set in a special way */
			if (defaultProperty(key)) {
				if ((err = setConfValForKey(t, key, val)) != NULL) {
					free(err); /* ignore, just fall back to default */
				}
			} else {
				list->key = strdup(key);
				list->val = strdup(val);
				list->ival = 0;
				list->type = STR;
				list++;
			}
		}
	}
}

/**
 * Frees the values allocated by readConfFile().
 */
inline void
freeConfFile(confkeyval *list) {
	while (list->key != NULL) {
		if (list->val != NULL) {
			free(list->val);
			list->val = NULL;
		}
		list++;
	}
}

/**
 * Returns true if the key is a default property.
 */
int
defaultProperty(const char *property) {
	if (property == NULL)
		return 0;
	return strcmp(property, "type") == 0 ||
		strcmp(property, "shared") == 0 ||
		strcmp(property, "nthreads") == 0 ||
		strcmp(property, "readonly") == 0 ||
		strcmp(property, "nclients") == 0 ||
		strcmp(property, "mfunnel") == 0 ||
		strcmp(property, "embedr") == 0 ||
		strcmp(property, "embedpy") == 0 ||
		strcmp(property, "embedpy3") == 0 ||
		strcmp(property, "optpipe") == 0;
}

/**
 * Returns a pointer to the key-value that has a matching key with the
 * given key, or NULL if no key was found.
 */
inline confkeyval *
findConfKey(confkeyval *list, const char *key) {
	while (list->key != NULL) {
		if (strcmp(list->key, key) == 0)
			return(list);
		list++;
	}
	return(NULL);
}

/**
 * Returns a pointer to the value for the given key, or NULL if not
 * found (or set to NULL)
 */
inline char *
getConfVal(confkeyval *list, const char *key) {
	while (list->key != NULL) {
		if (strcmp(list->key, key) == 0)
			return(list->val);
		list++;
	}
	return(NULL);
}

/**
 * Returns the int-representation of the value for the given key, or
 * 0 if not found.
 */
inline int
getConfNum(confkeyval *list, const char *key) {
	while (list->key != NULL) {
		if (strcmp(list->key, key) == 0)
			return(list->ival);
		list++;
	}
	return(0);
}

/**
 * Sets the value in the given confkeyval struct to val ensuring it is
 * of the desired type.  In case of type BOOL, val is converted to "yes"
 * or "no", based on val.  If the type does not match, this function
 * returns a malloced diagnostic message, or if everything is
 * successful, NULL.  If val is NULL, this function always returns
 * successful and unsets the value for the given key.  Upon an error,
 * the original value for the key is left untouched.
 */
char *
setConfVal(confkeyval *ckv, const char *val) {
	int ival = 0;

	/* handle the unset directly */
	if (val == NULL) {
		if (ckv->val != NULL) {
			free(ckv->val);
			ckv->val = NULL;
			ckv->ival = 0;
		}
		return(NULL);
	}

	/* check the input */
	switch (ckv->type) {
		case INVALID: {
			char buf[256];
			snprintf(buf, sizeof(buf),
					"key '%s' is unitialised (invalid value), internal error",
					ckv->key);
			return(strdup(buf));
		}
		case INT: {
			const char *p = val;
			while (*p >= '0' && *p <= '9')
				p++;
			if (*p != '\0') {
				char buf[256];
				snprintf(buf, sizeof(buf),
						"key '%s' requires an integer-type value, got: %s",
						ckv->key, val);
				return(strdup(buf));
			}
			ival = atoi(val);
		}; break;
		case BOOLEAN: {
			if (strcasecmp(val, "true") == 0 ||
					strcasecmp(val, "yes") == 0 ||
					strcmp(val, "1") == 0)
			{
				val = "yes";
				ival = 1;
			} else if (strcasecmp(val, "false") == 0 ||
					strcasecmp(val, "no") == 0 ||
					strcmp(val, "0") == 0)
			{
				val = "no";
				ival = 0;
			} else {
				char buf[256];
				snprintf(buf, sizeof(buf),
						"key '%s' requires a boolean-type value, got: %s",
						ckv->key, val);
				return(strdup(buf));
			}
		}; break;
		case MURI: {
			if (strncmp(val, "mapi:monetdb://",
						sizeof("mapi:monetdb://") -1) != 0)
			{
				char buf[256];
				snprintf(buf, sizeof(buf),
						"key '%s' requires a mapi:monetdb:// URI value, got: %s",
						ckv->key, val);
				return(strdup(buf));
			}
			/* TODO: check full URL? */
		}; break;
		case STR:
		case OTHER:
			/* leave as is, not much to check */
		break;
	}
	if (ckv->val != NULL)
		free(ckv->val);
	ckv->val = strdup(val);
	ckv->ival = ival;

	return(NULL);
}

char *
setConfValForKey(confkeyval *list, const char *key, const char *val) {
	char buf[256];

	while (list->key != NULL) {
		if (strcmp(list->key, key) == 0) {
			return setConfVal(list, val);
		}
		list++;
	}
	snprintf(buf, sizeof(buf), "key '%s' is not recognized, internal error", key);
	return(strdup(buf));
}

/**
 * Fills the array pointed to by buf with a human representation of t.
 * The argument longness represents the number of units to print
 * starting from the biggest unit that has a non-zero value for t.
 */
inline void
secondsToString(char *buf, time_t t, int longness)
{
	time_t p;
	size_t i = 0;

	p = 1 * 60 * 60 * 24 * 7 * 52;
	if (t > p) {
		i += sprintf(buf + i, "%dy", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 52;
	if (t > p) {
		i += sprintf(buf + i, "%dw", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 7;
	if (t > p) {
		i += sprintf(buf + i, "%dd", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 24;
	if (t > p) {
		i += sprintf(buf + i, "%dh", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 60;
	if (t > p) {
		i += sprintf(buf + i, "%dm", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}

	/* t must be < 60 */
	if (--longness == 0 || !(i > 0 && t == 0)) {
		sprintf(buf + i, "%ds", (int)(t));
	} else {
		buf[--i] = '\0';
	}
}

/**
 * Fills the array pointed to by ret, with the string from in,
 * abbreviating it when it is longer than width chars long.
 * The array pointed to by ret must be at least of size width + 1.
 */
inline void
abbreviateString(char *ret, const char *in, size_t width)
{
	size_t len;
	size_t off;

	if ((len = strlen(in)) > width) {
		/* position abbreviation dots in the middle (Mac style, iso
		 * Windows style) */
		memcpy(ret, in, (width / 2) - 2);
		memcpy(ret + (width / 2) - 2, "...", 3);
		off = len - (width - ((width / 2) - 2) - 3);
		memcpy(ret + (width / 2) + 1, in + off, (len - off) + 1);
	} else {
		sprintf(ret, "%s", in);
	}
}

static char seedChars[] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
	'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
	'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

/**
 * Fills the array pointed to by buf of size len with a random salt.
 * Padds the remaining bytes in buf with null-bytes.
 */
void
generateSalt(char *buf, unsigned int len)
{
	unsigned int c;
	unsigned int size;
	unsigned int fill;
	unsigned int min;

#ifdef HAVE_OPENSSL
	if (RAND_bytes((unsigned char *) &size, (int) sizeof(size)) < 0)
#else
#ifdef HAVE_COMMONCRYPTO
	if (CCRandomGenerateBytes(&size, sizeof(size)) != kCCSuccess)
#endif
#endif
#ifndef STATIC_CODE_ANALYSIS
		size = (unsigned int)rand();
#else
		size = 0;
#endif
	fill = len * 0.75;
	min = len * 0.42;
	size = (size % (fill - min)) + min;
#ifdef HAVE_OPENSSL
	if (RAND_bytes((unsigned char *) buf, (int) size) >= 0) {
		for (c = 0; c < size; c++)
			buf[c] = seedChars[((unsigned char *) buf)[c] % 62];
	} else
#else
#ifdef HAVE_COMMONCRYPTO
	if (CCRandomGenerateBytes(buf, size) >= 0) {
		for (c = 0; c < size; c++)
			buf[c] = seedChars[((unsigned char *) buf)[c] % 62];
	} else
#endif
#endif
		for (c = 0; c < size; c++) {
#ifndef STATIC_CODE_ANALYSIS
			buf[c] = seedChars[rand() % 62];
#else
			buf[c] = seedChars[0];
#endif
		}
	for ( ; c < len; c++)
		buf[c] = '\0';
}

/**
 * Creates a file path read/writable for the user only containing a
 * random passphrase.
 */
char *
generatePassphraseFile(const char *path)
{
	int fd;
	FILE *f;
	char buf[48];
	unsigned int len = sizeof(buf);

	/* delete such that we are sure we recreate the file with restricted
	 * permissions */
	unlink(path);
	if ((fd = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR)) == -1) {
		char err[512];
		snprintf(err, sizeof(err), "unable to open '%s': %s",
				path, strerror(errno));
		return(strdup(err));
	}

	generateSalt(buf, len);
	if ((f = fdopen(fd, "w")) == NULL) {
		char err[512];
		snprintf(err, sizeof(err), "unable to open '%s': %s",
				path, strerror(errno));
		close(fd);
		return(strdup(err));
	}
	if (fwrite(buf, 1, len, f) < len) {
		char err[512];
		snprintf(err, sizeof(err), "cannot write secret: %s",
				strerror(errno));
		fclose(f);
		return(strdup(err));
	}
	fclose(f);
	return(NULL);
}

void
sleep_ms(size_t ms)
{
	struct timeval tv;

	tv.tv_sec = ms / 1000;
	tv.tv_usec = 1000 * (ms % 1000);
	(void) select(0, NULL, NULL, NULL, &tv);
}

/* vim:set ts=4 sw=4 noexpandtab: */
