/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * @f monet_options
 * @a N.J. Nes
 * @* A simple option handling library
 * @T
 * The monet server and clients make use of command line options and a (possibly)
 * shared config file. With this library a set (represented by set,setlen) of
 * options is created. An option is stored as name and value strings with a
 * special flag indicating the origin of the options, (builtin, system config
 * file, special config file or command line option).
 *
 */
#include "monetdb_config.h"
#include "monet_options.h"
#ifndef HAVE_GETOPT_LONG
#  include "monet_getopt.h"
#else
# ifdef HAVE_GETOPT_H
#  include "getopt.h"
# endif
#endif
#include <string.h>
#include <ctype.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifndef HAVE_GETOPT_LONG
#  include "getopt.c"
#  include "getopt1.c"
#endif

#ifdef NATIVE_WIN32
#define getpid _getpid
#endif

/* these two are used if the set parameter passed into functions is NULL */
static int default_setlen = 0;
static opt *default_set = NULL;

static int
mo_default_set(opt **Set, int setlen)
{
	if (*Set == NULL) {
		if (default_set == NULL) {
			default_setlen = mo_builtin_settings(&default_set);
			default_setlen = mo_system_config(&default_set, default_setlen);
		}
		*Set = default_set;
		setlen = default_setlen;
	}
	return setlen;
}

void
mo_print_options(opt *set, int setlen)
{
	int i = 0;

	setlen = mo_default_set(&set, setlen);
	for (i = 0; i < setlen; i++) {
		if (set[i].kind == opt_builtin) {
			fprintf(stderr, "# builtin opt \t%s = %s\n", set[i].name, set[i].value);
		}
	}
	for (i = 0; i < setlen; i++) {
		if (set[i].kind == opt_config) {
			fprintf(stderr, "# config opt \t%s = %s\n", set[i].name, set[i].value);
		}
	}
	for (i = 0; i < setlen; i++) {
		if (set[i].kind == opt_cmdline) {
			fprintf(stderr, "# cmdline opt \t%s = %s\n", set[i].name, set[i].value);
		}
	}
}


char *
mo_find_option(opt *set, int setlen, const char *name)
{
	opt *o = NULL;
	int i;

	setlen = mo_default_set(&set, setlen);
	for (i = 0; i < setlen; i++) {
		if (strcmp(set[i].name, name) == 0)
			if (!o || o->kind < set[i].kind)
				o = set + i;
	}
	if (o)
		return o->value;
	return NULL;
}

static int
mo_config_file(opt **Set, int setlen, char *file)
{
	char buf[BUFSIZ];
	FILE *fd = NULL;
	opt *set;

	if (Set == NULL) {
		if (default_set == NULL) {
			set = NULL;
			setlen = mo_default_set(&set, 0);
		} else
			setlen = default_setlen;
		Set = &default_set;
	}
	set = *Set;
	fd = fopen(file, "r");
	if (fd == NULL) {
		fprintf(stderr, "Could not open file %s\n", file);
		return setlen;
	}
	while (fgets(buf, BUFSIZ, fd) != NULL) {
		char *s, *t, *val;
		int quote;

		for (s = buf; *s && isspace((unsigned char) *s); s++)
			;
		if (*s == '#')
			continue;	/* commentary */
		if (*s == 0)
			continue;	/* empty line */

		val = strchr(s, '=');
		if (val == NULL) {
			fprintf(stderr, "mo_config_file: syntax error in %s at %s\n", file, s);
			break;
		}
		*val = 0;

		for (t = s; *t && !isspace((unsigned char) *t); t++)
			;
		*t = 0;

		/* skip any leading blanks in the value part */
		for (val++; *val && isspace((unsigned char) *val); val++)
			;

		/* search to unquoted # */
		quote = 0;
		for (t = val; *t; t++) {
			if (*t == '"')
				quote = !quote;
			else if (!quote && *t == '#')
				break;
		}
		if (quote) {
			fprintf(stderr, "mo_config_file: wrong number of quotes in %s at %s\n", file, val);
			break;
		}
		/* remove trailing white space */
		while (isspace((unsigned char) t[-1]))
			t--;
		*t++ = 0;

		/* treat value as empty if it consists only of white space */
		if (t <= val)
			val = t - 1;

		opt *tmp = realloc(set, (setlen + 1) * sizeof(opt));
		if (tmp == NULL)
			break;
		*Set = set = tmp;
		set[setlen].kind = opt_config;
		set[setlen].name = strdup(s);
		set[setlen].value = malloc((size_t) (t - val));
		if (set[setlen].name == NULL || set[setlen].value == NULL) {
			free(set[setlen].name);
			free(set[setlen].value);
			break;
		}
		for (t = val, s = set[setlen].value; *t; t++)
			if (*t != '"')
				*s++ = *t;
		*s = 0;
		setlen++;
	}
	(void) fclose(fd);
	return setlen;
}

int
mo_system_config(opt **Set, int setlen)
{
	char *cfg;

	if (Set == NULL) {
		if (default_set == NULL) {
			opt *set = NULL;

			setlen = mo_default_set(&set, 0);
		} else
			setlen = default_setlen;
		Set = &default_set;
	}
	cfg = mo_find_option(*Set, setlen, "config");
	if (!cfg)
		return setlen;
	setlen = mo_config_file(Set, setlen, cfg);
	return setlen;
}

int
mo_builtin_settings(opt **Set)
{
	int i = 0;
	opt *set;

	if (Set == NULL)
		return 0;

#define N_OPTIONS	5	/*MUST MATCH # OPTIONS BELOW */
	set = malloc(sizeof(opt) * N_OPTIONS);
	if (set == NULL)
		return 0;

	*Set = set;
	set[i].kind = opt_builtin;
	set[i].name = strdup("gdk_dbpath");
	set[i].value = strdup(LOCALSTATEDIR DIR_SEP_STR "monetdb5" DIR_SEP_STR
			      "dbfarm" DIR_SEP_STR "demo");
	if (set[i].name == NULL || set[i].value == NULL) {
		free(set[i].name);
		free(set[i].value);
		return i;
	}
	i++;
	set[i].kind = opt_builtin;
	set[i].name = strdup("mapi_port");
	set[i].value = strdup(MAPI_PORT_STR);
	if (set[i].name == NULL || set[i].value == NULL) {
		free(set[i].name);
		free(set[i].value);
		return i;
	}
	i++;
	set[i].kind = opt_builtin;
	set[i].name = strdup("sql_optimizer");
	set[i].value = strdup("default_pipe");
	if (set[i].name == NULL || set[i].value == NULL) {
		free(set[i].name);
		free(set[i].value);
		return i;
	}
	i++;
	set[i].kind = opt_builtin;
	set[i].name = strdup("sql_debug");
	set[i].value = strdup("0");
	if (set[i].name == NULL || set[i].value == NULL) {
		free(set[i].name);
		free(set[i].value);
		return i;
	}
	i++;
	set[i].kind = opt_builtin;
	set[i].name = strdup("raw_strings");
	set[i].value = strdup("false");
	if (set[i].name == NULL || set[i].value == NULL) {
		free(set[i].name);
		free(set[i].value);
		return i;
	}
	i++;

	assert(i == N_OPTIONS);
	return i;
}

int
mo_add_option(opt **Set, int setlen, opt_kind kind, const char *name, const char *value)
{
	opt *set;

	if (Set == NULL) {
		if (default_set == NULL) {
			set = NULL;
			setlen = mo_default_set(&set, 0);
		} else
			setlen = default_setlen;
		Set = &default_set;
	}
	opt *tmp = (opt *) realloc(*Set, (setlen + 1) * sizeof(opt));
	if (tmp == NULL)
		return setlen;
	*Set = set = tmp;
	set[setlen].kind = kind;
	set[setlen].name = strdup(name);
	set[setlen].value = strdup(value);
	if (set[setlen].name == NULL || set[setlen].value == NULL) {
		free(set[setlen].name);
		free(set[setlen].value);
		return setlen;
	}
	return setlen + 1;
}

void
mo_free_options(opt *set, int setlen)
{
	int i;

	if (set == NULL) {
		set = default_set;
		setlen = default_setlen;
		default_set = NULL;
		default_setlen = 0;
	}
	for (i = 0; i < setlen; i++) {
		if (set[i].name)
			free(set[i].name);
		if (set[i].value)
			free(set[i].value);
	}
	free(set);
}
