/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

#include <sql_config.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <strings.h>	/* for strcasecmp() */
#ifdef HAVE_PWD_H
#include <pwd.h>
#endif

#ifdef HAVE_LIMITS_H 
#include <limits.h>	/* PATH_MAX */
#endif

#ifndef PATH_MAX
#define PATH_MAX	256
#endif

/* this does not do any reallocation */
static void trim_spaces(char *str)
{
	static const char *start_crap = " \t";
	static const char *end_crap = " \t\r\n";
	int start = 0, end = strlen(str) - 1;
	while (strchr(start_crap, str[start]) != NULL)
		start++;
	while (end >= 0 && strchr(end_crap, str[end]) != NULL)
		end--;

	if (start <= end) {
		memmove(str, &str[start], end - start + 1);
		str[end - start + 1] = 0;
	} else {
		str[0] = 0;
	}
}

/* Searches for a section, returns non-zero on success
 * If successeful, positions on the line after the section
 * name.
 */
static int find_section(FILE * ini, const char *section)
{
	char buf[1024];
	int found = 0;
	int pos;
	char *r;

	while (!found && (r = fgets(buf, 1024, ini)) != NULL) {
		/* find first meaningful char */
		pos = 0;
		while (buf[pos] == ' ' || buf[pos] == '\t') {
			pos++;
		}

		if (buf[pos] == 0 || buf[pos] == ';' || buf[pos] == '#') {
			/* comment or empty line */
			continue;
		}

		if (buf[pos] == '[') {
			char *end = strchr(&buf[++pos], ']');

			if (end) {
				int len = end - &buf[pos];
				char *sect_name = (char *) malloc(len + 1);
				strncpy(sect_name, &buf[pos], len);
				sect_name[len] = 0;
				trim_spaces(sect_name);
				found = !strcasecmp(sect_name, section);
				free(sect_name);
			}
		}
	}
	return found;
}

/* Returns NULL if not found, otherwise result must be freed
 * Expects ini to be positioned at the beginning of a section
 */
static char *find_key(FILE * ini, const char *key)
{
	char buf[1024];
	int pos;
	char *r = NULL;
	char *tmp;
	char *value = NULL;

	/* if any of those starts a line, the line is ignored */
	static const char *skipchars = ";#=\r\n";

	while (!value && (r = fgets(buf, 1024, ini)) != NULL) {
		pos = 0;

		/* skip crap at beginning */
		while (buf[pos] == ' ' || buf[pos] == '\t') {
			pos++;
		}

		if (buf[pos] == 0 || strchr(skipchars, buf[pos]) != NULL) {
			/* comment or empty line */
			continue;
		}

		if (buf[pos] == '[') {
			/* new section */
			break;
		}

		tmp = strchr(buf, '=');
		if (tmp) {
			char *name = (char *) malloc((tmp - buf + 1));
			strncpy(name, buf, tmp - buf);
			name[tmp - buf] = 0;
			trim_spaces(name);

			if (!strcasecmp(name, key)) {
				tmp++;
				value = (char *) malloc(strlen(tmp) + 1);
				strcpy(value, tmp);
				trim_spaces(value);
			}

			free(name);
		}
	}

	return value;
}

/* First look for key in the user ini file, then in system.
 * Do not mix up values from both.
 */

static char *find_key_by_dsn(const char *dsn, const char *key,
			     const char *file)
{
	char buf[PATH_MAX];
	FILE *fp;

	char *ret = NULL;

#ifdef HAVE_PWD_H
	if (file[0] == '~') {
		struct passwd *pwd;
		pwd = getpwuid(getuid());
		if (!pwd) {
			return NULL;
		}
		snprintf(buf, PATH_MAX, "%s%s", pwd->pw_dir, file + 1);
		file = buf;
	}
#endif 
	fp = fopen(file, "r");

	if (fp && find_section(fp, dsn)) {
		ret = find_key(fp, key);
	}

	if (fp)
		fclose(fp);

	return ret;
}



int __SQLGetPrivateProfileString
    (char *section, char *entry, char *def_value,
     char *buf, int buf_len, char *filename) {
	char *value;
	char *src;

	if (buf == NULL || buf_len <= 1) {
		/* no need to bother */
		return -1;
	}

	if (section == NULL || entry == NULL) {
		return -1;
	}

	value = find_key_by_dsn(section, entry, filename);

	if (value) {
		src = value;
	} else {
		src = def_value;
	}

	if (src) {
		strncpy(buf, src, buf_len - 1);
		buf[buf_len - 1] = 0;
	} else {
		buf[0] = 0;
	}

	if (value) {
		free(value);
	}

	return strlen(buf);
}
